"""kvbench CLI: benchmark a Redis-compatible KV store.

Usage:
    python -m kvbench.cli --host 127.0.0.1 --port 6379 --ops 20000 \\
        --clients 4 --output report.md
    python -m kvbench.cli --baseline --ops 20000 --output report.md
    python -m kvbench.cli --host 127.0.0.1 --port 6379 --baseline \\
        --ops 20000 --output report.md
    # same workload against a second server, side by side in one report
    python -m kvbench.cli --host 127.0.0.1 --port 6379 \\
        --compare 127.0.0.1:6380 --ops 20000 --output report.md
    # fixed wall-clock run instead of a fixed op count
    python -m kvbench.cli --host 127.0.0.1 --port 6379 \\
        --duration 10s --clients 4 --output report.md
    # warm up for 2s before measuring, so caches and connections
    # are warm and the first seconds do not drag the percentiles down
    python -m kvbench.cli --host 127.0.0.1 --port 6379 \\
        --ops 20000 --clients 4 --warmup 2s --output report.md
    # sweep the keyspace: the same workload re-run at 100, 1000,
    # 10000 and 100000 keys, with ops/sec and p99 per size
    python -m kvbench.cli --host 127.0.0.1 --port 6379 \\
        --ops 20000 --clients 4 \\
        --sweep-keyspace 100,1000,10000,100000 --output report.md
    # sweep the client count: the same workload re-run at 1, 2, 4
    # and 8 clients, with ops/sec and p99 per count
    python -m kvbench.cli --host 127.0.0.1 --port 6379 \\
        --ops 20000 --sweep-clients 1,2,4,8 --output report.md
    # record the run in the history file, then compare later runs
    # against it (ops/sec trend over time)
    python -m kvbench.cli --host 127.0.0.1 --port 6379 \\
        --ops 20000 --clients 4 --history
    python -m kvbench.cli --show-history
    # ops/sec trend chart per backend, with sparklines, from the
    # same history file
    python -m kvbench.cli --trend
    # compare this run against a previous run's JSON report, side
    # by side, and fail the run when it regressed past 5%
    python -m kvbench.cli --host 127.0.0.1 --port 6379 \\
        --ops 20000 --clients 4 --compare-file report.json \\
        --regression-threshold 5
    # same comparison, but against a run recorded in the history
    # file instead of a report file — -1 is the most recent
    # recorded run, 0 the oldest, or give an ISO timestamp for the
    # latest run at or before that time
    python -m kvbench.cli --host 127.0.0.1 --port 6379 \\
        --ops 20000 --clients 4 --compare-history -1 \\
        --regression-threshold 5
    # same comparison, but only against a recorded run that used
    # the same workload as this run — the latest matching run per
    # backend, so the gate stays apples-to-apples when the history
    # file mixes workloads
    python -m kvbench.cli --host 127.0.0.1 --port 6379 \\
        --ops 20000 --clients 4 --compare-history -1 \\
        --compare-same-workload --regression-threshold 5
    # SLO gate: fail the run (exit 4) when p99 is above 1 ms or
    # more than 1% of ops error
    python -m kvbench.cli --host 127.0.0.1 --port 6379 \\
        --ops 20000 --clients 4 --slo-p99-ms 1 --slo-error-rate 1
    # per-operation SLO gate: SET ops get their own p99 and error
    # rate budgets, on top of the global gate
    python -m kvbench.cli --host 127.0.0.1 --port 6379 \\
        --ops 20000 --clients 4 --slo-op SET:5,0.1 \\
        --slo-op GET:2,0.1
    # SLO preset file: the thresholds live in a JSON file instead
    # of on the command line, so CI can share one budget across
    # runs; CLI flags still override the preset values
    python -m kvbench.cli --host 127.0.0.1 --port 6379 \\
        --ops 20000 --clients 4 --slo-preset slo.json
"""

import argparse
import os
import sys

from . import __version__
from .bench import DictClient, Workload, format_duration, \
    run_client_sweep, run_keyspace_sweep, run_workload
from .compare import compare_results, format_verdict, load_baseline, \
    load_history_baseline
from .history import DEFAULT_HISTORY_FILE, format_history, \
    format_trend, load_history, record_run, write_trend
from .report import _workload_dict, render_csv, write_csv, write_json, \
    write_report
from .resp import RespClient
from .slo import evaluate_slo, format_slo_verdict, load_slo_preset


def json_path_for(output_path, json_arg):
    """Resolve where the JSON report goes.

    An explicit ``--json`` path wins; otherwise the JSON report sits
    alongside the Markdown one, with the output extension swapped for
    ``.json`` (``report.md`` -> ``report.json``; ``report`` -> ``report.json``).
    """
    if json_arg:
        return json_arg
    stem, ext = os.path.splitext(output_path)
    return stem + ".json" if ext else output_path + ".json"


# Named read/write mix presets, as SET:GET:INCR weights. Reads are
# GETs, writes are SETs plus a small INCR share so counters still get
# exercised; the default 50:40:10 sits between balanced and
# write-heavy. ``--mix read-heavy`` is the same shape as
# ``--mix 80:20`` (read:write), just with INCR kept in the mix.
MIX_PRESETS = {
    "read-heavy": (15, 80, 5),
    "balanced": (45, 45, 10),
    "write-heavy": (80, 15, 5),
}


def _parse_mix(text):
    """Parse a ``--mix`` value into (set, get, incr, label).

    Accepts three forms:

    - ``SET:GET:INCR`` weights, e.g. ``50:40:10`` (label is "").
    - ``READ:WRITE`` ratio, e.g. ``80:20`` — reads are GETs, writes
      are SETs, INCR weight 0 (label is "").
    - a preset name: ``read-heavy``, ``balanced``, ``write-heavy``
      (label is the preset name, so the report can name the mix).
    """
    if text in MIX_PRESETS:
        set_w, get_w, incr_w = MIX_PRESETS[text]
        return set_w, get_w, incr_w, text
    parts = text.split(":")
    if len(parts) not in (2, 3):
        raise ValueError(
            "mix must be SET:GET:INCR, READ:WRITE, or a preset "
            "(read-heavy, balanced, write-heavy)")
    try:
        values = [int(p) for p in parts]
    except ValueError as exc:
        raise ValueError("mix weights must be integers") from exc
    if any(v < 0 for v in values) or sum(values) <= 0:
        raise ValueError("mix weights must be non-negative and sum > 0")
    if len(values) == 2:
        # READ:WRITE — reads are GETs, writes are SETs, no INCRs.
        read_w, write_w = values
        return write_w, read_w, 0, ""
    return values[0], values[1], values[2], ""


def _parse_duration(text):
    """Parse a ``--duration`` value into seconds (float).

    Accepts a bare number of seconds (``10``), seconds with an
    ``s`` suffix (``10s``), or minutes with an ``m`` suffix
    (``2m``); fractional values such as ``1.5s`` or ``0.5m`` are
    allowed. The value must be positive.
    """
    if not text:
        raise ValueError(
            "duration must be a positive length of time, e.g. 10s or 2m")
    suffix = text[-1].lower()
    if suffix in ("s", "m"):
        number, multiplier = text[:-1], 60.0 if suffix == "m" else 1.0
    else:
        number, multiplier = text, 1.0
    try:
        value = float(number) * multiplier
    except ValueError as exc:
        raise ValueError(
            "duration must be a positive length of time, e.g. 10s or 2m"
        ) from exc
    if value <= 0:
        raise ValueError("duration must be positive")
    return value


def _parse_keyspace_sweep(text):
    """Parse a ``--sweep-keyspace`` value into a sorted list of
    distinct positive ints, e.g. ``100,1000,10000`` -> [100, 1000,
    10000]. Duplicates are dropped and the sizes are sorted, so
    ``1000,100,1000`` sweeps 100 then 1000.
    """
    if not text:
        raise ValueError(
            "sweep-keyspace must list keyspace sizes, "
            "e.g. 100,1000,10000")
    sizes = []
    for part in text.split(","):
        part = part.strip()
        if not part:
            raise ValueError(
                "sweep-keyspace sizes must be comma-separated "
                "integers, e.g. 100,1000,10000")
        try:
            size = int(part)
        except ValueError as exc:
            raise ValueError(
                "sweep-keyspace sizes must be integers") from exc
        if size <= 0:
            raise ValueError("sweep-keyspace sizes must be positive")
        sizes.append(size)
    return sorted(set(sizes))


def _parse_client_sweep(text):
    """Parse a ``--sweep-clients`` value into a sorted list of
    distinct positive ints, e.g. ``1,2,4,8`` -> [1, 2, 4, 8].
    Duplicates are dropped and the counts are sorted, so
    ``4,1,4`` sweeps 1 then 4.
    """
    if not text:
        raise ValueError(
            "sweep-clients must list client counts, e.g. 1,2,4,8")
    counts = []
    for part in text.split(","):
        part = part.strip()
        if not part:
            raise ValueError(
                "sweep-clients counts must be comma-separated "
                "integers, e.g. 1,2,4,8")
        try:
            count = int(part)
        except ValueError as exc:
            raise ValueError(
                "sweep-clients counts must be integers") from exc
        if count <= 0:
            raise ValueError("sweep-clients counts must be positive")
        counts.append(count)
    return sorted(set(counts))


def _parse_slo_op(text):
    """Parse a ``--slo-op`` value into (op, p99_ms, error_rate_pct).

    Form is ``OP:P99,ERR``, e.g. ``SET:5,0.1`` — SET ops breach
    when their p99 is above 5 ms or their error rate is above 0.1%.
    Either threshold may be left empty (``SET:5,`` gates p99 only,
    ``SET:,0.1`` gates the error rate only), but at least one must
    be set. The op name is case-insensitive and must be SET, GET or
    INCR; the p99 threshold must be non-negative and the error-rate
    threshold between 0 and 100.
    """
    if not text or ":" not in text:
        raise ValueError(
            "slo-op must be OP:P99,ERR, e.g. SET:5,0.1")
    op, _, rest = text.partition(":")
    op = op.strip().upper()
    if op not in ("SET", "GET", "INCR"):
        raise ValueError(
            "slo-op op must be SET, GET or INCR")
    parts = rest.split(",")
    if len(parts) != 2:
        raise ValueError(
            "slo-op must be OP:P99,ERR, e.g. SET:5,0.1")
    p99_text, rate_text = parts[0].strip(), parts[1].strip()
    if not p99_text and not rate_text:
        raise ValueError(
            "slo-op must set at least one threshold, e.g. SET:5,0.1")
    p99_ms = None
    error_rate_pct = None
    try:
        if p99_text:
            p99_ms = float(p99_text)
        if rate_text:
            error_rate_pct = float(rate_text)
    except ValueError as exc:
        raise ValueError(
            "slo-op thresholds must be numbers, e.g. SET:5,0.1") from exc
    if p99_ms is not None and p99_ms < 0:
        raise ValueError("slo-op p99 threshold must be non-negative")
    if error_rate_pct is not None and not (0 <= error_rate_pct <= 100):
        raise ValueError(
            "slo-op error-rate threshold must be between 0 and 100")
    return op, p99_ms, error_rate_pct


def _parse_host_port(text):
    """Parse a HOST:PORT target, e.g. ``127.0.0.1:6380``."""
    if ":" not in text:
        raise ValueError(
            "compare target must be HOST:PORT, e.g. 127.0.0.1:6380")
    host, _, port_text = text.rpartition(":")
    if not host or not port_text:
        raise ValueError(
            "compare target must be HOST:PORT, e.g. 127.0.0.1:6380")
    try:
        port = int(port_text)
    except ValueError as exc:
        raise ValueError("compare port must be an integer") from exc
    if not 0 < port < 65536:
        raise ValueError("compare port must be between 1 and 65535")
    return host, port


def build_parser():
    parser = argparse.ArgumentParser(
        prog="kvbench",
        description="Benchmark a Redis-compatible KV store (e.g. cinder).",
    )
    parser.add_argument("--version", action="version", version=__version__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=6379)
    parser.add_argument("--ops", type=int, default=None,
                        help="total ops to run (default 10000; "
                        "mutually exclusive with --duration)")
    parser.add_argument("--duration", default=None, metavar="DURATION",
                        help="run for a fixed wall-clock time instead of "
                        "a fixed op count, e.g. 10s or 2m")
    parser.add_argument("--warmup", default=None, metavar="DURATION",
                        help="run the workload for this long before "
                        "measurement (e.g. 2s or 1m) to warm caches and "
                        "connections; warmup ops are excluded from the "
                        "results")
    parser.add_argument("--clients", type=int, default=4)
    parser.add_argument("--keys", type=int, default=1_000)
    parser.add_argument("--value-size", type=int, default=32)
    parser.add_argument("--mix", default="50:40:10",
                        help="SET:GET:INCR weights (e.g. 50:40:10), a "
                        "READ:WRITE ratio (e.g. 80:20), or a preset: "
                        "read-heavy, balanced, write-heavy")
    parser.add_argument("--pipeline", type=int, default=1,
                        help="commands sent per round trip "
                        "(1 = sequential, default 1)")
    parser.add_argument("--distribution", default="uniform",
                        choices=["uniform", "zipfian"],
                        help="how keys are picked: uniform (every key "
                        "equally likely) or zipfian (a skewed Zipfian "
                        "distribution where a hot set takes most of "
                        "the traffic)")
    parser.add_argument("--zipf-skew", type=float, default=1.0,
                        help="Zipf exponent for --distribution zipfian "
                        "(0 = uniform, 1.0 = classic YCSB default)")
    parser.add_argument("--hot-keys", type=int, default=5,
                        help="how many of the hottest keys (by access "
                        "count) get per-key latency stats in the "
                        "reports (0 disables; default 5)")
    parser.add_argument("--sweep-keyspace", default=None, metavar="KEYS",
                        help="re-run the same workload once per "
                        "keyspace size, e.g. 100,1000,10000, and "
                        "report ops/sec and p99 per size")
    parser.add_argument("--sweep-clients", default=None, metavar="COUNTS",
                        help="re-run the same workload once per "
                        "client count, e.g. 1,2,4,8, and report "
                        "ops/sec and p99 per count")
    parser.add_argument("--seed", type=int, default=1234)
    parser.add_argument("--baseline", action="store_true",
                        help="also run the in-process dict baseline")
    parser.add_argument("--compare", action="append", default=[],
                        metavar="HOST:PORT",
                        help="also run the same workload against another "
                        "server and add it to the report; repeatable, "
                        "e.g. --compare 127.0.0.1:6380")
    parser.add_argument("--output", default="report.md",
                        help="where to write the Markdown report")
    parser.add_argument("--json", default=None, metavar="PATH",
                        help="where to write the JSON report "
                        "(default: same name as --output, .json extension)")
    parser.add_argument("--csv", default=None, metavar="PATH",
                        help="also write a per-second throughput CSV "
                        "(backend, second, ops, ops/sec, mean/max "
                        "latency) for plotting, e.g. timeseries.csv")
    parser.add_argument("--history", action="store_true",
                        help="record this run in the history file "
                        "(default ~/.kvbench/history.jsonl) so later "
                        "runs can be compared against it")
    parser.add_argument("--history-file", default=None, metavar="PATH",
                        help="where to read/write run history "
                        "(default ~/.kvbench/history.jsonl)")
    parser.add_argument("--show-history", action="store_true",
                        help="print the recorded run history and exit "
                        "(no benchmark is run)")
    parser.add_argument("--trend", action="store_true",
                        help="print an ops/sec trend chart per backend "
                        "from the recorded run history and exit "
                        "(no benchmark is run)")
    parser.add_argument("--trend-out", default=None, metavar="PATH",
                        help="with --trend, also write the trend chart "
                        "to this Markdown file (e.g. trend.md) so it "
                        "can be committed or attached to a report")
    parser.add_argument("--history-limit", type=int, default=20,
                        help="how many recent runs --show-history and "
                        "--trend print per backend (default 20)")
    parser.add_argument("--compare-file", default=None, metavar="PATH",
                        help="compare this run against a previous run's "
                        "JSON report (e.g. report.json): ops/sec and "
                        "p99 side by side per backend, with the "
                        "percentage change and a verdict in both "
                        "reports")
    parser.add_argument("--compare-history", default=None,
                        metavar="SELECTOR",
                        help="compare this run against a run recorded "
                        "in the history file (see --history-file) "
                        "instead of a JSON report: pick it by index "
                        "(0 for the oldest run, -1 or 'latest' for "
                        "the most recent) or by ISO timestamp (the "
                        "latest run at or before that time), "
                        "e.g. --compare-history -1")
    parser.add_argument("--compare-same-workload", action="store_true",
                        help="with --compare-history, only consider "
                        "recorded runs whose workload matches this "
                        "run's, so the baseline is the latest run "
                        "that measured the same workload")
    parser.add_argument("--regression-threshold", type=float, default=None,
                        metavar="PCT",
                        help="with --compare-file or --compare-history, "
                        "fail the run (exit 3) "
                        "when any backend's ops/sec fell by more than "
                        "PCT percent, or its p99 rose by more than PCT "
                        "percent, versus the baseline run")
    parser.add_argument("--slo-p99-ms", type=float, default=None,
                        metavar="MS",
                        help="SLO gate: fail the run (exit 4) when any "
                        "backend's measured p99 latency is above MS "
                        "milliseconds")
    parser.add_argument("--slo-error-rate", type=float, default=None,
                        metavar="PCT",
                        help="SLO gate: fail the run (exit 4) when any "
                        "backend's error rate is above PCT percent of "
                        "attempted ops")
    parser.add_argument("--slo-op", action="append", default=[],
                        metavar="OP:P99,ERR",
                        help="per-operation SLO gate: fail the run "
                        "(exit 4) when one operation type breaches its "
                        "own thresholds, e.g. --slo-op SET:5,0.1 gates "
                        "SET ops on p99 5 ms and error rate 0.1%%; "
                        "either threshold may be left empty "
                        "(SET:5, gates p99 only); repeatable, a later "
                        "entry for the same op replaces the earlier one")
    parser.add_argument("--slo-preset", default=None, metavar="PATH",
                        help="load SLO thresholds from a JSON preset "
                        "file (p99_ms, error_rate_pct, per_op) instead "
                        "of passing them on the command line; CLI "
                        "flags (--slo-p99-ms, --slo-error-rate, "
                        "--slo-op) override the preset values")
    return parser


def main(argv=None):
    args = build_parser().parse_args(argv)
    history_path = args.history_file or DEFAULT_HISTORY_FILE
    if args.show_history:
        # History-only mode: no server is contacted, so this works
        # even when nothing is listening.
        entries = load_history(history_path)
        print(format_history(entries, limit=args.history_limit))
        return 0
    if args.trend_out and not args.trend:
        print("kvbench: --trend-out needs --trend: it writes the "
              "trend chart to a file, and there is no trend chart "
              "without --trend", file=sys.stderr)
        return 2
    if args.trend:
        # Trend-only mode: same history file, rendered as a
        # per-backend ops/sec-over-time chart instead of a table.
        entries = load_history(history_path)
        text = format_trend(entries, limit=args.history_limit)
        print(text)
        if args.trend_out:
            write_trend(args.trend_out, entries,
                        limit=args.history_limit)
            print(f"Trend written to {args.trend_out}")
        return 0
    try:
        set_w, get_w, incr_w, mix_label = _parse_mix(args.mix)
        compare_targets = [_parse_host_port(t) for t in args.compare]
        duration_s = (_parse_duration(args.duration)
                      if args.duration else None)
        warmup_s = (_parse_duration(args.warmup)
                    if args.warmup else 0.0)
        sweep_sizes = (_parse_keyspace_sweep(args.sweep_keyspace)
                       if args.sweep_keyspace else [])
        client_counts = (_parse_client_sweep(args.sweep_clients)
                         if args.sweep_clients else [])
        if duration_s is not None and args.ops is not None:
            raise ValueError(
                "--ops and --duration are mutually exclusive: "
                "pick a fixed op count or a fixed run time, not both")
        if args.compare_file is not None \
                and args.compare_history is not None:
            raise ValueError(
                "--compare-file and --compare-history are mutually "
                "exclusive: pick a JSON report or a history run, "
                "not both")
        if args.compare_same_workload \
                and args.compare_history is None:
            raise ValueError(
                "--compare-same-workload needs --compare-history: "
                "it filters which recorded history run is picked "
                "as the baseline")
        if args.regression_threshold is not None \
                and args.compare_file is None \
                and args.compare_history is None:
            raise ValueError(
                "--regression-threshold needs --compare-file or "
                "--compare-history: there is nothing to compare "
                "against without a baseline run")
        if args.regression_threshold is not None \
                and args.regression_threshold < 0:
            raise ValueError(
                "--regression-threshold must be non-negative")
        slo_per_op = {}
        # A preset file supplies the thresholds; CLI flags override
        # it — a global flag replaces the preset's value, and a
        # --slo-op entry replaces the preset's entry for that op.
        # A bad preset file fails fast (exit 2) before any benchmark
        # runs, like the other argument errors above.
        slo_p99_ms = args.slo_p99_ms
        slo_error_rate = args.slo_error_rate
        if args.slo_preset:
            preset = load_slo_preset(args.slo_preset)
            if preset["p99_ms"] is not None:
                slo_p99_ms = preset["p99_ms"]
            if preset["error_rate_pct"] is not None:
                slo_error_rate = preset["error_rate_pct"]
            slo_per_op.update(preset["per_op"])
        if args.slo_p99_ms is not None:
            slo_p99_ms = args.slo_p99_ms
        if args.slo_error_rate is not None:
            slo_error_rate = args.slo_error_rate
        for text in args.slo_op:
            op, p99_ms, error_rate_pct = _parse_slo_op(text)
            # A later entry for the same op replaces the earlier one.
            slo_per_op[op] = (p99_ms, error_rate_pct)
    except ValueError as exc:
        print(f"kvbench: {exc}", file=sys.stderr)
        return 2
    # The workload is built before the baseline is loaded: the
    # --compare-same-workload filter needs this run's workload to
    # pick a matching recorded run, and building it is cheap — a
    # bad baseline path or selector still fails fast, before any
    # benchmark runs.
    try:
        workload = Workload(
            ops=(args.ops if args.ops is not None else
                 (0 if duration_s is not None else 10_000)),
            clients=args.clients,
            keys=args.keys,
            value_size=args.value_size,
            set_weight=set_w,
            get_weight=get_w,
            incr_weight=incr_w,
            seed=args.seed,
            pipeline=args.pipeline,
            mix_label=mix_label,
            duration_s=duration_s,
            warmup_s=warmup_s,
            distribution=args.distribution,
            zipf_skew=args.zipf_skew,
            hot_keys=args.hot_keys,
            sweep_keyspace=sweep_sizes,
            sweep_clients=client_counts,
            slo_p99_ms=slo_p99_ms,
            slo_error_rate_pct=slo_error_rate,
            slo_per_op=slo_per_op,
            slo_preset=args.slo_preset,
        )
    except ValueError as exc:
        print(f"kvbench: {exc}", file=sys.stderr)
        return 2
    # Load the baseline run before running anything: a bad path or
    # selector fails fast instead of after a benchmark that took
    # minutes.
    baseline_doc = None
    baseline_source = "report"
    baseline_label = args.compare_file
    if args.compare_file:
        try:
            baseline_doc = load_baseline(args.compare_file)
        except ValueError as exc:
            print(f"kvbench: {exc}", file=sys.stderr)
            return 1
    elif args.compare_history is not None:
        try:
            baseline_doc = load_history_baseline(
                history_path, args.compare_history,
                workload=(_workload_dict(workload)
                          if args.compare_same_workload else None))
        except ValueError as exc:
            print(f"kvbench: {exc}", file=sys.stderr)
            return 1
        baseline_source = "history"
        baseline_label = (f"{history_path} "
                          f"(history run {args.compare_history}"
                          + (", same workload"
                             if args.compare_same_workload else "")
                          + ")")
    command_line = " ".join(sys.argv)
    results = []

    def _warmup_note(result):
        return (f", warmup {result.warmup_ops} ops"
                if result.warmup_ops else "")

    def _run_sweep(factory, result):
        """Re-run the workload once per swept keyspace size and
        attach the per-size summaries to ``result``.

        Prints one line per size. A sweep that stops early (the
        backend went away mid-sweep) keeps the sizes that completed
        and says so on stderr instead of failing the whole run —
        the main result already landed.
        """
        if not sweep_sizes:
            return
        result.sweep = run_keyspace_sweep(factory, workload, sweep_sizes)
        for entry in result.sweep:
            print(f"  sweep {entry['keys']:,} keys: "
                  f"{entry['ops_per_sec']:,.0f} ops/sec, "
                  f"p99 {entry['p99_ms']:.3f} ms")
        if len(result.sweep) < len(sweep_sizes):
            print(f"kvbench: keyspace sweep on {result.name} stopped "
                  f"after {len(result.sweep)} of {len(sweep_sizes)} "
                  "sizes (backend unreachable)", file=sys.stderr)

    def _run_client_sweep(factory, result):
        """Re-run the workload once per swept client count and
        attach the per-count summaries to ``result``.

        Prints one line per count. A sweep that stops early (the
        backend went away mid-sweep) keeps the counts that
        completed and says so on stderr instead of failing the
        whole run — the main result already landed.
        """
        if not client_counts:
            return
        result.client_sweep = run_client_sweep(
            factory, workload, client_counts)
        for entry in result.client_sweep:
            print(f"  client sweep {entry['clients']} clients: "
                  f"{entry['ops_per_sec']:,.0f} ops/sec, "
                  f"p99 {entry['p99_ms']:.3f} ms")
        if len(result.client_sweep) < len(client_counts):
            print(f"kvbench: client sweep on {result.name} stopped "
                  f"after {len(result.client_sweep)} of "
                  f"{len(client_counts)} counts (backend unreachable)",
                  file=sys.stderr)

    def server_factory():
        return RespClient(args.host, args.port)

    scope = (f"{format_duration(duration_s)} duration"
             if duration_s is not None else f"{workload.ops} ops")
    if warmup_s:
        scope += f", {format_duration(warmup_s)} warmup"
    print(f"kvbench {__version__}: {scope}, "
          f"{workload.clients} clients, pipeline depth "
          f"{workload.pipeline} against {args.host}:{args.port}")
    try:
        result = run_workload(server_factory, workload,
                              f"cinder ({args.host}:{args.port})")
    except (ConnectionError, OSError) as exc:
        print(f"kvbench: cannot reach {args.host}:{args.port}: {exc}",
              file=sys.stderr)
        return 1
    results.append(result)
    print(f"  {result.name}: {result.ops_per_sec:,.0f} ops/sec, "
          f"p50 {result.summary['p50_ms']:.3f} ms, "
          f"p95 {result.summary['p95_ms']:.3f} ms, "
          f"errors {result.errors}{_warmup_note(result)}")
    _run_sweep(server_factory, result)
    _run_client_sweep(server_factory, result)

    for host, port in compare_targets:
        def compare_factory(host=host, port=port):
            return RespClient(host, port)

        print(f"kvbench {__version__}: same workload against "
              f"{host}:{port}")
        try:
            compared = run_workload(compare_factory, workload,
                                    f"compare ({host}:{port})")
        except (ConnectionError, OSError) as exc:
            print(f"kvbench: cannot reach {host}:{port}: {exc}",
                  file=sys.stderr)
            return 1
        results.append(compared)
        print(f"  {compared.name}: {compared.ops_per_sec:,.0f} ops/sec, "
              f"p50 {compared.summary['p50_ms']:.3f} ms, "
              f"p95 {compared.summary['p95_ms']:.3f} ms, "
              f"errors {compared.errors}{_warmup_note(compared)}")
        _run_sweep(compare_factory, compared)
        _run_client_sweep(compare_factory, compared)

    if args.baseline:
        baseline = run_workload(DictClient, workload, "baseline (dict)")
        results.append(baseline)
        print(f"  {baseline.name}: {baseline.ops_per_sec:,.0f} ops/sec, "
              f"p50 {baseline.summary['p50_ms']:.3f} ms, "
              f"p95 {baseline.summary['p95_ms']:.3f} ms"
              f"{_warmup_note(baseline)}")
        _run_sweep(DictClient, baseline)
        _run_client_sweep(DictClient, baseline)

    comparison = None
    if baseline_doc is not None:
        comparison = compare_results(
            results, baseline_doc, _workload_dict(workload),
            baseline_label, args.regression_threshold,
            source=baseline_source)
    # The SLO gate is evaluated before the reports are written, so
    # the verdict lands in both reports even when the run fails.
    slo = evaluate_slo(results, slo_p99_ms, slo_error_rate,
                       slo_per_op, preset=args.slo_preset)
    write_report(results, workload, command_line, args.output,
                 comparison, slo)
    print(f"report written to {args.output}")
    json_path = json_path_for(args.output, args.json)
    write_json(results, workload, command_line, json_path, comparison,
               slo)
    print(f"JSON report written to {json_path}")
    if args.csv:
        write_csv(results, args.csv)
        print(f"CSV written to {args.csv}")
    if args.history:
        count = record_run(history_path, results, workload, command_line)
        print(f"recorded {count} run(s) in history file {history_path}")
    # Exit codes: 3 when the regression gate fired, 4 when the SLO
    # gate fired. A run that breaches both exits 4 — the SLO gate
    # is the run's own explicit budget, so it takes precedence.
    exit_code = 0
    if comparison is not None:
        for entry in comparison["entries"]:
            print("  " + format_verdict(entry,
                                         comparison["threshold_pct"]))
        if not comparison["workload_matches"]:
            print(
                "kvbench: warning: the workload differs from the "
                "baseline run ("
                + ", ".join(comparison["workload_differences"])
                + "), so the comparison is approximate",
                file=sys.stderr)
        if comparison["regressions"]:
            print(
                f"kvbench: {comparison['regressions']} backend(s) "
                "regressed beyond the "
                f"{comparison['threshold_pct']:g}% threshold",
                file=sys.stderr)
            exit_code = 3
    if slo is not None:
        for entry in slo["entries"]:
            print("  " + format_slo_verdict(entry))
        if slo["breaches"]:
            print(
                f"kvbench: SLO gate failed: {slo['breaches']} "
                "backend(s) breached a threshold",
                file=sys.stderr)
            exit_code = 4
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
