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
    # record the run in the history file, then compare later runs
    # against it (ops/sec trend over time)
    python -m kvbench.cli --host 127.0.0.1 --port 6379 \\
        --ops 20000 --clients 4 --history
    python -m kvbench.cli --show-history
"""

import argparse
import os
import sys

from . import __version__
from .bench import DictClient, Workload, format_duration, \
    run_keyspace_sweep, run_workload
from .history import DEFAULT_HISTORY_FILE, format_history, \
    load_history, record_run
from .report import write_json, write_report
from .resp import RespClient


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
    parser.add_argument("--history-limit", type=int, default=20,
                        help="how many recent runs --show-history prints "
                        "(default 20)")
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
    try:
        set_w, get_w, incr_w, mix_label = _parse_mix(args.mix)
        compare_targets = [_parse_host_port(t) for t in args.compare]
        duration_s = (_parse_duration(args.duration)
                      if args.duration else None)
        warmup_s = (_parse_duration(args.warmup)
                    if args.warmup else 0.0)
        sweep_sizes = (_parse_keyspace_sweep(args.sweep_keyspace)
                       if args.sweep_keyspace else [])
        if duration_s is not None and args.ops is not None:
            raise ValueError(
                "--ops and --duration are mutually exclusive: "
                "pick a fixed op count or a fixed run time, not both")
    except ValueError as exc:
        print(f"kvbench: {exc}", file=sys.stderr)
        return 2
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
        )
    except ValueError as exc:
        print(f"kvbench: {exc}", file=sys.stderr)
        return 2
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

    if args.baseline:
        baseline = run_workload(DictClient, workload, "baseline (dict)")
        results.append(baseline)
        print(f"  {baseline.name}: {baseline.ops_per_sec:,.0f} ops/sec, "
              f"p50 {baseline.summary['p50_ms']:.3f} ms, "
              f"p95 {baseline.summary['p95_ms']:.3f} ms"
              f"{_warmup_note(baseline)}")
        _run_sweep(DictClient, baseline)

    write_report(results, workload, command_line, args.output)
    print(f"report written to {args.output}")
    json_path = json_path_for(args.output, args.json)
    write_json(results, workload, command_line, json_path)
    print(f"JSON report written to {json_path}")
    if args.history:
        count = record_run(history_path, results, workload, command_line)
        print(f"recorded {count} run(s) in history file {history_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
