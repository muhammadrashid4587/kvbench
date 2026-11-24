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
    # sweep the value size: the same workload re-run at 32, 128 and
    # 1024 byte values, with ops/sec and p99 per size
    python -m kvbench.cli --host 127.0.0.1 --port 6379 \\
        --ops 20000 --clients 4 \\
        --sweep-value-size 32,128,1024 --output report.md
    # sweep the pipeline depth: the same workload re-run at 1, 5 and
    # 10 commands per round trip, with ops/sec and p99 per depth
    python -m kvbench.cli --host 127.0.0.1 --port 6379 \\
        --ops 20000 --clients 4 \\
        --sweep-pipeline 1,5,10 --output report.md
    # record the run in the history file, then compare later runs
    # against it (ops/sec trend over time)
    python -m kvbench.cli --host 127.0.0.1 --port 6379 \\
        --ops 20000 --clients 4 --history
    python -m kvbench.cli --show-history
    # ops/sec trend chart per backend, with sparklines, from the
    # same history file
    python -m kvbench.cli --trend
    # best-run vs worst-run comparison per backend from the same
    # history file, with the spread between them, overall and
    # per operation; --compare-same-workload limits it to runs
    # that used the latest recorded run's workload
    python -m kvbench.cli --best-worst --compare-same-workload
    # prune old runs from the history file, per backend: keep the
    # 20 most recent runs of each backend, or drop runs older
    # than a date — --dry-run shows what would be removed
    python -m kvbench.cli --prune --prune-keep 20
    python -m kvbench.cli --prune --prune-before 2026-11-01T00:00:00Z \
        --dry-run
    # export the history as portable JSON, and merge an export
    # back into the history on another machine — identical runs
    # are skipped, conflicting runs keep the existing record
    python -m kvbench.cli --history-export history.json
    python -m kvbench.cli --history-import history.json
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
    # benchmark a password-protected server: AUTH on connect, with
    # the password redacted from the recorded command line, so it
    # never lands in the reports or the history file
    python -m kvbench.cli --ops 20000 --clients 4 \\
        --password secret
    # ACL-style auth: AUTH username password
    python -m kvbench.cli --ops 20000 --clients 4 \\
        --username alice --password secret
"""

import argparse
import os
import sys
from datetime import datetime, timezone

from . import __version__
from .bench import DictClient, Workload, format_duration, \
    run_client_sweep, run_keyspace_sweep, run_pipeline_sweep, \
    run_repeated_workload, run_value_size_sweep, run_workload
from .compare import compare_results, format_verdict, load_baseline, \
    load_history_baseline
from .history import DEFAULT_HISTORY_FILE, export_history, \
    format_best_worst, format_history, format_import, format_prune, \
    format_trend, import_history, load_history, \
    prune_history, record_run, write_best_worst, \
    write_best_worst_json, write_trend
from .report import _workload_dict, render_csv, write_csv, write_json, \
    write_report
from .resp import RespClient, RespError
from .slo import evaluate_slo, format_slo_verdict, load_slo_preset


def _redact_command_line(argv):
    """Render an argv list as a command line with the password hidden.

    The value after ``--password`` (``--password secret``) or in
    ``--password=secret`` form is replaced with ``***``, so the
    password never lands in the Markdown/JSON reports or the
    history file, which all record the command line. Every other
    argument is passed through unchanged.
    """
    out = []
    hide_next = False
    for arg in argv:
        if hide_next:
            out.append("***")
            hide_next = False
            continue
        if arg == "--password":
            out.append(arg)
            hide_next = True
        elif arg.startswith("--password="):
            out.append("--password=***")
        else:
            out.append(arg)
    return " ".join(out)


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


# Named read/write mix presets, as SET:GET:INCR:DEL weights. Reads are
# GETs, writes are SETs plus a small INCR share so counters still get
# exercised, and DELs are a small share of the writes — a cache
# workload deletes far less often than it reads or writes. The
# default 45:40:10:5 sits between balanced and write-heavy.
# ``--mix read-heavy`` is the same shape as ``--mix 80:20``
# (read:write), just with INCR and DEL kept in the mix.
MIX_PRESETS = {
    "read-heavy": (15, 75, 5, 5),
    "balanced": (40, 45, 10, 5),
    "write-heavy": (75, 15, 5, 5),
}


def _parse_mix(text):
    """Parse a ``--mix`` value into (set, get, incr, del, label).

    Accepts four forms:

    - ``SET:GET:INCR:DEL`` weights, e.g. ``45:40:10:5`` (label is "").
    - ``SET:GET:INCR`` weights, e.g. ``50:40:10`` — the old form,
      DEL weight 0 (label is "").
    - ``READ:WRITE`` ratio, e.g. ``80:20`` — reads are GETs, writes
      are SETs, INCR and DEL weight 0 (label is "").
    - a preset name: ``read-heavy``, ``balanced``, ``write-heavy``
      (label is the preset name, so the report can name the mix).
    """
    if text in MIX_PRESETS:
        set_w, get_w, incr_w, del_w = MIX_PRESETS[text]
        return set_w, get_w, incr_w, del_w, text
    parts = text.split(":")
    if len(parts) not in (2, 3, 4):
        raise ValueError(
            "mix must be SET:GET:INCR:DEL, SET:GET:INCR, READ:WRITE, "
            "or a preset (read-heavy, balanced, write-heavy)")
    try:
        values = [int(p) for p in parts]
    except ValueError as exc:
        raise ValueError("mix weights must be integers") from exc
    if any(v < 0 for v in values) or sum(values) <= 0:
        raise ValueError("mix weights must be non-negative and sum > 0")
    if len(values) == 2:
        # READ:WRITE — reads are GETs, writes are SETs, no INCRs
        # or DELs.
        read_w, write_w = values
        return write_w, read_w, 0, 0, ""
    if len(values) == 3:
        # SET:GET:INCR — the old form, no DELs.
        return values[0], values[1], values[2], 0, ""
    return values[0], values[1], values[2], values[3], ""


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


def _parse_value_size_sweep(text):
    """Parse a ``--sweep-value-size`` value into a sorted list of
    distinct positive ints, e.g. ``32,128,1024`` -> [32, 128, 1024].
    Duplicates are dropped and the sizes are sorted, so
    ``128,32,128`` sweeps 32 then 128.
    """
    if not text:
        raise ValueError(
            "sweep-value-size must list value sizes in bytes, "
            "e.g. 32,128,1024")
    sizes = []
    for part in text.split(","):
        part = part.strip()
        if not part:
            raise ValueError(
                "sweep-value-size sizes must be comma-separated "
                "integers, e.g. 32,128,1024")
        try:
            size = int(part)
        except ValueError as exc:
            raise ValueError(
                "sweep-value-size sizes must be integers") from exc
        if size <= 0:
            raise ValueError("sweep-value-size sizes must be positive")
        sizes.append(size)
    return sorted(set(sizes))


def _parse_pipeline_sweep(text):
    """Parse a ``--sweep-pipeline`` value into a sorted list of
    distinct positive ints, e.g. ``1,5,10`` -> [1, 5, 10].
    Duplicates are dropped and the depths are sorted, so
    ``10,1,10`` sweeps 1 then 10.
    """
    if not text:
        raise ValueError(
            "sweep-pipeline must list pipeline depths, "
            "e.g. 1,5,10")
    depths = []
    for part in text.split(","):
        part = part.strip()
        if not part:
            raise ValueError(
                "sweep-pipeline depths must be comma-separated "
                "integers, e.g. 1,5,10")
        try:
            depth = int(part)
        except ValueError as exc:
            raise ValueError(
                "sweep-pipeline depths must be integers") from exc
        if depth <= 0:
            raise ValueError("sweep-pipeline depths must be positive")
        depths.append(depth)
    return sorted(set(depths))


def _parse_slo_op(text):
    """Parse a ``--slo-op`` value into (op, p99_ms, error_rate_pct).

    Form is ``OP:P99,ERR``, e.g. ``SET:5,0.1`` — SET ops breach
    when their p99 is above 5 ms or their error rate is above 0.1%.
    Either threshold may be left empty (``SET:5,`` gates p99 only,
    ``SET:,0.1`` gates the error rate only), but at least one must
    be set. The op name is case-insensitive and must be SET, GET, INCR
    or DEL; the p99 threshold must be non-negative and the error-rate
    threshold between 0 and 100.
    """
    if not text or ":" not in text:
        raise ValueError(
            "slo-op must be OP:P99,ERR, e.g. SET:5,0.1")
    op, _, rest = text.partition(":")
    op = op.strip().upper()
    if op not in ("SET", "GET", "INCR", "DEL"):
        raise ValueError(
            "slo-op op must be SET, GET, INCR or DEL")
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


def _parse_prune_before(text):
    """Parse a ``--prune-before`` value into an aware datetime.

    An ISO timestamp (``2026-11-01T00:00:00Z`` or ``2026-11-01
    00:00``); a timestamp with no timezone is read as UTC, the
    same rule ``--compare-history`` uses for timestamps.
    """
    if not text:
        raise ValueError(
            "prune-before must be an ISO timestamp, "
            "e.g. 2026-11-01T00:00:00Z")
    candidate = (text[:-1] + "+00:00" if text.endswith("Z") else text)
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError as exc:
        raise ValueError(
            "prune-before must be an ISO timestamp, "
            "e.g. 2026-11-01T00:00:00Z") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


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
    parser.add_argument("--timeout", type=float, default=5.0,
                        help="socket timeout in seconds for the server "
                        "connection (connect and read; default 5.0) — "
                        "raise it for a loaded or remote server, lower "
                        "it to fail fast on a dead one")
    parser.add_argument("--password", default=None, metavar="PASSWORD",
                        help="authenticate on connect (AUTH) before "
                        "benchmarking, for a password-protected server "
                        "(e.g. real Redis with requirepass); the "
                        "password is never written to the reports or "
                        "the history — the recorded command line "
                        "shows *** in its place")
    parser.add_argument("--username", default=None, metavar="USERNAME",
                        help="with --password, authenticate as this "
                        "user (AUTH username password, the Redis 6 "
                        "ACL form); needs --password")
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
    parser.add_argument("--key-prefix", default="kvbench",
                        metavar="PREFIX",
                        help="prefix for every benchmark key "
                        "(<prefix>:<rank> for SET/GET, "
                        "<prefix>:counter:<rank> for INCR; default "
                        "kvbench) — a different prefix keeps runs "
                        "against a shared store from colliding, and "
                        "the prefix is recorded in both reports")
    parser.add_argument("--value-size", type=int, default=32)
    parser.add_argument("--mix", default="45:40:10:5",
                        help="SET:GET:INCR:DEL weights (e.g. 45:40:10:5), "
                        "a SET:GET:INCR triple (e.g. 50:40:10, no DELs), "
                        "a READ:WRITE ratio (e.g. 80:20), or a preset: "
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
    parser.add_argument("--sweep-value-size", default=None,
                        metavar="BYTES",
                        help="re-run the same workload once per "
                        "value size in bytes, e.g. 32,128,1024, and "
                        "report ops/sec and p99 per size")
    parser.add_argument("--sweep-pipeline", default=None,
                        metavar="DEPTHS",
                        help="re-run the same workload once per "
                        "pipeline depth, e.g. 1,5,10, and report "
                        "ops/sec and p99 per depth")
    parser.add_argument("--repeat", type=int, default=1, metavar="N",
                        help="run the benchmark N times and report the "
                        "median ops/sec and p99 over the N runs "
                        "(per operation too); the per-run values are "
                        "recorded in the JSON report (default 1: a "
                        "single run, unchanged behaviour)")
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
    parser.add_argument("--best-worst", action="store_true",
                        help="print a best-run vs worst-run comparison "
                        "per backend from the recorded run history and "
                        "exit (no benchmark is run)")
    parser.add_argument("--best-worst-out", default=None, metavar="PATH",
                        help="with --best-worst, also write the "
                        "comparison to this Markdown file (e.g. "
                        "best-worst.md) so it can be committed or "
                        "attached to a report")
    parser.add_argument("--best-worst-json", default=None, metavar="PATH",
                        help="with --best-worst, also write the "
                        "comparison as JSON to this file, so a script "
                        "can read the spread without parsing Markdown")
    parser.add_argument("--history-limit", type=int, default=20,
                        help="how many recent runs --show-history and "
                        "--trend print per backend (default 20)")
    parser.add_argument("--history-export", default=None, metavar="PATH",
                        help="export the recorded run history as "
                        "portable JSON to this file and exit (no "
                        "benchmark is run); use "
                        "--history-export-backend to export one "
                        "backend instead of all of them")
    parser.add_argument("--history-export-backend", action="append",
                        default=[], metavar="NAME",
                        help="with --history-export, only export runs "
                        "of this backend (e.g. cinder "
                        "(127.0.0.1:6379)); repeatable, other backends "
                        "are left out")
    parser.add_argument("--history-import", default=None, metavar="PATH",
                        help="import a history export (see "
                        "--history-export) into the history file and "
                        "exit (no benchmark is run): new runs are "
                        "appended, identical runs are skipped, and "
                        "runs that conflict with a recorded run keep "
                        "the existing record")
    parser.add_argument("--prune", action="store_true",
                        help="prune old runs from the history file "
                        "(see --history-file) and exit (no benchmark "
                        "is run): keep the N most recent runs per "
                        "backend (--prune-keep), drop runs older than "
                        "a date (--prune-before), or clear it out "
                        "(--prune-all)")
    parser.add_argument("--prune-keep", type=int, default=None,
                        metavar="N",
                        help="with --prune, keep the N most recent "
                        "runs of each backend and drop the rest "
                        "(e.g. --prune-keep 20)")
    parser.add_argument("--prune-before", default=None,
                        metavar="TIMESTAMP",
                        help="with --prune, drop runs older than this "
                        "ISO timestamp, e.g. 2026-11-01T00:00:00Z "
                        "(a timestamp with no timezone is read as UTC)")
    parser.add_argument("--prune-backend", action="append", default=[],
                        metavar="NAME",
                        help="with --prune, only prune runs of this "
                        "backend (e.g. cinder (127.0.0.1:6379)); "
                        "repeatable, other backends are left alone")
    parser.add_argument("--prune-all", action="store_true",
                        help="with --prune, remove every run of the "
                        "selected backends: the explicit flag for "
                        "clearing a backend's history out entirely")
    parser.add_argument("--dry-run", action="store_true",
                        help="with --prune, show what would be removed "
                        "without changing the history file")
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
                        "that measured the same workload; with "
                        "--best-worst, only consider runs whose "
                        "workload matches the latest recorded run's")
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
    if args.history_export_backend and not args.history_export:
        print("kvbench: --history-export-backend needs "
              "--history-export: it only applies when exporting "
              "the history file", file=sys.stderr)
        return 2
    if args.history_export and args.history_import:
        print("kvbench: --history-export and --history-import are "
              "mutually exclusive: export or import in one run, "
              "not both", file=sys.stderr)
        return 2
    if args.history_export:
        # Export-only mode: no server is contacted. The export
        # is portable JSON, so a history can move between
        # machines and be merged back with --history-import.
        entries = load_history(history_path)
        document = export_history(
            args.history_export, entries,
            backends=args.history_export_backend)
        print(f"Exported {len(document['records'])} run(s) to "
              f"{args.history_export}")
        return 0
    if args.history_import:
        # Import-only mode: no server is contacted. A bad export
        # file fails (exit 2) and the history is left untouched.
        try:
            summary = import_history(args.history_import, history_path)
        except ValueError as exc:
            print(f"kvbench: {exc}", file=sys.stderr)
            return 2
        print(format_import(summary, args.history_import, history_path))
        return 0
    if args.trend_out and not args.trend:
        print("kvbench: --trend-out needs --trend: it writes the "
              "trend chart to a file, and there is no trend chart "
              "without --trend", file=sys.stderr)
        return 2
    if args.best_worst_out and not args.best_worst:
        print("kvbench: --best-worst-out needs --best-worst: it writes "
              "the best-vs-worst comparison to a file, and there is "
              "no comparison without --best-worst", file=sys.stderr)
        return 2
    if args.best_worst_json and not args.best_worst:
        print("kvbench: --best-worst-json needs --best-worst: it "
              "writes the best-vs-worst comparison to a file, and "
              "there is no comparison without --best-worst",
              file=sys.stderr)
        return 2
    if not args.prune:
        for flag, given in (
                ("--prune-keep", args.prune_keep is not None),
                ("--prune-before", args.prune_before is not None),
                ("--prune-backend", bool(args.prune_backend)),
                ("--prune-all", args.prune_all),
                ("--dry-run", args.dry_run)):
            if given:
                print(f"kvbench: {flag} needs --prune: it only "
                      "applies when pruning the history file",
                      file=sys.stderr)
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
    if args.best_worst:
        # Best-vs-worst mode: same history file, rendered as a
        # per-backend comparison of the fastest and slowest runs
        # instead of a chart over time. With
        # --compare-same-workload there is no current run to match
        # against, so the reference workload is the latest
        # recorded run's — the comparison stays apples-to-apples
        # within the runs that measured the same thing.
        entries = load_history(history_path)
        workload = None
        if args.compare_same_workload and entries:
            workload = entries[-1].get("workload")
        text = format_best_worst(entries, workload=workload)
        print(text)
        if args.best_worst_out:
            write_best_worst(args.best_worst_out, entries,
                             workload=workload)
            print(f"Best vs worst written to {args.best_worst_out}")
        if args.best_worst_json:
            write_best_worst_json(args.best_worst_json, entries,
                                  workload=workload)
            print(f"Best vs worst JSON written to "
                  f"{args.best_worst_json}")
        return 0
    if args.prune:
        # Prune-only mode: no server is contacted. A prune that
        # would empty a backend is refused unless --prune-all was
        # given — the explicit flag for clearing a backend out.
        try:
            if args.prune_all and (args.prune_keep is not None
                                   or args.prune_before is not None):
                raise ValueError(
                    "--prune-all removes every run: it cannot be "
                    "combined with --prune-keep or --prune-before")
            if args.prune_keep is not None and args.prune_keep < 1:
                raise ValueError("--prune-keep must be at least 1")
            if not args.prune_all and args.prune_keep is None \
                    and args.prune_before is None:
                raise ValueError(
                    "--prune needs --prune-keep, --prune-before or "
                    "--prune-all: pick what to prune")
            before = (_parse_prune_before(args.prune_before)
                      if args.prune_before else None)
            summary = prune_history(
                history_path, keep=args.prune_keep, before=before,
                backends=args.prune_backend, allow_all=args.prune_all,
                dry_run=args.dry_run)
        except ValueError as exc:
            print(f"kvbench: {exc}", file=sys.stderr)
            return 2
        print(format_prune(summary, history_path))
        return 0
    try:
        if args.username is not None and args.password is None:
            raise ValueError(
                "--username needs --password: AUTH takes a username "
                "and a password together")
        if args.password is not None and args.password == "":
            raise ValueError(
                "--password must not be empty: pass a real password, "
                "or leave it out for a server with no auth")
        set_w, get_w, incr_w, del_w, mix_label = _parse_mix(args.mix)
        compare_targets = [_parse_host_port(t) for t in args.compare]
        duration_s = (_parse_duration(args.duration)
                      if args.duration else None)
        warmup_s = (_parse_duration(args.warmup)
                    if args.warmup else 0.0)
        sweep_sizes = (_parse_keyspace_sweep(args.sweep_keyspace)
                       if args.sweep_keyspace else [])
        client_counts = (_parse_client_sweep(args.sweep_clients)
                         if args.sweep_clients else [])
        value_sizes = (_parse_value_size_sweep(args.sweep_value_size)
                       if args.sweep_value_size else [])
        pipeline_depths = (_parse_pipeline_sweep(args.sweep_pipeline)
                           if args.sweep_pipeline else [])
        if args.repeat < 1:
            raise ValueError(
                "--repeat must be a positive integer")
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
                and args.compare_history is None \
                and not args.best_worst:
            raise ValueError(
                "--compare-same-workload needs --compare-history or "
                "--best-worst: it filters which recorded history "
                "runs are compared")
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
            key_prefix=args.key_prefix,
            value_size=args.value_size,
            set_weight=set_w,
            get_weight=get_w,
            incr_weight=incr_w,
            del_weight=del_w,
            seed=args.seed,
            pipeline=args.pipeline,
            timeout_s=args.timeout,
            auth=args.password is not None,
            mix_label=mix_label,
            duration_s=duration_s,
            warmup_s=warmup_s,
            distribution=args.distribution,
            zipf_skew=args.zipf_skew,
            hot_keys=args.hot_keys,
            sweep_keyspace=sweep_sizes,
            sweep_clients=client_counts,
            sweep_value_size=value_sizes,
            sweep_pipeline=pipeline_depths,
            repeat=args.repeat,
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
    # The recorded command line is the one actually run: the argv
    # passed to main(), or sys.argv when main() was called bare —
    # with the password redacted, so it never lands in the reports
    # or the history file.
    command_line = _redact_command_line(
        list(argv) if argv is not None else sys.argv)
    results = []

    def _warmup_note(result):
        return (f", warmup {result.warmup_ops} ops"
                if result.warmup_ops else "")

    def _run_sweep(factory, result):
        """Re-run the workload once per swept keyspace size and
        attach the per-size summaries to ``result``.

        With ``--repeat`` above 1 each size runs that many times
        and the per-size ops/sec and p99 are the medians over the
        runs (see :func:`kvbench.bench.run_keyspace_sweep`).
        Prints one line per size. A sweep that stops early (the
        backend went away mid-sweep) keeps the sizes that completed
        and says so on stderr instead of failing the whole run —
        the main result already landed.
        """
        if not sweep_sizes:
            return
        result.sweep = run_keyspace_sweep(factory, workload, sweep_sizes,
                                          args.repeat)
        for entry in result.sweep:
            print(f"  sweep {entry['keys']:,} keys: "
                  f"{entry['ops_per_sec']:,.0f} ops/sec, "
                  f"p99 {entry['p99_ms']:.3f} ms"
                  + (f" (median of {args.repeat} runs)"
                     if args.repeat > 1 else ""))
        if len(result.sweep) < len(sweep_sizes):
            print(f"kvbench: keyspace sweep on {result.name} stopped "
                  f"after {len(result.sweep)} of {len(sweep_sizes)} "
                  "sizes (backend unreachable)", file=sys.stderr)

    def _run_client_sweep(factory, result):
        """Re-run the workload once per swept client count and
        attach the per-count summaries to ``result``.

        With ``--repeat`` above 1 each count runs that many times
        and the per-count ops/sec and p99 are the medians over the
        runs (see :func:`kvbench.bench.run_client_sweep`).
        Prints one line per count. A sweep that stops early (the
        backend went away mid-sweep) keeps the counts that
        completed and says so on stderr instead of failing the
        whole run — the main result already landed.
        """
        if not client_counts:
            return
        result.client_sweep = run_client_sweep(
            factory, workload, client_counts, args.repeat)
        for entry in result.client_sweep:
            print(f"  client sweep {entry['clients']} clients: "
                  f"{entry['ops_per_sec']:,.0f} ops/sec, "
                  f"p99 {entry['p99_ms']:.3f} ms"
                  + (f" (median of {args.repeat} runs)"
                     if args.repeat > 1 else ""))
        if len(result.client_sweep) < len(client_counts):
            print(f"kvbench: client sweep on {result.name} stopped "
                  f"after {len(result.client_sweep)} of "
                  f"{len(client_counts)} counts (backend unreachable)",
                  file=sys.stderr)

    def _run_value_size_sweep(factory, result):
        """Re-run the workload once per swept value size and attach
        the per-size summaries to ``result``.

        With ``--repeat`` above 1 each size runs that many times
        and the per-size ops/sec and p99 are the medians over the
        runs (see :func:`kvbench.bench.run_value_size_sweep`).
        Prints one line per size. A sweep that stops early (the
        backend went away mid-sweep) keeps the sizes that completed
        and says so on stderr instead of failing the whole run —
        the main result already landed.
        """
        if not value_sizes:
            return
        result.value_sweep = run_value_size_sweep(
            factory, workload, value_sizes, args.repeat)
        for entry in result.value_sweep:
            print(f"  value-size sweep {entry['value_size']:,} bytes: "
                  f"{entry['ops_per_sec']:,.0f} ops/sec, "
                  f"p99 {entry['p99_ms']:.3f} ms"
                  + (f" (median of {args.repeat} runs)"
                     if args.repeat > 1 else ""))
        if len(result.value_sweep) < len(value_sizes):
            print(f"kvbench: value-size sweep on {result.name} stopped "
                  f"after {len(result.value_sweep)} of "
                  f"{len(value_sizes)} sizes (backend unreachable)",
                  file=sys.stderr)

    def _run_pipeline_sweep(factory, result):
        """Re-run the workload once per swept pipeline depth and
        attach the per-depth summaries to ``result``.

        With ``--repeat`` above 1 each depth runs that many times
        and the per-depth ops/sec and p99 are the medians over the
        runs (see :func:`kvbench.bench.run_pipeline_sweep`).
        Prints one line per depth. A sweep that stops early (the
        backend went away mid-sweep) keeps the depths that completed
        and says so on stderr instead of failing the whole run —
        the main result already landed.
        """
        if not pipeline_depths:
            return
        result.pipeline_sweep = run_pipeline_sweep(
            factory, workload, pipeline_depths, args.repeat)
        for entry in result.pipeline_sweep:
            print(f"  pipeline sweep {entry['pipeline']} commands: "
                  f"{entry['ops_per_sec']:,.0f} ops/sec, "
                  f"p99 {entry['p99_ms']:.3f} ms"
                  + (f" (median of {args.repeat} runs)"
                     if args.repeat > 1 else ""))
        if len(result.pipeline_sweep) < len(pipeline_depths):
            print(f"kvbench: pipeline sweep on {result.name} stopped "
                  f"after {len(result.pipeline_sweep)} of "
                  f"{len(pipeline_depths)} depths (backend "
                  "unreachable)", file=sys.stderr)

    def server_factory():
        return RespClient(args.host, args.port, timeout=args.timeout,
                          username=args.username,
                          password=args.password)

    def _run(factory, name):
        """Run the workload once, or --repeat times and report the
        medians. In repeat mode one line per run prints first (its
        ops/sec, p99 and errors), and the result the caller prints
        afterwards carries the medians — see
        :func:`kvbench.bench.run_repeated_workload`."""
        if args.repeat > 1:
            result = run_repeated_workload(factory, workload, name,
                                           args.repeat)
            for entry in result.repeat_runs:
                print(f"  run {entry['run']}/{args.repeat}: "
                      f"{entry['ops_per_sec']:,.0f} ops/sec, "
                      f"p99 {entry['p99_ms']:.3f} ms, "
                      f"errors {entry['errors']}")
            return result
        return run_workload(factory, workload, name)

    # Appended to each summary line in repeat mode, so the printed
    # ops/sec and p99 read as the medians they are.
    median_note = (f", median of {args.repeat} runs"
                   if args.repeat > 1 else "")

    scope = (f"{format_duration(duration_s)} duration"
             if duration_s is not None else f"{workload.ops} ops")
    if warmup_s:
        scope += f", {format_duration(warmup_s)} warmup"
    print(f"kvbench {__version__}: {scope}, "
          f"{workload.clients} clients, pipeline depth "
          f"{workload.pipeline} against {args.host}:{args.port}")
    try:
        result = _run(server_factory, f"cinder ({args.host}:{args.port})")
    except (ConnectionError, OSError, RespError) as exc:
        # RespError here is an AUTH failure at connect (a wrong
        # password raises from the RespClient constructor): the run
        # never started, so fail cleanly instead of crashing on a
        # traceback from inside a worker thread.
        print(f"kvbench: cannot reach {args.host}:{args.port}: {exc}",
              file=sys.stderr)
        return 1
    results.append(result)
    print(f"  {result.name}: {result.ops_per_sec:,.0f} ops/sec, "
          f"p50 {result.summary['p50_ms']:.3f} ms, "
          f"p95 {result.summary['p95_ms']:.3f} ms, "
          f"errors {result.errors}{_warmup_note(result)}{median_note}")
    _run_sweep(server_factory, result)
    _run_client_sweep(server_factory, result)
    _run_value_size_sweep(server_factory, result)
    _run_pipeline_sweep(server_factory, result)

    for host, port in compare_targets:
        def compare_factory(host=host, port=port):
            return RespClient(host, port, timeout=args.timeout,
                              username=args.username,
                              password=args.password)

        print(f"kvbench {__version__}: same workload against "
              f"{host}:{port}")
        try:
            compared = _run(compare_factory, f"compare ({host}:{port})")
        except (ConnectionError, OSError, RespError) as exc:
            print(f"kvbench: cannot reach {host}:{port}: {exc}",
                  file=sys.stderr)
            return 1
        results.append(compared)
        print(f"  {compared.name}: {compared.ops_per_sec:,.0f} ops/sec, "
              f"p50 {compared.summary['p50_ms']:.3f} ms, "
              f"p95 {compared.summary['p95_ms']:.3f} ms, "
              f"errors {compared.errors}{_warmup_note(compared)}"
              f"{median_note}")
        _run_sweep(compare_factory, compared)
        _run_client_sweep(compare_factory, compared)
        _run_value_size_sweep(compare_factory, compared)
        _run_pipeline_sweep(compare_factory, compared)

    if args.baseline:
        baseline = _run(DictClient, "baseline (dict)")
        results.append(baseline)
        print(f"  {baseline.name}: {baseline.ops_per_sec:,.0f} ops/sec, "
              f"p50 {baseline.summary['p50_ms']:.3f} ms, "
              f"p95 {baseline.summary['p95_ms']:.3f} ms"
              f"{_warmup_note(baseline)}{median_note}")
        _run_sweep(DictClient, baseline)
        _run_client_sweep(DictClient, baseline)
        _run_value_size_sweep(DictClient, baseline)
        _run_pipeline_sweep(DictClient, baseline)

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
