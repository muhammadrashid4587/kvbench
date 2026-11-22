"""kvbench CLI: benchmark a Redis-compatible KV store.

Usage:
    python -m kvbench.cli --host 127.0.0.1 --port 6379 --ops 20000 \\
        --clients 4 --output report.md
    python -m kvbench.cli --baseline --ops 20000 --output report.md
    python -m kvbench.cli --host 127.0.0.1 --port 6379 --baseline \\
        --ops 20000 --output report.md
"""

import argparse
import os
import sys

from . import __version__
from .bench import DictClient, Workload, run_workload
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


def _parse_mix(text):
    parts = text.split(":")
    if len(parts) != 3:
        raise ValueError("mix must be SET:GET:INCR, e.g. 50:40:10")
    try:
        values = [int(p) for p in parts]
    except ValueError as exc:
        raise ValueError("mix weights must be integers") from exc
    if any(v < 0 for v in values) or sum(values) <= 0:
        raise ValueError("mix weights must be non-negative and sum > 0")
    return values


def build_parser():
    parser = argparse.ArgumentParser(
        prog="kvbench",
        description="Benchmark a Redis-compatible KV store (e.g. cinder).",
    )
    parser.add_argument("--version", action="version", version=__version__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=6379)
    parser.add_argument("--ops", type=int, default=10_000)
    parser.add_argument("--clients", type=int, default=4)
    parser.add_argument("--keys", type=int, default=1_000)
    parser.add_argument("--value-size", type=int, default=32)
    parser.add_argument("--mix", default="50:40:10",
                        help="SET:GET:INCR weights, e.g. 50:40:10")
    parser.add_argument("--pipeline", type=int, default=1,
                        help="commands sent per round trip "
                        "(1 = sequential, default 1)")
    parser.add_argument("--seed", type=int, default=1234)
    parser.add_argument("--baseline", action="store_true",
                        help="also run the in-process dict baseline")
    parser.add_argument("--output", default="report.md",
                        help="where to write the Markdown report")
    parser.add_argument("--json", default=None, metavar="PATH",
                        help="where to write the JSON report "
                        "(default: same name as --output, .json extension)")
    return parser


def main(argv=None):
    args = build_parser().parse_args(argv)
    try:
        set_w, get_w, incr_w = _parse_mix(args.mix)
    except ValueError as exc:
        print(f"kvbench: {exc}", file=sys.stderr)
        return 2
    try:
        workload = Workload(
            ops=args.ops,
            clients=args.clients,
            keys=args.keys,
            value_size=args.value_size,
            set_weight=set_w,
            get_weight=get_w,
            incr_weight=incr_w,
            seed=args.seed,
            pipeline=args.pipeline,
        )
    except ValueError as exc:
        print(f"kvbench: {exc}", file=sys.stderr)
        return 2
    command_line = " ".join(sys.argv)
    results = []

    def server_factory():
        return RespClient(args.host, args.port)

    print(f"kvbench {__version__}: {workload.ops} ops, "
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
          f"errors {result.errors}")

    if args.baseline:
        baseline = run_workload(DictClient, workload, "baseline (dict)")
        results.append(baseline)
        print(f"  {baseline.name}: {baseline.ops_per_sec:,.0f} ops/sec, "
              f"p50 {baseline.summary['p50_ms']:.3f} ms, "
              f"p95 {baseline.summary['p95_ms']:.3f} ms")

    write_report(results, workload, command_line, args.output)
    print(f"report written to {args.output}")
    json_path = json_path_for(args.output, args.json)
    write_json(results, workload, command_line, json_path)
    print(f"JSON report written to {json_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
