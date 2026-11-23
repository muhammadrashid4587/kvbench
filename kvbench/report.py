"""Markdown and JSON report writers."""

import json
import platform
import sys
from datetime import datetime, timezone

from . import __version__
from .bench import format_duration
from .distribution import ZipfianSampler


def _workload_dict(workload):
    return {
        # In duration mode there is no fixed op target: ops is null
        # and the run length is under duration_s.
        "ops": None if workload.duration_s is not None else workload.ops,
        "duration_s": workload.duration_s,
        # Warmup seconds before measurement (0 when no warmup); the
        # warmup ops themselves are per-result, under warmup_ops.
        "warmup_s": workload.warmup_s,
        "clients": workload.clients,
        "keys": workload.keys,
        "value_size": workload.value_size,
        "mix": {
            "set": workload.set_weight,
            "get": workload.get_weight,
            "incr": workload.incr_weight,
        },
        "mix_label": workload.mix_label or None,
        "seed": workload.seed,
        "pipeline": workload.pipeline,
        "distribution": workload.distribution,
        # The skew only means anything for zipfian; null otherwise.
        "zipf_skew": (workload.zipf_skew
                      if workload.distribution == "zipfian" else None),
    }


def _result_dict(result):
    return {
        "name": result.name,
        "ops": result.ops,
        "elapsed_s": result.elapsed_s,
        "ops_per_sec": result.ops_per_sec,
        "errors": result.errors,
        "error_sample": result.error_sample,
        "summary": result.summary,
        "per_op": result.per_op_summary,
        "histogram": result.histogram,
        "time_series": result.time_series,
        # Share of successful ops that hit the hottest 1% of keys
        # (~0.01 under uniform; much higher under zipfian).
        "hot_hit_rate": result.hot_hit_rate,
        # Ops run in the warmup phase, before measurement. They are
        # not part of ``ops`` and contribute no latencies.
        "warmup_ops": result.warmup_ops,
    }


def to_json_dict(results, workload, command_line):
    """Build the JSON-serializable report document."""
    return {
        "tool": "kvbench",
        "version": __version__,
        "generated": datetime.now(timezone.utc).isoformat(),
        "python": (
            f"{sys.version.split()[0]} on {platform.system()} "
            f"{platform.machine()} ({platform.python_implementation()})"
        ),
        "command": command_line,
        "workload": _workload_dict(workload),
        "results": [_result_dict(r) for r in results],
    }


def render_json(results, workload, command_line):
    """Render the report as a JSON string."""
    return json.dumps(
        to_json_dict(results, workload, command_line), indent=2
    ) + "\n"


def write_json(results, workload, command_line, path):
    """Write the JSON report to ``path`` and return its text."""
    text = render_json(results, workload, command_line)
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)
    return text


def _row(result):
    s = result.summary
    return (
        f"| {result.name} | {result.ops} | {result.ops_per_sec:,.0f} | "
        f"{s['mean_ms']:.3f} | {s['p50_ms']:.3f} | {s['p95_ms']:.3f} | "
        f"{s['p99_ms']:.3f} | {result.errors} |"
    )


def _histogram_lines(result):
    """Text-bar histogram lines for one result, or [] when empty."""
    buckets = result.histogram
    if not buckets:
        return []
    total = result.summary["count"]
    lines = [f"### Latency histogram — {result.name}", ""]
    for bucket in buckets:
        label = (
            "> 1000 ms"
            if bucket["upper_ms"] is None
            else f"<= {bucket['upper_ms']:g} ms"
        )
        # Scale the bar to the largest bucket so the shape reads at a
        # glance; cap the width so long runs stay readable.
        width = round(bucket["count"] / max(b["count"] for b in buckets) * 40)
        lines.append(
            f"- {label}: {bucket['count']} "
            f"({bucket['count'] / total * 100:.1f}%) {'#' * width}"
        )
    lines.append("")
    return lines


def _per_op_lines(result):
    """Per-operation stats table for one result, or [] when empty."""
    per_op = result.per_op_summary
    if not per_op:
        return []
    lines = [
        f"### Per-operation stats — {result.name}",
        "",
        "| Op | Ops | Ops/sec | Mean (ms) | p50 (ms) | p95 (ms) | "
        "p99 (ms) |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for op, s in per_op.items():
        lines.append(
            f"| {op} | {s['count']} | {s['ops_per_sec']:,.0f} | "
            f"{s['mean_ms']:.3f} | {s['p50_ms']:.3f} | "
            f"{s['p95_ms']:.3f} | {s['p99_ms']:.3f} |"
        )
    lines.append("")
    return lines


def _time_series_lines(result):
    """Per-second ops/latency table for one result, or [] when empty."""
    series = result.time_series
    if not series:
        return []
    lines = [
        f"### Latency over time — {result.name}",
        "",
        "| Second | Ops | Mean (ms) | Max (ms) |",
        "| ---: | ---: | ---: | ---: |",
    ]
    for bucket in series:
        lines.append(
            f"| {bucket['second']} | {bucket['ops']} | "
            f"{bucket['mean_ms']:.3f} | {bucket['max_ms']:.3f} |"
        )
    lines.append("")
    return lines


def render_markdown(results, workload, command_line):
    """Render one or more BenchmarkResults as a Markdown report."""
    if workload.duration_s is not None:
        scope_line = (
            f"- Duration: {format_duration(workload.duration_s)} "
            "(fixed wall-clock run; total ops vary)"
        )
    else:
        scope_line = f"- Total ops: {workload.ops}"
    if workload.distribution == "zipfian":
        sampler = ZipfianSampler(workload.keys, workload.zipf_skew)
        distribution_line = (
            f"- Key distribution: zipfian (skew {workload.zipf_skew:g}; "
            f"the hottest 1% of keys take ~"
            f"{sampler.hot_share() * 100:.0f}% of ops)"
        )
    else:
        distribution_line = "- Key distribution: uniform"
    if workload.warmup_s > 0:
        warmup_line = (
            f"- Warmup: {format_duration(workload.warmup_s)} "
            "(same workload, before measurement; warmup ops and "
            "latencies are excluded from the results)"
        )
    else:
        warmup_line = None
    lines = [
        "# kvbench report",
        "",
        f"Generated: {datetime.now(timezone.utc).isoformat()}",
        f"Python: {sys.version.split()[0]} on {platform.system()} "
        f"{platform.machine()} ({platform.python_implementation()})",
        f"Command: `{command_line}`",
        "",
        "## Workload",
        "",
        scope_line,
        f"- Clients (threads): {workload.clients}",
        f"- Keyspace: {workload.keys} keys",
        distribution_line,
        f"- Value size: {workload.value_size} bytes",
        f"- Mix: SET {workload.set_weight} / GET {workload.get_weight} / "
        f"INCR {workload.incr_weight}"
        + (f" ({workload.mix_label})" if workload.mix_label else ""),
        f"- Pipeline depth: {workload.pipeline} command(s) per round trip",
    ]
    if warmup_line is not None:
        lines.append(warmup_line)
    lines += [
        "",
        "## Results",
        "",
        "| Backend | Ops | Ops/sec | Mean (ms) | p50 (ms) | p95 (ms) | "
        "p99 (ms) | Errors |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for result in results:
        lines.append(_row(result))
    per_op_lines = []
    for result in results:
        per_op_lines += _per_op_lines(result)
    if per_op_lines:
        lines += ["", "## Per-operation stats", ""] + per_op_lines
    hist_lines = []
    for result in results:
        hist_lines += _histogram_lines(result)
    if hist_lines:
        lines += ["", "## Latency histograms", ""] + hist_lines
    ts_lines = []
    for result in results:
        ts_lines += _time_series_lines(result)
    if ts_lines:
        lines += ["", "## Latency over time", ""] + ts_lines
    lines += [
        "## Notes",
        "",
        "- Numbers are machine-dependent. Reproduce them with the command "
        "above on the same machine and server.",
        "- The `baseline (dict)` backend is an in-process Python dict: no "
        "network, no persistence, no expiry. It is an upper bound on what "
        "this workload can do on this machine, not a fair head-to-head "
        "with a real server.",
        "- Latencies are measured per operation around the client call, "
        "so they include network round-trip time for server backends.",
        "- The Latency over time section buckets ops by the second they "
        "completed; a second with 0 ops marks a stall.",
        "- The Per-operation stats section splits ops/sec and latency "
        "percentiles by operation (SET, GET, INCR), so a read-heavy "
        "mix shows whether reads or writes dominate the tail.",
    ]
    if workload.duration_s is not None:
        lines.append(
            "- In duration mode the run stops after the requested "
            "wall-clock time, so the total op count varies from run to "
            "run; ops/sec is total ops divided by the actual elapsed time."
        )
    if workload.pipeline > 1:
        lines.append(
            "- With a pipeline depth above 1, latencies are the batch "
            "round-trip time divided by the batch size: amortized per-op "
            "cost, not the latency of any single reply."
        )
    if workload.distribution == "zipfian":
        for result in results:
            lines.append(
                f"- Hot-set hit rate on {result.name}: "
                f"{result.hot_hit_rate * 100:.1f}% of ops hit the "
                "hottest 1% of keys."
            )
    if workload.warmup_s > 0:
        lines.append(
            "- Before measurement each backend ran the same workload "
            f"for {format_duration(workload.warmup_s)} as a warmup, so "
            "caches and connections were warm; warmup ops and their "
            "latencies are excluded from the results, and the elapsed "
            "time covers the measured phase only."
        )
        for result in results:
            lines.append(
                f"- Warmup ops on {result.name}: {result.warmup_ops} "
                "(excluded from results)."
            )
    for result in results:
        if result.error_sample:
            lines.append(
                f"- First error from {result.name}: {result.error_sample}"
            )
    lines.append("")
    return "\n".join(lines)


def write_report(results, workload, command_line, path):
    """Write the Markdown report to ``path`` and return its text."""
    text = render_markdown(results, workload, command_line)
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)
    return text
