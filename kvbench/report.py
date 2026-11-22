"""Markdown and JSON report writers."""

import json
import platform
import sys
from datetime import datetime, timezone

from . import __version__


def _workload_dict(workload):
    return {
        "ops": workload.ops,
        "clients": workload.clients,
        "keys": workload.keys,
        "value_size": workload.value_size,
        "mix": {
            "set": workload.set_weight,
            "get": workload.get_weight,
            "incr": workload.incr_weight,
        },
        "seed": workload.seed,
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


def render_markdown(results, workload, command_line):
    """Render one or more BenchmarkResults as a Markdown report."""
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
        f"- Total ops: {workload.ops}",
        f"- Clients (threads): {workload.clients}",
        f"- Keyspace: {workload.keys} keys",
        f"- Value size: {workload.value_size} bytes",
        f"- Mix: SET {workload.set_weight} / GET {workload.get_weight} / "
        f"INCR {workload.incr_weight}",
        "",
        "## Results",
        "",
        "| Backend | Ops | Ops/sec | Mean (ms) | p50 (ms) | p95 (ms) | "
        "p99 (ms) | Errors |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for result in results:
        lines.append(_row(result))
    lines += [
        "",
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
    ]
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
