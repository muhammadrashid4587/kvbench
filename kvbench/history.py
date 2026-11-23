"""Run history: record benchmark runs and show the ops/sec trend.

Every run recorded with ``--history`` appends one JSON record per
backend to a JSONL file (default ``~/.kvbench/history.jsonl``), so
later runs can be compared against earlier ones on the same machine
and workload. ``--show-history`` prints the most recent entries as
a table with the ops/sec change versus the previous run of the same
backend, so a regression — or a cinder speedup — reads at a glance.

Records are plain JSON lines; a corrupt line is skipped on load
rather than failing the whole history, so a half-written file never
blocks the trend view.
"""

import json
import os
from datetime import datetime, timezone

from . import __version__

DEFAULT_HISTORY_FILE = os.path.join(
    os.path.expanduser("~"), ".kvbench", "history.jsonl"
)


def _workload_record(workload):
    return {
        "ops": (None if workload.duration_s is not None
                else workload.ops),
        "duration_s": workload.duration_s,
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
        "zipf_skew": (workload.zipf_skew
                      if workload.distribution == "zipfian" else None),
    }


def build_record(result, workload, command_line):
    """Build one history record for a single backend result."""
    summary = result.summary
    return {
        "tool": "kvbench",
        "version": __version__,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "command": command_line,
        "backend": result.name,
        "ops": result.ops,
        "elapsed_s": result.elapsed_s,
        "ops_per_sec": result.ops_per_sec,
        "mean_ms": summary["mean_ms"],
        "p50_ms": summary["p50_ms"],
        "p95_ms": summary["p95_ms"],
        "p99_ms": summary["p99_ms"],
        "errors": result.errors,
        "workload": _workload_record(workload),
    }


def record_run(path, results, workload, command_line):
    """Append one record per result to the history file at ``path``.

    Creates the parent directory when needed. Returns the number of
    records written.
    """
    directory = os.path.dirname(os.path.abspath(path))
    os.makedirs(directory, exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        for result in results:
            f.write(json.dumps(
                build_record(result, workload, command_line)) + "\n")
    return len(results)


def load_history(path):
    """Load history records from ``path``, oldest first.

    Returns [] when the file does not exist. A line that is not
    valid JSON, or not a JSON object, is skipped instead of failing
    the load.
    """
    if not os.path.exists(path):
        return []
    entries = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(record, dict) and "backend" in record:
                entries.append(record)
    return entries


def _ops_sec_delta(entry, previous):
    """Ops/sec change of ``entry`` versus ``previous`` (same backend).

    Returns a string such as ``+3.2%`` or ``-12.5%``, or ``-`` when
    there is no previous run to compare against.
    """
    if previous is None:
        return "-"
    prev = previous.get("ops_per_sec")
    current = entry.get("ops_per_sec")
    if not prev or not current:
        return "-"
    change = (current - prev) / prev * 100
    return f"{change:+.1f}%"


def format_history(entries, limit=20):
    """Render history entries as a trend table (most recent first).

    ``entries`` are in file order (oldest first); the table shows the
    newest ``limit`` entries, newest first, with the ops/sec change
    versus the previous run of the same backend.
    """
    if not entries:
        return "No runs recorded yet. Re-run a benchmark with --history."
    window = entries[-limit:]
    # Previous run per backend, scanning in file order so the delta
    # is always against the run that came immediately before.
    previous_by_backend = {}
    deltas = {}
    for entry in entries:
        backend = entry.get("backend")
        deltas[id(entry)] = _ops_sec_delta(
            entry, previous_by_backend.get(backend))
        previous_by_backend[backend] = entry
    lines = [
        "## kvbench history",
        "",
        "| Time (UTC) | Backend | Ops | Ops/sec | p99 (ms) | "
        "Errors | Ops/sec vs prev |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for entry in reversed(window):
        timestamp = entry.get("timestamp", "")[:19].replace("T", " ")
        lines.append(
            f"| {timestamp} | {entry.get('backend', '?')} | "
            f"{entry.get('ops', 0)} | "
            f"{entry.get('ops_per_sec', 0):,.0f} | "
            f"{entry.get('p99_ms', 0):.3f} | "
            f"{entry.get('errors', 0)} | "
            f"{deltas[id(entry)]} |"
        )
    lines.append("")
    return "\n".join(lines)
