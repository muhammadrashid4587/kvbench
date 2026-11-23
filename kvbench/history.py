"""Run history: record benchmark runs and show the ops/sec trend.

Every run recorded with ``--history`` appends one JSON record per
backend to a JSONL file (default ``~/.kvbench/history.jsonl``), so
later runs can be compared against earlier ones on the same machine
and workload. ``--show-history`` prints the most recent entries as
a table with the ops/sec change versus the previous run of the same
backend, so a regression — or a cinder speedup — reads at a glance.

Records are plain JSON lines; a corrupt line is skipped on load
rather than failing the whole history, so a half-written file never
blocks the trend view. ``--trend`` renders the same history as an
ops/sec-over-time chart per backend, with a sparkline and the
change versus the previous run, and since 0.22.0 each record also
carries per-op throughput and latency, so the trend view tracks
SET, GET and INCR separately — a slow write no longer hides
inside the overall ops/sec. ``--best-worst`` picks the best and
worst runs per backend (and per operation) out of the same
history, with the spread between them, so a backend whose
throughput swings run to run reads at a glance.
"""

import json
import os
from datetime import datetime, timezone

from . import __version__

DEFAULT_HISTORY_FILE = os.path.join(
    os.path.expanduser("~"), ".kvbench", "history.jsonl"
)

# Workload settings that have to match for a comparison to be
# apples-to-apples. Compared key by key; a difference is reported
# by name, not as a hard failure. This is the same key set
# ``kvbench.compare.workload_differences`` uses, so the best/worst
# workload filter and the --compare-same-workload filter agree.
_WORKLOAD_KEYS = (
    "ops",
    "duration_s",
    "clients",
    "keys",
    "value_size",
    "mix",
    "mix_label",
    "seed",
    "pipeline",
    "distribution",
    "zipf_skew",
)


def workload_matches(record_workload, reference_workload):
    """Whether a recorded run's workload matches a reference one.

    True when every compared setting is equal. A record with no
    workload dict never matches — it measured something unknown,
    so it cannot be an apples-to-apples comparison.
    """
    if not isinstance(record_workload, dict):
        return False
    if not isinstance(reference_workload, dict):
        return False
    return all(record_workload.get(key) == reference_workload.get(key)
               for key in _WORKLOAD_KEYS)


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
        # Per-op throughput and latency, so the trend view can track
        # each operation type on its own: a SET regression hides
        # inside the overall ops/sec when GETs dominate the mix.
        # Ops that never ran are left out, so a read-only mix
        # records GET alone; records written before 0.22.0 have no
        # "per_op" key at all, and the trend view skips those runs
        # for the per-op sections instead of failing.
        "per_op": {
            op: {
                "ops": entry["count"],
                "ops_per_sec": entry["ops_per_sec"],
                "mean_ms": entry["mean_ms"],
                "p50_ms": entry["p50_ms"],
                "p95_ms": entry["p95_ms"],
                "p99_ms": entry["p99_ms"],
            }
            for op, entry in result.per_op_summary.items()
        },
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


def _sparkline(values):
    """Render a list of numbers as a Unicode block sparkline.

    ``▁`` is the lowest value, ``█`` the highest, oldest first.
    A single value, or values that are all equal, render as the
    middle block — there is no trend to show, only a level.
    """
    if not values:
        return "-"
    blocks = "▁▂▃▄▅▆▇█"
    low, high = min(values), max(values)
    if high == low:
        return "▅" * len(values)
    span = high - low
    return "".join(
        blocks[min(7, int((v - low) / span * 7 + 0.5))]
        for v in values)


def _per_op_trend_lines(runs, window):
    """Per-operation trend sections for one backend.

    ``runs`` are all recorded runs of the backend (oldest first),
    ``window`` the most recent ones being rendered. Each operation
    type that ran gets its own table — ops/sec, the change versus
    the previous run of this backend that ran the same op, and p99
    — plus a sparkline and the first-to-last change. Runs recorded
    before 0.22.0 carry no ``per_op`` data and are skipped for
    these sections; an op that never ran in the window gets no
    section at all.
    """
    lines = []
    for op in ("SET", "GET", "INCR"):
        # The delta is against the previous run that actually ran
        # this op, scanning all runs so a run outside the window
        # still counts as the previous one — the same rule the
        # overall table uses.
        previous_by_id = {}
        previous = None
        for entry in runs:
            op_entry = (entry.get("per_op") or {}).get(op)
            if op_entry is None:
                previous_by_id[id(entry)] = None
                continue
            previous_by_id[id(entry)] = _ops_sec_delta(op_entry, previous)
            previous = op_entry
        values = [
            (entry.get("per_op") or {}).get(op, {}).get("ops_per_sec")
            for entry in window
        ]
        values = [v for v in values if isinstance(v, (int, float))]
        if not values:
            continue
        lines.append(f"#### {op} ops/sec trend")
        lines.append("")
        lines.append("| Time (UTC) | Ops/sec | Ops/sec vs prev | "
                     "p99 (ms) |")
        lines.append("| --- | ---: | ---: | ---: |")
        for entry in reversed(window):
            op_entry = (entry.get("per_op") or {}).get(op)
            if op_entry is None:
                continue
            timestamp = entry.get("timestamp", "")[:19].replace("T", " ")
            ops_sec = op_entry.get("ops_per_sec")
            ops_cell = (f"{ops_sec:,.0f}"
                        if isinstance(ops_sec, (int, float)) else "-")
            p99 = op_entry.get("p99_ms")
            p99_cell = (f"{p99:.3f}"
                        if isinstance(p99, (int, float)) else "-")
            lines.append(
                f"| {timestamp} | {ops_cell} | "
                f"{previous_by_id[id(entry)]} | {p99_cell} |"
            )
        lines.append("")
        lines.append(
            f"{op} trend (oldest → newest): {_sparkline(values)}")
        if len(values) >= 2:
            first, last = values[0], values[-1]
            change = (last - first) / first * 100 if first else 0.0
            lines.append(
                f"{op} first → last: {first:,.0f} → {last:,.0f} "
                f"ops/sec ({change:+.1f}%)")
        lines.append("")
    return lines


def format_trend(entries, limit=20):
    """Render an ops/sec-over-time trend chart per backend.

    ``entries`` are in file order (oldest first). Each backend gets
    its own section: a table of its most recent ``limit`` runs with
    ops/sec, the change versus the previous run of that backend,
    and p99 alongside, plus a sparkline of ops/sec over those runs
    (oldest to newest) and the first-to-last change, followed by
    the same chart per operation type (SET, GET, INCR) for the ops
    that ran, so each op's throughput trend reads on its own. A
    corrupt line never reaches here — ``load_history`` already
    skips it — and a backend with a single recorded run shows no
    delta, just its one value.
    """
    if not entries:
        return "No runs recorded yet. Re-run a benchmark with --history."
    # Group runs per backend, keeping first-seen backend order.
    by_backend = {}
    for entry in entries:
        by_backend.setdefault(entry.get("backend", "?"), []).append(entry)
    sections = ["## kvbench ops/sec trend", ""]
    for backend, runs in by_backend.items():
        window = runs[-limit:]
        # Ops/sec change versus the previous run of this backend.
        previous_by_id = {}
        previous = None
        for entry in runs:
            previous_by_id[id(entry)] = _ops_sec_delta(entry, previous)
            previous = entry
        values = [e.get("ops_per_sec") for e in window
                  if isinstance(e.get("ops_per_sec"), (int, float))]
        sections.append(f"### {backend}")
        sections.append("")
        sections.append("| Time (UTC) | Ops/sec | Ops/sec vs prev | "
                        "p99 (ms) |")
        sections.append("| --- | ---: | ---: | ---: |")
        for entry in reversed(window):
            timestamp = entry.get("timestamp", "")[:19].replace("T", " ")
            ops_sec = entry.get("ops_per_sec")
            ops_cell = (f"{ops_sec:,.0f}"
                        if isinstance(ops_sec, (int, float)) else "-")
            p99 = entry.get("p99_ms")
            p99_cell = (f"{p99:.3f}"
                        if isinstance(p99, (int, float)) else "-")
            sections.append(
                f"| {timestamp} | {ops_cell} | "
                f"{previous_by_id[id(entry)]} | {p99_cell} |"
            )
        sections.append("")
        sections.append(f"Trend (oldest → newest): {_sparkline(values)}")
        if len(values) >= 2:
            first, last = values[0], values[-1]
            change = (last - first) / first * 100 if first else 0.0
            sections.append(
                f"First → last: {first:,.0f} → {last:,.0f} ops/sec "
                f"({change:+.1f}%)")
        sections.append("")
        # Per-op trend, same shape as the overall chart, one section
        # per operation type that ran in this window.
        sections.extend(_per_op_trend_lines(runs, window))
    return "\n".join(sections)


def write_trend(path, entries, limit=20):
    """Render the ops/sec trend chart and write it to ``path``.

    Creates the parent directory when needed. Returns the rendered
    text — the same text ``--trend`` prints to stdout — so the file
    can be committed or attached to a report.
    """
    text = format_trend(entries, limit=limit)
    directory = os.path.dirname(os.path.abspath(path))
    os.makedirs(directory, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)
        if not text.endswith("\n"):
            f.write("\n")
    return text


def _run_numbers(entry):
    """A run's headline numbers as a small dict, or None.

    The timestamp is kept for the table; a run with no numeric
    ops/sec cannot be ranked, so it is skipped instead of
    crashing the comparison.
    """
    ops_sec = entry.get("ops_per_sec")
    if not isinstance(ops_sec, (int, float)):
        return None
    p99 = entry.get("p99_ms")
    return {
        "timestamp": entry.get("timestamp", "")[:19].replace("T", " "),
        "ops_per_sec": ops_sec,
        "p99_ms": p99 if isinstance(p99, (int, float)) else None,
    }


def _delta_pct(best, worst):
    """Percentage change of ``best`` versus ``worst``.

    None when the worst value is zero or missing — there is no
    meaningful percentage change from nothing.
    """
    if not worst:
        return None
    return (best - worst) / worst * 100


def _best_worst_pair(runs):
    """The best and worst runs of ``runs`` by ops/sec.

    ``runs`` are (entry, numbers) pairs. Best is the highest
    ops/sec, worst the lowest; ties keep the earliest run, so
    the pick is stable. Returns (best_numbers, worst_numbers,
    ops_sec_delta_pct, p99_delta_pct), or None when there are
    no usable runs.
    """
    if not runs:
        return None
    best = max(runs, key=lambda pair: pair[1]["ops_per_sec"])
    worst = min(runs, key=lambda pair: pair[1]["ops_per_sec"])
    return (
        best[1],
        worst[1],
        _delta_pct(best[1]["ops_per_sec"], worst[1]["ops_per_sec"]),
        _delta_pct(best[1]["p99_ms"], worst[1]["p99_ms"]),
    )


def best_worst_comparison(entries, workload=None):
    """Best-run vs worst-run comparison per backend.

    ``entries`` are in file order (oldest first). Each backend
    gets its best run (highest ops/sec) and its worst run
    (lowest), with ops/sec and p99 for each and the percentage
    change between them — best vs worst, so a wide spread says
    the backend's performance varies a lot run to run, while a
    narrow one says it is steady. The same pick is made per
    operation type (SET, GET, INCR) from the per-op data, so a
    slow-write run that still won overall still shows up as the
    worst SET run. When ``workload`` is given (a workload dict
    in the shape of a history record's ``workload``), only runs
    whose workload matches it are considered — the same
    apples-to-apples rule ``--compare-same-workload`` uses, so a
    quick smoke run never becomes the worst run next to a
    full-size one. A backend with a single usable run has that
    run as both best and worst, with 0.0% deltas; runs recorded
    before 0.22.0 carry no ``per_op`` data and simply get no
    per-op sections.

    Returns a JSON-serializable dict with a ``backends`` list.
    """
    if workload is not None:
        entries = [e for e in entries
                   if workload_matches(e.get("workload"), workload)]
    by_backend = {}
    for entry in entries:
        by_backend.setdefault(entry.get("backend", "?"), []).append(entry)
    backends = []
    for backend, runs in by_backend.items():
        usable = [(e, _run_numbers(e)) for e in runs]
        usable = [(e, n) for e, n in usable if n is not None]
        pair = _best_worst_pair(usable)
        if pair is None:
            continue
        best, worst, ops_delta, p99_delta = pair
        per_op = {}
        for op in ("SET", "GET", "INCR"):
            op_runs = []
            for entry in runs:
                op_entry = (entry.get("per_op") or {}).get(op)
                if not isinstance(op_entry, dict):
                    continue
                numbers = _run_numbers(op_entry)
                if numbers is None:
                    continue
                # A per-op entry carries no timestamp of its own;
                # the table shows the run it came from.
                numbers["timestamp"] = entry.get(
                    "timestamp", "")[:19].replace("T", " ")
                op_runs.append((entry, numbers))
            op_pair = _best_worst_pair(op_runs)
            if op_pair is None:
                continue
            op_best, op_worst, op_ops_delta, op_p99_delta = op_pair
            per_op[op] = {
                "best": op_best,
                "worst": op_worst,
                "ops_sec_delta_pct": op_ops_delta,
                "p99_delta_pct": op_p99_delta,
            }
        backends.append({
            "backend": backend,
            "runs": len(usable),
            "best": best,
            "worst": worst,
            "ops_sec_delta_pct": ops_delta,
            "p99_delta_pct": p99_delta,
            "per_op": per_op,
        })
    return {
        "workload": workload,
        "backends": backends,
    }


def _fmt_ops_sec(value):
    return f"{value:,.0f}" if isinstance(value, (int, float)) else "-"


def _fmt_p99(value):
    return f"{value:.3f}" if isinstance(value, (int, float)) else "-"


def _fmt_delta(value):
    return f"{value:+.1f}%" if value is not None else "n/a"


def _best_worst_lines(title, best, worst, ops_delta, p99_delta):
    """One best-vs-worst table: ops/sec and p99 side by side."""
    return [
        f"#### {title}",
        "",
        "| | Best run | Worst run | Best vs worst |",
        "| --- | ---: | ---: | ---: |",
        f"| Time (UTC) | {best['timestamp']} | "
        f"{worst['timestamp']} | |",
        f"| Ops/sec | {_fmt_ops_sec(best['ops_per_sec'])} | "
        f"{_fmt_ops_sec(worst['ops_per_sec'])} | "
        f"{_fmt_delta(ops_delta)} |",
        f"| p99 (ms) | {_fmt_p99(best['p99_ms'])} | "
        f"{_fmt_p99(worst['p99_ms'])} | "
        f"{_fmt_delta(p99_delta)} |",
        "",
    ]


def _describe_workload(workload):
    """A one-line summary of a workload dict for the filter note."""
    if not isinstance(workload, dict):
        return ""
    mix = workload.get("mix") or {}
    parts = []
    if workload.get("ops") is not None:
        parts.append(f"ops={workload['ops']:,}")
    if workload.get("duration_s") is not None:
        parts.append(f"duration={workload['duration_s']}s")
    parts.append(f"clients={workload.get('clients')}")
    parts.append(f"keys={workload.get('keys')}")
    parts.append(
        f"mix={mix.get('set')}:{mix.get('get')}:{mix.get('incr')}")
    parts.append(f"pipeline={workload.get('pipeline')}")
    if workload.get("distribution") not in (None, "uniform"):
        parts.append(f"distribution={workload['distribution']}")
    return ", ".join(parts)


def format_best_worst(entries, workload=None):
    """Render the best-vs-worst comparison as Markdown.

    One section per backend: a table with the best and worst
    runs' ops/sec and p99 and the percentage change between
    them, then the same table per operation type that ran. When
    ``workload`` is given, only runs matching it were compared,
    and the header says which workload that was. An empty
    history — or a filter that matches nothing — prints the
    same "No runs recorded yet" message as ``--trend``.
    """
    comparison = best_worst_comparison(entries, workload=workload)
    if not comparison["backends"]:
        return "No runs recorded yet. Re-run a benchmark with --history."
    sections = ["## kvbench best vs worst run", ""]
    if workload is not None:
        sections.append(
            "Only runs with the same workload as the latest "
            "recorded run are compared "
            f"({_describe_workload(workload)}).")
        sections.append("")
    for backend in comparison["backends"]:
        sections.append(f"### {backend['backend']}")
        sections.append("")
        if backend["runs"] == 1:
            sections.append(
                "A single recorded run is both the best and the "
                "worst — there is no spread to compare yet.")
            sections.append("")
        sections.extend(_best_worst_lines(
            "Overall", backend["best"], backend["worst"],
            backend["ops_sec_delta_pct"], backend["p99_delta_pct"]))
        for op in ("SET", "GET", "INCR"):
            op_entry = backend["per_op"].get(op)
            if op_entry is None:
                continue
            sections.extend(_best_worst_lines(
                f"{op}", op_entry["best"], op_entry["worst"],
                op_entry["ops_sec_delta_pct"],
                op_entry["p99_delta_pct"]))
    return "\n".join(sections)


def write_best_worst(path, entries, workload=None):
    """Render the best-vs-worst comparison and write it to ``path``.

    Creates the parent directory when needed. Returns the rendered
    text — the same text ``--best-worst`` prints to stdout — so
    the file can be committed or attached to a report.
    """
    text = format_best_worst(entries, workload=workload)
    directory = os.path.dirname(os.path.abspath(path))
    os.makedirs(directory, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)
        if not text.endswith("\n"):
            f.write("\n")
    return text


def write_best_worst_json(path, entries, workload=None):
    """Write the best-vs-worst comparison as JSON to ``path``.

    Creates the parent directory when needed. Returns the
    comparison dict — the same shape ``best_worst_comparison``
    returns — so a script can read the spread without parsing
    the Markdown table.
    """
    comparison = best_worst_comparison(entries, workload=workload)
    directory = os.path.dirname(os.path.abspath(path))
    os.makedirs(directory, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(comparison, f, indent=2)
        f.write("\n")
    return comparison


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
