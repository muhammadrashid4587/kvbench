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
SET, GET, INCR and DEL separately — a slow write no longer hides
inside the overall ops/sec. ``--best-worst`` picks the best and
worst runs per backend (and per operation) out of the same
history, with the spread between them, so a backend whose
throughput swings run to run reads at a glance. Since 0.37.0
each record also carries its sweep steps — one ops/sec and p99
per keyspace size, client count, value size and pipeline depth —
so the trend view tracks every sweep step on its own, and a step's
throughput is only ever compared with the same step in other
runs.
"""

import json
import os
import tempfile
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


def _sweeps_record(result):
    """A run's sweep steps in trend shape, or {} when no sweep ran.

    One trimmed entry per step — the step value (``keys``,
    ``clients``, ``value_size`` or ``pipeline``), ops, ops/sec,
    mean and p99 —
    per sweep kind that ran, keyed ``keyspace`` / ``clients`` /
    ``value_size`` / ``pipeline``. The per-op histograms and repeat detail stay
    in the JSON report; the trend only needs ops/sec and p99 per
    step, the same numbers the overall trend tracks per run, so a
    sweep step's ops/sec at 100 keys is only ever compared with
    the same step in other runs.
    """
    sweeps = {}
    for kind, steps, step_key in (
            ("keyspace", result.sweep, "keys"),
            ("clients", result.client_sweep, "clients"),
            ("value_size", result.value_sweep, "value_size"),
            ("pipeline", result.pipeline_sweep, "pipeline")):
        entries = [
            {step_key: step[step_key],
             "ops": step["ops"],
             "ops_per_sec": step["ops_per_sec"],
             "mean_ms": step["mean_ms"],
             "p99_ms": step["p99_ms"]}
            for step in steps
        ]
        if entries:
            sweeps[kind] = entries
    return sweeps


def build_record(result, workload, command_line):
    """Build one history record for a single backend result."""
    summary = result.summary
    record = {
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
    # Sweep steps, so the trend view can track each step over
    # time: a keyspace step's ops/sec at 100 keys is only
    # comparable with the same step in other runs. Only the
    # numbers the trend needs are kept — ops, ops/sec, mean and
    # p99 — not the per-op histograms or repeat detail, which
    # already live in the JSON report. Only kinds that ran are
    # recorded, so a run with no sweep keeps the old record shape
    # exactly; records written before 0.37.0 carry no "sweeps"
    # key at all, and the trend view skips those runs for the
    # sweep sections instead of failing.
    sweeps = _sweeps_record(result)
    if sweeps:
        record["sweeps"] = sweeps
    return record


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
    for op in ("SET", "GET", "INCR", "DEL"):
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


# (history key, step key, section label, step unit) for the four
# sweep kinds, in the order the report renders their sections.
_SWEEP_KINDS = (
    ("keyspace", "keys", "Keyspace sweep", "keys"),
    ("clients", "clients", "Client sweep", "clients"),
    ("value_size", "value_size", "Value size sweep", "byte values"),
    ("pipeline", "pipeline", "Pipeline sweep", "commands"),
)


def _sweep_step_entry(entry, kind, step_key, step_value):
    """The recorded step of one sweep kind/value in ``entry``, or None.

    A run that ran no sweep of that kind — or ran it at other step
    values — has no entry for this step, so it is skipped for this
    section instead of inventing one.
    """
    for step in (entry.get("sweeps") or {}).get(kind, []):
        if isinstance(step, dict) and step.get(step_key) == step_value:
            return step
    return None


def _sweep_trend_lines(runs, window):
    """Per-sweep-step trend sections for one backend.

    ``runs`` are all recorded runs of the backend (oldest first),
    ``window`` the most recent ones being rendered. Each sweep step
    that ran — one keyspace size, one client count, one value size,
    one pipeline depth —
    gets its own table: ops/sec, the change versus the previous run
    that recorded the same step, and p99, plus a sparkline and the
    first-to-last change. The table is the same shape as the
    overall and per-op trend tables, built from the same
    ``_ops_sec_delta`` and ``_sparkline`` helpers, so the views
    cannot disagree. Runs recorded before 0.37.0 carry no
    ``sweeps`` data and are skipped for these sections; a step
    that never ran in the window gets no section at all.
    """
    lines = []
    for kind, step_key, label, unit in _SWEEP_KINDS:
        steps = sorted({
            step.get(step_key)
            for entry in window
            for step in (entry.get("sweeps") or {}).get(kind, [])
            if isinstance(step, dict)
            and isinstance(step.get(step_key), (int, float))
        })
        for step_value in steps:
            # The delta is against the previous run that recorded
            # this same sweep step, scanning all runs so a run
            # outside the window still counts — the same rule the
            # overall and per-op tables use.
            previous_by_id = {}
            previous = None
            for entry in runs:
                step = _sweep_step_entry(entry, kind, step_key,
                                         step_value)
                if step is None:
                    previous_by_id[id(entry)] = None
                    continue
                previous_by_id[id(entry)] = _ops_sec_delta(step, previous)
                previous = step
            values = [
                _sweep_step_entry(entry, kind, step_key, step_value)
                for entry in window
            ]
            values = [step.get("ops_per_sec") for step in values
                      if step is not None
                      and isinstance(step.get("ops_per_sec"),
                                     (int, float))]
            if not values:
                continue
            step_label = f"{step_value:,} {unit}"
            lines.append(
                f"#### {label} — {step_label} ops/sec trend")
            lines.append("")
            lines.append("| Time (UTC) | Ops/sec | Ops/sec vs prev | "
                         "p99 (ms) |")
            lines.append("| --- | ---: | ---: | ---: |")
            for entry in reversed(window):
                step = _sweep_step_entry(entry, kind, step_key,
                                         step_value)
                if step is None:
                    continue
                timestamp = entry.get("timestamp", "")[:19].replace(
                    "T", " ")
                ops_sec = step.get("ops_per_sec")
                ops_cell = (f"{ops_sec:,.0f}"
                            if isinstance(ops_sec, (int, float)) else "-")
                p99 = step.get("p99_ms")
                p99_cell = (f"{p99:.3f}"
                            if isinstance(p99, (int, float)) else "-")
                lines.append(
                    f"| {timestamp} | {ops_cell} | "
                    f"{previous_by_id[id(entry)]} | {p99_cell} |"
                )
            lines.append("")
            lines.append(
                f"{label} — {step_label} trend (oldest → newest): "
                f"{_sparkline(values)}")
            if len(values) >= 2:
                first, last = values[0], values[-1]
                change = (last - first) / first * 100 if first else 0.0
                lines.append(
                    f"{label} — {step_label} first → last: "
                    f"{first:,.0f} → {last:,.0f} "
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
    the same chart per operation type (SET, GET, INCR, DEL) for the ops
    that ran, so each op's throughput trend reads on its own, and
    the same chart per sweep step (keyspace size, client count,
    value size, pipeline depth) for the steps that ran, so a step's throughput
    trend reads on its own too. A corrupt line never reaches here
    — ``load_history`` already skips it — and a backend with a
    single recorded run shows no delta, just its one value.
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
        # Per-sweep-step trend, same shape again, one section per
        # sweep step that ran in this window.
        sections.extend(_sweep_trend_lines(runs, window))
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
    operation type (SET, GET, INCR, DEL) from the per-op data, so a
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
        for op in ("SET", "GET", "INCR", "DEL"):
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
        for op in ("SET", "GET", "INCR", "DEL"):
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


def _parse_record_timestamp(record):
    """A record's timestamp as an aware datetime, or None.

    Records written by kvbench carry ISO timestamps with a UTC
    offset; a record with a missing or unparseable timestamp
    cannot be dated, so pruning by age leaves it alone rather
    than guessing.
    """
    text = record.get("timestamp")
    if not isinstance(text, str):
        return None
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def prune_history(path, keep=None, before=None, backends=None,
                  allow_all=False, dry_run=False):
    """Prune old runs from the history file, per backend.

    ``keep`` keeps the N most recent runs of each backend;
    ``before`` (an aware datetime) drops runs older than that
    time; both may be given, and a run must pass both to stay.
    ``backends`` limits the prune to the named backends — other
    backends are left alone entirely. With ``allow_all`` every
    run of the selected backends is removed (``keep`` and
    ``before`` must be None then); without it a backend is never
    left with zero runs, so a prune that would empty a backend
    raises ValueError instead of deleting its whole history —
    pass ``allow_all`` explicitly to clear a backend out. A dry
    run computes the same selection but writes nothing.

    Returns a summary dict: total removed and kept counts, and
    per backend the removed and kept counts plus the timestamp
    range of the removed runs (oldest to newest, or None when
    nothing was removed for that backend). The file is rewritten
    atomically (temp file + rename), keeping the surviving runs
    in their original order; corrupt lines are dropped on
    rewrite, since ``load_history`` already skips them.
    """
    entries = load_history(path)
    if not entries:
        return {"removed": 0, "kept": 0, "backends": {},
                "dry_run": dry_run}
    # Group runs per backend, keeping first-seen backend order;
    # the selection is per backend, so a busy backend never
    # crowds a quiet one out of the history.
    by_backend = {}
    for entry in entries:
        by_backend.setdefault(entry.get("backend", "?"), []).append(entry)
    selected = set(backends) if backends else None
    kept_ids = set()
    summary_backends = {}
    for backend, runs in by_backend.items():
        if selected is not None and backend not in selected:
            for entry in runs:
                kept_ids.add(id(entry))
            summary_backends[backend] = {
                "removed": 0, "kept": len(runs), "removed_range": None,
            }
            continue
        if allow_all:
            remove = list(runs)
        else:
            remove = []
            keep_from = len(runs) - keep if keep is not None else 0
            for index, entry in enumerate(runs):
                if index < keep_from:
                    remove.append(entry)
                    continue
                timestamp = _parse_record_timestamp(entry)
                if (before is not None and timestamp is not None
                        and timestamp < before):
                    remove.append(entry)
        remove_ids = {id(entry) for entry in remove}
        kept = [entry for entry in runs
                if id(entry) not in remove_ids]
        if not allow_all and not kept:
            raise ValueError(
                f"pruning would remove every recorded run of "
                f"{backend!r}: pass --prune-all to clear a backend "
                f"out entirely")
        for entry in kept:
            kept_ids.add(id(entry))
        timestamps = sorted(str(e.get("timestamp", "")) for e in remove)
        summary_backends[backend] = {
            "removed": len(remove),
            "kept": len(kept),
            "removed_range": (
                (timestamps[0][:19].replace("T", " "),
                 timestamps[-1][:19].replace("T", " "))
                if timestamps else None),
        }
    removed = sum(b["removed"] for b in summary_backends.values())
    kept_total = sum(b["kept"] for b in summary_backends.values())
    if removed and not dry_run:
        kept_entries = [entry for entry in entries
                        if id(entry) in kept_ids]
        directory = os.path.dirname(os.path.abspath(path))
        os.makedirs(directory, exist_ok=True)
        fd, tmp_path = tempfile.mkstemp(
            dir=directory, prefix=".history-", suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                for entry in kept_entries:
                    f.write(json.dumps(entry) + "\n")
            os.replace(tmp_path, path)
        except BaseException:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise
    return {"removed": removed, "kept": kept_total,
            "backends": summary_backends, "dry_run": dry_run}


def format_prune(summary, path):
    """Render a prune summary as text.

    One line per backend — how many runs were removed (with the
    timestamp range of the removed runs) and how many were kept —
    plus a total. A dry run says so up front and reports what
    would be removed; an empty history prints the same "No runs
    recorded yet" message as ``--show-history``.
    """
    if not summary["backends"]:
        return "No runs recorded yet. Re-run a benchmark with --history."
    verb = "would remove" if summary["dry_run"] else "removed"
    lines = []
    if summary["dry_run"]:
        lines.append(f"Dry run — nothing was removed from {path}:")
    else:
        lines.append(f"Pruned {path}:")
    lines.append("")
    for backend, counts in summary["backends"].items():
        removed = counts["removed"]
        kept = counts["kept"]
        if removed:
            oldest, newest = counts["removed_range"]
            lines.append(
                f"- {backend}: {verb} {removed} run(s) "
                f"({oldest} to {newest}), kept {kept} run(s)")
        else:
            lines.append(
                f"- {backend}: nothing to prune, kept {kept} run(s)")
    lines.append("")
    if summary["removed"]:
        lines.append(
            f"{'Would remove' if summary['dry_run'] else 'Removed'} "
            f"{summary['removed']} run(s), kept {summary['kept']} "
            f"run(s) in total.")
    else:
        lines.append("Nothing to prune.")
    return "\n".join(lines)


def export_history(path, entries, backends=None):
    """Write history entries as portable JSON to ``path``.

    The file is one JSON document — ``{"tool": "kvbench",
    "version": ..., "exported_at": ..., "records": [...]}`` — so
    a history can move between machines and be merged back with
    ``import_history``. ``backends`` limits the export to the
    named backends; None exports every backend. Creates the
    parent directory when needed. Returns the exported document.
    """
    selected = set(backends) if backends else None
    records = [entry for entry in entries
               if selected is None
               or entry.get("backend") in selected]
    document = {
        "tool": "kvbench",
        "version": __version__,
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "records": records,
    }
    directory = os.path.dirname(os.path.abspath(path))
    os.makedirs(directory, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(document, f, indent=2)
        f.write("\n")
    return document


def load_export(path):
    """Load an exported history file, or raise ValueError.

    Validates the file shape: it must be a JSON object with a
    ``records`` list, and every record must be an object carrying
    a ``backend`` and a ``timestamp`` — the (backend, timestamp)
    pair ``import_history`` merges on. A missing or unreadable
    file, invalid JSON, or a record without those fields is bad
    input, so nothing is imported from it.
    """
    try:
        with open(path, encoding="utf-8") as f:
            document = json.load(f)
    except OSError as exc:
        raise ValueError(
            f"cannot read history export {path}: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"history export {path} is not valid JSON: {exc}") from exc
    if not isinstance(document, dict):
        raise ValueError(
            f"history export {path} must be a JSON object with a "
            f"'records' list")
    records = document.get("records")
    if not isinstance(records, list):
        raise ValueError(
            f"history export {path} must carry a 'records' list")
    for record in records:
        if not isinstance(record, dict):
            raise ValueError(
                f"history export {path} holds a record that is not "
                f"a JSON object")
        backend = record.get("backend")
        timestamp = record.get("timestamp")
        if not isinstance(backend, str) or not backend:
            raise ValueError(
                f"history export {path} holds a record with no "
                f"'backend'")
        if not isinstance(timestamp, str) or not timestamp:
            raise ValueError(
                f"history export {path} holds a record with no "
                f"'timestamp'")
    return records


def import_history(path, history_path):
    """Merge an exported history file into the history file.

    ``path`` is an export written by ``export_history`` (validated
    by ``load_export`` first — a bad file raises ValueError and
    nothing is merged). Records merge by (backend, timestamp): a
    record whose key is not in the history is appended; one whose
    key exists and is identical is skipped as a duplicate; one
    whose key exists but differs is a conflict — the existing
    record is kept and the imported one dropped, so an import
    never overwrites a recorded run. The file is rewritten
    atomically (temp file + rename) only when new records were
    added, keeping the surviving runs in their original order
    with new records appended in export order; corrupt lines are
    dropped on rewrite, since ``load_history`` already skips them.

    Returns a summary dict: added, duplicates and conflicts
    counts.
    """
    imported = load_export(path)
    existing = load_history(history_path)
    by_key = {}
    for entry in existing:
        by_key[(entry.get("backend"), entry.get("timestamp"))] = entry
    added = duplicates = conflicts = 0
    new_entries = []
    for record in imported:
        key = (record.get("backend"), record.get("timestamp"))
        current = by_key.get(key)
        if current is None:
            new_entries.append(record)
            by_key[key] = record
            added += 1
        elif current == record:
            duplicates += 1
        else:
            conflicts += 1
    if new_entries:
        directory = os.path.dirname(os.path.abspath(history_path))
        os.makedirs(directory, exist_ok=True)
        fd, tmp_path = tempfile.mkstemp(
            dir=directory, prefix=".history-", suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                for entry in existing + new_entries:
                    f.write(json.dumps(entry) + "\n")
            os.replace(tmp_path, history_path)
        except BaseException:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise
    return {"added": added, "duplicates": duplicates,
            "conflicts": conflicts}


def format_import(summary, export_path, history_path):
    """Render an import summary as text.

    One line per outcome — how many runs were added, how many
    were already recorded (duplicates), and how many conflicted
    with an existing run (the existing record was kept). An
    export that held no new runs says so instead of reporting
    zeroes.
    """
    lines = [f"Imported {export_path} into {history_path}:"]
    lines.append("")
    if summary["added"]:
        lines.append(f"- added {summary['added']} run(s)")
    if summary["duplicates"]:
        lines.append(
            f"- skipped {summary['duplicates']} duplicate run(s) "
            f"already in the history")
    if summary["conflicts"]:
        lines.append(
            f"- kept the existing record for "
            f"{summary['conflicts']} conflicting run(s) (same "
            f"backend and timestamp, different content)")
    if not (summary["added"] or summary["duplicates"]
            or summary["conflicts"]):
        lines.append("- nothing new to import: the export held no "
                     "records")
    lines.append("")
    return "\n".join(lines)


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
