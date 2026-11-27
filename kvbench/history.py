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
throughput swings run to run reads at a glance. Since 0.43.0
each record also carries its per-operation latency histograms,
so the best/worst view shows each picked run's latency shape —
the best and worst runs' full per-op maps, and each per-op
pick's own buckets — drawn with the same shared helper as the
single-run report, so the views cannot disagree. Since 0.44.0
the trend view shows each backend's latest run's per-operation
latency histograms too, drawn with the same shared helper, so
a p99 move in the trend names the op whose latency shape moved.
Since 0.45.0 the trend view also compares the first and last
runs' per-operation latency histograms — a per-op table of the
p50/p99 shift and the bucket drift, plus the first run's
histogram block drawn with the same shared helper next to the
latest run's — so a p99 move in the trend shows how each op's
latency shape moved between the first and last runs. Since
0.46.0 the best/worst view compares the best and worst runs'
per-operation latency histograms the same way — the same
per-op table, built from the same shared rows, so the two
views cannot disagree — so a p99 move between the runs shows
how each op's latency shape moved.
Since 0.37.0
each record also carries its sweep steps — one ops/sec and p99
per keyspace size, client count, value size and pipeline depth —
so the trend view tracks every sweep step on its own, and a step's
throughput is only ever compared with the same step in other
runs. Since 0.49.0 each recorded step also carries its
per-operation latency histograms, so the trend view shows each
step's latency shape — the same log-spaced buckets per operation
as the main run's ``histograms_by_op`` — not just its p99.
Since 0.50.0 the best/worst view shows the same per-step
treatment — each sweep step's best and worst runs by the step's
own ops/sec, with the step's per-op latency histograms from
both runs — so a step's spread reads the same way in both
views. Since 0.51.0 the ``--best-worst-json`` output carries
the same per-step sections under each backend's ``sweeps`` —
the step's best and worst runs, the deltas, and the step's
per-op histograms from both runs — so a script reading the
JSON sees what the Markdown table shows. Since 0.52.0 each
per-step section also carries the step's per-op histogram
comparison between its best and worst runs — each op's p50/p99
in the two steps, the p99 change, and the bucket drift — in
both the Markdown and JSON views, built from the same shared
rows, so the views cannot disagree; a step record carries no
per-op percentile fields, so the step's p50/p99 are read off
its histogram buckets.
Since 0.53.0 ``--trend-json`` writes the same trend as JSON —
the runs table, sparkline and first-to-last change per backend,
the per-op sections, the latest run's per-op histograms and the
first-to-last histogram comparison — with the same per-sweep-step
sections ``--best-worst-json`` carries, built from the same
shared helper, so a script can read the trend without parsing
the Markdown chart.
"""

import json
import os
import tempfile
from datetime import datetime, timezone

from . import __version__
from .report import _per_op_histogram_block

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
    record = {
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
            # "del" belongs here too: the recorded mix must match
            # the JSON report's (_workload_dict), or
            # --compare-same-workload never matches a recorded run
            # — the two dicts differ on this key alone. Records
            # written before 0.39.0 carry no "del" and still do not
            # match; new ones do.
            "del": workload.del_weight,
        },
        "mix_label": workload.mix_label or None,
        "seed": workload.seed,
        "pipeline": workload.pipeline,
        "distribution": workload.distribution,
        "zipf_skew": (workload.zipf_skew
                      if workload.distribution == "zipfian" else None),
    }
    if workload.workload_file is not None:
        # Only a run from a config file records it: a command-line
        # run keeps the old record shape exactly. The resolved
        # values are the fields above, so the run is reproducible
        # from this record even without the file.
        record["workload_file"] = workload.workload_file
    return record


def _sweeps_record(result):
    """A run's sweep steps in trend shape, or {} when no sweep ran.

    One trimmed entry per step — the step value (``keys``,
    ``clients``, ``value_size``, ``pipeline`` or ``mix``), ops,
    ops/sec, mean and p99, plus the step's per-operation latency
    histograms (``histograms_by_op`` — the same log-spaced buckets
    per operation as the main run's, so the trend view can show
    each step's latency shape, not just its p99) —
    per sweep kind that ran, keyed ``keyspace`` / ``clients`` /
    ``value_size`` / ``pipeline`` / ``mix``. The repeat detail
    stays in the JSON report; the trend only needs ops/sec, p99
    and the per-op histograms per step, the same numbers the
    overall trend tracks per run, so a sweep step's ops/sec at
    100 keys is only ever compared with the same step in other
    runs. A step that carries no ``histograms_by_op`` key — a
    hand-built result, or a run recorded before 0.49.0 —
    records none, and the trend renders a note naming that
    instead of a histogram block.
    """
    sweeps = {}
    for kind, steps, step_key in (
            ("keyspace", result.sweep, "keys"),
            ("clients", result.client_sweep, "clients"),
            ("value_size", result.value_sweep, "value_size"),
            ("pipeline", result.pipeline_sweep, "pipeline"),
            ("mix", result.mix_sweep, "mix")):
        entries = []
        for step in steps:
            entry = {step_key: step[step_key],
                     "ops": step["ops"],
                     "ops_per_sec": step["ops_per_sec"],
                     "mean_ms": step["mean_ms"],
                     "p99_ms": step["p99_ms"]}
            if "histograms_by_op" in step:
                entry["histograms_by_op"] = step["histograms_by_op"]
            entries.append(entry)
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
        # Per-operation latency histograms — the same log-spaced
        # buckets per operation that ran as the JSON report's
        # ``histograms_by_op`` — so the best/worst view can show
        # each picked run's latency shape, not just its p99.
        # Records written before 0.43.0 carry no
        # "histograms_by_op" key at all, and the best/worst view
        # renders no histogram blocks for those runs instead of
        # failing.
        "histograms_by_op": result.per_op_histograms,
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


def _p99_first_last_line(label, p99_values):
    """A p99 first-to-last change line, or None.

    ``label`` is the text before "first → last" — "p99" for the
    overall trend, "SET p99" for a per-op section, "Keyspace sweep
    — 100 keys p99" for a sweep step — and ``p99_values`` the
    recorded p99s in window order (oldest first). Fewer than two
    values means there is no change to report: a single run, or
    runs whose recorded p99 is missing, show no line. One helper
    formats the line for every trend section, so the overall,
    per-op and sweep-step views cannot disagree.
    """
    if len(p99_values) < 2:
        return None
    first, last = p99_values[0], p99_values[-1]
    change = (last - first) / first * 100 if first else 0.0
    return (f"{label} first → last: {first:.3f} → {last:.3f} ms "
            f"({change:+.1f}%)")


def _per_op_trend_lines(runs, window):
    """Per-operation trend sections for one backend.

    ``runs`` are all recorded runs of the backend (oldest first),
    ``window`` the most recent ones being rendered. Each operation
    type that ran gets its own table — ops/sec, the change versus
    the previous run of this backend that ran the same op, and p99
    — plus a sparkline and the first-to-last change in ops/sec and
    in p99. Runs recorded before 0.22.0 carry no ``per_op`` data
    and are skipped for these sections; an op that never ran in
    the window gets no section at all.
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
        # The p99 first-to-last change, alongside the ops/sec one:
        # an op whose tail crept up reads at a glance even when
        # its throughput moved the other way. A run that never
        # ran the op, or one whose recorded p99 is missing, is
        # skipped, like a run with no recorded ops/sec.
        p99_values = []
        for entry in window:
            op_entry = (entry.get("per_op") or {}).get(op)
            if op_entry is None:
                continue
            p99 = op_entry.get("p99_ms")
            if isinstance(p99, (int, float)):
                p99_values.append(p99)
        line = _p99_first_last_line(f"{op} p99", p99_values)
        if line is not None:
            lines.append(line)
        lines.append("")
    return lines


# (history key, step key, section label, step unit) for the five
# sweep kinds, in the order the report renders their sections.
# The mix kind's step value is a "READ:WRITE" string, not a number,
# so the trend renders it as e.g. "90:10 read:write" instead of
# "90,10 read:write".
_SWEEP_KINDS = (
    ("keyspace", "keys", "Keyspace sweep", "keys"),
    ("clients", "clients", "Client sweep", "clients"),
    ("value_size", "value_size", "Value size sweep", "byte values"),
    ("pipeline", "pipeline", "Pipeline sweep", "commands"),
    ("mix", "mix", "Mix sweep", "read:write"),
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


def _sweep_step_histograms(step):
    """A sweep step's cleaned per-op histograms, or the reason there are none.

    Returns (histograms, reason): ``histograms`` is the cleaned
    per-op mapping when the step carries valid bucket lists,
    else None; ``reason`` is None then, or one of
    ``"before-0.49"``, ``"no-latencies"``, ``"malformed"`` naming
    why the step carries no drawable histograms — a step
    recorded before 0.49.0 carries no ``histograms_by_op`` key
    at all, a step recorded with no per-op latencies carries an
    empty mapping, and a hand-made history file may carry
    something else entirely. Both the Markdown and JSON views
    build their notes from this one helper, so the two views
    cannot disagree about why a step has no histograms.
    """
    histograms = step.get("histograms_by_op")
    if histograms is None:
        return None, "before-0.49"
    cleaned = _histograms_or_none(histograms)
    if cleaned is not None:
        return cleaned, None
    if not histograms:
        return None, "no-latencies"
    return None, "malformed"


_SWEEP_STEP_HISTOGRAM_REASONS = {
    "before-0.49": "this step was recorded before 0.49.0, so it "
                   "carries no per-op histograms.",
    "no-latencies": "this step recorded no per-op latencies, so "
                    "there are no histograms to show.",
    "malformed": "the recorded per-op histograms for this step "
                 "are malformed, so no histograms are shown.",
}


def _sweep_step_histogram_block(label, step_label, entry, step, pick):
    """Per-op latency histogram block for one sweep step of one run, or a note.

    ``pick`` names which run the step comes from — "latest" for
    the trend view, "best" or "worst" for the best/worst view —
    so the heading and the note name the run instead of leaving
    two blocks (or two notes) for one step indistinguishable.
    The block is drawn with the same shared helper the latest
    run's block and the single-run report use, so the views
    cannot disagree. A step recorded before 0.49.0 carries no
    ``histograms_by_op`` key at all; a step recorded with no
    per-op latencies carries an empty mapping; a hand-made
    history file may carry something else entirely — each
    renders a note naming the reason instead of a block, so the
    views never crash on a record shape they cannot draw. The
    returned lines carry no trailing blank line; the caller
    adds the section separator.
    """
    timestamp = entry.get("timestamp", "")[:19].replace("T", " ")
    heading = (f"#### {label} — {step_label} — per-op latency "
               f"histogram — {pick} run ({timestamp})")
    context = f"{label} — {step_label} ({pick} run, {timestamp})"
    cleaned, reason = _sweep_step_histograms(step)
    if cleaned is not None:
        block = _per_op_histogram_block(heading, cleaned)
        while block and block[-1] == "":
            block.pop()
        return block
    return [
        f"- {context}: {_SWEEP_STEP_HISTOGRAM_REASONS[reason]}",
    ]


def _sweep_step_histogram_lines(label, step_label, window, kind,
                                step_key, step_value):
    """Per-op latency histogram block for one sweep step, or a note.

    ``window`` is the trend window (oldest first); the block shows
    the latest run in it that recorded this step, so a step that
    stopped running still shows the shape it last measured
    instead of nothing. The block is drawn with the same shared
    helper the latest run's block and the single-run report use,
    so the views cannot disagree. A step recorded before 0.49.0
    carries no ``histograms_by_op`` key at all; a step recorded
    with no per-op latencies carries an empty mapping; a
    hand-made history file may carry something else entirely —
    each renders a note naming the reason instead of a block, so
    the trend never crashes on a record shape it cannot draw.
    The returned lines carry no trailing blank line; the caller
    adds the section separator.
    """
    latest = None
    latest_step = None
    for entry in reversed(window):
        step = _sweep_step_entry(entry, kind, step_key, step_value)
        if step is not None:
            latest = entry
            latest_step = step
            break
    if latest is None:
        return []
    return _sweep_step_histogram_block(
        label, step_label, latest, latest_step, "latest")


def _sweep_trend_lines(runs, window):
    """Per-sweep-step trend sections for one backend.

    ``runs`` are all recorded runs of the backend (oldest first),
    ``window`` the most recent ones being rendered. Each sweep step
    that ran — one keyspace size, one client count, one value size,
    one pipeline depth, one read:write ratio —
    gets its own table: ops/sec, the change versus the previous run
    that recorded the same step, and p99, plus a sparkline and the
    first-to-last change in ops/sec and in p99. The table is the same shape as the
    overall and per-op trend tables, built from the same
    ``_ops_sec_delta`` and ``_sparkline`` helpers, so the views
    cannot disagree. Runs recorded before 0.37.0 carry no
    ``sweeps`` data and are skipped for these sections; a step
    that never ran in the window gets no section at all. Each
    section also ends with the step's per-operation latency
    histograms from the latest run that recorded it — drawn with
    the same shared helper as the latest run's block, so a p99
    move in a step names the op whose latency shape moved — or a
    note naming why there are none (recorded before 0.49.0, no
    per-op latencies, or malformed histograms).
    """
    lines = []
    for kind, step_key, label, unit in _SWEEP_KINDS:
        steps = sorted({
            step.get(step_key)
            for entry in window
            for step in (entry.get("sweeps") or {}).get(kind, [])
            if isinstance(step, dict)
            and isinstance(step.get(step_key), (int, float, str))
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
            if isinstance(step_value, str):
                # A mix ratio reads as "90:10 read:write", not
                # "90:10, read:write" — a string step value has no
                # thousands separator to apply.
                step_label = f"{step_value} {unit}"
            else:
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
            # The p99 first-to-last change, alongside the ops/sec
            # one: a step whose tail crept up reads at a glance
            # even when its throughput moved the other way. A
            # step with no recorded p99 is skipped, like a step
            # with no recorded ops/sec.
            p99_values = []
            for entry in window:
                step = _sweep_step_entry(entry, kind, step_key,
                                         step_value)
                if step is None:
                    continue
                p99 = step.get("p99_ms")
                if isinstance(p99, (int, float)):
                    p99_values.append(p99)
            line = _p99_first_last_line(
                f"{label} — {step_label} p99", p99_values)
            if line is not None:
                lines.append(line)
            # The step's per-op latency histograms from the latest
            # run that recorded it — the same shared helper as
            # the latest run's block — so a p99 move in the step
            # names the op whose latency shape moved.
            lines.extend(_sweep_step_histogram_lines(
                label, step_label, window, kind, step_key,
                step_value))
            lines.append("")
    return lines


def _latest_run_histograms(entry):
    """A trend run's cleaned per-op histograms, or the reason there are none.

    Returns (histograms, reason): ``histograms`` is the cleaned
    per-op mapping when the run carries valid bucket lists,
    else None; ``reason`` is None then, or one of
    ``"before-0.43"``, ``"no-latencies"``, ``"malformed"`` naming
    why the run carries no drawable histograms — a run recorded
    before 0.43.0 carries no ``histograms_by_op`` key at all, a
    run recorded with no per-op latencies carries an empty
    mapping, and a hand-made history file may carry something
    else entirely. Both the Markdown and JSON views build their
    notes from this one helper, so the two views cannot disagree
    about why a run has no histograms.
    """
    histograms = entry.get("histograms_by_op")
    if histograms is None:
        return None, "before-0.43"
    cleaned = _histograms_or_none(histograms)
    if cleaned is not None:
        return cleaned, None
    if not histograms:
        return None, "no-latencies"
    return None, "malformed"


_TREND_HISTOGRAM_REASONS = {
    "before-0.43": "recorded before 0.43.0, so this run carries "
                   "no per-op histograms.",
    "no-latencies": "this run recorded no per-op latencies, so "
                    "there are no histograms to show.",
    "malformed": "the recorded per-op histograms are malformed, "
                 "so no histograms are shown.",
}


def _trend_histogram_lines(entry):
    """Per-op latency histogram block for one trend run, or a note.

    ``entry`` is the backend's latest run in the trend window —
    the last trend step — so the chart ends with where each
    op's latency sits now, not just where throughput went. The
    block is drawn with the same shared helper the best/worst
    view and the single-run report use, so the views cannot
    disagree. A run recorded before 0.43.0 carries no
    ``histograms_by_op`` key at all; a run recorded with no
    per-op latencies carries an empty mapping; a hand-made
    history file may carry something else entirely — each
    renders a note naming the reason instead of a block, so the
    trend never crashes on a record shape it cannot draw.
    """
    timestamp = entry.get("timestamp", "")[:19].replace("T", " ")
    cleaned, reason = _latest_run_histograms(entry)
    if cleaned is not None:
        return _per_op_histogram_block(
            "#### Per-op latency histogram — latest run "
            f"({timestamp})",
            cleaned)
    return [
        f"- {timestamp}: {_TREND_HISTOGRAM_REASONS[reason]}",
        "",
    ]


def _bucket_drift(first_buckets, last_buckets):
    """Largest single-bucket share move between two bucket lists.

    Returns ``(change_pp, label)`` — the signed change in
    percentage points of the bucket whose share of ops moved
    most between the first and last runs, and that bucket's
    label (``<= 0.25 ms`` or ``> 1000 ms`` for the overflow
    bucket) — or None when either list holds no samples. Buckets
    are scanned in edge order, so a tie keeps the fastest
    bucket and the pick is stable.
    """
    first_total = sum(b["count"] for b in first_buckets)
    last_total = sum(b["count"] for b in last_buckets)
    if not first_total or not last_total:
        return None
    first_shares = {b["upper_ms"]: b["count"] / first_total * 100
                    for b in first_buckets}
    last_shares = {b["upper_ms"]: b["count"] / last_total * 100
                   for b in last_buckets}
    # The overflow bucket (upper_ms None) sorts last.
    edges = sorted(set(first_shares) | set(last_shares),
                   key=lambda upper: (upper is None, upper or 0))
    best = None
    for upper in edges:
        change = (last_shares.get(upper, 0.0)
                  - first_shares.get(upper, 0.0))
        if best is None or abs(change) > abs(best[0]):
            label = ("> 1000 ms" if upper is None
                     else f"<= {upper:g} ms")
            best = (change, label)
    return best


def _histogram_comparison_notes(first, last, first_label, last_label,
                                comparison_name):
    """Why no per-op histogram comparison is shown, or [].

    ``first``/``last`` are two history records; the labels name
    them in the note ("first"/"last" for the trend,
    "worst"/"best" for best-vs-worst) and ``comparison_name``
    names the comparison ("first-to-last" or "best-vs-worst").
    A run recorded before 0.43.0 carries no ``histograms_by_op``
    key at all; a run recorded with no per-op latencies carries
    an empty mapping; a hand-made history file may carry
    something else entirely — each renders a note naming the
    reason, so neither view crashes on a record shape it cannot
    draw. The notes carry no bullet prefix; callers add one.
    """
    notes = []
    for which, entry in ((first_label, first), (last_label, last)):
        timestamp = entry.get("timestamp", "")[:19].replace("T", " ")
        histograms = entry.get("histograms_by_op")
        if histograms is None:
            notes.append(
                f"The {which} run ({timestamp}) was recorded "
                "before 0.43.0, so this run carries no per-op "
                f"histograms — no {comparison_name} histogram "
                "comparison is shown.")
        elif not histograms:
            notes.append(
                f"The {which} run ({timestamp}) recorded no "
                "per-op latencies, so there are no histograms to "
                f"show — no {comparison_name} histogram "
                "comparison is shown.")
        elif _histograms_or_none(histograms) is None:
            notes.append(
                f"The {which} run's ({timestamp}) recorded "
                "per-op histograms are malformed, so no "
                f"histograms are shown — no {comparison_name} "
                "histogram comparison is shown.")
    return notes


def _histogram_percentile(buckets, p):
    """p-th percentile read off a histogram bucket list, or None.

    ``buckets`` is one op's cleaned bucket list (each bucket a
    dict with ``upper_ms`` and ``count``) and ``p`` a percentile
    in (0, 100]. Nearest-rank, the same rule
    :func:`kvbench.stats.percentile` uses on raw samples: the
    percentile is the upper edge of the bucket holding the
    rank-th sample (``ceil(p/100 * total)``), an upper bound on
    the true percentile since every sample in the bucket sits at
    or below that edge. Returns None when the list holds no
    samples, or when the rank lands in the overflow bucket —
    whose ``upper_ms`` is None, so there is no edge to report.
    """
    import math

    total = sum(b["count"] for b in buckets)
    if not total:
        return None
    rank = math.ceil(p / 100 * total)
    cumulative = 0
    for bucket in buckets:
        cumulative += bucket["count"]
        if cumulative >= rank:
            return bucket["upper_ms"]
    return None


def _histogram_comparison_rows(first, last):
    """Per-op comparison rows between two runs' histograms.

    One row per op that ran in either run: each op's p50 and p99
    in the first and second runs, the p99 percentage change from
    the first run to the second, and the bucket drift (the
    largest single-bucket share move, in percentage points, with
    the bucket it moved in — see :func:`_bucket_drift`). The
    p50/p99 shift reads from each run's ``per_op`` entry
    (records since 0.22.0); an op that never ran in one of the
    runs shows None for that side, and the p99 change is None
    when either side has no p99 or the first p99 is zero. The
    rows name their sides "from" and "to" — the trend compares
    from the first run to the last, best-vs-worst from the worst
    run to the best — so one shape serves both views and they
    cannot disagree. Returns None when either run carries no
    usable histograms; callers check
    :func:`_histogram_comparison_notes` first, which names the
    reason. The rows themselves come from the shared
    :func:`_histogram_comparison_rows_from`, which the sweep-step
    comparison uses too.
    """
    first_clean = _histograms_or_none(first.get("histograms_by_op"))
    last_clean = _histograms_or_none(last.get("histograms_by_op"))
    if first_clean is None or last_clean is None:
        return None
    return _histogram_comparison_rows_from(
        first_clean, last_clean,
        first.get("per_op") or {}, last.get("per_op") or {})


def _histogram_comparison_rows_from(first_clean, last_clean,
                                    first_per_op, last_per_op):
    """Per-op comparison rows from two cleaned histogram maps.

    ``first_clean``/``last_clean`` are the two sides' cleaned
    ``histograms_by_op`` maps (op name to bucket list), and
    ``first_per_op``/``last_per_op`` their per-op percentile
    maps (op name to a dict with ``p50_ms``/``p99_ms``) — the
    run record's ``per_op`` for whole runs, or the percentiles
    read off the step's own histogram buckets (see
    :func:`_sweep_step_per_op`) for sweep steps, whose records
    carry no per-op percentile fields. One row per op that ran
    in either side, in the same shape
    :func:`_histogram_comparison_rows` documents: each op's
    p50/p99 on both sides, the p99 percentage change, and the
    bucket drift via :func:`_bucket_drift`. An op that never
    ran on one side shows None for that side, and the p99
    change is None when either side has no p99 or the first
    p99 is zero. The drift is None for an op that ran on only
    one side. Both maps are already cleaned, so this never
    returns None — callers check usability first.
    """
    rows = []
    for op in ("SET", "GET", "INCR", "DEL"):
        if op not in first_clean and op not in last_clean:
            continue
        first_op = first_per_op.get(op) or {}
        last_op = last_per_op.get(op) or {}
        p50_first = first_op.get("p50_ms")
        p50_last = last_op.get("p50_ms")
        p99_first = first_op.get("p99_ms")
        p99_last = last_op.get("p99_ms")
        if (isinstance(p99_first, (int, float))
                and isinstance(p99_last, (int, float))
                and p99_first):
            p99_change = (p99_last - p99_first) / p99_first * 100
        else:
            p99_change = None
        drift_pp = None
        drift_bucket = None
        if op in first_clean and op in last_clean:
            moved = _bucket_drift(first_clean[op], last_clean[op])
            if moved is not None:
                drift_pp, drift_bucket = moved
        rows.append({
            "op": op,
            "p50_from_ms": p50_first,
            "p50_to_ms": p50_last,
            "p99_from_ms": p99_first,
            "p99_to_ms": p99_last,
            "p99_change_pct": p99_change,
            "bucket_drift_pp": drift_pp,
            "bucket_drift_bucket": drift_bucket,
        })
    return rows


def _histogram_comparison_table_lines(heading, from_label, to_label,
                                      rows):
    """The per-op histogram comparison table under one heading.

    ``rows`` are :func:`_histogram_comparison_rows` rows; the
    labels name the two sides in the column headers
    ("first"/"last" for the trend, "worst"/"best" for
    best-vs-worst). A side with no p50/p99 shows ``-``, and the
    p99 change and bucket drift show ``-`` when they cannot be
    computed.
    """
    lines = [heading, ""]
    lines.append(f"| Op | p50 {from_label} (ms) | p50 {to_label} (ms) | "
                 f"p99 {from_label} (ms) | p99 {to_label} (ms) | "
                 "p99 change | Bucket drift |")
    lines.append("| --- | ---: | ---: | ---: | ---: | ---: | ---: |")

    def _fmt(value):
        return (f"{value:.3f}"
                if isinstance(value, (int, float)) else "-")

    for row in rows:
        change = row["p99_change_pct"]
        change_cell = f"{change:+.1f}%" if change is not None else "-"
        drift_pp = row["bucket_drift_pp"]
        drift = (f"{drift_pp:+.1f} pp ({row['bucket_drift_bucket']})"
                 if drift_pp is not None else "-")
        lines.append(
            f"| {row['op']} | {_fmt(row['p50_from_ms'])} | "
            f"{_fmt(row['p50_to_ms'])} | {_fmt(row['p99_from_ms'])} | "
            f"{_fmt(row['p99_to_ms'])} | {change_cell} | {drift} |")
    lines.append("")
    return lines


def _trend_histogram_comparison(first, last):
    """The first-to-last per-op histogram comparison for one backend, as data.

    Returns a JSON-serializable dict with ``"from": "first"`` and
    ``"to": "last"``: ``rows`` is the shared
    :func:`_histogram_comparison_rows` table — each op's p50/p99
    in the first and last runs, the p99 change, and the bucket
    drift — with ``notes`` empty, or ``rows`` is None and
    ``notes`` names why there is no comparison: the first and
    last runs are the same run (a single run in the window), or
    one of them was recorded before 0.43.0, recorded no per-op
    latencies, or carries malformed histograms. The Markdown
    view renders the same comparison from this one helper, so
    the two views cannot disagree.
    """
    if first is last:
        return {
            "from": "first",
            "to": "last",
            "rows": None,
            "notes": ["Only one run in the trend window, so there "
                      "is no first-to-last histogram comparison."],
        }
    notes = _histogram_comparison_notes(
        first, last, "first", "last", "first-to-last")
    if notes:
        return {
            "from": "first",
            "to": "last",
            "rows": None,
            "notes": notes,
        }
    return {
        "from": "first",
        "to": "last",
        "rows": _histogram_comparison_rows(first, last),
        "notes": [],
    }


def _trend_histogram_comparison_lines(first, last):
    """Per-op histogram comparison between the trend's first and last runs.

    A per-op table — each op's p50 and p99 in the first and last
    runs, the p99 change, and the bucket drift (the largest
    single-bucket share move, in percentage points, with the
    bucket it moved in) — followed by the first run's per-op
    histogram block, drawn with the same shared helper as the
    latest run's block just above, so the two shapes can be read
    side by side. The p50/p99 shift reads from each run's
    ``per_op`` entry (records since 0.22.0); an op that never ran
    in one of the runs, or a record with no per-op data, shows
    ``-`` for that side instead of a number. A run recorded
    before 0.43.0 carries no ``histograms_by_op`` key at all; a
    run recorded with no per-op latencies carries an empty
    mapping; a hand-made history file may carry something else
    entirely — each renders a note naming the reason instead of
    a comparison, the same notes the latest-run block uses, so
    the trend never crashes on a record shape it cannot draw.
    The table itself is built by the shared
    :func:`_histogram_comparison_rows` /
    :func:`_histogram_comparison_table_lines` helpers, so the
    best-vs-worst view renders the same table from the same
    rows and the views cannot disagree — and the JSON view
    carries the same comparison from
    :func:`_trend_histogram_comparison`, so a script reading it
    sees what the table shows.
    """
    first_ts = first.get("timestamp", "")[:19].replace("T", " ")
    comparison = _trend_histogram_comparison(first, last)
    if comparison["rows"] is None:
        return [f"- {note}" for note in comparison["notes"]] + [""]
    lines = _histogram_comparison_table_lines(
        "#### Per-op histogram comparison — first vs last run",
        "first", "last", comparison["rows"])
    # The first run's shapes, next to the latest run's block just
    # above — the same shared helper draws both, so the views
    # cannot disagree.
    lines.extend(_per_op_histogram_block(
        f"#### Per-op latency histogram — first run ({first_ts})",
        _histograms_or_none(first["histograms_by_op"])))
    return lines


def format_trend(entries, limit=20):
    """Render an ops/sec-over-time trend chart per backend.

    ``entries`` are in file order (oldest first). Each backend gets
    its own section: a table of its most recent ``limit`` runs with
    ops/sec, the change versus the previous run of that backend,
    and p99 alongside, plus a sparkline of ops/sec over those runs
    (oldest to newest) and the first-to-last change in ops/sec and
    in p99, followed by
    the same chart per operation type (SET, GET, INCR, DEL) for the ops
    that ran, so each op's throughput trend reads on its own, with
    a p99 first-to-last change per op beside the ops/sec one, and
    the same chart per sweep step (keyspace size, client count,
    value size, pipeline depth, read:write ratio) for the steps
    that ran, so a step's throughput trend reads on its own too,
    with a p99 first-to-last change per step beside the ops/sec
    one, so a step whose tail crept up reads at a glance,
    and each sweep-step section ends with that step's
    per-operation latency histograms from the latest run that
    recorded it — or a note naming why there are none — so a p99
    move in a step names the op whose latency shape moved,
    and the latest run's per-operation latency histograms, so a
    p99 move in the trend names the op whose latency shape moved,
    plus a per-op histogram comparison between the first and
    last runs in the window — each op's p50/p99 shift and bucket
    drift, with the first run's histogram block drawn with the
    same shared helper — so a p99 move shows how each op's
    latency shape moved. Runs recorded before 0.43.0 carry no
    per-op histograms and render a note naming that instead of
    histogram blocks, as do runs recorded with no per-op
    latencies and records whose histograms are malformed. A
    corrupt line never reaches here — ``load_history`` already
    skips it — and a backend with a single recorded run shows no
    delta, just its one value, and no first-to-last comparison.
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
        # The p99 first-to-last change, alongside the ops/sec one:
        # a backend whose tail crept up reads at a glance even
        # when its throughput moved the other way. A run whose
        # recorded p99 is missing is skipped, like a run with no
        # recorded ops/sec.
        p99_values = [e.get("p99_ms") for e in window
                      if isinstance(e.get("p99_ms"), (int, float))]
        line = _p99_first_last_line("p99", p99_values)
        if line is not None:
            sections.append(line)
        sections.append("")
        # Per-op trend, same shape as the overall chart, one section
        # per operation type that ran in this window.
        sections.extend(_per_op_trend_lines(runs, window))
        # Per-sweep-step trend, same shape again, one section per
        # sweep step that ran in this window.
        sections.extend(_sweep_trend_lines(runs, window))
        # Per-op latency histograms for the backend's latest run
        # in the window — the last trend step — so the chart ends
        # with where each op's latency sits now.
        sections.extend(_trend_histogram_lines(window[-1]))
        # Per-op histogram comparison between the first and last
        # runs in the window, so a p99 move shows how each op's
        # latency shape moved between them.
        sections.extend(
            _trend_histogram_comparison_lines(window[0], window[-1]))
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


def _trend_first_to_last(values):
    """First-to-last change over ``values`` (oldest first), or None.

    ``values`` are the recorded numbers in window order; fewer
    than two means there is no change to report. Returns the
    first and last values and the percentage change between
    them — the same numbers the Markdown first-to-last lines
    render, so the JSON and Markdown views cannot disagree.
    """
    if len(values) < 2:
        return None
    first, last = values[0], values[-1]
    return {
        "first": first,
        "last": last,
        "delta_pct": (last - first) / first * 100 if first else 0.0,
    }


def _trend_table_rows(runs, window, pick):
    """One trend table as JSON data, in window order (oldest first).

    ``pick(entry)`` returns the numbers source for a run — the
    run itself for the overall table, its per-op entry for a
    per-op table — or None when the run did not measure it.
    Each row carries the timestamp, ops/sec, the numeric change
    versus the previous run that measured the same thing
    (scanning all runs, so a run outside the window still
    counts — the same rule the Markdown tables use), and p99.
    A run whose ops/sec or p99 is not a number still gets a
    row, with None in its place — the Markdown table shows "-"
    for it.
    """
    previous_by_id = {}
    previous = None
    for entry in runs:
        source = pick(entry)
        if source is None:
            previous_by_id[id(entry)] = None
            continue
        previous_by_id[id(entry)] = previous
        previous = source
    rows = []
    for entry in window:
        source = pick(entry)
        if source is None:
            continue
        ops_sec = source.get("ops_per_sec")
        prev = previous_by_id[id(entry)]
        delta = None
        if (prev is not None
                and isinstance(ops_sec, (int, float))
                and isinstance(prev.get("ops_per_sec"), (int, float))
                and prev["ops_per_sec"]):
            delta = ((ops_sec - prev["ops_per_sec"])
                     / prev["ops_per_sec"] * 100)
        p99 = source.get("p99_ms")
        rows.append({
            "timestamp": entry.get("timestamp", "")[:19].replace(
                "T", " "),
            "ops_per_sec": (ops_sec
                            if isinstance(ops_sec, (int, float))
                            else None),
            "ops_sec_delta_pct": delta,
            "p99_ms": p99 if isinstance(p99, (int, float)) else None,
        })
    return rows


def _trend_section_data(runs, window, pick):
    """One trend section as JSON data: its table plus first-to-last.

    ``pick`` is the same selector :func:`_trend_table_rows`
    takes. Returns the section's rows and the first-to-last
    change in ops/sec and in p99 over the rows' numeric values
    — None for a side with fewer than two values, so a single
    run carries no change to report, like the Markdown view.
    """
    rows = _trend_table_rows(runs, window, pick)
    return {
        "runs": rows,
        "first_to_last": {
            "ops_per_sec": _trend_first_to_last(
                [row["ops_per_sec"] for row in rows
                 if row["ops_per_sec"] is not None]),
            "p99_ms": _trend_first_to_last(
                [row["p99_ms"] for row in rows
                 if row["p99_ms"] is not None]),
        },
    }


def trend_comparison(entries, limit=20):
    """Ops/sec trend per backend, as JSON-serializable data.

    ``entries`` are in file order (oldest first). Each backend
    gets the same trend the Markdown view renders, as data: its
    most recent ``limit`` runs with ops/sec, the change versus
    the previous run, and p99, plus the sparkline and the
    first-to-last change in ops/sec and in p99, the same table
    per operation type (SET, GET, INCR, DEL) that ran, the
    latest run's per-operation latency histograms — or a note
    naming why there are none — and the per-op histogram
    comparison between the first and last runs in the window.
    Each backend also carries a ``sweeps`` list — one entry per
    sweep step that ran in the window, with the step's best and
    worst runs by the step's own ops/sec, the percentage changes
    between them, and the step's per-op latency histograms from
    both runs, plus the step's per-op histogram comparison —
    the same per-step sections ``--best-worst-json`` carries,
    built from the same shared
    :func:`_best_worst_sweep_entries` helper, so a script
    reading the trend JSON sees what the best/worst views show
    for the same step. An empty history carries no backends.

    Returns a JSON-serializable dict with a ``backends`` list.
    """
    if not entries:
        return {"backends": []}
    by_backend = {}
    for entry in entries:
        by_backend.setdefault(entry.get("backend", "?"), []).append(entry)
    backends = []
    for backend, runs in by_backend.items():
        window = runs[-limit:]
        overall = _trend_section_data(runs, window, lambda e: e)
        values = [row["ops_per_sec"] for row in overall["runs"]
                  if row["ops_per_sec"] is not None]
        per_op = {}
        for op in ("SET", "GET", "INCR", "DEL"):
            section = _trend_section_data(
                runs, window,
                lambda e, op=op: (e.get("per_op") or {}).get(op))
            if section["runs"]:
                per_op[op] = section
        latest = window[-1]
        timestamp = latest.get("timestamp", "")[:19].replace("T", " ")
        histograms, reason = _latest_run_histograms(latest)
        notes = []
        if reason is not None:
            notes.append(
                f"{timestamp}: {_TREND_HISTOGRAM_REASONS[reason]}")
        backends.append({
            "backend": backend,
            "runs": overall["runs"],
            "sparkline": _sparkline(values),
            "first_to_last": overall["first_to_last"],
            "per_op": per_op,
            "sweeps": _best_worst_sweep_entries(window),
            "histograms_by_op": histograms,
            "notes": notes,
            "histogram_comparison": _trend_histogram_comparison(
                window[0], window[-1]),
        })
    return {"backends": backends}


def write_trend_json(path, entries, limit=20):
    """Write the ops/sec trend as JSON to ``path``.

    Creates the parent directory when needed. Returns the
    comparison dict — the same shape :func:`trend_comparison`
    returns — so a script can read the trend without parsing
    the Markdown chart. Each backend carries its per-step sweep
    sections under ``sweeps`` — one entry per step that ran in
    the window, with the step's best and worst runs, the deltas,
    the step's per-op histograms from both runs, and the step's
    per-op histogram comparison between them — the same sections
    ``--best-worst-json`` carries.
    """
    comparison = trend_comparison(entries, limit=limit)
    directory = os.path.dirname(os.path.abspath(path))
    os.makedirs(directory, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(comparison, f, indent=2)
        f.write("\n")
    return comparison


def _histogram_or_none(buckets):
    """One op's latency histogram bucket list, or None.

    A bucket list is the same log-spaced buckets
    :func:`kvbench.stats.histogram` produces (each bucket a dict
    with ``upper_ms`` and ``count``). A record that carries
    something else — a hand-edited history file, say — reads as
    no histogram, instead of crashing the best/worst view on a
    shape it cannot draw.
    """
    if not isinstance(buckets, list) or not buckets:
        return None
    for bucket in buckets:
        if (not isinstance(bucket, dict)
                or "upper_ms" not in bucket
                or "count" not in bucket):
            return None
    return buckets


def _histograms_or_none(value):
    """A per-op histogram mapping, or None when it is not one.

    A history record written since 0.43.0 carries
    ``histograms_by_op`` (op name to the same log-spaced bucket
    list the single-run report uses); an older record carries
    none, and a hand-made file may carry something else entirely.
    Only a non-empty dict of op name to valid bucket lists passes
    through — anything else reads as no histograms, instead of
    crashing the best/worst view on a shape it cannot draw. The
    compare view validates baseline histograms the same way, so
    it imports this helper rather than keeping a second copy.
    """
    if not isinstance(value, dict) or not value:
        return None
    cleaned = {}
    for op, buckets in value.items():
        valid = _histogram_or_none(buckets)
        if valid is None:
            return None
        cleaned[op] = valid
    return cleaned


def _run_numbers(entry, histograms=None):
    """A run's headline numbers as a small dict, or None.

    The timestamp is kept for the table; a run with no numeric
    ops/sec cannot be ranked, so it is skipped instead of
    crashing the comparison. ``histograms`` is the run's
    per-operation latency histograms (op name to bucket list),
    carried only when the run has them — the best/worst tables
    show the picked runs' latency shapes alongside their p99.
    """
    ops_sec = entry.get("ops_per_sec")
    if not isinstance(ops_sec, (int, float)):
        return None
    p99 = entry.get("p99_ms")
    numbers = {
        "timestamp": entry.get("timestamp", "")[:19].replace("T", " "),
        "ops_per_sec": ops_sec,
        "p99_ms": p99 if isinstance(p99, (int, float)) else None,
    }
    if histograms is not None:
        numbers["histograms_by_op"] = histograms
    return numbers


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


def _best_worst_histogram_comparison(usable, best, worst):
    """The best-vs-worst per-op histogram comparison for one backend.

    ``usable`` are (entry, numbers) pairs and ``best``/``worst``
    the picked numbers dicts. Returns a JSON-serializable dict
    with ``"from": "worst"`` and ``"to": "best"``: ``rows`` is
    the shared :func:`_histogram_comparison_rows` table — each
    op's p50/p99 in the worst and best runs, the p99 change, and
    the bucket drift — with ``notes`` empty, or ``rows`` is None
    and ``notes`` names why there is no comparison: the best and
    worst runs are the same run (a single recorded run), or one
    of them was recorded before 0.43.0, recorded no per-op
    latencies, or carries malformed histograms.
    """
    best_entry = next(entry for entry, numbers in usable
                      if numbers is best)
    worst_entry = next(entry for entry, numbers in usable
                       if numbers is worst)
    if best_entry is worst_entry:
        return {
            "from": "worst",
            "to": "best",
            "rows": None,
            "notes": ["The best and worst runs are the same run, "
                      "so there is no best-vs-worst histogram "
                      "comparison."],
        }
    notes = _histogram_comparison_notes(
        worst_entry, best_entry, "worst", "best", "best-vs-worst")
    if notes:
        return {
            "from": "worst",
            "to": "best",
            "rows": None,
            "notes": notes,
        }
    return {
        "from": "worst",
        "to": "best",
        "rows": _histogram_comparison_rows(worst_entry, best_entry),
        "notes": [],
    }


def _sweep_step_per_op(histograms):
    """Per-op p50/p99 read off a sweep step's cleaned histograms.

    ``histograms`` is the step's cleaned ``histograms_by_op``
    map. A sweep step record carries no per-op percentile
    fields — only the step's overall p99 and the per-op bucket
    lists — so each op's p50 and p99 are read off its own
    buckets with :func:`_histogram_percentile`: the upper edge
    of the bucket holding the percentile rank, an upper bound on
    the true percentile, or None when the rank lands in the
    overflow bucket (no upper edge to report).
    """
    return {op: {"p50_ms": _histogram_percentile(buckets, 50),
                 "p99_ms": _histogram_percentile(buckets, 99)}
            for op, buckets in histograms.items()}


def _best_worst_sweep_histogram_comparison(label, step_label,
                                           best_entry, worst_entry,
                                           best_step, worst_step):
    """The best-vs-worst per-op histogram comparison for one sweep step.

    ``best_step``/``worst_step`` are the step dicts from the
    step's best and worst runs (see
    :func:`_best_worst_sweep_pairs`); ``label``/``step_label``
    name the step in the notes. Returns a JSON-serializable
    dict with ``"from": "worst"`` and ``"to": "best"``: ``rows``
    is the shared :func:`_histogram_comparison_rows_from` table
    — each op's p50/p99 in the worst and best runs' steps, the
    p99 change, and the bucket drift — with ``notes`` empty and
    ``percentiles_from_histograms`` True, or ``rows`` is None
    and ``notes`` names why there is no comparison: the step's
    best and worst runs are the same run (a step recorded in
    only one run), or one of the steps was recorded before
    0.49.0, recorded no per-op latencies, or carries malformed
    histograms. The Markdown and JSON views both build from
    this one helper, so the two views cannot disagree. The
    p50/p99 in the rows are read off the steps' histogram
    buckets (see :func:`_sweep_step_per_op`), because a step
    record carries no per-op percentile fields — the flag says
    so, so a script reading the JSON does not mistake them for
    the exact percentiles a whole run's ``per_op`` carries.
    """
    if best_entry is worst_entry:
        return {
            "from": "worst",
            "to": "best",
            "rows": None,
            "notes": [f"The best and worst runs for {label} — "
                      f"{step_label} are the same run, so there "
                      "is no best-vs-worst histogram comparison "
                      "for this step."],
        }
    notes = []
    cleaned = {}
    for pick, entry, step in (("best", best_entry, best_step),
                              ("worst", worst_entry, worst_step)):
        step_cleaned, reason = _sweep_step_histograms(step)
        if step_cleaned is not None:
            cleaned[pick] = step_cleaned
            continue
        timestamp = entry.get("timestamp", "")[:19].replace("T", " ")
        notes.append(
            f"{label} — {step_label} ({pick} run, {timestamp}): "
            f"{_SWEEP_STEP_HISTOGRAM_REASONS[reason]} No "
            "best-vs-worst histogram comparison is shown for "
            "this step.")
    if notes:
        return {
            "from": "worst",
            "to": "best",
            "rows": None,
            "notes": notes,
        }
    return {
        "from": "worst",
        "to": "best",
        "rows": _histogram_comparison_rows_from(
            cleaned["worst"], cleaned["best"],
            _sweep_step_per_op(cleaned["worst"]),
            _sweep_step_per_op(cleaned["best"])),
        "notes": [],
        "percentiles_from_histograms": True,
    }


def _best_worst_sweep_pairs(runs):
    """Per-sweep-step best-vs-worst picks for one backend, as data.

    ``runs`` are the backend's recorded runs (oldest first).
    Yields one tuple per sweep step that ran — one keyspace size,
    one client count, one value size, one pipeline depth, one
    read:write ratio — with the step's kind, step key, label and
    unit, its raw value and the step label the report renders,
    the best and worst runs for that step by the step's own
    ops/sec with the percentage changes between them, and the
    history entries those picks came from. The Markdown and JSON
    views both build from these picks, so the two views cannot
    disagree. A step recorded in only one run has that run as
    both best and worst, with 0.0% deltas; a step that never ran
    gets no entry at all, and a step whose recorded ops/sec is
    not a number is skipped instead of crashing the comparison.
    """
    for kind, step_key, label, unit in _SWEEP_KINDS:
        steps = sorted({
            step.get(step_key)
            for entry in runs
            for step in (entry.get("sweeps") or {}).get(kind, [])
            if isinstance(step, dict)
            and isinstance(step.get(step_key), (int, float, str))
        })
        for step_value in steps:
            if isinstance(step_value, str):
                # A mix ratio reads as "90:10 read:write", not
                # "90:10, read:write" — a string step value has no
                # thousands separator to apply.
                step_label = f"{step_value} {unit}"
            else:
                step_label = f"{step_value:,} {unit}"
            pairs = []
            for entry in runs:
                step = _sweep_step_entry(entry, kind, step_key,
                                         step_value)
                if step is None:
                    continue
                numbers = _run_numbers(step)
                if numbers is None:
                    continue
                # A step carries no timestamp of its own; the
                # table shows the run it came from.
                numbers["timestamp"] = entry.get(
                    "timestamp", "")[:19].replace("T", " ")
                pairs.append((entry, numbers))
            pair = _best_worst_pair(pairs)
            if pair is None:
                continue
            best, worst, ops_delta, p99_delta = pair
            best_entry = next(entry for entry, numbers in pairs
                              if numbers is best)
            worst_entry = next(entry for entry, numbers in pairs
                               if numbers is worst)
            yield (kind, step_key, label, unit, step_value,
                   step_label, best, worst, ops_delta, p99_delta,
                   best_entry, worst_entry)


def _best_worst_sweep_lines(runs):
    """Per-sweep-step best-vs-worst sections for one backend.

    ``runs`` are the backend's recorded runs (oldest first).
    Each sweep step that ran — one keyspace size, one client
    count, one value size, one pipeline depth, one read:write
    ratio — gets its own best-vs-worst table: the best and worst
    runs for that step by the step's own ops/sec, with the p99
    for each and the percentage change between them — the same
    table shape as the overall and per-op best/worst tables,
    built from the same ``_best_worst_lines`` helper, so the
    views cannot disagree — followed by the step's per-op
    latency histograms from its best and worst runs, drawn with
    the same shared helper the trend's per-step blocks use, so
    a p99 move in a step names the op whose latency shape
    moved — followed by the step's per-op histogram comparison
    between its best and worst runs (each op's p50/p99 in the
    two steps, the p99 change, and the bucket drift), built
    from the same shared helper the JSON view uses, so the
    views cannot disagree. The picks come from the shared
    ``_best_worst_sweep_pairs`` helper the JSON view uses too.
    A step recorded in only one run has that run as both
    best and worst, with 0.0% deltas, and renders a note
    naming that instead of a comparison table; a step recorded
    before 0.49.0, one recorded with no per-op latencies, or
    one whose recorded histograms are malformed renders a note
    naming the reason instead of a block or a table. A step
    that never ran gets no section at all, and a step whose
    recorded ops/sec is not a number is skipped instead of
    crashing the comparison.
    """
    lines = []
    for (kind, step_key, label, unit, step_value, step_label,
         best, worst, ops_delta, p99_delta,
         best_entry, worst_entry) in _best_worst_sweep_pairs(runs):
        lines.extend(_best_worst_lines(
            f"{label} — {step_label}", best, worst,
            ops_delta, p99_delta))
        best_step = _sweep_step_entry(best_entry, kind, step_key,
                                      step_value)
        worst_step = _sweep_step_entry(worst_entry, kind, step_key,
                                       step_value)
        # The step's per-op latency histograms from its best
        # and worst runs — the same shared helper the
        # trend's per-step blocks use, so a p99 move in the
        # step names the op whose latency shape moved.
        lines.extend(_sweep_step_histogram_block(
            label, step_label, best_entry, best_step, "best"))
        lines.append("")
        lines.extend(_sweep_step_histogram_block(
            label, step_label, worst_entry, worst_step, "worst"))
        lines.append("")
        # The step's per-op histogram comparison between its
        # best and worst runs — the same table the overall
        # best-vs-worst comparison renders, built from the same
        # shared rows, so a p99 move in a step shows how each
        # op's latency shape moved. A step with a single run,
        # or steps without usable histograms, gets a note
        # naming the reason instead of a table.
        comparison = _best_worst_sweep_histogram_comparison(
            label, step_label, best_entry, worst_entry,
            best_step, worst_step)
        if comparison["rows"] is not None:
            lines.extend(_histogram_comparison_table_lines(
                f"#### {label} — {step_label} — per-op histogram "
                "comparison — best vs worst run",
                "worst", "best", comparison["rows"]))
        else:
            for note in comparison["notes"]:
                lines.append(f"- {note}")
            lines.append("")
    return lines


def _best_worst_sweep_entries(runs):
    """Per-sweep-step best-vs-worst sections for one backend, as data.

    ``runs`` are the backend's recorded runs (oldest first).
    One entry per sweep step that ran — one keyspace size, one
    client count, one value size, one pipeline depth, one
    read:write ratio — with the step's kind, label and raw value,
    the step label the Markdown view renders, the best and worst
    runs for that step by the step's own ops/sec with the
    percentage changes between them, and the step's per-op
    latency histograms from both runs under ``histograms_by_op``
    on the best and worst numbers — plus the step's per-op
    histogram comparison under ``histogram_comparison`` (each
    op's p50/p99 in the worst and best runs' steps, the p99
    change, and the bucket drift, or notes naming why there is
    none — the same comparison the Markdown view renders, built
    from the same shared helper). A pick whose step carries no
    drawable histograms contributes a note under ``notes``
    naming the reason instead — recorded before 0.49.0, no
    per-op latencies, or malformed histograms — the same reasons
    the Markdown view renders, built from the same shared
    helper, so a script reading the JSON sees what the table
    shows. The picks come from the shared
    ``_best_worst_sweep_pairs`` helper the Markdown view uses
    too, so the two views cannot disagree.
    """
    entries = []
    for (kind, step_key, label, unit, step_value, step_label,
         best, worst, ops_delta, p99_delta,
         best_entry, worst_entry) in _best_worst_sweep_pairs(runs):
        best_numbers = dict(best)
        worst_numbers = dict(worst)
        notes = []
        best_step = _sweep_step_entry(best_entry, kind, step_key,
                                      step_value)
        worst_step = _sweep_step_entry(worst_entry, kind, step_key,
                                       step_value)
        for pick, numbers, entry, step in (
                ("best", best_numbers, best_entry, best_step),
                ("worst", worst_numbers, worst_entry, worst_step)):
            cleaned, reason = _sweep_step_histograms(step)
            if cleaned is not None:
                numbers["histograms_by_op"] = cleaned
            else:
                timestamp = entry.get("timestamp", "")[:19].replace(
                    "T", " ")
                notes.append(
                    f"{label} — {step_label} ({pick} run, "
                    f"{timestamp}): "
                    f"{_SWEEP_STEP_HISTOGRAM_REASONS[reason]}")
        entries.append({
            "kind": kind,
            "label": label,
            "step": step_value,
            "step_label": step_label,
            "best": best_numbers,
            "worst": worst_numbers,
            "ops_sec_delta_pct": ops_delta,
            "p99_delta_pct": p99_delta,
            "histogram_comparison": _best_worst_sweep_histogram_comparison(
                label, step_label, best_entry, worst_entry,
                best_step, worst_step),
            "notes": notes,
        })
    return entries


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
    worst SET run. Each picked run also carries its per-operation
    latency histograms — the best and worst runs' full
    ``histograms_by_op`` maps, and each per-op pick's own
    ``histogram`` bucket list — the same log-spaced buckets the
    single-run report uses, so a p99 move names the op whose
    latency shape moved. Each backend also carries a
    ``histogram_comparison`` between its best and worst runs —
    the same per-op table the trend's first-to-last comparison
    renders (each op's p50/p99 in the worst and best runs, the
    p99 change, and the bucket drift), built from the same
    shared rows, so the two views cannot disagree — under
    ``rows`` with ``"from": "worst"`` and ``"to": "best"``, or
    under ``notes`` naming why there is none (a single run, a
    run recorded before 0.43.0, a run with no per-op latencies,
    or malformed histograms). Each backend also carries a
    ``sweeps`` list — one entry per sweep step that ran, with
    the step's best and worst runs by the step's own ops/sec,
    the percentage changes between them, and the step's per-op
    latency histograms from both runs (or notes naming why a
    pick has none) — plus the step's per-op histogram
    comparison under ``histogram_comparison`` (each op's p50/p99
    in the worst and best runs' steps, the p99 change, and the
    bucket drift, or notes naming why there is none) — the same
    sections the Markdown view renders, built from the same
    shared picks, so the JSON and Markdown views cannot
    disagree. When ``workload`` is given (a
    workload dict in the shape of a history record's
    ``workload``), only runs whose workload matches it are
    considered — the same apples-to-apples rule
    ``--compare-same-workload`` uses, so a quick smoke run
    never becomes the worst run next to a full-size one. A
    backend with a single usable run has that run as both best
    and worst, with 0.0% deltas; runs recorded before 0.22.0
    carry no ``per_op`` data and simply get no per-op sections,
    and runs recorded before 0.43.0 carry no
    ``histograms_by_op`` data and simply carry no histograms.

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
        usable = [(e, _run_numbers(
            e, _histograms_or_none(e.get("histograms_by_op"))))
            for e in runs]
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
                # The op's histogram lives on the record, not on
                # the per-op entry — the same buckets the overall
                # run's map carries for this op, so the per-op
                # pick's shape cannot disagree with the run's.
                histograms = _histograms_or_none(
                    entry.get("histograms_by_op"))
                if histograms is not None and op in histograms:
                    numbers["histogram"] = histograms[op]
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
            "histogram_comparison": _best_worst_histogram_comparison(
                usable, best, worst),
            "sweeps": _best_worst_sweep_entries(runs),
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
    them, then the per-operation latency histograms of the best
    and worst runs, then a per-op histogram comparison between
    the two runs — each op's p50/p99 in the worst and best runs,
    the p99 change, and the bucket drift, built from the same
    shared rows as the trend's first-to-last comparison, so the
    views cannot disagree — then the same table per operation
    type that ran, each followed by that op's histograms for its
    own best and worst runs — the per-op picks are independent of the
    overall picks, so a slow-write run that still won overall
    shows its own shape. Each sweep step that ran then gets its
    own best-vs-worst section — the best and worst runs for that
    step by the step's own ops/sec, with the step's per-op
    latency histograms from both runs, drawn with the same
    shared helper the trend's per-step blocks use, so a p99 move
    in a step names the op whose latency shape moved — followed
    by the step's per-op histogram comparison between its best
    and worst runs, built from the same shared helper the JSON
    view uses. A backend
    with a single run, or runs without usable histograms, gets a
    note naming the reason instead of a comparison table. When
    ``workload`` is given, only runs matching it were compared,
    and the header says which workload that was. An empty history
    — or a filter that matches nothing — prints the same "No runs
    recorded yet" message as ``--trend``. Runs recorded before
    0.43.0 carry no per-op histograms and render a note instead
    of histogram blocks; a sweep step recorded before 0.49.0
    carries no per-step histograms and renders a note instead
    of a block.
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
        entries = [e for e in entries
                   if workload_matches(e.get("workload"), workload)]
    by_backend = {}
    for entry in entries:
        by_backend.setdefault(entry.get("backend", "?"), []).append(entry)
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
        # Per-operation latency histograms for the best and worst
        # runs — the same shared histogram-block helper the
        # single-run report and the compare view use, so the
        # shapes cannot disagree. Runs recorded before 0.43.0
        # carry no histograms, so those backends get a note
        # instead of blocks.
        sections.extend(_per_op_histogram_block(
            "#### Per-op latency histogram — best run",
            backend["best"].get("histograms_by_op")))
        sections.extend(_per_op_histogram_block(
            "#### Per-op latency histogram — worst run",
            backend["worst"].get("histograms_by_op")))
        if (not backend["best"].get("histograms_by_op")
                and not backend["worst"].get("histograms_by_op")):
            sections.append(
                f"- {backend['backend']}: the history file carries "
                "no per-op histograms, so no histograms are shown.")
            sections.append("")
        # Per-op histogram comparison between the best and worst
        # runs — the same table the trend's first-to-last
        # comparison renders, built from the same shared rows,
        # so a p99 move between the runs shows how each op's
        # latency shape moved. A backend with a single run, or
        # runs without usable histograms, gets a note naming the
        # reason instead of a table.
        histogram_comparison = backend["histogram_comparison"]
        if histogram_comparison["rows"] is not None:
            sections.extend(_histogram_comparison_table_lines(
                "#### Per-op histogram comparison — best vs worst run",
                "worst", "best", histogram_comparison["rows"]))
        else:
            for note in histogram_comparison["notes"]:
                sections.append(f"- {note}")
            sections.append("")
        for op in ("SET", "GET", "INCR", "DEL"):
            op_entry = backend["per_op"].get(op)
            if op_entry is None:
                continue
            sections.extend(_best_worst_lines(
                f"{op}", op_entry["best"], op_entry["worst"],
                op_entry["ops_sec_delta_pct"],
                op_entry["p99_delta_pct"]))
            # That op's histogram for its own best and worst
            # runs — the per-op picks are independent of the
            # overall picks, so their shapes live here, not in
            # the overall blocks above.
            for label, pick in (("best", op_entry["best"]),
                                ("worst", op_entry["worst"])):
                histogram = pick.get("histogram")
                if histogram is None:
                    continue
                sections.extend(_per_op_histogram_block(
                    f"#### {op} — latency histogram ({label} run)",
                    {op: histogram}))
        # Per-sweep-step best-vs-worst: each step that ran gets
        # its own table and the step's per-op histograms from
        # its best and worst runs — the same shared helper the
        # trend's per-step blocks use — so a p99 move in a step
        # names the op whose latency shape moved.
        sections.extend(_best_worst_sweep_lines(
            by_backend.get(backend["backend"], [])))
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
    the Markdown table. Each backend carries its per-step sweep
    sections under ``sweeps`` — one entry per step that ran,
    with the step's best and worst runs, the deltas, the step's
    per-op histograms from both runs, and the step's per-op
    histogram comparison between them — the same sections the
    Markdown view renders.
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
