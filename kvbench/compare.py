"""Compare a run against a previous run's JSON report.

``--compare-file PATH`` loads a JSON report written by an earlier
kvbench run (the ``--json`` output) and compares this run's results
against it, backend by backend, matched by name: ops/sec and p99
side by side, with the percentage change and a verdict per backend.
``--regression-threshold PCT`` turns the comparison into a gate:
the run exits non-zero when any backend's ops/sec fell by more than
PCT, or its p99 rose by more than PCT, versus the baseline file.

``--compare-history SELECTOR`` does the same thing against a run
recorded in the history file (the JSONL file ``--history`` writes
and ``--trend`` reads) instead of a report file, so a regression
check needs no report file lying around. The selector picks one
recorded run per backend: ``latest`` (or a negative index) counts
back from the most recent run, a non-negative index counts forward
from the oldest, and an ISO timestamp picks the latest run at or
before that time.

A comparison only means something when the two runs used the same
workload, so the comparison also records which workload settings
differed (ops, clients, keys, mix, pipeline, ...) — the report
warns when they do not match instead of silently comparing runs
that measured different things.
"""

import json
import os
from datetime import datetime, timezone

from .history import load_history

# Workload settings that have to match for a comparison to be
# apples-to-apples. Compared key by key; a difference is reported
# by name, not as a hard failure.
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


def _parse_history_selector(selector):
    """Parse a ``--compare-history`` selector.

    Returns ``("index", n)`` for ``latest`` (n = -1) and for an
    integer (0 is the oldest recorded run, -1 the most recent), or
    ``("time", when)`` for an ISO timestamp (a naive timestamp is
    read as UTC). Raises ValueError for anything else, with a
    message that says what a selector looks like.
    """
    if selector == "latest":
        return ("index", -1)
    try:
        return ("index", int(selector))
    except ValueError:
        pass
    text = str(selector).strip()
    try:
        when = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(
            f"invalid --compare-history selector {selector!r}: use "
            "'latest', an index (0 for the oldest run, -1 for the "
            "most recent), or an ISO timestamp "
            "(e.g. 2026-11-22T15:00:00Z)") from exc
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    return ("time", when)


def _record_timestamp(record):
    """A history record's timestamp as an aware datetime, or None
    when the record carries no parseable timestamp."""
    try:
        when = datetime.fromisoformat(
            str(record.get("timestamp") or "").replace("Z", "+00:00"))
    except ValueError:
        return None
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    return when


def load_history_baseline(path, selector):
    """Load a baseline run from a history file.

    ``path`` is the JSONL history file (the one ``--history``
    writes and ``--trend`` reads); ``selector`` picks one recorded
    run per backend — an index, ``latest``, or an ISO timestamp
    (see :func:`_parse_history_selector`). Returns a baseline
    document in the same shape as a JSON report (``generated``,
    ``command``, ``workload``, ``results`` with ``ops_per_sec``,
    ``errors`` and a ``summary`` holding ``p99_ms``), so
    :func:`compare_results` works on it unchanged.

    Raises ValueError when the file does not exist, holds no runs,
    the selector is invalid, an index is out of range for a
    backend, or no recorded run falls at or before a timestamp.
    A history record's workload has the same keys a JSON report's
    does, so the workload comparison still applies — the baseline
    workload carried here is the first chosen run's, since picks
    for different backends can come from different runs.
    """
    if not os.path.exists(path):
        raise ValueError(f"history file not found: {path}")
    entries = load_history(path)
    if not entries:
        raise ValueError(
            f"history file {path} holds no runs to compare against")
    kind, value = _parse_history_selector(selector)
    by_backend = {}
    for entry in entries:
        by_backend.setdefault(entry.get("backend", "?"), []).append(entry)
    chosen = []
    for backend, runs in by_backend.items():
        if kind == "index":
            index = value if value >= 0 else len(runs) + value
            # Backends recorded fewer times than the index asks for
            # are skipped, not an error: a backend added later has
            # fewer runs than an older one, and the comparison still
            # works for the backends that do have that run.
            if index < 0 or index >= len(runs):
                continue
            chosen.append(runs[index])
        else:
            at_or_before = [r for r in runs
                            if (_record_timestamp(r) is not None
                                and _record_timestamp(r) <= value)]
            if not at_or_before:
                continue
            chosen.append(at_or_before[-1])
    if not chosen:
        if kind == "index":
            raise ValueError(
                f"history run {selector!r} is out of range for every "
                f"backend in {path}: no backend has that many "
                "recorded runs")
        raise ValueError(
            f"no run in history file {path} at or before {selector}")
    first = chosen[0]
    results = [{
        "name": record.get("backend", "?"),
        "ops": record.get("ops"),
        "elapsed_s": record.get("elapsed_s"),
        "ops_per_sec": record.get("ops_per_sec") or 0.0,
        "errors": record.get("errors"),
        "summary": {"p99_ms": record.get("p99_ms") or 0.0},
    } for record in chosen]
    generated = max(
        (record.get("timestamp") or "" for record in chosen),
        default=None)
    return {
        "tool": "kvbench",
        "generated": generated,
        "command": first.get("command"),
        "workload": first.get("workload"),
        "results": results,
    }


def load_baseline(path):
    """Load a previous run's JSON report from ``path``.

    Returns the parsed document. Raises ValueError when the file
    does not exist, is not valid JSON, is not a kvbench report
    (missing a ``results`` list), or holds no usable results —
    each with a message that says what is wrong with the file.
    """
    if not os.path.exists(path):
        raise ValueError(f"compare file not found: {path}")
    try:
        with open(path, encoding="utf-8") as f:
            doc = json.load(f)
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(
            f"compare file {path} is not a readable JSON report: {exc}"
        ) from exc
    if not isinstance(doc, dict) or not isinstance(
            doc.get("results"), list):
        raise ValueError(
            f"compare file {path} is not a kvbench JSON report "
            "(no 'results' list)")
    if not doc["results"]:
        raise ValueError(
            f"compare file {path} holds no results to compare against")
    return doc


def _delta_pct(current, baseline):
    """Percentage change of ``current`` versus ``baseline``.

    None when the baseline is zero or missing — there is no
    meaningful percentage change from nothing.
    """
    if not baseline:
        return None
    return (current - baseline) / baseline * 100


def _verdict(ops_sec_delta_pct, p99_delta_pct, regression):
    """One-word verdict for a backend's comparison.

    A regression (past the threshold, when one was given) wins over
    everything else. Otherwise the ops/sec change decides: up by at
    least 1% is faster, down by at least 1% is slower, anything
    smaller is noise and reads as about the same.
    """
    if regression:
        return "regression"
    if ops_sec_delta_pct is None:
        return "about the same"
    if ops_sec_delta_pct >= 1.0:
        return "faster"
    if ops_sec_delta_pct <= -1.0:
        return "slower"
    return "about the same"


def workload_differences(current_workload, baseline_workload):
    """Names of the workload settings that differ between two runs.

    ``current_workload`` and ``baseline_workload`` are the
    ``workload`` dicts from the two JSON reports (see
    :func:`kvbench.report._workload_dict`). Returns [] when the
    workloads match, so the comparison is apples-to-apples. The
    sweep settings are deliberately not compared: a sweep re-runs
    the same workload at other sizes/counts, but the main result
    it is compared against is the same either way.
    """
    if not isinstance(baseline_workload, dict):
        return list(_WORKLOAD_KEYS)
    return [key for key in _WORKLOAD_KEYS
            if current_workload.get(key) != baseline_workload.get(key)]


def compare_results(results, baseline_doc, current_workload,
                    baseline_path, threshold_pct=None, source="report"):
    """Compare ``results`` against a loaded baseline document.

    Backends are matched by name. Each matched backend gets an
    entry with the current and baseline ops/sec and p99, the
    percentage change of each, the error counts, a verdict, and a
    ``regression`` flag: True when a threshold was given and the
    ops/sec fell by more than the threshold, or the p99 rose by
    more than it. Backends present in only one of the two runs are
    listed as unmatched instead of being silently dropped.
    ``source`` is ``"report"`` for a baseline loaded from a JSON
    report file, ``"history"`` for one loaded from the history
    file — it only changes how the report describes the baseline.

    Returns a JSON-serializable dict (the ``comparison`` section of
    the JSON report).
    """
    baseline_results = {
        entry.get("name"): entry
        for entry in baseline_doc.get("results", [])
        if isinstance(entry, dict) and entry.get("name")
    }
    entries = []
    unmatched_current = []
    for result in results:
        baseline = baseline_results.pop(result.name, None)
        if baseline is None:
            unmatched_current.append(result.name)
            continue
        summary = result.summary
        baseline_summary = baseline.get("summary") or {}
        # A baseline report always carries these numbers; coerce a
        # missing one to 0.0 so a hand-made file renders instead of
        # crashing, and its delta stays None (no percentage change
        # from nothing).
        baseline_ops = baseline.get("ops_per_sec") or 0.0
        baseline_p99 = baseline_summary.get("p99_ms") or 0.0
        ops_delta = _delta_pct(result.ops_per_sec, baseline_ops)
        p99_delta = _delta_pct(summary["p99_ms"], baseline_p99)
        regression = False
        if threshold_pct is not None:
            if (ops_delta is not None
                    and ops_delta < -threshold_pct):
                regression = True
            if (p99_delta is not None
                    and p99_delta > threshold_pct):
                regression = True
        entries.append({
            "name": result.name,
            "ops_per_sec": result.ops_per_sec,
            "baseline_ops_per_sec": baseline_ops,
            "ops_sec_delta_pct": ops_delta,
            "p99_ms": summary["p99_ms"],
            "baseline_p99_ms": baseline_p99,
            "p99_delta_pct": p99_delta,
            "errors": result.errors,
            "baseline_errors": baseline.get("errors"),
            "verdict": _verdict(ops_delta, p99_delta, regression),
            "regression": regression,
        })
    differences = workload_differences(
        current_workload, baseline_doc.get("workload"))
    return {
        "file": baseline_path,
        "source": source,
        "baseline_generated": baseline_doc.get("generated"),
        "baseline_command": baseline_doc.get("command"),
        "workload_matches": not differences,
        "workload_differences": differences,
        "threshold_pct": threshold_pct,
        "entries": entries,
        "unmatched_baseline": sorted(baseline_results),
        "unmatched_current": unmatched_current,
        "regressions": sum(1 for e in entries if e["regression"]),
    }


def format_verdict(entry, threshold_pct=None):
    """Human-readable verdict line for one comparison entry.

    E.g. ``cinder: faster — ops/sec +12.3%, p99 -4.1%`` or
    ``cinder: regression — ops/sec -8.0% (beyond the 5% threshold)``.
    """
    ops_delta = entry["ops_sec_delta_pct"]
    p99_delta = entry["p99_delta_pct"]
    ops_text = (f"{ops_delta:+.1f}%"
                if ops_delta is not None else "n/a")
    p99_text = (f"{p99_delta:+.1f}%"
                if p99_delta is not None else "n/a")
    if entry["regression"]:
        reasons = []
        threshold = threshold_pct
        if (ops_delta is not None and threshold is not None
                and ops_delta < -threshold):
            reasons.append(f"ops/sec {ops_delta:+.1f}%")
        if (p99_delta is not None and threshold is not None
                and p99_delta > threshold):
            reasons.append(f"p99 {p99_delta:+.1f}%")
        detail = ", ".join(reasons) or (
            f"ops/sec {ops_text}, p99 {p99_text}")
        return (f"{entry['name']}: regression — {detail} "
                f"(beyond the {threshold:g}% threshold)")
    return (f"{entry['name']}: {entry['verdict']} — "
            f"ops/sec {ops_text}, p99 {p99_text}")
