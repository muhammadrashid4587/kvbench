"""Compare a run against a previous run's JSON report.

``--compare-file PATH`` loads a JSON report written by an earlier
kvbench run (the ``--json`` output) and compares this run's results
against it, backend by backend, matched by name: ops/sec and p99
side by side, with the percentage change and a verdict per backend,
plus a per-operation breakdown — each op's p99 and ops/sec delta
versus the baseline — and the per-operation latency histograms of
both runs, so a p99 move names the op whose latency shape moved.
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
before that time. With ``--compare-same-workload`` the selector
only sees recorded runs whose workload matches the current run's,
so the baseline is the latest run that measured the same thing —
the regression gate stays apples-to-apples even when the history
file mixes workloads.

A comparison only means something when the two runs used the same
workload, so the comparison also records which workload settings
differed (ops, clients, keys, mix, pipeline, ...) — the report
warns when they do not match instead of silently comparing runs
that measured different things.
"""

import json
import os
from datetime import datetime, timezone

from .history import _WORKLOAD_KEYS, load_history


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


def load_history_baseline(path, selector, workload=None):
    """Load a baseline run from a history file.

    ``path`` is the JSONL history file (the one ``--history``
    writes and ``--trend`` reads); ``selector`` picks one recorded
    run per backend — an index, ``latest``, or an ISO timestamp
    (see :func:`_parse_history_selector`). When ``workload`` is
    given (a workload dict in the shape of
    :func:`kvbench.report._workload_dict`), only recorded runs
    whose workload matches it are considered, so the selector
    counts within the runs that measured the same workload as
    this one. Returns a baseline document in the same shape as a
    JSON report (``generated``, ``command``, ``workload``,
    ``results`` with ``ops_per_sec``, ``errors`` and a ``summary``
    holding ``p99_ms``), so :func:`compare_results` works on it
    unchanged. Records since 0.22.0 also carry per-op ops/sec
    and p99 under ``per_op``, which is passed through so the
    per-op comparison works the same way as for a report
    baseline; older records carry none.

    Raises ValueError when the file does not exist, holds no runs,
    the selector is invalid, an index is out of range for a
    backend, or no recorded run falls at or before a timestamp.
    With a workload filter, the same errors apply to the matching
    runs only — a backend whose runs all used a different
    workload is left out, and a filter that matches no backend
    at all is an error. A history record's workload has the same
    keys a JSON report's does, so the workload comparison still
    applies — the baseline workload carried here is the first
    chosen run's, since picks for different backends can come
    from different runs.
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
    if workload is not None:
        # Only runs that measured the same workload count: a
        # record with no workload (or a workload that differs in
        # any compared setting) is not a baseline for this run.
        by_backend = {
            backend: [r for r in runs
                      if not workload_differences(
                          workload, r.get("workload"))]
            for backend, runs in by_backend.items()
        }
        by_backend = {b: runs for b, runs in by_backend.items()
                      if runs}
        qualifier = " with the same workload"
    else:
        qualifier = ""
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
                f"backend in {path}{qualifier}: no backend has that "
                "many recorded runs")
        raise ValueError(
            f"no run in history file {path} at or before {selector}"
            f"{qualifier}")
    first = chosen[0]
    results = [{
        "name": record.get("backend", "?"),
        "ops": record.get("ops"),
        "elapsed_s": record.get("elapsed_s"),
        "ops_per_sec": record.get("ops_per_sec") or 0.0,
        "errors": record.get("errors"),
        "summary": {"p99_ms": record.get("p99_ms") or 0.0},
        # Per-op throughput and latency, when the record carries
        # them (records since 0.22.0 do): the same per-op
        # comparison as a report baseline, so a history baseline
        # still names the op that moved.
        "per_op": record.get("per_op"),
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


def _histograms_or_none(value):
    """A per-op histogram mapping, or None when it is not one.

    A baseline JSON report written since 0.32.0 carries
    ``histograms_by_op`` per result (op name to the same
    log-spaced bucket list the single-run report uses); a history
    baseline or an older report carries none, and a hand-made
    file may carry something else entirely. Only a non-empty dict
    of op name to bucket lists (each bucket a dict with
    ``upper_ms`` and ``count``) passes through — anything else
    reads as no baseline histograms, instead of crashing the
    report on a shape it cannot draw.
    """
    if not isinstance(value, dict) or not value:
        return None
    for buckets in value.values():
        if not isinstance(buckets, list):
            return None
        for bucket in buckets:
            if (not isinstance(bucket, dict)
                    or "upper_ms" not in bucket
                    or "count" not in bucket):
                return None
    return value


def _per_op_or_none(value):
    """A baseline per-op stats mapping, or None when it is not one.

    A baseline JSON report carries ``per_op`` per result (op name
    to the :func:`kvbench.stats.summarize` dict plus
    ``ops_per_sec``), and a history record carries the same op
    names to ``ops_per_sec`` and ``p99_ms`` since 0.22.0; a
    history baseline or an older report carries none, and a
    hand-made file may carry something else entirely. Only a
    non-empty dict of op name to dicts with numeric
    ``ops_per_sec`` and ``p99_ms`` passes through — anything else
    reads as no baseline per-op stats, instead of crashing the
    report on a shape it cannot compare.
    """
    if not isinstance(value, dict) or not value:
        return None
    for entry in value.values():
        if not isinstance(entry, dict):
            return None
        if (not isinstance(entry.get("ops_per_sec"), (int, float))
                or not isinstance(entry.get("p99_ms"), (int, float))):
            return None
    return value


def _per_op_deltas(current_per_op, baseline_per_op):
    """Per-op p99 and ops/sec deltas versus the baseline run.

    ``current_per_op`` is the current run's ``per_op_summary``
    (op name to summary dict, in SET/GET/INCR/DEL order);
    ``baseline_per_op`` is the validated baseline mapping from
    :func:`_per_op_or_none`, or None when the baseline carries
    no per-op stats. One entry per op that ran in the current
    run, with the current and baseline ops/sec and p99 and the
    percentage change of each; an op the baseline never ran —
    or a baseline with no per-op stats at all — keeps its
    current numbers with the baseline fields and deltas None,
    so the table still shows what this run measured.
    """
    deltas = []
    for op, entry in current_per_op.items():
        baseline = (baseline_per_op or {}).get(op)
        if baseline is None:
            deltas.append({
                "op": op,
                "ops": entry.get("count"),
                "ops_per_sec": entry["ops_per_sec"],
                "baseline_ops_per_sec": None,
                "ops_sec_delta_pct": None,
                "p99_ms": entry["p99_ms"],
                "baseline_p99_ms": None,
                "p99_delta_pct": None,
            })
            continue
        deltas.append({
            "op": op,
            "ops": entry.get("count"),
            "ops_per_sec": entry["ops_per_sec"],
            "baseline_ops_per_sec": baseline["ops_per_sec"],
            "ops_sec_delta_pct": _delta_pct(
                entry["ops_per_sec"], baseline["ops_per_sec"]),
            "p99_ms": entry["p99_ms"],
            "baseline_p99_ms": baseline["p99_ms"],
            "p99_delta_pct": _delta_pct(
                entry["p99_ms"], baseline["p99_ms"]),
        })
    return deltas


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
    it is compared against is the same either way. The socket
    timeout is not compared either: it is a transport setting,
    not part of the workload, and a run that timed out shows it
    in its error count.
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
    more than it. Each entry also carries the per-operation
    p99 and ops/sec deltas under ``per_op`` — one row per op
    that ran, with the current and baseline ops/sec and p99 and
    the percentage change of each, so a p99 move names the op
    that moved, not just that the p99 moved — and the
    per-operation latency histograms of both runs — the current
    run's under
    ``histograms_by_op`` (the same log-spaced buckets as the
    single-run report's, from the same helper, so the comparison
    cannot disagree with it) and the baseline run's under
    ``baseline_histograms_by_op`` when the baseline report
    carries them (JSON reports since 0.32.0 do); a history
    baseline or an older report carries none, so the field is
    None and only the current run's histograms render. Backends
    present in only one of the two runs are listed as unmatched
    instead of being silently dropped.
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
            "per_op": _per_op_deltas(
                result.per_op_summary,
                _per_op_or_none(baseline.get("per_op"))),
            "histograms_by_op": result.per_op_histograms,
            "baseline_histograms_by_op": _histograms_or_none(
                baseline.get("histograms_by_op")),
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
