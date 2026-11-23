"""Compare a run against a previous run's JSON report.

``--compare-file PATH`` loads a JSON report written by an earlier
kvbench run (the ``--json`` output) and compares this run's results
against it, backend by backend, matched by name: ops/sec and p99
side by side, with the percentage change and a verdict per backend.
``--regression-threshold PCT`` turns the comparison into a gate:
the run exits non-zero when any backend's ops/sec fell by more than
PCT, or its p99 rose by more than PCT, versus the baseline file.

A comparison only means something when the two runs used the same
workload, so the comparison also records which workload settings
differed (ops, clients, keys, mix, pipeline, ...) — the report
warns when they do not match instead of silently comparing runs
that measured different things.
"""

import json
import os

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
                    baseline_path, threshold_pct=None):
    """Compare ``results`` against a loaded baseline document.

    Backends are matched by name. Each matched backend gets an
    entry with the current and baseline ops/sec and p99, the
    percentage change of each, the error counts, a verdict, and a
    ``regression`` flag: True when a threshold was given and the
    ops/sec fell by more than the threshold, or the p99 rose by
    more than it. Backends present in only one of the two runs are
    listed as unmatched instead of being silently dropped.

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
