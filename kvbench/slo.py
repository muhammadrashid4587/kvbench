"""SLO gate: fail a run when a backend breaches a latency or error
budget.

``--slo-p99-ms`` caps the measured p99 latency (milliseconds) and
``--slo-error-rate`` caps the error rate (percent of ops that
failed). Either flag on its own is a gate; both together gate on
both. A backend breaches when its measured value is strictly above
the threshold — exactly at the threshold still passes, so a
threshold of 0 only fails when something actually went wrong. The
gate is evaluated after the reports are written, so a breached run
still leaves a report showing the verdict, and the process exits 4
(a distinct code from the regression gate's 3).
"""

def error_rate_pct(result):
    """Error rate of a result, as a percentage of all ops attempted.

    ``result.ops`` counts successes plus errors, so the rate is the
    share of attempted ops that failed. A run with no ops at all has
    a 0% error rate — there is nothing to fail.
    """
    if result.ops <= 0:
        return 0.0
    return result.errors / result.ops * 100


def per_op_error_rate_pct(result, op):
    """Error rate of one operation type, as a percentage of its
    attempted ops.

    Attempted ops for ``op`` are its successful latencies plus its
    recorded errors (see ``BenchmarkResult.errors_by_op``). An op
    that never ran has a 0% error rate.
    """
    attempted = (len(result.latencies_by_op.get(op, ()))
                 + result.errors_by_op.get(op, 0))
    if attempted <= 0:
        return 0.0
    return result.errors_by_op.get(op, 0) / attempted * 100


def _evaluate_per_op(result, per_op):
    """Per-operation SLO verdicts for one result.

    ``per_op`` maps an op name (``SET``/``GET``/``INCR``) to a
    ``(p99_ms, error_rate_pct)`` pair, either of which may be None
    (that side of the gate is not set for that op). Ops that never
    ran — no successful latencies and no errors — are skipped: there
    is nothing to gate. Returns a list of per-op entry dicts in
    stable SET/GET/INCR order.
    """
    entries = []
    for op in ("SET", "GET", "INCR"):
        if op not in per_op:
            continue
        p99_threshold, rate_threshold = per_op[op]
        samples = result.latencies_by_op.get(op, ())
        errors = result.errors_by_op.get(op, 0)
        if not samples and not errors:
            continue
        measured_p99 = (result.per_op_summary[op]["p99_ms"]
                        if samples else 0.0)
        rate = per_op_error_rate_pct(result, op)
        p99_breach = (p99_threshold is not None
                      and measured_p99 > p99_threshold)
        rate_breach = (rate_threshold is not None
                       and rate > rate_threshold)
        entries.append({
            "op": op,
            "ops": len(samples) + errors,
            "p99_ms": measured_p99,
            "p99_threshold_ms": p99_threshold,
            "p99_breach": p99_breach,
            "error_rate_pct": rate,
            "error_rate_threshold_pct": rate_threshold,
            "error_rate_breach": rate_breach,
            "verdict": "breach" if (p99_breach or rate_breach)
                       else "pass",
        })
    return entries


def evaluate_slo(results, p99_ms=None, error_rate_threshold_pct=None,
                 per_op=None):
    """Evaluate the SLO gate for every result.

    Returns None when no threshold was given (no gate). Otherwise a
    JSON-serializable dict with the thresholds, one entry per
    backend (measured p99 and error rate, the thresholds that apply,
    which ones breached, and a ``pass``/``breach`` verdict), and the
    count of breaching backends.

    ``per_op`` maps an op name (``SET``/``GET``/``INCR``) to a
    ``(p99_ms, error_rate_pct)`` pair, either of which may be None:
    each operation type is gated on its own thresholds as well as
    the global ones, so a slow SET breaches even when the overall
    p99 is fine. A backend breaches when any of its global or
    per-op thresholds is breached.
    """
    per_op = per_op or {}
    if (p99_ms is None and error_rate_threshold_pct is None
            and not per_op):
        return None
    entries = []
    for result in results:
        measured_p99 = result.summary["p99_ms"]
        rate = error_rate_pct(result)
        p99_breach = p99_ms is not None and measured_p99 > p99_ms
        rate_breach = (error_rate_threshold_pct is not None
                       and rate > error_rate_threshold_pct)
        per_op_entries = _evaluate_per_op(result, per_op)
        entries.append({
            "name": result.name,
            "p99_ms": measured_p99,
            "p99_threshold_ms": p99_ms,
            "p99_breach": p99_breach,
            "error_rate_pct": rate,
            "error_rate_threshold_pct": error_rate_threshold_pct,
            "error_rate_breach": rate_breach,
            "per_op": per_op_entries,
            "verdict": "breach" if (p99_breach or rate_breach
                                    or any(e["verdict"] == "breach"
                                           for e in per_op_entries))
                       else "pass",
        })
    return {
        "p99_threshold_ms": p99_ms,
        "error_rate_threshold_pct": error_rate_threshold_pct,
        "per_op_thresholds": {
            op: {"p99_ms": thresholds[0],
                 "error_rate_pct": thresholds[1]}
            for op, thresholds in per_op.items()
        } if per_op else None,
        "entries": entries,
        "breaches": sum(1 for e in entries
                        if e["verdict"] == "breach"),
    }


def format_slo_verdict(entry):
    """Human-readable verdict line for one SLO entry.

    E.g. ``cinder: SLO passed — p99 0.123 ms (limit 1 ms), error
    rate 0.00% (limit 1%)`` or ``cinder: SLO breach — p99 2.500 ms
    (limit 1 ms)``. Only the thresholds that were set are shown.
    """
    parts = []
    if entry["p99_threshold_ms"] is not None:
        parts.append(
            f"p99 {entry['p99_ms']:.3f} ms "
            f"(limit {entry['p99_threshold_ms']:g} ms)")
    if entry["error_rate_threshold_pct"] is not None:
        parts.append(
            f"error rate {entry['error_rate_pct']:.2f}% "
            f"(limit {entry['error_rate_threshold_pct']:g}%)")
    for per_op in entry.get("per_op", []):
        # Only breaching per-op entries are named: a passing op
        # adds noise, a breaching one is the reason the run failed.
        if per_op["verdict"] != "breach":
            continue
        op_parts = []
        if per_op["p99_threshold_ms"] is not None:
            op_parts.append(
                f"p99 {per_op['p99_ms']:.3f} ms "
                f"(limit {per_op['p99_threshold_ms']:g} ms)")
        if per_op["error_rate_threshold_pct"] is not None:
            op_parts.append(
                f"error rate {per_op['error_rate_pct']:.2f}% "
                f"(limit {per_op['error_rate_threshold_pct']:g}%)")
        parts.append(f"{per_op['op']} " + ", ".join(op_parts))
    detail = ", ".join(parts) or "no thresholds set"
    if entry["verdict"] == "breach":
        return f"{entry['name']}: SLO breach — {detail}"
    return f"{entry['name']}: SLO passed — {detail}"
