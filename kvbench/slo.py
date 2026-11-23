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


def evaluate_slo(results, p99_ms=None, error_rate_threshold_pct=None):
    """Evaluate the SLO gate for every result.

    Returns None when no threshold was given (no gate). Otherwise a
    JSON-serializable dict with the thresholds, one entry per
    backend (measured p99 and error rate, the thresholds that apply,
    which ones breached, and a ``pass``/``breach`` verdict), and the
    count of breaching backends.
    """
    if p99_ms is None and error_rate_threshold_pct is None:
        return None
    entries = []
    for result in results:
        measured_p99 = result.summary["p99_ms"]
        rate = error_rate_pct(result)
        p99_breach = p99_ms is not None and measured_p99 > p99_ms
        rate_breach = (error_rate_threshold_pct is not None
                       and rate > error_rate_threshold_pct)
        entries.append({
            "name": result.name,
            "p99_ms": measured_p99,
            "p99_threshold_ms": p99_ms,
            "p99_breach": p99_breach,
            "error_rate_pct": rate,
            "error_rate_threshold_pct": error_rate_threshold_pct,
            "error_rate_breach": rate_breach,
            "verdict": "breach" if (p99_breach or rate_breach)
                       else "pass",
        })
    return {
        "p99_threshold_ms": p99_ms,
        "error_rate_threshold_pct": error_rate_threshold_pct,
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
    detail = ", ".join(parts) or "no thresholds set"
    if entry["verdict"] == "breach":
        return f"{entry['name']}: SLO breach — {detail}"
    return f"{entry['name']}: SLO passed — {detail}"
