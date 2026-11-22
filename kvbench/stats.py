"""Latency statistics.

Latencies are collected in nanoseconds and reported in milliseconds.
Percentiles use the nearest-rank method: sort the samples, take the
value at index ceil(p/100 * n) - 1 (clamped to the last sample).
"""


def percentile(sorted_samples, p):
    """p-th percentile of an already-sorted sequence (nearest-rank)."""
    if not sorted_samples:
        raise ValueError("percentile of empty data")
    if not 0 < p <= 100:
        raise ValueError("p must be in (0, 100]")
    import math

    index = math.ceil(p / 100 * len(sorted_samples)) - 1
    return sorted_samples[max(0, min(index, len(sorted_samples) - 1))]


def summarize(latencies_ns):
    """Summarize a list of latencies (ns) into a dict of ms figures."""
    if not latencies_ns:
        return {
            "count": 0,
            "mean_ms": 0.0,
            "p50_ms": 0.0,
            "p95_ms": 0.0,
            "p99_ms": 0.0,
            "min_ms": 0.0,
            "max_ms": 0.0,
        }
    ordered = sorted(latencies_ns)
    n = len(ordered)
    return {
        "count": n,
        "mean_ms": sum(ordered) / n / 1_000_000,
        "p50_ms": percentile(ordered, 50) / 1_000_000,
        "p95_ms": percentile(ordered, 95) / 1_000_000,
        "p99_ms": percentile(ordered, 99) / 1_000_000,
        "min_ms": ordered[0] / 1_000_000,
        "max_ms": ordered[-1] / 1_000_000,
    }


# Log-spaced bucket upper edges in ms. Latencies above the last edge
# land in the overflow bucket, whose upper_ms is None.
HISTOGRAM_EDGES_MS = (
    0.1, 0.25, 0.5, 1, 2, 5, 10, 25, 50, 100, 250, 500, 1000,
)


def histogram(latencies_ns):
    """Bucket latencies (ns) into log-spaced ms buckets.

    Returns a list of ``{"upper_ms": float | None, "count": int}``
    dicts, one per edge plus a final overflow bucket (``upper_ms`` is
    ``None``). Empty trailing buckets are dropped so a fast run does
    not print a wall of zeroes; the overflow bucket is kept only when
    it holds samples.
    """
    counts = [0] * (len(HISTOGRAM_EDGES_MS) + 1)
    for latency_ns in latencies_ns:
        ms = latency_ns / 1_000_000
        placed = False
        for i, edge in enumerate(HISTOGRAM_EDGES_MS):
            if ms <= edge:
                counts[i] += 1
                placed = True
                break
        if not placed:
            counts[-1] += 1
    buckets = [
        {"upper_ms": edge, "count": counts[i]}
        for i, edge in enumerate(HISTOGRAM_EDGES_MS)
    ]
    buckets.append({"upper_ms": None, "count": counts[-1]})
    # Drop empty trailing buckets (the overflow bucket counts as
    # non-empty only when it holds samples).
    while buckets and buckets[-1]["count"] == 0:
        buckets.pop()
    return buckets
