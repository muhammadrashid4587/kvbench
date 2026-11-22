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
