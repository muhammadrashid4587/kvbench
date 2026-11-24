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


def median(values):
    """Median of a sequence: the middle value, or the mean of the
    two middle values for an even count."""
    if not values:
        raise ValueError("median of empty data")
    ordered = sorted(values)
    n = len(ordered)
    mid = n // 2
    if n % 2:
        return ordered[mid]
    return (ordered[mid - 1] + ordered[mid]) / 2


def spread_summary(values):
    """Summarize the run-to-run spread of a sequence of values.

    Returns ``{"min", "median", "max", "spread", "spread_pct"}``:
    the smallest and largest values, the median, the spread
    (max - min) and the spread as a percentage of the median —
    how much the runs vary relative to their typical value, so a
    wide spread says the runs were noisy and a narrow one says
    the median is trustworthy. ``spread_pct`` is None when the
    median is 0: a percentage of nothing is meaningless, and the
    spread itself still reports. Raises on empty input, like
    :func:`median`.
    """
    if not values:
        raise ValueError("spread of empty data")
    ordered = sorted(values)
    med = median(values)
    spread = ordered[-1] - ordered[0]
    return {
        "min": ordered[0],
        "median": med,
        "max": ordered[-1],
        "spread": spread,
        "spread_pct": (spread / med * 100) if med else None,
    }


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


def attribute_tail(latencies_ns, times_s, by_op, by_client, hot_ns,
                   cold_ns):
    """Attribute a high tail latency to its contributing factor.

    "Slow" ops are the ops at or above the overall p99 latency —
    the slowest ~1% when latencies are distinct, more when many ops
    tie at the p99 value. For each dimension (operation type,
    client, hot vs cold keys, time) every factor reports its p99,
    its share of all ops and its share of slow ops. A factor
    "dominates" when slow ops land on it far more often than its
    share of all ops would predict (slow share exceeds ops share by
    at least 10 percentage points) and it is genuinely slower than
    the rest of that dimension (its p99 is at least twice the p99
    of all other ops in the dimension, over at least 10 samples —
    a p99 over fewer samples is noise). The dominant factor is the
    one with the largest such excess; when none qualifies, no single
    factor dominates the tail.

    Time clustering compares the busiest second's share of slow ops
    against its share of all ops: slow ops are "clustered" when
    they are at least twice as concentrated in one second as the
    run's ops overall (concentration >= 2.0), "spread out"
    otherwise, and time dominates when at least 10 slow ops land in
    that second. A run that fits in a single second has
    concentration 1.0 by construction — there is nowhere for slow
    ops to cluster.

    Returns a dict with the overall p95/p99, the slow threshold,
    per-factor tables (``by_op``, ``by_client``, ``by_segment``),
    a ``time`` dict, the ``dominant`` factor (or None) and a
    human-readable ``finding``.
    """
    empty_time = {
        "slow_seconds": 0,
        "total_seconds": 0,
        "busiest_second": None,
        "busiest_slow_share": 0.0,
        "busiest_ops_share": 0.0,
        "concentration": 0.0,
        "verdict": "no timing data",
    }
    if not latencies_ns:
        return {
            "p99_ms": 0.0,
            "p95_ms": 0.0,
            "slow_threshold_ms": 0.0,
            "slow_ops": 0,
            "finding": "no samples to attribute",
            "dominant": None,
            "by_op": [],
            "by_client": [],
            "by_segment": [],
            "time": dict(empty_time, verdict="no samples"),
        }
    total = len(latencies_ns)
    ordered = sorted(latencies_ns)
    summary = summarize(latencies_ns)
    threshold_ns = percentile(ordered, 99)
    slow_total = sum(1 for v in latencies_ns if v >= threshold_ns)

    def excess(entry):
        return entry["slow_share"] - entry["ops_share"]

    def p99_ratio(entry_p99_ms, rest_samples):
        """Factor p99 divided by the p99 of everything else.

        None when there is nothing else to compare against (a
        dimension with a single factor); infinite when the rest is
        all zero-latency and the factor is not.
        """
        if not rest_samples:
            return None
        rest_p99 = summarize(rest_samples)["p99_ms"]
        if rest_p99 <= 0:
            return float("inf") if entry_p99_ms > 0 else None
        return entry_p99_ms / rest_p99

    def factor(label, samples, rest_samples):
        """Per-factor attribution entry, or None when empty.

        ``rest_samples`` are the latencies outside this factor in
        the same dimension, for the p99 ratio.
        """
        if not samples:
            return None
        slow = sum(1 for v in samples if v >= threshold_ns)
        entry = summarize(samples)
        return {
            "factor": label,
            "count": len(samples),
            "p99_ms": entry["p99_ms"],
            "p99_ratio": p99_ratio(entry["p99_ms"], rest_samples),
            "slow_ops": slow,
            "slow_share": slow / slow_total if slow_total else 0.0,
            "ops_share": len(samples) / total,
        }

    by_op_map = by_op or {}
    op_entries = []
    for op in ("SET", "GET", "INCR", "DEL"):
        rest = [v for other, s in by_op_map.items()
                if other != op for v in s]
        entry = factor(op, by_op_map.get(op), rest)
        if entry is not None:
            op_entries.append(entry)
    by_client_map = by_client or {}
    client_entries = []
    for index in sorted(by_client_map):
        rest = [v for other, s in by_client_map.items()
                if other != index for v in s]
        entry = factor(index, by_client_map[index], rest)
        if entry is not None:
            client_entries.append(entry)
    segment_entries = []
    for label, samples, rest in (("hot", hot_ns, cold_ns),
                                 ("cold", cold_ns, hot_ns)):
        entry = factor(label, samples, rest)
        if entry is not None:
            segment_entries.append(entry)

    time_info = dict(empty_time)
    if times_s and len(times_s) == total:
        total_seconds = max(0, int(max(times_s))) + 1
        slow_per_second = [0] * total_seconds
        ops_per_second = [0] * total_seconds
        for latency_ns, t in zip(latencies_ns, times_s):
            second = max(0, int(t))
            ops_per_second[second] += 1
            if latency_ns >= threshold_ns:
                slow_per_second[second] += 1
        busiest = max(range(total_seconds),
                      key=lambda s: (slow_per_second[s], -s))
        slow_share = (slow_per_second[busiest] / slow_total
                      if slow_total else 0.0)
        ops_share = ops_per_second[busiest] / total
        concentration = slow_share / ops_share if ops_share else 0.0
        time_info = {
            "slow_seconds": sum(1 for n in slow_per_second if n),
            "total_seconds": total_seconds,
            "busiest_second": busiest,
            "busiest_slow_share": slow_share,
            "busiest_ops_share": ops_share,
            "concentration": concentration,
            "verdict": ("clustered" if concentration >= 2.0
                        else "spread out"),
        }

    # The dominant factor is the largest significant excess across
    # all dimensions; dimensions are checked in a fixed order so a
    # tie resolves deterministically.
    candidates = []
    for entry in op_entries:
        candidates.append(("operation", entry))
    for entry in client_entries:
        candidates.append(("client", entry))
    for entry in segment_entries:
        candidates.append(("keyspace", entry))
    if time_info["verdict"] == "clustered":
        time_entry = {
            "factor": time_info["busiest_second"],
            "count": ops_per_second[time_info["busiest_second"]],
            "p99_ms": summary["p99_ms"],
            "p99_ratio": None,
            "slow_ops": slow_per_second[time_info["busiest_second"]],
            "slow_share": time_info["busiest_slow_share"],
            "ops_share": time_info["busiest_ops_share"],
            "concentration": time_info["concentration"],
        }
        candidates.append(("time", time_entry))
    dominant = None
    best_excess = 0.0
    for dimension, entry in candidates:
        if dimension == "time":
            # Time has no p99 ratio (the complement seconds hold
            # slow ops too); it qualifies on concentration plus a
            # minimum slow-op count in the busiest second.
            qualifies = (entry["slow_ops"] >= 10
                         and entry["concentration"] >= 2.0
                         and excess(entry) >= 0.10)
        else:
            qualifies = (entry["count"] >= 10
                         and excess(entry) >= 0.10
                         and entry["p99_ratio"] is not None
                         and entry["p99_ratio"] >= 2.0)
        if qualifies and excess(entry) > best_excess:
            best_excess = excess(entry)
            dominant = {
                "dimension": dimension,
                "factor": entry["factor"],
                "p99_ms": entry["p99_ms"],
                "slow_share": entry["slow_share"],
                "ops_share": entry["ops_share"],
            }
    if dominant is None:
        finding = "no single factor dominates the tail"
    elif dominant["dimension"] == "operation":
        finding = (
            f"{dominant['factor']} dominates the tail — p99 "
            f"{dominant['p99_ms']:.3f} ms vs "
            f"{summary['p99_ms']:.3f} ms overall, and "
            f"{dominant['slow_share'] * 100:.0f}% of slow ops are "
            f"{dominant['factor']} (vs "
            f"{dominant['ops_share'] * 100:.0f}% of all ops)"
        )
    elif dominant["dimension"] == "client":
        finding = (
            f"client {dominant['factor']} dominates the tail — p99 "
            f"{dominant['p99_ms']:.3f} ms vs "
            f"{summary['p99_ms']:.3f} ms overall, and "
            f"{dominant['slow_share'] * 100:.0f}% of slow ops ran on "
            f"client {dominant['factor']} (vs "
            f"{dominant['ops_share'] * 100:.0f}% of all ops)"
        )
    elif dominant["dimension"] == "keyspace":
        finding = (
            f"{dominant['factor']} keys dominate the tail — p99 "
            f"{dominant['p99_ms']:.3f} ms vs "
            f"{summary['p99_ms']:.3f} ms overall, and "
            f"{dominant['slow_share'] * 100:.0f}% of slow ops hit "
            f"{dominant['factor']} keys (vs "
            f"{dominant['ops_share'] * 100:.0f}% of all ops)"
        )
    else:
        finding = (
            "slow ops cluster in time — second "
            f"{dominant['factor']} holds "
            f"{dominant['slow_share'] * 100:.0f}% of slow ops but "
            f"only {dominant['ops_share'] * 100:.0f}% of all ops"
        )
    return {
        "p99_ms": summary["p99_ms"],
        "p95_ms": summary["p95_ms"],
        "slow_threshold_ms": threshold_ns / 1_000_000,
        "slow_ops": slow_total,
        "finding": finding,
        "dominant": dominant,
        "by_op": op_entries,
        "by_client": client_entries,
        "by_segment": segment_entries,
        "time": time_info,
    }


def time_series(latencies_ns, times_s):
    """Per-second latency buckets, to spot stalls over a run.

    ``times_s`` holds one completion time per latency, in seconds
    since the run started. Returns a list of ``{"second": int,
    "ops": int, "mean_ms": float, "max_ms": float}`` dicts, one per
    second from 0 through the last second that holds a sample.
    Seconds with no ops are kept (ops 0, mean/max 0.0): a stall shows
    up as an empty second instead of disappearing from the series.
    """
    if not latencies_ns:
        return []
    if len(latencies_ns) != len(times_s):
        raise ValueError("latencies and times must have the same length")
    last_second = max(0, int(max(times_s)))
    buckets = [{"second": s, "ops": 0, "mean_ms": 0.0, "max_ms": 0.0}
               for s in range(last_second + 1)]
    totals_ns = [0] * (last_second + 1)
    for latency_ns, t in zip(latencies_ns, times_s):
        second = max(0, int(t))
        bucket = buckets[second]
        bucket["ops"] += 1
        totals_ns[second] += latency_ns
        ms = latency_ns / 1_000_000
        if ms > bucket["max_ms"]:
            bucket["max_ms"] = ms
    for bucket, total_ns in zip(buckets, totals_ns):
        if bucket["ops"]:
            bucket["mean_ms"] = total_ns / bucket["ops"] / 1_000_000
    return buckets


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
