"""Markdown and JSON report writers."""

import csv
import io
import json
import platform
import sys
from datetime import datetime, timezone

from . import __version__
from .bench import format_duration
from .distribution import ZipfianSampler


def _workload_dict(workload):
    doc = {
        # In duration mode there is no fixed op target: ops is null
        # and the run length is under duration_s.
        "ops": None if workload.duration_s is not None else workload.ops,
        "duration_s": workload.duration_s,
        # Warmup seconds before measurement (0 when no warmup); the
        # warmup ops themselves are per-result, under warmup_ops.
        "warmup_s": workload.warmup_s,
        "clients": workload.clients,
        "keys": workload.keys,
        # Prefix for every benchmark key (default "kvbench").
        "key_prefix": workload.key_prefix,
        "value_size": workload.value_size,
        "mix": {
            "set": workload.set_weight,
            "get": workload.get_weight,
            "incr": workload.incr_weight,
            "del": workload.del_weight,
        },
        "mix_label": workload.mix_label or None,
        "seed": workload.seed,
        "pipeline": workload.pipeline,
        # Socket timeout for the server connection in seconds; the
        # same value for connect and read, the RespClient default.
        "timeout_s": workload.timeout_s,
        "distribution": workload.distribution,
        # The skew only means anything for zipfian; null otherwise.
        "zipf_skew": (workload.zipf_skew
                      if workload.distribution == "zipfian" else None),
        # How many of the hottest keys get per-key stats (0 disables).
        "hot_keys": workload.hot_keys,
        # Keyspace sizes swept in this run, or null when no sweep.
        "sweep_keyspace": (list(workload.sweep_keyspace)
                           if workload.sweep_keyspace else None),
        # Client counts swept in this run, or null when no sweep.
        "sweep_clients": (list(workload.sweep_clients)
                          if workload.sweep_clients else None),
        # Value sizes (bytes) swept in this run, or null when no sweep.
        "sweep_value_size": (list(workload.sweep_value_size)
                             if workload.sweep_value_size else None),
        # Pipeline depths swept in this run, or null when no sweep.
        "sweep_pipeline": (list(workload.sweep_pipeline)
                           if workload.sweep_pipeline else None),
        # Read:write ratios swept in this run, as "READ:WRITE"
        # strings in sweep order (read-heaviest first), or null
        # when no sweep.
        "sweep_mix": ([f"{read}:{write}"
                       for read, write in workload.sweep_mix]
                      if workload.sweep_mix else None),
        # SLO gate thresholds for this run, or null when not set.
        "slo_p99_ms": workload.slo_p99_ms,
        "slo_error_rate_pct": workload.slo_error_rate_pct,
        # Per-operation SLO thresholds for this run: op name to
        # {"p99_ms", "error_rate_pct"}, or null when not set.
        "slo_per_op": (
            {op: {"p99_ms": thresholds[0],
                  "error_rate_pct": thresholds[1]}
             for op, thresholds in workload.slo_per_op.items()}
            if workload.slo_per_op else None),
        # The preset file the SLO thresholds were loaded from, or
        # null when they were given on the command line.
        "slo_preset": workload.slo_preset,
    }
    if workload.workload_file is not None:
        # Only a run from a config file records it: a command-line
        # run keeps the old workload shape exactly. The file is
        # where the workload was declared; the resolved values are
        # the fields above, so the run is reproducible from this
        # record even without the file. It is deliberately not one
        # of the compared keys (see kvbench.compare): two runs with
        # the same resolved values are the same workload, whether
        # one was declared in a file and the other on the command
        # line.
        doc["workload_file"] = workload.workload_file
    if workload.auth:
        # Only an authenticated run records it: a run without auth
        # keeps the old workload shape exactly, and the password
        # itself is never recorded — only that AUTH was used.
        doc["auth"] = True
    if workload.repeat > 1:
        # Only a repeated run records the repeat count: a single
        # run's workload dict is unchanged, so old reports and
        # --compare-same-workload keep matching.
        doc["repeat"] = workload.repeat
    return doc


def _result_dict(result, top_n=5):
    doc = {
        "name": result.name,
        "ops": result.ops,
        "elapsed_s": result.elapsed_s,
        "ops_per_sec": result.ops_per_sec,
        "errors": result.errors,
        "error_sample": result.error_sample,
        "summary": result.summary,
        "per_op": result.per_op_summary,
        # Per-key latency stats for the hottest keys by access count:
        # count, mean, p50, p95, p99, min and max per key.
        "hot_keys": result.hot_key_summary(top_n),
        # Tail-latency attribution: which op, client, keyspace
        # segment or time window the slowest ops come from, and the
        # finding naming the dominant factor, if any.
        "tail": result.tail_report,
        "histogram": result.histogram,
        # Per-operation latency histograms: op name ("SET", "GET",
        # "INCR", "DEL") to the same log-spaced buckets as
        # ``histogram``, so a slow op type's latency shape shows
        # separately from the overall distribution. Ops that never
        # ran are left out.
        "histograms_by_op": result.per_op_histograms,
        "time_series": result.time_series,
        # Share of successful ops that hit the hottest 1% of keys
        # (~0.01 under uniform; much higher under zipfian).
        "hot_hit_rate": result.hot_hit_rate,
        # Ops run in the warmup phase, before measurement. They are
        # not part of ``ops`` and contribute no latencies.
        "warmup_ops": result.warmup_ops,
        # Keyspace sweep: one per-size summary per swept size
        # (keys, ops, ops/sec, mean and p99, plus the per-op latency
        # histograms of that run under ``histograms_by_op`` — the
        # same log-spaced buckets per op as the main run's); with
        # ``--repeat`` above 1 each size runs that many times, the
        # ops/sec and p99 are the medians over the runs, and the
        # entry carries the per-run values and the run-to-run
        # spread under ``repeat`` (``runs`` and ``spread``, the
        # same shapes as the main run's ``repeat``); [] when no
        # sweep ran.
        "keyspace_sweep": result.sweep,
        # Client-count sweep: one per-count summary per swept count
        # (clients, ops, ops/sec, mean and p99, plus the per-op
        # latency histograms of that run under
        # ``histograms_by_op``); with ``--repeat`` above 1 each
        # count runs that many times and the entry carries the
        # per-run values and spread under ``repeat``; [] when no
        # sweep ran.
        "client_sweep": result.client_sweep,
        # Value-size sweep: one per-size summary per swept size
        # (value_size in bytes, ops, ops/sec, mean and p99, plus
        # the per-op latency histograms of that run under
        # ``histograms_by_op``); with ``--repeat`` above 1 each
        # size runs that many times and the entry carries the
        # per-run values and spread under ``repeat``; [] when no
        # sweep ran.
        "value_size_sweep": result.value_sweep,
        # Pipeline-depth sweep: one per-depth summary per swept
        # depth (pipeline in commands per round trip, ops, ops/sec,
        # mean and p99, plus the per-op latency histograms of that
        # run under ``histograms_by_op``); with ``--repeat`` above 1
        # each depth runs that many times and the entry carries the
        # per-run values and spread under ``repeat``; [] when no
        # sweep ran.
        "pipeline_sweep": result.pipeline_sweep,
        # Read:write mix sweep: one per-ratio summary per swept
        # ratio (mix as a "READ:WRITE" string, ops, ops/sec, mean
        # and p99, plus the per-op latency histograms of that run
        # under ``histograms_by_op``); with ``--repeat`` above 1
        # each ratio runs that many times and the entry carries the
        # per-run values and spread under ``repeat``; [] when no
        # sweep ran.
        "mix_sweep": result.mix_sweep,
    }
    if result.repeat_runs:
        # Only a repeated run carries this: a single run's result
        # dict is unchanged. ``runs`` is one summary per run (run
        # index, ops, elapsed, ops/sec, errors, mean/p50/p95/p99,
        # per-op ops/sec and p99, and the per-operation latency
        # histograms of that run under ``histograms_by_op`` — the
        # same log-spaced buckets per op as the main run's); the
        # top-level ops/sec, summary p99 and per-op figures are
        # the medians over them.
        doc["repeat"] = {
            "runs": result.repeat_runs,
            "median_ops_per_sec": result.median_ops_per_sec,
            "median_p99_ms": result.median_p99_ms,
            "median_per_op": result.median_per_op,
        }
        if result.repeat_spread:
            # Only a run repeated more than once carries a spread:
            # one run has no spread to report, so a single
            # ``--repeat 1`` run keeps the old shape. ``spread``
            # is min, median, max, spread (max - min) and spread
            # as a percentage of the median, for ops/sec and p99
            # overall and per operation.
            doc["repeat"]["spread"] = result.repeat_spread
    return doc


def to_json_dict(results, workload, command_line, comparison=None,
                 slo=None):
    """Build the JSON-serializable report document."""
    doc = {
        "tool": "kvbench",
        "version": __version__,
        "generated": datetime.now(timezone.utc).isoformat(),
        "python": (
            f"{sys.version.split()[0]} on {platform.system()} "
            f"{platform.machine()} ({platform.python_implementation()})"
        ),
        "command": command_line,
        "workload": _workload_dict(workload),
        "results": [_result_dict(r, workload.hot_keys) for r in results],
    }
    if comparison is not None:
        doc["comparison"] = comparison
    if slo is not None:
        doc["slo"] = slo
    return doc


def render_json(results, workload, command_line, comparison=None,
                slo=None):
    """Render the report as a JSON string."""
    return json.dumps(
        to_json_dict(results, workload, command_line, comparison, slo),
        indent=2,
    ) + "\n"


def write_json(results, workload, command_line, path, comparison=None,
               slo=None):
    """Write the JSON report to ``path`` and return its text."""
    text = render_json(results, workload, command_line, comparison, slo)
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)
    return text


# CSV header: one row per backend per second of the run.
_CSV_HEADER = ("backend", "second", "ops", "ops_per_sec",
               "mean_ms", "max_ms")


def render_csv(results):
    """Render per-second throughput as CSV text.

    One row per backend per second, from the same per-second series
    the Markdown report shows under Latency over time. Each row
    covers exactly one second, so ``ops_per_sec`` is the ops
    completed in that second; a row with 0 ops is a stall second.
    Results with no time series (built without completion times)
    contribute no rows. The output is plain CSV with a header, so
    it plots directly (e.g. in a spreadsheet or with matplotlib)
    without parsing the Markdown report.
    """
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(_CSV_HEADER)
    for result in results:
        for bucket in result.time_series:
            writer.writerow([
                result.name,
                bucket["second"],
                bucket["ops"],
                bucket["ops"],
                f"{bucket['mean_ms']:.3f}",
                f"{bucket['max_ms']:.3f}",
            ])
    return buf.getvalue()


def write_csv(results, path):
    """Write the throughput CSV to ``path`` and return its text."""
    text = render_csv(results)
    with open(path, "w", encoding="utf-8", newline="") as f:
        f.write(text)
    return text


def _row(result):
    s = result.summary
    return (
        f"| {result.name} | {result.ops} | {result.ops_per_sec:,.0f} | "
        f"{s['mean_ms']:.3f} | {s['p50_ms']:.3f} | {s['p95_ms']:.3f} | "
        f"{s['p99_ms']:.3f} | {result.errors} |"
    )


def _histogram_bucket_lines(buckets, total):
    """Text-bar lines for one histogram's buckets.

    Each line is the bucket label, count and share, with a bar scaled
    to the largest bucket so the shape reads at a glance (capped at
    40 characters so long runs stay readable).
    """
    lines = []
    for bucket in buckets:
        label = (
            "> 1000 ms"
            if bucket["upper_ms"] is None
            else f"<= {bucket['upper_ms']:g} ms"
        )
        width = round(bucket["count"] / max(b["count"] for b in buckets) * 40)
        lines.append(
            f"- {label}: {bucket['count']} "
            f"({bucket['count'] / total * 100:.1f}%) {'#' * width}"
        )
    return lines


def _histogram_lines(result):
    """Text-bar histogram lines for one result, or [] when empty."""
    buckets = result.histogram
    if not buckets:
        return []
    lines = [f"### Latency histogram — {result.name}", ""]
    lines += _histogram_bucket_lines(buckets, result.summary["count"])
    lines.append("")
    return lines


def _per_op_histogram_block(heading, per_op):
    """Text-bar per-op histograms under one heading, or [].

    ``per_op`` is an op name to log-spaced bucket list mapping —
    the same shape as a result's ``histograms_by_op`` — so the
    helper serves both the main run's per-op section and each
    sweep step's. Each op's buckets are scaled to its own largest
    bucket, and ops with no samples are absent from the mapping.
    """
    if not per_op:
        return []
    lines = [heading, ""]
    for op, buckets in per_op.items():
        total = sum(b["count"] for b in buckets)
        lines.append(f"{op} ({total} ops)")
        lines.append("")
        lines += _histogram_bucket_lines(buckets, total)
        lines.append("")
    return lines


def _per_op_histogram_lines(result):
    """Per-operation text-bar histograms for one result, or [].

    One bucket list per operation that ran, using the same
    log-spaced buckets as the overall histogram, each scaled to its
    own largest bucket — so a slow op type's shape shows separately
    from the overall distribution instead of being swamped by the
    fast ops.
    """
    return _per_op_histogram_block(
        f"### Per-operation latency histogram — {result.name}",
        result.per_op_histograms,
    )


def _histogram_comparison_table_lines(heading, from_label, to_label,
                                      rows):
    """The per-op histogram comparison table under one heading.

    ``rows`` are :func:`kvbench.history._histogram_comparison_rows`
    rows, or the same shape from the compare view's best/worst
    histogram comparison; the labels name the two sides in the
    column headers ("first"/"last" for the trend, "worst"/"best"
    for best-vs-worst, "prev"/"now" for the compare view). A
    side with no p50/p99 shows ``-``, and the p99 change and
    bucket drift show ``-`` when they cannot be computed.
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


def _sweep_per_op_histogram_lines(label, entries):
    """Per-operation histograms for each sweep step, or [].

    ``label`` formats one step (e.g. ``lambda e: f"{e['keys']} keys"``);
    each entry carries ``histograms_by_op`` in the same shape as a
    regular run's — one log-spaced bucket list per operation that
    ran in that step — so a sweep step's slow op type shows the
    same way the main run's does. Entries without the key (older
    reports, or hand-built results) are skipped, so the table
    still renders.
    """
    lines = []
    for entry in entries:
        per_op = entry.get("histograms_by_op")
        if not per_op:
            continue
        lines += _per_op_histogram_block(
            f"#### {label(entry)} — per-op latency histogram", per_op)
    return lines


def _per_op_lines(result):
    """Per-operation stats table for one result, or [] when empty."""
    per_op = result.per_op_summary
    if not per_op:
        return []
    lines = [
        f"### Per-operation stats — {result.name}",
        "",
        "| Op | Ops | Ops/sec | Mean (ms) | p50 (ms) | p95 (ms) | "
        "p99 (ms) |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for op, s in per_op.items():
        lines.append(
            f"| {op} | {s['count']} | {s['ops_per_sec']:,.0f} | "
            f"{s['mean_ms']:.3f} | {s['p50_ms']:.3f} | "
            f"{s['p95_ms']:.3f} | {s['p99_ms']:.3f} |"
        )
    lines.append("")
    return lines


def _hot_key_lines(result, top_n=5):
    """Per-key latency table for one result, or [] when empty."""
    hot_keys = result.hot_key_summary(top_n)
    if not hot_keys:
        return []
    lines = [
        f"### Per-key latency — {result.name}",
        "",
        "| Key | Ops | Mean (ms) | p50 (ms) | p95 (ms) | p99 (ms) |",
        "| --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for entry in hot_keys:
        lines.append(
            f"| `{entry['key']}` | {entry['count']} | "
            f"{entry['mean_ms']:.3f} | {entry['p50_ms']:.3f} | "
            f"{entry['p95_ms']:.3f} | {entry['p99_ms']:.3f} |"
        )
    lines.append("")
    return lines


def _tail_lines(result):
    """Tail-latency attribution section for one result.

    The finding names the factor that dominates the tail (one slow
    op type, one slow client, hot or cold keys, or a slow time
    window), or says no single factor does. The tables underneath
    show the same breakdown the finding is drawn from: per-op p99
    and slow-op share, per-client p99 and slow-op share (only when
    there is more than one client), hot vs cold p99 and slow-op
    share, and whether slow ops cluster in one second or spread
    over the run.
    """
    tail = result.tail_report
    if not tail["by_op"] and not tail["by_client"] \
            and not tail["by_segment"]:
        return []
    lines = [
        f"### Tail-latency attribution — {result.name}",
        "",
        f"Finding: {tail['finding']}",
        "",
        f"Slow ops are the {tail['slow_ops']} ops at or above the "
        f"p99 latency of {tail['p99_ms']:.3f} ms.",
        "",
    ]
    if tail["by_op"]:
        lines += [
            "| Op | Ops | p99 (ms) | Slow ops | Slow share |",
            "| --- | ---: | ---: | ---: | ---: |",
        ]
        for entry in tail["by_op"]:
            lines.append(
                f"| {entry['factor']} | {entry['count']} | "
                f"{entry['p99_ms']:.3f} | {entry['slow_ops']} | "
                f"{entry['slow_share'] * 100:.1f}% |"
            )
        lines.append("")
    if len(tail["by_client"]) > 1:
        lines += [
            "| Client | Ops | p99 (ms) | Slow ops | Slow share |",
            "| ---: | ---: | ---: | ---: | ---: |",
        ]
        for entry in tail["by_client"]:
            lines.append(
                f"| {entry['factor']} | {entry['count']} | "
                f"{entry['p99_ms']:.3f} | {entry['slow_ops']} | "
                f"{entry['slow_share'] * 100:.1f}% |"
            )
        lines.append("")
    if tail["by_segment"]:
        lines += [
            "| Keys | Ops | p99 (ms) | Slow ops | Slow share |",
            "| --- | ---: | ---: | ---: | ---: |",
        ]
        for entry in tail["by_segment"]:
            lines.append(
                f"| {entry['factor']} | {entry['count']} | "
                f"{entry['p99_ms']:.3f} | {entry['slow_ops']} | "
                f"{entry['slow_share'] * 100:.1f}% |"
            )
        lines.append("")
    time_info = tail["time"]
    if time_info["verdict"] == "clustered":
        lines.append(
            f"Slow ops cluster in time: second "
            f"{time_info['busiest_second']} holds "
            f"{time_info['busiest_slow_share'] * 100:.0f}% of slow "
            f"ops but only "
            f"{time_info['busiest_ops_share'] * 100:.0f}% of all ops."
        )
    elif time_info["verdict"] == "spread out":
        lines.append(
            f"Slow ops are spread out in time: they land in "
            f"{time_info['slow_seconds']} of "
            f"{time_info['total_seconds']} seconds, and the "
            f"busiest second holds "
            f"{time_info['busiest_slow_share'] * 100:.0f}% of them."
        )
    lines.append("")
    return lines


def _sweep_repeat_lines(label, entries):
    """Per-run values for repeated sweep steps, or [].

    ``label`` formats one step (e.g. ``lambda e: f"{e['keys']} keys"``);
    each entry that ran ``--repeat`` more than once carries its
    per-run values and run-to-run spread under ``repeat`` (the same
    shapes as a repeated result's ``repeat_runs`` and
    ``repeat_spread``) — the step's table row already shows the
    medians, so this block shows the runs behind them: one row per
    run with ops/sec and p99, plus the spread of each as a
    percentage of its median. The full per-run detail (mean, p50,
    per-op values, per-op histograms) lives in the JSON report
    under the step's ``repeat.runs``. Entries without ``repeat``
    (a single run, or a sweep recorded before 0.36.0) are skipped,
    so the table still renders.
    """
    lines = []
    for entry in entries:
        repeated = entry.get("repeat")
        if not repeated:
            continue
        runs = repeated.get("runs") or []
        if not runs:
            continue
        lines += [
            f"#### {label(entry)} — repeat runs",
            "",
            f"Median of {len(runs)} runs "
            f"({entry['ops_per_sec']:,.0f} ops/sec, "
            f"p99 {entry['p99_ms']:.3f} ms).",
            "",
            "| Run | Ops/sec | p99 (ms) |",
            "| ---: | ---: | ---: |",
        ]
        for run in runs:
            lines.append(
                f"| {run['run']} | {run['ops_per_sec']:,.0f} | "
                f"{run['p99_ms']:.3f} |"
            )
        spread = repeated.get("spread")
        if spread:
            ops = spread["overall"]["ops_per_sec"]
            p99 = spread["overall"]["p99_ms"]
            lines.append(
                f"Spread: ops/sec {ops['spread']:,.0f} "
                f"({_spread_pct_text(ops)} of the median), p99 "
                f"{p99['spread']:.3f} ms "
                f"({_spread_pct_text(p99)} of the median)."
            )
        lines.append("")
    return lines


def _sweep_lines(result):
    """Keyspace sweep table for one result, or [] when no sweep ran.

    The table is followed by a per-operation latency histogram
    per swept size, so a size where one op type slows down shows
    which op it is, not just that the p99 moved. When a step ran
    ``--repeat`` more than once its table row shows the medians
    over the runs, and a per-step repeat block shows the runs
    behind them.
    """
    if not result.sweep:
        return []
    lines = [
        f"### Keyspace sweep — {result.name}",
        "",
        "| Keys | Ops | Ops/sec | Mean (ms) | p99 (ms) |",
        "| ---: | ---: | ---: | ---: | ---: |",
    ]
    for entry in result.sweep:
        lines.append(
            f"| {entry['keys']:,} | {entry['ops']} | "
            f"{entry['ops_per_sec']:,.0f} | {entry['mean_ms']:.3f} | "
            f"{entry['p99_ms']:.3f} |"
        )
    lines.append("")
    lines += _sweep_repeat_lines(
        lambda entry: f"{entry['keys']:,} keys", result.sweep)
    lines += _sweep_per_op_histogram_lines(
        lambda entry: f"{entry['keys']:,} keys", result.sweep)
    return lines


def _spread_pct_text(entry):
    """Spread as a percentage of the median, or "n/a".

    ``spread_pct`` is null when the median is 0 — a percentage of
    nothing is meaningless — so the table shows n/a instead of a
    division-by-zero number.
    """
    pct = entry["spread_pct"]
    return f"{pct:.1f}%" if pct is not None else "n/a"


def _repeat_spread_lines(result):
    """Run-to-run spread table for a repeated result, or [].

    One row per figure — ops/sec and p99 overall, then ops/sec and
    p99 per operation that ran — with min, median, max, spread
    (max - min) and spread as a percentage of the median, so a run
    that swings wildly reads at a glance. Empty for a single run:
    one run has no spread to report.
    """
    spread = result.repeat_spread
    if not spread:
        return []
    lines = [
        f"### Repeat-run spread — {result.name}",
        "",
        "How much the runs vary: min, median, max, spread "
        "(max - min) and spread as a percentage of the median, for "
        "ops/sec and p99 overall and per operation — a wide spread "
        "says the runs were noisy, a narrow one says the median is "
        "trustworthy.",
        "",
        "| Metric | Min | Median | Max | Spread | Spread % |",
        "| --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    overall = spread["overall"]
    ops = overall["ops_per_sec"]
    lines.append(
        f"| ops/sec | {ops['min']:,.0f} | {ops['median']:,.0f} | "
        f"{ops['max']:,.0f} | {ops['spread']:,.0f} | "
        f"{_spread_pct_text(ops)} |"
    )
    p99 = overall["p99_ms"]
    lines.append(
        f"| p99 (ms) | {p99['min']:.3f} | {p99['median']:.3f} | "
        f"{p99['max']:.3f} | {p99['spread']:.3f} | "
        f"{_spread_pct_text(p99)} |"
    )
    for op, entry in spread["per_op"].items():
        ops = entry["ops_per_sec"]
        lines.append(
            f"| {op} ops/sec | {ops['min']:,.0f} | "
            f"{ops['median']:,.0f} | {ops['max']:,.0f} | "
            f"{ops['spread']:,.0f} | {_spread_pct_text(ops)} |"
        )
        p99 = entry["p99_ms"]
        lines.append(
            f"| {op} p99 (ms) | {p99['min']:.3f} | "
            f"{p99['median']:.3f} | {p99['max']:.3f} | "
            f"{p99['spread']:.3f} | {_spread_pct_text(p99)} |"
        )
    lines.append("")
    return lines


def _repeat_lines(result):
    """Per-run table for a repeated result, or [] for a single run.

    One row per run with its ops/sec and p99, so the spread behind
    the reported medians is visible; the summary table above
    already shows the medians themselves. The table is followed by
    the run-to-run spread summary (min, median, max, spread and
    spread as a percentage of the median, overall and per
    operation), and then by a per-operation latency histogram per
    run — the same shared helper the sweep sections use, so a
    run where one op type slows down names the op. Runs recorded
    before 0.34.0 carry no ``histograms_by_op`` and render the
    table only.
    """
    if not result.repeat_runs:
        return []
    lines = [
        f"### Repeat runs — {result.name}",
        "",
        f"Median of {len(result.repeat_runs)} runs "
        f"({result.median_ops_per_sec:,.0f} ops/sec, "
        f"p99 {result.median_p99_ms:.3f} ms).",
        "",
        "| Run | Ops | Ops/sec | Mean (ms) | p50 (ms) | p95 (ms) | "
        "p99 (ms) | Errors |",
        "| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for entry in result.repeat_runs:
        lines.append(
            f"| {entry['run']} | {entry['ops']} | "
            f"{entry['ops_per_sec']:,.0f} | {entry['mean_ms']:.3f} | "
            f"{entry['p50_ms']:.3f} | {entry['p95_ms']:.3f} | "
            f"{entry['p99_ms']:.3f} | {entry['errors']} |"
        )
    lines.append("")
    lines += _repeat_spread_lines(result)
    lines += _sweep_per_op_histogram_lines(
        lambda entry: f"Run {entry['run']}", result.repeat_runs)
    return lines


def _client_sweep_lines(result):
    """Client-count sweep table for one result, or [] when no sweep ran.

    The table is followed by a per-operation latency histogram
    per swept count, so a count where one op type slows down shows
    which op it is, not just that the p99 moved. When a step ran
    ``--repeat`` more than once its table row shows the medians
    over the runs, and a per-step repeat block shows the runs
    behind them.
    """
    if not result.client_sweep:
        return []
    lines = [
        f"### Client sweep — {result.name}",
        "",
        "| Clients | Ops | Ops/sec | Mean (ms) | p99 (ms) |",
        "| ---: | ---: | ---: | ---: | ---: |",
    ]
    for entry in result.client_sweep:
        lines.append(
            f"| {entry['clients']} | {entry['ops']} | "
            f"{entry['ops_per_sec']:,.0f} | {entry['mean_ms']:.3f} | "
            f"{entry['p99_ms']:.3f} |"
        )
    lines.append("")
    lines += _sweep_repeat_lines(
        lambda entry: f"{entry['clients']} clients",
        result.client_sweep)
    lines += _sweep_per_op_histogram_lines(
        lambda entry: f"{entry['clients']} clients",
        result.client_sweep)
    return lines


def _value_size_sweep_lines(result):
    """Value-size sweep table for one result, or [] when no sweep ran.

    The table is followed by a per-operation latency histogram
    per swept size, so a size where one op type slows down shows
    which op it is, not just that the p99 moved. When a step ran
    ``--repeat`` more than once its table row shows the medians
    over the runs, and a per-step repeat block shows the runs
    behind them.
    """
    if not result.value_sweep:
        return []
    lines = [
        f"### Value size sweep — {result.name}",
        "",
        "| Value size (bytes) | Ops | Ops/sec | Mean (ms) | p99 (ms) |",
        "| ---: | ---: | ---: | ---: | ---: |",
    ]
    for entry in result.value_sweep:
        lines.append(
            f"| {entry['value_size']:,} | {entry['ops']} | "
            f"{entry['ops_per_sec']:,.0f} | {entry['mean_ms']:.3f} | "
            f"{entry['p99_ms']:.3f} |"
        )
    lines.append("")
    lines += _sweep_repeat_lines(
        lambda entry: f"{entry['value_size']:,} byte values",
        result.value_sweep)
    lines += _sweep_per_op_histogram_lines(
        lambda entry: f"{entry['value_size']:,} byte values",
        result.value_sweep)
    return lines


def _pipeline_sweep_lines(result):
    """Pipeline-depth sweep table for one result, or [] when no sweep ran.

    The table is followed by a per-operation latency histogram
    per swept depth, so a depth where one op type slows down shows
    which op it is, not just that the p99 moved. When a step ran
    ``--repeat`` more than once its table row shows the medians
    over the runs, and a per-step repeat block shows the runs
    behind them.
    """
    if not result.pipeline_sweep:
        return []
    lines = [
        f"### Pipeline sweep — {result.name}",
        "",
        "| Pipeline depth | Ops | Ops/sec | Mean (ms) | p99 (ms) |",
        "| ---: | ---: | ---: | ---: | ---: |",
    ]
    for entry in result.pipeline_sweep:
        lines.append(
            f"| {entry['pipeline']} | {entry['ops']} | "
            f"{entry['ops_per_sec']:,.0f} | {entry['mean_ms']:.3f} | "
            f"{entry['p99_ms']:.3f} |"
        )
    lines.append("")
    lines += _sweep_repeat_lines(
        lambda entry: f"{entry['pipeline']} commands",
        result.pipeline_sweep)
    lines += _sweep_per_op_histogram_lines(
        lambda entry: f"{entry['pipeline']} commands",
        result.pipeline_sweep)
    return lines


def _mix_sweep_lines(result):
    """Read:write mix sweep table for one result, or [] when no sweep ran.

    The table is followed by a per-operation latency histogram
    per swept ratio, so a ratio where one op type slows down shows
    which op it is, not just that the p99 moved. When a step ran
    ``--repeat`` more than once its table row shows the medians
    over the runs, and a per-step repeat block shows the runs
    behind them.
    """
    if not result.mix_sweep:
        return []
    lines = [
        f"### Mix sweep — {result.name}",
        "",
        "| Mix (read:write) | Ops | Ops/sec | Mean (ms) | p99 (ms) |",
        "| ---: | ---: | ---: | ---: | ---: |",
    ]
    for entry in result.mix_sweep:
        lines.append(
            f"| {entry['mix']} | {entry['ops']} | "
            f"{entry['ops_per_sec']:,.0f} | {entry['mean_ms']:.3f} | "
            f"{entry['p99_ms']:.3f} |"
        )
    lines.append("")
    lines += _sweep_repeat_lines(
        lambda entry: f"mix {entry['mix']}",
        result.mix_sweep)
    lines += _sweep_per_op_histogram_lines(
        lambda entry: f"mix {entry['mix']}",
        result.mix_sweep)
    return lines


def _time_series_lines(result):
    """Per-second ops/latency table for one result, or [] when empty."""
    series = result.time_series
    if not series:
        return []
    lines = [
        f"### Latency over time — {result.name}",
        "",
        "| Second | Ops | Mean (ms) | Max (ms) |",
        "| ---: | ---: | ---: | ---: |",
    ]
    for bucket in series:
        lines.append(
            f"| {bucket['second']} | {bucket['ops']} | "
            f"{bucket['mean_ms']:.3f} | {bucket['max_ms']:.3f} |"
        )
    lines.append("")
    return lines


def _best_worst_op_lines(name, best_worst):
    """Best/worst op section for one compared backend, or [].

    ``best_worst`` is the ``best_worst`` dict
    :func:`kvbench.compare.compare_results` puts on each entry —
    the op whose ops/sec rose most and the op whose p99 fell
    most, and the other end of each — so the Markdown renders
    from the same picks the JSON report carries, and the two
    views cannot disagree. Each pick also carries its op's
    latency histogram from both runs, so the section draws the
    now and prev shapes for the op that moved most, right under
    the table. A comparison built before 0.59.0 carries picks
    with no histograms and renders only the table. A pick
    with no delta for a measure shows n/a for that measure.
    The picked ops' per-op histogram comparison table — each
    op's p50/p99 in both runs, the p99 change, and the bucket
    drift — renders under the histogram blocks, from the same
    ``histogram_comparison`` rows the JSON report carries, drawn
    by the shared :func:`_histogram_comparison_table_lines`
    helper the trend and best-vs-worst views use too, so the
    shape move reads as numbers as well as bars and the views
    cannot disagree. A comparison built before 0.60.0 carries
    no ``histogram_comparison`` and renders no table; when the
    baseline carries no per-op histograms the table's notes
    name the reason instead.
    """
    if not best_worst:
        return []
    lines = [
        f"#### {name} — best/worst op vs previous run",
        "",
    ]
    best_ops = best_worst.get("best_by_ops_sec")
    worst_ops = best_worst.get("worst_by_ops_sec")
    best_p99 = best_worst.get("best_by_p99")
    worst_p99 = best_worst.get("worst_by_p99")
    if (best_ops is not None or worst_ops is not None
            or best_p99 is not None or worst_p99 is not None):
        def _cell(pick, key):
            if pick is None:
                return "n/a"
            delta = pick.get(key)
            if delta is None:
                return f"{pick['op']} (n/a)"
            return f"{pick['op']} ({delta:+.1f}%)"

        lines += [
            "| | Best op | Worst op |",
            "| --- | --- | --- |",
            f"| Ops/sec Δ | {_cell(best_ops, 'ops_sec_delta_pct')} | "
            f"{_cell(worst_ops, 'ops_sec_delta_pct')} |",
            f"| p99 Δ | {_cell(best_p99, 'p99_delta_pct')} | "
            f"{_cell(worst_p99, 'p99_delta_pct')} |",
            "",
        ]
    for note in best_worst.get("notes") or []:
        lines.append(f"- {note}")
    if best_worst.get("notes"):
        lines.append("")
    # Each best/worst op's latency histogram from both runs, so
    # the op that moved most also shows how its latency shape
    # moved — rendered from the same pick dicts the JSON report
    # carries, so the two views cannot disagree. The same op
    # can be best on one measure and worst on another, so each
    # op renders once, in pick order. A baseline that carries no
    # per-op histograms renders only this run's shape; the
    # comparison section below already notes why the baseline's
    # are missing.
    seen = set()
    for key in ("best_by_ops_sec", "worst_by_ops_sec",
                "best_by_p99", "worst_by_p99"):
        pick = best_worst.get(key)
        if not pick or pick["op"] in seen:
            continue
        seen.add(pick["op"])
        if pick.get("histogram"):
            lines += _per_op_histogram_block(
                f"##### {name} — {pick['op']} latency histogram "
                "(now)",
                {pick["op"]: pick["histogram"]})
        if pick.get("baseline_histogram"):
            lines += _per_op_histogram_block(
                f"##### {name} — {pick['op']} latency histogram "
                "(prev)",
                {pick["op"]: pick["baseline_histogram"]})
    # The picked ops' per-op histogram comparison table, so the
    # shape move reads as numbers as well as bars — rendered
    # from the same shared rows the JSON report carries, so the
    # two views cannot disagree. A best_worst dict built before
    # 0.60.0 carries no histogram_comparison and renders no
    # table; when the baseline carries no per-op histograms the
    # notes name the reason instead of a table.
    histogram_comparison = best_worst.get("histogram_comparison")
    if histogram_comparison:
        rows = histogram_comparison.get("rows")
        if rows:
            lines += _histogram_comparison_table_lines(
                f"##### {name} — best/worst op histogram comparison",
                "prev", "now", rows)
        for note in histogram_comparison.get("notes") or []:
            lines.append(f"- {note}")
        if histogram_comparison.get("notes"):
            lines.append("")
    return lines


def _comparison_lines(comparison):
    """Side-by-side comparison section, or [] when no comparison.

    One row per backend matched by name, with the current and
    baseline ops/sec and p99 and the percentage change of each,
    plus a verdict line per backend. The table is followed by a
    per-operation p99 / ops-sec delta table per compared backend
    — one row per op that ran — and a best/worst op section per
    compared backend — the op whose ops/sec rose most and the
    op whose p99 fell most, and the other end of each, with a
    note when a comparison has a single op or identical runs,
    and each best/worst op's latency histogram from both runs,
    and a per-op histogram comparison table for the picked ops
    (each op's p50/p99 in both runs, the p99 change, and the
    bucket drift) — all built from the same dicts the JSON
    report carries, so the views cannot disagree — and then by
    a per-operation
    latency histogram per compared backend — the
    current run's and the baseline run's, drawn by the same
    shared helper as the single-run per-op histograms, so the
    comparison cannot disagree with them — and a note where the
    baseline run carries no per-op histograms to draw. Backends
    present in only one of the two runs are named rather than
    silently dropped, and a workload that differs from the
    baseline run's is flagged: the numbers still print, but the
    comparison is approximate.
    """
    if not comparison:
        return []
    lines = [
        "## Comparison with previous run",
        "",
        f"Baseline: `{comparison['file']}`",
    ]
    if comparison.get("baseline_generated"):
        lines.append(
            f"Baseline generated: {comparison['baseline_generated']}")
    if not comparison["workload_matches"]:
        lines.append(
            "Warning: the workload differs from the baseline run "
            f"({', '.join(comparison['workload_differences'])}), so "
            "this comparison is approximate — re-run with the same "
            "workload for an apples-to-apples comparison."
        )
    lines.append("")
    if comparison["entries"]:
        lines += [
            "| Backend | Ops/sec now | Ops/sec prev | Ops/sec Δ | "
            "p99 now (ms) | p99 prev (ms) | p99 Δ | Verdict |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
        ]
        for entry in comparison["entries"]:
            ops_delta = entry["ops_sec_delta_pct"]
            p99_delta = entry["p99_delta_pct"]
            ops_delta_text = (f"{ops_delta:+.1f}%"
                              if ops_delta is not None else "n/a")
            p99_delta_text = (f"{p99_delta:+.1f}%"
                              if p99_delta is not None else "n/a")
            lines.append(
                f"| {entry['name']} | "
                f"{entry['ops_per_sec']:,.0f} | "
                f"{entry['baseline_ops_per_sec']:,.0f} | "
                f"{ops_delta_text} | "
                f"{entry['p99_ms']:.3f} | "
                f"{entry['baseline_p99_ms']:.3f} | "
                f"{p99_delta_text} | {entry['verdict']} |"
            )
        lines.append("")
        # Per-operation p99 / ops-sec deltas per compared backend:
        # one row per op that ran, with the current and baseline
        # ops/sec and p99 and the percentage change of each, so a
        # p99 move names the op that moved. A baseline that
        # carries no per-op stats — a history baseline from a
        # record before 0.22.0, or a report written before per-op
        # stats existed — still shows this run's numbers with
        # n/a deltas, and a note saying why the baseline's are
        # missing.
        for entry in comparison["entries"]:
            per_op = entry.get("per_op")
            if not per_op:
                continue
            lines += [
                f"#### {entry['name']} — per-op p99 / ops-sec deltas",
                "",
                "| Op | Ops/sec now | Ops/sec prev | Ops/sec Δ | "
                "p99 now (ms) | p99 prev (ms) | p99 Δ |",
                "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
            ]
            for row in per_op:
                ops_delta = row["ops_sec_delta_pct"]
                p99_delta = row["p99_delta_pct"]
                ops_delta_text = (f"{ops_delta:+.1f}%"
                                  if ops_delta is not None else "n/a")
                p99_delta_text = (f"{p99_delta:+.1f}%"
                                  if p99_delta is not None else "n/a")
                baseline_ops = (f"{row['baseline_ops_per_sec']:,.0f}"
                                if row["baseline_ops_per_sec"] is not None
                                else "n/a")
                baseline_p99 = (f"{row['baseline_p99_ms']:.3f}"
                                if row["baseline_p99_ms"] is not None
                                else "n/a")
                lines.append(
                    f"| {row['op']} | {row['ops_per_sec']:,.0f} | "
                    f"{baseline_ops} | {ops_delta_text} | "
                    f"{row['p99_ms']:.3f} | {baseline_p99} | "
                    f"{p99_delta_text} |"
                )
            lines.append("")
            if all(row["baseline_ops_per_sec"] is None
                   for row in per_op):
                if comparison.get("source") == "history":
                    reason = ("the history file carries no "
                              "per-operation stats")
                else:
                    reason = ("the baseline report carries no "
                              "per-operation stats")
                lines.append(
                    f"- {entry['name']}: {reason}, so the per-op "
                    "deltas are n/a."
                )
                lines.append("")
            # Best/worst op vs the baseline: the op whose ops/sec
            # rose most and the op whose p99 fell most, and the
            # other end of each — rendered from the entry's own
            # ``best_worst`` dict, the same one the JSON report
            # carries, so the two views cannot disagree.
            lines += _best_worst_op_lines(
                entry["name"], entry.get("best_worst"))
        # Per-operation latency histograms per compared backend:
        # the current run's, and the baseline run's when it carries
        # them — a history baseline or a report written before
        # 0.32.0 carries none, so only the current run's render,
        # with a note saying why the baseline's are missing.
        for entry in comparison["entries"]:
            current = entry.get("histograms_by_op")
            baseline_hists = entry.get("baseline_histograms_by_op")
            lines += _per_op_histogram_block(
                f"#### {entry['name']} — per-op latency histogram "
                "(now)",
                current)
            if baseline_hists:
                lines += _per_op_histogram_block(
                    f"#### {entry['name']} — per-op latency histogram "
                    "(prev)",
                    baseline_hists)
            elif current:
                if comparison.get("source") == "history":
                    reason = ("the history file carries no per-op "
                              "histograms")
                else:
                    reason = ("the baseline report carries no "
                              "per-op histograms")
                lines.append(
                    f"- {entry['name']}: {reason}, so only the "
                    "current run's histograms are shown."
                )
                lines.append("")
    for name in comparison["unmatched_baseline"]:
        lines.append(
            f"- `{name}` is in the baseline run but not in this run, "
            "so it has no comparison."
        )
    for name in comparison["unmatched_current"]:
        lines.append(
            f"- `{name}` is in this run but not in the baseline run, "
            "so it has no comparison."
        )
    if comparison["unmatched_baseline"] or \
            comparison["unmatched_current"]:
        lines.append("")
    if comparison["threshold_pct"] is not None:
        if comparison["regressions"]:
            lines.append(
                f"Regression gate: {comparison['regressions']} backend(s) "
                "regressed beyond the "
                f"{comparison['threshold_pct']:g}% threshold — "
                "ops/sec down, or p99 up, by more than that."
            )
        else:
            lines.append(
                "Regression gate: no backend regressed beyond the "
                f"{comparison['threshold_pct']:g}% threshold."
            )
        lines.append("")
    return lines


def _slo_lines(slo):
    """SLO gate section, or [] when no gate was set.

    One row per backend with the measured p99 and error rate next
    to the thresholds that were set, plus a pass/fail verdict. A
    threshold that was not set shows as a dash — the gate only
    applies to the thresholds that were given.
    """
    if not slo:
        return []
    thresholds = []
    if slo["p99_threshold_ms"] is not None:
        thresholds.append(
            f"p99 at most {slo['p99_threshold_ms']:g} ms")
    if slo["error_rate_threshold_pct"] is not None:
        thresholds.append(
            "error rate at most "
            f"{slo['error_rate_threshold_pct']:g}%")
    if slo.get("per_op_thresholds"):
        for op, op_thresholds in slo["per_op_thresholds"].items():
            op_parts = []
            if op_thresholds["p99_ms"] is not None:
                op_parts.append(
                    f"p99 at most {op_thresholds['p99_ms']:g} ms")
            if op_thresholds["error_rate_pct"] is not None:
                op_parts.append(
                    "error rate at most "
                    f"{op_thresholds['error_rate_pct']:g}%")
            thresholds.append(f"{op}: " + "; ".join(op_parts))
    lines = [
        "## SLO gate",
        "",
        "Thresholds: " + "; ".join(thresholds) + ".",
        "",
    ]
    if slo.get("preset"):
        # Name the file the thresholds came from; CLI flags may have
        # overridden individual preset values, but the file is where
        # the run's budgets started.
        lines.append(
            f"Thresholds loaded from preset file `{slo['preset']}`; "
            "CLI flags override preset values."
        )
        lines.append("")
    lines += [
        "| Backend | p99 (ms) | p99 limit (ms) | Error rate | "
        "Error limit | Verdict |",
        "| --- | ---: | ---: | ---: | ---: | --- |",
    ]
    for entry in slo["entries"]:
        p99_limit = (
            f"{entry['p99_threshold_ms']:g}"
            if entry["p99_threshold_ms"] is not None else "—")
        err_limit = (
            f"{entry['error_rate_threshold_pct']:g}%"
            if entry["error_rate_threshold_pct"] is not None else "—")
        lines.append(
            f"| {entry['name']} | {entry['p99_ms']:.3f} | "
            f"{p99_limit} | {entry['error_rate_pct']:.2f}% | "
            f"{err_limit} | {entry['verdict']} |"
        )
    lines.append("")
    # Per-operation verdicts: one row per op that ran and was gated,
    # so a slow SET shows up even when the overall p99 is fine.
    per_op_rows = []
    for entry in slo["entries"]:
        for per_op in entry.get("per_op", []):
            p99_limit = (
                f"{per_op['p99_threshold_ms']:g}"
                if per_op["p99_threshold_ms"] is not None else "—")
            err_limit = (
                f"{per_op['error_rate_threshold_pct']:g}%"
                if per_op["error_rate_threshold_pct"] is not None
                else "—")
            per_op_rows.append(
                f"| {entry['name']} | {per_op['op']} | "
                f"{per_op['ops']} | {per_op['p99_ms']:.3f} | "
                f"{p99_limit} | {per_op['error_rate_pct']:.2f}% | "
                f"{err_limit} | {per_op['verdict']} |"
            )
    if per_op_rows:
        lines += [
            "| Backend | Op | Ops | p99 (ms) | p99 limit (ms) | "
            "Error rate | Error limit | Verdict |",
            "| --- | --- | ---: | ---: | ---: | ---: | ---: | --- |",
        ] + per_op_rows + [""]
    if slo["breaches"]:
        lines.append(
            f"SLO gate: FAILED — {slo['breaches']} backend(s) "
            "breached a threshold (exit code 4)."
        )
    else:
        lines.append(
            "SLO gate: passed — no backend breached a threshold."
        )
    lines.append("")
    return lines


def render_markdown(results, workload, command_line, comparison=None,
                    slo=None):
    """Render one or more BenchmarkResults as a Markdown report."""
    if workload.duration_s is not None:
        scope_line = (
            f"- Duration: {format_duration(workload.duration_s)} "
            "(fixed wall-clock run; total ops vary)"
        )
    else:
        scope_line = f"- Total ops: {workload.ops}"
    if workload.distribution == "zipfian":
        sampler = ZipfianSampler(workload.keys, workload.zipf_skew)
        distribution_line = (
            f"- Key distribution: zipfian (skew {workload.zipf_skew:g}; "
            f"the hottest 1% of keys take ~"
            f"{sampler.hot_share() * 100:.0f}% of ops)"
        )
    else:
        distribution_line = "- Key distribution: uniform"
    if workload.warmup_s > 0:
        warmup_line = (
            f"- Warmup: {format_duration(workload.warmup_s)} "
            "(same workload, before measurement; warmup ops and "
            "latencies are excluded from the results)"
        )
    else:
        warmup_line = None
    if workload.sweep_keyspace:
        sweep_line = (
            "- Keyspace sweep: "
            + ", ".join(f"{s:,}" for s in workload.sweep_keyspace)
            + " keys (the same workload re-run "
            + (f"{workload.repeat} times"
               if workload.repeat > 1 else "once")
            + " per size; per-size ops/sec and p99 are "
            + ("the medians over the runs and are "
               if workload.repeat > 1 else "")
            + "in the Keyspace sweep section below)"
        )
    else:
        sweep_line = None
    if workload.sweep_clients:
        client_sweep_line = (
            "- Client sweep: "
            + ", ".join(str(c) for c in workload.sweep_clients)
            + " clients (the same workload re-run "
            + (f"{workload.repeat} times"
               if workload.repeat > 1 else "once")
            + " per client count; per-count ops/sec and p99 are "
            + ("the medians over the runs and are "
               if workload.repeat > 1 else "")
            + "in the Client sweep section below)"
        )
    else:
        client_sweep_line = None
    if workload.sweep_value_size:
        value_sweep_line = (
            "- Value size sweep: "
            + ", ".join(f"{s:,}" for s in workload.sweep_value_size)
            + " bytes (the same workload re-run "
            + (f"{workload.repeat} times"
               if workload.repeat > 1 else "once")
            + " per value size; per-size ops/sec and p99 are "
            + ("the medians over the runs and are "
               if workload.repeat > 1 else "")
            + "in the Value size sweep section below)"
        )
    else:
        value_sweep_line = None
    if workload.sweep_pipeline:
        pipeline_sweep_line = (
            "- Pipeline sweep: "
            + ", ".join(str(d) for d in workload.sweep_pipeline)
            + " commands per round trip (the same workload re-run "
            + (f"{workload.repeat} times"
               if workload.repeat > 1 else "once")
            + " per pipeline depth; per-depth ops/sec and p99 are "
            + ("the medians over the runs and are "
               if workload.repeat > 1 else "")
            + "in the Pipeline sweep section below)"
        )
    else:
        pipeline_sweep_line = None
    if workload.sweep_mix:
        mix_sweep_line = (
            "- Mix sweep: "
            + ", ".join(f"{read}:{write}"
                        for read, write in workload.sweep_mix)
            + " read:write (the same workload re-run "
            + (f"{workload.repeat} times"
               if workload.repeat > 1 else "once")
            + " per read:write ratio; per-ratio ops/sec and p99 are "
            + ("the medians over the runs and are "
               if workload.repeat > 1 else "")
            + "in the Mix sweep section below)"
        )
    else:
        mix_sweep_line = None
    if workload.repeat > 1:
        repeat_line = (
            f"- Repeat: {workload.repeat} runs (the same workload "
            "run that many times; the reported ops/sec and p99 are "
            "the medians over the runs, per operation as well as "
            "overall, and the per-run values are in the Repeat runs "
            "section below)"
        )
    else:
        repeat_line = None
    if workload.auth:
        auth_line = (
            "- Authentication: enabled (the client sent AUTH on "
            "connect; the password is never recorded in this report)"
        )
    else:
        auth_line = None
    if workload.workload_file is not None:
        workload_file_line = (
            f"- Workload file: `{workload.workload_file}` (the "
            "values below are the resolved config — re-run with "
            "the same file to reproduce this workload)"
        )
    else:
        workload_file_line = None
    lines = [
        "# kvbench report",
        "",
        f"Generated: {datetime.now(timezone.utc).isoformat()}",
        f"Python: {sys.version.split()[0]} on {platform.system()} "
        f"{platform.machine()} ({platform.python_implementation()})",
        f"Command: `{command_line}`",
        "",
        "## Workload",
        "",
        scope_line,
        f"- Clients (threads): {workload.clients}",
        f"- Keyspace: {workload.keys} keys",
        f"- Key prefix: {workload.key_prefix}",
        distribution_line,
        f"- Value size: {workload.value_size} bytes",
        f"- Mix: SET {workload.set_weight} / GET {workload.get_weight} / "
        f"INCR {workload.incr_weight} / DEL {workload.del_weight}"
        + (f" ({workload.mix_label})" if workload.mix_label else ""),
        f"- Pipeline depth: {workload.pipeline} command(s) per round trip",
        f"- Socket timeout: {workload.timeout_s:g} s",
    ]
    if warmup_line is not None:
        lines.append(warmup_line)
    if sweep_line is not None:
        lines.append(sweep_line)
    if client_sweep_line is not None:
        lines.append(client_sweep_line)
    if value_sweep_line is not None:
        lines.append(value_sweep_line)
    if pipeline_sweep_line is not None:
        lines.append(pipeline_sweep_line)
    if mix_sweep_line is not None:
        lines.append(mix_sweep_line)
    if repeat_line is not None:
        lines.append(repeat_line)
    if workload_file_line is not None:
        lines.append(workload_file_line)
    if auth_line is not None:
        lines.append(auth_line)
    lines += [
        "",
        "## Results",
        "",
        "| Backend | Ops | Ops/sec | Mean (ms) | p50 (ms) | p95 (ms) | "
        "p99 (ms) | Errors |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for result in results:
        lines.append(_row(result))
    comparison_lines = _comparison_lines(comparison)
    if comparison_lines:
        lines += [""] + comparison_lines
    slo_lines = _slo_lines(slo)
    if slo_lines:
        lines += [""] + slo_lines
    sweep_lines = []
    for result in results:
        sweep_lines += _sweep_lines(result)
    if sweep_lines:
        lines += ["", "## Keyspace sweep", ""] + sweep_lines
    client_sweep_lines = []
    for result in results:
        client_sweep_lines += _client_sweep_lines(result)
    if client_sweep_lines:
        lines += ["", "## Client sweep", ""] + client_sweep_lines
    value_sweep_lines = []
    for result in results:
        value_sweep_lines += _value_size_sweep_lines(result)
    if value_sweep_lines:
        lines += ["", "## Value size sweep", ""] + value_sweep_lines
    pipeline_sweep_lines = []
    for result in results:
        pipeline_sweep_lines += _pipeline_sweep_lines(result)
    if pipeline_sweep_lines:
        lines += ["", "## Pipeline sweep", ""] + pipeline_sweep_lines
    mix_sweep_lines = []
    for result in results:
        mix_sweep_lines += _mix_sweep_lines(result)
    if mix_sweep_lines:
        lines += ["", "## Mix sweep", ""] + mix_sweep_lines
    repeat_lines = []
    for result in results:
        repeat_lines += _repeat_lines(result)
    if repeat_lines:
        lines += ["", "## Repeat runs", ""] + repeat_lines
    per_op_lines = []
    for result in results:
        per_op_lines += _per_op_lines(result)
    if per_op_lines:
        lines += ["", "## Per-operation stats", ""] + per_op_lines
    tail_lines = []
    for result in results:
        tail_lines += _tail_lines(result)
    if tail_lines:
        lines += ["", "## Tail-latency attribution", ""] + tail_lines
    hot_key_lines = []
    for result in results:
        hot_key_lines += _hot_key_lines(result, workload.hot_keys)
    if hot_key_lines:
        lines += ["", "## Per-key latency (hottest keys)", ""] + \
            hot_key_lines
    hist_lines = []
    for result in results:
        hist_lines += _histogram_lines(result)
    if hist_lines:
        lines += ["", "## Latency histograms", ""] + hist_lines
    per_op_hist_lines = []
    for result in results:
        per_op_hist_lines += _per_op_histogram_lines(result)
    if per_op_hist_lines:
        lines += ["", "## Per-operation latency histograms", ""] + \
            per_op_hist_lines
    ts_lines = []
    for result in results:
        ts_lines += _time_series_lines(result)
    if ts_lines:
        lines += ["", "## Latency over time", ""] + ts_lines
    lines += [
        "## Notes",
        "",
        "- Numbers are machine-dependent. Reproduce them with the command "
        "above on the same machine and server.",
        "- The `baseline (dict)` backend is an in-process Python dict: no "
        "network, no persistence, no expiry. It is an upper bound on what "
        "this workload can do on this machine, not a fair head-to-head "
        "with a real server.",
        "- Latencies are measured per operation around the client call, "
        "so they include network round-trip time for server backends.",
        "- The Latency over time section buckets ops by the second they "
        "completed; a second with 0 ops marks a stall.",
        "- The Per-operation stats section splits ops/sec and latency "
        "percentiles by operation (SET, GET, INCR, DEL), so a read-heavy "
        "mix shows whether reads or writes dominate the tail.",
        "- The Per-operation latency histograms section draws the same "
        "log-spaced buckets per operation, so a slow op type's latency "
        "shape shows separately from the overall distribution instead "
        "of being swamped by the fast ops. The JSON report carries the "
        "same buckets per result under `histograms_by_op`.",
        "- The Tail-latency attribution section names the factor the "
        "slowest ops come from: one operation type, one client, hot "
        "or cold keys, or a single slow second. Slow ops are the ops "
        "at or above the overall p99 latency; a factor only counts "
        "as dominant when slow ops land on it far more often than "
        "its share of all ops would predict. The JSON report carries "
        "the same breakdown per result under `tail`.",
    ]
    if workload.hot_keys > 0 and hot_key_lines:
        lines.append(
            "- The Per-key latency section shows count, mean, p50, p95 "
            "and p99 for the hottest keys by access count (top "
            f"{workload.hot_keys} by default, --hot-keys to change it): "
            "under a zipfian distribution these are the keys the skew "
            "concentrates on, so a hot key slower than the overall p99 "
            "points at contention on that key rather than the server as "
            "a whole. Counters report under their own key "
            "(`kvbench:counter:<rank>`)."
        )
    if workload.duration_s is not None:
        lines.append(
            "- In duration mode the run stops after the requested "
            "wall-clock time, so the total op count varies from run to "
            "run; ops/sec is total ops divided by the actual elapsed time."
        )
    if workload.pipeline > 1:
        lines.append(
            "- With a pipeline depth above 1, latencies are the batch "
            "round-trip time divided by the batch size: amortized per-op "
            "cost, not the latency of any single reply."
        )
    if workload.distribution == "zipfian":
        for result in results:
            lines.append(
                f"- Hot-set hit rate on {result.name}: "
                f"{result.hot_hit_rate * 100:.1f}% of ops hit the "
                "hottest 1% of keys."
            )
    if workload.warmup_s > 0:
        lines.append(
            "- Before measurement each backend ran the same workload "
            f"for {format_duration(workload.warmup_s)} as a warmup, so "
            "caches and connections were warm; warmup ops and their "
            "latencies are excluded from the results, and the elapsed "
            "time covers the measured phase only."
        )
        for result in results:
            lines.append(
                f"- Warmup ops on {result.name}: {result.warmup_ops} "
                "(excluded from results)."
            )
    if workload.sweep_keyspace:
        lines.append(
            "- The Keyspace sweep section re-runs the same workload "
            "once per keyspace size, so ops/sec and p99 per size show "
            "how keyspace size affects throughput; the main results "
            "table above uses the base keyspace size. Each swept size "
            "also gets its own per-operation latency histograms, so a "
            "size where one op type slows down names the op. The JSON "
            "report carries the same buckets per sweep step under "
            "`keyspace_sweep[].histograms_by_op`."
        )
    if workload.sweep_clients:
        lines.append(
            "- The Client sweep section re-runs the same workload "
            "once per client count, so ops/sec and p99 per count show "
            "how concurrency affects throughput — where adding "
            "clients stops helping and the server saturates; the main "
            "results table above uses the base client count. Each "
            "swept count also gets its own per-operation latency "
            "histograms, so a count where one op type slows down "
            "names the op. The JSON report carries the same buckets "
            "per sweep step under `client_sweep[].histograms_by_op`."
        )
    if workload.sweep_value_size:
        lines.append(
            "- The Value size sweep section re-runs the same workload "
            "once per value size, so ops/sec and p99 per size show "
            "how value size affects throughput; the main results "
            "table above uses the base value size. Each swept size "
            "also gets its own per-operation latency histograms, so a "
            "size where one op type slows down names the op. The JSON "
            "report carries the same buckets per sweep step under "
            "`value_size_sweep[].histograms_by_op`."
        )
    if workload.sweep_pipeline:
        lines.append(
            "- The Pipeline sweep section re-runs the same workload "
            "once per pipeline depth, so ops/sec and p99 per depth "
            "show how pipelining affects throughput — where batching "
            "more commands per round trip stops helping; the main "
            "results table above uses the base pipeline depth. Each "
            "swept depth also gets its own per-operation latency "
            "histograms, so a depth where one op type slows down "
            "names the op. The JSON report carries the same buckets "
            "per sweep step under `pipeline_sweep[].histograms_by_op`."
        )
    if workload.sweep_mix:
        lines.append(
            "- The Mix sweep section re-runs the same workload once "
            "per read:write ratio, so ops/sec and p99 per ratio show "
            "how the read:write mix affects throughput — where a "
            "store's writes start costing more than its reads; the "
            "main results table above uses the base mix. Each swept "
            "ratio also gets its own per-operation latency "
            "histograms, so a ratio where one op type slows down "
            "names the op. The JSON report carries the same buckets "
            "per sweep step under `mix_sweep[].histograms_by_op`."
        )
    if workload.repeat > 1:
        lines.append(
            "- With --repeat the workload ran "
            f"{workload.repeat} times and the reported ops/sec and "
            "p99 are the medians over the runs, per operation as "
            "well as overall; the mean, p50, p95, histogram, tail "
            "attribution and per-second series come from the run "
            "whose ops/sec is the median, and the Repeat runs "
            "section lists every run's ops/sec and p99, followed by "
            "a spread summary (min, median, max, spread and spread "
            "as a percentage of the median, overall and per "
            "operation) and each run's own per-operation latency "
            "histograms, so a run where one op type slows down "
            "names the op. The JSON report carries the same "
            "per-run values under `results[].repeat.runs`, with "
            "each run's buckets under `histograms_by_op` and the "
            "spread summary under `results[].repeat.spread`."
        )
    if comparison is not None:
        baseline_desc = (
            "a run recorded in the history file"
            if comparison.get("source") == "history"
            else "an earlier JSON report")
        lines.append(
            "- The Comparison with previous run section compares "
            f"this run against {baseline_desc} "
            f"(`{comparison['file']}`): ops/sec and p99 side by "
            "side per backend, with the percentage change and a "
            "verdict. Backends are matched by name; a backend in "
            "only one of the two runs has no comparison. Each "
            "compared backend also carries a per-operation p99 / "
            "ops-sec delta table — one row per op that ran, with "
            "the current and baseline ops/sec and p99 and the "
            "percentage change of each — under "
            "`comparison.entries[].per_op` in JSON, so a p99 move "
            "names the op that moved. Each compared backend also "
            "carries its per-operation "
            "latency histograms — the current run's, and the "
            "baseline run's when the baseline report carries them "
            "(JSON reports since 0.32.0 do; a history baseline "
            "carries none) — under "
            "`comparison.entries[].histograms_by_op` and "
            "`baseline_histograms_by_op` in JSON, and as blocks "
            "under the comparison table in Markdown, so a p99 move "
            "names the op whose latency shape moved."
        )
        if not comparison["workload_matches"]:
            lines.append(
                "- The baseline run used a different workload "
                f"({', '.join(comparison['workload_differences'])}), "
                "so the comparison is approximate."
            )
        if comparison["threshold_pct"] is not None:
            lines.append(
                "- The regression gate treats a backend as "
                "regressed when its ops/sec fell by more than "
                f"{comparison['threshold_pct']:g}%, or its p99 rose "
                f"by more than {comparison['threshold_pct']:g}%, "
                "versus the baseline run."
            )
    if slo is not None:
        # Name only the thresholds that were set: the gate applies
        # to whichever of the two was given, or both, plus any
        # per-operation thresholds.
        slo_thresholds = []
        if slo["p99_threshold_ms"] is not None:
            slo_thresholds.append(
                f"p99 at most {slo['p99_threshold_ms']:g} ms")
        if slo["error_rate_threshold_pct"] is not None:
            slo_thresholds.append(
                "error rate at most "
                f"{slo['error_rate_threshold_pct']:g}%")
        if slo.get("per_op_thresholds"):
            for op, op_thresholds in slo["per_op_thresholds"].items():
                op_parts = []
                if op_thresholds["p99_ms"] is not None:
                    op_parts.append(
                        f"p99 at most {op_thresholds['p99_ms']:g} ms")
                if op_thresholds["error_rate_pct"] is not None:
                    op_parts.append(
                        "error rate at most "
                        f"{op_thresholds['error_rate_pct']:g}%")
                slo_thresholds.append(
                    f"{op} " + " and ".join(op_parts))
        lines.append(
            "- The SLO gate section checks each backend against "
            + " and ".join(slo_thresholds)
            + ": a backend breaches when its measured value is "
            "above the threshold, and a breach fails the run with "
            "exit code 4 — after the reports are written, so the "
            "breach is visible in them. The JSON report carries "
            "the same gate under `slo`."
            + (f" The thresholds were loaded from preset file "
               f"`{slo['preset']}`; CLI flags override preset "
               "values." if slo.get("preset") else "")
        )
    for result in results:
        if result.error_sample:
            lines.append(
                f"- First error from {result.name}: {result.error_sample}"
            )
    lines.append("")
    return "\n".join(lines)


def write_report(results, workload, command_line, path, comparison=None,
                 slo=None):
    """Write the Markdown report to ``path`` and return its text."""
    text = render_markdown(results, workload, command_line, comparison,
                           slo)
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)
    return text
