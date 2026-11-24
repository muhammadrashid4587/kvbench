"""Workload runner.

Runs a configurable SET/GET/INCR/DEL mix against any backend that exposes a
``command(*args)`` method — a :class:`kvbench.resp.RespClient` talking to
a server, or the in-process :class:`DictClient` baseline. Each thread
gets its own client, so the runner works the same way for both.

The baseline is an in-process dict behind a lock. It has no network, no
persistence and no expiry, so its numbers are an upper bound on what the
workload can do on this machine — not a fair head-to-head with a real
server. The report says so.
"""

import random
import threading
import time
from dataclasses import dataclass, field, replace

from .distribution import ZipfianSampler
from .stats import attribute_tail, histogram, median, spread_summary, \
    summarize, time_series


def format_duration(seconds):
    """Format a duration in seconds as ``10s`` or ``2m``.

    Whole minutes render as minutes (``120`` -> ``2m``), whole
    seconds as seconds (``10`` -> ``10s``); anything else keeps its
    fractional seconds (``1.5`` -> ``1.5s``).
    """
    if seconds >= 60 and seconds % 60 == 0:
        return f"{int(seconds // 60)}m"
    if seconds == int(seconds):
        return f"{int(seconds)}s"
    return f"{seconds:g}s"


@dataclass
class Workload:
    ops: int = 10_000
    clients: int = 4
    keys: int = 1_000
    # Prefix for every benchmark key ("<prefix>:<rank>" for SET/GET,
    # "<prefix>:counter:<rank>" for INCR). "kvbench" is the old
    # behaviour; a different prefix lets runs against a shared store
    # avoid colliding with each other — and with other tools' keys —
    # while the reports record which prefix was used.
    key_prefix: str = "kvbench"
    value_size: int = 32
    set_weight: int = 50
    get_weight: int = 40
    incr_weight: int = 10
    # Weight of DEL in the mix: deleting a key it previously SET
    # (or one another thread SET — keys are shared, so a DEL may
    # land on a key that was never SET, or one already deleted;
    # either way the server replies without error and the op is
    # measured like any other). The default 5 keeps deletes a
    # small share of the workload, like a real cache workload
    # where evictions and explicit deletes are rare next to
    # reads and writes.
    del_weight: int = 5
    seed: int = 1234
    # Fixed wall-clock run length in seconds, or None for a fixed op
    # count. In duration mode ``ops`` is ignored (normalized to 0):
    # workers run until the deadline and the total op count is
    # whatever completed in that time.
    duration_s: float = None
    # Warmup period in seconds, run before measurement: workers run
    # the same workload against the backend (warming caches,
    # connections and JITs) and the warmup ops are discarded — they
    # are not counted in ``ops`` and their latencies never reach the
    # result. The measured phase is re-seeded after warmup, so the
    # measured workload is identical with or without a warmup.
    warmup_s: float = 0.0
    # Human-readable name of the mix, when it came from a named
    # preset (``read-heavy`` etc.). Empty for an explicit weight
    # string; the report shows the preset name next to the weights.
    mix_label: str = ""
    # Commands sent per round trip. 1 is the old sequential behaviour:
    # one command, wait for its reply, then the next. Above 1 the
    # runner sends ``pipeline`` commands at once and reads all their
    # replies, so the per-op latencies are the batch round-trip time
    # divided by the batch size — amortized per-op cost, not the
    # latency of any single reply.
    pipeline: int = 1
    # How keys are picked: "uniform" (every key equally likely, the
    # old behaviour) or "zipfian" (a skewed Zipfian distribution where
    # a small hot set takes most of the traffic, like a real workload).
    distribution: str = "uniform"
    # Zipf exponent for the zipfian distribution: rank r is picked
    # with probability proportional to 1 / (r + 1) ** zipf_skew.
    # 0 degenerates to uniform; 1.0 is the classic YCSB default.
    # Ignored unless distribution is "zipfian".
    zipf_skew: float = 1.0
    # How many of the hottest keys (by access count) get their own
    # per-key latency stats in the reports. 0 disables per-key
    # tracking entirely.
    hot_keys: int = 5
    # Socket timeout in seconds for the RESP connection: connect and
    # read timeout, passed to RespClient. 5.0 is the old hardcoded
    # value; a loaded or remote server may need more, and a dead
    # server fails faster with less.
    timeout_s: float = 5.0
    # Whether the run authenticates on connect (``--password``):
    # the client sends AUTH before any benchmark op. Only the fact
    # that auth was used is carried here — never the password or
    # username, so they cannot leak into a report or the history.
    auth: bool = False
    # Keyspace sizes for a sweep: when non-empty the same workload is
    # re-run once per size (see run_keyspace_sweep) and the per-size
    # ops/sec and p99 land in the reports, so a run shows how
    # keyspace size affects throughput. Empty means no sweep.
    sweep_keyspace: list = field(default_factory=list)
    # Client counts for a sweep: when non-empty the same workload is
    # re-run once per count (see run_client_sweep) and the per-count
    # ops/sec and p99 land in the reports, so a run shows how
    # concurrency affects throughput — where adding clients stops
    # helping and the server saturates. Empty means no sweep.
    sweep_clients: list = field(default_factory=list)
    # Value sizes (bytes) for a sweep: when non-empty the same
    # workload is re-run once per size (see run_value_size_sweep)
    # and the per-size ops/sec and p99 land in the reports, so a
    # run shows how value size affects throughput — a store that
    # copies values on every SET slows as values grow, while one
    # that only moves pointers does not. Empty means no sweep.
    sweep_value_size: list = field(default_factory=list)
    # Pipeline depths for a sweep: when non-empty the same workload
    # is re-run once per depth (see run_pipeline_sweep) and the
    # per-depth ops/sec and p99 land in the reports, so a run shows
    # how pipelining affects throughput — where batching more
    # commands per round trip stops helping. Empty means no sweep.
    sweep_pipeline: list = field(default_factory=list)
    # Read:write ratios for a sweep: when non-empty the same
    # workload is re-run once per ratio (see run_mix_sweep) and the
    # per-ratio ops/sec and p99 land in the reports, so a run shows
    # how the read:write mix affects throughput — a store whose
    # reads are cheap and writes expensive shows it as the mix
    # moves from read-only to write-only. Each ratio is a (read,
    # write) pair of non-negative ints summing above 0; a step
    # runs GETs for the read share and SETs for the write share,
    # with no INCRs or DELs, the same meaning as a two-part
    # ``--mix``. Empty means no sweep.
    sweep_mix: list = field(default_factory=list)
    # SLO gate thresholds: p99 latency cap in milliseconds and error
    # rate cap in percent of attempted ops. None means that side of
    # the gate is not set; the gate only applies when at least one
    # threshold is given. The values are carried on the workload so
    # the reports can record what the run was gated on.
    slo_p99_ms: float = None
    slo_error_rate_pct: float = None
    # Per-operation SLO thresholds: op name ("SET"/"GET"/"INCR"/"DEL") to a
    # (p99_ms, error_rate_pct) pair, either of which may be None.
    # Empty means no per-op gate; the global thresholds above still
    # apply. Validated the same way as the global thresholds.
    slo_per_op: dict = field(default_factory=dict)
    # Path of the SLO preset file the thresholds were loaded from
    # (``--slo-preset``), or None when no preset was used. Recorded
    # so the reports can say where the thresholds came from; the
    # thresholds themselves live in the fields above.
    slo_preset: str = None
    # Path of the workload config file the workload was loaded from
    # (``--workload``), or None when the workload came from the
    # command line. Recorded so a report (and a history record) says
    # where the workload was declared; the resolved values live in
    # the fields above, so the run is reproducible from the record
    # even without the file.
    workload_file: str = None
    # How many times to run the benchmark (``--repeat``): 1 is the
    # old single-run behaviour. Above 1 the workload is run that
    # many times and the reported ops/sec and p99 are the medians
    # over the runs (see run_repeated_workload) — a median is
    # steadier than one run when the machine is noisy. Each repeat
    # is a full fresh run, warmup included.
    repeat: int = 1

    def __post_init__(self):
        if not self.key_prefix:
            raise ValueError("key prefix must be non-empty")
        if self.clients <= 0 or self.keys <= 0:
            raise ValueError("clients and keys must be positive")
        if self.distribution not in ("uniform", "zipfian"):
            raise ValueError(
                "distribution must be 'uniform' or 'zipfian'")
        if self.zipf_skew < 0:
            raise ValueError("zipf skew must be non-negative")
        if self.duration_s is not None:
            if self.duration_s <= 0:
                raise ValueError("duration must be positive")
            # Duration mode: the op count is whatever completes in
            # the time budget, so there is no fixed target.
            self.ops = 0
        elif self.ops <= 0:
            raise ValueError("ops must be positive")
        if self.warmup_s < 0:
            raise ValueError("warmup must be non-negative")
        if self.hot_keys < 0:
            raise ValueError("hot_keys must be non-negative")
        if self.timeout_s <= 0:
            raise ValueError("timeout must be positive")
        if not isinstance(self.repeat, int) or self.repeat < 1:
            raise ValueError("repeat must be a positive integer")
        if any(not isinstance(size, int) or size <= 0
               for size in self.sweep_keyspace):
            raise ValueError("sweep keyspace sizes must be positive integers")
        if any(not isinstance(count, int) or count <= 0
               for count in self.sweep_clients):
            raise ValueError("sweep client counts must be positive integers")
        if any(not isinstance(size, int) or size <= 0
               for size in self.sweep_value_size):
            raise ValueError(
                "sweep value sizes must be positive integers")
        if any(not isinstance(depth, int) or depth <= 0
               for depth in self.sweep_pipeline):
            raise ValueError(
                "sweep pipeline depths must be positive integers")
        if any(not isinstance(ratio, (tuple, list))
               or len(ratio) != 2
               or any(not isinstance(v, int) or v < 0 for v in ratio)
               or sum(ratio) <= 0
               for ratio in self.sweep_mix):
            raise ValueError(
                "sweep mix ratios must be READ:WRITE pairs of "
                "non-negative integers summing above 0")
        if self.pipeline < 1:
            raise ValueError("pipeline depth must be at least 1")
        if self.slo_p99_ms is not None and self.slo_p99_ms < 0:
            raise ValueError("SLO p99 threshold must be non-negative")
        if self.slo_error_rate_pct is not None and not (
                0 <= self.slo_error_rate_pct <= 100):
            raise ValueError(
                "SLO error-rate threshold must be between 0 and 100")
        for op, thresholds in self.slo_per_op.items():
            if op not in ("SET", "GET", "INCR", "DEL"):
                raise ValueError(
                    "per-op SLO op must be SET, GET, INCR or DEL")
            p99_ms, error_rate_pct = thresholds
            if p99_ms is not None and p99_ms < 0:
                raise ValueError(
                    "per-op SLO p99 threshold must be non-negative")
            if error_rate_pct is not None and not (
                    0 <= error_rate_pct <= 100):
                raise ValueError(
                    "per-op SLO error-rate threshold must be "
                    "between 0 and 100")
            if p99_ms is None and error_rate_pct is None:
                raise ValueError(
                    "per-op SLO must set at least one threshold")
        total = (self.set_weight + self.get_weight + self.incr_weight
                 + self.del_weight)
        if total <= 0:
            raise ValueError("at least one op weight must be positive")
        self._total = total


@dataclass
class BenchmarkResult:
    name: str
    ops: int
    elapsed_s: float
    errors: int = 0
    error_sample: str = ""
    latencies_ns: list = field(default_factory=list)
    # Completion time of each op in latencies_ns, in seconds since the
    # run started. Parallel to latencies_ns: one entry per successful
    # op, in the same order.
    times_s: list = field(default_factory=list)
    # Per-op latencies (ns), keyed by op name ("SET", "GET", "INCR",
    # "DEL").
    # Each list holds the same amortized per-op latencies as
    # latencies_ns, split by operation, so the report can show
    # per-type ops/sec and latency percentiles.
    latencies_by_op: dict = field(default_factory=dict)
    # Per-op error counts, keyed by op name ("SET", "GET", "INCR",
    # "DEL").
    # Together with latencies_by_op this gives each op's attempted
    # count (successes plus errors), so the SLO gate can cap the
    # error rate per operation type, not just overall.
    errors_by_op: dict = field(default_factory=dict)
    # Per-key latencies (ns), keyed by the full key string
    # ("kvbench:0", "kvbench:counter:3", ...). Each list holds the
    # same amortized per-op latencies as latencies_ns, split by key,
    # so the report can show latency stats for the hottest keys.
    # This duplicates the latencies in latencies_ns (one extra
    # reference per op), so a very long duration run uses roughly
    # twice the latency memory; per-key tracking is off when the
    # workload sets hot_keys to 0.
    latencies_by_key: dict = field(default_factory=dict)
    # Per-client latencies (ns), keyed by thread index (each thread
    # is one client). Same shape as latencies_by_op, so the report
    # can show per-client p99 and attribute the tail to one slow
    # client. This duplicates the latencies in latencies_ns (one
    # extra reference per op), like latencies_by_key.
    latencies_by_client: dict = field(default_factory=dict)
    # Hot vs cold key latencies (ns): ops whose key was in the
    # hottest 1% of the keyspace vs everything else, so the report
    # can show whether the tail lives on hot keys or cold ones.
    hot_latencies_ns: list = field(default_factory=list)
    cold_latencies_ns: list = field(default_factory=list)
    # Successful ops whose key was in the hot set: the hottest 1% of
    # the keyspace. Under a uniform distribution this is ~1% of ops;
    # under zipfian it is the measured hot-set hit rate, so a report
    # can show whether the skewed traffic actually landed on hot keys.
    hot_ops: int = 0
    # Ops run during the warmup phase, before measurement. They are
    # not part of ``ops`` and contribute no latencies; the count is
    # kept so a report can show how much warming actually happened.
    warmup_ops: int = 0
    # Keyspace sweep results for this backend: one entry per swept
    # size, in ascending size order, each with the size (``keys``),
    # the ops completed, and the ops/sec, mean and p99 latency of
    # that run, plus the per-operation latency histograms of that
    # run (``histograms_by_op`` — the same log-spaced buckets per
    # op as the main run's, so a slow op type's shape shows per
    # size). With ``--repeat`` above 1 each size runs that many
    # times: the ops/sec and p99 are the medians over the runs and
    # the entry carries the per-run values and the run-to-run
    # spread under ``repeat`` (``runs`` and ``spread``, the same
    # shapes as the main run's ``repeat_runs`` and
    # ``repeat_spread``). Empty when no sweep ran
    # (``--sweep-keyspace`` not given). The full latency samples
    # of the sweep runs are not kept — only these summaries.
    sweep: list = field(default_factory=list)
    # Client-count sweep results for this backend: one entry per
    # swept count, in ascending count order, each with the count
    # (``clients``), the ops completed, and the ops/sec, mean and
    # p99 latency of that run, plus the per-operation latency
    # histograms of that run (``histograms_by_op``). With
    # ``--repeat`` above 1 each count runs that many times: the
    # ops/sec and p99 are the medians over the runs and the entry
    # carries the per-run values and the run-to-run spread under
    # ``repeat``. Empty when no sweep ran (``--sweep-clients`` not
    # given). Same shape as ``sweep``.
    client_sweep: list = field(default_factory=list)
    # Value-size sweep results for this backend: one entry per
    # swept size, in ascending size order, each with the size
    # (``value_size`` in bytes), the ops completed, and the ops/sec,
    # mean and p99 latency of that run, plus the per-operation
    # latency histograms of that run (``histograms_by_op``). With
    # ``--repeat`` above 1 each size runs that many times: the
    # ops/sec and p99 are the medians over the runs and the entry
    # carries the per-run values and the run-to-run spread under
    # ``repeat``. Empty when no sweep ran (``--sweep-value-size``
    # not given). Same shape as ``sweep``.
    value_sweep: list = field(default_factory=list)
    # Pipeline-depth sweep results for this backend: one entry per
    # swept depth, in ascending depth order, each with the depth
    # (``pipeline`` in commands per round trip), the ops completed,
    # and the ops/sec, mean and p99 latency of that run, plus the
    # per-operation latency histograms of that run
    # (``histograms_by_op``). With ``--repeat`` above 1 each depth
    # runs that many times: the ops/sec and p99 are the medians
    # over the runs and the entry carries the per-run values and
    # the run-to-run spread under ``repeat``. Empty when no sweep
    # ran (``--sweep-pipeline`` not given). Same shape as ``sweep``.
    pipeline_sweep: list = field(default_factory=list)
    # Read:write mix sweep results for this backend: one entry per
    # swept ratio, in read-heaviest-first order, each with the
    # ratio (``mix`` as a ``"READ:WRITE"`` string), the ops
    # completed, and the ops/sec, mean and p99 latency of that run,
    # plus the per-operation latency histograms of that run
    # (``histograms_by_op``). With ``--repeat`` above 1 each ratio
    # runs that many times: the ops/sec and p99 are the medians
    # over the runs and the entry carries the per-run values and
    # the run-to-run spread under ``repeat``. Empty when no sweep
    # ran (``--sweep-mix`` not given). Same shape as ``sweep``.
    mix_sweep: list = field(default_factory=list)
    # Repeated-run detail (see run_repeated_workload): one summary
    # dict per run, in run order, each with the run index (``run``),
    # ops, elapsed, ops/sec, errors, mean/p50/p95/p99, the
    # per-op ops/sec and p99, and the per-operation latency
    # histograms of that run (``histograms_by_op`` — the same
    # log-spaced buckets per op as the main run's, so a run where
    # one op type slows down names the op). Empty for a single
    # run — the JSON shape for a single run is unchanged.
    repeat_runs: list = field(default_factory=list)
    # Median ops/sec over the repeated runs, or None for a single
    # run. When set, ``ops_per_sec`` reports this instead of
    # ops / elapsed.
    median_ops_per_sec: float = None
    # Median p99 (ms) over the repeated runs, or None for a single
    # run. When set, ``summary`` reports this as its p99.
    median_p99_ms: float = None
    # Median per-op figures over the repeated runs, or {} for a
    # single run: op name to {"ops_per_sec", "p99_ms"}. An op that
    # ran in no repeat is absent. When set, ``per_op_summary``
    # reports these medians for the ops it covers.
    median_per_op: dict = field(default_factory=dict)
    # Run-to-run spread summary over the repeated runs, or {} for
    # a single run: {"overall": {"ops_per_sec", "p99_ms"},
    # "per_op": {op: {"ops_per_sec", "p99_ms"}}} where each leaf is
    # the :func:`kvbench.stats.spread_summary` dict (min, median,
    # max, spread, spread as a percentage of the median) for that
    # figure over the runs. Empty when fewer than two runs ran —
    # one run has no spread to report.
    repeat_spread: dict = field(default_factory=dict)

    @property
    def hot_hit_rate(self):
        """Share of successful ops that hit the hottest 1% of keys."""
        total = len(self.latencies_ns)
        return self.hot_ops / total if total > 0 else 0.0

    @property
    def ops_per_sec(self):
        if self.median_ops_per_sec is not None:
            return self.median_ops_per_sec
        return self.ops / self.elapsed_s if self.elapsed_s > 0 else 0.0

    @property
    def summary(self):
        # The median p99 replaces the representative run's own p99:
        # the reported p99 is the median over the repeated runs,
        # while mean/p50/p95/min/max come from the representative
        # (median) run's latencies.
        summary = summarize(self.latencies_ns)
        if self.median_p99_ms is not None:
            summary["p99_ms"] = self.median_p99_ms
        return summary

    @property
    def per_op_summary(self):
        """Per-op latency summaries, in stable SET/GET/INCR/DEL order.

        Each entry is the :func:`summarize` dict for that op plus its
        own ``ops_per_sec`` (count / elapsed). Ops with no samples are
        left out, so a read-only mix reports GET alone. For a
        repeated run the ops/sec and p99 of each op are the medians
        over the repeats, like the overall figures.
        """
        out = {}
        for op in ("SET", "GET", "INCR", "DEL"):
            samples = self.latencies_by_op.get(op)
            if not samples:
                continue
            entry = summarize(samples)
            entry["ops_per_sec"] = (
                len(samples) / self.elapsed_s if self.elapsed_s > 0 else 0.0
            )
            medians = self.median_per_op.get(op)
            if medians is not None:
                entry["ops_per_sec"] = medians["ops_per_sec"]
                entry["p99_ms"] = medians["p99_ms"]
            out[op] = entry
        return out

    @property
    def histogram(self):
        return histogram(self.latencies_ns)

    @property
    def per_op_histograms(self):
        """Per-operation latency histograms, in SET/GET/INCR/DEL order.

        Each entry is the :func:`histogram` bucket list for that op's
        latencies — the same log-spaced buckets as the overall
        histogram — so a slow op type's latency shape shows
        separately from the overall distribution. Ops with no
        samples are left out, so a read-only mix reports GET alone.
        """
        out = {}
        for op in ("SET", "GET", "INCR", "DEL"):
            samples = self.latencies_by_op.get(op)
            if not samples:
                continue
            out[op] = histogram(samples)
        return out

    def hot_key_summary(self, top_n=5):
        """Latency stats for the ``top_n`` hottest keys, by access count.

        Each entry is the :func:`summarize` dict for that key plus its
        ``key`` string, ordered by access count descending (ties broken
        by key, so the order is deterministic). Keys that never ran
        are absent, and ``top_n`` of 0 returns []. A key that was
        both SET/GET and INCRed reports under one key string each:
        ``<prefix>:<rank>`` for SET/GET and
        ``<prefix>:counter:<rank>`` for INCR, because counters live
        in their own keyspace.
        """
        if top_n <= 0 or not self.latencies_by_key:
            return []
        ranked = sorted(
            self.latencies_by_key.items(),
            key=lambda item: (-len(item[1]), item[0]),
        )
        out = []
        for key, samples in ranked[:top_n]:
            entry = summarize(samples)
            entry["key"] = key
            out.append(entry)
        return out

    @property
    def time_series(self):
        # Results built without completion times (e.g. older callers)
        # have no series rather than an error.
        if not self.times_s:
            return []
        return time_series(self.latencies_ns, self.times_s)

    @property
    def tail_report(self):
        """Tail-latency attribution for this result.

        See :func:`kvbench.stats.attribute_tail`: slow ops (at or
        above the overall p99) are broken down by operation, by
        client, by hot vs cold keys and by second, and the finding
        names the factor that dominates the tail, if any.
        """
        return attribute_tail(
            self.latencies_ns, self.times_s, self.latencies_by_op,
            self.latencies_by_client, self.hot_latencies_ns,
            self.cold_latencies_ns,
        )


class DictStore:
    """In-process dict store with the same command surface as the server.

    Used as the baseline backend. A lock makes INCR atomic across
    threads, matching the single-threaded server's semantics.
    """

    def __init__(self):
        self._data = {}
        self._lock = threading.Lock()

    def command(self, *args):
        op = args[0].upper()
        if op == "SET":
            with self._lock:
                self._data[args[1]] = args[2]
            return "OK"
        if op == "GET":
            with self._lock:
                return self._data.get(args[1])
        if op == "INCR":
            with self._lock:
                value = int(self._data.get(args[1], "0")) + 1
                self._data[args[1]] = str(value)
                return value
        if op == "DEL":
            with self._lock:
                return 1 if self._data.pop(args[1], None) is not None \
                    else 0
        if op == "PING":
            return "PONG"
        raise ValueError(f"unsupported op for baseline: {op}")


class DictClient:
    """Baseline backend: a dict wrapped in a ``command()`` interface."""

    def __init__(self, store=None):
        self._store = store or DictStore()

    def command(self, *args):
        return self._store.command(*args)

    def pipeline(self, commands):
        """Run several commands, one entry per command.

        Mirrors :meth:`kvbench.resp.RespClient.pipeline`: a reply, or
        the exception for that command, per entry.
        """
        replies = []
        for cmd in commands:
            try:
                replies.append(self.command(*cmd))
            except Exception as exc:  # noqa: BLE001 - returned, not raised
                replies.append(exc)
        return replies

    def close(self):
        pass


def _pick_op(rng, workload):
    roll = rng.random() * workload._total
    if roll < workload.set_weight:
        return "SET"
    if roll < workload.set_weight + workload.get_weight:
        return "GET"
    if roll < workload.set_weight + workload.get_weight \
            + workload.incr_weight:
        return "INCR"
    return "DEL"


def run_workload(client_factory, workload, name):
    """Run the workload against ``client_factory()`` backends.

    ``client_factory`` is called once per thread and must return an
    object with ``command(*args)`` and ``close()``. Returns a
    :class:`BenchmarkResult`.

    With ``workload.duration_s`` set, workers run until the shared
    wall-clock deadline instead of a fixed op count, and the result's
    ``ops`` is however many completed in that time.

    With ``workload.warmup_s`` set, workers first run the same
    workload for that long and discard everything it produces —
    latencies, errors, hot counts — so the measured phase starts
    with a warm server. The measured phase is re-seeded after
    warmup, so its keys and op mix are identical to a run with no
    warmup at all. Warmup ops are reported on the result as
    ``warmup_ops``; the result's ``elapsed_s`` covers the measured
    phase only, and in duration mode the duration clock starts
    after warmup ends.
    """
    per_thread = workload.ops // workload.clients
    remainder = workload.ops % workload.clients
    latencies = []
    times = []
    latencies_by_op = {}
    latencies_by_key = {}
    latencies_by_client = {}
    hot_latencies = []
    cold_latencies = []
    hot_ops = 0
    warmup_ops = 0
    errors = 0
    errors_by_op = {}
    error_sample = ""
    thread_errors = []
    lock = threading.Lock()
    # Hot set: the hottest 1% of the keyspace (at least one key).
    hot_count = max(1, workload.keys // 100)

    def worker(thread_index, op_count):
        nonlocal errors, error_sample, hot_ops, warmup_ops
        rng = random.Random(workload.seed + thread_index)
        # One sampler per thread: cheap to build, and sharing one
        # across threads would need a lock on every pick.
        sampler = (ZipfianSampler(workload.keys, workload.zipf_skew)
                   if workload.distribution == "zipfian" else None)
        value = "x" * workload.value_size
        try:
            client = client_factory()
        except Exception as exc:  # noqa: BLE001 - surfaced after join
            # A failed client (e.g. connection refused) used to die
            # silently inside the thread and produce a 0-op result.
            # Record it so run_workload can raise it to the caller.
            with lock:
                thread_errors.append(exc)
            return

        def run_phase(count, phase_deadline, collect, time_base):
            """Run batches until ``count`` is exhausted or the
            deadline is reached; return (latencies, times, by_op,
            by_key, hot, hot_lat, cold_lat, errors, errors_by_op,
            sample, ops_run).

            With ``collect`` False this is a warmup phase: batches
            still run against the backend, but no latencies, times,
            hot counts or errors are kept — only the op count.
            ``time_base`` is the perf_counter origin for completion
            times; warmup phases pass the run start, measured phases
            pass the moment warmup ended.
            """
            local_lat = []
            local_times = []
            local_by_op = {}
            local_by_key = {}
            local_hot = 0
            local_hot_lat = []
            local_cold_lat = []
            local_errors = 0
            local_errors_by_op = {}
            local_sample = ""
            done = 0
            # ``remaining`` is None in duration mode and in warmup:
            # workers run until the shared deadline instead of a
            # fixed op count.
            remaining = count
            while remaining is None or remaining > 0:
                if phase_deadline is not None and \
                        time.perf_counter() >= phase_deadline:
                    break
                batch_size = (workload.pipeline if remaining is None
                              else min(workload.pipeline, remaining))
                batch = []
                batch_hot = []
                for _ in range(batch_size):
                    op = _pick_op(rng, workload)
                    # Zipfian picks a rank; uniform picks a flat
                    # random key. Either way the hot flag is whether
                    # the rank is in the hottest 1% of the keyspace.
                    rank = (sampler.pick(rng) if sampler is not None
                            else rng.randrange(workload.keys))
                    batch_hot.append(rank < hot_count)
                    # Counters live in their own keyspace: INCR on a key
                    # previously SET to a non-numeric value would error,
                    # exactly as it does on a real Redis server.
                    if op == "INCR":
                        key = f"{workload.key_prefix}:counter:{rank}"
                        batch.append(("INCR", key))
                    else:
                        key = f"{workload.key_prefix}:{rank}"
                        if op == "SET":
                            batch.append(("SET", key, value))
                        elif op == "DEL":
                            # DEL shares the SET/GET keyspace: it
                            # deletes a key the workload SET earlier
                            # (or one another thread already deleted
                            # — deleting a missing key is a success
                            # that replies 0, not an error).
                            batch.append(("DEL", key))
                        else:
                            batch.append(("GET", key))
                start = time.perf_counter_ns()
                try:
                    run_batch = getattr(client, "pipeline", None)
                    if run_batch is not None:
                        replies = run_batch(batch)
                    else:
                        # No pipeline support: run the batch one op at
                        # a time, capturing per-op errors the same way.
                        replies = []
                        for cmd in batch:
                            try:
                                replies.append(client.command(*cmd))
                            except Exception as exc:  # noqa: BLE001
                                replies.append(exc)
                except Exception as exc:  # noqa: BLE001 - count and continue
                    # The whole batch failed (e.g. transport error):
                    # every op in it counts as an error, split by op
                    # so the per-op error rate stays honest.
                    done += batch_size
                    if collect:
                        local_errors += batch_size
                        for cmd in batch:
                            local_errors_by_op[cmd[0]] = \
                                local_errors_by_op.get(cmd[0], 0) + 1
                        if not local_sample:
                            local_sample = f"{type(exc).__name__}: {exc}"
                    if remaining is not None:
                        remaining -= batch_size
                    continue
                batch_ns = time.perf_counter_ns() - start
                # Amortized per-op latency: the batch round trip divided
                # by the batch size (see Workload.pipeline).
                per_op_ns = batch_ns // batch_size
                # Completion time of this batch, in seconds since the
                # phase's time base; every op in the batch shares it,
                # so the per-second series buckets by batch completion.
                end_s = time.perf_counter() - time_base
                done += batch_size
                # Replies come back in batch order, so each reply pairs
                # with its command's op for the per-op split.
                for cmd, is_hot, reply in zip(batch, batch_hot, replies):
                    if isinstance(reply, Exception):
                        if collect:
                            local_errors += 1
                            local_errors_by_op[cmd[0]] = \
                                local_errors_by_op.get(cmd[0], 0) + 1
                            if not local_sample:
                                local_sample = (f"{type(reply).__name__}: "
                                                f"{reply}")
                        continue
                    if collect:
                        local_lat.append(per_op_ns)
                        local_times.append(end_s)
                        local_by_op.setdefault(cmd[0], []).append(per_op_ns)
                        if workload.hot_keys > 0:
                            local_by_key.setdefault(
                                cmd[1], []).append(per_op_ns)
                        if is_hot:
                            local_hot += 1
                            local_hot_lat.append(per_op_ns)
                        else:
                            local_cold_lat.append(per_op_ns)
                if remaining is not None:
                    remaining -= batch_size
            return (local_lat, local_times, local_by_op, local_by_key,
                    local_hot, local_hot_lat, local_cold_lat,
                    local_errors, local_errors_by_op, local_sample,
                    done)

        try:
            local_warmup = 0
            if warmup_deadline is not None:
                (_, _, _, _, _, _, _, _, _, _, local_warmup) = run_phase(
                    None, warmup_deadline, collect=False, time_base=start)
                # Re-seed so the measured phase draws the same keys
                # and op mix as a run with no warmup at all.
                rng = random.Random(workload.seed + thread_index)
            (local_lat, local_times, local_by_op, local_by_key,
             local_hot, local_hot_lat, local_cold_lat, local_errors,
             local_errors_by_op, local_sample, _) = run_phase(
                op_count, deadline, collect=True,
                time_base=measure_start)
        finally:
            client.close()
        with lock:
            latencies.extend(local_lat)
            times.extend(local_times)
            for op, samples in local_by_op.items():
                latencies_by_op.setdefault(op, []).extend(samples)
            for key, samples in local_by_key.items():
                latencies_by_key.setdefault(key, []).extend(samples)
            # One worker is one client: its measured latencies are
            # that client's latencies.
            latencies_by_client.setdefault(
                thread_index, []).extend(local_lat)
            hot_latencies.extend(local_hot_lat)
            cold_latencies.extend(local_cold_lat)
            hot_ops += local_hot
            warmup_ops += local_warmup
            errors += local_errors
            for op, count in local_errors_by_op.items():
                errors_by_op[op] = errors_by_op.get(op, 0) + count
            if local_sample and not error_sample:
                error_sample = local_sample

    threads = []
    start = time.perf_counter()
    # Warmup runs first, against the same clock: the measured phase
    # starts when warmup ends, so elapsed and the duration-mode
    # deadline both count from there, not from thread start.
    measure_start = start + workload.warmup_s
    warmup_deadline = (measure_start if workload.warmup_s > 0 else None)
    # In duration mode every worker runs until this deadline; the
    # total op count is whatever completes in that time.
    deadline = (measure_start + workload.duration_s
                if workload.duration_s is not None else None)
    for i in range(workload.clients):
        # None means "run until the deadline" for that worker.
        count = None if deadline is not None else \
            per_thread + (1 if i < remainder else 0)
        t = threading.Thread(target=worker, args=(i, count))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()
    if thread_errors:
        raise thread_errors[0]
    elapsed = time.perf_counter() - measure_start

    return BenchmarkResult(
        name=name,
        ops=len(latencies) + errors,
        elapsed_s=elapsed,
        errors=errors,
        error_sample=error_sample,
        latencies_ns=latencies,
        times_s=times,
        latencies_by_op=latencies_by_op,
        errors_by_op=errors_by_op,
        latencies_by_key=latencies_by_key,
        latencies_by_client=latencies_by_client,
        hot_latencies_ns=hot_latencies,
        cold_latencies_ns=cold_latencies,
        hot_ops=hot_ops,
        warmup_ops=warmup_ops,
    )


def run_repeated_workload(client_factory, workload, name, repeat):
    """Run the workload ``repeat`` times and report the medians.

    Each run is a fresh :func:`run_workload` call — same workload,
    same seed, so every run measures the same keys and op mix; only
    the machine noise differs. Returns the run whose ops/sec is the
    median (the lower middle for an even count) as the
    representative result: its latencies back the mean, p50, p95,
    histogram, per-op splits, tail attribution and time series, and
    its sweep fields stay empty (sweeps run on the result — see
    the CLI — and repeat per step there when ``repeat`` is above 1,
    through this same function, so a sweep step's medians agree
    with the main run's). The result's reported ops/sec and p99 are the
    medians over all ``repeat`` runs, per operation as well as
    overall, and ``repeat_runs`` carries one summary dict per run
    (run index, ops, elapsed, ops/sec, errors, mean/p50/p95/p99,
    per-op ops/sec and p99, and the per-operation latency
    histograms of that run under ``histograms_by_op`` — the same
    log-spaced buckets per op as the main run's, so a run where
    one op type slows down names the op), so a report can show
    the spread. With more than one run ``repeat_spread`` carries
    the run-to-run spread summary (min, median, max, spread and
    spread as a percentage of the median) for ops/sec and p99,
    overall and per operation — a single run has no spread, so
    the field stays empty. A run that cannot reach the backend raises, like
    :func:`run_workload` does — the repeat stops there instead of
    reporting a median over partial runs.
    """
    if repeat < 1:
        raise ValueError("repeat must be a positive integer")
    runs = []
    for _ in range(repeat):
        runs.append(run_workload(client_factory, workload, name))
    per_run = []
    for index, run in enumerate(runs):
        run_summary = run.summary
        per_run.append({
            "run": index + 1,
            "ops": run.ops,
            "elapsed_s": run.elapsed_s,
            "ops_per_sec": run.ops_per_sec,
            "errors": run.errors,
            "mean_ms": run_summary["mean_ms"],
            "p50_ms": run_summary["p50_ms"],
            "p95_ms": run_summary["p95_ms"],
            "p99_ms": run_summary["p99_ms"],
            "per_op": {
                op: {
                    "ops": entry["count"],
                    "ops_per_sec": entry["ops_per_sec"],
                    "p99_ms": entry["p99_ms"],
                }
                for op, entry in run.per_op_summary.items()
            },
            # Per-operation latency histograms of this run: the
            # same log-spaced buckets per op as the main run's
            # ``histograms_by_op``, so a run where one op type
            # slows down names the op.
            "histograms_by_op": run.per_op_histograms,
        })
    # The representative run is the median by ops/sec; the reported
    # ops/sec is the median over the runs (identical to the
    # representative run's own ops/sec for an odd count).
    by_ops = sorted(runs, key=lambda r: r.ops_per_sec)
    representative = by_ops[(len(by_ops) - 1) // 2]
    median_per_op = {}
    for op in ("SET", "GET", "INCR", "DEL"):
        ops_values = [entry["per_op"][op]["ops_per_sec"]
                      for entry in per_run if op in entry["per_op"]]
        p99_values = [entry["per_op"][op]["p99_ms"]
                      for entry in per_run if op in entry["per_op"]]
        if ops_values:
            median_per_op[op] = {
                "ops_per_sec": median(ops_values),
                "p99_ms": median(p99_values),
            }
    representative.repeat_runs = per_run
    representative.median_ops_per_sec = median(
        [entry["ops_per_sec"] for entry in per_run])
    representative.median_p99_ms = median(
        [entry["p99_ms"] for entry in per_run])
    representative.median_per_op = median_per_op
    if len(per_run) > 1:
        # Run-to-run spread: min, median, max, spread (max - min)
        # and spread as a percentage of the median, for ops/sec and
        # p99 overall and per operation. A single run has no spread
        # to report, so the field stays empty.
        per_op_spread = {}
        for op in ("SET", "GET", "INCR", "DEL"):
            ops_values = [entry["per_op"][op]["ops_per_sec"]
                          for entry in per_run if op in entry["per_op"]]
            p99_values = [entry["per_op"][op]["p99_ms"]
                          for entry in per_run if op in entry["per_op"]]
            if ops_values:
                per_op_spread[op] = {
                    "ops_per_sec": spread_summary(ops_values),
                    "p99_ms": spread_summary(p99_values),
                }
        representative.repeat_spread = {
            "overall": {
                "ops_per_sec": spread_summary(
                    [entry["ops_per_sec"] for entry in per_run]),
                "p99_ms": spread_summary(
                    [entry["p99_ms"] for entry in per_run]),
            },
            "per_op": per_op_spread,
        }
    return representative


def _sweep_step(client_factory, swept, name, step, repeat):
    """Run one sweep step, once or ``repeat`` times, and summarize it.

    With ``repeat`` of 1 this is a single :func:`run_workload` call
    and the entry is the old shape (no ``repeat`` key). Above 1 the
    step runs through :func:`run_repeated_workload` — the same
    machinery the main run's ``--repeat`` uses, so a sweep step's
    reported ops/sec and p99 are the medians over its runs, exactly
    like the main run's — and the entry carries the per-run values
    and the run-to-run spread under ``repeat`` (``runs`` and
    ``spread``, the same shapes as a repeated result's
    ``repeat_runs`` and ``repeat_spread``). ``step`` holds the
    step-identifying keys (``{"keys": size}`` etc.); it is merged
    into the returned entry.
    """
    if not isinstance(repeat, int) or repeat < 1:
        raise ValueError("repeat must be a positive integer")
    if repeat > 1:
        result = run_repeated_workload(client_factory, swept, name,
                                       repeat)
    else:
        result = run_workload(client_factory, swept, name)
    summary = result.summary
    entry = dict(step)
    entry.update({
        "ops": result.ops,
        "ops_per_sec": result.ops_per_sec,
        "mean_ms": summary["mean_ms"],
        "p99_ms": summary["p99_ms"],
        "histograms_by_op": result.per_op_histograms,
    })
    if repeat > 1:
        entry["repeat"] = {
            "runs": result.repeat_runs,
            "spread": result.repeat_spread,
        }
    return entry


def run_keyspace_sweep(client_factory, workload, sizes, repeat=1):
    """Run the same workload once per keyspace size in ``sizes``.

    Each run is a fresh copy of ``workload`` with ``keys`` replaced
    by that size; everything else (ops, clients, mix, distribution,
    pipeline, warmup, seed) is identical, so the per-size numbers
    isolate the effect of keyspace size on throughput. Returns a
    list of ``{"keys", "ops", "ops_per_sec", "mean_ms", "p99_ms",
    "histograms_by_op"}`` dicts, in the order ``sizes`` was given —
    the per-op histograms are the same log-spaced buckets per
    operation as the main run's ``histograms_by_op``, so a slow op
    type's latency shape shows per size. With ``repeat`` above 1
    each size runs that many times (see :func:`_sweep_step`): the
    reported ops/sec and p99 are the medians over the runs, and
    each entry carries the per-run values and the run-to-run
    spread under ``repeat``. If a run cannot reach
    the backend the sweep stops there and the sizes completed so far
    are returned — the caller reports the shortfall.
    """
    out = []
    for size in sizes:
        swept = replace(workload, keys=size)
        try:
            entry = _sweep_step(client_factory, swept,
                                f"sweep ({size} keys)", {"keys": size},
                                repeat)
        except (ConnectionError, OSError):
            break
        out.append(entry)
    return out


def run_client_sweep(client_factory, workload, counts, repeat=1):
    """Run the same workload once per client count in ``counts``.

    Each run is a fresh copy of ``workload`` with ``clients``
    replaced by that count; everything else (ops, keys, mix,
    distribution, pipeline, warmup, seed) is identical, so the
    per-count numbers isolate the effect of concurrency on
    throughput — where adding clients stops helping and the server
    saturates. Returns a list of ``{"clients", "ops", "ops_per_sec",
    "mean_ms", "p99_ms", "histograms_by_op"}`` dicts, in the order
    ``counts`` was given — the per-op histograms are the same
    log-spaced buckets per operation as the main run's
    ``histograms_by_op``, so a slow op type's latency shape shows
    per count. With ``repeat`` above 1 each count runs that many
    times (see :func:`_sweep_step`): the reported ops/sec and p99
    are the medians over the runs, and each entry carries the
    per-run values and the run-to-run spread under ``repeat``. If
    a run cannot reach the backend the sweep stops
    there and the counts completed so far are returned — the caller
    reports the shortfall.
    """
    out = []
    for count in counts:
        swept = replace(workload, clients=count)
        try:
            entry = _sweep_step(client_factory, swept,
                                f"sweep ({count} clients)",
                                {"clients": count}, repeat)
        except (ConnectionError, OSError):
            break
        out.append(entry)
    return out


def run_value_size_sweep(client_factory, workload, sizes, repeat=1):
    """Run the same workload once per value size in ``sizes``.

    Each run is a fresh copy of ``workload`` with ``value_size``
    replaced by that size in bytes; everything else (ops, clients,
    keys, mix, distribution, pipeline, warmup, seed) is identical,
    so the per-size numbers isolate the effect of value size on
    throughput — whether the store's cost grows with the bytes it
    moves. Returns a list of ``{"value_size", "ops", "ops_per_sec",
    "mean_ms", "p99_ms", "histograms_by_op"}`` dicts, in the order
    ``sizes`` was given — the per-op histograms are the same
    log-spaced buckets per operation as the main run's
    ``histograms_by_op``, so a slow op type's latency shape shows
    per size. With ``repeat`` above 1 each size runs that many
    times (see :func:`_sweep_step`): the reported ops/sec and p99
    are the medians over the runs, and each entry carries the
    per-run values and the run-to-run spread under ``repeat``. If
    a run cannot reach the backend the sweep stops
    there and the sizes completed so far are returned — the caller
    reports the shortfall.
    """
    out = []
    for size in sizes:
        swept = replace(workload, value_size=size)
        try:
            entry = _sweep_step(client_factory, swept,
                                f"sweep ({size} byte values)",
                                {"value_size": size}, repeat)
        except (ConnectionError, OSError):
            break
        out.append(entry)
    return out


def run_pipeline_sweep(client_factory, workload, depths, repeat=1):
    """Run the same workload once per pipeline depth in ``depths``.

    Each run is a fresh copy of ``workload`` with ``pipeline``
    replaced by that depth; everything else (ops, clients, keys,
    mix, distribution, warmup, seed) is identical, so the
    per-depth numbers isolate the effect of pipelining on
    throughput — where batching more commands per round trip stops
    helping. Returns a list of ``{"pipeline", "ops", "ops_per_sec",
    "mean_ms", "p99_ms", "histograms_by_op"}`` dicts, in the order
    ``depths`` was given — the per-op histograms are the same
    log-spaced buckets per operation as the main run's
    ``histograms_by_op``, so a slow op type's latency shape shows
    per depth. With ``repeat`` above 1 each depth runs that many
    times (see :func:`_sweep_step`): the reported ops/sec and p99
    are the medians over the runs, and each entry carries the
    per-run values and the run-to-run spread under ``repeat``. If
    a run cannot reach the backend the sweep stops there and the
    depths completed so far are returned — the caller reports the
    shortfall.
    """
    out = []
    for depth in depths:
        swept = replace(workload, pipeline=depth)
        try:
            entry = _sweep_step(client_factory, swept,
                                f"sweep (pipeline depth {depth})",
                                {"pipeline": depth}, repeat)
        except (ConnectionError, OSError):
            break
        out.append(entry)
    return out


def run_mix_sweep(client_factory, workload, mixes, repeat=1):
    """Run the same workload once per read:write ratio in ``mixes``.

    Each run is a fresh copy of ``workload`` with the mix replaced
    by that ratio — reads are GETs, writes are SETs, with no INCRs
    or DELs, the same meaning as a two-part ``--mix`` — and the
    mix label cleared, since the step's mix is the ratio itself,
    not the base workload's mix; everything else (ops, clients,
    keys, distribution, pipeline, warmup, seed) is identical, so
    the per-ratio numbers isolate the effect of the read:write
    mix on throughput — where a store's writes start costing more
    than its reads. Returns a list of ``{"mix", "ops",
    "ops_per_sec", "mean_ms", "p99_ms", "histograms_by_op"}``
    dicts, ``mix`` a ``"READ:WRITE"`` string, in the order
    ``mixes`` was given — the per-op histograms are the same
    log-spaced buckets per operation as the main run's
    ``histograms_by_op``, so a slow op type's latency shape shows
    per ratio. With ``repeat`` above 1 each ratio runs that many
    times (see :func:`_sweep_step`): the reported ops/sec and p99
    are the medians over the runs, and each entry carries the
    per-run values and the run-to-run spread under ``repeat``. If
    a run cannot reach the backend the sweep stops there and the
    ratios completed so far are returned — the caller reports the
    shortfall.
    """
    out = []
    for read, write in mixes:
        swept = replace(workload, set_weight=write, get_weight=read,
                        incr_weight=0, del_weight=0, mix_label="")
        label = f"{read}:{write}"
        try:
            entry = _sweep_step(client_factory, swept,
                                f"sweep (mix {label})",
                                {"mix": label}, repeat)
        except (ConnectionError, OSError):
            break
        out.append(entry)
    return out
