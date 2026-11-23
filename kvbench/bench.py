"""Workload runner.

Runs a configurable SET/GET/INCR mix against any backend that exposes a
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
from dataclasses import dataclass, field

from .distribution import ZipfianSampler
from .stats import histogram, summarize, time_series


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
    value_size: int = 32
    set_weight: int = 50
    get_weight: int = 40
    incr_weight: int = 10
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

    def __post_init__(self):
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
        if self.pipeline < 1:
            raise ValueError("pipeline depth must be at least 1")
        total = self.set_weight + self.get_weight + self.incr_weight
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
    # Per-op latencies (ns), keyed by op name ("SET", "GET", "INCR").
    # Each list holds the same amortized per-op latencies as
    # latencies_ns, split by operation, so the report can show
    # per-type ops/sec and latency percentiles.
    latencies_by_op: dict = field(default_factory=dict)
    # Per-key latencies (ns), keyed by the full key string
    # ("kvbench:0", "kvbench:counter:3", ...). Each list holds the
    # same amortized per-op latencies as latencies_ns, split by key,
    # so the report can show latency stats for the hottest keys.
    # This duplicates the latencies in latencies_ns (one extra
    # reference per op), so a very long duration run uses roughly
    # twice the latency memory; per-key tracking is off when the
    # workload sets hot_keys to 0.
    latencies_by_key: dict = field(default_factory=dict)
    # Successful ops whose key was in the hot set: the hottest 1% of
    # the keyspace. Under a uniform distribution this is ~1% of ops;
    # under zipfian it is the measured hot-set hit rate, so a report
    # can show whether the skewed traffic actually landed on hot keys.
    hot_ops: int = 0
    # Ops run during the warmup phase, before measurement. They are
    # not part of ``ops`` and contribute no latencies; the count is
    # kept so a report can show how much warming actually happened.
    warmup_ops: int = 0

    @property
    def hot_hit_rate(self):
        """Share of successful ops that hit the hottest 1% of keys."""
        total = len(self.latencies_ns)
        return self.hot_ops / total if total > 0 else 0.0

    @property
    def ops_per_sec(self):
        return self.ops / self.elapsed_s if self.elapsed_s > 0 else 0.0

    @property
    def summary(self):
        return summarize(self.latencies_ns)

    @property
    def per_op_summary(self):
        """Per-op latency summaries, in stable SET/GET/INCR order.

        Each entry is the :func:`summarize` dict for that op plus its
        own ``ops_per_sec`` (count / elapsed). Ops with no samples are
        left out, so a read-only mix reports GET alone.
        """
        out = {}
        for op in ("SET", "GET", "INCR"):
            samples = self.latencies_by_op.get(op)
            if not samples:
                continue
            entry = summarize(samples)
            entry["ops_per_sec"] = (
                len(samples) / self.elapsed_s if self.elapsed_s > 0 else 0.0
            )
            out[op] = entry
        return out

    @property
    def histogram(self):
        return histogram(self.latencies_ns)

    def hot_key_summary(self, top_n=5):
        """Latency stats for the ``top_n`` hottest keys, by access count.

        Each entry is the :func:`summarize` dict for that key plus its
        ``key`` string, ordered by access count descending (ties broken
        by key, so the order is deterministic). Keys that never ran
        are absent, and ``top_n`` of 0 returns []. A key that was
        both SET/GET and INCRed reports under one key string each:
        ``kvbench:<rank>`` for SET/GET and ``kvbench:counter:<rank>``
        for INCR, because counters live in their own keyspace.
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
    return "INCR"


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
    hot_ops = 0
    warmup_ops = 0
    errors = 0
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
            by_key, hot, errors, sample, ops_run).

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
            local_errors = 0
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
                        key = f"kvbench:counter:{rank}"
                        batch.append(("INCR", key))
                    else:
                        key = f"kvbench:{rank}"
                        if op == "SET":
                            batch.append(("SET", key, value))
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
                    # every op in it counts as an error.
                    done += batch_size
                    if collect:
                        local_errors += batch_size
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
                if remaining is not None:
                    remaining -= batch_size
            return (local_lat, local_times, local_by_op, local_by_key,
                    local_hot, local_errors, local_sample, done)

        try:
            local_warmup = 0
            if warmup_deadline is not None:
                (_, _, _, _, _, _, _, local_warmup) = run_phase(
                    None, warmup_deadline, collect=False, time_base=start)
                # Re-seed so the measured phase draws the same keys
                # and op mix as a run with no warmup at all.
                rng = random.Random(workload.seed + thread_index)
            (local_lat, local_times, local_by_op, local_by_key,
             local_hot, local_errors, local_sample, _) = run_phase(
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
            hot_ops += local_hot
            warmup_ops += local_warmup
            errors += local_errors
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
        latencies_by_key=latencies_by_key,
        hot_ops=hot_ops,
        warmup_ops=warmup_ops,
    )
