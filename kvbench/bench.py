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

    def __post_init__(self):
        if self.clients <= 0 or self.keys <= 0:
            raise ValueError("clients and keys must be positive")
        if self.duration_s is not None:
            if self.duration_s <= 0:
                raise ValueError("duration must be positive")
            # Duration mode: the op count is whatever completes in
            # the time budget, so there is no fixed target.
            self.ops = 0
        elif self.ops <= 0:
            raise ValueError("ops must be positive")
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
    """
    per_thread = workload.ops // workload.clients
    remainder = workload.ops % workload.clients
    latencies = []
    times = []
    latencies_by_op = {}
    errors = 0
    error_sample = ""
    thread_errors = []
    lock = threading.Lock()

    def worker(thread_index, op_count):
        nonlocal errors, error_sample
        rng = random.Random(workload.seed + thread_index)
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
        local_lat = []
        local_times = []
        local_by_op = {}
        local_errors = 0
        local_sample = ""
        try:
            # ``remaining`` is None in duration mode: workers run
            # until the shared deadline instead of a fixed op count.
            remaining = op_count
            while remaining is None or remaining > 0:
                if deadline is not None and \
                        time.perf_counter() >= deadline:
                    break
                batch_size = (workload.pipeline if remaining is None
                              else min(workload.pipeline, remaining))
                batch = []
                for _ in range(batch_size):
                    op = _pick_op(rng, workload)
                    # Counters live in their own keyspace: INCR on a key
                    # previously SET to a non-numeric value would error,
                    # exactly as it does on a real Redis server.
                    if op == "INCR":
                        key = (f"kvbench:counter:"
                               f"{rng.randrange(workload.keys)}")
                        batch.append(("INCR", key))
                    else:
                        key = f"kvbench:{rng.randrange(workload.keys)}"
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
                # run started; every op in the batch shares it, so the
                # per-second series buckets by batch completion.
                end_s = time.perf_counter() - start
                # Replies come back in batch order, so each reply pairs
                # with its command's op for the per-op split.
                for cmd, reply in zip(batch, replies):
                    if isinstance(reply, Exception):
                        local_errors += 1
                        if not local_sample:
                            local_sample = (f"{type(reply).__name__}: "
                                            f"{reply}")
                        continue
                    local_lat.append(per_op_ns)
                    local_times.append(end_s)
                    local_by_op.setdefault(cmd[0], []).append(per_op_ns)
                if remaining is not None:
                    remaining -= batch_size
        finally:
            client.close()
        with lock:
            latencies.extend(local_lat)
            times.extend(local_times)
            for op, samples in local_by_op.items():
                latencies_by_op.setdefault(op, []).extend(samples)
            errors += local_errors
            if local_sample and not error_sample:
                error_sample = local_sample

    threads = []
    start = time.perf_counter()
    # In duration mode every worker runs until this deadline; the
    # total op count is whatever completes in that time.
    deadline = (start + workload.duration_s
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
    elapsed = time.perf_counter() - start

    return BenchmarkResult(
        name=name,
        ops=len(latencies) + errors,
        elapsed_s=elapsed,
        errors=errors,
        error_sample=error_sample,
        latencies_ns=latencies,
        times_s=times,
        latencies_by_op=latencies_by_op,
    )
