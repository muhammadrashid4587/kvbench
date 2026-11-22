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

from .stats import summarize


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

    def __post_init__(self):
        if self.ops <= 0 or self.clients <= 0 or self.keys <= 0:
            raise ValueError("ops, clients and keys must be positive")
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

    @property
    def ops_per_sec(self):
        return self.ops / self.elapsed_s if self.elapsed_s > 0 else 0.0

    @property
    def summary(self):
        return summarize(self.latencies_ns)


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
    """
    per_thread = workload.ops // workload.clients
    remainder = workload.ops % workload.clients
    latencies = []
    errors = 0
    error_sample = ""
    lock = threading.Lock()

    def worker(thread_index, op_count):
        nonlocal errors, error_sample
        rng = random.Random(workload.seed + thread_index)
        value = "x" * workload.value_size
        client = client_factory()
        local_lat = []
        local_errors = 0
        local_sample = ""
        try:
            for _ in range(op_count):
                op = _pick_op(rng, workload)
                # Counters live in their own keyspace: INCR on a key
                # previously SET to a non-numeric value would error,
                # exactly as it does on a real Redis server.
                if op == "INCR":
                    key = f"kvbench:counter:{rng.randrange(workload.keys)}"
                else:
                    key = f"kvbench:{rng.randrange(workload.keys)}"
                start = time.perf_counter_ns()
                try:
                    if op == "SET":
                        client.command("SET", key, value)
                    elif op == "GET":
                        client.command("GET", key)
                    else:
                        client.command("INCR", key)
                except Exception as exc:  # noqa: BLE001 - count and continue
                    local_errors += 1
                    if not local_sample:
                        local_sample = f"{type(exc).__name__}: {exc}"
                    continue
                local_lat.append(time.perf_counter_ns() - start)
        finally:
            client.close()
        with lock:
            latencies.extend(local_lat)
            errors += local_errors
            if local_sample and not error_sample:
                error_sample = local_sample

    threads = []
    start = time.perf_counter()
    for i in range(workload.clients):
        count = per_thread + (1 if i < remainder else 0)
        t = threading.Thread(target=worker, args=(i, count))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()
    elapsed = time.perf_counter() - start

    return BenchmarkResult(
        name=name,
        ops=len(latencies) + errors,
        elapsed_s=elapsed,
        errors=errors,
        error_sample=error_sample,
        latencies_ns=latencies,
    )
