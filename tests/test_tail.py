"""Tests for kvbench tail-latency attribution.

A fake backend injects slow responses for one op type, one client,
one key, or one time window, and the attribution must name that
factor as the dominant one. The sleeps are small (10-20 ms) and the
op counts are small, so each test runs in about a second.
"""

import json
import time
import unittest

from kvbench.bench import BenchmarkResult, Workload, run_workload
from kvbench.report import render_json, render_markdown


class SlowFakeClient:
    """A dict backend that sleeps on commands matching a predicate.

    ``slow`` is called as ``slow(op, key, call_number)`` and returns
    True when this command should be slow. No ``pipeline`` method:
    the runner falls back to one command at a time, so every op is
    timed on its own. Key rank is the trailing ``:<rank>`` of the
    key (``kvbench:0`` -> 0, ``kvbench:counter:3`` -> 3).
    """

    def __init__(self, slow, sleep_s=0.01):
        self._slow = slow
        self._sleep_s = sleep_s
        self._calls = 0
        self._data = {}

    @staticmethod
    def _rank(key):
        return int(key.rsplit(":", 1)[-1])

    def command(self, *args):
        self._calls += 1
        op = args[0]
        key = args[1]
        if self._slow(op, self._rank(key), self._calls):
            time.sleep(self._sleep_s)
        if op == "SET":
            self._data[key] = args[2]
            return "OK"
        if op == "GET":
            return self._data.get(key)
        if op == "INCR":
            value = int(self._data.get(key, "0")) + 1
            self._data[key] = str(value)
            return value
        raise ValueError(f"unsupported op: {op}")

    def close(self):
        pass


class TestTailAttributionUnit(unittest.TestCase):
    def test_synthetic_slow_op_is_named(self):
        # Hand-built result, no sleeps: 50 SETs at 10 ms, 50 GETs at
        # 0.1 ms. The tail is all SET, so the finding must name SET.
        result = BenchmarkResult(
            name="synthetic",
            ops=100,
            elapsed_s=0.5,
            latencies_ns=[10_000_000] * 50 + [100_000] * 50,
            times_s=[0.01 * i for i in range(100)],
            latencies_by_op={
                "SET": [10_000_000] * 50,
                "GET": [100_000] * 50,
            },
            latencies_by_client={0: [10_000_000] * 50 + [100_000] * 50},
            hot_latencies_ns=[100_000] * 2,
            cold_latencies_ns=[10_000_000] * 50 + [100_000] * 48,
        )
        tail = result.tail_report
        self.assertEqual(tail["slow_ops"], 50)
        self.assertIsNotNone(tail["dominant"])
        self.assertEqual(tail["dominant"]["dimension"], "operation")
        self.assertEqual(tail["dominant"]["factor"], "SET")
        self.assertIn("SET dominates the tail", tail["finding"])
        self.assertEqual(tail["time"]["verdict"], "spread out")

    def test_flat_run_has_no_dominant_factor(self):
        # All latencies identical across ops: every factor's slow
        # share equals its ops share, so nothing dominates.
        result = BenchmarkResult(
            name="flat",
            ops=100,
            elapsed_s=0.01,
            latencies_ns=[100_000] * 100,
            times_s=[0.001 * i for i in range(100)],
            latencies_by_op={
                "SET": [100_000] * 50,
                "GET": [100_000] * 50,
            },
            latencies_by_client={0: [100_000] * 100},
            hot_latencies_ns=[100_000],
            cold_latencies_ns=[100_000] * 99,
        )
        tail = result.tail_report
        self.assertEqual(tail["slow_ops"], 100)
        self.assertIsNone(tail["dominant"])
        self.assertEqual(tail["finding"],
                         "no single factor dominates the tail")

    def test_empty_result_has_no_dominant_factor(self):
        result = BenchmarkResult(name="empty", ops=0, elapsed_s=0.0)
        tail = result.tail_report
        self.assertEqual(tail["slow_ops"], 0)
        self.assertIsNone(tail["dominant"])
        self.assertEqual(tail["finding"], "no samples to attribute")
        self.assertEqual(tail["by_op"], [])
        self.assertEqual(tail["time"]["verdict"], "no samples")


class TestTailAttributionLive(unittest.TestCase):
    def test_slow_op_type_is_named(self):
        # Only SET sleeps (10 ms); GET stays fast. 200 ops, one
        # client: the tail must be attributed to SET.
        factory = lambda: SlowFakeClient(
            lambda op, rank, n: op == "SET", sleep_s=0.01)
        workload = Workload(ops=200, clients=1, keys=100, value_size=8,
                            set_weight=50, get_weight=50, incr_weight=0)
        result = run_workload(factory, workload, "fake")
        tail = result.tail_report
        self.assertIsNotNone(tail["dominant"])
        self.assertEqual(tail["dominant"]["dimension"], "operation")
        self.assertEqual(tail["dominant"]["factor"], "SET")
        set_entry = next(e for e in tail["by_op"]
                         if e["factor"] == "SET")
        get_entry = next(e for e in tail["by_op"]
                         if e["factor"] == "GET")
        self.assertGreater(set_entry["p99_ms"], 5.0)
        self.assertLess(get_entry["p99_ms"], 5.0)
        self.assertGreater(set_entry["slow_share"], 0.9)

    def test_slow_client_is_named(self):
        # The first client created sleeps on every command (10 ms);
        # the other is fast. 200 ops over 2 clients: the tail must
        # be attributed to one client, the slow one.
        state = {"created": 0}

        def factory():
            state["created"] += 1
            if state["created"] == 1:
                return SlowFakeClient(lambda op, rank, n: True,
                                      sleep_s=0.01)
            return SlowFakeClient(lambda op, rank, n: False)

        workload = Workload(ops=200, clients=2, keys=100, value_size=8)
        result = run_workload(factory, workload, "fake")
        tail = result.tail_report
        self.assertIsNotNone(tail["dominant"])
        self.assertEqual(tail["dominant"]["dimension"], "client")
        slow_client = tail["dominant"]["factor"]
        slow_entry = next(e for e in tail["by_client"]
                          if e["factor"] == slow_client)
        fast_entry = next(e for e in tail["by_client"]
                          if e["factor"] != slow_client)
        self.assertGreater(slow_entry["p99_ms"], 5.0)
        self.assertLess(fast_entry["p99_ms"], 5.0)
        self.assertGreater(slow_entry["slow_share"], 0.9)

    def test_slow_hot_key_is_named(self):
        # Only rank 0 (the hottest key) sleeps (10 ms), under a
        # zipfian distribution so rank 0 takes ~19% of ops. The tail
        # must be attributed to hot keys.
        factory = lambda: SlowFakeClient(
            lambda op, rank, n: rank == 0, sleep_s=0.01)
        workload = Workload(ops=200, clients=1, keys=100, value_size=8,
                            distribution="zipfian", zipf_skew=1.0)
        result = run_workload(factory, workload, "fake")
        tail = result.tail_report
        self.assertIsNotNone(tail["dominant"])
        self.assertEqual(tail["dominant"]["dimension"], "keyspace")
        self.assertEqual(tail["dominant"]["factor"], "hot")
        hot_entry = next(e for e in tail["by_segment"]
                         if e["factor"] == "hot")
        cold_entry = next(e for e in tail["by_segment"]
                          if e["factor"] == "cold")
        self.assertGreater(hot_entry["p99_ms"], 5.0)
        self.assertLess(cold_entry["p99_ms"], 5.0)
        self.assertGreater(hot_entry["slow_share"], 0.9)

    def test_slow_time_window_is_clustered(self):
        # The first 60 calls sleep 20 ms (~1.2 s), then everything
        # is fast: slow ops cluster in the first seconds of a run
        # that spans more than one second.
        factory = lambda: SlowFakeClient(
            lambda op, rank, n: n <= 60, sleep_s=0.02)
        workload = Workload(ops=2000, clients=1, keys=100, value_size=8)
        result = run_workload(factory, workload, "fake")
        tail = result.tail_report
        self.assertEqual(tail["time"]["verdict"], "clustered")
        self.assertIsNotNone(tail["dominant"])
        self.assertEqual(tail["dominant"]["dimension"], "time")
        self.assertGreater(tail["time"]["concentration"], 2.0)

    def test_evenly_slow_ops_are_spread_out(self):
        # Every 20th call sleeps 5 ms, evenly through the run: slow
        # ops are spread over time, so no time window dominates.
        factory = lambda: SlowFakeClient(
            lambda op, rank, n: n % 20 == 0, sleep_s=0.005)
        workload = Workload(ops=2000, clients=1, keys=100, value_size=8)
        result = run_workload(factory, workload, "fake")
        tail = result.tail_report
        self.assertEqual(tail["time"]["verdict"], "spread out")
        self.assertLess(tail["time"]["concentration"], 2.0)

    def test_fast_uniform_run_has_no_dominant_factor(self):
        # A fast backend with no injected slowness: slow ops are
        # just the tail of one distribution, so nothing dominates.
        # SET-only over one client, so the only dimension that
        # could fire is hot vs cold keys — and with ~1% hot ops
        # the hot share of the tail stays at its baseline.
        workload = Workload(ops=2000, clients=1, keys=100, value_size=8,
                            set_weight=100, get_weight=0, incr_weight=0)
        result = run_workload(
            lambda: SlowFakeClient(lambda op, rank, n: False),
            workload, "fake")
        tail = result.tail_report
        self.assertIsNone(tail["dominant"])
        self.assertEqual(tail["finding"],
                         "no single factor dominates the tail")


class TestTailReportOutput(unittest.TestCase):
    def test_tail_section_in_markdown_and_json(self):
        workload = Workload(ops=100, clients=2, keys=10, value_size=8,
                            set_weight=50, get_weight=40, incr_weight=10)
        result = BenchmarkResult(
            name="cinder (127.0.0.1:6379)",
            ops=100,
            elapsed_s=0.01,
            latencies_ns=[10_000_000] * 50 + [100_000] * 50,
            times_s=[0.005 * i for i in range(100)],
            latencies_by_op={
                "SET": [10_000_000] * 50,
                "GET": [100_000] * 50,
            },
            latencies_by_client={
                0: [10_000_000] * 25 + [100_000] * 25,
                1: [10_000_000] * 25 + [100_000] * 25,
            },
            hot_latencies_ns=[100_000],
            cold_latencies_ns=[10_000_000] * 50 + [100_000] * 49,
        )
        text = render_markdown([result], workload, "kvbench")
        self.assertIn("## Tail-latency attribution", text)
        self.assertIn("SET dominates the tail", text)
        self.assertIn("| Op | Ops | p99 (ms) | Slow ops | Slow share |",
                      text)
        self.assertIn("| Client | Ops | p99 (ms) | Slow ops | Slow share |",
                      text)
        self.assertIn("| Keys | Ops | p99 (ms) | Slow ops | Slow share |",
                      text)
        parsed = json.loads(render_json([result], workload, "kvbench"))
        tail = parsed["results"][0]["tail"]
        self.assertEqual(tail["dominant"]["dimension"], "operation")
        self.assertEqual(tail["dominant"]["factor"], "SET")
        self.assertEqual(tail["slow_ops"], 50)
        self.assertIn("SET dominates the tail", tail["finding"])

    def test_tail_section_absent_for_empty_result(self):
        workload = Workload(ops=100, clients=2, keys=10, value_size=8)
        result = BenchmarkResult(name="empty", ops=0, elapsed_s=0.0)
        text = render_markdown([result], workload, "kvbench")
        self.assertNotIn("## Tail-latency attribution", text)


if __name__ == "__main__":
    unittest.main()
