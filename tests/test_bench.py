"""Tests for the kvbench workload runner and dict baseline."""

import unittest

from kvbench.bench import DictClient, DictStore, Workload, run_workload


class TestDictBaseline(unittest.TestCase):
    def test_set_get_incr(self):
        store = DictStore()
        self.assertEqual(store.command("SET", "k", "v"), "OK")
        self.assertEqual(store.command("GET", "k"), "v")
        self.assertIsNone(store.command("GET", "missing"))
        self.assertEqual(store.command("INCR", "n"), 1)
        self.assertEqual(store.command("INCR", "n"), 2)
        self.assertEqual(store.command("PING"), "PONG")

    def test_baseline_run_completes(self):
        workload = Workload(ops=200, clients=4, keys=20, value_size=8)
        result = run_workload(DictClient, workload, "baseline (dict)")
        self.assertEqual(result.ops, 200)
        self.assertEqual(result.errors, 0)
        self.assertGreater(result.ops_per_sec, 0)
        self.assertEqual(result.summary["count"], 200)
        self.assertGreaterEqual(result.summary["p50_ms"], 0)

    def test_pipelined_run_completes(self):
        workload = Workload(ops=200, clients=4, keys=20, value_size=8,
                            pipeline=10)
        result = run_workload(DictClient, workload, "baseline (dict)")
        self.assertEqual(result.ops, 200)
        self.assertEqual(result.errors, 0)
        self.assertGreater(result.ops_per_sec, 0)
        self.assertEqual(result.summary["count"], 200)

    def test_pipelined_run_with_uneven_split(self):
        # 203 ops across 4 clients with depth 10 leaves partial batches
        # on every thread; every op must still be counted exactly once.
        workload = Workload(ops=203, clients=4, keys=20, value_size=8,
                            pipeline=10)
        result = run_workload(DictClient, workload, "baseline (dict)")
        self.assertEqual(result.ops, 203)
        self.assertEqual(result.errors, 0)
        self.assertEqual(result.summary["count"], 203)

    def test_pipeline_depth_one_matches_sequential(self):
        workload = Workload(ops=200, clients=4, keys=20, value_size=8,
                            pipeline=1)
        result = run_workload(DictClient, workload, "baseline (dict)")
        self.assertEqual(result.ops, 200)
        self.assertEqual(result.errors, 0)
        self.assertEqual(result.summary["count"], 200)

    def test_dict_client_pipeline_captures_errors(self):
        client = DictClient()
        replies = client.pipeline([
            ("SET", "k", "v"),
            ("BOGUS", "k"),
            ("GET", "k"),
        ])
        self.assertEqual(replies[0], "OK")
        self.assertIsInstance(replies[1], ValueError)
        self.assertEqual(replies[2], "v")

    def test_invalid_workload(self):
        with self.assertRaises(ValueError):
            Workload(ops=0, clients=1, keys=10)
        with self.assertRaises(ValueError):
            Workload(ops=10, clients=1, keys=10,
                     set_weight=0, get_weight=0, incr_weight=0)
        with self.assertRaises(ValueError):
            Workload(ops=10, clients=1, keys=10, pipeline=0)


if __name__ == "__main__":
    unittest.main()
