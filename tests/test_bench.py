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

    def test_invalid_workload(self):
        with self.assertRaises(ValueError):
            Workload(ops=0, clients=1, keys=10)
        with self.assertRaises(ValueError):
            Workload(ops=10, clients=1, keys=10,
                     set_weight=0, get_weight=0, incr_weight=0)


if __name__ == "__main__":
    unittest.main()
