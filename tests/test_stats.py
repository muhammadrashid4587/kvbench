"""Tests for kvbench.stats percentile math."""

import unittest

from kvbench.stats import percentile, summarize


class TestPercentile(unittest.TestCase):
    def test_known_data(self):
        data = sorted(range(1, 101))  # 1..100
        self.assertEqual(percentile(data, 50), 50)
        self.assertEqual(percentile(data, 95), 95)
        self.assertEqual(percentile(data, 99), 99)
        self.assertEqual(percentile(data, 100), 100)

    def test_single_sample(self):
        self.assertEqual(percentile([7], 50), 7)
        self.assertEqual(percentile([7], 99), 7)

    def test_small_data(self):
        # nearest-rank: ceil(0.5 * 4) - 1 = index 1
        self.assertEqual(percentile([10, 20, 30, 40], 50), 20)
        # ceil(0.95 * 4) - 1 = index 3
        self.assertEqual(percentile([10, 20, 30, 40], 95), 40)

    def test_empty_raises(self):
        with self.assertRaises(ValueError):
            percentile([], 50)

    def test_bad_p_raises(self):
        with self.assertRaises(ValueError):
            percentile([1, 2], 0)
        with self.assertRaises(ValueError):
            percentile([1, 2], 101)


class TestSummarize(unittest.TestCase):
    def test_known_latencies(self):
        # 1..10 ms in ns
        lat = [i * 1_000_000 for i in range(1, 11)]
        s = summarize(lat)
        self.assertEqual(s["count"], 10)
        self.assertAlmostEqual(s["mean_ms"], 5.5)
        self.assertAlmostEqual(s["p50_ms"], 5.0)
        self.assertAlmostEqual(s["p95_ms"], 10.0)
        self.assertAlmostEqual(s["min_ms"], 1.0)
        self.assertAlmostEqual(s["max_ms"], 10.0)

    def test_empty(self):
        s = summarize([])
        self.assertEqual(s["count"], 0)
        self.assertEqual(s["mean_ms"], 0.0)


if __name__ == "__main__":
    unittest.main()
