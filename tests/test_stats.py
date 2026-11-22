"""Tests for kvbench.stats percentile math."""

import unittest

from kvbench.stats import histogram, percentile, summarize


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


class TestHistogram(unittest.TestCase):
    def test_known_latencies(self):
        # 0.05, 0.1, 0.2, 0.6, 2.0 ms and one 2000 ms outlier.
        lat = [50_000, 100_000, 200_000, 600_000, 2_000_000,
               2_000_000_000]
        buckets = histogram(lat)
        by_upper = {b["upper_ms"]: b["count"] for b in buckets}
        self.assertEqual(by_upper[0.1], 2)   # 0.05 and 0.1 ms
        self.assertEqual(by_upper[0.25], 1)  # 0.2 ms
        self.assertEqual(by_upper[0.5], 0)
        self.assertEqual(by_upper[1], 1)     # 0.6 ms
        self.assertEqual(by_upper[2], 1)     # 2.0 ms, edge inclusive
        self.assertEqual(by_upper[None], 1)  # overflow: 2000 ms
        self.assertEqual(sum(by_upper.values()), 6)

    def test_trailing_empty_buckets_dropped(self):
        # All samples fast: only buckets up to the last non-empty one.
        buckets = histogram([100_000] * 10)
        self.assertEqual(len(buckets), 1)
        self.assertEqual(buckets[0], {"upper_ms": 0.1, "count": 10})

    def test_empty(self):
        self.assertEqual(histogram([]), [])


if __name__ == "__main__":
    unittest.main()
