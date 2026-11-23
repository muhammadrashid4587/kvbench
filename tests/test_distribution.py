"""Tests for the Zipfian key distribution sampler."""

import random
import unittest

from kvbench.distribution import ZipfianSampler


class TestZipfianSampler(unittest.TestCase):
    def test_skew_one_concentrates_on_hot_keys(self):
        # With 1000 keys and skew 1.0 the hottest 1% (10 keys) should
        # take ~39% of picks (H_10 / H_1000). 100k seeded picks give
        # ~0.15pp sampling error, so a ±5pp band is generous.
        sampler = ZipfianSampler(1000, skew=1.0)
        rng = random.Random(42)
        picks = [sampler.pick(rng) for _ in range(100_000)]
        hot = sum(1 for rank in picks if rank < 10)
        self.assertAlmostEqual(hot / len(picks), 0.39, delta=0.05)
        # And the coldest half of the keyspace takes only a small
        # share: sum_{r=500}^{999} 1/(r+1) / H_1000 is about 9%.
        cold = sum(1 for rank in picks if rank >= 500)
        self.assertAlmostEqual(cold / len(picks), 0.09, delta=0.03)

    def test_expected_hot_share_matches_empirical(self):
        sampler = ZipfianSampler(1000, skew=1.0)
        self.assertAlmostEqual(sampler.hot_share(), 0.39, delta=0.01)
        self.assertAlmostEqual(sampler.hot_share(0.1), 0.69, delta=0.01)

    def test_skew_zero_is_uniform(self):
        # Skew 0 makes every rank equally likely: the top 1% takes
        # ~1% of picks, same as any other 1% slice.
        sampler = ZipfianSampler(1000, skew=0.0)
        rng = random.Random(7)
        picks = [sampler.pick(rng) for _ in range(100_000)]
        hot = sum(1 for rank in picks if rank < 10)
        self.assertAlmostEqual(hot / len(picks), 0.01, delta=0.005)
        self.assertAlmostEqual(sampler.hot_share(), 0.01, delta=0.001)

    def test_higher_skew_concentrates_more(self):
        # Skew 1.5 pushes more mass onto the hottest keys than 1.0.
        mild = ZipfianSampler(1000, skew=1.0)
        steep = ZipfianSampler(1000, skew=1.5)
        self.assertGreater(steep.hot_share(), mild.hot_share())
        self.assertGreater(steep.hot_share(), 0.5)

    def test_picks_stay_in_range(self):
        sampler = ZipfianSampler(20, skew=1.0)
        rng = random.Random(1)
        for _ in range(10_000):
            rank = sampler.pick(rng)
            self.assertGreaterEqual(rank, 0)
            self.assertLess(rank, 20)

    def test_single_key_keyspace(self):
        # One key: every pick is rank 0 and the hot share is 100%.
        sampler = ZipfianSampler(1, skew=1.0)
        rng = random.Random(1)
        self.assertEqual(sampler.pick(rng), 0)
        self.assertEqual(sampler.hot_share(), 1.0)

    def test_validation(self):
        with self.assertRaises(ValueError):
            ZipfianSampler(0, skew=1.0)
        with self.assertRaises(ValueError):
            ZipfianSampler(100, skew=-0.5)


if __name__ == "__main__":
    unittest.main()
