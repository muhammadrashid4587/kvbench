"""Key distribution samplers.

Real workloads are skewed: a small hot set of keys takes most of the
traffic (the classic YCSB Zipfian workload). Uniform key picking hides
that — a server that caches hot keys well looks no different from one
that does not. This module provides the sampler behind
``--distribution zipfian``.
"""

import bisect


class ZipfianSampler:
    """Zipfian key sampler over ranks ``0..n-1``.

    Rank ``r`` (0 is the hottest key) is picked with probability
    proportional to ``1 / (r + 1) ** skew``. With the default skew of
    1.0 and 1000 keys, the hottest 1% of keys take about 39% of the
    picks; skew 0 degenerates to uniform.

    Built by inverse-transform sampling over the exact CDF, so the
    empirical distribution matches the Zipf law for any keyspace size
    and skew. Construction is O(n) and each sample is O(log n) via
    bisect — cheap enough to build one sampler per worker thread.
    """

    def __init__(self, n, skew=1.0):
        if n <= 0:
            raise ValueError("keyspace must be positive")
        if skew < 0:
            raise ValueError("zipf skew must be non-negative")
        weights = [1.0 / (rank + 1) ** skew for rank in range(n)]
        total = sum(weights)
        cdf = []
        acc = 0.0
        for weight in weights:
            acc += weight / total
            cdf.append(acc)
        # Guard the last entry against float drift so pick() can never
        # return an out-of-range rank.
        cdf[-1] = 1.0
        self.cdf = cdf
        self.n = n
        self.skew = skew

    def pick(self, rng):
        """Return a key rank in ``0..n-1`` using ``rng.random()``."""
        return bisect.bisect_left(self.cdf, rng.random())

    def hot_share(self, hot_fraction=0.01):
        """Expected share of picks landing in the hottest keys.

        ``hot_fraction`` is the share of the keyspace counted as hot
        (default 1%): the returned value is the probability mass of
        the hottest ``max(1, int(n * hot_fraction))`` ranks.
        """
        hot = max(1, int(self.n * hot_fraction))
        return self.cdf[hot - 1]
