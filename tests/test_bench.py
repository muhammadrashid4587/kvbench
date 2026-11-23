"""Tests for the kvbench workload runner and dict baseline."""

import time
import unittest

from kvbench.bench import DictClient, DictStore, Workload, \
    format_duration, run_workload


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

    def test_mix_ratio_honored_within_tolerance(self):
        # A 20:80:0 mix over 5000 ops should land near 20% SET /
        # 80% GET, with no INCRs at all. Tolerance is wide (±5pp)
        # against the ~0.6pp sampling error at this size.
        workload = Workload(ops=5000, clients=4, keys=50, value_size=8,
                            set_weight=20, get_weight=80, incr_weight=0)
        result = run_workload(DictClient, workload, "baseline (dict)")
        per_op = result.per_op_summary
        self.assertEqual(set(per_op), {"SET", "GET"})
        set_share = per_op["SET"]["count"] / result.ops
        get_share = per_op["GET"]["count"] / result.ops
        self.assertAlmostEqual(set_share, 0.20, delta=0.05)
        self.assertAlmostEqual(get_share, 0.80, delta=0.05)
        self.assertEqual(
            per_op["SET"]["count"] + per_op["GET"]["count"], result.ops)

    def test_per_op_stats_split_latencies(self):
        workload = Workload(ops=2000, clients=4, keys=20, value_size=8,
                            set_weight=50, get_weight=40, incr_weight=10)
        result = run_workload(DictClient, workload, "baseline (dict)")
        per_op = result.per_op_summary
        self.assertEqual(set(per_op), {"SET", "GET", "INCR"})
        total = sum(s["count"] for s in per_op.values())
        self.assertEqual(total, result.ops)
        self.assertEqual(total, result.summary["count"])
        for op, s in per_op.items():
            self.assertGreater(s["count"], 0)
            self.assertGreater(s["ops_per_sec"], 0)
            self.assertGreaterEqual(s["mean_ms"], 0)
            self.assertGreaterEqual(s["p50_ms"], 0)
            self.assertGreaterEqual(s["p95_ms"], s["p50_ms"])
            self.assertGreaterEqual(s["p99_ms"], s["p95_ms"])
        # Per-op ops/sec adds back up to the overall rate.
        combined = sum(s["ops_per_sec"] for s in per_op.values())
        self.assertAlmostEqual(combined, result.ops_per_sec, delta=1.0)

    def test_per_op_empty_for_unrun_ops(self):
        # A SET-only mix reports SET alone; no empty op rows.
        workload = Workload(ops=200, clients=2, keys=20, value_size=8,
                            set_weight=100, get_weight=0, incr_weight=0)
        result = run_workload(DictClient, workload, "baseline (dict)")
        self.assertEqual(set(result.per_op_summary), {"SET"})
        self.assertEqual(result.per_op_summary["SET"]["count"], 200)

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

    def test_client_factory_failure_surfaces(self):
        # A client that cannot connect used to die silently inside the
        # worker thread and produce a 0-op result; the caller must see
        # the failure instead.
        def failing_factory():
            raise ConnectionError("connection refused")

        workload = Workload(ops=10, clients=2, keys=5)
        with self.assertRaises(ConnectionError):
            run_workload(failing_factory, workload, "unreachable")

    def test_time_series_counts_all_ops(self):
        workload = Workload(ops=200, clients=4, keys=20, value_size=8)
        result = run_workload(DictClient, workload, "baseline (dict)")
        series = result.time_series
        self.assertGreater(len(series), 0)
        self.assertEqual(series[0]["second"], 0)
        self.assertEqual(sum(b["ops"] for b in series), 200)
        self.assertEqual(len(result.times_s), 200)
        # Bucket indices are contiguous from second 0.
        self.assertEqual([b["second"] for b in series],
                         list(range(len(series))))
        with self.assertRaises(ValueError):
            Workload(ops=0, clients=1, keys=10)
        with self.assertRaises(ValueError):
            Workload(ops=10, clients=1, keys=10,
                     set_weight=0, get_weight=0, incr_weight=0)
        with self.assertRaises(ValueError):
            Workload(ops=10, clients=1, keys=10, pipeline=0)


class TestZipfianDistribution(unittest.TestCase):
    def test_zipfian_run_reports_hot_hit_rate(self):
        # Zipfian with skew 1.0 over 1000 keys: the hottest 1% should
        # take ~39% of ops. 20000 ops give ~0.3pp sampling error, so
        # a ±5pp band is generous.
        workload = Workload(ops=20000, clients=4, keys=1000,
                            value_size=8, distribution="zipfian",
                            zipf_skew=1.0)
        result = run_workload(DictClient, workload, "baseline (dict)")
        self.assertEqual(result.ops, 20000)
        self.assertEqual(result.errors, 0)
        self.assertAlmostEqual(result.hot_hit_rate, 0.39, delta=0.05)
        self.assertEqual(result.hot_ops,
                         round(result.hot_hit_rate * result.ops))

    def test_uniform_run_hot_rate_is_about_one_percent(self):
        # Uniform picking lands ~1% of ops on the hottest 1% of keys.
        workload = Workload(ops=20000, clients=4, keys=1000,
                            value_size=8)
        result = run_workload(DictClient, workload, "baseline (dict)")
        self.assertAlmostEqual(result.hot_hit_rate, 0.01, delta=0.005)

    def test_zipfian_validation(self):
        with self.assertRaises(ValueError):
            Workload(ops=10, clients=1, keys=10,
                     distribution="normal")
        with self.assertRaises(ValueError):
            Workload(ops=10, clients=1, keys=10,
                     distribution="zipfian", zipf_skew=-1.0)


class TestHotKeyLatency(unittest.TestCase):
    def test_zipfian_hot_key_is_rank_zero(self):
        # Under zipfian the hottest key by access count is rank 0:
        # it takes the largest share of ops, far above the rest.
        workload = Workload(ops=20000, clients=4, keys=1000,
                            value_size=8, distribution="zipfian",
                            zipf_skew=1.0)
        result = run_workload(DictClient, workload, "baseline (dict)")
        hot = result.hot_key_summary()
        self.assertEqual(len(hot), 5)
        self.assertEqual(hot[0]["key"], "kvbench:0")
        # Counts descend by rank; rank 0 alone takes ~13% at skew 1.
        counts = [entry["count"] for entry in hot]
        self.assertEqual(counts, sorted(counts, reverse=True))
        self.assertGreater(hot[0]["count"], 2000)
        for entry in hot:
            self.assertGreater(entry["count"], 0)
            self.assertGreaterEqual(entry["mean_ms"], 0)
            self.assertGreaterEqual(entry["p50_ms"], 0)
            self.assertGreaterEqual(entry["p95_ms"], entry["p50_ms"])
            self.assertGreaterEqual(entry["p99_ms"], entry["p95_ms"])
        # Per-key counts add back up to the total op count.
        total = sum(len(samples)
                    for samples in result.latencies_by_key.values())
        self.assertEqual(total, result.ops)

    def test_hot_key_summary_top_n_and_zero(self):
        workload = Workload(ops=2000, clients=2, keys=20, value_size=8)
        result = run_workload(DictClient, workload, "baseline (dict)")
        self.assertEqual(len(result.hot_key_summary(3)), 3)
        self.assertEqual(result.hot_key_summary(0), [])
        self.assertEqual(len(result.hot_key_summary(100)),
                         min(100, len(result.latencies_by_key)))

    def test_hot_keys_zero_disables_tracking(self):
        workload = Workload(ops=200, clients=2, keys=20, value_size=8,
                            hot_keys=0)
        result = run_workload(DictClient, workload, "baseline (dict)")
        self.assertEqual(result.latencies_by_key, {})
        self.assertEqual(result.hot_key_summary(), [])
        self.assertEqual(result.ops, 200)

    def test_hot_keys_validation(self):
        with self.assertRaises(ValueError):
            Workload(ops=10, clients=1, keys=10, hot_keys=-1)

    def test_warmup_excluded_from_per_key_stats(self):
        # Warmup ops run against the backend but must not land in
        # the per-key stats: counts still add up to the measured ops.
        workload = Workload(ops=200, clients=2, keys=20, value_size=8,
                            warmup_s=0.2)
        result = run_workload(DictClient, workload, "baseline (dict)")
        self.assertGreater(result.warmup_ops, 0)
        total = sum(len(samples)
                    for samples in result.latencies_by_key.values())
        self.assertEqual(total, result.ops)


class TestDurationMode(unittest.TestCase):
    def test_duration_run_stops_near_requested_time(self):
        # A 0.5s run against the fast dict baseline should stop
        # close to the deadline with a real op count and throughput.
        workload = Workload(clients=2, keys=20, value_size=8,
                            duration_s=0.5)
        self.assertEqual(workload.ops, 0)  # no fixed op target
        result = run_workload(DictClient, workload, "baseline (dict)")
        self.assertGreater(result.ops, 0)
        self.assertEqual(result.errors, 0)
        self.assertEqual(result.summary["count"], result.ops)
        self.assertGreater(result.ops_per_sec, 0)
        # Elapsed is measured from thread start to join, so it lands
        # just past the 0.5s deadline (in-flight batch at most).
        self.assertGreaterEqual(result.elapsed_s, 0.4)
        self.assertLess(result.elapsed_s, 2.0)

    def test_duration_run_with_pipeline(self):
        workload = Workload(clients=2, keys=20, value_size=8,
                            duration_s=0.5, pipeline=10)
        result = run_workload(DictClient, workload, "baseline (dict)")
        self.assertGreater(result.ops, 0)
        self.assertEqual(result.errors, 0)
        self.assertGreaterEqual(result.elapsed_s, 0.4)
        self.assertLess(result.elapsed_s, 2.0)

    def test_duration_validation(self):
        with self.assertRaises(ValueError):
            Workload(duration_s=0)
        with self.assertRaises(ValueError):
            Workload(duration_s=-5)
        # Clients and keys still need to be positive in duration mode.
        with self.assertRaises(ValueError):
            Workload(clients=0, duration_s=1)
        with self.assertRaises(ValueError):
            Workload(keys=0, duration_s=1)

    def test_format_duration(self):
        self.assertEqual(format_duration(10), "10s")
        self.assertEqual(format_duration(10.0), "10s")
        self.assertEqual(format_duration(1.5), "1.5s")
        self.assertEqual(format_duration(120), "2m")
        self.assertEqual(format_duration(90), "90s")


class TestWarmupMode(unittest.TestCase):
    def test_warmup_excluded_from_results(self):
        # A 0.3s warmup runs real ops, but none of them land in the
        # measured result: ops, latencies and elapsed cover the
        # measured phase only.
        workload = Workload(ops=200, clients=2, keys=20, value_size=8,
                            warmup_s=0.3)
        result = run_workload(DictClient, workload, "baseline (dict)")
        self.assertEqual(result.ops, 200)
        self.assertEqual(result.errors, 0)
        self.assertEqual(result.summary["count"], 200)
        self.assertGreater(result.warmup_ops, 0)
        self.assertEqual(result.summary["count"] + result.warmup_ops,
                         result.ops + result.warmup_ops)
        # Elapsed excludes the 0.3s warmup: the measured 200 dict ops
        # finish far faster than the warmup took.
        self.assertLess(result.elapsed_s, 1.0)
        self.assertLess(max(result.times_s), 1.0)

    def test_warmup_with_duration_mode(self):
        # In duration mode the duration clock starts after warmup
        # ends, so elapsed is the duration, not duration + warmup.
        workload = Workload(clients=2, keys=20, value_size=8,
                            duration_s=0.5, warmup_s=0.3)
        result = run_workload(DictClient, workload, "baseline (dict)")
        self.assertGreater(result.ops, 0)
        self.assertEqual(result.errors, 0)
        self.assertGreater(result.warmup_ops, 0)
        self.assertGreaterEqual(result.elapsed_s, 0.4)
        self.assertLess(result.elapsed_s, 2.0)

    def test_warmup_keeps_measured_workload_identical(self):
        # The measured phase is re-seeded after warmup, so the same
        # workload draws the same keys and op mix with or without
        # a warmup: per-op counts and hot counts match exactly.
        warmed = Workload(ops=2000, clients=2, keys=20, value_size=8,
                          warmup_s=0.2)
        plain = Workload(ops=2000, clients=2, keys=20, value_size=8)
        warmed_result = run_workload(DictClient, warmed,
                                     "baseline (dict)")
        plain_result = run_workload(DictClient, plain, "baseline (dict)")
        # Latencies differ run to run, but the op counts per type
        # must be identical: the same keys and op mix were drawn.
        warmed_counts = {op: s["count"] for op, s in
                         warmed_result.per_op_summary.items()}
        plain_counts = {op: s["count"] for op, s in
                        plain_result.per_op_summary.items()}
        self.assertEqual(warmed_counts, plain_counts)
        self.assertEqual(warmed_result.hot_ops, plain_result.hot_ops)

    def test_warmup_validation(self):
        with self.assertRaises(ValueError):
            Workload(ops=10, clients=1, keys=10, warmup_s=-1)
        # Zero warmup is the default: no warmup phase at all.
        workload = Workload(ops=10, clients=1, keys=10, warmup_s=0)
        result = run_workload(DictClient, workload, "baseline (dict)")
        self.assertEqual(result.warmup_ops, 0)


if __name__ == "__main__":
    unittest.main()
