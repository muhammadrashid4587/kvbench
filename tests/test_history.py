"""Tests for kvbench.history: recording runs and the trend view."""

import io
import json
import os
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout

from kvbench import __version__
from kvbench.bench import BenchmarkResult, Workload
from kvbench.cli import main
from kvbench.history import DEFAULT_HISTORY_FILE, \
    best_worst_comparison, build_record, export_history, \
    format_best_worst, format_history, format_import, \
    format_trend, import_history, load_export, load_history, \
    record_run, trend_comparison, write_best_worst, \
    write_best_worst_json, write_trend, write_trend_json, \
    _sparkline


def _make_result(name="cinder (127.0.0.1:6379)", ops=1000,
                 ops_per_sec=50000.0, latencies_by_op=None,
                 latency_ns=20_000):
    # 1000 samples of latency_ns each: mean/p50/p95/p99 all
    # latency_ns / 1e6 ms (0.020 ms at the 20_000 ns default).
    return BenchmarkResult(
        name=name, ops=ops, elapsed_s=ops / ops_per_sec,
        latencies_ns=[latency_ns] * ops,
        latencies_by_op=latencies_by_op or {})


def _make_mixed_result(ops_per_sec=50000.0, latency_ns=20_000):
    # A 50:40:10 SET:GET:INCR mix, so per-op trends have three ops
    # to track: SET 500 samples, GET 400, INCR 100, all latency_ns.
    return _make_result(
        ops=1000, ops_per_sec=ops_per_sec,
        latencies_by_op={
            "SET": [latency_ns] * 500,
            "GET": [latency_ns] * 400,
            "INCR": [latency_ns] * 100,
        })


def _make_workload():
    return Workload(ops=1000, clients=4, keys=1000, value_size=32,
                    set_weight=50, get_weight=40, incr_weight=10,
                    seed=1234, pipeline=1)


class TestBuildRecord(unittest.TestCase):
    def test_record_carries_result_and_workload(self):
        record = build_record(_make_result(), _make_workload(),
                              "kvbench --ops 1000")
        self.assertEqual(record["tool"], "kvbench")
        self.assertEqual(record["version"], __version__)
        self.assertEqual(record["backend"], "cinder (127.0.0.1:6379)")
        self.assertEqual(record["ops"], 1000)
        self.assertEqual(record["ops_per_sec"], 50000.0)
        self.assertEqual(record["p99_ms"], 0.020)
        self.assertEqual(record["errors"], 0)
        self.assertEqual(record["workload"]["clients"], 4)
        self.assertEqual(record["workload"]["keys"], 1000)
        self.assertEqual(record["workload"]["mix"]["set"], 50)
        self.assertIn("timestamp", record)

    def test_duration_mode_records_null_ops(self):
        workload = _make_workload()
        workload.duration_s = 10.0
        record = build_record(_make_result(), workload, "kvbench")
        self.assertIsNone(record["workload"]["ops"])
        self.assertEqual(record["workload"]["duration_s"], 10.0)

    def test_record_carries_per_op_stats(self):
        # Elapsed is 1000 / 50000 = 0.02 s, so SET at 500 samples
        # runs 25,000 ops/sec, GET 20,000, INCR 5,000.
        record = build_record(_make_mixed_result(), _make_workload(),
                              "kvbench --ops 1000")
        per_op = record["per_op"]
        self.assertEqual(per_op["SET"]["ops"], 500)
        self.assertEqual(per_op["SET"]["ops_per_sec"], 25000.0)
        self.assertEqual(per_op["SET"]["p99_ms"], 0.020)
        self.assertEqual(per_op["GET"]["ops"], 400)
        self.assertEqual(per_op["GET"]["ops_per_sec"], 20000.0)
        self.assertEqual(per_op["INCR"]["ops"], 100)
        self.assertEqual(per_op["INCR"]["ops_per_sec"], 5000.0)

    def test_record_with_no_per_op_latencies_has_empty_per_op(self):
        record = build_record(_make_result(), _make_workload(),
                              "kvbench --ops 1000")
        self.assertEqual(record["per_op"], {})

    def test_record_carries_per_op_histograms(self):
        # The same log-spaced buckets as the JSON report's
        # histograms_by_op: 500 SET samples of 20_000 ns (0.020 ms)
        # all land in the <= 0.1 ms bucket.
        record = build_record(_make_mixed_result(), _make_workload(),
                              "kvbench --ops 1000")
        histograms = record["histograms_by_op"]
        self.assertEqual(histograms["SET"],
                         [{"upper_ms": 0.1, "count": 500}])
        self.assertEqual(histograms["GET"],
                         [{"upper_ms": 0.1, "count": 400}])
        self.assertEqual(histograms["INCR"],
                         [{"upper_ms": 0.1, "count": 100}])

    def test_record_with_no_per_op_latencies_has_empty_histograms(self):
        record = build_record(_make_result(), _make_workload(),
                              "kvbench --ops 1000")
        self.assertEqual(record["histograms_by_op"], {})

    def test_record_carries_sweep_steps(self):
        # Only the numbers the trend needs are kept — ops, ops/sec,
        # mean and p99 per step, plus the step's per-operation
        # latency histograms — not the repeat detail, which already
        # lives in the JSON report.
        step_histograms = {"SET": [{"upper_ms": 0.1, "count": 1000}]}
        result = _make_result()
        result.sweep = [
            {"keys": 100, "ops": 1000, "ops_per_sec": 40000.0,
             "mean_ms": 0.020, "p99_ms": 0.020,
             "histograms_by_op": step_histograms},
            {"keys": 1000, "ops": 1000, "ops_per_sec": 60000.0,
             "mean_ms": 0.020, "p99_ms": 0.020,
             "histograms_by_op": step_histograms,
             "repeat": {"runs": [], "spread": {}}},
        ]
        result.client_sweep = [
            {"clients": 4, "ops": 1000, "ops_per_sec": 50000.0,
             "mean_ms": 0.020, "p99_ms": 0.020,
             "histograms_by_op": step_histograms},
        ]
        result.value_sweep = [
            {"value_size": 128, "ops": 1000, "ops_per_sec": 45000.0,
             "mean_ms": 0.020, "p99_ms": 0.020,
             "histograms_by_op": step_histograms},
        ]
        result.pipeline_sweep = [
            {"pipeline": 5, "ops": 1000, "ops_per_sec": 55000.0,
             "mean_ms": 0.020, "p99_ms": 0.020,
             "histograms_by_op": step_histograms},
        ]
        result.mix_sweep = [
            {"mix": "90:10", "ops": 1000, "ops_per_sec": 52000.0,
             "mean_ms": 0.020, "p99_ms": 0.020,
             "histograms_by_op": step_histograms},
        ]
        record = build_record(result, _make_workload(), "kvbench")
        self.assertEqual(record["sweeps"], {
            "keyspace": [
                {"keys": 100, "ops": 1000, "ops_per_sec": 40000.0,
                 "mean_ms": 0.020, "p99_ms": 0.020,
                 "histograms_by_op": step_histograms},
                {"keys": 1000, "ops": 1000, "ops_per_sec": 60000.0,
                 "mean_ms": 0.020, "p99_ms": 0.020,
                 "histograms_by_op": step_histograms},
            ],
            "clients": [
                {"clients": 4, "ops": 1000, "ops_per_sec": 50000.0,
                 "mean_ms": 0.020, "p99_ms": 0.020,
                 "histograms_by_op": step_histograms},
            ],
            "value_size": [
                {"value_size": 128, "ops": 1000,
                 "ops_per_sec": 45000.0, "mean_ms": 0.020,
                 "p99_ms": 0.020,
                 "histograms_by_op": step_histograms},
            ],
            "pipeline": [
                {"pipeline": 5, "ops": 1000,
                 "ops_per_sec": 55000.0, "mean_ms": 0.020,
                 "p99_ms": 0.020,
                 "histograms_by_op": step_histograms},
            ],
            "mix": [
                {"mix": "90:10", "ops": 1000,
                 "ops_per_sec": 52000.0, "mean_ms": 0.020,
                 "p99_ms": 0.020,
                 "histograms_by_op": step_histograms},
            ],
        })

    def test_record_sweep_step_without_histograms_key(self):
        # A step that carries no histograms_by_op — a hand-built
        # result, or a run recorded before 0.49.0 — records no
        # key for it, so the trend can tell it apart from a step
        # recorded with no per-op latencies.
        result = _make_result()
        result.sweep = [
            {"keys": 100, "ops": 1000, "ops_per_sec": 40000.0,
             "mean_ms": 0.020, "p99_ms": 0.020},
        ]
        record = build_record(result, _make_workload(), "kvbench")
        self.assertEqual(record["sweeps"], {
            "keyspace": [
                {"keys": 100, "ops": 1000, "ops_per_sec": 40000.0,
                 "mean_ms": 0.020, "p99_ms": 0.020},
            ],
        })

    def test_record_with_no_sweeps_has_no_sweeps_key(self):
        # A run with no sweep keeps the old record shape exactly.
        record = build_record(_make_result(), _make_workload(),
                              "kvbench --ops 1000")
        self.assertNotIn("sweeps", record)


class TestRecordAndLoad(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, "history.jsonl")

    def tearDown(self):
        self.tmp.cleanup()

    def test_round_trip_one_record_per_result(self):
        workload = _make_workload()
        count = record_run(
            self.path,
            [_make_result(), _make_result(name="baseline (dict)")],
            workload, "kvbench --ops 1000")
        self.assertEqual(count, 2)
        entries = load_history(self.path)
        self.assertEqual(len(entries), 2)
        self.assertEqual(entries[0]["backend"],
                         "cinder (127.0.0.1:6379)")
        self.assertEqual(entries[1]["backend"], "baseline (dict)")

    def test_appends_across_runs_in_order(self):
        workload = _make_workload()
        record_run(self.path, [_make_result(ops_per_sec=40000.0)],
                   workload, "kvbench")
        record_run(self.path, [_make_result(ops_per_sec=60000.0)],
                   workload, "kvbench")
        entries = load_history(self.path)
        self.assertEqual([e["ops_per_sec"] for e in entries],
                         [40000.0, 60000.0])

    def test_creates_parent_directory(self):
        nested = os.path.join(self.tmp.name, "a", "b", "history.jsonl")
        record_run(nested, [_make_result()], _make_workload(), "kvbench")
        self.assertEqual(len(load_history(nested)), 1)

    def test_missing_file_loads_empty(self):
        self.assertEqual(load_history(self.path), [])

    def test_corrupt_lines_are_skipped(self):
        with open(self.path, "w", encoding="utf-8") as f:
            f.write("not json at all\n")
            f.write(json.dumps({"backend": "cinder", "ops_per_sec": 1})
                    + "\n")
            f.write("[1, 2, 3]\n")
            f.write("\n")
        entries = load_history(self.path)
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0]["backend"], "cinder")


class TestFormatHistory(unittest.TestCase):
    def _entries(self):
        workload = _make_workload()
        return [
            build_record(_make_result(ops_per_sec=40000.0), workload,
                         "kvbench"),
            build_record(_make_result(ops_per_sec=50000.0), workload,
                         "kvbench"),
        ]

    def test_empty_history_message(self):
        self.assertIn("No runs recorded yet", format_history([]))

    def test_table_shows_backend_and_ops_sec(self):
        text = format_history(self._entries())
        self.assertIn("cinder (127.0.0.1:6379)", text)
        self.assertIn("50,000", text)
        self.assertIn("40,000", text)

    def test_delta_against_previous_run_of_same_backend(self):
        # 50000 vs 40000 is +25.0%; the first run has no previous.
        text = format_history(self._entries())
        self.assertIn("+25.0%", text)
        self.assertIn("| - |", text)

    def test_delta_ignores_other_backends(self):
        workload = _make_workload()
        entries = [
            build_record(_make_result(name="baseline (dict)",
                                      ops_per_sec=90000.0),
                         workload, "kvbench"),
            build_record(_make_result(ops_per_sec=40000.0), workload,
                         "kvbench"),
        ]
        text = format_history(entries)
        # The cinder run is the first of its backend: no delta.
        self.assertNotIn("+", text)
        self.assertEqual(text.count("| - |"), 2)

    def test_limit_keeps_most_recent(self):
        workload = _make_workload()
        entries = [
            build_record(_make_result(ops_per_sec=float(10000 + i)),
                         workload, "kvbench")
            for i in range(5)
        ]
        text = format_history(entries, limit=2)
        self.assertIn("10,004", text)
        self.assertIn("10,003", text)
        self.assertNotIn("10,000", text)


class TestSparkline(unittest.TestCase):
    def test_rising_values_climb_the_blocks(self):
        self.assertEqual(_sparkline([0, 50, 100]), "▁▅█")

    def test_flat_values_are_the_middle_block(self):
        self.assertEqual(_sparkline([100, 100, 100]), "▅▅▅")

    def test_single_value_is_one_middle_block(self):
        self.assertEqual(_sparkline([42]), "▅")

    def test_empty_values_have_no_sparkline(self):
        self.assertEqual(_sparkline([]), "-")


class TestFormatTrend(unittest.TestCase):
    def _entries(self, rates=(40000.0, 50000.0, 60000.0)):
        workload = _make_workload()
        return [
            build_record(_make_result(ops_per_sec=rate), workload,
                         "kvbench")
            for rate in rates
        ]

    def test_empty_history_message(self):
        self.assertIn("No runs recorded yet", format_trend([]))

    def test_single_run_has_no_delta_and_one_block(self):
        text = format_trend(self._entries(rates=(50000.0,)))
        self.assertIn("50,000", text)
        self.assertIn("0.020", text)  # p99 alongside
        self.assertIn("| - |", text)
        self.assertIn("Trend (oldest → newest): ▅", text)
        self.assertNotIn("First → last", text)

    def test_table_has_delta_and_p99_per_run(self):
        # 50000 vs 40000 is +25.0%, 60000 vs 50000 is +20.0%.
        text = format_trend(self._entries())
        self.assertIn("+25.0%", text)
        self.assertIn("+20.0%", text)
        self.assertIn("0.020", text)
        self.assertIn("40,000", text)
        self.assertIn("60,000", text)

    def test_sparkline_runs_oldest_to_newest(self):
        text = format_trend(self._entries())
        self.assertIn("Trend (oldest → newest): ▁▅█", text)
        self.assertIn(
            "First → last: 40,000 → 60,000 ops/sec (+50.0%)", text)

    def test_overall_p99_first_to_last_change(self):
        # A backend whose tail moved shows a p99 first-to-last
        # change beside the ops/sec one, so a backend that got
        # slower in the tail reads at a glance even when its
        # throughput rose — here ops/sec is up 25% while p99 is
        # up 50%.
        workload = _make_workload()
        entries = [
            build_record(_make_result(ops_per_sec=40000.0,
                                      latency_ns=20_000),
                         workload, "kvbench"),
            build_record(_make_result(ops_per_sec=50000.0,
                                      latency_ns=30_000),
                         workload, "kvbench"),
        ]
        text = format_trend(entries)
        self.assertIn(
            "First → last: 40,000 → 50,000 ops/sec (+25.0%)", text)
        self.assertIn(
            "p99 first → last: 0.020 → 0.030 ms (+50.0%)", text)

    def test_overall_single_run_has_no_p99_first_to_last(self):
        # One run has no first-to-last change to report, for p99
        # just as for ops/sec.
        text = format_trend(self._entries(rates=(50000.0,)))
        self.assertNotIn("p99 first → last", text)

    def test_overall_p99_missing_shows_no_p99_line(self):
        # A run whose recorded p99 is missing contributes no p99
        # value, so with both runs missing there is no p99 line
        # — while the ops/sec first-to-last change still renders.
        workload = _make_workload()
        entries = [
            build_record(_make_result(ops_per_sec=rate), workload,
                         "kvbench")
            for rate in (40000.0, 50000.0)
        ]
        for entry in entries:
            del entry["p99_ms"]
        text = format_trend(entries)
        self.assertIn(
            "First → last: 40,000 → 50,000 ops/sec (+25.0%)", text)
        self.assertNotIn("p99 first → last", text)

    def test_each_backend_gets_its_own_section(self):
        workload = _make_workload()
        entries = [
            build_record(_make_result(name="baseline (dict)",
                                      ops_per_sec=90000.0),
                         workload, "kvbench"),
            build_record(_make_result(name="baseline (dict)",
                                      ops_per_sec=100000.0),
                         workload, "kvbench"),
            build_record(_make_result(ops_per_sec=40000.0), workload,
                         "kvbench"),
        ]
        text = format_trend(entries)
        self.assertIn("### baseline (dict)", text)
        self.assertIn("### cinder (127.0.0.1:6379)", text)
        # The baseline delta is its own: +11.1%, not polluted by
        # the cinder run between them; both backends' first runs
        # have no previous run to compare against.
        self.assertIn("+11.1%", text)
        self.assertEqual(text.count("| - |"), 2)

    def test_limit_keeps_most_recent_runs_per_backend(self):
        workload = _make_workload()
        entries = [
            build_record(_make_result(ops_per_sec=float(10000 + i)),
                         workload, "kvbench")
            for i in range(5)
        ]
        text = format_trend(entries, limit=2)
        self.assertIn("10,004", text)
        self.assertIn("10,003", text)
        self.assertNotIn("10,000", text)
        self.assertIn("Trend (oldest → newest): ▁█", text)

    def test_per_op_sections_track_each_op_separately(self):
        workload = _make_workload()
        entries = [
            build_record(_make_mixed_result(ops_per_sec=40000.0),
                         workload, "kvbench"),
            build_record(_make_mixed_result(ops_per_sec=50000.0),
                         workload, "kvbench"),
        ]
        text = format_trend(entries)
        # SET: 500 samples over 0.025 s is 20,000 ops/sec, over
        # 0.02 s is 25,000 — a +25.0% delta, same as the overall
        # trend; GET 16,000 -> 20,000 and INCR 4,000 -> 5,000.
        self.assertIn("#### SET ops/sec trend", text)
        self.assertIn("#### GET ops/sec trend", text)
        self.assertIn("#### INCR ops/sec trend", text)
        self.assertIn("20,000", text)
        self.assertIn("25,000", text)
        self.assertIn("16,000", text)
        self.assertIn("4,000", text)
        self.assertIn("SET trend (oldest → newest): ▁█", text)
        self.assertIn("SET first → last: 20,000 → 25,000 ops/sec "
                      "(+25.0%)", text)
        self.assertIn("GET first → last: 16,000 → 20,000 ops/sec "
                      "(+25.0%)", text)

    def test_per_op_delta_skips_runs_without_that_op(self):
        workload = _make_workload()
        entries = [
            build_record(_make_mixed_result(ops_per_sec=40000.0),
                         workload, "kvbench"),
            # A read-only run in between: no SET or INCR samples.
            build_record(_make_result(
                ops_per_sec=60000.0,
                latencies_by_op={"GET": [20_000] * 1000}),
                workload, "kvbench"),
            build_record(_make_mixed_result(ops_per_sec=50000.0),
                         workload, "kvbench"),
        ]
        text = format_trend(entries)
        # SET's delta is 25,000 vs the first run's 20,000 (+25.0%),
        # not against the read-only run, which never ran SET. The
        # SET section shows only the two mixed runs.
        self.assertIn("SET first → last: 20,000 → 25,000 ops/sec "
                      "(+25.0%)", text)
        self.assertEqual(text.count("#### SET ops/sec trend"), 1)
        # The read-only run still appears in the GET section.
        self.assertIn("GET first → last: 16,000 → 20,000 ops/sec "
                      "(+25.0%)", text)

    def test_per_op_p99_first_to_last_change(self):
        # An op whose tail moved shows a p99 first-to-last change
        # beside its ops/sec one, so a slow write reads at a
        # glance even when the op's throughput rose — here every
        # op's ops/sec is up 25% while its p99 is up 50%.
        workload = _make_workload()
        entries = [
            build_record(_make_mixed_result(ops_per_sec=40000.0,
                                            latency_ns=20_000),
                         workload, "kvbench"),
            build_record(_make_mixed_result(ops_per_sec=50000.0,
                                            latency_ns=30_000),
                         workload, "kvbench"),
        ]
        text = format_trend(entries)
        self.assertIn(
            "SET p99 first → last: 0.020 → 0.030 ms (+50.0%)", text)
        self.assertIn(
            "GET p99 first → last: 0.020 → 0.030 ms (+50.0%)", text)
        self.assertIn(
            "INCR p99 first → last: 0.020 → 0.030 ms (+50.0%)",
            text)

    def test_per_op_p99_missing_shows_no_per_op_p99_line(self):
        # Per-op p99s missing from the record leave no per-op
        # p99 line — while the overall p99 line still renders,
        # since the overall p99 is recorded.
        workload = _make_workload()
        entries = [
            build_record(_make_mixed_result(ops_per_sec=rate),
                         workload, "kvbench")
            for rate in (40000.0, 50000.0)
        ]
        for entry in entries:
            for op_entry in entry["per_op"].values():
                del op_entry["p99_ms"]
        text = format_trend(entries)
        self.assertIn(
            "p99 first → last: 0.020 → 0.020 ms (+0.0%)", text)
        self.assertNotIn("SET p99 first → last", text)
        self.assertNotIn("GET p99 first → last", text)
        self.assertNotIn("INCR p99 first → last", text)

    def test_records_without_per_op_render_no_per_op_sections(self):
        # History written before 0.22.0 has no "per_op" key: the
        # overall trend still renders and nothing crashes.
        workload = _make_workload()
        entries = [
            build_record(_make_result(ops_per_sec=rate), workload,
                         "kvbench")
            for rate in (40000.0, 50000.0)
        ]
        for entry in entries:
            del entry["per_op"]
        text = format_trend(entries)
        self.assertIn("40,000", text)
        self.assertIn("+25.0%", text)
        self.assertNotIn("#### SET ops/sec trend", text)
        self.assertNotIn("#### GET ops/sec trend", text)
        self.assertNotIn("#### INCR ops/sec trend", text)

    def test_trend_shows_latest_runs_per_op_histograms(self):
        # Two mixed runs; the latest run's SET latencies are
        # slower (200_000 ns lands in the 0.25 ms bucket, the
        # first run's 20_000 ns in the 0.1 ms bucket), so the
        # latest run's block shows the slower shape — and the
        # first-to-last comparison below it shows the first
        # run's shape, so the two can be read side by side.
        workload = _make_workload()
        first = _make_mixed_result(ops_per_sec=40000.0)
        latest = _make_result(
            ops=1000, ops_per_sec=50000.0,
            latencies_by_op={
                "SET": [200_000] * 500,
                "GET": [20_000] * 400,
                "INCR": [20_000] * 100,
            })
        entries = [
            build_record(first, workload, "kvbench"),
            build_record(latest, workload, "kvbench"),
        ]
        text = format_trend(entries)
        timestamp = entries[-1]["timestamp"][:19].replace("T", " ")
        self.assertIn(
            "#### Per-op latency histogram — latest run "
            f"({timestamp})",
            text)
        self.assertIn("SET (500 ops)", text)
        self.assertIn("GET (400 ops)", text)
        self.assertIn("INCR (100 ops)", text)
        # The latest run's SET shape, not the first run's.
        self.assertIn("- <= 0.25 ms: 500 (100.0%)", text)
        self.assertEqual(
            text.count("#### Per-op latency histogram — latest run"),
            1)
        # The comparison below it carries the first run's shape.
        self.assertIn(
            "#### Per-op histogram comparison — first vs last run",
            text)
        self.assertIn("- <= 0.1 ms: 500 (100.0%)", text)

    def test_trend_without_histograms_names_the_reason(self):
        # A history record written before 0.43.0 carries no
        # "histograms_by_op" key: the trend still renders, with
        # a note saying why there is no histogram block.
        workload = _make_workload()
        entries = [
            build_record(_make_mixed_result(), workload, "kvbench"),
        ]
        del entries[0]["histograms_by_op"]
        text = format_trend(entries)
        self.assertIn("recorded before 0.43.0", text)
        self.assertNotIn("Per-op latency histogram — latest run",
                         text)

    def test_trend_with_no_per_op_latencies_names_the_reason(self):
        # A run recorded with no per-op latencies carries an
        # empty histograms mapping: a note, not a block.
        workload = _make_workload()
        entries = [
            build_record(_make_result(), workload, "kvbench"),
        ]
        self.assertEqual(entries[0]["histograms_by_op"], {})
        text = format_trend(entries)
        self.assertIn("recorded no per-op latencies", text)
        self.assertNotIn("Per-op latency histogram — latest run",
                         text)

    def test_trend_with_malformed_histograms_names_the_reason(self):
        # A hand-made history file may carry a histograms
        # mapping that is not op name to bucket list: a note
        # naming that, instead of a crash.
        workload = _make_workload()
        entries = [
            build_record(_make_mixed_result(), workload, "kvbench"),
        ]
        entries[0]["histograms_by_op"] = {"SET": {"upper_ms": 0.1}}
        text = format_trend(entries)
        self.assertIn("histograms are malformed", text)
        self.assertNotIn("Per-op latency histogram — latest run",
                         text)

    def test_trend_compares_first_and_last_run_histograms(self):
        # Two mixed runs; the latest run's SET latencies are
        # slower (200_000 ns lands in the 0.25 ms bucket, the
        # first run's 20_000 ns in the 0.1 ms bucket), while GET
        # and INCR stay put — the comparison table names SET as
        # the op that moved, and the first run's block shows
        # where it started.
        workload = _make_workload()
        first = _make_mixed_result(ops_per_sec=40000.0)
        latest = _make_result(
            ops=1000, ops_per_sec=50000.0,
            latencies_by_op={
                "SET": [200_000] * 500,
                "GET": [20_000] * 400,
                "INCR": [20_000] * 100,
            })
        entries = [
            build_record(first, workload, "kvbench"),
            build_record(latest, workload, "kvbench"),
        ]
        text = format_trend(entries)
        first_ts = entries[0]["timestamp"][:19].replace("T", " ")
        self.assertIn(
            "#### Per-op histogram comparison — first vs last run",
            text)
        # SET p50/p99 0.020 -> 0.200 ms (+900.0%); its whole
        # share moved out of the <= 0.1 ms bucket.
        self.assertIn(
            "| SET | 0.020 | 0.200 | 0.020 | 0.200 | +900.0% | "
            "-100.0 pp (<= 0.1 ms) |",
            text)
        # GET did not move at all: +0.0% and no bucket drift.
        self.assertIn(
            "| GET | 0.020 | 0.020 | 0.020 | 0.020 | +0.0% | "
            "+0.0 pp (<= 0.1 ms) |",
            text)
        self.assertIn(
            "#### Per-op latency histogram — first run "
            f"({first_ts})",
            text)
        self.assertEqual(
            text.count("#### Per-op latency histogram — first run"),
            1)
        # The first run's SET shape, not the latest run's.
        self.assertIn("- <= 0.1 ms: 500 (100.0%)", text)

    def test_trend_comparison_with_single_run_names_no_comparison(self):
        # One run in the window is both first and last: there is
        # nothing to compare, so the trend says so instead of
        # rendering a comparison table.
        workload = _make_workload()
        entries = [
            build_record(_make_mixed_result(), workload, "kvbench"),
        ]
        text = format_trend(entries)
        self.assertIn("no first-to-last histogram comparison", text)
        self.assertNotIn(
            "#### Per-op histogram comparison — first vs last run",
            text)

    def test_trend_comparison_without_histograms_names_the_reason(self):
        # The first run was recorded before 0.43.0 and carries no
        # "histograms_by_op" key: the comparison renders a note
        # naming that instead of a table.
        workload = _make_workload()
        entries = [
            build_record(_make_mixed_result(ops_per_sec=40000.0),
                         workload, "kvbench"),
            build_record(_make_mixed_result(ops_per_sec=50000.0),
                         workload, "kvbench"),
        ]
        del entries[0]["histograms_by_op"]
        text = format_trend(entries)
        self.assertIn("recorded before 0.43.0", text)
        self.assertIn(
            "no first-to-last histogram comparison is shown", text)
        self.assertNotIn(
            "#### Per-op histogram comparison — first vs last run",
            text)

    def test_trend_comparison_with_no_per_op_latencies_names_the_reason(self):
        # The last run recorded no per-op latencies and carries an
        # empty histograms mapping: a note, not a comparison.
        workload = _make_workload()
        entries = [
            build_record(_make_mixed_result(ops_per_sec=40000.0),
                         workload, "kvbench"),
            build_record(_make_result(ops_per_sec=50000.0),
                         workload, "kvbench"),
        ]
        self.assertEqual(entries[1]["histograms_by_op"], {})
        text = format_trend(entries)
        self.assertIn("recorded no per-op latencies", text)
        self.assertIn(
            "no first-to-last histogram comparison is shown", text)
        self.assertNotIn(
            "#### Per-op histogram comparison — first vs last run",
            text)

    def test_trend_comparison_with_malformed_histograms_names_the_reason(self):
        # The first run's recorded histograms are not op name to
        # bucket list: a note naming that, instead of a crash.
        workload = _make_workload()
        entries = [
            build_record(_make_mixed_result(ops_per_sec=40000.0),
                         workload, "kvbench"),
            build_record(_make_mixed_result(ops_per_sec=50000.0),
                         workload, "kvbench"),
        ]
        entries[0]["histograms_by_op"] = {"SET": {"upper_ms": 0.1}}
        text = format_trend(entries)
        self.assertIn("histograms are malformed", text)
        self.assertIn(
            "no first-to-last histogram comparison is shown", text)
        self.assertNotIn(
            "#### Per-op histogram comparison — first vs last run",
            text)

    def test_trend_comparison_marks_ops_missing_from_one_run(self):
        # A read-only last run never ran SET: the SET row shows
        # the first run's numbers with "-" for the last run's
        # side, and no drift, instead of inventing numbers.
        workload = _make_workload()
        entries = [
            build_record(_make_mixed_result(ops_per_sec=40000.0),
                         workload, "kvbench"),
            build_record(_make_result(
                ops_per_sec=50000.0,
                latencies_by_op={"GET": [20_000] * 1000}),
                workload, "kvbench"),
        ]
        text = format_trend(entries)
        self.assertIn(
            "| SET | 0.020 | - | 0.020 | - | - | - |", text)
        self.assertIn(
            "| GET | 0.020 | 0.020 | 0.020 | 0.020 | +0.0% | "
            "+0.0 pp (<= 0.1 ms) |",
            text)

    def test_each_backend_gets_its_own_latest_histogram(self):
        workload = _make_workload()
        entries = [
            build_record(_make_mixed_result(ops_per_sec=40000.0),
                         workload, "kvbench"),
            build_record(_make_result(
                name="baseline (dict)", ops_per_sec=90000.0,
                latencies_by_op={"GET": [20_000] * 1000}),
                workload, "kvbench"),
        ]
        text = format_trend(entries)
        self.assertEqual(
            text.count("#### Per-op latency histogram — latest run"),
            2)
        self.assertIn("GET (1000 ops)", text)

    def test_corrupt_lines_never_reach_the_chart(self):
        # load_history skips the corrupt line; the trend renders
        # from the records that survived.
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "history.jsonl")
            with open(path, "w", encoding="utf-8") as f:
                f.write("not json at all\n")
                for entry in self._entries(rates=(40000.0, 50000.0)):
                    f.write(json.dumps(entry) + "\n")
                f.write("[1, 2, 3]\n")
            text = format_trend(load_history(path))
            self.assertIn("40,000", text)
            self.assertIn("50,000", text)
            self.assertIn("+25.0%", text)


class TestSweepTrend(unittest.TestCase):
    def _sweep_result(self, ops_per_sec, keys=100, clients=None,
                      value_size=None, pipeline=None, mix=None,
                      p99_ms=0.020):
        result = _make_result(ops_per_sec=ops_per_sec)
        if keys is not None:
            result.sweep = [{"keys": keys, "ops": 1000,
                             "ops_per_sec": ops_per_sec,
                             "mean_ms": 0.020, "p99_ms": p99_ms,
                             "histograms_by_op": {}}]
        if clients is not None:
            result.client_sweep = [{"clients": clients, "ops": 1000,
                                    "ops_per_sec": ops_per_sec,
                                    "mean_ms": 0.020, "p99_ms": p99_ms,
                                    "histograms_by_op": {}}]
        if value_size is not None:
            result.value_sweep = [{"value_size": value_size,
                                   "ops": 1000,
                                   "ops_per_sec": ops_per_sec,
                                   "mean_ms": 0.020, "p99_ms": p99_ms,
                                   "histograms_by_op": {}}]
        if pipeline is not None:
            result.pipeline_sweep = [{"pipeline": pipeline,
                                      "ops": 1000,
                                      "ops_per_sec": ops_per_sec,
                                      "mean_ms": 0.020, "p99_ms": p99_ms,
                                      "histograms_by_op": {}}]
        if mix is not None:
            result.mix_sweep = [{"mix": mix,
                                 "ops": 1000,
                                 "ops_per_sec": ops_per_sec,
                                 "mean_ms": 0.020, "p99_ms": p99_ms,
                                 "histograms_by_op": {}}]
        return result

    def test_sweep_recorded_then_trended_across_two_runs(self):
        # The core of the feature: a sweep step recorded in one
        # run and again in the next shows up in the trend with a
        # delta and a first-to-last change, in the same shape as
        # the overall trend table.
        workload = _make_workload()
        entries = [
            build_record(self._sweep_result(40000.0), workload,
                         "kvbench"),
            build_record(self._sweep_result(50000.0), workload,
                         "kvbench"),
        ]
        text = format_trend(entries)
        self.assertIn(
            "#### Keyspace sweep — 100 keys ops/sec trend", text)
        self.assertIn("| 40,000 | - | 0.020 |", text)
        self.assertIn("| 50,000 | +25.0% | 0.020 |", text)
        self.assertIn(
            "Keyspace sweep — 100 keys trend (oldest → newest): ▁█",
            text)
        self.assertIn(
            "Keyspace sweep — 100 keys first → last: 40,000 → "
            "50,000 ops/sec (+25.0%)", text)

    def test_sweep_step_p99_first_to_last_change(self):
        # A step whose tail moved shows a p99 first-to-last change
        # beside the ops/sec one, so a step that got slower in
        # the tail reads at a glance even when its throughput
        # rose — here ops/sec is up 25% while p99 is up 50%.
        workload = _make_workload()
        entries = [
            build_record(self._sweep_result(40000.0, p99_ms=0.020),
                         workload, "kvbench"),
            build_record(self._sweep_result(50000.0, p99_ms=0.030),
                         workload, "kvbench"),
        ]
        text = format_trend(entries)
        self.assertIn(
            "Keyspace sweep — 100 keys p99 first → last: 0.020 → "
            "0.030 ms (+50.0%)", text)

    def test_sweep_step_single_run_has_no_p99_first_to_last(self):
        # One run of a step has no first-to-last change to report,
        # for p99 just as for ops/sec.
        workload = _make_workload()
        entries = [
            build_record(self._sweep_result(40000.0), workload,
                         "kvbench"),
        ]
        text = format_trend(entries)
        self.assertIn(
            "#### Keyspace sweep — 100 keys ops/sec trend", text)
        self.assertNotIn("p99 first → last", text)

    def test_sweep_delta_skips_runs_without_that_step(self):
        workload = _make_workload()
        entries = [
            build_record(self._sweep_result(40000.0), workload,
                         "kvbench"),
            # A run with no sweep in between: the step's delta
            # still compares against the first run.
            build_record(_make_result(ops_per_sec=60000.0), workload,
                         "kvbench"),
            build_record(self._sweep_result(60000.0), workload,
                         "kvbench"),
        ]
        text = format_trend(entries)
        self.assertIn(
            "Keyspace sweep — 100 keys first → last: 40,000 → "
            "60,000 ops/sec (+50.0%)", text)
        self.assertEqual(
            text.count("#### Keyspace sweep — 100 keys ops/sec trend"),
            1)

    def test_all_five_sweep_kinds_render(self):
        workload = _make_workload()
        entries = [
            build_record(
                self._sweep_result(40000.0, keys=100, clients=4,
                                   value_size=128, pipeline=5,
                                   mix="90:10"),
                workload, "kvbench"),
            build_record(
                self._sweep_result(50000.0, keys=100, clients=4,
                                   value_size=128, pipeline=5,
                                   mix="90:10"),
                workload, "kvbench"),
        ]
        text = format_trend(entries)
        self.assertIn(
            "#### Keyspace sweep — 100 keys ops/sec trend", text)
        self.assertIn(
            "#### Client sweep — 4 clients ops/sec trend", text)
        self.assertIn(
            "#### Value size sweep — 128 byte values ops/sec trend",
            text)
        self.assertIn(
            "#### Pipeline sweep — 5 commands ops/sec trend", text)
        self.assertIn(
            "#### Mix sweep — 90:10 read:write ops/sec trend", text)
        self.assertIn(
            "Client sweep — 4 clients first → last: 40,000 → "
            "50,000 ops/sec (+25.0%)", text)

    def test_pipeline_sweep_step_trended_across_two_runs(self):
        # A pipeline step recorded in one run and again in the
        # next shows up in the trend with a delta and a
        # first-to-last change, like the other sweep kinds.
        workload = _make_workload()
        entries = [
            build_record(
                self._sweep_result(40000.0, keys=None, pipeline=5),
                workload, "kvbench"),
            build_record(
                self._sweep_result(50000.0, keys=None, pipeline=5),
                workload, "kvbench"),
        ]
        text = format_trend(entries)
        self.assertIn(
            "#### Pipeline sweep — 5 commands ops/sec trend", text)
        self.assertIn("| 40,000 | - | 0.020 |", text)
        self.assertIn("| 50,000 | +25.0% | 0.020 |", text)
        self.assertIn(
            "Pipeline sweep — 5 commands trend (oldest → newest): ▁█",
            text)
        self.assertIn(
            "Pipeline sweep — 5 commands first → last: 40,000 → "
            "50,000 ops/sec (+25.0%)", text)

    def test_mix_sweep_step_trended_across_two_runs(self):
        # A mix step recorded in one run and again in the next
        # shows up in the trend with a delta and a first-to-last
        # change, like the other sweep kinds — the ratio is a
        # string step value, so it renders as "90:10 read:write".
        workload = _make_workload()
        entries = [
            build_record(
                self._sweep_result(40000.0, keys=None, mix="90:10"),
                workload, "kvbench"),
            build_record(
                self._sweep_result(50000.0, keys=None, mix="90:10"),
                workload, "kvbench"),
        ]
        text = format_trend(entries)
        self.assertIn(
            "#### Mix sweep — 90:10 read:write ops/sec trend", text)
        self.assertIn("| 40,000 | - | 0.020 |", text)
        self.assertIn("| 50,000 | +25.0% | 0.020 |", text)
        self.assertIn(
            "Mix sweep — 90:10 read:write trend (oldest → newest): ▁█",
            text)
        self.assertIn(
            "Mix sweep — 90:10 read:write first → last: 40,000 → "
            "50,000 ops/sec (+25.0%)", text)

    def test_records_without_sweeps_render_no_sweep_sections(self):
        # History written before 0.37.0 has no "sweeps" key: the
        # overall trend still renders and nothing crashes.
        workload = _make_workload()
        entries = [
            build_record(_make_result(ops_per_sec=rate), workload,
                         "kvbench")
            for rate in (40000.0, 50000.0)
        ]
        for entry in entries:
            self.assertNotIn("sweeps", entry)
        text = format_trend(entries)
        self.assertIn("40,000", text)
        self.assertIn("+25.0%", text)
        self.assertNotIn("Keyspace sweep —", text)
        self.assertNotIn("Client sweep —", text)
        self.assertNotIn("Value size sweep —", text)
        self.assertNotIn("Pipeline sweep —", text)


class TestSweepStepHistograms(unittest.TestCase):
    def _step_record(self, ops_per_sec, set_count=500,
                     get_count=400, keys=100, histograms="real"):
        # One recorded run whose keyspace step carries per-op
        # histograms: SET set_count samples and GET get_count
        # samples, all in the <= 0.1 ms bucket. ``histograms``
        # picks the shape under test: "real" (valid bucket
        # lists), "empty" (no per-op latencies), "malformed"
        # (a bucket list that is not a list), or "missing" (no
        # key at all — a run recorded before 0.49.0).
        result = _make_result(ops_per_sec=ops_per_sec)
        step = {"keys": keys, "ops": 1000, "ops_per_sec": ops_per_sec,
                "mean_ms": 0.020, "p99_ms": 0.020}
        if histograms == "real":
            step["histograms_by_op"] = {
                "SET": [{"upper_ms": 0.1, "count": set_count}],
                "GET": [{"upper_ms": 0.1, "count": get_count}],
            }
        elif histograms == "empty":
            step["histograms_by_op"] = {}
        elif histograms == "malformed":
            step["histograms_by_op"] = {"SET": {}}
        result.sweep = [step]
        return build_record(result, _make_workload(), "kvbench")

    def test_step_histogram_block_renders_latest_run(self):
        # The core of the feature: each sweep-step section ends
        # with the step's per-op histograms from the latest run
        # that recorded it, drawn with the shared helper — the
        # same log-spaced buckets as the single-run report.
        entries = [
            self._step_record(40000.0, set_count=500),
            self._step_record(50000.0, set_count=300),
        ]
        text = format_trend(entries)
        self.assertIn(
            "#### Keyspace sweep — 100 keys — per-op latency "
            "histogram — latest run", text)
        self.assertIn("SET (300 ops)", text)
        self.assertIn("GET (400 ops)", text)
        self.assertIn("- <= 0.1 ms: 300 (100.0%)", text)
        # The first run's shape is not drawn — only the latest
        # run's is.
        self.assertNotIn("SET (500 ops)", text)

    def test_step_histogram_uses_last_run_that_recorded_it(self):
        # A step that stopped running still shows the shape it
        # last measured: the latest run recorded no sweep at all,
        # so the block comes from the earlier run.
        entries = [
            self._step_record(40000.0, set_count=500),
            build_record(_make_result(ops_per_sec=60000.0),
                         _make_workload(), "kvbench"),
        ]
        text = format_trend(entries)
        self.assertIn("SET (500 ops)", text)
        self.assertIn(
            "#### Keyspace sweep — 100 keys — per-op latency "
            "histogram — latest run", text)

    def test_step_recorded_before_0_49_shows_note(self):
        # A step with no histograms_by_op key was recorded before
        # 0.49.0: the section names that instead of a block.
        entries = [self._step_record(40000.0, histograms="missing")]
        text = format_trend(entries)
        self.assertIn(
            "Keyspace sweep — 100 keys (", text)
        self.assertIn(
            "this step was recorded before 0.49.0, so it carries "
            "no per-op histograms.", text)
        self.assertNotIn(
            "Keyspace sweep — 100 keys — per-op latency histogram",
            text)

    def test_step_with_no_per_op_latencies_shows_note(self):
        # A step recorded with an empty histograms mapping ran
        # no per-op latencies: the section names that instead of
        # a block.
        entries = [self._step_record(40000.0, histograms="empty")]
        text = format_trend(entries)
        self.assertIn(
            "this step recorded no per-op latencies, so there are "
            "no histograms to show.", text)
        self.assertNotIn(
            "Keyspace sweep — 100 keys — per-op latency histogram",
            text)

    def test_step_malformed_histograms_shows_note(self):
        # A hand-made history file may carry something that is
        # not a per-op histogram mapping: the section names that
        # instead of crashing or drawing a block.
        entries = [self._step_record(40000.0,
                                     histograms="malformed")]
        text = format_trend(entries)
        self.assertIn(
            "the recorded per-op histograms for this step are "
            "malformed, so no histograms are shown.", text)
        self.assertNotIn(
            "Keyspace sweep — 100 keys — per-op latency histogram",
            text)

    def test_each_step_gets_its_own_histogram_block(self):
        # Two steps in the same run each get their own block, so
        # a slow step's shape reads separately from a fast one's.
        entries = [
            build_record(self._two_step_result(), _make_workload(),
                         "kvbench"),
        ]
        text = format_trend(entries)
        self.assertIn(
            "#### Keyspace sweep — 100 keys — per-op latency "
            "histogram — latest run", text)
        self.assertIn(
            "#### Keyspace sweep — 1,000 keys — per-op latency "
            "histogram — latest run", text)
        self.assertIn("SET (500 ops)", text)
        self.assertIn("SET (800 ops)", text)

    def _two_step_result(self):
        result = _make_result(ops_per_sec=40000.0)
        result.sweep = [
            {"keys": 100, "ops": 1000, "ops_per_sec": 40000.0,
             "mean_ms": 0.020, "p99_ms": 0.020,
             "histograms_by_op": {
                 "SET": [{"upper_ms": 0.1, "count": 500}]}},
            {"keys": 1000, "ops": 1000, "ops_per_sec": 60000.0,
             "mean_ms": 0.020, "p99_ms": 0.020,
             "histograms_by_op": {
                 "SET": [{"upper_ms": 0.1, "count": 800}]}},
        ]
        return result

    def test_step_histograms_survive_history_round_trip(self):
        # The recorded JSON carries the per-step histograms, so a
        # trend rendered from the file on disk — not just from
        # in-memory records — shows the block.
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "history.jsonl")
            record_run(
                path,
                [self._two_step_result()],
                _make_workload(), "kvbench")
            with open(path, encoding="utf-8") as f:
                stored = json.loads(f.readline())
            self.assertEqual(
                stored["sweeps"]["keyspace"][0]["histograms_by_op"],
                {"SET": [{"upper_ms": 0.1, "count": 500}]})
            text = format_trend(load_history(path))
            self.assertIn("SET (500 ops)", text)
            self.assertIn("SET (800 ops)", text)


class TestWriteTrend(unittest.TestCase):
    def _entries(self, rates=(40000.0, 50000.0, 60000.0)):
        workload = _make_workload()
        return [
            build_record(_make_result(ops_per_sec=rate), workload,
                         "kvbench")
            for rate in rates
        ]

    def test_file_matches_the_stdout_chart(self):
        entries = self._entries()
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "trend.md")
            text = write_trend(path, entries)
            self.assertEqual(text, format_trend(entries))
            with open(path, encoding="utf-8") as f:
                self.assertEqual(f.read(), text)

    def test_empty_history_writes_the_message(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "trend.md")
            text = write_trend(path, [])
            self.assertIn("No runs recorded yet", text)
            with open(path, encoding="utf-8") as f:
                self.assertIn("No runs recorded yet", f.read())

    def test_file_ends_with_a_newline(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "trend.md")
            write_trend(path, [])
            with open(path, encoding="utf-8") as f:
                self.assertTrue(f.read().endswith("\n"))

    def test_creates_parent_directory(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "reports", "trend.md")
            write_trend(path, self._entries())
            with open(path, encoding="utf-8") as f:
                self.assertIn("60,000", f.read())


class TestTrendCli(unittest.TestCase):
    def test_trend_flag_prints_chart_and_exits_zero(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "history.jsonl")
            record_run(path, [_make_result(ops_per_sec=40000.0),
                              _make_result(ops_per_sec=50000.0)],
                       _make_workload(), "kvbench")
            out = io.StringIO()
            with redirect_stdout(out):
                code = main(["--trend", "--history-file", path])
            self.assertEqual(code, 0)
            text = out.getvalue()
            self.assertIn("ops/sec trend", text)
            self.assertIn("+25.0%", text)
            self.assertIn("▁█", text)

    def test_trend_flag_prints_per_op_sections(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "history.jsonl")
            record_run(path, [_make_mixed_result(ops_per_sec=40000.0),
                              _make_mixed_result(ops_per_sec=50000.0)],
                       _make_workload(), "kvbench")
            out = io.StringIO()
            with redirect_stdout(out):
                code = main(["--trend", "--history-file", path])
            self.assertEqual(code, 0)
            text = out.getvalue()
            self.assertIn("#### SET ops/sec trend", text)
            self.assertIn("#### GET ops/sec trend", text)
            self.assertIn("#### INCR ops/sec trend", text)
            self.assertIn("SET first → last: 20,000 → 25,000 ops/sec "
                          "(+25.0%)", text)

    def test_trend_with_empty_history_exits_zero(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "history.jsonl")
            out = io.StringIO()
            with redirect_stdout(out):
                code = main(["--trend", "--history-file", path])
            self.assertEqual(code, 0)
            self.assertIn("No runs recorded yet", out.getvalue())

    def test_trend_out_writes_file_and_still_prints(self):
        with tempfile.TemporaryDirectory() as tmp:
            history = os.path.join(tmp, "history.jsonl")
            record_run(history, [_make_result(ops_per_sec=40000.0),
                                 _make_result(ops_per_sec=50000.0)],
                       _make_workload(), "kvbench")
            out_path = os.path.join(tmp, "trend.md")
            out = io.StringIO()
            with redirect_stdout(out):
                code = main(["--trend", "--history-file", history,
                             "--trend-out", out_path])
            self.assertEqual(code, 0)
            # The chart still prints to stdout, and the same chart
            # lands in the file so it can be committed or attached.
            stdout = out.getvalue()
            self.assertIn("ops/sec trend", stdout)
            self.assertIn("+25.0%", stdout)
            self.assertIn(f"Trend written to {out_path}", stdout)
            with open(out_path, encoding="utf-8") as f:
                file_text = f.read()
            self.assertIn("ops/sec trend", file_text)
            self.assertIn("+25.0%", file_text)
            self.assertIn("▁█", file_text)
            self.assertNotIn("Trend written to", file_text)

    def test_trend_out_without_trend_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            out_path = os.path.join(tmp, "trend.md")
            err = io.StringIO()
            with redirect_stdout(io.StringIO()):
                from contextlib import redirect_stderr
                with redirect_stderr(err):
                    code = main(["--trend-out", out_path])
            self.assertEqual(code, 2)
            self.assertIn("--trend-out needs --trend", err.getvalue())
            self.assertFalse(os.path.exists(out_path))


class TestTrendJson(unittest.TestCase):
    def _step_record(self, ops_per_sec, set_count=500,
                     get_count=400, keys=100, clients=None,
                     value_size=None, pipeline=None, mix=None,
                     histograms="real", p99_ms=0.020,
                     set_buckets=None, get_buckets=None):
        # One recorded run whose sweep step(s) carry per-op
        # histograms: SET set_count samples and GET get_count
        # samples, all in the <= 0.1 ms bucket. ``histograms``
        # picks the shape under test: "real" (valid bucket
        # lists), "empty" (no per-op latencies), "malformed"
        # (a bucket list that is not a list), or "missing" (no
        # key at all — a run recorded before 0.49.0).
        # ``set_buckets``/``get_buckets`` replace the
        # single-bucket lists with explicit ones, so a test can
        # put the two runs' tails in different buckets.
        result = _make_result(ops_per_sec=ops_per_sec)
        step = {"keys": keys, "ops": 1000,
                "ops_per_sec": ops_per_sec,
                "mean_ms": 0.020, "p99_ms": p99_ms}
        if histograms == "real":
            step["histograms_by_op"] = {
                "SET": set_buckets or [
                    {"upper_ms": 0.1, "count": set_count}],
                "GET": get_buckets or [
                    {"upper_ms": 0.1, "count": get_count}],
            }
        elif histograms == "empty":
            step["histograms_by_op"] = {}
        elif histograms == "malformed":
            step["histograms_by_op"] = {"SET": {}}
        result.sweep = [step]
        if clients is not None:
            client_step = dict(step)
            client_step["clients"] = clients
            del client_step["keys"]
            result.client_sweep = [client_step]
        if value_size is not None:
            value_step = dict(step)
            value_step["value_size"] = value_size
            del value_step["keys"]
            result.value_sweep = [value_step]
        if pipeline is not None:
            pipeline_step = dict(step)
            pipeline_step["pipeline"] = pipeline
            del pipeline_step["keys"]
            result.pipeline_sweep = [pipeline_step]
        if mix is not None:
            mix_step = dict(step)
            mix_step["mix"] = mix
            del mix_step["keys"]
            result.mix_sweep = [mix_step]
        return build_record(result, _make_workload(), "kvbench")

    def test_trend_json_carries_runs_deltas_and_first_to_last(self):
        # The core of the JSON side: each backend carries its
        # trend window as data — the runs oldest first, the
        # numeric change versus the previous run, the sparkline
        # and the first-to-last change in ops/sec and in p99 —
        # the same numbers the Markdown chart shows.
        workload = _make_workload()
        entries = [
            build_record(_make_mixed_result(ops_per_sec=40000.0),
                         workload, "kvbench"),
            build_record(_make_mixed_result(ops_per_sec=50000.0),
                         workload, "kvbench"),
        ]
        backend = trend_comparison(entries)["backends"][0]
        self.assertEqual(backend["backend"],
                         "cinder (127.0.0.1:6379)")
        self.assertEqual(len(backend["runs"]), 2)
        self.assertEqual(backend["runs"][0]["ops_per_sec"], 40000.0)
        self.assertIsNone(backend["runs"][0]["ops_sec_delta_pct"])
        self.assertEqual(backend["runs"][1]["ops_per_sec"], 50000.0)
        self.assertAlmostEqual(
            backend["runs"][1]["ops_sec_delta_pct"], 25.0)
        self.assertEqual(backend["runs"][0]["p99_ms"], 0.020)
        self.assertEqual(backend["runs"][1]["p99_ms"], 0.020)
        self.assertEqual(backend["sparkline"], "▁█")
        first_to_last = backend["first_to_last"]
        self.assertEqual(first_to_last["ops_per_sec"]["first"],
                         40000.0)
        self.assertEqual(first_to_last["ops_per_sec"]["last"],
                         50000.0)
        self.assertAlmostEqual(
            first_to_last["ops_per_sec"]["delta_pct"], 25.0)
        self.assertEqual(first_to_last["p99_ms"]["first"], 0.020)
        self.assertEqual(first_to_last["p99_ms"]["last"], 0.020)
        self.assertAlmostEqual(first_to_last["p99_ms"]["delta_pct"],
                               0.0)
        # The Markdown chart renders the same numbers.
        text = format_trend(entries)
        self.assertIn("First → last: 40,000 → 50,000 ops/sec "
                      "(+25.0%)", text)
        self.assertIn("p99 first → last: 0.020 → 0.020 ms (+0.0%)",
                      text)

    def test_trend_json_carries_per_op_sections(self):
        workload = _make_workload()
        entries = [
            build_record(_make_mixed_result(ops_per_sec=40000.0),
                         workload, "kvbench"),
            build_record(_make_mixed_result(ops_per_sec=50000.0),
                         workload, "kvbench"),
        ]
        per_op = trend_comparison(entries)["backends"][0]["per_op"]
        self.assertEqual(sorted(per_op), ["GET", "INCR", "SET"])
        set_section = per_op["SET"]
        self.assertEqual(
            [row["ops_per_sec"] for row in set_section["runs"]],
            [20000.0, 25000.0])
        self.assertAlmostEqual(
            set_section["runs"][1]["ops_sec_delta_pct"], 25.0)
        self.assertEqual(
            set_section["first_to_last"]["ops_per_sec"]["first"],
            20000.0)
        self.assertEqual(
            set_section["first_to_last"]["ops_per_sec"]["last"],
            25000.0)
        self.assertAlmostEqual(
            set_section["first_to_last"]["ops_per_sec"]["delta_pct"],
            25.0)
        self.assertEqual(
            set_section["first_to_last"]["p99_ms"]["first"], 0.020)

    def test_trend_json_carries_sweep_sections(self):
        # The core of the per-step treatment: each backend's
        # "sweeps" carries one entry per step that ran in the
        # window, with the step's best and worst runs by the
        # step's own ops/sec, the deltas, and the step's per-op
        # histograms from both runs — the same sections
        # --best-worst-json carries.
        entries = [
            self._step_record(40000.0, set_count=500),
            self._step_record(60000.0, set_count=300),
        ]
        sweeps = trend_comparison(entries)["backends"][0]["sweeps"]
        self.assertEqual(len(sweeps), 1)
        step = sweeps[0]
        self.assertEqual(step["kind"], "keyspace")
        self.assertEqual(step["label"], "Keyspace sweep")
        self.assertEqual(step["step"], 100)
        self.assertEqual(step["step_label"], "100 keys")
        self.assertEqual(step["best"]["ops_per_sec"], 60000.0)
        self.assertEqual(step["worst"]["ops_per_sec"], 40000.0)
        self.assertAlmostEqual(step["ops_sec_delta_pct"], 50.0)
        self.assertAlmostEqual(step["p99_delta_pct"], 0.0)
        self.assertEqual(step["notes"], [])
        self.assertEqual(step["best"]["histograms_by_op"]["SET"],
                         [{"upper_ms": 0.1, "count": 300}])
        self.assertEqual(step["worst"]["histograms_by_op"]["SET"],
                         [{"upper_ms": 0.1, "count": 500}])
        comparison = step["histogram_comparison"]
        self.assertEqual(comparison["from"], "worst")
        self.assertEqual(comparison["to"], "best")
        self.assertEqual(comparison["notes"], [])
        self.assertTrue(comparison["percentiles_from_histograms"])
        by_op = {row["op"]: row for row in comparison["rows"]}
        self.assertEqual(by_op["SET"]["p99_from_ms"], 0.1)
        self.assertEqual(by_op["SET"]["p99_to_ms"], 0.1)
        self.assertAlmostEqual(by_op["SET"]["p99_change_pct"], 0.0)
        self.assertAlmostEqual(by_op["SET"]["bucket_drift_pp"], 0.0)

    def test_trend_json_sweep_pick_is_per_step_not_per_run(self):
        # The step's best run is picked by the step's own
        # ops/sec: here the run that won overall lost this step,
        # so the JSON best pick carries the other run's shape —
        # while the overall first-to-last still follows the
        # runs themselves.
        entries = [
            self._step_record(40000.0, set_count=500, p99_ms=0.020),
            self._step_record(60000.0, set_count=300, p99_ms=0.020),
        ]
        entries[0]["sweeps"]["keyspace"][0]["ops_per_sec"] = 70000.0
        entries[1]["sweeps"]["keyspace"][0]["ops_per_sec"] = 50000.0
        backend = trend_comparison(entries)["backends"][0]
        step = backend["sweeps"][0]
        self.assertEqual(step["best"]["ops_per_sec"], 70000.0)
        self.assertEqual(step["worst"]["ops_per_sec"], 50000.0)
        self.assertAlmostEqual(step["ops_sec_delta_pct"], 40.0)
        self.assertEqual(step["best"]["histograms_by_op"]["SET"],
                         [{"upper_ms": 0.1, "count": 500}])
        self.assertEqual(
            backend["first_to_last"]["ops_per_sec"]["first"], 40000.0)
        self.assertEqual(
            backend["first_to_last"]["ops_per_sec"]["last"], 60000.0)

    def test_trend_json_all_five_sweep_kinds(self):
        entries = [
            self._step_record(40000.0, keys=100, clients=4,
                              value_size=128, pipeline=5,
                              mix="90:10"),
            self._step_record(50000.0, keys=100, clients=4,
                              value_size=128, pipeline=5,
                              mix="90:10"),
        ]
        sweeps = trend_comparison(entries)["backends"][0]["sweeps"]
        by_kind = {s["kind"]: s for s in sweeps}
        self.assertEqual(
            sorted(by_kind),
            ["clients", "keyspace", "mix", "pipeline",
             "value_size"])
        self.assertEqual(by_kind["keyspace"]["step"], 100)
        self.assertEqual(by_kind["keyspace"]["step_label"],
                         "100 keys")
        self.assertEqual(by_kind["clients"]["step"], 4)
        self.assertEqual(by_kind["clients"]["step_label"],
                         "4 clients")
        self.assertEqual(by_kind["value_size"]["step"], 128)
        self.assertEqual(by_kind["value_size"]["step_label"],
                         "128 byte values")
        self.assertEqual(by_kind["pipeline"]["step"], 5)
        self.assertEqual(by_kind["pipeline"]["step_label"],
                         "5 commands")
        self.assertEqual(by_kind["mix"]["step"], "90:10")
        self.assertEqual(by_kind["mix"]["step_label"],
                         "90:10 read:write")
        for step in sweeps:
            self.assertEqual(step["best"]["ops_per_sec"], 50000.0)
            self.assertEqual(step["worst"]["ops_per_sec"], 40000.0)
            self.assertEqual(step["notes"], [])

    def test_trend_json_single_run_step_has_no_spread(self):
        # One run of a step is both its best and its worst, with
        # 0.0% deltas — and both picks carry that run's shape —
        # and the step's histogram comparison is a note, not a
        # table.
        entries = [self._step_record(50000.0, set_count=500)]
        step = trend_comparison(entries)["backends"][0]["sweeps"][0]
        self.assertEqual(step["best"]["ops_per_sec"], 50000.0)
        self.assertEqual(step["worst"]["ops_per_sec"], 50000.0)
        self.assertAlmostEqual(step["ops_sec_delta_pct"], 0.0)
        self.assertAlmostEqual(step["p99_delta_pct"], 0.0)
        self.assertEqual(step["notes"], [])
        self.assertEqual(step["best"]["histograms_by_op"]["SET"],
                         [{"upper_ms": 0.1, "count": 500}])
        self.assertEqual(step["worst"]["histograms_by_op"]["SET"],
                         [{"upper_ms": 0.1, "count": 500}])
        comparison = step["histogram_comparison"]
        self.assertIsNone(comparison["rows"])
        self.assertEqual(len(comparison["notes"]), 1)
        self.assertIn(
            "are the same run, so there is no best-vs-worst "
            "histogram comparison for this step.",
            comparison["notes"][0])

    def test_trend_json_step_recorded_before_0_49_has_note(self):
        entries = [self._step_record(40000.0, histograms="missing"),
                   self._step_record(50000.0, histograms="missing")]
        step = trend_comparison(entries)["backends"][0]["sweeps"][0]
        self.assertNotIn("histograms_by_op", step["best"])
        self.assertNotIn("histograms_by_op", step["worst"])
        self.assertEqual(len(step["notes"]), 2)
        self.assertIn(
            "Keyspace sweep — 100 keys (best run",
            step["notes"][0])
        self.assertIn(
            "this step was recorded before 0.49.0, so it carries "
            "no per-op histograms.", step["notes"][0])

    def test_trend_json_step_with_no_per_op_latencies_has_note(self):
        entries = [self._step_record(40000.0, histograms="empty"),
                   self._step_record(50000.0, histograms="empty")]
        step = trend_comparison(entries)["backends"][0]["sweeps"][0]
        self.assertNotIn("histograms_by_op", step["best"])
        self.assertNotIn("histograms_by_op", step["worst"])
        self.assertEqual(len(step["notes"]), 2)
        self.assertIn(
            "this step recorded no per-op latencies, so there are "
            "no histograms to show.", step["notes"][0])

    def test_trend_json_step_malformed_histograms_has_note(self):
        entries = [self._step_record(40000.0,
                                     histograms="malformed"),
                   self._step_record(50000.0,
                                     histograms="malformed")]
        step = trend_comparison(entries)["backends"][0]["sweeps"][0]
        self.assertNotIn("histograms_by_op", step["best"])
        self.assertNotIn("histograms_by_op", step["worst"])
        self.assertEqual(len(step["notes"]), 2)
        self.assertIn(
            "the recorded per-op histograms for this step are "
            "malformed, so no histograms are shown.",
            step["notes"][0])

    def test_trend_json_records_without_sweeps_have_empty_sweeps(self):
        # History written before 0.37.0 has no "sweeps" key: the
        # trend JSON carries an empty sweeps list, not no key.
        workload = _make_workload()
        entries = [
            build_record(_make_result(ops_per_sec=40000.0),
                         workload, "kvbench"),
            build_record(_make_result(ops_per_sec=50000.0),
                         workload, "kvbench"),
        ]
        for entry in entries:
            self.assertNotIn("sweeps", entry)
        backend = trend_comparison(entries)["backends"][0]
        self.assertEqual(backend["sweeps"], [])
        self.assertEqual(backend["sweep_trends"], [])

    def test_trend_json_carries_sweep_trend_sections(self):
        # The other half of the per-step treatment: each
        # backend's "sweep_trends" carries one entry per step
        # that ran in the window, with the step's own runs
        # table, sparkline and first-to-last change in ops/sec
        # and in p99, and the step's per-op histograms from
        # the latest run that recorded it — the same sections
        # the Markdown --trend chart renders per step.
        entries = [
            self._step_record(40000.0, set_count=500),
            self._step_record(60000.0, set_count=300),
        ]
        trends = trend_comparison(entries)["backends"][0][
            "sweep_trends"]
        self.assertEqual(len(trends), 1)
        step = trends[0]
        self.assertEqual(step["kind"], "keyspace")
        self.assertEqual(step["label"], "Keyspace sweep")
        self.assertEqual(step["step"], 100)
        self.assertEqual(step["step_label"], "100 keys")
        self.assertEqual(len(step["runs"]), 2)
        self.assertEqual(step["runs"][0]["ops_per_sec"], 40000.0)
        self.assertIsNone(step["runs"][0]["ops_sec_delta_pct"])
        self.assertEqual(step["runs"][1]["ops_per_sec"], 60000.0)
        self.assertAlmostEqual(
            step["runs"][1]["ops_sec_delta_pct"], 50.0)
        self.assertEqual(step["runs"][1]["p99_ms"], 0.020)
        self.assertEqual(step["sparkline"], "▁█")
        first_to_last = step["first_to_last"]
        self.assertEqual(first_to_last["ops_per_sec"]["first"],
                         40000.0)
        self.assertEqual(first_to_last["ops_per_sec"]["last"],
                         60000.0)
        self.assertAlmostEqual(
            first_to_last["ops_per_sec"]["delta_pct"], 50.0)
        self.assertEqual(first_to_last["p99_ms"]["first"], 0.020)
        self.assertEqual(first_to_last["p99_ms"]["last"], 0.020)
        self.assertAlmostEqual(
            first_to_last["p99_ms"]["delta_pct"], 0.0)
        self.assertEqual(step["notes"], [])
        self.assertEqual(step["histograms_by_op"]["SET"],
                         [{"upper_ms": 0.1, "count": 300}])
        self.assertEqual(step["histograms_by_op"]["GET"],
                         [{"upper_ms": 0.1, "count": 400}])

    def test_trend_json_sweep_trend_agrees_with_markdown(self):
        # Both views build their per-step trend sections from
        # the same shared helpers, so the JSON table, sparkline
        # and first-to-last lines match the Markdown chart's
        # numbers for the same step.
        entries = [
            self._step_record(40000.0, set_count=500),
            self._step_record(60000.0, set_count=300),
        ]
        step = trend_comparison(entries)["backends"][0][
            "sweep_trends"][0]
        text = format_trend(entries)
        self.assertIn("| 60,000 | +50.0% | 0.020 |", text)
        self.assertIn(
            "Keyspace sweep — 100 keys trend (oldest → newest): "
            f"{step['sparkline']}", text)
        self.assertIn(
            "Keyspace sweep — 100 keys first → last: "
            "40,000 → 60,000 ops/sec (+50.0%)", text)
        self.assertIn(
            "Keyspace sweep — 100 keys p99 first → last: "
            "0.020 → 0.020 ms (+0.0%)", text)

    def test_trend_json_sweep_trend_all_five_kinds(self):
        entries = [
            self._step_record(40000.0, keys=100, clients=4,
                              value_size=128, pipeline=5,
                              mix="90:10"),
            self._step_record(50000.0, keys=100, clients=4,
                              value_size=128, pipeline=5,
                              mix="90:10"),
        ]
        trends = trend_comparison(entries)["backends"][0][
            "sweep_trends"]
        by_kind = {s["kind"]: s for s in trends}
        self.assertEqual(
            sorted(by_kind),
            ["clients", "keyspace", "mix", "pipeline",
             "value_size"])
        self.assertEqual(by_kind["keyspace"]["step_label"],
                         "100 keys")
        self.assertEqual(by_kind["clients"]["step_label"],
                         "4 clients")
        self.assertEqual(by_kind["value_size"]["step_label"],
                         "128 byte values")
        self.assertEqual(by_kind["pipeline"]["step_label"],
                         "5 commands")
        self.assertEqual(by_kind["mix"]["step_label"],
                         "90:10 read:write")
        for step in trends:
            self.assertEqual(len(step["runs"]), 2)
            self.assertEqual(
                step["first_to_last"]["ops_per_sec"]["first"],
                40000.0)
            self.assertEqual(
                step["first_to_last"]["ops_per_sec"]["last"],
                50000.0)
            self.assertEqual(step["notes"], [])
            self.assertIn("histograms_by_op", step)

    def test_trend_json_sweep_trend_delta_scans_past_window(self):
        # The delta is against the previous run that recorded
        # the same step, scanning all runs — so a run outside
        # the window still counts, the same rule the Markdown
        # tables use: with limit 1 only the latest run is in
        # the window, but its delta is against the run before.
        entries = [
            self._step_record(40000.0),
            self._step_record(50000.0),
            self._step_record(60000.0),
        ]
        step = trend_comparison(entries, limit=1)["backends"][0][
            "sweep_trends"][0]
        self.assertEqual(len(step["runs"]), 1)
        self.assertAlmostEqual(
            step["runs"][0]["ops_sec_delta_pct"], 20.0)
        self.assertIsNone(
            step["first_to_last"]["ops_per_sec"])
        self.assertIsNone(step["first_to_last"]["p99_ms"])
        self.assertEqual(step["sparkline"], "▅")

    def test_trend_json_sweep_trend_step_recorded_before_0_49_has_note(self):
        entries = [self._step_record(40000.0, histograms="missing"),
                   self._step_record(50000.0, histograms="missing")]
        step = trend_comparison(entries)["backends"][0][
            "sweep_trends"][0]
        self.assertNotIn("histograms_by_op", step)
        self.assertEqual(len(step["notes"]), 1)
        self.assertIn(
            "Keyspace sweep — 100 keys (latest run,",
            step["notes"][0])
        self.assertIn(
            "this step was recorded before 0.49.0, so it carries "
            "no per-op histograms.", step["notes"][0])

    def test_trend_json_sweep_trend_step_with_no_per_op_latencies_has_note(self):
        entries = [self._step_record(40000.0, histograms="empty"),
                   self._step_record(50000.0, histograms="empty")]
        step = trend_comparison(entries)["backends"][0][
            "sweep_trends"][0]
        self.assertNotIn("histograms_by_op", step)
        self.assertEqual(len(step["notes"]), 1)
        self.assertIn(
            "this step recorded no per-op latencies, so there are "
            "no histograms to show.", step["notes"][0])

    def test_trend_json_sweep_trend_step_malformed_histograms_has_note(self):
        entries = [self._step_record(40000.0,
                                     histograms="malformed"),
                   self._step_record(50000.0,
                                     histograms="malformed")]
        step = trend_comparison(entries)["backends"][0][
            "sweep_trends"][0]
        self.assertNotIn("histograms_by_op", step)
        self.assertEqual(len(step["notes"]), 1)
        self.assertIn(
            "the recorded per-op histograms for this step are "
            "malformed, so no histograms are shown.",
            step["notes"][0])

    def test_trend_json_sweeps_agree_with_best_worst_json(self):
        # Both views build their per-step sections from the same
        # shared helper, so the trend JSON's sweeps are exactly
        # the best/worst JSON's sweeps for the same runs.
        entries = [
            self._step_record(40000.0, set_count=500),
            self._step_record(60000.0, set_count=300),
        ]
        self.assertEqual(
            trend_comparison(entries)["backends"][0]["sweeps"],
            best_worst_comparison(entries)["backends"][0]["sweeps"])

    def test_trend_json_latest_run_histograms(self):
        workload = _make_workload()
        entries = [
            build_record(_make_mixed_result(ops_per_sec=40000.0),
                         workload, "kvbench"),
            build_record(_make_mixed_result(ops_per_sec=50000.0),
                         workload, "kvbench"),
        ]
        backend = trend_comparison(entries)["backends"][0]
        self.assertEqual(backend["histograms_by_op"]["SET"],
                         [{"upper_ms": 0.1, "count": 500}])
        self.assertEqual(backend["histograms_by_op"]["GET"],
                         [{"upper_ms": 0.1, "count": 400}])
        self.assertEqual(backend["notes"], [])

    def test_trend_json_latest_run_before_0_43_has_note(self):
        # The latest run was recorded before 0.43.0 and carries
        # no "histograms_by_op" key: a note naming that instead
        # of a histogram map — the same reason the Markdown view
        # renders.
        workload = _make_workload()
        entries = [
            build_record(_make_mixed_result(ops_per_sec=40000.0),
                         workload, "kvbench"),
            build_record(_make_mixed_result(ops_per_sec=50000.0),
                         workload, "kvbench"),
        ]
        del entries[1]["histograms_by_op"]
        backend = trend_comparison(entries)["backends"][0]
        self.assertIsNone(backend["histograms_by_op"])
        self.assertEqual(len(backend["notes"]), 1)
        self.assertIn("recorded before 0.43.0, so this run carries "
                      "no per-op histograms.", backend["notes"][0])

    def test_trend_json_latest_run_no_per_op_latencies_has_note(self):
        workload = _make_workload()
        entries = [
            build_record(_make_result(ops_per_sec=40000.0),
                         workload, "kvbench"),
            build_record(_make_result(ops_per_sec=50000.0),
                         workload, "kvbench"),
        ]
        backend = trend_comparison(entries)["backends"][0]
        self.assertIsNone(backend["histograms_by_op"])
        self.assertEqual(len(backend["notes"]), 1)
        self.assertIn("this run recorded no per-op latencies, so "
                      "there are no histograms to show.",
                      backend["notes"][0])

    def test_trend_json_latest_run_malformed_histograms_has_note(self):
        workload = _make_workload()
        entries = [
            build_record(_make_mixed_result(ops_per_sec=40000.0),
                         workload, "kvbench"),
            build_record(_make_mixed_result(ops_per_sec=50000.0),
                         workload, "kvbench"),
        ]
        entries[1]["histograms_by_op"] = {"SET": {}}
        backend = trend_comparison(entries)["backends"][0]
        self.assertIsNone(backend["histograms_by_op"])
        self.assertEqual(len(backend["notes"]), 1)
        self.assertIn("the recorded per-op histograms are "
                      "malformed, so no histograms are shown.",
                      backend["notes"][0])

    def test_trend_json_histogram_comparison_first_vs_last(self):
        # The core of the JSON side of the comparison: each op's
        # p50/p99 in the first and last runs, the p99 change and
        # the bucket drift — the same numbers the Markdown table
        # shows, so the two views cannot disagree.
        workload = _make_workload()
        entries = [
            build_record(_make_mixed_result(ops_per_sec=40000.0),
                         workload, "kvbench"),
            build_record(
                _make_result(
                    ops=1000, ops_per_sec=50000.0,
                    latencies_by_op={
                        "SET": [200_000] * 500,
                        "GET": [20_000] * 400,
                        "INCR": [20_000] * 100,
                    }),
                workload, "kvbench"),
        ]
        comparison = trend_comparison(entries)["backends"][0][
            "histogram_comparison"]
        self.assertEqual(comparison["from"], "first")
        self.assertEqual(comparison["to"], "last")
        self.assertEqual(comparison["notes"], [])
        by_op = {row["op"]: row for row in comparison["rows"]}
        self.assertEqual(by_op["SET"]["p50_from_ms"], 0.020)
        self.assertEqual(by_op["SET"]["p50_to_ms"], 0.200)
        self.assertEqual(by_op["SET"]["p99_from_ms"], 0.020)
        self.assertEqual(by_op["SET"]["p99_to_ms"], 0.200)
        self.assertAlmostEqual(by_op["SET"]["p99_change_pct"], 900.0)
        self.assertAlmostEqual(by_op["SET"]["bucket_drift_pp"], -100.0)
        self.assertEqual(by_op["SET"]["bucket_drift_bucket"],
                         "<= 0.1 ms")
        self.assertAlmostEqual(by_op["GET"]["p99_change_pct"], 0.0)
        self.assertAlmostEqual(by_op["GET"]["bucket_drift_pp"], 0.0)
        # The Markdown view renders the same row from the same
        # shared helper.
        self.assertIn(
            "| SET | 0.020 | 0.200 | 0.020 | 0.200 | +900.0% | "
            "-100.0 pp (<= 0.1 ms) |", format_trend(entries))

    def test_trend_json_histogram_comparison_single_run_has_note(self):
        workload = _make_workload()
        entries = [
            build_record(_make_mixed_result(), workload, "kvbench"),
        ]
        comparison = trend_comparison(entries)["backends"][0][
            "histogram_comparison"]
        self.assertIsNone(comparison["rows"])
        self.assertEqual(len(comparison["notes"]), 1)
        self.assertIn("Only one run in the trend window, so there "
                      "is no first-to-last histogram comparison.",
                      comparison["notes"][0])

    def test_trend_json_histogram_comparison_before_0_43_has_note(self):
        workload = _make_workload()
        entries = [
            build_record(_make_mixed_result(ops_per_sec=40000.0),
                         workload, "kvbench"),
            build_record(_make_mixed_result(ops_per_sec=50000.0),
                         workload, "kvbench"),
        ]
        del entries[0]["histograms_by_op"]
        comparison = trend_comparison(entries)["backends"][0][
            "histogram_comparison"]
        self.assertIsNone(comparison["rows"])
        self.assertEqual(len(comparison["notes"]), 1)
        self.assertIn("recorded before 0.43.0", comparison["notes"][0])
        self.assertIn("no first-to-last histogram comparison is "
                      "shown", comparison["notes"][0])

    def test_trend_json_per_op_histogram_comparison(self):
        # Each per-op section in the trend JSON carries its own
        # first-to-last histogram comparison — that op's p50/p99
        # in the first and last runs that ran it, the p99 change
        # and the bucket drift — built from the same shared rows
        # as the overall comparison, so the per-op numbers agree
        # with the overall table's row for that op.
        workload = _make_workload()
        entries = [
            build_record(_make_mixed_result(ops_per_sec=40000.0),
                         workload, "kvbench"),
            build_record(
                _make_result(
                    ops=1000, ops_per_sec=50000.0,
                    latencies_by_op={
                        "SET": [200_000] * 500,
                        "GET": [20_000] * 400,
                        "INCR": [20_000] * 100,
                    }),
                workload, "kvbench"),
        ]
        backend = trend_comparison(entries)["backends"][0]
        set_comparison = backend["per_op"]["SET"][
            "histogram_comparison"]
        self.assertEqual(set_comparison["from"], "first")
        self.assertEqual(set_comparison["to"], "last")
        self.assertEqual(set_comparison["notes"], [])
        self.assertEqual(len(set_comparison["rows"]), 1)
        row = set_comparison["rows"][0]
        self.assertEqual(row["op"], "SET")
        self.assertEqual(row["p50_from_ms"], 0.020)
        self.assertEqual(row["p50_to_ms"], 0.200)
        self.assertEqual(row["p99_from_ms"], 0.020)
        self.assertEqual(row["p99_to_ms"], 0.200)
        self.assertAlmostEqual(row["p99_change_pct"], 900.0)
        self.assertAlmostEqual(row["bucket_drift_pp"], -100.0)
        self.assertEqual(row["bucket_drift_bucket"], "<= 0.1 ms")
        # The same row the overall comparison carries for SET.
        overall = {r["op"]: r
                   for r in backend["histogram_comparison"]["rows"]}
        self.assertEqual(row, overall["SET"])
        # An op whose latency did not move compares flat.
        get_comparison = backend["per_op"]["GET"][
            "histogram_comparison"]
        self.assertEqual(len(get_comparison["rows"]), 1)
        self.assertAlmostEqual(
            get_comparison["rows"][0]["p99_change_pct"], 0.0)
        self.assertAlmostEqual(
            get_comparison["rows"][0]["bucket_drift_pp"], 0.0)

    def test_trend_json_per_op_histogram_comparison_single_run_has_note(
            self):
        workload = _make_workload()
        entries = [
            build_record(_make_mixed_result(), workload, "kvbench"),
        ]
        comparison = trend_comparison(entries)["backends"][0][
            "per_op"]["SET"]["histogram_comparison"]
        self.assertIsNone(comparison["rows"])
        self.assertEqual(len(comparison["notes"]), 1)
        self.assertIn("Only one run in the trend window ran SET, so "
                      "there is no first-to-last histogram "
                      "comparison for SET.", comparison["notes"][0])

    def test_trend_json_per_op_histogram_comparison_before_0_43_has_note(
            self):
        workload = _make_workload()
        entries = [
            build_record(_make_mixed_result(ops_per_sec=40000.0),
                         workload, "kvbench"),
            build_record(_make_mixed_result(ops_per_sec=50000.0),
                         workload, "kvbench"),
        ]
        del entries[0]["histograms_by_op"]
        comparison = trend_comparison(entries)["backends"][0][
            "per_op"]["SET"]["histogram_comparison"]
        self.assertIsNone(comparison["rows"])
        self.assertEqual(len(comparison["notes"]), 1)
        self.assertIn("recorded before 0.43.0",
                      comparison["notes"][0])
        self.assertIn("no first-to-last histogram comparison is "
                      "shown", comparison["notes"][0])

    def test_trend_json_per_op_histogram_comparison_uses_runs_that_ran_the_op(
            self):
        # The first run is read-only, so SET ran in only one run
        # in the window and has no comparison, while GET compares
        # the first and last runs — the per-op comparison follows
        # the runs that ran the op, not the window's first and
        # last runs.
        workload = _make_workload()
        read_only = _make_result(
            ops=1000, ops_per_sec=40000.0,
            latencies_by_op={"GET": [20_000] * 1000})
        entries = [
            build_record(read_only, workload, "kvbench"),
            build_record(
                _make_result(
                    ops=1000, ops_per_sec=50000.0,
                    latencies_by_op={
                        "SET": [200_000] * 500,
                        "GET": [20_000] * 400,
                        "INCR": [20_000] * 100,
                    }),
                workload, "kvbench"),
        ]
        per_op = trend_comparison(entries)["backends"][0]["per_op"]
        set_comparison = per_op["SET"]["histogram_comparison"]
        self.assertIsNone(set_comparison["rows"])
        self.assertEqual(len(set_comparison["notes"]), 1)
        self.assertIn("Only one run in the trend window ran SET",
                      set_comparison["notes"][0])
        get_comparison = per_op["GET"]["histogram_comparison"]
        self.assertEqual(len(get_comparison["rows"]), 1)
        row = get_comparison["rows"][0]
        self.assertEqual(row["op"], "GET")
        self.assertEqual(row["p99_from_ms"], 0.020)
        self.assertEqual(row["p99_to_ms"], 0.020)
        self.assertAlmostEqual(row["p99_change_pct"], 0.0)

    def test_trend_json_empty_history(self):
        self.assertEqual(trend_comparison([]), {"backends": []})

    def test_trend_json_limit_window(self):
        # Only the most recent runs in the window are compared:
        # with limit 1 the single run in the window is both
        # first and last, and the step's best and worst are that
        # run too.
        entries = [
            self._step_record(40000.0, set_count=500),
            self._step_record(60000.0, set_count=300),
        ]
        backend = trend_comparison(entries, limit=1)["backends"][0]
        self.assertEqual(len(backend["runs"]), 1)
        self.assertEqual(backend["runs"][0]["ops_per_sec"], 60000.0)
        self.assertIsNone(
            backend["first_to_last"]["ops_per_sec"])
        self.assertIsNone(backend["first_to_last"]["p99_ms"])
        step = backend["sweeps"][0]
        self.assertEqual(step["best"]["ops_per_sec"], 60000.0)
        self.assertEqual(step["worst"]["ops_per_sec"], 60000.0)

    def test_trend_json_file_round_trip(self):
        workload = _make_workload()
        entries = [
            build_record(_make_mixed_result(ops_per_sec=40000.0),
                         workload, "kvbench"),
            build_record(_make_mixed_result(ops_per_sec=50000.0),
                         workload, "kvbench"),
        ]
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "reports", "trend.json")
            comparison = write_trend_json(path, entries)
            with open(path, encoding="utf-8") as f:
                parsed = json.load(f)
            self.assertEqual(parsed, comparison)
            self.assertEqual(
                parsed["backends"][0]["runs"][1]["ops_per_sec"],
                50000.0)

    def test_trend_json_writes_file_and_still_prints(self):
        with tempfile.TemporaryDirectory() as tmp:
            history = os.path.join(tmp, "history.jsonl")
            record_run(history, [_make_result(ops_per_sec=40000.0),
                                 _make_result(ops_per_sec=50000.0)],
                       _make_workload(), "kvbench")
            json_path = os.path.join(tmp, "trend.json")
            out = io.StringIO()
            with redirect_stdout(out):
                code = main(["--trend", "--history-file", history,
                             "--trend-json", json_path])
            self.assertEqual(code, 0)
            stdout = out.getvalue()
            self.assertIn("ops/sec trend", stdout)
            self.assertIn("+25.0%", stdout)
            self.assertIn(f"Trend JSON written to {json_path}", stdout)
            with open(json_path, encoding="utf-8") as f:
                parsed = json.load(f)
            self.assertEqual(
                parsed["backends"][0]["runs"][1]["ops_per_sec"],
                50000.0)
            self.assertEqual(
                parsed["backends"][0]["sweeps"], [])

    def test_trend_json_without_trend_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            out_path = os.path.join(tmp, "trend.json")
            err = io.StringIO()
            with redirect_stdout(io.StringIO()):
                from contextlib import redirect_stderr
                with redirect_stderr(err):
                    code = main(["--trend-json", out_path])
            self.assertEqual(code, 2)
            self.assertIn("--trend-json needs --trend",
                          err.getvalue())
            self.assertFalse(os.path.exists(out_path))

    def test_trend_json_with_empty_history_exits_zero(self):
        with tempfile.TemporaryDirectory() as tmp:
            history = os.path.join(tmp, "history.jsonl")
            json_path = os.path.join(tmp, "trend.json")
            out = io.StringIO()
            with redirect_stdout(out):
                code = main(["--trend", "--history-file", history,
                             "--trend-json", json_path])
            self.assertEqual(code, 0)
            self.assertIn("No runs recorded yet", out.getvalue())
            self.assertIn(f"Trend JSON written to {json_path}",
                          out.getvalue())
            with open(json_path, encoding="utf-8") as f:
                self.assertEqual(json.load(f), {"backends": []})


class TestBestWorstComparison(unittest.TestCase):
    def _entries(self, rates=(40000.0, 60000.0, 50000.0)):
        workload = _make_workload()
        return [
            build_record(_make_result(ops_per_sec=rate), workload,
                         "kvbench")
            for rate in rates
        ]

    def _result_with_latency(self, ops_per_sec, latency_ns,
                             latencies_by_op=None):
        # 1000 samples of latency_ns each: p99 is latency_ns ns.
        return BenchmarkResult(
            name="cinder (127.0.0.1:6379)", ops=1000,
            elapsed_s=1000 / ops_per_sec,
            latencies_ns=[latency_ns] * 1000,
            latencies_by_op=latencies_by_op or {})

    def test_picks_best_and_worst_by_ops_sec(self):
        comparison = best_worst_comparison(self._entries())
        self.assertEqual(len(comparison["backends"]), 1)
        backend = comparison["backends"][0]
        self.assertEqual(backend["runs"], 3)
        self.assertEqual(backend["best"]["ops_per_sec"], 60000.0)
        self.assertEqual(backend["worst"]["ops_per_sec"], 40000.0)
        # 60000 vs 40000 is +50.0%; p99 is 0.020 ms in both.
        self.assertAlmostEqual(backend["ops_sec_delta_pct"], 50.0)
        self.assertAlmostEqual(backend["p99_delta_pct"], 0.0)

    def test_p99_delta_between_best_and_worst(self):
        workload = _make_workload()
        entries = [
            build_record(
                self._result_with_latency(40000.0, 40_000),
                workload, "kvbench"),
            build_record(
                self._result_with_latency(60000.0, 10_000),
                workload, "kvbench"),
        ]
        backend = best_worst_comparison(entries)["backends"][0]
        self.assertEqual(backend["best"]["p99_ms"], 0.010)
        self.assertEqual(backend["worst"]["p99_ms"], 0.040)
        # Best p99 0.010 vs worst 0.040 is -75.0%.
        self.assertAlmostEqual(backend["p99_delta_pct"], -75.0)

    def test_single_run_is_both_best_and_worst(self):
        backend = best_worst_comparison(
            self._entries(rates=(50000.0,)))["backends"][0]
        self.assertEqual(backend["runs"], 1)
        self.assertEqual(backend["best"]["ops_per_sec"], 50000.0)
        self.assertEqual(backend["worst"]["ops_per_sec"], 50000.0)
        self.assertAlmostEqual(backend["ops_sec_delta_pct"], 0.0)
        self.assertAlmostEqual(backend["p99_delta_pct"], 0.0)

    def test_empty_entries_have_no_backends(self):
        self.assertEqual(best_worst_comparison([])["backends"], [])

    def test_each_backend_is_compared_on_its_own(self):
        workload = _make_workload()
        entries = [
            build_record(_make_result(name="baseline (dict)",
                                      ops_per_sec=90000.0),
                         workload, "kvbench"),
            build_record(_make_result(name="baseline (dict)",
                                      ops_per_sec=100000.0),
                         workload, "kvbench"),
            build_record(_make_result(ops_per_sec=40000.0), workload,
                         "kvbench"),
        ]
        comparison = best_worst_comparison(entries)
        by_name = {b["backend"]: b for b in comparison["backends"]}
        self.assertEqual(
            by_name["baseline (dict)"]["best"]["ops_per_sec"], 100000.0)
        self.assertEqual(
            by_name["baseline (dict)"]["worst"]["ops_per_sec"], 90000.0)
        self.assertEqual(
            by_name["cinder (127.0.0.1:6379)"]["best"]["ops_per_sec"],
            40000.0)

    def test_per_op_picks_are_independent_of_the_overall_pick(self):
        workload = _make_workload()
        # Run A: slower overall, but SET-heavy (500 SET samples).
        # Run B: faster overall, but SET-light (100 SET samples).
        entries = [
            build_record(
                self._result_with_latency(
                    40000.0, 20_000,
                    {"SET": [20_000] * 500, "GET": [20_000] * 400,
                     "INCR": [20_000] * 100}),
                workload, "kvbench"),
            build_record(
                self._result_with_latency(
                    50000.0, 20_000,
                    {"SET": [20_000] * 100, "GET": [20_000] * 800,
                     "INCR": [20_000] * 100}),
                workload, "kvbench"),
        ]
        per_op = best_worst_comparison(entries)["backends"][0]["per_op"]
        # SET: 500 samples over 0.025 s is 20,000/s in run A,
        # 100 over 0.02 s is 5,000/s in run B — run A wins SET
        # even though run B wins overall.
        self.assertEqual(per_op["SET"]["best"]["ops_per_sec"], 20000.0)
        self.assertEqual(per_op["SET"]["worst"]["ops_per_sec"], 5000.0)
        self.assertAlmostEqual(per_op["SET"]["ops_sec_delta_pct"], 300.0)
        # GET: 16,000/s in run A, 40,000/s in run B.
        self.assertEqual(per_op["GET"]["best"]["ops_per_sec"], 40000.0)
        self.assertEqual(per_op["GET"]["worst"]["ops_per_sec"], 16000.0)

    def test_workload_filter_keeps_only_matching_runs(self):
        workload = _make_workload()
        other_workload = Workload(
            ops=1000, clients=8, keys=1000, value_size=32,
            set_weight=50, get_weight=40, incr_weight=10,
            seed=1234, pipeline=1)
        entries = [
            # A 90000 ops/sec run with a different workload must
            # not become the best run when the filter is on.
            build_record(_make_result(ops_per_sec=90000.0),
                         other_workload, "kvbench"),
            build_record(_make_result(ops_per_sec=40000.0),
                         workload, "kvbench"),
            build_record(_make_result(ops_per_sec=60000.0),
                         workload, "kvbench"),
        ]
        reference = entries[1]["workload"]
        backend = best_worst_comparison(
            entries, workload=reference)["backends"][0]
        self.assertEqual(backend["runs"], 2)
        self.assertEqual(backend["best"]["ops_per_sec"], 60000.0)
        self.assertEqual(backend["worst"]["ops_per_sec"], 40000.0)

    def test_records_without_per_op_have_no_per_op_sections(self):
        entries = self._entries(rates=(40000.0, 50000.0))
        for entry in entries:
            del entry["per_op"]
        backend = best_worst_comparison(entries)["backends"][0]
        self.assertEqual(backend["per_op"], {})
        self.assertEqual(backend["best"]["ops_per_sec"], 50000.0)

    def test_best_and_worst_carry_per_op_histograms(self):
        workload = _make_workload()
        entries = [
            build_record(_make_mixed_result(ops_per_sec=40000.0),
                         workload, "kvbench"),
            build_record(_make_mixed_result(ops_per_sec=60000.0),
                         workload, "kvbench"),
        ]
        backend = best_worst_comparison(entries)["backends"][0]
        # Both picked runs carry the full per-op map — the same
        # buckets the JSON report's histograms_by_op uses.
        for pick in (backend["best"], backend["worst"]):
            self.assertEqual(pick["histograms_by_op"]["SET"],
                             [{"upper_ms": 0.1, "count": 500}])
            self.assertEqual(pick["histograms_by_op"]["GET"],
                             [{"upper_ms": 0.1, "count": 400}])

    def test_per_op_picks_carry_that_ops_histogram(self):
        workload = _make_workload()
        # Run A: slower overall, but SET-heavy (500 SET samples).
        # Run B: faster overall, but SET-light (100 SET samples).
        entries = [
            build_record(
                self._result_with_latency(
                    40000.0, 20_000,
                    {"SET": [20_000] * 500, "GET": [20_000] * 400,
                     "INCR": [20_000] * 100}),
                workload, "kvbench"),
            build_record(
                self._result_with_latency(
                    50000.0, 20_000,
                    {"SET": [20_000] * 100, "GET": [20_000] * 800,
                     "INCR": [20_000] * 100}),
                workload, "kvbench"),
        ]
        per_op = best_worst_comparison(entries)["backends"][0]["per_op"]
        # The SET pick is run A (500 SET samples); its histogram
        # is that run's SET buckets, not the overall pick's.
        self.assertEqual(per_op["SET"]["best"]["histogram"],
                         [{"upper_ms": 0.1, "count": 500}])
        self.assertEqual(per_op["SET"]["worst"]["histogram"],
                         [{"upper_ms": 0.1, "count": 100}])
        self.assertEqual(per_op["GET"]["best"]["histogram"],
                         [{"upper_ms": 0.1, "count": 800}])

    def test_records_without_histograms_carry_no_histogram_keys(self):
        entries = self._entries(rates=(40000.0, 50000.0))
        for entry in entries:
            del entry["histograms_by_op"]
        backend = best_worst_comparison(entries)["backends"][0]
        self.assertNotIn("histograms_by_op", backend["best"])
        self.assertNotIn("histograms_by_op", backend["worst"])
        self.assertEqual(backend["best"]["ops_per_sec"], 50000.0)

    def test_malformed_histograms_are_ignored(self):
        entries = self._entries(rates=(40000.0, 50000.0))
        entries[0]["histograms_by_op"] = {"SET": "not a list"}
        entries[1]["histograms_by_op"] = {"SET": [{"upper_ms": 0.1}]}
        backend = best_worst_comparison(entries)["backends"][0]
        self.assertNotIn("histograms_by_op", backend["best"])
        self.assertNotIn("histograms_by_op", backend["worst"])

    def test_histogram_comparison_compares_best_and_worst_runs(self):
        # The best run (50k ops/sec) has slower SET latencies
        # (200_000 ns) than the worst run (40k ops/sec, 20_000
        # ns): the comparison names SET as the op that moved,
        # with its p50/p99 in both runs, the p99 change, and the
        # bucket drift — the same rows the trend's first-to-last
        # comparison builds.
        workload = _make_workload()
        entries = [
            build_record(_make_mixed_result(ops_per_sec=40000.0),
                         workload, "kvbench"),
            build_record(
                _make_result(
                    ops=1000, ops_per_sec=50000.0,
                    latencies_by_op={
                        "SET": [200_000] * 500,
                        "GET": [20_000] * 400,
                        "INCR": [20_000] * 100,
                    }),
                workload, "kvbench"),
        ]
        comparison = best_worst_comparison(entries)["backends"][0][
            "histogram_comparison"]
        self.assertEqual(comparison["from"], "worst")
        self.assertEqual(comparison["to"], "best")
        self.assertEqual(comparison["notes"], [])
        by_op = {row["op"]: row for row in comparison["rows"]}
        set_row = by_op["SET"]
        self.assertEqual(set_row["p50_from_ms"], 0.020)
        self.assertEqual(set_row["p50_to_ms"], 0.200)
        self.assertEqual(set_row["p99_from_ms"], 0.020)
        self.assertEqual(set_row["p99_to_ms"], 0.200)
        self.assertAlmostEqual(set_row["p99_change_pct"], 900.0)
        self.assertAlmostEqual(set_row["bucket_drift_pp"], -100.0)
        self.assertEqual(set_row["bucket_drift_bucket"], "<= 0.1 ms")
        # GET did not move at all: +0.0% and no bucket drift.
        get_row = by_op["GET"]
        self.assertAlmostEqual(get_row["p99_change_pct"], 0.0)
        self.assertAlmostEqual(get_row["bucket_drift_pp"], 0.0)
        self.assertEqual(get_row["bucket_drift_bucket"], "<= 0.1 ms")

    def test_histogram_comparison_single_run_has_note(self):
        backend = best_worst_comparison(
            self._entries(rates=(50000.0,)))["backends"][0]
        comparison = backend["histogram_comparison"]
        self.assertIsNone(comparison["rows"])
        self.assertEqual(len(comparison["notes"]), 1)
        self.assertIn("same run", comparison["notes"][0])
        self.assertIn("no best-vs-worst histogram comparison",
                      comparison["notes"][0])

    def test_histogram_comparison_without_histograms_names_the_reason(
            self):
        # The worst run was recorded before 0.43.0 and carries no
        # "histograms_by_op" key: the comparison is a note naming
        # that instead of rows.
        entries = self._entries(rates=(40000.0, 50000.0))
        del entries[0]["histograms_by_op"]
        comparison = best_worst_comparison(entries)["backends"][0][
            "histogram_comparison"]
        self.assertIsNone(comparison["rows"])
        self.assertIn("recorded before 0.43.0",
                      comparison["notes"][0])
        self.assertIn(
            "no best-vs-worst histogram comparison is shown",
            comparison["notes"][0])

    def test_histogram_comparison_with_no_per_op_latencies_names_the_reason(
            self):
        # The best run recorded no per-op latencies and carries an
        # empty histograms mapping: a note, not rows.
        workload = _make_workload()
        entries = [
            build_record(_make_mixed_result(ops_per_sec=40000.0),
                         workload, "kvbench"),
            build_record(_make_result(ops_per_sec=50000.0),
                         workload, "kvbench"),
        ]
        self.assertEqual(entries[1]["histograms_by_op"], {})
        comparison = best_worst_comparison(entries)["backends"][0][
            "histogram_comparison"]
        self.assertIsNone(comparison["rows"])
        self.assertIn("recorded no per-op latencies",
                      comparison["notes"][0])
        self.assertIn(
            "no best-vs-worst histogram comparison is shown",
            comparison["notes"][0])

    def test_histogram_comparison_with_malformed_histograms_names_the_reason(
            self):
        # The best run's recorded histograms are not op name to
        # bucket list: a note naming that, instead of a crash.
        workload = _make_workload()
        entries = [
            build_record(_make_mixed_result(ops_per_sec=40000.0),
                         workload, "kvbench"),
            build_record(_make_mixed_result(ops_per_sec=50000.0),
                         workload, "kvbench"),
        ]
        entries[1]["histograms_by_op"] = {"SET": {"upper_ms": 0.1}}
        comparison = best_worst_comparison(entries)["backends"][0][
            "histogram_comparison"]
        self.assertIsNone(comparison["rows"])
        self.assertIn("histograms are malformed",
                      comparison["notes"][0])
        self.assertIn(
            "no best-vs-worst histogram comparison is shown",
            comparison["notes"][0])

    def test_histogram_comparison_marks_ops_missing_from_one_run(self):
        # The best run is read-only and never ran SET: the SET
        # row shows the worst run's numbers and None for the
        # best side, with no p99 change and no bucket drift.
        workload = _make_workload()
        entries = [
            build_record(_make_mixed_result(ops_per_sec=40000.0),
                         workload, "kvbench"),
            build_record(
                _make_result(
                    ops=1000, ops_per_sec=50000.0,
                    latencies_by_op={"GET": [20_000] * 1000}),
                workload, "kvbench"),
        ]
        comparison = best_worst_comparison(entries)["backends"][0][
            "histogram_comparison"]
        by_op = {row["op"]: row for row in comparison["rows"]}
        set_row = by_op["SET"]
        self.assertEqual(set_row["p50_from_ms"], 0.020)
        self.assertIsNone(set_row["p50_to_ms"])
        self.assertEqual(set_row["p99_from_ms"], 0.020)
        self.assertIsNone(set_row["p99_to_ms"])
        self.assertIsNone(set_row["p99_change_pct"])
        self.assertIsNone(set_row["bucket_drift_pp"])
        self.assertIsNone(set_row["bucket_drift_bucket"])


class TestFormatBestWorst(unittest.TestCase):
    def _entries(self, rates=(40000.0, 60000.0, 50000.0)):
        workload = _make_workload()
        return [
            build_record(_make_result(ops_per_sec=rate), workload,
                         "kvbench")
            for rate in rates
        ]

    def test_empty_history_message(self):
        self.assertIn("No runs recorded yet", format_best_worst([]))

    def test_table_shows_best_worst_and_delta(self):
        text = format_best_worst(self._entries())
        self.assertIn("best vs worst", text)
        self.assertIn("60,000", text)
        self.assertIn("40,000", text)
        self.assertIn("+50.0%", text)
        self.assertIn("0.020", text)  # p99 alongside

    def test_single_run_note(self):
        text = format_best_worst(self._entries(rates=(50000.0,)))
        self.assertIn("single recorded run is both the best and the "
                      "worst", text)
        self.assertIn("50,000", text)
        self.assertIn("+0.0%", text)

    def test_per_op_tables(self):
        workload = _make_workload()
        entries = [
            build_record(_make_mixed_result(ops_per_sec=40000.0),
                         workload, "kvbench"),
            build_record(_make_mixed_result(ops_per_sec=50000.0),
                         workload, "kvbench"),
        ]
        text = format_best_worst(entries)
        self.assertIn("#### SET", text)
        self.assertIn("#### GET", text)
        self.assertIn("#### INCR", text)
        self.assertIn("25,000", text)  # best SET ops/sec
        self.assertIn("20,000", text)  # worst SET ops/sec

    def test_best_and_worst_histogram_blocks_render(self):
        workload = _make_workload()
        entries = [
            build_record(_make_mixed_result(ops_per_sec=40000.0),
                         workload, "kvbench"),
            build_record(_make_mixed_result(ops_per_sec=60000.0),
                         workload, "kvbench"),
        ]
        text = format_best_worst(entries)
        # One per-op histogram block per picked run, drawn with
        # the same shared helper as the single-run report, so the
        # shapes cannot disagree with --compare.
        self.assertIn("#### Per-op latency histogram — best run", text)
        self.assertIn("#### Per-op latency histogram — worst run", text)
        self.assertIn("SET (500 ops)", text)
        self.assertIn("GET (400 ops)", text)
        self.assertIn("<= 0.1 ms: 500 (100.0%)", text)

    def test_per_op_histogram_blocks_render(self):
        workload = _make_workload()
        entries = [
            build_record(_make_mixed_result(ops_per_sec=40000.0),
                         workload, "kvbench"),
            build_record(_make_mixed_result(ops_per_sec=60000.0),
                         workload, "kvbench"),
        ]
        text = format_best_worst(entries)
        # Each per-op section carries that op's histograms for
        # its own best and worst runs.
        self.assertIn("#### SET — latency histogram (best run)", text)
        self.assertIn("#### SET — latency histogram (worst run)", text)
        self.assertIn("#### GET — latency histogram (best run)", text)

    def test_histogram_comparison_table(self):
        workload = _make_workload()
        entries = [
            build_record(_make_mixed_result(ops_per_sec=40000.0),
                         workload, "kvbench"),
            build_record(
                _make_result(
                    ops=1000, ops_per_sec=50000.0,
                    latencies_by_op={
                        "SET": [200_000] * 500,
                        "GET": [20_000] * 400,
                        "INCR": [20_000] * 100,
                    }),
                workload, "kvbench"),
        ]
        text = format_best_worst(entries)
        self.assertIn(
            "#### Per-op histogram comparison — best vs worst run",
            text)
        # Worst first, best second: SET p50/p99 0.020 -> 0.200
        # (+900.0%); its whole share moved out of the <= 0.1 ms
        # bucket.
        self.assertIn(
            "| SET | 0.020 | 0.200 | 0.020 | 0.200 | +900.0% | "
            "-100.0 pp (<= 0.1 ms) |",
            text)
        # GET did not move at all: +0.0% and no bucket drift.
        self.assertIn(
            "| GET | 0.020 | 0.020 | 0.020 | 0.020 | +0.0% | "
            "+0.0 pp (<= 0.1 ms) |",
            text)

    def test_histogram_comparison_single_run_names_no_comparison(self):
        # One run is both best and worst: a note, not a table.
        text = format_best_worst(self._entries(rates=(50000.0,)))
        self.assertIn("no best-vs-worst histogram comparison", text)
        self.assertNotIn(
            "#### Per-op histogram comparison — best vs worst run",
            text)

    def test_histogram_comparison_without_histograms_names_the_reason(
            self):
        # The worst run was recorded before 0.43.0 and carries no
        # "histograms_by_op" key: a note naming that, not a table.
        entries = self._entries(rates=(40000.0, 50000.0))
        del entries[0]["histograms_by_op"]
        text = format_best_worst(entries)
        self.assertIn("recorded before 0.43.0", text)
        self.assertIn(
            "no best-vs-worst histogram comparison is shown", text)
        self.assertNotIn(
            "#### Per-op histogram comparison — best vs worst run",
            text)

    def test_histogram_comparison_with_malformed_histograms_names_the_reason(
            self):
        # The best run's recorded histograms are malformed: a
        # note naming that, not a table.
        workload = _make_workload()
        entries = [
            build_record(_make_mixed_result(ops_per_sec=40000.0),
                         workload, "kvbench"),
            build_record(_make_mixed_result(ops_per_sec=50000.0),
                         workload, "kvbench"),
        ]
        entries[1]["histograms_by_op"] = {"SET": {"upper_ms": 0.1}}
        text = format_best_worst(entries)
        self.assertIn("histograms are malformed", text)
        self.assertIn(
            "no best-vs-worst histogram comparison is shown", text)
        self.assertNotIn(
            "#### Per-op histogram comparison — best vs worst run",
            text)

    def test_histogram_comparison_marks_ops_missing_from_one_run(self):
        # The best run is read-only and never ran SET: the SET
        # row shows "-" for the best side.
        workload = _make_workload()
        entries = [
            build_record(_make_mixed_result(ops_per_sec=40000.0),
                         workload, "kvbench"),
            build_record(
                _make_result(
                    ops=1000, ops_per_sec=50000.0,
                    latencies_by_op={"GET": [20_000] * 1000}),
                workload, "kvbench"),
        ]
        text = format_best_worst(entries)
        self.assertIn(
            "| SET | 0.020 | - | 0.020 | - | - | - |",
            text)

    def test_no_histograms_renders_note_not_blocks(self):
        entries = self._entries()
        for entry in entries:
            del entry["histograms_by_op"]
        text = format_best_worst(entries)
        self.assertNotIn("latency histogram", text)
        self.assertIn("carries no per-op histograms", text)

    def test_workload_note_names_the_workload(self):
        entries = self._entries()
        text = format_best_worst(entries,
                                 workload=entries[0]["workload"])
        self.assertIn("same workload as the latest recorded run",
                      text)
        self.assertIn("ops=1,000", text)
        self.assertIn("clients=4", text)

    def test_filter_with_no_matching_runs_reports_empty(self):
        entries = self._entries()
        other = Workload(ops=1000, clients=8, keys=1000,
                         value_size=32, set_weight=50, get_weight=40,
                         incr_weight=10, seed=1234, pipeline=1)
        reference = build_record(_make_result(), other,
                                 "kvbench")["workload"]
        self.assertIn("No runs recorded yet",
                      format_best_worst(entries, workload=reference))


class TestBestWorstSweepSteps(unittest.TestCase):
    def _step_record(self, ops_per_sec, set_count=500,
                     get_count=400, keys=100, clients=None,
                     value_size=None, pipeline=None, mix=None,
                     histograms="real", p99_ms=0.020,
                     set_buckets=None, get_buckets=None):
        # One recorded run whose sweep step(s) carry per-op
        # histograms: SET set_count samples and GET get_count
        # samples, all in the <= 0.1 ms bucket. ``histograms``
        # picks the shape under test: "real" (valid bucket
        # lists), "empty" (no per-op latencies), "malformed"
        # (a bucket list that is not a list), or "missing" (no
        # key at all — a run recorded before 0.49.0).
        # ``set_buckets``/``get_buckets`` replace the
        # single-bucket lists with explicit ones, so a test can
        # put the two runs' tails in different buckets.
        result = _make_result(ops_per_sec=ops_per_sec)
        step = {"keys": keys, "ops": 1000,
                "ops_per_sec": ops_per_sec,
                "mean_ms": 0.020, "p99_ms": p99_ms}
        if histograms == "real":
            step["histograms_by_op"] = {
                "SET": set_buckets or [
                    {"upper_ms": 0.1, "count": set_count}],
                "GET": get_buckets or [
                    {"upper_ms": 0.1, "count": get_count}],
            }
        elif histograms == "empty":
            step["histograms_by_op"] = {}
        elif histograms == "malformed":
            step["histograms_by_op"] = {"SET": {}}
        result.sweep = [step]
        if clients is not None:
            client_step = dict(step)
            client_step["clients"] = clients
            del client_step["keys"]
            result.client_sweep = [client_step]
        if value_size is not None:
            value_step = dict(step)
            value_step["value_size"] = value_size
            del value_step["keys"]
            result.value_sweep = [value_step]
        if pipeline is not None:
            pipeline_step = dict(step)
            pipeline_step["pipeline"] = pipeline
            del pipeline_step["keys"]
            result.pipeline_sweep = [pipeline_step]
        if mix is not None:
            mix_step = dict(step)
            mix_step["mix"] = mix
            del mix_step["keys"]
            result.mix_sweep = [mix_step]
        return build_record(result, _make_workload(), "kvbench")

    def test_step_table_and_histogram_blocks(self):
        # The core of the feature: each sweep-step section in
        # --best-worst carries its own best-vs-worst table and
        # the step's per-op histograms from its best and worst
        # runs — the best run's shape (SET 300) under the best
        # heading, the worst run's (SET 500) under the worst.
        entries = [
            self._step_record(40000.0, set_count=500),
            self._step_record(60000.0, set_count=300),
        ]
        text = format_best_worst(entries)
        self.assertIn("#### Keyspace sweep — 100 keys", text)
        self.assertIn("| Ops/sec | 60,000 | 40,000 | +50.0% |", text)
        self.assertIn(
            "#### Keyspace sweep — 100 keys — per-op latency "
            "histogram — best run", text)
        self.assertIn(
            "#### Keyspace sweep — 100 keys — per-op latency "
            "histogram — worst run", text)
        self.assertIn("SET (300 ops)", text)
        self.assertIn("SET (500 ops)", text)
        self.assertIn("GET (400 ops)", text)
        self.assertIn("- <= 0.1 ms: 300 (100.0%)", text)

    def test_step_histogram_comparison_table(self):
        # The core of the feature: each sweep-step section ends
        # with a per-op histogram comparison between the step's
        # best and worst runs — each op's p50/p99 in the two
        # steps, the p99 change, and the bucket drift. The
        # worst run's SET tail sits in the <= 0.25 ms bucket
        # (p99 0.250) while the best run's stays in <= 0.1 ms
        # (p99 0.100), so SET reads -60.0% with the <= 0.1 ms
        # bucket up 20.0 pp; GET is identical in both runs.
        entries = [
            self._step_record(
                40000.0,
                set_buckets=[{"upper_ms": 0.1, "count": 400},
                             {"upper_ms": 0.25, "count": 100}]),
            self._step_record(
                60000.0,
                set_buckets=[{"upper_ms": 0.1, "count": 500}]),
        ]
        text = format_best_worst(entries)
        self.assertIn(
            "#### Keyspace sweep — 100 keys — per-op histogram "
            "comparison — best vs worst run", text)
        self.assertIn(
            "| SET | 0.100 | 0.100 | 0.250 | 0.100 | -60.0% | "
            "+20.0 pp (<= 0.1 ms) |", text)
        self.assertIn(
            "| GET | 0.100 | 0.100 | 0.100 | 0.100 | +0.0% | "
            "+0.0 pp (<= 0.1 ms) |", text)

    def test_step_histogram_comparison_single_run_has_note(self):
        # One run of a step is both its best and its worst, so
        # there is no comparison — a note names that instead of
        # a table, while the step's own table still shows 0.0%
        # deltas.
        entries = [self._step_record(50000.0, set_count=500)]
        text = format_best_worst(entries)
        self.assertIn(
            "The best and worst runs for Keyspace sweep — 100 "
            "keys are the same run, so there is no best-vs-worst "
            "histogram comparison for this step.", text)
        self.assertNotIn(
            "per-op histogram comparison — best vs worst run",
            text)

    def test_step_histogram_comparison_before_0_49_has_note(self):
        # Steps with no histograms_by_op key were recorded
        # before 0.49.0: each pick's comparison note names that
        # instead of a table.
        entries = [self._step_record(40000.0, histograms="missing"),
                   self._step_record(50000.0, histograms="missing")]
        text = format_best_worst(entries)
        self.assertEqual(
            text.count("No best-vs-worst histogram comparison is "
                       "shown for this step."), 2)
        self.assertNotIn(
            "per-op histogram comparison — best vs worst run",
            text)

    def test_step_histogram_comparison_no_latencies_has_note(self):
        entries = [self._step_record(40000.0, histograms="empty"),
                   self._step_record(50000.0, histograms="empty")]
        text = format_best_worst(entries)
        self.assertEqual(
            text.count("No best-vs-worst histogram comparison is "
                       "shown for this step."), 2)
        self.assertNotIn(
            "per-op histogram comparison — best vs worst run",
            text)

    def test_step_histogram_comparison_malformed_has_note(self):
        entries = [self._step_record(40000.0,
                                     histograms="malformed"),
                   self._step_record(50000.0,
                                     histograms="malformed")]
        text = format_best_worst(entries)
        self.assertEqual(
            text.count("No best-vs-worst histogram comparison is "
                       "shown for this step."), 2)
        self.assertNotIn(
            "per-op histogram comparison — best vs worst run",
            text)

    def test_step_histogram_comparison_overflow_bucket_shows_dash(self):
        # An op whose p50/p99 rank lands in the overflow bucket
        # has no upper edge to report, so its percentiles read
        # as "-" instead of a number — while the bucket drift
        # still names the overflow move. The worst run's SET
        # tail sits in <= 0.25 ms; the best run's spreads into
        # the overflow bucket, so its p50/p99 are "-".
        entries = [
            self._step_record(
                40000.0,
                set_buckets=[{"upper_ms": 0.1, "count": 80},
                             {"upper_ms": 0.25, "count": 20}]),
            self._step_record(
                60000.0,
                set_buckets=[{"upper_ms": 0.1, "count": 10},
                             {"upper_ms": 0.25, "count": 10},
                             {"upper_ms": None, "count": 80}]),
        ]
        text = format_best_worst(entries)
        self.assertIn(
            "| SET | 0.100 | - | 0.250 | - | - | "
            "+80.0 pp (> 1000 ms) |", text)

    def test_all_five_sweep_kinds_render_histogram_comparison(self):
        entries = [
            self._step_record(40000.0, keys=100, clients=4,
                              value_size=128, pipeline=5,
                              mix="90:10"),
            self._step_record(50000.0, keys=100, clients=4,
                              value_size=128, pipeline=5,
                              mix="90:10"),
        ]
        text = format_best_worst(entries)
        for heading in (
                "#### Keyspace sweep — 100 keys — per-op histogram "
                "comparison — best vs worst run",
                "#### Client sweep — 4 clients — per-op histogram "
                "comparison — best vs worst run",
                "#### Value size sweep — 128 byte values — per-op "
                "histogram comparison — best vs worst run",
                "#### Pipeline sweep — 5 commands — per-op "
                "histogram comparison — best vs worst run",
                "#### Mix sweep — 90:10 read:write — per-op "
                "histogram comparison — best vs worst run"):
            self.assertIn(heading, text)

    def test_step_pick_is_per_step_not_per_run(self):
        # The step's best run is picked by the step's own
        # ops/sec: here the run that won overall lost this step,
        # so the step's best block shows the other run's shape.
        entries = [
            self._step_record(40000.0, set_count=500, p99_ms=0.020),
            self._step_record(60000.0, set_count=300, p99_ms=0.020),
        ]
        # Flip the step rates: the first run's step is faster.
        entries[0]["sweeps"]["keyspace"][0]["ops_per_sec"] = 70000.0
        entries[1]["sweeps"]["keyspace"][0]["ops_per_sec"] = 50000.0
        text = format_best_worst(entries)
        self.assertIn("| Ops/sec | 70,000 | 50,000 | +40.0% |", text)
        best_heading = (
            "#### Keyspace sweep — 100 keys — per-op latency "
            "histogram — best run")
        best_block = text[text.index(best_heading):]
        self.assertIn("SET (500 ops)", best_block)

    def test_all_five_sweep_kinds_render(self):
        entries = [
            self._step_record(40000.0, keys=100, clients=4,
                              value_size=128, pipeline=5,
                              mix="90:10"),
            self._step_record(50000.0, keys=100, clients=4,
                              value_size=128, pipeline=5,
                              mix="90:10"),
        ]
        text = format_best_worst(entries)
        self.assertIn("#### Keyspace sweep — 100 keys", text)
        self.assertIn("#### Client sweep — 4 clients", text)
        self.assertIn(
            "#### Value size sweep — 128 byte values", text)
        self.assertIn("#### Pipeline sweep — 5 commands", text)
        self.assertIn("#### Mix sweep — 90:10 read:write", text)
        self.assertEqual(
            text.count("#### Keyspace sweep — 100 keys\n"), 1)

    def test_single_run_step_has_no_spread(self):
        # One run of a step is both its best and its worst, with
        # 0.0% deltas — and both blocks show that run's shape.
        entries = [self._step_record(50000.0, set_count=500)]
        text = format_best_worst(entries)
        self.assertIn("#### Keyspace sweep — 100 keys", text)
        self.assertIn("| Ops/sec | 50,000 | 50,000 | +0.0% |", text)
        self.assertIn(
            "#### Keyspace sweep — 100 keys — per-op latency "
            "histogram — best run", text)
        self.assertIn(
            "#### Keyspace sweep — 100 keys — per-op latency "
            "histogram — worst run", text)

    def test_step_recorded_before_0_49_shows_note(self):
        # A step with no histograms_by_op key was recorded before
        # 0.49.0: each pick's section names that instead of a
        # block.
        entries = [self._step_record(40000.0, histograms="missing"),
                   self._step_record(50000.0, histograms="missing")]
        text = format_best_worst(entries)
        self.assertIn(
            "this step was recorded before 0.49.0, so it carries "
            "no per-op histograms.", text)
        self.assertNotIn(
            "Keyspace sweep — 100 keys — per-op latency histogram",
            text)

    def test_step_with_no_per_op_latencies_shows_note(self):
        entries = [self._step_record(40000.0, histograms="empty"),
                   self._step_record(50000.0, histograms="empty")]
        text = format_best_worst(entries)
        self.assertIn(
            "this step recorded no per-op latencies, so there are "
            "no histograms to show.", text)
        self.assertNotIn(
            "Keyspace sweep — 100 keys — per-op latency histogram",
            text)

    def test_step_malformed_histograms_shows_note(self):
        entries = [self._step_record(40000.0,
                                     histograms="malformed"),
                   self._step_record(50000.0,
                                     histograms="malformed")]
        text = format_best_worst(entries)
        self.assertIn(
            "the recorded per-op histograms for this step are "
            "malformed, so no histograms are shown.", text)
        self.assertNotIn(
            "Keyspace sweep — 100 keys — per-op latency histogram",
            text)

    def test_records_without_sweeps_render_no_sweep_sections(self):
        # History written before 0.37.0 has no "sweeps" key: the
        # best/worst view renders no step sections at all.
        workload = _make_workload()
        entries = [
            build_record(_make_result(ops_per_sec=40000.0),
                         workload, "kvbench"),
            build_record(_make_result(ops_per_sec=50000.0),
                         workload, "kvbench"),
        ]
        for entry in entries:
            self.assertNotIn("sweeps", entry)
        text = format_best_worst(entries)
        self.assertNotIn("sweep —", text)

    def test_json_carries_step_table_and_histograms(self):
        # The core of the JSON side: each backend's "sweeps"
        # carries one entry per step that ran, with the step's
        # best and worst runs by the step's own ops/sec, the
        # deltas, and the step's per-op histograms from both
        # runs — the best run's shape (SET 300) on best, the
        # worst run's (SET 500) on worst — the same sections the
        # Markdown view renders, with the same step labels.
        entries = [
            self._step_record(40000.0, set_count=500),
            self._step_record(60000.0, set_count=300),
        ]
        sweeps = best_worst_comparison(entries)["backends"][0][
            "sweeps"]
        self.assertEqual(len(sweeps), 1)
        step = sweeps[0]
        self.assertEqual(step["kind"], "keyspace")
        self.assertEqual(step["label"], "Keyspace sweep")
        self.assertEqual(step["step"], 100)
        self.assertEqual(step["step_label"], "100 keys")
        self.assertEqual(step["best"]["ops_per_sec"], 60000.0)
        self.assertEqual(step["worst"]["ops_per_sec"], 40000.0)
        self.assertAlmostEqual(step["ops_sec_delta_pct"], 50.0)
        self.assertAlmostEqual(step["p99_delta_pct"], 0.0)
        self.assertEqual(step["notes"], [])
        self.assertEqual(step["best"]["histograms_by_op"]["SET"],
                         [{"upper_ms": 0.1, "count": 300}])
        self.assertEqual(step["worst"]["histograms_by_op"]["SET"],
                         [{"upper_ms": 0.1, "count": 500}])
        self.assertEqual(step["best"]["histograms_by_op"]["GET"],
                         [{"upper_ms": 0.1, "count": 400}])
        # The JSON step label is the Markdown heading's label,
        # so the two views cannot disagree about a step.
        self.assertIn("#### Keyspace sweep — 100 keys",
                      format_best_worst(entries))

    def test_json_step_pick_is_per_step_not_per_run(self):
        # The step's best run is picked by the step's own
        # ops/sec: here the run that won overall lost this step,
        # so the JSON best pick carries the other run's shape.
        entries = [
            self._step_record(40000.0, set_count=500, p99_ms=0.020),
            self._step_record(60000.0, set_count=300, p99_ms=0.020),
        ]
        entries[0]["sweeps"]["keyspace"][0]["ops_per_sec"] = 70000.0
        entries[1]["sweeps"]["keyspace"][0]["ops_per_sec"] = 50000.0
        step = best_worst_comparison(entries)["backends"][0][
            "sweeps"][0]
        self.assertEqual(step["best"]["ops_per_sec"], 70000.0)
        self.assertEqual(step["worst"]["ops_per_sec"], 50000.0)
        self.assertAlmostEqual(step["ops_sec_delta_pct"], 40.0)
        self.assertEqual(step["best"]["histograms_by_op"]["SET"],
                         [{"upper_ms": 0.1, "count": 500}])

    def test_json_all_five_sweep_kinds(self):
        entries = [
            self._step_record(40000.0, keys=100, clients=4,
                              value_size=128, pipeline=5,
                              mix="90:10"),
            self._step_record(50000.0, keys=100, clients=4,
                              value_size=128, pipeline=5,
                              mix="90:10"),
        ]
        sweeps = best_worst_comparison(entries)["backends"][0][
            "sweeps"]
        by_kind = {s["kind"]: s for s in sweeps}
        self.assertEqual(
            sorted(by_kind),
            ["clients", "keyspace", "mix", "pipeline",
             "value_size"])
        self.assertEqual(by_kind["keyspace"]["step"], 100)
        self.assertEqual(by_kind["keyspace"]["step_label"],
                         "100 keys")
        self.assertEqual(by_kind["clients"]["step"], 4)
        self.assertEqual(by_kind["clients"]["step_label"],
                         "4 clients")
        self.assertEqual(by_kind["value_size"]["step"], 128)
        self.assertEqual(by_kind["value_size"]["step_label"],
                         "128 byte values")
        self.assertEqual(by_kind["pipeline"]["step"], 5)
        self.assertEqual(by_kind["pipeline"]["step_label"],
                         "5 commands")
        self.assertEqual(by_kind["mix"]["step"], "90:10")
        self.assertEqual(by_kind["mix"]["step_label"],
                         "90:10 read:write")
        for step in sweeps:
            self.assertEqual(step["best"]["ops_per_sec"], 50000.0)
            self.assertEqual(step["worst"]["ops_per_sec"], 40000.0)
            self.assertEqual(step["notes"], [])

    def test_json_single_run_step_has_no_spread(self):
        # One run of a step is both its best and its worst, with
        # 0.0% deltas — and both picks carry that run's shape.
        entries = [self._step_record(50000.0, set_count=500)]
        step = best_worst_comparison(entries)["backends"][0][
            "sweeps"][0]
        self.assertEqual(step["best"]["ops_per_sec"], 50000.0)
        self.assertEqual(step["worst"]["ops_per_sec"], 50000.0)
        self.assertAlmostEqual(step["ops_sec_delta_pct"], 0.0)
        self.assertAlmostEqual(step["p99_delta_pct"], 0.0)
        self.assertEqual(step["notes"], [])
        self.assertEqual(step["best"]["histograms_by_op"]["SET"],
                         [{"upper_ms": 0.1, "count": 500}])
        self.assertEqual(step["worst"]["histograms_by_op"]["SET"],
                         [{"upper_ms": 0.1, "count": 500}])

    def test_json_step_recorded_before_0_49_has_note(self):
        # A step with no histograms_by_op key was recorded before
        # 0.49.0: each pick carries a note naming that instead of
        # a histogram map — the same reason the Markdown view
        # renders, for the best and the worst pick separately.
        entries = [self._step_record(40000.0, histograms="missing"),
                   self._step_record(50000.0, histograms="missing")]
        step = best_worst_comparison(entries)["backends"][0][
            "sweeps"][0]
        self.assertNotIn("histograms_by_op", step["best"])
        self.assertNotIn("histograms_by_op", step["worst"])
        self.assertEqual(len(step["notes"]), 2)
        self.assertIn(
            "Keyspace sweep — 100 keys (best run",
            step["notes"][0])
        self.assertIn(
            "this step was recorded before 0.49.0, so it carries "
            "no per-op histograms.", step["notes"][0])
        self.assertIn(
            "Keyspace sweep — 100 keys (worst run",
            step["notes"][1])

    def test_json_step_with_no_per_op_latencies_has_note(self):
        entries = [self._step_record(40000.0, histograms="empty"),
                   self._step_record(50000.0, histograms="empty")]
        step = best_worst_comparison(entries)["backends"][0][
            "sweeps"][0]
        self.assertNotIn("histograms_by_op", step["best"])
        self.assertNotIn("histograms_by_op", step["worst"])
        self.assertEqual(len(step["notes"]), 2)
        self.assertIn(
            "this step recorded no per-op latencies, so there are "
            "no histograms to show.", step["notes"][0])

    def test_json_step_malformed_histograms_has_note(self):
        entries = [self._step_record(40000.0,
                                     histograms="malformed"),
                   self._step_record(50000.0,
                                     histograms="malformed")]
        step = best_worst_comparison(entries)["backends"][0][
            "sweeps"][0]
        self.assertNotIn("histograms_by_op", step["best"])
        self.assertNotIn("histograms_by_op", step["worst"])
        self.assertEqual(len(step["notes"]), 2)
        self.assertIn(
            "the recorded per-op histograms for this step are "
            "malformed, so no histograms are shown.",
            step["notes"][0])

    def test_json_step_histogram_comparison(self):
        # The core of the JSON side: each sweep entry carries
        # the step's per-op histogram comparison under
        # "histogram_comparison" — each op's p50/p99 in the
        # worst and best runs' steps, the p99 change, and the
        # bucket drift — the same numbers the Markdown table
        # shows, so the two views cannot disagree. The rows flag
        # that the step's p50/p99 are read off its histogram
        # buckets, not from per-op percentile fields.
        entries = [
            self._step_record(
                40000.0,
                set_buckets=[{"upper_ms": 0.1, "count": 400},
                             {"upper_ms": 0.25, "count": 100}]),
            self._step_record(
                60000.0,
                set_buckets=[{"upper_ms": 0.1, "count": 500}]),
        ]
        step = best_worst_comparison(entries)["backends"][0][
            "sweeps"][0]
        comparison = step["histogram_comparison"]
        self.assertEqual(comparison["from"], "worst")
        self.assertEqual(comparison["to"], "best")
        self.assertEqual(comparison["notes"], [])
        self.assertTrue(comparison["percentiles_from_histograms"])
        by_op = {row["op"]: row for row in comparison["rows"]}
        self.assertEqual(by_op["SET"]["p50_from_ms"], 0.1)
        self.assertEqual(by_op["SET"]["p50_to_ms"], 0.1)
        self.assertEqual(by_op["SET"]["p99_from_ms"], 0.25)
        self.assertEqual(by_op["SET"]["p99_to_ms"], 0.1)
        self.assertAlmostEqual(by_op["SET"]["p99_change_pct"], -60.0)
        self.assertAlmostEqual(by_op["SET"]["bucket_drift_pp"], 20.0)
        self.assertEqual(by_op["SET"]["bucket_drift_bucket"],
                         "<= 0.1 ms")
        self.assertAlmostEqual(by_op["GET"]["p99_change_pct"], 0.0)
        self.assertAlmostEqual(by_op["GET"]["bucket_drift_pp"], 0.0)
        # The Markdown view renders the same row from the same
        # shared helper.
        self.assertIn(
            "| SET | 0.100 | 0.100 | 0.250 | 0.100 | -60.0% | "
            "+20.0 pp (<= 0.1 ms) |", format_best_worst(entries))

    def test_json_step_histogram_comparison_single_run_has_note(self):
        entries = [self._step_record(50000.0, set_count=500)]
        comparison = best_worst_comparison(entries)["backends"][0][
            "sweeps"][0]["histogram_comparison"]
        self.assertIsNone(comparison["rows"])
        self.assertEqual(len(comparison["notes"]), 1)
        self.assertIn(
            "are the same run, so there is no best-vs-worst "
            "histogram comparison for this step.",
            comparison["notes"][0])

    def test_json_step_histogram_comparison_before_0_49_has_note(self):
        entries = [self._step_record(40000.0, histograms="missing"),
                   self._step_record(50000.0, histograms="missing")]
        comparison = best_worst_comparison(entries)["backends"][0][
            "sweeps"][0]["histogram_comparison"]
        self.assertIsNone(comparison["rows"])
        self.assertEqual(len(comparison["notes"]), 2)
        self.assertIn("Keyspace sweep — 100 keys (best run",
                      comparison["notes"][0])
        self.assertIn(
            "this step was recorded before 0.49.0, so it carries "
            "no per-op histograms.", comparison["notes"][0])
        self.assertIn(
            "No best-vs-worst histogram comparison is shown for "
            "this step.", comparison["notes"][0])
        self.assertIn("Keyspace sweep — 100 keys (worst run",
                      comparison["notes"][1])

    def test_json_step_histogram_comparison_overflow_shows_none(self):
        # A p50/p99 rank that lands in the overflow bucket has
        # no upper edge to report, so the JSON carries None for
        # that side — the same "-" the Markdown table shows.
        entries = [
            self._step_record(
                40000.0,
                set_buckets=[{"upper_ms": 0.1, "count": 80},
                             {"upper_ms": 0.25, "count": 20}]),
            self._step_record(
                60000.0,
                set_buckets=[{"upper_ms": 0.1, "count": 10},
                             {"upper_ms": 0.25, "count": 10},
                             {"upper_ms": None, "count": 80}]),
        ]
        comparison = best_worst_comparison(entries)["backends"][0][
            "sweeps"][0]["histogram_comparison"]
        by_op = {row["op"]: row for row in comparison["rows"]}
        self.assertEqual(by_op["SET"]["p50_from_ms"], 0.1)
        self.assertIsNone(by_op["SET"]["p50_to_ms"])
        self.assertEqual(by_op["SET"]["p99_from_ms"], 0.25)
        self.assertIsNone(by_op["SET"]["p99_to_ms"])
        self.assertIsNone(by_op["SET"]["p99_change_pct"])
        self.assertAlmostEqual(by_op["SET"]["bucket_drift_pp"], 80.0)
        self.assertEqual(by_op["SET"]["bucket_drift_bucket"],
                         "> 1000 ms")

    def test_json_file_round_trip_carries_sweep_histogram_comparison(self):
        entries = [
            self._step_record(
                40000.0,
                set_buckets=[{"upper_ms": 0.1, "count": 400},
                             {"upper_ms": 0.25, "count": 100}]),
            self._step_record(
                60000.0,
                set_buckets=[{"upper_ms": 0.1, "count": 500}]),
        ]
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "best-worst.json")
            comparison = write_best_worst_json(path, entries)
            with open(path, encoding="utf-8") as f:
                parsed = json.load(f)
            self.assertEqual(parsed, comparison)
            step_comparison = parsed["backends"][0]["sweeps"][0][
                "histogram_comparison"]
            by_op = {row["op"]: row
                     for row in step_comparison["rows"]}
            self.assertAlmostEqual(
                by_op["SET"]["p99_change_pct"], -60.0)
            self.assertTrue(
                step_comparison["percentiles_from_histograms"])

    def test_json_records_without_sweeps_have_empty_sweeps(self):
        # History written before 0.37.0 has no "sweeps" key: the
        # JSON carries an empty list, not a missing key.
        workload = _make_workload()
        entries = [
            build_record(_make_result(ops_per_sec=40000.0),
                         workload, "kvbench"),
            build_record(_make_result(ops_per_sec=50000.0),
                         workload, "kvbench"),
        ]
        for entry in entries:
            self.assertNotIn("sweeps", entry)
        backend = best_worst_comparison(entries)["backends"][0]
        self.assertEqual(backend["sweeps"], [])

    def test_json_step_with_non_numeric_ops_sec_is_skipped(self):
        # A step whose recorded ops/sec is not a number cannot be
        # ranked, so it gets no section instead of crashing the
        # comparison — the same rule the Markdown view uses.
        entries = [
            self._step_record(40000.0),
            self._step_record(50000.0),
        ]
        for entry in entries:
            entry["sweeps"]["keyspace"][0]["ops_per_sec"] = "fast"
        backend = best_worst_comparison(entries)["backends"][0]
        self.assertEqual(backend["sweeps"], [])

    def test_json_workload_filter_limits_sweep_sections(self):
        # With --compare-same-workload only matching runs are
        # compared: a step that ran only in a run with a
        # different workload gets no JSON section.
        other = Workload(ops=1000, clients=8, keys=1000,
                         value_size=32, set_weight=50, get_weight=40,
                         incr_weight=10, seed=1234, pipeline=1)
        entries = [
            self._step_record(40000.0, keys=100),
            self._step_record(50000.0, keys=100),
        ]
        foreign = self._step_record(60000.0, keys=200)
        foreign["workload"] = build_record(
            _make_result(), other, "kvbench")["workload"]
        entries.append(foreign)
        backend = best_worst_comparison(
            entries, workload=entries[0]["workload"])["backends"][0]
        steps = [s["step"] for s in backend["sweeps"]]
        self.assertEqual(steps, [100])

    def test_json_file_round_trip_carries_sweeps(self):
        entries = [
            self._step_record(40000.0, set_count=500),
            self._step_record(60000.0, set_count=300),
        ]
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "best-worst.json")
            comparison = write_best_worst_json(path, entries)
            with open(path, encoding="utf-8") as f:
                parsed = json.load(f)
            self.assertEqual(parsed, comparison)
            step = parsed["backends"][0]["sweeps"][0]
            self.assertEqual(step["step_label"], "100 keys")
            self.assertEqual(step["best"]["ops_per_sec"], 60000.0)
            self.assertEqual(
                step["best"]["histograms_by_op"]["SET"],
                [{"upper_ms": 0.1, "count": 300}])

    def test_workload_filter_limits_sweep_sections(self):
        # With --compare-same-workload only matching runs are
        # compared: a step that ran only in a run with a
        # different workload gets no section.
        other = Workload(ops=1000, clients=8, keys=1000,
                         value_size=32, set_weight=50, get_weight=40,
                         incr_weight=10, seed=1234, pipeline=1)
        entries = [
            self._step_record(40000.0, keys=100),
            self._step_record(50000.0, keys=100),
        ]
        foreign = self._step_record(60000.0, keys=200)
        foreign["workload"] = build_record(
            _make_result(), other, "kvbench")["workload"]
        entries.append(foreign)
        text = format_best_worst(entries,
                                 workload=entries[0]["workload"])
        self.assertIn("#### Keyspace sweep — 100 keys", text)
        self.assertNotIn("#### Keyspace sweep — 200 keys", text)


class TestWriteBestWorst(unittest.TestCase):
    def _entries(self, rates=(40000.0, 50000.0)):
        workload = _make_workload()
        return [
            build_record(_make_result(ops_per_sec=rate), workload,
                         "kvbench")
            for rate in rates
        ]

    def test_markdown_file_matches_stdout_text(self):
        entries = self._entries()
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "best-worst.md")
            text = write_best_worst(path, entries)
            self.assertEqual(text, format_best_worst(entries))
            with open(path, encoding="utf-8") as f:
                self.assertEqual(f.read(), text)

    def test_json_file_carries_best_and_worst(self):
        entries = self._entries()
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "best-worst.json")
            comparison = write_best_worst_json(path, entries)
            with open(path, encoding="utf-8") as f:
                parsed = json.load(f)
            self.assertEqual(parsed, comparison)
            backend = parsed["backends"][0]
            self.assertEqual(backend["best"]["ops_per_sec"], 50000.0)
            self.assertEqual(backend["worst"]["ops_per_sec"], 40000.0)
            self.assertAlmostEqual(backend["ops_sec_delta_pct"], 25.0)

    def test_json_file_carries_histograms(self):
        workload = _make_workload()
        entries = [
            build_record(_make_mixed_result(ops_per_sec=40000.0),
                         workload, "kvbench"),
            build_record(_make_mixed_result(ops_per_sec=60000.0),
                         workload, "kvbench"),
        ]
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "best-worst.json")
            comparison = write_best_worst_json(path, entries)
            with open(path, encoding="utf-8") as f:
                parsed = json.load(f)
            self.assertEqual(parsed, comparison)
            backend = parsed["backends"][0]
            self.assertEqual(
                backend["best"]["histograms_by_op"]["SET"],
                [{"upper_ms": 0.1, "count": 500}])
            self.assertEqual(
                backend["worst"]["histograms_by_op"]["SET"],
                [{"upper_ms": 0.1, "count": 500}])
            self.assertEqual(
                backend["per_op"]["SET"]["best"]["histogram"],
                [{"upper_ms": 0.1, "count": 500}])

    def test_json_file_carries_histogram_comparison(self):
        workload = _make_workload()
        entries = [
            build_record(_make_mixed_result(ops_per_sec=40000.0),
                         workload, "kvbench"),
            build_record(
                _make_result(
                    ops=1000, ops_per_sec=50000.0,
                    latencies_by_op={
                        "SET": [200_000] * 500,
                        "GET": [20_000] * 400,
                        "INCR": [20_000] * 100,
                    }),
                workload, "kvbench"),
        ]
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "best-worst.json")
            comparison = write_best_worst_json(path, entries)
            with open(path, encoding="utf-8") as f:
                parsed = json.load(f)
            self.assertEqual(parsed, comparison)
            histogram_comparison = parsed["backends"][0][
                "histogram_comparison"]
            self.assertEqual(histogram_comparison["from"], "worst")
            self.assertEqual(histogram_comparison["to"], "best")
            by_op = {row["op"]: row
                     for row in histogram_comparison["rows"]}
            self.assertAlmostEqual(
                by_op["SET"]["p99_change_pct"], 900.0)
            self.assertAlmostEqual(
                by_op["SET"]["bucket_drift_pp"], -100.0)

    def test_creates_parent_directory(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "reports", "best-worst.md")
            write_best_worst(path, self._entries())
            with open(path, encoding="utf-8") as f:
                self.assertIn("50,000", f.read())


class TestBestWorstCli(unittest.TestCase):
    def _record(self, path, rates, clients=4):
        workload = Workload(ops=1000, clients=clients, keys=1000,
                            value_size=32, set_weight=50,
                            get_weight=40, incr_weight=10, seed=1234,
                            pipeline=1)
        record_run(path,
                   [_make_result(ops_per_sec=rate) for rate in rates],
                   workload, "kvbench")

    def test_best_worst_flag_prints_comparison_and_exits_zero(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "history.jsonl")
            self._record(path, (40000.0, 60000.0))
            out = io.StringIO()
            with redirect_stdout(out):
                code = main(["--best-worst", "--history-file", path])
            self.assertEqual(code, 0)
            text = out.getvalue()
            self.assertIn("best vs worst", text)
            self.assertIn("60,000", text)
            self.assertIn("40,000", text)
            self.assertIn("+50.0%", text)

    def test_best_worst_prints_histograms(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "history.jsonl")
            record_run(
                path,
                [_make_mixed_result(ops_per_sec=40000.0),
                 _make_mixed_result(ops_per_sec=60000.0)],
                _make_workload(), "kvbench")
            out = io.StringIO()
            with redirect_stdout(out):
                code = main(["--best-worst", "--history-file", path])
            self.assertEqual(code, 0)
            text = out.getvalue()
            self.assertIn("Per-op latency histogram — best run", text)
            self.assertIn("Per-op latency histogram — worst run", text)
            self.assertIn("SET — latency histogram (best run)", text)
            self.assertIn("SET (500 ops)", text)

    def test_best_worst_with_empty_history_exits_zero(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "history.jsonl")
            out = io.StringIO()
            with redirect_stdout(out):
                code = main(["--best-worst", "--history-file", path])
            self.assertEqual(code, 0)
            self.assertIn("No runs recorded yet", out.getvalue())

    def test_best_worst_out_and_json_write_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            history = os.path.join(tmp, "history.jsonl")
            self._record(history, (40000.0, 50000.0))
            md_path = os.path.join(tmp, "best-worst.md")
            json_path = os.path.join(tmp, "best-worst.json")
            out = io.StringIO()
            with redirect_stdout(out):
                code = main(["--best-worst", "--history-file", history,
                             "--best-worst-out", md_path,
                             "--best-worst-json", json_path])
            self.assertEqual(code, 0)
            stdout = out.getvalue()
            self.assertIn(f"Best vs worst written to {md_path}", stdout)
            self.assertIn(
                f"Best vs worst JSON written to {json_path}", stdout)
            with open(md_path, encoding="utf-8") as f:
                self.assertIn("+25.0%", f.read())
            with open(json_path, encoding="utf-8") as f:
                parsed = json.load(f)
            self.assertEqual(
                parsed["backends"][0]["best"]["ops_per_sec"], 50000.0)

    def test_best_worst_out_without_flag_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            out_path = os.path.join(tmp, "best-worst.md")
            err = io.StringIO()
            with redirect_stdout(io.StringIO()):
                from contextlib import redirect_stderr
                with redirect_stderr(err):
                    code = main(["--best-worst-out", out_path])
            self.assertEqual(code, 2)
            self.assertIn("--best-worst-out needs --best-worst",
                          err.getvalue())
            self.assertFalse(os.path.exists(out_path))

    def test_best_worst_json_without_flag_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            out_path = os.path.join(tmp, "best-worst.json")
            err = io.StringIO()
            with redirect_stdout(io.StringIO()):
                from contextlib import redirect_stderr
                with redirect_stderr(err):
                    code = main(["--best-worst-json", out_path])
            self.assertEqual(code, 2)
            self.assertIn("--best-worst-json needs --best-worst",
                          err.getvalue())
            self.assertFalse(os.path.exists(out_path))

    def test_best_worst_with_same_workload_filters(self):
        # A 90000 ops/sec run with 8 clients must not become the
        # best run: the latest recorded run used 4 clients, so the
        # filter only sees the 4-client runs.
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "history.jsonl")
            self._record(path, (90000.0,), clients=8)
            self._record(path, (40000.0, 60000.0), clients=4)
            out = io.StringIO()
            with redirect_stdout(out):
                code = main(["--best-worst", "--history-file", path,
                             "--compare-same-workload"])
            self.assertEqual(code, 0)
            text = out.getvalue()
            self.assertIn("same workload as the latest recorded run",
                          text)
            self.assertIn("60,000", text)
            self.assertIn("40,000", text)
            self.assertNotIn("90,000", text)

    def test_compare_same_workload_without_compare_history_or_best_worst(self):
        err = io.StringIO()
        with redirect_stdout(io.StringIO()):
            from contextlib import redirect_stderr
            with redirect_stderr(err):
                code = main(["--compare-same-workload"])
        self.assertEqual(code, 2)
        self.assertIn("--compare-same-workload needs --compare-history "
                      "or --best-worst", err.getvalue())


class TestDefaultPath(unittest.TestCase):
    def test_default_path_is_under_home(self):
        self.assertTrue(DEFAULT_HISTORY_FILE.startswith(
            os.path.expanduser("~")))
        self.assertTrue(DEFAULT_HISTORY_FILE.endswith("history.jsonl"))


class TestExportHistory(unittest.TestCase):
    def _entries(self, rates=(40000.0, 50000.0)):
        workload = _make_workload()
        return [
            build_record(_make_result(ops_per_sec=rate), workload,
                         "kvbench")
            for rate in rates
        ]

    def test_export_writes_portable_json(self):
        entries = self._entries()
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "history.json")
            document = export_history(path, entries)
            self.assertEqual(document["tool"], "kvbench")
            self.assertEqual(document["version"], __version__)
            self.assertIn("exported_at", document)
            self.assertEqual(document["records"], entries)
            with open(path, encoding="utf-8") as f:
                parsed = json.load(f)
            self.assertEqual(parsed, document)

    def test_export_filters_to_one_backend(self):
        workload = _make_workload()
        entries = [
            build_record(_make_result(name="baseline (dict)",
                                      ops_per_sec=90000.0),
                         workload, "kvbench"),
            build_record(_make_result(ops_per_sec=40000.0), workload,
                         "kvbench"),
        ]
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "history.json")
            document = export_history(
                path, entries,
                backends=["cinder (127.0.0.1:6379)"])
            self.assertEqual(len(document["records"]), 1)
            self.assertEqual(document["records"][0]["ops_per_sec"],
                             40000.0)

    def test_export_of_empty_history_writes_empty_records(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "history.json")
            document = export_history(path, [])
            self.assertEqual(document["records"], [])
            with open(path, encoding="utf-8") as f:
                self.assertEqual(json.load(f)["records"], [])

    def test_export_creates_parent_directory(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "exports", "history.json")
            export_history(path, self._entries())
            with open(path, encoding="utf-8") as f:
                self.assertEqual(len(json.load(f)["records"]), 2)


class TestLoadExport(unittest.TestCase):
    def _write(self, path, text):
        with open(path, "w", encoding="utf-8") as f:
            f.write(text)

    def test_round_trip_through_export(self):
        workload = _make_workload()
        entries = [build_record(_make_result(), workload, "kvbench")]
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "history.json")
            export_history(path, entries)
            self.assertEqual(load_export(path), entries)

    def test_missing_file_raises_value_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(ValueError):
                load_export(os.path.join(tmp, "nope.json"))

    def test_invalid_json_raises_value_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "history.json")
            self._write(path, "not json at all")
            with self.assertRaises(ValueError):
                load_export(path)

    def test_top_level_list_raises_value_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "history.json")
            self._write(path, json.dumps([{"backend": "cinder"}]))
            with self.assertRaises(ValueError):
                load_export(path)

    def test_missing_records_list_raises_value_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "history.json")
            self._write(path, json.dumps({"tool": "kvbench"}))
            with self.assertRaises(ValueError):
                load_export(path)

    def test_record_without_backend_raises_value_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "history.json")
            self._write(path, json.dumps({"records": [
                {"timestamp": "2026-11-23T10:00:00+00:00"}]}))
            with self.assertRaises(ValueError):
                load_export(path)

    def test_record_without_timestamp_raises_value_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "history.json")
            self._write(path, json.dumps({"records": [
                {"backend": "cinder"}]}))
            with self.assertRaises(ValueError):
                load_export(path)

    def test_non_object_record_raises_value_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "history.json")
            self._write(path, json.dumps({"records": ["cinder"]}))
            with self.assertRaises(ValueError):
                load_export(path)


class TestImportHistory(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.history = os.path.join(self.tmp.name, "history.jsonl")
        self.export = os.path.join(self.tmp.name, "history.json")

    def tearDown(self):
        self.tmp.cleanup()

    def _entries(self, rates=(40000.0, 50000.0), name=None,
                 start_day=1):
        # Timestamps are set explicitly so the (backend,
        # timestamp) merge key is deterministic.
        workload = _make_workload()
        entries = []
        for day, rate in enumerate(rates, start=start_day):
            entry = build_record(
                _make_result(name=name or "cinder (127.0.0.1:6379)",
                             ops_per_sec=rate),
                workload, "kvbench")
            entry["timestamp"] = f"2026-11-0{day}T10:00:00+00:00"
            entries.append(entry)
        return entries

    def test_import_into_empty_history_adds_all(self):
        entries = self._entries()
        export_history(self.export, entries)
        summary = import_history(self.export, self.history)
        self.assertEqual(summary, {"added": 2, "duplicates": 0,
                                   "conflicts": 0})
        self.assertEqual(load_history(self.history), entries)

    def test_reimport_is_all_duplicates_and_adds_nothing(self):
        entries = self._entries()
        export_history(self.export, entries)
        import_history(self.export, self.history)
        summary = import_history(self.export, self.history)
        self.assertEqual(summary, {"added": 0, "duplicates": 2,
                                   "conflicts": 0})
        self.assertEqual(len(load_history(self.history)), 2)

    def test_conflict_keeps_the_existing_record(self):
        entries = self._entries()
        export_history(self.export, entries)
        import_history(self.export, self.history)
        # Same backend and timestamp as the first run, but a
        # different ops/sec: the existing record must win.
        changed = [dict(entries[0])]
        changed[0]["ops_per_sec"] = 99999.0
        export_history(self.export, changed)
        summary = import_history(self.export, self.history)
        self.assertEqual(summary, {"added": 0, "duplicates": 0,
                                   "conflicts": 1})
        self.assertEqual(load_history(self.history)[0]["ops_per_sec"],
                         40000.0)

    def test_new_records_are_appended_in_export_order(self):
        first = self._entries(rates=(40000.0,))
        export_history(self.export, first)
        import_history(self.export, self.history)
        # Later timestamps (days 2 and 3): both are new records.
        second = self._entries(rates=(50000.0, 60000.0), start_day=2)
        # Same backend, later timestamps: both are new.
        export_history(self.export, second)
        summary = import_history(self.export, self.history)
        self.assertEqual(summary["added"], 2)
        loaded = load_history(self.history)
        self.assertEqual([e["ops_per_sec"] for e in loaded],
                         [40000.0, 50000.0, 60000.0])

    def test_same_timestamp_different_backend_is_a_new_record(self):
        cinder = self._entries(rates=(40000.0,))
        baseline = self._entries(rates=(90000.0,),
                                 name="baseline (dict)")
        export_history(self.export, cinder + baseline)
        summary = import_history(self.export, self.history)
        self.assertEqual(summary["added"], 2)
        self.assertEqual(
            [e["backend"] for e in load_history(self.history)],
            ["cinder (127.0.0.1:6379)", "baseline (dict)"])

    def test_bad_export_raises_and_leaves_history_untouched(self):
        entries = self._entries()
        record_run(self.history, [_make_result()], _make_workload(),
                   "kvbench")
        before = load_history(self.history)
        with open(self.export, "w", encoding="utf-8") as f:
            f.write("not json at all")
        with self.assertRaises(ValueError):
            import_history(self.export, self.history)
        self.assertEqual(load_history(self.history), before)

    def test_import_creates_parent_directory(self):
        nested = os.path.join(self.tmp.name, "a", "b", "history.jsonl")
        export_history(self.export, self._entries())
        summary = import_history(self.export, nested)
        self.assertEqual(summary["added"], 2)
        self.assertEqual(len(load_history(nested)), 2)

    def test_format_import_names_each_outcome(self):
        text = format_import(
            {"added": 2, "duplicates": 1, "conflicts": 1},
            "history.json", "history.jsonl")
        self.assertIn("added 2 run(s)", text)
        self.assertIn("skipped 1 duplicate run(s)", text)
        self.assertIn("kept the existing record for 1 conflicting "
                      "run(s)", text)

    def test_format_import_with_nothing_new(self):
        text = format_import(
            {"added": 0, "duplicates": 0, "conflicts": 0},
            "history.json", "history.jsonl")
        self.assertIn("nothing new to import", text)


class TestHistoryExportImportCli(unittest.TestCase):
    def _record(self, path, rates, name=None):
        workload = _make_workload()
        record_run(path,
                   [_make_result(name=name or
                                 "cinder (127.0.0.1:6379)",
                                 ops_per_sec=rate)
                    for rate in rates],
                   workload, "kvbench")

    def test_export_flag_writes_file_and_exits_zero(self):
        with tempfile.TemporaryDirectory() as tmp:
            history = os.path.join(tmp, "history.jsonl")
            self._record(history, (40000.0, 50000.0))
            out_path = os.path.join(tmp, "history.json")
            out = io.StringIO()
            with redirect_stdout(out):
                code = main(["--history-export", out_path,
                             "--history-file", history])
            self.assertEqual(code, 0)
            self.assertIn("Exported 2 run(s)", out.getvalue())
            with open(out_path, encoding="utf-8") as f:
                parsed = json.load(f)
            self.assertEqual(len(parsed["records"]), 2)
            self.assertEqual(parsed["tool"], "kvbench")

    def test_export_backend_filters_the_export(self):
        with tempfile.TemporaryDirectory() as tmp:
            history = os.path.join(tmp, "history.jsonl")
            self._record(history, (40000.0,))
            self._record(history, (90000.0,), name="baseline (dict)")
            out_path = os.path.join(tmp, "history.json")
            out = io.StringIO()
            with redirect_stdout(out):
                code = main(["--history-export", out_path,
                             "--history-file", history,
                             "--history-export-backend",
                             "baseline (dict)"])
            self.assertEqual(code, 0)
            with open(out_path, encoding="utf-8") as f:
                parsed = json.load(f)
            self.assertEqual(len(parsed["records"]), 1)
            self.assertEqual(parsed["records"][0]["backend"],
                             "baseline (dict)")

    def test_export_backend_without_export_fails(self):
        err = io.StringIO()
        with redirect_stdout(io.StringIO()):
            with redirect_stderr(err):
                code = main(["--history-export-backend", "cinder"])
        self.assertEqual(code, 2)
        self.assertIn("--history-export-backend needs --history-export",
                      err.getvalue())

    def test_export_and_import_together_fail(self):
        err = io.StringIO()
        with redirect_stdout(io.StringIO()):
            with redirect_stderr(err):
                code = main(["--history-export", "a.json",
                             "--history-import", "b.json"])
        self.assertEqual(code, 2)
        self.assertIn("--history-export and --history-import are "
                      "mutually exclusive", err.getvalue())

    def test_import_flag_merges_and_reports_counts(self):
        with tempfile.TemporaryDirectory() as tmp:
            # Two source runs (Nov 1 and Nov 2); the target
            # already holds the Nov 1 run, identical, so the
            # import adds one run and skips one duplicate.
            workload = _make_workload()
            source_entries = []
            for day, rate in ((1, 40000.0), (2, 50000.0)):
                entry = build_record(_make_result(ops_per_sec=rate),
                                     workload, "kvbench")
                entry["timestamp"] = (
                    f"2026-11-0{day}T10:00:00+00:00")
                source_entries.append(entry)
            export_path = os.path.join(tmp, "history.json")
            export_history(export_path, source_entries)
            target = os.path.join(tmp, "target.jsonl")
            with open(target, "w", encoding="utf-8") as f:
                f.write(json.dumps(source_entries[0]) + "\n")
            out = io.StringIO()
            with redirect_stdout(out):
                code = main(["--history-import", export_path,
                             "--history-file", target])
            self.assertEqual(code, 0)
            text = out.getvalue()
            self.assertIn("added 1 run(s)", text)
            self.assertIn("skipped 1 duplicate run(s)", text)
            self.assertEqual(len(load_history(target)), 2)

    def test_import_bad_file_fails_with_exit_two(self):
        with tempfile.TemporaryDirectory() as tmp:
            history = os.path.join(tmp, "history.jsonl")
            self._record(history, (40000.0,))
            bad = os.path.join(tmp, "bad.json")
            with open(bad, "w", encoding="utf-8") as f:
                f.write('{"records": [{"backend": "cinder"}]}')
            err = io.StringIO()
            with redirect_stdout(io.StringIO()):
                with redirect_stderr(err):
                    code = main(["--history-import", bad,
                                 "--history-file", history])
            self.assertEqual(code, 2)
            self.assertIn("no 'timestamp'", err.getvalue())
            # The history file is untouched by the failed import.
            self.assertEqual(len(load_history(history)), 1)

    def test_import_missing_file_fails_with_exit_two(self):
        with tempfile.TemporaryDirectory() as tmp:
            history = os.path.join(tmp, "history.jsonl")
            err = io.StringIO()
            with redirect_stdout(io.StringIO()):
                with redirect_stderr(err):
                    code = main(["--history-import",
                                 os.path.join(tmp, "nope.json"),
                                 "--history-file", history])
            self.assertEqual(code, 2)
            self.assertIn("cannot read history export", err.getvalue())


if __name__ == "__main__":
    unittest.main()
