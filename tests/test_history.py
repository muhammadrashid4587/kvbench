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
    record_run, write_best_worst, write_best_worst_json, \
    write_trend, _sparkline


def _make_result(name="cinder (127.0.0.1:6379)", ops=1000,
                 ops_per_sec=50000.0, latencies_by_op=None):
    # 1000 samples of 20_000 ns each: mean/p50/p95/p99 all 0.020 ms.
    return BenchmarkResult(
        name=name, ops=ops, elapsed_s=ops / ops_per_sec,
        latencies_ns=[20_000] * ops,
        latencies_by_op=latencies_by_op or {})


def _make_mixed_result(ops_per_sec=50000.0):
    # A 50:40:10 SET:GET:INCR mix, so per-op trends have three ops
    # to track: SET 500 samples, GET 400, INCR 100, all 20_000 ns.
    return _make_result(
        ops=1000, ops_per_sec=ops_per_sec,
        latencies_by_op={
            "SET": [20_000] * 500,
            "GET": [20_000] * 400,
            "INCR": [20_000] * 100,
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
