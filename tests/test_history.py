"""Tests for kvbench.history: recording runs and the trend view."""

import io
import json
import os
import tempfile
import unittest
from contextlib import redirect_stdout

from kvbench import __version__
from kvbench.bench import BenchmarkResult, Workload
from kvbench.cli import main
from kvbench.history import DEFAULT_HISTORY_FILE, build_record, \
    format_history, format_trend, load_history, record_run, \
    write_trend, _sparkline


def _make_result(name="cinder (127.0.0.1:6379)", ops=1000,
                 ops_per_sec=50000.0):
    # 1000 samples of 20_000 ns each: mean/p50/p95/p99 all 0.020 ms.
    return BenchmarkResult(
        name=name, ops=ops, elapsed_s=ops / ops_per_sec,
        latencies_ns=[20_000] * ops)


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


class TestDefaultPath(unittest.TestCase):
    def test_default_path_is_under_home(self):
        self.assertTrue(DEFAULT_HISTORY_FILE.startswith(
            os.path.expanduser("~")))
        self.assertTrue(DEFAULT_HISTORY_FILE.endswith("history.jsonl"))


if __name__ == "__main__":
    unittest.main()
