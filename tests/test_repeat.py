"""Tests for --repeat N: run the benchmark N times and report the
median ops/sec and p99 over the N runs.

The reported ops/sec and p99 (overall and per operation) are the
medians over the per-run values, the per-run values are recorded in
the JSON report under ``results[].repeat.runs``, and a single run
(``--repeat 1`` or no flag) keeps the old JSON shape exactly — no
``repeat`` key anywhere.
"""

import json
import os
import tempfile
import unittest
from contextlib import redirect_stdout
from io import StringIO

from kvbench.bench import (DictClient, Workload, run_repeated_workload)
from kvbench.cli import build_parser, main
from kvbench.history import build_record
from kvbench.report import render_json, render_markdown, to_json_dict
from kvbench.stats import histogram, median

try:
    from test_cli import _start_server
except ModuleNotFoundError:  # pragma: no cover - import fallback
    # The fake RESP server lives in test_cli; when this module is
    # loaded outside `discover -s tests` (which puts the tests dir
    # on sys.path), add the tests dir so the import still resolves.
    import sys

    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from test_cli import _start_server


def _median_of(values):
    ordered = sorted(values)
    n = len(ordered)
    mid = n // 2
    if n % 2:
        return ordered[mid]
    return (ordered[mid - 1] + ordered[mid]) / 2


class TestMedian(unittest.TestCase):
    def test_odd_count_is_the_middle_value(self):
        self.assertEqual(median([3, 1, 2]), 2)

    def test_even_count_is_the_mean_of_the_middle_two(self):
        self.assertEqual(median([1, 2, 3, 4]), 2.5)

    def test_single_value_is_itself(self):
        self.assertEqual(median([7]), 7)

    def test_empty_raises(self):
        with self.assertRaises(ValueError):
            median([])


class TestRepeatValidation(unittest.TestCase):
    def test_workload_rejects_zero_repeat(self):
        with self.assertRaises(ValueError):
            Workload(repeat=0)

    def test_workload_rejects_negative_repeat(self):
        with self.assertRaises(ValueError):
            Workload(repeat=-2)

    def test_parser_accepts_repeat(self):
        self.assertEqual(
            build_parser().parse_args(["--repeat", "3"]).repeat, 3)

    def test_non_positive_repeat_exits_2_before_any_benchmark(self):
        # The validation runs before any server is contacted, so no
        # fake server is needed: 0 and -1 both fail fast.
        self.assertEqual(main(["--repeat", "0", "--ops", "10"]), 2)
        self.assertEqual(main(["--repeat", "-1", "--ops", "10"]), 2)

    def test_non_integer_repeat_exits_2(self):
        with self.assertRaises(SystemExit):
            build_parser().parse_args(["--repeat", "abc"])


class TestRepeatedWorkload(unittest.TestCase):
    def test_medians_come_from_the_per_run_values(self):
        workload = Workload(ops=200, clients=2)
        result = run_repeated_workload(DictClient, workload,
                                       "baseline (dict)", 3)
        self.assertEqual(len(result.repeat_runs), 3)
        self.assertEqual([entry["run"] for entry in result.repeat_runs],
                         [1, 2, 3])
        runs = result.repeat_runs
        self.assertEqual(
            result.ops_per_sec,
            _median_of([entry["ops_per_sec"] for entry in runs]))
        self.assertEqual(
            result.summary["p99_ms"],
            _median_of([entry["p99_ms"] for entry in runs]))
        for op in ("SET", "GET", "INCR"):
            self.assertEqual(
                result.per_op_summary[op]["ops_per_sec"],
                _median_of([entry["per_op"][op]["ops_per_sec"]
                            for entry in runs]))
            self.assertEqual(
                result.per_op_summary[op]["p99_ms"],
                _median_of([entry["per_op"][op]["p99_ms"]
                            for entry in runs]))

    def test_representative_run_carries_real_latencies(self):
        workload = Workload(ops=200, clients=2)
        result = run_repeated_workload(DictClient, workload,
                                       "baseline (dict)", 3)
        # Mean, p50 and the histogram come from the representative
        # (median) run's own latencies, not from the medians.
        self.assertGreater(result.summary["count"], 0)
        self.assertGreater(result.summary["mean_ms"], 0.0)
        self.assertTrue(result.histogram)

    def test_repeat_one_is_a_single_run(self):
        workload = Workload(ops=200, clients=2)
        result = run_repeated_workload(DictClient, workload,
                                       "baseline (dict)", 1)
        self.assertEqual(len(result.repeat_runs), 1)
        self.assertEqual(result.ops_per_sec,
                         result.repeat_runs[0]["ops_per_sec"])
        self.assertEqual(result.summary["p99_ms"],
                         result.repeat_runs[0]["p99_ms"])

    def test_each_run_carries_per_op_histograms(self):
        workload = Workload(ops=200, clients=2)
        result = run_repeated_workload(DictClient, workload,
                                       "baseline (dict)", 3)
        for entry in result.repeat_runs:
            per_op = entry["histograms_by_op"]
            # The same ops that ran, with the same log-spaced
            # buckets as the main run's histograms_by_op.
            self.assertEqual(set(per_op), set(entry["per_op"]))
            for op, buckets in per_op.items():
                self.assertEqual(
                    sum(b["count"] for b in buckets),
                    entry["per_op"][op]["ops"])
                # The bucket edges are the same fixed log-spaced
                # edges as the main run's histogram, whatever the
                # samples: compare against a histogram built from
                # any sample set.
                self.assertEqual(
                    [b["upper_ms"] for b in buckets],
                    [b["upper_ms"]
                     for b in histogram(result.latencies_ns)])
                self.assertTrue(all(b["count"] >= 0 for b in buckets))

    def test_repeat_histograms_render_in_markdown_and_json(self):
        workload = Workload(ops=200, clients=2, repeat=3)
        result = run_repeated_workload(DictClient, workload,
                                       "baseline (dict)", 3)
        text = render_markdown([result], workload, "kvbench")
        self.assertIn("#### Run 1 — per-op latency histogram", text)
        self.assertIn("#### Run 3 — per-op latency histogram", text)
        self.assertIn("SET (", text)
        parsed = json.loads(render_json([result], workload, "kvbench"))
        runs = parsed["results"][0]["repeat"]["runs"]
        self.assertEqual(len(runs), 3)
        for entry in runs:
            self.assertEqual(set(entry["histograms_by_op"]),
                             set(entry["per_op"]))
            for op, buckets in entry["histograms_by_op"].items():
                self.assertEqual(sum(b["count"] for b in buckets),
                                 entry["per_op"][op]["ops"])

    def test_repeat_without_histograms_renders_table_only(self):
        # Entries recorded before 0.34.0 carry no
        # histograms_by_op: the table still renders and no
        # per-run histogram block is invented.
        workload = Workload(ops=200, clients=2, repeat=3)
        result = run_repeated_workload(DictClient, workload,
                                       "baseline (dict)", 3)
        for entry in result.repeat_runs:
            del entry["histograms_by_op"]
        text = render_markdown([result], workload, "kvbench")
        self.assertIn("Repeat runs", text)
        self.assertIn("Median of 3 runs", text)
        self.assertNotIn("— per-op latency histogram", text)
        parsed = json.loads(render_json([result], workload, "kvbench"))
        for entry in parsed["results"][0]["repeat"]["runs"]:
            self.assertNotIn("histograms_by_op", entry)

    def test_repeat_zero_raises(self):
        with self.assertRaises(ValueError):
            run_repeated_workload(DictClient, Workload(ops=10),
                                  "baseline (dict)", 0)

    def test_history_record_uses_the_medians(self):
        workload = Workload(ops=200, clients=2)
        result = run_repeated_workload(DictClient, workload,
                                       "baseline (dict)", 3)
        record = build_record(result, workload, "kvbench --repeat 3")
        self.assertEqual(record["ops_per_sec"], result.ops_per_sec)
        self.assertEqual(record["p99_ms"], result.summary["p99_ms"])


class TestRepeatCli(unittest.TestCase):
    def test_repeat_run_records_per_run_values_in_json(self):
        server = _start_server()
        try:
            with tempfile.TemporaryDirectory() as tmp:
                output = os.path.join(tmp, "report.md")
                out = StringIO()
                with redirect_stdout(out):
                    code = main([
                        "--host", "127.0.0.1",
                        "--port", str(server.server_address[1]),
                        "--ops", "200", "--clients", "2",
                        "--repeat", "3",
                        "--output", output,
                    ])
                self.assertEqual(code, 0)
                stdout = out.getvalue()
                self.assertIn("run 1/3:", stdout)
                self.assertIn("run 3/3:", stdout)
                self.assertIn("median of 3 runs", stdout)
                with open(os.path.join(tmp, "report.json"),
                          encoding="utf-8") as f:
                    doc = json.load(f)
                self.assertEqual(doc["workload"]["repeat"], 3)
                entry = doc["results"][0]
                self.assertEqual(len(entry["repeat"]["runs"]), 3)
                runs = entry["repeat"]["runs"]
                self.assertEqual(
                    entry["ops_per_sec"],
                    _median_of([r["ops_per_sec"] for r in runs]))
                self.assertEqual(
                    entry["summary"]["p99_ms"],
                    _median_of([r["p99_ms"] for r in runs]))
                self.assertEqual(
                    entry["repeat"]["median_ops_per_sec"],
                    entry["ops_per_sec"])
                self.assertEqual(
                    entry["repeat"]["median_p99_ms"],
                    entry["summary"]["p99_ms"])
                # Each run carries its own per-op histograms, in
                # both reports.
                for run in runs:
                    self.assertEqual(set(run["histograms_by_op"]),
                                     set(run["per_op"]))
                    for op, buckets in run["histograms_by_op"].items():
                        self.assertEqual(
                            sum(b["count"] for b in buckets),
                            run["per_op"][op]["ops"])
                with open(output, encoding="utf-8") as f:
                    text = f.read()
                self.assertIn("Repeat runs", text)
                self.assertIn("Median of 3 runs", text)
                self.assertIn("- Repeat: 3 runs", text)
                self.assertIn("#### Run 1 — per-op latency histogram",
                              text)
                self.assertIn("#### Run 3 — per-op latency histogram",
                              text)
        finally:
            server.shutdown()
            server.server_close()


class TestSingleRunJsonUnchanged(unittest.TestCase):
    def test_no_repeat_keys_for_a_single_run(self):
        server = _start_server()
        try:
            with tempfile.TemporaryDirectory() as tmp:
                output = os.path.join(tmp, "report.md")
                out = StringIO()
                with redirect_stdout(out):
                    code = main([
                        "--host", "127.0.0.1",
                        "--port", str(server.server_address[1]),
                        "--ops", "200", "--clients", "2",
                        "--output", output,
                    ])
                self.assertEqual(code, 0)
                self.assertNotIn("median of", out.getvalue())
                with open(os.path.join(tmp, "report.json"),
                          encoding="utf-8") as f:
                    doc = json.load(f)
                self.assertNotIn("repeat", doc["workload"])
                self.assertNotIn("repeat", doc["results"][0])
                with open(output, encoding="utf-8") as f:
                    text = f.read()
                self.assertNotIn("Repeat runs", text)
        finally:
            server.shutdown()
            server.server_close()

    def test_explicit_repeat_one_keeps_the_old_shape(self):
        server = _start_server()
        try:
            with tempfile.TemporaryDirectory() as tmp:
                output = os.path.join(tmp, "report.md")
                out = StringIO()
                with redirect_stdout(out):
                    code = main([
                        "--host", "127.0.0.1",
                        "--port", str(server.server_address[1]),
                        "--ops", "200", "--clients", "2",
                        "--repeat", "1",
                        "--output", output,
                    ])
                self.assertEqual(code, 0)
                with open(os.path.join(tmp, "report.json"),
                          encoding="utf-8") as f:
                    doc = json.load(f)
                self.assertNotIn("repeat", doc["workload"])
                self.assertNotIn("repeat", doc["results"][0])
        finally:
            server.shutdown()
            server.server_close()

    def test_to_json_dict_shape_for_a_plain_result(self):
        # A result built the old way (no repeat fields set) renders
        # exactly the old keys, in the old order.
        workload = Workload(ops=200, clients=2)
        result = run_repeated_workload(DictClient, workload,
                                       "baseline (dict)", 1)
        plain = type(result)(
            name=result.name, ops=result.ops,
            elapsed_s=result.elapsed_s, errors=result.errors,
            latencies_ns=list(result.latencies_ns),
            times_s=list(result.times_s),
            latencies_by_op=dict(result.latencies_by_op))
        doc = to_json_dict([plain], workload, "kvbench")
        self.assertNotIn("repeat", doc["workload"])
        self.assertNotIn("repeat", doc["results"][0])


if __name__ == "__main__":
    unittest.main()
