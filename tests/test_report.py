"""Tests for kvbench.report Markdown and JSON output."""

import json
import unittest

from kvbench.bench import BenchmarkResult, Workload
from kvbench.cli import json_path_for
from kvbench.report import render_json, render_markdown, to_json_dict


def _workload():
    return Workload(ops=100, clients=2, keys=10, value_size=8,
                    set_weight=50, get_weight=40, incr_weight=10)


def _result():
    return BenchmarkResult(
        name="cinder (127.0.0.1:6379)",
        ops=100,
        elapsed_s=0.01,
        errors=0,
        latencies_ns=[100_000] * 100,
    )


class TestReport(unittest.TestCase):
    def test_report_contains_results(self):
        workload = Workload(ops=100, clients=2, keys=10, value_size=8,
                            set_weight=50, get_weight=40, incr_weight=10)
        result = BenchmarkResult(
            name="cinder (127.0.0.1:6379)",
            ops=100,
            elapsed_s=0.01,
            errors=0,
            latencies_ns=[100_000] * 100,
        )
        text = render_markdown([result], workload,
                               "kvbench --ops 100 --clients 2")
        self.assertIn("# kvbench report", text)
        self.assertIn("cinder (127.0.0.1:6379)", text)
        self.assertIn("| Backend | Ops | Ops/sec |", text)
        self.assertIn("10,000", text)  # 100 ops / 0.01 s
        self.assertIn("0.100", text)  # mean latency ms
        self.assertIn("Total ops: 100", text)
        self.assertIn("Clients (threads): 2", text)
        self.assertIn("kvbench --ops 100 --clients 2", text)
        self.assertIn("machine-dependent", text)
        self.assertIn("baseline (dict)", text)  # the notes mention it

    def test_histogram_in_markdown(self):
        workload = _workload()
        result = _result()
        text = render_markdown([result], workload, "kvbench")
        self.assertIn("## Latency histograms", text)
        self.assertIn("Latency histogram — cinder (127.0.0.1:6379)", text)
        self.assertIn("<= 0.1 ms: 100 (100.0%)", text)

    def test_histogram_in_json(self):
        parsed = json.loads(render_json([_result()], _workload(),
                                        "kvbench"))
        hist = parsed["results"][0]["histogram"]
        self.assertEqual(hist, [{"upper_ms": 0.1, "count": 100}])

    def test_error_sample_listed(self):
        workload = Workload(ops=10, clients=1, keys=5)
        result = BenchmarkResult(
            name="cinder", ops=10, elapsed_s=0.01, errors=2,
            error_sample="ConnectionError: refused",
            latencies_ns=[100_000] * 8,
        )
        text = render_markdown([result], workload, "kvbench")
        self.assertIn("First error from cinder: ConnectionError: refused",
                      text)
        self.assertIn("| 2 |", text)

    def test_json_round_trip(self):
        doc = to_json_dict([_result()], _workload(),
                           "kvbench --ops 100 --clients 2")
        text = render_json([_result()], _workload(),
                           "kvbench --ops 100 --clients 2")
        parsed = json.loads(text)
        # to_json_dict and render_json run at different moments, so the
        # generated timestamp legitimately differs; compare the rest.
        self.assertIn("generated", parsed)
        parsed.pop("generated")
        doc.pop("generated")
        self.assertEqual(parsed, doc)
        self.assertEqual(parsed["tool"], "kvbench")
        self.assertEqual(parsed["version"], "0.2.4")
        self.assertEqual(parsed["command"], "kvbench --ops 100 --clients 2")
        self.assertEqual(parsed["workload"]["ops"], 100)
        self.assertEqual(parsed["workload"]["clients"], 2)
        self.assertEqual(parsed["workload"]["keys"], 10)
        self.assertEqual(parsed["workload"]["value_size"], 8)
        self.assertEqual(parsed["workload"]["mix"],
                         {"set": 50, "get": 40, "incr": 10})
        self.assertEqual(parsed["workload"]["seed"], 1234)
        self.assertEqual(parsed["workload"]["pipeline"], 1)
        row = parsed["results"][0]
        self.assertEqual(row["name"], "cinder (127.0.0.1:6379)")
        self.assertEqual(row["ops"], 100)
        self.assertAlmostEqual(row["ops_per_sec"], 10_000)
        self.assertEqual(row["errors"], 0)
        self.assertEqual(row["error_sample"], "")
        self.assertEqual(row["summary"]["count"], 100)
        self.assertAlmostEqual(row["summary"]["mean_ms"], 0.1)
        self.assertAlmostEqual(row["summary"]["p50_ms"], 0.1)
        self.assertAlmostEqual(row["summary"]["p95_ms"], 0.1)
        self.assertAlmostEqual(row["summary"]["p99_ms"], 0.1)

    def test_json_error_sample(self):
        result = BenchmarkResult(
            name="cinder", ops=10, elapsed_s=0.01, errors=2,
            error_sample="ConnectionError: refused",
            latencies_ns=[100_000] * 8,
        )
        parsed = json.loads(render_json([result], _workload(), "kvbench"))
        self.assertEqual(parsed["results"][0]["errors"], 2)
        self.assertEqual(parsed["results"][0]["error_sample"],
                         "ConnectionError: refused")

    def test_pipeline_in_reports(self):
        workload = Workload(ops=100, clients=2, keys=10, value_size=8,
                            pipeline=10)
        text = render_markdown([_result()], workload, "kvbench")
        self.assertIn("Pipeline depth: 10 command(s) per round trip", text)
        self.assertIn("amortized per-op", text)
        parsed = json.loads(render_json([_result()], workload, "kvbench"))
        self.assertEqual(parsed["workload"]["pipeline"], 10)

    def test_time_series_in_reports(self):
        workload = _workload()
        result = BenchmarkResult(
            name="cinder", ops=3, elapsed_s=2.5, errors=0,
            latencies_ns=[1_000_000, 2_000_000, 4_000_000],
            times_s=[0.2, 0.9, 2.3],
        )
        parsed = json.loads(render_json([result], workload, "kvbench"))
        series = parsed["results"][0]["time_series"]
        self.assertEqual(len(series), 3)
        self.assertEqual(series[0]["ops"], 2)
        self.assertAlmostEqual(series[0]["mean_ms"], 1.5)
        self.assertEqual(series[1]["ops"], 0)  # empty second kept
        self.assertEqual(series[2]["ops"], 1)
        text = render_markdown([result], workload, "kvbench")
        self.assertIn("## Latency over time", text)
        self.assertIn("Latency over time — cinder", text)
        self.assertIn("| 0 | 2 | 1.500 | 2.000 |", text)
        self.assertIn("| 1 | 0 | 0.000 | 0.000 |", text)
        self.assertIn("a second with 0 ops marks a stall", text)

    def test_time_series_empty_without_times(self):
        # Results built without times (e.g. older callers) render no
        # time-series section and an empty series in JSON.
        parsed = json.loads(render_json([_result()], _workload(),
                                        "kvbench"))
        self.assertEqual(parsed["results"][0]["time_series"], [])
        text = render_markdown([_result()], _workload(), "kvbench")
        self.assertNotIn("## Latency over time", text)

    def test_json_path_for(self):
        self.assertEqual(json_path_for("report.md", None), "report.json")
        self.assertEqual(json_path_for("out/run.md", None), "out/run.json")
        self.assertEqual(json_path_for("report", None), "report.json")
        self.assertEqual(json_path_for("report.md", "custom.json"),
                         "custom.json")


if __name__ == "__main__":
    unittest.main()
