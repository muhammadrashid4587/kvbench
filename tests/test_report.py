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


def _result_with_per_op():
    return BenchmarkResult(
        name="cinder (127.0.0.1:6379)",
        ops=100,
        elapsed_s=0.01,
        errors=0,
        latencies_ns=[100_000] * 60 + [200_000] * 40,
        latencies_by_op={
            "SET": [100_000] * 60,
            "GET": [200_000] * 40,
        },
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
        self.assertEqual(parsed["version"], "0.6.0")
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

    def test_zipfian_in_reports(self):
        workload = Workload(ops=100, clients=2, keys=1000, value_size=8,
                            distribution="zipfian", zipf_skew=1.0)
        result = BenchmarkResult(
            name="cinder", ops=100, elapsed_s=1.0, errors=0,
            latencies_ns=[100_000] * 100, hot_ops=39)
        text = render_markdown([result], workload, "kvbench")
        self.assertIn("Key distribution: zipfian (skew 1", text)
        self.assertIn("hottest 1% of keys take ~39% of ops", text)
        self.assertIn("Hot-set hit rate on cinder: 39.0%", text)
        parsed = json.loads(render_json([result], workload, "kvbench"))
        self.assertEqual(parsed["workload"]["distribution"], "zipfian")
        self.assertEqual(parsed["workload"]["zipf_skew"], 1.0)
        self.assertAlmostEqual(parsed["results"][0]["hot_hit_rate"], 0.39)

    def test_uniform_distribution_in_reports(self):
        workload = _workload()
        text = render_markdown([_result()], workload, "kvbench")
        self.assertIn("Key distribution: uniform", text)
        self.assertNotIn("Hot-set hit rate", text)
        parsed = json.loads(render_json([_result()], workload, "kvbench"))
        self.assertEqual(parsed["workload"]["distribution"], "uniform")
        self.assertIsNone(parsed["workload"]["zipf_skew"])
        self.assertEqual(parsed["results"][0]["hot_hit_rate"], 0.0)

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

    def test_per_op_in_json(self):
        parsed = json.loads(render_json([_result_with_per_op()],
                                        _workload(), "kvbench"))
        per_op = parsed["results"][0]["per_op"]
        self.assertEqual(per_op["SET"]["count"], 60)
        self.assertAlmostEqual(per_op["SET"]["mean_ms"], 0.1)
        self.assertAlmostEqual(per_op["SET"]["ops_per_sec"], 6_000)
        self.assertEqual(per_op["GET"]["count"], 40)
        self.assertAlmostEqual(per_op["GET"]["mean_ms"], 0.2)
        self.assertAlmostEqual(per_op["GET"]["ops_per_sec"], 4_000)
        self.assertNotIn("INCR", per_op)  # never ran: left out

    def test_per_op_in_markdown(self):
        text = render_markdown([_result_with_per_op()], _workload(),
                               "kvbench")
        self.assertIn("## Per-operation stats", text)
        self.assertIn(
            "Per-operation stats — cinder (127.0.0.1:6379)", text)
        self.assertIn("| Op | Ops | Ops/sec | Mean (ms) | p50 (ms) | "
                      "p95 (ms) | p99 (ms) |", text)
        self.assertIn("| SET | 60 | 6,000 | 0.100 |", text)
        self.assertIn("| GET | 40 | 4,000 | 0.200 |", text)
        self.assertNotIn("| INCR |", text)

    def test_per_op_absent_without_split(self):
        # Results built without per-op latencies (e.g. older callers)
        # render no per-op section and an empty split in JSON.
        parsed = json.loads(render_json([_result()], _workload(),
                                        "kvbench"))
        self.assertEqual(parsed["results"][0]["per_op"], {})
        text = render_markdown([_result()], _workload(), "kvbench")
        self.assertNotIn("## Per-operation stats", text)

    def test_mix_label_in_reports(self):
        workload = Workload(ops=100, clients=2, keys=10, value_size=8,
                            set_weight=15, get_weight=80, incr_weight=5,
                            mix_label="read-heavy")
        text = render_markdown([_result()], workload, "kvbench")
        self.assertIn("Mix: SET 15 / GET 80 / INCR 5 (read-heavy)", text)
        parsed = json.loads(render_json([_result()], workload, "kvbench"))
        self.assertEqual(parsed["workload"]["mix_label"], "read-heavy")
        # No label for an explicit weight string.
        parsed_plain = json.loads(render_json([_result()], _workload(),
                                              "kvbench"))
        self.assertIsNone(parsed_plain["workload"]["mix_label"])

    def test_warmup_in_reports(self):
        workload = Workload(ops=100, clients=2, keys=10, value_size=8,
                            warmup_s=2)
        result = BenchmarkResult(
            name="cinder", ops=100, elapsed_s=0.01, errors=0,
            latencies_ns=[100_000] * 100, warmup_ops=1234)
        text = render_markdown([result], workload, "kvbench")
        self.assertIn("Warmup: 2s", text)
        self.assertIn("Warmup ops on cinder: 1234", text)
        self.assertIn("excluded from the results", text)
        parsed = json.loads(render_json([result], workload, "kvbench"))
        self.assertEqual(parsed["workload"]["warmup_s"], 2)
        self.assertEqual(parsed["results"][0]["warmup_ops"], 1234)

    def test_no_warmup_omits_warmup_lines(self):
        workload = _workload()
        text = render_markdown([_result()], workload, "kvbench")
        self.assertNotIn("Warmup:", text)
        self.assertNotIn("Warmup ops on", text)
        parsed = json.loads(render_json([_result()], workload, "kvbench"))
        self.assertEqual(parsed["workload"]["warmup_s"], 0.0)
        self.assertEqual(parsed["results"][0]["warmup_ops"], 0)

    def test_json_path_for(self):
        self.assertEqual(json_path_for("report.md", None), "report.json")
        self.assertEqual(json_path_for("out/run.md", None), "out/run.json")
        self.assertEqual(json_path_for("report", None), "report.json")
        self.assertEqual(json_path_for("report.md", "custom.json"),
                         "custom.json")


class TestDurationReports(unittest.TestCase):
    def test_duration_in_markdown(self):
        workload = Workload(clients=2, keys=10, value_size=8,
                            duration_s=10)
        result = BenchmarkResult(
            name="cinder (127.0.0.1:6379)",
            ops=52341,
            elapsed_s=10.02,
            errors=0,
            latencies_ns=[100_000] * 100,
        )
        text = render_markdown([result], workload, "kvbench --duration 10s")
        self.assertIn("Duration: 10s", text)
        self.assertNotIn("Total ops:", text)
        self.assertIn("In duration mode the run stops after the "
                      "requested", text)
        # Throughput over the elapsed time is in the results table.
        self.assertIn("5,224", text)  # 52341 / 10.02 ops/sec

    def test_duration_in_json(self):
        workload = Workload(clients=2, keys=10, value_size=8,
                            duration_s=90)
        parsed = json.loads(render_json([_result()], workload, "kvbench"))
        self.assertEqual(parsed["workload"]["duration_s"], 90)
        self.assertIsNone(parsed["workload"]["ops"])
        row = parsed["results"][0]
        self.assertEqual(row["ops"], 100)
        self.assertAlmostEqual(row["ops_per_sec"], 10_000)

    def test_ops_mode_has_no_duration(self):
        parsed = json.loads(render_json([_result()], _workload(),
                                        "kvbench"))
        self.assertIsNone(parsed["workload"]["duration_s"])
        self.assertEqual(parsed["workload"]["ops"], 100)
        text = render_markdown([_result()], _workload(), "kvbench")
        self.assertIn("Total ops: 100", text)
        self.assertNotIn("In duration mode", text)


if __name__ == "__main__":
    unittest.main()
