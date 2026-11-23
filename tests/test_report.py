"""Tests for kvbench.report Markdown and JSON output."""

import csv
import io
import json
import unittest

from kvbench.bench import BenchmarkResult, Workload
from kvbench.cli import json_path_for
from kvbench.report import render_csv, render_json, render_markdown, \
    to_json_dict, write_csv


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


def _result_with_per_key():
    return BenchmarkResult(
        name="cinder (127.0.0.1:6379)",
        ops=100,
        elapsed_s=0.01,
        errors=0,
        latencies_ns=[100_000] * 60 + [200_000] * 40,
        latencies_by_key={
            "kvbench:0": [100_000] * 40,
            "kvbench:1": [200_000] * 30,
            "kvbench:2": [100_000] * 30,
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
        self.assertEqual(parsed["version"], "0.13.0")
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

    def test_csv_rows_from_time_series(self):
        result = BenchmarkResult(
            name="cinder", ops=3, elapsed_s=2.5, errors=0,
            latencies_ns=[1_000_000, 2_000_000, 4_000_000],
            times_s=[0.2, 0.9, 2.3],
        )
        rows = list(csv.DictReader(io.StringIO(render_csv([result]))))
        self.assertEqual(len(rows), 3)
        # Each row covers one second, so ops_per_sec is the ops in
        # that second; mean/max are the per-second latencies.
        self.assertEqual(rows[0]["backend"], "cinder")
        self.assertEqual(rows[0]["second"], "0")
        self.assertEqual(rows[0]["ops"], "2")
        self.assertEqual(rows[0]["ops_per_sec"], "2")
        self.assertEqual(rows[0]["mean_ms"], "1.500")
        self.assertEqual(rows[0]["max_ms"], "2.000")
        # The empty second is kept: a stall shows up as a 0-op row.
        self.assertEqual(rows[1]["second"], "1")
        self.assertEqual(rows[1]["ops"], "0")
        self.assertEqual(rows[1]["ops_per_sec"], "0")
        self.assertEqual(rows[1]["mean_ms"], "0.000")
        self.assertEqual(rows[2]["second"], "2")
        self.assertEqual(rows[2]["ops"], "1")

    def test_csv_header_and_multiple_backends(self):
        first = BenchmarkResult(
            name="cinder", ops=1, elapsed_s=0.5, errors=0,
            latencies_ns=[1_000_000], times_s=[0.1],
        )
        second = BenchmarkResult(
            name="baseline (dict)", ops=1, elapsed_s=0.5, errors=0,
            latencies_ns=[500_000], times_s=[0.1],
        )
        text = render_csv([first, second])
        self.assertTrue(text.startswith(
            "backend,second,ops,ops_per_sec,mean_ms,max_ms\r\n"))
        rows = list(csv.DictReader(io.StringIO(text)))
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["backend"], "cinder")
        self.assertEqual(rows[1]["backend"], "baseline (dict)")
        self.assertEqual(rows[1]["mean_ms"], "0.500")

    def test_csv_header_only_without_times(self):
        # Results built without completion times have no series, so
        # the CSV is just the header — it still parses cleanly.
        rows = list(csv.DictReader(io.StringIO(render_csv([_result()]))))
        self.assertEqual(rows, [])

    def test_write_csv_round_trip(self):
        import os
        import tempfile
        result = BenchmarkResult(
            name="cinder", ops=1, elapsed_s=0.5, errors=0,
            latencies_ns=[1_000_000], times_s=[0.1],
        )
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "timeseries.csv")
            self.assertEqual(write_csv([result], path),
                             render_csv([result]))
            with open(path, encoding="utf-8", newline="") as f:
                rows = list(csv.DictReader(f))
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["backend"], "cinder")

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

    def test_hot_keys_in_json(self):
        parsed = json.loads(render_json([_result_with_per_key()],
                                        _workload(), "kvbench"))
        self.assertEqual(parsed["workload"]["hot_keys"], 5)
        hot = parsed["results"][0]["hot_keys"]
        self.assertEqual(len(hot), 3)
        # Ordered by access count: kvbench:0 (40) first, then the
        # two 30-op keys in key order.
        self.assertEqual(hot[0]["key"], "kvbench:0")
        self.assertEqual(hot[0]["count"], 40)
        self.assertAlmostEqual(hot[0]["mean_ms"], 0.1)
        self.assertEqual(hot[1]["key"], "kvbench:1")
        self.assertAlmostEqual(hot[1]["mean_ms"], 0.2)
        self.assertEqual(hot[2]["key"], "kvbench:2")

    def test_hot_keys_in_markdown(self):
        text = render_markdown([_result_with_per_key()], _workload(),
                               "kvbench")
        self.assertIn("## Per-key latency (hottest keys)", text)
        self.assertIn(
            "Per-key latency — cinder (127.0.0.1:6379)", text)
        self.assertIn(
            "| Key | Ops | Mean (ms) | p50 (ms) | p95 (ms) | p99 (ms) |",
            text)
        self.assertIn("| `kvbench:0` | 40 | 0.100 |", text)
        self.assertIn("| `kvbench:1` | 30 | 0.200 |", text)
        self.assertIn("hottest keys by access count", text)

    def test_hot_keys_absent_without_per_key_data(self):
        # Results built without per-key latencies (e.g. older callers
        # or --hot-keys 0) render no per-key section and an empty
        # list in JSON.
        parsed = json.loads(render_json([_result()], _workload(),
                                        "kvbench"))
        self.assertEqual(parsed["results"][0]["hot_keys"], [])
        text = render_markdown([_result()], _workload(), "kvbench")
        self.assertNotIn("## Per-key latency", text)
        self.assertNotIn("hottest keys by access count", text)

    def test_hot_keys_zero_omits_section(self):
        workload = Workload(ops=100, clients=2, keys=10, value_size=8,
                            hot_keys=0)
        parsed = json.loads(render_json([_result_with_per_key()],
                                        workload, "kvbench"))
        self.assertEqual(parsed["workload"]["hot_keys"], 0)
        self.assertEqual(parsed["results"][0]["hot_keys"], [])
        text = render_markdown([_result_with_per_key()], workload,
                               "kvbench")
        self.assertNotIn("## Per-key latency", text)

    def test_hot_keys_top_n_respected(self):
        workload = Workload(ops=100, clients=2, keys=10, value_size=8,
                            hot_keys=2)
        parsed = json.loads(render_json([_result_with_per_key()],
                                        workload, "kvbench"))
        hot = parsed["results"][0]["hot_keys"]
        self.assertEqual(len(hot), 2)
        self.assertEqual(hot[0]["key"], "kvbench:0")
        text = render_markdown([_result_with_per_key()], workload,
                               "kvbench")
        self.assertIn("| `kvbench:0` | 40 |", text)
        self.assertNotIn("`kvbench:2`", text)

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

    def test_sweep_in_markdown_and_json(self):
        workload = Workload(ops=100, clients=2, keys=10, value_size=8,
                            sweep_keyspace=[10, 100])
        result = BenchmarkResult(
            name="cinder", ops=100, elapsed_s=0.01, errors=0,
            latencies_ns=[100_000] * 100,
            sweep=[
                {"keys": 10, "ops": 100, "ops_per_sec": 10_000,
                 "mean_ms": 0.1, "p99_ms": 0.12},
                {"keys": 100, "ops": 100, "ops_per_sec": 9_000,
                 "mean_ms": 0.11, "p99_ms": 0.15},
            ],
        )
        text = render_markdown([result], workload, "kvbench")
        self.assertIn("Keyspace sweep: 10, 100 keys", text)
        self.assertIn("## Keyspace sweep", text)
        self.assertIn("Keyspace sweep — cinder", text)
        self.assertIn("| Keys | Ops | Ops/sec | Mean (ms) | p99 (ms) |",
                      text)
        self.assertIn("| 10 | 100 | 10,000 | 0.100 | 0.120 |", text)
        self.assertIn("| 100 | 100 | 9,000 | 0.110 | 0.150 |", text)
        self.assertIn("how keyspace size affects throughput", text)
        parsed = json.loads(render_json([result], workload, "kvbench"))
        self.assertEqual(parsed["workload"]["sweep_keyspace"], [10, 100])
        sweep = parsed["results"][0]["keyspace_sweep"]
        self.assertEqual(len(sweep), 2)
        self.assertEqual(sweep[0]["keys"], 10)
        self.assertAlmostEqual(sweep[0]["ops_per_sec"], 10_000)
        self.assertAlmostEqual(sweep[0]["p99_ms"], 0.12)
        self.assertEqual(sweep[1]["keys"], 100)

    def test_no_sweep_omits_section(self):
        workload = _workload()
        text = render_markdown([_result()], workload, "kvbench")
        self.assertNotIn("## Keyspace sweep", text)
        self.assertNotIn("Keyspace sweep:", text)
        self.assertNotIn("how keyspace size affects throughput", text)
        parsed = json.loads(render_json([_result()], workload, "kvbench"))
        self.assertIsNone(parsed["workload"]["sweep_keyspace"])
        self.assertEqual(parsed["results"][0]["keyspace_sweep"], [])

    def test_client_sweep_in_markdown_and_json(self):
        workload = Workload(ops=100, clients=2, keys=10, value_size=8,
                            sweep_clients=[1, 4])
        result = BenchmarkResult(
            name="cinder", ops=100, elapsed_s=0.01, errors=0,
            latencies_ns=[100_000] * 100,
            client_sweep=[
                {"clients": 1, "ops": 100, "ops_per_sec": 8_000,
                 "mean_ms": 0.12, "p99_ms": 0.15},
                {"clients": 4, "ops": 100, "ops_per_sec": 10_000,
                 "mean_ms": 0.1, "p99_ms": 0.12},
            ],
        )
        text = render_markdown([result], workload, "kvbench")
        self.assertIn("Client sweep: 1, 4 clients", text)
        self.assertIn("## Client sweep", text)
        self.assertIn("Client sweep — cinder", text)
        self.assertIn("| Clients | Ops | Ops/sec | Mean (ms) | p99 (ms) |",
                      text)
        self.assertIn("| 1 | 100 | 8,000 | 0.120 | 0.150 |", text)
        self.assertIn("| 4 | 100 | 10,000 | 0.100 | 0.120 |", text)
        self.assertIn("how concurrency affects throughput", text)
        parsed = json.loads(render_json([result], workload, "kvbench"))
        self.assertEqual(parsed["workload"]["sweep_clients"], [1, 4])
        sweep = parsed["results"][0]["client_sweep"]
        self.assertEqual(len(sweep), 2)
        self.assertEqual(sweep[0]["clients"], 1)
        self.assertAlmostEqual(sweep[0]["ops_per_sec"], 8_000)
        self.assertAlmostEqual(sweep[0]["p99_ms"], 0.15)
        self.assertEqual(sweep[1]["clients"], 4)

    def test_no_client_sweep_omits_section(self):
        workload = _workload()
        text = render_markdown([_result()], workload, "kvbench")
        self.assertNotIn("## Client sweep", text)
        self.assertNotIn("Client sweep:", text)
        self.assertNotIn("how concurrency affects throughput", text)
        parsed = json.loads(render_json([_result()], workload, "kvbench"))
        self.assertIsNone(parsed["workload"]["sweep_clients"])
        self.assertEqual(parsed["results"][0]["client_sweep"], [])

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
