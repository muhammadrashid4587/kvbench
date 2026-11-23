"""Tests for kvbench.compare and the --compare-file CLI mode."""

import json
import os
import socketserver
import tempfile
import threading
import unittest

from kvbench.bench import BenchmarkResult, Workload
from kvbench.cli import main
from kvbench.compare import compare_results, format_verdict, \
    load_baseline, load_history_baseline, workload_differences
from kvbench.report import _workload_dict, render_json, render_markdown


class _FakeRespHandler(socketserver.StreamRequestHandler):
    """Minimal RESP: SET/GET/INCR against the server's own dict."""

    def handle(self):
        store = self.server.store
        while True:
            line = self.rfile.readline()
            if not line:
                return
            if not line.startswith(b"*"):
                return
            nargs = int(line[1:-2])
            args = []
            for _ in range(nargs):
                length_line = self.rfile.readline()
                length = int(length_line[1:-2])
                args.append(self.rfile.read(length + 2)[:-2].decode())
            op = args[0].upper()
            if op == "SET":
                store[args[1]] = args[2]
                self.wfile.write(b"+OK\r\n")
            elif op == "GET":
                value = store.get(args[1])
                if value is None:
                    self.wfile.write(b"$-1\r\n")
                else:
                    raw = value.encode()
                    self.wfile.write(
                        f"${len(raw)}\r\n".encode() + raw + b"\r\n")
            elif op == "INCR":
                value = int(store.get(args[1], "0")) + 1
                store[args[1]] = str(value)
                self.wfile.write(f":{value}\r\n".encode())
            else:
                self.wfile.write(b"-ERR unknown command\r\n")
            self.wfile.flush()


class _FakeRespServer(socketserver.ThreadingTCPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.store = {}


def _start_server():
    server = _FakeRespServer(("127.0.0.1", 0), _FakeRespHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server


def _workload():
    return Workload(ops=100, clients=2, keys=10, value_size=8,
                    set_weight=50, get_weight=40, incr_weight=10)


def _result(name="cinder (127.0.0.1:6379)", latency_ns=100_000,
            elapsed_s=0.01, ops=100):
    return BenchmarkResult(
        name=name,
        ops=ops,
        elapsed_s=elapsed_s,
        errors=0,
        latencies_ns=[latency_ns] * ops,
    )


def _baseline_doc(name="cinder (127.0.0.1:6379)", ops_per_sec=10_000,
                  p99_ms=0.1, workload=None, generated="2026-11-23T00:00:00+00:00"):
    return {
        "tool": "kvbench",
        "version": "0.13.0",
        "generated": generated,
        "command": "kvbench --ops 100 --clients 2",
        "workload": workload if workload is not None
        else _workload_dict(_workload()),
        "results": [{
            "name": name,
            "ops": 100,
            "elapsed_s": 0.01,
            "ops_per_sec": ops_per_sec,
            "errors": 0,
            "error_sample": "",
            "summary": {
                "count": 100,
                "mean_ms": p99_ms,
                "p50_ms": p99_ms,
                "p95_ms": p99_ms,
                "p99_ms": p99_ms,
                "min_ms": p99_ms,
                "max_ms": p99_ms,
            },
        }],
    }


class TestLoadBaseline(unittest.TestCase):
    def test_loads_a_report_written_by_kvbench(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "report.json")
            with open(path, "w", encoding="utf-8") as f:
                f.write(render_json([_result()], _workload(), "kvbench"))
            doc = load_baseline(path)
            self.assertEqual(doc["tool"], "kvbench")
            self.assertEqual(len(doc["results"]), 1)

    def test_missing_file(self):
        with self.assertRaises(ValueError) as ctx:
            load_baseline("/nonexistent/report.json")
        self.assertIn("not found", str(ctx.exception))

    def test_invalid_json(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "bad.json")
            with open(path, "w", encoding="utf-8") as f:
                f.write("{not json")
            with self.assertRaises(ValueError) as ctx:
                load_baseline(path)
            self.assertIn("not a readable JSON report",
                          str(ctx.exception))

    def test_not_a_kvbench_report(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "other.json")
            with open(path, "w", encoding="utf-8") as f:
                json.dump({"hello": "world"}, f)
            with self.assertRaises(ValueError) as ctx:
                load_baseline(path)
            self.assertIn("not a kvbench JSON report",
                          str(ctx.exception))

    def test_empty_results(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "empty.json")
            with open(path, "w", encoding="utf-8") as f:
                json.dump({"tool": "kvbench", "results": []}, f)
            with self.assertRaises(ValueError) as ctx:
                load_baseline(path)
            self.assertIn("no results", str(ctx.exception))


class TestLoadHistoryBaseline(unittest.TestCase):
    def _write_history(self, tmp, records):
        path = os.path.join(tmp, "history.jsonl")
        with open(path, "w", encoding="utf-8") as f:
            for record in records:
                f.write(json.dumps(record) + "\n")
        return path

    def _record(self, backend, ops_per_sec, p99_ms, timestamp,
                errors=0):
        return {
            "tool": "kvbench",
            "version": "0.18.0",
            "timestamp": timestamp,
            "command": "kvbench --ops 100 --clients 2",
            "backend": backend,
            "ops": 100,
            "elapsed_s": 0.01,
            "ops_per_sec": ops_per_sec,
            "mean_ms": p99_ms,
            "p50_ms": p99_ms,
            "p95_ms": p99_ms,
            "p99_ms": p99_ms,
            "errors": errors,
            "workload": _workload_dict(_workload()),
        }

    def _history_path(self, tmp):
        cinder = "cinder (127.0.0.1:6379)"
        return self._write_history(tmp, [
            self._record(cinder, 8_000, 0.12,
                         "2026-11-21T10:00:00+00:00"),
            self._record("baseline (dict)", 50_000, 0.02,
                         "2026-11-21T10:00:00+00:00"),
            self._record(cinder, 9_000, 0.11,
                         "2026-11-22T10:00:00+00:00"),
            self._record(cinder, 10_000, 0.10,
                         "2026-11-23T10:00:00+00:00"),
        ])

    def test_latest_picks_most_recent_per_backend(self):
        with tempfile.TemporaryDirectory() as tmp:
            doc = load_history_baseline(
                self._history_path(tmp), "latest")
            by_name = {r["name"]: r for r in doc["results"]}
            self.assertEqual(by_name["cinder (127.0.0.1:6379)"]
                             ["ops_per_sec"], 10_000)
            self.assertEqual(by_name["baseline (dict)"]
                             ["ops_per_sec"], 50_000)
            self.assertEqual(doc["generated"],
                             "2026-11-23T10:00:00+00:00")

    def test_negative_one_is_latest(self):
        with tempfile.TemporaryDirectory() as tmp:
            doc = load_history_baseline(
                self._history_path(tmp), "-1")
            by_name = {r["name"]: r for r in doc["results"]}
            self.assertEqual(by_name["cinder (127.0.0.1:6379)"]
                             ["ops_per_sec"], 10_000)

    def test_index_zero_is_oldest(self):
        with tempfile.TemporaryDirectory() as tmp:
            doc = load_history_baseline(
                self._history_path(tmp), "0")
            by_name = {r["name"]: r for r in doc["results"]}
            self.assertEqual(by_name["cinder (127.0.0.1:6379)"]
                             ["ops_per_sec"], 8_000)
            self.assertEqual(doc["generated"],
                             "2026-11-21T10:00:00+00:00")

    def test_positive_index_counts_forward(self):
        with tempfile.TemporaryDirectory() as tmp:
            doc = load_history_baseline(
                self._history_path(tmp), "1")
            by_name = {r["name"]: r for r in doc["results"]}
            self.assertEqual(by_name["cinder (127.0.0.1:6379)"]
                             ["ops_per_sec"], 9_000)
            # The dict backend has only one recorded run, so index
            # 1 has nothing for it — it is skipped, not invented.
            self.assertNotIn("baseline (dict)", by_name)

    def test_negative_two_counts_back_from_latest(self):
        with tempfile.TemporaryDirectory() as tmp:
            doc = load_history_baseline(
                self._history_path(tmp), "-2")
            by_name = {r["name"]: r for r in doc["results"]}
            self.assertEqual(by_name["cinder (127.0.0.1:6379)"]
                             ["ops_per_sec"], 9_000)
            self.assertNotIn("baseline (dict)", by_name)

    def test_timestamp_picks_latest_at_or_before(self):
        with tempfile.TemporaryDirectory() as tmp:
            doc = load_history_baseline(
                self._history_path(tmp), "2026-11-22T12:00:00Z")
            by_name = {r["name"]: r for r in doc["results"]}
            self.assertEqual(by_name["cinder (127.0.0.1:6379)"]
                             ["ops_per_sec"], 9_000)
            # The dict backend's one run (Nov 21) is at or before
            # the timestamp, so it is picked too.
            self.assertEqual(by_name["baseline (dict)"]
                             ["ops_per_sec"], 50_000)

    def test_timestamp_skips_backend_with_no_run_yet(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = self._write_history(tmp, [
                self._record("cinder (127.0.0.1:6379)", 8_000, 0.12,
                             "2026-11-21T10:00:00+00:00"),
                self._record("baseline (dict)", 50_000, 0.02,
                             "2026-11-23T10:00:00+00:00"),
            ])
            doc = load_history_baseline(
                path, "2026-11-22T00:00:00Z")
            by_name = {r["name"]: r for r in doc["results"]}
            self.assertEqual(by_name["cinder (127.0.0.1:6379)"]
                             ["ops_per_sec"], 8_000)
            # The dict backend's first run is after the timestamp,
            # so it is left out rather than invented.
            self.assertNotIn("baseline (dict)", by_name)

    def test_timestamp_before_all_runs_is_an_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(ValueError) as ctx:
                load_history_baseline(
                    self._history_path(tmp), "2026-11-01T00:00:00Z")
            self.assertIn("at or before", str(ctx.exception))

    def test_index_out_of_range_is_an_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            # No backend has 6 recorded runs, so nothing is chosen.
            with self.assertRaises(ValueError) as ctx:
                load_history_baseline(
                    self._history_path(tmp), "5")
            self.assertIn("out of range", str(ctx.exception))
            with self.assertRaises(ValueError):
                load_history_baseline(
                    self._history_path(tmp), "-4")

    def test_invalid_selector_is_an_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(ValueError) as ctx:
                load_history_baseline(
                    self._history_path(tmp), "yesterday")
            self.assertIn("invalid --compare-history selector",
                          str(ctx.exception))

    def test_missing_history_file(self):
        with self.assertRaises(ValueError) as ctx:
            load_history_baseline("/nonexistent/history.jsonl", "-1")
        self.assertIn("not found", str(ctx.exception))

    def test_empty_history_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = self._write_history(tmp, [])
            with self.assertRaises(ValueError) as ctx:
                load_history_baseline(path, "-1")
            self.assertIn("no runs", str(ctx.exception))

    def test_baseline_doc_compares_like_a_report(self):
        with tempfile.TemporaryDirectory() as tmp:
            doc = load_history_baseline(
                self._history_path(tmp), "0")
            comparison = compare_results(
                [_result()], doc, _workload_dict(_workload()),
                "history.jsonl (history run 0)", source="history")
            entry = comparison["entries"][0]
            self.assertAlmostEqual(entry["ops_sec_delta_pct"], 25.0)
            self.assertEqual(entry["verdict"], "faster")
            self.assertEqual(comparison["source"], "history")
            self.assertTrue(comparison["workload_matches"])


class TestCompareResults(unittest.TestCase):
    def test_faster_run(self):
        # 100 ops at 100us each over 0.01 s: 10,000 ops/sec, p99 0.1 ms.
        # Baseline: 8,000 ops/sec, p99 0.12 ms.
        comparison = compare_results(
            [_result()], _baseline_doc(ops_per_sec=8_000, p99_ms=0.12),
            _workload_dict(_workload()), "prev.json")
        entry = comparison["entries"][0]
        self.assertEqual(entry["name"], "cinder (127.0.0.1:6379)")
        self.assertAlmostEqual(entry["ops_sec_delta_pct"], 25.0)
        self.assertAlmostEqual(entry["p99_delta_pct"], -16.666666, places=4)
        self.assertEqual(entry["verdict"], "faster")
        self.assertFalse(entry["regression"])
        self.assertEqual(comparison["regressions"], 0)
        self.assertTrue(comparison["workload_matches"])
        self.assertEqual(comparison["workload_differences"], [])
        self.assertEqual(comparison["unmatched_baseline"], [])
        self.assertEqual(comparison["unmatched_current"], [])
        self.assertIsNone(comparison["threshold_pct"])

    def test_slower_run(self):
        comparison = compare_results(
            [_result()], _baseline_doc(ops_per_sec=12_000, p99_ms=0.08),
            _workload_dict(_workload()), "prev.json")
        entry = comparison["entries"][0]
        self.assertAlmostEqual(entry["ops_sec_delta_pct"], -16.666666,
                               places=4)
        self.assertAlmostEqual(entry["p99_delta_pct"], 25.0)
        self.assertEqual(entry["verdict"], "slower")
        self.assertFalse(entry["regression"])

    def test_about_the_same_within_one_percent(self):
        comparison = compare_results(
            [_result()], _baseline_doc(ops_per_sec=10_050, p99_ms=0.1005),
            _workload_dict(_workload()), "prev.json")
        entry = comparison["entries"][0]
        self.assertEqual(entry["verdict"], "about the same")
        self.assertFalse(entry["regression"])

    def test_regression_on_ops_sec_drop_past_threshold(self):
        comparison = compare_results(
            [_result()], _baseline_doc(ops_per_sec=12_000, p99_ms=0.1),
            _workload_dict(_workload()), "prev.json",
            threshold_pct=5.0)
        entry = comparison["entries"][0]
        self.assertTrue(entry["regression"])
        self.assertEqual(entry["verdict"], "regression")
        self.assertEqual(comparison["regressions"], 1)
        self.assertEqual(comparison["threshold_pct"], 5.0)

    def test_regression_on_p99_rise_past_threshold(self):
        # Ops/sec identical; p99 up 20% beats the 5% threshold.
        comparison = compare_results(
            [_result()], _baseline_doc(ops_per_sec=10_000, p99_ms=0.08),
            _workload_dict(_workload()), "prev.json",
            threshold_pct=5.0)
        entry = comparison["entries"][0]
        self.assertTrue(entry["regression"])
        self.assertEqual(entry["verdict"], "regression")

    def test_within_threshold_is_not_a_regression(self):
        comparison = compare_results(
            [_result()], _baseline_doc(ops_per_sec=10_400, p99_ms=0.098),
            _workload_dict(_workload()), "prev.json",
            threshold_pct=5.0)
        entry = comparison["entries"][0]
        self.assertFalse(entry["regression"])
        self.assertEqual(entry["verdict"], "slower")
        self.assertEqual(comparison["regressions"], 0)

    def test_unmatched_backends_are_listed(self):
        doc = _baseline_doc()
        doc["results"].append(_baseline_doc(
            name="baseline (dict)", ops_per_sec=50_000)["results"][0])
        comparison = compare_results(
            [_result()], doc, _workload_dict(_workload()), "prev.json")
        self.assertEqual(len(comparison["entries"]), 1)
        self.assertEqual(comparison["unmatched_baseline"],
                         ["baseline (dict)"])
        self.assertEqual(comparison["unmatched_current"], [])
        # The other way round: a current backend missing from the file.
        comparison = compare_results(
            [_result(), _result(name="baseline (dict)")],
            _baseline_doc(), _workload_dict(_workload()), "prev.json")
        self.assertEqual(len(comparison["entries"]), 1)
        self.assertEqual(comparison["unmatched_current"],
                         ["baseline (dict)"])

    def test_workload_differences_flagged(self):
        other = _workload_dict(
            Workload(ops=100, clients=4, keys=10, value_size=8))
        comparison = compare_results(
            [_result()], _baseline_doc(), other, "prev.json")
        self.assertFalse(comparison["workload_matches"])
        self.assertEqual(comparison["workload_differences"], ["clients"])

    def test_zero_baseline_ops_has_no_delta(self):
        comparison = compare_results(
            [_result()], _baseline_doc(ops_per_sec=0, p99_ms=0.0),
            _workload_dict(_workload()), "prev.json")
        entry = comparison["entries"][0]
        self.assertIsNone(entry["ops_sec_delta_pct"])
        self.assertIsNone(entry["p99_delta_pct"])
        self.assertEqual(entry["verdict"], "about the same")
        self.assertFalse(entry["regression"])

    def test_format_verdict(self):
        comparison = compare_results(
            [_result()], _baseline_doc(ops_per_sec=8_000, p99_ms=0.12),
            _workload_dict(_workload()), "prev.json")
        self.assertEqual(
            format_verdict(comparison["entries"][0]),
            "cinder (127.0.0.1:6379): faster — ops/sec +25.0%, "
            "p99 -16.7%")
        regressed = compare_results(
            [_result()], _baseline_doc(ops_per_sec=12_000, p99_ms=0.1),
            _workload_dict(_workload()), "prev.json",
            threshold_pct=5.0)
        self.assertEqual(
            format_verdict(regressed["entries"][0], 5.0),
            "cinder (127.0.0.1:6379): regression — ops/sec -16.7% "
            "(beyond the 5% threshold)")


class TestWorkloadDifferences(unittest.TestCase):
    def test_identical_workloads(self):
        self.assertEqual(
            workload_differences(_workload_dict(_workload()),
                                 _workload_dict(_workload())), [])

    def test_missing_baseline_workload(self):
        differences = workload_differences(
            _workload_dict(_workload()), None)
        self.assertIn("clients", differences)
        self.assertIn("ops", differences)


class TestComparisonInReports(unittest.TestCase):
    def _comparison(self, **kwargs):
        return compare_results(
            [_result()],
            _baseline_doc(**kwargs),
            _workload_dict(_workload()), "prev.json")

    def test_markdown_section(self):
        text = render_markdown([_result()], _workload(), "kvbench",
                               self._comparison(ops_per_sec=8_000,
                                                p99_ms=0.12))
        self.assertIn("## Comparison with previous run", text)
        self.assertIn("Baseline: `prev.json`", text)
        self.assertIn("Baseline generated: 2026-11-23", text)
        self.assertIn(
            "| Backend | Ops/sec now | Ops/sec prev | Ops/sec Δ | "
            "p99 now (ms) | p99 prev (ms) | p99 Δ | Verdict |", text)
        self.assertIn(
            "| cinder (127.0.0.1:6379) | 10,000 | 8,000 | +25.0% | "
            "0.100 | 0.120 | -16.7% | faster |", text)
        self.assertIn("compares this run against an earlier JSON report",
                      text)

    def test_markdown_workload_warning(self):
        comparison = compare_results(
            [_result()], _baseline_doc(),
            _workload_dict(Workload(ops=100, clients=4, keys=10)),
            "prev.json")
        text = render_markdown([_result()], _workload(), "kvbench",
                               comparison)
        self.assertIn("workload differs from the baseline run", text)
        self.assertIn("clients", text)

    def test_markdown_regression_gate_lines(self):
        comparison = compare_results(
            [_result()], _baseline_doc(ops_per_sec=12_000, p99_ms=0.1),
            _workload_dict(_workload()), "prev.json", threshold_pct=5.0)
        text = render_markdown([_result()], _workload(), "kvbench",
                               comparison)
        self.assertIn("1 backend(s) regressed beyond the 5% threshold",
                      text)
        self.assertIn("| regression |", text)

    def test_json_carries_comparison(self):
        parsed = json.loads(render_json(
            [_result()], _workload(), "kvbench",
            self._comparison(ops_per_sec=8_000, p99_ms=0.12)))
        comparison = parsed["comparison"]
        self.assertEqual(comparison["file"], "prev.json")
        self.assertTrue(comparison["workload_matches"])
        self.assertEqual(len(comparison["entries"]), 1)
        self.assertAlmostEqual(
            comparison["entries"][0]["ops_sec_delta_pct"], 25.0)
        self.assertEqual(comparison["entries"][0]["verdict"], "faster")

    def test_markdown_notes_name_history_baseline(self):
        comparison = compare_results(
            [_result()], _baseline_doc(ops_per_sec=8_000, p99_ms=0.12),
            _workload_dict(_workload()),
            "history.jsonl (history run -1)", source="history")
        text = render_markdown([_result()], _workload(), "kvbench",
                               comparison)
        self.assertIn("a run recorded in the history file", text)
        self.assertIn("Baseline: `history.jsonl (history run -1)`",
                      text)

    def test_no_comparison_omits_section_and_key(self):
        text = render_markdown([_result()], _workload(), "kvbench")
        self.assertNotIn("## Comparison with previous run", text)
        parsed = json.loads(render_json([_result()], _workload(),
                                        "kvbench"))
        self.assertNotIn("comparison", parsed)


class TestCompareFileCli(unittest.TestCase):
    def setUp(self):
        self.server = _start_server()
        self.port = self.server.server_address[1]

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()

    def _argv(self, extra):
        return ["--host", "127.0.0.1", "--port", str(self.port)] + extra

    def _run_baseline(self, tmp):
        """Run a benchmark against the fake server and return its
        JSON path."""
        out = os.path.join(tmp, "base.md")
        code = main(self._argv(["--ops", "100", "--clients", "1",
                                "--output", out]))
        self.assertEqual(code, 0)
        return os.path.join(tmp, "base.json")

    def test_compare_file_end_to_end(self):
        with tempfile.TemporaryDirectory() as tmp:
            baseline_json = self._run_baseline(tmp)
            out = os.path.join(tmp, "run.md")
            code = main(self._argv(["--ops", "100", "--clients", "1",
                                    "--output", out,
                                    "--compare-file", baseline_json]))
            self.assertEqual(code, 0)
            with open(out, encoding="utf-8") as f:
                text = f.read()
            self.assertIn("## Comparison with previous run", text)
            parsed = json.loads(open(os.path.join(tmp, "run.json"),
                                     encoding="utf-8").read())
            self.assertIn("comparison", parsed)
            self.assertEqual(len(parsed["comparison"]["entries"]), 1)
            self.assertEqual(
                parsed["comparison"]["entries"][0]["name"],
                f"cinder (127.0.0.1:{self.port})")

    def test_regression_threshold_exits_3(self):
        with tempfile.TemporaryDirectory() as tmp:
            # A baseline with absurdly fast numbers, so the real run
            # always regresses past the threshold.
            baseline_json = os.path.join(tmp, "fast.json")
            doc = _baseline_doc(
                name=f"cinder (127.0.0.1:{self.port})",
                ops_per_sec=10**9, p99_ms=0.0001)
            with open(baseline_json, "w", encoding="utf-8") as f:
                json.dump(doc, f)
            out = os.path.join(tmp, "run.md")
            code = main(self._argv(["--ops", "100", "--clients", "1",
                                    "--output", out,
                                    "--compare-file", baseline_json,
                                    "--regression-threshold", "5"]))
            self.assertEqual(code, 3)
            # The reports are still written, so the regression is
            # visible in them even though the exit code failed.
            with open(out, encoding="utf-8") as f:
                text = f.read()
            self.assertIn("## Comparison with previous run", text)
            self.assertIn("regression", text)

    def test_threshold_without_compare_file_is_an_error(self):
        code = main(self._argv(["--ops", "10",
                                "--regression-threshold", "5"]))
        self.assertEqual(code, 2)

    def test_negative_threshold_is_an_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            baseline_json = self._run_baseline(tmp)
            code = main(self._argv(["--ops", "10",
                                    "--compare-file", baseline_json,
                                    "--regression-threshold", "-1"]))
            self.assertEqual(code, 2)

    def test_missing_compare_file_fails_fast(self):
        # No benchmark runs: the bad path is caught before any work.
        code = main(self._argv(["--ops", "10",
                                "--compare-file",
                                "/nonexistent/prev.json"]))
        self.assertEqual(code, 1)


class TestCompareHistoryCli(unittest.TestCase):
    def setUp(self):
        self.server = _start_server()
        self.port = self.server.server_address[1]

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()

    def _argv(self, extra):
        return ["--host", "127.0.0.1", "--port", str(self.port)] + extra

    def _run_baseline(self, tmp):
        """Run a benchmark against the fake server and return its
        JSON path."""
        out = os.path.join(tmp, "base.md")
        code = main(self._argv(["--ops", "100", "--clients", "1",
                                "--output", out]))
        self.assertEqual(code, 0)
        return os.path.join(tmp, "base.json")

    def _run_with_history(self, tmp):
        """Run a benchmark that records itself in a history file,
        and return the history path."""
        history = os.path.join(tmp, "history.jsonl")
        code = main(self._argv(["--ops", "100", "--clients", "1",
                                "--output", os.path.join(tmp, "base.md"),
                                "--history", "--history-file", history]))
        self.assertEqual(code, 0)
        return history

    def test_compare_history_end_to_end(self):
        with tempfile.TemporaryDirectory() as tmp:
            history = self._run_with_history(tmp)
            out = os.path.join(tmp, "run.md")
            code = main(self._argv(["--ops", "100", "--clients", "1",
                                    "--output", out,
                                    "--compare-history", "-1",
                                    "--history-file", history]))
            self.assertEqual(code, 0)
            with open(out, encoding="utf-8") as f:
                text = f.read()
            self.assertIn("## Comparison with previous run", text)
            self.assertIn("history run -1", text)
            self.assertIn("a run recorded in the history file", text)
            parsed = json.loads(open(os.path.join(tmp, "run.json"),
                                     encoding="utf-8").read())
            comparison = parsed["comparison"]
            self.assertEqual(comparison["source"], "history")
            self.assertEqual(len(comparison["entries"]), 1)
            self.assertEqual(
                comparison["entries"][0]["name"],
                f"cinder (127.0.0.1:{self.port})")

    def test_compare_history_by_timestamp(self):
        with tempfile.TemporaryDirectory() as tmp:
            history = self._run_with_history(tmp)
            out = os.path.join(tmp, "run.md")
            code = main(self._argv(["--ops", "100", "--clients", "1",
                                    "--output", out,
                                    "--compare-history", "2999-01-01T00:00:00Z",
                                    "--history-file", history]))
            self.assertEqual(code, 0)
            with open(out, encoding="utf-8") as f:
                text = f.read()
            self.assertIn("## Comparison with previous run", text)

    def test_regression_threshold_exits_3_against_history(self):
        with tempfile.TemporaryDirectory() as tmp:
            history = os.path.join(tmp, "history.jsonl")
            record = {
                "tool": "kvbench", "version": "0.19.0",
                "timestamp": "2026-11-23T00:00:00+00:00",
                "command": "kvbench --ops 100 --clients 1",
                "backend": f"cinder (127.0.0.1:{self.port})",
                "ops": 100, "elapsed_s": 0.01,
                "ops_per_sec": 10**9, "mean_ms": 0.0001,
                "p50_ms": 0.0001, "p95_ms": 0.0001,
                "p99_ms": 0.0001, "errors": 0,
                "workload": _workload_dict(_workload()),
            }
            with open(history, "w", encoding="utf-8") as f:
                f.write(json.dumps(record) + "\n")
            out = os.path.join(tmp, "run.md")
            code = main(self._argv(["--ops", "100", "--clients", "1",
                                    "--output", out,
                                    "--compare-history", "-1",
                                    "--history-file", history,
                                    "--regression-threshold", "5"]))
            self.assertEqual(code, 3)

    def test_compare_file_and_compare_history_conflict(self):
        with tempfile.TemporaryDirectory() as tmp:
            baseline_json = self._run_baseline(tmp)
            code = main(self._argv(["--ops", "10",
                                    "--compare-file", baseline_json,
                                    "--compare-history", "-1"]))
            self.assertEqual(code, 2)

    def test_threshold_works_with_compare_history(self):
        # --regression-threshold without --compare-file is fine
        # when --compare-history supplies the baseline. The
        # threshold is deliberately huge: two live runs against
        # the fake server jitter well past 5%, and this test only
        # checks the flag combination is accepted, not the gate.
        with tempfile.TemporaryDirectory() as tmp:
            history = self._run_with_history(tmp)
            code = main(self._argv(["--ops", "100", "--clients", "1",
                                    "--output", os.path.join(tmp, "r.md"),
                                    "--compare-history", "-1",
                                    "--history-file", history,
                                    "--regression-threshold", "1000"]))
            self.assertEqual(code, 0)

    def test_missing_history_file_fails_fast(self):
        code = main(self._argv(["--ops", "10",
                                "--compare-history", "-1",
                                "--history-file",
                                "/nonexistent/history.jsonl"]))
        self.assertEqual(code, 1)

    def test_bad_selector_fails_fast(self):
        with tempfile.TemporaryDirectory() as tmp:
            history = self._run_with_history(tmp)
            code = main(self._argv(["--ops", "10",
                                    "--compare-history", "yesterday",
                                    "--history-file", history]))
            self.assertEqual(code, 1)


if __name__ == "__main__":
    unittest.main()
