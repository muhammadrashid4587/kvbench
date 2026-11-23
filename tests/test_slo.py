"""Tests for the kvbench SLO gate (--slo-p99-ms / --slo-error-rate).

The gate fails the run (exit 4) when a backend's measured p99
latency or error rate is above the threshold, and the verdict lands
in both the Markdown and JSON reports.
"""

import json
import os
import socketserver
import tempfile
import threading
import unittest

from kvbench.bench import BenchmarkResult, Workload
from kvbench.cli import main
from kvbench.report import render_json, render_markdown, to_json_dict
from kvbench.slo import error_rate_pct, evaluate_slo, format_slo_verdict


def _result(name="cinder (127.0.0.1:6379)", ops=100, errors=0,
            latencies_ns=None):
    return BenchmarkResult(
        name=name,
        ops=ops,
        elapsed_s=0.01,
        errors=errors,
        latencies_ns=(
            latencies_ns if latencies_ns is not None
            else [100_000] * 100),
    )


def _workload():
    return Workload(ops=100, clients=2, keys=10, value_size=8,
                    set_weight=50, get_weight=40, incr_weight=10)


class TestErrorRate(unittest.TestCase):
    def test_errors_over_attempted_ops(self):
        # ops counts successes plus errors: 5 failed of 100 attempted.
        self.assertAlmostEqual(
            error_rate_pct(_result(ops=100, errors=5)), 5.0)

    def test_no_ops_is_zero_rate(self):
        self.assertEqual(error_rate_pct(_result(ops=0, errors=0)), 0.0)

    def test_no_errors_is_zero_rate(self):
        self.assertEqual(error_rate_pct(_result(ops=100, errors=0)), 0.0)


class TestEvaluateSlo(unittest.TestCase):
    def test_no_thresholds_is_no_gate(self):
        self.assertIsNone(evaluate_slo([_result()]))

    def test_p99_within_threshold_passes(self):
        # All latencies are 0.1 ms; a 1 ms cap passes.
        slo = evaluate_slo([_result()], p99_ms=1.0)
        self.assertEqual(slo["breaches"], 0)
        self.assertEqual(slo["entries"][0]["verdict"], "pass")
        self.assertFalse(slo["entries"][0]["p99_breach"])

    def test_p99_above_threshold_breaches(self):
        # p99 is 5 ms (one 5 ms sample in 100); a 1 ms cap breaches.
        latencies = [100_000] * 98 + [5_000_000] * 2
        slo = evaluate_slo([_result(latencies_ns=latencies)], p99_ms=1.0)
        self.assertEqual(slo["breaches"], 1)
        entry = slo["entries"][0]
        self.assertEqual(entry["verdict"], "breach")
        self.assertTrue(entry["p99_breach"])
        self.assertAlmostEqual(entry["p99_ms"], 5.0)
        self.assertEqual(entry["p99_threshold_ms"], 1.0)

    def test_p99_exactly_at_threshold_passes(self):
        # Breach is strictly above the threshold, so exactly at it
        # still passes.
        slo = evaluate_slo([_result()], p99_ms=0.1)
        self.assertEqual(slo["entries"][0]["verdict"], "pass")

    def test_error_rate_above_threshold_breaches(self):
        slo = evaluate_slo(
            [_result(ops=100, errors=5)], error_rate_threshold_pct=1.0)
        self.assertEqual(slo["breaches"], 1)
        entry = slo["entries"][0]
        self.assertTrue(entry["error_rate_breach"])
        self.assertAlmostEqual(entry["error_rate_pct"], 5.0)

    def test_error_rate_at_threshold_passes(self):
        slo = evaluate_slo(
            [_result(ops=100, errors=5)], error_rate_threshold_pct=5.0)
        self.assertEqual(slo["entries"][0]["verdict"], "pass")

    def test_both_thresholds_each_breach_counts_once(self):
        latencies = [100_000] * 98 + [5_000_000] * 2
        slo = evaluate_slo(
            [_result(ops=100, errors=5, latencies_ns=latencies)],
            p99_ms=1.0, error_rate_threshold_pct=1.0)
        entry = slo["entries"][0]
        self.assertTrue(entry["p99_breach"])
        self.assertTrue(entry["error_rate_breach"])
        self.assertEqual(slo["breaches"], 1)

    def test_per_backend_verdicts(self):
        slow = _result(name="slow", latencies_ns=[5_000_000] * 100)
        fast = _result(name="fast")
        slo = evaluate_slo([slow, fast], p99_ms=1.0)
        self.assertEqual(slo["breaches"], 1)
        self.assertEqual(
            [e["verdict"] for e in slo["entries"]],
            ["breach", "pass"])

    def test_thresholds_recorded(self):
        slo = evaluate_slo([_result()], p99_ms=2.5, error_rate_threshold_pct=0.5)
        self.assertEqual(slo["p99_threshold_ms"], 2.5)
        self.assertEqual(slo["error_rate_threshold_pct"], 0.5)


class TestFormatSloVerdict(unittest.TestCase):
    def test_pass_line(self):
        slo = evaluate_slo([_result()], p99_ms=1.0, error_rate_threshold_pct=1.0)
        line = format_slo_verdict(slo["entries"][0])
        self.assertIn("SLO passed", line)
        self.assertIn("p99 0.100 ms", line)
        self.assertIn("error rate 0.00%", line)

    def test_breach_line(self):
        latencies = [100_000] * 98 + [5_000_000] * 2
        slo = evaluate_slo([_result(latencies_ns=latencies)], p99_ms=1.0)
        line = format_slo_verdict(slo["entries"][0])
        self.assertIn("SLO breach", line)
        self.assertIn("p99 5.000 ms", line)
        self.assertIn("limit 1 ms", line)

    def test_only_set_thresholds_shown(self):
        slo = evaluate_slo([_result()], error_rate_threshold_pct=1.0)
        line = format_slo_verdict(slo["entries"][0])
        self.assertNotIn("p99", line)
        self.assertIn("error rate 0.00%", line)


class TestWorkloadValidation(unittest.TestCase):
    def test_negative_p99_threshold_rejected(self):
        with self.assertRaises(ValueError):
            Workload(slo_p99_ms=-1.0)

    def test_error_rate_above_100_rejected(self):
        with self.assertRaises(ValueError):
            Workload(slo_error_rate_pct=101.0)

    def test_error_rate_below_zero_rejected(self):
        with self.assertRaises(ValueError):
            Workload(slo_error_rate_pct=-1.0)

    def test_valid_thresholds_accepted(self):
        workload = Workload(slo_p99_ms=1.0, slo_error_rate_pct=1.0)
        self.assertEqual(workload.slo_p99_ms, 1.0)
        self.assertEqual(workload.slo_error_rate_pct, 1.0)


class TestSloReport(unittest.TestCase):
    def test_markdown_gate_section(self):
        slo = evaluate_slo([_result()], p99_ms=1.0, error_rate_threshold_pct=1.0)
        text = render_markdown([_result()], _workload(), "kvbench",
                               slo=slo)
        self.assertIn("## SLO gate", text)
        self.assertIn("p99 at most 1 ms", text)
        self.assertIn("error rate at most 1%", text)
        self.assertIn("SLO gate: passed", text)
        self.assertIn("| pass |", text)

    def test_markdown_breach_section(self):
        latencies = [100_000] * 98 + [5_000_000] * 2
        result = _result(latencies_ns=latencies)
        slo = evaluate_slo([result], p99_ms=1.0)
        text = render_markdown([result], _workload(), "kvbench", slo=slo)
        self.assertIn("## SLO gate", text)
        self.assertIn("SLO gate: FAILED", text)
        self.assertIn("exit code 4", text)
        self.assertIn("| breach |", text)

    def test_markdown_without_gate_has_no_section(self):
        text = render_markdown([_result()], _workload(), "kvbench")
        self.assertNotIn("## SLO gate", text)
        self.assertNotIn("SLO gate", text)

    def test_json_carries_slo(self):
        workload = Workload(
            ops=100, clients=2, keys=10, value_size=8,
            set_weight=50, get_weight=40, incr_weight=10,
            slo_p99_ms=1.0, slo_error_rate_pct=1.0)
        slo = evaluate_slo([_result()], p99_ms=1.0, error_rate_threshold_pct=1.0)
        doc = json.loads(render_json([_result()], workload, "kvbench",
                                     slo=slo))
        self.assertIn("slo", doc)
        self.assertEqual(doc["slo"]["breaches"], 0)
        self.assertEqual(doc["slo"]["entries"][0]["verdict"], "pass")
        self.assertEqual(doc["workload"]["slo_p99_ms"], 1.0)
        self.assertEqual(doc["workload"]["slo_error_rate_pct"], 1.0)

    def test_json_without_gate_has_no_slo(self):
        doc = to_json_dict([_result()], _workload(), "kvbench")
        self.assertNotIn("slo", doc)
        self.assertIsNone(doc["workload"]["slo_p99_ms"])
        self.assertIsNone(doc["workload"]["slo_error_rate_pct"])


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


class TestSloCli(unittest.TestCase):
    def setUp(self):
        self.server = _start_server()
        self.port = self.server.server_address[1]
        self.tmp = tempfile.TemporaryDirectory()
        self.output = os.path.join(self.tmp.name, "report.md")

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.tmp.cleanup()

    def _run(self, *extra):
        return main([
            "--host", "127.0.0.1", "--port", str(self.port),
            "--ops", "200", "--clients", "2",
            "--output", self.output,
        ] + list(extra))

    def test_generous_threshold_passes(self):
        # Local p99 is well under a second; the run exits 0 and the
        # report records the gate passing.
        self.assertEqual(self._run("--slo-p99-ms", "1000"), 0)
        with open(self.output, encoding="utf-8") as f:
            self.assertIn("SLO gate: passed", f.read())
        with open(self.output.replace(".md", ".json"),
                  encoding="utf-8") as f:
            doc = json.load(f)
        self.assertEqual(doc["slo"]["breaches"], 0)

    def test_impossible_threshold_fails_with_exit_4(self):
        # A 1-nanosecond p99 cap cannot pass; the reports are still
        # written, and the run exits 4.
        self.assertEqual(self._run("--slo-p99-ms", "0.000001"), 4)
        with open(self.output, encoding="utf-8") as f:
            text = f.read()
        self.assertIn("SLO gate: FAILED", text)
        self.assertIn("exit code 4", text)
        with open(self.output.replace(".md", ".json"),
                  encoding="utf-8") as f:
            doc = json.load(f)
        self.assertEqual(doc["slo"]["breaches"], 1)

    def test_negative_threshold_rejected(self):
        self.assertEqual(self._run("--slo-p99-ms", "-1"), 2)

    def test_error_rate_above_100_rejected(self):
        self.assertEqual(self._run("--slo-error-rate", "101"), 2)


if __name__ == "__main__":
    unittest.main()
