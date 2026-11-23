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
from kvbench.cli import _parse_slo_op, main
from kvbench.report import render_json, render_markdown, to_json_dict
from kvbench.slo import error_rate_pct, evaluate_slo, format_slo_verdict, \
    load_slo_preset, per_op_error_rate_pct


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


def _per_op_result():
    """A result with per-op latencies and errors to gate on.

    SET: 43 fast ops plus 2 slow (5 ms) ones, so SET p99 is 5 ms
    while GET p99 is 0.1 ms; SET also has 5 errors out of 50
    attempted (10%), GET has none. The global p99 is 5 ms and the
    global error rate is 5%.
    """
    return BenchmarkResult(
        name="cinder (127.0.0.1:6379)",
        ops=100,
        elapsed_s=0.01,
        errors=5,
        latencies_ns=[100_000] * 93 + [5_000_000] * 2,
        latencies_by_op={
            "SET": [100_000] * 43 + [5_000_000] * 2,
            "GET": [100_000] * 50,
        },
        errors_by_op={"SET": 5},
    )


class TestParseSloOp(unittest.TestCase):
    def test_full_form(self):
        self.assertEqual(_parse_slo_op("SET:5,0.1"), ("SET", 5.0, 0.1))

    def test_op_is_case_insensitive(self):
        self.assertEqual(_parse_slo_op("get:2,1"), ("GET", 2.0, 1.0))

    def test_p99_only(self):
        self.assertEqual(_parse_slo_op("SET:5,"), ("SET", 5.0, None))

    def test_error_rate_only(self):
        self.assertEqual(_parse_slo_op("INCR:,0.5"), ("INCR", None, 0.5))

    def test_unknown_op_rejected(self):
        with self.assertRaises(ValueError):
            _parse_slo_op("DEL:5,0.1")

    def test_missing_colon_rejected(self):
        with self.assertRaises(ValueError):
            _parse_slo_op("SET5,0.1")

    def test_missing_comma_rejected(self):
        with self.assertRaises(ValueError):
            _parse_slo_op("SET:5")

    def test_neither_threshold_rejected(self):
        with self.assertRaises(ValueError):
            _parse_slo_op("SET:,")

    def test_negative_p99_rejected(self):
        with self.assertRaises(ValueError):
            _parse_slo_op("SET:-1,0.1")

    def test_error_rate_above_100_rejected(self):
        with self.assertRaises(ValueError):
            _parse_slo_op("SET:5,101")

    def test_non_numeric_threshold_rejected(self):
        with self.assertRaises(ValueError):
            _parse_slo_op("SET:fast,0.1")


class TestLoadSloPreset(unittest.TestCase):
    def _write_preset(self, text):
        tmp = tempfile.NamedTemporaryFile(
            "w", suffix=".json", delete=False, encoding="utf-8")
        tmp.write(text)
        tmp.close()
        self.addCleanup(os.unlink, tmp.name)
        return tmp.name

    def test_full_preset(self):
        path = self._write_preset(json.dumps({
            "p99_ms": 1.0,
            "error_rate_pct": 0.5,
            "per_op": {
                "SET": {"p99_ms": 5, "error_rate_pct": 0.1},
                "GET": {"p99_ms": 2},
            },
        }))
        preset = load_slo_preset(path)
        self.assertEqual(preset["p99_ms"], 1.0)
        self.assertEqual(preset["error_rate_pct"], 0.5)
        self.assertEqual(preset["per_op"], {
            "SET": (5.0, 0.1), "GET": (2.0, None)})

    def test_partial_preset_defaults_to_none(self):
        # Every key is optional: a preset may set only the p99 cap.
        path = self._write_preset(json.dumps({"p99_ms": 2.5}))
        preset = load_slo_preset(path)
        self.assertEqual(preset["p99_ms"], 2.5)
        self.assertIsNone(preset["error_rate_pct"])
        self.assertEqual(preset["per_op"], {})

    def test_empty_object_is_no_thresholds(self):
        path = self._write_preset("{}")
        preset = load_slo_preset(path)
        self.assertIsNone(preset["p99_ms"])
        self.assertIsNone(preset["error_rate_pct"])
        self.assertEqual(preset["per_op"], {})

    def test_op_name_is_case_insensitive(self):
        path = self._write_preset(json.dumps({
            "per_op": {"set": {"p99_ms": 5}}}))
        preset = load_slo_preset(path)
        self.assertEqual(preset["per_op"], {"SET": (5.0, None)})

    def test_missing_file_rejected(self):
        with self.assertRaises(ValueError) as caught:
            load_slo_preset("/nonexistent/slo.json")
        self.assertIn("not found", str(caught.exception))

    def test_invalid_json_rejected(self):
        path = self._write_preset("{not json")
        with self.assertRaises(ValueError) as caught:
            load_slo_preset(path)
        self.assertIn("not valid JSON", str(caught.exception))

    def test_non_object_rejected(self):
        path = self._write_preset("[1, 2]")
        with self.assertRaises(ValueError) as caught:
            load_slo_preset(path)
        self.assertIn("must be a JSON object", str(caught.exception))

    def test_unknown_key_rejected(self):
        path = self._write_preset(json.dumps({"p95_ms": 1.0}))
        with self.assertRaises(ValueError) as caught:
            load_slo_preset(path)
        self.assertIn("unknown key", str(caught.exception))

    def test_negative_p99_rejected(self):
        path = self._write_preset(json.dumps({"p99_ms": -1}))
        with self.assertRaises(ValueError):
            load_slo_preset(path)

    def test_error_rate_above_100_rejected(self):
        path = self._write_preset(json.dumps({"error_rate_pct": 101}))
        with self.assertRaises(ValueError):
            load_slo_preset(path)

    def test_non_numeric_threshold_rejected(self):
        path = self._write_preset(json.dumps({"p99_ms": "fast"}))
        with self.assertRaises(ValueError):
            load_slo_preset(path)

    def test_unknown_op_rejected(self):
        path = self._write_preset(json.dumps({
            "per_op": {"DEL": {"p99_ms": 1}}}))
        with self.assertRaises(ValueError):
            load_slo_preset(path)

    def test_per_op_with_no_threshold_rejected(self):
        path = self._write_preset(json.dumps({
            "per_op": {"SET": {}}}))
        with self.assertRaises(ValueError):
            load_slo_preset(path)

    def test_per_op_must_be_an_object(self):
        path = self._write_preset(json.dumps({"per_op": [1]}))
        with self.assertRaises(ValueError):
            load_slo_preset(path)


class TestPerOpErrorRate(unittest.TestCase):
    def test_errors_over_attempted_ops_of_that_op(self):
        # SET attempted 50 ops (45 successes + 5 errors): 10%.
        self.assertAlmostEqual(
            per_op_error_rate_pct(_per_op_result(), "SET"), 10.0)

    def test_op_with_no_errors_is_zero_rate(self):
        self.assertEqual(
            per_op_error_rate_pct(_per_op_result(), "GET"), 0.0)

    def test_op_that_never_ran_is_zero_rate(self):
        self.assertEqual(
            per_op_error_rate_pct(_per_op_result(), "INCR"), 0.0)


class TestEvaluatePerOpSlo(unittest.TestCase):
    def test_per_op_only_gate_is_a_gate(self):
        slo = evaluate_slo([_result()],
                           per_op={"SET": (1000.0, 100.0)})
        self.assertIsNotNone(slo)
        self.assertEqual(slo["breaches"], 0)

    def test_per_op_p99_breach_while_global_passes(self):
        # Global cap is generous (10 ms); only SET's own 1 ms cap
        # breaches, on its 5 ms p99.
        slo = evaluate_slo([_per_op_result()], p99_ms=10.0,
                           per_op={"SET": (1.0, None)})
        self.assertEqual(slo["breaches"], 1)
        entry = slo["entries"][0]
        self.assertEqual(entry["verdict"], "breach")
        self.assertFalse(entry["p99_breach"])
        self.assertEqual(len(entry["per_op"]), 1)
        per_op = entry["per_op"][0]
        self.assertEqual(per_op["op"], "SET")
        self.assertTrue(per_op["p99_breach"])
        self.assertAlmostEqual(per_op["p99_ms"], 5.0)
        self.assertEqual(per_op["verdict"], "breach")

    def test_per_op_error_rate_breach_while_global_passes(self):
        # Global error cap is 10% (the run's 5% passes); SET's own
        # 1% cap breaches on its 10% error rate.
        slo = evaluate_slo([_per_op_result()],
                           error_rate_threshold_pct=10.0,
                           per_op={"SET": (None, 1.0)})
        self.assertEqual(slo["breaches"], 1)
        per_op = slo["entries"][0]["per_op"][0]
        self.assertTrue(per_op["error_rate_breach"])
        self.assertAlmostEqual(per_op["error_rate_pct"], 10.0)

    def test_other_ops_pass_and_report(self):
        slo = evaluate_slo([_per_op_result()], p99_ms=10.0,
                           per_op={"SET": (1.0, None),
                                   "GET": (1.0, None)})
        per_ops = {e["op"]: e for e in slo["entries"][0]["per_op"]}
        self.assertEqual(per_ops["SET"]["verdict"], "breach")
        self.assertEqual(per_ops["GET"]["verdict"], "pass")
        self.assertAlmostEqual(per_ops["GET"]["p99_ms"], 0.1)

    def test_op_that_never_ran_is_skipped(self):
        # INCR has no samples and no errors in this result, so no
        # per-op entry is produced for it.
        slo = evaluate_slo([_per_op_result()],
                           per_op={"INCR": (1.0, 1.0)})
        self.assertEqual(slo["entries"][0]["per_op"], [])
        self.assertEqual(slo["entries"][0]["verdict"], "pass")

    def test_per_op_thresholds_recorded(self):
        slo = evaluate_slo([_per_op_result()],
                           per_op={"SET": (5.0, 0.1)})
        self.assertEqual(slo["per_op_thresholds"], {
            "SET": {"p99_ms": 5.0, "error_rate_pct": 0.1}})

    def test_no_thresholds_at_all_is_no_gate(self):
        self.assertIsNone(evaluate_slo([_result()], per_op={}))

    def test_breach_line_names_the_op(self):
        slo = evaluate_slo([_per_op_result()], p99_ms=10.0,
                           per_op={"SET": (1.0, None)})
        line = format_slo_verdict(slo["entries"][0])
        self.assertIn("SLO breach", line)
        self.assertIn("SET p99 5.000 ms", line)
        self.assertIn("limit 1 ms", line)

    def test_pass_line_omits_passing_ops(self):
        slo = evaluate_slo([_per_op_result()], p99_ms=10.0,
                           per_op={"SET": (1000.0, None)})
        line = format_slo_verdict(slo["entries"][0])
        self.assertIn("SLO passed", line)
        self.assertNotIn("SET", line)


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

    def test_per_op_unknown_op_rejected(self):
        with self.assertRaises(ValueError):
            Workload(slo_per_op={"DEL": (1.0, None)})

    def test_per_op_negative_p99_rejected(self):
        with self.assertRaises(ValueError):
            Workload(slo_per_op={"SET": (-1.0, None)})

    def test_per_op_error_rate_above_100_rejected(self):
        with self.assertRaises(ValueError):
            Workload(slo_per_op={"SET": (None, 101.0)})

    def test_per_op_no_threshold_rejected(self):
        with self.assertRaises(ValueError):
            Workload(slo_per_op={"SET": (None, None)})

    def test_valid_per_op_thresholds_accepted(self):
        workload = Workload(slo_per_op={"SET": (5.0, 0.1)})
        self.assertEqual(workload.slo_per_op, {"SET": (5.0, 0.1)})


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

    def test_markdown_per_op_table(self):
        slo = evaluate_slo([_per_op_result()], p99_ms=10.0,
                           per_op={"SET": (1.0, 1.0), "GET": (1.0, None)})
        text = render_markdown([_per_op_result()], _workload(), "kvbench",
                               slo=slo)
        self.assertIn("## SLO gate", text)
        self.assertIn("SET: p99 at most 1 ms; error rate at most 1%",
                      text)
        self.assertIn("| SET | 50 | 5.000 | 1 | 10.00% | 1% | breach |",
                      text)
        self.assertIn("| GET | 50 | 0.100 | 1 | 0.00% | — | pass |",
                      text)
        self.assertIn("SLO gate: FAILED", text)

    def test_markdown_without_per_op_has_no_per_op_table(self):
        slo = evaluate_slo([_result()], p99_ms=1.0)
        text = render_markdown([_result()], _workload(), "kvbench",
                               slo=slo)
        self.assertIn("## SLO gate", text)
        self.assertNotIn("| Op |", text)

    def test_json_carries_per_op_slo(self):
        workload = Workload(
            ops=100, clients=2, keys=10, value_size=8,
            set_weight=50, get_weight=40, incr_weight=10,
            slo_per_op={"SET": (10.0, 15.0)})
        slo = evaluate_slo([_per_op_result()], p99_ms=10.0,
                           per_op={"SET": (10.0, 15.0)})
        doc = json.loads(render_json([_per_op_result()], workload,
                                     "kvbench", slo=slo))
        self.assertEqual(doc["workload"]["slo_per_op"], {
            "SET": {"p99_ms": 10.0, "error_rate_pct": 15.0}})
        per_op = doc["slo"]["entries"][0]["per_op"]
        self.assertEqual(len(per_op), 1)
        self.assertEqual(per_op[0]["op"], "SET")
        self.assertEqual(per_op[0]["verdict"], "pass")
        self.assertEqual(doc["slo"]["per_op_thresholds"], {
            "SET": {"p99_ms": 10.0, "error_rate_pct": 15.0}})

    def test_preset_recorded_in_both_reports(self):
        workload = Workload(
            ops=100, clients=2, keys=10, value_size=8,
            set_weight=50, get_weight=40, incr_weight=10,
            slo_p99_ms=1.0, slo_preset="slo.json")
        slo = evaluate_slo([_result()], p99_ms=1.0, preset="slo.json")
        text = render_markdown([_result()], workload, "kvbench", slo=slo)
        self.assertIn("Thresholds loaded from preset file `slo.json`",
                      text)
        doc = json.loads(render_json([_result()], workload, "kvbench",
                                     slo=slo))
        self.assertEqual(doc["workload"]["slo_preset"], "slo.json")
        self.assertEqual(doc["slo"]["preset"], "slo.json")

    def test_no_preset_means_null_preset(self):
        doc = to_json_dict([_result()], _workload(), "kvbench")
        self.assertIsNone(doc["workload"]["slo_preset"])


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

    def test_generous_per_op_threshold_passes(self):
        self.assertEqual(
            self._run("--slo-op", "SET:1000,100",
                      "--slo-op", "GET:1000,100"), 0)
        with open(self.output, encoding="utf-8") as f:
            text = f.read()
        self.assertIn("SLO gate: passed", text)
        self.assertIn("| SET |", text)
        self.assertIn("| GET |", text)
        with open(self.output.replace(".md", ".json"),
                  encoding="utf-8") as f:
            doc = json.load(f)
        self.assertEqual(doc["slo"]["breaches"], 0)
        self.assertEqual(
            doc["workload"]["slo_per_op"]["SET"]["p99_ms"], 1000.0)

    def test_impossible_per_op_threshold_fails_with_exit_4(self):
        # A 1-nanosecond SET p99 cap cannot pass, even with no
        # global gate set; the reports are still written.
        self.assertEqual(
            self._run("--slo-op", "SET:0.000001,"), 4)
        with open(self.output, encoding="utf-8") as f:
            text = f.read()
        self.assertIn("SLO gate: FAILED", text)
        self.assertIn("SET p99", text)
        with open(self.output.replace(".md", ".json"),
                  encoding="utf-8") as f:
            doc = json.load(f)
        self.assertEqual(doc["slo"]["breaches"], 1)
        per_op = doc["slo"]["entries"][0]["per_op"]
        self.assertEqual(
            [e["op"] for e in per_op if e["verdict"] == "breach"],
            ["SET"])

    def test_invalid_per_op_rejected(self):
        self.assertEqual(self._run("--slo-op", "DEL:1,1"), 2)
        self.assertEqual(self._run("--slo-op", "SET:1"), 2)
        self.assertEqual(self._run("--slo-op", "SET:,"), 2)

    def _write_preset(self, data):
        path = os.path.join(self.tmp.name, "slo.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f)
        return path

    def test_preset_thresholds_gate_the_run(self):
        # A generous preset passes; an impossible one fails with
        # exit 4, and both reports name the preset file.
        generous = self._write_preset({"p99_ms": 1000})
        self.assertEqual(self._run("--slo-preset", generous), 0)
        with open(self.output, encoding="utf-8") as f:
            text = f.read()
        self.assertIn("SLO gate: passed", text)
        self.assertIn(f"preset file `{generous}`", text)
        with open(self.output.replace(".md", ".json"),
                  encoding="utf-8") as f:
            doc = json.load(f)
        self.assertEqual(doc["workload"]["slo_preset"], generous)
        self.assertEqual(doc["slo"]["preset"], generous)
        self.assertEqual(doc["workload"]["slo_p99_ms"], 1000.0)
        impossible = self._write_preset({"p99_ms": 0.000001})
        self.assertEqual(self._run("--slo-preset", impossible), 4)

    def test_preset_per_op_thresholds_gate_the_run(self):
        preset = self._write_preset(
            {"per_op": {"SET": {"p99_ms": 0.000001}}})
        self.assertEqual(self._run("--slo-preset", preset), 4)
        with open(self.output.replace(".md", ".json"),
                  encoding="utf-8") as f:
            doc = json.load(f)
        self.assertEqual(doc["slo"]["per_op_thresholds"], {
            "SET": {"p99_ms": 0.000001, "error_rate_pct": None}})

    def test_cli_flags_override_preset(self):
        # The preset's impossible p99 cap is overridden by a
        # generous CLI flag, so the run passes.
        preset = self._write_preset({"p99_ms": 0.000001,
                                     "error_rate_pct": 0.0})
        self.assertEqual(self._run(
            "--slo-preset", preset, "--slo-p99-ms", "1000"), 0)
        with open(self.output.replace(".md", ".json"),
                  encoding="utf-8") as f:
            doc = json.load(f)
        self.assertEqual(doc["workload"]["slo_p99_ms"], 1000.0)
        # And a CLI per-op flag overrides the preset's entry for
        # that op only: a preset whose SET cap is impossible passes
        # once the CLI replaces the SET entry.
        per_op_preset = self._write_preset(
            {"p99_ms": 1000,
             "per_op": {"SET": {"p99_ms": 0.000001}}})
        self.assertEqual(self._run("--slo-preset", per_op_preset), 4)
        self.assertEqual(self._run(
            "--slo-preset", per_op_preset,
            "--slo-op", "SET:1000,"), 0)

    def test_missing_preset_file_rejected(self):
        missing = os.path.join(self.tmp.name, "nope.json")
        self.assertEqual(self._run("--slo-preset", missing), 2)

    def test_invalid_preset_file_rejected(self):
        path = os.path.join(self.tmp.name, "bad.json")
        with open(path, "w", encoding="utf-8") as f:
            f.write("{not json")
        self.assertEqual(self._run("--slo-preset", path), 2)
        with open(path, "w", encoding="utf-8") as f:
            json.dump({"p99_ms": -1}, f)
        self.assertEqual(self._run("--slo-preset", path), 2)


if __name__ == "__main__":
    unittest.main()
