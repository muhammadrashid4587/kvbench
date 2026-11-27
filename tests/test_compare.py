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
    load_baseline, load_history_baseline, workload_differences, \
    write_comparison_json
from kvbench.report import _workload_dict, render_json, render_markdown
from kvbench.stats import histogram


class _FakeRespHandler(socketserver.StreamRequestHandler):
    """Minimal RESP: SET/GET/INCR/DEL against the server's own dict."""

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
            elif op == "DEL":
                removed = 1 if args[1] in store else 0
                store.pop(args[1], None)
                self.wfile.write(f":{removed}\r\n".encode())
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


def _result_with_ops(name="cinder (127.0.0.1:6379)"):
    """A result with per-op latencies: 50 SETs at 0.1 ms and
    50 GETs at 0.2 ms, so its per-op histograms have something
    to draw."""
    by_op = {"SET": [100_000] * 50, "GET": [200_000] * 50}
    return BenchmarkResult(
        name=name,
        ops=100,
        elapsed_s=0.01,
        errors=0,
        latencies_ns=by_op["SET"] + by_op["GET"],
        latencies_by_op=by_op,
    )


def _baseline_histograms():
    """Baseline per-op histograms: 50 SETs at 0.1 ms, 50 GETs at
    0.4 ms — the GET bucket differs from the current run's, so a
    test can tell the two blocks apart."""
    return {"SET": histogram([100_000] * 50),
            "GET": histogram([400_000] * 50)}


def _baseline_per_op():
    """Baseline per-op stats: SET at 4,000 ops/sec and p99 0.2 ms,
    GET at 5,000 ops/sec and p99 0.1 ms — both differ from the
    current run's, so a test can tell the deltas apart. The p50s
    match the p99s (single-value samples), so the best/worst
    histogram comparison table can tell the two sides apart too."""
    return {
        "SET": {"count": 50, "ops_per_sec": 4_000, "p50_ms": 0.2,
                "p99_ms": 0.2},
        "GET": {"count": 50, "ops_per_sec": 5_000, "p50_ms": 0.1,
                "p99_ms": 0.1},
    }


def _baseline_doc(name="cinder (127.0.0.1:6379)", ops_per_sec=10_000,
                  p99_ms=0.1, workload=None, generated="2026-11-23T00:00:00+00:00",
                  histograms_by_op=None, per_op=None):
    result = {
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
    }
    if histograms_by_op is not None:
        result["histograms_by_op"] = histograms_by_op
    if per_op is not None:
        result["per_op"] = per_op
    return {
        "tool": "kvbench",
        "version": "0.13.0",
        "generated": generated,
        "command": "kvbench --ops 100 --clients 2",
        "workload": workload if workload is not None
        else _workload_dict(_workload()),
        "results": [result],
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
                errors=0, workload=None, per_op=None):
        record = {
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
            "workload": workload if workload is not None
            else _workload_dict(_workload()),
        }
        if per_op is not None:
            record["per_op"] = per_op
        return record

    def _other_workload(self):
        # Same shape as the default workload, but 4 clients
        # instead of 2 — a different workload, so it must never
        # be picked as a baseline for the default one.
        return _workload_dict(Workload(
            ops=100, clients=4, keys=10, value_size=8,
            set_weight=50, get_weight=40, incr_weight=10))

    def _mixed_workload_history(self, tmp):
        cinder = "cinder (127.0.0.1:6379)"
        return self._write_history(tmp, [
            self._record(cinder, 8_000, 0.12,
                         "2026-11-21T10:00:00+00:00"),
            self._record(cinder, 9_000, 0.11,
                         "2026-11-22T10:00:00+00:00",
                         workload=self._other_workload()),
            self._record(cinder, 10_000, 0.10,
                         "2026-11-23T10:00:00+00:00",
                         workload=self._other_workload()),
        ])

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

    def test_history_baseline_carries_per_op_deltas(self):
        # A record since 0.22.0 carries per-op ops/sec and p99,
        # so the history baseline compares per op the same way a
        # report baseline does.
        with tempfile.TemporaryDirectory() as tmp:
            path = self._write_history(tmp, [
                self._record("cinder (127.0.0.1:6379)", 8_000, 0.12,
                             "2026-11-23T10:00:00+00:00",
                             per_op={
                                 "SET": {"ops": 50, "ops_per_sec": 4_000,
                                         "p99_ms": 0.2},
                                 "GET": {"ops": 50, "ops_per_sec": 5_000,
                                         "p99_ms": 0.1},
                             }),
            ])
            doc = load_history_baseline(path, "latest")
            comparison = compare_results(
                [_result_with_ops()], doc,
                _workload_dict(_workload()),
                "history.jsonl (history run -1)", source="history")
            per_op = comparison["entries"][0]["per_op"]
            self.assertAlmostEqual(
                per_op[0]["ops_sec_delta_pct"], 25.0)
            self.assertAlmostEqual(per_op[0]["p99_delta_pct"], -50.0)
            self.assertAlmostEqual(
                per_op[1]["ops_sec_delta_pct"], 0.0)
            self.assertAlmostEqual(per_op[1]["p99_delta_pct"], 100.0)

    def test_history_record_without_per_op_has_no_deltas(self):
        # A record before 0.22.0 carries no per_op: the rows
        # still list this run's numbers, with n/a deltas.
        with tempfile.TemporaryDirectory() as tmp:
            doc = load_history_baseline(
                self._history_path(tmp), "0")
            comparison = compare_results(
                [_result_with_ops()], doc,
                _workload_dict(_workload()),
                "history.jsonl (history run 0)", source="history")
            per_op = comparison["entries"][0]["per_op"]
            self.assertEqual([row["op"] for row in per_op],
                             ["SET", "GET"])
            for row in per_op:
                self.assertIsNone(row["baseline_ops_per_sec"])
                self.assertIsNone(row["ops_sec_delta_pct"])
                self.assertIsNone(row["p99_delta_pct"])

    def test_same_workload_latest_skips_newer_mismatched_runs(self):
        # The two newest runs used a different workload (4
        # clients); the latest run with the default workload is
        # the oldest one, at 8,000 ops/sec.
        with tempfile.TemporaryDirectory() as tmp:
            doc = load_history_baseline(
                self._mixed_workload_history(tmp), "latest",
                workload=_workload_dict(_workload()))
            by_name = {r["name"]: r for r in doc["results"]}
            self.assertEqual(by_name["cinder (127.0.0.1:6379)"]
                             ["ops_per_sec"], 8_000)

    def test_same_workload_without_filter_picks_latest_anyway(self):
        # Without the filter the newest run wins, whatever its
        # workload was — the default stays unchanged.
        with tempfile.TemporaryDirectory() as tmp:
            doc = load_history_baseline(
                self._mixed_workload_history(tmp), "latest")
            by_name = {r["name"]: r for r in doc["results"]}
            self.assertEqual(by_name["cinder (127.0.0.1:6379)"]
                             ["ops_per_sec"], 10_000)

    def test_same_workload_index_counts_within_matching_runs(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = self._write_history(tmp, [
                self._record("cinder (127.0.0.1:6379)", 8_000, 0.12,
                             "2026-11-21T10:00:00+00:00"),
                self._record("cinder (127.0.0.1:6379)", 9_000, 0.11,
                             "2026-11-22T10:00:00+00:00",
                             workload=self._other_workload()),
                self._record("cinder (127.0.0.1:6379)", 10_000, 0.10,
                             "2026-11-23T10:00:00+00:00"),
            ])
            doc = load_history_baseline(
                path, "1", workload=_workload_dict(_workload()))
            by_name = {r["name"]: r for r in doc["results"]}
            # Index 1 among the matching runs is the Nov 23 run;
            # the mismatched Nov 22 run is not counted.
            self.assertEqual(by_name["cinder (127.0.0.1:6379)"]
                             ["ops_per_sec"], 10_000)

    def test_same_workload_timestamp_skips_mismatched_runs(self):
        with tempfile.TemporaryDirectory() as tmp:
            doc = load_history_baseline(
                self._mixed_workload_history(tmp),
                "2026-11-23T12:00:00Z",
                workload=_workload_dict(_workload()))
            by_name = {r["name"]: r for r in doc["results"]}
            self.assertEqual(by_name["cinder (127.0.0.1:6379)"]
                             ["ops_per_sec"], 8_000)

    def test_same_workload_no_matching_run_is_an_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = self._write_history(tmp, [
                self._record("cinder (127.0.0.1:6379)", 9_000, 0.11,
                             "2026-11-22T10:00:00+00:00",
                             workload=self._other_workload()),
            ])
            with self.assertRaises(ValueError) as ctx:
                load_history_baseline(
                    path, "latest",
                    workload=_workload_dict(_workload()))
            self.assertIn("same workload", str(ctx.exception))

    def test_same_workload_record_without_workload_never_matches(self):
        with tempfile.TemporaryDirectory() as tmp:
            record = self._record("cinder (127.0.0.1:6379)",
                                  9_000, 0.11,
                                  "2026-11-22T10:00:00+00:00")
            del record["workload"]
            path = self._write_history(tmp, [record])
            with self.assertRaises(ValueError) as ctx:
                load_history_baseline(
                    path, "latest",
                    workload=_workload_dict(_workload()))
            self.assertIn("same workload", str(ctx.exception))

    def test_same_workload_baseline_compares_clean(self):
        # The filtered baseline used the same workload, so the
        # comparison reports no workload differences.
        with tempfile.TemporaryDirectory() as tmp:
            doc = load_history_baseline(
                self._mixed_workload_history(tmp), "latest",
                workload=_workload_dict(_workload()))
            comparison = compare_results(
                [_result()], doc, _workload_dict(_workload()),
                "history.jsonl (history run latest, same workload)",
                source="history")
            self.assertTrue(comparison["workload_matches"])
            self.assertEqual(comparison["workload_differences"], [])


class TestCompareResults(unittest.TestCase):
    def test_write_comparison_json_round_trips(self):
        # The export is the exact dict compare_results returns:
        # it round-trips unchanged, and the parent directory is
        # created when needed.
        comparison = compare_results(
            [_result_with_ops()],
            _baseline_doc(ops_per_sec=8_000, p99_ms=0.12,
                          histograms_by_op=_baseline_histograms()),
            _workload_dict(_workload()), "prev.json")
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "nested", "comparison.json")
            returned = write_comparison_json(path, comparison)
            self.assertIs(returned, comparison)
            with open(path, encoding="utf-8") as f:
                parsed = json.load(f)
            self.assertEqual(parsed, comparison)

    def test_entries_carry_per_op_histograms(self):
        comparison = compare_results(
            [_result_with_ops()],
            _baseline_doc(ops_per_sec=8_000, p99_ms=0.12,
                          histograms_by_op=_baseline_histograms()),
            _workload_dict(_workload()), "prev.json")
        entry = comparison["entries"][0]
        # The current run's histograms are the result's own
        # per-op histograms — same buckets as the single-run
        # report, from the same helper.
        self.assertEqual(entry["histograms_by_op"],
                         _result_with_ops().per_op_histograms)
        self.assertEqual(
            entry["histograms_by_op"]["SET"][0],
            {"upper_ms": 0.1, "count": 50})
        self.assertEqual(
            entry["histograms_by_op"]["GET"][1],
            {"upper_ms": 0.25, "count": 50})
        # The baseline run's histograms come from the baseline
        # report, with the GETs in a slower bucket.
        self.assertEqual(entry["baseline_histograms_by_op"],
                         _baseline_histograms())
        self.assertEqual(
            entry["baseline_histograms_by_op"]["GET"][2],
            {"upper_ms": 0.5, "count": 50})

    def test_baseline_without_histograms_is_none(self):
        # A report written before 0.32.0 carries no
        # histograms_by_op: the field is None, not invented.
        comparison = compare_results(
            [_result_with_ops()], _baseline_doc(),
            _workload_dict(_workload()), "prev.json")
        entry = comparison["entries"][0]
        self.assertEqual(entry["histograms_by_op"],
                         _result_with_ops().per_op_histograms)
        self.assertIsNone(entry["baseline_histograms_by_op"])

    def test_malformed_baseline_histograms_are_ignored(self):
        # A hand-made baseline file may carry anything: a shape
        # the renderer cannot draw reads as no baseline
        # histograms instead of crashing the report.
        for bad in ({"SET": "not a list"}, {"SET": [{"count": 1}]},
                    ["SET"], "histograms"):
            doc = _baseline_doc(histograms_by_op=bad)
            comparison = compare_results(
                [_result_with_ops()], doc,
                _workload_dict(_workload()), "prev.json")
            self.assertIsNone(
                comparison["entries"][0]["baseline_histograms_by_op"])

    def test_entries_carry_per_op_deltas(self):
        # Current run: SET 50 ops at 0.1 ms over 0.01 s — 5,000
        # ops/sec, p99 0.1 ms; GET 50 ops at 0.2 ms — 5,000
        # ops/sec, p99 0.2 ms. Baseline: SET 4,000 ops/sec at
        # p99 0.2 ms, GET 5,000 ops/sec at p99 0.1 ms.
        comparison = compare_results(
            [_result_with_ops()],
            _baseline_doc(ops_per_sec=8_000, p99_ms=0.12,
                          per_op=_baseline_per_op()),
            _workload_dict(_workload()), "prev.json")
        per_op = comparison["entries"][0]["per_op"]
        self.assertEqual([row["op"] for row in per_op],
                         ["SET", "GET"])
        set_row, get_row = per_op
        self.assertEqual(set_row["ops"], 50)
        self.assertAlmostEqual(set_row["ops_per_sec"], 5_000)
        self.assertEqual(set_row["baseline_ops_per_sec"], 4_000)
        self.assertAlmostEqual(set_row["ops_sec_delta_pct"], 25.0)
        self.assertAlmostEqual(set_row["p50_ms"], 0.1)
        self.assertEqual(set_row["baseline_p50_ms"], 0.2)
        self.assertAlmostEqual(set_row["p99_ms"], 0.1)
        self.assertEqual(set_row["baseline_p99_ms"], 0.2)
        self.assertAlmostEqual(set_row["p99_delta_pct"], -50.0)
        self.assertAlmostEqual(get_row["ops_sec_delta_pct"], 0.0)
        self.assertEqual(get_row["baseline_ops_per_sec"], 5_000)
        self.assertAlmostEqual(get_row["p50_ms"], 0.2)
        self.assertEqual(get_row["baseline_p50_ms"], 0.1)
        self.assertAlmostEqual(get_row["p99_delta_pct"], 100.0)

    def test_op_missing_from_baseline_has_no_delta(self):
        # The baseline ran SET only: GET keeps its current
        # numbers with the baseline fields and deltas None.
        baseline_per_op = {"SET": {"count": 50, "ops_per_sec": 4_000,
                                   "p99_ms": 0.2}}
        comparison = compare_results(
            [_result_with_ops()],
            _baseline_doc(per_op=baseline_per_op),
            _workload_dict(_workload()), "prev.json")
        per_op = comparison["entries"][0]["per_op"]
        self.assertAlmostEqual(
            per_op[0]["ops_sec_delta_pct"], 25.0)
        self.assertAlmostEqual(per_op[0]["p50_ms"], 0.1)
        # The inline baseline carries no p50, like a history
        # record: the field is None, not invented.
        self.assertIsNone(per_op[0]["baseline_p50_ms"])
        self.assertIsNone(per_op[1]["baseline_ops_per_sec"])
        self.assertIsNone(per_op[1]["baseline_p99_ms"])
        self.assertIsNone(per_op[1]["ops_sec_delta_pct"])
        self.assertIsNone(per_op[1]["p99_delta_pct"])
        self.assertAlmostEqual(per_op[1]["ops_per_sec"], 5_000)
        self.assertAlmostEqual(per_op[1]["p99_ms"], 0.2)

    def test_baseline_without_per_op_keeps_current_numbers(self):
        # A report written before per-op stats existed carries
        # no per_op: the rows still list this run's numbers, with
        # the baseline fields and deltas None, not invented.
        comparison = compare_results(
            [_result_with_ops()], _baseline_doc(),
            _workload_dict(_workload()), "prev.json")
        per_op = comparison["entries"][0]["per_op"]
        self.assertEqual([row["op"] for row in per_op],
                         ["SET", "GET"])
        for row in per_op:
            self.assertIsNone(row["baseline_ops_per_sec"])
            self.assertIsNone(row["baseline_p99_ms"])
            self.assertIsNone(row["ops_sec_delta_pct"])
            self.assertIsNone(row["p99_delta_pct"])

    def test_entries_carry_best_worst(self):
        # Same numbers as test_entries_carry_per_op_deltas: SET
        # +25.0% ops/sec and -50.0% p99, GET +0.0% and +100.0% —
        # so SET is best on both measures and GET worst on both.
        comparison = compare_results(
            [_result_with_ops()],
            _baseline_doc(ops_per_sec=8_000, p99_ms=0.12,
                          per_op=_baseline_per_op(),
                          histograms_by_op=_baseline_histograms()),
            _workload_dict(_workload()), "prev.json")
        best_worst = comparison["entries"][0]["best_worst"]
        self.assertEqual(best_worst["best_by_ops_sec"]["op"], "SET")
        self.assertAlmostEqual(
            best_worst["best_by_ops_sec"]["ops_sec_delta_pct"], 25.0)
        self.assertEqual(best_worst["worst_by_ops_sec"]["op"], "GET")
        self.assertAlmostEqual(
            best_worst["worst_by_ops_sec"]["ops_sec_delta_pct"], 0.0)
        self.assertEqual(best_worst["best_by_p99"]["op"], "SET")
        self.assertAlmostEqual(
            best_worst["best_by_p99"]["p99_delta_pct"], -50.0)
        self.assertEqual(best_worst["worst_by_p99"]["op"], "GET")
        self.assertAlmostEqual(
            best_worst["worst_by_p99"]["p99_delta_pct"], 100.0)
        self.assertEqual(best_worst["notes"], [])
        # Each pick carries its op's latency histogram from both
        # runs: SET ran at 0.1 ms in both, so its two shapes
        # match; GET ran at 0.2 ms now and 0.4 ms before, so its
        # two shapes differ — the test can tell them apart.
        self.assertEqual(
            best_worst["best_by_ops_sec"]["histogram"],
            histogram([100_000] * 50))
        self.assertEqual(
            best_worst["best_by_ops_sec"]["baseline_histogram"],
            histogram([100_000] * 50))
        self.assertEqual(
            best_worst["worst_by_p99"]["histogram"],
            histogram([200_000] * 50))
        self.assertEqual(
            best_worst["worst_by_p99"]["baseline_histogram"],
            histogram([400_000] * 50))
        # The picked ops' per-op histogram comparison table:
        # SET ran at 0.1 ms in both runs, so its p50/p99 are
        # unchanged and its buckets did not move; GET ran at
        # 0.2 ms now and 0.4 ms before, so its p99 doubled and
        # its whole share moved from the <= 0.5 ms bucket to
        # the <= 0.25 ms bucket.
        histogram_comparison = best_worst["histogram_comparison"]
        self.assertEqual(histogram_comparison["from"], "prev")
        self.assertEqual(histogram_comparison["to"], "now")
        self.assertEqual(histogram_comparison["notes"], [])
        rows = histogram_comparison["rows"]
        self.assertEqual([row["op"] for row in rows],
                         ["SET", "GET"])
        set_row, get_row = rows
        self.assertAlmostEqual(set_row["p50_from_ms"], 0.2)
        self.assertAlmostEqual(set_row["p50_to_ms"], 0.1)
        self.assertAlmostEqual(set_row["p99_from_ms"], 0.2)
        self.assertAlmostEqual(set_row["p99_to_ms"], 0.1)
        self.assertAlmostEqual(set_row["p99_change_pct"], -50.0)
        self.assertAlmostEqual(set_row["bucket_drift_pp"], 0.0)
        self.assertEqual(set_row["bucket_drift_bucket"], "<= 0.1 ms")
        self.assertAlmostEqual(get_row["p50_from_ms"], 0.1)
        self.assertAlmostEqual(get_row["p50_to_ms"], 0.2)
        self.assertAlmostEqual(get_row["p99_from_ms"], 0.1)
        self.assertAlmostEqual(get_row["p99_to_ms"], 0.2)
        self.assertAlmostEqual(get_row["p99_change_pct"], 100.0)
        self.assertAlmostEqual(get_row["bucket_drift_pp"], 100.0)
        self.assertEqual(get_row["bucket_drift_bucket"], "<= 0.25 ms")

    def test_best_worst_histogram_comparison_without_baseline_histograms(
            self):
        # A baseline with per-op stats but no histograms — a
        # report written before 0.32.0, or a history baseline —
        # still picks best/worst ops, but there is no histogram
        # comparison table: a note names the reason instead.
        comparison = compare_results(
            [_result_with_ops()],
            _baseline_doc(ops_per_sec=8_000, p99_ms=0.12,
                          per_op=_baseline_per_op()),
            _workload_dict(_workload()), "prev.json")
        histogram_comparison = (
            comparison["entries"][0]["best_worst"]
            ["histogram_comparison"])
        self.assertEqual(histogram_comparison["from"], "prev")
        self.assertEqual(histogram_comparison["to"], "now")
        self.assertIsNone(histogram_comparison["rows"])
        self.assertEqual(histogram_comparison["notes"], [
            "The baseline run carries no per-op histograms, so "
            "there is no histogram comparison for the best/worst "
            "ops."])

    def test_best_worst_histogram_comparison_without_baseline_per_op(
            self):
        # A baseline with no per-op stats has no best/worst
        # picks and no histogram comparison table either.
        comparison = compare_results(
            [_result_with_ops()], _baseline_doc(),
            _workload_dict(_workload()), "prev.json")
        histogram_comparison = (
            comparison["entries"][0]["best_worst"]
            ["histogram_comparison"])
        self.assertIsNone(histogram_comparison["rows"])
        self.assertEqual(histogram_comparison["notes"], [
            "The baseline carries no per-operation stats, so "
            "there is no histogram comparison for the best/worst "
            "ops."])

    def test_best_worst_histogram_comparison_baseline_without_p50(
            self):
        # A baseline whose per-op stats carry no p50 — a history
        # record carries only ops/sec and p99 — still gets a
        # table: the previous p50 reads as None, and the p99
        # change and drift still compute from the histograms.
        baseline_per_op = {
            "SET": {"count": 50, "ops_per_sec": 4_000,
                    "p99_ms": 0.2},
            "GET": {"count": 50, "ops_per_sec": 5_000,
                    "p99_ms": 0.1},
        }
        comparison = compare_results(
            [_result_with_ops()],
            _baseline_doc(ops_per_sec=8_000, p99_ms=0.12,
                          per_op=baseline_per_op,
                          histograms_by_op=_baseline_histograms()),
            _workload_dict(_workload()), "prev.json")
        histogram_comparison = (
            comparison["entries"][0]["best_worst"]
            ["histogram_comparison"])
        self.assertEqual(histogram_comparison["notes"], [])
        rows = histogram_comparison["rows"]
        self.assertEqual([row["op"] for row in rows],
                         ["SET", "GET"])
        self.assertIsNone(rows[0]["p50_from_ms"])
        self.assertAlmostEqual(rows[0]["p50_to_ms"], 0.1)
        self.assertAlmostEqual(rows[0]["p99_change_pct"], -50.0)
        self.assertIsNone(rows[1]["p50_from_ms"])
        self.assertAlmostEqual(rows[1]["p50_to_ms"], 0.2)
        self.assertAlmostEqual(rows[1]["p99_change_pct"], 100.0)

    def test_best_worst_without_baseline_histograms(self):
        # A baseline with per-op stats but no histograms — a
        # report written before 0.32.0, or a history baseline —
        # still picks best/worst ops, but each pick carries only
        # this run's histogram and None for the baseline's.
        comparison = compare_results(
            [_result_with_ops()],
            _baseline_doc(ops_per_sec=8_000, p99_ms=0.12,
                          per_op=_baseline_per_op()),
            _workload_dict(_workload()), "prev.json")
        best_worst = comparison["entries"][0]["best_worst"]
        self.assertEqual(
            best_worst["best_by_ops_sec"]["histogram"],
            histogram([100_000] * 50))
        self.assertIsNone(
            best_worst["best_by_ops_sec"]["baseline_histogram"])
        self.assertEqual(
            best_worst["worst_by_p99"]["histogram"],
            histogram([200_000] * 50))
        self.assertIsNone(
            best_worst["worst_by_p99"]["baseline_histogram"])

    def test_best_worst_single_op(self):
        # Only SET ran: the best and worst op are the same op,
        # and a note says so instead of a spread.
        result = BenchmarkResult(
            name="cinder (127.0.0.1:6379)",
            ops=50, elapsed_s=0.01, errors=0,
            latencies_ns=[100_000] * 50,
            latencies_by_op={"SET": [100_000] * 50})
        baseline_per_op = {"SET": {"count": 50, "ops_per_sec": 4_000,
                                   "p99_ms": 0.2}}
        comparison = compare_results(
            [result], _baseline_doc(per_op=baseline_per_op),
            _workload_dict(_workload()), "prev.json")
        best_worst = comparison["entries"][0]["best_worst"]
        for key in ("best_by_ops_sec", "worst_by_ops_sec",
                    "best_by_p99", "worst_by_p99"):
            self.assertEqual(best_worst[key]["op"], "SET")
        self.assertEqual(best_worst["notes"], [
            "Only one op ran (SET), so the best and worst op are "
            "the same op."])

    def test_best_worst_identical_runs(self):
        # Every per-op delta is 0.0: the picks are the first op
        # in order, and a note says the runs are identical.
        identical_per_op = {
            "SET": {"count": 50, "ops_per_sec": 5_000, "p99_ms": 0.1},
            "GET": {"count": 50, "ops_per_sec": 5_000, "p99_ms": 0.2},
        }
        comparison = compare_results(
            [_result_with_ops()],
            _baseline_doc(per_op=identical_per_op),
            _workload_dict(_workload()), "prev.json")
        best_worst = comparison["entries"][0]["best_worst"]
        for key in ("best_by_ops_sec", "worst_by_ops_sec",
                    "best_by_p99", "worst_by_p99"):
            self.assertEqual(best_worst[key]["op"], "SET")
        self.assertEqual(best_worst["notes"], [
            "The two runs are identical — every op's ops/sec "
            "and p99 are unchanged, so the best and worst op "
            "are the same."])

    def test_best_worst_without_baseline_per_op(self):
        # A baseline with no per-op stats has no best or worst
        # op, and a note says why.
        comparison = compare_results(
            [_result_with_ops()], _baseline_doc(),
            _workload_dict(_workload()), "prev.json")
        best_worst = comparison["entries"][0]["best_worst"]
        for key in ("best_by_ops_sec", "worst_by_ops_sec",
                    "best_by_p99", "worst_by_p99"):
            self.assertIsNone(best_worst[key])
        self.assertEqual(best_worst["notes"], [
            "The baseline carries no per-operation stats, so "
            "there is no best or worst op."])

    def test_best_worst_op_missing_from_baseline(self):
        # The baseline ran SET only: the picks cover SET alone,
        # and a note names GET as left out.
        baseline_per_op = {"SET": {"count": 50, "ops_per_sec": 4_000,
                                   "p99_ms": 0.2}}
        comparison = compare_results(
            [_result_with_ops()],
            _baseline_doc(per_op=baseline_per_op),
            _workload_dict(_workload()), "prev.json")
        best_worst = comparison["entries"][0]["best_worst"]
        for key in ("best_by_ops_sec", "worst_by_ops_sec",
                    "best_by_p99", "worst_by_p99"):
            self.assertEqual(best_worst[key]["op"], "SET")
        self.assertEqual(best_worst["notes"], [
            "GET never ran in the baseline run, so it is not "
            "part of the best/worst picks."])

    def test_malformed_baseline_per_op_is_ignored(self):
        # A hand-made baseline file may carry anything: a shape
        # the comparison cannot read reads as no baseline per-op
        # stats instead of crashing the report.
        for bad in ({"SET": {"ops_per_sec": "fast"}},
                    {"SET": {"p99_ms": 0.2}},
                    {"SET": "not a dict"},
                    ["SET"], "per-op"):
            doc = _baseline_doc(per_op=bad)
            comparison = compare_results(
                [_result_with_ops()], doc,
                _workload_dict(_workload()), "prev.json")
            per_op = comparison["entries"][0]["per_op"]
            self.assertTrue(all(
                row["baseline_ops_per_sec"] is None
                and row["ops_sec_delta_pct"] is None
                for row in per_op))

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

    def test_markdown_per_op_histograms(self):
        comparison = compare_results(
            [_result_with_ops()],
            _baseline_doc(ops_per_sec=8_000, p99_ms=0.12,
                          histograms_by_op=_baseline_histograms()),
            _workload_dict(_workload()), "prev.json")
        text = render_markdown([_result_with_ops()], _workload(),
                               "kvbench", comparison)
        # One block per run, drawn by the shared helper: the
        # current run's SETs land in the 0.1 ms bucket, the
        # baseline run's GETs in the 0.5 ms bucket.
        self.assertIn(
            "#### cinder (127.0.0.1:6379) — per-op latency histogram "
            "(now)", text)
        self.assertIn(
            "#### cinder (127.0.0.1:6379) — per-op latency histogram "
            "(prev)", text)
        self.assertIn("SET (50 ops)", text)
        self.assertIn("- <= 0.1 ms: 50 (100.0%)", text)
        self.assertIn("GET (50 ops)", text)
        self.assertIn("- <= 0.5 ms: 50 (100.0%)", text)
        self.assertIn("per-operation latency histograms", text)

    def test_markdown_baseline_without_histograms_notes_it(self):
        comparison = compare_results(
            [_result_with_ops()], _baseline_doc(),
            _workload_dict(_workload()), "prev.json")
        text = render_markdown([_result_with_ops()], _workload(),
                               "kvbench", comparison)
        self.assertIn(
            "#### cinder (127.0.0.1:6379) — per-op latency histogram "
            "(now)", text)
        self.assertNotIn("per-op latency histogram (prev)", text)
        self.assertIn("the baseline report carries no per-op "
                      "histograms", text)

    def test_markdown_history_baseline_notes_it(self):
        # A history baseline carries no per-op histograms either,
        # and the note names the history file as the reason.
        comparison = compare_results(
            [_result_with_ops()], _baseline_doc(),
            _workload_dict(_workload()),
            "history.jsonl (history run -1)", source="history")
        text = render_markdown([_result_with_ops()], _workload(),
                               "kvbench", comparison)
        self.assertIn(
            "#### cinder (127.0.0.1:6379) — per-op latency histogram "
            "(now)", text)
        self.assertNotIn("per-op latency histogram (prev)", text)
        self.assertIn("the history file carries no per-op "
                      "histograms", text)

    def test_markdown_per_op_delta_table(self):
        comparison = compare_results(
            [_result_with_ops()],
            _baseline_doc(ops_per_sec=8_000, p99_ms=0.12,
                          per_op=_baseline_per_op()),
            _workload_dict(_workload()), "prev.json")
        text = render_markdown([_result_with_ops()], _workload(),
                               "kvbench", comparison)
        self.assertIn(
            "#### cinder (127.0.0.1:6379) — per-op p99 / ops-sec "
            "deltas", text)
        self.assertIn(
            "| Op | Ops/sec now | Ops/sec prev | Ops/sec Δ | "
            "p99 now (ms) | p99 prev (ms) | p99 Δ |", text)
        # SET: 5,000 now vs 4,000 prev (+25.0%), p99 0.100 vs
        # 0.200 (-50.0%); GET: 5,000 vs 5,000 (+0.0%), p99 0.200
        # vs 0.100 (+100.0%).
        self.assertIn(
            "| SET | 5,000 | 4,000 | +25.0% | 0.100 | 0.200 | "
            "-50.0% |", text)
        self.assertIn(
            "| GET | 5,000 | 5,000 | +0.0% | 0.200 | 0.100 | "
            "+100.0% |", text)
        self.assertIn("per-operation p99 / ops-sec delta table", text)

    def test_markdown_baseline_without_per_op_notes_it(self):
        comparison = compare_results(
            [_result_with_ops()], _baseline_doc(),
            _workload_dict(_workload()), "prev.json")
        text = render_markdown([_result_with_ops()], _workload(),
                               "kvbench", comparison)
        # The table still lists this run's per-op numbers, with
        # n/a where the baseline has none, and a note says why.
        self.assertIn(
            "#### cinder (127.0.0.1:6379) — per-op p99 / ops-sec "
            "deltas", text)
        self.assertIn("| SET | 5,000 | n/a | n/a | 0.100 | n/a | n/a |",
                      text)
        self.assertIn("the baseline report carries no per-operation "
                      "stats", text)

    def test_markdown_history_baseline_without_per_op_notes_it(self):
        comparison = compare_results(
            [_result_with_ops()], _baseline_doc(),
            _workload_dict(_workload()),
            "history.jsonl (history run -1)", source="history")
        text = render_markdown([_result_with_ops()], _workload(),
                               "kvbench", comparison)
        self.assertIn("the history file carries no per-operation "
                      "stats", text)

    def test_json_carries_per_op_deltas(self):
        parsed = json.loads(render_json(
            [_result_with_ops()], _workload(), "kvbench",
            compare_results(
                [_result_with_ops()],
                _baseline_doc(ops_per_sec=8_000, p99_ms=0.12,
                              per_op=_baseline_per_op()),
                _workload_dict(_workload()), "prev.json")))
        per_op = parsed["comparison"]["entries"][0]["per_op"]
        self.assertEqual([row["op"] for row in per_op],
                         ["SET", "GET"])
        self.assertAlmostEqual(per_op[0]["ops_sec_delta_pct"], 25.0)
        self.assertAlmostEqual(per_op[0]["p99_delta_pct"], -50.0)
        self.assertAlmostEqual(per_op[1]["ops_sec_delta_pct"], 0.0)
        self.assertAlmostEqual(per_op[1]["p99_delta_pct"], 100.0)

    def test_markdown_best_worst_section(self):
        comparison = compare_results(
            [_result_with_ops()],
            _baseline_doc(ops_per_sec=8_000, p99_ms=0.12,
                          per_op=_baseline_per_op(),
                          histograms_by_op=_baseline_histograms()),
            _workload_dict(_workload()), "prev.json")
        text = render_markdown([_result_with_ops()], _workload(),
                               "kvbench", comparison)
        # SET: +25.0% ops/sec, -50.0% p99; GET: +0.0%, +100.0% —
        # so SET is best on both measures and GET worst on both.
        self.assertIn(
            "#### cinder (127.0.0.1:6379) — best/worst op vs "
            "previous run", text)
        self.assertIn("| Ops/sec Δ | SET (+25.0%) | GET (+0.0%) |", text)
        self.assertIn("| p99 Δ | SET (-50.0%) | GET (+100.0%) |", text)
        # Each best/worst op's latency histogram from both runs,
        # drawn under the table: SET is best on both measures
        # and GET worst on both, so each op renders once, with
        # a now block and a prev block.
        for op in ("SET", "GET"):
            self.assertIn(
                f"##### cinder (127.0.0.1:6379) — {op} latency "
                "histogram (now)", text)
            self.assertIn(
                f"##### cinder (127.0.0.1:6379) — {op} latency "
                "histogram (prev)", text)
        self.assertEqual(
            text.count("— SET latency histogram (now)"), 1)
        self.assertEqual(
            text.count("— GET latency histogram (now)"), 1)
        # The picked ops' per-op histogram comparison table,
        # under the histogram blocks: SET unchanged, GET's p99
        # doubled with its whole share moving buckets.
        self.assertIn(
            "##### cinder (127.0.0.1:6379) — best/worst op "
            "histogram comparison", text)
        self.assertIn(
            "| Op | p50 prev (ms) | p50 now (ms) | p99 prev (ms) | "
            "p99 now (ms) | p99 change | Bucket drift |", text)
        self.assertIn(
            "| SET | 0.200 | 0.100 | 0.200 | 0.100 | -50.0% | "
            "+0.0 pp (<= 0.1 ms) |", text)
        self.assertIn(
            "| GET | 0.100 | 0.200 | 0.100 | 0.200 | +100.0% | "
            "+100.0 pp (<= 0.25 ms) |", text)

    def test_markdown_best_worst_histogram_comparison_no_baseline_hists(
            self):
        # A baseline with per-op stats but no histograms renders
        # no comparison table: a note names the reason instead.
        comparison = compare_results(
            [_result_with_ops()],
            _baseline_doc(ops_per_sec=8_000, p99_ms=0.12,
                          per_op=_baseline_per_op()),
            _workload_dict(_workload()), "prev.json")
        text = render_markdown([_result_with_ops()], _workload(),
                               "kvbench", comparison)
        self.assertNotIn("best/worst op histogram comparison", text)
        self.assertIn(
            "The baseline run carries no per-op histograms, so "
            "there is no histogram comparison for the best/worst "
            "ops.", text)

    def test_markdown_best_worst_without_baseline_histograms(self):
        # A baseline with per-op stats but no histograms renders
        # only this run's shape for each best/worst op — no prev
        # block, and no crash on the missing shape.
        comparison = compare_results(
            [_result_with_ops()],
            _baseline_doc(ops_per_sec=8_000, p99_ms=0.12,
                          per_op=_baseline_per_op()),
            _workload_dict(_workload()), "prev.json")
        text = render_markdown([_result_with_ops()], _workload(),
                               "kvbench", comparison)
        self.assertIn(
            "##### cinder (127.0.0.1:6379) — SET latency "
            "histogram (now)", text)
        self.assertIn(
            "##### cinder (127.0.0.1:6379) — GET latency "
            "histogram (now)", text)
        self.assertNotIn("latency histogram (prev)", text)

    def test_markdown_best_worst_identical_runs_notes_it(self):
        identical_per_op = {
            "SET": {"count": 50, "ops_per_sec": 5_000, "p99_ms": 0.1},
            "GET": {"count": 50, "ops_per_sec": 5_000, "p99_ms": 0.2},
        }
        comparison = compare_results(
            [_result_with_ops()],
            _baseline_doc(per_op=identical_per_op),
            _workload_dict(_workload()), "prev.json")
        text = render_markdown([_result_with_ops()], _workload(),
                               "kvbench", comparison)
        self.assertIn(
            "#### cinder (127.0.0.1:6379) — best/worst op vs "
            "previous run", text)
        self.assertIn("The two runs are identical", text)

    def test_markdown_best_worst_without_baseline_per_op_notes_it(self):
        comparison = compare_results(
            [_result_with_ops()], _baseline_doc(),
            _workload_dict(_workload()), "prev.json")
        text = render_markdown([_result_with_ops()], _workload(),
                               "kvbench", comparison)
        self.assertIn(
            "#### cinder (127.0.0.1:6379) — best/worst op vs "
            "previous run", text)
        self.assertIn("there is no best or worst op", text)

    def test_json_carries_best_worst(self):
        parsed = json.loads(render_json(
            [_result_with_ops()], _workload(), "kvbench",
            compare_results(
                [_result_with_ops()],
                _baseline_doc(ops_per_sec=8_000, p99_ms=0.12,
                              per_op=_baseline_per_op(),
                              histograms_by_op=_baseline_histograms()),
                _workload_dict(_workload()), "prev.json")))
        best_worst = parsed["comparison"]["entries"][0]["best_worst"]
        # The JSON carries the same picks the Markdown table
        # renders, so the two views cannot disagree.
        self.assertEqual(best_worst["best_by_ops_sec"]["op"], "SET")
        self.assertAlmostEqual(
            best_worst["best_by_ops_sec"]["ops_sec_delta_pct"], 25.0)
        self.assertEqual(best_worst["worst_by_p99"]["op"], "GET")
        self.assertAlmostEqual(
            best_worst["worst_by_p99"]["p99_delta_pct"], 100.0)
        self.assertEqual(best_worst["notes"], [])
        # ... and the same picks' latency histograms from both
        # runs, so a script reading the JSON sees what the
        # Markdown blocks draw.
        self.assertEqual(
            best_worst["best_by_ops_sec"]["histogram"],
            histogram([100_000] * 50))
        self.assertEqual(
            best_worst["best_by_ops_sec"]["baseline_histogram"],
            histogram([100_000] * 50))
        self.assertEqual(
            best_worst["worst_by_p99"]["histogram"],
            histogram([200_000] * 50))
        self.assertEqual(
            best_worst["worst_by_p99"]["baseline_histogram"],
            histogram([400_000] * 50))
        # ... and the same picked ops' histogram comparison
        # table, so a script reading the JSON sees what the
        # Markdown table shows.
        histogram_comparison = best_worst["histogram_comparison"]
        self.assertEqual(histogram_comparison["from"], "prev")
        self.assertEqual(histogram_comparison["to"], "now")
        self.assertEqual(histogram_comparison["notes"], [])
        rows = histogram_comparison["rows"]
        self.assertEqual([row["op"] for row in rows],
                         ["SET", "GET"])
        self.assertAlmostEqual(rows[0]["p99_change_pct"], -50.0)
        self.assertAlmostEqual(rows[0]["bucket_drift_pp"], 0.0)
        self.assertAlmostEqual(rows[1]["p99_change_pct"], 100.0)
        self.assertAlmostEqual(rows[1]["bucket_drift_pp"], 100.0)
        self.assertEqual(rows[1]["bucket_drift_bucket"], "<= 0.25 ms")

    def test_json_carries_per_op_histograms(self):
        parsed = json.loads(render_json(
            [_result_with_ops()], _workload(), "kvbench",
            compare_results(
                [_result_with_ops()],
                _baseline_doc(ops_per_sec=8_000, p99_ms=0.12,
                              histograms_by_op=_baseline_histograms()),
                _workload_dict(_workload()), "prev.json")))
        entry = parsed["comparison"]["entries"][0]
        self.assertEqual(
            entry["histograms_by_op"]["SET"][0],
            {"upper_ms": 0.1, "count": 50})
        self.assertEqual(
            entry["baseline_histograms_by_op"]["GET"][2],
            {"upper_ms": 0.5, "count": 50})

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

    def test_compare_file_reports_per_op_histograms(self):
        with tempfile.TemporaryDirectory() as tmp:
            baseline_json = self._run_baseline(tmp)
            out = os.path.join(tmp, "run.md")
            code = main(self._argv(["--ops", "100", "--clients", "1",
                                    "--output", out,
                                    "--compare-file", baseline_json]))
            self.assertEqual(code, 0)
            with open(out, encoding="utf-8") as f:
                text = f.read()
            name = f"cinder (127.0.0.1:{self.port})"
            self.assertIn(
                f"#### {name} — per-op latency histogram (now)", text)
            self.assertIn(
                f"#### {name} — per-op latency histogram (prev)", text)
            parsed = json.loads(open(os.path.join(tmp, "run.json"),
                                     encoding="utf-8").read())
            entry = parsed["comparison"]["entries"][0]
            # Both runs measured real ops against the fake server,
            # so both histograms carry the ops that ran, and the
            # bucket counts add up to the op count.
            self.assertIn("SET", entry["histograms_by_op"])
            self.assertIn("GET", entry["histograms_by_op"])
            self.assertIn("SET", entry["baseline_histograms_by_op"])
            self.assertIn("GET", entry["baseline_histograms_by_op"])
            for key in ("histograms_by_op",
                        "baseline_histograms_by_op"):
                total = sum(
                    bucket["count"]
                    for buckets in entry[key].values()
                    for bucket in buckets)
                self.assertEqual(total, 100)

    def test_compare_file_json_end_to_end(self):
        with tempfile.TemporaryDirectory() as tmp:
            baseline_json = self._run_baseline(tmp)
            out = os.path.join(tmp, "run.md")
            export = os.path.join(tmp, "nested", "comparison.json")
            code = main(self._argv(["--ops", "100", "--clients", "1",
                                    "--output", out,
                                    "--compare-file", baseline_json,
                                    "--compare-file-json", export]))
            self.assertEqual(code, 0)
            with open(export, encoding="utf-8") as f:
                exported = json.load(f)
            parsed = json.loads(open(os.path.join(tmp, "run.json"),
                                     encoding="utf-8").read())
            # The export is the exact comparison the JSON report
            # carries, so the two cannot disagree.
            self.assertEqual(exported, parsed["comparison"])
            self.assertEqual(exported["source"], "report")
            self.assertEqual(len(exported["entries"]), 1)
            # The export carries the best/worst op section too,
            # since it writes the same comparison dict.
            best_worst = exported["entries"][0]["best_worst"]
            self.assertEqual(
                set(best_worst),
                {"best_by_ops_sec", "worst_by_ops_sec",
                 "best_by_p99", "worst_by_p99", "notes",
                 "histogram_comparison"})
            self.assertIsNotNone(best_worst["best_by_ops_sec"])
            self.assertIsNotNone(best_worst["worst_by_p99"])
            # ... and the picked ops' histogram comparison
            # table, since it is part of the same dict.
            histogram_comparison = best_worst["histogram_comparison"]
            self.assertEqual(histogram_comparison["from"], "prev")
            self.assertEqual(histogram_comparison["to"], "now")
            self.assertIsNotNone(histogram_comparison["rows"])

    def test_compare_file_json_without_compare_file(self):
        code = main(self._argv(["--ops", "10",
                                "--compare-file-json",
                                "comparison.json"]))
        self.assertEqual(code, 2)

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

    def test_compare_history_json_end_to_end(self):
        with tempfile.TemporaryDirectory() as tmp:
            history = self._run_with_history(tmp)
            out = os.path.join(tmp, "run.md")
            export = os.path.join(tmp, "nested", "comparison.json")
            code = main(self._argv(["--ops", "100", "--clients", "1",
                                    "--output", out,
                                    "--compare-history", "-1",
                                    "--compare-history-json", export,
                                    "--history-file", history]))
            self.assertEqual(code, 0)
            with open(export, encoding="utf-8") as f:
                exported = json.load(f)
            parsed = json.loads(open(os.path.join(tmp, "run.json"),
                                     encoding="utf-8").read())
            # The export is the exact comparison the JSON report
            # carries, so the two cannot disagree.
            self.assertEqual(exported, parsed["comparison"])
            self.assertEqual(exported["source"], "history")
            self.assertEqual(len(exported["entries"]), 1)

    def test_compare_history_json_without_compare_history(self):
        code = main(self._argv(["--ops", "10",
                                "--compare-history-json",
                                "comparison.json"]))
        self.assertEqual(code, 2)

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

    def _write_history_records(self, tmp, records):
        """Write history records (dicts) to a temp history file."""
        history = os.path.join(tmp, "history.jsonl")
        with open(history, "w", encoding="utf-8") as f:
            for record in records:
                f.write(json.dumps(record) + "\n")
        return history

    def _history_record(self, ops_per_sec, p99_ms, timestamp,
                        workload):
        return {
            "tool": "kvbench", "version": "0.20.0",
            "timestamp": timestamp,
            "command": "kvbench --ops 100 --clients 1",
            "backend": f"cinder (127.0.0.1:{self.port})",
            "ops": 100, "elapsed_s": 0.01,
            "ops_per_sec": ops_per_sec, "mean_ms": p99_ms,
            "p50_ms": p99_ms, "p95_ms": p99_ms,
            "p99_ms": p99_ms, "errors": 0,
            "workload": workload,
        }

    def test_compare_same_workload_picks_matching_run(self):
        # The newest recorded run used 2 clients; this run uses
        # 1, so with the filter the baseline is the older
        # 1-client run at 8,000 ops/sec — not the newest run.
        with tempfile.TemporaryDirectory() as tmp:
            matching = _workload_dict(Workload(ops=100, clients=1))
            other = _workload_dict(Workload(ops=100, clients=2))
            history = self._write_history_records(tmp, [
                self._history_record(
                    8_000, 0.12, "2026-11-21T10:00:00+00:00", matching),
                self._history_record(
                    10**9, 0.0001, "2026-11-23T10:00:00+00:00", other),
            ])
            out = os.path.join(tmp, "run.md")
            code = main(self._argv(["--ops", "100", "--clients", "1",
                                    "--mix", "50:40:10:5",
                                    "--output", out,
                                    "--compare-history", "-1",
                                    "--compare-same-workload",
                                    "--history-file", history]))
            self.assertEqual(code, 0)
            with open(out, encoding="utf-8") as f:
                text = f.read()
            self.assertIn("## Comparison with previous run", text)
            self.assertIn("same workload", text)
            parsed = json.loads(open(os.path.join(tmp, "run.json"),
                                     encoding="utf-8").read())
            comparison = parsed["comparison"]
            self.assertTrue(comparison["workload_matches"])
            self.assertEqual(
                comparison["entries"][0]["baseline_ops_per_sec"],
                8_000)

    def test_compare_same_workload_off_picks_newest_run(self):
        # Same history, no filter: the newest run wins even
        # though its workload differs, and the report warns.
        with tempfile.TemporaryDirectory() as tmp:
            matching = _workload_dict(Workload(ops=100, clients=1))
            other = _workload_dict(Workload(ops=100, clients=2))
            history = self._write_history_records(tmp, [
                self._history_record(
                    8_000, 0.12, "2026-11-21T10:00:00+00:00", matching),
                self._history_record(
                    10**9, 0.0001, "2026-11-23T10:00:00+00:00", other),
            ])
            out = os.path.join(tmp, "run.md")
            code = main(self._argv(["--ops", "100", "--clients", "1",
                                    "--output", out,
                                    "--compare-history", "-1",
                                    "--history-file", history]))
            self.assertEqual(code, 0)
            parsed = json.loads(open(os.path.join(tmp, "run.json"),
                                     encoding="utf-8").read())
            comparison = parsed["comparison"]
            self.assertFalse(comparison["workload_matches"])
            self.assertEqual(
                comparison["entries"][0]["baseline_ops_per_sec"],
                10**9)

    def test_compare_same_workload_without_compare_history(self):
        code = main(self._argv(["--ops", "10",
                                "--compare-same-workload"]))
        self.assertEqual(code, 2)

    def test_compare_same_workload_no_matching_run_fails_fast(self):
        # The history holds only a 2-client run; this run uses 1
        # client, so there is no same-workload baseline and the
        # run fails before any benchmark starts.
        with tempfile.TemporaryDirectory() as tmp:
            other = _workload_dict(Workload(ops=100, clients=2))
            history = self._write_history_records(tmp, [
                self._history_record(
                    10**9, 0.0001, "2026-11-23T10:00:00+00:00", other),
            ])
            code = main(self._argv(["--ops", "10",
                                    "--compare-history", "-1",
                                    "--compare-same-workload",
                                    "--history-file", history]))
            self.assertEqual(code, 1)


if __name__ == "__main__":
    unittest.main()
