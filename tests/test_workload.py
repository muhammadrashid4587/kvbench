"""Tests for the workload config file (--workload).

A workload config file (JSON) declares the workload once — op mix,
key count, value size, pipeline depth, sweeps and more — so a run can
be checked in and re-run exactly. A config file and command-line
workload flags conflict (exit 2) instead of silently merging, and
the config path plus the resolved values land in the JSON report and
the history record, so a run is reproducible from its record.
"""

import io
import json
import os
import socketserver
import sys
import tempfile
import threading
import unittest

from kvbench.cli import _workload_conflicts, main
from kvbench.workload import _parse_mix_sweep, load_workload_file


class TestParseMixSweep(unittest.TestCase):
    def test_comma_list_read_heaviest_first(self):
        self.assertEqual(
            _parse_mix_sweep("100:0,90:10,50:50,10:90"),
            [(100, 0), (90, 10), (50, 50), (10, 90)])

    def test_single_ratio(self):
        self.assertEqual(_parse_mix_sweep("80:20"), [(80, 20)])

    def test_sorted_and_deduped(self):
        self.assertEqual(_parse_mix_sweep("50:50,100:0,50:50"),
                         [(100, 0), (50, 50)])
        self.assertEqual(_parse_mix_sweep(" 10:90 , 90:10 "),
                         [(90, 10), (10, 90)])

    def test_write_only_and_read_only(self):
        self.assertEqual(_parse_mix_sweep("0:100"), [(0, 100)])
        self.assertEqual(_parse_mix_sweep("100:0"), [(100, 0)])

    def test_invalid(self):
        for bad in ("", "0:0", "-5:10", "10:-5", "90", "90:10:5",
                    "read-heavy", "90:abc", "90,,10", "90:10,",
                    ",90:10", "90.5:10"):
            with self.assertRaises(ValueError, msg=bad):
                _parse_mix_sweep(bad)


class _FakeRespHandler(socketserver.StreamRequestHandler):
    """Minimal RESP: PING/SET/GET/INCR/DEL against the server's dict."""

    def handle(self):
        store = self.server.store
        while True:
            line = self.rfile.readline()
            if not line.startswith(b"*"):
                return
            try:
                nargs = int(line[1:-2])
            except ValueError:
                return
            args = []
            for _ in range(nargs):
                length_line = self.rfile.readline()
                length = int(length_line[1:-2])
                args.append(self.rfile.read(length + 2)[:-2].decode())
            op = args[0].upper()
            if op == "PING":
                self.wfile.write(b"+PONG\r\n")
            elif op == "SET":
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


def _write_config(tmp, data):
    """Write a config dict (or raw text) to a temp file, return path."""
    path = os.path.join(tmp, "workload.json")
    with open(path, "w", encoding="utf-8") as f:
        if isinstance(data, str):
            f.write(data)
        else:
            json.dump(data, f)
    return path


class TestLoadWorkloadFile(unittest.TestCase):
    def test_empty_object_returns_empty(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = _write_config(tmp, {})
            self.assertEqual(load_workload_file(path), {})

    def test_full_file_normalizes(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = _write_config(tmp, {
                "ops": 20000,
                "clients": 4,
                "keys": 1000,
                "key_prefix": "bench",
                "value_size": 128,
                "mix": "45:40:10:5",
                "pipeline": 5,
                "distribution": "zipfian",
                "zipf_skew": 0.9,
                "hot_keys": 3,
                "seed": 42,
                "timeout": 2.5,
                "warmup": "2s",
                "repeat": 3,
                "sweep_keyspace": [1000, 100, 1000],
                "sweep_clients": [4, 1, 2],
                "sweep_value_size": [128, 32],
                "sweep_pipeline": [10, 1, 5],
                "sweep_mix": ["50:50", "100:0", "50:50"],
            })
            settings = load_workload_file(path)
            self.assertEqual(settings["ops"], 20000)
            self.assertEqual(settings["clients"], 4)
            self.assertEqual(settings["keys"], 1000)
            self.assertEqual(settings["key_prefix"], "bench")
            self.assertEqual(settings["value_size"], 128)
            self.assertEqual(settings["mix"], (45, 40, 10, 5, ""))
            self.assertEqual(settings["pipeline"], 5)
            self.assertEqual(settings["distribution"], "zipfian")
            self.assertAlmostEqual(settings["zipf_skew"], 0.9)
            self.assertEqual(settings["hot_keys"], 3)
            self.assertEqual(settings["seed"], 42)
            self.assertAlmostEqual(settings["timeout_s"], 2.5)
            self.assertEqual(settings["warmup_s"], 2.0)
            self.assertEqual(settings["repeat"], 3)
            # Sweeps are sorted and deduplicated, like the flags.
            self.assertEqual(settings["sweep_keyspace"], [100, 1000])
            self.assertEqual(settings["sweep_clients"], [1, 2, 4])
            self.assertEqual(settings["sweep_value_size"], [32, 128])
            self.assertEqual(settings["sweep_pipeline"], [1, 5, 10])
            # The mix sweep is read-heaviest first, duplicates
            # dropped, like the flag.
            self.assertEqual(settings["sweep_mix"],
                             [(100, 0), (50, 50)])

    def test_mix_preset_and_ratio(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.assertEqual(
                load_workload_file(
                    _write_config(tmp, {"mix": "read-heavy"}))["mix"],
                (15, 75, 5, 5, "read-heavy"))
            self.assertEqual(
                load_workload_file(
                    _write_config(tmp, {"mix": "80:20"}))["mix"],
                (20, 80, 0, 0, ""))

    def test_duration_forms(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.assertEqual(
                load_workload_file(
                    _write_config(tmp, {"duration": "2m"}))["duration_s"],
                120.0)
            self.assertEqual(
                load_workload_file(
                    _write_config(tmp, {"duration": 10}))["duration_s"],
                10.0)
            self.assertEqual(
                load_workload_file(
                    _write_config(tmp, {"duration": "1.5s"}))["duration_s"],
                1.5)

    def test_warmup_zero_number_means_no_warmup(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.assertEqual(
                load_workload_file(
                    _write_config(tmp, {"warmup": 0}))["warmup_s"], 0.0)

    def test_missing_file(self):
        with self.assertRaisesRegex(ValueError, "not found"):
            load_workload_file("/nonexistent/workload.json")

    def test_invalid_json(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = _write_config(tmp, "{not json")
            with self.assertRaisesRegex(ValueError, "not valid JSON"):
                load_workload_file(path)

    def test_non_object(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = _write_config(tmp, "[1, 2]")
            with self.assertRaisesRegex(ValueError, "must be a JSON "
                                                    "object"):
                load_workload_file(path)

    def test_unknown_key(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = _write_config(tmp, {"ops": 100, "bogus": 1})
            with self.assertRaisesRegex(ValueError, "unknown key"):
                load_workload_file(path)

    def test_invalid_values(self):
        bad = [
            {"ops": 0},
            {"ops": -5},
            {"ops": "20000"},
            {"ops": True},
            {"ops": 20000.5},
            {"ops": 100, "duration": "10s"},
            {"duration": "0s"},
            {"duration": -10},
            {"duration": "soon"},
            {"warmup": -1},
            {"clients": 0},
            {"keys": 0},
            {"value_size": 0},
            {"pipeline": 0},
            {"hot_keys": -1},
            {"seed": "1234"},
            {"repeat": 0},
            {"timeout": 0},
            {"timeout": "5"},
            {"zipf_skew": -0.5},
            {"distribution": "random"},
            {"mix": 123},
            {"mix": "not-a-mix"},
            {"mix": "10:20:30:40:50"},
            {"key_prefix": ""},
            {"key_prefix": 7},
            {"sweep_keyspace": "100,1000"},
            {"sweep_keyspace": [100, 0]},
            {"sweep_keyspace": [100, "1000"]},
            {"sweep_clients": [1.5]},
            {"sweep_mix": "100:0,90:10"},
            {"sweep_mix": [100, 90]},
            {"sweep_mix": ["0:0"]},
            {"sweep_mix": ["90:10", 5]},
            {"sweep_mix": ["read-heavy"]},
            {"sweep_mix": ["90"]},
            {"sweep_mix": ["90:10:5"]},
        ]
        with tempfile.TemporaryDirectory() as tmp:
            for data in bad:
                with self.subTest(data=data):
                    with self.assertRaises(ValueError):
                        load_workload_file(_write_config(tmp, data))

    def test_empty_sweep_list_means_no_sweep(self):
        with tempfile.TemporaryDirectory() as tmp:
            settings = load_workload_file(
                _write_config(tmp, {"sweep_keyspace": []}))
            self.assertEqual(settings["sweep_keyspace"], [])
            settings = load_workload_file(
                _write_config(tmp, {"sweep_mix": []}))
            self.assertEqual(settings["sweep_mix"], [])


class TestWorkloadConflicts(unittest.TestCase):
    def test_no_conflict_without_workload(self):
        self.assertEqual(_workload_conflicts(
            ["--ops", "100", "--clients", "2"]), ["--ops", "--clients"])

    def test_flag_and_equals_forms(self):
        self.assertEqual(_workload_conflicts(
            ["--workload", "w.json", "--clients=2", "--mix", "80:20"]),
            ["--clients", "--mix"])

    def test_non_workload_flags_are_not_conflicts(self):
        self.assertEqual(_workload_conflicts(
            ["--workload", "w.json", "--host", "127.0.0.1",
             "--output", "report.md", "--baseline", "--history"]),
            [])

    def test_conflict_exits_2_before_connecting(self):
        # No server is running: a conflict must fail in validation
        # (exit 2), not when the benchmark tries to connect.
        with tempfile.TemporaryDirectory() as tmp:
            config = _write_config(tmp, {"ops": 100})
            for extra in (["--ops", "100"], ["--clients=2"],
                          ["--mix", "80:20"],
                          ["--sweep-keyspace", "100,1000"],
                          ["--sweep-mix", "100:0,50:50"],
                          ["--repeat", "2"], ["--timeout", "1"],
                          ["--duration", "10s"]):
                with self.subTest(extra=extra):
                    stderr = io.StringIO()
                    old_stderr = sys.stderr
                    sys.stderr = stderr
                    try:
                        code = main(["--workload", config] + extra)
                    finally:
                        sys.stderr = old_stderr
                    self.assertEqual(code, 2)
                    self.assertIn("conflicts with", stderr.getvalue())

    def test_missing_workload_file_exits_2(self):
        stderr = io.StringIO()
        old_stderr = sys.stderr
        sys.stderr = stderr
        try:
            code = main(["--workload", "/nonexistent/workload.json"])
        finally:
            sys.stderr = old_stderr
        self.assertEqual(code, 2)
        self.assertIn("not found", stderr.getvalue())

    def test_bad_workload_file_exits_2(self):
        with tempfile.TemporaryDirectory() as tmp:
            config = _write_config(tmp, {"ops": 100, "bogus": 1})
            stderr = io.StringIO()
            old_stderr = sys.stderr
            sys.stderr = stderr
            try:
                code = main(["--workload", config])
            finally:
                sys.stderr = old_stderr
            self.assertEqual(code, 2)
            self.assertIn("unknown key", stderr.getvalue())


class TestWorkloadEndToEnd(unittest.TestCase):
    def test_run_from_config_records_workload(self):
        server = _start_server()
        try:
            with tempfile.TemporaryDirectory() as tmp:
                config = _write_config(tmp, {
                    "ops": 200, "clients": 2, "keys": 100,
                    "mix": "read-heavy", "value_size": 64,
                })
                output = os.path.join(tmp, "report.md")
                history_file = os.path.join(tmp, "history.jsonl")
                code = main([
                    "--host", "127.0.0.1",
                    "--port", str(server.server_address[1]),
                    "--workload", config,
                    "--output", output,
                    "--history", "--history-file", history_file,
                ])
                self.assertEqual(code, 0)
                with open(os.path.join(tmp, "report.json"),
                          encoding="utf-8") as f:
                    doc = json.load(f)
                workload = doc["workload"]
                self.assertEqual(workload["workload_file"], config)
                self.assertEqual(workload["ops"], 200)
                self.assertEqual(workload["clients"], 2)
                self.assertEqual(workload["keys"], 100)
                self.assertEqual(workload["value_size"], 64)
                self.assertEqual(workload["mix"],
                                 {"set": 15, "get": 75, "incr": 5,
                                  "del": 5})
                self.assertEqual(workload["mix_label"], "read-heavy")
                with open(output, encoding="utf-8") as f:
                    text = f.read()
                self.assertIn(f"Workload file: `{config}`", text)
                with open(history_file, encoding="utf-8") as f:
                    record = json.loads(f.readline())
                self.assertEqual(record["workload"]["workload_file"],
                                 config)
                self.assertEqual(record["workload"]["ops"], 200)
                self.assertEqual(record["workload"]["clients"], 2)
        finally:
            server.shutdown()
            server.server_close()

    def test_config_with_defaults_for_absent_keys(self):
        # Keys the file does not set keep the command-line defaults.
        server = _start_server()
        try:
            with tempfile.TemporaryDirectory() as tmp:
                config = _write_config(tmp, {"ops": 100})
                output = os.path.join(tmp, "report.md")
                code = main([
                    "--host", "127.0.0.1",
                    "--port", str(server.server_address[1]),
                    "--workload", config, "--output", output,
                ])
                self.assertEqual(code, 0)
                with open(os.path.join(tmp, "report.json"),
                          encoding="utf-8") as f:
                    doc = json.load(f)
                workload = doc["workload"]
                self.assertEqual(workload["ops"], 100)
                self.assertEqual(workload["clients"], 4)
                self.assertEqual(workload["keys"], 1000)
                self.assertEqual(workload["value_size"], 32)
                self.assertEqual(workload["mix"],
                                 {"set": 45, "get": 40, "incr": 10,
                                  "del": 5})
        finally:
            server.shutdown()
            server.server_close()

    def test_duration_config_run(self):
        server = _start_server()
        try:
            with tempfile.TemporaryDirectory() as tmp:
                config = _write_config(tmp, {
                    "duration": "1s", "clients": 1,
                })
                output = os.path.join(tmp, "report.md")
                code = main([
                    "--host", "127.0.0.1",
                    "--port", str(server.server_address[1]),
                    "--workload", config, "--output", output,
                ])
                self.assertEqual(code, 0)
                with open(os.path.join(tmp, "report.json"),
                          encoding="utf-8") as f:
                    doc = json.load(f)
                self.assertIsNone(doc["workload"]["ops"])
                self.assertEqual(doc["workload"]["duration_s"], 1.0)
        finally:
            server.shutdown()
            server.server_close()

    def test_sweep_config_run(self):
        server = _start_server()
        try:
            with tempfile.TemporaryDirectory() as tmp:
                config = _write_config(tmp, {
                    "ops": 100, "clients": 1,
                    "sweep_keyspace": [100, 50],
                })
                output = os.path.join(tmp, "report.md")
                code = main([
                    "--host", "127.0.0.1",
                    "--port", str(server.server_address[1]),
                    "--workload", config, "--output", output,
                ])
                self.assertEqual(code, 0)
                with open(os.path.join(tmp, "report.json"),
                          encoding="utf-8") as f:
                    doc = json.load(f)
                self.assertEqual(
                    doc["workload"]["sweep_keyspace"], [50, 100])
                self.assertEqual(
                    [s["keys"] for s in doc["results"][0]["keyspace_sweep"]],
                    [50, 100])
        finally:
            server.shutdown()
            server.server_close()

    def test_non_workload_flags_combine(self):
        # --baseline is a target flag, not a workload flag.
        server = _start_server()
        try:
            with tempfile.TemporaryDirectory() as tmp:
                config = _write_config(tmp, {"ops": 100, "clients": 1})
                output = os.path.join(tmp, "report.md")
                code = main([
                    "--host", "127.0.0.1",
                    "--port", str(server.server_address[1]),
                    "--workload", config, "--baseline",
                    "--output", output,
                ])
                self.assertEqual(code, 0)
                with open(output, encoding="utf-8") as f:
                    self.assertIn("baseline (dict)", f.read())
        finally:
            server.shutdown()
            server.server_close()

    def test_config_run_matches_cli_run_for_same_workload(self):
        # A config run and a CLI run with the same resolved values
        # are the same workload: --compare-same-workload finds the
        # recorded config run as the baseline for the CLI run.
        server = _start_server()
        try:
            with tempfile.TemporaryDirectory() as tmp:
                config = _write_config(tmp, {
                    "ops": 100, "clients": 1, "keys": 100,
                })
                history_file = os.path.join(tmp, "history.jsonl")
                code = main([
                    "--host", "127.0.0.1",
                    "--port", str(server.server_address[1]),
                    "--workload", config,
                    "--output", os.path.join(tmp, "report.md"),
                    "--history", "--history-file", history_file,
                ])
                self.assertEqual(code, 0)
                code = main([
                    "--host", "127.0.0.1",
                    "--port", str(server.server_address[1]),
                    "--ops", "100", "--clients", "1", "--keys", "100",
                    "--output", os.path.join(tmp, "report2.md"),
                    "--compare-history", "-1",
                    "--compare-same-workload",
                    "--history-file", history_file,
                ])
                self.assertEqual(code, 0)
                with open(os.path.join(tmp, "report2.json"),
                          encoding="utf-8") as f:
                    doc = json.load(f)
                self.assertTrue(doc["comparison"]["workload_matches"])
                self.assertEqual(
                    doc["comparison"]["workload_differences"], [])
        finally:
            server.shutdown()
            server.server_close()


if __name__ == "__main__":
    unittest.main()
