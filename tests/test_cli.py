"""Tests for kvbench.cli --compare mode.

A tiny in-process RESP server stands in for a second Redis-compatible
server, so ``main()`` runs the same workload against two targets and
both rows land in the report.
"""

import csv
import json
import os
import socketserver
import tempfile
import threading
import unittest

from kvbench.cli import MIX_PRESETS, _parse_client_sweep, _parse_duration, \
    _parse_host_port, _parse_keyspace_sweep, _parse_mix, \
    _parse_value_size_sweep, build_parser, main


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


class TestParseMix(unittest.TestCase):
    def test_three_part_weights(self):
        self.assertEqual(_parse_mix("50:40:10"), (50, 40, 10, ""))
        self.assertEqual(_parse_mix("80:15:5"), (80, 15, 5, ""))

    def test_two_part_read_write_ratio(self):
        # READ:WRITE — reads are GETs, writes are SETs, no INCRs.
        self.assertEqual(_parse_mix("80:20"), (20, 80, 0, ""))
        self.assertEqual(_parse_mix("0:100"), (100, 0, 0, ""))

    def test_presets(self):
        for name, weights in MIX_PRESETS.items():
            self.assertEqual(_parse_mix(name), (*weights, name))
        self.assertEqual(_parse_mix("read-heavy"), (15, 80, 5,
                                                    "read-heavy"))
        self.assertEqual(_parse_mix("balanced"), (45, 45, 10, "balanced"))
        self.assertEqual(_parse_mix("write-heavy"), (80, 15, 5,
                                                     "write-heavy"))

    def test_invalid(self):
        for bad in ("", "80", "50:40:10:0", "a:b:c", "50:-1:10",
                    "0:0:0", "readheavy", "READ-HEAVY"):
            with self.assertRaises(ValueError, msg=bad):
                _parse_mix(bad)


class TestParseDuration(unittest.TestCase):
    def test_bare_seconds(self):
        self.assertEqual(_parse_duration("10"), 10.0)
        self.assertEqual(_parse_duration("0.5"), 0.5)

    def test_seconds_suffix(self):
        self.assertEqual(_parse_duration("10s"), 10.0)
        self.assertEqual(_parse_duration("1.5s"), 1.5)

    def test_minutes_suffix(self):
        self.assertEqual(_parse_duration("2m"), 120.0)
        self.assertEqual(_parse_duration("0.5m"), 30.0)
        self.assertEqual(_parse_duration("1.5m"), 90.0)

    def test_invalid(self):
        for bad in ("", "0", "0s", "0m", "-5s", "-1m", "10x", "s", "m",
                    "abc", "10ss", "1h"):
            with self.assertRaises(ValueError, msg=bad):
                _parse_duration(bad)


class TestParseHostPort(unittest.TestCase):
    def test_valid(self):
        self.assertEqual(_parse_host_port("127.0.0.1:6380"),
                         ("127.0.0.1", 6380))
        self.assertEqual(_parse_host_port("localhost:6379"),
                         ("localhost", 6379))

    def test_invalid(self):
        for bad in ("127.0.0.1", "127.0.0.1:", ":6379", "host:notaport",
                    "host:0", "host:65536", "host:-1"):
            with self.assertRaises(ValueError, msg=bad):
                _parse_host_port(bad)


class TestCompareFlag(unittest.TestCase):
    def test_compare_repeatable(self):
        args = build_parser().parse_args(
            ["--compare", "127.0.0.1:6380",
             "--compare", "127.0.0.1:6381"])
        self.assertEqual(args.compare,
                         ["127.0.0.1:6380", "127.0.0.1:6381"])

    def test_compare_defaults_empty(self):
        args = build_parser().parse_args([])
        self.assertEqual(args.compare, [])


class TestDistributionFlag(unittest.TestCase):
    def test_defaults_are_uniform(self):
        args = build_parser().parse_args([])
        self.assertEqual(args.distribution, "uniform")
        self.assertEqual(args.zipf_skew, 1.0)

    def test_zipfian_with_skew(self):
        args = build_parser().parse_args(
            ["--distribution", "zipfian", "--zipf-skew", "1.5"])
        self.assertEqual(args.distribution, "zipfian")
        self.assertEqual(args.zipf_skew, 1.5)

    def test_bad_distribution_rejected(self):
        with self.assertRaises(SystemExit):
            build_parser().parse_args(["--distribution", "normal"])


class TestCompareRun(unittest.TestCase):
    def test_two_servers_in_one_report(self):
        server_a = _start_server()
        server_b = _start_server()
        try:
            with tempfile.TemporaryDirectory() as tmp:
                output = os.path.join(tmp, "report.md")
                code = main([
                    "--host", "127.0.0.1",
                    "--port", str(server_a.server_address[1]),
                    "--compare",
                    f"127.0.0.1:{server_b.server_address[1]}",
                    "--ops", "200", "--clients", "2",
                    "--output", output,
                ])
                self.assertEqual(code, 0)
                with open(output, encoding="utf-8") as f:
                    text = f.read()
                self.assertIn(
                    f"cinder (127.0.0.1:{server_a.server_address[1]})",
                    text)
                self.assertIn(
                    f"compare (127.0.0.1:{server_b.server_address[1]})",
                    text)
                with open(os.path.join(tmp, "report.json"),
                          encoding="utf-8") as f:
                    json_text = f.read()
                self.assertIn("compare (127.0.0.1:", json_text)
        finally:
            server_a.shutdown()
            server_a.server_close()
            server_b.shutdown()
            server_b.server_close()

    def test_bad_compare_target_rejected(self):
        code = main(["--compare", "notaport", "--ops", "10"])
        self.assertEqual(code, 2)

    def test_unreachable_compare_target_fails(self):
        server_a = _start_server()
        try:
            with tempfile.TemporaryDirectory() as tmp:
                code = main([
                    "--host", "127.0.0.1",
                    "--port", str(server_a.server_address[1]),
                    "--compare", "127.0.0.1:1",
                    "--ops", "10", "--clients", "1",
                    "--output", os.path.join(tmp, "report.md"),
                ])
                self.assertEqual(code, 1)
        finally:
            server_a.shutdown()
            server_a.server_close()


class TestDurationRun(unittest.TestCase):
    def test_duration_run_reports_duration_and_throughput(self):
        server = _start_server()
        try:
            with tempfile.TemporaryDirectory() as tmp:
                output = os.path.join(tmp, "report.md")
                code = main([
                    "--host", "127.0.0.1",
                    "--port", str(server.server_address[1]),
                    "--duration", "0.5s", "--clients", "2",
                    "--output", output,
                ])
                self.assertEqual(code, 0)
                with open(output, encoding="utf-8") as f:
                    text = f.read()
                self.assertIn("Duration: 0.5s", text)
                self.assertNotIn("Total ops:", text)
                # The results table still carries the completed ops
                # and the throughput over the elapsed time.
                self.assertIn("| Backend | Ops | Ops/sec |", text)
                with open(os.path.join(tmp, "report.json"),
                          encoding="utf-8") as f:
                    parsed = json.load(f)
                self.assertEqual(parsed["workload"]["duration_s"], 0.5)
                self.assertIsNone(parsed["workload"]["ops"])
                row = parsed["results"][0]
                self.assertGreater(row["ops"], 0)
                self.assertGreater(row["ops_per_sec"], 0)
                self.assertGreaterEqual(row["elapsed_s"], 0.4)
                self.assertLess(row["elapsed_s"], 3.0)
        finally:
            server.shutdown()
            server.server_close()

    def test_ops_and_duration_conflict(self):
        code = main(["--ops", "100", "--duration", "1s"])
        self.assertEqual(code, 2)

    def test_bad_duration_rejected(self):
        for bad in ("bogus", "0s", "1h"):
            code = main(["--duration", bad])
            self.assertEqual(code, 2, msg=bad)
        # A leading dash would be read as an option flag, so pass it
        # with "="; the parser must still reject the value itself.
        self.assertEqual(main(["--duration=-5s"]), 2)


class TestParseKeyspaceSweep(unittest.TestCase):
    def test_comma_list(self):
        self.assertEqual(_parse_keyspace_sweep("100,1000,10000,100000"),
                         [100, 1000, 10000, 100000])

    def test_single_size(self):
        self.assertEqual(_parse_keyspace_sweep("100"), [100])

    def test_sorted_and_deduped(self):
        self.assertEqual(_parse_keyspace_sweep("1000,100,1000"),
                         [100, 1000])
        self.assertEqual(_parse_keyspace_sweep(" 100 , 200 "),
                         [100, 200])

    def test_invalid(self):
        for bad in ("", "0", "-5", "10,abc", "10,,20", "10.5", "10,0",
                    ",100"):
            with self.assertRaises(ValueError, msg=bad):
                _parse_keyspace_sweep(bad)


class TestSweepFlag(unittest.TestCase):
    def test_defaults_no_sweep(self):
        args = build_parser().parse_args([])
        self.assertIsNone(args.sweep_keyspace)

    def test_sweep_value_kept(self):
        args = build_parser().parse_args(
            ["--sweep-keyspace", "100,1000"])
        self.assertEqual(args.sweep_keyspace, "100,1000")


class TestSweepRun(unittest.TestCase):
    def test_sweep_run_reports_per_size(self):
        server = _start_server()
        try:
            with tempfile.TemporaryDirectory() as tmp:
                output = os.path.join(tmp, "report.md")
                code = main([
                    "--host", "127.0.0.1",
                    "--port", str(server.server_address[1]),
                    "--ops", "200", "--clients", "2",
                    "--sweep-keyspace", "10,100",
                    "--output", output,
                ])
                self.assertEqual(code, 0)
                with open(output, encoding="utf-8") as f:
                    text = f.read()
                self.assertIn("Keyspace sweep: 10, 100 keys", text)
                self.assertIn("## Keyspace sweep", text)
                self.assertIn("| 10 | 200 |", text)
                self.assertIn("| 100 | 200 |", text)
                self.assertIn("how keyspace size affects throughput", text)
                with open(os.path.join(tmp, "report.json"),
                          encoding="utf-8") as f:
                    parsed = json.load(f)
                self.assertEqual(parsed["workload"]["sweep_keyspace"],
                                 [10, 100])
                sweep = parsed["results"][0]["keyspace_sweep"]
                self.assertEqual(len(sweep), 2)
                self.assertEqual(sweep[0]["keys"], 10)
                self.assertEqual(sweep[0]["ops"], 200)
                self.assertGreater(sweep[0]["ops_per_sec"], 0)
                self.assertGreaterEqual(sweep[0]["p99_ms"], 0)
                self.assertEqual(sweep[1]["keys"], 100)
        finally:
            server.shutdown()
            server.server_close()

    def test_bad_sweep_rejected(self):
        for bad in ("bogus", "0", "10,abc", "10,,20"):
            code = main(["--sweep-keyspace", bad, "--ops", "10"])
            self.assertEqual(code, 2, msg=bad)


class TestParseClientSweep(unittest.TestCase):
    def test_comma_list(self):
        self.assertEqual(_parse_client_sweep("1,2,4,8"), [1, 2, 4, 8])

    def test_single_count(self):
        self.assertEqual(_parse_client_sweep("4"), [4])

    def test_sorted_and_deduped(self):
        self.assertEqual(_parse_client_sweep("4,1,4"), [1, 4])
        self.assertEqual(_parse_client_sweep(" 2 , 8 "), [2, 8])

    def test_invalid(self):
        for bad in ("", "0", "-2", "1,abc", "1,,2", "1.5", "2,0", ",4"):
            with self.assertRaises(ValueError, msg=bad):
                _parse_client_sweep(bad)


class TestClientSweepFlag(unittest.TestCase):
    def test_defaults_no_sweep(self):
        args = build_parser().parse_args([])
        self.assertIsNone(args.sweep_clients)

    def test_sweep_value_kept(self):
        args = build_parser().parse_args(
            ["--sweep-clients", "1,2,4"])
        self.assertEqual(args.sweep_clients, "1,2,4")


class TestClientSweepRun(unittest.TestCase):
    def test_client_sweep_run_reports_per_count(self):
        server = _start_server()
        try:
            with tempfile.TemporaryDirectory() as tmp:
                output = os.path.join(tmp, "report.md")
                code = main([
                    "--host", "127.0.0.1",
                    "--port", str(server.server_address[1]),
                    "--ops", "200", "--clients", "2",
                    "--sweep-clients", "1,4",
                    "--output", output,
                ])
                self.assertEqual(code, 0)
                with open(output, encoding="utf-8") as f:
                    text = f.read()
                self.assertIn("Client sweep: 1, 4 clients", text)
                self.assertIn("## Client sweep", text)
                self.assertIn("| 1 | 200 |", text)
                self.assertIn("| 4 | 200 |", text)
                self.assertIn("how concurrency affects throughput", text)
                with open(os.path.join(tmp, "report.json"),
                          encoding="utf-8") as f:
                    parsed = json.load(f)
                self.assertEqual(parsed["workload"]["sweep_clients"],
                                 [1, 4])
                sweep = parsed["results"][0]["client_sweep"]
                self.assertEqual(len(sweep), 2)
                self.assertEqual(sweep[0]["clients"], 1)
                self.assertEqual(sweep[0]["ops"], 200)
                self.assertGreater(sweep[0]["ops_per_sec"], 0)
                self.assertGreaterEqual(sweep[0]["p99_ms"], 0)
                self.assertEqual(sweep[1]["clients"], 4)
        finally:
            server.shutdown()
            server.server_close()

    def test_bad_client_sweep_rejected(self):
        for bad in ("bogus", "0", "1,abc", "1,,2"):
            code = main(["--sweep-clients", bad, "--ops", "10"])
            self.assertEqual(code, 2, msg=bad)


class TestParseValueSizeSweep(unittest.TestCase):
    def test_comma_list(self):
        self.assertEqual(_parse_value_size_sweep("32,128,1024"),
                         [32, 128, 1024])

    def test_single_size(self):
        self.assertEqual(_parse_value_size_sweep("64"), [64])

    def test_sorted_and_deduped(self):
        self.assertEqual(_parse_value_size_sweep("128,32,128"),
                         [32, 128])
        self.assertEqual(_parse_value_size_sweep(" 64 , 256 "),
                         [64, 256])

    def test_invalid(self):
        for bad in ("", "0", "-8", "32,abc", "32,,128", "32.5",
                    "32,0", ",64"):
            with self.assertRaises(ValueError, msg=bad):
                _parse_value_size_sweep(bad)


class TestValueSizeSweepFlag(unittest.TestCase):
    def test_defaults_no_sweep(self):
        args = build_parser().parse_args([])
        self.assertIsNone(args.sweep_value_size)

    def test_sweep_value_kept(self):
        args = build_parser().parse_args(
            ["--sweep-value-size", "32,128"])
        self.assertEqual(args.sweep_value_size, "32,128")


class TestValueSizeSweepRun(unittest.TestCase):
    def test_value_size_sweep_run_reports_per_size(self):
        server = _start_server()
        try:
            with tempfile.TemporaryDirectory() as tmp:
                output = os.path.join(tmp, "report.md")
                code = main([
                    "--host", "127.0.0.1",
                    "--port", str(server.server_address[1]),
                    "--ops", "200", "--clients", "2",
                    "--sweep-value-size", "8,64",
                    "--output", output,
                ])
                self.assertEqual(code, 0)
                with open(output, encoding="utf-8") as f:
                    text = f.read()
                self.assertIn("Value size sweep: 8, 64 bytes", text)
                self.assertIn("## Value size sweep", text)
                self.assertIn("| 8 | 200 |", text)
                self.assertIn("| 64 | 200 |", text)
                self.assertIn("how value size affects throughput", text)
                with open(os.path.join(tmp, "report.json"),
                          encoding="utf-8") as f:
                    parsed = json.load(f)
                self.assertEqual(parsed["workload"]["sweep_value_size"],
                                 [8, 64])
                sweep = parsed["results"][0]["value_size_sweep"]
                self.assertEqual(len(sweep), 2)
                self.assertEqual(sweep[0]["value_size"], 8)
                self.assertEqual(sweep[0]["ops"], 200)
                self.assertGreater(sweep[0]["ops_per_sec"], 0)
                self.assertGreaterEqual(sweep[0]["p99_ms"], 0)
                self.assertEqual(sweep[1]["value_size"], 64)
        finally:
            server.shutdown()
            server.server_close()

    def test_bad_value_size_sweep_rejected(self):
        for bad in ("bogus", "0", "32,abc", "32,,128"):
            code = main(["--sweep-value-size", bad, "--ops", "10"])
            self.assertEqual(code, 2, msg=bad)


class TestWarmupFlag(unittest.TestCase):
    def test_defaults_no_warmup(self):
        args = build_parser().parse_args([])
        self.assertIsNone(args.warmup)

    def test_warmup_value_kept(self):
        args = build_parser().parse_args(["--warmup", "2s"])
        self.assertEqual(args.warmup, "2s")


class TestWarmupRun(unittest.TestCase):
    def test_warmup_run_reports_warmup(self):
        server = _start_server()
        try:
            with tempfile.TemporaryDirectory() as tmp:
                output = os.path.join(tmp, "report.md")
                code = main([
                    "--host", "127.0.0.1",
                    "--port", str(server.server_address[1]),
                    "--ops", "200", "--clients", "2",
                    "--warmup", "0.2s",
                    "--output", output,
                ])
                self.assertEqual(code, 0)
                with open(output, encoding="utf-8") as f:
                    text = f.read()
                self.assertIn("Warmup: 0.2s", text)
                self.assertIn("Warmup ops on cinder", text)
                with open(os.path.join(tmp, "report.json"),
                          encoding="utf-8") as f:
                    parsed = json.load(f)
                self.assertEqual(parsed["workload"]["warmup_s"], 0.2)
                row = parsed["results"][0]
                self.assertEqual(row["ops"], 200)
                self.assertGreater(row["warmup_ops"], 0)
        finally:
            server.shutdown()
            server.server_close()

    def test_bad_warmup_rejected(self):
        for bad in ("bogus", "0s", "1h"):
            code = main(["--warmup", bad, "--ops", "10"])
            self.assertEqual(code, 2, msg=bad)
        # A leading dash would be read as an option flag, so pass it
        # with "="; the parser must still reject the value itself.
        self.assertEqual(main(["--warmup=-5s", "--ops", "10"]), 2)


class TestTimeoutFlag(unittest.TestCase):
    def test_defaults_five_seconds(self):
        args = build_parser().parse_args([])
        self.assertEqual(args.timeout, 5.0)

    def test_timeout_value_kept(self):
        args = build_parser().parse_args(["--timeout", "10"])
        self.assertEqual(args.timeout, 10.0)


class TestTimeoutRun(unittest.TestCase):
    def test_timeout_run_records_timeout(self):
        server = _start_server()
        try:
            with tempfile.TemporaryDirectory() as tmp:
                output = os.path.join(tmp, "report.md")
                code = main([
                    "--host", "127.0.0.1",
                    "--port", str(server.server_address[1]),
                    "--ops", "200", "--clients", "2",
                    "--timeout", "10",
                    "--output", output,
                ])
                self.assertEqual(code, 0)
                with open(output, encoding="utf-8") as f:
                    text = f.read()
                self.assertIn("Socket timeout: 10 s", text)
                with open(os.path.join(tmp, "report.json"),
                          encoding="utf-8") as f:
                    parsed = json.load(f)
                self.assertEqual(parsed["workload"]["timeout_s"], 10.0)
        finally:
            server.shutdown()
            server.server_close()

    def test_bad_timeout_rejected(self):
        for bad in ("0", "-1"):
            code = main(["--timeout", bad, "--ops", "10"])
            self.assertEqual(code, 2, msg=bad)
        # A leading dash would be read as an option flag, so pass it
        # with "="; the parser must still reject the value itself.
        self.assertEqual(main(["--timeout=-5", "--ops", "10"]), 2)


class TestKeyPrefixFlag(unittest.TestCase):
    def test_default_prefix_is_kvbench(self):
        args = build_parser().parse_args([])
        self.assertEqual(args.key_prefix, "kvbench")

    def test_key_prefix_value_kept(self):
        args = build_parser().parse_args(["--key-prefix", "nightly"])
        self.assertEqual(args.key_prefix, "nightly")


class TestKeyPrefixRun(unittest.TestCase):
    def test_prefix_run_records_prefix_and_prefixed_keys(self):
        server = _start_server()
        try:
            with tempfile.TemporaryDirectory() as tmp:
                output = os.path.join(tmp, "report.md")
                code = main([
                    "--host", "127.0.0.1",
                    "--port", str(server.server_address[1]),
                    "--ops", "200", "--clients", "2",
                    "--key-prefix", "nightly",
                    "--output", output,
                ])
                self.assertEqual(code, 0)
                with open(output, encoding="utf-8") as f:
                    text = f.read()
                self.assertIn("Key prefix: nightly", text)
                with open(os.path.join(tmp, "report.json"),
                          encoding="utf-8") as f:
                    parsed = json.load(f)
                self.assertEqual(parsed["workload"]["key_prefix"],
                                 "nightly")
                # The run touched only prefixed keys on the server.
                self.assertTrue(server.store)
                self.assertTrue(all(
                    name.startswith("nightly:")
                    for name in server.store))
                hot = parsed["results"][0]["hot_keys"]
                self.assertTrue(hot)
                self.assertTrue(all(
                    entry["key"].startswith("nightly:")
                    for entry in hot))
        finally:
            server.shutdown()
            server.server_close()

    def test_empty_prefix_rejected(self):
        self.assertEqual(
            main(["--key-prefix", "", "--ops", "10"]), 2)


class TestCsvFlag(unittest.TestCase):
    def test_defaults_no_csv(self):
        args = build_parser().parse_args([])
        self.assertIsNone(args.csv)

    def test_csv_value_kept(self):
        args = build_parser().parse_args(["--csv", "timeseries.csv"])
        self.assertEqual(args.csv, "timeseries.csv")


class TestCsvRun(unittest.TestCase):
    def test_csv_written_with_throughput_rows(self):
        server = _start_server()
        try:
            with tempfile.TemporaryDirectory() as tmp:
                output = os.path.join(tmp, "report.md")
                csv_path = os.path.join(tmp, "timeseries.csv")
                code = main([
                    "--host", "127.0.0.1",
                    "--port", str(server.server_address[1]),
                    "--ops", "200", "--clients", "2",
                    "--output", output, "--csv", csv_path,
                ])
                self.assertEqual(code, 0)
                with open(csv_path, encoding="utf-8", newline="") as f:
                    rows = list(csv.DictReader(f))
                self.assertGreater(len(rows), 0)
                self.assertEqual(rows[0]["backend"],
                                 f"cinder (127.0.0.1:"
                                 f"{server.server_address[1]})")
                self.assertEqual(rows[0]["second"], "0")
                # One row per second, so ops_per_sec is the ops in
                # that second, and the rows cover all 200 ops.
                self.assertEqual(rows[0]["ops_per_sec"],
                                 rows[0]["ops"])
                self.assertEqual(sum(int(r["ops"]) for r in rows), 200)
                self.assertGreater(float(rows[0]["mean_ms"]), 0)
        finally:
            server.shutdown()
            server.server_close()

    def test_no_csv_without_flag(self):
        server = _start_server()
        try:
            with tempfile.TemporaryDirectory() as tmp:
                output = os.path.join(tmp, "report.md")
                code = main([
                    "--host", "127.0.0.1",
                    "--port", str(server.server_address[1]),
                    "--ops", "200", "--clients", "2",
                    "--output", output,
                ])
                self.assertEqual(code, 0)
                self.assertFalse(os.path.exists(
                    os.path.join(tmp, "timeseries.csv")))
        finally:
            server.shutdown()
            server.server_close()


if __name__ == "__main__":
    unittest.main()
