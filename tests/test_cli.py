"""Tests for kvbench.cli --compare mode.

A tiny in-process RESP server stands in for a second Redis-compatible
server, so ``main()`` runs the same workload against two targets and
both rows land in the report.
"""

import os
import socketserver
import tempfile
import threading
import unittest

from kvbench.cli import MIX_PRESETS, _parse_host_port, _parse_mix, \
    build_parser, main


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


if __name__ == "__main__":
    unittest.main()
