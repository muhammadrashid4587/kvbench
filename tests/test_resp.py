"""Tests for kvbench.resp against a tiny fake RESP server."""

import socketserver
import threading
import unittest

from kvbench.resp import RespClient, RespError


class FakeHandler(socketserver.BaseRequestHandler):
    """Speaks just enough RESP for the client tests."""

    def handle(self):
        store = {}
        rfile = self.request.makefile("rb")
        while True:
            line = rfile.readline()
            if not line:
                return
            if not line.startswith(b"*"):
                return
            count = int(line[1:-2])
            args = []
            for _ in range(count):
                header = rfile.readline()
                length = int(header[1:-2])
                args.append(rfile.read(length + 2)[:-2].decode())
            cmd = args[0].upper()
            if cmd == "PING":
                reply = b"+PONG\r\n"
            elif cmd == "SET":
                store[args[1]] = args[2]
                reply = b"+OK\r\n"
            elif cmd == "GET":
                value = store.get(args[1])
                if value is None:
                    reply = b"$-1\r\n"
                else:
                    raw = value.encode()
                    reply = b"$%d\r\n%s\r\n" % (len(raw), raw)
            elif cmd == "INCR":
                value = int(store.get(args[1], "0")) + 1
                store[args[1]] = str(value)
                reply = b":%d\r\n" % value
            elif cmd == "BOOM":
                reply = b"-ERR boom\r\n"
            else:
                reply = b"-ERR unknown command\r\n"
            self.request.sendall(reply)


class FakeServer(socketserver.ThreadingTCPServer):
    daemon_threads = True
    allow_reuse_address = True


class TestRespClient(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = FakeServer(("127.0.0.1", 0), FakeHandler)
        cls.port = cls.server.server_address[1]
        cls.thread = threading.Thread(target=cls.server.serve_forever,
                                      daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()

    def test_ping(self):
        with RespClient("127.0.0.1", self.port) as client:
            self.assertEqual(client.command("PING"), "PONG")

    def test_set_get_roundtrip(self):
        with RespClient("127.0.0.1", self.port) as client:
            self.assertEqual(client.command("SET", "k", "v"), "OK")
            self.assertEqual(client.command("GET", "k"), "v")
            self.assertIsNone(client.command("GET", "missing"))

    def test_incr(self):
        with RespClient("127.0.0.1", self.port) as client:
            self.assertEqual(client.command("INCR", "counter"), 1)
            self.assertEqual(client.command("INCR", "counter"), 2)

    def test_error_raises(self):
        with RespClient("127.0.0.1", self.port) as client:
            with self.assertRaises(RespError):
                client.command("BOOM")


if __name__ == "__main__":
    unittest.main()
