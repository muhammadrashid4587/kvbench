"""Minimal RESP client (Redis Serialization Protocol).

Speaks just enough RESP to benchmark a Redis-compatible server such as
cinder: commands are sent as arrays of bulk strings, and replies are
parsed into Python values (str, int, list, None).

No dependencies beyond the standard library.
"""

import socket


class RespError(Exception):
    """Raised when the server replies with an error (-ERR ...) line."""


def _encode_command(*args):
    parts = [f"*{len(args)}\r\n".encode()]
    for arg in args:
        raw = arg if isinstance(arg, bytes) else str(arg).encode()
        parts.append(f"${len(raw)}\r\n".encode())
        parts.append(raw)
        parts.append(b"\r\n")
    return b"".join(parts)


class RespClient:
    """A single RESP connection. One client per thread; not thread-safe."""

    def __init__(self, host="127.0.0.1", port=6379, timeout=5.0):
        self.host = host
        self.port = port
        self._sock = socket.create_connection((host, port), timeout=timeout)
        self._sock.settimeout(timeout)
        self._rfile = self._sock.makefile("rb")

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def close(self):
        try:
            self._rfile.close()
        finally:
            self._sock.close()

    def command(self, *args):
        """Send a command and return the parsed reply."""
        self._sock.sendall(_encode_command(*args))
        return self._read_reply()

    def pipeline(self, commands):
        """Send several commands at once and read all replies in order.

        ``commands`` is an iterable of argument tuples. Returns one
        entry per command: the parsed reply, or the RespError for that
        reply. Errors are returned, not raised, so the reply stream
        stays aligned and the remaining replies still arrive. A
        transport failure (e.g. the connection dropping) still raises.
        """
        commands = list(commands)
        if not commands:
            return []
        self._sock.sendall(
            b"".join(_encode_command(*cmd) for cmd in commands)
        )
        replies = []
        for _ in commands:
            try:
                replies.append(self._read_reply())
            except RespError as exc:
                replies.append(exc)
        return replies

    def _read_line(self):
        line = self._rfile.readline()
        if not line:
            raise ConnectionError("connection closed by server")
        return line

    def _read_reply(self):
        line = self._read_line()
        kind = line[:1]
        payload = line[1:-2]  # strip type byte and trailing CRLF
        if kind == b"+":  # simple string
            return payload.decode()
        if kind == b"-":  # error
            raise RespError(payload.decode())
        if kind == b":":  # integer
            return int(payload)
        if kind == b"$":  # bulk string
            length = int(payload)
            if length == -1:
                return None
            data = self._rfile.read(length + 2)[:-2]
            return data.decode()
        if kind == b"*":  # array
            count = int(payload)
            if count == -1:
                return None
            return [self._read_reply() for _ in range(count)]
        raise ValueError(f"unknown RESP reply type: {kind!r}")
