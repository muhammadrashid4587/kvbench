# kvbench

A benchmark CLI for Redis-compatible key-value stores — built to measure
[cinder](https://github.com/muhammadrashid4587/cinder), the Redis clone in
C. Pure Python 3 standard library, no dependencies.

## What it is

- A small RESP client (`kvbench/resp.py`) that talks to any
  Redis-compatible server over a socket.
- A workload runner (`kvbench/bench.py`) that runs a configurable
  SET/GET/INCR mix across N threads, measuring per-op latency.
- Latency stats (`kvbench/stats.py`): ops/sec, mean, p50, p95, p99.
- A Markdown report writer (`kvbench/report.py`).
- A baseline mode that runs the exact same workload against an
  in-process Python dict, so the report can compare the server with a
  no-network upper bound on the same machine.

## What it is not

- Not a Redis benchmark replacement (redis-benchmark exists). This is a
  small, readable tool for measuring cinder during development.
- Not a correctness tester for Redis semantics. It checks that commands
  succeed, not that every edge case matches Redis.
- Not a load generator for production. It opens one connection per
  thread and does one round trip per operation — no pipelining.

## Usage

Start cinder (or any Redis-compatible server) first:

```sh
# in the cinder repo
./cinder --port 6379
```

Then, from this repo:

```sh
# 20k ops, 4 clients, default 50:40:10 SET:GET:INCR mix
python -m kvbench.cli --host 127.0.0.1 --port 6379 --ops 20000 --clients 4

# same run plus the in-process dict baseline, for comparison
python -m kvbench.cli --host 127.0.0.1 --port 6379 --ops 20000 --clients 4 \
    --baseline --output report.md

# different mix and value size
python -m kvbench.cli --ops 50000 --clients 8 --mix 80:15:5 --value-size 128
```

Options: `--host` (default 127.0.0.1), `--port` (default 6379),
`--ops` (default 10000), `--clients` (default 4), `--keys` (default
1000), `--value-size` (default 32), `--mix SET:GET:INCR` (default
50:40:10), `--seed` (default 1234), `--baseline`, `--output` (default
report.md).

## Report format

`report.md` contains the workload settings and one row per backend:

| Backend | Ops | Ops/sec | Mean (ms) | p50 (ms) | p95 (ms) | p99 (ms) | Errors |

The baseline row is labelled `baseline (dict)`.

## Reproducing numbers

Numbers are machine-dependent. To reproduce a report, run the exact
command printed in the report (`Command:` line) on the same machine,
with the same server build and the same server flags. The report also
records the Python version and platform. The baseline is an in-process
dict — no network, no persistence, no expiry — so treat it as an upper
bound on what the workload can do on this machine, not as a fair
head-to-head with a real server.

## Tests

```sh
make test
# or: python3 -m unittest discover -s tests -v
```

## Roadmap

- Pipelined mode (N commands per round trip).
- Percentile histograms in the report.
- JSON output alongside Markdown.
- Latency over time (per-second buckets) to spot stalls.
