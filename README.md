# kvbench

A benchmark CLI for Redis-compatible key-value stores — built to measure
[cinder](https://github.com/muhammadrashid4587/cinder), the Redis clone in
C. Pure Python 3 standard library, no dependencies.

## What it is

- A small RESP client (`kvbench/resp.py`) that talks to any
  Redis-compatible server over a socket.
- A workload runner (`kvbench/bench.py`) that runs a configurable
  SET/GET/INCR mix across N threads, measuring per-op latency.
  With `--pipeline N` it sends N commands per round trip instead of
  one, so throughput under pipelining is measured the same way.
  The mix is set with `--mix`: explicit weights (`50:40:10`), a
  read:write ratio (`80:20`), or a named preset (`read-heavy`,
  `balanced`, `write-heavy`). The report splits ops/sec and latency
  percentiles per operation (SET vs GET vs INCR), so a read-heavy
  mix shows whether reads or writes dominate the tail.
- Latency stats (`kvbench/stats.py`): ops/sec, mean, p50, p95, p99,
  plus a log-spaced latency histogram and a per-second time series
  (ops, mean and max latency per second) to spot stalls.
- A Markdown report writer (`kvbench/report.py`).
- A baseline mode that runs the exact same workload against an
  in-process Python dict, so the report can compare the server with a
  no-network upper bound on the same machine.
- A compare mode (`--compare HOST:PORT`, repeatable) that runs the
  same workload against a second server — cinder vs real Redis, or
  two cinder builds — and puts both in one report, side by side.

## What it is not

- Not a Redis benchmark replacement (redis-benchmark exists). This is a
  small, readable tool for measuring cinder during development.
- Not a correctness tester for Redis semantics. It checks that commands
  succeed, not that every edge case matches Redis.
- Not a load generator for production. It opens one connection per
  thread; the pipeline depth is configurable, but there is no
  connection pooling or TLS.

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

# read-heavy preset, or an explicit 80:20 read:write ratio
python -m kvbench.cli --ops 20000 --clients 4 --mix read-heavy
python -m kvbench.cli --ops 20000 --clients 4 --mix 80:20

# pipelined mode: 10 commands per round trip
python -m kvbench.cli --ops 20000 --clients 4 --pipeline 10

# compare two servers with the same workload, one report
python -m kvbench.cli --host 127.0.0.1 --port 6379 \
    --compare 127.0.0.1:6380 --ops 20000 --clients 4
```

Pipelined latencies are the batch round-trip time divided by the batch
size, so they measure amortized per-op cost under pipelining, not the
latency of any single reply. The report records the pipeline depth in
its workload settings, and the JSON report carries it under
`workload.pipeline`.

Options: `--host` (default 127.0.0.1), `--port` (default 6379),
`--ops` (default 10000), `--clients` (default 4), `--keys` (default
1000), `--value-size` (default 32), `--mix` (default 50:40:10 — a
`SET:GET:INCR` weight triple, a `READ:WRITE` ratio such as `80:20`,
or a preset: `read-heavy` 15:80:5, `balanced` 45:45:10,
`write-heavy` 80:15:5), `--pipeline` (default 1, sequential),
`--seed` (default 1234), `--baseline`, `--compare HOST:PORT`
(repeatable: also run the same workload against another server and
add it to the report), `--output` (default report.md), `--json`
(default: same name as `--output` with a `.json` extension).

Every run writes a JSON report alongside the Markdown one, with the
same workload settings and per-backend results (ops, ops/sec, elapsed,
errors, first error, and the full latency summary) in a machine-readable
form. Use `--json path.json` to put it somewhere else.

## Report format

`report.md` contains the workload settings and one row per backend:

| Backend | Ops | Ops/sec | Mean (ms) | p50 (ms) | p95 (ms) | p99 (ms) | Errors |

The baseline row is labelled `baseline (dict)`.

Below the table, each backend gets a latency histogram section: the
share of ops in each log-spaced bucket (`<= 0.1 ms`, `<= 0.25 ms`,
`<= 0.5 ms`, `1 ms`, `2 ms`, `5 ms`, `10 ms`, `25 ms`, `50 ms`,
`100 ms`, `250 ms`, `500 ms`, `1000 ms`, and an overflow bucket for
anything slower), drawn as text bars. The JSON report carries the
same buckets per result under `histogram`.

Below the table, each backend gets a per-operation stats table:
ops, ops/sec, mean, p50, p95 and p99 latency split by SET, GET and
INCR. Ops that never ran (e.g. INCR in an `80:20` read:write mix)
are left out. The JSON report carries the same split per result
under `per_op`.

Below the histograms, each backend gets a latency-over-time table:
one row per second of the run with the ops completed in that second
and their mean and max latency. A second with 0 ops marks a stall —
the workload made no progress in that second. The JSON report
carries the same series per result under `time_series`.

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

- ~~Configurable read/write mix (`--mix read-heavy` / `80:20`) with
  per-operation stats in both reports.~~ Done in 0.3.0.
- ~~Compare mode: run the same workload against a second server and
  put both in one report.~~ Done in 0.2.4.
- ~~Latency over time (per-second buckets) to spot stalls.~~ Done in
  0.2.3.
- ~~Pipelined mode (N commands per round trip).~~ Done in 0.2.2.
- ~~Percentile histograms in the report.~~ Done in 0.2.1.
- ~~JSON output alongside Markdown.~~ Done in 0.2.0.
