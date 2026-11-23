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
  With `--duration 10s` (or `2m`) it runs for a fixed wall-clock
  time instead of a fixed op count, and reports the total ops
  completed and ops/sec over the actual elapsed time — useful for
  soaking a server at steady load rather than racing a fixed
  number of ops.
  The mix is set with `--mix`: explicit weights (`50:40:10`), a
  read:write ratio (`80:20`), or a named preset (`read-heavy`,
  `balanced`, `write-heavy`). The report splits ops/sec and latency
  percentiles per operation (SET vs GET vs INCR), so a read-heavy
  mix shows whether reads or writes dominate the tail.
  Keys are picked uniformly by default, or with `--distribution
  zipfian` from a Zipfian distribution (`--zipf-skew`, default 1.0):
  a small hot set of keys takes most of the traffic, like a real
  workload — with 1000 keys the hottest 1% take about 39% of ops.
  The report records the expected hot-set share and the measured
  hot-set hit rate per backend, so a run shows whether the skewed
  traffic actually landed on hot keys.
  With `--warmup 2s` (or `1m`) the runner first runs the same
  workload for that long and throws it away — warming caches,
  connections and JITs — so the cold first seconds do not drag the
  percentiles down. Warmup ops and latencies never reach the
  results, and the measured phase is re-seeded, so its keys and op
  mix are identical to a run with no warmup at all.
  With `--hot-keys N` (default 5) the reports show per-key latency
  stats — count, mean, p50, p95 and p99 — for the N hottest keys
  by access count, so a hot key slower than the overall p99 points
  at contention on that key rather than the server as a whole.
  Under a zipfian distribution these are the keys the skew
  concentrates on; counters report under their own key
  (`kvbench:counter:<rank>`).
  With `--sweep-keyspace 100,1000,10000,100000` the same workload
  is re-run once per keyspace size, and the reports show ops/sec
  and p99 per size, so a run shows how keyspace size affects
  throughput — a hash-table resize or a cache that stops fitting
  shows up as a step down in the sweep table.
  With `--sweep-clients 1,2,4,8` the same workload is re-run once
  per client count, and the reports show ops/sec and p99 per
  count, so a run shows how concurrency affects throughput — where
  adding clients stops helping and the server saturates.
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

# duration mode: run for 10 seconds instead of a fixed op count
python -m kvbench.cli --duration 10s --clients 4

# compare two servers with the same workload, one report
python -m kvbench.cli --host 127.0.0.1 --port 6379 \
    --compare 127.0.0.1:6380 --ops 20000 --clients 4

# skewed workload: Zipfian key distribution, hottest 1% of keys
# take most of the traffic (skew 1.0 is the classic YCSB default)
python -m kvbench.cli --ops 20000 --clients 4 \
    --distribution zipfian --zipf-skew 1.0

# warm up for 2 seconds before measuring, so the cold first
# seconds do not drag the percentiles down
python -m kvbench.cli --ops 20000 --clients 4 --warmup 2s

# sweep the keyspace: the same workload re-run at 100, 1000,
# 10000 and 100000 keys, with ops/sec and p99 per size
python -m kvbench.cli --ops 20000 --clients 4 \
    --sweep-keyspace 100,1000,10000,100000

# sweep the client count: the same workload re-run at 1, 2, 4
# and 8 clients, with ops/sec and p99 per count
python -m kvbench.cli --ops 20000 --sweep-clients 1,2,4,8

# record the run in the history file, then look at the ops/sec
# trend over time (default file: ~/.kvbench/history.jsonl)
python -m kvbench.cli --ops 20000 --clients 4 --history
python -m kvbench.cli --show-history
```

Pipelined latencies are the batch round-trip time divided by the batch
size, so they measure amortized per-op cost under pipelining, not the
latency of any single reply. The report records the pipeline depth in
its workload settings, and the JSON report carries it under
`workload.pipeline`.

Options: `--host` (default 127.0.0.1), `--port` (default 6379),
`--ops` (default 10000), `--duration` (e.g. `10s` or `2m`; a fixed
wall-clock run instead of a fixed op count — mutually exclusive
with `--ops`), `--clients` (default 4), `--keys` (default
1000), `--value-size` (default 32), `--mix` (default 50:40:10 — a
`SET:GET:INCR` weight triple, a `READ:WRITE` ratio such as `80:20`,
or a preset: `read-heavy` 15:80:5, `balanced` 45:45:10,
`write-heavy` 80:15:5), `--pipeline` (default 1, sequential),
`--distribution` (`uniform` or `zipfian`, default `uniform`),
`--zipf-skew` (default 1.0, the Zipf exponent for zipfian key
picking; 0 is uniform), `--hot-keys` (default 5, how many of the
hottest keys by access count get per-key latency stats in the
reports; 0 disables), `--warmup` (e.g. `2s` or `1m`; run the
workload for this long before measurement, warming caches and
connections — warmup ops and latencies are excluded from the
results, and in duration mode the duration clock starts after
warmup ends),
`--seed` (default 1234), `--baseline`, `--compare HOST:PORT`
(repeatable: also run the same workload against another server and
add it to the report), `--sweep-keyspace` (comma-separated keyspace
sizes, e.g. `100,1000,10000,100000`: re-run the same workload once
per size and report ops/sec and p99 per size in both reports),
`--sweep-clients` (comma-separated client counts, e.g. `1,2,4,8`:
re-run the same workload once per count and report ops/sec and p99
per count in both reports),
`--output` (default report.md), `--json`
(default: same name as `--output` with a `.json` extension),
`--history` (record this run in the history file so later runs can
be compared against it), `--history-file` (where the history lives,
default `~/.kvbench/history.jsonl`), `--show-history` (print the
recorded run history and exit, no benchmark is run),
`--history-limit` (how many recent runs `--show-history` prints,
default 20).

Every run writes a JSON report alongside the Markdown one, with the
same workload settings and per-backend results (ops, ops/sec, elapsed,
errors, first error, and the full latency summary) in a machine-readable
form. Use `--json path.json` to put it somewhere else. The JSON
workload section carries the key distribution (`distribution` and
`zipf_skew`, null for uniform), and each result carries its measured
hot-set hit rate (`hot_hit_rate`: the share of ops that hit the
hottest 1% of keys).

## Run history

With `--history` every run appends one JSON record per backend to
`~/.kvbench/history.jsonl` (use `--history-file PATH` to put it
somewhere else): timestamp, backend, ops, ops/sec, mean/p50/p95/p99
latency, errors, and the workload settings. `python -m kvbench.cli
--show-history` prints the most recent runs (20 by default,
`--history-limit N` to change it) as a table with the ops/sec change
versus the previous run of the same backend, so a cinder speedup —
or a regression — reads at a glance. The history file is plain
JSONL, one record per line, and a corrupt line is skipped on load
rather than failing the trend view.

## Report format

`report.md` contains the workload settings and one row per backend:

| Backend | Ops | Ops/sec | Mean (ms) | p50 (ms) | p95 (ms) | p99 (ms) | Errors |

The baseline row is labelled `baseline (dict)`.

In duration mode the workload section shows the run length
(`Duration: 10s`) instead of a total op count, and the results table
still shows the ops completed and ops/sec over the actual elapsed
time. The JSON report carries the duration under
`workload.duration_s` (with `workload.ops` null).

Below the table, each backend gets a latency histogram section: the
share of ops in each log-spaced bucket (`<= 0.1 ms`, `<= 0.25 ms`,
`<= 0.5 ms`, `1 ms`, `2 ms`, `5 ms`, `10 ms`, `25 ms`, `50 ms`,
`100 ms`, `250 ms`, `500 ms`, `1000 ms`, and an overflow bucket for
anything slower), drawn as text bars. The JSON report carries the
same buckets per result under `histogram`.

With `--sweep-keyspace` the report has a Keyspace sweep section
right below the results table: one table per backend with a row per
swept size — keys, ops, ops/sec, mean and p99 — so the effect of
keyspace size on throughput reads at a glance. The JSON report
carries the same rows per result under `keyspace_sweep`, and the
workload section carries the swept sizes under `sweep_keyspace`.

With `--sweep-clients` the report has a Client sweep section right
below the Keyspace sweep (or the results table, when there is no
keyspace sweep): one table per backend with a row per swept count
— clients, ops, ops/sec, mean and p99 — so the effect of
concurrency on throughput reads at a glance. The JSON report
carries the same rows per result under `client_sweep`, and the
workload section carries the swept counts under `sweep_clients`.

Below the table, each backend gets a per-operation stats table:
ops, ops/sec, mean, p50, p95 and p99 latency split by SET, GET and
INCR. Ops that never ran (e.g. INCR in an `80:20` read:write mix)
are left out. The JSON report carries the same split per result
under `per_op`.

Below that, each backend gets a per-key latency table: count,
mean, p50, p95 and p99 for the hottest keys by access count (top
5 by default, `--hot-keys` to change it, 0 to disable). The JSON
report carries the same entries per result under `hot_keys`, and
the workload section carries the count under `hot_keys`.

Below the histograms, each backend gets a latency-over-time table:
one row per second of the run with the ops completed in that second
and their mean and max latency. A second with 0 ops marks a stall —
the workload made no progress in that second. The JSON report
carries the same series per result under `time_series`.

With `--distribution zipfian` the workload section shows the skew
and the expected hot-set share, and the notes list the measured
hot-set hit rate per backend: the share of ops that hit the hottest
1% of keys.

With `--warmup` the workload section shows the warmup length, and
the notes list how many warmup ops each backend ran: those ops and
their latencies are excluded from the results, and the elapsed time
covers the measured phase only. The JSON report carries the warmup
under `workload.warmup_s` and each result's warmup op count under
`warmup_ops`.

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

- ~~Client-count sweep (`--sweep-clients 1,2,4,8`): re-run the
  same workload at several client counts, with ops/sec and p99
  per count in both reports.~~ Done in 0.10.0.
- ~~Run history (`--history` / `--show-history`): record each run
  in a local JSONL history file (`~/.kvbench/history.jsonl`) and
  show the ops/sec trend over time, with the change versus the
  previous run of the same backend.~~ Done in 0.9.0.
- ~~Keyspace-size sweep (`--sweep-keyspace 100,1000,10000,100000`):
  re-run the same workload at several keyspace sizes, with ops/sec
  and p99 per size in both reports.~~ Done in 0.8.0.
- ~~Per-key latency for hot keys (`--hot-keys N`): count, mean,
  p50/p95/p99 per key for the hottest keys by access count, in both
  reports.~~ Done in 0.7.0.
- ~~Warmup phase (`--warmup 2s` / `1m`): run the same workload
  before measurement to warm caches and connections, with warmup
  ops and latencies excluded from both reports.~~ Done in 0.6.0.
- ~~Zipfian key distribution (`--distribution zipfian` /
  `--zipf-skew`): skewed traffic where a hot set of keys takes most
  of the ops, with the expected and measured hot-set hit rate in
  both reports.~~ Done in 0.5.0.
- ~~Duration mode (`--duration 10s` / `2m`): run for a fixed
  wall-clock time instead of a fixed op count, with the duration
  and computed throughput in both reports.~~ Done in 0.4.0.
- ~~Configurable read/write mix (`--mix read-heavy` / `80:20`) with
  per-operation stats in both reports.~~ Done in 0.3.0.
- ~~Compare mode: run the same workload against a second server and
  put both in one report.~~ Done in 0.2.4.
- ~~Latency over time (per-second buckets) to spot stalls.~~ Done in
  0.2.3.
- ~~Pipelined mode (N commands per round trip).~~ Done in 0.2.2.
- ~~Percentile histograms in the report.~~ Done in 0.2.1.
- ~~JSON output alongside Markdown.~~ Done in 0.2.0.
