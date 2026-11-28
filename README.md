# kvbench

A benchmark CLI for Redis-compatible key-value stores — built to measure
[cinder](https://github.com/muhammadrashid4587/cinder), the Redis clone in
C. Pure Python 3 standard library, no dependencies.

## What it is

- A small RESP client (`kvbench/resp.py`) that talks to any
  Redis-compatible server over a socket. The connect and read
  timeout is 5 seconds by default; `--timeout SECONDS` raises it
  for a loaded or remote server, or lowers it to fail fast on a
  dead one. The timeout is recorded in both reports, so a run
  that timed out can be told apart from one that was just slow.
  With `--password` the client sends AUTH on connect (`AUTH
  username password` with `--username`, the Redis 6 ACL form),
  so a password-protected server can be benchmarked — a wrong
  password fails the run (exit 1) before any op is counted, and
  the password itself is never written to the reports or the
  history: the recorded command line shows `***` in its place,
  while both reports record that the run authenticated.
- A workload runner (`kvbench/bench.py`) that runs a configurable
  SET/GET/INCR/DEL mix across N threads, measuring per-op latency.
  With `--pipeline N` it sends N commands per round trip instead of
  one, so throughput under pipelining is measured the same way.
  With `--duration 10s` (or `2m`) it runs for a fixed wall-clock
  time instead of a fixed op count, and reports the total ops
  completed and ops/sec over the actual elapsed time — useful for
  soaking a server at steady load rather than racing a fixed
  number of ops.
  The mix is set with `--mix`: explicit weights (`45:40:10:5`), a
  read:write ratio (`80:20`), or a named preset (`read-heavy`,
  `balanced`, `write-heavy`). DEL deletes a key the workload SET
  earlier (deleting a missing key replies 0, not an error, so it
  measures like any other op); the default mix carries a small
  5% DEL share, like a real cache workload where deletes are rare
  next to reads and writes. The report splits ops/sec and latency
  percentiles per operation (SET vs GET vs INCR vs DEL), so a
  read-heavy mix shows whether reads or writes dominate the tail.
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
  (`<prefix>:counter:<rank>`).
  With `--key-prefix NAME` (default `kvbench`) every key is
  prefixed with that name (`<prefix>:<rank>` for SET/GET,
  `<prefix>:counter:<rank>` for INCR), so runs against a shared
  store do not collide with each other — or with another tool's
  keys — and the prefix is recorded in both reports, so a report
  shows which keys the run touched.
  With `--sweep-keyspace 100,1000,10000,100000` the same workload
  is re-run once per keyspace size, and the reports show ops/sec
  and p99 per size, plus a per-operation latency histogram per
  size, so a run shows how keyspace size affects throughput — a
  hash-table resize or a cache that stops fitting shows up as a
  step down in the sweep table.
  With `--sweep-clients 1,2,4,8` the same workload is re-run once
  per client count, and the reports show ops/sec and p99 per
  count, plus a per-operation latency histogram per count, so a run
  shows how concurrency affects throughput — where adding clients
  stops helping and the server saturates.
  With `--sweep-value-size 32,128,1024` the same workload is
  re-run once per value size in bytes, and the reports show ops/sec
  and p99 per size, plus a per-operation latency histogram per
  size, so a run shows how value size affects throughput — a store
  that copies values on every SET slows as values grow, while one
  that only moves pointers does not.
  With `--sweep-pipeline 1,5,10` the same workload is re-run once
  per pipeline depth, and the reports show ops/sec and p99 per
  depth, plus a per-operation latency histogram per depth, so a run
  shows how pipelining affects throughput — where batching more
  commands per round trip stops helping.
  With `--sweep-mix 100:0,90:10,50:50,10:90` the same workload is
  re-run once per read:write ratio, and the reports show ops/sec
  and p99 per ratio, plus a per-operation latency histogram per
  ratio, so a run shows how the read:write mix affects throughput
  — where a store's writes start costing more than its reads.
  Each ratio is a read:write split — reads are GETs, writes are
  SETs, with no INCRs or DELs, the same meaning as a two-part
  `--mix` — so `100:0` is a read-only run and `0:100` a
  write-only one, and the ratios run read-heaviest first.
  When the tail is high the reports attribute it: slow ops (at or
  above the overall p99) are broken down by operation type, by
  client, by hot vs cold keys, and by second, and the finding
  names the factor that dominates the tail — one slow op type, one
  slow client, hot or cold keys, or a single slow second — or says
  no single factor does, so a high p99 points at the cause instead
  of just the number.
- Latency stats (`kvbench/stats.py`): ops/sec, mean, p50, p95, p99,
  plus a log-spaced latency histogram, overall and per operation,
  and a per-second time series
  (ops, mean and max latency per second) to spot stalls.
- A Markdown report writer (`kvbench/report.py`).
- An SLO gate (`kvbench/slo.py`): `--slo-p99-ms` and
  `--slo-error-rate` cap the measured p99 latency and error rate,
  and fail the run (exit code 4) when a backend breaches either
  threshold, with the gate verdict in both reports — so a
  benchmark in CI stays green only while the server stays fast
  and reliable enough. With `--slo-op OP:P99,ERR` (repeatable)
  each operation type carries its own p99 and error-rate
  thresholds — e.g. `--slo-op SET:5,0.1` gates SET ops on p99
  5 ms and error rate 0.1% — with per-op verdicts in both
  reports, on top of the global gate, so a slow write fails the
  run even when the overall p99 is fine.
  With `--slo-preset PATH` the thresholds live in a JSON file
  instead of on the command line — `{"p99_ms": 1,
  "error_rate_pct": 1, "per_op": {"SET": {"p99_ms": 5,
  "error_rate_pct": 0.1}}}` — so CI can share one budget across
  runs; every key is optional, and CLI flags
  (`--slo-p99-ms`, `--slo-error-rate`, `--slo-op`) override the
  preset values. A missing, unreadable or invalid preset file
  fails the run (exit 2) before any benchmark starts, and both
  reports name the preset file the thresholds came from.
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
# 20k ops, 4 clients, default 45:40:10:5 SET:GET:INCR:DEL mix
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

# workload from a JSON config file, so the benchmark can be checked
# in and re-run exactly — a config file and command-line workload
# flags conflict, so pass one or the other, not both
python -m kvbench.cli --host 127.0.0.1 --port 6379 --workload workload.json

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

# sweep the value size: the same workload re-run at 32, 128 and
# 1024 byte values, with ops/sec and p99 per size
python -m kvbench.cli --ops 20000 --clients 4 \
    --sweep-value-size 32,128,1024

# sweep the pipeline depth: the same workload re-run at 1, 5 and
# 10 commands per round trip, with ops/sec and p99 per depth
python -m kvbench.cli --ops 20000 --clients 4 \
    --sweep-pipeline 1,5,10

# sweep the read:write mix: the same workload re-run at 100:0,
# 90:10, 50:50 and 10:90 read:write, with ops/sec and p99 per ratio
python -m kvbench.cli --ops 20000 --clients 4 \
    --sweep-mix 100:0,90:10,50:50,10:90

# record the run in the history file, then look at the ops/sec
# trend over time (default file: ~/.kvbench/history.jsonl)
python -m kvbench.cli --ops 20000 --clients 4 --history
python -m kvbench.cli --show-history

# ops/sec trend chart per backend, with sparklines, from the
# same history file
python -m kvbench.cli --trend

# a sweep run recorded the same way also records each sweep step,
# so --trend shows one trend section per step that ran
python -m kvbench.cli --ops 20000 --clients 4 \
    --sweep-keyspace 100,1000 --history
python -m kvbench.cli --trend

# prune old runs from the history file, per backend — keep the
# 20 most recent runs of each backend, or drop runs older than
# a date; --dry-run shows what would be removed first
python -m kvbench.cli --prune --prune-keep 20
python -m kvbench.cli --prune --prune-before 2026-11-01T00:00:00Z \
    --dry-run

# export the history as portable JSON, and merge an export back
# into the history on another machine — identical runs are
# skipped, conflicting runs keep the existing record
python -m kvbench.cli --history-export history.json
python -m kvbench.cli --history-import history.json

# same trend chart, written to a Markdown file as well, so it
# can be committed or attached to a report
python -m kvbench.cli --trend --trend-out trend.md

# same trend chart, written as JSON as well, so a script can
# read the trend — the runs, the sparkline, the first-to-last
# change, the per-op sections (each with its own first-to-last
# histogram comparison) and the per-step sweep sections —
# without parsing the Markdown chart
python -m kvbench.cli --trend --trend-json trend.json

# compare this run against a previous run's JSON report, side by
# side, and fail the run when it regressed past 5%
python -m kvbench.cli --ops 20000 --clients 4 \
    --compare-file report.json --regression-threshold 5

# same comparison, but against a run recorded in the history file
# instead of a report file — -1 is the most recent recorded run,
# 0 the oldest, or give an ISO timestamp for the latest run at or
# before that time
python -m kvbench.cli --ops 20000 --clients 4 \
    --compare-history -1 --regression-threshold 5
python -m kvbench.cli --ops 20000 --clients 4 \
    --compare-history 2026-11-22T15:00:00Z

# same comparison, but only against a recorded run that used the
# same workload as this run — the latest matching run per backend
python -m kvbench.cli --ops 20000 --clients 4 \
    --compare-history -1 --compare-same-workload \
    --regression-threshold 5

# same comparison, but also written out as a standalone JSON file
# — the exact comparison the run's JSON report carries under
# "comparison", so a script can read it without parsing the report
python -m kvbench.cli --ops 20000 --clients 4 \
    --compare-history -1 --compare-history-json comparison.json

# same standalone export, but for a comparison against a previous
# run's JSON report instead of a history run
python -m kvbench.cli --ops 20000 --clients 4 \
    --compare-file report.json --compare-file-json comparison.json

# SLO gate: fail the run (exit 4) when p99 is above 1 ms or more
# than 1% of ops error — the verdict lands in both reports
python -m kvbench.cli --ops 20000 --clients 4 \
    --slo-p99-ms 1 --slo-error-rate 1

# per-operation SLOs: SET ops get their own p99 and error-rate
# budgets, on top of the global gate — either threshold may be
# left empty (SET:5, gates p99 only)
python -m kvbench.cli --ops 20000 --clients 4 \
    --slo-op SET:5,0.1 --slo-op GET:2,0.1

# SLO preset file: thresholds live in a JSON file (see below for
# the format) instead of on the command line — CLI flags still
# override the preset values
python -m kvbench.cli --ops 20000 --clients 4 \
    --slo-preset slo.json

# also write a per-second throughput CSV for plotting
python -m kvbench.cli --ops 20000 --clients 4 --duration 10s \
    --csv timeseries.csv

# a loaded or remote server may need more than the 5 s default
# before a reply counts as a timeout — or fail fast on a dead one
python -m kvbench.cli --ops 20000 --clients 4 --timeout 30
python -m kvbench.cli --ops 20000 --clients 4 --timeout 1

# benchmark a password-protected server (real Redis with
# requirepass): AUTH on connect, and the password never lands in
# the reports or the history — the recorded command line shows ***
# in its place
python -m kvbench.cli --ops 20000 --clients 4 --password secret
# ACL-style auth: AUTH username password
python -m kvbench.cli --ops 20000 --clients 4 \
    --username alice --password secret

# run the same benchmark 3 times and report the median ops/sec and
# p99 over the 3 runs, so one noisy run does not set the numbers —
# the per-run values land in the JSON report under
# results[].repeat.runs, each run with its own per-operation
# latency histograms, and the Markdown report shows a per-run
# per-op histogram block under the Repeat runs table — plus a
# spread summary (min, max, spread and spread as a percentage of
# the median, overall and per operation) under
# results[].repeat.spread in JSON and in a table in Markdown —
# plus a fastest-vs-slowest comparison of the repeat runs
# (overall p50/p99 and p99 change, and a per-op table with p50,
# p99, p99 change and bucket drift) under
# results[].repeat.histogram_comparison in JSON and in a section
# in Markdown, so a noisy repeat run shows which op's latency
# shape moved — the comparison also carries the fastest and
# slowest runs' per-op histograms under
# fastest_histograms_by_op and slowest_histograms_by_op, the
# same runs the Repeat runs section already draws a per-op
# block for — plus a per-op fastest-vs-slowest comparison for
# each op that ran, under repeat.histogram_comparison.per_op in
# JSON and as a section per op in the Repeat runs section in
# Markdown, so a repeat run where one op's runs swing while the
# overall winner stays steady still names the op — plus the
# fastest and slowest runs' histogram blocks for that op next to
# each per-op section, the two runs' latency shapes for the op
# alone drawn with the same shared helper as the per-op blocks,
# from the bucket lists the comparison already carries under
# fastest_histogram and slowest_histogram for the op, so the
# shape move reads as bars as well as numbers; a single
# repeat run renders a note naming the
# reason instead of a comparison
python -m kvbench.cli --ops 20000 --clients 4 --repeat 3

# repeat composes with the sweeps: each sweep step runs 3 times
# and reports the median ops/sec and p99 per step, so one noisy
# step does not set the numbers — the per-run values and the
# spread land under each step's repeat key in JSON, and the
# Markdown report shows a per-step repeat block under each sweep
# table — plus each step's own fastest-vs-slowest repeat-run
# histogram comparison (overall p50/p99 and p99 change, and a
# per-op table with p50, p99, p99 change and bucket drift) under
# each step's repeat.histogram_comparison in JSON and in the
# per-step repeat block in Markdown, so a noisy step shows which
# op's latency shape moved — plus the fastest and slowest runs'
# per-op histogram blocks next to each step's comparison table,
# the two runs' latency shapes drawn with the same shared helper
# as the step's own per-op blocks, so the shape move reads as
# bars as well as numbers, with the same two maps under
# fastest_histograms_by_op and slowest_histograms_by_op in the
# step's repeat.histogram_comparison in JSON — plus a per-op
# fastest-vs-slowest comparison for each op that ran, under
# repeat.histogram_comparison.per_op in JSON and as a section per
# op in the per-step repeat block in Markdown, so a step where
# one op's runs swing while the overall winner stays steady still
# names the op — plus the fastest and slowest runs' histogram
# blocks for that op next to each per-op section, the two runs'
# latency shapes for the op alone drawn with the same shared
# helper, from the bucket lists under fastest_histogram and
# slowest_histogram for the op; a step whose runs are not
# comparable renders a note naming the reason instead, and no
# blocks
python -m kvbench.cli --ops 20000 --clients 4 --repeat 3 \
    --sweep-keyspace 100,1000,10000

# prefix all keys, so a run against a shared store does not collide
# with another run's keys (or another tool's)
python -m kvbench.cli --ops 20000 --clients 4 --key-prefix nightly
```

Pipelined latencies are the batch round-trip time divided by the batch
size, so they measure amortized per-op cost under pipelining, not the
latency of any single reply. The report records the pipeline depth in
its workload settings, and the JSON report carries it under
`workload.pipeline`.

Options: `--host` (default 127.0.0.1), `--port` (default 6379),
`--timeout` (default 5.0, socket timeout in seconds for the
server connection — connect and read; raise it for a loaded or
remote server, lower it to fail fast on a dead one; a non-positive
value is rejected),
`--password` (authenticate on connect with AUTH before
benchmarking, for a password-protected server; a wrong password
fails the run (exit 1) before any op runs; the password is never
written to the reports or the history — the recorded command line
shows `***` in its place),
`--username` (with `--password`, authenticate as this user — AUTH
username password, the Redis 6 ACL form; needs `--password`),
`--repeat` (default 1, how many times to run the benchmark: above
1 the reported ops/sec and p99 are the medians over the runs,
per operation as well as overall, and the per-run values are
recorded in the JSON report; each sweep step runs that many times
too and reports the median ops/sec and p99 per step, with its
per-run values and spread under the step's `repeat` key; a
non-positive value is rejected),
`--ops` (default 10000), `--duration` (e.g. `10s` or `2m`; a fixed
wall-clock run instead of a fixed op count — mutually exclusive
with `--ops`), `--clients` (default 4), `--keys` (default
1000), `--key-prefix` (default `kvbench` — every key is
`<prefix>:<rank>` for SET/GET and `<prefix>:counter:<rank>` for
INCR; a different prefix keeps runs against a shared store from
colliding, and the prefix is recorded in both reports; an empty
prefix is rejected), `--value-size` (default 32), `--mix` (default 45:40:10:5 — a
`SET:GET:INCR:DEL` weight quadruple (a `SET:GET:INCR` triple is
accepted too, with DEL weight 0), a `READ:WRITE` ratio such as
`80:20`, or a preset: `read-heavy` 15:75:5:5, `balanced`
40:45:10:5, `write-heavy` 75:15:5:5), `--pipeline` (default 1, sequential),
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
`--sweep-value-size` (comma-separated value sizes in bytes, e.g.
`32,128,1024`: re-run the same workload once per size and report
ops/sec and p99 per size in both reports),
`--sweep-pipeline` (comma-separated pipeline depths, e.g.
`1,5,10`: re-run the same workload once per depth and report
ops/sec and p99 per depth in both reports),
`--sweep-mix` (comma-separated read:write ratios, e.g.
`100:0,90:10,50:50,10:90`: re-run the same workload once per
ratio — reads are GETs, writes are SETs, no INCRs or DELs — and
report ops/sec and p99 per ratio in both reports, read-heaviest
first),
`--output` (default report.md), `--json`
(default: same name as `--output` with a `.json` extension),
`--csv` (write a per-second throughput CSV alongside the reports:
one row per backend per second with ops, ops/sec, mean and max
latency — see below),
`--history` (record this run in the history file so later runs can
be compared against it), `--history-file` (where the history lives,
default `~/.kvbench/history.jsonl`), `--show-history` (print the
recorded run history and exit, no benchmark is run),
`--trend` (print an ops/sec trend chart per backend from the
recorded run history and exit, no benchmark is run),
`--trend-out PATH` (with `--trend`, also write the trend chart to
a Markdown file — the same text printed to stdout — so it can be
committed or attached to a report; needs `--trend`),
`--trend-json PATH` (with `--trend`, also write the trend chart
as JSON to this file, so a script can read the trend without
parsing the Markdown chart; needs `--trend`),
`--best-worst` (print a best-run vs worst-run comparison per
backend from the recorded run history and exit, no benchmark is
run — ops/sec and p99 for the best and worst runs, overall and
per operation, with the percentage change between them, plus
each picked run's per-operation latency histograms),
`--best-worst-out PATH` (with `--best-worst`, also write the
comparison to a Markdown file; needs `--best-worst`),
`--best-worst-json PATH` (with `--best-worst`, also write the
comparison as JSON; needs `--best-worst`),
`--history-limit` (how many recent runs `--show-history` and
default 20),
`--history-export PATH` (export the recorded run history as
portable JSON to this file and exit, no benchmark is run — all
backends, or only the ones named with `--history-export-backend`),
`--history-export-backend NAME` (with `--history-export`, only
export runs of this backend, e.g. `cinder (127.0.0.1:6379)`;
repeatable, other backends are left out),
`--history-import PATH` (import a history export — see
`--history-export` — into the history file and exit, no
benchmark is run: new runs are appended, identical runs are
skipped, and runs that conflict with a recorded run keep the
existing record; a bad export file fails with exit 2 and the
history is left untouched),
`--prune` (prune old runs from the history file and exit, no
benchmark is run — see below),
`--prune-keep N` (with `--prune`, keep the N most recent runs
of each backend and drop the rest, e.g. `--prune-keep 20`),
`--prune-before TIMESTAMP` (with `--prune`, drop runs older
than this ISO timestamp, e.g. `2026-11-01T00:00:00Z`; a
timestamp with no timezone is read as UTC),
`--prune-backend NAME` (with `--prune`, only prune runs of
this backend, e.g. `cinder (127.0.0.1:6379)`; repeatable, other
backends are left alone),
`--prune-all` (with `--prune`, remove every run of the selected
backends — the explicit flag for clearing a backend's history
out entirely; it cannot be combined with `--prune-keep` or
`--prune-before`),
`--dry-run` (with `--prune`, show what would be removed
without changing the history file),
`--compare-file` (compare this run against a previous run's JSON
report — ops/sec and p99 side by side per backend, with the
percentage change and a verdict in both reports; see below),
`--compare-history` (compare this run against a run recorded in
the history file instead of a JSON report — pick it by index, `0`
for the oldest run, `-1` or `latest` for the most recent, or by
ISO timestamp for the latest run at or before that time, e.g.
`--compare-history -1`; the history file is `--history-file`,
default `~/.kvbench/history.jsonl`; mutually exclusive with
`--compare-file`),
`--compare-history-json PATH` (with `--compare-history`, also
write the comparison as a standalone JSON file — the exact
comparison the run's JSON report carries under `comparison`,
same entries, per-op deltas and per-op histograms, so a script
can read it without parsing the report; needs
`--compare-history`),
`--compare-file-json PATH` (with `--compare-file`, also
write the comparison as a standalone JSON file — the exact
comparison the run's JSON report carries under `comparison`,
same entries, per-op deltas and per-op histograms, so a script
can read it without parsing the report; needs
`--compare-file`),
`--compare-same-workload` (with `--compare-history`, only
consider recorded runs whose workload matches this run's, so the
baseline is the latest run that measured the same workload; with
`--best-worst`, only consider runs whose workload matches the
latest recorded run's),
`--regression-threshold` (with `--compare-file` or
`--compare-history`, exit 3 when any backend's ops/sec fell by
more than that many percent, or its p99 rose by more than that
many percent, versus the baseline run),
`--slo-p99-ms` (SLO gate: exit 4 when any backend's measured p99
latency is above that many milliseconds),
`--slo-error-rate` (SLO gate: exit 4 when any backend's error rate
is above that many percent of attempted ops),
`--slo-op OP:P99,ERR` (repeatable: per-operation SLO gate — gate
one operation type on its own p99 and error-rate thresholds, e.g.
`SET:5,0.1` means SET ops breach when their p99 is above 5 ms or
their error rate is above 0.1%; either threshold may be left empty,
e.g. `SET:5,` gates p99 only; a later entry for the same op
replaces the earlier one).
`--slo-preset PATH` (load SLO thresholds from a JSON preset file
instead of passing them on the command line — see the SLO gate
section for the format; CLI flags override preset values).
`--workload FILE` (load the workload from a JSON config file
instead of the command line — see the Workload config file section
for the format; a config file and command-line workload flags
conflict, so pass one or the other, not both).

## Workload config file

With `--workload FILE` the workload comes from a JSON file instead
of the command line, so a benchmark can be checked in and re-run
exactly — the same file, the same workload, on any machine. Every
key is optional; a key the file does not set keeps the command-line
default:

```json
{
  "ops": 20000,
  "clients": 4,
  "keys": 1000,
  "key_prefix": "kvbench",
  "value_size": 32,
  "mix": "45:40:10:5",
  "pipeline": 1,
  "distribution": "uniform",
  "zipf_skew": 1.0,
  "hot_keys": 5,
  "seed": 1234,
  "timeout": 5.0,
  "duration": "10s",
  "warmup": "2s",
  "repeat": 1,
  "sweep_keyspace": [100, 1000],
  "sweep_clients": [1, 2, 4],
  "sweep_value_size": [32, 128],
  "sweep_pipeline": [1, 5],
  "sweep_mix": ["100:0", "90:10"]
}
```

`mix` takes the same forms as `--mix` (`SET:GET:INCR:DEL` weights,
a `SET:GET:INCR` triple, a `READ:WRITE` ratio, or a preset name);
`duration` and `warmup` take the same forms as `--duration`
(`10s`, `2m`, or a bare number of seconds — a JSON number is
seconds directly, so `"duration": 10` means 10 seconds); the
sweeps are JSON arrays of positive integers, sorted and
deduplicated like the comma-separated flags. `sweep_mix` is a list
of `"READ:WRITE"` ratio strings, parsed like the `--sweep-mix`
flag (read-heaviest first, duplicates dropped). `ops` and `duration`
are mutually exclusive, like the flags.

A config file and command-line workload flags conflict: passing
`--workload` together with `--ops`, `--duration`, `--warmup`,
`--clients`, `--keys`, `--key-prefix`, `--value-size`, `--mix`,
`--pipeline`, `--distribution`, `--zipf-skew`, `--hot-keys`,
`--seed`, `--timeout`, `--repeat`, or any `--sweep-*` flag fails
the run with exit 2 instead of silently merging the two. Server,
target, output, comparison, SLO and history flags are not workload
flags and still combine with `--workload` (`--baseline`,
`--compare`, `--history`, `--slo-preset` and the rest all work the
same way). A missing, unreadable, invalid, or out-of-range config
file also fails with exit 2 before any benchmark starts.

Both reports record the workload the file declared: the JSON
report carries the config path under `workload.workload_file`
alongside the resolved values, the Markdown report names the file
in its Workload section, and a run recorded with `--history`
carries both in its history record — so a run is reproducible
from its record even without the file. Two runs with the same
resolved values are the same workload for `--compare-same-workload`,
whether one was declared in a file and the other on the command
line. The checked-in `workload.json` is an example file with the
defaults.

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

With `--trend` the same history renders as an ops/sec-over-time
chart, one section per backend: a table of the most recent runs
(20 by default, `--history-limit N` to change it) with ops/sec,
the change versus the previous run of that backend, and p99
alongside, plus a sparkline of ops/sec over those runs (oldest to
newest) and the first-to-last change in ops/sec and in p99 — a
backend whose tail crept up reads at a glance even when its
throughput moved the other way. A backend with a single
recorded run shows its one value with no delta and a single
block, and no first-to-last lines; an empty history prints the
same "No runs recorded yet"
message as `--show-history`. Below the overall chart each backend
also gets a per-operation trend section — one table per op (SET,
GET, INCR, DEL) that ran, with that op's ops/sec, its change versus
the previous run that ran the same op, and its p99, plus its own
sparkline and first-to-last change in ops/sec and in p99 — so a
SET regression reads
at a glance even when GETs dominate the mix, and a SET tail that
crept up reads at a glance even when SET throughput moved the
other way. Runs recorded
before 0.22.0 carry no per-op data and simply show no per-op
sections; a run that never ran an op (a read-only mix has no
SET) leaves that op out of its own table. Each per-op section
ends with that op's first-to-last histogram comparison — that
op's p50/p99 in the first and last runs that ran it, the p99
change and the bucket drift — plus the first run's histogram
block for that op, drawn with the same shared helper as the
latest run's block, so that op's two shapes can be read side
by side next to its comparison; a section whose op ran in
only one run in the window, or whose first or last run that
ran the op was recorded before 0.43.0, recorded no per-op
latencies, or carries malformed histograms, renders a note
naming the reason instead of a table, and no first-run block.
Each backend's trend
also ends with the latest run's per-operation latency
histograms — the same log-spaced buckets per operation that ran
as the regular run's `histograms_by_op`, drawn with the same
shared helper as the `--best-worst` output and the single-run
report, so the views cannot disagree — so a p99 move in the
trend names the op whose latency shape moved. History records
carry the histograms since 0.43.0; a latest run recorded before
that, one recorded with no per-op latencies, or one whose
recorded histograms are malformed shows a note naming the
reason instead of a histogram block. Below the latest run's
histograms each backend also gets a per-op histogram comparison
between the first and last runs in the trend — a table with each
op's p50 and p99 in the first and last runs, the p99 change, and
the bucket drift (the largest single-bucket share move, in
percentage points, with the bucket it moved in) — plus the first
run's per-op histogram block, drawn with the same shared helper
as the latest run's, so the two shapes can be read side by side
and a p99 move shows how each op's latency shape moved between
the first and last runs. A backend with a single run in the
window has no comparison; a first or last run recorded before
0.43.0, one recorded with no per-op latencies, or one whose
recorded histograms are malformed shows a note naming the reason
instead of a comparison.

A sweep run recorded with `--history` also records each sweep
step — one ops/sec and p99 per keyspace size, client count, value
size, pipeline depth and read:write ratio, plus the step's
per-operation latency histograms — and below the per-op sections each backend gets one
trend section per step that ran, with the same table, delta,
sparkline and first-to-last change as the overall chart, plus a
p99 first-to-last change per step, and the step's per-operation
latency histograms from the latest run that recorded it, drawn
with the same shared helper as the latest run's block, and the
step's first-to-last per-op histogram comparison — each op's
p50/p99 in the first and last runs that recorded the step, the
p99 change, and the bucket drift, built from the same shared
sweep-step rows the best/worst view already uses, and the first
run's per-op histogram block next to the comparison table,
drawn with the same shared helper as the latest run's block, so
the two shapes can be read side by side — a keyspace
step's ops/sec at 100 keys is only ever compared with the same
step in other runs, so a step that slows down reads at a glance
even when the overall ops/sec moved the other way, and a step
whose tail crept up reads at a glance even when its ops/sec
moved the other way, and a p99 move in a step names the op whose
latency shape moved, and shows how that shape moved between the
first and last runs. Runs recorded before 0.37.0 carry no sweep
data and simply show no sweep sections; a step that never ran in
the window gets no section at all. A step recorded before 0.49.0
carries no per-op histograms, a step recorded with no per-op
latencies carries an empty mapping, and a step whose recorded
histograms are malformed carries something else — each renders a
note naming the reason instead of a histogram block or a
comparison table, and a step recorded in only one run in the
window renders a note instead of a comparison table, and no
first-run block — a single run's first shape is the latest
run's block just above, so there is nothing more to show.

With `--trend-out PATH` the same chart is also written to a
Markdown file — the exact text printed to stdout — so the trend
can be committed next to a report or attached to one instead of
being re-rendered from the history file each time. The parent
directory is created when needed, and `--trend-out` without
`--trend` is rejected (exit 2), since there is no chart to
write without it.

With `--trend-json PATH` the same trend is also written as JSON
— one entry per backend under `backends`, with the runs in the
window (timestamp, ops/sec, the change versus the previous run,
and p99), the sparkline, the first-to-last change in ops/sec and
in p99, the same table per operation type that ran under
`per_op` — each op's section also carrying its own
first-to-last per-op histogram comparison under
`histogram_comparison` (that op's p50/p99 in the first and
last runs that ran it, the p99 change, and the bucket drift,
or a note naming why there is none), and the first run's
histograms for that op under `first_histograms_by_op`,
present exactly when the Markdown chart shows a first-run
block for the op — the latest run's
per-operation latency histograms under
`histograms_by_op` (or a note under `notes` naming why there are
none), and the per-op histogram comparison between the first and
last runs under `histogram_comparison` — plus one per-step trend
section per sweep step that ran in the window under
`sweep_trends`: the step's runs table, sparkline and first-to-last
change in ops/sec and in p99, and the step's per-op latency
histograms from the latest run that recorded it (or a note under
`notes` naming why there are none), and the step's first-to-last
per-op histogram comparison under `histogram_comparison` (each
op's p50/p99 in the first and last runs that recorded the step,
the p99 change, and the bucket drift, or a note naming why there
is none), and the first run's per-op histograms under
`first_histograms_by_op` — the same shapes the Markdown chart
draws next to the comparison table, present exactly when the
chart shows a first-run block — the same per-step trend sections the Markdown chart
renders, built from the same shared helper, so a script reading
the trend JSON sees what the chart shows for the same step — plus one per-step sweep section per sweep step that ran in the window under `sweeps`:
the step's best and worst runs by the step's own ops/sec, the
percentage changes between them, and the step's per-op latency
histograms from both runs — or notes naming why a pick has none
— plus the step's per-op histogram comparison under
`histogram_comparison` on each sweep entry, with `rows` carrying
the same per-op numbers or `notes` naming why there is none —
the same per-step sections `--best-worst-json` carries, built
from the same shared helper, so a script reading the trend JSON
sees what the best/worst views show for the same step. Like
`--trend-out`, `--trend-json` without `--trend` is rejected
(exit 2).

With `--best-worst` the same history renders as a best-run vs
worst-run comparison, one section per backend: a table with the
best and worst runs' ops/sec and p99 side by side and the
percentage change between them, so a backend whose throughput
swings run to run reads at a glance — a wide spread says the
runs vary a lot, a narrow one says the backend is steady. Each
backend also gets the same table per operation type (SET, GET,
INCR, DEL) that ran, picked independently by that op's ops/sec, so a
slow-write run that still won overall still shows up as the
worst SET run. Each picked run also carries its per-operation
latency histograms — the best and worst runs' full per-op
histogram blocks under the overall table, and each per-op
pick's own histogram under that op's table — the same
log-spaced buckets as the regular run's `histograms_by_op`,
drawn with the same shared helper as the single-run report
and the `--compare` output, so a p99 move names the op whose
latency shape moved. Each per-op section also carries its own
per-op histogram comparison table — that op's p50 and p99 in
its worst and best runs, the p99 change, and the bucket drift
— built from the same shared rows as the overall comparison's
row for that op, so the per-op numbers agree with the overall
table, and a note naming the reason instead when the op's
best and worst runs are the same run, or a run was recorded
before 0.43.0, with no per-op latencies, or with malformed
histograms. Below the best and worst runs' histogram
blocks each backend also gets a per-op histogram comparison
between the two runs — a table with each op's p50 and p99 in
the worst and best runs, the p99 change, and the bucket drift
(the largest single-bucket share move, in percentage points,
with the bucket it moved in) — the same table the `--trend`
view renders for its first and last runs, built from the same
shared rows, so a p99 move between the runs shows how each
op's latency shape moved. History records carry the histograms
since 0.43.0; runs recorded before that show a note instead of
histogram blocks. A backend with a single recorded run has that run
as both best and worst, with 0.0% deltas and no histogram
comparison, and a best or worst run recorded before 0.43.0,
one recorded with no per-op latencies, or one whose recorded
histograms are malformed renders a note naming the reason
instead of a comparison table. Below the per-op sections each
backend also gets one best-vs-worst section per sweep step that
ran — the best and worst runs for that step by the step's own
ops/sec, with the step's per-operation latency histograms from
both runs, drawn with the same shared helper as the `--trend`
view's per-step blocks, so the two views cannot disagree. The
step's best and worst runs are picked independently of the
overall pick, so a step that slowed down in the run that won
overall still shows its own shape. Each step's section ends
with a per-op histogram comparison between its best and worst
runs — each op's p50 and p99 in the two steps, the p99 change,
and the bucket drift — built from the same shared rows as the
overall comparison, so a p99 move in a step shows how each
op's latency shape moved. A step record carries no per-op
percentile fields, so the step's p50 and p99 are read off its
histogram buckets (the upper edge of the bucket holding the
percentile rank). A step recorded before
0.49.0 carries no per-step histograms, a step recorded with no
per-op latencies carries an empty mapping, and a step whose
recorded histograms are malformed carries something else — each
renders a note naming the reason instead of a block, and a step
recorded in only one run renders a note instead of a
comparison table. With
`--compare-same-workload` only runs whose workload matches the
latest recorded run's are considered — the same
apples-to-apples rule `--compare-history` uses, so a quick smoke
run never becomes the worst run next to a full-size one — and
the header names the workload that was matched.
`--best-worst-out PATH` writes the same comparison to a Markdown
file (the exact text printed to stdout), and
`--best-worst-json PATH` writes it as JSON (best and worst run
per backend and per op, with ops/sec, p99, the deltas, and the
per-op histograms — `histograms_by_op` on the best and worst
runs, `histogram` on each per-op pick — plus each per-op
section's own histogram comparison under
`per_op.<OP>.histogram_comparison`, with `rows` carrying that
op's p50/p99 in its worst and best runs, the p99 change and the
bucket drift, or `notes` naming why there is none — plus the per-op
histogram comparison under `histogram_comparison`, with `rows`
carrying each op's p50/p99 in the worst and best runs, the p99
change and the bucket drift, or `notes` naming why there is
none — plus one per-step sweep section per sweep step that ran
under `sweeps`: the step's best and worst runs by the step's
own ops/sec, the deltas, and the step's per-op histograms from
both runs — `histograms_by_op` on the best and worst step
picks — or `notes` naming why a pick has none — plus the
step's per-op histogram comparison under `histogram_comparison`
on each sweep entry, with `rows` carrying the same per-op
numbers or `notes` naming why there is none, so a script
reading the JSON sees what the Markdown table shows), so a
script can read the spread without parsing the table. Both need
`--best-worst` (exit 2 otherwise), like `--trend-out` needs
`--trend`.

With `--prune` old runs are removed from the history file, per
backend, so a history that grows by a run a day stays small
enough to read: `--prune-keep N` keeps the N most recent runs
of each backend and drops the rest, `--prune-before TIMESTAMP`
drops runs older than that ISO timestamp (a timestamp with no
timezone is read as UTC, the same rule `--compare-history`
uses), and both may be given — a run must pass both to stay.
`--prune-backend NAME` (repeatable) limits the prune to the
named backends; other backends are left alone entirely. A
prune never empties a backend on its own: a selection that
would remove every recorded run of a backend is refused (exit
2) unless `--prune-all` is given — the explicit flag for
clearing a backend's history out entirely, which cannot be
combined with `--prune-keep` or `--prune-before`. With
`--dry-run` the same selection is computed and printed — one
line per backend with how many runs would be removed, the
timestamp range of those runs, and how many would be kept —
but the file is not changed. The prune rewrites the file
atomically (temp file + rename) and keeps the surviving runs in
their original order; corrupt lines are dropped on rewrite,
since loading already skips them, and a run whose timestamp
cannot be parsed is kept by `--prune-before` — its age cannot
be known, so the prune does not guess.

With `--history-export PATH` the same history is written out as
portable JSON — one document with a `records` list, the tool and
version that wrote it, and the export time — so a history can
move between machines: `--history-export-backend NAME`
(repeatable) exports only that backend's runs, and an empty
history exports an empty records list. With `--history-import
PATH` an export is merged back into the history file
(`--history-file`, default `~/.kvbench/history.jsonl`): records
merge by (backend, timestamp), so a record already in the
history with identical content is skipped as a duplicate, a
record with the same backend and timestamp but different
content is a conflict — the existing record is kept and the
imported one dropped, so an import never overwrites a recorded
run — and new records are appended in export order. The import
prints how many runs were added, skipped as duplicates, and
kept over conflicts. A missing, unreadable or malformed export
file — not JSON, no `records` list, or a record with no backend
or timestamp — fails the run (exit 2) before the history file
is touched, so a bad import never half-merges.

## Comparing against a previous run

With `--compare-file PATH` the run is compared against a previous
run's JSON report (the `--json` output of an earlier run, usually
the last one): each backend is matched by name, and both reports
gain a Comparison with previous run section with ops/sec and p99
side by side, the percentage change of each, and a verdict per
backend — `faster`, `slower`, `about the same` (within 1%), or
`regression` when a threshold was given and passed. Each compared
backend also carries a per-operation p99 / ops-sec delta table —
one row per op that ran, with the current and baseline ops/sec and
p99 and the percentage change of each — under
`comparison.entries[].per_op` in JSON, so a p99 move names the op
that moved, not just that the p99 moved. Each compared backend
also carries a best/worst op section — the op whose ops/sec rose
most and the op whose p99 fell most, and the other end of each —
under `comparison.entries[].best_worst` in JSON and as a table
under the per-op table in Markdown, built from the same per-op
deltas, so the two views cannot disagree and the op that moved
most reads at a glance. Each best/worst op also carries its
per-operation latency histogram from both runs — the current
run's buckets under `histogram` and the baseline run's under
`baseline_histogram` on each pick — drawn as now/prev blocks
under the table in Markdown, so the op that moved most also
shows how its latency shape moved; the same op picked on both
measures renders once, and a baseline that carries no per-op
histograms renders only this run's shape. Below the histogram
blocks the best/worst section also carries a per-op histogram
comparison table for the picked ops — each op's p50 and p99 in
both runs, the p99 change, and the bucket drift — under
`comparison.entries[].best_worst.histogram_comparison` in JSON,
built from the same shared pick dicts, so the shape move reads
as numbers as well as bars; a baseline that carries no per-op
histograms renders a note naming the reason instead of a table,
and the per-op delta rows carry each side's p50 as well, so the
table's numbers agree with the delta table just above it. A comparison with a single op, or two
identical runs, carries a note saying so instead of a spread, and
a baseline that carries no per-op stats carries a note saying
there is no best or worst op. A history baseline
compares per op the same way when its record carries per-op stats
(records since 0.22.0 do); a baseline that carries none still
shows this run's per-op numbers with n/a deltas, and a note
saying why the baseline's are missing. Each compared
backend also carries its per-operation latency histograms — the
current run's, and the baseline run's when the baseline report
carries them (JSON reports since 0.32.0 do) — under
`comparison.entries[].histograms_by_op` and
`baseline_histograms_by_op` in JSON, and as blocks under the
comparison table in Markdown, so a p99 move names the op whose
latency shape moved. Backends in
only one of the two runs are named rather than silently dropped,
and when the workload differs from the baseline run's (ops,
clients, keys, mix, pipeline, ...) the reports warn that the
comparison is approximate, so a comparison only means something
when both runs used the same workload. The JSON report carries the
same comparison under `comparison`, with the baseline file, its
generation time, the workload differences, and the per-backend
entries.

With `--regression-threshold PCT` the comparison becomes a gate:
the run exits 3 when any backend's ops/sec fell by more than PCT
percent, or its p99 rose by more than PCT percent, versus the
baseline run — after the reports are written, so the regression
is visible in them. A missing or unreadable baseline file fails
the run before any benchmark starts. `--regression-threshold`
without `--compare-file` or `--compare-history` is rejected.
Small runs are noisy on a shared machine, so use a threshold well
above run-to-run jitter for the workload you gate on.

With `--compare-history SELECTOR` the baseline comes from the
history file (`--history-file`, default
`~/.kvbench/history.jsonl`) instead of a JSON report: one recorded
run per backend is picked out of the history and compared the same
way — same side-by-side table, same verdicts, same regression
gate, same workload-difference warning. The selector is per
backend, since a run is recorded once per backend:

- `latest`, or a negative index: count back from the most recent
  run of that backend (`-1` is the latest, `-2` the one before).
- a non-negative index: count forward from the oldest run (`0`
  is the first run ever recorded for that backend).
- an ISO timestamp (`2026-11-22T15:00:00Z`, or `2026-11-22
  15:00` — a timestamp with no timezone is read as UTC): the
  latest run of that backend at or before that time.

A backend with no run matching the selector — an index past how
many runs it has, or a timestamp before its first run — is left
out of the comparison rather than invented, and shows up as an
unmatched backend in the reports; a selector that matches no
backend at all fails the run before any benchmark starts, as does
a missing history file or an invalid selector. The reports name
the history file and the selector as the baseline, and the JSON
report carries the baseline under `comparison` with
`"source": "history"` (a report-file baseline carries
`"source": "report"`). `--compare-file` and `--compare-history`
together are rejected — pick one baseline. With
`--compare-history-json PATH` the same comparison is also
written as a standalone JSON file — the exact `comparison` dict
the run's JSON report carries, written through the shared helper
in `compare.py`, so the file and the report cannot disagree; a
script can read the comparison without parsing the report. Like
`--trend-json`, it needs its mode: without `--compare-history`
it is rejected (exit 2). The same export exists for a
report-file baseline: `--compare-file-json PATH` writes that
comparison the same way, through the same shared helper, and
needs `--compare-file` (exit 2 without it).

With `--compare-same-workload` the selector only sees recorded
runs whose workload matches this run's (same ops, clients, keys,
mix, pipeline, distribution and seed — the same settings the
workload-difference warning checks), so the baseline per backend
is the latest run that measured the same workload. The selector
then counts within the matching runs only: `-1` is the latest
matching run, `0` the oldest matching run, and a timestamp picks
the latest matching run at or before that time. A history file
that mixes workloads — a quick 100-op smoke run recorded next to
the nightly 20k-op run — no longer makes the regression gate
compare against the wrong run; without the flag the latest run
wins whatever its workload was, and the reports warn. When no
recorded run has the same workload the run fails before any
benchmark starts, and the reports name the baseline as e.g.
`history.jsonl (history run -1, same workload)`. The flag needs
`--compare-history`; on its own it is rejected.

## SLO gate

With `--slo-p99-ms MS` the run is gated on absolute latency: any
backend whose measured p99 is above MS milliseconds breaches the
gate. With `--slo-error-rate PCT` it is gated on reliability: any
backend whose error rate — failed ops as a share of all ops
attempted — is above PCT percent breaches it. Either flag on its
own is a gate; both together gate on both, and a backend only
needs to breach one of them to fail. Exactly at the threshold
still passes, so a threshold of 0 only fails when something
actually went wrong.

With `--slo-op OP:P99,ERR` (repeatable) each operation type gets
its own budgets on top of the global gate: `SET:5,0.1` gates SET
ops on p99 5 ms and error rate 0.1%, so a slow write fails the run
even when the overall p99 is fine. Either threshold may be left
empty (`SET:5,` gates p99 only, `SET:,0.1` gates the error rate
only); a later entry for the same op replaces the earlier one.
An op that never ran in the mix has no per-op verdict — there is
nothing to gate.

Both reports carry the gate: the Markdown report gains an SLO
gate section with the thresholds, the measured p99 and error rate
per backend, and a `pass`/`breach` verdict, plus a per-operation
table with the same verdicts per op when `--slo-op` was given,
and the JSON report carries the same gate under `slo` (with the
thresholds also recorded under `workload`, and the per-op entries
under each backend's `per_op`).

With `--slo-preset PATH` the thresholds come from a JSON file
instead of the command line, so one budget can be shared across
runs and checked into CI:

```json
{
  "p99_ms": 1,
  "error_rate_pct": 1,
  "per_op": {
    "SET": {"p99_ms": 5, "error_rate_pct": 0.1},
    "GET": {"p99_ms": 2}
  }
}
```

Every key is optional — a preset may set only the global p99, only
per-op thresholds, or any combination — and a per-op entry may set
either threshold or both, but at least one. The same ranges apply
as on the command line: p99 non-negative, error rate between 0
and 100. CLI flags override the preset: `--slo-p99-ms` replaces
the preset's global p99, `--slo-error-rate` its error rate, and
`--slo-op` replaces the preset's entry for that op, leaving the
rest of the preset in place. Both reports name the preset file
(`workload.slo_preset` and `slo.preset` in JSON, a line in the
Markdown SLO gate section), so a report shows where its budgets
came from. A missing, unreadable, invalid, or out-of-range preset
file fails the run with exit 2 before any benchmark starts. The gate is evaluated after the
reports are written, so a breached run still leaves a report
showing the verdict — and then the process exits 4, a distinct
code from the regression gate's 3. A run that breaches both gates
exits 4: the SLO gate is the run's own explicit budget, so it
takes precedence. Negative thresholds are rejected, and the error
rate must be between 0 and 100.

Unlike the regression gate, the SLO gate needs no baseline file:
it checks the run against budgets you choose, so it works on a
first run and in CI, where the question is not "did we get
slower than last time" but "is this fast and reliable enough to
ship".

## Continuous benchmarking

`.github/workflows/bench.yml` runs kvbench against cinder on every
push to this repo (and on manual dispatch): it checks out cinder,
builds it with `make`, starts it on port 6379, and runs

```sh
python3 -m kvbench.cli --host 127.0.0.1 --port 6379 \
    --ops 20000 --clients 4 --slo-preset slo.json --output report.md
```

The SLO gate is the gate: kvbench exits 4 when a backend breaches
the preset, which fails the workflow, so a push that makes cinder
too slow or too error-prone goes red. Both reports are uploaded as
the `kvbench-report` artifact even when the gate fails, so a failed
run still leaves its report behind to look at.

The budgets live in `slo.json`, the checked-in preset: global p99
at most 5 ms, error rate at most 1%, SET p99 at most 10 ms and
GET p99 at most 5 ms (each with error rate at most 0.1%). The
numbers are deliberately loose — about 4x the p99 measured on a
dev VM (~1 ms), where run-to-run jitter alone moves p99 between
0.8 and 1.2 ms — so the gate catches a real regression, not
runner noise. Tighten them only from numbers measured on the
runner itself.

The workflow itself cannot be executed end-to-end from a dev VM
(there is no GitHub Actions runner there); the exact commands it
runs are verified locally instead — build cinder, start it, run
the command above — and `tests/test_workflow.py` checks the
workflow file parses, triggers on push, checks out the real
cinder repo, and passes only flags the real CLI parser accepts.
To run what CI runs, locally:

```sh
# in the cinder repo
make
./cinder --port 6379 --data-dir ./data --aof-fsync everysec &
# in this repo
python3 -m kvbench.cli --host 127.0.0.1 --port 6379 \
    --ops 20000 --clients 4 --slo-preset slo.json --output report.md
```

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

Each backend also gets a per-operation latency histogram section:
the same log-spaced buckets drawn separately for each operation
that ran (SET, GET, INCR, DEL), each scaled to its own largest
bucket, so a slow op type's latency shape shows separately from the
overall distribution instead of being swamped by the fast ops. The
JSON report carries the same buckets per result under
`histograms_by_op`.

With `--sweep-keyspace` the report has a Keyspace sweep section
right below the results table: one table per backend with a row per
swept size — keys, ops, ops/sec, mean and p99 — so the effect of
keyspace size on throughput reads at a glance. Each swept size also
gets its own per-operation latency histogram block under the table,
with the same log-spaced buckets per op as the regular run's
per-op histograms, so a size where one op type slows down names
the op. The JSON report
carries the same rows per result under `keyspace_sweep`, with the
per-op buckets per step under `histograms_by_op`, and the
workload section carries the swept sizes under `sweep_keyspace`.

With `--sweep-clients` the report has a Client sweep section right
below the Keyspace sweep (or the results table, when there is no
keyspace sweep): one table per backend with a row per swept count
— clients, ops, ops/sec, mean and p99 — so the effect of
concurrency on throughput reads at a glance. Each swept count also
gets its own per-operation latency histogram block under the table.
The JSON report
carries the same rows per result under `client_sweep`, with the
per-op buckets per step under `histograms_by_op`, and the
workload section carries the swept counts under `sweep_clients`.

With `--sweep-value-size` the report has a Value size sweep section
right below the Client sweep (or the Keyspace sweep, or the results
table, when there is no client sweep): one table per backend with a
row per swept size — value size in bytes, ops, ops/sec, mean and
p99 — so the effect of value size on throughput reads at a glance:
a store that copies values on every SET slows as values grow, while
one that only moves pointers does not. Each swept size also gets
its own per-operation latency histogram block under the table. The JSON report carries the
same rows per result under `value_size_sweep`, with the
per-op buckets per step under `histograms_by_op`, and the
workload section carries the swept sizes under `sweep_value_size`.

With `--sweep-pipeline` the report has a Pipeline sweep section
right below the Value size sweep (or the Client sweep, the
Keyspace sweep, or the results table, when there is no value-size
sweep): one table per backend with a row per swept depth —
pipeline depth in commands per round trip, ops, ops/sec, mean and
p99 — so the effect of pipelining on throughput reads at a glance:
where batching more commands per round trip stops helping. Each
swept depth also gets its own per-operation latency histogram block
under the table. The JSON report carries the same rows per result
under `pipeline_sweep`, with the per-op buckets per step under
`histograms_by_op`, and the workload section carries the swept
depths under `sweep_pipeline`.

With `--sweep-mix` the report has a Mix sweep section right below
the Pipeline sweep (or the Value size sweep, the Client sweep, the
Keyspace sweep, or the results table, when there is no pipeline
sweep): one table per backend with a row per swept ratio — mix as
a read:write ratio, ops, ops/sec, mean and p99 — so the effect of
the read:write mix on throughput reads at a glance: where a
store's writes start costing more than its reads. Each swept ratio
also gets its own per-operation latency histogram block under the
table. The JSON report carries the same rows per result under
`mix_sweep`, with the per-op buckets per step under
`histograms_by_op`, and the workload section carries the swept
ratios under `sweep_mix`.

With `--repeat N` and any sweep, each sweep step runs N times and
its table row shows the median ops/sec and p99 over the runs, with
a per-step repeat block below the table listing each run's ops/sec
and p99 and the spread of each as a percentage of its median, plus
the step's fastest-vs-slowest repeat-run histogram comparison —
the step's fastest and slowest runs by ops/sec, their overall
p50/p99 and the p99 change between them, and a per-op table with
each op's p50 and p99 in both runs, the p99 change and the bucket
drift, built from the same shared rows the single-run repeat
comparison uses, so a noisy step shows which op's latency shape
moved; a step whose runs are not comparable renders a note naming
the reason instead of a table. The JSON report carries the same
per-run values and spread under each step's `repeat` key (`runs`
and `spread`, the same shapes as the main run's `repeat`), plus
the comparison under each step's `repeat.histogram_comparison`.
Each step's repeat block also carries a per-op fastest-vs-slowest
comparison for each op that ran — the op's own fastest and
slowest runs by that op's ops/sec, with the op's own histogram
comparison table, so a step where one op's runs swing while the
overall winner stays steady still names the op — under
`repeat.histogram_comparison.per_op` in JSON, with the picked
runs' bucket lists for the op under `fastest_histogram` and
`slowest_histogram`, and the Markdown per-op section draws the
two runs' histogram blocks for the op next to the table from
those same bucket lists; an op whose runs are not comparable renders
a note naming the reason instead of a table, and no blocks.
A sweep with `--repeat 1` keeps the old shape exactly — no
`repeat` key on any step.

With `--repeat N` the report has a Repeat runs section: one table
per backend with a row per run — ops, ops/sec, mean and p99 — so
the spread behind the reported medians reads at a glance. Below
the table a spread summary table lists, for ops/sec and p99
overall and per operation, the min, median, max, spread (max -
min) and spread as a percentage of the median, so a run that
swings wildly reads at a glance; a single run has no spread, so
the table is absent for `--repeat 1`. Each run also gets its own
per-operation latency histogram block under the table, with the
same log-spaced buckets per op as the regular run's per-op
histograms, so a run where one op type slows down names the op.
Below the histogram blocks a fastest-vs-slowest comparison
compares the fastest and slowest repeat runs by ops/sec — the
two runs' overall p50 and p99 and the p99 change between them,
plus a per-op table with each op's p50 and p99 in both runs,
the p99 change and the bucket drift, built from the same shared
rows the trend and best/worst views use, so a noisy repeat run
shows which op's latency shape moved; a single repeat run, or
runs whose ops/sec are all identical, renders a note naming
the reason instead of a comparison. Below the comparison each
op that ran gets its own fastest-vs-slowest section — the op's
own fastest and slowest runs by that op's ops/sec, with the
op's own histogram comparison table, so a repeat run where one
op's runs swing while the overall winner stays steady still
names the op — plus the fastest and slowest runs' histogram
blocks for that op alone next to the table, drawn with the same
shared helper from the bucket lists the comparison carries under
`fastest_histogram` and `slowest_histogram` for the op, so the
shape move reads as bars as well as numbers; an op whose runs are
not comparable renders a note naming the reason instead of a
table, and no blocks. The JSON report
carries the same rows per result under `repeat.runs`, with the
per-op buckets per run under `histograms_by_op`, the spread
summary under `repeat.spread`, and the comparison under
`repeat.histogram_comparison`, with the per-op comparisons
under `repeat.histogram_comparison.per_op`.

Below the table, each backend gets a per-operation stats table:
ops, ops/sec, mean, p50, p95 and p99 latency split by SET, GET,
INCR and DEL. Ops that never ran (e.g. INCR in an `80:20` read:write mix)
are left out. The JSON report carries the same split per result
under `per_op`.

Below that, each backend gets a tail-latency attribution section:
a finding naming the factor the slowest ops come from — one
operation type, one client, hot or cold keys, or a single slow
second — or that no single factor dominates the tail, plus the
tables the finding is drawn from: per-op p99 and slow-op share,
per-client p99 and slow-op share, hot vs cold p99 and slow-op
share, and whether slow ops cluster in one second or spread over
the run. Slow ops are the ops at or above the overall p99
latency. A factor only counts as dominant when slow ops land on
it far more often than its share of all ops would predict (at
least 10 points more) and it is genuinely slower than the rest of
its dimension (p99 at least twice the p99 of everything else in
that dimension, over at least 10 samples). The JSON report carries
the same breakdown per result under `tail`.

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

With `--csv PATH` the same per-second series is also written as a
plain CSV, one row per backend per second: `backend,second,ops,
ops_per_sec,mean_ms,max_ms`. Each row covers exactly one second, so
`ops_per_sec` is the ops completed in that second; a row with 0 ops
is a stall second. The file plots directly — ops/sec over time in a
spreadsheet or with matplotlib — without parsing the Markdown
report. The CSV is opt-in only; without `--csv` no CSV file is
written.

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

- Best/worst op section in the `--compare` output
  (`--compare-file` / `--compare-history`): each compared
  backend carries a best/worst op section — the op whose ops/sec
  rose most and the op whose p99 fell most, and the other end of
  each — under `comparison.entries[].best_worst` in JSON and as
  a table under the per-op delta table in Markdown, built by one
  shared helper in `compare.py` from the entry's own per-op
  deltas, so the JSON and Markdown views cannot disagree and
  the op that moved most reads at a glance; the
  `--compare-file-json` export carries it too, since it writes
  the same comparison dict. A comparison with a single op, or
  two identical runs, carries a note saying so instead of a
  spread, an op the baseline never ran is left out of the picks
  with a note naming it, and a baseline that carries no per-op
  stats carries a note saying there is no best or worst op.
  Done in 0.58.0. Each best/worst op also carries its
  per-operation latency histogram from both runs — the current
  run's buckets under `histogram` and the baseline run's under
  `baseline_histogram` on each pick, drawn as now/prev blocks
  under the best/worst table in Markdown, built by the same
  shared helper so the JSON and Markdown views cannot disagree
  and the op that moved most also shows how its latency shape
  moved; the same op picked on both measures renders once, and
  a baseline that carries no per-op histograms (a report before
  0.32.0, or a history baseline) renders only this run's shape.
  Done in 0.59.0. A per-op histogram comparison table for the
  best/worst ops — each picked op's p50/p99 in both runs, the
  p99 change and the bucket drift, like the trend view's
  histogram comparison — under the best/worst histogram blocks
  in Markdown and under `comparison.entries[].best_worst.
  histogram_comparison` in JSON, built from the same shared
  pick dicts so the JSON and Markdown views cannot disagree
  and the shape move reads as numbers as well as bars; the
  per-op delta rows also carry each side's p50 now, so the
  table's numbers agree with the delta table just above it,
  and a baseline that carries no per-op histograms (a report
  before 0.32.0, or a history baseline) renders a note naming
  the reason instead of a table. The `--compare-file-json`
  export carries it too, since it writes the same comparison
  dict. Done in 0.60.0. A per-op histogram comparison table in
  the Markdown per-op trend sections — each op's section ends
  with its first-to-last comparison, that op's p50/p99 in the
  first and last runs that ran it, the p99 change and the bucket
  drift, rendered from the same shared rows the trend JSON
  carries for the op, so a reader of the chart sees the per-op
  numbers without parsing JSON and the two views cannot
  disagree; a section whose op ran in only one run in the
  window, or whose first or last run that ran the op was
  recorded before 0.43.0, recorded no per-op latencies, or
  carries malformed histograms, renders a note naming the
  reason instead of a table. Done in 0.61.0. A per-op histogram
  comparison table in each sweep-step trend section — each step's
  section ends with its first-to-last comparison, each op's
  p50/p99 in the first and last runs that recorded the step, the
  p99 change and the bucket drift, rendered from the same shared
  sweep-step rows the best/worst view already uses, so the chart
  and the trend JSON (under `histogram_comparison` in each
  `sweep_trends` entry) cannot disagree; a step recorded in only
  one run in the window, or whose first or last run that recorded
  it was recorded before 0.49.0, recorded no per-op latencies, or
  carries malformed histograms, renders a note naming the reason
  instead of a table. The step's p50 and p99 are read off its
  histogram buckets (the upper edge of the bucket holding the
  percentile rank), and the JSON flags that with
  `percentiles_from_histograms`. Done in 0.62.0. Each sweep-step
  trend section also carries the first run's per-op histogram
  block next to the comparison table, drawn with the same shared
  helper as the latest run's block, so the two shapes can be read
  side by side next to the comparison table, like the overall
  trend's first-run block sits next to its comparison — and the
  trend JSON carries the same first run's histograms under
  `first_histograms_by_op` in each `sweep_trends` entry, present
  exactly when the chart shows a first-run block, so a script
  reading the JSON sees what the chart shows. A step recorded in
  only one run in the window carries no first-run block and no
  field — its first shape is the latest run's block just above.
  Done in 0.63.0. Each per-op trend section also carries the
  first run's per-op histogram block for that op next to its
  comparison table — that op's buckets from the first run that
  ran it, drawn with the same shared helper as the latest run's
  block, so that op's two shapes can be read side by side next
  to its comparison, like each sweep-step section's first-run
  block sits next to its own comparison — and the trend JSON
  carries the same first run's histograms under
  `first_histograms_by_op` in each `per_op` section, present
  exactly when the chart shows a first-run block for the op, so
  a script reading the JSON sees what the chart shows. A
  section whose op ran in only one run in the window carries no
  first-run block and no field — its first shape is the latest
  run's block — and a first or last run recorded before 0.43.0,
  with no per-op latencies, or with malformed histograms
  renders a note naming the reason instead of a comparison,
  and no first-run block; an op that ran only in the last run
  still compares, but carries no first shape to draw. Done in
  0.64.0. Next: a per-op histogram comparison table in each
  per-op best/worst section — that op's p50/p99 in its best and
  worst runs, the p99 change and the bucket drift, rendered
  from the same shared rows the best/worst JSON carries for the
  op, so a reader of the comparison sees the per-op numbers
  without parsing JSON and the two views cannot disagree. Done
  in 0.65.0. Next: the same per-op histogram comparison in the
  repeat-run section of a single-run report — the fastest vs
  slowest repeat run's p50/p99 per op, the p99 change and the
  bucket drift, from the same shared rows, so a noisy repeat
  run shows which op's latency shape moved. Done in 0.66.0.
  Next: the same fastest-vs-slowest comparison in each
  sweep-step repeat block — a step that ran `--repeat` more
  than once compares its own fastest and slowest runs the same
  way, from the same shared rows. Done in 0.67.0.
  Next: the fastest and slowest runs' per-op histogram blocks
  next to each sweep-step repeat comparison table — the two
  runs' latency shapes drawn with the same shared helper as
  the step's own per-op blocks, so the shape move reads as
  bars as well as numbers; the comparison also carries the two
  runs' per-op histograms under `fastest_histograms_by_op`
  and `slowest_histograms_by_op` in JSON, the same maps the
  Markdown blocks draw, present exactly when the blocks show
  — and the main repeat section's comparison carries them too,
  since it shares the helper, though it draws no dedicated
  blocks because it already draws every run's per-op block. A
  step whose runs are not comparable renders a note naming the
  reason instead, and no blocks. Done in 0.68.0.
  Next: a per-op fastest-vs-slowest repeat-run comparison in
  each sweep-step repeat block — each op's own fastest and
  slowest runs by that op's ops/sec, with its own histogram
  comparison table, so a step where one op's runs swing while
  the overall winner stays steady still names the op. Done in
  0.69.0.
  Next: the same per-op fastest-vs-slowest repeat-run
  comparison in the main repeat section — each op's own fastest
  and slowest runs by that op's ops/sec, with its own histogram
  comparison table, so a repeat run where one op's runs swing
  while the overall winner stays steady still names the op. Done
  in 0.70.0.
  Next: the fastest and slowest runs' histogram blocks for that
  op next to each per-op fastest-vs-slowest section — in the
  main repeat section and in each sweep-step repeat block — the
  two runs' latency shapes for the op alone, drawn with the
  same shared helper as the per-op blocks, from the bucket
  lists the comparison already carries under
  `fastest_histogram` and `slowest_histogram` for the op, so
  the shape move reads as bars as well as numbers next to the
  per-op table. Done in 0.71.0.
- `--compare-file-json PATH`: with `--compare-file`, also
  write the comparison as a standalone JSON file — the exact
  dict `compare_results` returns (the same one the run's JSON
  report carries under `comparison`), written through the same
  shared helper in `compare.py` as `--compare-history-json`,
  so the file cannot disagree with the report: same entries,
  same per-op deltas, same per-op histograms of both runs,
  same workload-match verdict and regression flags. A report
  baseline carries per-op histograms (reports since 0.32.0
  do), so those fields read exactly as they do in the report.
  Like `--compare-history-json`, the flag needs its mode
  (exit 2 without `--compare-file`). Done in 0.57.0.
- `--compare-history-json PATH`: with `--compare-history`, also
  write the comparison as a standalone JSON file — the exact
  dict `compare_results` returns (the same one the run's JSON
  report carries under `comparison`), written through one
  shared helper in `compare.py`, so the file cannot disagree
  with the report: same entries, same per-op deltas, same
  per-op histograms of both runs, same workload-match verdict
  and regression flags. A history baseline carries no per-op
  histograms, and a baseline recorded before 0.22.0 carries no
  per-op stats, so those fields read exactly as they do in the
  report instead of being filled in by the export. Like
  `--trend-json` and `--best-worst-json`, the flag needs its
  mode (exit 2 without `--compare-history`). Done in 0.56.0.
- Per-operation histogram comparisons in the `--trend-json`
  per-op sections: each op's section carries its own
  first-to-last comparison under `histogram_comparison` — that
  op's p50 and p99 in the first and last runs that ran it, the
  p99 change, and the bucket drift (the largest single-bucket
  share move, in percentage points, with the bucket it moved
  in) — built from the same shared rows as the overall
  first-to-last comparison, so the per-op numbers agree with
  the overall table's row for that op and a script reading the
  trend JSON sees how each op's latency shape moved without
  parsing the Markdown chart. A section whose op ran in only
  one run in the window, or whose first or last run that ran
  the op was recorded before 0.43.0, recorded no per-op
  latencies, or carries malformed histograms, carries a note
  naming the reason instead of rows. The Markdown per-op
  trend sections render the same comparison table since
  0.61.0. Done in 0.55.0.
- Per-sweep-step trend sections in the `--trend-json` output:
  each backend's `sweep_trends` list carries one entry per sweep
  step that ran in the trend window — the step's runs table
  (timestamp, ops/sec, the change versus the previous run that
  recorded the same step, and p99), the sparkline, the
  first-to-last change in ops/sec and in p99, and the step's
  per-op latency histograms from the latest run that recorded
  it — built from the same shared helper as the Markdown
  `--trend` view's per-step sections, so the JSON and Markdown
  views cannot disagree and a script reading the trend JSON
  sees what the chart shows for the same step. A step recorded
  before 0.49.0, one recorded with no per-op latencies, or one
  whose recorded histograms are malformed contributes a note
  naming the reason instead of a histogram map; a step with no
  numeric ops/sec in the window gets no section at all. Done in
  0.54.0.
- Per-sweep-step sections in the `--trend-json` output: each
  backend's `sweeps` list carries one entry per sweep step that
  ran in the trend window — the step's best and worst runs by
  the step's own ops/sec, the percentage changes between them,
  and the step's per-op latency histograms from both runs —
  built from the same shared helper as the `--best-worst-json`
  per-step sections, so the JSON and Markdown views cannot
  disagree and a script reading the trend JSON sees what the
  best/worst views show for the same step. Each sweep entry also
  carries the step's per-op histogram comparison under
  `histogram_comparison` (each op's p50/p99 in the worst and
  best runs' steps, the p99 change, and the bucket drift, or
  notes naming why there is none), and the trend JSON carries
  the rest of the chart too — the runs table, the sparkline, the
  first-to-last change in ops/sec and in p99, the per-op
  sections, the latest run's per-op histograms (or a note naming
  why there are none), and the first-to-last histogram
  comparison — so a script can read the trend without parsing
  the Markdown chart. A step recorded before 0.49.0, one
  recorded with no per-op latencies, or one whose recorded
  histograms are malformed contributes a note naming the reason
  instead of a histogram map. Done in 0.53.0.
- Per-sweep-step histogram comparisons in the `--best-worst`
  output: each sweep step's section ends with a per-op table
  comparing the step's best and worst runs — each op's p50 and
  p99 in the two steps, the p99 change, and the bucket drift
  (the largest single-bucket share move, in percentage points,
  with the bucket it moved in) — built from the same shared
  rows as the overall best-vs-worst comparison, so the views
  cannot disagree, and the comparison lands under
  `histogram_comparison` in each `--best-worst-json` sweep
  entry (`rows` with the per-op numbers, or `notes` naming why
  there is none), so a p99 move in a step shows how each op's
  latency shape moved. A step record carries no per-op
  percentile fields, so the step's p50 and p99 are read off its
  histogram buckets (the upper edge of the bucket holding the
  percentile rank), and the JSON flags that with
  `percentiles_from_histograms`. A step recorded in only one
  run has no comparison, and a step recorded before 0.49.0, one
  recorded with no per-op latencies, or one whose recorded
  histograms are malformed renders a note naming the reason
  instead of a table. Done in 0.52.0.
- Per-sweep-step best-vs-worst sections in the `--best-worst-json`
  output: each backend's `sweeps` list carries one entry per sweep
  step that ran — the step's best and worst runs by the step's own
  ops/sec, the percentage changes between them, and the step's
  per-op latency histograms from both runs — built from the same
  shared picks as the Markdown per-step sections, so the JSON and
  Markdown views cannot disagree and a script reading the JSON
  sees what the table shows. A step recorded before 0.49.0, one
  recorded with no per-op latencies, or one whose recorded
  histograms are malformed contributes a note naming the reason
  instead of a histogram map. Done in 0.51.0.
- Per-sweep-step best-vs-worst sections in the `--best-worst`
  output: each sweep step that ran gets its own best-vs-worst
  section below the per-op sections — the best and worst runs
  for that step by the step's own ops/sec, with the step's
  per-op latency histograms from both runs, drawn with the same
  shared histogram-block helper the `--trend` view's per-step
  blocks use, so the two views cannot disagree and a p99 move
  in a step names the op whose latency shape moved. The step's
  best and worst runs are picked independently of the overall
  pick, so a step that slowed down in the run that won overall
  still shows its own shape. A step recorded in only one run
  has that run as both best and worst, with 0.0% deltas; a step
  recorded before 0.49.0, one recorded with no per-op
  latencies, or one whose recorded histograms are malformed
  renders a note naming the reason instead of a block. Done in
  0.50.0.
- Per-operation latency histograms per sweep step in the
  `--trend` output: each sweep-step trend section ends with
  the step's per-op histograms from the latest run that
  recorded it — the same log-spaced buckets per operation as
  the main run's `histograms_by_op`, drawn with the same
  shared histogram-block helper as the latest run's block and
  the single-run report, so the views cannot disagree — so a
  p99 move in a step names the op whose latency shape moved.
  The step's histograms are recorded in the history record
  itself (JSON, under each step's `histograms_by_op`), and a
  step recorded before 0.49.0, one recorded with no per-op
  latencies, or one whose recorded histograms are malformed
  renders a note naming the reason instead of a block. Done in
  0.49.0.
- P99 first-to-last change in the overall and per-operation
  `--trend` sections: the same p99 first-to-last line the
  sweep-step sections gained in 0.47.0, now beside the ops/sec
  one in the overall chart and in each per-op trend section —
  the overall p99 and each op's p99 in the first and last runs
  in the window and the percentage change between them — built
  from the same recorded p99s the tables already show, through
  one shared helper that formats the line for the overall,
  per-op and sweep-step sections alike, so the views cannot
  disagree. A backend with a single run in the window, or runs
  whose recorded p99 is missing, show no p99 line. Done in
  0.48.0.
- P99 first-to-last change per sweep step in the `--trend`
  output: each sweep-step trend section gains a p99
  first-to-last line beside the ops/sec one — the step's p99
  in the first and last runs in the window and the percentage
  change between them — built from the same recorded per-step
  p99 the table already shows, so a step whose tail crept up
  reads at a glance even when its ops/sec moved the other way.
  A step with a single run in the window, or a step whose
  recorded p99 is missing, shows no p99 line. Done in 0.47.0.
- Per-operation histogram comparison in the `--best-worst`
  output: each backend's comparison gains a per-op table
  comparing its best and worst runs — each op's p50 and p99 in
  the worst and best runs, the p99 change, and the bucket drift
  (the largest single-bucket share move, in percentage points,
  with the bucket it moved in) — built from the same shared
  rows as the `--trend` view's first-to-last comparison, so the
  two views cannot disagree, and the comparison lands under
  `histogram_comparison` in the `--best-worst-json` output
  (`rows` with the per-op numbers, or `notes` naming why there
  is none), so a p99 move between the runs shows how each op's
  latency shape moved. An op that never ran in one of the runs
  shows `-` for that side; a backend with a single run has no
  comparison, and a best or worst run recorded before 0.43.0,
  one recorded with no per-op latencies, or one whose recorded
  histograms are malformed renders a note naming the reason
  instead of a table. Done in 0.46.0.
- Per-operation histogram comparison in each per-op `--best-worst`
  section: each op's section carries its own table — that op's
  p50 and p99 in its own worst and best runs, the p99 change,
  and the bucket drift — built from the same shared rows as the
  overall comparison, so the per-op numbers agree with the
  overall table's row for that op, and the comparison lands
  under `histogram_comparison` on each `per_op` entry in the
  `--best-worst-json` output (`rows` with the op's numbers, or
  `notes` naming why there is none), so a script reading the
  JSON sees what the Markdown table shows. A section whose
  op's best and worst runs are the same run, or whose best or
  worst run was recorded before 0.43.0, with no per-op
  latencies, or with malformed histograms, renders a note
  naming the reason instead of a table. Done in 0.65.0.
- Per-operation histogram comparison across the `--trend`
  first and last runs: each backend's trend ends with a per-op
  table comparing its first and last runs in the window — each
  op's p50 and p99 in both runs, the p99 change, and the bucket
  drift (the largest single-bucket share move, in percentage
  points, with the bucket it moved in) — plus the first run's
  per-operation latency histogram block, drawn with the same
  shared histogram-block helper as the latest run's block just
  above, so the two shapes can be read side by side and a p99
  move shows how each op's latency shape moved between the
  first and last runs. An op that never ran in one of the runs
  shows `-` for that side; a backend with a single run in the
  window has no comparison, and a first or last run recorded
  before 0.43.0, one recorded with no per-op latencies, or one
  whose recorded histograms are malformed renders a note naming
  the reason instead of a comparison. Done in 0.45.0.
- Per-operation latency histograms in the `--trend` output:
  each backend's trend ends with its latest run's per-operation
  latency histograms — the same log-spaced buckets per operation
  that ran as the regular run's `histograms_by_op`, drawn with
  the shared histogram-block helper so the trend view cannot
  disagree with `--best-worst`, `--compare` or the single-run
  output — so a p99 move in the trend names the op whose
  latency shape moved, not just that the p99 moved. The
  histograms are recorded in the history record
  (`histograms_by_op`) when a run is recorded with `--history`;
  a latest run recorded before 0.43.0 carries none, one
  recorded with no per-op latencies carries an empty mapping,
  and a hand-made record may carry a malformed one — each
  renders a note naming the reason instead of a histogram
  block. Done in 0.44.0.
- Per-operation latency histograms in the `--best-worst`
  output: each picked run carries the same log-spaced buckets
  per operation that ran as the regular run's
  `histograms_by_op` — the best and worst runs' full maps under
  `best` / `worst` (`histograms_by_op`) in JSON and as
  per-run histogram blocks under the overall table in Markdown,
  plus each per-op pick's own buckets under
  `per_op[OP].best` / `worst` (`histogram`) in JSON and as a
  histogram block under that op's table in Markdown — reusing
  the shared histogram-block helper so the best/worst view
  cannot disagree with `--compare` or the single-run output,
  so a p99 move names the op whose latency shape moved, not
  just that the p99 moved. The histograms are recorded in the
  history record (`histograms_by_op`) when a run is recorded
  with `--history`; runs recorded before 0.43.0 carry none and
  render a note instead of histogram blocks. The per-op
  histogram validator is shared with the compare view, so both
  views agree on what counts as a valid histogram. Done in
  0.43.0.
- Per-operation p99 / ops-sec deltas in the `--compare` output
  (`--compare-file` / `--compare-history`): each compared
  backend carries a per-op breakdown — one row per op that ran,
  with the current and baseline ops/sec and p99 and the
  percentage change of each — under
  `comparison.entries[].per_op` in JSON and as a per-backend
  table under the comparison table in Markdown, reusing the
  per-op stats and the delta machinery the aggregate comparison
  already uses, so a p99 move names the op that moved, not just
  that the p99 moved. A history baseline compares per op the
  same way when its record carries per-op stats (records since
  0.22.0 do); a baseline that carries none — a history record
  before 0.22.0, or a report written before per-op stats
  existed — still shows this run's per-op numbers with n/a
  deltas, and a note saying why the baseline's are missing. Done
  in 0.42.0.
- Per-operation latency histograms in the `--compare` output
  (`--compare-file` / `--compare-history`): each compared
  backend carries the same log-spaced buckets per operation
  that ran as the regular run's `histograms_by_op` — the
  current run's under `comparison.entries[].histograms_by_op`
  in JSON and as a per-backend histogram block under the
  comparison table in Markdown, plus the baseline run's under
  `baseline_histograms_by_op` when the baseline report carries
  them (JSON reports since 0.32.0 do), reusing the shared
  histogram-block helper so the comparison can't disagree
  with the single-run output — so a p99 move names the op
  whose latency shape moved, not just that the p99 moved. A
  history baseline carries no per-op histograms, and reports
  written before 0.32.0 carry none either, so those
  comparisons show the current run's histograms only, with a
  note saying why the baseline's are missing. Done in 0.41.0.
- `--sweep-mix` mode: a sweep mode that varies the read:write
  mix across steps — `--sweep-mix 100:0,90:10,50:50,10:90`
  re-runs the same workload once per ratio, like the existing
  sweeps vary key count, client count, value size and pipeline
  depth — recording per-step ops/sec and p99 in both reports,
  with a per-step per-operation latency histogram block,
  composing with `--repeat` (median per step, per-step repeat
  block), and flowing through the run history and trend views
  under `sweeps.mix`, so a mix step's ops/sec is only ever
  compared with the same step in other runs. Each ratio is a
  read:write split — reads are GETs, writes are SETs, no INCRs
  or DELs — and the ratios run read-heaviest first. A run with
  no mix sweep keeps the old shapes exactly, and runs recorded
  before 0.40.0 carry no mix steps. Done in 0.40.0.
- Workload config file (`--workload FILE`): declare the workload
  once in a JSON file (op mix, key count, value size, pipeline
  depth, duration, warmup, seed, timeout, repeat and all five
  sweeps — every key optional, absent keys keep the command-line
  defaults) so a benchmark can be checked in and re-run exactly,
  with a config file and command-line workload flags conflicting
  (exit 2) instead of silently merging, and the config path plus
  the resolved values recorded in the JSON report
  (`workload.workload_file`), the Markdown report's Workload
  section, and the history record, so a run is reproducible from
  its record. A run from a file and a run from the command line
  with the same resolved values are the same workload for
  `--compare-same-workload`. A run with no config file keeps the
  old shapes exactly, and runs recorded before 0.39.0 carry no
  `workload_file`. Done in 0.39.0.
- `--sweep-pipeline` mode: a sweep mode that varies the pipeline
  depth across steps — `--sweep-pipeline 1,5,10` re-runs the same
  workload once per depth, like the existing sweeps vary key count,
  client count and value size — recording per-step ops/sec and p99
  in both reports, with a per-step per-operation latency histogram
  block, composing with `--repeat` (median per step, per-step
  repeat block), and flowing through the run history and trend
  views under `sweeps.pipeline`, so a pipeline step's ops/sec is
  only ever compared with the same step in other runs. A run with
  no pipeline sweep keeps the old shapes exactly, and runs
  recorded before 0.38.0 carry no pipeline steps. Done in 0.38.0.
- Sweep steps in the run history and trend views: a sweep run
  recorded with `--history` also records each sweep step — one
  ops/sec and p99 per keyspace size, client count and value size —
  under `sweeps` in the history record (only the numbers the trend
  needs, not the per-op histograms or repeat detail, which stay in
  the JSON report), and `--trend` renders one section per step
  that ran, with the same table, delta, sparkline and first-to-last
  change as the overall and per-op trend tables, reusing the same
  helpers so the views cannot disagree — so a sweep step's ops/sec
  at 100 keys is only ever compared with the same step in other
  runs. A run with no sweep keeps the old record shape exactly,
  and runs recorded before 0.37.0 carry no `sweeps` and render no
  sweep sections. Done in 0.37.0.
- Compose `--repeat` with the sweeps: with `--repeat N` each
  sweep step (keyspace, client count, value size) runs N times
  and reports the median ops/sec and p99 per step, reusing the
  existing repeat machinery (`run_repeated_workload`) so a sweep
  step's medians agree with the main run's — each step carries
  its per-run values and run-to-run spread under `repeat` in
  JSON (`keyspace_sweep[]` / `client_sweep[]` /
  `value_size_sweep[].repeat`) and a per-step repeat block in
  Markdown, and a sweep with `--repeat 1` keeps the old shape
  exactly. Done in 0.36.0.
- Run-to-run spread summary for `--repeat` runs: for each op and
  overall, min / max / spread (max - min) and spread as a
  percentage of the median, under `results[].repeat.spread` in
  JSON and as a table under the Repeat runs table in Markdown —
  so a run that swings wildly reads at a glance instead of just
  moving the median. A single run has no spread to report, so
  the section is absent for `--repeat 1`. Done in 0.35.0.
- Per-operation latency histograms in the `--repeat` runs:
  each run in the repeat table carries the same log-spaced
  buckets per operation that ran as the regular run's
  `histograms_by_op` — under `results[].repeat.runs[].histograms_by_op`
  in JSON, and as a per-run per-op histogram block under the
  Repeat runs table in Markdown, reusing the shared
  histogram-block helper from the sweep work — so a repeat run
  where one op type slows down names the op instead of just
  moving the median. Repeat entries written before 0.34.0
  carry no `histograms_by_op` and render the table only.
  Done in 0.34.0.
- ~~Per-operation latency histograms for sweep runs: each sweep step~~
  (keyspace, client count, value size) carries the same log-spaced
  buckets per operation that ran as the regular run's
  `histograms_by_op` — under `keyspace_sweep[]`,
  `client_sweep[]` and `value_size_sweep[]` in JSON, and as a
  per-step per-op histogram block under each sweep table in
  Markdown — so a sweep step where one op type slows down names
  the op instead of just moving the p99. Sweep entries written
  before 0.33.0 carry no `histograms_by_op` and render the table
  only. Done in 0.33.0.
- Per-operation latency histograms: the same log-spaced buckets
  as the overall histogram, drawn separately for each operation
  that ran (SET, GET, INCR, DEL) in a Per-operation latency
  histograms section in the Markdown report, with the same buckets
  per result under `histograms_by_op` in JSON — so a slow op type's
  latency shape shows separately from the overall distribution
  instead of being swamped by the fast ops. The existing report
  shape is unchanged: the overall `histogram` and per-op stats
  table are exactly as before. Done in 0.32.0.
- ~~DEL op in the mix (`--mix 45:40:10:5` by default): DEL
  deletes a key the workload SET earlier, sharing the SET/GET
  keyspace (`<prefix>:<rank>`, not the INCR counter keyspace),
  with per-op ops/sec and latency in both reports, in the history
  and trend views, in tail attribution, and as a per-op SLO target
  (`--slo-op DEL:5,0.1`); the old three-part `--mix` form
  (`50:40:10`) still works, with DEL weight 0, and the presets
  carry a 5% DEL share.~~ Done in 0.31.0.
- ~~Authentication (`--password` / `--username`): AUTH on connect
  for password-protected servers — `AUTH password`, or `AUTH
  username password` with `--username` (the Redis 6 ACL form) —
  failing fast (exit 1) on a wrong password; the password is never
  recorded (the command line in both reports and the history shows
  `***` in its place), while both reports record that the run
  authenticated (`workload.auth` in JSON, an Authentication line
  in Markdown); a run without `--password` keeps the old report
  shape exactly.~~ Done in 0.30.0.
- ~~Value-size sweep (`--sweep-value-size 32,128,1024`): re-run
  the same workload at several value sizes, with ops/sec and p99
  per size in both reports, so a run shows how value size affects
  throughput.~~ Done in 0.29.0.
- ~~Key prefix (`--key-prefix NAME`): prefix every benchmark key
  (`<prefix>:<rank>` for SET/GET, `<prefix>:counter:<rank>` for
  INCR) so runs against a shared store do not collide, with the
  prefix recorded in both reports (`workload.key_prefix` in JSON,
  a line in the Markdown workload section); the default prefix
  `kvbench` keeps old key names, and an empty prefix is rejected
  (exit 2) before any benchmark runs.~~ Done in 0.28.0.
- ~~Repeat runs (`--repeat N`): run the benchmark N times and
  report the median ops/sec and p99 over the N runs, per
  operation as well as overall, with the per-run values recorded
  in the JSON report (`results[].repeat.runs`) and a per-run
  table in the Markdown report; the mean, p50, p95, histogram,
  tail attribution and per-second series come from the run whose
  ops/sec is the median. A single run keeps the old JSON shape
  exactly (no `repeat` key), and a non-positive N fails (exit 2)
  before any benchmark runs.~~ Done in 0.27.0.
- ~~Configurable socket timeout (`--timeout SECONDS`): the
  connect and read timeout for the RESP connection, 5 s by
  default — raise it for a loaded or remote server, lower it to
  fail fast on a dead one — recorded in both reports
  (`workload.timeout_s`, a line in the Markdown workload
  section), and a non-positive value fails (exit 2) before any
  benchmark runs. The timeout is a transport setting, not part
  of the workload, so `--compare-same-workload` does not treat a
  changed timeout as a different workload.~~ Done in 0.26.0.
- ~~History export/import (`--history-export` / `--history-import`):
  write the run history out as portable JSON (all backends or
  one backend) and merge an export back into a history file,
  merging by (backend, timestamp) — identical records deduped,
  conflicts keep the existing record, and a bad export file
  fails (exit 2) before anything is merged.~~ Done in 0.25.0.
- ~~Per-backend history pruning (`--prune`): keep the N most
  recent runs per backend (`--prune-keep`), or drop runs older
  than a date (`--prune-before`), with a per-backend filter
  (`--prune-backend`), a dry run showing what would be removed
  (`--dry-run`), and an explicit `--prune-all` to clear a
  backend out entirely — a prune never empties a backend
  without it.~~ Done in 0.24.0.
- ~~Best vs worst run comparison (`--best-worst`): pick the best
  and worst runs per backend out of the history file (by ops/sec,
  overall and per operation) and render a Markdown/JSON comparison
  with ops/sec, p99 and the delta between them, with the same
  workload filter behaviour as `--compare-same-workload` (matching
  the latest recorded run's workload), plus `--best-worst-out` and
  `--best-worst-json` file exports.~~ Done in 0.23.0.
- ~~Per-op trend tracking in `--trend`: each history record
  carries per-op ops/sec and latency, and the trend chart renders
  a per-op section per backend (SET, GET, INCR) with its own
  table, sparkline and first-to-last change, so a slow op type
  no longer hides inside the overall ops/sec. Records written
  before 0.22.0 carry no per-op data and render no per-op
  sections.~~ Done in 0.22.0.
- ~~Trend chart as a Markdown file (`--trend-out PATH`): write
  the same ops/sec trend chart `--trend` prints to stdout to a
  Markdown file as well, so it can be committed or attached to a
  report.~~ Done in 0.21.0.
- ~~Compare against a history run with the same workload
  (`--compare-history` with `--compare-same-workload`): only
  recorded runs whose workload matches this run's are considered
  as baselines, so the selector picks the latest run that measured
  the same workload and the regression gate stays apples-to-apples
  when the history file mixes workloads.~~ Done in 0.20.0.
- ~~Compare against a run from the history file (`--compare-file`
  against a history-file run): pick a backend's run out of
  `~/.kvbench/history.jsonl` as the baseline instead of a JSON
  report file, so a regression check needs no report file lying
  around.~~ Done in 0.19.0.
- Ops/sec trend chart in Markdown (`--trend`): render the run
  history as a per-backend ops/sec-over-time chart — a table with
  ops/sec, the delta versus the previous run, and p99 alongside,
  plus a sparkline and the first-to-last change — handling empty
  history, corrupt lines (skipped, same as `--show-history`) and
  single-run history.~~ Done in 0.18.0.
- GitHub Actions workflow (`.github/workflows/bench.yml`): run
  kvbench against cinder on every push, gated by the checked-in
  SLO preset (`slo.json`: global p99 5 ms, error rate 1%, SET
  p99 10 ms, GET p99 5 ms), with the Markdown and JSON reports
  uploaded as an artifact even when the gate fails. The
  workflow itself cannot run on a dev VM, so its exact commands
  are verified locally and `tests/test_workflow.py` checks the
  workflow file parses and passes only real CLI flags. Done in
  0.17.0.
- ~~SLO preset file (`--slo-preset PATH`): load SLO thresholds
  from a JSON preset file (`p99_ms`, `error_rate_pct`, `per_op`),
  validatable (a bad file exits 2 before any benchmark runs),
  composable with CLI flags (CLI overrides preset), and recorded
  in both reports (`workload.slo_preset` / `slo.preset` in JSON,
  named in the Markdown SLO gate section).~~ Done in 0.16.0.
- ~~Per-operation SLOs (`--slo-op OP:P99,ERR`, repeatable): each
  operation type carries its own p99 and error-rate thresholds on
  top of the global SLO gate, with per-op verdicts in both the
  Markdown and JSON reports, and a breach fails the run (exit 4)
  even when the overall p99 is fine.~~ Done in 0.15.0.
- ~~SLO gate (`--slo-p99-ms` / `--slo-error-rate`): fail the run
  (exit 4) when a backend's measured p99 latency or error rate
  breaches the threshold, with the gate verdict in both the
  Markdown and JSON reports.~~ Done in 0.14.0.
- ~~Compare against a previous run (`--compare-file PATH`):
  load an earlier run's JSON report and compare the current run
  against it side by side (ops/sec and p99 deltas, verdict lines),
  with `--regression-threshold PCT` failing the run when a backend
  regresses past the threshold.~~ Done in 0.13.0.
- ~~Throughput-over-time CSV export (`--csv PATH`): one row per
  backend per second with ops, ops/sec, mean and max latency, so
  throughput over time plots directly without parsing the
  report.~~ Done in 0.12.0.
- ~~Tail-latency attribution: when p99 is high, break the slowest
  ops down by operation type, by client, by hot vs cold keys and
  by second, and name the factor that dominates the tail in both
  reports.~~ Done in 0.11.0.
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
