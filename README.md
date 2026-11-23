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
  When the tail is high the reports attribute it: slow ops (at or
  above the overall p99) are broken down by operation type, by
  client, by hot vs cold keys, and by second, and the finding
  names the factor that dominates the tail — one slow op type, one
  slow client, hot or cold keys, or a single slow second — or says
  no single factor does, so a high p99 points at the cause instead
  of just the number.
- Latency stats (`kvbench/stats.py`): ops/sec, mean, p50, p95, p99,
  plus a log-spaced latency histogram and a per-second time series
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

# ops/sec trend chart per backend, with sparklines, from the
# same history file
python -m kvbench.cli --trend

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
`--csv` (write a per-second throughput CSV alongside the reports:
one row per backend per second with ops, ops/sec, mean and max
latency — see below),
`--history` (record this run in the history file so later runs can
be compared against it), `--history-file` (where the history lives,
default `~/.kvbench/history.jsonl`), `--show-history` (print the
recorded run history and exit, no benchmark is run),
`--trend` (print an ops/sec trend chart per backend from the
recorded run history and exit, no benchmark is run),
`--history-limit` (how many recent runs `--show-history` and
default 20),
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
newest) and the first-to-last change. A backend with a single
recorded run shows its one value with no delta and a single
block; an empty history prints the same "No runs recorded yet"
message as `--show-history`.

## Comparing against a previous run

With `--compare-file PATH` the run is compared against a previous
run's JSON report (the `--json` output of an earlier run, usually
the last one): each backend is matched by name, and both reports
gain a Comparison with previous run section with ops/sec and p99
side by side, the percentage change of each, and a verdict per
backend — `faster`, `slower`, `about the same` (within 1%), or
`regression` when a threshold was given and passed. Backends in
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
together are rejected — pick one baseline.

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
