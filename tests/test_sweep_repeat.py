"""Tests for composing --repeat N with the sweeps: repeat each sweep
step N times and report the median per step.

Each sweep step runs through the same :func:`run_repeated_workload`
machinery the main run's ``--repeat`` uses, so a step's reported
ops/sec and p99 are the medians over its runs — the same medians the
main run reports — and the per-run values and the run-to-run spread
land in the JSON report under each step's ``repeat`` (``runs`` and
``spread``) and in the Markdown report as a per-step repeat block.
A sweep with ``--repeat 1`` (or no flag) keeps the old shape exactly:
no ``repeat`` key on any step.
"""

import json
import os
import tempfile
import unittest
from contextlib import redirect_stdout
from io import StringIO

from kvbench.bench import (DictClient, Workload, run_client_sweep,
                           run_keyspace_sweep, run_mix_sweep,
                           run_pipeline_sweep,
                           run_value_size_sweep, run_workload)
from kvbench.cli import main
from kvbench.report import (_fastest_slowest_histogram_comparison,
                            _histogram_comparison_rows_from,
                            _histograms_or_none, _per_op_histogram_block,
                            _per_op_fastest_slowest_histogram_comparison,
                            render_json, render_markdown)

try:
    from test_cli import _start_server
except ModuleNotFoundError:  # pragma: no cover - import fallback
    # The fake RESP server lives in test_cli; when this module is
    # loaded outside `discover -s tests` (which puts the tests dir
    # on sys.path), add the tests dir so the import still resolves.
    import sys

    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from test_cli import _start_server


def _median_of(values):
    ordered = sorted(values)
    n = len(ordered)
    mid = n // 2
    if n % 2:
        return ordered[mid]
    return (ordered[mid - 1] + ordered[mid]) / 2


def _check_repeated_step(entry, repeat):
    """A repeated sweep step's medians come from its per-run values."""
    runs = entry["repeat"]["runs"]
    assert len(runs) == repeat
    assert [run["run"] for run in runs] == list(range(1, repeat + 1))
    assert entry["ops_per_sec"] == _median_of(
        [run["ops_per_sec"] for run in runs])
    assert entry["p99_ms"] == _median_of(
        [run["p99_ms"] for run in runs])
    # The spread summary's medians are the same medians, and its
    # min/max bound the per-run values.
    spread = entry["repeat"]["spread"]["overall"]
    assert spread["ops_per_sec"]["median"] == entry["ops_per_sec"]
    assert spread["p99_ms"]["median"] == entry["p99_ms"]
    assert spread["ops_per_sec"]["min"] == min(
        run["ops_per_sec"] for run in runs)
    assert spread["ops_per_sec"]["max"] == max(
        run["ops_per_sec"] for run in runs)
    # Each run carries per-op histograms that partition its ops,
    # the same shape as the main run's histograms_by_op.
    for run in runs:
        per_op = run["histograms_by_op"]
        assert set(per_op) == set(run["per_op"])
        for op, buckets in per_op.items():
            assert sum(b["count"] for b in buckets) == \
                run["per_op"][op]["ops"]


class TestRepeatedKeyspaceSweep(unittest.TestCase):
    def test_each_step_runs_repeat_times_and_reports_medians(self):
        workload = Workload(ops=200, clients=2, keys=10, value_size=8)
        sweep = run_keyspace_sweep(DictClient, workload, [5, 20, 100],
                                   repeat=3)
        self.assertEqual([entry["keys"] for entry in sweep],
                         [5, 20, 100])
        for entry in sweep:
            self.assertEqual(entry["ops"], 200)
            _check_repeated_step(entry, 3)
        # The base workload is untouched: the sweep copies it per size.
        self.assertEqual(workload.keys, 10)

    def test_repeat_one_keeps_the_old_shape(self):
        workload = Workload(ops=200, clients=2, keys=10)
        for sweep in (run_keyspace_sweep(DictClient, workload, [5, 20],
                                         repeat=1),
                      run_keyspace_sweep(DictClient, workload, [5, 20])):
            for entry in sweep:
                self.assertNotIn("repeat", entry)
                self.assertEqual(
                    set(entry),
                    {"keys", "ops", "ops_per_sec", "mean_ms", "p99_ms",
                     "histograms_by_op"})

    def test_repeat_zero_raises(self):
        workload = Workload(ops=10, clients=1, keys=5)
        with self.assertRaises(ValueError):
            run_keyspace_sweep(DictClient, workload, [5], repeat=0)

    def test_sweep_stops_on_unreachable_backend(self):
        def failing_factory():
            raise ConnectionError("connection refused")

        workload = Workload(ops=10, clients=1, keys=5)
        sweep = run_keyspace_sweep(failing_factory, workload, [5, 10],
                                   repeat=3)
        self.assertEqual(sweep, [])


class TestRepeatedClientSweep(unittest.TestCase):
    def test_each_step_runs_repeat_times_and_reports_medians(self):
        workload = Workload(ops=200, clients=2, keys=10, value_size=8)
        sweep = run_client_sweep(DictClient, workload, [1, 2, 4],
                                 repeat=3)
        self.assertEqual([entry["clients"] for entry in sweep],
                         [1, 2, 4])
        for entry in sweep:
            self.assertEqual(entry["ops"], 200)
            _check_repeated_step(entry, 3)
        # The base workload is untouched: the sweep copies it per count.
        self.assertEqual(workload.clients, 2)

    def test_repeat_one_keeps_the_old_shape(self):
        workload = Workload(ops=200, clients=2, keys=10)
        sweep = run_client_sweep(DictClient, workload, [1, 2], repeat=1)
        for entry in sweep:
            self.assertNotIn("repeat", entry)


class TestRepeatedValueSizeSweep(unittest.TestCase):
    def test_each_step_runs_repeat_times_and_reports_medians(self):
        workload = Workload(ops=200, clients=2, keys=10, value_size=8)
        sweep = run_value_size_sweep(DictClient, workload, [8, 64, 512],
                                     repeat=3)
        self.assertEqual([entry["value_size"] for entry in sweep],
                         [8, 64, 512])
        for entry in sweep:
            self.assertEqual(entry["ops"], 200)
            _check_repeated_step(entry, 3)
        # The base workload is untouched: the sweep copies it per size.
        self.assertEqual(workload.value_size, 8)

    def test_repeat_one_keeps_the_old_shape(self):
        workload = Workload(ops=200, clients=2, keys=10, value_size=8)
        sweep = run_value_size_sweep(DictClient, workload, [8, 64],
                                     repeat=1)
        for entry in sweep:
            self.assertNotIn("repeat", entry)


class TestRepeatedPipelineSweep(unittest.TestCase):
    def test_each_step_runs_repeat_times_and_reports_medians(self):
        workload = Workload(ops=200, clients=2, keys=10, value_size=8)
        sweep = run_pipeline_sweep(DictClient, workload, [1, 5, 10],
                                   repeat=3)
        self.assertEqual([entry["pipeline"] for entry in sweep],
                         [1, 5, 10])
        for entry in sweep:
            self.assertEqual(entry["ops"], 200)
            _check_repeated_step(entry, 3)
        # The base workload is untouched: the sweep copies it per depth.
        self.assertEqual(workload.pipeline, 1)

    def test_repeat_one_keeps_the_old_shape(self):
        workload = Workload(ops=200, clients=2, keys=10, value_size=8)
        sweep = run_pipeline_sweep(DictClient, workload, [1, 5],
                                   repeat=1)
        for entry in sweep:
            self.assertNotIn("repeat", entry)
            self.assertEqual(
                set(entry),
                {"pipeline", "ops", "ops_per_sec", "mean_ms", "p99_ms",
                 "histograms_by_op"})

    def test_sweep_stops_on_unreachable_backend(self):
        def failing_factory():
            raise ConnectionError("connection refused")

        workload = Workload(ops=10, clients=1, keys=5)
        sweep = run_pipeline_sweep(failing_factory, workload, [1, 5],
                                   repeat=3)
        self.assertEqual(sweep, [])


class TestRepeatedMixSweep(unittest.TestCase):
    def test_each_step_runs_repeat_times_and_reports_medians(self):
        workload = Workload(ops=200, clients=2, keys=10, value_size=8)
        sweep = run_mix_sweep(DictClient, workload,
                              [(100, 0), (90, 10), (50, 50), (10, 90),
                               (0, 100)],
                              repeat=3)
        self.assertEqual([entry["mix"] for entry in sweep],
                         ["100:0", "90:10", "50:50", "10:90", "0:100"])
        for entry in sweep:
            self.assertEqual(entry["ops"], 200)
            _check_repeated_step(entry, 3)
        # The base workload is untouched: the sweep copies it per ratio.
        self.assertEqual(
            (workload.set_weight, workload.get_weight,
             workload.incr_weight, workload.del_weight),
            (50, 40, 10, 5))
        self.assertEqual(workload.mix_label, "")

    def test_each_step_runs_only_its_mix(self):
        # A step's ops are its ratio's ops only: reads are GETs,
        # writes are SETs, with no INCRs or DELs.
        workload = Workload(ops=200, clients=2, keys=10)
        sweep = run_mix_sweep(DictClient, workload,
                              [(100, 0), (50, 50), (0, 100)])
        self.assertEqual(set(sweep[0]["histograms_by_op"]), {"GET"})
        self.assertEqual(set(sweep[1]["histograms_by_op"]),
                         {"SET", "GET"})
        self.assertEqual(set(sweep[2]["histograms_by_op"]), {"SET"})

    def test_repeat_one_keeps_the_old_shape(self):
        workload = Workload(ops=200, clients=2, keys=10, value_size=8)
        sweep = run_mix_sweep(DictClient, workload,
                              [(100, 0), (50, 50)], repeat=1)
        for entry in sweep:
            self.assertNotIn("repeat", entry)
            self.assertEqual(
                set(entry),
                {"mix", "ops", "ops_per_sec", "mean_ms", "p99_ms",
                 "histograms_by_op"})

    def test_repeat_zero_raises(self):
        workload = Workload(ops=10, clients=1, keys=5)
        with self.assertRaises(ValueError):
            run_mix_sweep(DictClient, workload, [(100, 0)], repeat=0)

    def test_sweep_stops_on_unreachable_backend(self):
        def failing_factory():
            raise ConnectionError("connection refused")

        workload = Workload(ops=10, clients=1, keys=5)
        sweep = run_mix_sweep(failing_factory, workload,
                              [(100, 0), (50, 50)], repeat=3)
        self.assertEqual(sweep, [])


class TestRepeatedSweepRendering(unittest.TestCase):
    def _result_with_repeated_sweeps(self):
        workload = Workload(ops=200, clients=2, repeat=3)
        result = run_workload(DictClient, workload, "baseline (dict)")
        result.sweep = run_keyspace_sweep(DictClient, workload, [5, 20],
                                          repeat=3)
        result.client_sweep = run_client_sweep(DictClient, workload,
                                               [1, 2], repeat=3)
        result.value_sweep = run_value_size_sweep(
            DictClient, workload, [8, 64], repeat=3)
        result.pipeline_sweep = run_pipeline_sweep(
            DictClient, workload, [1, 5], repeat=3)
        result.mix_sweep = run_mix_sweep(
            DictClient, workload, [(100, 0), (50, 50)], repeat=3)
        return result, workload

    def test_repeated_sweep_renders_in_markdown_and_json(self):
        result, workload = self._result_with_repeated_sweeps()
        text = render_markdown([result], workload, "kvbench")
        # One per-step repeat block per sweep, with the median line
        # and the spread line; the table rows above them already
        # show the medians.
        self.assertIn("#### 5 keys — repeat runs", text)
        self.assertIn("#### 20 keys — repeat runs", text)
        self.assertIn("#### 1 clients — repeat runs", text)
        self.assertIn("#### 2 clients — repeat runs", text)
        self.assertIn("#### 8 byte values — repeat runs", text)
        self.assertIn("#### 64 byte values — repeat runs", text)
        self.assertIn("#### 1 commands — repeat runs", text)
        self.assertIn("#### 5 commands — repeat runs", text)
        self.assertIn("#### mix 100:0 — repeat runs", text)
        self.assertIn("#### mix 50:50 — repeat runs", text)
        self.assertIn("Median of 3 runs", text)
        self.assertIn("Spread: ops/sec", text)
        parsed = json.loads(render_json([result], workload, "kvbench"))
        doc = parsed["results"][0]
        for key in ("keyspace_sweep", "client_sweep",
                    "value_size_sweep", "pipeline_sweep",
                    "mix_sweep"):
            for entry in doc[key]:
                self.assertEqual(len(entry["repeat"]["runs"]), 3)
                self.assertEqual(
                    entry["ops_per_sec"],
                    _median_of([run["ops_per_sec"]
                                for run in entry["repeat"]["runs"]]))
                self.assertEqual(
                    entry["p99_ms"],
                    _median_of([run["p99_ms"]
                                for run in entry["repeat"]["runs"]]))
                self.assertEqual(
                    entry["repeat"]["spread"]["overall"]
                    ["ops_per_sec"]["median"],
                    entry["ops_per_sec"])

    def test_sweep_without_repeat_renders_table_only(self):
        # Entries recorded before 0.36.0 carry no ``repeat``: the
        # table still renders and no per-step repeat block is
        # invented.
        result, workload = self._result_with_repeated_sweeps()
        for entry in result.sweep:
            del entry["repeat"]
        text = render_markdown([result], workload, "kvbench")
        self.assertIn("Keyspace sweep", text)
        self.assertNotIn("keys — repeat runs", text)
        parsed = json.loads(render_json([result], workload, "kvbench"))
        for entry in parsed["results"][0]["keyspace_sweep"]:
            self.assertNotIn("repeat", entry)


class TestSweepStepFastestSlowestComparison(unittest.TestCase):
    def _steps_with_distinct_speeds(self):
        # A repeated keyspace sweep with forced, distinct ops/sec
        # per run, so the fastest/slowest picks are deterministic:
        # run 2 is fastest, run 1 slowest, in every step. The
        # per-op figures and histograms stay the runs' own, so
        # the rows stay real.
        workload = Workload(ops=200, clients=2, keys=10)
        sweep = run_keyspace_sweep(DictClient, workload, [5, 20],
                                   repeat=3)
        for entry in sweep:
            for run, ops_per_sec in zip(
                    entry["repeat"]["runs"], (10000.0, 20000.0, 15000.0)):
                run["ops_per_sec"] = ops_per_sec
        return sweep

    def test_comparison_picks_fastest_and_slowest_by_ops_sec(self):
        sweep = self._steps_with_distinct_speeds()
        for entry in sweep:
            comparison = _fastest_slowest_histogram_comparison(
                entry["repeat"]["runs"])
            self.assertEqual(comparison["from"], "slowest")
            self.assertEqual(comparison["to"], "fastest")
            self.assertEqual(comparison["fastest_run"], 2)
            self.assertEqual(comparison["slowest_run"], 1)
            self.assertEqual(comparison["notes"], [])
            runs = {run["run"]: run
                    for run in entry["repeat"]["runs"]}
            overall = comparison["overall"]
            self.assertEqual(overall["p50_slowest_ms"],
                             runs[1]["p50_ms"])
            self.assertEqual(overall["p50_fastest_ms"],
                             runs[2]["p50_ms"])
            self.assertEqual(overall["p99_slowest_ms"],
                             runs[1]["p99_ms"])
            self.assertEqual(overall["p99_fastest_ms"],
                             runs[2]["p99_ms"])
            rows = {row["op"]: row for row in comparison["rows"]}
            self.assertEqual(set(rows), set(runs[1]["per_op"]))
            for op, row in rows.items():
                self.assertEqual(row["p50_from_ms"],
                                 runs[1]["per_op"][op]["p50_ms"])
                self.assertEqual(row["p50_to_ms"],
                                 runs[2]["per_op"][op]["p50_ms"])
                self.assertEqual(row["p99_from_ms"],
                                 runs[1]["per_op"][op]["p99_ms"])
                self.assertEqual(row["p99_to_ms"],
                                 runs[2]["per_op"][op]["p99_ms"])
                self.assertIsNotNone(row["bucket_drift_pp"])
                self.assertIsNotNone(row["bucket_drift_bucket"])

    def test_comparison_rows_agree_with_shared_rows(self):
        sweep = self._steps_with_distinct_speeds()
        for entry in sweep:
            comparison = _fastest_slowest_histogram_comparison(
                entry["repeat"]["runs"])
            runs = {run["run"]: run
                    for run in entry["repeat"]["runs"]}
            expected = _histogram_comparison_rows_from(
                _histograms_or_none(runs[1]["histograms_by_op"]),
                _histograms_or_none(runs[2]["histograms_by_op"]),
                runs[1]["per_op"], runs[2]["per_op"])
            self.assertEqual(comparison["rows"], expected)

    def test_comparison_in_markdown_and_json(self):
        workload = Workload(ops=200, clients=2, repeat=3)
        result = run_workload(DictClient, workload, "baseline (dict)")
        result.sweep = self._steps_with_distinct_speeds()
        text = render_markdown([result], workload, "kvbench")
        # One comparison block per step, under that step's repeat
        # block, with the fastest/slowest sentence and the table.
        for label in ("5 keys", "20 keys"):
            self.assertIn(
                f"##### {label} — fastest vs slowest repeat run",
                text)
            self.assertIn(
                f"##### {label} — fastest vs slowest repeat run "
                "histogram comparison", text)
        self.assertIn("Fastest run 2 vs slowest run 1", text)
        self.assertIn("p99 slowest (ms)", text)
        self.assertIn("p99 fastest (ms)", text)
        parsed = json.loads(render_json([result], workload, "kvbench"))
        for entry, step in zip(parsed["results"][0]["keyspace_sweep"],
                               result.sweep):
            comparison = entry["repeat"]["histogram_comparison"]
            self.assertEqual(comparison["from"], "slowest")
            self.assertEqual(comparison["to"], "fastest")
            self.assertEqual(comparison["fastest_run"], 2)
            self.assertEqual(comparison["slowest_run"], 1)
            self.assertEqual(comparison["notes"], [])
            self.assertTrue(comparison["rows"])
            self.assertIn("overall", comparison)
            # The JSON comparison is the same dict the Markdown
            # block renders, from the same shared helper — apart
            # from "per_op", which carries the per-op comparisons
            # (covered by
            # TestSweepStepPerOpFastestSlowestComparison).
            self.assertEqual(
                {key: value for key, value in comparison.items()
                 if key != "per_op"},
                _fastest_slowest_histogram_comparison(
                    step["repeat"]["runs"]))

    def test_comparison_carries_fastest_and_slowest_histograms(self):
        sweep = self._steps_with_distinct_speeds()
        for entry in sweep:
            comparison = _fastest_slowest_histogram_comparison(
                entry["repeat"]["runs"])
            runs = {run["run"]: run
                    for run in entry["repeat"]["runs"]}
            self.assertEqual(
                comparison["fastest_histograms_by_op"],
                _histograms_or_none(runs[2]["histograms_by_op"]))
            self.assertEqual(
                comparison["slowest_histograms_by_op"],
                _histograms_or_none(runs[1]["histograms_by_op"]))
        # No comparison, no histograms: identical runs pick the
        # same run on both sides.
        entry = sweep[0]
        for run in entry["repeat"]["runs"]:
            run["ops_per_sec"] = 10000.0
        comparison = _fastest_slowest_histogram_comparison(
            entry["repeat"]["runs"])
        self.assertIsNone(comparison["rows"])
        self.assertIsNone(comparison["fastest_histograms_by_op"])
        self.assertIsNone(comparison["slowest_histograms_by_op"])

    def test_fastest_slowest_blocks_in_markdown_and_json(self):
        workload = Workload(ops=200, clients=2, repeat=3)
        result = run_workload(DictClient, workload, "baseline (dict)")
        result.sweep = self._steps_with_distinct_speeds()
        text = render_markdown([result], workload, "kvbench")
        runs = {run["run"]: run
                for run in result.sweep[0]["repeat"]["runs"]}
        for label in ("5 keys", "20 keys"):
            # One fastest block and one slowest block per step,
            # drawn with the same shared helper as the step's own
            # per-op blocks — fastest first, then slowest.
            fastest_block = _per_op_histogram_block(
                f"##### {label} — fastest repeat run (run 2) "
                "per-op latency histogram",
                runs[2]["histograms_by_op"])
            slowest_block = _per_op_histogram_block(
                f"##### {label} — slowest repeat run (run 1) "
                "per-op latency histogram",
                runs[1]["histograms_by_op"])
            self.assertIn("\n".join(fastest_block), text)
            self.assertIn("\n".join(slowest_block), text)
            self.assertLess(text.index("\n".join(fastest_block)),
                            text.index("\n".join(slowest_block)))
        parsed = json.loads(render_json([result], workload, "kvbench"))
        for entry, step in zip(parsed["results"][0]["keyspace_sweep"],
                               result.sweep):
            comparison = entry["repeat"]["histogram_comparison"]
            step_runs = {run["run"]: run
                         for run in step["repeat"]["runs"]}
            # The JSON comparison carries the same two maps the
            # Markdown blocks draw, so a script reading the JSON
            # sees what the report shows.
            self.assertEqual(
                comparison["fastest_histograms_by_op"],
                _histograms_or_none(
                    step_runs[2]["histograms_by_op"]))
            self.assertEqual(
                comparison["slowest_histograms_by_op"],
                _histograms_or_none(
                    step_runs[1]["histograms_by_op"]))
            # Apart from "per_op" — the per-op comparisons, covered
            # by TestSweepStepPerOpFastestSlowestComparison — the
            # JSON comparison is the same dict the shared helper
            # returns.
            self.assertEqual(
                {key: value for key, value in comparison.items()
                 if key != "per_op"},
                _fastest_slowest_histogram_comparison(
                    step["repeat"]["runs"]))

    def test_no_blocks_when_runs_not_comparable(self):
        # Identical runs: no overall comparison, so no overall
        # fastest/slowest blocks either — the note names the
        # reason instead. The per-op comparisons still compare,
        # since the per-op ops/sec differ, so the per-op sections
        # draw their own blocks for the op.
        sweep = self._steps_with_distinct_speeds()
        for entry in sweep:
            for run in entry["repeat"]["runs"]:
                run["ops_per_sec"] = 10000.0
        workload = Workload(ops=200, clients=2, repeat=3)
        result = run_workload(DictClient, workload, "baseline (dict)")
        result.sweep = sweep
        text = render_markdown([result], workload, "kvbench")
        for label in ("5 keys", "20 keys"):
            self.assertNotIn(
                f"##### {label} — fastest repeat run (run", text)
            self.assertNotIn(
                f"##### {label} — slowest repeat run (run", text)
        self.assertIn("SET fastest repeat run (run", text)
        self.assertIn("SET slowest repeat run (run", text)
        # The fastest run loses its histograms: same for the
        # overall comparison — a note, and no overall blocks.
        sweep = self._steps_with_distinct_speeds()
        for entry in sweep:
            del entry["repeat"]["runs"][1]["histograms_by_op"]
        result.sweep = sweep
        text = render_markdown([result], workload, "kvbench")
        for label in ("5 keys", "20 keys"):
            self.assertNotIn(
                f"##### {label} — fastest repeat run (run", text)
            self.assertNotIn(
                f"##### {label} — slowest repeat run (run", text)
        self.assertIn("Repeat run 2 (the fastest run) carries no "
                      "per-op histograms, so there is no "
                      "fastest-vs-slowest histogram comparison.", text)

    def test_comparison_covers_all_sweep_types(self):
        # Every sweep type's per-step repeat block carries the
        # comparison — they all render through the same helper.
        workload = Workload(ops=200, clients=2, repeat=3)
        result = run_workload(DictClient, workload, "baseline (dict)")
        result.sweep = run_keyspace_sweep(DictClient, workload, [5],
                                          repeat=3)
        result.client_sweep = run_client_sweep(DictClient, workload,
                                               [1], repeat=3)
        result.value_sweep = run_value_size_sweep(
            DictClient, workload, [8], repeat=3)
        result.pipeline_sweep = run_pipeline_sweep(
            DictClient, workload, [1], repeat=3)
        result.mix_sweep = run_mix_sweep(
            DictClient, workload, [(100, 0)], repeat=3)
        for entries in (result.sweep, result.client_sweep,
                        result.value_sweep, result.pipeline_sweep,
                        result.mix_sweep):
            for entry in entries:
                for run, ops_per_sec in zip(
                        entry["repeat"]["runs"],
                        (10000.0, 20000.0, 15000.0)):
                    run["ops_per_sec"] = ops_per_sec
        text = render_markdown([result], workload, "kvbench")
        for heading in ("5 keys — fastest vs slowest repeat run",
                        "1 clients — fastest vs slowest repeat run",
                        "8 byte values — fastest vs slowest repeat run",
                        "1 commands — fastest vs slowest repeat run",
                        "mix 100:0 — fastest vs slowest repeat run"):
            self.assertIn(f"##### {heading}", text)
        # Every sweep type's per-step repeat block also carries
        # the fastest and slowest runs' per-op histogram blocks.
        for heading in ("5 keys — fastest repeat run (run 2)",
                        "5 keys — slowest repeat run (run 1)",
                        "1 clients — fastest repeat run (run 2)",
                        "1 clients — slowest repeat run (run 1)",
                        "8 byte values — fastest repeat run (run 2)",
                        "8 byte values — slowest repeat run (run 1)",
                        "1 commands — fastest repeat run (run 2)",
                        "1 commands — slowest repeat run (run 1)",
                        "mix 100:0 — fastest repeat run (run 2)",
                        "mix 100:0 — slowest repeat run (run 1)"):
            self.assertIn(f"##### {heading} per-op latency histogram",
                          text)
        parsed = json.loads(render_json([result], workload, "kvbench"))
        doc = parsed["results"][0]
        for key in ("keyspace_sweep", "client_sweep",
                    "value_size_sweep", "pipeline_sweep",
                    "mix_sweep"):
            for entry in doc[key]:
                comparison = entry["repeat"]["histogram_comparison"]
                self.assertEqual(comparison["fastest_run"], 2)
                self.assertEqual(comparison["slowest_run"], 1)
                self.assertEqual(comparison["notes"], [])

    def test_identical_runs_render_a_note(self):
        sweep = self._steps_with_distinct_speeds()
        entry = sweep[0]
        for run in entry["repeat"]["runs"]:
            run["ops_per_sec"] = 10000.0
        comparison = _fastest_slowest_histogram_comparison(
            entry["repeat"]["runs"])
        self.assertIsNone(comparison["rows"])
        self.assertEqual(
            comparison["notes"],
            ["The fastest and slowest repeat runs are the same run, "
             "so there is no fastest-vs-slowest histogram "
             "comparison."])
        workload = Workload(ops=200, clients=2, repeat=3)
        result = run_workload(DictClient, workload, "baseline (dict)")
        result.sweep = sweep
        text = render_markdown([result], workload, "kvbench")
        self.assertIn("The fastest and slowest repeat runs are the "
                      "same run, so there is no fastest-vs-slowest "
                      "histogram comparison.", text)
        parsed = json.loads(render_json([result], workload, "kvbench"))
        # Apart from "per_op" — the per-op comparisons, covered by
        # TestSweepStepPerOpFastestSlowestComparison — the JSON
        # comparison is the same dict the shared helper returns.
        self.assertEqual(
            {key: value for key, value in
             parsed["results"][0]["keyspace_sweep"][0]["repeat"]
             ["histogram_comparison"].items() if key != "per_op"},
            comparison)

    def test_missing_histograms_render_a_note(self):
        sweep = self._steps_with_distinct_speeds()
        entry = sweep[0]
        # The fastest run (run 2) loses its histograms: the note
        # names the run.
        del entry["repeat"]["runs"][1]["histograms_by_op"]
        comparison = _fastest_slowest_histogram_comparison(
            entry["repeat"]["runs"])
        self.assertIsNone(comparison["rows"])
        self.assertEqual(
            comparison["notes"],
            ["Repeat run 2 (the fastest run) carries no per-op "
             "histograms, so there is no fastest-vs-slowest "
             "histogram comparison."])

    def test_single_repeat_step_has_no_comparison(self):
        workload = Workload(ops=200, clients=2, keys=10)
        result = run_workload(DictClient, workload, "baseline (dict)")
        result.sweep = run_keyspace_sweep(DictClient, workload, [5],
                                          repeat=1)
        text = render_markdown([result], workload, "kvbench")
        self.assertIn("Keyspace sweep", text)
        self.assertNotIn("fastest vs slowest repeat run", text)
        parsed = json.loads(render_json([result], workload, "kvbench"))
        for entry in parsed["results"][0]["keyspace_sweep"]:
            self.assertNotIn("repeat", entry)


class TestSweepStepPerOpFastestSlowestComparison(unittest.TestCase):
    def _steps_with_per_op_speeds(self):
        # A repeated keyspace sweep with forced per-op ops/sec, so
        # the per-op picks are deterministic and differ from the
        # overall picks: overall run 2 is fastest and run 1
        # slowest, but SET is fastest in run 3 and slowest in run
        # 1, while GET is fastest in run 1 and slowest in run 3.
        # The per-op figures and histograms stay the runs' own,
        # so the rows stay real.
        workload = Workload(ops=200, clients=2, keys=10)
        sweep = run_keyspace_sweep(DictClient, workload, [5, 20],
                                   repeat=3)
        per_op_speeds = {
            "SET": (10000.0, 20000.0, 30000.0),
            "GET": (30000.0, 20000.0, 10000.0),
        }
        for entry in sweep:
            for run, ops_per_sec in zip(
                    entry["repeat"]["runs"],
                    (10000.0, 20000.0, 15000.0)):
                run["ops_per_sec"] = ops_per_sec
            for run in entry["repeat"]["runs"]:
                for op, speeds in per_op_speeds.items():
                    if op in run["per_op"]:
                        run["per_op"][op]["ops_per_sec"] = \
                            speeds[run["run"] - 1]
        return sweep

    def test_per_op_picks_by_that_ops_ops_sec(self):
        sweep = self._steps_with_per_op_speeds()
        for entry in sweep:
            runs = {run["run"]: run
                    for run in entry["repeat"]["runs"]}
            # SET's own winner is run 3, its slowest run 1 — not
            # the overall fastest (run 2).
            set_comparison = \
                _per_op_fastest_slowest_histogram_comparison(
                    entry["repeat"]["runs"], "SET")
            self.assertEqual(set_comparison["op"], "SET")
            self.assertEqual(set_comparison["from"], "slowest")
            self.assertEqual(set_comparison["to"], "fastest")
            self.assertEqual(set_comparison["fastest_run"], 3)
            self.assertEqual(set_comparison["slowest_run"], 1)
            self.assertEqual(set_comparison["notes"], [])
            self.assertEqual(len(set_comparison["rows"]), 1)
            row = set_comparison["rows"][0]
            self.assertEqual(row["op"], "SET")
            self.assertEqual(row["p50_from_ms"],
                             runs[1]["per_op"]["SET"]["p50_ms"])
            self.assertEqual(row["p50_to_ms"],
                             runs[3]["per_op"]["SET"]["p50_ms"])
            self.assertEqual(row["p99_from_ms"],
                             runs[1]["per_op"]["SET"]["p99_ms"])
            self.assertEqual(row["p99_to_ms"],
                             runs[3]["per_op"]["SET"]["p99_ms"])
            self.assertIsNotNone(row["bucket_drift_pp"])
            self.assertIsNotNone(row["bucket_drift_bucket"])
            self.assertEqual(
                set_comparison["fastest_histogram"],
                runs[3]["histograms_by_op"]["SET"])
            self.assertEqual(
                set_comparison["slowest_histogram"],
                runs[1]["histograms_by_op"]["SET"])
            # GET's own winner is run 1, its slowest run 3 — the
            # reverse of SET, and neither is the overall winner.
            get_comparison = \
                _per_op_fastest_slowest_histogram_comparison(
                    entry["repeat"]["runs"], "GET")
            self.assertEqual(get_comparison["fastest_run"], 1)
            self.assertEqual(get_comparison["slowest_run"], 3)
            self.assertEqual(get_comparison["notes"], [])
            self.assertEqual(len(get_comparison["rows"]), 1)
            self.assertEqual(get_comparison["rows"][0]["op"], "GET")
            self.assertEqual(
                get_comparison["fastest_histogram"],
                runs[1]["histograms_by_op"]["GET"])
            self.assertEqual(
                get_comparison["slowest_histogram"],
                runs[3]["histograms_by_op"]["GET"])

    def test_rows_agree_with_shared_rows(self):
        sweep = self._steps_with_per_op_speeds()
        for entry in sweep:
            runs = {run["run"]: run
                    for run in entry["repeat"]["runs"]}
            for op, fastest_run, slowest_run in (
                    ("SET", 3, 1), ("GET", 1, 3)):
                comparison = \
                    _per_op_fastest_slowest_histogram_comparison(
                        entry["repeat"]["runs"], op)
                expected = _histogram_comparison_rows_from(
                    {op: _histograms_or_none(
                        runs[slowest_run]["histograms_by_op"])[op]},
                    {op: _histograms_or_none(
                        runs[fastest_run]["histograms_by_op"])[op]},
                    runs[slowest_run]["per_op"],
                    runs[fastest_run]["per_op"])
                self.assertEqual(comparison["rows"], expected)

    def test_per_op_comparison_in_markdown_and_json(self):
        workload = Workload(ops=200, clients=2, repeat=3)
        result = run_workload(DictClient, workload, "baseline (dict)")
        result.sweep = self._steps_with_per_op_speeds()
        text = render_markdown([result], workload, "kvbench")
        # One per-op section per op that ran, under each step's
        # repeat block, with the op's sentence and its table.
        for label in ("5 keys", "20 keys"):
            for op in ("SET", "GET", "INCR", "DEL"):
                self.assertIn(
                    f"##### {label} — {op} fastest vs slowest "
                    "repeat run", text)
                self.assertIn(
                    f"##### {label} — {op} fastest vs slowest "
                    "repeat run histogram comparison", text)
        self.assertIn("Fastest run 3 vs slowest run 1 for SET", text)
        self.assertIn("Fastest run 1 vs slowest run 3 for GET", text)
        # Each per-op section also draws the fastest and slowest
        # runs' histogram blocks for that op alone, next to its
        # table — fastest first, then slowest — from the bucket
        # lists the comparison already carries.
        for label in ("5 keys", "20 keys"):
            for op, fastest_run, slowest_run in (
                    ("SET", 3, 1), ("GET", 1, 3)):
                self.assertIn(
                    f"##### {label} — {op} fastest repeat run "
                    f"(run {fastest_run}) latency histogram", text)
                self.assertIn(
                    f"##### {label} — {op} slowest repeat run "
                    f"(run {slowest_run}) latency histogram", text)
        parsed = json.loads(render_json([result], workload, "kvbench"))
        for entry, step in zip(parsed["results"][0]["keyspace_sweep"],
                               result.sweep):
            per_op = entry["repeat"]["histogram_comparison"]["per_op"]
            runs = {run["run"]: run
                    for run in step["repeat"]["runs"]}
            self.assertEqual(set(per_op),
                             set(runs[1]["per_op"]))
            for op, comparison in per_op.items():
                # The JSON per-op comparison is the same dict the
                # Markdown section renders, from the same helper.
                self.assertEqual(
                    comparison,
                    _per_op_fastest_slowest_histogram_comparison(
                        step["repeat"]["runs"], op))
                self.assertEqual(comparison["notes"], [])
                self.assertEqual(len(comparison["rows"]), 1)
                self.assertEqual(comparison["rows"][0]["op"], op)

    def test_identical_per_op_ops_sec_renders_a_note(self):
        sweep = self._steps_with_per_op_speeds()
        entry = sweep[0]
        for run in entry["repeat"]["runs"]:
            run["per_op"]["SET"]["ops_per_sec"] = 10000.0
        comparison = _per_op_fastest_slowest_histogram_comparison(
            entry["repeat"]["runs"], "SET")
        self.assertIsNone(comparison["rows"])
        self.assertIsNone(comparison["fastest_histogram"])
        self.assertIsNone(comparison["slowest_histogram"])
        self.assertEqual(
            comparison["notes"],
            ["The fastest and slowest repeat runs for SET are the "
             "same run, so there is no fastest-vs-slowest "
             "histogram comparison for SET."])
        workload = Workload(ops=200, clients=2, repeat=3)
        result = run_workload(DictClient, workload, "baseline (dict)")
        result.sweep = sweep
        text = render_markdown([result], workload, "kvbench")
        self.assertIn("The fastest and slowest repeat runs for SET "
                      "are the same run, so there is no "
                      "fastest-vs-slowest histogram comparison for "
                      "SET.", text)
        parsed = json.loads(render_json([result], workload, "kvbench"))
        self.assertEqual(
            parsed["results"][0]["keyspace_sweep"][0]["repeat"]
            ["histogram_comparison"]["per_op"]["SET"],
            comparison)

    def test_op_in_one_run_only_renders_a_note(self):
        sweep = self._steps_with_per_op_speeds()
        entry = sweep[0]
        # Only run 1 ran GET: the other runs lose the op.
        for run in entry["repeat"]["runs"][1:]:
            del run["per_op"]["GET"]
        comparison = _per_op_fastest_slowest_histogram_comparison(
            entry["repeat"]["runs"], "GET")
        self.assertIsNone(comparison["rows"])
        self.assertEqual(
            comparison["notes"],
            ["Only one repeat run ran GET, so there is no "
             "fastest-vs-slowest histogram comparison for GET."])

    def test_missing_histogram_for_picked_run_renders_a_note(self):
        sweep = self._steps_with_per_op_speeds()
        entry = sweep[0]
        # The fastest run for SET (run 3) loses its SET buckets.
        del entry["repeat"]["runs"][2]["histograms_by_op"]["SET"]
        comparison = _per_op_fastest_slowest_histogram_comparison(
            entry["repeat"]["runs"], "SET")
        self.assertIsNone(comparison["rows"])
        self.assertEqual(
            comparison["notes"],
            ["Repeat run 3 (the fastest run for SET) carries no "
             "histogram for SET, so there is no fastest-vs-slowest "
             "histogram comparison for SET."])
        # The fastest run loses its histograms entirely: same, by
        # the no-per-op-histograms note.
        sweep = self._steps_with_per_op_speeds()
        entry = sweep[0]
        del entry["repeat"]["runs"][2]["histograms_by_op"]
        comparison = _per_op_fastest_slowest_histogram_comparison(
            entry["repeat"]["runs"], "SET")
        self.assertIsNone(comparison["rows"])
        self.assertEqual(
            comparison["notes"],
            ["Repeat run 3 (the fastest run for SET) carries no "
             "per-op histograms, so there is no fastest-vs-slowest "
             "histogram comparison for SET."])

    def test_per_op_comparison_covers_all_sweep_types(self):
        # Every sweep type's per-step repeat block carries the
        # per-op sections — they all render through the same
        # helper.
        workload = Workload(ops=200, clients=2, repeat=3)
        result = run_workload(DictClient, workload, "baseline (dict)")
        result.sweep = run_keyspace_sweep(DictClient, workload, [5],
                                          repeat=3)
        result.client_sweep = run_client_sweep(DictClient, workload,
                                               [1], repeat=3)
        result.value_sweep = run_value_size_sweep(
            DictClient, workload, [8], repeat=3)
        result.pipeline_sweep = run_pipeline_sweep(
            DictClient, workload, [1], repeat=3)
        result.mix_sweep = run_mix_sweep(
            DictClient, workload, [(100, 0), (50, 50)], repeat=3)
        text = render_markdown([result], workload, "kvbench")
        for heading in ("5 keys — SET fastest vs slowest repeat run",
                        "1 clients — SET fastest vs slowest repeat "
                        "run",
                        "8 byte values — SET fastest vs slowest "
                        "repeat run",
                        "1 commands — SET fastest vs slowest repeat "
                        "run",
                        "mix 100:0 — GET fastest vs slowest repeat "
                        "run",
                        "mix 50:50 — SET fastest vs slowest repeat "
                        "run"):
            self.assertIn(f"##### {heading}", text)
        # A read-only mix step ran no SET, so it gets no SET
        # section — only GET.
        self.assertNotIn("mix 100:0 — SET fastest vs slowest "
                         "repeat run", text)
        parsed = json.loads(render_json([result], workload, "kvbench"))
        doc = parsed["results"][0]
        for key in ("keyspace_sweep", "client_sweep",
                    "value_size_sweep", "pipeline_sweep",
                    "mix_sweep"):
            for entry in doc[key]:
                per_op = entry["repeat"]["histogram_comparison"][
                    "per_op"]
                self.assertTrue(per_op)
                for op, comparison in per_op.items():
                    self.assertEqual(comparison["op"], op)
        self.assertEqual(
            set(doc["mix_sweep"][0]["repeat"]
                ["histogram_comparison"]["per_op"]),
            {"GET"})
        self.assertEqual(
            set(doc["mix_sweep"][1]["repeat"]
                ["histogram_comparison"]["per_op"]),
            {"SET", "GET"})


class TestRepeatedSweepCli(unittest.TestCase):
    def test_sweep_and_repeat_compose_end_to_end(self):
        server = _start_server()
        try:
            with tempfile.TemporaryDirectory() as tmp:
                output = os.path.join(tmp, "report.md")
                out = StringIO()
                with redirect_stdout(out):
                    code = main([
                        "--host", "127.0.0.1",
                        "--port", str(server.server_address[1]),
                        "--ops", "200", "--clients", "2",
                        "--sweep-keyspace", "100,1000",
                        "--sweep-clients", "1,2",
                        "--sweep-value-size", "32,128",
                        "--sweep-pipeline", "1,5",
                        "--sweep-mix", "100:0,50:50",
                        "--repeat", "2",
                        "--output", output,
                    ])
                self.assertEqual(code, 0)
                self.assertIn("median of 2 runs", out.getvalue())
                with open(output, encoding="utf-8") as f:
                    text = f.read()
                self.assertIn("#### 100 keys — repeat runs", text)
                self.assertIn("#### 1 clients — repeat runs", text)
                self.assertIn("#### 32 byte values — repeat runs",
                              text)
                self.assertIn("#### 1 commands — repeat runs", text)
                self.assertIn("#### 5 commands — repeat runs", text)
                self.assertIn("#### mix 100:0 — repeat runs", text)
                self.assertIn("#### mix 50:50 — repeat runs", text)
                # Each step's repeat block also carries a per-op
                # fastest-vs-slowest section per op that ran.
                self.assertIn("##### 100 keys — SET fastest vs "
                              "slowest repeat run", text)
                self.assertIn("##### 100 keys — GET fastest vs "
                              "slowest repeat run", text)
                self.assertIn("##### mix 100:0 — GET fastest vs "
                              "slowest repeat run", text)
                with open(os.path.join(tmp, "report.json"),
                          encoding="utf-8") as f:
                    parsed = json.load(f)
                self.assertEqual(parsed["workload"]["repeat"], 2)
                self.assertEqual(parsed["workload"]["sweep_pipeline"],
                                 [1, 5])
                self.assertEqual(parsed["workload"]["sweep_mix"],
                                 ["100:0", "50:50"])
                doc = parsed["results"][0]
                for key in ("keyspace_sweep", "client_sweep",
                            "value_size_sweep", "pipeline_sweep",
                            "mix_sweep"):
                    self.assertEqual(len(doc[key]), 2)
                    for entry in doc[key]:
                        self.assertEqual(
                            len(entry["repeat"]["runs"]), 2)
                        self.assertEqual(
                            entry["ops_per_sec"],
                            _median_of([run["ops_per_sec"] for run in
                                        entry["repeat"]["runs"]]))
        finally:
            server.shutdown()
            server.server_close()


if __name__ == "__main__":
    unittest.main()
