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
                           run_keyspace_sweep, run_pipeline_sweep,
                           run_value_size_sweep, run_workload)
from kvbench.cli import main
from kvbench.report import render_json, render_markdown

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
        self.assertIn("Median of 3 runs", text)
        self.assertIn("Spread: ops/sec", text)
        parsed = json.loads(render_json([result], workload, "kvbench"))
        doc = parsed["results"][0]
        for key in ("keyspace_sweep", "client_sweep",
                    "value_size_sweep", "pipeline_sweep"):
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
                with open(os.path.join(tmp, "report.json"),
                          encoding="utf-8") as f:
                    parsed = json.load(f)
                self.assertEqual(parsed["workload"]["repeat"], 2)
                self.assertEqual(parsed["workload"]["sweep_pipeline"],
                                 [1, 5])
                doc = parsed["results"][0]
                for key in ("keyspace_sweep", "client_sweep",
                            "value_size_sweep", "pipeline_sweep"):
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
