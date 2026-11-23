"""Tests for kvbench.history pruning and the --prune CLI mode."""

import io
import json
import os
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime, timezone

from kvbench.bench import BenchmarkResult, Workload
from kvbench.cli import main
from kvbench.history import build_record, format_prune, \
    load_history, prune_history


def _make_result(name="cinder (127.0.0.1:6379)", ops_per_sec=50000.0):
    return BenchmarkResult(
        name=name, ops=1000, elapsed_s=1000 / ops_per_sec,
        latencies_ns=[20_000] * 1000, latencies_by_op={})


def _make_workload():
    return Workload(ops=1000, clients=4, keys=1000, value_size=32,
                    set_weight=50, get_weight=40, incr_weight=10,
                    seed=1234, pipeline=1)


def _write_entries(path, entries):
    with open(path, "w", encoding="utf-8") as f:
        for entry in entries:
            f.write(json.dumps(entry) + "\n")


class TestPruneHistory(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, "history.jsonl")

    def tearDown(self):
        self.tmp.cleanup()

    def _entries(self):
        # Five cinder runs, Nov 1-5, and two baseline runs, Nov 2
        # and Nov 4 — timestamps are set explicitly so the prune
        # selection is deterministic.
        workload = _make_workload()
        entries = []
        for day, rate in ((1, 40000.0), (2, 41000.0), (3, 42000.0),
                          (4, 43000.0), (5, 44000.0)):
            entry = build_record(_make_result(ops_per_sec=rate),
                                 workload, "kvbench")
            entry["timestamp"] = (
                f"2026-11-0{day}T10:00:00+00:00")
            entries.append(entry)
        for day, rate in ((2, 90000.0), (4, 95000.0)):
            entry = build_record(
                _make_result(name="baseline (dict)", ops_per_sec=rate),
                workload, "kvbench")
            entry["timestamp"] = f"2026-11-0{day}T10:00:00+00:00"
            entries.append(entry)
        return entries

    def test_keep_n_most_recent_per_backend(self):
        _write_entries(self.path, self._entries())
        summary = prune_history(self.path, keep=2)
        self.assertEqual(summary["removed"], 3)
        self.assertEqual(summary["kept"], 4)
        remaining = load_history(self.path)
        # Cinder keeps Nov 4 and 5; the baseline had only two runs,
        # so both stay.
        self.assertEqual(
            [e["ops_per_sec"] for e in remaining
             if e["backend"].startswith("cinder")],
            [43000.0, 44000.0])
        self.assertEqual(
            [e["ops_per_sec"] for e in remaining
             if e["backend"] == "baseline (dict)"],
            [90000.0, 95000.0])

    def test_before_drops_runs_older_than_the_date(self):
        _write_entries(self.path, self._entries())
        before = datetime(2026, 11, 4, tzinfo=timezone.utc)
        summary = prune_history(self.path, before=before)
        self.assertEqual(summary["removed"], 4)
        self.assertEqual(summary["kept"], 3)
        remaining = load_history(self.path)
        self.assertEqual(
            [e["timestamp"][:10] for e in remaining],
            ["2026-11-04", "2026-11-05", "2026-11-04"])
        # A run exactly at the cutoff stays: only older runs go.
        self.assertIn("2026-11-04T10:00:00+00:00",
                      [e["timestamp"] for e in remaining])

    def test_keep_and_before_combine(self):
        # Keep the 3 most recent cinder runs AND drop runs older
        # than Nov 4: Nov 3 goes to the keep limit, Nov 1-2 to the
        # date, leaving Nov 4-5 — and the Nov 2 baseline run goes
        # to the date too, since --prune-before applies per
        # backend.
        _write_entries(self.path, self._entries())
        before = datetime(2026, 11, 4, tzinfo=timezone.utc)
        summary = prune_history(self.path, keep=3, before=before)
        self.assertEqual(summary["removed"], 4)
        remaining = load_history(self.path)
        self.assertEqual(
            [e["ops_per_sec"] for e in remaining
             if e["backend"].startswith("cinder")],
            [43000.0, 44000.0])
        self.assertEqual(
            [e["ops_per_sec"] for e in remaining
             if e["backend"] == "baseline (dict)"],
            [95000.0])

    def test_backend_filter_leaves_other_backends_alone(self):
        _write_entries(self.path, self._entries())
        summary = prune_history(
            self.path, keep=1,
            backends=["cinder (127.0.0.1:6379)"])
        self.assertEqual(summary["removed"], 4)
        remaining = load_history(self.path)
        self.assertEqual(
            [e["ops_per_sec"] for e in remaining
             if e["backend"].startswith("cinder")],
            [44000.0])
        self.assertEqual(
            [e["ops_per_sec"] for e in remaining
             if e["backend"] == "baseline (dict)"],
            [90000.0, 95000.0])

    def test_dry_run_writes_nothing(self):
        _write_entries(self.path, self._entries())
        with open(self.path, encoding="utf-8") as f:
            before_text = f.read()
        summary = prune_history(self.path, keep=2, dry_run=True)
        self.assertTrue(summary["dry_run"])
        self.assertEqual(summary["removed"], 3)
        with open(self.path, encoding="utf-8") as f:
            self.assertEqual(f.read(), before_text)

    def test_refuses_to_empty_a_backend_without_prune_all(self):
        _write_entries(self.path, self._entries())
        with self.assertRaises(ValueError) as caught:
            prune_history(self.path,
                          before=datetime(2027, 1, 1,
                                          tzinfo=timezone.utc))
        self.assertIn("prune-all", str(caught.exception))
        # The file is untouched when the prune is refused.
        self.assertEqual(len(load_history(self.path)), 7)

    def test_prune_all_empties_selected_backends(self):
        _write_entries(self.path, self._entries())
        summary = prune_history(
            self.path, allow_all=True,
            backends=["cinder (127.0.0.1:6379)"])
        self.assertEqual(summary["removed"], 5)
        remaining = load_history(self.path)
        self.assertEqual(len(remaining), 2)
        self.assertTrue(all(e["backend"] == "baseline (dict)"
                            for e in remaining))

    def test_missing_file_returns_empty_summary(self):
        summary = prune_history(self.path, keep=2)
        self.assertEqual(summary["removed"], 0)
        self.assertEqual(summary["backends"], {})
        self.assertFalse(os.path.exists(self.path))

    def test_nothing_to_prune_writes_nothing(self):
        _write_entries(self.path, self._entries())
        with open(self.path, encoding="utf-8") as f:
            before_text = f.read()
        summary = prune_history(self.path, keep=20)
        self.assertEqual(summary["removed"], 0)
        with open(self.path, encoding="utf-8") as f:
            self.assertEqual(f.read(), before_text)

    def test_corrupt_lines_are_dropped_on_rewrite(self):
        # load_history skips the corrupt line, so a prune that
        # rewrites the file drops it too — the history only ever
        # holds records it can read back.
        with open(self.path, "w", encoding="utf-8") as f:
            f.write("not json at all\n")
            for entry in self._entries()[:3]:
                f.write(json.dumps(entry) + "\n")
        prune_history(self.path, keep=2)
        self.assertEqual(len(load_history(self.path)), 2)

    def test_unparseable_timestamp_is_kept_by_before(self):
        entries = self._entries()
        entries[0]["timestamp"] = "not a timestamp"
        _write_entries(self.path, entries)
        before = datetime(2026, 11, 4, tzinfo=timezone.utc)
        prune_history(self.path, before=before)
        remaining = load_history(self.path)
        # The undated run survives: its age cannot be known, so
        # the prune does not guess.
        self.assertIn("not a timestamp",
                      [e["timestamp"] for e in remaining])

    def test_surviving_runs_keep_their_original_order(self):
        _write_entries(self.path, self._entries())
        prune_history(self.path, keep=2)
        remaining = load_history(self.path)
        self.assertEqual(
            [e["timestamp"][:10] for e in remaining],
            ["2026-11-04", "2026-11-05", "2026-11-02", "2026-11-04"])


class TestFormatPrune(unittest.TestCase):
    def test_empty_summary_message(self):
        self.assertIn("No runs recorded yet",
                      format_prune({"removed": 0, "kept": 0,
                                    "backends": {}, "dry_run": False},
                                   "history.jsonl"))

    def test_summary_names_removed_range_and_kept(self):
        summary = {
            "removed": 3, "kept": 4, "dry_run": False,
            "backends": {
                "cinder (127.0.0.1:6379)": {
                    "removed": 3, "kept": 2,
                    "removed_range": ("2026-11-01 10:00:00",
                                      "2026-11-03 10:00:00")},
                "baseline (dict)": {
                    "removed": 0, "kept": 2, "removed_range": None},
            },
        }
        text = format_prune(summary, "history.jsonl")
        self.assertIn("cinder (127.0.0.1:6379): removed 3 run(s) "
                      "(2026-11-01 10:00:00 to 2026-11-03 10:00:00), "
                      "kept 2 run(s)", text)
        self.assertIn("baseline (dict): nothing to prune, kept 2 run(s)",
                      text)
        self.assertIn("Removed 3 run(s), kept 4 run(s) in total.", text)

    def test_dry_run_summary_says_nothing_was_removed(self):
        summary = {
            "removed": 3, "kept": 4, "dry_run": True,
            "backends": {
                "cinder (127.0.0.1:6379)": {
                    "removed": 3, "kept": 2,
                    "removed_range": ("2026-11-01 10:00:00",
                                      "2026-11-03 10:00:00")},
            },
        }
        text = format_prune(summary, "history.jsonl")
        self.assertIn("Dry run — nothing was removed", text)
        self.assertIn("would remove 3 run(s)", text)
        self.assertIn("Would remove 3 run(s), kept 4 run(s) in total.",
                      text)

    def test_nothing_to_prune(self):
        summary = {
            "removed": 0, "kept": 7, "dry_run": False,
            "backends": {
                "cinder (127.0.0.1:6379)": {
                    "removed": 0, "kept": 5, "removed_range": None},
            },
        }
        self.assertIn("Nothing to prune.",
                      format_prune(summary, "history.jsonl"))


class TestPruneCli(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, "history.jsonl")

    def tearDown(self):
        self.tmp.cleanup()

    def _record(self, rates, name="cinder (127.0.0.1:6379)"):
        workload = _make_workload()
        entries = []
        for day, rate in enumerate(rates, start=1):
            entry = build_record(_make_result(name=name,
                                              ops_per_sec=rate),
                                 workload, "kvbench")
            entry["timestamp"] = f"2026-11-0{day}T10:00:00+00:00"
            entries.append(entry)
        _write_entries(self.path, entries)

    def _run_main(self, argv):
        out, err = io.StringIO(), io.StringIO()
        with redirect_stdout(out), redirect_stderr(err):
            code = main(argv)
        return code, out.getvalue(), err.getvalue()

    def test_prune_flag_keeps_n_and_exits_zero(self):
        self._record((40000.0, 50000.0, 60000.0))
        code, out, _ = self._run_main(
            ["--prune", "--prune-keep", "2", "--history-file",
             self.path])
        self.assertEqual(code, 0)
        self.assertIn("removed 1 run(s)", out)
        self.assertIn("kept 2 run(s)", out)
        remaining = load_history(self.path)
        self.assertEqual([e["ops_per_sec"] for e in remaining],
                         [50000.0, 60000.0])

    def test_prune_before_flag_drops_old_runs(self):
        self._record((40000.0, 50000.0, 60000.0))
        code, out, _ = self._run_main(
            ["--prune", "--prune-before", "2026-11-02T00:00:00Z",
             "--history-file", self.path])
        self.assertEqual(code, 0)
        self.assertIn("removed 1 run(s)", out)
        remaining = load_history(self.path)
        self.assertEqual([e["ops_per_sec"] for e in remaining],
                         [50000.0, 60000.0])

    def test_prune_without_selector_fails(self):
        code, _, err = self._run_main(
            ["--prune", "--history-file", self.path])
        self.assertEqual(code, 2)
        self.assertIn("--prune needs --prune-keep, --prune-before or "
                      "--prune-all", err)

    def test_prune_keep_without_prune_fails(self):
        code, _, err = self._run_main(
            ["--prune-keep", "2", "--history-file", self.path])
        self.assertEqual(code, 2)
        self.assertIn("--prune-keep needs --prune", err)

    def test_dry_run_without_prune_fails(self):
        code, _, err = self._run_main(
            ["--dry-run", "--history-file", self.path])
        self.assertEqual(code, 2)
        self.assertIn("--dry-run needs --prune", err)

    def test_prune_all_with_keep_fails(self):
        code, _, err = self._run_main(
            ["--prune", "--prune-all", "--prune-keep", "2",
             "--history-file", self.path])
        self.assertEqual(code, 2)
        self.assertIn("--prune-all removes every run", err)

    def test_prune_keep_zero_fails(self):
        code, _, err = self._run_main(
            ["--prune", "--prune-keep", "0", "--history-file",
             self.path])
        self.assertEqual(code, 2)
        self.assertIn("--prune-keep must be at least 1", err)

    def test_prune_before_bad_timestamp_fails(self):
        code, _, err = self._run_main(
            ["--prune", "--prune-before", "not a date",
             "--history-file", self.path])
        self.assertEqual(code, 2)
        self.assertIn("prune-before must be an ISO timestamp", err)

    def test_prune_all_clears_the_history(self):
        self._record((40000.0, 50000.0))
        code, out, _ = self._run_main(
            ["--prune", "--prune-all", "--history-file", self.path])
        self.assertEqual(code, 0)
        self.assertIn("Removed 2 run(s)", out)
        self.assertEqual(load_history(self.path), [])

    def test_prune_that_would_empty_a_backend_is_refused(self):
        self._record((40000.0, 50000.0))
        code, _, err = self._run_main(
            ["--prune", "--prune-before", "2027-01-01T00:00:00Z",
             "--history-file", self.path])
        self.assertEqual(code, 2)
        self.assertIn("prune-all", err)
        self.assertEqual(len(load_history(self.path)), 2)

    def test_dry_run_prints_and_leaves_the_file_alone(self):
        self._record((40000.0, 50000.0, 60000.0))
        with open(self.path, encoding="utf-8") as f:
            before_text = f.read()
        code, out, _ = self._run_main(
            ["--prune", "--prune-keep", "2", "--dry-run",
             "--history-file", self.path])
        self.assertEqual(code, 0)
        self.assertIn("Dry run", out)
        self.assertIn("would remove 1 run(s)", out)
        with open(self.path, encoding="utf-8") as f:
            self.assertEqual(f.read(), before_text)

    def test_prune_with_empty_history_exits_zero(self):
        code, out, _ = self._run_main(
            ["--prune", "--prune-keep", "20", "--history-file",
             self.path])
        self.assertEqual(code, 0)
        self.assertIn("No runs recorded yet", out)


if __name__ == "__main__":
    unittest.main()
