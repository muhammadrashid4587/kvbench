"""Tests for the GitHub Actions benchmark workflow (.github/workflows/bench.yml).

The workflow runs kvbench against cinder on every push, gated by the
checked-in SLO preset (slo.json). There is no Actions runner on a dev
VM, so these tests verify the workflow file itself instead: that it
parses, triggers on push, checks out and builds the real cinder repo,
starts it on the port kvbench talks to, runs kvbench with flags the
real CLI parser accepts, and uploads both reports as an artifact.
"""

import importlib.util
import os
import shlex
import unittest

from kvbench.cli import build_parser
from kvbench.slo import load_slo_preset

yaml = None
if importlib.util.find_spec("yaml") is not None:
    import yaml

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
WORKFLOW_PATH = os.path.join(REPO_ROOT, ".github", "workflows", "bench.yml")
PRESET_PATH = os.path.join(REPO_ROOT, "slo.json")


def _load_workflow():
    with open(WORKFLOW_PATH, encoding="utf-8") as f:
        return yaml.safe_load(f)


def _steps(workflow):
    return workflow["jobs"]["bench"]["steps"]


def _bench_step(workflow):
    for step in _steps(workflow):
        if "kvbench.cli" in step.get("run", ""):
            return step
    raise AssertionError("no step runs kvbench.cli")


@unittest.skipUnless(yaml is not None, "PyYAML not installed")
class TestBenchWorkflow(unittest.TestCase):
    def test_workflow_parses(self):
        workflow = _load_workflow()
        self.assertIn("jobs", workflow)
        self.assertIn("bench", workflow["jobs"])

    def test_triggers_on_push(self):
        workflow = _load_workflow()
        # YAML 1.1 parses a bare `on:` key as boolean True, and a
        # trigger list with no values as a dict of trigger names.
        triggers = workflow.get("on", workflow.get(True))
        if isinstance(triggers, dict):
            names = list(triggers.keys())
        elif isinstance(triggers, list):
            names = triggers
        else:
            names = [triggers]
        self.assertIn("push", names)

    def test_checks_out_cinder(self):
        workflow = _load_workflow()
        checkouts = [s for s in _steps(workflow)
                     if s.get("uses", "").startswith("actions/checkout@")]
        self.assertGreaterEqual(len(checkouts), 2)
        cinder = [s for s in checkouts
                  if s.get("with", {}).get("repository")
                  == "muhammadrashid4587/cinder"]
        self.assertEqual(len(cinder), 1)
        self.assertEqual(cinder[0]["with"]["path"], "cinder")

    def test_builds_cinder(self):
        workflow = _load_workflow()
        runs = [s.get("run", "") for s in _steps(workflow)]
        self.assertTrue(any("make" in run and "cinder" in run
                            for run in runs),
                        "no step builds cinder with make")

    def test_starts_cinder_on_bench_port(self):
        workflow = _load_workflow()
        runs = [s.get("run", "") for s in _steps(workflow)]
        self.assertTrue(any("./cinder --port 6379" in run for run in runs),
                        "no step starts cinder on port 6379")

    def test_bench_command_uses_real_cli_flags(self):
        workflow = _load_workflow()
        run = _bench_step(workflow)["run"]
        argv = shlex.split(run)
        self.assertEqual(argv[:3], ["python3", "-m", "kvbench.cli"])
        # The real parser rejects unknown flags, so this proves every
        # flag the workflow passes is a flag kvbench actually has.
        args = build_parser().parse_args(argv[3:])
        self.assertEqual(args.host, "127.0.0.1")
        self.assertEqual(args.port, 6379)
        self.assertEqual(args.ops, 20000)
        self.assertEqual(args.clients, 4)
        self.assertEqual(args.slo_preset, "slo.json")
        self.assertEqual(args.output, "report.md")

    def test_bench_command_references_preset_in_repo(self):
        workflow = _load_workflow()
        args = build_parser().parse_args(
            shlex.split(_bench_step(workflow)["run"])[3:])
        preset_path = os.path.join(REPO_ROOT, args.slo_preset)
        self.assertTrue(os.path.exists(preset_path),
                        f"preset {args.slo_preset} is not in the repo")

    def test_uploads_both_reports_even_on_failure(self):
        workflow = _load_workflow()
        uploads = [s for s in _steps(workflow)
                   if s.get("uses", "").startswith("actions/upload-artifact@")]
        self.assertEqual(len(uploads), 1)
        self.assertEqual(uploads[0].get("if"), "always()")
        self.assertIn("report.md", uploads[0]["with"]["path"])
        self.assertIn("report.json", uploads[0]["with"]["path"])


class TestCiSloPreset(unittest.TestCase):
    def test_preset_loads_with_expected_thresholds(self):
        preset = load_slo_preset(PRESET_PATH)
        self.assertEqual(preset["p99_ms"], 5)
        self.assertEqual(preset["error_rate_pct"], 1)
        self.assertEqual(preset["per_op"]["SET"], (10, 0.1))
        self.assertEqual(preset["per_op"]["GET"], (5, 0.1))


if __name__ == "__main__":
    unittest.main()
