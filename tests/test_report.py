"""Tests for kvbench.report Markdown output."""

import unittest

from kvbench.bench import BenchmarkResult, Workload
from kvbench.report import render_markdown


class TestReport(unittest.TestCase):
    def test_report_contains_results(self):
        workload = Workload(ops=100, clients=2, keys=10, value_size=8,
                            set_weight=50, get_weight=40, incr_weight=10)
        result = BenchmarkResult(
            name="cinder (127.0.0.1:6379)",
            ops=100,
            elapsed_s=0.01,
            errors=0,
            latencies_ns=[100_000] * 100,
        )
        text = render_markdown([result], workload,
                               "kvbench --ops 100 --clients 2")
        self.assertIn("# kvbench report", text)
        self.assertIn("cinder (127.0.0.1:6379)", text)
        self.assertIn("| Backend | Ops | Ops/sec |", text)
        self.assertIn("10,000", text)  # 100 ops / 0.01 s
        self.assertIn("0.100", text)  # mean latency ms
        self.assertIn("Total ops: 100", text)
        self.assertIn("Clients (threads): 2", text)
        self.assertIn("kvbench --ops 100 --clients 2", text)
        self.assertIn("machine-dependent", text)
        self.assertIn("baseline (dict)", text)  # the notes mention it

    def test_error_sample_listed(self):
        workload = Workload(ops=10, clients=1, keys=5)
        result = BenchmarkResult(
            name="cinder", ops=10, elapsed_s=0.01, errors=2,
            error_sample="ConnectionError: refused",
            latencies_ns=[100_000] * 8,
        )
        text = render_markdown([result], workload, "kvbench")
        self.assertIn("First error from cinder: ConnectionError: refused",
                      text)
        self.assertIn("| 2 |", text)


if __name__ == "__main__":
    unittest.main()
