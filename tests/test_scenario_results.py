from __future__ import annotations

import contextlib
import io
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parent))
import scenario_results as results  # noqa: E402


def sample_scenario(value):
    return {"result": "PASS", "value": value}


class ResultFileTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / "nested" / "python-results.jsonl"
        environment = patch.dict(os.environ, {results.RESULTS_PATH_ENV: str(self.path), results.ENVIRONMENT_ENV: "unit-test env"})
        environment.start()
        self.addCleanup(environment.stop)
        commit = patch.object(results, "git_commit", return_value="abc123")
        commit.start()
        self.addCleanup(commit.stop)

    def lines(self):
        return [json.loads(line) for line in self.path.read_text(encoding="utf-8").splitlines()]

    def test_key_is_repository_relative_path_and_function_name(self):
        self.assertEqual(results.result_key(sample_scenario), "tests/test_scenario_results.py#sample_scenario")

    # Scenarios: performance-and-verification.md:PRF-SCN-114
    def test_record_result_appends_json_lines_with_run_fields(self):
        results.record_result("tests/x.py#scenario_a", "pass")
        results.record_result("tests/x.py#scenario_b", "skip")
        first, second = self.lines()
        self.assertEqual(first["test"], "tests/x.py#scenario_a")
        self.assertEqual(first["status"], "pass")
        self.assertEqual(first["commit"], "abc123")
        self.assertEqual(first["environment"], "unit-test env")
        self.assertEqual(first["command"], " ".join(sys.argv))
        self.assertRegex(first["finishedAt"], r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
        self.assertEqual(set(first), {"test", "status", "commit", "environment", "command", "finishedAt"})
        self.assertEqual(second["status"], "skip")

    def test_record_result_rejects_unknown_status(self):
        with self.assertRaises(ValueError):
            results.record_result("tests/x.py#a", "PASS")
        self.assertFalse(self.path.exists())

    # Scenarios: performance-and-verification.md:PRF-SCN-114
    def test_decorator_records_pass_from_result_dict_and_returns_value_unchanged(self):
        @results.scenario_result
        def scenario_ok():
            return {"result": "PASS", "extra": 1}

        self.assertEqual(scenario_ok(), {"result": "PASS", "extra": 1})
        self.assertEqual(scenario_ok.__name__, "scenario_ok")
        (record,) = self.lines()
        self.assertTrue(record["test"].endswith("#scenario_ok"))
        self.assertEqual(record["status"], "pass")

    def test_decorator_records_fail_for_fail_result_and_skip_for_skipped(self):
        @results.scenario_result
        def scenario_bad():
            return {"result": "FAIL"}

        @results.scenario_result
        def scenario_skipped():
            return {"result": "SKIPPED"}

        scenario_bad()
        scenario_skipped()
        self.assertEqual([record["status"] for record in self.lines()], ["fail", "skip"])

    # Scenarios: performance-and-verification.md:PRF-SCN-114
    def test_decorator_records_fail_and_reraises_exception(self):
        @results.scenario_result
        def scenario_raises():
            raise RuntimeError("boom")

        with self.assertRaisesRegex(RuntimeError, "boom"):
            scenario_raises()
        (record,) = self.lines()
        self.assertEqual(record["status"], "fail")

    def test_write_failure_does_not_change_scenario_behavior(self):
        blocker = Path(self.directory.name) / "blocker"
        blocker.write_text("file", encoding="utf-8")
        with patch.dict(os.environ, {results.RESULTS_PATH_ENV: str(blocker / "results.jsonl")}):
            @results.scenario_result
            def scenario_unwritable():
                return {"result": "PASS"}

            stderr = io.StringIO()
            with contextlib.redirect_stderr(stderr):
                outcome = scenario_unwritable()
        self.assertEqual(outcome, {"result": "PASS"})
        self.assertIn("could not record", stderr.getvalue())


if __name__ == "__main__":
    unittest.main()
