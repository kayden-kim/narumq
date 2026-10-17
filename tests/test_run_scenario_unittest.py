from __future__ import annotations

import contextlib
import io
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from run_scenario_unittest import ScenarioTestResult, run_files


class ScenarioUnittestRunnerTests(unittest.TestCase):
    # Scenarios: performance-and-verification.md:PRF-SCN-114
    def test_failed_subtest_is_not_overwritten_by_passing_subtest(self):
        class Subject(unittest.TestCase):
            def test_transition(self):
                for value in (False, True):
                    with self.subTest(value=value):
                        self.assertTrue(value)

        result = unittest.TextTestRunner(stream=io.StringIO(), resultclass=ScenarioTestResult).run(
            unittest.defaultTestLoader.loadTestsFromTestCase(Subject)
        )
        self.assertFalse(result.wasSuccessful())
        self.assertEqual(result.outcomes, {"tests/test_run_scenario_unittest.py#test_transition": "fail"})

    # Scenarios: performance-and-verification.md:PRF-SCN-114
    def test_skip_and_expected_failure_are_not_passes(self):
        class Subject(unittest.TestCase):
            @unittest.skip("external fixture unavailable")
            def test_external(self):
                self.fail("must not execute")

            @unittest.expectedFailure
            def test_unfinished(self):
                self.fail("known defect")

        result = unittest.TextTestRunner(stream=io.StringIO(), resultclass=ScenarioTestResult).run(
            unittest.defaultTestLoader.loadTestsFromTestCase(Subject)
        )
        self.assertEqual(result.outcomes, {
            "tests/test_run_scenario_unittest.py#test_external": "skip",
            "tests/test_run_scenario_unittest.py#test_unfinished": "skip",
        })

    # Scenarios: performance-and-verification.md:PRF-SCN-114
    def test_import_failure_clears_stale_results_and_preserves_the_error(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fixture = root / "import_failure.py"
            fixture.write_text('raise RuntimeError("fixture import failed")\n', encoding="utf-8")
            output = root / "results" / "run.jsonl"
            output.parent.mkdir()
            output.write_text('{"test":"old","status":"pass"}\n', encoding="utf-8")
            with patch.dict(os.environ, {"NARUMQ_SCENARIO_RESULTS": str(output)}), patch.dict(sys.modules):
                with self.assertRaisesRegex(RuntimeError, "fixture import failed"):
                    run_files([str(fixture)])
            self.assertEqual(output.read_bytes(), b"")

    # Scenarios: performance-and-verification.md:PRF-SCN-114
    def test_class_setup_failure_creates_empty_results_and_fails(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fixture = root / "setup_failure.py"
            fixture.write_text(
                "import unittest\n"
                "class Subject(unittest.TestCase):\n"
                "    @classmethod\n"
                "    def setUpClass(cls):\n"
                "        raise RuntimeError('class setup failed')\n"
                "    def test_unreachable(self):\n"
                "        raise AssertionError('must not run')\n",
                encoding="utf-8",
            )
            output = root / "results" / "run.jsonl"
            with patch.dict(os.environ, {"NARUMQ_SCENARIO_RESULTS": str(output)}), patch.dict(sys.modules):
                with contextlib.redirect_stderr(io.StringIO()):
                    status = run_files([str(fixture)])
            self.assertEqual(status, 1)
            self.assertEqual(output.read_bytes(), b"")


if __name__ == "__main__":
    unittest.main()
