"""Run unittest files and emit real method outcomes for scenario reports."""
from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path

from scenario_results import REPOSITORY_ROOT, record_result, results_path


class ScenarioTestResult(unittest.TextTestResult):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.outcomes: dict[str, str] = {}

    def remember(self, test, status):
        module = sys.modules[type(test).__module__]
        filename = Path(module.__file__).resolve()
        relative = filename.relative_to(REPOSITORY_ROOT).as_posix()
        self.merge(self.outcomes, relative + "#" + test._testMethodName, status)

    @staticmethod
    def merge(target, key, status):
        precedence = {"pass": 0, "skip": 1, "fail": 2}
        if key not in target or precedence[status] > precedence[target[key]]:
            target[key] = status

    def addSuccess(self, test):
        super().addSuccess(test)
        self.remember(test, "pass")

    def addFailure(self, test, error):
        super().addFailure(test, error)
        self.remember(test, "fail")

    def addError(self, test, error):
        super().addError(test, error)
        # Class/module fixture errors fail the command without inventing method outcomes.
        if hasattr(test, "_testMethodName"):
            self.remember(test, "fail")

    def addSkip(self, test, reason):
        super().addSkip(test, reason)
        if hasattr(test, "_testMethodName"):
            self.remember(test, "skip")

    def addExpectedFailure(self, test, error):
        super().addExpectedFailure(test, error)
        self.remember(test, "skip")

    def addUnexpectedSuccess(self, test):
        super().addUnexpectedSuccess(test)
        self.remember(test, "fail")

    def addSubTest(self, test, subtest, error):
        super().addSubTest(test, subtest, error)
        self.remember(test, "fail" if error else "pass")


def run_files(filenames):
    target = results_path()
    target.parent.mkdir(parents=True, exist_ok=True)
    # This file belongs to this invocation, including failures before the first test.
    target.write_text("", encoding="utf-8")
    suite = unittest.TestSuite()
    loader = unittest.TestLoader()
    for index, filename in enumerate(filenames):
        path = Path(filename).resolve()
        name = "scenario_unittest_" + str(index)
        spec = importlib.util.spec_from_file_location(name, path)
        if spec is None or spec.loader is None:
            raise ValueError("cannot load unittest file: " + filename)
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
        suite.addTests(loader.loadTestsFromModule(module))
    result = unittest.TextTestRunner(verbosity=2, resultclass=ScenarioTestResult).run(suite)
    for key, status in result.outcomes.items():
        record_result(key, status)
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    if len(sys.argv) < 2:
        raise SystemExit("usage: python tests/run_scenario_unittest.py <unittest-file>...")
    raise SystemExit(run_files(sys.argv[1:]))
