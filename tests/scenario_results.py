"""Append scenario results as JSON Lines for ``go run ./tools/verify scenarios --results FILE``.

Each record is one line:
``{"test": "<repo path>#<def name>", "status": "pass|fail|skip", "commit": "...", "environment": "...",
"command": "...", "finishedAt": "..."}``. The ``test`` key is the repository-relative file path and the
function name, the same key the report tool derives from a ``# Scenarios:`` comment above the def.

``scenario_result`` wraps a scenario function so that it records ``pass`` or ``fail`` when the function
finishes. It never changes the function's return value or exceptions. A write error only prints a warning.
"""
from __future__ import annotations

import datetime
import functools
import json
import os
import platform
import subprocess
import sys
from pathlib import Path
from typing import Any, Callable, Optional

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RESULTS_PATH = REPOSITORY_ROOT / ".artifacts" / "verification" / "scenarios" / "python-results.jsonl"
RESULTS_PATH_ENV = "NARUMQ_SCENARIO_RESULTS"
ENVIRONMENT_ENV = "NARUMQ_SCENARIO_ENVIRONMENT"
STATUSES = ("pass", "fail", "skip")


def results_path() -> Path:
    override = os.environ.get(RESULTS_PATH_ENV)
    return Path(override) if override else DEFAULT_RESULTS_PATH


def result_key(function: Callable[..., Any]) -> str:
    """Return ``<repo-relative file>#<name>`` for a function defined in this repository."""
    filename = Path(function.__code__.co_filename).resolve()
    try:
        relative = filename.relative_to(REPOSITORY_ROOT).as_posix()
    except ValueError:
        relative = filename.as_posix()
    return relative + "#" + function.__name__


def git_commit() -> str:
    """Return the HEAD commit, with ``+dirty`` when the work tree has uncommitted changes, or ``unknown``."""
    try:
        head = subprocess.run(["git", "rev-parse", "HEAD"], cwd=REPOSITORY_ROOT, capture_output=True, text=True,
                              timeout=30, check=True).stdout.strip()
        dirty = subprocess.run(["git", "status", "--porcelain"], cwd=REPOSITORY_ROOT, capture_output=True, text=True,
                               timeout=30, check=True).stdout.strip()
    except (OSError, subprocess.SubprocessError):
        return "unknown"
    return head + ("+dirty" if dirty else "")


def environment_description() -> str:
    override = os.environ.get(ENVIRONMENT_ENV)
    if override:
        return override
    return "python {} {}/{}".format(platform.python_version(), platform.system(), platform.machine())


def record_result(key: str, status: str, path: Optional[Path] = None) -> dict:
    """Append one record and return it. Raises ``ValueError`` for an unknown status."""
    if status not in STATUSES:
        raise ValueError("status must be one of {}".format(", ".join(STATUSES)))
    record = {
        "test": key,
        "status": status,
        "commit": git_commit(),
        "environment": environment_description(),
        "command": " ".join(sys.argv),
        "finishedAt": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    target = path or results_path()
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("a", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(record, ensure_ascii=False) + "\n")
    return record


def outcome_status(outcome: Any) -> str:
    """Map a scenario return value to a status. A dict with ``result`` PASS, FAIL or SKIPPED is judged by it."""
    if isinstance(outcome, dict):
        result = str(outcome.get("result", "PASS")).upper()
        if result == "FAIL":
            return "fail"
        if result in ("SKIP", "SKIPPED"):
            return "skip"
    return "pass"


def _record_quietly(key: str, status: str) -> None:
    try:
        record_result(key, status)
    except (OSError, ValueError) as error:
        print("scenario_results: could not record {} {}: {}".format(key, status, error), file=sys.stderr)


def scenario_result(function: Callable[..., Any]) -> Callable[..., Any]:
    """Record ``pass``/``fail``/``skip`` when the wrapped scenario function finishes."""
    key = result_key(function)

    @functools.wraps(function)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        try:
            outcome = function(*args, **kwargs)
        except Exception:
            _record_quietly(key, "fail")
            raise
        _record_quietly(key, outcome_status(outcome))
        return outcome

    return wrapper
