"""Pure judgements of the torn-tail run: log lines, refusal, truncation evidence, aggregate verdict.

Acceptance: docs/specs/cluster-session-and-delivery.md "EKS 전체 voter 전원 손실·torn-tail·큰 snapshot 재시드 기준".
"""
from __future__ import annotations

import hashlib
import json
import re

import wal_frames

# The broker logs this INFO line when it cuts a torn WAL tail on open (fields: path, valid_bytes,
# dropped_bytes, reason). The fallback regex tolerates another wording of the same event.
TORN_TAIL_MESSAGE = "raft wal torn tail truncated"
TORN_TAIL_FALLBACK = re.compile(r"wal.*torn.*(truncat|cut|drop)|torn.*wal.*(truncat|cut|drop)", re.IGNORECASE)
TORN_TAIL_FIELDS = ("path", "valid_bytes", "dropped_bytes", "reason")
MAX_LINE = 300


def digest(value: str | bytes) -> str:
    return hashlib.sha256(value.encode("utf-8") if isinstance(value, str) else value).hexdigest()


def find_torn_tail_log(logs: str | None) -> dict:
    """Search broker log text for the truncation event. Returns what matched, never the whole log."""
    if logs is None:
        return {"logRead": False, "logged": False, "lines": []}
    lines: list[str] = []
    fields: dict = {}
    for line in logs.splitlines():
        if TORN_TAIL_MESSAGE in line or TORN_TAIL_FALLBACK.search(line):
            lines.append(line.strip()[:MAX_LINE])
            if not fields:
                try:
                    record = json.loads(line)
                except ValueError:
                    record = {}
                if isinstance(record, dict):
                    fields = {key: record[key] for key in TORN_TAIL_FIELDS if key in record}
    return {"logRead": True, "logged": bool(lines), "lines": lines[:3], "fields": fields,
            "exactMessage": any(TORN_TAIL_MESSAGE in line for line in lines)}


def truncation_evidence(injected: bytes, now: bytes | None, valid_bytes: int) -> dict:
    """File evidence read from the running broker that the injected tail was cut.

    The prefix up to `valid_bytes` is unchanged, the injected tail is not on disk any more, and the file is still a
    chain of valid frames (a partial last frame may be in flight while the broker appends). A compaction that
    rewrote the WAL since the start makes the prefix differ; the caller then relies on the log line.
    """
    if now is None:
        return {"fileRead": False, "cut": False}
    verdict = wal_frames.classify(now)
    prefix = now[:valid_bytes] == injected[:valid_bytes] and len(now) >= valid_bytes
    tail_gone = now[:len(injected)] != injected
    return {"fileRead": True, "sizeNow": len(now), "sha256Now": digest(now), "prefixPreserved": prefix,
            "injectedTailGone": tail_gone, "frameChainOutcome": verdict["outcome"],
            "cut": prefix and tail_gone and verdict["outcome"] in ("ok", "truncate")}


def classify_refusal(status: dict | None, logs: str | None, reason: str, previous_pod_uid: str,
                     pod_uid: str) -> dict:
    """The broker must end with a non-zero exit code and name the corrupt WAL. `waiting` means not terminated yet."""
    if status is None:
        return {"verdict": "waiting", "reason": "container_status_absent"}
    result = {"restartCount": status.get("restartCount"), "ready": bool(status.get("ready")),
              "newPod": pod_uid != previous_pod_uid}
    if not result["newPod"]:
        return {**result, "verdict": "waiting", "reason": "old_pod_still_present"}
    if result["ready"]:
        return {**result, "verdict": "fail", "reason": "container_became_ready"}
    terminated = (status.get("state") or {}).get("terminated") or (status.get("lastState") or {}).get("terminated")
    if not terminated:
        return {**result, "verdict": "waiting", "reason": "not_terminated"}
    result.update(exitCode=terminated.get("exitCode"), terminationReason=terminated.get("reason"))
    if terminated.get("exitCode") in (0, None):
        return {**result, "verdict": "fail", "reason": "exit_code_zero"}
    if logs is None:
        return {**result, "verdict": "waiting", "reason": "logs_unavailable"}
    line = next((entry for entry in logs.splitlines() if wal_frames.GO_ERROR_NAME in entry and reason in entry), None)
    result.update(logSha256=digest(logs), logMatched=line is not None)
    if line is None:
        return {**result, "verdict": "fail", "reason": "corruption_message_absent"}
    return {**result, "verdict": "refused", "reason": "non_zero_exit_with_corruption_message",
            "logLine": line.strip()[:MAX_LINE]}


def case_criteria(name: str) -> tuple[str, ...]:
    group = wal_frames.CASES[name]["group"]
    common = (f"{name}.injectedBytesVerified",)
    if group == "T1":
        return common + (f"{name}.readyAfterInjection", f"{name}.tailCut", f"{name}.markerIntact")
    return common + (f"{name}.refusedStart", f"{name}.neverReady", f"{name}.walUnchangedByRefusedStart",
                     f"{name}.restoredShaMatches", f"{name}.readyAfterRestore", f"{name}.markerIntact")


GLOBAL_CRITERIA = ("baseline.markerPublished", "final.threeReadyAndQuorum", "final.durableSession",
                   "cleanup.helperRemoved", "cleanup.workersUncordoned")


def criteria_for(cases: list[str]) -> list[str]:
    names = list(GLOBAL_CRITERIA)
    for name in cases:
        names.extend(case_criteria(name))
    return names


def aggregate(criteria: dict, cases: list[str], failed_step: str | None = None) -> dict:
    """PASS only when every named criterion is exactly True and no step failed."""
    verdicts = {name: criteria.get(name) is True for name in criteria_for(cases)}
    failed = [name for name, ok in verdicts.items() if not ok]
    return {"result": "PASS" if not failed and failed_step is None else "FAIL", "criteria": verdicts,
            "failedCriteria": failed, "failedStep": failed_step}
