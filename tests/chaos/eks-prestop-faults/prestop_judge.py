"""Offline judgments for the DRN-SCN-030 preStop, grace-period and admission harness.

Every function takes observed data and returns ``[(check name, ok, observed)]`` so the live runner can feed the
rows to ``Checks.check`` and the unit tests need no cluster. Nothing here assumes that a failing or long-running
preStop hook keeps a Pod alive: the spec (drain-and-rolling-update.md section 7) says it must not.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Any

sys.dont_write_bytecode = True
sys.path.append(str(Path(__file__).resolve().parents[1]))  # tests/chaos holds eks_broker.py
from eks_broker import PrerequisiteMissing  # noqa: E402

Check = tuple[str, bool, Any]

DEFAULT_DRAIN_TIMEOUT_MILLIS = 600_000
LOAD_BALANCER_WAIT_SECONDS = 30  # first 30 of ceil(timeoutMillis/1000)+30+30
SIGTERM_SHUTDOWN_SECONDS = 30  # second 30
TIMEOUT_ANNOTATION = "narumq.io/drain-timeout-millis"
POLICY_NAME = "narumq-statefulset-termination-grace"
PRESTOP_EVENT_REASON = "FailedPreStopHook"
GENERIC_EVENT_MESSAGE = "PreStopHook failed"
UNAUTHORIZED_REASON = "Drain query failed: HTTP 401"
# Failure reasons the live preStop script prints to stderr (deploy/kubernetes/eks-ec2-ha/statefulset.yaml).
KNOWN_HOOK_REASONS = ("Drain query failed: HTTP ", "Drain start failed: HTTP ", "Drain did not complete",
                      "Drain completion timed out", "Invalid or missing member slot")
# kubelet extends an expired grace period by 2 s, then runtime stop and the final status sync add a few seconds.
KUBELET_SLACK_SECONDS = 40.0
# ``kubectl delete`` returns before polling starts, so the observed elapsed time can be a little short.
OBSERVATION_TOLERANCE_SECONDS = 2.0


def minimum_grace_seconds(timeout_millis: int) -> int:
    """``ceil(timeoutMillis/1000) + 30 + 30`` from drain-and-rolling-update.md section 7."""
    if isinstance(timeout_millis, bool) or not isinstance(timeout_millis, int) or timeout_millis <= 0:
        raise ValueError(f"drain timeout must be a positive integer of milliseconds: {timeout_millis!r}")
    return -(-timeout_millis // 1000) + LOAD_BALANCER_WAIT_SECONDS + SIGTERM_SHUTDOWN_SECONDS


def drain_timeout_millis(annotations: dict[str, str] | None) -> int:
    """The deployed 600000 ms timeout; an annotation may only require a longer bound."""
    value = (annotations or {}).get(TIMEOUT_ANNOTATION)
    if value is None:
        return DEFAULT_DRAIN_TIMEOUT_MILLIS
    if not re.fullmatch(r"[0-9]+", value):
        raise ValueError(f"{TIMEOUT_ANNOTATION} must be an integer: {value!r}")
    timeout = int(value)
    if timeout < DEFAULT_DRAIN_TIMEOUT_MILLIS:
        raise ValueError(f"{TIMEOUT_ANNOTATION} must not shorten the deployed preStop timeout")
    return timeout


def grace_satisfies_formula(grace_seconds: int, timeout_millis: int) -> bool:
    return isinstance(grace_seconds, int) and grace_seconds >= minimum_grace_seconds(timeout_millis)


def extract_prestop_command(statefulset: dict[str, Any]) -> tuple[list[str], str]:
    """The real preStop exec command and the broker image of the live StatefulSet; raise if either is absent."""
    containers = statefulset.get("spec", {}).get("template", {}).get("spec", {}).get("containers", [])
    broker = next((c for c in containers if c.get("name") == "narumq"), None)
    if broker is None:
        raise PrerequisiteMissing("live StatefulSet has no container named narumq")
    command = ((broker.get("lifecycle") or {}).get("preStop") or {}).get("exec", {}).get("command")
    if not isinstance(command, list) or len(command) != 3 or command[:2] != ["/bin/sh", "-c"] or not command[2].strip():
        raise PrerequisiteMissing(f"live StatefulSet preStop is not an exec /bin/sh -c script: {command!r}")
    if not broker.get("image"):
        raise PrerequisiteMissing("live StatefulSet broker container has no image")
    return list(command), broker["image"]


def judge_live_statefulset(statefulset: dict[str, Any]) -> list[Check]:
    """The deployed broker StatefulSet itself obeys the grace formula the policy enforces."""
    grace = statefulset["spec"]["template"]["spec"].get("terminationGracePeriodSeconds")
    timeout = drain_timeout_millis(statefulset["metadata"].get("annotations"))
    return [("live StatefulSet terminationGracePeriodSeconds satisfies ceil(timeoutMillis/1000)+30+30",
             grace_satisfies_formula(grace, timeout), {"grace": grace, "timeoutMillis": timeout, "minimum": minimum_grace_seconds(timeout)})]


def failure_reason(stderr: str) -> str:
    """Last meaningful stderr line of a hook run, without the trailer ``kubectl exec`` appends."""
    lines = [line.strip() for line in (stderr or "").splitlines()
             if line.strip() and not line.startswith("command terminated with exit code")]
    return lines[-1] if lines else ""


def known_hook_reason(reason: str) -> bool:
    return any(reason.startswith(prefix) for prefix in KNOWN_HOOK_REASONS)


def judge_failed_hook_exec(result: dict[str, Any], expected_reason: str) -> list[Check]:
    """Run of the real script in the probe container: exit 1 and the documented failure reason on stderr."""
    reason = failure_reason(result.get("stderr", ""))
    return [("real preStop script exits 1 against the failing management endpoint", result.get("exitCode") == 1, result.get("exitCode")),
            ("hook stderr records the failure reason", reason == expected_reason, {"expected": expected_reason, "stderr": reason})]


def summarize_events(items: list[dict[str, Any]], uid: str | None = None) -> list[dict[str, Any]]:
    """Reduce Event objects of one Pod to the fields that matter, oldest first."""
    events = []
    for item in items:
        involved = item.get("involvedObject", {})
        if uid is not None and involved.get("uid") != uid:
            continue
        stamp = item.get("lastTimestamp") or item.get("eventTime") or item.get("firstTimestamp") or item["metadata"].get("creationTimestamp")
        events.append({"reason": item.get("reason"), "type": item.get("type"), "message": item.get("message", ""),
                       "count": item.get("count", 1), "at": stamp, "component": (item.get("source") or {}).get("component"),
                       "host": (item.get("source") or {}).get("host"), "container": (involved.get("fieldPath") or "")})
    return sorted(events, key=lambda event: event["at"] or "")


def event_message_status(message: str, reason: str) -> str:
    """``generic``: the kubelet does not copy hook output into the Event (since the secret-leak fix);
    ``matches``: the Event carries the hook failure reason; ``mismatch``: it carries something else."""
    text = (message or "").strip()
    if text == GENERIC_EVENT_MESSAGE:
        return "generic"
    return "matches" if reason in text else "mismatch"


def judge_failed_prestop_event(events: list[dict[str, Any]], reason: str) -> list[Check]:
    matches = [event for event in events if event["reason"] == PRESTOP_EVENT_REASON]
    status = [event_message_status(event["message"], reason) for event in matches]
    return [("kubelet recorded a FailedPreStopHook Warning Event", bool(matches) and all(event["type"] == "Warning" for event in matches),
             [{"type": event["type"], "message": event["message"], "at": event["at"]} for event in matches]),
            ("FailedPreStopHook message is the generic kubelet text or names the hook failure reason", bool(status) and "mismatch" not in status,
             {"messageStatus": status, "eventCarriesStderr": "matches" in status})]


def judge_killing_event(events: list[dict[str, Any]]) -> list[Check]:
    killing = [event for event in events if event["reason"] == "Killing"]
    return [("kubelet recorded a Killing Event for the terminated container", bool(killing),
             [{"message": event["message"], "at": event["at"]} for event in killing])]


def judge_termination(observation: dict[str, Any], grace_seconds: int, expect_forced: bool,
                      slack: float = KUBELET_SLACK_SECONDS, tolerance: float = OBSERVATION_TOLERANCE_SECONDS) -> list[Check]:
    """``observation``: ``{"gone": bool, "elapsedSeconds": float|None, "deletionGracePeriodSeconds": int|None}``.

    A hook that fails must not hold the Pod; a hook that never finishes ends at the grace period (plus the kubelet
    extension), not later. Both are bounded by ``grace + slack``; only the forced case also has a lower bound.
    """
    elapsed = observation.get("elapsedSeconds")
    gone = bool(observation.get("gone")) and elapsed is not None
    window = [grace_seconds - tolerance if expect_forced else 0.0, grace_seconds + slack]
    rows: list[Check] = [
        ("Pod object was removed (the hook did not block deletion permanently)", gone, observation),
        ("DELETE applied the Pod's own grace period", observation.get("deletionGracePeriodSeconds") == grace_seconds,
         observation.get("deletionGracePeriodSeconds")),
        ("Pod removal time is inside the bounded window", gone and window[0] <= elapsed <= window[1], {"elapsedSeconds": elapsed, "window": window}),
    ]
    if not expect_forced:
        rows.append(("failed hook did not consume the grace period", gone and elapsed < grace_seconds, {"elapsedSeconds": elapsed, "grace": grace_seconds}))
    return rows


def judge_duplicate_hooks(before_status: int, runs: list[dict[str, Any]], polled: list[dict[str, Any]], after: dict[str, Any] | None) -> list[Check]:
    """Two concurrent runs of the real hook against one follower.

    ``runs``: ``{"exitCode", "stderr"}`` each; ``polled``: every drain GET made while they ran
    (``{"status", "operationId"}``); ``after``: the drain object after both returned. A non-zero exit is a legitimate
    observation as long as the hook printed a documented failure reason; the single operation must still be COMPLETED.
    """
    reasons = [failure_reason(run.get("stderr", "")) for run in runs]
    failed = [(run.get("exitCode"), reason) for run, reason in zip(runs, reasons) if run.get("exitCode") != 0]
    operations = sorted({row["operationId"] for row in polled if row.get("operationId")} | ({after["operationId"]} if after and after.get("operationId") else set()))
    completed = bool(after) and after.get("phase") == "COMPLETED" and after.get("safeToStop") is True
    return [
        ("follower had no drain record before the duplicate calls (GET 404)", before_status == 404, before_status),
        ("both hook runs ended with a recorded exit code", len(runs) == 2 and all(isinstance(run.get("exitCode"), int) for run in runs),
         [run.get("exitCode") for run in runs]),
        ("every non-zero exit recorded a documented failure reason", all(code != 0 and known_hook_reason(reason) for code, reason in failed),
         {"failures": failed}),
        ("at least one run completed the drain with exit 0", any(run.get("exitCode") == 0 for run in runs), [run.get("exitCode") for run in runs]),
        ("exactly one drain operation id exists after the duplicate calls", len(operations) == 1, operations),
        ("the single operation is COMPLETED with safeToStop", completed, after),
    ]


def judge_probe_pod_command(pod: dict[str, Any], command: list[str]) -> list[Check]:
    containers = pod["spec"]["containers"]
    hook = ((containers[0].get("lifecycle") or {}).get("preStop") or {}).get("exec", {}).get("command")
    labels = pod["metadata"].get("labels", {})
    return [("probe Pod runs the live preStop command byte for byte", hook == command, {"matches": hook == command}),
            ("probe Pod is outside every broker selector (no app=narumq label)", "app" not in labels, labels)]


ERROR_REASON = re.compile(r"Error from server \((\w+)\)")


def judge_admission_rejected(returncode: int, stderr: str, grace_seconds: int, minimum: int, policy: str = POLICY_NAME) -> list[Check]:
    reason = ERROR_REASON.search(stderr or "")
    return [
        ("API server rejected the StatefulSet", returncode != 0 and reason is not None and reason.group(1) in ("Invalid", "Forbidden"),
         {"returncode": returncode, "reason": reason.group(1) if reason else None}),
        ("rejection names the admission policy", policy in (stderr or ""), policy),
        ("rejection message states the submitted grace and the formula minimum",
         f"terminationGracePeriodSeconds {grace_seconds} is shorter than" in (stderr or "") and f"= {minimum}" in (stderr or ""),
         {"grace": grace_seconds, "minimum": minimum}),
    ]


def judge_admission_accepted(returncode: int, stdout: str, stderr: str) -> list[Check]:
    return [("API server accepted the StatefulSet that satisfies the formula", returncode == 0 and "created" in (stdout or ""),
             {"returncode": returncode, "stdout": (stdout or "").strip(), "stderr": (stderr or "").strip()})]


def judge_policy_status(policy: dict[str, Any]) -> list[Check]:
    """Type checking of the CEL expressions must report no warning once the controller has filled the status."""
    warnings = ((policy.get("status") or {}).get("typeChecking") or {}).get("expressionWarnings") or []
    return [("policy CEL type checking reports no warning", not warnings, warnings)]
