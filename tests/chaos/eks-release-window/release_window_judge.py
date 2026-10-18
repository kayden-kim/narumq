"""Pure judgment for DRN-SCN-020: upgrade and rollback between published N-1 and N use the same safe-to-stop barrier.

Nothing here touches a cluster or the network. ``main.py`` feeds these functions with observed data, and
``tests/test_eks_release_window.py`` feeds them synthetic observations. Inputs that the scenario cannot do without
(a published-release record, a published pair, a fixture that starts at N-1) raise ``MissingPublishedRelease`` or
``ReleasePrerequisiteError`` so ``scenario_result`` records a failure instead of a skip.
"""

from __future__ import annotations

import json
import re
import sys
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))  # tests/chaos: eks_rollout_ops

from eks_rollout_ops import (  # noqa: E402
    MissingPublishedRelease, ReleaseError, is_synthetic_version, parse_version, repository_of, tag_resolves_to,
    versions_compatible,
)

RECORD_SCHEMA_VERSION = 1
SOURCE_TYPES = ("github-release", "registry-tag")
ORDINALS = (0, 1, 2)
# Keys of the approval event ``details`` the controller writes (internal/application/rollout/service.go approveDelete).
EVIDENCE_KEYS = (
    "rollout", "rolloutUid", "workload", "ordinal", "nodeId", "podUid", "podResourceVersion", "attemptKey", "drainOperationId",
    "targetImage", "targetReleaseVersion", "drainPhase", "drainState", "drainSafeToStop", "connectionsInitial",
    "connectionsRemaining", "sessionsReady", "sessionsTakenOver", "sessionsFailed", "pendingWAL", "underReplicated",
    "drainFailureCode", "blockingReason", "safeToStopObservedAt",
)
CONDITION_NAMES = (
    "allowedSafeToStop", "drainSafeToStop", "connectionsRemaining0", "pendingWAL0", "underReplicated0", "sessionsFailed0",
    "drainPhaseCompleted", "drainStateSafeToStop", "noDrainFailureCode", "noBlockingReason", "approvedAfterSafeToStop",
)
_TIMESTAMP = re.compile(r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d{1,9}))?(Z|[+-]\d{2}:\d{2})$")


class ReleasePrerequisiteError(RuntimeError):
    """The live fixture or an input does not satisfy a scenario prerequisite."""


def parse_time(value: Any) -> Decimal:
    """RFC 3339 epoch seconds, preserving nanoseconds on every supported Python version."""
    if not isinstance(value, str) or (match := _TIMESTAMP.fullmatch(value)) is None:
        raise ValueError(f"timestamp {value!r} is not RFC 3339")
    seconds, fraction, offset = match.groups()
    parsed = datetime.fromisoformat(seconds + ("+00:00" if offset == "Z" else offset))
    return Decimal(int(parsed.timestamp())) + Decimal(int((fraction or "").ljust(9, "0"))) / 1_000_000_000


# Published release record --------------------------------------------------------------------------
def load_release_records(path: Path | None) -> dict[str, Any]:
    """Read ``--release-record``; a missing file or schema violation raises ``MissingPublishedRelease``."""
    if path is None:
        raise MissingPublishedRelease("no --release-record: DRN-SCN-020 needs a published-release record for N-1 and N")
    if not Path(path).is_file():
        raise MissingPublishedRelease(f"release record {path} does not exist")
    try:
        document = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    except ValueError as error:
        raise MissingPublishedRelease(f"release record {path} is not JSON: {error}") from error
    if not isinstance(document, dict) or document.get("schemaVersion") != RECORD_SCHEMA_VERSION \
            or not isinstance(document.get("releases"), list):
        raise MissingPublishedRelease(f"release record {path} must be {{schemaVersion: {RECORD_SCHEMA_VERSION}, releases: [...]}}")
    return document


def _record_problems(record: Any, now: float) -> list[str]:
    if not isinstance(record, dict):
        return ["record is not an object"]
    problems = [f"field {name} is missing" for name in ("version", "digest", "repository", "publishedAt", "source") if not record.get(name)]
    source = record.get("source")
    if isinstance(source, dict):
        if source.get("type") not in SOURCE_TYPES:
            problems.append(f"source.type must be one of {SOURCE_TYPES}")
        elif source["type"] == "github-release" and not re.match(r"^https://github\.com/[^/\s]+/[^/\s]+/releases/tag/\S+$", str(source.get("url", ""))):
            problems.append("source.url must be a GitHub release tag URL")
        elif source["type"] == "registry-tag" and not source.get("reference"):
            problems.append("source.reference (repository:tag) is missing")
    elif source:
        problems.append("source must be an object")
    if record.get("publishedAt"):
        try:
            if parse_time(record["publishedAt"]) > now:
                problems.append("publishedAt is in the future")
        except ValueError as error:
            problems.append(f"publishedAt: {error}")
    return problems


def bind_published(image: dict[str, Any], records: dict[str, Any], now: float, tag_manifest: bytes | None = None) -> dict[str, Any]:
    """The published-release record for a verified image, or ``MissingPublishedRelease`` naming what is absent.

    The record must match the image on version, digest and repository, carry a publication time not in the future,
    and name its source. A ``registry-tag`` source must be resolved by the caller (``tag_manifest`` is the manifest
    the tag points to) and must lead to this platform digest; a ``github-release`` URL must end in the version tag.
    """
    matches = [r for r in records["releases"] if isinstance(r, dict) and r.get("version") == image["version"]]
    if not matches:
        raise MissingPublishedRelease(f"no published-release record for version {image['version']} ({image['reference']})")
    if len(matches) > 1:
        raise MissingPublishedRelease(f"release record names version {image['version']} {len(matches)} times")
    record = matches[0]
    problems = _record_problems(record, now)
    if problems:
        raise MissingPublishedRelease(f"release record for {image['version']} is incomplete: {'; '.join(problems)}")
    if record["digest"] != image["digest"]:
        raise MissingPublishedRelease(f"release record digest {record['digest']} differs from the image digest {image['digest']}")
    if record["repository"] != image["repository"]:
        raise MissingPublishedRelease(f"release record repository {record['repository']} differs from the image repository {image['repository']}")
    source = record["source"]
    if source["type"] == "github-release":
        tag = source["url"].rsplit("/", 1)[-1]
        if tag not in (image["version"], "v" + image["version"]):
            raise MissingPublishedRelease(f"GitHub release URL tag {tag!r} does not name version {image['version']}")
    else:
        reference = source["reference"]
        if repository_of(reference) != image["repository"] or reference.rsplit(":", 1)[-1] not in (image["version"], "v" + image["version"]):
            raise MissingPublishedRelease(f"registry tag {reference!r} does not name {image['repository']}:{image['version']}")
        if tag_manifest is None:
            raise MissingPublishedRelease(f"registry tag {reference!r} was not resolved")
        if not tag_resolves_to(tag_manifest, image["digest"]):
            raise MissingPublishedRelease(f"registry tag {reference!r} does not resolve to {image['digest']}")
    return {"version": record["version"], "digest": record["digest"], "repository": record["repository"],
            "publishedAt": record["publishedAt"], "source": source}


def judge_release_pair(previous: dict[str, Any], current: dict[str, Any]) -> list[str]:
    """Problems that make (N-1, N) unusable as the published pair; an empty list means usable."""
    problems = []
    try:
        old, new = parse_version(previous["version"]), parse_version(current["version"])
    except ReleaseError as error:
        return [str(error)]
    for label, image, version in (("N-1", previous, old), ("N", current, new)):
        if is_synthetic_version(version):
            problems.append(f"{label} release {image['version']} is a synthetic 0.0.x fixture build, not a published release")
    if new <= old:
        problems.append(f"N {current['version']} is not a later release than N-1 {previous['version']}")
    if not versions_compatible(old, new):
        problems.append(f"N-1 {previous['version']} and N {current['version']} are outside the adjacent-minor window (ADR-0018)")
    if previous["digest"] == current["digest"]:
        problems.append("N-1 and N are the same image digest")
    if previous["repository"] != current["repository"]:
        problems.append("N-1 and N come from different repositories; the StatefulSet template swap needs one repository")
    return problems


def check_active_features(active: list[str], previous: dict[str, Any], current: dict[str, Any]) -> list[str]:
    """The controller refuses a target that lacks a committed active feature; rollback to N-1 needs N-1 to support it."""
    return [f"active feature {feature} is not supported by {label} {image['version']}"
            for feature in active for label, image in (("N-1", previous), ("N", current)) if feature not in image["features"]]


def judge_start_state(members: list[dict[str, Any]], pods: dict[str, dict[str, Any]], previous: dict[str, Any]) -> list[str]:
    """The fixture starts at N-1: three voters report N-1 and three Ready Pods run the N-1 digest."""
    problems = []
    if len(members) != 3:
        problems.append(f"cluster features report {len(members)} voters, expected 3")
    problems += [f"voter {m['nodeId']} reports release {m['releaseVersion']}, expected {previous['version']}"
                 for m in members if m["releaseVersion"] != previous["version"]]
    if sorted(pods) != [f"narumq-{o}" for o in ORDINALS]:
        problems.append(f"broker Pods are {sorted(pods)}")
    for name, pod in sorted(pods.items()):
        if pod["image"] != previous["reference"] or not (pod.get("imageID") or "").endswith(previous["digest"]):
            problems.append(f"{name} does not run the N-1 digest")
        if not pod["ready"] or pod["deleting"]:
            problems.append(f"{name} is not Ready")
    return problems


# Barrier judgment ----------------------------------------------------------------------------------
def approval_conditions(event: dict[str, Any]) -> dict[str, bool]:
    """Every condition one approval must satisfy; a missing field makes its condition False, never True."""
    details = event.get("details") or {}
    try:
        after_safe = parse_time(event["occurredAt"]) >= parse_time(details["safeToStopObservedAt"])
    except (KeyError, ValueError):
        after_safe = False
    return {
        "allowedSafeToStop": event.get("result") == "ALLOWED" and event.get("reason") == "safe_to_stop",
        "drainSafeToStop": details.get("drainSafeToStop") is True,
        "connectionsRemaining0": details.get("connectionsRemaining") == 0,
        "pendingWAL0": details.get("pendingWAL") == 0,
        "underReplicated0": details.get("underReplicated") == 0,
        "sessionsFailed0": details.get("sessionsFailed") == 0,
        "drainPhaseCompleted": details.get("drainPhase") == "COMPLETED",
        "drainStateSafeToStop": details.get("drainState") == "SAFE_TO_STOP",
        "noDrainFailureCode": details.get("drainFailureCode") in ("", None),
        "noBlockingReason": details.get("blockingReason") in ("", None),
        "approvedAfterSafeToStop": after_safe,
    }


def judge_direction(label: str, target: dict[str, Any], before: dict[str, dict[str, Any]], after: dict[str, dict[str, Any]],
                    final_status: dict[str, Any], approvals: list[dict[str, Any]], features: dict[str, Any],
                    quorum_samples: list[dict[str, Any]]) -> dict[str, Any]:
    """Judge one direction (upgrade or rollback) and return its problems plus the barrier fingerprint to compare."""
    problems: list[str] = []
    if final_status.get("phase") != "Completed" or sorted(final_status.get("completedOrdinals", [])) != list(ORDINALS):
        problems.append(f"{label}: rollout ended {final_status.get('phase')} with completed {final_status.get('completedOrdinals')} "
                        f"({final_status.get('failureCode')})")
    for name, pod in sorted(before.items()):
        now = after.get(name)
        if now is None or now["uid"] == pod["uid"]:
            problems.append(f"{label}: {name} was not replaced (UID unchanged)")
        elif now["image"] != target["reference"] or not (now.get("imageID") or "").endswith(target["digest"]):
            problems.append(f"{label}: {name} does not run the target digest")
        elif not now["ready"] or now["deleting"]:
            problems.append(f"{label}: {name} is not Ready")
    if sorted(after) != sorted(before) or len(before) != 3:
        problems.append(f"{label}: broker Pod set changed from {sorted(before)} to {sorted(after)}")
    members = features.get("members", [])
    if len(members) != 3 or any(m["releaseVersion"] != target["version"] or m["features"] != target["features"] for m in members):
        problems.append(f"{label}: cluster features do not report {target['version']} with {target['features']} on all three voters: {members}")
    if len(quorum_samples) < 3:
        problems.append(f"{label}: only {len(quorum_samples)} quorum samples")
    bad = [s for s in quorum_samples if s.get("quorumAvailable") is not True]
    if bad:
        problems.append(f"{label}: quorum was not reported available in {len(bad)} of {len(quorum_samples)} samples: {bad[:2]}")
    if len(approvals) != 3:
        problems.append(f"{label}: {len(approvals)} delete approvals in the audit chain, expected 3")
    keys, conditions, order = None, [], []
    for event in approvals:
        details = event.get("details") or {}
        ordinal = details.get("ordinal")
        order.append(ordinal)
        pod_name = f"narumq-{ordinal}"
        if event.get("target", {}).get("id") != pod_name or details.get("nodeId") != (ordinal + 1 if isinstance(ordinal, int) else None):
            problems.append(f"{label}: approval target {event.get('target')} does not match ordinal {ordinal}")
        if pod_name in before and details.get("podUid") != before[pod_name]["uid"]:
            problems.append(f"{label}: approval for {pod_name} names Pod UID {details.get('podUid')}, not the replaced one")
        if details.get("targetImage") != target["reference"] or details.get("targetReleaseVersion") != target["version"]:
            problems.append(f"{label}: approval for {pod_name} names target {details.get('targetImage')} {details.get('targetReleaseVersion')}")
        missing = [k for k in EVIDENCE_KEYS if k not in details]
        if missing:
            problems.append(f"{label}: approval for {pod_name} lacks evidence fields {missing}")
        keys = sorted(details) if keys is None else keys
        if sorted(details) != keys:
            problems.append(f"{label}: approvals within the direction carry different evidence field sets")
        outcome = approval_conditions(event)
        conditions.append(outcome)
        problems += [f"{label}: approval for {pod_name} fails {name}" for name, ok in outcome.items() if not ok]
    if sorted(o for o in order if isinstance(o, int)) != list(ORDINALS):
        problems.append(f"{label}: approvals cover ordinals {order}, expected each of {list(ORDINALS)} once")
    return {"direction": label, "passed": not problems, "problems": problems, "evidenceKeys": keys or [],
            "ordinalOrder": order, "conditions": conditions, "target": target["version"]}


def compare_directions(upgrade: dict[str, Any], rollback: dict[str, Any]) -> list[str]:
    """The two directions must pass the same barrier: same evidence fields, conditions, and ordinal order."""
    problems = []
    if not upgrade["evidenceKeys"] or upgrade["evidenceKeys"] != rollback["evidenceKeys"]:
        problems.append(f"evidence field sets differ: {upgrade['evidenceKeys']} vs {rollback['evidenceKeys']}")
    for name in CONDITION_NAMES:
        outcomes = {direction["direction"]: sorted({c.get(name) for c in direction["conditions"]}, key=str)
                    for direction in (upgrade, rollback)}
        if outcomes[upgrade["direction"]] != [True] or outcomes[rollback["direction"]] != [True]:
            problems.append(f"barrier condition {name} is not satisfied in both directions: {outcomes}")
    if upgrade["ordinalOrder"] != rollback["ordinalOrder"]:
        problems.append(f"replacement order differs: {upgrade['ordinalOrder']} vs {rollback['ordinalOrder']}")
    return problems


def judge_release_window(previous: dict[str, Any], current: dict[str, Any], upgrade: dict[str, Any],
                         rollback: dict[str, Any]) -> dict[str, Any]:
    """Final verdict over both directions and their comparison."""
    problems = judge_release_pair(previous, current) + upgrade["problems"] + rollback["problems"] + compare_directions(upgrade, rollback)
    return {"passed": not problems, "problems": problems}
