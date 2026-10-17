"""Pure judgment for DRN-SCN-019: 100 consecutive rolling updates at the measured maximum expected traffic.

Nothing here touches a cluster. ``main.py`` feeds these functions with observed data and
``tests/test_eks_rolling_capacity.py`` feeds them synthetic data.

The row says the absolute gap, duplicate and reorder limits are decided only after the real ``B_conn``, ``B_pub`` and
``B_sub`` are fixed (docs/specs/drain-and-rolling-update.md §10). Neither the capacity runner's artifacts nor the
specs carry such a field, so the harness requires them as an explicit ``--cutoffs`` file bound to the capacity
result by its profile hash, and raises ``MissingCapacityBaseline`` while either input is absent.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
import sys
from pathlib import Path
from typing import Any

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))  # tests/chaos: eks_rollout_ops

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "eks-release-window"))  # the shared safe-to-stop barrier judge

from eks_rollout_ops import ReleaseError, canonical_json, parse_version, versions_compatible  # noqa: E402
from release_window_judge import approval_conditions  # noqa: E402

CELL_CLIENTS = 53
CELL_STREAMS = 8
ROUNDS = 100
QOS_LEVELS = ("0", "1", "2")
KINDS = ("gap", "duplicate", "reorder")
REQUIRED_FEATURES = ("proposal-recovery", "session-metadata", "shared-subscription-reroute")
# Fixed by ADR-0007 "qualification profile" and docs/specs/performance-and-verification.md §3.9.
QUALIFICATION_PROFILE = {"instanceType": "m7g.large", "brokers": 3, "zones": 3, "brokerCpuMillicores": 1000,
                         "brokerMemoryBytes": 4 * 1024 ** 3, "volumeSize": "20Gi", "volumeType": "gp3"}
# QoS 1/2 gap and QoS 2 duplicate are 0 in every acceptance of this area (capacity spec §3.9, DRN-SCN-018, PRD §8);
# a cutoff file that allows them would weaken the delivery guarantee and is refused.
CUTOFF_FLOORS = (("gap", "1"), ("gap", "2"), ("duplicate", "2"))
COUNT_GROUPS = ("published", "acked", "expected", "delivered", "gap", "duplicate", "reorder", "cleanSessionLoss", "unacked",
                "cleanPublisherInterrupted")
SCALARS = ("afterHorizon", "unexpectedDeliveries", "retainedChecks", "retainedMismatches", "protocolErrors", "ackTimeouts",
           "connectRefusals", "sessionNotPresent", "reconnects311", "reconnects5", "will311Received", "will5Received",
           "connectAttempts")


class MissingCapacityBaseline(RuntimeError):
    """The measured B_* result or the absolute cutoffs that must precede this scenario do not exist (or are unusable)."""


class CapacityMismatch(RuntimeError):
    """The capacity result is internally inconsistent or was measured on another profile than the live fixture."""


class CutoffError(RuntimeError):
    """The cutoff file violates its schema or weakens a delivery guarantee."""


class GeneratorSaturated(RuntimeError):
    """The load generator was saturated, so the measurement is invalid (never a product failure)."""


class RoundFailed(RuntimeError):
    """A rolling-update round failed; the run stops at the first failure."""

    def __init__(self, round_number: int, reason: str):
        super().__init__(f"round {round_number}: {reason}")
        self.round_number, self.reason = round_number, reason


def _load(path: Path, what: str) -> Any:
    if not path.is_file():
        raise MissingCapacityBaseline(f"capacity result lacks {what}: {path} does not exist")
    try:
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except ValueError as error:
        raise MissingCapacityBaseline(f"{what} {path} is not JSON: {error}") from error


def _digest_of_image_id(image_id: str | None) -> str | None:
    if not image_id or "@sha256:" not in image_id:
        return None
    return "sha256:" + image_id.rsplit("@sha256:", 1)[1]


def load_capacity_baseline(directory: Path | None) -> dict[str, Any]:
    """Validate a real accepted EKS capacity result (tests/benchmark/capacity/eks/controller.py output).

    ``directory`` holds ``generator-output/capacity-summary.json``, ``generator-output/manifest.json``,
    ``controller/controller-manifest.json`` and ``controller/broker-pod-resources.json``, as the controller collects
    them. The result must be an adopted EKS step with recorded ``B_*`` values (``bounds`` not null); a run without an
    adopted step, an aborted run or a local-smoke run is not a baseline.
    """
    if directory is None:
        raise MissingCapacityBaseline("no --capacity-result: DRN-SCN-019 runs at the measured B_conn/B_pub/B_sub and none was given")
    root = Path(directory)
    summary = _load(root / "generator-output" / "capacity-summary.json", "capacity-summary.json")
    manifest = _load(root / "generator-output" / "manifest.json", "generator manifest.json")
    controller = _load(root / "controller" / "controller-manifest.json", "controller-manifest.json")
    resources = _load(root / "controller" / "broker-pod-resources.json", "broker-pod-resources.json")
    if summary.get("schemaVersion") != 1 or summary.get("purpose") != "eks-capacity" \
            or not str(summary.get("profileId", "")).startswith("capacity-eks"):
        raise MissingCapacityBaseline(f"capacity-summary.json is not an EKS capacity result: purpose={summary.get('purpose')!r} "
                                      f"profileId={summary.get('profileId')!r}")
    if summary.get("aborted"):
        raise MissingCapacityBaseline(f"the capacity run aborted: {summary['aborted']}")
    adopted, bounds = summary.get("adopted"), summary.get("bounds")
    if not adopted or not bounds:
        raise MissingCapacityBaseline("the capacity run adopted no step, so B_conn/B_pub/B_sub were never recorded: "
                                      f"{summary.get('boundsNullReason') or 'bounds is null'}")
    if summary.get("releaseGating") is not False:
        raise CapacityMismatch("capacity-summary.json releaseGating is not false; the runner never emits any other value")
    profile = manifest.get("profile") or {}
    if manifest.get("profileSha256") != summary.get("profileSha256") or manifest.get("purpose") != "eks-capacity" or not profile:
        raise CapacityMismatch("generator manifest.json and capacity-summary.json describe different profiles")
    problems = []
    cells, rate = adopted.get("cells"), adopted.get("ratePerStreamPerSecond")
    if not isinstance(cells, int) or not isinstance(rate, int) or cells < 1 or rate < 1:
        raise CapacityMismatch(f"adopted step has no cells/rate: {adopted}")
    conn, pub, sub = bounds.get("B_conn", {}), bounds.get("B_pub", {}), bounds.get("B_sub", {})
    if conn.get("connections") != CELL_CLIENTS * cells:
        problems.append(f"B_conn.connections {conn.get('connections')} != 53 x {cells} cells")
    if conn.get("tlsClients", 0) + conn.get("tcpClients", 0) != conn.get("connections"):
        problems.append("B_conn TLS + TCP clients do not add up to the connections")
    if pub.get("offeredPublishPerSecond") != CELL_STREAMS * cells * rate or adopted.get("offeredPublishPerSecond") != pub.get("offeredPublishPerSecond"):
        problems.append(f"B_pub.offeredPublishPerSecond {pub.get('offeredPublishPerSecond')} != 8 x {cells} x {rate}")
    if not isinstance(sub.get("measureN1DeliveriesPerSecondMedian"), (int, float)) or sub["measureN1DeliveriesPerSecondMedian"] <= 0:
        problems.append("B_sub has no positive measureN1 delivery rate")
    confirm = profile.get("confirmRuns")
    if not isinstance(confirm, int) or confirm < 1 or sub.get("confirmRuns") != confirm or len(adopted.get("confirmRuns", [])) != confirm:
        problems.append(f"the adopted step was not confirmed {confirm} times (B_sub.confirmRuns={sub.get('confirmRuns')})")
    passed = [r for r in summary.get("confirm", []) if r.get("step") == adopted.get("step") and r.get("verdict") == "pass"]
    if len(passed) != confirm:
        problems.append(f"{len(passed)} passing confirm records for the adopted step, expected {confirm}")
    active = list(summary.get("activeClusterFeatures") or [])
    missing = [f for f in REQUIRED_FEATURES if f not in active]
    if missing:
        problems.append(f"capacity run did not activate {missing}")
    brokers = [p for p in controller.get("pods", []) if re.fullmatch(r"narumq-[0-9]+", str(p.get("name", "")))]
    digests = {_digest_of_image_id(next((c.get("imageID") for c in p.get("containers", []) if c.get("name") == "narumq"), None)) for p in brokers}
    if len(brokers) != QUALIFICATION_PROFILE["brokers"] or len(digests) != 1 or None in digests:
        problems.append(f"controller manifest does not show three broker Pods on one digest: {sorted(map(str, digests))}")
    instance_types = sorted({p.get("instanceType") for p in brokers})
    if instance_types != [QUALIFICATION_PROFILE["instanceType"]]:
        problems.append(f"capacity brokers ran on {instance_types}, not {QUALIFICATION_PROFILE['instanceType']}")
    if len({p.get("zone") for p in brokers}) != QUALIFICATION_PROFILE["zones"]:
        problems.append("capacity brokers did not run in three distinct zones")
    for pod in resources.get("brokers", []):
        broker = next((c for c in pod.get("containers", []) if c.get("name") == "narumq"), {})
        for section in ("requests", "limits"):
            values = broker.get(section, {})
            if values.get("cpuMillicores") != profile.get("brokerCPUs", 0) * 1000 or values.get("memoryBytes") != profile.get("brokerMemoryBytes"):
                problems.append(f"broker container {section} of {pod.get('name')} differ from the capacity profile")
    if len(resources.get("brokers", [])) != QUALIFICATION_PROFILE["brokers"]:
        problems.append("broker-pod-resources.json does not list three brokers")
    if problems:
        raise CapacityMismatch("capacity result is internally inconsistent: " + "; ".join(problems))
    return {"profileId": summary["profileId"], "profileSha256": summary["profileSha256"], "cells": cells,
            "ratePerStreamPerSecond": rate, "bounds": bounds, "profile": profile, "brokerDigest": next(iter(digests)),
            "instanceTypes": instance_types, "activeFeatures": active, "runnerImage": controller.get("capacityRunnerImage"),
            "capacityRunId": controller.get("runId"), "step": adopted["step"]}


def check_live_fixture(baseline: dict[str, Any], live: dict[str, Any], image_a_digest: str) -> list[str]:
    """The live fixture must be the profile the capacity result was measured on (ADR-0007 descriptor, same image).

    ``live`` holds ``brokerWorkers`` ([{pod, instanceType, zone}]), ``brokerDigests`` (digest per Pod), ``volumes``
    ([{pvc, size, volumeType}]), ``brokerResources`` ({requests, limits: {cpuMillicores, memoryBytes}}) and
    ``activeFeatures``.
    """
    problems = []
    workers = live.get("brokerWorkers", [])
    if len(workers) != QUALIFICATION_PROFILE["brokers"] or {w["instanceType"] for w in workers} != set(baseline["instanceTypes"]):
        problems.append(f"live broker workers {sorted({w.get('instanceType') for w in workers})} differ from the capacity run {baseline['instanceTypes']}")
    if len({w.get("zone") for w in workers}) != QUALIFICATION_PROFILE["zones"]:
        problems.append("live brokers do not run in three distinct zones")
    if set(live.get("brokerDigests", {}).values()) != {baseline["brokerDigest"]} or len(live.get("brokerDigests", {})) != 3:
        problems.append(f"live broker Pods do not all run the capacity-measured digest {baseline['brokerDigest']}")
    if image_a_digest != baseline["brokerDigest"]:
        problems.append(f"--image-a digest {image_a_digest} is not the capacity-measured broker digest {baseline['brokerDigest']}")
    for volume in live.get("volumes", []):
        if volume.get("size") != QUALIFICATION_PROFILE["volumeSize"] or volume.get("volumeType") != QUALIFICATION_PROFILE["volumeType"]:
            problems.append(f"volume {volume.get('pvc')} is {volume.get('size')} {volume.get('volumeType')}, not 20Gi gp3")
    if len(live.get("volumes", [])) != 3:
        problems.append("live fixture does not have three broker volumes")
    resources = live.get("brokerResources", {})
    for section in ("requests", "limits"):
        values = resources.get(section, {})
        if values.get("cpuMillicores") != QUALIFICATION_PROFILE["brokerCpuMillicores"] or values.get("memoryBytes") != QUALIFICATION_PROFILE["brokerMemoryBytes"]:
            problems.append(f"live broker container {section} are {values}, not 1 CPU / 4 GiB")
    missing = [f for f in baseline["activeFeatures"] if f not in live.get("activeFeatures", [])]
    if missing:
        problems.append(f"live cluster does not have the capacity run's active features {missing}")
    return problems


# Cutoffs ---------------------------------------------------------------------------------------------
def load_cutoffs(path: Path | None, baseline: dict[str, Any]) -> dict[str, Any]:
    """Absolute gap / duplicate / reorder cutoffs decided after B_* (a required explicit input)."""
    if path is None:
        raise MissingCapacityBaseline("no --cutoffs: the spec leaves the absolute gap/duplicate/reorder cutoffs to be decided "
                                      "after the real B_conn/B_pub/B_sub are fixed; supply the decided values")
    if not Path(path).is_file():
        raise MissingCapacityBaseline(f"cutoffs file {path} does not exist")
    try:
        cutoffs = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    except ValueError as error:
        raise CutoffError(f"cutoffs file {path} is not JSON: {error}") from error
    validate_cutoffs(cutoffs, baseline)
    return cutoffs


def validate_cutoffs(cutoffs: Any, baseline: dict[str, Any]) -> None:
    if not isinstance(cutoffs, dict) or cutoffs.get("schemaVersion") != 1:
        raise CutoffError("cutoffs must be an object with schemaVersion 1")
    if cutoffs.get("capacityProfileSha256") != baseline["profileSha256"]:
        raise CapacityMismatch("cutoffs were decided for another capacity profile "
                               f"({cutoffs.get('capacityProfileSha256')} != {baseline['profileSha256']})")
    for field in ("decidedAt", "decidedBy"):
        if not cutoffs.get(field) or not isinstance(cutoffs[field], str):
            raise CutoffError(f"cutoffs.{field} is required")
    for kind in KINDS:
        for qos in QOS_LEVELS:
            entry = (cutoffs.get(kind) or {}).get(qos)
            if not isinstance(entry, dict) or set(entry) != {"maxCount", "maxRatio"}:
                raise CutoffError(f"cutoffs.{kind}.{qos} must be {{maxCount, maxRatio}}")
            if not isinstance(entry["maxCount"], int) or isinstance(entry["maxCount"], bool) or entry["maxCount"] < 0:
                raise CutoffError(f"cutoffs.{kind}.{qos}.maxCount must be a non-negative integer")
            if not isinstance(entry["maxRatio"], (int, float)) or isinstance(entry["maxRatio"], bool) or not 0 <= entry["maxRatio"] <= 1:
                raise CutoffError(f"cutoffs.{kind}.{qos}.maxRatio must be within [0, 1]")
    retained = cutoffs.get("retainedMismatch")
    if not isinstance(retained, dict) or set(retained) != {"maxCount"} or not isinstance(retained["maxCount"], int) or retained["maxCount"] < 0:
        raise CutoffError("cutoffs.retainedMismatch must be {maxCount}")
    for kind, qos in CUTOFF_FLOORS:
        if cutoffs[kind][qos]["maxCount"] != 0 or cutoffs[kind][qos]["maxRatio"] != 0:
            raise CutoffError(f"cutoffs.{kind}.{qos} must be 0: QoS {qos} {kind} is never allowed by the delivery guarantee")


# Generator measurement ---------------------------------------------------------------------------
def merge_generator_summaries(shards: list[dict[str, Any]]) -> dict[str, Any]:
    """Sum the final summaries of every generator shard; any missing or unfinished shard raises ``MissingCapacityBaseline``."""
    if not shards:
        raise MissingCapacityBaseline("no generator summary was collected")
    expected = shards[0].get("shards")
    tokens = {s.get("runToken") for s in shards}
    indexes = sorted(s.get("shard") for s in shards)
    if len(tokens) != 1 or indexes != list(range(expected or 0)):
        raise MissingCapacityBaseline(f"generator shards {indexes} of {expected} (tokens {sorted(map(str, tokens))}) are not one complete run")
    if not all(s.get("finalized") is True for s in shards):
        raise MissingCapacityBaseline("a generator shard did not finalize its ledger")
    merged: dict[str, Any] = {"runToken": next(iter(tokens)), "shards": expected,
                              "counts": {g: {q: 0 for q in QOS_LEVELS} for g in COUNT_GROUPS}, "scalars": {k: 0 for k in SCALARS},
                              "clientsExpected": 0, "clientsConnected": 0,
                              "saturation": {"cpuPercentMax": 0.0, "scheduleLagMaxMillis": 0.0, "saturated": False}}
    for shard in shards:
        for group in COUNT_GROUPS:
            for qos in QOS_LEVELS:
                merged["counts"][group][qos] += int(shard["counts"][group][qos])
        for name in SCALARS:
            merged["scalars"][name] += int(shard["scalars"][name])
        merged["clientsExpected"] += int(shard["clientsExpected"])
        merged["clientsConnected"] += int(shard["clientsConnected"])
        saturation = shard["saturation"]
        merged["saturation"]["cpuPercentMax"] = max(merged["saturation"]["cpuPercentMax"], float(saturation["cpuPercentMax"]))
        merged["saturation"]["scheduleLagMaxMillis"] = max(merged["saturation"]["scheduleLagMaxMillis"], float(saturation["scheduleLagMaxMillis"]))
        merged["saturation"]["saturated"] = merged["saturation"]["saturated"] or bool(saturation["saturated"])
    wills = {"mqtt311Received": merged["scalars"]["will311Received"], "mqtt5Received": merged["scalars"]["will5Received"]}
    merged["wills"] = wills
    return merged


def judge_measurement(merged: dict[str, Any], cutoffs: dict[str, Any], baseline: dict[str, Any]) -> dict[str, Any]:
    """Gap, duplicate and reorder are judged separately per QoS against the absolute cutoffs."""
    problems: list[str] = []
    table: dict[str, dict[str, Any]] = {}
    counts, scalars = merged["counts"], merged["scalars"]
    for qos in QOS_LEVELS:
        if counts["published"][qos] <= 0 or counts["delivered"][qos] <= 0:
            problems.append(f"INVALID: no QoS {qos} traffic was published and delivered (published {counts['published'][qos]}, "
                            f"delivered {counts['delivered'][qos]})")
    for kind in KINDS:
        for qos in QOS_LEVELS:
            count = counts[kind][qos]
            base = counts["expected"][qos] if kind == "gap" else counts["delivered"][qos]
            ratio = (count / base) if base > 0 else (0.0 if count == 0 else math.inf)
            limit = cutoffs[kind][qos]
            ok = count <= limit["maxCount"] and ratio <= limit["maxRatio"]
            table[f"{kind}.qos{qos}"] = {"count": count, "base": base, "ratio": ratio, "maxCount": limit["maxCount"],
                                         "maxRatio": limit["maxRatio"], "ok": ok}
            if not ok:
                problems.append(f"{kind} QoS {qos}: {count} of {base} (ratio {ratio:.3g}) exceeds maxCount {limit['maxCount']} / maxRatio {limit['maxRatio']}")
    if scalars["retainedChecks"] <= 0:
        problems.append("INVALID: no retained-set check ran")
    if scalars["retainedMismatches"] > cutoffs["retainedMismatch"]["maxCount"]:
        problems.append(f"retained set mismatched {scalars['retainedMismatches']} times (max {cutoffs['retainedMismatch']['maxCount']})")
    if scalars["unexpectedDeliveries"]:
        problems.append(f"{scalars['unexpectedDeliveries']} deliveries reached a subscriber the oracle does not expect")
    if scalars["protocolErrors"]:
        problems.append(f"{scalars['protocolErrors']} MQTT protocol errors")
    if merged["wills"]["mqtt5Received"]:
        problems.append(f"{merged['wills']['mqtt5Received']} MQTT 5 Wills (Will Delay longer than the reconnect time) were published")
    if merged["saturation"]["saturated"]:
        problems.append(f"INVALID: generator saturated (cpu {merged['saturation']['cpuPercentMax']:.0f}%, "
                        f"schedule lag {merged['saturation']['scheduleLagMaxMillis']:.0f} ms)")
    expected_clients = CELL_CLIENTS * baseline["cells"]
    if merged["clientsExpected"] != expected_clients:
        problems.append(f"INVALID: generator ran {merged['clientsExpected']} clients, B_conn is {expected_clients}")
    if merged["clientsConnected"] != expected_clients:
        problems.append(f"{merged['clientsConnected']} of {expected_clients} clients were connected at the end")
    return {"passed": not problems, "problems": problems, "table": table}


def judge_will_outcomes(delta: dict[str, int]) -> dict[str, Any]:
    """``unexpected_supported_client`` must not grow; ``unplanned_failure`` is reported next to it."""
    problems = []
    if delta.get("unexpected_supported_client", None) != 0:
        problems.append(f"unexpected_supported_client delta is {delta.get('unexpected_supported_client')}, expected 0")
    return {"passed": not problems, "problems": problems, "delta": delta, "unplannedFailure": delta.get("unplanned_failure")}


# Rounds -----------------------------------------------------------------------------------------------
def chain_link(previous: dict[str, Any] | None) -> str:
    """Hash that the next round record stores as ``prevSha256``."""
    return hashlib.sha256(canonical_json(previous)).hexdigest() if previous is not None else "0" * 64


def judge_rounds(records: list[dict[str, Any]], run_identity: dict[str, Any], expected: int = ROUNDS) -> dict[str, Any]:
    """All rounds exist, are consecutive, passed, belong to this run and fixture, and chain-verify."""
    problems = []
    if len(records) != expected:
        problems.append(f"{len(records)} round records, expected {expected} consecutive rolling updates")
    previous = None
    for index, record in enumerate(records, start=1):
        if record.get("round") != index:
            problems.append(f"record {index} is round {record.get('round')}")
        if record.get("runIdentity") != run_identity:
            problems.append(f"round {index} belongs to another run or fixture")
        if record.get("prevSha256") != chain_link(previous):
            problems.append(f"round {index} does not chain to round {index - 1}")
        if record.get("passed") is not True:
            problems.append(f"round {index} did not pass: {record.get('problems')}")
        previous = record
    return {"passed": not problems, "problems": problems, "rounds": len(records)}


def judge_round(record: dict[str, Any]) -> list[str]:
    """Round gate: every Pod that was not on the target replaced and Ready on it, quorum available, clients back.

    ``record`` carries ``target.digest`` (the digest every Pod must run), ``before``/``after``
    (``ops.pod_summary`` plus the digest of each Pod's imageID), the final rollout ``status``, ``quorum`` samples and the generator client counts.
    A resumed round (the controller finishing an interrupted rollout) may find Pods already on the target.
    """
    problems = []
    status, target = record["status"], record["target"]["digest"]
    if status.get("phase") != "Completed" or sorted(status.get("completedOrdinals", [])) != [0, 1, 2]:
        problems.append(f"rollout ended {status.get('phase')} ({status.get('failureCode')}) with completed {status.get('completedOrdinals')}")
    if len(record["after"]) != 3:
        problems.append(f"{len(record['after'])} broker Pods after the round, expected 3")
    for name, before in sorted(record["before"].items()):
        after = record["after"].get(name)
        if after is None:
            problems.append(f"{name} disappeared")
        elif before["digest"] != target and after["uid"] == before["uid"]:
            problems.append(f"{name} was not replaced")
        elif not after["ready"] or after["deleting"] or after["digest"] != target:
            problems.append(f"{name} is not Ready on the target image")
    quorum = record["quorum"]
    if len(quorum) < 3 or any(s.get("quorumAvailable") is not True for s in quorum):
        problems.append(f"quorum was not reported available in every sample ({len(quorum)} samples)")
    if record.get("clientsConnected") != record.get("clientsExpected") or not record.get("clientsExpected"):
        problems.append(f"{record.get('clientsConnected')} of {record.get('clientsExpected')} clients reconnected after the round")
    return problems


def judge_round_approvals(approvals: list[dict[str, Any]], target_image: str) -> list[str]:
    """One round's audit approvals: ordinals 0, 1 and 2 once each, every barrier condition true, target image recorded."""
    problems = []
    ordinals = sorted((a.get("details") or {}).get("ordinal", -1) for a in approvals)
    if ordinals != [0, 1, 2]:
        problems.append(f"approved ordinals {ordinals}, expected exactly 0, 1 and 2")
    for event in approvals:
        details = event.get("details") or {}
        failed = sorted(name for name, ok in approval_conditions(event).items() if not ok)
        if failed:
            problems.append(f"ordinal {details.get('ordinal')} approval fails {failed}")
        if details.get("targetImage") != target_image:
            problems.append(f"ordinal {details.get('ordinal')} approval targets {details.get('targetImage')}, not {target_image}")
    return problems


def judge_image_pair(image_a: dict[str, Any], image_b: dict[str, Any], baseline: dict[str, Any]) -> list[str]:
    """Image A is the capacity-measured build; image B is a different build inside the ADR-0018 window."""
    problems = []
    if image_a["digest"] != baseline["brokerDigest"]:
        problems.append(f"image A {image_a['digest']} is not the capacity-measured broker digest {baseline['brokerDigest']}")
    if image_a["digest"] == image_b["digest"]:
        problems.append("image A and image B are the same digest: the controller replaces no Pod, so no rolling update would happen")
    if image_a["repository"] != image_b["repository"]:
        problems.append("image A and image B are in different repositories")
    try:
        if not versions_compatible(parse_version(image_a["version"]), parse_version(image_b["version"])):
            problems.append(f"releases {image_a['version']} and {image_b['version']} are outside the ADR-0018 compatibility window")
    except ReleaseError as error:
        problems.append(str(error))
    return problems


def parse_cpu_millicores(value: Any) -> int:
    text = str(value)
    return int(text[:-1]) if text.endswith("m") else int(float(text) * 1000)


def parse_memory_bytes(value: Any) -> int:
    text = str(value)
    units = {"Ki": 1024, "Mi": 1024 ** 2, "Gi": 1024 ** 3, "Ti": 1024 ** 4, "k": 1000, "M": 1000 ** 2, "G": 1000 ** 3}
    for suffix, factor in units.items():
        if text.endswith(suffix):
            return int(float(text[:-len(suffix)]) * factor)
    return int(text)


def check_resume_identity(state: dict[str, Any], identity: dict[str, Any]) -> list[str]:
    """A saved run may only be continued by the same run id, fixture, capacity result, images and generator geometry."""
    if state.get("schemaVersion") != 1:
        return [f"state schemaVersion {state.get('schemaVersion')} is not 1"]
    return [f"state {key} {state.get('runIdentity', {}).get(key)!r} differs from {value!r}"
            for key, value in identity.items() if state.get("runIdentity", {}).get(key) != value]
