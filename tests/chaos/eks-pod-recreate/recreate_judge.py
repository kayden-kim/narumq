"""Offline judgments for the DRN-SCN-033 Pod-recreation harness (same PVC and EBS volume, quorum, Ready).

Each function takes observed data and returns ``[(check name, ok, observed)]``; the live runner feeds the rows to
``Checks.check`` and the unit tests need no cluster.
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

VOLUME_ID = re.compile(r"^vol-[0-9a-f]{8,17}$")
EBS_CSI_DRIVER = "ebs.csi.aws.com"
EXPECTED_MEMBERS = 3
# Sampling is requested every 2 s; one slow Management API call (timeout 8 s) must not hide an outage window.
MAX_SAMPLE_GAP_SECONDS = 6.0
MIN_SAMPLES = 3


def choose_target(pods: dict[str, dict[str, Any]], slots: dict[int, str], leader_node_id: str) -> tuple[str, str, str]:
    """(target pod, target node id, survivor pod): a follower is the target and a different Pod is the observer."""
    candidates = [(f"narumq-{ordinal}", node) for ordinal, node in sorted(slots.items()) if node != str(leader_node_id) and f"narumq-{ordinal}" in pods]
    if not candidates:
        raise PrerequisiteMissing(f"NO_FOLLOWER: leader {leader_node_id}, slots {slots}, pods {sorted(pods)}")
    target, node = candidates[0]
    survivor = next((name for name in sorted(pods) if name != target), None)
    if survivor is None:
        raise PrerequisiteMissing("NO_SURVIVOR_POD")
    return target, node, survivor


def preflight_problems(pods: dict[str, dict[str, Any]], cluster: dict[str, Any]) -> list[str]:
    problems = []
    if len(pods) != EXPECTED_MEMBERS:
        problems.append(f"expected {EXPECTED_MEMBERS} broker Pods, found {sorted(pods)}")
    if not all(pod.get("ready") and not pod.get("deleting") for pod in pods.values()):
        problems.append(f"not every broker Pod is Ready: {sorted((n, p.get('ready')) for n, p in pods.items())}")
    if cluster.get("quorumAvailable") is not True:
        problems.append("quorumAvailable is not true")
    if (cluster.get("nodesByState") or {}).get("ready") != EXPECTED_MEMBERS:
        problems.append(f"nodesByState.ready is not {EXPECTED_MEMBERS}: {cluster.get('nodesByState')}")
    if cluster.get("memberCount") != EXPECTED_MEMBERS:
        problems.append(f"memberCount is not {EXPECTED_MEMBERS}: {cluster.get('memberCount')}")
    return problems


def volume_id_of(pv: dict[str, Any]) -> str:
    csi = pv.get("spec", {}).get("csi") or {}
    handle = csi.get("volumeHandle", "")
    if csi.get("driver") != EBS_CSI_DRIVER or not VOLUME_ID.match(handle):
        raise PrerequisiteMissing(f"PV {pv.get('metadata', {}).get('name')} is not an EBS CSI volume: driver={csi.get('driver')!r} handle={handle!r}")
    return handle


def storage_snapshot(pod: dict[str, Any], pvc: dict[str, Any], pv: dict[str, Any], volume: dict[str, Any], instance_id: str) -> dict[str, Any]:
    """Everything that identifies the storage behind one broker Pod, from the raw Kubernetes and EC2 objects."""
    claims = [v["persistentVolumeClaim"]["claimName"] for v in pod["spec"].get("volumes", []) if "persistentVolumeClaim" in v]
    return {
        "pod": pod["metadata"]["name"], "podUID": pod["metadata"]["uid"], "node": pod["spec"].get("nodeName"), "instanceId": instance_id,
        "podClaims": claims, "pvc": pvc["metadata"]["name"], "pvcUID": pvc["metadata"]["uid"], "pvcPhase": pvc.get("status", {}).get("phase"),
        "pv": pv["metadata"]["name"], "pvName": pvc["spec"].get("volumeName"), "volumeHandle": volume_id_of(pv),
        "volumeId": volume.get("VolumeId"), "volumeState": volume.get("State"), "zone": volume.get("AvailabilityZone"),
        "volumeType": volume.get("VolumeType"), "sizeGiB": volume.get("Size"), "encrypted": volume.get("Encrypted"),
        "attachments": [{"InstanceId": a.get("InstanceId"), "State": a.get("State")} for a in volume.get("Attachments", [])],
    }


def judge_same_storage(before: dict[str, Any], after: dict[str, Any]) -> list[Check]:
    """The recreated Pod is a new object on the same PVC, PV and EBS volume, attached to the instance it now runs on."""
    attachments = after.get("attachments", [])
    return [
        ("recreated Pod has a new UID", before["podUID"] != after["podUID"], {"before": before["podUID"], "after": after["podUID"]}),
        ("recreated Pod mounts the same claim name", before["pvc"] == after["pvc"] and after["podClaims"] == before["podClaims"] and len(after["podClaims"]) == 1,
         {"before": before["podClaims"], "after": after["podClaims"]}),
        ("same PVC object (UID unchanged, still Bound)", before["pvcUID"] == after["pvcUID"] and after["pvcPhase"] == "Bound",
         {"before": before["pvcUID"], "after": after["pvcUID"], "phase": after["pvcPhase"]}),
        ("same PV", before["pv"] == after["pv"] and after["pv"] == after["pvName"], {"before": before["pv"], "after": after["pv"], "boundTo": after["pvName"]}),
        ("same CSI volumeHandle", before["volumeHandle"] == after["volumeHandle"], {"before": before["volumeHandle"], "after": after["volumeHandle"]}),
        ("EC2 reports the volume the PV names", after["volumeId"] == after["volumeHandle"], {"ec2": after["volumeId"], "handle": after["volumeHandle"]}),
        ("EBS volume is in-use with exactly one attachment in state attached",
         after["volumeState"] == "in-use" and len(attachments) == 1 and attachments[0]["State"] == "attached", {"state": after["volumeState"], "attachments": attachments}),
        ("EBS volume is attached to the instance of the recreated Pod's node",
         len(attachments) == 1 and attachments[0]["InstanceId"] == after["instanceId"], {"attachment": attachments, "podInstance": after["instanceId"], "node": after["node"]}),
        ("EBS volume identity is unchanged (zone, type, size, encryption)",
         all(before[key] == after[key] for key in ("zone", "volumeType", "sizeGiB", "encrypted")),
         {key: [before[key], after[key]] for key in ("zone", "volumeType", "sizeGiB", "encrypted")}),
    ]


def judge_quorum_samples(samples: list[dict[str, Any]], deleted_at: float, ready_at: float, max_gap: float = MAX_SAMPLE_GAP_SECONDS) -> list[Check]:
    """Survivor Management API samples (``at`` epoch seconds) from before the DELETE until the replacement is Ready."""
    failed = [s for s in samples if s.get("error") or s.get("quorumAvailable") is not True]
    gaps = [round(b["at"] - a["at"], 3) for a, b in zip(samples, samples[1:])]
    leaders = [s.get("leaderNodeId") for s in samples]
    min_ready = min(((s.get("nodesByState") or {}).get("ready", EXPECTED_MEMBERS) for s in samples if not s.get("error")), default=None)
    return [
        (f"at least {MIN_SAMPLES} survivor samples were taken", len(samples) >= MIN_SAMPLES, len(samples)),
        ("no survivor sample failed to read the cluster summary", not any(s.get("error") for s in samples), [s["error"] for s in samples if s.get("error")][:3]),
        ("quorumAvailable is true in every survivor sample", bool(samples) and not failed, {"failed": len(failed), "total": len(samples), "first": failed[:2]}),
        (f"memberCount stays {EXPECTED_MEMBERS} in every sample", bool(samples) and all(s.get("memberCount") == EXPECTED_MEMBERS for s in samples),
         sorted({str(s.get("memberCount")) for s in samples})),
        ("a leader is reported in every sample", bool(samples) and all(leader not in (None, "") for leader in leaders), sorted({str(x) for x in leaders})),
        ("samples cover the DELETE through the recovery", bool(samples) and samples[0]["at"] <= deleted_at and samples[-1]["at"] >= ready_at,
         {"first": samples[0]["at"] if samples else None, "deleted": deleted_at, "ready": ready_at, "last": samples[-1]["at"] if samples else None}),
        (f"consecutive samples are at most {max_gap:g} s apart", bool(gaps) and max(gaps) <= max_gap, {"maxGapSeconds": max(gaps) if gaps else None}),
        ("the sampler saw the outage window (fewer than 3 READY nodes at some point)", min_ready is not None and min_ready < EXPECTED_MEMBERS, {"minReady": min_ready}),
    ]


def judge_recovery(before_pods: dict[str, dict[str, Any]], after_pods: dict[str, dict[str, Any]], target: str, node_row: dict[str, Any] | None,
                   cluster: dict[str, Any]) -> list[Check]:
    survivors = {name: pod["uid"] for name, pod in before_pods.items() if name != target}
    return [
        ("all three broker Pods are Ready and none is deleting", len(after_pods) == EXPECTED_MEMBERS and all(p["ready"] and not p["deleting"] for p in after_pods.values()),
         {n: (p["ready"], p["deleting"]) for n, p in after_pods.items()}),
        ("the recreated target Pod is Ready", bool(after_pods.get(target, {}).get("ready")), after_pods.get(target, {}).get("ready")),
        ("survivor Pods kept their UIDs (only the target was replaced)", all(after_pods.get(name, {}).get("uid") == uid for name, uid in survivors.items()),
         {name: after_pods.get(name, {}).get("uid") == uid for name, uid in survivors.items()}),
        ("/api/v1/nodes reports the target node READY and queryable",
         bool(node_row) and node_row.get("ready") is True and str(node_row.get("state", "")).upper() == "READY" and node_row.get("queryStatus") in (None, "OK"),
         {k: (node_row or {}).get(k) for k in ("nodeId", "state", "ready", "queryStatus")}),
        ("cluster summary shows quorum and three READY nodes", cluster.get("quorumAvailable") is True and (cluster.get("nodesByState") or {}).get("ready") == EXPECTED_MEMBERS,
         {"quorumAvailable": cluster.get("quorumAvailable"), "nodesByState": cluster.get("nodesByState")}),
    ]
