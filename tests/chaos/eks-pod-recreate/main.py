"""EKS acceptance harness for DRN-SCN-033 (DEP-17, DEP-21, NFR-07): delete a healthy broker Pod and check the recreation.

Status: prepared, not yet executed live. It needs a disposable EKS fixture that does not exist at the time of writing.

Row (docs/specs/drain-and-rolling-update.md section 10): a healthy Pod in an isolated EKS fixture is deleted and
recreated; the same PVC and EBS volume must be attached again, quorum must hold, and the Pod must be Ready again.

What the harness does
  1. Preflight (a miss raises ``PrerequisiteMissing``; the scenario is then recorded as FAIL): three Ready broker
     Pods, ``quorumAvailable``, three READY nodes, memberCount 3, an EBS CSI PV behind the target.
  2. Picks a follower (Management API leader id mapped through the ``slot-N`` ConfigMap entries) as the target and a
     different Pod as the survivor observer. Records Pod UID and node, PVC name and UID, PV, CSI volumeHandle, and the
     ``aws ec2 describe-volumes`` state, zone and attachments.
  3. Starts a sampler against the survivor's ``/api/v1/cluster`` (port-forward pinned to that Pod, every 2 s) and
     deletes the target Pod with a UID precondition. The Pod's own grace period applies, so the live preStop hook
     drains it: this is the normal planned path.
  4. Waits for a new Pod UID, Ready, three Ready brokers, then re-reads the storage, requires the EBS volume to be
     attached to the instance of the Pod's new node, and requires the target node READY in ``/api/v1/nodes``.
  5. Judges (recreate_judge.py) and raises ``AcceptanceFailure`` after saving ``pod-recreate-result.json`` if any
     check failed: same PVC (name and UID), same PV and volumeHandle, volume in-use with one attachment on the right
     instance, quorumAvailable true in every survivor sample, survivors untouched, all brokers Ready, elapsed time.

Prerequisites: a disposable EKS fixture that passes ``eks_fixture.build_fixture`` (kubeconfig context == run id, EKS
tags, namespace label), AWS credentials for that account (``AWS_PROFILE``, ``AWS_REGION``) allowed to call
``ec2:DescribeVolumes``, and the three-broker ``narumq`` StatefulSet with gp3 EBS PVCs.

Mutations: one graceful Pod DELETE of one follower. Nothing else is changed; there is nothing to clean up because the
StatefulSet recreates the Pod. The port-forwards are closed in ``finally``.

Run (PowerShell):
  python tests/chaos/eks-pod-recreate/main.py --run-id RUN --cluster CLUSTER --private DIR --evidence DIR
"""

from __future__ import annotations

import argparse
import sys
import threading
import time
from pathlib import Path
from typing import Any

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
for _directory in (HERE, HERE.parent, HERE.parents[1]):  # this dir, tests/chaos, tests (scenario_results)
    if str(_directory) not in sys.path:
        sys.path.append(str(_directory))

import eks_broker  # noqa: E402
import recreate_judge as judge  # noqa: E402
from eks_broker import AcceptanceFailure, Management, PrerequisiteMissing  # noqa: E402
from eks_fixture import add_fixture_arguments, build_fixture, rollout_faults  # noqa: E402
from scenario_results import scenario_result  # noqa: E402

SAMPLE_INTERVAL_SECONDS = 2.0
READY_TIMEOUT_SECONDS = 900.0  # the live preStop hook may wait up to 600 s before the Pod is deleted
ATTACH_TIMEOUT_SECONDS = 180.0


def feed(checks: Any, rows: list[judge.Check]) -> None:
    for name, ok, observed in rows:
        checks.check(name, ok, observed)


class QuorumSampler(threading.Thread):
    """Reads the survivor's cluster summary every ``interval`` seconds until stopped."""

    def __init__(self, management: Management, interval: float):
        super().__init__(daemon=True)
        self.management, self.interval, self.rows, self.stop = management, interval, [], threading.Event()

    def run(self) -> None:
        while not self.stop.is_set():
            started = time.monotonic()
            sample: dict[str, Any] = {"at": time.time()}
            try:
                cluster = self.management.data("/api/v1/cluster")
                sample.update({key: cluster.get(key) for key in ("quorumAvailable", "leaderNodeId", "memberCount", "nodesByState")})
            except Exception as error:  # a failed read is a failed sample, never a skipped one
                sample["error"] = f"{type(error).__name__}: {error}"
            sample["durationSeconds"] = round(time.monotonic() - started, 3)
            self.rows.append(sample)
            self.stop.wait(max(0.0, self.interval - (time.monotonic() - started)))


def snapshot(fixture: Any, pod_name: str) -> dict[str, Any]:
    pod = fixture.kjson("get", "pod", pod_name)
    claims = [v["persistentVolumeClaim"]["claimName"] for v in pod["spec"].get("volumes", []) if "persistentVolumeClaim" in v]
    if len(claims) != 1:
        raise PrerequisiteMissing(f"POD_CLAIM_UNEXPECTED: {pod_name} mounts {claims}")
    pvc = fixture.kjson("get", "pvc", claims[0])
    if not pvc["spec"].get("volumeName"):
        raise PrerequisiteMissing(f"PVC_NOT_BOUND: {claims[0]}")
    pv = fixture.kjson("get", "pv", pvc["spec"]["volumeName"])
    volume = fixture.aws("ec2", "describe-volumes", "--volume-ids", judge.volume_id_of(pv))["Volumes"][0]
    instance = eks_broker.provider_instance_id(fixture.kjson("get", "node", pod["spec"]["nodeName"]))
    return judge.storage_snapshot(pod, pvc, pv, volume, instance)


def attached_snapshot(fixture: Any, pod_name: str) -> dict[str, Any]:
    """Re-read the storage until EC2 shows the volume attached to the instance the Pod now runs on."""
    def attempt() -> dict[str, Any] | None:
        current = snapshot(fixture, pod_name)
        attachments = current["attachments"]
        return current if len(attachments) == 1 and attachments[0]["State"] == "attached" and attachments[0]["InstanceId"] == current["instanceId"] else None

    try:
        return rollout_faults().wait_for(attempt, ATTACH_TIMEOUT_SECONDS, 5, f"EBS volume of {pod_name} attached to the Pod's instance")
    except TimeoutError:
        return snapshot(fixture, pod_name)  # the judgment then fails with the observed attachment


def old_pod_events(fixture: Any, name: str, uid: str) -> list[dict[str, Any]]:
    items = fixture.kjson("get", "events", "--field-selector", f"involvedObject.name={name}")["items"]
    return sorted(({"reason": i.get("reason"), "type": i.get("type"), "message": i.get("message"),
                    "at": i.get("lastTimestamp") or i.get("eventTime")} for i in items if i.get("involvedObject", {}).get("uid") == uid),
                  key=lambda event: event["at"] or "")


# Scenarios: drain-and-rolling-update.md:DRN-SCN-033
@scenario_result
def scenario_pod_recreate_same_ebs(fixture: Any, ready_timeout: float = READY_TIMEOUT_SECONDS, interval: float = SAMPLE_INTERVAL_SECONDS) -> dict[str, Any]:
    module = rollout_faults()
    checks, started = module.Checks(), time.time()
    token = eks_broker.management_token(fixture)
    slots = eks_broker.slot_map(fixture)
    pods_before = fixture.broker_pods()
    probe = Management(fixture, sorted(pods_before)[0], token)
    try:
        cluster_before = probe.data("/api/v1/cluster")
    finally:
        probe.close()
    problems = judge.preflight_problems(pods_before, cluster_before)
    if problems:
        raise PrerequisiteMissing(f"PRECONDITION_FAILED: {problems}")
    target, node_id, survivor = judge.choose_target(pods_before, slots, str(cluster_before["leaderNodeId"]))
    before = snapshot(fixture, target)
    attachments = before["attachments"]
    checks.check("before: the target's EBS volume is in-use on the Pod's own instance",
                 before["volumeState"] == "in-use" and len(attachments) == 1 and attachments[0]["InstanceId"] == before["instanceId"] and attachments[0]["State"] == "attached",
                 {"state": before["volumeState"], "attachments": attachments, "instance": before["instanceId"]})
    evidence: dict[str, Any] = {"target": target, "targetNodeId": node_id, "survivor": survivor, "leaderNodeId": cluster_before["leaderNodeId"],
                                "before": before, "clusterBefore": cluster_before}
    sampler = QuorumSampler(Management(fixture, survivor, token), interval)
    check_management = Management(fixture, survivor, token)
    try:
        sampler.start()
        module.wait_for(lambda: bool(sampler.rows), 60, 0.5, "first survivor sample")
        deleted_at = time.time()
        delete_response = eks_broker.delete_pod_with_uid(fixture, target, before["podUID"])
        clock = time.monotonic()
        replacement, pod_samples = eks_broker.wait_replaced_and_ready(fixture, target, before["podUID"], ready_timeout)
        ready_at, recovery_seconds = time.time(), round(time.monotonic() - clock, 3)
        after = attached_snapshot(fixture, target)

        def converged() -> dict[str, Any] | None:
            try:
                cluster = check_management.data("/api/v1/cluster")
                rows = {str(n.get("nodeId")): n for n in check_management.data("/api/v1/nodes")}
            except (eks_broker.ManagementError, OSError, ValueError):
                return None
            row = rows.get(node_id)
            ok = cluster.get("quorumAvailable") is True and (cluster.get("nodesByState") or {}).get("ready") == 3 and bool(row) and row.get("ready") is True
            return {"cluster": cluster, "nodeRow": row} if ok else None

        final = module.wait_for(converged, 300, 3, "three READY nodes after the Pod was recreated")
        time.sleep(2 * interval)  # keep sampling past the first converged reading
        sampler.stop.set()
        sampler.join(30)
        pods_after = fixture.broker_pods()
        gone = next((s for s in pod_samples if not any(p["name"] == target and p["uid"] == before["podUID"] for p in s["pods"])), None)
        evidence.update(deletedAt=deleted_at, readyAt=ready_at, recoverySeconds=recovery_seconds, deleteResponseDeletionTimestamp=(delete_response.get("metadata") or {}).get("deletionTimestamp"),
                        oldPodObjectGoneSeconds=None if gone is None else round(gone["at"] - deleted_at, 1), after=after, final=final, podSamples=pod_samples,
                        samples=sampler.rows, oldPodEvents=old_pod_events(fixture, target, before["podUID"]), replacementUID=replacement["metadata"]["uid"])
        feed(checks, judge.judge_same_storage(before, after))
        feed(checks, judge.judge_quorum_samples(sampler.rows, deleted_at, ready_at))
        feed(checks, judge.judge_recovery(pods_before, pods_after, target, final["nodeRow"], final["cluster"]))
        checks.check("recovery time is recorded", recovery_seconds > 0, recovery_seconds)
    except Exception as error:
        checks.check("scenario ran to completion", False, f"{type(error).__name__}: {error}")
        evidence["samples"] = sampler.rows
    finally:
        sampler.stop.set()
        if sampler.is_alive():
            sampler.join(30)
        sampler.management.close()
        check_management.close()
    result = checks.result("pod_recreate_same_ebs", startedAt=started, finishedAt=time.time(), **evidence)
    fixture.save_result("pod-recreate-result.json", result)
    if result["result"] != "PASS":
        failing = [item["check"] for item in result["checks"] if not item["ok"]]
        raise AcceptanceFailure(f"DRN-SCN-033 failed {len(failing)} check(s): {failing}")
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    add_fixture_arguments(parser)
    parser.add_argument("--ready-timeout", type=float, default=READY_TIMEOUT_SECONDS, help="seconds to wait for the replacement Pod to be Ready")
    args = parser.parse_args(argv)
    fixture = build_fixture(args)
    result = scenario_pod_recreate_same_ebs(fixture, args.ready_timeout)
    print(f"DRN-SCN-033 PASS in {result['recoverySeconds']} s; evidence in {args.evidence}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
