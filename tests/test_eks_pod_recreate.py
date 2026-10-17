"""Offline tests of the DRN-SCN-033 Pod-recreation harness judgments (target choice, storage identity, quorum samples).

They judge observed data only. The scenario itself runs tests/chaos/eks-pod-recreate/main.py against a real
disposable EKS fixture, so nothing here is scenario-tagged.
"""
from __future__ import annotations

import importlib.util
import sys
import time
import unittest
from pathlib import Path

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parents[1]
HARNESS = ROOT / "tests/chaos/eks-pod-recreate"
sys.path.insert(0, str(HARNESS))
sys.path.insert(0, str(ROOT / "tests/chaos"))
import recreate_judge as judge  # noqa: E402
from eks_broker import PrerequisiteMissing  # noqa: E402

spec = importlib.util.spec_from_file_location("eks_pod_recreate_main", HARNESS / "main.py")
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)

VOLUME = "vol-09c3a0e11277903d9"


def pod(name, uid, node="ip-10-0-0-1", ready=True, deleting=None, claim="data-narumq-1") -> dict:
    return {"metadata": {"name": name, "uid": uid}, "spec": {"nodeName": node, "volumes": [{"name": "data", "persistentVolumeClaim": {"claimName": claim}}]},
            "ready": ready, "deleting": deleting, "uid": uid}


def pvc(uid="pvc-uid-1", name="data-narumq-1", pv="pv-1", phase="Bound") -> dict:
    return {"metadata": {"name": name, "uid": uid}, "spec": {"volumeName": pv}, "status": {"phase": phase}}


def pv(name="pv-1", handle=VOLUME, driver="ebs.csi.aws.com") -> dict:
    return {"metadata": {"name": name}, "spec": {"csi": {"driver": driver, "volumeHandle": handle}}}


def volume(instance="i-old", state="in-use", attach="attached", volume_id=VOLUME, zone="ap-northeast-2b") -> dict:
    return {"VolumeId": volume_id, "State": state, "AvailabilityZone": zone, "VolumeType": "gp3", "Size": 20, "Encrypted": True,
            "Attachments": [{"InstanceId": instance, "State": attach}] if instance else []}


def snap(pod_uid="pod-uid-1", instance="i-old", **kw) -> dict:
    return judge.storage_snapshot(pod("narumq-1", pod_uid), pvc(**kw.get("pvc", {})), pv(**kw.get("pv", {})), volume(instance=instance, **kw.get("volume", {})), instance)


def names(rows) -> dict:
    return {name: ok for name, ok, _ in rows}


def failing(rows) -> list:
    return [name for name, ok, _ in rows if not ok]


class TargetTests(unittest.TestCase):
    PODS = {"narumq-0": {}, "narumq-1": {}, "narumq-2": {}}
    SLOTS = {0: "1", 1: "2", 2: "3"}

    def test_follower_target_and_distinct_survivor(self):
        self.assertEqual(judge.choose_target(self.PODS, self.SLOTS, "1"), ("narumq-1", "2", "narumq-0"))
        self.assertEqual(judge.choose_target(self.PODS, self.SLOTS, "2"), ("narumq-0", "1", "narumq-1"))

    def test_leader_is_never_the_target(self):
        for leader in ("1", "2", "3"):
            target, node, survivor = judge.choose_target(self.PODS, self.SLOTS, leader)
            self.assertNotEqual(node, leader)
            self.assertNotEqual(target, survivor)

    def test_no_follower_is_a_missing_prerequisite(self):
        with self.assertRaises(PrerequisiteMissing):
            judge.choose_target({"narumq-0": {}}, {0: "1"}, "1")
        with self.assertRaises(PrerequisiteMissing):
            judge.choose_target({}, self.SLOTS, "1")

    def test_preflight(self):
        ready = {n: {"ready": True, "deleting": None} for n in self.PODS}
        cluster = {"quorumAvailable": True, "memberCount": 3, "nodesByState": {"ready": 3}}
        self.assertEqual(judge.preflight_problems(ready, cluster), [])
        self.assertTrue(judge.preflight_problems({**ready, "narumq-2": {"ready": False, "deleting": None}}, cluster))
        self.assertTrue(judge.preflight_problems(ready, {**cluster, "quorumAvailable": False}))
        self.assertTrue(judge.preflight_problems(ready, {**cluster, "nodesByState": {"ready": 2}}))
        self.assertTrue(judge.preflight_problems(ready, {**cluster, "memberCount": 4}))
        self.assertTrue(judge.preflight_problems({"narumq-0": ready["narumq-0"]}, cluster))


class VolumeIdentityTests(unittest.TestCase):
    def test_ebs_csi_handle(self):
        self.assertEqual(judge.volume_id_of(pv()), VOLUME)

    def test_non_ebs_volumes_are_refused(self):
        for bad in (pv(driver="efs.csi.aws.com"), pv(handle="fs-123"), pv(handle=""), {"spec": {}, "metadata": {"name": "x"}}):
            with self.assertRaises(PrerequisiteMissing):
                judge.volume_id_of(bad)


class SameStorageTests(unittest.TestCase):
    def test_same_pvc_pv_and_ebs_reattached_to_the_new_instance(self):
        before = snap("pod-uid-1", "i-old")
        after = snap("pod-uid-2", "i-new")
        self.assertEqual(failing(judge.judge_same_storage(before, after)), [])

    def test_same_pod_uid_is_not_a_recreation(self):
        self.assertIn("recreated Pod has a new UID", failing(judge.judge_same_storage(snap("pod-uid-1"), snap("pod-uid-1", "i-new"))))

    def test_pvc_replaced_is_detected(self):
        after = snap("pod-uid-2", "i-new", pvc={"uid": "pvc-uid-2"})
        self.assertIn("same PVC object (UID unchanged, still Bound)", failing(judge.judge_same_storage(snap(), after)))

    def test_pvc_not_bound_is_detected(self):
        after = snap("pod-uid-2", "i-new", pvc={"phase": "Pending"})
        self.assertIn("same PVC object (UID unchanged, still Bound)", failing(judge.judge_same_storage(snap(), after)))

    def test_pv_or_volume_handle_changed_is_detected(self):
        other = "vol-0123456789abcdef0"
        after = snap("pod-uid-2", "i-new", pv={"handle": other}, volume={"volume_id": other}, pvc={"pv": "pv-1"})
        self.assertIn("same CSI volumeHandle", failing(judge.judge_same_storage(snap(), after)))
        after_pv = snap("pod-uid-2", "i-new", pv={"name": "pv-2"}, pvc={"pv": "pv-2"})
        self.assertIn("same PV", failing(judge.judge_same_storage(snap(), after_pv)))

    def test_pv_not_bound_to_the_claim_volume_is_detected(self):
        after = snap("pod-uid-2", "i-new", pv={"name": "pv-1"}, pvc={"pv": "pv-9"})
        self.assertIn("same PV", failing(judge.judge_same_storage(snap(), after)))

    def test_ec2_volume_must_be_the_one_the_pv_names(self):
        after = snap("pod-uid-2", "i-new", volume={"volume_id": "vol-0123456789abcdef0"})
        self.assertIn("EC2 reports the volume the PV names", failing(judge.judge_same_storage(snap(), after)))

    def test_attachment_on_the_old_instance_fails(self):
        after = snap("pod-uid-2", "i-new")
        after["attachments"] = [{"InstanceId": "i-old", "State": "attached"}]
        self.assertIn("EBS volume is attached to the instance of the recreated Pod's node", failing(judge.judge_same_storage(snap(), after)))

    def test_detached_volume_or_pending_attachment_fails(self):
        detached = snap("pod-uid-2", "i-new", volume={"state": "available"})
        detached["attachments"] = []
        self.assertIn("EBS volume is in-use with exactly one attachment in state attached", failing(judge.judge_same_storage(snap(), detached)))
        attaching = snap("pod-uid-2", "i-new", volume={"attach": "attaching"})
        self.assertIn("EBS volume is in-use with exactly one attachment in state attached", failing(judge.judge_same_storage(snap(), attaching)))
        double = snap("pod-uid-2", "i-new")
        double["attachments"].append({"InstanceId": "i-old", "State": "attached"})
        self.assertIn("EBS volume is in-use with exactly one attachment in state attached", failing(judge.judge_same_storage(snap(), double)))

    def test_changed_zone_is_detected(self):
        after = snap("pod-uid-2", "i-new", volume={"zone": "ap-northeast-2c"})
        self.assertIn("EBS volume identity is unchanged (zone, type, size, encryption)", failing(judge.judge_same_storage(snap(), after)))

    def test_different_claim_mounted_is_detected(self):
        after = snap("pod-uid-2", "i-new")
        after["podClaims"] = ["data-narumq-2"]
        self.assertIn("recreated Pod mounts the same claim name", failing(judge.judge_same_storage(snap(), after)))

    def test_snapshot_records_the_claim_and_attachment(self):
        value = snap()
        self.assertEqual((value["podClaims"], value["pvc"], value["pv"], value["volumeHandle"], value["volumeState"]),
                         (["data-narumq-1"], "data-narumq-1", "pv-1", VOLUME, "in-use"))
        self.assertEqual(value["attachments"], [{"InstanceId": "i-old", "State": "attached"}])


def samples(count=10, start=1000.0, step=2.0, ready=lambda i: 2 if 3 <= i <= 6 else 3, **override) -> list:
    rows = []
    for i in range(count):
        row = {"at": start + i * step, "quorumAvailable": True, "leaderNodeId": "1", "memberCount": 3, "nodesByState": {"ready": ready(i)}}
        row.update(override)
        rows.append(row)
    return rows


class QuorumSampleTests(unittest.TestCase):
    def test_continuous_quorum_through_the_outage_passes(self):
        self.assertEqual(failing(judge.judge_quorum_samples(samples(), deleted_at=1004.0, ready_at=1012.0)), [])

    def test_one_sample_without_quorum_fails(self):
        rows = samples()
        rows[4]["quorumAvailable"] = False
        self.assertIn("quorumAvailable is true in every survivor sample", failing(judge.judge_quorum_samples(rows, 1004.0, 1012.0)))

    def test_missing_quorum_field_is_not_treated_as_true(self):
        rows = samples()
        del rows[2]["quorumAvailable"]
        self.assertIn("quorumAvailable is true in every survivor sample", failing(judge.judge_quorum_samples(rows, 1004.0, 1012.0)))

    def test_read_errors_fail(self):
        rows = samples()
        rows[5] = {"at": rows[5]["at"], "error": "TimeoutError: timed out"}
        result = failing(judge.judge_quorum_samples(rows, 1004.0, 1012.0))
        self.assertIn("no survivor sample failed to read the cluster summary", result)
        self.assertIn("quorumAvailable is true in every survivor sample", result)

    def test_member_count_and_leader(self):
        rows = samples()
        rows[1]["memberCount"] = 2
        rows[2]["leaderNodeId"] = None
        result = failing(judge.judge_quorum_samples(rows, 1004.0, 1012.0))
        self.assertIn("memberCount stays 3 in every sample", result)
        self.assertIn("a leader is reported in every sample", result)

    def test_samples_must_cover_the_delete_and_the_recovery(self):
        result = failing(judge.judge_quorum_samples(samples(), deleted_at=999.0, ready_at=1012.0))
        self.assertIn("samples cover the DELETE through the recovery", result)
        self.assertIn("samples cover the DELETE through the recovery", failing(judge.judge_quorum_samples(samples(), 1004.0, 1100.0)))

    def test_sampling_gap_is_bounded(self):
        rows = samples()
        for row in rows[5:]:
            row["at"] += 30
        self.assertIn(f"consecutive samples are at most {judge.MAX_SAMPLE_GAP_SECONDS:g} s apart", failing(judge.judge_quorum_samples(rows, 1004.0, 1040.0)))

    def test_sampler_must_have_seen_the_outage(self):
        self.assertIn("the sampler saw the outage window (fewer than 3 READY nodes at some point)",
                      failing(judge.judge_quorum_samples(samples(ready=lambda i: 3), 1004.0, 1012.0)))

    def test_too_few_or_no_samples_fail(self):
        self.assertTrue(failing(judge.judge_quorum_samples([], 1004.0, 1012.0)))
        self.assertIn("at least 3 survivor samples were taken", failing(judge.judge_quorum_samples(samples(2), 1000.0, 1001.0)))


def broker_pods(**override) -> dict:
    pods = {f"narumq-{i}": {"uid": f"uid-{i}", "ready": True, "deleting": None} for i in range(3)}
    for name, change in override.items():
        pods[name.replace("_", "-")] = {**pods[name.replace("_", "-")], **change}
    return pods


class RecoveryTests(unittest.TestCase):
    ROW = {"nodeId": "2", "state": "READY", "ready": True, "queryStatus": "OK"}
    CLUSTER = {"quorumAvailable": True, "nodesByState": {"ready": 3}}

    def judged(self, after, row="default", cluster=None):
        return failing(judge.judge_recovery(broker_pods(), after, "narumq-1", self.ROW if row == "default" else row, cluster or self.CLUSTER))

    def test_recovered(self):
        self.assertEqual(self.judged(broker_pods(narumq_1={"uid": "uid-new"})), [])

    def test_target_not_ready(self):
        self.assertTrue(self.judged(broker_pods(narumq_1={"uid": "uid-new", "ready": False})))

    def test_survivor_replaced_is_detected(self):
        self.assertIn("survivor Pods kept their UIDs (only the target was replaced)", self.judged(broker_pods(narumq_1={"uid": "uid-new"}, narumq_0={"uid": "uid-x"})))

    def test_node_row_must_be_ready(self):
        self.assertTrue(self.judged(broker_pods(), row={**self.ROW, "ready": False}))
        self.assertTrue(self.judged(broker_pods(), row={**self.ROW, "state": "DRAINING"}))
        self.assertTrue(self.judged(broker_pods(), row=None))

    def test_cluster_must_have_three_ready_and_quorum(self):
        self.assertTrue(self.judged(broker_pods(), cluster={"quorumAvailable": True, "nodesByState": {"ready": 2}}))
        self.assertTrue(self.judged(broker_pods(), cluster={"quorumAvailable": False, "nodesByState": {"ready": 3}}))

    def test_pod_still_deleting_fails(self):
        self.assertTrue(self.judged(broker_pods(narumq_1={"deleting": "2026-10-17T00:00:00Z"})))


def run_sampler(management) -> list:
    sampler = runner.QuorumSampler(management, 0.01)
    sampler.start()
    deadline = time.time() + 5
    while not sampler.rows and time.time() < deadline:
        time.sleep(0.01)
    sampler.stop.set()
    sampler.join(5)
    return sampler.rows


class SamplerTests(unittest.TestCase):
    def test_sampler_records_errors_as_failed_samples(self):
        class Broken:
            def data(self, path):
                raise TimeoutError("timed out")

        rows = run_sampler(Broken())
        self.assertTrue(rows)
        self.assertIn("TimeoutError", rows[0]["error"])
        self.assertTrue(failing(judge.judge_quorum_samples(rows, rows[0]["at"], rows[0]["at"])))

    def test_sampler_records_the_four_summary_fields(self):
        class Fixed:
            def data(self, path):
                assert path == "/api/v1/cluster"
                return {"quorumAvailable": True, "leaderNodeId": "1", "memberCount": 3, "nodesByState": {"ready": 3}, "extra": 1}

        row = run_sampler(Fixed())[0]
        self.assertEqual({k: row[k] for k in ("quorumAvailable", "leaderNodeId", "memberCount", "nodesByState")},
                         {"quorumAvailable": True, "leaderNodeId": "1", "memberCount": 3, "nodesByState": {"ready": 3}})
        self.assertNotIn("extra", row)
        self.assertNotIn("error", row)


if __name__ == "__main__":
    unittest.main()
