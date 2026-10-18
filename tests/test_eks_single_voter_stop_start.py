"""Unit tests for the CLU-SCN-137 single-voter stop/start harness (tests/chaos/eks-worker-recovery/single_voter_stop_start.py).

No AWS, kubectl or MQTT is used: the fixture, tunnels, MQTT clients and the in-container file reads are fakes.
These tests check the harness logic (plan validation, judges, ordering, cleanup). They do NOT execute the
scenario and carry no scenario tag; the real acceptance link is `SingleVoterRun.execute` against a live fixture.
The full-run tests redirect `@scenario_result` records to a temporary file so a fake run never lands in the
real results file.
"""
from __future__ import annotations

import copy
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import sys
import tempfile
import threading
import types
import unittest
from unittest.mock import patch

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parents[1]
WORKER_DIR = ROOT / "tests/chaos/eks-worker-recovery"
sys.path.insert(0, str(WORKER_DIR))
import power_loss  # noqa: E402
import single_voter_stop_start as svs  # noqa: E402

main_spec = importlib.util.spec_from_file_location("eks_single_voter_main", WORKER_DIR / "main.py")
worker_main = importlib.util.module_from_spec(main_spec)
main_spec.loader.exec_module(worker_main)

RUN = "narumq-ha-acceptance-20261017"
DIGEST = "sha256:" + "a" * 64
TARGET, OBSERVER = "narumq-1", "narumq-0"
VOTERS = [{"pod": f"narumq-{i}", "podUID": f"uid-{i}", "nodeId": str(i + 1), "workerName": f"worker-{i}",
           "instanceId": f"i-{i}", "volumeId": f"vol-{i}", "pvcName": f"data-narumq-{i}", "pvcUID": f"pvc-uid-{i}",
           "pvName": f"pv-{i}"} for i in range(3)]
SNAPSHOTS = ["snap-aaaaaaaa", "snap-bbbbbbbb", "snap-cccccccc"]
WAL, META, SNAP = "/data/wal.bin", "/data/cluster.meta", "/data/snapshot-00000000000000000010.bin"


def plan(**overrides) -> dict:
    value = {"runId": RUN, "clusterName": RUN, "vpcId": "vpc-1", "targetPod": TARGET, "observerPod": OBSERVER,
             "voters": copy.deepcopy(VOTERS), "asgNames": ["eks-" + RUN + "-asg"], "snapshotIds": list(SNAPSHOTS)}
    value.update(overrides)
    return value


def entry(inode=1, size=10, mtime=100, sha="a" * 64) -> dict:
    return {"inode": inode, "size": size, "mtime": mtime, "sha256": sha}


class HttpError(Exception):
    def __init__(self, code: int):
        super().__init__(code)
        self.code = code


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class World:
    """Stateful stand-in for the fixture, the broker files and the MQTT retained store."""
    kubectl = ["kubectl"]
    environment: dict = {}

    def __init__(self, polls_needed: int = 2, create_snapshots: bool = False):
        self.run_id = RUN
        self.polls_needed = polls_needed
        self.polls = 0
        self.started = False
        self.suspended = False
        self.states = {v["instanceId"]: "running" for v in VOTERS}
        self.stop_calls: list = []
        self.start_calls: list = []
        self.snapshots: dict[str, dict] = {}
        self.pending_describes = 1 if create_snapshots else 0
        self.created: list[str] = []
        self.files = {WAL: {"inode": 11, "mtime": 1000, "data": b"W" * 64},
                      META: {"inode": 12, "mtime": 900, "data": b"meta"},
                      SNAP: {"inode": 13, "mtime": 950, "data": b"snapshot"}}
        self.on_start: list = []
        self.on_stop: list = []
        self.retained: dict[str, bytes] = {}
        self.current_pod: str | None = None
        self.no_marker_pods: set[str] = set()
        self.puback_fails = False
        self.pv_handle: str | None = None
        self.snapshot_state = "completed"
        self.drifting = False
        self.api_paths: list[str] = []
        self.exec_calls = 0
        for index, voter in enumerate(VOTERS):
            self.snapshots[SNAPSHOTS[index]] = self.snapshot(SNAPSHOTS[index], voter["volumeId"])

    # -- AWS ----------------------------------------------------------------------------------------------
    def snapshot(self, snapshot_id: str, volume_id: str, state: str = "completed") -> dict:
        return {"SnapshotId": snapshot_id, "VolumeId": volume_id, "State": state,
                "Progress": "100%" if state == "completed" else "50%", "OwnerId": power_loss.ACCOUNT,
                "Encrypted": True, "StartTime": "2026-10-17T00:00:00+00:00"}

    def instance(self, instance_id: str) -> dict:
        return {"InstanceId": instance_id, "VpcId": "vpc-1", "State": {"Name": self.states[instance_id]},
                "Tags": [{"Key": "aws:autoscaling:groupName", "Value": "eks-" + RUN + "-asg"}]}

    def aws(self, *args):
        if args[:2] == ("sts", "get-caller-identity"):
            return {"Account": power_loss.ACCOUNT}
        tags = {"Purpose": "disposable-acceptance-fixture", "DataClass": "synthetic-test-data-only", "AcceptanceRun": RUN}
        if args[:2] == ("eks", "describe-cluster"):
            return {"cluster": {"resourcesVpcConfig": {"vpcId": "vpc-1"}, "tags": tags}}
        if args[:2] == ("ec2", "describe-vpcs"):
            return {"Vpcs": [{"VpcId": "vpc-1", "Tags": [{"Key": k, "Value": v} for k, v in tags.items()]}]}
        if args[:2] == ("ec2", "describe-volumes"):
            voter = next(v for v in VOTERS if v["volumeId"] == args[3])
            return {"Volumes": [{"VolumeId": voter["volumeId"], "AvailabilityZone": "ap-northeast-2a", "Size": 20,
                                 "Encrypted": True, "CreateTime": "2026-10-01T00:00:00+00:00", "State": "in-use",
                                 "Attachments": [{"InstanceId": voter["instanceId"], "State": "attached",
                                                  "AttachTime": "2026-10-01T00:01:00+00:00", "Device": "/dev/xvdaa"}]}]}
        if args[:2] == ("ec2", "describe-instances"):
            return {"Reservations": [{"Instances": [self.instance(i) for i in args[3:]]}]}
        if args[:2] == ("ec2", "describe-snapshots"):
            ids = list(args[3:])
            found = [dict(self.snapshots[i]) for i in ids]
            if self.pending_describes:
                self.pending_describes -= 1
                found = [dict(s, State="pending", Progress="10%") for s in found]
            if self.snapshot_state != "completed":
                found = [dict(s, State=self.snapshot_state, Progress="10%") for s in found]
            return {"Snapshots": found}
        if args[:2] == ("ec2", "create-snapshot"):
            volume_id = args[args.index("--volume-id") + 1]
            snapshot_id = SNAPSHOTS[len(self.created)]
            self.created.append(snapshot_id)
            self.snapshots[snapshot_id] = self.snapshot(snapshot_id, volume_id)
            return {"SnapshotId": snapshot_id}
        if args[:2] == ("ec2", "stop-instances"):
            self.stop_calls.append(args)
            ids = [i for i in args[args.index("--instance-ids") + 1:] if not i.startswith("--")]
            for hook in self.on_stop:
                hook(self)
            return {"StoppingInstances": [{"InstanceId": i} for i in ids]}
        if args[:2] == ("ec2", "start-instances"):
            self.start_calls.append(args)
            self.started = True
            ids = list(args[3:])
            for i in ids:
                self.states[i] = "running"
            for hook in self.on_start:
                hook(self)
            return {"StartingInstances": [{"InstanceId": i} for i in ids]}
        if args[:2] == ("autoscaling", "describe-auto-scaling-groups"):
            return {"AutoScalingGroups": [{"AutoScalingGroupName": "eks-" + RUN + "-asg",
                                           "Tags": [{"Key": "eks:cluster-name", "Value": RUN}]}]}
        raise AssertionError(args)

    def command(self, args, timeout=90, stdin=None):
        if args[:3] == ["aws", "ec2", "wait"]:
            if args[3] == "instance-stopped":
                for i in args[5:]:
                    self.states[i] = "stopped"
            return ""
        if args[:3] == ["aws", "autoscaling", "suspend-processes"]:
            self.suspended = True
            return ""
        if "logs" in args:
            return ""
        raise AssertionError(args)

    # -- Kubernetes and management API ------------------------------------------------------------------------
    def recovering(self) -> bool:
        return self.started and self.polls <= self.polls_needed

    def pod(self, voter: dict) -> dict:
        waiting = self.recovering() and voter["pod"] == TARGET
        restarted = self.started and voter["pod"] == TARGET
        status = {"name": "narumq", "restartCount": 1 if restarted else 0, "ready": not waiting,
                  "imageID": f"repo/narumq@{DIGEST}",
                  "state": {"waiting": {"reason": "CrashLoopBackOff"}} if waiting else {"running": {"startedAt": "t"}}}
        return {"metadata": {"name": voter["pod"], "uid": voter["podUID"], "labels": {"app": "narumq"}},
                "spec": {"nodeName": voter["workerName"]},
                "status": {"conditions": [{"type": "Ready", "status": "False" if waiting else "True"}],
                           "containerStatuses": [status]}}

    def node(self, voter: dict) -> dict:
        return {"metadata": {"name": voter["workerName"]},
                "spec": {"providerID": f"aws:///ap-northeast-2a/{voter['instanceId']}"},
                "status": {"conditions": [{"type": "Ready", "status": "True"}]}}

    def get(self, kind, *args):
        if kind == "namespace":
            return {"metadata": {"labels": {"narumq.io/acceptance-run": RUN}}}
        if kind == "nodes":
            return {"items": [self.node(v) for v in VOTERS]}
        if kind == "pods":
            self.polls += 1 if self.started else 0
            return {"items": [self.pod(v) for v in VOTERS]}
        if kind == "pvc":
            voter = next(v for v in VOTERS if v["pvcName"] == args[0])
            return {"metadata": {"uid": voter["pvcUID"]}, "spec": {"volumeName": voter["pvName"]}}
        if kind == "pv":
            voter = next(v for v in VOTERS if v["pvName"] == args[0])
            handle = self.pv_handle if self.pv_handle and voter["pod"] == TARGET else voter["volumeId"]
            return {"metadata": {"name": voter["pvName"]}, "spec": {"csi": {"volumeHandle": handle}}}
        raise AssertionError((kind, args))

    def get_node(self, name):
        return next((self.node(v) for v in VOTERS if v["workerName"] == name), None)

    def attachment_probe(self, pv_name, volume_id):
        return {"ebs": {"state": "in-use"}}

    def api(self, path, port=23880):
        self.api_paths.append(path)
        if path == "/api/v1/cluster":
            return {"clusterId": RUN, "quorumAvailable": True, "nodesByState": {"ready": 3}}
        if path.startswith("/api/v1/nodes?"):
            rows = []
            for voter in VOTERS:
                draining = self.recovering() and voter["pod"] == TARGET
                rows.append({"nodeId": voter["nodeId"], "state": "DRAINING" if draining else "READY",
                             "ready": not draining, "readinessReason": "draining" if draining else None,
                             "uptimeMillis": 99, "role": "FOLLOWER", "membershipPhase": "VOTER", "queryStatus": "OK",
                             "lastAppliedIndex": 150 if self.started else 100})
            return rows
        if path.endswith("/drain"):
            raise HttpError(404)
        raise AssertionError(path)

    # -- in-container reads -----------------------------------------------------------------------------------
    def inventory_text(self) -> str:
        self.exec_calls += 1
        lines = []
        for path in sorted(self.files):
            item = self.files[path]
            mtime = item["mtime"] + (self.exec_calls if self.drifting and path == WAL else 0)
            lines.append(f"{item['inode']} {len(item['data'])} {mtime} {sha(item['data'])} {path}")
        return "\n".join(lines + ["END"]) + "\n"

    def prefix_text(self, script: str) -> str:
        body = script.split("<<'NARUMQ_LIST'\n", 1)[1].split("\nNARUMQ_LIST", 1)[0]
        lines = []
        for row in body.splitlines():
            path, size = row.split(" ")
            item = self.files.get(path)
            if item is None:
                lines.append(f"MISSING {path}")
            else:
                lines.append(f"FILE {item['inode']} {len(item['data'])} {item['mtime']} {sha(item['data'][:int(size)])} {path}")
        return "\n".join(lines + ["END"]) + "\n"

    def exec_runner(self, pod, script, args=()):
        assert pod == TARGET
        if script == svs.INVENTORY_SCRIPT:
            assert args == ("/data",)
            return self.inventory_text()
        if "NARUMQ_LIST" in script:
            return self.prefix_text(script)
        raise AssertionError(script)


class FakeTunnel:
    world: World | None = None

    def __init__(self, fixture, pod, uid, management_port, mqtt_port):
        self.pod = pod

    def start(self):
        FakeTunnel.world.current_pod = self.pod
        return {"pod": self.pod}

    def check(self):
        pass

    def close(self):
        pass


class FakeClient:
    def __init__(self, world: World, identity: str, port: int):
        self.world = world
        self.acks: list = []
        self.messages: list = []
        self.received = threading.Event()

    def publish(self, topic, payload, retain=False):
        if self.world.puback_fails:
            raise AssertionError("PUBACK not received")
        self.world.retained[topic] = payload
        self.acks.append({"mid": 1, "reason": "Success", "reasonValue": 0})

    def subscribe(self, topic):
        payload = self.world.retained.get(topic)
        if payload is not None and self.world.current_pod not in self.world.no_marker_pods:
            self.messages.append({"topic": topic, "sha256": sha(payload), "qos": 1, "retain": True, "dup": False})
            self.received.set()

    def close(self):
        pass


class PlanTests(unittest.TestCase):
    def test_rejects_unsafe_plans(self):
        no_snapshots = plan()
        del no_snapshots["snapshotIds"]
        both = plan(createSnapshots=True)
        broken = [plan(runId="other"), plan(clusterName="production"), plan(voters=VOTERS[:2]),
                  plan(targetPod="narumq-9"), plan(observerPod=TARGET), plan(asgNames=[]), plan(asgNames=["a", "a"]),
                  plan(holdSeconds=5), plan(holdSeconds=True), plan(readyDeadlineSeconds=10),
                  plan(snapshotWaitSeconds=1), plan(imageDigest="latest"), plan(snapshotIds=SNAPSHOTS[:2]),
                  plan(snapshotIds=["snap-aaaaaaaa"] * 3), plan(snapshotIds=["x", "y", "z"]), no_snapshots, both,
                  plan(createSnapshots=False), plan(dataRoots=[]), plan(dataRoots=["data"]),
                  plan(dataRoots=["/data; rm -rf /"]),
                  plan(voters=[dict(VOTERS[0]), dict(VOTERS[1]), dict(VOTERS[1], pod="narumq-2")])]
        for value in broken:
            with self.subTest(value=sorted(value)), self.assertRaises(ValueError):
                svs.validate_plan(value, RUN)
        missing = plan()
        del missing["voters"][0]["volumeId"]
        with self.assertRaises(ValueError):
            svs.validate_plan(missing, RUN)

    def test_aggregate_needs_every_criterion_and_no_failed_step(self):
        everything = {name: True for name in svs.CRITERIA}
        self.assertEqual(svs.aggregate(everything, None)["result"], "PASS")
        self.assertEqual(svs.aggregate(everything, "recovery")["result"], "FAIL")
        judged = svs.aggregate(dict(everything, **{"recovery.dataFilesUnchanged": 1}), None)
        self.assertEqual((judged["result"], judged["failedCriteria"]), ("FAIL", ["recovery.dataFilesUnchanged"]))


class InventoryTests(unittest.TestCase):
    def test_parse_inventory_round_trip_and_rejections(self):
        text = f"11 5 100 {'b' * 64} {WAL}\n12 4 90 {'c' * 64} {META}\nEND\n"
        files = svs.parse_inventory(text)
        self.assertEqual(files[WAL], {"inode": 11, "size": 5, "mtime": 100, "sha256": "b" * 64})
        for bad in ("", f"11 5 100 {'b' * 64} {WAL}\n", f"11 5 100 {'b' * 64} {WAL}\nEND\nEXTRA\n",
                    f"11 5 100 nothex {WAL}\nEND\n", f"11 5 100 {'b' * 64} /data/a b\nEND\n",
                    f"11 5 100 {'b' * 64} {WAL}\n12 5 100 {'b' * 64} {WAL}\nEND\n", "garbage\nEND\n"):
            with self.subTest(bad=bad), self.assertRaises(power_loss.PowerLossError):
                svs.parse_inventory(bad)

    def test_prefix_script_validates_and_report_parses(self):
        for bad in ({"/data/a b": 1}, {"data/wal.bin": 1}, {WAL: -1}, {WAL: True}):
            with self.subTest(bad=bad), self.assertRaises(power_loss.PowerLossError):
                svs.prefix_script(bad)
        report = svs.parse_prefix_report(f"MISSING {META}\nFILE 11 70 100 {'d' * 64} {WAL}\nEND\n")
        self.assertIsNone(report[META])
        self.assertEqual(report[WAL]["size"], 70)
        with self.assertRaises(power_loss.PowerLossError):
            svs.parse_prefix_report("FILE 1 2\nEND\n")


class DataJudgeTests(unittest.TestCase):
    def setUp(self):
        self.base = {WAL: entry(11, 64, 1000, "1" * 64), META: entry(12, 4, 900, "2" * 64),
                     SNAP: entry(13, 8, 950, "3" * 64)}

    def judge(self, mutate, prefix=None):
        after = copy.deepcopy(self.base)
        mutate(after)
        return svs.judge_data_files(self.base, after, prefix)

    def reasons(self, verdict):
        return sorted((v["path"], v["reason"]) for v in verdict["violations"])

    def test_identical_and_replay_appended_wal_pass(self):
        self.assertEqual(self.judge(lambda a: None)["result"], "PASS")

        def grow(after):
            after[WAL].update(size=100, mtime=2000, sha256="9" * 64)
            after["/data/snapshot-00000000000000000020.bin"] = entry(20, 3, 2000, "4" * 64)

        verdict = self.judge(grow, {WAL: entry(11, 100, 2000, "1" * 64)})
        self.assertEqual(verdict["result"], "PASS")
        self.assertEqual(verdict["walGrowthBytes"], {WAL: 36})
        self.assertEqual(verdict["added"], ["/data/snapshot-00000000000000000020.bin"])

    def test_wal_alterations_fail(self):
        cases = {
            "truncated": (lambda a: a[WAL].update(size=60), None, "truncated"),
            "same size different bytes": (lambda a: a[WAL].update(sha256="8" * 64), None, "prefix_altered"),
            "grown prefix differs": (lambda a: a[WAL].update(size=100, sha256="9" * 64), {WAL: entry(11, 100, 0, "7" * 64)},
                                     "prefix_altered_or_unverified"),
            "grown prefix unverified": (lambda a: a[WAL].update(size=100, sha256="9" * 64), {},
                                        "prefix_altered_or_unverified"),
            "grown prefix file missing": (lambda a: a[WAL].update(size=100), {WAL: None}, "prefix_altered_or_unverified"),
            "rewritten via rename": (lambda a: a[WAL].update(inode=99), None, "replaced_inode_changed"),
        }
        for name, (mutate, prefix, reason) in cases.items():
            with self.subTest(name):
                verdict = self.judge(mutate, prefix)
                self.assertEqual(verdict["result"], "FAIL")
                self.assertEqual(self.reasons(verdict), [(WAL, reason)])

    def test_other_files_must_stay_identical(self):
        self.assertEqual(self.reasons(self.judge(lambda a: a.pop(META))), [(META, "removed")])
        self.assertEqual(self.reasons(self.judge(lambda a: a[SNAP].update(sha256="5" * 64))), [(SNAP, "modified")])
        self.assertEqual(self.reasons(self.judge(lambda a: a[SNAP].update(size=9))), [(SNAP, "modified")])
        self.assertEqual(self.reasons(self.judge(lambda a: a[META].update(mtime=901))), [(META, "mtime_changed")])
        self.assertEqual(self.reasons(self.judge(lambda a: a[META].update(inode=1))), [(META, "replaced_inode_changed")])

    def test_baseline_without_non_empty_wal_fails(self):
        for base in ({META: entry(12, 4)}, {WAL: entry(11, 0), META: entry(12, 4)}):
            verdict = svs.judge_data_files(base, copy.deepcopy(base))
            self.assertEqual(verdict["result"], "FAIL")
            self.assertEqual(verdict["violations"][0]["reason"], "baseline_wal_missing_or_empty")


class SnapshotJudgeTests(unittest.TestCase):
    def setUp(self):
        self.world = World()
        self.volumes = {v["volumeId"]: svs.describe_volume(self.world.aws("ec2", "describe-volumes", "--volume-ids",
                                                                           v["volumeId"])["Volumes"][0])
                        for v in VOTERS}

    def described(self):
        return [dict(self.world.snapshots[i]) for i in SNAPSHOTS]

    def test_three_completed_snapshots_one_per_volume_pass(self):
        verdict = svs.judge_snapshots(self.described(), VOTERS, self.volumes)
        self.assertTrue(verdict["ok"], verdict["problems"])
        self.assertEqual(len(verdict["snapshots"]), 3)

    def test_each_defect_fails(self):
        cases = {
            "two only": lambda s: s[:2],
            "pending": lambda s: [dict(s[0], State="pending", Progress="50%")] + s[1:],
            "duplicate volume": lambda s: [s[0], s[1], dict(s[2], VolumeId=s[0]["VolumeId"])],
            "foreign": lambda s: [dict(s[0], OwnerId="1")] + s[1:],
            "unencrypted": lambda s: [dict(s[0], Encrypted=False)] + s[1:],
            "unknown volume": lambda s: s[:2] + [dict(s[2], VolumeId="vol-x")],
        }
        for name, change in cases.items():
            with self.subTest(name):
                self.assertFalse(svs.judge_snapshots(change(self.described()), VOTERS, self.volumes)["ok"])


class SameVolumeJudgeTests(unittest.TestCase):
    def setUp(self):
        self.planned = VOTERS[1]
        self.base = {"volumeId": "vol-1", "az": "a", "sizeGiB": 20, "encrypted": True, "createTime": "t0"}
        self.fleet = {"vpcId": "vpc-1", "asgNames": ["g"]}

    def current(self, **changes):
        value = {"pvcUID": "pvc-uid-1", "pvName": "pv-1", "volumeHandle": "vol-1", "nodeInstanceId": "i-1",
                 "volume": {**self.base, "state": "in-use",
                            "attachments": [{"instanceId": "i-1", "state": "attached"}]},
                 "instance": {"state": "running", "vpcId": "vpc-1", "group": "g"}}
        value.update(changes)
        return value

    def test_same_instance_and_asg_replacement_with_same_volume_pass(self):
        verdict = svs.judge_same_volume(self.base, self.planned, self.current(), self.fleet)
        self.assertTrue(verdict["ok"], verdict["problems"])
        self.assertFalse(verdict["instanceReplaced"])
        replaced = self.current(nodeInstanceId="i-9")
        replaced["volume"]["attachments"] = [{"instanceId": "i-9", "state": "attached"}]
        verdict = svs.judge_same_volume(self.base, self.planned, replaced, self.fleet)
        self.assertTrue(verdict["ok"], verdict["problems"])
        self.assertEqual((verdict["instanceReplaced"], verdict["attachedInstanceId"]), (True, "i-9"))

    def test_changed_identity_or_attachment_fails(self):
        wrong_volume = self.current()
        wrong_volume["volume"]["volumeId"] = "vol-new"
        recreated = self.current()
        recreated["volume"]["createTime"] = "t1"
        detached = self.current()
        detached["volume"]["attachments"] = []
        elsewhere = self.current()
        elsewhere["volume"]["attachments"] = [{"instanceId": "i-2", "state": "attached"}]
        attaching = self.current()
        attaching["volume"]["attachments"] = [{"instanceId": "i-1", "state": "attaching"}]
        cases = [self.current(volumeHandle="vol-new"), self.current(pvcUID="other"), self.current(pvName="pv-x"),
                 wrong_volume, recreated, detached, elsewhere, attaching,
                 self.current(instance={"state": "stopped", "vpcId": "vpc-1", "group": "g"}),
                 self.current(instance={"state": "running", "vpcId": "vpc-2", "group": "g"}),
                 self.current(instance={"state": "running", "vpcId": "vpc-1", "group": "other"})]
        for index, current in enumerate(cases):
            with self.subTest(index=index):
                self.assertFalse(svs.judge_same_volume(self.base, self.planned, current, self.fleet)["ok"])


class RunTests(unittest.TestCase):
    def execute(self, world: World, run_plan: dict | None = None, mutate_run=None):
        FakeTunnel.world = world
        health = {"asgInstances": {"eks-" + RUN + "-asg": ["i-0", "i-1", "i-2"]}}
        resumed: list = []
        with tempfile.TemporaryDirectory() as text:
            root = Path(text)
            private, output = root / "private", root / "output"
            private.mkdir()
            output.mkdir()
            run = svs.SingleVoterRun(world, run_plan or plan(), private, output, sleep=lambda s: None,
                                     tunnel_factory=FakeTunnel,
                                     client_factory=lambda identity, port, clean, expiry: FakeClient(world, identity, port),
                                     exec_runner=world.exec_runner)
            run.settings.update(holdSeconds=0, readyDeadlineSeconds=30, postResumeSeconds=60, snapshotWaitSeconds=30)
            if mutate_run:
                mutate_run(run)
            error, judged = None, None
            results = root / "scenario-results.jsonl"
            with patch.dict(os.environ, {"NARUMQ_SCENARIO_RESULTS": str(results)}), \
                    patch.object(power_loss, "check_ports", lambda ports: ports), \
                    patch.object(power_loss.az_outage, "suspended_processes",
                                 lambda fixture, group: set(power_loss.PROCESSES) if world.suspended else set()), \
                    patch.object(power_loss.az_outage, "resume_suspended",
                                 lambda fixture, suspended: resumed.append(dict(suspended)) or {"failed": [], "resumed": suspended}), \
                    patch.object(power_loss.az_outage, "restore_health", lambda fixture, plan_, ids: dict(health)), \
                    patch.object(power_loss.az_outage, "observe_post_resume", lambda *a, **k: {"result": "PASS"}), \
                    patch.object(svs, "MARKER_WAIT_SECONDS", 0), \
                    patch.object(power_loss, "DIAGNOSTIC_INTERVAL_SECONDS", 0):
                try:
                    judged = run.execute()
                except power_loss.PowerLossError as exc:
                    error = exc.code
            files = {p.name: json.loads(p.read_text(encoding="utf-8")) for p in output.glob("*.json")}
            records = [json.loads(line) for line in results.read_text(encoding="utf-8").splitlines()] \
                if results.exists() else []
            return run, judged, error, files, resumed, records

    def test_clean_run_passes_with_one_target_stop_and_all_three_marker_reads(self):
        world = World()
        world.on_start.append(lambda w: w.files[WAL].update(data=w.files[WAL]["data"] + b"R" * 8, mtime=2000))
        run, judged, error, files, resumed, records = self.execute(world)
        self.assertIsNone(error)
        self.assertEqual((judged["result"], judged["failedCriteria"]), ("PASS", []))
        self.assertEqual(len(world.stop_calls), 1)
        call = world.stop_calls[0]
        self.assertEqual([i for i in call if i.startswith("i-")], ["i-1"])
        self.assertIn("--skip-os-shutdown", call)
        self.assertEqual(len(world.start_calls), 1)
        self.assertEqual([i for i in world.start_calls[0] if i.startswith("i-")], ["i-1"])
        self.assertEqual(resumed, [run.suspended_by_tool])
        self.assertEqual(files["single-voter-stop-request.json"]["instanceIds"], ["i-1"])
        marker = files["single-voter-marker-after-recovery.json"]
        self.assertEqual(sorted(marker["readsByNodeId"]), ["1", "2", "3"])
        self.assertTrue(all(item["ok"] and item["sha256s"] == [run.marker["sha256"]]
                            for item in marker["readsByNodeId"].values()))
        self.assertEqual(files["single-voter-data-verify.json"]["walGrowthBytes"], {WAL: 8})
        self.assertEqual(files["single-voter-snapshots.json"]["snapshotIds"], SNAPSHOTS)
        self.assertEqual(files["single-voter-same-volume.json"]["volumeId"], "vol-1")
        self.assertEqual(files["single-voter-recovery.json"]["restartCounts"][TARGET], 1)
        timeline = files["single-voter-voters.json"]["timeline"]
        self.assertTrue(any(s["voters"]["2"]["node"] and s["voters"]["2"]["node"]["state"] == "DRAINING" for s in timeline))
        self.assertFalse(any("/health" in path for path in world.api_paths))
        self.assertEqual([(r["test"].rsplit("#", 1)[1], r["status"]) for r in records], [("execute", "pass")])

    def test_created_snapshots_are_waited_on_and_tagged_with_the_run(self):
        world = World(create_snapshots=True)
        value = plan()
        del value["snapshotIds"]
        value["createSnapshots"] = True
        run, judged, error, files, _, _ = self.execute(world, value)
        self.assertIsNone(error)
        self.assertEqual(judged["result"], "PASS")
        self.assertEqual(world.created, SNAPSHOTS)
        self.assertTrue(files["single-voter-snapshots.json"]["created"])
        self.assertIn("single-voter-snapshots-created.json", files)

    def test_data_alterations_fail_after_recovery_but_autoscaling_is_still_resumed(self):
        mutations = {
            "wal truncated": lambda w: w.files[WAL].update(data=w.files[WAL]["data"][:40]),
            "wal bytes changed": lambda w: w.files[WAL].update(data=b"X" * 64),
            "wal prefix changed while growing": lambda w: w.files[WAL].update(data=b"X" * 80),
            "wal replaced": lambda w: w.files[WAL].update(inode=99),
            "snapshot removed": lambda w: w.files.pop(SNAP),
        }
        for name, mutation in mutations.items():
            with self.subTest(name):
                world = World()
                world.on_start.append(mutation)
                run, judged, error, _, resumed, _ = self.execute(world)
                self.assertIsNone(error)
                self.assertEqual(judged["result"], "FAIL")
                self.assertEqual(judged["failedCriteria"], ["recovery.dataFilesUnchanged"])
                self.assertEqual(len(resumed), 1)

    def test_marker_missing_on_the_restarted_voter_fails(self):
        world = World()
        world.on_start.append(lambda w: w.no_marker_pods.add(TARGET))
        run, judged, error, files, _, _ = self.execute(world)
        self.assertIsNone(error)
        self.assertEqual(judged["failedCriteria"], ["durable.retainedMarkerSha256AllVoters"])
        reads = files["single-voter-marker-after-recovery.json"]["readsByNodeId"]
        self.assertEqual([k for k, v in reads.items() if not v["ok"]], ["2"])

    def test_different_volume_after_recovery_fails(self):
        world = World()
        world.on_start.append(lambda w: setattr(w, "pv_handle", "vol-other"))
        run, judged, error, files, _, _ = self.execute(world)
        self.assertIsNone(error)
        self.assertEqual(judged["failedCriteria"], ["recovery.sameVolumeReattached"])
        self.assertIn("pvc_or_pv_identity_changed", files["single-voter-same-volume.json"]["problems"])

    def test_incomplete_snapshots_or_missing_puback_stop_before_any_fault(self):
        for name, setup, code in (("pending", lambda w: setattr(w, "snapshot_state", "pending"),
                                   "snapshots_not_completed_within_bound"),
                                  ("errored", lambda w: setattr(w, "snapshot_state", "error"), "snapshot_failed"),
                                  ("no puback", lambda w: setattr(w, "puback_fails", True), "marker_puback_missing"),
                                  ("marker unreadable", lambda w: w.no_marker_pods.update(v["pod"] for v in VOTERS),
                                   "marker_not_readable_before_fault")):
            with self.subTest(name):
                world = World()
                setup(world)

                def tighten(run):
                    run.settings["snapshotWaitSeconds"] = 0

                run, judged, error, _, _, records = self.execute(world, mutate_run=tighten)
                self.assertEqual(error, code)
                self.assertEqual((world.stop_calls, world.start_calls, world.suspended), ([], [], False))
                self.assertEqual([r["status"] for r in records], ["fail"])

    def test_data_directory_that_keeps_changing_blocks_the_fault(self):
        world = World()
        world.drifting = True
        run, judged, error, _, resumed, _ = self.execute(world)
        self.assertEqual(error, "data_dir_not_quiescent")
        self.assertEqual((world.stop_calls, world.start_calls), ([], []))
        self.assertEqual(len(resumed), 1)

    def test_another_instance_stopped_fails_and_cleanup_starts_before_resuming(self):
        world = World()
        world.on_stop.append(lambda w: w.states.update({"i-2": "stopped"}))
        run, judged, error, files, resumed, _ = self.execute(world)
        self.assertEqual(error, "instances_not_in_expected_state")
        self.assertFalse(run.criteria["fault.onlyTargetStopped"])
        self.assertEqual(len(world.start_calls), 1)
        self.assertTrue(files["power-loss-asg-resume.json"]["startedByCleanup"])


class CliTests(unittest.TestCase):
    def test_mode_requires_execute_and_plan(self):
        with tempfile.TemporaryDirectory() as text, self.assertRaises(SystemExit) as caught:
            worker_main.main(["single-voter-stop-start", "--private", text, "--output", str(Path(text) / "out")])
        self.assertEqual(caught.exception.code, 2)
        with tempfile.TemporaryDirectory() as text, self.assertRaises(SystemExit) as caught:
            svs.main(["--private", text, "--output", str(Path(text) / "out"), "--plan", str(Path(text) / "p.json")])
        self.assertEqual(caught.exception.code, 2)

    def test_missing_prerequisite_fails_with_a_stable_code_and_touches_nothing(self):
        with tempfile.TemporaryDirectory() as text:
            root = Path(text)
            output = root / RUN
            args = types.SimpleNamespace(private=root, output=output, plan=root / "plan.json", execute=True)
            with patch("builtins.print"):
                self.assertEqual(svs.run_cli(args), 1)
            summary = json.loads((output / "summary.json").read_text(encoding="utf-8"))
            self.assertEqual((summary["result"], summary["errorCode"], summary["failedStep"]),
                             ("FAIL", "prerequisite_missing:admin.token", "prerequisites"))

    def test_bad_plan_is_reported_without_touching_the_cluster(self):
        with tempfile.TemporaryDirectory() as text:
            root = Path(text)
            for name in svs.PRIVATE_FILES:
                (root / name).write_text("x", encoding="utf-8")
            (root / "plan.json").write_text(json.dumps(plan(runId="other")), encoding="utf-8")
            output = root / RUN
            args = types.SimpleNamespace(private=root, output=output, plan=root / "plan.json", execute=True)
            world = World()
            with patch.object(svs, "Fixture", lambda private, run_id: world), \
                    patch.object(svs.shutil, "which", lambda name: "aws"), \
                    patch.object(svs, "default_client_factory", lambda private: None), \
                    patch.dict(sys.modules, {"paho": types.ModuleType("paho"), "paho.mqtt": types.ModuleType("paho.mqtt"),
                                             "paho.mqtt.client": types.ModuleType("paho.mqtt.client")}), \
                    patch("builtins.print"):
                self.assertEqual(svs.run_cli(args), 1)
            summary = json.loads((output / "summary.json").read_text(encoding="utf-8"))
            self.assertEqual((summary["result"], summary["errorCode"], summary["failedStep"]), ("FAIL", "ValueError", "plan"))
            self.assertEqual((world.stop_calls, world.start_calls), ([], []))


if __name__ == "__main__":
    unittest.main()
