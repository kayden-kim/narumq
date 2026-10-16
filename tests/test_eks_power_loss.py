"""Unit tests for the CLU-14 P1 full-voter power-loss tool (tests/chaos/eks-worker-recovery/power_loss.py).

No AWS, kubectl or MQTT is used: the fixture, tunnels, streams and oracle are fakes.
"""
from __future__ import annotations

import copy
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest.mock import patch

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parents[1]
WORKER_DIR = ROOT / "tests/chaos/eks-worker-recovery"
sys.path.insert(0, str(WORKER_DIR))
import power_loss  # noqa: E402

main_spec = importlib.util.spec_from_file_location("eks_power_loss_main", WORKER_DIR / "main.py")
worker_main = importlib.util.module_from_spec(main_spec)
main_spec.loader.exec_module(worker_main)

RUN = "narumq-ha-acceptance-20261016"
DIGEST = "sha256:" + "a" * 64
VOTERS = [{"pod": f"narumq-{i}", "podUID": f"uid-{i}", "nodeId": str(i + 1), "workerName": f"worker-{i}",
           "instanceId": f"i-{i}", "volumeId": f"vol-{i}", "pvcName": f"data-narumq-{i}", "pvcUID": f"pvc-uid-{i}",
           "pvName": f"pv-{i}"} for i in range(3)]


def plan(**overrides) -> dict:
    value = {"runId": RUN, "clusterName": RUN, "vpcId": "vpc-1", "durableScenario": "powerloss",
             "voters": copy.deepcopy(VOTERS), "asgNames": ["eks-" + RUN + "-asg"], "publisherPod": "narumq-0",
             "subscriberPod": "narumq-1"}
    value.update(overrides)
    return value


class HttpError(Exception):
    def __init__(self, code: int):
        super().__init__(code)
        self.code = code


class FakeCluster:
    """Stateful stand-in for the fixture: pods become Ready again some polls after `start-instances`."""
    kubectl = ["kubectl"]

    def __init__(self, polls_needed: int = 3, drain_phases: list | None = None):
        self.run_id = RUN
        self.state = "running"
        self.started = False
        self.suspended = False
        self.stop_calls: list = []
        self.start_calls: list = []
        self.api_paths: list[str] = []
        self.api_methods_other_than_get: list = []
        self.polls = 0
        self.polls_needed = polls_needed
        self.drain_phases = drain_phases if drain_phases is not None else []
        self.restarted_pod = "narumq-1"
        self.digest = DIGEST
        self.stuck = False
        self.logs = ""
        self.instances_after_stop = "stopped"

    # -- state -----------------------------------------------------------------------------------------
    def recovering(self) -> bool:
        return self.started and (self.stuck or self.polls <= self.polls_needed)

    def drain_phase(self):
        index = min(self.polls, len(self.drain_phases)) - 1
        if not self.recovering() or not self.drain_phases or index < 0:
            return None
        return self.drain_phases[index]

    def pod(self, voter: dict) -> dict:
        waiting = self.recovering() and voter["pod"] == self.restarted_pod
        restarts = 1 if self.started and voter["pod"] == self.restarted_pod else 0
        status = {"name": "narumq", "restartCount": restarts, "ready": not waiting, "imageID": f"repo/narumq@{self.digest}",
                  "state": {"waiting": {"reason": "CrashLoopBackOff"}} if waiting else {"running": {"startedAt": "t"}}}
        if restarts:
            status["lastState"] = {"terminated": {"reason": "Error", "exitCode": 137, "startedAt": "a", "finishedAt": "b"}}
        return {"metadata": {"name": voter["pod"], "uid": voter["podUID"], "labels": {"app": "narumq"}},
                "spec": {"nodeName": voter["workerName"]},
                "status": {"conditions": [{"type": "Ready", "status": "False" if waiting else "True"}],
                           "containerStatuses": [status]}}

    def rows(self) -> list[dict]:
        rows = []
        for voter in VOTERS:
            draining = self.recovering() and voter["pod"] == self.restarted_pod
            rows.append({"nodeId": voter["nodeId"], "state": "DRAINING" if draining else "READY", "ready": not draining,
                         "readinessReason": "node is draining" if draining else None, "uptimeMillis": 1234,
                         "role": "FOLLOWER", "membershipPhase": "VOTER", "queryStatus": "OK",
                         "lastAppliedIndex": 500 if self.started else 100})
        return rows

    # -- fixture surface --------------------------------------------------------------------------------
    def get(self, kind, *args):
        if kind == "namespace":
            return {"metadata": {"labels": {"narumq.io/acceptance-run": RUN}}}
        if kind == "nodes":
            return {"items": [{"metadata": {"name": v["workerName"]},
                               "spec": {"providerID": f"aws:///ap-northeast-2a/{v['instanceId']}"},
                               "status": {"conditions": [{"type": "Ready", "status": "True"}]}} for v in VOTERS]}
        if kind == "pods":
            self.polls += 1 if self.started else 0
            return {"items": [self.pod(v) for v in VOTERS]}
        if kind == "pvc":
            voter = next(v for v in VOTERS if v["pvcName"] == args[0])
            return {"metadata": {"uid": voter["pvcUID"]}, "spec": {"volumeName": voter["pvName"]}}
        if kind == "pv":
            voter = next(v for v in VOTERS if v["pvName"] == args[0])
            return {"metadata": {"name": voter["pvName"]}, "spec": {"csi": {"volumeHandle": voter["volumeId"]}}}
        raise AssertionError((kind, args))

    def api(self, path, port=23880):
        self.api_paths.append(path)
        if path == "/api/v1/cluster":
            return {"clusterId": RUN, "quorumAvailable": True, "nodesByState": {"ready": 3}}
        if path.startswith("/api/v1/nodes?"):
            return self.rows()
        if path.endswith("/drain"):
            phase = self.drain_phase()
            if phase is None:
                raise HttpError(404)
            return {"phase": phase, "state": "DRAINING", "failure": {"code": "x"} if phase == "FAILED" else None}
        raise AssertionError(path)

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
            return {"Volumes": [{"Attachments": [{"InstanceId": voter["instanceId"]}]}]}
        if args[:2] == ("ec2", "describe-instances"):
            return {"Reservations": [{"Instances": [{"InstanceId": i, "VpcId": "vpc-1", "State": {"Name": self.state},
                                                     "Tags": [{"Key": "aws:autoscaling:groupName", "Value": "eks-" + RUN + "-asg"}]}
                                                    for i in args[3:]]}]}
        if args[:2] == ("ec2", "stop-instances"):
            self.stop_calls.append(args)
            ids = args[args.index("--instance-ids") + 1:]
            ids = [i for i in ids if not i.startswith("--")]
            return {"StoppingInstances": [{"InstanceId": i} for i in ids]}
        if args[:2] == ("ec2", "start-instances"):
            self.start_calls.append(args)
            self.started = True
            self.state = "running"
            return {"StartingInstances": [{"InstanceId": i} for i in args[3:]]}
        if args[:2] == ("autoscaling", "describe-auto-scaling-groups"):
            return {"AutoScalingGroups": [{"AutoScalingGroupName": "eks-" + RUN + "-asg",
                                           "Tags": [{"Key": "eks:cluster-name", "Value": RUN}]}]}
        raise AssertionError(args)

    def command(self, args, timeout=90, stdin=None):
        if args[:3] == ["aws", "ec2", "wait"]:
            if args[3] == "instance-stopped":
                self.state = self.instances_after_stop
            return ""
        if args[:3] == ["aws", "autoscaling", "suspend-processes"]:
            self.suspended = True
            return ""
        if "logs" in args:
            return self.logs
        raise AssertionError(args)


class FakeTunnel:
    created: list = []

    def __init__(self, fixture, pod, uid, management_port, mqtt_port):
        self.args = (pod, uid, management_port, mqtt_port)
        FakeTunnel.created.append(self.args)

    def start(self):
        return {"pod": self.args[0], "podUID": self.args[1]}

    def check(self):
        pass

    def close(self):
        pass


class FakePublisher:
    def __init__(self, private, run_id, scenario, port, output, interval=0.2, max_messages=60):
        self.entries = [{"outcome": "acked"} for _ in range(6)]
        self.capped_at = None

    def start(self):
        pass

    def wait_for_acked(self, count, timeout):
        return True

    def stop(self):
        return {"acked": 6, "rejected": 0, "unknown": 1, "connectionLost": True}


def fake_streams(verify_result: dict | None = None):
    result = {"result": "PASS", "label": "after-power-loss", "sessionPresent": True, "acked": 6, "rejected": 0,
              "unknown": 1, "missingAcked": [], "rejectedDelivered": [], "unknownDelivered": [], "duplicates": 0}
    result.update(verify_result or {})
    return types.SimpleNamespace(prepare_subscriber=lambda *a, **k: {"prepared": True}, DurablePublisher=FakePublisher,
                                 verify_subscriber=lambda *a, **k: result)


class FakeOracle:
    retained = b"marker"
    fail_verify = False

    def prepare(self, target_port, publisher_port):
        pass

    def verify(self, port):
        if FakeOracle.fail_verify:
            raise AssertionError("retained payload changed")


class PlanTests(unittest.TestCase):
    def test_valid_plan_returns_bounded_settings(self):
        value = plan()
        settings = power_loss.validate_plan(value, RUN)
        self.assertEqual((settings["holdSeconds"], settings["readyDeadlineSeconds"], settings["faultAfterAcks"]),
                         (120, 900, 5))
        self.assertEqual(value["instanceIds"], ["i-0", "i-1", "i-2"])
        self.assertEqual(value["durableIntervalSeconds"], 0.5)

    def test_rejects_unsafe_plans(self):
        broken = [plan(runId="other"), plan(clusterName="production"), plan(voters=VOTERS[:2]),
                  plan(publisherPod="narumq-1"), plan(publisherPod="narumq-9"), plan(asgNames=[]),
                  plan(asgNames=["a", "a"]), plan(holdSeconds=5), plan(holdSeconds=True), plan(readyDeadlineSeconds=10),
                  plan(faultAfterAcks=0), plan(imageDigest="latest"), plan(durableScenario="Bad Name"),
                  plan(voters=[dict(VOTERS[0]), dict(VOTERS[1]), dict(VOTERS[1], pod="narumq-2")])]
        for value in broken:
            with self.subTest(value=sorted(value)), self.assertRaises(ValueError):
                power_loss.validate_plan(value, RUN)
        missing = plan()
        del missing["voters"][0]["volumeId"]
        with self.assertRaises(ValueError):
            power_loss.validate_plan(missing, RUN)

    def test_aggregate_needs_every_criterion_and_no_failed_step(self):
        everything = {name: True for name in power_loss.CRITERIA}
        self.assertEqual(power_loss.aggregate(everything, None)["result"], "PASS")
        self.assertEqual(power_loss.aggregate(everything, "recovery")["result"], "FAIL")
        partial = dict(everything, **{"durable.streamOracle": 1})
        judged = power_loss.aggregate(partial, None)
        self.assertEqual((judged["result"], judged["failedCriteria"]), ("FAIL", ["durable.streamOracle"]))
        self.assertIn("recovery.noFailedDrainRecord", power_loss.CRITERIA)
        self.assertIn("recovery.voterDiagnosticsRecorded", power_loss.CRITERIA)


class PreflightTests(unittest.TestCase):
    def setUp(self):
        self.cluster = FakeCluster()
        self.plan = plan()
        power_loss.validate_plan(self.plan, RUN)

    def test_passes_and_records_digest_and_restart_counts(self):
        result = power_loss.preflight(self.cluster, self.plan)
        self.assertEqual(result["image"]["digest"], DIGEST)
        self.assertEqual(result["restartCounts"], {"narumq-0": 0, "narumq-1": 0, "narumq-2": 0})
        self.assertNotIn("/health", " ".join(self.cluster.api_paths))

    def test_rejects_unowned_cluster_vpc_and_instance_before_stop(self):
        original = self.cluster.aws
        for target in ("cluster", "vpc", "instance", "asg"):
            def altered(*args, target=target):
                response = copy.deepcopy(original(*args))
                if target == "cluster" and args[:2] == ("eks", "describe-cluster"):
                    response["cluster"]["tags"]["AcceptanceRun"] = "old-run"
                if target == "vpc" and args[:2] == ("ec2", "describe-vpcs"):
                    response["Vpcs"][0]["Tags"] = []
                if target == "instance" and args[:2] == ("ec2", "describe-instances"):
                    response["Reservations"][0]["Instances"][0]["VpcId"] = "old-vpc"
                if target == "asg" and args[:2] == ("autoscaling", "describe-auto-scaling-groups"):
                    response["AutoScalingGroups"][0]["Tags"] = []
                return response
            self.cluster.aws = altered
            with self.subTest(target=target), self.assertRaises(ValueError):
                power_loss.preflight(self.cluster, self.plan)
            self.assertEqual(self.cluster.stop_calls, [])
        self.cluster.aws = original

    def test_rejects_differing_missing_or_unplanned_digest(self):
        self.cluster.digest = "no-digest"
        with self.assertRaises(power_loss.PowerLossError) as caught:
            power_loss.preflight(self.cluster, self.plan)
        self.assertEqual(caught.exception.code, "image_digest_unavailable")
        self.cluster.digest = DIGEST
        self.plan["imageDigest"] = "sha256:" + "b" * 64
        with self.assertRaises(power_loss.PowerLossError) as caught:
            power_loss.preflight(self.cluster, self.plan)
        self.assertEqual(caught.exception.code, "image_digest_differs_between_voters_or_from_plan")

    def test_rejects_changed_pod_uid_and_existing_drain_record(self):
        self.plan["voters"][1]["podUID"] = "other"
        with self.assertRaises(power_loss.PowerLossError) as caught:
            power_loss.preflight(self.cluster, self.plan)
        self.assertEqual(caught.exception.code, "voter_identity_changed:narumq-1")
        plan_again = plan()
        power_loss.validate_plan(plan_again, RUN)
        self.cluster.drain_phases = ["COMPLETED"]
        self.cluster.started, self.cluster.polls = True, 1
        with self.assertRaises(power_loss.PowerLossError) as caught:
            power_loss.preflight(self.cluster, plan_again)
        self.assertTrue(caught.exception.code.startswith(("drain_record_present", "voter_identity", "cluster_identity")))


class DiagnosticsTests(unittest.TestCase):
    def test_container_diagnostics_report_restart_and_last_termination(self):
        cluster = FakeCluster()
        cluster.started = True
        cluster.polls = 99
        diag = power_loss.container_diagnostics(cluster.pod(VOTERS[1]))
        self.assertEqual((diag["podUID"], diag["restartCount"]), ("uid-1", 1))
        self.assertEqual(diag["lastTermination"], {"reason": "Error", "exitCode": 137, "startedAt": "a", "finishedAt": "b"})
        fresh = power_loss.container_diagnostics(FakeCluster().pod(VOTERS[0]))
        self.assertIsNone(fresh["lastTermination"])

    def test_node_diagnostics_keep_state_reason_and_uptime(self):
        row = FakeCluster().rows()[0] | {"secret": "x"}
        node = power_loss.node_diagnostics(row)
        self.assertEqual((node["state"], node["readinessReason"], node["uptimeMillis"]), ("READY", None, 1234))
        self.assertNotIn("secret", node)
        self.assertIsNone(power_loss.node_diagnostics(None))

    def test_read_drain_record_distinguishes_absent_present_and_unreadable(self):
        cluster = FakeCluster(drain_phases=["COMPLETED", "FAILED"])
        cluster.started, cluster.polls = True, 1
        self.assertEqual(power_loss.read_drain_record(cluster, "2", 1)["phase"], "COMPLETED")
        cluster.polls = 2
        failed = power_loss.read_drain_record(cluster, "2", 1)
        self.assertEqual((failed["phase"], failed["hasFailure"]), ("FAILED", True))
        cluster.polls = 50
        self.assertEqual(power_loss.read_drain_record(cluster, "2", 1), {"present": False})

        class Broken(FakeCluster):
            def api(self, path, port=23880):
                raise HttpError(500)

        self.assertEqual(power_loss.read_drain_record(Broken(), "2", 1), {"present": None, "errorCode": "HttpError"})

    def test_diagnostics_complete_requires_api_state_and_uptime(self):
        voters = {v["nodeId"]: {"container": {"podUID": v["podUID"], "restartCount": 0},
                                "node": {"state": "READY", "uptimeMillis": 5}} for v in VOTERS}
        self.assertTrue(power_loss.diagnostics_complete({"apiReachable": True, "voters": voters}))
        self.assertFalse(power_loss.diagnostics_complete({"apiReachable": False, "voters": voters}))
        voters["1"]["node"] = None
        self.assertFalse(power_loss.diagnostics_complete({"apiReachable": True, "voters": voters}))

    def test_torn_tail_log_parsing(self):
        line = json.dumps({"msg": power_loss.TORN_TAIL_MESSAGE, "path": "/data/wal.bin", "valid_bytes": 10,
                           "dropped_bytes": 3, "reason": "short header"})
        found = power_loss.torn_tail_log("noise\n" + line + "\n")
        self.assertEqual((found["logged"], found["exactMessage"], found["fields"]["dropped_bytes"]), (True, True, 3))
        self.assertEqual(power_loss.torn_tail_log("nothing\n")["logged"], False)
        self.assertFalse(power_loss.torn_tail_log(None)["logRead"])
        fallback = power_loss.torn_tail_log("level=INFO msg=\"WAL torn tail truncated\"\n")
        self.assertTrue(fallback["logged"] and not fallback["exactMessage"])


class RunTests(unittest.TestCase):
    def build(self, directory: Path, cluster: FakeCluster, verify_result: dict | None = None):
        private, output = directory / "private", directory / "output"
        private.mkdir()
        output.mkdir()
        value = plan()
        run = power_loss.PowerLossRun(cluster, value, private, output, sleep=lambda s: None, tunnel_factory=FakeTunnel,
                                      streams=fake_streams(verify_result), oracle_factory=FakeOracle)
        run.settings.update(holdSeconds=0, readyDeadlineSeconds=30, postResumeSeconds=60)
        return run, output

    def execute(self, cluster: FakeCluster, verify_result: dict | None = None):
        FakeOracle.fail_verify = False
        FakeTunnel.created.clear()
        health = {"asgInstances": {RUN + "-asg": ["i-0", "i-1", "i-2"]}}
        with tempfile.TemporaryDirectory() as text:
            run, output = self.build(Path(text), cluster, verify_result)
            error = None
            with patch.object(power_loss, "check_ports", lambda ports: ports), \
                    patch.object(power_loss.az_outage, "suspended_processes",
                                 lambda fixture, group: set(power_loss.PROCESSES) if cluster.suspended else set()), \
                    patch.object(power_loss.az_outage, "resume_suspended",
                                 lambda fixture, suspended: {"failed": [], "resumed": suspended}), \
                    patch.object(power_loss.az_outage, "restore_health", lambda fixture, plan_, ids: dict(health)), \
                    patch.object(power_loss.az_outage, "observe_post_resume", lambda *a, **k: {"result": "PASS"}):
                try:
                    run.execute()
                except power_loss.PowerLossError as exc:
                    error = exc.code
            files = {p.name: json.loads(p.read_text(encoding="utf-8")) for p in output.glob("power-loss-*.json")}
            return run, error, files

    def test_clean_run_stops_all_voters_in_one_call_and_passes(self):
        cluster = FakeCluster(polls_needed=0)
        run, error, files = self.execute(cluster)
        self.assertIsNone(error)
        self.assertEqual(len(cluster.stop_calls), 1)
        call = cluster.stop_calls[0]
        self.assertEqual(sorted(i for i in call if i.startswith("i-")), ["i-0", "i-1", "i-2"])
        self.assertIn("--skip-os-shutdown", call)
        self.assertEqual(len(cluster.start_calls), 1)
        judged = power_loss.aggregate(run.criteria, None)
        self.assertEqual((judged["result"], judged["failedCriteria"]), ("PASS", []))
        self.assertEqual(files["power-loss-stop-request.json"]["oneCall"], True)
        self.assertFalse(any("/health" in path for path in cluster.api_paths))
        voters = files["power-loss-voters.json"]
        self.assertFalse(voters["healthReadyCalled"])
        self.assertEqual(sorted(voters["final"]["voters"]), ["1", "2", "3"])

    def test_restarted_draining_voter_with_completed_record_is_waited_on_and_recorded(self):
        cluster = FakeCluster(polls_needed=3, drain_phases=["COMPLETED", "COMPLETED", "COMPLETED"])
        run, error, files = self.execute(cluster)
        self.assertIsNone(error)
        voters = files["power-loss-voters.json"]
        draining = [s for s in voters["timeline"] if s["voters"]["2"]["node"] and s["voters"]["2"]["node"]["state"] == "DRAINING"]
        self.assertTrue(draining)
        first = draining[0]["voters"]["2"]
        self.assertTrue(first["restartedSincePreflight"])
        self.assertEqual(first["container"]["lastTermination"]["exitCode"], 137)
        self.assertEqual(first["node"]["readinessReason"], "node is draining")
        self.assertEqual(first["drainRecord"]["phase"], "COMPLETED")
        self.assertEqual(voters["drainRecordsSeen"], {})
        self.assertTrue(run.criteria["recovery.drainRecordsCleared"])
        self.assertTrue(run.criteria["recovery.noFailedDrainRecord"])
        self.assertEqual(power_loss.aggregate(run.criteria, None)["result"], "PASS")
        self.assertFalse(any("/health" in path for path in cluster.api_paths))

    def test_failed_drain_record_fails_the_run_and_is_never_deleted(self):
        cluster = FakeCluster(polls_needed=5, drain_phases=["FAILED"] * 5)
        run, error, files = self.execute(cluster)
        self.assertEqual(error, "drain_record_failed:narumq-1")
        self.assertFalse(run.criteria["recovery.noFailedDrainRecord"])
        self.assertEqual(files["power-loss-voters.json"]["failedDrainRecord"]["nodeId"], "2")
        self.assertTrue(run.criteria["asg.resumed"])
        self.assertEqual(power_loss.aggregate(run.criteria, "recovery")["result"], "FAIL")
        self.assertFalse(any("/health" in path for path in cluster.api_paths))

    def test_stuck_voter_records_diagnostics_and_fails_within_bound(self):
        cluster = FakeCluster(polls_needed=0)
        cluster.stuck = True
        with patch.object(power_loss, "DIAGNOSTIC_INTERVAL_SECONDS", 0):
            with tempfile.TemporaryDirectory() as text:
                run, output = self.build(Path(text), cluster)
                run.settings["readyDeadlineSeconds"] = 0
                run.marks["startRequested"] = power_loss.time.monotonic()
                with self.assertRaises(power_loss.PowerLossError) as caught:
                    run.step_recovery()
                self.assertEqual(caught.exception.code, "three_voters_not_ready_within_bound")
                self.assertFalse(run.criteria["recovery.threeReadyWithinBound"])
                saved = json.loads((output / "power-loss-voters.json").read_text(encoding="utf-8"))
                self.assertTrue(saved["timeline"])

    def test_failure_after_stop_request_starts_instances_before_resuming_autoscaling(self):
        cluster = FakeCluster()
        cluster.instances_after_stop = "stopping"
        run, error, files = self.execute(cluster)
        self.assertEqual(error, "instances_not_all_stopped")
        self.assertEqual(len(cluster.start_calls), 1)
        self.assertTrue(files["power-loss-asg-resume.json"]["startedByCleanup"])

    def test_durable_loss_or_changed_marker_fails_the_criteria(self):
        run, error, _ = self.execute(FakeCluster(polls_needed=0), {"result": "FAIL", "missingAcked": [3]})
        self.assertIsNone(error)
        self.assertFalse(run.criteria["durable.streamOracle"])
        FakeOracle.fail_verify = False
        cluster = FakeCluster(polls_needed=0)
        with tempfile.TemporaryDirectory() as text:
            run, _ = self.build(Path(text), cluster)
            FakeOracle.fail_verify = True
            try:
                run.baseline = {"retainedSha256": "x", "maxLastAppliedIndex": 1}
                run.step_verify()
            finally:
                FakeOracle.fail_verify = False
            self.assertFalse(run.criteria["durable.retainedMarkerSha256"])
            self.assertFalse(run.criteria["durable.offlineSession"])


class CliTests(unittest.TestCase):
    def test_mode_requires_execute_and_plan(self):
        with tempfile.TemporaryDirectory() as text, self.assertRaises(SystemExit) as caught:
            worker_main.main(["power-loss", "--private", text, "--output", str(Path(text) / "out")])
        self.assertEqual(caught.exception.code, 2)

    def test_bad_plan_is_reported_without_touching_the_cluster(self):
        with tempfile.TemporaryDirectory() as text:
            root = Path(text)
            (root / "plan.json").write_text(json.dumps(plan(runId="other")), encoding="utf-8")
            output = root / RUN
            args = types.SimpleNamespace(private=root, output=output, plan=root / "plan.json", execute=True)
            with patch.object(power_loss, "Fixture", lambda private, run_id: FakeCluster()), patch("builtins.print"):
                self.assertEqual(power_loss.run_cli(args), 1)
            summary = json.loads((output / "summary.json").read_text(encoding="utf-8"))
            self.assertEqual((summary["result"], summary["errorCode"], summary["failedStep"]), ("FAIL", "ValueError", "plan"))
            self.assertIn("EBS internal write-cache", summary["scope"])


if __name__ == "__main__":
    unittest.main()
