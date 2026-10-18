from __future__ import annotations

import base64
import copy
import datetime
import hashlib
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parents[1]
RESEED_DIR = ROOT / "tests/chaos/eks-volume-reseed"
sys.path.insert(0, str(RESEED_DIR))
import reseed  # noqa: E402
import mqtt_helpers  # noqa: E402

main_spec = importlib.util.spec_from_file_location("eks_volume_reseed_main", RESEED_DIR / "main.py")
reseed_main = importlib.util.module_from_spec(main_spec)
main_spec.loader.exec_module(reseed_main)

RUN = "narumq-ha-acceptance-20261016"
DNS = [reseed.slot_dns(i) for i in range(3)]


def ready_condition() -> dict:
    return {"conditions": [{"type": "Ready", "status": "True"}]}


def plan(**overrides) -> dict:
    value = {"runId": RUN, "clusterName": RUN, "vpcId": "vpc-1", "slotOrdinal": 2, "targetPod": "narumq-2",
             "targetNodeId": "3", "newNodeId": "4", "oldPodUID": "pod-old", "oldWorkerName": "worker-old",
             "oldNodeUID": "node-old-uid", "oldInstanceId": "i-old", "pvcName": "data-narumq-2", "pvcUID": "pvc-old",
             "pvName": "pv-old", "volumeId": "vol-old", "zone": "ap-northeast-2a", "spareWorkerName": "worker-spare",
             "spareInstanceId": "i-spare", "observerPod": "narumq-0", "observerPodUID": "pod-observer",
             "observerWorkerName": "worker-observer", "observerInstanceId": "i-observer", "durableScenario": "reseed"}
    value.update(overrides)
    return value


def bootstrap_config(node_id: int, ordinal: int) -> dict:
    return {"mode": "cluster", "clusterId": RUN, "nodeId": node_id, "directory": "/data",
            "listenAddress": "0.0.0.0:7000", "advertisedAddress": reseed.slot_address(ordinal),
            "localDnsName": reseed.slot_dns(ordinal), "clusterProtocolMajor": 2, "clusterProtocolMinor": 0,
            "stateSchemaMajor": 1, "stateSchemaMinor": 0, "role": "bootstrap",
            "bootstrapMembers": [{"memberId": i + 1, "address": reseed.slot_address(i), "dnsSan": reseed.slot_dns(i),
                                  "certificateSha256": "0" * 64} for i in range(3)],
            "seedAddresses": [], "peerTLS": {"certFile": "/peer-tls/node.crt", "keyFile": "/peer-tls/node.key",
                                             "caFile": "/peer-tls/ca.crt"},
            "management": {"listenAddress": "0.0.0.0:8080", "auditDirectory": "/audit"},
            "mqtt": {"tcpListenAddress": "0.0.0.0:1883"}}


def configmap(slot2: str = "3") -> dict:
    data = {"slot-0": "1", "slot-1": "2", "slot-2": slot2}
    for node_id, ordinal in ((1, 0), (2, 1), (3, 2)):
        data[f"node-{node_id}.json"] = json.dumps(bootstrap_config(node_id, ordinal), indent=2) + "\n"
    return {"kind": "ConfigMap", "apiVersion": "v1", "metadata": {"name": reseed.CONFIGMAP, "resourceVersion": "7"},
            "data": data}


def node_row(node_id: str, phase: str = "VOTER", role: str | None = "FOLLOWER", status: str = "OK", **extra) -> dict:
    return {"nodeId": node_id, "membershipPhase": phase, "queryStatus": status, "role": role, "ready": status == "OK",
            "lastAppliedIndex": 100 if status == "OK" else None, **extra}


def three_voters() -> list[dict]:
    return [node_row("1", role="LEADER"), node_row("2"), node_row("3")]


def pod(name: str, uid: str, node: str, ready_state: bool = True) -> dict:
    status = ready_condition() if ready_state else {"conditions": [{"type": "Ready", "status": "False"}]}
    return {"metadata": {"name": name, "uid": uid, "labels": {"app": "narumq"}}, "spec": {"nodeName": node},
            "status": status}


class FakePreflightFixture:
    """Static, internally consistent healthy fixture; tests change one fact at a time."""
    kubectl = ["kubectl"]

    def __init__(self, **changes):
        self.run_id = RUN
        self.state = {"cluster": {"clusterId": RUN, "quorumAvailable": True, "leaderNodeId": "1"},
                      "nodes": three_voters(), "configmap": configmap(), "delete_on_termination": False,
                      "spare": {"metadata": {"uid": "spare-uid", "labels": {"topology.kubernetes.io/zone": "ap-northeast-2a"}},
                                "spec": {"providerID": "aws:///ap-northeast-2a/i-spare"}, "status": ready_condition()},
                      "spare_state": "running", "extra_pod_on_spare": False}
        self.state.update(changes)
        self.mutations: list = []

    def aws(self, *args):
        if args[:2] == ("sts", "get-caller-identity"):
            return {"Account": reseed.ACCOUNT}
        if args[:2] == ("eks", "describe-cluster"):
            return {"cluster": {"resourcesVpcConfig": {"vpcId": "vpc-1"}}}
        if args[:2] == ("ec2", "describe-volumes"):
            return {"Volumes": [{"AvailabilityZone": "ap-northeast-2a", "State": "in-use", "Tags": [],
                                 "Attachments": [{"InstanceId": "i-old"}]}]}
        raise AssertionError(args)

    def get(self, *args):
        kind = args[0]
        if kind == "namespace":
            return {"metadata": {"labels": {"narumq.io/acceptance-run": RUN}}}
        if kind == "configmap":
            return copy.deepcopy(self.state["configmap"])
        if kind == "pods":
            items = [pod("narumq-0", "pod-observer", "worker-observer"), pod("narumq-1", "pod-1", "worker-1"),
                     pod("narumq-2", "pod-old", "worker-old")]
            if self.state["extra_pod_on_spare"]:
                items[1]["spec"]["nodeName"] = "worker-spare"
            return {"items": items}
        if kind == "pvc":
            return {"metadata": {"uid": "pvc-old"}, "spec": {"volumeName": "pv-old"}}
        if kind == "pv":
            return {"metadata": {"name": "pv-old"}, "spec": {"csi": {"volumeHandle": "vol-old"},
                                                             "claimRef": {"name": "data-narumq-2", "uid": "pvc-old"}}}
        raise AssertionError(args)

    def get_node(self, name):
        if name == "worker-old":
            return {"metadata": {"uid": "node-old-uid"}, "spec": {"providerID": "aws:///ap-northeast-2a/i-old"},
                    "status": ready_condition()}
        if name == "worker-spare":
            return copy.deepcopy(self.state["spare"])
        raise AssertionError(name)

    def instance(self, instance_id):
        return {"State": {"Name": "running"}, "VpcId": "vpc-1", "BlockDeviceMappings": [
            {"DeviceName": "/dev/xvdb", "Ebs": {"VolumeId": "vol-old",
                                                "DeleteOnTermination": self.state["delete_on_termination"]}}]}

    def instance_state(self, instance_id):
        return self.state["spare_state"]

    def api(self, path, port=23880):
        if path == "/api/v1/cluster":
            return copy.deepcopy(self.state["cluster"])
        if path.startswith("/api/v1/nodes"):
            return copy.deepcopy(self.state["nodes"])
        if path == "/api/v1/cluster/features":
            return {"activeFeatures": [], "members": [{"nodeId": i, "releaseVersion": "1.2.0", "features": []} for i in "123"]}
        raise AssertionError(path)


class PlanTests(unittest.TestCase):
    def test_valid_plan_is_normalised(self):
        loaded = reseed.load_plan(plan(targetNodeId=3, newNodeId=4), RUN)
        self.assertEqual((loaded["targetNodeId"], loaded["newNodeId"]), ("3", "4"))
        self.assertEqual(loaded["durableIntervalSeconds"], reseed.DEFAULT_STREAM_INTERVAL_SECONDS)

    def test_stream_settings_are_bounded_by_the_per_session_limit(self):
        loaded = reseed.load_plan(plan(durableIntervalSeconds=10, durableMaxMessages=40), RUN)
        self.assertEqual(reseed_main.durable_stream.plan_settings(loaded), (10, 40))
        self.assertEqual(reseed_main.durable_stream.plan_settings(reseed.load_plan(plan(), RUN)), (25.0, 60))
        for bad in ({"durableMaxMessages": 61}, {"durableMaxMessages": 0}, {"durableIntervalSeconds": 0.1},
                    {"durableIntervalSeconds": 31}, {"durableIntervalSeconds": True}):
            with self.subTest(bad), self.assertRaises(reseed.PlanError):
                reseed.load_plan(plan(**bad), RUN)

    def test_invalid_plans_are_rejected(self):
        cases = {"run": plan(runId="other"), "cluster": plan(clusterName="prod"), "pod": plan(targetPod="narumq-1"),
                 "pvc": plan(pvcName="data-narumq-1"), "old": plan(newNodeId="3"), "lower": plan(newNodeId="2"),
                 "leading zero": plan(newNodeId="04"), "bool": plan(newNodeId=True), "ordinal": plan(slotOrdinal=3),
                 "scenario": plan(durableScenario="Bad Name"), "observer": plan(observerPod="narumq-2"),
                 "spare": plan(spareWorkerName="worker-old"), "observer worker": plan(observerWorkerName="worker-old")}
        for name, value in cases.items():
            with self.subTest(name), self.assertRaises(reseed.PlanError):
                reseed.load_plan(value, RUN)
        incomplete = plan()
        del incomplete["volumeId"]
        with self.assertRaises(reseed.PlanError):
            reseed.load_plan(incomplete, RUN)


class PreflightTests(unittest.TestCase):
    def run_preflight(self, **changes):
        fixture = FakePreflightFixture(**changes)
        return reseed.run_preflight(fixture, plan())

    def assertRejected(self, code, **changes):
        with self.assertRaises(reseed.PreflightRejected) as caught:
            self.run_preflight(**changes)
        self.assertEqual(caught.exception.code, code)

    def test_healthy_fixture_passes_and_reports_joiner_seeds(self):
        evidence, context = self.run_preflight()
        self.assertEqual(evidence["members"]["voters"], ["1", "2", "3"])
        self.assertEqual(evidence["joinerSeedAddresses"], [reseed.slot_address(0), reseed.slot_address(1)])
        self.assertEqual(context["survivors"], ["1", "2"])
        self.assertEqual(context["releaseVersion"], "1.2.0")
        self.assertEqual(context["spareNodeUID"], "spare-uid")

    def test_slot_mismatch_is_rejected(self):
        self.assertRejected("slot_mismatch", configmap=configmap(slot2="9"))

    def test_leader_target_is_rejected(self):
        rows = [node_row("1"), node_row("2"), node_row("3", role="LEADER")]
        self.assertRejected("target_is_leader", nodes=rows,
                            cluster={"clusterId": RUN, "quorumAvailable": True, "leaderNodeId": "3"})

    def test_new_id_reused_or_below_a_retired_id_is_rejected(self):
        rows = three_voters() + [node_row("5", phase="RETIRED", role=None, status="RETIRED")]
        slots = {0: "1", 1: "2", 2: "3"}
        with self.assertRaises(reseed.PreflightRejected) as retired:
            reseed.check_members(rows, slots, plan(newNodeId="5"), "1")
        self.assertEqual(retired.exception.code, "new_id_in_use")
        with self.assertRaises(reseed.PreflightRejected) as below:
            reseed.check_members(rows, slots, plan(newNodeId="4"), "1")
        self.assertEqual(below.exception.code, "new_id_not_above_committed_ids")
        self.assertEqual(reseed.check_members(rows, slots, plan(newNodeId="6"), "1")["retired"], ["5"])

    def test_not_exactly_three_voters_is_rejected(self):
        self.assertRejected("not_three_voters_on_slots", nodes=[node_row("1", role="LEADER"), node_row("2")])
        learner = three_voters() + [node_row("4", phase="LEARNER", role="LEARNER")]
        self.assertRejected("not_three_voters_on_slots", nodes=learner)

    def test_unreachable_voter_is_rejected(self):
        rows = three_voters()
        rows[1] = node_row("2", status="UNREACHABLE")
        self.assertRejected("voter_not_observed", nodes=rows)

    def test_missing_or_unsuitable_spare_is_rejected(self):
        with self.assertRaises(reseed.PreflightRejected) as missing:
            reseed.check_spare(None, "running", [], plan())
        self.assertEqual(missing.exception.code, "spare_missing")
        fixture = FakePreflightFixture()
        fixture.get_node = lambda name: None if name == "worker-spare" else FakePreflightFixture.get_node(fixture, name)
        with self.assertRaises(reseed.PreflightRejected) as absent:
            reseed.run_preflight(fixture, plan())
        self.assertEqual(absent.exception.code, "spare_missing")
        self.assertRejected("spare_hosts_broker", extra_pod_on_spare=True)
        self.assertRejected("spare_not_ready", spare_state="stopped")
        other_zone = copy.deepcopy(FakePreflightFixture().state["spare"])
        other_zone["metadata"]["labels"]["topology.kubernetes.io/zone"] = "ap-northeast-2c"
        self.assertRejected("spare_zone_mismatch", spare=other_zone)
        cordoned = copy.deepcopy(FakePreflightFixture().state["spare"])
        cordoned["spec"]["unschedulable"] = True
        self.assertRejected("spare_not_ready", spare=cordoned)

    def test_volume_deleted_on_termination_is_rejected(self):
        self.assertRejected("volume_deleted_on_termination", delete_on_termination=True)

    def test_other_account_or_quorum_loss_is_rejected(self):
        fixture = FakePreflightFixture()
        fixture.aws = lambda *args: {"Account": "111111111111"} if args[0] == "sts" else {}
        with self.assertRaises(reseed.PreflightRejected) as caught:
            reseed.run_preflight(fixture, plan())
        self.assertEqual(caught.exception.code, "account_mismatch")
        self.assertRejected("cluster_not_owned_or_no_quorum",
                            cluster={"clusterId": RUN, "quorumAvailable": False, "leaderNodeId": "1"})

    def test_release_version_must_match_across_survivors(self):
        features = {"members": [{"nodeId": "1", "releaseVersion": "1.2.0"}, {"nodeId": "2", "releaseVersion": "1.3.0"}]}
        with self.assertRaises(reseed.PreflightRejected):
            reseed.release_version(features, ["1", "2"])


class GuardTests(unittest.TestCase):
    LOG = "starting\nlevel=error msg=\"cluster bootstrap refused: data directory is empty\"\n"

    @staticmethod
    def status(exit_code=None, restarts=0, ready_state=False, current=False):
        terminated = None if exit_code is None else {"exitCode": exit_code, "reason": "Error",
                                                      "finishedAt": "2026-10-16T00:00:00Z"}
        state = {"terminated": terminated} if current and terminated else {"waiting": {"reason": "CrashLoopBackOff"}}
        last = {} if current or terminated is None else {"terminated": terminated}
        return {"name": "narumq", "ready": ready_state, "restartCount": restarts, "state": state, "lastState": last}

    def test_exit_one_with_refusal_message_is_refused(self):
        for current in (True, False):
            result = reseed.classify_guard(self.status(1, restarts=0 if current else 1, current=current), self.LOG)
            self.assertEqual(result["verdict"], "refused")
            self.assertIn("cluster bootstrap refused", result["logLine"])
            self.assertEqual(result["exitCode"], 1)
            self.assertEqual(result["logSha256"], hashlib.sha256(self.LOG.encode()).hexdigest())

    def test_other_exit_codes_missing_message_or_ready_fail(self):
        for code in (0, 2, 137):
            self.assertEqual(reseed.classify_guard(self.status(code, current=True), self.LOG)["verdict"], "fail")
        self.assertEqual(reseed.classify_guard(self.status(1, current=True), "panic: other")["reason"],
                         "refusal_message_absent")
        self.assertEqual(reseed.classify_guard(self.status(ready_state=True), self.LOG)["reason"], "container_became_ready")

    def test_running_or_missing_logs_keep_waiting(self):
        self.assertEqual(reseed.classify_guard(None, None)["verdict"], "waiting")
        self.assertEqual(reseed.classify_guard(self.status(), None)["verdict"], "waiting")
        self.assertEqual(reseed.classify_guard(self.status(1, current=True), None)["reason"], "logs_unavailable")

    class Fixture:
        kubectl = ["kubectl"]

        def __init__(self, pods, logs):
            self.pods = list(pods)
            self.logs = logs
            self.commands: list = []

        def get_pod(self, name):
            return self.pods.pop(0) if len(self.pods) > 1 else self.pods[0]

        def command(self, args, timeout=90, stdin=None):
            self.commands.append(args)
            if self.logs is None:
                raise RuntimeError("no logs")
            return self.logs

    def guard_pod(self, uid, status):
        value = pod("narumq-2", uid, "worker-spare", ready_state=False)
        value["status"]["containerStatuses"] = [status] if status else []
        value["spec"]["volumes"] = [{"name": "data", "persistentVolumeClaim": {"claimName": "data-narumq-2"}}]
        return value

    def watch(self, pods, logs, timeout=30):
        clock = iter(range(0, 1000, 5))
        return reseed.watch_guard(self.Fixture(pods, logs), plan(), timeout=timeout, interval=0,
                                  sleep=lambda s: None, monotonic=lambda: next(clock))

    def test_watch_ignores_old_pod_then_reports_refusal_with_previous_logs(self):
        fixture = self.Fixture([self.guard_pod("pod-old", None), self.guard_pod("pod-new", None),
                                self.guard_pod("pod-new", self.status(1, restarts=1))], self.LOG)
        clock = iter(range(0, 1000, 1))
        result = reseed.watch_guard(fixture, plan(), timeout=60, interval=0, sleep=lambda s: None,
                                    monotonic=lambda: next(clock))
        self.assertEqual(result["verdict"], "refused")
        self.assertEqual(result["podUID"], "pod-new")
        self.assertEqual(result["restartCount"], 1)
        self.assertEqual(result["claims"], ["data-narumq-2"])
        self.assertIn("--previous", fixture.commands[0])

    def test_watch_fails_on_another_exit_code_and_times_out_without_termination(self):
        result = self.watch([self.guard_pod("pod-new", self.status(2, current=True))], self.LOG)
        self.assertEqual((result["verdict"], result["reason"]), ("fail", "unexpected_exit_code"))
        timed_out = self.watch([self.guard_pod("pod-new", None)], self.LOG, timeout=10)
        self.assertEqual(timed_out["verdict"], "timeout")


class ConfigTests(unittest.TestCase):
    def setUp(self):
        self.slots = {0: "1", 1: "2", 2: "3"}
        self.old = bootstrap_config(3, 2)

    def test_joiner_config_derivation(self):
        original = copy.deepcopy(self.old)
        joiner = reseed.derive_joiner_config(self.old, plan(), self.slots)
        self.assertEqual(self.old, original)
        self.assertEqual((joiner["nodeId"], joiner["role"], joiner["bootstrapMembers"]), (4, "joiner", []))
        self.assertEqual(joiner["seedAddresses"], [reseed.slot_address(0), reseed.slot_address(1)])
        for key in ("advertisedAddress", "localDnsName", "listenAddress", "peerTLS", "management", "mqtt", "clusterId",
                    "clusterProtocolMajor", "stateSchemaMinor", "directory"):
            self.assertEqual(joiner[key], self.old[key], key)
        self.assertEqual(set(joiner), set(self.old))

    def test_joiner_config_for_other_slots_seeds_the_other_two(self):
        joiner = reseed.derive_joiner_config(bootstrap_config(1, 0), plan(slotOrdinal=0, targetPod="narumq-0",
                                                                           pvcName="data-narumq-0", targetNodeId="1"),
                                             self.slots)
        self.assertEqual(joiner["seedAddresses"], [reseed.slot_address(1), reseed.slot_address(2)])

    def test_joiner_config_rejects_foreign_address_or_id(self):
        for key, value in (("advertisedAddress", "other:7000"), ("localDnsName", "other"), ("nodeId", 9)):
            broken = {**self.old, key: value}
            with self.subTest(key), self.assertRaises(reseed.PreflightRejected):
                reseed.derive_joiner_config(broken, plan(), self.slots)

    def test_configmap_edits(self):
        original = configmap()
        joiner_text = reseed.config_text(reseed.derive_joiner_config(self.old, plan(), self.slots))
        updated = reseed.configmap_with_reseed(original, plan(), joiner_text)
        self.assertEqual(updated["data"]["slot-2"], "4")
        self.assertEqual(json.loads(updated["data"]["node-4.json"])["role"], "joiner")
        self.assertIn("node-3.json", updated["data"])
        self.assertEqual(original, configmap())
        cleaned = reseed.configmap_without_old(updated, plan())
        self.assertNotIn("node-3.json", cleaned["data"])
        self.assertEqual(cleaned["data"]["slot-2"], "4")
        with self.assertRaises(reseed.PreflightRejected):
            reseed.configmap_with_reseed(updated, plan(), joiner_text)
        with self.assertRaises(reseed.PreflightRejected):
            reseed.configmap_without_old(original, plan())

    def test_secret_replacement_keeps_other_keys_and_returns_previous_values(self):
        secret = {"data": {"node-2.crt": "b2xk", "node-2.key": "b2xrZXk=", "node-1.crt": "a2VlcA==", "ca.crt": "Y2E="}}
        updated, previous = reseed.secret_with_certificate(secret, 2, b"CERT", b"KEY")
        self.assertEqual(previous, {"node-2.crt": "b2xk", "node-2.key": "b2xrZXk="})
        self.assertEqual(base64.b64decode(updated["data"]["node-2.crt"]), b"CERT")
        self.assertEqual(updated["data"]["node-1.crt"], "a2VlcA==")
        with self.assertRaises(reseed.PreflightRejected):
            reseed.secret_with_certificate({"data": {}}, 2, b"C", b"K")

    def test_stage_body_copies_release_and_protocol_from_existing_records(self):
        body = reseed.stage_body(plan(), "4", "ab" * 32, {"oldConfig": self.old, "releaseVersion": "1.2.0"})
        self.assertEqual(body, {"memberId": 4, "address": reseed.slot_address(2), "dnsSan": DNS[2],
                                "certificateSha256": "ab" * 32, "releaseVersion": "1.2.0", "clusterProtocolMajor": 2,
                                "clusterProtocolMinor": 0, "stateSchemaMajor": 1, "stateSchemaMinor": 0})

    def test_idempotency_key_is_printable_ascii_up_to_128_bytes(self):
        self.assertEqual(reseed.idempotency_key("run", "stage", "4"), "run-stage-4")
        long_key = reseed.idempotency_key("x" * 200, "stage", "4")
        self.assertLessEqual(len(long_key), 128)
        self.assertRegex(long_key, r"^[!-~]+$")


class CertificateTests(unittest.TestCase):
    @staticmethod
    def authority():
        from cryptography import x509
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import ec
        from cryptography.x509.oid import NameOID
        key = ec.generate_private_key(ec.SECP256R1())
        name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "fixture peer CA")])
        now = datetime.datetime.now(datetime.timezone.utc)
        cert = (x509.CertificateBuilder().subject_name(name).issuer_name(name).public_key(key.public_key())
                .serial_number(1).not_valid_before(now - datetime.timedelta(hours=1))
                .not_valid_after(now + datetime.timedelta(days=1))
                .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
                .sign(key, hashes.SHA256()))
        return (cert.public_bytes(serialization.Encoding.PEM),
                key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                                  serialization.NoEncryption()))

    def test_certificate_san_eku_signature_and_fingerprint(self):
        from cryptography import x509
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import ec
        from cryptography.x509.oid import ExtendedKeyUsageOID
        ca_pem, ca_key = self.authority()
        material = reseed.generate_peer_certificate(ca_pem, ca_key, DNS[2])
        leaf = x509.load_pem_x509_certificate(material["certificatePem"])
        san = leaf.extensions.get_extension_for_class(x509.SubjectAlternativeName).value
        self.assertEqual(san.get_values_for_type(x509.DNSName), [DNS[2]])
        self.assertEqual(len(list(san)), 1)
        eku = leaf.extensions.get_extension_for_class(x509.ExtendedKeyUsage).value
        self.assertEqual(set(eku), {ExtendedKeyUsageOID.SERVER_AUTH, ExtendedKeyUsageOID.CLIENT_AUTH})
        self.assertFalse(leaf.extensions.get_extension_for_class(x509.BasicConstraints).value.ca)
        self.assertEqual(material["sha256"], hashlib.sha256(leaf.public_bytes(serialization.Encoding.DER)).hexdigest())
        self.assertIsInstance(leaf.public_key(), ec.EllipticCurvePublicKey)
        self.assertEqual(leaf.public_key().curve.name, "secp256r1")
        ca = x509.load_pem_x509_certificate(ca_pem)
        ca.public_key().verify(leaf.signature, leaf.tbs_certificate_bytes, ec.ECDSA(leaf.signature_hash_algorithm))
        self.assertEqual(leaf.issuer, ca.subject)
        key = serialization.load_pem_private_key(material["keyPem"], password=None)
        self.assertEqual(key.public_key().public_numbers(), leaf.public_key().public_numbers())
        self.assertLess(leaf.not_valid_before_utc, datetime.datetime.now(datetime.timezone.utc))
        self.assertGreater(leaf.not_valid_after_utc, datetime.datetime.now(datetime.timezone.utc))

    def test_each_certificate_has_a_fresh_key_and_fingerprint(self):
        ca_pem, ca_key = self.authority()
        first = reseed.generate_peer_certificate(ca_pem, ca_key, DNS[2])
        second = reseed.generate_peer_certificate(ca_pem, ca_key, DNS[2])
        self.assertNotEqual(first["sha256"], second["sha256"])
        self.assertNotEqual(first["keyPem"], second["keyPem"])

    def test_key_material_stays_in_the_private_directory(self):
        ca_pem, ca_key = self.authority()
        material = reseed.generate_peer_certificate(ca_pem, ca_key, DNS[2])
        with tempfile.TemporaryDirectory() as private_text, tempfile.TemporaryDirectory() as output_text:
            private, output = Path(private_text), Path(output_text)
            names = reseed.write_private_material(private, plan(), material, {"node-2.crt": "old", "node-2.key": "oldkey"})
            self.assertEqual((private / names["key"]).read_bytes(), material["keyPem"])
            self.assertEqual(list(output.iterdir()), [])
            with self.assertRaises(FileExistsError):
                reseed.write_private_material(private, plan(), material, {})


class JudgementTests(unittest.TestCase):
    def test_aggregate_requires_every_criterion(self):
        criteria = {name: True for name in reseed.CRITERIA}
        self.assertEqual(reseed.aggregate(criteria)["result"], "PASS")
        for name in reseed.CRITERIA:
            with self.subTest(name):
                broken = {**criteria, name: False}
                judged = reseed.aggregate(broken)
                self.assertEqual((judged["result"], judged["failedCriteria"]), ("FAIL", [name]))
        missing = dict(criteria)
        del missing["durable.streamOracle"]
        self.assertEqual(reseed.aggregate(missing)["failedCriteria"], ["durable.streamOracle"])
        self.assertEqual(reseed.aggregate({**criteria, "guard.twoReadyBrokers": 1})["result"], "FAIL")
        self.assertEqual(reseed.aggregate(criteria, failed_step="verify")["result"], "FAIL")
        self.assertEqual(reseed.aggregate({})["result"], "FAIL")

    @staticmethod
    def sample(offset: int, ok=True, field_ok=True, stream="management"):
        started = datetime.datetime(2026, 10, 16, 0, 0, offset, tzinfo=datetime.timezone.utc).isoformat()
        field = "quorumAvailable" if stream == "management" else "puback"
        return {"startedAt": started, "ok": ok, **({"data": {field: field_ok}} if ok else {"errorCode": "URLError"})}

    def window(self, **changes):
        samples = {"management": [self.sample(i) for i in range(0, 40, 2)],
                   "mqtt": [self.sample(i, stream="mqtt") for i in range(0, 40, 2)]}
        samples.update(changes)
        start = datetime.datetime(2026, 10, 16, 0, 0, 10, tzinfo=datetime.timezone.utc).isoformat()
        end = datetime.datetime(2026, 10, 16, 0, 0, 30, tzinfo=datetime.timezone.utc).isoformat()
        return reseed.window_judgment(samples, start, end)

    def test_window_passes_only_when_every_sample_inside_succeeds(self):
        judged = self.window()
        self.assertEqual(judged["result"], "PASS")
        self.assertEqual(judged["streams"]["management"]["samples"], 11)
        outside = [self.sample(i) for i in range(0, 40, 2)]
        outside[1] = self.sample(2, ok=False)
        self.assertEqual(self.window(management=outside)["result"], "PASS")
        failed_inside = [self.sample(i) for i in range(0, 40, 2)]
        failed_inside[8] = self.sample(16, ok=False)
        self.assertEqual(self.window(management=failed_inside)["result"], "FAIL")
        no_quorum = [self.sample(i, field_ok=(i != 20)) for i in range(0, 40, 2)]
        self.assertEqual(self.window(management=no_quorum)["result"], "FAIL")
        refused = [self.sample(i, field_ok=(i != 14), stream="mqtt") for i in range(0, 40, 2)]
        self.assertEqual(self.window(mqtt=refused)["result"], "FAIL")
        self.assertEqual(self.window(mqtt=[])["result"], "FAIL")

    def test_journal_counts(self):
        entries = [{"seq": 1, "sentAt": "2026-10-16T00:00:01+00:00", "outcome": "acked"},
                   {"seq": 2, "sentAt": "2026-10-16T00:00:05+00:00", "outcome": "rejected"},
                   {"seq": 3, "sentAt": "2026-10-16T00:00:09+00:00", "outcome": "unknown"}]
        self.assertEqual(reseed.journal_counts(entries), {"acked": 1, "rejected": 1, "unknown": 1})
        self.assertEqual(reseed.journal_counts(entries, "2026-10-16T00:00:04+00:00", "2026-10-16T00:00:10+00:00"),
                         {"acked": 0, "rejected": 1, "unknown": 1})


class MutationHelperTests(unittest.TestCase):
    class Fixture:
        kubectl = ["kubectl", "--context", RUN]

        def __init__(self, error=None):
            self.error = error
            self.calls: list = []
            self.patches: list = []

        def command(self, args, timeout=90, stdin=None):
            self.calls.append((args, stdin))
            if self.error:
                raise self.error
            return ""

        def patch_node(self, name, operations):
            self.patches.append((name, operations))

    def test_delete_sends_uid_precondition_once(self):
        fixture = self.Fixture()
        reseed.delete_with_uid(fixture, "/api/v1/namespaces/narumq/persistentvolumeclaims/data-narumq-2", "pvc-old")
        args, stdin = fixture.calls[0]
        self.assertEqual(args[-4:], ["--raw", "/api/v1/namespaces/narumq/persistentvolumeclaims/data-narumq-2", "-f", "-"])
        self.assertEqual(json.loads(stdin)["preconditions"], {"uid": "pvc-old"})
        self.assertEqual(len(fixture.calls), 1)

    def test_failed_commands_become_mutation_rejected(self):
        for error in (RuntimeError("command exit 1"), subprocess.TimeoutExpired("kubectl", 1)):
            with self.subTest(type(error).__name__):
                with self.assertRaises(reseed.MutationRejected):
                    reseed.delete_with_uid(self.Fixture(error), "/api/v1/x", "uid")
                with self.assertRaises(reseed.MutationRejected):
                    reseed.replace_object(self.Fixture(error), {"kind": "Secret"})

    def test_replace_passes_the_object_on_stdin_not_argv(self):
        fixture = self.Fixture()
        reseed.replace_object(fixture, {"kind": "Secret", "data": {"node-2.key": "c2VjcmV0"}})
        args, stdin = fixture.calls[0]
        self.assertNotIn("c2VjcmV0", " ".join(args))
        self.assertIn("c2VjcmV0", stdin)

    def test_cordon_is_identity_guarded(self):
        fixture = self.Fixture()
        reseed.set_unschedulable(fixture, "worker-spare", "spare-uid", True)
        name, operations = fixture.patches[0]
        self.assertEqual(name, "worker-spare")
        self.assertEqual(operations[0], {"op": "test", "path": "/metadata/uid", "value": "spare-uid"})
        self.assertEqual(operations[1]["value"], True)

    def test_wait_until_returns_or_raises_without_real_sleep(self):
        ticks = iter(range(100))
        sleeps: list = []
        values = iter([None, None, {"ok": True}])
        result = reseed.wait_until(lambda: next(values), 30, 3.0, sleeps.append, lambda: next(ticks))
        self.assertEqual((result, sleeps), ({"ok": True}, [3.0, 3.0]))
        with self.assertRaises(TimeoutError):
            reseed.wait_until(lambda: None, 3, 1.0, lambda s: None, lambda: next(ticks) * 2)


class MarkerTests(unittest.TestCase):
    class Client:
        def __init__(self, identity, password, port, clean, expiry, *, acks=None, retained=None):
            self.identity = identity
            self.password = password
            self.port = port
            self.acks = acks if acks is not None else []
            self.messages = [] if retained is None else retained
            self.published: list = []
            self.closed = False

            class Event:
                def wait(inner, timeout):
                    return bool(self.messages)
            self.received = Event()

        def publish(self, topic, payload, retain=False):
            self.published.append((topic, payload, retain))
            self.acks.append({"mid": 1, "reason": "Success", "reasonValue": 0})

        def subscribe(self, topic):
            self.topic = topic

        def close(self):
            self.closed = True

    def test_marker_publish_and_read_report_the_sha256_only(self):
        with tempfile.TemporaryDirectory() as text:
            private = Path(text)
            (private / "mqtt.password").write_text("secret-password\n", encoding="utf-8")
            clients: list = []

            def factory(*args):
                client = self.Client(*args)
                clients.append(client)
                return client

            published = mqtt_helpers.publish_marker(private, RUN, "reseed", 23883, factory)
            payload = mqtt_helpers.marker_payload(RUN, "reseed")
            self.assertEqual(published["sha256"], hashlib.sha256(payload).hexdigest())
            self.assertEqual(clients[0].published, [(f"acceptance/{RUN}/reseed/retained", payload, True)])
            self.assertEqual(clients[0].identity, "ebs-worker-publisher")
            self.assertNotIn("secret-password", json.dumps(published))
            digest = hashlib.sha256(payload).hexdigest()
            message = {"topic": f"acceptance/{RUN}/reseed/retained", "sha256": digest, "retain": True}
            found = mqtt_helpers.read_marker(private, RUN, 24883, lambda *a: self.Client(*a, retained=[message]))
            self.assertEqual(found["sha256"], digest)
            absent = mqtt_helpers.read_marker(private, RUN, 24883, lambda *a: self.Client(*a))
            self.assertEqual(absent, {"found": False, "sha256": None})

    def test_refused_puback_is_an_error(self):
        with tempfile.TemporaryDirectory() as text:
            private = Path(text)
            (private / "mqtt.password").write_text("x", encoding="utf-8")

            class Refusing(self.Client):
                def publish(self, topic, payload, retain=False):
                    self.acks.append({"mid": 1, "reason": "Quota", "reasonValue": 0x97})

            with self.assertRaises(AssertionError):
                mqtt_helpers.publish_marker(private, RUN, "reseed", 23883, Refusing)


class ClusterSimulation:
    """Stateful stand-in for the EKS fixture used by the end-to-end reseed orchestration test."""
    kubectl = ["kubectl"]

    def __init__(self):
        self.configmap = configmap()
        self.secret = {"kind": "Secret", "metadata": {"name": reseed.SECRET}, "data": {
            "node-0.crt": "MA==", "node-0.key": "MA==", "node-1.crt": "MQ==", "node-1.key": "MQ==",
            "node-2.crt": base64.b64encode(b"old-crt").decode(), "node-2.key": base64.b64encode(b"old-key").decode(),
            "ca.crt": "Q0E="}}
        self.pod_uid = "pod-guard"
        self.recreated = False
        self.removed = False
        self.replaced: list[str] = []
        self.requests: list = []
        self.deleted: list = []

    def rows(self) -> list[dict]:
        rows = [node_row("1", role="LEADER", lastAppliedIndex=150), node_row("2", lastAppliedIndex=150)]
        rows.append(node_row("3", phase="RETIRED", role=None, status="RETIRED") if self.removed
                    else node_row("3", status="UNREACHABLE"))
        if self.requests:
            rows.append(node_row("4", lastAppliedIndex=160) if self.recreated
                        else node_row("4", phase="STAGED", role=None, status="UNREACHABLE"))
        return rows

    def api(self, path, port=23880):
        if path == "/api/v1/cluster":
            return {"clusterId": RUN, "quorumAvailable": True, "leaderNodeId": "1",
                    "nodesByState": {"ready": 3 if self.recreated else 2}}
        if path.startswith("/api/v1/nodes"):
            return self.rows()
        raise AssertionError(path)

    def api_request(self, method, path, body=None, port=23880, idempotency_key=None):
        if method == "GET" and "/membership-operations/" in path:
            phase = "SUCCEEDED" if (self.recreated or path.endswith("op-remove")) else "STAGED"
            return 200, {"data": {"operationId": path.rsplit("/", 1)[1], "action": "ADD", "memberId": 4, "phase": phase,
                                  "committedIndex": 1}}
        self.requests.append((method, path, body, port, idempotency_key))
        if method == "POST" and body["memberId"] == 3:
            return 409, {"error": {"code": "member_id_reserved", "message": "reserved"}}
        if method == "POST":
            return 202, {"data": {"operationId": "op-stage", "action": "ADD", "memberId": body["memberId"],
                                  "phase": "STAGED", "committedIndex": 41}}
        self.removed = True
        return 202, {"data": {"operationId": "op-remove", "action": "REMOVE", "memberId": 3, "phase": "REMOVING",
                              "committedIndex": 60}}

    def get(self, *args):
        if args[0] == "configmap":
            return copy.deepcopy(self.configmap)
        if args[0] == "secret":
            return copy.deepcopy(self.secret)
        if args[0] == "pods":
            return {"items": [pod("narumq-0", "pod-observer", "w0"), pod("narumq-1", "pod-1", "w1"),
                              pod("narumq-2", "pod-new", "w2")]} if self.recreated else \
                {"items": [pod("narumq-0", "pod-observer", "w0"), pod("narumq-1", "pod-1", "w1")]}
        raise AssertionError(args)

    def command(self, args, timeout=90, stdin=None):
        assert "replace" in args
        obj = json.loads(stdin)
        self.replaced.append(obj["kind"])
        if obj["kind"] == "ConfigMap":
            self.configmap = obj
        else:
            self.secret = obj
        return ""

    def get_pod(self, name):
        return pod(name, "pod-new" if self.recreated else self.pod_uid, "w2")

    def delete_pod(self, name, uid):
        assert uid == self.pod_uid
        self.deleted.append((name, uid))
        self.recreated = True


class FakeTunnel:
    created: list = []

    def __init__(self, fixture, pod_name, uid, management_port, mqtt_port):
        self.args = (pod_name, uid, management_port, mqtt_port)
        FakeTunnel.created.append(self.args)

    def start(self):
        return {"pod": self.args[0], "podUID": self.args[1]}

    def check(self):
        pass

    def close(self):
        pass


class OrchestrationTests(unittest.TestCase):
    def build(self, directory: Path):
        private, output = directory / "private", directory / "output"
        private.mkdir()
        output.mkdir()
        ca_pem, ca_key = CertificateTests.authority()
        simulation = ClusterSimulation()
        run = reseed_main.Run(simulation, reseed.load_plan(plan(), RUN), output, private, sleep=lambda s: None)
        slots = {0: "1", 1: "2", 2: "3"}
        run.context.update(slots=slots, survivors=["1", "2"], oldConfig=bootstrap_config(3, 2),
                           joinerConfig=reseed.derive_joiner_config(bootstrap_config(3, 2), plan(), slots),
                           releaseVersion="1.2.0", ca=(ca_pem, ca_key), guardPodUID="pod-guard", spareNodeUID="spare-uid")
        run.baseline.update(appliedIndex=150, markerSha256="m" * 64)
        return run, simulation, private, output

    def test_reseed_applies_stage_config_secret_pod_and_removal_in_order(self):
        with tempfile.TemporaryDirectory() as text:
            run, simulation, private, output = self.build(Path(text))
            run.step_reseed()
            self.assertEqual([r[:2] for r in simulation.requests],
                             [("POST", "/api/v1/cluster/members"), ("DELETE", "/api/v1/cluster/members/3")])
            stage_body = simulation.requests[0][2]
            self.assertEqual((stage_body["memberId"], stage_body["dnsSan"], stage_body["address"]),
                             (4, DNS[2], reseed.slot_address(2)))
            self.assertEqual(simulation.replaced, ["ConfigMap", "Secret", "ConfigMap"])
            self.assertEqual(simulation.configmap["data"]["slot-2"], "4")
            self.assertNotIn("node-3.json", simulation.configmap["data"])
            self.assertEqual(json.loads(simulation.configmap["data"]["node-4.json"])["role"], "joiner")
            self.assertEqual(simulation.deleted, [("narumq-2", "pod-guard")])
            self.assertTrue(run.criteria["reseed.newIdVoter"] and run.criteria["reseed.oldIdRetired"])
            # stage certificate hash is the hash of the certificate placed in the Secret
            installed = base64.b64decode(simulation.secret["data"]["node-2.crt"])
            from cryptography import x509
            from cryptography.hazmat.primitives import serialization
            der = x509.load_pem_x509_certificate(installed).public_bytes(serialization.Encoding.DER)
            self.assertEqual(stage_body["certificateSha256"], hashlib.sha256(der).hexdigest())
            # key material exists only in the private directory
            key_pem = base64.b64decode(simulation.secret["data"]["node-2.key"])
            self.assertTrue(any((private / name).read_bytes() == key_pem for name in
                                (p.name for p in private.iterdir())))
            leaked = [p.name for p in output.rglob("*") if p.is_file() and
                      (key_pem in p.read_bytes() or simulation.secret["data"]["node-2.key"].encode() in p.read_bytes()
                       or b"PRIVATE KEY" in p.read_bytes() or b"old-key" in p.read_bytes()
                       or base64.b64encode(b"old-key") in p.read_bytes())]
            self.assertEqual(leaked, [])

    def test_failed_configmap_write_that_did_not_apply_stops_the_run(self):
        with tempfile.TemporaryDirectory() as text:
            run, simulation, _, _ = self.build(Path(text))

            def failing(args, timeout=90, stdin=None):
                raise RuntimeError("command exit 1")
            simulation.command = failing
            with self.assertRaises(reseed_main.StepFailed) as caught:
                run.step_reseed()
            self.assertEqual(caught.exception.code, "configMapReseed_not_applied")
            self.assertEqual(simulation.deleted, [])
            self.assertEqual(len(simulation.requests), 1)

    def test_verify_sets_durable_criteria(self):
        with tempfile.TemporaryDirectory() as text:
            run, simulation, _, output = self.build(Path(text))
            run.step_reseed()
            run.context["newPodUID"] = "pod-new"
            run.certificate = {"sha256": "c" * 64}

            class Publisher:
                def stop(self):
                    return {"acked": 30, "rejected": 0, "unknown": 1, "connectionLost": False}

            class Monitor:
                def stop(self):
                    pass

            run.publisher, run.monitor = Publisher(), Monitor()
            FakeTunnel.created.clear()
            durable = {"result": "PASS", "label": "after-reseed", "sessionPresent": True, "acked": 30, "rejected": 0,
                       "unknown": 1, "missingAcked": [], "rejectedDelivered": [], "unknownDelivered": [],
                       "duplicates": 0}
            with patch.object(reseed_main, "PodTunnel", FakeTunnel), \
                    patch.object(reseed_main.durable_stream, "verify_subscriber", return_value=durable) as verify, \
                    patch.object(reseed_main.mqtt_helpers, "read_marker",
                                 return_value={"found": True, "sha256": "m" * 64}):
                run.step_verify()
            self.assertEqual(FakeTunnel.created, [("narumq-2", "pod-new", 24880, 24883)])
            self.assertEqual(verify.call_args.args[-3:], (24883, output, "after-reseed"))
            run.criteria.update({"loss.instanceTerminated": True, "loss.volumeDeleted": True, "loss.pvcRecreated": True,
                                 "guard.exit1AndRefusalMessage": True, "guard.survivorSamplesAllOk": True,
                                 "guard.twoReadyBrokers": True})
            judged = reseed.aggregate(run.criteria)
            self.assertEqual((judged["result"], judged["failedCriteria"]), ("PASS", []))
            self.assertEqual(simulation.requests[-1][0:2], ("POST", "/api/v1/cluster/members"))
            self.assertEqual(simulation.requests[-1][2]["memberId"], 3)

    def test_verify_fails_on_low_applied_index_or_marker_mismatch(self):
        with tempfile.TemporaryDirectory() as text:
            run, simulation, _, _ = self.build(Path(text))
            run.step_reseed()
            run.context["newPodUID"] = "pod-new"
            run.certificate = {"sha256": "c" * 64}
            run.baseline["appliedIndex"] = 161

            class Stub:
                def stop(self):
                    return {"acked": 5}

            run.publisher = run.monitor = Stub()
            with patch.object(reseed_main, "PodTunnel", FakeTunnel), \
                    patch.object(reseed_main.durable_stream, "verify_subscriber",
                                 return_value={"result": "FAIL", "missingAcked": [3]}), \
                    patch.object(reseed_main.mqtt_helpers, "read_marker", return_value={"found": True, "sha256": "x" * 64}):
                run.step_verify()
            self.assertFalse(run.criteria["durable.appliedIndexAtLeastBaseline"])
            self.assertFalse(run.criteria["durable.streamOracle"])
            self.assertFalse(run.criteria["durable.retainedMarkerSha256"])

    def test_guard_step_judges_window_pods_and_exit(self):
        with tempfile.TemporaryDirectory() as text:
            run, simulation, _, _ = self.build(Path(text))
            run.fault_requested_at = "2026-10-16T00:00:10+00:00"
            samples = {"management": [JudgementTests.sample(i) for i in range(10, 60, 2)],
                       "mqtt": [JudgementTests.sample(i, stream="mqtt") for i in range(10, 60, 2)]}

            class Monitor:
                def snapshot(self):
                    return samples

            run.monitor = Monitor()
            refusal = {"verdict": "refused", "reason": "exit_1_with_refusal", "podUID": "pod-guard",
                       "claims": ["data-narumq-2"], "restartCount": 1, "exitCode": 1, "observations": []}
            fresh = {"metadata": {"uid": "pvc-fresh"}}
            simulation.get = lambda *args: fresh if args[0] == "pvc" else ClusterSimulation.get(simulation, *args)
            with patch.object(reseed, "watch_guard", return_value=refusal), \
                    patch.object(reseed_main, "pods_of", return_value=[pod("narumq-0", "a", "w0"), pod("narumq-1", "b", "w1"),
                                                                       pod("narumq-2", "pod-guard", "w2", False)]):
                simulation.api = lambda path, port=23880: {"quorumAvailable": True, "nodesByState": {"ready": 2}}
                run.step_guard()
            self.assertTrue(all(run.criteria[name] for name in (
                "guard.exit1AndRefusalMessage", "guard.survivorSamplesAllOk", "guard.twoReadyBrokers")))
            self.assertEqual(run.context["guardPodUID"], "pod-guard")

    def test_cli_requires_execute_and_a_fresh_output_directory(self):
        with tempfile.TemporaryDirectory() as text, self.assertRaises(SystemExit) as caught:
            reseed_main.main(["run", "--private", text, "--output", str(Path(text) / "out"), "--plan",
                              str(Path(text) / "plan.json")])
        self.assertEqual(caught.exception.code, 2)

    def test_cli_reports_plan_errors_without_touching_the_cluster(self):
        with tempfile.TemporaryDirectory() as text:
            root = Path(text)
            (root / "admin.token").write_text("token", encoding="utf-8")
            (root / "plan.json").write_text(json.dumps(plan(runId="other")), encoding="utf-8")
            output = root / RUN
            with patch("builtins.print"), patch.object(reseed, "check_dependencies", return_value=[]):
                code = reseed_main.main(["run", "--private", str(root), "--output", str(output), "--plan",
                                         str(root / "plan.json"), "--execute"])
            self.assertEqual(code, 1)
            summary = json.loads((output / "summary.json").read_text(encoding="utf-8"))
            self.assertEqual((summary["result"], summary["failedStep"], summary["errorCode"]), ("FAIL", "plan", "PlanError"))


class PortTests(unittest.TestCase):
    def test_default_ports_cover_all_tunnels(self):
        self.assertEqual(reseed.tunnel_ports(), [23880, 23883, 24880, 24883, 22880, 22883])

    def test_free_ports_pass_and_a_held_port_fails_closed(self):
        import socket
        with socket.socket() as held, socket.socket() as free:
            held.bind(("127.0.0.1", 0))
            held.listen()
            free.bind(("127.0.0.1", 0))
            free_port = free.getsockname()[1]
        self.assertEqual(reseed.check_local_ports([free_port]), [free_port])
        with socket.socket() as held:
            held.bind(("127.0.0.1", 0))
            held.listen()
            held_port = held.getsockname()[1]
            with self.assertRaises(reseed.PreflightRejected) as caught:
                reseed.check_local_ports([free_port, held_port])
            self.assertEqual(caught.exception.code, f"local_port_unavailable:{held_port}")

    class Untouchable:
        def __getattr__(self, name):
            raise AssertionError(f"fixture used before the port check: {name}")

    def test_run_and_resume_stop_at_the_port_check_before_touching_the_fixture(self):
        import socket
        with tempfile.TemporaryDirectory() as text, socket.socket() as held:
            held.bind(("127.0.0.1", 0))
            held.listen()
            port = held.getsockname()[1]
            run = reseed_main.Run(self.Untouchable(), reseed.load_plan(plan(), RUN), Path(text), Path(text))
            with patch.object(reseed, "LEADER_PORTS", (port, 1)):
                for execute in (run.execute, run.execute_resume):
                    with self.assertRaises(reseed.PreflightRejected) as caught:
                        execute()
                    self.assertEqual(caught.exception.code, f"local_port_unavailable:{port}")
                    self.assertEqual(run.step, "ports")


LOG_LINE = '{"level":"ERROR","error":"cluster bootstrap refused: a configured peer reports an existing cluster"}\n'


class ResumeSimulation(ClusterSimulation):
    """The state the interrupted run stopped in: slot 2 still names member 3, the guard Pod crash-loops."""

    def __init__(self, material: dict):
        super().__init__()
        self.secret["data"]["node-2.crt"] = base64.b64encode(material["certificatePem"]).decode()
        self.secret["data"]["node-2.key"] = base64.b64encode(material["keyPem"]).decode()
        self.pvc_uid = "pvc-new"
        self.logs = LOG_LINE
        self.exit_code = 1
        self.leader = "1"
        self.survivors_ready = True
        self.spare_present = True
        self.extra_rows: list[dict] = []
        self.commands: list = []

    def guard_pod(self):
        value = pod("narumq-2", "pod-guard", "w2", ready_state=False)
        value["status"]["containerStatuses"] = [{
            "name": "narumq", "ready": False, "restartCount": 1, "state": {"waiting": {"reason": "CrashLoopBackOff"}},
            "lastState": {"terminated": {"exitCode": self.exit_code, "reason": "Error"}}}]
        value["spec"]["volumes"] = [{"name": "data", "persistentVolumeClaim": {"claimName": "data-narumq-2"}}]
        return value

    def rows(self):
        return super().rows() + self.extra_rows

    def aws(self, *args):
        if args[:2] == ("sts", "get-caller-identity"):
            return {"Account": reseed.ACCOUNT}
        if args[:2] == ("eks", "describe-cluster"):
            return {"cluster": {"resourcesVpcConfig": {"vpcId": "vpc-1"}}}
        raise AssertionError(args)

    def api(self, path, port=23880):
        if path == "/api/v1/cluster/features":
            return {"members": [{"nodeId": i, "releaseVersion": "1.2.0"} for i in "123"]}
        value = super().api(path, port)
        if path == "/api/v1/cluster":
            value = {**value, "leaderNodeId": self.leader}
        return value

    def get(self, *args):
        kind = args[0]
        if kind == "namespace":
            return {"metadata": {"labels": {"narumq.io/acceptance-run": RUN}}}
        if kind == "pvc":
            return None if self.pvc_uid is None else {"metadata": {"uid": self.pvc_uid}}
        if kind == "pods" and not self.recreated:
            items = [pod("narumq-0", "pod-observer", "w0", self.survivors_ready), pod("narumq-1", "pod-1", "w1"),
                     self.guard_pod()]
            return {"items": items}
        return super().get(*args)

    def get_node(self, name):
        if name == "worker-spare" and self.spare_present:
            return {"metadata": {"uid": "spare-uid"}, "spec": {"providerID": "aws:///ap-northeast-2a/i-spare"},
                    "status": ready_condition()}
        return None

    def command(self, args, timeout=90, stdin=None):
        self.commands.append(args)
        if "logs" in args:
            return self.logs
        return super().command(args, timeout, stdin)


class ResumeTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        root = Path(self.directory.name)
        self.private, self.original = root / "private", root / RUN
        self.private.mkdir()
        self.original.mkdir()
        ca_pem, ca_key = CertificateTests.authority()
        (self.private / "ca.crt").write_bytes(ca_pem)
        (self.private / "ca.key").write_bytes(ca_key)
        self.material = reseed.generate_peer_certificate(ca_pem, ca_key, DNS[2])
        self.sim = ResumeSimulation(self.material)
        snapshot = {"node-2.crt": self.sim.secret["data"]["node-2.crt"], "node-2.key": self.sim.secret["data"]["node-2.key"]}
        (self.private / f"reseed-{RUN}-slot2-rollback-secret.json").write_text(json.dumps(snapshot), encoding="utf-8")
        (self.private / f"reseed-{RUN}-slot2-node4.crt").write_bytes(b"unused first-run certificate")
        (self.private / f"reseed-{RUN}-slot2-node4.key").write_bytes(b"unused first-run key")
        self.plan = reseed.load_plan(plan(), RUN)
        criteria = {name: name.split(".")[0] in ("loss", "guard") for name in reseed.CRITERIA}
        self.write("summary.json", {"result": "FAIL", "runId": RUN, "failedStep": "reseed",
                                    "errorCode": "stage_response_unknown", "criteria": criteria})
        self.write("reseed-loss.json", {"pvcReplaced": {"pvcUID": "pvc-new", "podUID": "pod-guard"}})
        self.write("reseed-baseline.json", {"baseline": {"appliedIndexLowerBound": 150},
                                            "marker": {"sha256": "a" * 64}, "markerOnTarget": {"sha256": "a" * 64}})
        self.write("reseed-guard.json", {"verdict": "refused", "podUID": "pod-guard", "freshPvc": {"uid": "pvc-new"}})
        self.write("reseed-reseed.json", {"stageRequest": {"idempotencyKey": f"{RUN}-stage-4"},
                                          "stageResponse": {"transportError": "RuntimeError"}})
        journal = [{"seq": i, "sentAt": f"2026-10-16T00:00:{i:02d}+00:00", "outcome": "acked", "reasonCode": 0,
                    "ackAt": f"2026-10-16T00:00:{i:02d}+00:00"} for i in range(1, 4)]
        (self.original / "durable-publish-reseed.jsonl").write_text(
            "".join(json.dumps(entry) + "\n" for entry in journal), encoding="utf-8")

    def write(self, name, value):
        (self.original / name).write_text(json.dumps(value), encoding="utf-8")

    def validate(self):
        records = reseed.load_original_records(self.original)
        return reseed.resume_validate(self.sim, self.plan, records, self.private)

    def assertMismatch(self, field):
        with self.assertRaises(reseed.PreflightRejected) as caught:
            self.validate()
        self.assertEqual(caught.exception.code, f"resume_state_mismatch:{field}")
        self.assertEqual(self.sim.replaced, [])
        self.assertEqual(self.sim.requests, [])
        self.assertEqual(self.sim.deleted, [])
        self.assertTrue(all("logs" in args for args in self.sim.commands))

    def test_state_matching_the_interrupted_run_is_accepted(self):
        evidence, context = self.validate()
        self.assertEqual((context["guardPodUID"], context["newPvcUID"], context["survivors"]),
                         ("pod-guard", "pvc-new", ["1", "2"]))
        self.assertEqual(context["baseline"], {"appliedIndex": 150, "markerSha256": "a" * 64})
        self.assertEqual(context["joinerConfig"]["role"], "joiner")
        self.assertEqual(context["joinerConfig"]["nodeId"], 4)
        self.assertEqual(context["releaseVersion"], "1.2.0")
        self.assertEqual(context["spareNodeUID"], "spare-uid")
        self.assertTrue(all(item["value"] for item in context["carried"].values()))
        self.assertEqual({item["source"] for item in context["carried"].values()},
                         {"../reseed-loss.json", "../reseed-guard.json"})
        self.assertTrue(evidence["secret"]["rollbackSnapshotPresent"])
        self.assertEqual(len(evidence["firstRunPrivateFiles"]), 3)
        self.assertNotIn("PRIVATE KEY", json.dumps(evidence))

    def test_every_state_mismatch_fails_before_any_mutation(self):
        cases = {
            "configmap.slot": lambda: self.sim.configmap["data"].update({"slot-2": "9"}),
            "configmap.joinerConfigPresent": lambda: self.sim.configmap["data"].update({"node-4.json": "{}"}),
            "configmap.oldConfig": lambda: self.sim.configmap["data"].pop("node-3.json"),
            "members.active": lambda: self.sim.extra_rows.append(node_row("4", phase="STAGED", role=None,
                                                                          status="UNREACHABLE")),
            "members.newIdPresent": lambda: self.sim.extra_rows.append(node_row("4", phase="RETIRED", role=None,
                                                                                status="RETIRED")),
            "members.newIdNotAboveCommitted": lambda: self.sim.extra_rows.append(
                node_row("7", phase="RETIRED", role=None, status="RETIRED")),
            "members.leader": lambda: setattr(self.sim, "leader", "3"),
            "pvc.uid": lambda: setattr(self.sim, "pvc_uid", "pvc-other"),
            "pods.guardRefusal": lambda: setattr(self.sim, "exit_code", 2),
            "pods.survivorsReady": lambda: setattr(self.sim, "survivors_ready", False),
            "spare.node": lambda: setattr(self.sim, "spare_present", False),
            "secret.slotCertificate": lambda: self.sim.secret["data"].update({"node-2.crt": "AAAA"}),
        }
        for field, mutate in cases.items():
            with self.subTest(field):
                self.setUp()
                mutate()
                self.assertMismatch(field)

    def test_guard_log_without_the_refusal_message_is_a_mismatch(self):
        self.sim.logs = "panic: something else\n"
        self.assertMismatch("pods.guardRefusal")

    def test_record_problems_are_mismatches(self):
        def edit(name, change):
            value = json.loads((self.original / name).read_text(encoding="utf-8"))
            change(value)
            self.write(name, value)

        cases = {
            "summary.failedStep": ("summary.json", lambda v: v.update(failedStep="verify")),
            "summary.criteria.loss.volumeDeleted": ("summary.json", lambda v: v["criteria"].update(
                {"loss.volumeDeleted": False})),
            "guard.record": ("reseed-guard.json", lambda v: v.update(verdict="fail")),
            "loss.newPvcUID": ("reseed-loss.json", lambda v: v["pvcReplaced"].update(pvcUID="pvc-old")),
            "baseline.marker": ("reseed-baseline.json", lambda v: v["marker"].update(sha256="short")),
            "baseline.appliedIndex": ("reseed-baseline.json", lambda v: v["baseline"].update(appliedIndexLowerBound=True)),
            "reseed.stageKey": ("reseed-reseed.json", lambda v: v["stageRequest"].update(idempotencyKey="other")),
            "reseed.alreadyMutated": ("reseed-reseed.json", lambda v: v["stageResponse"].update(httpStatus=202)),
        }
        for field, (name, change) in cases.items():
            with self.subTest(field):
                self.setUp()
                edit(name, change)
                self.assertMismatch(field)
        self.setUp()
        (self.original / "reseed-guard.json").unlink()
        with self.assertRaises(reseed.PreflightRejected) as caught:
            reseed.load_original_records(self.original)
        self.assertEqual(caught.exception.code, "resume_state_mismatch:records.reseed-guard")

    def resume_run(self, number=1):
        out = self.original / f"resume-{number}"
        out.mkdir()
        run = reseed_main.Run(self.sim, self.plan, out, self.private, sleep=lambda s: None, original=self.original)
        run.private_suffix = f"-resume{number}"
        return run, out

    def test_resume_reseeds_with_the_reconstructed_context_and_leaves_originals_untouched(self):
        before = reseed.hash_records(self.original, recursive=False)
        run, out = self.resume_run()
        run.step_resume()
        self.assertEqual((out / "durable-publish-reseed.jsonl").read_bytes(),
                         (self.original / "durable-publish-reseed.jsonl").read_bytes())
        self.assertEqual(run.baseline, {"appliedIndex": 150, "markerSha256": "a" * 64})
        self.assertEqual(sum(run.criteria.values()), 6)
        run.step_reseed()
        self.assertEqual([r[:2] for r in self.sim.requests],
                         [("POST", "/api/v1/cluster/members"), ("DELETE", "/api/v1/cluster/members/3")])
        self.assertEqual(self.sim.requests[0][4], f"{RUN}-stage-4")
        self.assertEqual(self.sim.deleted, [("narumq-2", "pod-guard")])
        self.assertEqual(run.evidence["reseed"]["stageResponse"]["disposition"], "created")
        self.assertEqual(self.sim.configmap["data"]["slot-2"], "4")
        # a second certificate is generated; the first run's files stay and the new ones use a suffix
        names = sorted(item.name for item in self.private.iterdir() if item.name.startswith("reseed-"))
        self.assertIn(f"reseed-{RUN}-slot2-node4-resume1.key", names)
        self.assertIn(f"reseed-{RUN}-slot2-node4.key", names)
        self.assertEqual((self.private / f"reseed-{RUN}-slot2-node4.key").read_bytes(), b"unused first-run key")
        run.context["newPodUID"] = "pod-new"
        durable = {"result": "PASS", "label": "after-reseed", "sessionPresent": True, "acked": 3, "rejected": 0,
                   "unknown": 0, "missingAcked": [], "rejectedDelivered": [], "unknownDelivered": [], "duplicates": 0}
        FakeTunnel.created.clear()
        with patch.object(reseed_main, "PodTunnel", FakeTunnel), \
                patch.object(reseed_main.durable_stream, "verify_subscriber", return_value=durable) as verify, \
                patch.object(reseed_main.mqtt_helpers, "read_marker", return_value={"found": True, "sha256": "a" * 64}):
            run.step_verify()
        self.assertEqual(verify.call_args.args[-3:], (24883, out, "after-reseed"))
        self.assertEqual(run.evidence["verify"]["publisher"]["source"], "journal of the interrupted run")
        self.assertEqual(run.evidence["verify"]["publisher"]["acked"], 3)
        judged = reseed.aggregate(run.criteria)
        self.assertEqual((judged["result"], judged["failedCriteria"]), ("PASS", []))
        self.assertEqual(reseed.hash_records(self.original, recursive=False), before)
        self.assertTrue(all(item.parent == out for item in out.glob("reseed-*.json")))

    def test_cli_resume_writes_only_below_resume_dir_and_reports_the_original(self):
        plan_path = Path(self.directory.name) / "plan.json"
        plan_path.write_text(json.dumps(plan()), encoding="utf-8")
        args = ["resume", "--private", str(self.private), "--output", str(self.original), "--plan", str(plan_path),
                "--execute", "--from", "reseed"]
        self.write("reseed-guard.json", {"verdict": "fail"})  # fails validation
        before = reseed.hash_records(self.original, recursive=False)
        with patch.object(reseed_main, "Fixture", return_value=self.sim), \
                patch.object(reseed, "check_local_ports", return_value=[1]), \
                patch.object(reseed, "check_dependencies", return_value=[]), \
                patch.object(reseed_main.Run, "step_observer", lambda run: None), patch("builtins.print"):
            self.assertEqual(reseed_main.main(args), 1)
            self.assertEqual(reseed_main.main(args), 1)
        for number in (1, 2):
            summary = json.loads((self.original / f"resume-{number}" / "summary.json").read_text(encoding="utf-8"))
            self.assertEqual((summary["result"], summary["failedStep"], summary["errorCode"]),
                             ("FAIL", "resume_validate", "resume_state_mismatch:guard.record"))
            self.assertEqual(summary["original"]["failedStep"], "reseed")
            self.assertTrue(summary["original"]["recordsUnchanged"])
            self.assertEqual(summary["resumeNumber"], number)
        self.assertEqual(reseed.hash_records(self.original, recursive=False), before)
        self.assertEqual(self.sim.requests, [])

    def test_cli_requires_from_execute_and_an_existing_output(self):
        base = ["resume", "--private", str(self.private), "--plan", "p.json"]
        for extra in (["--output", str(self.original), "--execute"],
                      ["--output", str(self.original), "--from", "reseed"],
                      ["--output", str(self.original / "missing"), "--execute", "--from", "reseed"],
                      ["--output", str(self.original), "--execute", "--from", "loss"]):
            with self.subTest(extra), self.assertRaises(SystemExit) as caught, patch("sys.stderr"):
                reseed_main.main(base + extra)
            self.assertEqual(caught.exception.code, 2)
        self.assertEqual(list(self.original.glob("resume-*")), [])


class NewPodSimulation(ResumeSimulation):
    """Live state after `resume --from reseed` committed stage 4: slot-2 names 4, the Pod was recreated by an image roll."""

    def __init__(self, material: dict):
        super().__init__(material)
        self.recreated = True
        self.operation_override = None
        self.configmap["data"]["slot-2"] = "4"
        joiner = reseed.derive_joiner_config(bootstrap_config(3, 2), plan(), {0: "1", 1: "2", 2: "3"})
        self.configmap["data"]["node-4.json"] = reseed.config_text(joiner)

    def rows(self):
        rows = [node_row("1", role="LEADER", lastAppliedIndex=150), node_row("2", lastAppliedIndex=150),
                node_row("3", phase="RETIRED", role=None, status="RETIRED") if self.removed
                else node_row("3", status="UNREACHABLE")]
        rows.append(node_row("4", lastAppliedIndex=160) if self.recreated
                    else node_row("4", phase="STAGED", role=None, status="UNREACHABLE"))
        return rows

    def api_request(self, method, path, body=None, port=23880, idempotency_key=None):
        if method == "GET" and "/membership-operations/" in path and self.operation_override is not None:
            return self.operation_override
        return super().api_request(method, path, body, port, idempotency_key)


def stage_record(operation_id: str, sha: str, **changes) -> dict:
    value = {"certificate": {"sha256": sha},
             "stageRequest": {"idempotencyKey": f"{RUN}-stage-4"},
             "stageResponse": {"httpStatus": 202, "operationId": operation_id, "phase": "STAGED", "memberId": 4},
             "configMapReseed": {"appliedAfterFreshRead": True}, "secretReseed": {"appliedAfterFreshRead": True},
             "podDelete": {"podUID": "pod-guard"}}
    value.update(changes)
    return value


class ResumeSourceTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.original = Path(self.directory.name) / RUN
        self.original.mkdir()

    def resume(self, number: int, record: dict | None):
        folder = self.original / f"resume-{number}"
        folder.mkdir()
        if record is not None:
            (folder / "reseed-reseed.json").write_text(json.dumps(record), encoding="utf-8")
        return folder

    def test_latest_qualifying_resume_is_the_source(self):
        self.assertIsNone(reseed.committed_stage_source(self.original))
        one = self.resume(1, stage_record("op-1", "1" * 64))
        self.assertEqual(reseed.committed_stage_source(self.original)[0], one)
        self.resume(2, {"continuedFrom": {"operationId": "op-1"}, "newPod": {"podUID": "x"}})  # continuation only
        self.resume(3, None)                                                                        # failed validation
        self.assertEqual(reseed.committed_stage_source(self.original)[0], one)
        self.resume(5, stage_record("op-5", "5" * 64))
        self.resume(9, stage_record("op-9", "9" * 64))
        self.assertEqual(reseed.committed_stage_source(self.original)[1]["stageResponse"]["operationId"], "op-9")

    def test_numeric_order_not_lexicographic(self):
        self.resume(2, stage_record("op-2", "2" * 64))
        self.resume(10, stage_record("op-10", "a" * 64))
        self.assertEqual(reseed.committed_stage_source(self.original)[1]["stageResponse"]["operationId"], "op-10")

    def test_incomplete_records_do_not_qualify(self):
        sha = "1" * 64
        broken = {
            "rejected stage": stage_record("op", sha, stageResponse={"httpStatus": 409, "operationId": "op"}),
            "transport error": stage_record("op", sha, stageResponse={"transportError": "RuntimeError"}),
            "no operation id": stage_record("op", sha, stageResponse={"httpStatus": 202}),
            "configmap not applied": stage_record("op", sha, configMapReseed={"appliedAfterFreshRead": False}),
            "secret missing": stage_record("op", sha, secretReseed={}),
            "no pod delete": {key: value for key, value in stage_record("op", sha).items() if key != "podDelete"},
            "pod delete failed": stage_record("op", sha, podDelete={"podUID": "p", "commandFailed": True,
                                                                     "appliedAfterFreshRead": False}),
            "no certificate": stage_record("op", "short"),
        }
        for name, record in broken.items():
            with self.subTest(name):
                folder = self.resume(len(list(self.original.glob("resume-*"))) + 1, record)
                self.assertIsNone(reseed.committed_stage_source(self.original), name)
                (folder / "reseed-reseed.json").unlink()
        ok = stage_record("op", sha, podDelete={"podUID": "p", "commandFailed": True, "appliedAfterFreshRead": True})
        self.resume(99, ok)
        self.assertIsNotNone(reseed.committed_stage_source(self.original))

    def test_a_stage_request_in_any_earlier_resume_blocks_resume_from_reseed(self):
        reseed.check_no_stage_attempt(self.original)
        self.resume(1, None)
        self.resume(2, {"newPod": {}})
        reseed.check_no_stage_attempt(self.original)
        self.resume(3, {"stageRequest": {"idempotencyKey": f"{RUN}-stage-4"}})
        with self.assertRaises(reseed.PreflightRejected) as caught:
            reseed.check_no_stage_attempt(self.original)
        self.assertEqual(caught.exception.code, "resume_state_mismatch:stage_already_attempted")

    def test_hash_records_covers_earlier_resume_directories_but_skips_the_current_one(self):
        (self.original / "summary.json").write_text("{}", encoding="utf-8")
        one = self.resume(1, stage_record("op", "1" * 64))
        current = self.resume(2, {"x": 1})
        everything = reseed.hash_records(self.original, skip=current)
        self.assertEqual(sorted(everything), ["resume-1/reseed-reseed.json", "summary.json"])
        self.assertEqual(sorted(reseed.hash_records(self.original, skip=current, recursive=False)), ["summary.json"])
        (one / "reseed-reseed.json").write_text("{}", encoding="utf-8")
        self.assertNotEqual(reseed.hash_records(self.original, skip=current), everything)


class NewPodResumeTests(unittest.TestCase):
    def setUp(self):
        base = ResumeTests("test_state_matching_the_interrupted_run_is_accepted")
        base.setUp()
        self.addCleanup(base.doCleanups)
        self.base, self.directory = base, base.directory
        self.private, self.original, self.plan, self.material = base.private, base.original, base.plan, base.material
        self.sim = NewPodSimulation(self.material)
        self.sha = self.material["sha256"]
        self.first = self.original / "resume-1"
        self.first.mkdir()
        (self.first / "summary.json").write_text(json.dumps({"result": "FAIL", "errorCode": "new_pod_not_ready"}),
                                                 encoding="utf-8")
        self.write_source(stage_record("op-stage", self.sha, newPod={"podUID": "never"}))

    def write_source(self, record):
        (self.first / "reseed-reseed.json").write_text(json.dumps(record), encoding="utf-8")

    def validate(self):
        records = reseed.load_original_records(self.original)
        return reseed.resume_validate_new_pod(self.sim, self.plan, records, self.original)

    def assertMismatch(self, field):
        with self.assertRaises(reseed.PreflightRejected) as caught:
            self.validate()
        self.assertEqual(caught.exception.code, f"resume_state_mismatch:{field}")
        self.assertEqual((self.sim.replaced, self.sim.requests, self.sim.deleted), ([], [], []))

    def test_state_after_the_committed_stage_is_accepted_and_context_comes_from_the_records(self):
        evidence, context = self.validate()
        self.assertEqual((context["operationId"], context["guardPodUID"], context["newPvcUID"]),
                         ("op-stage", "pod-guard", "pvc-new"))
        self.assertEqual((context["survivors"], context["releaseVersion"], context["certificateSha256"]),
                         (["1", "2"], "1.2.0", self.sha))
        self.assertEqual(context["baseline"], {"appliedIndex": 150, "markerSha256": "a" * 64})
        self.assertEqual((context["joinerConfig"]["nodeId"], context["joinerConfig"]["role"]), (4, "joiner"))
        self.assertEqual(context["oldConfig"]["nodeId"], 3)
        self.assertEqual(evidence["source"]["directory"], "resume-1")
        self.assertFalse(evidence["secret"]["certificateGenerated"])
        self.assertFalse(evidence["stageReposted"])
        self.assertEqual(evidence["stageOperation"]["phase"], "SUCCEEDED")
        self.assertEqual((self.sim.replaced, self.sim.requests), ([], []))

    def test_every_state_mismatch_fails_before_any_mutation(self):
        cases = {
            "configmap.slot": lambda: self.sim.configmap["data"].update({"slot-2": "3"}),
            "configmap.joinerConfig": lambda: self.sim.configmap["data"].pop("node-4.json"),
            "configmap.oldConfig": lambda: self.sim.configmap["data"].pop("node-3.json"),
            "secret.slotCertificate": lambda: self.sim.secret["data"].update({"node-2.crt": "AAAA"}),
            "stage.operation": lambda: setattr(self.sim, "operation_override", (404, None)),
            "members.oldVoter": lambda: setattr(self.sim, "removed", True),
            "members.survivorVoters": lambda: self.sim.rows_patch.update({"2": "LEARNER"}),
            "members.leader": lambda: setattr(self.sim, "leader", "3"),
            "resume.source": lambda: self.write_source(stage_record("op-stage", self.sha,
                                                                    secretReseed={"appliedAfterFreshRead": False})),
        }
        original_rows = NewPodSimulation.rows
        self.sim.rows_patch = {}

        def patched(sim):
            rows = original_rows(sim)
            for row in rows:
                row["membershipPhase"] = sim.rows_patch.get(row["nodeId"], row["membershipPhase"])
            return rows

        for field, mutate in cases.items():
            with self.subTest(field), patch.object(NewPodSimulation, "rows", patched):
                self.setUp()
                self.sim.rows_patch = {}
                mutate()
                self.assertMismatch(field)

    def test_stage_operation_must_be_an_add_for_the_new_id_that_has_not_failed(self):
        base = {"operationId": "op-stage", "action": "ADD", "memberId": 4, "phase": "SUCCEEDED"}
        bad = {"other member": {**base, "memberId": 5}, "remove": {**base, "action": "REMOVE"},
               "failed": {**base, "phase": "FAILED"}, "cancelled": {**base, "phase": "CANCELLED"},
               "no phase": {key: value for key, value in base.items() if key != "phase"},
               "other id": {**base, "operationId": "op-other"}, "http 500": None}
        for name, data in bad.items():
            with self.subTest(name):
                self.sim.operation_override = (500, None) if data is None else (200, {"data": data})
                self.assertMismatch("stage.operation")
        for phase in ("STAGED", "LEARNER", "CATCHING_UP", "PROMOTING", "SUCCEEDED"):
            self.sim.operation_override = (200, {"data": {**base, "phase": phase}})
            self.assertEqual(self.validate()[0]["stageOperation"]["phase"], phase)

    def test_original_record_problems_are_mismatches(self):
        value = json.loads((self.original / "summary.json").read_text(encoding="utf-8"))
        value["failedStep"] = "verify"
        self.base.write("summary.json", value)
        self.assertMismatch("summary.failedStep")

    def new_pod_run(self, number=2):
        out = self.original / f"resume-{number}"
        out.mkdir()
        return reseed_main.Run(self.sim, self.plan, out, self.private, sleep=lambda s: None,
                               original=self.original), out

    def test_continues_at_new_pod_ready_without_certificate_stage_or_pod_delete(self):
        before = reseed.hash_records(self.original)
        before_private = sorted(item.name for item in self.private.iterdir())
        run, out = self.new_pod_run()
        run.step_resume_new_pod()
        self.assertEqual(run.baseline, {"appliedIndex": 150, "markerSha256": "a" * 64})
        self.assertEqual(sum(run.criteria.values()), 6)
        self.assertTrue((out / "durable-publish-reseed.jsonl").is_file())
        run.step_new_pod()
        self.assertEqual([r[:2] for r in self.sim.requests], [("DELETE", "/api/v1/cluster/members/3")])
        self.assertEqual(self.sim.replaced, ["ConfigMap"])  # cleanup only; Secret and slot ConfigMap not rewritten
        self.assertEqual(self.sim.deleted, [])
        self.assertEqual(run.context["newPodUID"], "pod-new")  # any UID except the deleted guard Pod's
        record = json.loads((out / "reseed-reseed.json").read_text(encoding="utf-8"))
        for key in ("stageRequest", "stageResponse", "certificate", "privateMaterial", "configMapReseed",
                    "secretReseed", "podDelete"):
            self.assertNotIn(key, record)
        self.assertEqual(record["continuedFrom"]["operationId"], "op-stage")
        self.assertEqual(record["newVoter"]["nodeId"], "4")
        self.assertEqual(record["stageSucceeded"]["phase"], "SUCCEEDED")
        self.assertTrue(run.criteria["reseed.newIdVoter"] and run.criteria["reseed.oldIdRetired"])
        self.assertEqual(sorted(item.name for item in self.private.iterdir()), before_private)
        durable = {"result": "PASS", "label": "after-reseed", "sessionPresent": True, "acked": 3, "rejected": 0,
                   "unknown": 0, "missingAcked": [], "rejectedDelivered": [], "unknownDelivered": [], "duplicates": 0}
        FakeTunnel.created.clear()
        with patch.object(reseed_main, "PodTunnel", FakeTunnel), \
                patch.object(reseed_main.durable_stream, "verify_subscriber", return_value=durable) as verify, \
                patch.object(reseed_main.mqtt_helpers, "read_marker", return_value={"found": True, "sha256": "a" * 64}):
            run.step_verify()
        self.assertEqual(FakeTunnel.created[0][:2], ("narumq-2", "pod-new"))
        self.assertEqual(verify.call_args.args[-3:], (24883, out, "after-reseed"))
        self.assertEqual(run.evidence["verify"]["publisher"]["acked"], 3)
        self.assertEqual(reseed.hash_records(self.original, skip=out), {k: v for k, v in before.items()
                                                                        if not k.startswith("resume-2/")})

    def test_pod_that_is_still_the_deleted_guard_pod_is_not_ready(self):
        self.sim.recreated = False
        self.sim.pod_uid = "pod-guard"
        run, out = self.new_pod_run()
        with patch.object(reseed_main, "NEW_POD_READY_SECONDS", 0):
            run.step_resume_new_pod()
            with self.assertRaises(reseed_main.StepFailed) as caught:
                run.step_new_pod()
        self.assertEqual(caught.exception.code, "new_pod_not_ready")
        self.assertEqual((self.sim.requests, self.sim.replaced, self.sim.deleted), ([], [], []))

    def test_cli_new_pod_validation_failure_is_recorded_below_a_new_resume_dir(self):
        plan_path = Path(self.directory.name) / "plan.json"
        plan_path.write_text(json.dumps(plan()), encoding="utf-8")
        self.sim.configmap["data"]["slot-2"] = "3"
        args = ["resume", "--private", str(self.private), "--output", str(self.original), "--plan", str(plan_path),
                "--execute", "--from", "new-pod"]
        before = reseed.hash_records(self.original)
        with patch.object(reseed_main, "Fixture", return_value=self.sim), \
                patch.object(reseed, "check_local_ports", return_value=[1]), \
                patch.object(reseed, "check_dependencies", return_value=[]), \
                patch.object(reseed_main.Run, "step_observer", lambda run: None), patch("builtins.print"):
            self.assertEqual(reseed_main.main(args), 1)
        summary = json.loads((self.original / "resume-2" / "summary.json").read_text(encoding="utf-8"))
        self.assertEqual((summary["from"], summary["failedStep"], summary["errorCode"]),
                         ("new-pod", "resume_validate", "resume_state_mismatch:configmap.slot"))
        self.assertTrue(summary["original"]["recordsUnchanged"])
        self.assertEqual(reseed.hash_records(self.original, skip=self.original / "resume-2"), before)
        self.assertEqual((self.sim.requests, self.sim.replaced), ([], []))

    def test_from_reseed_is_refused_once_an_earlier_resume_sent_the_stage(self):
        out = self.original / "resume-2"
        out.mkdir()

        class Untouchable:
            def __getattr__(self, name):
                raise AssertionError(f"live call before the stage guard: {name}")

        run = reseed_main.Run(Untouchable(), self.plan, out, self.private, original=self.original)
        with self.assertRaises(reseed.PreflightRejected) as caught:
            run.step_resume()
        self.assertEqual(caught.exception.code, "resume_state_mismatch:stage_already_attempted")
        self.assertEqual(list(out.iterdir()), [])

    def test_cli_accepts_new_pod_choice(self):
        self.assertIn("new-pod", reseed.RESUME_FROM_STEPS)
        base = ["resume", "--private", str(self.private), "--plan", "p.json", "--output", str(self.original)]
        with self.assertRaises(SystemExit) as caught, patch("sys.stderr"):
            reseed_main.main(base + ["--from", "new-pod"])  # still requires --execute
        self.assertEqual(caught.exception.code, 2)


class VerifySimulation(NewPodSimulation):
    """Live state after the whole reseed: old id retired and its config gone; only verify is left."""

    def __init__(self, material: dict):
        super().__init__(material)
        self.removed = True
        self.configmap["data"].pop("node-3.json")
        self.hide_rows: set[str] = set()
        self.phase_patch: dict[str, str] = {}

    def rows(self):
        rows = [row for row in super().rows() if row["nodeId"] not in self.hide_rows]
        for row in rows:
            row["membershipPhase"] = self.phase_patch.get(row["nodeId"], row["membershipPhase"])
        return rows


DURABLE_UNTOUCHED = {"label": "after-reseed", "sessionPresent": None, "receivedDistinct": 0, "result": "FAIL",
                     "errorCode": "ModuleNotFoundError"}


class VerifyResumeTests(unittest.TestCase):
    def setUp(self):
        base = ResumeTests("test_state_matching_the_interrupted_run_is_accepted")
        base.setUp()
        self.addCleanup(base.doCleanups)
        self.base, self.directory = base, base.directory
        self.private, self.original, self.plan, self.material = base.private, base.original, base.plan, base.material
        self.sim = VerifySimulation(self.material)
        self.stage_dir = self.original / "resume-1"
        self.stage_dir.mkdir()
        (self.stage_dir / "reseed-reseed.json").write_text(json.dumps(
            stage_record("op-stage", self.material["sha256"], newPod={"podUID": "never"})), encoding="utf-8")
        self.last = self.original / "resume-3"
        self.last.mkdir()
        self.write_last("reseed-reseed.json", {
            "continuedFrom": {"operationId": "op-stage", "deletedPodUID": "pod-guard"}, "newVoter": {"nodeId": "4"},
            "stageSucceeded": {"phase": "SUCCEEDED"}, "removeObserved": {"oldMembershipPhase": "RETIRED"},
            "configMapCleanup": {"appliedAfterFreshRead": True}})
        self.write_last("reseed-verify.json", {"restageOldId": {"at": "t", "httpStatus": 409, "servedByLeader": "1",
                                                                "errorCode": "member_id_reserved"}})
        self.write_last("durable-verify-reseed-after-reseed.json", DURABLE_UNTOUCHED)

    def write_last(self, name, value):
        (self.last / name).write_text(json.dumps(value), encoding="utf-8")

    def validate(self, current=None):
        records = reseed.load_original_records(self.original)
        return reseed.resume_validate_verify(self.sim, self.plan, records, self.original, current)

    def assertMismatch(self, field):
        with self.assertRaises(reseed.PreflightRejected) as caught:
            self.validate()
        self.assertEqual(caught.exception.code, f"resume_state_mismatch:{field}")
        self.assertEqual((self.sim.replaced, self.sim.requests, self.sim.deleted), ([], [], []))

    def test_finished_reseed_with_an_unconnected_session_is_accepted(self):
        evidence, context = self.validate()
        self.assertEqual((context["guardPodUID"], context["newPodUID"], context["newPvcUID"]),
                         ("pod-guard", "pod-new", "pvc-new"))
        self.assertEqual((context["survivors"], context["joinerConfig"]["nodeId"]), (["1", "2"], 4))
        self.assertEqual(context["baseline"], {"appliedIndex": 150, "markerSha256": "a" * 64})
        self.assertEqual(evidence["source"]["directory"], "resume-3")
        self.assertEqual(evidence["restage"], {"carriedFrom": "resume-3/reseed-verify.json", "willPost": False})
        self.assertEqual(evidence["durableSession"]["verifyRecordsChecked"],
                         ["resume-3/durable-verify-reseed-after-reseed.json"])
        self.assertEqual((self.sim.replaced, self.sim.requests, self.sim.deleted), ([], [], []))

    def test_every_state_mismatch_fails_before_any_mutation(self):
        def set_source_guard(uid):
            self.write_last("reseed-reseed.json", {
                "continuedFrom": {"deletedPodUID": uid}, "newVoter": {}, "stageSucceeded": {}, "removeObserved": {},
                "configMapCleanup": {"appliedAfterFreshRead": True}})

        cases = {
            "configmap.slot": lambda: self.sim.configmap["data"].update({"slot-2": "3"}),
            "configmap.oldConfigPresent": lambda: self.sim.configmap["data"].update({"node-3.json": "{}"}),
            "configmap.joinerConfig": lambda: self.sim.configmap["data"].pop("node-4.json"),
            "members.voters": lambda: self.sim.phase_patch.update({"2": "LEARNER"}),
            "members.oldRetired": lambda: self.sim.hide_rows.add("3"),
            "members.leader": lambda: setattr(self.sim, "leader", "9"),
            "pods.newPod": lambda: set_source_guard("pod-new"),
            "resume.source": lambda: self.write_last("reseed-reseed.json", {"newVoter": {}}),
            "resume.guardPodUID": lambda: set_source_guard(""),
            "restage.oldCertificate": lambda: (self.write_last("reseed-verify.json", {}),
                                               self.sim.configmap["data"].update({"node-1.json": "{}"})),
        }
        for field, mutate in cases.items():
            with self.subTest(field):
                self.setUp()
                mutate()
                self.assertMismatch(field)

    def test_a_not_ready_new_pod_is_a_mismatch(self):
        self.sim.get_pod = lambda name: pod(name, "pod-new", "w2", ready_state=False)
        self.assertMismatch("pods.newPod")

    def test_any_earlier_verify_that_touched_the_session_blocks_the_run_before_any_live_call(self):
        touched = {
            "session present": {**DURABLE_UNTOUCHED, "sessionPresent": True, "receivedDistinct": 0},
            "session absent but connected": {**DURABLE_UNTOUCHED, "sessionPresent": False},
            "messages received": {**DURABLE_UNTOUCHED, "receivedDistinct": 2},
            "count wrong type": {**DURABLE_UNTOUCHED, "receivedDistinct": "0"},
        }

        class Untouchable:
            def __getattr__(self, name):
                raise AssertionError(f"live call before the durable session check: {name}")

        places = {"resume dir": lambda value: self.write_last("durable-verify-reseed-other.json", value),
                  "original dir": lambda value: (self.original / "durable-verify-reseed-x.json").write_text(
                      json.dumps(value), encoding="utf-8")}
        for place, write in places.items():
            for name, value in touched.items():
                with self.subTest(f"{place}: {name}"):
                    self.setUp()
                    write(value)
                    out = self.original / "resume-4"
                    out.mkdir()
                    run = reseed_main.Run(Untouchable(), self.plan, out, self.private, original=self.original)
                    with self.assertRaises(reseed.PreflightRejected) as caught:
                        run.step_resume_verify()
                    self.assertEqual(caught.exception.code, "resume_state_mismatch:durable_session_touched")
                    self.assertEqual(list(out.iterdir()), [])
        self.setUp()
        (self.last / "durable-verify-reseed-after-reseed.json").write_text("{not json", encoding="utf-8")
        self.assertMismatch("durable_session_touched")

    def test_only_this_scenario_and_earlier_dirs_are_checked(self):
        self.write_last("durable-verify-other-scenario-x.json", {"sessionPresent": True, "receivedDistinct": 5})
        out = self.original / "resume-4"
        out.mkdir()
        (out / "durable-verify-reseed-after-reseed.json").write_text(json.dumps({"sessionPresent": True}),
                                                                     encoding="utf-8")
        evidence, _ = self.validate(current=out)  # the directory being written is not an earlier attempt
        self.assertTrue(evidence["durableSession"]["untouched"])

    def test_source_needs_the_whole_reseed_finished(self):
        self.assertEqual(reseed.verify_source(self.original)[0], self.last)
        self.write_last("reseed-reseed.json", {"newVoter": {}, "stageSucceeded": {}, "removeObserved": {},
                                               "configMapCleanup": {"appliedAfterFreshRead": False}})
        self.assertIsNone(reseed.verify_source(self.original))
        later = self.original / "resume-4"
        later.mkdir()
        (later / "reseed-reseed.json").write_text(json.dumps({
            "newVoter": {}, "stageSucceeded": {}, "removeObserved": {}, "configMapCleanup": {"appliedAfterFreshRead": True}}),
            encoding="utf-8")
        self.assertEqual(reseed.verify_source(self.original)[0], later)

    def test_carried_restage_only_for_409_member_id_reserved_newest_first_and_never_the_current_dir(self):
        self.assertEqual(reseed.carried_restage(self.original, None)["carriedFrom"], "resume-3/reseed-verify.json")
        newer = self.original / "resume-4"
        newer.mkdir()
        (newer / "reseed-verify.json").write_text(json.dumps({"restageOldId": {
            "httpStatus": 409, "errorCode": "member_id_reserved", "carriedFrom": "resume-3/reseed-verify.json"}}),
            encoding="utf-8")
        self.assertEqual(reseed.carried_restage(self.original, None)["carriedFrom"], "resume-3/reseed-verify.json")
        (newer / "reseed-verify.json").write_text(json.dumps({"restageOldId": {"httpStatus": 500}}), encoding="utf-8")
        self.assertEqual(reseed.carried_restage(self.original, None)["carriedFrom"], "resume-3/reseed-verify.json")
        self.write_last("reseed-verify.json", {"restageOldId": {"httpStatus": 409, "errorCode": "member_id_reserved"}})
        self.assertIsNone(reseed.carried_restage(self.original, self.last))
        self.write_last("reseed-verify.json", {"restageOldId": {"transportError": "RuntimeError"}})
        self.assertIsNone(reseed.carried_restage(self.original, None))

    def verify_run(self, number=4):
        out = self.original / f"resume-{number}"
        out.mkdir()
        return reseed_main.Run(self.sim, self.plan, out, self.private, sleep=lambda s: None,
                               original=self.original), out

    def run_verify(self, run):
        durable = {"result": "PASS", "label": "after-reseed", "sessionPresent": True, "acked": 3, "rejected": 0,
                   "unknown": 0, "missingAcked": [], "rejectedDelivered": [], "unknownDelivered": [], "duplicates": 0}
        FakeTunnel.created.clear()
        with patch.object(reseed_main, "PodTunnel", FakeTunnel), \
                patch.object(reseed_main.durable_stream, "verify_subscriber", return_value=durable) as verify, \
                patch.object(reseed_main.mqtt_helpers, "read_marker", return_value={"found": True, "sha256": "a" * 64}):
            run.step_verify()
        return verify

    def test_verify_resume_carries_the_restage_409_and_sends_no_mutation(self):
        before = reseed.hash_records(self.original)
        run, out = self.verify_run()
        run.step_resume_verify()
        self.assertEqual(sum(run.criteria.values()), 6)
        self.assertTrue((out / "durable-publish-reseed.jsonl").is_file())
        verify = self.run_verify(run)
        self.assertEqual(self.sim.requests, [])
        self.assertEqual((self.sim.replaced, self.sim.deleted), ([], []))
        restage = run.evidence["verify"]["restageOldId"]
        self.assertEqual((restage["httpStatus"], restage["errorCode"], restage["carriedFrom"]),
                         (409, "member_id_reserved", "resume-3/reseed-verify.json"))
        self.assertEqual(FakeTunnel.created[0][:2], ("narumq-2", "pod-new"))
        self.assertEqual(verify.call_args.args[-3:], (24883, out, "after-reseed"))
        judged = reseed.aggregate(run.criteria)
        self.assertEqual((judged["result"], judged["failedCriteria"]), ("PASS", []))
        self.assertEqual(reseed.hash_records(self.original, skip=out), before)

    def test_verify_without_an_earlier_restage_posts_once_with_the_restage_key(self):
        (self.last / "reseed-verify.json").unlink()
        run, out = self.verify_run()
        run.step_resume_verify()
        self.run_verify(run)
        self.assertEqual([r[:2] for r in self.sim.requests], [("POST", "/api/v1/cluster/members")])
        self.assertEqual(self.sim.requests[0][4], f"{RUN}-restage-3")
        self.assertEqual((self.sim.requests[0][2]["memberId"], self.sim.requests[0][2]["certificateSha256"]),
                         (3, "0" * 64))
        self.assertNotIn("carriedFrom", run.evidence["verify"]["restageOldId"])
        self.assertTrue(run.criteria["reseed.oldIdStage409MemberIdReserved"])

    def test_cli_verify_records_below_a_new_dir_and_reports_validation_failures(self):
        plan_path = Path(self.directory.name) / "plan.json"
        plan_path.write_text(json.dumps(plan()), encoding="utf-8")
        self.write_last("durable-verify-reseed-after-reseed.json", {**DURABLE_UNTOUCHED, "sessionPresent": True})
        args = ["resume", "--private", str(self.private), "--output", str(self.original), "--plan", str(plan_path),
                "--execute", "--from", "verify"]
        before = reseed.hash_records(self.original)
        with patch.object(reseed, "check_dependencies", return_value=[]), \
                patch.object(reseed_main, "Fixture", return_value=self.sim), \
                patch.object(reseed, "check_local_ports", return_value=[1]), \
                patch.object(reseed_main.Run, "step_observer", lambda run: None), patch("builtins.print"):
            self.assertEqual(reseed_main.main(args), 1)
        summary = json.loads((self.original / "resume-4" / "summary.json").read_text(encoding="utf-8"))
        self.assertEqual((summary["from"], summary["failedStep"], summary["errorCode"]),
                         ("verify", "resume_validate", "resume_state_mismatch:durable_session_touched"))
        self.assertTrue(summary["original"]["recordsUnchanged"])
        self.assertEqual(reseed.hash_records(self.original, skip=self.original / "resume-4"), before)
        self.assertEqual((self.sim.requests, self.sim.replaced, self.sim.deleted), ([], [], []))


class DependencyTests(unittest.TestCase):
    def test_missing_module_is_reported_by_name(self):
        self.assertEqual(reseed.check_dependencies(("json", "re")), ["json", "re"])
        with self.assertRaises(reseed.PreflightRejected) as caught:
            reseed.check_dependencies(("json", "no_such_module_for_reseed_test"))
        self.assertEqual(caught.exception.code, "dependency_missing:no_such_module_for_reseed_test")

    def test_required_modules_cover_mqtt_and_certificates(self):
        self.assertTrue({"paho.mqtt.client", "cryptography.x509"} <= set(reseed.REQUIRED_MODULES))

    def test_every_mode_stops_before_any_live_call(self):
        def fixture_must_not_be_built(*args, **kwargs):
            raise AssertionError("Fixture built before the dependency check")

        with tempfile.TemporaryDirectory() as text, \
                patch.object(reseed, "REQUIRED_MODULES", ("no_such_module_for_reseed_test",)), \
                patch.object(reseed_main, "Fixture", fixture_must_not_be_built), patch("builtins.print"):
            root = Path(text)
            (root / "plan.json").write_text(json.dumps(plan()), encoding="utf-8")
            fresh = root / RUN
            self.assertEqual(reseed_main.main(["run", "--private", str(root), "--output", str(fresh), "--plan",
                                               str(root / "plan.json"), "--execute"]), 1)
            summary = json.loads((fresh / "summary.json").read_text(encoding="utf-8"))
            self.assertEqual((summary["failedStep"], summary["errorCode"]),
                             ("dependencies", "dependency_missing:no_such_module_for_reseed_test"))
            self.assertEqual(sorted(item.name for item in fresh.iterdir()), ["summary.json"])
            original = root / "interrupted"
            original.mkdir()
            (original / "summary.json").write_text("{}", encoding="utf-8")
            before = reseed.hash_records(original)
            for number, step in enumerate(reseed.RESUME_FROM_STEPS, 1):
                self.assertEqual(reseed_main.main(["resume", "--private", str(root), "--output", str(original),
                                                   "--plan", str(root / "plan.json"), "--execute", "--from", step]), 1)
                summary = json.loads((original / f"resume-{number}" / "summary.json").read_text(encoding="utf-8"))
                self.assertEqual((summary["from"], summary["failedStep"], summary["errorCode"]),
                                 (step, "dependencies", "dependency_missing:no_such_module_for_reseed_test"))
                self.assertEqual(sorted(item.name for item in (original / f"resume-{number}").iterdir()), ["summary.json"])
            self.assertEqual(reseed.hash_records(original, recursive=False), before)

    def test_dependency_check_precedes_plan_loading(self):
        with tempfile.TemporaryDirectory() as text, \
                patch.object(reseed, "REQUIRED_MODULES", ("no_such_module_for_reseed_test",)), \
                patch("builtins.print"):
            root = Path(text)
            (root / "plan.json").write_text("not json", encoding="utf-8")
            output = root / RUN
            self.assertEqual(reseed_main.main(["run", "--private", str(root), "--output", str(output), "--plan",
                                               str(root / "plan.json"), "--execute"]), 1)
            self.assertEqual(json.loads((output / "summary.json").read_text(encoding="utf-8"))["failedStep"],
                             "dependencies")


if __name__ == "__main__":
    unittest.main()
