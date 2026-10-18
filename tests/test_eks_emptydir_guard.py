"""Fake-based tests of the EKS emptyDir bootstrap guard harness (validation, Pod construction, log parsing, judges).

These tests exercise harness logic against a scripted fixture. They are not the CLU-SCN-136 acceptance: that runs
tests/chaos/eks-emptydir-guard/main.py against a real disposable EKS fixture, so nothing here is scenario-tagged.
"""
from __future__ import annotations

import copy
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import unittest

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parents[1]
GUARD_DIR = ROOT / "tests/chaos/eks-emptydir-guard"
sys.path.insert(0, str(GUARD_DIR))
import emptydir_guard as guard  # noqa: E402

main_spec = importlib.util.spec_from_file_location("eks_emptydir_guard_main", GUARD_DIR / "main.py")
guard_main = importlib.util.module_from_spec(main_spec)
main_spec.loader.exec_module(guard_main)

RUN = "narumq-clu14-20261017T000000Z-abcdef"
CLUSTER = RUN
GUARD_POD = guard.guard_pod_name(RUN)
REFUSAL = ("cluster bootstrap refused: a configured peer reports an existing cluster; restore the original volume or "
           "replace this member with a new member ID (member {member} term {term} commit index {commit})")


def plan(**overrides) -> dict:
    value = {"runId": RUN, "clusterName": CLUSTER, "vpcId": "vpc-1", "targetMemberId": 3, "peerMemberId": 1,
             "expectedTerm": 6, "expectedCommitIndex": 100}
    value.update(overrides)
    return value


def bootstrap_config(node_id: int, ordinal: int) -> dict:
    return {"mode": "cluster", "clusterId": "narumq-cluster-1", "nodeId": node_id, "directory": "/data",
            "listenAddress": "0.0.0.0:7000", "advertisedAddress": guard.slot_address(ordinal),
            "localDnsName": guard.slot_dns(ordinal), "role": "bootstrap",
            "bootstrapMembers": [{"memberId": i + 1, "address": guard.slot_address(i), "dnsSan": guard.slot_dns(i),
                                  "certificateSha256": "0" * 64} for i in range(3)],
            "seedAddresses": [], "peerTLS": {"certFile": "/peer-tls/node.crt", "keyFile": "/peer-tls/node.key",
                                             "caFile": "/peer-tls/ca.crt"},
            "management": {"listenAddress": "0.0.0.0:8080", "auditDirectory": "/audit"}}


def configmap() -> dict:
    data = {"slot-0": "1", "slot-1": "2", "slot-2": "3"}
    for node_id, ordinal in ((1, 0), (2, 1), (3, 2)):
        data[f"node-{node_id}.json"] = json.dumps(bootstrap_config(node_id, ordinal))
    return {"kind": "ConfigMap", "data": data}


def secret() -> dict:
    keys = ["admin.token", "dashboard.key", "security.json", "ca.crt"] + [
        f"node-{i}.{ext}" for i in range(3) for ext in ("crt", "key")]
    return {"kind": "Secret", "data": {key: "x" for key in keys}}


def statefulset() -> dict:
    return {"spec": {"replicas": 3, "template": {"spec": {"containers": [{"name": "narumq", "image": "repo/narumq@sha256:1"}]}}}}


def broker(ordinal: int, restarts: int = 0, ready: bool = True, uid: str | None = None) -> dict:
    return {"metadata": {"name": f"narumq-{ordinal}", "uid": uid or f"uid-{ordinal}"},
            "spec": {"nodeName": f"node-{ordinal}"},
            "status": {"conditions": [{"type": "Ready", "status": "True" if ready else "False"}],
                       "containerStatuses": [{"name": "narumq", "restartCount": restarts}]}}


def guard_log(member=1, term=6, commit=100, before=(), after=(), exit_code=1, refusal=True) -> str:
    lines = ["DATA_DIRECTORY_PRESENT=1", "EMPTYDIR_BEFORE_BEGIN", *before, "EMPTYDIR_BEFORE_END"]
    if refusal:
        lines.append(json.dumps({"level": "ERROR", "msg": "narumq startup or runtime failure",
                                 "error": REFUSAL.format(member=member, term=term, commit=commit)}))
    lines += [f"NARUMQ_EXIT_CODE={exit_code}", "EMPTYDIR_AFTER_BEGIN", *after, "EMPTYDIR_AFTER_END"]
    return "\n".join(lines) + "\n"


def peer_log(term: int, node_id: int = 1) -> str:
    return "\n".join([
        json.dumps({"msg": "raft node status changed", "node_id": node_id, "role": "follower", "leader_id": 2, "term": 4}),
        json.dumps({"msg": "something else", "node_id": node_id, "term": 99}),
        json.dumps({"msg": "raft node status changed", "node_id": node_id, "role": "leader", "leader_id": node_id,
                    "term": term}),
    ]) + "\n"


class CommandFailed(RuntimeError):
    pass


class Clock:
    def __init__(self):
        self.now = 0.0

    def sleep(self, seconds):
        self.now += seconds

    def monotonic(self):
        return self.now


class FakeTunnel:
    instances: list = []

    def __init__(self, fixture, pod, pod_uid, management_port, mqtt_port):
        self.args = (pod, pod_uid, management_port, mqtt_port)
        self.closed = False
        FakeTunnel.instances.append(self)

    def start(self):
        return {"pod": self.args[0], "podUID": self.args[1]}

    def close(self):
        self.closed = True


class FakeFixture:
    kubectl = ["kubectl"]

    def __init__(self, guard_log_text: str | None = None, exit_code: int = 1, terminates: bool = True):
        self.statefulset = statefulset()
        self.configmap = configmap()
        self.secret = secret()
        self.brokers = [broker(i) for i in range(3)]
        self.brokers_after: list | None = None
        self.guard_pod: dict | None = None
        self.preexisting_guard_pod = False
        self.guard_log_text = guard_log() if guard_log_text is None else guard_log_text
        self.exit_code = exit_code
        self.terminates = terminates
        self.create_fails = False
        self.peer_logs = [peer_log(6), peer_log(6)]
        self.applied = [100, 100]
        self.quorum = True
        self.commands: list[list[str]] = []
        self.created: dict | None = None

    def get(self, *args):
        kind = args[0]
        if kind == "statefulset":
            return copy.deepcopy(self.statefulset)
        if kind == "configmap":
            return copy.deepcopy(self.configmap)
        if kind == "secret":
            return copy.deepcopy(self.secret)
        if kind == "pods":
            created = self.created is not None
            items = self.brokers_after if created and self.brokers_after is not None else self.brokers
            return {"items": copy.deepcopy(items)}
        raise AssertionError(args)

    def get_node(self, name):
        return {"spec": {"providerID": f"aws:///ap-northeast-2a/i-{name}"}}

    def get_pod(self, name):
        assert name == GUARD_POD
        if self.preexisting_guard_pod and self.created is None and self.guard_pod is None:
            return {"metadata": {"name": name, "uid": "other", "labels": {}}}
        return copy.deepcopy(self.guard_pod)

    def command(self, args, timeout=90, stdin=None):
        self.commands.append(list(args))
        if "create" in args:
            if self.create_fails:
                raise CommandFailed("command exit 1")
            self.created = json.loads(stdin)
            status = {"phase": "Failed", "containerStatuses": [{"name": "narumq", "restartCount": 0, "state": {
                "terminated": {"exitCode": self.exit_code, "reason": "Error"}}}]} if self.terminates else {
                "phase": "Pending", "containerStatuses": [{"name": "narumq", "restartCount": 0, "state": {
                    "waiting": {"reason": "ImagePullBackOff"}}}]}
            self.guard_pod = {"metadata": {**self.created["metadata"], "uid": "guard-uid"}, "spec": self.created["spec"],
                              "status": status}
            return ""
        if "logs" in args:
            name = args[args.index("logs") + 1]
            if name == GUARD_POD:
                return self.guard_log_text
            assert name == "narumq-0"
            return self.peer_logs.pop(0) if len(self.peer_logs) > 1 else self.peer_logs[0]
        if "delete" in args:
            self.guard_pod = None
            return ""
        raise AssertionError(args)

    def api(self, path, port=0):
        if path == "/api/v1/cluster":
            return {"quorumAvailable": self.quorum}
        if path == "/api/v1/nodes":
            value = self.applied.pop(0) if len(self.applied) > 1 else self.applied[0]
            return [{"nodeId": "1", "queryStatus": "OK", "lastAppliedIndex": value},
                    {"nodeId": "2", "queryStatus": "OK", "lastAppliedIndex": 0}]
        raise AssertionError(path)


def run_guard(fixture: FakeFixture, plan_value: dict | None = None, guard_seconds: float = 30) -> tuple[dict, Path]:
    FakeTunnel.instances = []
    clock = Clock()
    output = Path(tempfile.mkdtemp(prefix="emptydir-guard-test-"))
    run = guard_main.GuardRun(fixture, guard.load_plan(plan_value or plan(), RUN), output, tunnel_factory=FakeTunnel,
                              ownership=lambda fx, pl, ids: {"instanceIds": ids}, guard_seconds=guard_seconds,
                              settle_seconds=4, cleanup_seconds=10, sleep=clock.sleep, monotonic=clock.monotonic)
    return run.execute(), output


class PlanTests(unittest.TestCase):
    def test_valid_plan_normalises_ids(self):
        loaded = guard.load_plan(plan(targetMemberId="3", peerMemberId=1), RUN)
        self.assertEqual((loaded["targetMemberId"], loaded["peerMemberId"]), (3, 1))

    def test_rejections_have_stable_codes(self):
        cases = [
            (plan(runId="other"), "plan_invalid:runId"),
            (plan(clusterName="prod-cluster"), "plan_invalid:clusterName"),
            (plan(peerMemberId=3), "plan_invalid:peerMemberId"),
            (plan(targetMemberId="03"), "plan_invalid:targetMemberId"),
            (plan(targetMemberId=True), "plan_invalid:targetMemberId"),
            (plan(expectedTerm=0), "plan_invalid:expectedTerm"),
            (plan(expectedCommitIndex="100"), "plan_invalid:expectedCommitIndex"),
            (plan(surprise=1), "plan_invalid:unknown_key"),
            ({key: value for key, value in plan().items() if key != "vpcId"}, "plan_missing:vpcId"),
            ({key: value for key, value in plan().items() if key != "peerMemberId"}, "plan_missing:peerMemberId"),
            ([], "plan_invalid:plan"),
        ]
        for raw, code in cases:
            with self.subTest(code=code, raw=raw), self.assertRaises(guard.GuardRejected) as caught:
                guard.load_plan(raw, RUN)
            self.assertEqual(caught.exception.code, code)

    def test_expectation_is_optional(self):
        loaded = guard.load_plan({k: v for k, v in plan().items() if not k.startswith("expected")}, RUN)
        self.assertNotIn("expectedTerm", loaded)


class MemberConfigTests(unittest.TestCase):
    def test_real_config_is_resolved_from_live_slots(self):
        facts = guard.member_config(configmap(), guard.load_plan(plan(), RUN))
        self.assertEqual((facts["targetOrdinal"], facts["peerOrdinal"]), (2, 0))
        self.assertEqual([peer["memberId"] for peer in facts["peers"]], [1, 2])

    def test_rejections(self):
        base = guard.load_plan(plan(), RUN)

        def mutate(change):
            cm = configmap()
            change(cm["data"])
            return cm

        def rewrite(cm_data, **fields):
            config = json.loads(cm_data["node-3.json"])
            config.update(fields)
            cm_data["node-3.json"] = json.dumps(config)

        cases = [
            (mutate(lambda d: d.pop("slot-2")), "target_member_slot_missing"),
            (mutate(lambda d: d.pop("slot-0")), "peer_member_slot_missing"),
            (mutate(lambda d: d.pop("node-3.json")), "target_member_config_missing"),
            (mutate(lambda d: d.update({"node-3.json": "{"})), "target_member_config_invalid"),
            (mutate(lambda d: rewrite(d, nodeId=2)), "target_member_config_invalid"),
            (mutate(lambda d: rewrite(d, role="joiner")), "target_member_config_not_bootstrap"),
            (mutate(lambda d: rewrite(d, directory="/var/data")), "target_member_config_invalid"),
            (mutate(lambda d: rewrite(d, advertisedAddress="elsewhere:7000")), "target_member_config_invalid"),
            (mutate(lambda d: rewrite(d, peerTLS={"certFile": "/x", "keyFile": "/y", "caFile": "/z"})),
             "target_member_config_invalid"),
            (mutate(lambda d: d.update({"slot-1": "x"})), "slot_invalid"),
        ]
        for cm, code in cases:
            with self.subTest(code=code), self.assertRaises(guard.GuardRejected) as caught:
                guard.member_config(cm, base)
            self.assertEqual(caught.exception.code, code)


class PodTests(unittest.TestCase):
    def setUp(self):
        self.pod = guard.build_pod(guard.load_plan(plan(), RUN), "repo/narumq@sha256:1", 2)

    def test_data_directory_is_emptydir_without_any_claim(self):
        volumes = {v["name"]: v for v in self.pod["spec"]["volumes"]}
        self.assertEqual(volumes["data"], {"name": "data", "emptyDir": {}})
        self.assertFalse(any("persistentVolumeClaim" in v for v in volumes.values()))
        mounts = {m["mountPath"]: m["name"] for m in self.pod["spec"]["containers"][0]["volumeMounts"]}
        self.assertEqual(mounts["/data"], "data")
        self.assertEqual(mounts["/audit"], "audit")

    def test_member_certificate_comes_from_the_slot_ordinal(self):
        tls = next(v for v in self.pod["spec"]["volumes"] if v["name"] == "peer-tls")["secret"]["items"]
        self.assertEqual(tls, [{"key": "node-2.crt", "path": "node.crt"}, {"key": "node-2.key", "path": "node.key"},
                               {"key": "ca.crt", "path": "ca.crt"}])

    def test_runs_the_members_real_config_once_and_never_selected_by_services(self):
        command = self.pod["spec"]["containers"][0]["command"]
        self.assertEqual(command[:2], ["/bin/sh", "-c"])
        self.assertIn('/narumq -cluster-config "/configs/node-3.json"', command[2])
        self.assertNotIn("__MEMBER__", command[2])
        self.assertEqual(self.pod["spec"]["restartPolicy"], "Never")
        labels = self.pod["metadata"]["labels"]
        self.assertNotEqual(labels["app"], "narumq")
        self.assertEqual(labels["narumq.io/acceptance-run"], RUN)
        self.assertTrue(guard.owned_guard_pod(self.pod, RUN))
        self.assertFalse(guard.owned_guard_pod(self.pod, "another-run"))
        self.assertEqual(self.pod["metadata"]["name"], GUARD_POD)

    def test_pod_name_is_stable_dns_label(self):
        self.assertRegex(GUARD_POD, r"^narumq-emptydir-guard-[0-9a-f]{10}$")
        self.assertEqual(GUARD_POD, guard.guard_pod_name(RUN))
        self.assertNotEqual(GUARD_POD, guard.guard_pod_name(RUN + "x"))


class ParsingTests(unittest.TestCase):
    def test_refusal_report(self):
        refusal = guard.parse_refusals(guard_log(member=1, term=6, commit=100))[0]
        self.assertEqual((refusal["memberId"], refusal["term"], refusal["commitIndex"]), (1, 6, 100))

    def test_refusal_without_report_has_no_numbers(self):
        refusal = guard.parse_refusals("level=error msg=\"cluster bootstrap refused: data directory is empty\"\n")[0]
        self.assertIsNone(refusal["memberId"])

    def test_listing_requires_ordered_unique_markers(self):
        log = guard_log(before=["/data/x"], after=["/data/x", "/data/y"])
        self.assertEqual(guard.parse_listing(log, "BEFORE"), ["/data/x"])
        self.assertEqual(guard.parse_listing(log, "AFTER"), ["/data/x", "/data/y"])
        self.assertIsNone(guard.parse_listing("EMPTYDIR_BEFORE_END\nEMPTYDIR_BEFORE_BEGIN\n", "BEFORE"))
        self.assertIsNone(guard.parse_listing(guard_log() + guard_log(), "BEFORE"))
        self.assertIsNone(guard.parse_listing("", "AFTER"))

    def test_exit_marker(self):
        self.assertEqual(guard.parse_exit_marker(guard_log(exit_code=1)), 1)
        self.assertIsNone(guard.parse_exit_marker(guard_log() + guard_log()))
        self.assertIsNone(guard.parse_exit_marker("starting\n"))

    def test_latest_raft_term_uses_the_members_own_last_status_entry(self):
        self.assertEqual(guard.latest_raft_term(peer_log(6), 1), 6)
        self.assertIsNone(guard.latest_raft_term(peer_log(6, node_id=2), 1))
        self.assertIsNone(guard.latest_raft_term("", 1))
        text = 'level=INFO msg="raft node status changed" node_id=1 role=leader leader_id=1 term=8\n'
        self.assertEqual(guard.latest_raft_term(text, 1), 8)


def observation(**overrides) -> dict:
    value = {"exitCode": 1, "logs": guard_log(),
             "peer": {"termBefore": 6, "termAfter": 6, "appliedBefore": 100, "appliedAfter": 100}}
    value.update(overrides)
    return value


class JudgeTests(unittest.TestCase):
    def judged(self, obs=None, **plan_overrides):
        return guard.judge_guard(guard.load_plan(plan(**plan_overrides), RUN), obs or observation())["criteria"]

    def test_matching_report_passes_every_judged_criterion(self):
        criteria = self.judged()
        self.assertTrue(all(value is True for value in criteria.values()), criteria)

    def test_exit_code_must_be_one_in_container_and_log(self):
        self.assertFalse(self.judged(observation(exitCode=0))["guard.exit1"])
        self.assertFalse(self.judged(observation(exitCode=None))["guard.exit1"])
        self.assertFalse(self.judged(observation(logs=guard_log(exit_code=2)))["guard.exit1"])

    def test_refusal_must_be_logged(self):
        criteria = self.judged(observation(logs=guard_log(refusal=False)))
        self.assertFalse(criteria["guard.refusalLogged"])
        self.assertFalse(criteria["guard.reportFromProgressedPeer"])

    def test_report_must_come_from_the_planned_peer(self):
        self.assertFalse(self.judged(observation(logs=guard_log(member=2)))["guard.reportFromProgressedPeer"])

    def test_progress_boundary_is_term_over_one_or_commit_over_three(self):
        def boundary(term, commit):
            logs = guard_log(term=term, commit=commit)
            obs = observation(logs=logs, peer={"termBefore": term, "termAfter": term, "appliedBefore": commit,
                                               "appliedAfter": commit})
            return self.judged(obs, expectedTerm=term, expectedCommitIndex=commit)["guard.progressBoundaryExceeded"]
        self.assertFalse(boundary(1, 3))
        self.assertTrue(boundary(2, 3))
        self.assertTrue(boundary(1, 4))

    def test_reported_term_must_equal_the_live_term_before_and_after(self):
        for peer in ({"termBefore": 5, "termAfter": 6, "appliedBefore": 100, "appliedAfter": 100},
                     {"termBefore": 6, "termAfter": 7, "appliedBefore": 100, "appliedAfter": 100},
                     {"termBefore": None, "termAfter": None, "appliedBefore": 100, "appliedAfter": 100}):
            with self.subTest(peer=peer):
                self.assertFalse(self.judged(observation(peer=peer))["guard.reportedTermEqualsLive"])

    def test_reported_commit_index_must_lie_within_live_applied_window(self):
        def commit(before, after, reported=100):
            peer = {"termBefore": 6, "termAfter": 6, "appliedBefore": before, "appliedAfter": after}
            return self.judged(observation(logs=guard_log(commit=reported), peer=peer),
                               expectedCommitIndex=reported)["guard.reportedCommitIndexMatchesLiveApplied"]
        self.assertTrue(commit(100, 100))
        self.assertTrue(commit(98, 103))
        self.assertFalse(commit(101, 101))
        self.assertFalse(commit(90, 99))
        self.assertFalse(commit(None, 100))

    def test_plan_expectation_is_exact_when_given(self):
        self.assertFalse(self.judged(expectedTerm=7)["guard.reportMatchesPlanExpectation"])
        self.assertFalse(self.judged(expectedCommitIndex=99)["guard.reportMatchesPlanExpectation"])
        raw = {k: v for k, v in plan().items() if not k.startswith("expected")}
        result = guard.judge_guard(guard.load_plan(raw, RUN), observation())["criteria"]
        self.assertTrue(result["guard.reportMatchesPlanExpectation"])

    def test_emptydir_must_be_listed_and_empty_before_and_after(self):
        self.assertFalse(self.judged(observation(logs=guard_log(after=["/data/raft"])))["emptyDir.unchangedAndEmpty"])
        self.assertFalse(self.judged(observation(logs=guard_log(before=["/data/x"], after=["/data/x"])))
                         ["emptyDir.unchangedAndEmpty"])
        self.assertFalse(self.judged(observation(logs=guard_log().replace("EMPTYDIR_AFTER_END\n", "")))
                         ["emptyDir.listedBeforeAndAfter"])
        self.assertFalse(self.judged(observation(logs=guard_log().replace("DATA_DIRECTORY_PRESENT=1", "DATA_DIRECTORY_PRESENT=0")))
                         ["emptyDir.listedBeforeAndAfter"])

    def test_aggregate_requires_every_named_criterion(self):
        criteria = {name: True for name in guard.CRITERIA}
        self.assertEqual(guard.aggregate(criteria)["result"], "PASS")
        self.assertEqual(guard.aggregate({**criteria, "guard.exit1": False})["failedCriteria"], ["guard.exit1"])
        self.assertEqual(guard.aggregate({name: True for name in guard.CRITERIA[1:]})["result"], "FAIL")
        self.assertEqual(guard.aggregate(criteria, failed_step="guard")["result"], "FAIL")
        self.assertEqual(guard.aggregate({**criteria, "guard.exit1": "yes"})["result"], "FAIL")


class WaitTests(unittest.TestCase):
    def test_wait_for_termination_timeout_reports_waiting_reason(self):
        fixture = FakeFixture(terminates=False)
        fixture.command(["create", "-f", "-"], stdin=json.dumps(guard.build_pod(guard.load_plan(plan(), RUN), "i", 2)))
        clock = Clock()
        with self.assertRaises(guard.GuardRejected) as caught:
            guard.wait_for_termination(fixture, GUARD_POD, "guard-uid", 9, sleep=clock.sleep, monotonic=clock.monotonic)
        self.assertEqual(caught.exception.code, "guard_timeout")
        self.assertIn("ImagePullBackOff", str(caught.exception))

    def test_replaced_or_vanished_pod_is_rejected(self):
        fixture = FakeFixture()
        with self.assertRaises(guard.GuardRejected) as caught:
            guard.wait_for_termination(fixture, GUARD_POD, "guard-uid", 5)
        self.assertEqual(caught.exception.code, "guard_pod_vanished")
        fixture.command(["create", "-f", "-"], stdin=json.dumps(guard.build_pod(guard.load_plan(plan(), RUN), "i", 2)))
        with self.assertRaises(guard.GuardRejected) as caught:
            guard.wait_for_termination(fixture, GUARD_POD, "different-uid", 5)
        self.assertEqual(caught.exception.code, "guard_pod_replaced")

    def test_wait_for_applied_returns_the_value_at_deadline(self):
        fixture = FakeFixture()
        fixture.applied = [90]
        clock = Clock()
        self.assertEqual(guard.wait_for_applied(fixture, 1, 1, 100, 4, sleep=clock.sleep, monotonic=clock.monotonic), 90)
        fixture.applied = [90, 100]
        self.assertEqual(guard.wait_for_applied(fixture, 1, 1, 100, 4, sleep=clock.sleep, monotonic=clock.monotonic), 100)


class RunTests(unittest.TestCase):
    def failed(self, report: dict) -> set:
        return set(report["failedCriteria"])

    def test_matching_refusal_passes_all_criteria_and_cleans_up(self):
        fixture = FakeFixture()
        report, output = run_guard(fixture)
        self.assertEqual(report["result"], "PASS", report)
        self.assertEqual(report["failedCriteria"], [])
        self.assertTrue(all(report["criteria"][name] is True for name in guard.CRITERIA))
        self.assertIsNone(fixture.guard_pod)
        self.assertTrue(all(tunnel.closed for tunnel in FakeTunnel.instances))
        self.assertTrue((output / "guard-evidence.json").is_file())
        self.assertEqual((output / "guard-pod.log").read_text(encoding="utf-8"), guard_log())
        self.assertTrue(report["facts"]["commitIndexEqualsLiveAppliedExactly"])
        self.assertEqual(fixture.created["spec"]["volumes"][3], {"name": "data", "emptyDir": {}})
        self.assertEqual(sum("delete" in command for command in fixture.commands), 1)

    def test_guard_exit_zero_fails(self):
        report, _ = run_guard(FakeFixture(guard_log_text=guard_log(exit_code=0), exit_code=0))
        self.assertEqual(report["result"], "FAIL")
        self.assertIn("guard.exit1", self.failed(report))

    def test_file_created_in_emptydir_fails(self):
        report, _ = run_guard(FakeFixture(guard_log_text=guard_log(after=["/data/raft"])))
        self.assertEqual(self.failed(report), {"emptyDir.unchangedAndEmpty"})

    def test_term_changed_during_run_fails(self):
        fixture = FakeFixture()
        fixture.peer_logs = [peer_log(6), peer_log(7)]
        report, _ = run_guard(fixture)
        self.assertEqual(self.failed(report), {"guard.reportedTermEqualsLive"})

    def test_commit_index_not_matching_live_fails(self):
        report, _ = run_guard(FakeFixture(guard_log_text=guard_log(commit=99), ), plan(expectedCommitIndex=99))
        self.assertEqual(self.failed(report), {"guard.reportedCommitIndexMatchesLiveApplied"})

    def test_plan_expectation_mismatch_fails(self):
        report, _ = run_guard(FakeFixture(), plan(expectedTerm=7))
        self.assertEqual(self.failed(report), {"guard.reportMatchesPlanExpectation"})

    def test_restarted_broker_after_run_fails(self):
        fixture = FakeFixture()
        fixture.brokers_after = [broker(0), broker(1, restarts=1), broker(2)]
        report, _ = run_guard(fixture)
        self.assertEqual(self.failed(report), {"brokers.threeReadyUnchangedAfter"})

    def test_quorum_loss_after_run_fails(self):
        fixture = FakeFixture()
        calls = {"n": 0}
        original = fixture.api

        def api(path, port=0):
            if path == "/api/v1/cluster":
                calls["n"] += 1
                return {"quorumAvailable": calls["n"] == 1}
            return original(path, port)
        fixture.api = api
        report, _ = run_guard(fixture)
        self.assertEqual(self.failed(report), {"quorum.availableAfter"})

    def test_not_three_ready_brokers_stops_before_any_mutation(self):
        fixture = FakeFixture()
        fixture.brokers = [broker(0), broker(1), broker(2, ready=False)]
        report, _ = run_guard(fixture)
        self.assertEqual((report["result"], report["errorCode"], report["failedStep"]),
                         ("FAIL", "brokers_not_three_ready", "preflight"))
        self.assertIsNone(fixture.created)
        self.assertFalse(any("create" in command for command in fixture.commands))

    def test_missing_secret_key_stops_before_any_mutation(self):
        fixture = FakeFixture()
        del fixture.secret["data"]["node-2.key"]
        report, _ = run_guard(fixture)
        self.assertEqual(report["errorCode"], "secret_key_missing:node-2.key")
        self.assertIsNone(fixture.created)

    def test_peer_term_unobservable_stops_before_creating_the_pod(self):
        fixture = FakeFixture()
        fixture.peer_logs = ["no status lines\n"]
        report, _ = run_guard(fixture)
        self.assertEqual(report["errorCode"], "peer_term_unobservable")
        self.assertIsNone(fixture.created)

    def test_preexisting_foreign_pod_with_the_name_is_never_deleted(self):
        fixture = FakeFixture()
        fixture.preexisting_guard_pod = True
        report, _ = run_guard(fixture)
        self.assertEqual(report["errorCode"], "guard_pod_exists")
        self.assertFalse(report["criteria"]["cleanup.guardPodDeleted"])
        self.assertFalse(any("delete" in command for command in fixture.commands))

    def test_create_failure_is_reported_and_nothing_is_deleted(self):
        fixture = FakeFixture()
        fixture.create_fails = True
        report, _ = run_guard(fixture)
        self.assertEqual((report["errorCode"], report["failedStep"]), ("guard_pod_create_failed", "create"))
        self.assertTrue(report["criteria"]["cleanup.guardPodDeleted"])
        self.assertFalse(any("delete" in command for command in fixture.commands))

    def test_timeout_still_deletes_the_owned_pod_and_fails(self):
        fixture = FakeFixture(terminates=False)
        report, _ = run_guard(fixture, guard_seconds=9)
        self.assertEqual((report["result"], report["errorCode"], report["failedStep"]), ("FAIL", "guard_timeout", "guard"))
        self.assertTrue(report["criteria"]["cleanup.guardPodDeleted"])
        self.assertIsNone(fixture.guard_pod)
        for name in ("guard.exit1", "guard.refusalLogged", "emptyDir.unchangedAndEmpty"):
            self.assertFalse(report["criteria"][name])


class EntryPointTests(unittest.TestCase):
    def test_execute_flag_is_required(self):
        with self.assertRaises(SystemExit) as caught:
            guard_main.main(["--private", "p", "--output", "o", "--plan", "x"])
        self.assertEqual(caught.exception.code, 2)

    def test_missing_private_files_have_a_stable_code(self):
        with tempfile.TemporaryDirectory() as directory, self.assertRaises(guard.GuardRejected) as caught:
            guard_main.check_private(Path(directory))
        self.assertEqual(caught.exception.code, "private_file_missing:kubectl.exe")

    def test_missing_aws_cli_has_a_stable_code(self):
        with self.assertRaises(guard.GuardRejected) as caught:
            guard_main.check_dependencies(which=lambda name: None)
        self.assertEqual(caught.exception.code, "dependency_missing:aws")

    def test_scenario_rejects_substituted_members_or_progress_before_external_access(self):
        for key, value in (("targetMemberId", 4), ("peerMemberId", 2),
                           ("expectedTerm", 7), ("expectedCommitIndex", 101)):
            with self.subTest(field=key), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                evidence = root / RUN
                evidence.mkdir()
                plan_file = root / "plan.json"
                plan_file.write_text(json.dumps(plan(**{key: value})), encoding="utf-8")
                report = guard_main.scenario_emptydir_bootstrap_guard.__wrapped__(
                    root / "missing-private", evidence, plan_file)
                self.assertEqual((report["result"], report["errorCode"]),
                                 ("FAIL", f"scenario_input_mismatch:{key}"))


if __name__ == "__main__":
    unittest.main()
