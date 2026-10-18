"""Offline unit tests for the DRN-SCN-022 judgments (replace_restart_judge.py) and the harness helpers in main.py."""

from __future__ import annotations

import copy
import sys
import unittest
from pathlib import Path

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent))

import replace_restart_judge as rrj  # noqa: E402

DIGEST_NEW = "sha256:" + "b" * 64
DIGEST_OLD = "sha256:" + "a" * 64
TARGET = {"image": "registry.example/narumq@" + DIGEST_NEW, "releaseVersion": "0.0.1", "features": ["a-feature", "b-feature"]}
CONTROLLER_USER = "system:serviceaccount:narumq:narumq-rollout-controller"
OPERATIONS = {0: "op-0", 1: "op-1", 2: "op-2"}
KEYS = {0: "rollout-key-0", 1: "rollout-key-1", 2: "rollout-key-2"}
OLD_CONTROLLER = {"name": "ctl-old", "uid": "ctl-uid-old", "ip": "10.0.0.1"}
NEW_CONTROLLER = {"name": "ctl-new", "uid": "ctl-uid-new", "ip": "10.0.0.2", "ready": True}
RESTART = {"deleted": OLD_CONTROLLER, "leaderPod": NEW_CONTROLLER, "deletedAt": 915.0, "leaderChangedAt": 925.0}
INITIAL = {f"narumq-{o}": {"uid": f"old-uid-{o}", "image": "registry.example/narumq@" + DIGEST_OLD, "imageID": "docker://x@" + DIGEST_OLD,
                          "ready": True, "deleting": None} for o in range(3)}
FINAL = {f"narumq-{o}": {"uid": f"new-uid-{o}", "image": TARGET["image"], "imageID": "docker://x@" + DIGEST_NEW,
                        "ready": True, "deleting": None} for o in range(3)}


def features(version: str, members: int = 3, feature_list: list[str] | None = None) -> tuple[int, dict]:
    return 200, {"data": {"members": [{"nodeId": str(i + 1), "releaseVersion": version, "features": feature_list or []} for i in range(members)]}}


WORKLOAD = {"image": TARGET["image"], "replicas": 3, "updateStrategy": "OnDelete"}


def status(**overrides):
    base = {"phase": "Draining", "currentOrdinal": 0, "drainOperationID": "op-0", "attemptKey": "rollout-key-0", "drainState": "DRAINING",
            "drainSafeToStop": False}
    return {**base, **overrides}


def drain(state="DRAINING", safe=False, phase="WAITING_FOR_LB", node="1", operation="op-0"):
    return 200, {"data": {"operationId": operation, "nodeId": node, "state": state, "phase": phase, "safeToStop": safe}}


def timeline_samples() -> list[dict]:
    """Status/Pod samples of a clean rollout: restart during ordinal 0, then ordinals 0..2 replaced in order."""
    samples = []
    t = 900.0
    pods = {name: pod["uid"] for name, pod in INITIAL.items()}
    for ordinal in range(3):
        for phase in ("Draining", "DrainSafe", "AwaitingReplacement"):
            t += 10
            samples.append({"at": t, "phase": phase, "ordinal": ordinal, "attemptKey": KEYS[ordinal], "operation": OPERATIONS[ordinal],
                            "completed": list(range(ordinal)), "failureCode": None, "pods": dict(pods)})
        pods = {**pods, f"narumq-{ordinal}": f"new-uid-{ordinal}"}
        t += 10
        samples.append({"at": t, "phase": "AwaitingReplacement", "ordinal": ordinal, "attemptKey": KEYS[ordinal], "operation": OPERATIONS[ordinal],
                        "completed": list(range(ordinal)), "failureCode": None, "pods": dict(pods)})
    return samples


def proxy_log() -> list[dict]:
    log = []
    for ordinal in range(3):
        base = 900.0 + 10 * ordinal
        log.append({"at": base, "client": OLD_CONTROLLER["ip"] if ordinal == 0 else NEW_CONTROLLER["ip"], "method": "POST",
                    "path": f"/api/v1/nodes/{ordinal + 1}/drain", "action": "post_params", "idempotencyKey": KEYS[ordinal]})
        log.append({"at": 1020.0 + ordinal, "client": NEW_CONTROLLER["ip"], "method": "GET", "path": f"/api/v1/nodes/{ordinal + 1}/drain",
                    "action": "relay", "idempotencyKey": None})
    return log


def approval(index: int, **detail_overrides) -> dict:
    details = {"rolloutUid": "rollout-uid", "ordinal": index, "nodeId": index + 1, "podUid": f"old-uid-{index}",
               "attemptKey": KEYS[index], "drainOperationId": OPERATIONS[index], "targetImage": TARGET["image"],
               "targetReleaseVersion": "0.0.1", "drainPhase": "COMPLETED", "drainState": "SAFE_TO_STOP", "drainSafeToStop": True,
               "connectionsInitial": 4, "connectionsRemaining": 0, "sessionsReady": 4, "sessionsTakenOver": 4, "sessionsFailed": 0,
               "pendingWAL": 0, "underReplicated": 0, "drainFailureCode": "", "blockingReason": "", "safeToStopObservedAt": "2026-10-17T00:00:00Z"}
    return {"action": "rollout.pod.delete_approved", "result": "ALLOWED", "reason": "safe_to_stop", "eventId": f"event-{index}",
            "requestId": f"event-{index}", "target": {"type": "broker_pod", "id": f"narumq-{index}"},
            "producer": {"component": "ROLLOUT_CONTROLLER", "instanceId": "ctl-uid-new", "version": "1.0.0"},
            "actor": {"subject": "narumq-rollout-controller"}, "details": {**details, **detail_overrides}}


def failed(rows) -> list[str]:
    return [name for name, ok, _ in rows if not ok]


class PrerequisiteTests(unittest.TestCase):
    def test_target_must_be_synthetic_0_0_1_digest(self):
        rrj.check_target(TARGET)
        for bad in ({**TARGET, "releaseVersion": "0.1.0"}, {**TARGET, "image": "registry.example/narumq:tag"},
                    {**TARGET, "features": ["b-feature", "a-feature"]}, {**TARGET, "features": []}):
            with self.assertRaises(rrj.ReplaceRestartPrerequisiteError):
                rrj.check_target(bad)

    def test_drain_params_must_outlast_a_restart_and_the_controller_deadline(self):
        rrj.check_drain_params(240_000, 600_000)
        for lb, timeout in ((1_000, 600_000), (240_000, 240_000), (240_000, 900_000)):
            with self.assertRaises(rrj.ReplaceRestartPrerequisiteError):
                rrj.check_drain_params(lb, timeout)

    def test_fixture_must_be_three_ready_brokers_at_0_0_0_with_target_in_template(self):
        rrj.check_fixture_before(WORKLOAD, INITIAL, features("0.0.0"), TARGET)
        cases = {
            "template not at target": (dict(WORKLOAD, image="registry.example/narumq@" + DIGEST_OLD), INITIAL, features("0.0.0")),
            "not OnDelete": (dict(WORKLOAD, updateStrategy="RollingUpdate"), INITIAL, features("0.0.0")),
            "fixture already at 0.0.1": (WORKLOAD, INITIAL, features("0.0.1")),
            "published release instead of 0.0.0": (WORKLOAD, INITIAL, features("1.2.3")),
            "mixed versions": (WORKLOAD, INITIAL, (200, {"data": {"members": [{"releaseVersion": "0.0.0"}, {"releaseVersion": "0.0.0"},
                                                                                {"releaseVersion": "0.0.1"}]}})),
            "two members": (WORKLOAD, INITIAL, features("0.0.0", members=2)),
            "management unavailable": (WORKLOAD, INITIAL, (503, None)),
            "pod already on target": (WORKLOAD, {**INITIAL, "narumq-1": {**INITIAL["narumq-1"], "imageID": "docker://x@" + DIGEST_NEW}}, features("0.0.0")),
            "pod not ready": (WORKLOAD, {**INITIAL, "narumq-2": {**INITIAL["narumq-2"], "ready": False}}, features("0.0.0")),
            "missing pod": (WORKLOAD, {k: v for k, v in INITIAL.items() if k != "narumq-2"}, features("0.0.0")),
        }
        for label, (workload, pods, feat) in cases.items():
            with self.subTest(label), self.assertRaises(rrj.ReplaceRestartPrerequisiteError):
                rrj.check_fixture_before(workload, pods, feat, TARGET)

    def test_proxy_routing_needs_both_management_names(self):
        aliases = [{"ip": "10.9.9.9", "hostnames": ["narumq-management.narumq.svc", "narumq-management.narumq.svc.cluster.local"]}]
        self.assertTrue(rrj.routes_through_proxy(aliases, "10.9.9.9", "narumq"))
        self.assertFalse(rrj.routes_through_proxy(aliases, "10.1.1.1", "narumq"))
        self.assertFalse(rrj.routes_through_proxy([{"ip": "10.9.9.9", "hostnames": ["narumq-management.narumq.svc"]}], "10.9.9.9", "narumq"))
        self.assertFalse(rrj.routes_through_proxy(None, "10.9.9.9", "narumq"))


class RestartPreconditionTests(unittest.TestCase):
    def test_active_unsafe_drain_allows_restart(self):
        self.assertEqual(failed(rrj.judge_restart_precondition(status(), drain())), [])

    def test_restart_after_safe_to_stop_is_rejected(self):
        self.assertTrue(failed(rrj.judge_restart_precondition(status(drainState="SAFE_TO_STOP", drainSafeToStop=True), drain())))
        self.assertTrue(failed(rrj.judge_restart_precondition(status(), drain("SAFE_TO_STOP", True, "COMPLETED"))))
        self.assertTrue(failed(rrj.judge_restart_precondition(status(deleteIntent=True), drain())))

    def test_other_operation_node_phase_or_missing_drain_is_rejected(self):
        self.assertTrue(failed(rrj.judge_restart_precondition(status(), drain(operation="other"))))
        self.assertTrue(failed(rrj.judge_restart_precondition(status(), drain(node="2"))))
        self.assertTrue(failed(rrj.judge_restart_precondition(status(phase="DrainSafe"), drain())))
        self.assertTrue(failed(rrj.judge_restart_precondition(status(drainOperationID=None), drain())))
        self.assertTrue(failed(rrj.judge_restart_precondition(status(), (404, None))))
        self.assertTrue(failed(rrj.judge_restart_precondition(status(), (200, {"data": {}}))))


class ContinuityTests(unittest.TestCase):
    before = status(currentPodUID="old-uid-0")

    def judge(self, samples=None, log=None, after=None, restart=RESTART):
        return rrj.judge_continuity(self.before, restart, timeline_samples() if samples is None else samples,
                                    proxy_log() if log is None else log, drain() if after is None else after)

    def test_same_operation_and_key_continue(self):
        self.assertEqual(failed(self.judge()), [])

    def test_unproxied_run_judges_status_and_broker_only(self):
        rows = rrj.judge_continuity(self.before, RESTART, timeline_samples(), None, drain())
        self.assertEqual(failed(rows), [])
        self.assertEqual(len(rows), 3)

    def test_new_key_or_operation_after_restart_fails(self):
        for field, value in (("attemptKey", "rollout-other"), ("operation", "op-other")):
            samples = timeline_samples()
            for sample in samples:
                if sample["at"] >= RESTART["leaderChangedAt"] and sample["ordinal"] == 0:
                    sample[field] = value
            self.assertTrue(failed(self.judge(samples=samples)), field)

    def test_no_sample_after_restart_fails(self):
        self.assertTrue(failed(self.judge(samples=[s for s in timeline_samples() if s["at"] < RESTART["leaderChangedAt"]])))

    def test_broker_reporting_another_operation_fails(self):
        self.assertTrue(failed(self.judge(after=drain(operation="op-second"))))

    def test_post_with_a_different_key_or_after_restart_fails(self):
        wrong_key = proxy_log() + [{"at": 950.0, "client": OLD_CONTROLLER["ip"], "method": "POST", "path": "/api/v1/nodes/1/drain",
                                    "action": "relay", "idempotencyKey": "rollout-other"}]
        self.assertTrue(failed(self.judge(log=wrong_key)))
        replay = proxy_log() + [{"at": 1030.0, "client": NEW_CONTROLLER["ip"], "method": "POST", "path": "/api/v1/nodes/1/drain",
                                 "action": "relay", "idempotencyKey": "rollout-key-0"}]
        self.assertTrue(failed(self.judge(log=replay)))

    def test_missing_post_or_new_leader_get_fails(self):
        self.assertTrue(failed(self.judge(log=[r for r in proxy_log() if r["method"] != "POST"])))
        self.assertTrue(failed(self.judge(log=[r for r in proxy_log() if r["method"] != "GET"])))

    def test_same_controller_pod_is_not_a_restart(self):
        same = {**RESTART, "leaderPod": {**OLD_CONTROLLER, "ready": True}}
        self.assertTrue(failed(self.judge(restart=same)))

    def test_one_operation_and_key_per_ordinal(self):
        self.assertEqual(failed(rrj.judge_single_operation_per_ordinal(timeline_samples(), proxy_log())), [])
        second = timeline_samples()
        second.append({**second[-1], "at": second[-1]["at"] + 1, "ordinal": 1, "operation": "op-1b"})
        self.assertTrue(failed(rrj.judge_single_operation_per_ordinal(second, proxy_log())))
        extra_post = proxy_log() + [{"at": 905.0, "client": "x", "method": "POST", "path": "/api/v1/nodes/2/drain", "action": "relay",
                                     "idempotencyKey": "rollout-key-0"}]
        self.assertTrue(failed(rrj.judge_single_operation_per_ordinal(timeline_samples(), extra_post)))
        self.assertTrue(failed(rrj.judge_single_operation_per_ordinal([s for s in timeline_samples() if s["ordinal"] != 2], proxy_log())))

    def test_carried_over_operation_is_not_counted_for_the_next_ordinal(self):
        samples = timeline_samples()
        carried = {**samples[-1], "at": samples[-1]["at"] + 1, "ordinal": 1, "operation": "op-0", "attemptKey": "rollout-key-0"}
        identity = rrj.ordinal_identity([samples[2], carried])
        self.assertEqual(sorted(identity), [0])


class ReplacementTests(unittest.TestCase):
    deletes = [{"at": "t", "user": CONTROLLER_USER, "pod": f"narumq-{o}", "code": 200, "preconditions": {"uid": f"old-uid-{o}", "resourceVersion": "5"}}
               for o in range(3)] + [{"at": "t", "user": "system:node:ip-1", "pod": "narumq-0", "code": 200, "preconditions": {"uid": "old-uid-0"}}]

    def judge(self, final=FINAL, samples=None, deletes=None):
        return rrj.judge_replacement(INITIAL, final, timeline_samples() if samples is None else samples,
                                     self.deletes if deletes is None else deletes, TARGET, CONTROLLER_USER)

    def test_clean_replacement_passes(self):
        self.assertEqual(failed(self.judge()), [])

    def test_unreplaced_or_twice_replaced_pod_fails(self):
        unreplaced = {**FINAL, "narumq-1": {**FINAL["narumq-1"], "uid": "old-uid-1"}}
        self.assertTrue(failed(self.judge(final=unreplaced)))
        samples = timeline_samples()
        samples.append({**samples[-1], "at": samples[-1]["at"] + 5, "pods": {**samples[-1]["pods"], "narumq-1": "intermediate"}})
        samples.append({**samples[-1], "at": samples[-1]["at"] + 5, "pods": {**samples[-1]["pods"], "narumq-1": "new-uid-1"}})
        self.assertTrue(failed(self.judge(samples=samples)))

    def test_not_ready_or_wrong_digest_fails(self):
        for patch in ({"ready": False}, {"imageID": "docker://x@" + DIGEST_OLD}, {"deleting": "2026-10-17T00:00:00Z"}):
            final = {**FINAL, "narumq-2": {**FINAL["narumq-2"], **patch}}
            self.assertTrue(failed(self.judge(final=final)), patch)

    def test_out_of_order_replacement_fails(self):
        old = {name: pod["uid"] for name, pod in INITIAL.items()}
        samples = [{"at": 1000.0, "pods": old},
                   {"at": 1010.0, "pods": {**old, "narumq-2": "new-uid-2"}},
                   {"at": 1020.0, "pods": {**old, "narumq-2": "new-uid-2", "narumq-1": "new-uid-1"}},
                   {"at": 1030.0, "pods": {name: pod["uid"] for name, pod in FINAL.items()}}]
        rows = dict((name, ok) for name, ok, _ in self.judge(samples=samples))
        self.assertFalse(rows["the three replacements arrived in ordinal order"])
        self.assertTrue(rows["every broker Pod UID was replaced exactly once (observed UID history is [initial, final])"])

    def test_delete_audit_violations_fail(self):
        self.assertTrue(failed(self.judge(deletes=self.deletes[:2])))
        twice = self.deletes + [self.deletes[1]]
        self.assertTrue(failed(self.judge(deletes=twice)))
        foreign = self.deletes + [{"at": "t", "user": "human", "pod": "narumq-1", "code": 200, "preconditions": None}]
        self.assertTrue(failed(self.judge(deletes=foreign)))
        wrong_uid = [dict(self.deletes[0], preconditions={"uid": "other"})] + self.deletes[1:3]
        self.assertTrue(failed(self.judge(deletes=wrong_uid)))

    def test_completion_requires_all_ordinals_and_no_failure(self):
        ok = {"phase": "Completed", "completedOrdinals": [2, 0, 1]}
        self.assertEqual(failed(rrj.judge_completion(ok)), [])
        self.assertTrue(failed(rrj.judge_completion({**ok, "completedOrdinals": [0, 1]})))
        self.assertTrue(failed(rrj.judge_completion({**ok, "phase": "Failed", "failureCode": "drain_timeout"})))

    def test_release_after_requires_three_voters_at_target(self):
        self.assertEqual(failed(rrj.judge_release_after(features("0.0.1", feature_list=["b-feature", "a-feature"]), TARGET)), [])
        self.assertTrue(failed(rrj.judge_release_after(features("0.0.0", feature_list=TARGET["features"]), TARGET)))
        self.assertTrue(failed(rrj.judge_release_after(features("0.0.1", feature_list=["a-feature"]), TARGET)))
        self.assertTrue(failed(rrj.judge_release_after(features("0.0.1", members=2, feature_list=TARGET["features"]), TARGET)))
        self.assertTrue(failed(rrj.judge_release_after((0, None), TARGET)))


class ApprovalTests(unittest.TestCase):
    def judge(self, events=None, exit_code=0):
        events = [approval(o) for o in range(3)] if events is None else events
        return rrj.judge_approvals(events, exit_code, "audit verification passed", "rollout-uid", INITIAL, timeline_samples(), TARGET, RESTART)

    def test_complete_approvals_pass(self):
        self.assertEqual(failed(self.judge()), [])

    def test_other_rollouts_events_are_ignored(self):
        foreign = approval(0, rolloutUid="other", podUid="x")
        self.assertEqual(failed(self.judge([*[approval(o) for o in range(3)], foreign])), [])

    def test_hash_chain_failure_fails(self):
        self.assertTrue(failed(self.judge(exit_code=1)))

    def test_missing_duplicate_or_foreign_approval_fails(self):
        self.assertTrue(failed(self.judge([approval(0), approval(1)])))
        self.assertTrue(failed(self.judge([approval(0), approval(1), approval(2), approval(2)])))
        self.assertTrue(failed(self.judge([approval(0), approval(1), approval(2, podUid="other-uid")])))

    def test_each_incomplete_barrier_field_fails(self):
        overrides = [{"drainSafeToStop": False}, {"drainState": "DRAINING"}, {"drainPhase": "WAITING_FOR_LB"}, {"connectionsRemaining": 1},
                     {"pendingWAL": 1}, {"underReplicated": 2}, {"sessionsFailed": 1}, {"sessionsReady": None}, {"safeToStopObservedAt": None},
                     {"drainFailureCode": "drain_timeout"}, {"blockingReason": "x"}, {"drainOperationId": "op-other"}, {"attemptKey": "other"},
                     {"targetReleaseVersion": "0.0.0"}, {"targetImage": "other"}, {"nodeId": 9}, {"ordinal": 2}]
        for override in overrides:
            events = [approval(0, **override), approval(1), approval(2)]
            self.assertTrue(failed(self.judge(events)), override)
        missing = approval(1)
        del missing["details"]["pendingWAL"]
        self.assertTrue(failed(self.judge([approval(0), missing, approval(2)])))

    def test_producer_must_be_the_restarted_controller(self):
        old = approval(0)
        old["producer"]["instanceId"] = "ctl-uid-old"
        self.assertTrue(failed(self.judge([old, approval(1), approval(2)])))

    def test_result_and_reason_must_be_allowed_safe_to_stop(self):
        for field, value in (("result", "DENIED"), ("reason", "other"), ("requestId", "different")):
            event = approval(0)
            event[field] = value
            self.assertTrue(failed(self.judge([event, approval(1), approval(2)])), field)


def pod_item(ordinal: int, **status_overrides) -> dict:
    state = {"name": "narumq", "started": True, "restartCount": 0, "ready": True, **status_overrides}
    return {"metadata": {"name": f"narumq-{ordinal}", "uid": f"new-uid-{ordinal}"},
            "spec": {"containers": [{"name": "narumq", "startupProbe": {"httpGet": {"path": "/health/startup", "port": "http-mgmt"}, "failureThreshold": 60}}]},
            "status": {"containerStatuses": [state]}}


class StartupTests(unittest.TestCase):
    def test_clean_startup_passes_even_with_a_few_probe_failures(self):
        events = [{"involvedObject": {"uid": "new-uid-0", "name": "narumq-0"}, "reason": "Unhealthy", "message": "Startup probe failed: x", "count": 4}]
        self.assertEqual(failed(rrj.judge_startup([pod_item(o) for o in range(3)], events)), [])

    def test_restart_not_started_or_missing_probe_fails(self):
        self.assertTrue(failed(rrj.judge_startup([pod_item(0), pod_item(1), pod_item(2, restartCount=1)], [])))
        self.assertTrue(failed(rrj.judge_startup([pod_item(0), pod_item(1), pod_item(2, started=False)], [])))
        self.assertTrue(failed(rrj.judge_startup([pod_item(0), pod_item(1)], [])))
        bare = pod_item(2)
        del bare["spec"]["containers"][0]["startupProbe"]
        self.assertTrue(failed(rrj.judge_startup([pod_item(0), pod_item(1), bare], [])))

    def test_probe_kill_or_exhausted_threshold_fails(self):
        pods = [pod_item(o) for o in range(3)]
        killing = [{"involvedObject": {"uid": "new-uid-1", "name": "narumq-1"}, "reason": "Killing", "message": "Container narumq failed startup probe, will be restarted"}]
        self.assertTrue(failed(rrj.judge_startup(pods, killing)))
        exhausted = [{"involvedObject": {"uid": "new-uid-1", "name": "narumq-1"}, "reason": "Unhealthy", "message": "Startup probe failed: x", "count": 60}]
        self.assertTrue(failed(rrj.judge_startup(pods, exhausted)))
        unrelated = [{"involvedObject": {"uid": "old-uid-1", "name": "narumq-1"}, "reason": "Killing", "message": "failed startup probe"}]
        self.assertEqual(failed(rrj.judge_startup(pods, unrelated)), [])


def access(user: str, resource: str, subresource: str, name: str, received="2026-10-17T00:05:00Z", completed="2026-10-17T00:06:00Z") -> dict:
    return {"auditID": f"{user}-{resource}-{subresource}-{name}-{received}", "user": user, "verb": "create", "resource": resource,
            "subresource": subresource, "name": name, "received": received, "completed": completed}


class NoManualAccessTests(unittest.TestCase):
    window = (rrj.timestamp("2026-10-17T00:00:00Z"), rrj.timestamp("2026-10-17T00:30:00Z"))

    def judge(self, records, allowed=1):
        return rrj.judge_no_manual_access(records, "operator", allowed, self.window)

    def test_no_access_or_only_the_harness_forward_passes(self):
        self.assertEqual(failed(self.judge([])), [])
        self.assertEqual(failed(self.judge([access("operator", "pods", "portforward", "narumq-1")])), [])

    def test_exec_proxy_and_attach_on_broker_pods_fail(self):
        for record in (access("operator", "pods", "exec", "narumq-0"), access("someone", "pods", "proxy", "narumq-2"),
                       access("operator", "services", "proxy", "narumq-management"), access("operator", "pods", "attach", "narumq-1")):
            self.assertTrue(failed(self.judge([record])), record)

    def test_foreign_port_forward_or_too_many_harness_forwards_fail(self):
        self.assertTrue(failed(self.judge([access("someone-else", "pods", "portforward", "narumq-0")])))
        two = [access("operator", "pods", "portforward", "narumq-0"), access("operator", "pods", "portforward", "narumq-1")]
        self.assertTrue(failed(self.judge(two, allowed=1)))
        self.assertEqual(failed(self.judge(two, allowed=2)), [])

    def test_unrelated_pods_and_other_windows_are_ignored(self):
        self.assertEqual(failed(self.judge([access("operator", "pods", "exec", "rollout-audit-helper"),
                                            access("operator", "pods", "exec", "narumq-rollout-controller-abc")])), [])
        before = access("operator", "pods", "exec", "narumq-0", "2026-10-16T00:00:00Z", "2026-10-16T00:01:00Z")
        after = access("operator", "pods", "exec", "narumq-0", "2026-10-17T01:00:00Z", "2026-10-17T01:01:00Z")
        self.assertEqual(failed(self.judge([before, after])), [])

    def test_open_stream_that_began_before_the_window_counts(self):
        spanning = access("someone", "pods", "exec", "narumq-0", "2026-10-16T23:00:00Z", None)
        self.assertTrue(failed(self.judge([spanning])))


class ForwardCounterTests(unittest.TestCase):
    """ForwardCounter lives in main.py; load it as the canonical module (no cluster access at import)."""

    @classmethod
    def setUpClass(cls):
        import eks_fixture
        cls.module = eks_fixture.rollout_faults()

    class FakeProcess:
        def __init__(self, alive=True):
            self.alive = alive

        def poll(self):
            return None if self.alive else 1

    class FakeForward:
        def __init__(self, process=None):
            self.process = process

        def ensure(self):
            if self.process is None or self.process.poll() is not None:
                self.process = ForwardCounterTests.FakeProcess()

    def test_counts_new_processes_and_preexisting(self):
        forward = self.FakeForward(self.FakeProcess())
        counter = self.module.ForwardCounter(forward)
        self.assertEqual(counter.allowed, 1)
        forward.ensure()
        self.assertEqual(counter.allowed, 1)
        forward.process.alive = False
        forward.ensure()
        forward.ensure()
        self.assertEqual(counter.allowed, 2)
        counter.restore()
        self.assertNotIn("ensure", forward.__dict__)

    def test_scenario_function_is_the_wrapped_drn_022_scenario(self):
        scenario = self.module.scenario_rollout_replace_with_controller_restart
        self.assertTrue(hasattr(scenario, "__wrapped__"))
        self.assertEqual(scenario.__wrapped__.__name__, "scenario_rollout_replace_with_controller_restart")


if __name__ == "__main__":
    unittest.main()
