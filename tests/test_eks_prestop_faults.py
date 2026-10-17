"""Offline tests of the DRN-SCN-030 harness logic (judgments, manifests, stub endpoint, policy package).

They exercise the observed-data judgments and the objects the live harness submits. They are not the scenario:
tests/chaos/eks-prestop-faults/main.py runs it against a real disposable EKS fixture, so nothing here is scenario-tagged.
"""
from __future__ import annotations

import importlib.util
import json
import socket
import subprocess
import sys
import time
import unittest
import urllib.error
import urllib.request
from pathlib import Path

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parents[1]
HARNESS = ROOT / "tests/chaos/eks-prestop-faults"
sys.path.insert(0, str(HARNESS))
sys.path.insert(0, str(ROOT / "tests/chaos"))
import prestop_judge as judge  # noqa: E402
import prestop_objects as objects  # noqa: E402
from eks_broker import PrerequisiteMissing  # noqa: E402

spec = importlib.util.spec_from_file_location("eks_prestop_faults_main", HARNESS / "main.py")
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)

HOOK = ["/bin/sh", "-c", "set -eu\nexit 1\n"]


def statefulset(grace=660, annotations=None, command=HOOK, image="registry/narumq@sha256:1") -> dict:
    container = {"name": "narumq", "image": image}
    if command is not None:
        container["lifecycle"] = {"preStop": {"exec": {"command": command}}}
    return {"metadata": {"name": "narumq", "annotations": annotations or {}, "resourceVersion": "7"},
            "spec": {"template": {"spec": {"terminationGracePeriodSeconds": grace, "containers": [container]}}}}


def names(rows) -> dict:
    return {name: ok for name, ok, _ in rows}


def free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


class FormulaTests(unittest.TestCase):
    def test_minimum_grace_is_ceil_seconds_plus_30_plus_30(self):
        self.assertEqual(judge.minimum_grace_seconds(600_000), 660)
        self.assertEqual(judge.minimum_grace_seconds(1_800_000), 1860)
        self.assertEqual(judge.minimum_grace_seconds(600_001), 661)
        self.assertEqual(judge.minimum_grace_seconds(1), 61)
        self.assertEqual(judge.minimum_grace_seconds(999), 61)
        self.assertEqual(judge.minimum_grace_seconds(1000), 61)

    def test_invalid_timeouts_are_refused(self):
        for value in (0, -1, 1.5, "600000", True):
            with self.assertRaises(ValueError, msg=repr(value)):
                judge.minimum_grace_seconds(value)

    def test_annotation_overrides_default_timeout(self):
        self.assertEqual(judge.drain_timeout_millis(None), 600_000)
        self.assertEqual(judge.drain_timeout_millis({}), 600_000)
        self.assertEqual(judge.drain_timeout_millis({judge.TIMEOUT_ANNOTATION: "1800000"}), 1_800_000)
        for value in ("ten minutes", "0", "1", "599999"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                judge.drain_timeout_millis({judge.TIMEOUT_ANNOTATION: value})

    def test_boundary(self):
        self.assertFalse(judge.grace_satisfies_formula(659, 600_000))
        self.assertTrue(judge.grace_satisfies_formula(660, 600_000))
        self.assertFalse(judge.grace_satisfies_formula(660, 1_800_000))
        self.assertTrue(judge.grace_satisfies_formula(1860, 1_800_000))

    def test_live_statefulset_judgment(self):
        self.assertTrue(all(ok for _, ok, _ in judge.judge_live_statefulset(statefulset(660))))
        self.assertFalse(all(ok for _, ok, _ in judge.judge_live_statefulset(statefulset(659))))



class HookJudgmentTests(unittest.TestCase):
    def test_extract_prestop_command(self):
        command, image = judge.extract_prestop_command(statefulset())
        self.assertEqual((command, image), (HOOK, "registry/narumq@sha256:1"))

    def test_missing_or_foreign_hook_is_a_missing_prerequisite(self):
        for sts in (statefulset(command=None), statefulset(command=["/bin/sh", "-c", "  "]), statefulset(command=["echo"]),
                    statefulset(image="")):
            with self.assertRaises(PrerequisiteMissing):
                judge.extract_prestop_command(sts)
        with self.assertRaises(PrerequisiteMissing):
            judge.extract_prestop_command({"spec": {"template": {"spec": {"containers": []}}}})

    def test_failure_reason_ignores_kubectl_trailer(self):
        stderr = "Drain query failed: HTTP 401\ncommand terminated with exit code 1\n"
        self.assertEqual(judge.failure_reason(stderr), "Drain query failed: HTTP 401")
        self.assertEqual(judge.failure_reason(""), "")
        self.assertEqual(judge.failure_reason("command terminated with exit code 1\n"), "")

    def test_exec_judgment(self):
        good = {"exitCode": 1, "stderr": "Drain query failed: HTTP 401\ncommand terminated with exit code 1\n"}
        self.assertTrue(all(ok for _, ok, _ in judge.judge_failed_hook_exec(good, judge.UNAUTHORIZED_REASON)))
        self.assertFalse(all(ok for _, ok, _ in judge.judge_failed_hook_exec({**good, "exitCode": 0}, judge.UNAUTHORIZED_REASON)))
        self.assertFalse(all(ok for _, ok, _ in judge.judge_failed_hook_exec({**good, "stderr": "Drain start failed: HTTP 409"}, judge.UNAUTHORIZED_REASON)))

    def test_probe_pod_must_carry_the_command_and_no_broker_label(self):
        pod = objects.probe_pod("unauthorized", "run-1", "narumq", "b", "s", HOOK, 30)
        self.assertTrue(all(ok for _, ok, _ in judge.judge_probe_pod_command(pod, HOOK)))
        self.assertFalse(all(ok for _, ok, _ in judge.judge_probe_pod_command(pod, ["/bin/sh", "-c", "exit 0"])))
        pod["metadata"]["labels"]["app"] = "narumq"
        self.assertFalse(names(judge.judge_probe_pod_command(pod, HOOK))["probe Pod is outside every broker selector (no app=narumq label)"])


class EventTests(unittest.TestCase):
    def event(self, reason, message, uid="u1", type_="Warning", stamp="2026-10-17T00:00:02Z"):
        return {"metadata": {"creationTimestamp": stamp}, "involvedObject": {"uid": uid, "fieldPath": "spec.containers{narumq}"},
                "reason": reason, "type": type_, "message": message, "lastTimestamp": stamp, "count": 1, "source": {"component": "kubelet"}}

    def test_summarize_filters_uid_and_orders(self):
        items = [self.event("Killing", "Stopping container narumq", stamp="2026-10-17T00:00:03Z"),
                 self.event("FailedPreStopHook", "PreStopHook failed", stamp="2026-10-17T00:00:04Z"),
                 self.event("Killing", "other pod", uid="u2")]
        events = judge.summarize_events(items, "u1")
        self.assertEqual([e["reason"] for e in events], ["Killing", "FailedPreStopHook"])

    def test_message_status(self):
        reason = judge.UNAUTHORIZED_REASON
        self.assertEqual(judge.event_message_status("PreStopHook failed", reason), "generic")
        self.assertEqual(judge.event_message_status('Exec lifecycle hook exited with 1: , message: "Drain query failed: HTTP 401\\n"', reason), "matches")
        self.assertEqual(judge.event_message_status("Exec lifecycle hook exited with 1: other", reason), "mismatch")

    def test_failed_prestop_event_judgment(self):
        reason = judge.UNAUTHORIZED_REASON
        generic = judge.summarize_events([self.event("FailedPreStopHook", "PreStopHook failed")])
        rows = judge.judge_failed_prestop_event(generic, reason)
        self.assertTrue(all(ok for _, ok, _ in rows))
        self.assertFalse(rows[1][2]["eventCarriesStderr"])
        detailed = judge.summarize_events([self.event("FailedPreStopHook", "failed: Drain query failed: HTTP 401")])
        self.assertTrue(judge.judge_failed_prestop_event(detailed, reason)[1][2]["eventCarriesStderr"])
        self.assertFalse(all(ok for _, ok, _ in judge.judge_failed_prestop_event([], reason)))
        wrong = judge.summarize_events([self.event("FailedPreStopHook", "failed: something else")])
        self.assertFalse(all(ok for _, ok, _ in judge.judge_failed_prestop_event(wrong, reason)))
        normal = judge.summarize_events([self.event("FailedPreStopHook", "PreStopHook failed", type_="Normal")])
        self.assertFalse(all(ok for _, ok, _ in judge.judge_failed_prestop_event(normal, reason)))

    def test_killing_event(self):
        self.assertTrue(all(ok for _, ok, _ in judge.judge_killing_event(judge.summarize_events([self.event("Killing", "Stopping container narumq")]))))
        self.assertFalse(all(ok for _, ok, _ in judge.judge_killing_event([])))


class TerminationWindowTests(unittest.TestCase):
    def obs(self, elapsed, grace, gone=True):
        return {"gone": gone, "elapsedSeconds": elapsed if gone else None, "deletionGracePeriodSeconds": grace}

    def test_failed_hook_pod_goes_away_well_before_grace(self):
        self.assertTrue(all(ok for _, ok, _ in judge.judge_termination(self.obs(3.5, 30), 30, expect_forced=False)))

    def test_failed_hook_that_consumed_the_grace_period_is_flagged(self):
        rows = names(judge.judge_termination(self.obs(31.0, 30), 30, expect_forced=False))
        self.assertTrue(rows["Pod removal time is inside the bounded window"])
        self.assertFalse(rows["failed hook did not consume the grace period"])

    def test_forced_termination_is_at_about_the_grace_period(self):
        self.assertTrue(all(ok for _, ok, _ in judge.judge_termination(self.obs(23.5, 20), 20, expect_forced=True)))
        self.assertTrue(all(ok for _, ok, _ in judge.judge_termination(self.obs(18.5, 20), 20, expect_forced=True)))  # within tolerance
        early = names(judge.judge_termination(self.obs(5.0, 20), 20, expect_forced=True))
        self.assertFalse(early["Pod removal time is inside the bounded window"])
        late = names(judge.judge_termination(self.obs(20 + judge.KUBELET_SLACK_SECONDS + 1, 20), 20, expect_forced=True))
        self.assertFalse(late["Pod removal time is inside the bounded window"])

    def test_pod_that_never_disappears_fails(self):
        rows = names(judge.judge_termination(self.obs(None, 30, gone=False), 30, expect_forced=False))
        self.assertFalse(rows["Pod object was removed (the hook did not block deletion permanently)"])
        self.assertFalse(rows["Pod removal time is inside the bounded window"])

    def test_wrong_delete_grace_is_flagged(self):
        rows = names(judge.judge_termination(self.obs(22.0, 30), 20, expect_forced=True))
        self.assertFalse(rows["DELETE applied the Pod's own grace period"])


class DuplicateHookTests(unittest.TestCase):
    COMPLETED = {"operationId": "op-1", "phase": "COMPLETED", "safeToStop": True}
    POLLED = [{"status": 200, "operationId": "op-1"}, {"status": 200, "operationId": "op-1"}]

    def judged(self, runs, polled=None, after="default", before=404):
        after = self.COMPLETED if after == "default" else after
        return names(judge.judge_duplicate_hooks(before, runs, self.POLLED if polled is None else polled, after))

    def test_both_succeed_with_one_operation(self):
        runs = [{"exitCode": 0, "stderr": ""}, {"exitCode": 0, "stderr": ""}]
        self.assertTrue(all(self.judged(runs).values()))

    def test_second_invocation_conflict_is_a_legitimate_recorded_observation(self):
        runs = [{"exitCode": 0, "stderr": ""}, {"exitCode": 1, "stderr": "Drain start failed: HTTP 409\ncommand terminated with exit code 1\n"}]
        self.assertTrue(all(self.judged(runs).values()))

    def test_undocumented_failure_reason_is_not_accepted(self):
        runs = [{"exitCode": 0, "stderr": ""}, {"exitCode": 1, "stderr": "segfault"}]
        self.assertFalse(self.judged(runs)["every non-zero exit recorded a documented failure reason"])
        silent = [{"exitCode": 0, "stderr": ""}, {"exitCode": 1, "stderr": ""}]
        self.assertFalse(self.judged(silent)["every non-zero exit recorded a documented failure reason"])

    def test_two_operation_ids_fail(self):
        runs = [{"exitCode": 0, "stderr": ""}, {"exitCode": 0, "stderr": ""}]
        polled = self.POLLED + [{"status": 200, "operationId": "op-2"}]
        self.assertFalse(self.judged(runs, polled)["exactly one drain operation id exists after the duplicate calls"])

    def test_both_failed_or_not_completed_fails(self):
        both = [{"exitCode": 1, "stderr": "Drain did not complete"}, {"exitCode": 1, "stderr": "Drain did not complete"}]
        self.assertFalse(self.judged(both, after={"operationId": "op-1", "phase": "FAILED", "safeToStop": False})["at least one run completed the drain with exit 0"])
        ok_runs = [{"exitCode": 0, "stderr": ""}, {"exitCode": 0, "stderr": ""}]
        self.assertFalse(self.judged(ok_runs, after={"operationId": "op-1", "phase": "DRAINING", "safeToStop": False})["the single operation is COMPLETED with safeToStop"])
        self.assertFalse(self.judged(ok_runs, after=None)["the single operation is COMPLETED with safeToStop"])

    def test_existing_drain_record_before_the_run_fails(self):
        runs = [{"exitCode": 0, "stderr": ""}, {"exitCode": 0, "stderr": ""}]
        self.assertFalse(self.judged(runs, before=200)["follower had no drain record before the duplicate calls (GET 404)"])

    def test_missing_exit_code_fails(self):
        self.assertFalse(self.judged([{"exitCode": 0, "stderr": ""}, {"exitCode": None, "stderr": ""}])["both hook runs ended with a recorded exit code"])


class AdmissionJudgmentTests(unittest.TestCase):
    REJECTED = ('Error from server (Invalid): error when creating "STDIN": StatefulSet.apps "x" is invalid: : ValidatingAdmissionPolicy '
                "'narumq-statefulset-termination-grace' with binding 'narumq-statefulset-termination-grace' denied request: "
                "terminationGracePeriodSeconds 30 is shorter than ceil(timeoutMillis/1000)+30+30 = 660 for a drain timeout of 600000 ms")

    def test_rejection_names_policy_grace_and_minimum(self):
        self.assertTrue(all(ok for _, ok, _ in judge.judge_admission_rejected(1, self.REJECTED, 30, 660)))

    def test_other_errors_are_not_a_policy_rejection(self):
        for stderr in ("Error from server (NotFound): namespaces \"narumq\" not found", "Unable to connect to the server: dial tcp: i/o timeout", ""):
            self.assertFalse(all(ok for _, ok, _ in judge.judge_admission_rejected(1, stderr, 30, 660)), stderr)
        self.assertFalse(all(ok for _, ok, _ in judge.judge_admission_rejected(0, self.REJECTED, 30, 660)))
        self.assertFalse(all(ok for _, ok, _ in judge.judge_admission_rejected(1, self.REJECTED, 30, 1860)))

    def test_accepted(self):
        self.assertTrue(all(ok for _, ok, _ in judge.judge_admission_accepted(0, "statefulset.apps/x created\n", "")))
        self.assertFalse(all(ok for _, ok, _ in judge.judge_admission_accepted(1, "", "Error from server (Invalid)")))

    def test_policy_status_warnings(self):
        self.assertTrue(all(ok for _, ok, _ in judge.judge_policy_status({"status": {}})))
        self.assertTrue(all(ok for _, ok, _ in judge.judge_policy_status({"status": {"typeChecking": {}}})))
        self.assertFalse(all(ok for _, ok, _ in judge.judge_policy_status({"status": {"typeChecking": {"expressionWarnings": [{"warning": "x"}]}}})))


class ObjectTests(unittest.TestCase):
    def test_probe_pod_shape(self):
        for mode, grace in (("unauthorized", 30), ("draining", 20)):
            pod = objects.probe_pod(mode, "run-1", "narumq", "broker:img", "stub:img", HOOK, grace)
            self.assertTrue(pod["metadata"]["name"].endswith("-0"))  # the hook derives its slot from ${HOSTNAME##*-}
            self.assertEqual(pod["metadata"]["labels"][objects.RUN_LABEL], "run-1")
            self.assertNotIn("app", pod["metadata"]["labels"])
            self.assertEqual(pod["spec"]["terminationGracePeriodSeconds"], grace)
            main, stub = pod["spec"]["containers"]
            self.assertEqual((main["name"], main["image"], stub["image"]), ("narumq", "broker:img", "stub:img"))
            self.assertEqual(main["lifecycle"]["preStop"]["exec"]["command"], HOOK)
            self.assertEqual(stub["command"][-1], mode)
            self.assertEqual({m["mountPath"] for m in main["volumeMounts"]}, {"/configs", "/secrets"})
        with self.assertRaises(ValueError):
            objects.probe_pod("other", "run-1", "narumq", "b", "s", HOOK, 30)

    def test_probe_files_match_what_the_hook_reads(self):
        config = objects.probe_config_map("run-1", "narumq")
        secret = objects.probe_secret("run-1", "narumq")
        self.assertEqual(config["data"], {"slot-0": "1"})  # slot file for pod ordinal 0, a canonical node id
        self.assertIn("admin.token", secret["stringData"])
        self.assertEqual(config["metadata"]["labels"][objects.RUN_LABEL], "run-1")
        self.assertEqual(secret["metadata"]["labels"][objects.RUN_LABEL], "run-1")

    def test_admission_statefulset_cannot_schedule_or_select_live_pods(self):
        sts = objects.admission_statefulset("probe", "run-1", "narumq", "img", 30, {judge.TIMEOUT_ANNOTATION: "1"})
        self.assertEqual(sts["spec"]["replicas"], 0)
        self.assertEqual(sts["spec"]["template"]["metadata"]["labels"]["app"], "narumq")  # what the policy matches
        self.assertNotIn("app", sts["spec"]["selector"]["matchLabels"])
        template_labels = sts["spec"]["template"]["metadata"]["labels"]
        self.assertTrue(all(template_labels[k] == v for k, v in sts["spec"]["selector"]["matchLabels"].items()))
        self.assertEqual(sts["metadata"]["labels"][objects.RUN_LABEL], "run-1")
        self.assertEqual(sts["metadata"]["annotations"], {judge.TIMEOUT_ANNOTATION: "1"})
        self.assertNotIn("annotations", objects.admission_statefulset("probe", "run-1", "narumq", "img", 660)["metadata"])


class StubEndpointTests(unittest.TestCase):
    def serve(self, mode):
        port = free_port()
        process = subprocess.Popen([sys.executable, "-c", objects.STUB_SCRIPT, mode, str(port)], stdout=subprocess.PIPE, text=True)
        self.addCleanup(lambda: (process.terminate(), process.wait(10), process.stdout.close()))
        for _ in range(100):
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                    return port, process
            except OSError:
                time.sleep(0.1)
        self.fail("stub did not listen")

    def fetch(self, port, headers=None):
        request = urllib.request.Request(f"http://127.0.0.1:{port}/api/v1/nodes/1/drain", headers=headers or {})
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                return response.status, response.read().decode()
        except urllib.error.HTTPError as error:
            with error:
                return error.code, error.read().decode()

    def test_unauthorized_mode_returns_401_so_the_hook_reports_it(self):
        port, process = self.serve("unauthorized")
        status, body = self.fetch(port, {"Authorization": "Bearer t", "Idempotency-Key": "k"})
        self.assertEqual(status, 401)
        journal = json.loads(process.stdout.readline())
        self.assertEqual((journal["method"], journal["path"], journal["bearer"], journal["idempotencyKey"]), ("GET", "/api/v1/nodes/1/drain", True, True))

    def test_draining_mode_never_matches_the_hook_completion_pattern(self):
        port, _ = self.serve("draining")
        status, body = self.fetch(port)
        self.assertEqual(status, 200)
        self.assertIn('"phase":"DRAINING"', body)  # compact JSON, as the hook's case patterns expect
        self.assertNotIn("COMPLETED", body)
        self.assertIn('"safeToStop":false', body)


if __name__ == "__main__":
    unittest.main()
