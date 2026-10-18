from __future__ import annotations

import importlib.util
import copy
import json
from pathlib import Path
import subprocess
import sys
import unittest
from unittest.mock import patch

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parents[1]
path = ROOT / "tests/benchmark/capacity/eks/controller.py"
spec = importlib.util.spec_from_file_location("eks_capacity_controller", path)
controller = importlib.util.module_from_spec(spec)
spec.loader.exec_module(controller)

DIGEST = "a" * 64
RUNNER = f"123.dkr.ecr.ap-northeast-2.amazonaws.com/capacity-runner@sha256:{DIGEST}"
TOOLBOX = f"123.dkr.ecr.ap-northeast-2.amazonaws.com/narumq@sha256:{'b' * 64}"


def job_values(**changes):
    values = {"RUN_ID": "cap-1", "CAPACITY_RUNNER_IMAGE": RUNNER, "TOOLBOX_IMAGE": TOOLBOX,
              "MQTT_ENDPOINT": "nlb.internal:1883", "MQTT_TLS_ENDPOINT": "nlb.internal:8883",
              "TLS_SERVER_NAME": "narumq-capacity.test", "GENERATOR_CPU": "3", "GENERATOR_MEMORY": "6Gi",
              "ACTIVE_DEADLINE_SECONDS": "7200"}
    values.update(changes)
    return values


class MarkerListingTests(unittest.TestCase):
    # Scenarios: performance-and-verification.md:PRF-SCN-053
    def test_only_final_marker_names_are_recognised(self):
        listing = "\n".join(["step-01-r1-ready.json", "step-01-r1-kill-ack.json", ".step-01-r1-kill.json.tmp",
                             "step-12-r10-n1-done.json", "step-01-r1-other.json", "run-complete.json",
                             "notes.txt", "step-01-r1-ready.json.tmp"])
        steps, run_level = controller.parse_control_listing(listing)
        self.assertEqual(steps, {"step-01-r1": {"ready", "kill-ack"}, "step-12-r10": {"n1-done"}})
        self.assertEqual(run_level, {"run-complete.json"})

    def test_leader_node_id_maps_to_statefulset_ordinal(self):
        self.assertEqual([controller.broker_pod_name(n) for n in (1, 2, 3)], ["narumq-0", "narumq-1", "narumq-2"])
        for bad in (0, 4, -1):
            with self.assertRaises(controller.ControllerError):
                controller.broker_pod_name(bad)

    # Scenarios: performance-and-verification.md:PRF-SCN-053
    def test_kill_marker_states_are_the_contract_states(self):
        marker = controller.kill_marker("down", 2, "narumq-1", "worker-b", "t", cordonedAt="c")
        self.assertEqual(marker, {"state": "down", "target": {"nodeId": "2", "pod": "narumq-1", "worker": "worker-b"},
                                  "at": "t", "cordonedAt": "c"})
        failed = controller.kill_marker("failed", 1, "narumq-0", "w", "t", error="resync timeout")
        self.assertEqual((failed["state"], failed["error"]), ("failed", "resync timeout"))
        with self.assertRaises(ValueError):
            controller.kill_marker("killed", 1, "p", "w", "t")


class FaultIdentityTests(unittest.TestCase):
    # Scenarios: performance-and-verification.md:PRF-SCN-055
    def test_signals_require_the_original_live_broker_identity(self):
        target = {"uid": "old-pod", "worker": "worker-a", "containerID": "containerd://old"}
        pod = {"metadata": {"uid": "old-pod"}, "spec": {"nodeName": "worker-a"},
               "status": {"containerStatuses": [
                   {"name": "narumq", "containerID": "containerd://old", "state": {"running": {}}},
                   {"name": "capacity-jwks", "containerID": "containerd://sidecar", "state": {"running": {}}}]}}
        self.assertTrue(controller.target_matches_pod(target, pod))
        cases = []
        for kind in ("pod", "worker", "container", "stopped", "missing-status"):
            changed = copy.deepcopy(pod)
            if kind == "pod":
                changed["metadata"]["uid"] = "new-pod"
            elif kind == "worker":
                changed["spec"]["nodeName"] = "worker-b"
            elif kind == "container":
                changed["status"]["containerStatuses"][0]["containerID"] = "containerd://new"
            elif kind == "stopped":
                changed["status"]["containerStatuses"][0]["state"] = {"terminated": {"exitCode": 137}}
            else:
                changed["status"] = {}
            cases.append((kind, changed))
        cases.append(("deleted", None))
        for kind, changed in cases:
            with self.subTest(kind=kind):
                self.assertFalse(controller.target_matches_pod(target, changed))
        self.assertFalse(controller.target_matches_pod(dict(target, containerID=None), pod))
        self.assertFalse(controller.target_matches_pod(dict(target, containerID="containerd://sidecar"), pod))


class StepMachineTests(unittest.TestCase):
    # Scenarios: performance-and-verification.md:PRF-SCN-053
    def test_full_sequence_requests_kills_restores_and_completes_once(self):
        machine = controller.StepMachine("step-01-r1", ack_timeout=5)
        self.assertIsNone(machine.next_action(set(), 0))
        self.assertEqual(machine.next_action({"ready"}, 1), "request-kill")
        self.assertIsNone(machine.next_action({"ready", "kill"}, 2))
        self.assertEqual(machine.next_action({"ready", "kill", "kill-ack"}, 3), "kill")
        self.assertIsNone(machine.next_action({"ready", "kill", "kill-ack"}, 4), "kill must run once")
        self.assertEqual(machine.next_action({"ready", "kill", "kill-ack", "n1-done"}, 5), "restore")
        self.assertIsNone(machine.next_action({"n1-done"}, 6), "restore in progress")
        machine.finish_restore(True)
        self.assertEqual(machine.next_action({"complete"}, 7), "complete")
        self.assertIsNone(machine.next_action({"complete"}, 8))

    # Scenarios: performance-and-verification.md:PRF-SCN-053
    def test_ack_timeout_boundary_and_no_kill_after_failure(self):
        machine = controller.StepMachine("step-01-r1", ack_timeout=5)
        machine.next_action({"ready"}, 10)
        self.assertIsNone(machine.next_action({"ready", "kill"}, 15), "exactly at the limit is still waiting")
        self.assertEqual(machine.next_action({"ready", "kill"}, 15.001), "ack-timeout")
        self.assertIsNone(machine.next_action({"ready", "kill", "kill-ack", "n1-done"}, 16))
        self.assertEqual(machine.state, "failed")

    def test_ack_in_time_wins_over_timeout_in_the_same_poll(self):
        machine = controller.StepMachine("step-01-r1", ack_timeout=5)
        machine.next_action({"ready"}, 0)
        self.assertEqual(machine.next_action({"ready", "kill-ack"}, 99), "kill")

    def test_n1_done_before_the_kill_is_ignored(self):
        machine = controller.StepMachine("step-01-r1", ack_timeout=5)
        machine.next_action({"ready"}, 0)
        self.assertIsNone(machine.next_action({"ready", "n1-done"}, 1))
        self.assertEqual(machine.state, "awaiting-ack")

    def test_failed_restore_still_reports_runner_completion(self):
        machine = controller.StepMachine("step-01-r1", ack_timeout=5)
        machine.next_action({"ready"}, 0)
        machine.next_action({"kill-ack"}, 1)
        machine.next_action({"n1-done"}, 2)
        machine.finish_restore(False, "resync timeout")
        self.assertEqual(machine.failure, "resync timeout")
        self.assertEqual(machine.next_action({"complete"}, 3), "complete")
        with self.assertRaises(ValueError):
            machine.finish_restore(True)


class ResyncTests(unittest.TestCase):
    @staticmethod
    def rows(leader_index=100, target_index=100, health="HEALTHY", leader_id=1, target_id=3, leader=True):
        rows = [{"nodeId": "2", "role": "FOLLOWER", "lastAppliedIndex": 100, "replicationHealth": "HEALTHY"},
                {"nodeId": str(target_id), "role": "FOLLOWER", "lastAppliedIndex": target_index,
                 "replicationHealth": health}]
        if leader:
            rows.append({"nodeId": str(leader_id), "role": "LEADER", "lastAppliedIndex": leader_index,
                         "replicationHealth": "HEALTHY"})
        return rows

    def test_target_at_watermark_and_healthy_is_resynced(self):
        outcome = controller.evaluate_resync(self.rows(), 3, None)
        self.assertEqual(outcome, {"reached": True, "watermark": 100, "leaderNodeId": 1, "targetIndex": 100})

    def test_target_behind_or_unhealthy_is_not_resynced(self):
        self.assertFalse(controller.evaluate_resync(self.rows(target_index=99), 3, None)["reached"])
        self.assertFalse(controller.evaluate_resync(self.rows(health="CATCHING_UP"), 3, None)["reached"])

    def test_watermark_is_fixed_at_the_first_observation(self):
        first = controller.evaluate_resync(self.rows(leader_index=100, target_index=50), 3, None)
        later = controller.evaluate_resync(self.rows(leader_index=500, target_index=100), 3, first["watermark"])
        self.assertEqual(later["watermark"], 100)
        self.assertTrue(later["reached"])

    def test_target_leader_needs_no_replication_health(self):
        rows = [{"nodeId": "3", "role": "LEADER", "lastAppliedIndex": 7, "replicationHealth": "UNKNOWN"}]
        self.assertTrue(controller.evaluate_resync(rows, 3, None)["reached"])

    def test_no_leader_or_missing_target_is_not_resynced(self):
        self.assertFalse(controller.evaluate_resync(self.rows(leader=False), 3, None)["reached"])
        rows = [{"nodeId": "1", "role": "LEADER", "lastAppliedIndex": 1, "replicationHealth": "HEALTHY"}]
        outcome = controller.evaluate_resync(rows, 3, None)
        self.assertFalse(outcome["reached"])
        self.assertIsNone(outcome["targetIndex"])


class ReadinessTests(unittest.TestCase):
    @staticmethod
    def pod(name, uid, ready=True, terminating=False, app="narumq"):
        metadata = {"name": name, "uid": uid, "labels": {"app": app}}
        if terminating:
            metadata["deletionTimestamp"] = "now"
        return {"metadata": metadata, "status": {"conditions": [{"type": "Ready", "status": "True" if ready else "False"}]}}

    def test_ready_broker_pods_exclude_unready_terminating_and_other_apps(self):
        pods = {"items": [self.pod("narumq-0", "u0"), self.pod("narumq-1", "u1", ready=False),
                          self.pod("narumq-2", "u2", terminating=True), self.pod("other", "u3", app="x")]}
        self.assertEqual(controller.ready_broker_pods(pods), {"narumq-0": "u0"})

    def test_restore_needs_three_ready_and_a_replacement_uid(self):
        ready = {"narumq-0": "u0", "narumq-1": "old", "narumq-2": "u2"}
        self.assertFalse(controller.restore_complete(ready, "narumq-1", "old"), "old Pod is still the killed one")
        self.assertFalse(controller.restore_complete({"narumq-0": "u0", "narumq-2": "u2"}, "narumq-1", "old"))
        self.assertFalse(controller.restore_complete({"narumq-0": "u0", "narumq-2": "u2", "narumq-9": "x"}, "narumq-1", "old"))
        self.assertTrue(controller.restore_complete({**ready, "narumq-1": "new"}, "narumq-1", "old"))


class ParsingTests(unittest.TestCase):
    def test_top_output_units(self):
        text = "narumq-0   812m   1500Mi\nnarumq-1   2   3Gi\nnarumq-2   500000000n   2048Ki\nbad\n"
        self.assertEqual(controller.parse_top(text), [
            {"name": "narumq-0", "cpuMillicores": 812, "memoryBytes": 1500 * 1024 ** 2},
            {"name": "narumq-1", "cpuMillicores": 2000, "memoryBytes": 3 * 1024 ** 3},
            {"name": "narumq-2", "cpuMillicores": 500, "memoryBytes": 2048 * 1024}])

    def test_profile_timeouts_are_required_and_positive(self):
        self.assertEqual(controller.profile_timeouts({"readyTimeoutMillis": 600000, "resyncTimeoutMillis": 300000}),
                         (600.0, 300.0))
        for bad in ({}, {"readyTimeoutMillis": 1}, {"readyTimeoutMillis": 0, "resyncTimeoutMillis": 1},
                    {"readyTimeoutMillis": "x", "resyncTimeoutMillis": 1}):
            with self.assertRaises(controller.ControllerError):
                controller.profile_timeouts(bad)


class RenderingTests(unittest.TestCase):
    def test_job_template_placeholders_match_the_controller_values(self):
        template = controller.JOB_TEMPLATE.read_text(encoding="utf-8")
        self.assertEqual(set(controller.JOB_PLACEHOLDER.findall(template)), set(controller.JOB_VALUES))
        rendered = controller.render_job(template, job_values())
        self.assertNotIn("${", rendered)
        self.assertIn(f"image: {RUNNER}", rendered)
        self.assertIn("--mqtt-tls=nlb.internal:8883", rendered)

    def test_job_rejects_tags_missing_values_and_unsafe_text(self):
        template = controller.JOB_TEMPLATE.read_text(encoding="utf-8")
        with self.assertRaises(controller.ControllerError):
            controller.render_job(template, job_values(CAPACITY_RUNNER_IMAGE="repo/capacity-runner:latest"))
        with self.assertRaises(controller.ControllerError):
            controller.render_job(template, {k: v for k, v in job_values().items() if k != "TOOLBOX_IMAGE"})
        with self.assertRaises(controller.ControllerError):
            controller.render_job(template, job_values(TLS_SERVER_NAME="x\ninjected: true"))
        with self.assertRaises(controller.ControllerError):
            controller.render_job(template, job_values(RUN_ID="Bad_Id"))

    def test_kustomization_pins_the_runner_digest(self):
        text = controller.render_kustomization("D:/overlay", RUNNER)
        self.assertIn("newName: \"123.dkr.ecr.ap-northeast-2.amazonaws.com/capacity-runner\"", text)
        self.assertIn(f"digest: sha256:{DIGEST}", text)
        with self.assertRaises(controller.ControllerError):
            controller.render_kustomization("D:/overlay", "repo/capacity-runner:1")


class CommandTests(unittest.TestCase):
    def test_leader_kill_is_a_forced_zero_grace_delete_and_cordon(self):
        args = controller.force_delete_args("narumq-1")
        self.assertEqual(args[:3], ["delete", "pod", "narumq-1"])
        self.assertIn("--grace-period=0", args)
        self.assertIn("--force", args)
        self.assertEqual(controller.cordon_args("w", True), ["cordon", "w"])
        self.assertEqual(controller.cordon_args("w", False), ["uncordon", "w"])

    # Scenarios: performance-and-verification.md:PRF-SCN-053
    def test_atomic_marker_write_uses_a_hidden_temp_file_and_rename(self):
        command = controller.atomic_write_command("step-01-r1-kill.json")
        self.assertEqual(command[:2], ["sh", "-c"])
        self.assertIn("cat > /control/.step-01-r1-kill.json.tmp && mv /control/.step-01-r1-kill.json.tmp "
                      "/control/step-01-r1-kill.json", command[2])
        for bad in ("../x.json", "a b.json", "a;rm.json", ""):
            with self.assertRaises(controller.ControllerError):
                controller.atomic_write_command(bad)

    def test_context_is_mandatory_so_the_default_cluster_is_never_used(self):
        with self.assertRaises(controller.ControllerError):
            controller.kubectl_global_args("kubectl", "", None)
        self.assertEqual(controller.kubectl_global_args("kubectl", "capacity", "kc"),
                         ["kubectl", "--kubeconfig", "kc", "--context", "capacity"])

    # Scenarios: performance-and-verification.md:PRF-SCN-053
    def test_marker_payload_goes_through_stdin_not_argv(self):
        calls = []

        class FakeKube:
            def run(self, args, **kwargs):
                calls.append((args, kwargs))
                return ""

        controller.PodChannel(FakeKube(), "gen-pod").write_json(
            "step-01-r1-kill.json", controller.kill_marker("killing", 1, "narumq-0", "w", "t"))
        args, kwargs = calls[0]
        self.assertNotIn("narumq-0", " ".join(args[args.index("--"):]))
        self.assertEqual(json.loads(kwargs["stdin"])["state"], "killing")
        self.assertIn("-i", args)

    def test_sensitive_command_failure_never_reports_stderr(self):
        failed = subprocess.CompletedProcess(["kubectl"], 1, stdout="", stderr="token=SECRET-VALUE")
        kube = controller.Kubectl(["kubectl"])
        with patch.object(controller.subprocess, "run", return_value=failed):
            with self.assertRaises(controller.CommandFailed) as sensitive:
                kube.run(["get", "secret"], sensitive=True)
            with self.assertRaises(controller.CommandFailed) as plain:
                kube.run(["get", "pods"])
        self.assertNotIn("SECRET-VALUE", str(sensitive.exception))
        self.assertIn("exit 1", str(sensitive.exception))
        self.assertIn("exit 1", str(plain.exception))


class WaitUntilTests(unittest.TestCase):
    def test_returns_first_truthy_value_and_times_out_on_the_clock(self):
        now = [0.0]
        values = iter([None, None, "ok"])
        result = controller.wait_until(lambda: next(values), 10, "x", clock=lambda: now[0],
                                       sleep=lambda seconds: now.__setitem__(0, now[0] + seconds))
        self.assertEqual(result, "ok")
        with self.assertRaises(controller.ControllerError):
            controller.wait_until(lambda: None, 3, "never", interval=1, clock=lambda: now[0],
                                  sleep=lambda seconds: now.__setitem__(0, now[0] + seconds))


class PodResourceTests(unittest.TestCase):
    @staticmethod
    def container(name, cpu=None, memory=None, restart=None, limits=True):
        values = {key: value for key, value in (("cpu", cpu), ("memory", memory)) if value is not None}
        entry = {"name": name, "resources": {"requests": dict(values), "limits": dict(values) if limits else {}}}
        if restart:
            entry["restartPolicy"] = restart
        return entry

    def test_quantities(self):
        self.assertEqual([controller.parse_cpu_millicores(v) for v in ("50m", "1", "1.5", "2", "250000000n")],
                         [50, 1000, 1500, 2000, 250])
        self.assertEqual([controller.parse_memory_bytes(v) for v in ("64Mi", "4Gi", "256Ki", "1G", "1000", "1.5Gi")],
                         [64 * 1024 ** 2, 4 * 1024 ** 3, 256 * 1024, 10 ** 9, 1000, 3 * 1024 ** 3 // 2])

    # Scenarios: performance-and-verification.md:PRF-SCN-053
    def test_broker_pod_totals_separate_all_containers_from_steady_state(self):
        pod = {"metadata": {"name": "narumq-0"}, "spec": {
            "initContainers": [self.container("select-peer-identity", "50m", "64Mi"),
                               self.container("capacity-security-config", "100m", "256Mi")],
            "containers": [self.container("narumq", "1", "4Gi"), self.container("capacity-jwks", "50m", "64Mi")]}}
        summary = controller.pod_resource_summary(pod)
        self.assertEqual([(c["name"], c["kind"]) for c in summary["containers"]], [
            ("narumq", "container"), ("capacity-jwks", "container"),
            ("select-peer-identity", "initContainer"), ("capacity-security-config", "initContainer")])
        self.assertEqual(summary["containers"][0]["requests"], {"cpuMillicores": 1000, "memoryBytes": 4 * 1024 ** 3})
        self.assertEqual(summary["steadyState"]["requests"], {"cpuMillicores": 1050, "memoryBytes": 4 * 1024 ** 3 + 64 * 1024 ** 2})
        self.assertEqual(summary["sumAllContainers"]["limits"],
                         {"cpuMillicores": 1200, "memoryBytes": 4 * 1024 ** 3 + (64 + 64 + 256) * 1024 ** 2})

    # Scenarios: performance-and-verification.md:PRF-SCN-053
    def test_native_sidecar_counts_in_steady_state_and_missing_values_stay_unknown(self):
        pod = {"metadata": {"name": "gen"}, "spec": {
            "initContainers": [self.container("toolbox", "50m", "32Mi", restart="Always")],
            "containers": [self.container("capacity-runner", "3", "6Gi"), self.container("bare", None, None, limits=False)]}}
        summary = controller.pod_resource_summary(pod)
        self.assertTrue(summary["containers"][2]["restartable"])
        self.assertEqual(summary["containers"][1]["limits"], {"cpuMillicores": None, "memoryBytes": None})
        self.assertEqual(summary["sumAllContainers"]["requests"], {"cpuMillicores": None, "memoryBytes": None})
        only = {"metadata": {"name": "gen"}, "spec": {
            "initContainers": [self.container("toolbox", "50m", "32Mi", restart="Always")],
            "containers": [self.container("capacity-runner", "3", "6Gi")]}}
        self.assertEqual(controller.pod_resource_summary(only)["steadyState"]["requests"],
                         {"cpuMillicores": 3050, "memoryBytes": 6 * 1024 ** 3 + 32 * 1024 ** 2})


class RuntimeDiagnosticsTests(unittest.TestCase):
    @staticmethod
    def pod(uid="u1", container_id="containerd://c1", restarts=0, last_state=None):
        return {"metadata": {"name": "narumq-0", "uid": uid, "labels": {"app": "narumq"}},
                "spec": {"nodeName": "worker-a"},
                "status": {"phase": "Running", "containerStatuses": [{
                    "name": "narumq", "containerID": container_id, "restartCount": restarts,
                    "ready": True, "state": {"running": {"startedAt": "now"}},
                    "lastState": last_state or {},
                }], "initContainerStatuses": [{
                    "name": "init", "restartCount": 0, "state": {"terminated": {"exitCode": 0}},
                }]}}

    class Kube:
        def __init__(self, pod):
            self.pod = pod
            self.cpu = "v2\nusage_usec 999\nnr_periods 10\nnr_throttled 3\nthrottled_usec 400\n"
            self.fail = False
            self.replacement = None

        def json(self, args, **kwargs):
            if self.fail:
                raise controller.CommandFailed("unavailable")
            return {"items": [copy.deepcopy(self.pod)]}

        def run(self, args, **kwargs):
            if self.replacement:
                self.pod = self.replacement
            return self.cpu

    class Journal:
        def __init__(self):
            self.rows = []

        def write(self, row):
            self.rows.append(row)

    # Scenarios: performance-and-verification.md:PRF-SCN-058
    def test_cpu_versions_missing_and_invalid_counters(self):
        self.assertEqual(controller.parse_cpu_stat("v1\nnr_periods 10\nnr_throttled 2\nthrottled_time 1200900"),
                         {"cgroupVersion": "v1", "nr_periods": 10, "nr_throttled": 2, "throttled_usec": 1200})
        for bad in ("", "v3", "v2\nnr_periods 0", "v2\nnr_periods -1\nnr_throttled 0\nthrottled_usec 0"):
            with self.subTest(bad=bad), self.assertRaises((ValueError, KeyError)):
                controller.parse_cpu_stat(bad)

    # Scenarios: performance-and-verification.md:PRF-SCN-058
    def test_deltas_reset_at_restart_replacement_counter_reset_and_failed_read(self):
        kube, journal = self.Kube(self.pod()), self.Journal()
        sampler = controller.RuntimeDiagnostics(kube, journal, "generator")

        def sample():
            sampler.sample("periodic")
            return journal.rows[-1]["cpu"][0]

        self.assertIsNone(sample()["delta"])
        kube.cpu = "v2\nnr_periods 15\nnr_throttled 4\nthrottled_usec 600"
        self.assertEqual(sample()["delta"], {"nr_periods": 5, "nr_throttled": 1, "throttled_usec": 200})
        kube.pod = self.pod(container_id="containerd://c2", restarts=1)
        self.assertIsNone(sample()["delta"])
        kube.pod = self.pod(uid="u2")
        self.assertIsNone(sample()["delta"])
        kube.cpu = "v2\nnr_periods 1\nnr_throttled 0\nthrottled_usec 0"
        self.assertIsNone(sample()["delta"])
        kube.cpu = "v2\nnr_periods 1"
        failed = sample()
        self.assertFalse(failed["available"])
        self.assertNotIn("counters", failed)
        kube.cpu = "v2\nnr_periods 2\nnr_throttled 1\nthrottled_usec 50"
        self.assertIsNone(sample()["delta"])
        kube.fail = True
        sampler.sample("periodic")
        self.assertFalse(journal.rows[-1]["available"])
        kube.fail = False
        self.assertIsNone(sample()["delta"])

    # Scenarios: performance-and-verification.md:PRF-SCN-058
    def test_replacement_during_exec_discards_cpu_but_keeps_oom_evidence(self):
        oom = {"terminated": {"reason": "OOMKilled", "exitCode": 137, "signal": 9,
                               "startedAt": "before", "finishedAt": "after"}}
        pod = self.pod(restarts=1, last_state=oom)
        kube, journal = self.Kube(pod), self.Journal()
        kube.replacement = self.pod(uid="new")
        controller.RuntimeDiagnostics(kube, journal, "generator").sample("step-ready", "step-01-r1", "requested")
        row = journal.rows[-1]
        self.assertEqual(row["requestedAt"], "requested")
        self.assertEqual(row["pods"][0]["containers"][0]["lastState"], oom)
        self.assertEqual(row["pods"][0]["containers"][0]["restartCount"], 1)
        self.assertEqual(row["pods"][0]["containers"][1]["kind"], "initContainer")
        self.assertFalse(row["cpu"][0]["available"])
        self.assertNotIn("counters", row["cpu"][0])

    # Scenarios: performance-and-verification.md:PRF-SCN-058
    def test_generator_and_nonrunning_broker_status_are_preserved_without_exec(self):
        pod = self.pod()
        pod["status"]["containerStatuses"][0]["state"] = {"terminated": {"reason": "OOMKilled", "exitCode": 137}}
        generator = copy.deepcopy(pod)
        generator["metadata"] = {"name": "generator", "uid": "g"}
        journal, kube = self.Journal(), self.Kube(pod)
        kube.json = lambda *args, **kwargs: {"items": [pod, generator]}
        kube.run = lambda *args, **kwargs: self.fail("must not exec a terminated broker or the generator")
        controller.RuntimeDiagnostics(kube, journal, "generator").sample("run-end")
        row = journal.rows[-1]
        self.assertEqual(len(row["pods"]), 2)
        self.assertEqual(row["pods"][1]["containers"][0]["state"]["terminated"]["reason"], "OOMKilled")
        self.assertFalse(row["cpu"][0]["available"])


if __name__ == "__main__":
    unittest.main()
