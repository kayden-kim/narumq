"""Opt-in Docker acceptance for PRF-SCN-056, PRF-SCN-057 and PRF-SCN-059.

Not part of the CI unit list. Needs a native linux/amd64 Docker engine, Go and network access to pull
``alpine:3.20``. It builds ``./cmd/narumq`` into a throwaway image, runs real containers and removes
everything it created (containers, image, scratch directory) afterwards.

    NARUMQ_ACCEPT_DOCKER=1 python tests/run_scenario_unittest.py tests/acceptance_docker_capacity.py

The run records pass/fail per test method in ``.artifacts/verification/scenarios/python-results.jsonl``
(override with ``NARUMQ_SCENARIO_RESULTS``). Without ``NARUMQ_ACCEPT_DOCKER=1`` every test is skipped.
"""
from __future__ import annotations

import json
import os
import shutil
import sys
import tempfile
import time
import unittest
import uuid
from pathlib import Path

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parent))
import capacity_docker_harness as harness  # noqa: E402

ENABLED = os.environ.get("NARUMQ_ACCEPT_DOCKER") == "1"
BROKER_POD = "narumq-1"  # Raft node 2: broker_pod_name(2)
PREFIX = "step-01-r1"


@unittest.skipUnless(ENABLED, "set NARUMQ_ACCEPT_DOCKER=1 to run the Docker capacity fault acceptance")
class DockerCapacityFaultAcceptance(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.controller = harness.load_controller()
        cls.workspace = harness.Workspace()
        try:
            cls.workspace.build_broker_image()
        except BaseException:
            cls.workspace.cleanup()
            raise

    @classmethod
    def tearDownClass(cls):
        cls.workspace.cleanup()

    def setUp(self):
        self.remove_containers()
        self.addCleanup(self.remove_containers)
        self.artifacts = Path(tempfile.mkdtemp(prefix="narumq-prf-artifacts-"))
        self.addCleanup(shutil.rmtree, self.artifacts, ignore_errors=True)

    def remove_containers(self):
        ids = self.workspace.container_ids()
        if ids:
            harness.docker("rm", "-f", *ids, check=False)

    def assert_threads(self, container, stopped):
        states = harness.thread_states(container)
        self.assertTrue(states, "no broker thread was listed")
        if stopped:
            self.assertEqual(set(states), {"T"}, f"not every broker thread is stopped: {states}")
        else:
            self.assertNotIn("T", states, f"a broker thread is still stopped: {states}")

    # Scenarios: performance-and-verification.md:PRF-SCN-056
    def test_freeze_sigcont_wrong_instance_delete_error_and_term_on_a_real_broker(self):
        controller, workspace = self.controller, self.workspace
        container = workspace.start_broker(BROKER_POD, "uid-prf-056")
        harness.wait_http_ready(container)
        identity = harness.broker_identity(container)
        self.assertEqual(identity["podUID"], "uid-prf-056")

        # Every broker thread stops and HTTP stops answering.
        frozen = harness.process_script(container, "freeze", identity["instance"])
        self.assertEqual(frozen.returncode, 0, frozen.stderr)
        report = json.loads(frozen.stdout)
        self.assertEqual(report["pid"], identity["pid"])
        states = harness.thread_states(container)
        self.assertEqual(len(states), report["stoppedThreads"])
        self.assertEqual(set(states), {"T"})
        self.assertFalse(harness.http_ready(container, 2), "HTTP answered while every broker thread was stopped")

        # A wrong execution UUID is rejected and changes nothing while the broker is stopped.
        wrong = str(uuid.uuid4())
        for mode in ("resume", "freeze"):
            rejected = harness.process_script(container, mode, wrong)
            self.assertNotEqual(rejected.returncode, 0, f"{mode} with a wrong instance was accepted")
        self.assert_threads(container, stopped=True)

        # SIGCONT with the right instance makes the broker Ready again.
        resumed = harness.process_script(container, "resume", identity["instance"])
        self.assertEqual(resumed.returncode, 0, resumed.stderr)
        harness.wait_http_ready(container, timeout=30)
        self.assert_threads(container, stopped=False)

        # A wrong instance cannot stop a running broker either.
        rejected = harness.process_script(container, "freeze", wrong)
        self.assertNotEqual(rejected.returncode, 0)
        self.assertTrue(harness.http_ready(container, 2))
        self.assert_threads(container, stopped=False)
        self.assertEqual(harness.broker_identity(container), identity)

        # A controller whose pod deletion fails resumes the real process.
        kube = harness.DockerKube(controller, workspace, delete_mode="error")

        def scenario(run):
            run.handle_request_kill(PREFIX, {"leaderNodeId": 2})
            run.handle_kill(PREFIX)

        run = harness.run_controller(controller, kube, self.artifacts, scenario)
        with self.assertRaises(controller.CommandFailed):
            run.run()
        self.assertEqual(kube.delete_log, [BROKER_POD])
        harness.wait_http_ready(container, timeout=30)
        self.assert_threads(container, stopped=False)
        self.assertEqual(harness.broker_identity(container), identity)
        events = [event["event"] for event in harness.read_jsonl(self.artifacts / "controller" / "events.jsonl")]
        self.assertIn("broker-frozen", events)
        self.assertIn("broker-resumed-on-abort", events)

        # A normal TERM reaches the broker through the wrapper and the container exits 0.
        harness.docker("kill", "--signal", "TERM", container)
        exit_code = harness.docker("wait", container, timeout=60).stdout.strip()
        self.assertEqual(exit_code, "0")
        state = harness.inspect_container(container)["State"]
        self.assertEqual((state["ExitCode"], state["OOMKilled"]), (0, False))

    # Scenarios: performance-and-verification.md:PRF-SCN-057
    def test_ctrl_c_controller_exception_and_restore_timeout_resume_the_original_process(self):
        controller, workspace = self.controller, self.workspace

        def ctrl_c(run):
            raise KeyboardInterrupt

        def controller_exception(run):
            raise RuntimeError("injected controller exception")

        def restore_timeout(run):
            ok, reason = run.handle_restore(PREFIX)
            self.assertFalse(ok)
            self.assertIn("timeout waiting for three ready broker Pods", reason)

        cases = (
            ("Ctrl+C", ctrl_c, KeyboardInterrupt),
            ("controller exception", controller_exception, RuntimeError),
            ("restore timeout", restore_timeout, None),
        )
        for index, (name, inject, expected) in enumerate(cases):
            with self.subTest(name):
                self.remove_containers()
                uid = f"uid-prf-057-{index}"
                container = workspace.start_broker(BROKER_POD, uid)
                harness.wait_http_ready(container)
                identity = harness.broker_identity(container)
                started_at = harness.inspect_container(container)["State"]["StartedAt"]
                kube = harness.DockerKube(controller, workspace, delete_mode="accept")
                observed = {}

                def scenario(run):
                    run.handle_request_kill(PREFIX, {"leaderNodeId": 2})
                    run.handle_kill(PREFIX)
                    # Deletion was accepted, the original Pod (same UID, same container) is still there and frozen.
                    pod = kube.json(["get", "pod", BROKER_POD, "-n", controller.NAMESPACE])
                    observed["uid"] = pod["metadata"]["uid"]
                    self.assertEqual(kube.delete_log, [BROKER_POD])
                    self.assertEqual(observed["uid"], uid)
                    self.assert_threads(container, stopped=True)
                    inject(run)

                artifacts = self.artifacts / f"case-{index}"
                run = harness.run_controller(controller, kube, artifacts, scenario, ready_timeout_ms=3000)
                if expected is None:
                    self.assertEqual(run.run(), 0)
                else:
                    with self.assertRaises(expected):
                        run.run()

                # The original identity resumed: same process, same container start, HTTP Ready again.
                harness.wait_http_ready(container, timeout=30)
                self.assert_threads(container, stopped=False)
                self.assertEqual(harness.broker_identity(container), identity)
                self.assertEqual(harness.inspect_container(container)["State"]["StartedAt"], started_at)
                events = [event["event"] for event in harness.read_jsonl(artifacts / "controller" / "events.jsonl")]
                self.assertIn("broker-resumed-on-abort", events)
                # The worker was cordoned for the injection and is uncordoned again.
                self.assertEqual(kube.cordon_log, [("cordon", harness.WORKER), ("uncordon", harness.WORKER)])
                self.assertTrue({"worker-uncordoned", "worker-uncordoned-on-abort"} & set(events), events)

    # Scenarios: performance-and-verification.md:PRF-SCN-059
    def test_real_cpu_stat_throttling_restart_baseline_and_oom_exit_137(self):
        controller, workspace = self.controller, self.workspace
        kube = harness.DockerKube(controller, workspace)
        burner = workspace.start_plain("narumq-0", "uid-prf-059-cpu", "while :; do :; done",
                                       "--cpus", "0.2", "--memory", "64m")
        hog = workspace.start_plain("narumq-2", "uid-prf-059-oom",
                                    'x=$(head -c 200000000 /dev/zero | tr "\\000" a); echo ${#x}',
                                    "--memory", "32m", "--memory-swap", "32m")
        self.assertEqual(harness.docker("wait", hog, timeout=120).stdout.strip(), "137")

        runtime_path = self.artifacts / "runtime-health.jsonl"
        diagnostics = controller.RuntimeDiagnostics(kube, controller.Journal(runtime_path), "capacity-generator")
        sampler = controller.ResourceSampler(kube, controller.Journal(self.artifacts / "broker-resource.jsonl"), 3600,
                                             diagnostics)
        sampler.start()
        self.addCleanup(sampler.stop)

        def sample(boundary):
            sampler.request(boundary)
            deadline = time.monotonic() + 90
            while time.monotonic() < deadline:
                if runtime_path.exists():
                    for record in harness.read_jsonl(runtime_path):
                        if record["boundary"] == boundary:
                            return record
                time.sleep(0.5)
            self.fail(f"no runtime diagnostics record for {boundary}")

        def cpu_of(record, pod):
            return next(item for item in record["cpu"] if item["pod"] == pod)

        def direct_cpu_stat():
            text = harness.docker("exec", burner, *controller.CPU_STAT_COMMAND).stdout
            return controller.parse_cpu_stat(text)

        first = sample("accept-first")
        self.assertTrue(first["available"], first.get("reason"))
        first_cpu = cpu_of(first, "narumq-0")
        self.assertTrue(first_cpu["available"], first_cpu.get("reason"))
        self.assertEqual(first_cpu["counters"]["cgroupVersion"], "v2")
        self.assertGreater(first_cpu["counters"]["nr_periods"], 0)
        self.assertIsNone(first_cpu["delta"], "the first sample has no baseline to subtract")

        # The limited burner is throttled; wait until the real cgroup shows more throttling than the first sample.
        deadline = time.monotonic() + 30
        while direct_cpu_stat()["nr_throttled"] <= first_cpu["counters"]["nr_throttled"]:
            self.assertLess(time.monotonic(), deadline, "the CPU-limited container was never throttled")
            time.sleep(0.5)
        second = sample("accept-second")
        second_cpu = cpu_of(second, "narumq-0")
        self.assertTrue(second_cpu["available"], second_cpu.get("reason"))
        keys = ("nr_periods", "nr_throttled", "throttled_usec")
        self.assertEqual(second_cpu["delta"], {key: second_cpu["counters"][key] - first_cpu["counters"][key] for key in keys})
        for key in keys:
            self.assertGreater(second_cpu["delta"][key], 0, key)
        self.assertGreaterEqual(direct_cpu_stat()["nr_throttled"], second_cpu["counters"]["nr_throttled"])

        # After a container restart the cumulative counters restart and no delta is reported.
        harness.docker("restart", "-t", "1", burner)
        restarted = sample("accept-restarted")
        restarted_cpu = cpu_of(restarted, "narumq-0")
        self.assertTrue(restarted_cpu["available"], restarted_cpu.get("reason"))
        self.assertIsNone(restarted_cpu["delta"], "a delta crossed a container restart")
        self.assertEqual([pod for pod in restarted["pods"] if pod["name"] == "narumq-0"][0]["containers"][0]["restartCount"], 1)

        # The real OOM kill is recorded with exit code 137, and Docker agrees.
        oom_pod = next(pod for pod in restarted["pods"] if pod["name"] == "narumq-2")
        terminated = oom_pod["containers"][0]["state"]["terminated"]
        self.assertEqual((terminated["reason"], terminated["exitCode"]), ("OOMKilled", 137))
        self.assertFalse(cpu_of(restarted, "narumq-2")["available"])
        docker_state = harness.inspect_container(hog)["State"]
        self.assertEqual((docker_state["OOMKilled"], docker_state["ExitCode"]), (True, 137))

        sampler.stop()
        resource_records = harness.read_jsonl(self.artifacts / "broker-resource.jsonl")
        self.assertGreaterEqual(len(resource_records), 3)
        self.assertTrue(all("available" in record for record in resource_records))


if __name__ == "__main__":
    unittest.main()
