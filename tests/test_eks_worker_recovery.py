from __future__ import annotations

import copy
import datetime
import importlib.util
import json
import contextlib
import io
from pathlib import Path
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parents[1]
path = ROOT / "tests/chaos/eks-worker-recovery/observer.py"
spec = importlib.util.spec_from_file_location("eks_worker_observer", path)
observer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(observer)
sys.path.insert(0, str(path.parent))
main_spec = importlib.util.spec_from_file_location("eks_worker_main", path.parent / "main.py")
worker_main = importlib.util.module_from_spec(main_spec)
main_spec.loader.exec_module(worker_main)
sys.path.pop(0)


class SurvivorTests(unittest.TestCase):
    def setUp(self):
        self.plan = {"observerPod": "narumq-0", "observerPodUID": "survivor-uid",
                     "observerWorkerName": "survivor-worker", "observerInstanceId": "i-survivor",
                     "targetPod": "narumq-2", "oldWorkerName": "target-worker", "oldInstanceId": "i-target"}
        self.pod = {"metadata": {"uid": "survivor-uid", "labels": {"app": "narumq"}},
                    "spec": {"nodeName": "survivor-worker"},
                    "status": {"conditions": [{"type": "Ready", "status": "True"}]}}
        self.node = {"metadata": {}, "spec": {"providerID": "aws:///az/i-survivor"},
                     "status": {"conditions": [{"type": "Ready", "status": "True"}]}}
        self.state = "running"
        self.aws_mutations = []
        owner = self
        class Fixture:
            def get(self, *args):
                return copy.deepcopy(owner.pod)
            def get_node(self, name):
                return copy.deepcopy(owner.node)
            def instance_state(self, instance):
                return owner.state
            def aws(self, *args):
                owner.aws_mutations.append(args)
                raise AssertionError("unsafe fixture must never reach AWS mutation")
        self.fixture = Fixture()

    def test_target_pod_worker_or_instance_cannot_be_used_as_survivor(self):
        for key, value in (("observerPod", "narumq-2"), ("observerWorkerName", "target-worker"),
                           ("observerInstanceId", "i-target")):
            with self.subTest(key=key), patch.object(worker_main.subprocess, "Popen") as process:
                plan = {**self.plan, key: value}
                with self.assertRaises(ValueError):
                    worker_main.SurvivorTunnel(self.fixture, plan).start()
                process.assert_not_called()
                self.assertEqual(self.aws_mutations, [])

    def test_changed_identity_notready_or_terminating_survivor_is_rejected(self):
        changes = (
            lambda: self.pod["metadata"].update(uid="replacement-uid"),
            lambda: self.pod["spec"].update(nodeName="another-worker"),
            lambda: self.node["spec"].update(providerID="aws:///az/i-other"),
            lambda: self.pod["status"].update(conditions=[]),
            lambda: self.node["status"].update(conditions=[]),
            lambda: self.pod["metadata"].update(deletionTimestamp="now"),
            lambda: setattr(self, "state", "terminated"),
        )
        for change in changes:
            self.setUp()
            with self.subTest(change=change), patch.object(worker_main.subprocess, "Popen") as process:
                change()
                with self.assertRaises(ValueError):
                    worker_main.SurvivorTunnel(self.fixture, self.plan).start()
                process.assert_not_called()
                self.assertEqual(self.aws_mutations, [])

    def test_fault_cli_persists_refusal_before_mqtt_or_aws_mutation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            plan = {**self.plan, "observerPod": self.plan["targetPod"]}
            plan_path = root / "plan.json"
            plan_path.write_text(json.dumps(plan), encoding="utf-8")
            output = root / "run"
            with patch.object(worker_main, "Fixture", return_value=self.fixture), \
                 patch.object(worker_main, "MQTTProbe") as mqtt, contextlib.redirect_stdout(io.StringIO()):
                result = worker_main.main(["fault", "--private", str(root), "--output", str(output),
                                           "--plan", str(plan_path), "--execute"])
            report = json.loads((output / "summary.json").read_text())
            self.assertEqual(result, 1)
            self.assertEqual(report["result"], "FAIL")
            self.assertEqual(report["errorCode"], "ValueError")
            mqtt.assert_not_called()
            self.assertEqual(self.aws_mutations, [])


class DrainRecordTests(unittest.TestCase):
    def fixture(self, outcome):
        calls = []
        class Fixture:
            def api(self, path, **kwargs):
                calls.append((path, kwargs))
                if isinstance(outcome, Exception):
                    raise outcome
                return outcome
        return Fixture(), calls

    def test_absent_drain_record_uses_owned_default_tunnel_not_a_separate_port(self):
        error = worker_main.urllib.error.HTTPError("u", 404, "missing", {}, io.BytesIO())
        self.addCleanup(error.close)
        fixture, calls = self.fixture(error)
        worker_main.require_drain_record_absent(fixture, "2")
        self.assertEqual(calls, [("/api/v1/nodes/2/drain", {})])

    def test_existing_or_unreadable_drain_record_blocks_fault(self):
        existing, _ = self.fixture({"phase": "RUNNING"})
        with self.assertRaises(ValueError):
            worker_main.require_drain_record_absent(existing, "2")
        error = worker_main.urllib.error.HTTPError("u", 503, "unavailable", {}, io.BytesIO())
        self.addCleanup(error.close)
        unavailable, _ = self.fixture(error)
        with self.assertRaises(worker_main.urllib.error.HTTPError):
            worker_main.require_drain_record_absent(unavailable, "2")




class MonitorTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="narumq-observer-test-")
        self.addCleanup(self.temporary.cleanup)
        self.output = Path(self.temporary.name)

    def test_mqtt_progresses_while_management_is_blocked(self):
        management_entered = threading.Event()
        release_management = threading.Event()
        third_publish = threading.Event()
        sequence = 0
        def management():
            management_entered.set()
            release_management.wait(5)
            return {"quorumAvailable": True}
        def mqtt():
            nonlocal sequence
            current = sequence
            sequence += 1
            if sequence == 3:
                third_publish.set()
            return {"puback": True, "sequence": current}
        monitor = observer.IndependentMonitor(management, mqtt, self.output, interval=0.001)
        monitor.start()
        try:
            self.assertTrue(management_entered.wait(5))
            self.assertTrue(third_publish.wait(5))
            while_blocked = monitor.snapshot()
            self.assertEqual(while_blocked["management"], [])
            self.assertEqual([s["data"]["sequence"] for s in while_blocked["mqtt"][:2]], [0, 1])
        finally:
            release_management.set()
            monitor.stop()
        self.assertEqual(monitor.summary()["result"], "PASS")
        journal = [json.loads(line) for line in (self.output / "mqtt-samples.jsonl").read_text().splitlines()]
        self.assertEqual([s["data"]["sequence"] for s in journal[:3]], [0, 1, 2])
        self.assertTrue(all(s["ok"] and s["data"]["puback"] for s in journal))

    def test_timeout_is_recorded_without_stopping_later_probes_or_leaking_error_text(self):
        recovered = threading.Event()
        calls = 0
        def management():
            nonlocal calls
            calls += 1
            if calls == 1:
                raise TimeoutError("Bearer very-secret-token")
            recovered.set()
            return {"quorumAvailable": True}
        monitor = observer.IndependentMonitor(management, lambda: {"puback": True}, self.output, interval=0.001)
        monitor.start()
        try:
            self.assertTrue(recovered.wait(5))
        finally:
            monitor.stop()
        samples = monitor.snapshot()["management"]
        self.assertEqual(samples[0]["errorCode"], "TimeoutError")
        self.assertTrue(any(s["ok"] for s in samples[1:]))
        self.assertEqual(monitor.summary()["result"], "FAIL")
        text = (self.output / "management-samples.jsonl").read_text()
        self.assertNotIn("very-secret-token", text)
        self.assertTrue(all(s["ok"] and s["data"]["puback"] for s in monitor.snapshot()["mqtt"]))

    def test_negative_quorum_or_puback_cannot_pass(self):
        for stream in ("management", "mqtt"):
            with self.subTest(stream=stream):
                monitor = observer.IndependentMonitor(lambda: {"quorumAvailable": stream != "management"},
                    lambda: {"puback": stream != "mqtt"}, self.output / stream, interval=1)
                monitor.start()
                try:
                    self.assertTrue(monitor.wait_initial(5))
                finally:
                    monitor.stop()
                self.assertEqual(monitor.summary()["result"], "FAIL")
                self.assertEqual(monitor.summary()["streams"][stream]["failed"], 1)

    def test_journal_failure_cannot_be_reported_as_pass(self):
        second = threading.Event()
        release = threading.Event()
        calls = 0
        def management():
            nonlocal calls
            calls += 1
            if calls == 2:
                second.set()
                release.wait(5)
            return {"quorumAvailable": True}
        monitor = observer.IndependentMonitor(management, lambda: {"puback": True}, self.output, interval=0.001)
        monitor.start()
        try:
            self.assertTrue(second.wait(5))
            monitor._streams["management"].close()
            release.set()
            with self.assertRaisesRegex(RuntimeError, "journal failed"):
                monitor.stop()
        finally:
            release.set()
        self.assertEqual(monitor.summary()["result"], "FAIL")
        self.assertEqual(monitor.summary()["journalErrors"], ["management:ValueError"])

    def test_shutdown_timeout_remains_a_failure_after_the_worker_finishes(self):
        entered = threading.Event()
        release = threading.Event()
        def management():
            entered.set()
            release.wait(5)
            return {"quorumAvailable": True}
        monitor = observer.IndependentMonitor(management, lambda: {"puback": True}, self.output, interval=1)
        monitor.start()
        try:
            self.assertTrue(entered.wait(5))
            with self.assertRaises(TimeoutError):
                monitor.stop(timeout=0.01)
            self.assertFalse(monitor.summary()["workersStoppedAndJournalsClosed"])
        finally:
            release.set()
            monitor.stop()
        self.assertTrue(monitor.summary()["workersStoppedAndJournalsClosed"])
        self.assertTrue(monitor.summary()["shutdownDeadlineExceeded"])
        self.assertEqual(monitor.summary()["result"], "FAIL")


class DiagnosticStreamTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="narumq-diagnostic-test-")
        self.addCleanup(self.temporary.cleanup)
        self.output = Path(self.temporary.name)

    def test_diagnostic_is_journaled_while_management_is_blocked_and_does_not_gate_pass(self):
        management_entered = threading.Event()
        release_management = threading.Event()
        second_node_sample = threading.Event()
        calls = 0
        def management():
            management_entered.set()
            release_management.wait(5)
            return {"quorumAvailable": True}
        def nodes():
            nonlocal calls
            calls += 1
            if calls == 2:
                second_node_sample.set()
            return [{"nodeId": "narumq-0", "queryStatus": "OK"}]
        monitor = observer.IndependentMonitor(management, lambda: {"puback": True}, self.output, interval=1,
                                              diagnostics={"nodes": nodes}, diagnostic_interval=0.001)
        monitor.start()
        try:
            self.assertTrue(management_entered.wait(5))
            self.assertTrue(second_node_sample.wait(5))
            self.assertEqual(monitor.snapshot()["management"], [])
        finally:
            release_management.set()
            monitor.stop()
        summary = monitor.summary()
        self.assertEqual(summary["result"], "PASS")
        self.assertNotIn("nodes", summary["streams"])
        self.assertGreaterEqual(summary["diagnostics"]["nodes"]["samples"], 2)
        self.assertEqual(summary["diagnostics"]["nodes"]["failed"], 0)
        journal = [json.loads(line) for line in (self.output / "nodes-samples.jsonl").read_text().splitlines()]
        self.assertTrue(all(s["ok"] for s in journal))
        self.assertEqual(journal[0]["data"], [{"nodeId": "narumq-0", "queryStatus": "OK"}])

    def test_diagnostic_error_is_recorded_without_leaking_text_or_failing_the_run(self):
        calls = 0
        recovered = threading.Event()
        def nodes():
            nonlocal calls
            calls += 1
            if calls == 1:
                raise TimeoutError("Bearer diagnostic-secret")
            recovered.set()
            return [{"nodeId": "narumq-0", "queryStatus": "OK"}]
        monitor = observer.IndependentMonitor(lambda: {"quorumAvailable": True}, lambda: {"puback": True},
                                              self.output, interval=0.001, diagnostics={"nodes": nodes},
                                              diagnostic_interval=0.001)
        monitor.start()
        try:
            self.assertTrue(recovered.wait(5))
        finally:
            monitor.stop()
        samples = monitor.snapshot()["nodes"]
        self.assertEqual(samples[0]["errorCode"], "TimeoutError")
        self.assertTrue(any(s["ok"] for s in samples[1:]))
        summary = monitor.summary()
        self.assertEqual(summary["result"], "PASS")
        self.assertEqual(summary["diagnostics"]["nodes"]["failed"], 1)
        self.assertNotIn("diagnostic-secret", (self.output / "nodes-samples.jsonl").read_text())

    def test_diagnostic_journal_failure_cannot_be_reported_as_pass(self):
        second = threading.Event()
        release = threading.Event()
        calls = 0
        def nodes():
            nonlocal calls
            calls += 1
            if calls == 2:
                second.set()
                release.wait(5)
            return [{"nodeId": "narumq-0", "queryStatus": "OK"}]
        monitor = observer.IndependentMonitor(lambda: {"quorumAvailable": True}, lambda: {"puback": True},
                                              self.output, interval=1, diagnostics={"nodes": nodes},
                                              diagnostic_interval=0.001)
        monitor.start()
        try:
            self.assertTrue(second.wait(5))
            monitor._streams["nodes"].close()
            release.set()
            with self.assertRaisesRegex(RuntimeError, "journal failed"):
                monitor.stop()
        finally:
            release.set()
        self.assertEqual(monitor.summary()["result"], "FAIL")
        self.assertEqual(monitor.summary()["journalErrors"], ["nodes:ValueError"])

    def test_diagnostic_that_does_not_stop_in_time_fails_the_run(self):
        entered = threading.Event()
        release = threading.Event()
        def nodes():
            entered.set()
            release.wait(5)
            return [{"nodeId": "narumq-0", "queryStatus": "OK"}]
        monitor = observer.IndependentMonitor(lambda: {"quorumAvailable": True}, lambda: {"puback": True},
                                              self.output, interval=1, diagnostics={"nodes": nodes},
                                              diagnostic_interval=1)
        monitor.start()
        try:
            self.assertTrue(entered.wait(5))
            with self.assertRaises(TimeoutError):
                monitor.stop(timeout=0.01)
        finally:
            release.set()
            monitor.stop()
        summary = monitor.summary()
        self.assertTrue(summary["shutdownDeadlineExceeded"])
        self.assertEqual(summary["result"], "FAIL")
        self.assertEqual(summary["diagnostics"]["nodes"]["samples"], 1)

    def test_summary_without_diagnostics_keeps_the_existing_keys(self):
        monitor = observer.IndependentMonitor(lambda: {"quorumAvailable": True}, lambda: {"puback": True},
                                              self.output, interval=1)
        monitor.start()
        try:
            self.assertTrue(monitor.wait_initial(5))
        finally:
            monitor.stop()
        summary = monitor.summary()
        self.assertEqual(summary["result"], "PASS")
        self.assertEqual(sorted(summary), ["journalErrors", "result", "scope", "shutdownDeadlineExceeded",
                                           "streams", "workersStoppedAndJournalsClosed"])
        self.assertFalse((self.output / "nodes-samples.jsonl").exists())

    def test_diagnostic_cannot_reuse_an_acceptance_name_or_use_a_non_positive_interval(self):
        with self.assertRaisesRegex(ValueError, "collides"):
            observer.IndependentMonitor(lambda: {}, lambda: {}, self.output, diagnostics={"mqtt": lambda: []})
        with self.assertRaisesRegex(ValueError, "positive"):
            observer.IndependentMonitor(lambda: {}, lambda: {}, self.output, diagnostics={"nodes": lambda: []},
                                        diagnostic_interval=0)


class NodeDiagnosticTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="narumq-node-diagnostic-test-")
        self.addCleanup(self.temporary.cleanup)
        self.private = Path(self.temporary.name)
        (self.private / "admin.token").write_text("test-token\n", encoding="utf-8")
        self.fixture = worker_main.Fixture(self.private, "run-test")

    def test_node_diagnostic_keeps_only_the_fields_needed_for_diagnosis(self):
        node = {"nodeId": "narumq-0", "membershipPhase": "VOTER", "queryStatus": "OK", "queryErrorCode": None,
                "state": "READY", "ready": True, "readinessReason": None,
                "version": "1.0", "uptimeMillis": 10, "lastErrorCode": "Bearer diagnostic-secret"}
        with patch.object(worker_main.Fixture, "api", return_value=[node]) as api:
            self.assertEqual(self.fixture.nodes_probe(), [{"nodeId": "narumq-0", "membershipPhase": "VOTER",
                                                           "queryStatus": "OK", "queryErrorCode": None,
                                                           "state": "READY", "ready": True, "readinessReason": None}])
        api.assert_called_once_with("/api/v1/nodes")

    def test_non_list_node_response_is_rejected(self):
        with patch.object(worker_main.Fixture, "api", return_value={"nodes": []}):
            with self.assertRaisesRegex(ValueError, "not a list"):
                self.fixture.nodes_probe()


class DiagnosticWiringTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="narumq-diagnostic-wiring-test-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def test_monitor_mode_writes_no_nodes_stream_and_summary_has_no_diagnostics(self):
        output = self.root / "monitor"
        with contextlib.ExitStack() as stack:
            fixture_class = stack.enter_context(patch.object(worker_main, "Fixture"))
            mqtt_class = stack.enter_context(patch.object(worker_main, "MQTTProbe"))
            fixture_class.return_value.management_probe.return_value = {"quorumAvailable": True}
            mqtt_class.return_value.probe.return_value = {"puback": True}
            code = worker_main.main(["monitor", "--private", str(self.root), "--output", str(output),
                                     "--seconds", "0.01"])
        self.assertEqual(code, 0)
        self.assertFalse((output / "nodes-samples.jsonl").exists())
        self.assertNotIn("diagnostics", json.loads((output / "summary.json").read_text(encoding="utf-8"))["monitor"])

    def test_fault_mode_passes_only_the_nodes_diagnostic_to_the_monitor(self):
        plan = self.root / "plan.json"
        plan.write_text(json.dumps({"observerPod": "narumq-0"}), encoding="utf-8")
        with contextlib.ExitStack() as stack:
            fixture_class = stack.enter_context(patch.object(worker_main, "Fixture"))
            tunnel_class = stack.enter_context(patch.object(worker_main, "SurvivorTunnel"))
            stack.enter_context(patch.object(worker_main, "preflight", return_value=[]))
            stack.enter_context(patch.object(worker_main, "MQTTProbe"))
            monitor_class = stack.enter_context(patch.object(worker_main, "IndependentMonitor"))
            tunnel_class.return_value.start.return_value = {"localPort": 23880}
            monitor_class.return_value.start.side_effect = RuntimeError("stop after wiring")
            monitor_class.return_value.summary.return_value = {"result": "FAIL"}
            code = worker_main.main(["fault", "--private", str(self.root), "--output", str(self.root / "fault"),
                                     "--plan", str(plan), "--execute"])
        self.assertEqual(code, 1)
        diagnostics = monitor_class.call_args.kwargs["diagnostics"]
        self.assertEqual(set(diagnostics), {"nodes"})
        diagnostics["nodes"]()
        fixture_class.return_value.nodes_probe.assert_called_once_with()
        tunnel_class.return_value.check.assert_called()



class DiagnosticBaselineTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="narumq-diagnostic-baseline-test-")
        self.addCleanup(self.temporary.cleanup)
        self.output = Path(self.temporary.name)

    def make_monitor(self, nodes):
        return observer.IndependentMonitor(lambda: {"quorumAvailable": True}, lambda: {"puback": True},
                                           self.output, interval=1, diagnostics={"nodes": nodes},
                                           diagnostic_interval=0.001)

    def test_baseline_is_available_once_the_first_healthy_sample_has_completed(self):
        monitor = self.make_monitor(lambda: [{"nodeId": "narumq-0", "queryStatus": "OK"}])
        monitor.start()
        try:
            baseline = monitor.wait_diagnostic_baseline("nodes", 5)
        finally:
            monitor.stop()
        self.assertTrue(baseline["available"])
        self.assertTrue(baseline["firstSampleOk"])
        self.assertIsNotNone(baseline["firstSampleFinishedAt"])
        self.assertFalse(baseline["firstSampleMayOverlapFault"])
        self.assertGreaterEqual(baseline["samplesAtWaitEnd"], 1)
        self.assertEqual(monitor.summary()["diagnostics"]["nodes"]["baseline"], baseline)

    def test_baseline_stops_waiting_at_the_limit_when_the_probe_blocks(self):
        release = threading.Event()
        def nodes():
            release.wait(5)
            return [{"nodeId": "narumq-0", "queryStatus": "OK"}]
        monitor = self.make_monitor(nodes)
        monitor.start()
        try:
            baseline = monitor.wait_diagnostic_baseline("nodes", 0.2)
        finally:
            release.set()
            monitor.stop()
        self.assertFalse(baseline["available"])
        self.assertIsNone(baseline["firstSampleFinishedAt"])
        self.assertTrue(baseline["firstSampleMayOverlapFault"])
        self.assertEqual(baseline["samplesAtWaitEnd"], 0)
        self.assertGreaterEqual(baseline["waitedSeconds"], 0.2)
        self.assertLess(baseline["waitedSeconds"], 2)

    def test_failed_first_sample_is_not_a_healthy_baseline_and_does_not_gate_pass(self):
        def nodes():
            raise TimeoutError("Bearer diagnostic-secret")
        monitor = self.make_monitor(nodes)
        monitor.start()
        try:
            baseline = monitor.wait_diagnostic_baseline("nodes", 5)
            self.assertTrue(monitor.wait_initial(5))
        finally:
            monitor.stop()
        self.assertFalse(baseline["available"])
        self.assertFalse(baseline["firstSampleOk"])
        self.assertFalse(baseline["firstSampleMayOverlapFault"])
        self.assertLess(baseline["waitedSeconds"], 5)
        self.assertEqual(monitor.summary()["result"], "PASS")
        self.assertNotIn("diagnostic-secret", json.dumps(monitor.summary()))

    def test_baseline_rejects_acceptance_streams_and_non_positive_limits(self):
        monitor = self.make_monitor(lambda: [])
        with self.assertRaisesRegex(ValueError, "not a diagnostic stream"):
            monitor.wait_diagnostic_baseline("management", 1)
        with self.assertRaisesRegex(ValueError, "positive"):
            monitor.wait_diagnostic_baseline("nodes", 0)


class DiagnosticBaselineWiringTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="narumq-baseline-wiring-test-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.plan = self.root / "plan.json"
        self.plan.write_text(json.dumps({"observerPod": "narumq-0"}), encoding="utf-8")

    def run_fault(self, name, nodes_probe, limit, release=None, pod_release="deleted"):
        """Run fault mode; the fake fault step records the monitor's baseline at the moment the fault is requested."""
        observed = {"calls": 0}
        monitors = []

        class RecordingMonitor(observer.IndependentMonitor):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                monitors.append(self)

        def fake_observe_fault(fixture, plan, output, original):
            observed["calls"] += 1
            observed["baselineAtFault"] = monitors[0].summary()["diagnostics"]["nodes"]["baseline"]
            if release is not None:
                release.set()
            return {"fencing": {"outcome": "out_of_service"}, "podRelease": {"outcome": pod_release}}

        with contextlib.ExitStack() as stack:
            fixture_class = stack.enter_context(patch.object(worker_main, "Fixture"))
            tunnel_class = stack.enter_context(patch.object(worker_main, "SurvivorTunnel"))
            stack.enter_context(patch.object(worker_main, "preflight", return_value=[]))
            mqtt_class = stack.enter_context(patch.object(worker_main, "MQTTProbe"))
            stack.enter_context(patch.object(worker_main, "IndependentMonitor", RecordingMonitor))
            stack.enter_context(patch.object(worker_main, "observe_fault", side_effect=fake_observe_fault))
            stack.enter_context(patch.object(worker_main, "DIAGNOSTIC_BASELINE_WAIT_SECONDS", limit))
            fixture = fixture_class.return_value
            fixture.management_probe.return_value = {"quorumAvailable": True}
            fixture.nodes_probe.side_effect = nodes_probe
            mqtt_class.return_value.probe.return_value = {"puback": True}
            tunnel_class.return_value.start.return_value = {"localPort": 23880}
            code = worker_main.main(["fault", "--private", str(self.root), "--output", str(self.root / name),
                                     "--plan", str(self.plan), "--execute"])
        summary = json.loads((self.root / name / "summary.json").read_text(encoding="utf-8"))
        return code, summary, observed

    def test_fault_is_requested_only_after_a_completed_healthy_nodes_baseline(self):
        code, summary, observed = self.run_fault("fault-ok", lambda: [{"nodeId": "narumq-0", "queryStatus": "OK"}],
                                                 limit=5)
        baseline = summary["monitor"]["diagnostics"]["nodes"]["baseline"]
        self.assertEqual(code, 0)
        self.assertEqual(observed["calls"], 1)
        self.assertTrue(observed["baselineAtFault"]["available"])
        self.assertTrue(baseline["available"])
        self.assertLessEqual(datetime.datetime.fromisoformat(baseline["firstSampleFinishedAt"]),
                             datetime.datetime.fromisoformat(summary["faultRequestedAt"]))
        self.assertEqual(summary["result"], "PASS")

    def test_blocked_nodes_baseline_lets_the_fault_proceed_after_the_limit_without_changing_the_result(self):
        release = threading.Event()
        def blocked():
            release.wait(5)
            return [{"nodeId": "narumq-0", "queryStatus": "OK"}]
        code, summary, observed = self.run_fault("fault-blocked", blocked, limit=0.2, release=release)
        baseline = observed["baselineAtFault"]
        self.assertEqual(observed["calls"], 1)
        self.assertFalse(baseline["available"])
        self.assertTrue(baseline["firstSampleMayOverlapFault"])
        self.assertGreaterEqual(baseline["waitedSeconds"], 0.2)
        self.assertIn("faultRequestedAt", summary)
        self.assertEqual(code, 0)
        self.assertEqual(summary["result"], "PASS")
        self.assertNotIn("errorCode", summary)

    def test_failing_nodes_baseline_lets_the_fault_proceed_without_changing_the_result(self):
        def failing():
            raise TimeoutError("Bearer diagnostic-secret")
        code, summary, observed = self.run_fault("fault-failing", failing, limit=5)
        baseline = observed["baselineAtFault"]
        self.assertEqual(observed["calls"], 1)
        self.assertFalse(baseline["available"])
        self.assertFalse(baseline["firstSampleOk"])
        self.assertFalse(baseline["firstSampleMayOverlapFault"])
        self.assertEqual(summary["monitor"]["diagnostics"]["nodes"]["failed"], 1)
        self.assertEqual(code, 0)
        self.assertEqual(summary["result"], "PASS")
        self.assertNotIn("diagnostic-secret", json.dumps(summary))

    def test_old_pod_that_survives_the_release_attempt_fails_the_run(self):
        healthy = lambda: [{"nodeId": "narumq-0", "queryStatus": "OK"}]
        for outcome in ("failed_unexplained", "delete_unconfirmed"):
            with self.subTest(outcome=outcome):
                code, summary, _ = self.run_fault("fault-" + outcome, healthy, limit=5, pod_release=outcome)
                self.assertEqual(code, 1)
                self.assertEqual(summary["result"], "FAIL")
        for outcome in ("node_absent", "pod_absent", "skipped_not_fenced"):
            with self.subTest(outcome=outcome):
                code, summary, _ = self.run_fault("fault-" + outcome, healthy, limit=5, pod_release=outcome)
                self.assertEqual(summary["result"], "PASS")


class NodeServer:
    """Stateful JSON Patch model: tests reject writes against stale identity."""
    def __init__(self):
        self.node = {"metadata": {"uid": "old-uid", "resourceVersion": "1"},
                     "spec": {"providerID": "aws:///az/i-old", "taints": [
                         {"key": "example.test/maintenance", "value": "keep", "effect": "NoSchedule"}]}}
        self.before_patch = lambda: None
        self.after_patch = lambda: None
        self.patches = []
        self.reads = 0

    def get(self, name):
        self.reads += 1
        return copy.deepcopy(self.node)

    def patch(self, name, operations):
        self.patches.append(copy.deepcopy(operations))
        self.before_patch()
        if self.node is None:
            raise observer.MutationRejected("HTTP 404")
        for op in operations[:2]:
            key = op["path"].rsplit("/", 1)[-1]
            if self.node["metadata"][key] != op["value"]:
                raise observer.MutationRejected("HTTP 422; failed operation not specified")
        self.node["spec"]["taints"] = copy.deepcopy(operations[2]["value"])
        self.node["metadata"]["resourceVersion"] = "2"
        self.after_patch()


class FenceTests(unittest.TestCase):
    def setUp(self):
        self.server = NodeServer()

    def fence(self, state=lambda _: "terminated"):
        return observer.fence_node(self.server.get, self.server.patch, state,
                                   "old-node", "old-uid", "i-old")

    def test_nonterminated_instance_never_allows_node_mutation(self):
        for state in ("running", "stopping", "stopped", "shutting-down", "unknown"):
            with self.subTest(state=state):
                with self.assertRaises(observer.FencingUnsafe):
                    self.fence(lambda _: state)
        self.assertEqual(self.server.patches, [])
        self.assertEqual(self.server.reads, 0)

    def test_state_change_before_patch_blocks_mutation(self):
        states = iter(("terminated", "running"))
        with self.assertRaises(observer.FencingUnsafe):
            self.fence(lambda _: next(states))
        self.assertEqual(self.server.patches, [])

    def test_absent_or_replaced_node_is_never_mutated(self):
        for replacement in (None, {"metadata": {"uid": "new-uid", "resourceVersion": "9"},
                                  "spec": {"providerID": "aws:///az/i-new"}}):
            with self.subTest(replacement=replacement):
                self.server.node = copy.deepcopy(replacement)
                result = self.fence()
                self.assertIn(result["outcome"], ("node_absent", "node_identity_changed"))
                self.assertFalse(result["mutationAttempted"])
                self.assertEqual(self.server.node, replacement)
        self.assertEqual(self.server.patches, [])

    def test_provider_change_with_same_uid_does_not_fence_another_instance(self):
        self.server.node["spec"]["providerID"] = "aws:///az/i-new"
        result = self.fence()
        self.assertEqual(result["outcome"], "node_identity_changed")
        self.assertEqual(self.server.patches, [])

    def test_patch_preserves_taints_and_requires_both_identity_preconditions(self):
        original = copy.deepcopy(self.server.node["spec"]["taints"])
        result = self.fence()
        self.assertTrue(result["mutationApplied"])
        self.assertEqual(self.server.node["spec"]["taints"], original + [
            {"key": observer.OUT_OF_SERVICE, "value": "fenced-terminated", "effect": "NoExecute"}])
        self.assertEqual(self.server.patches[0][:2], [
            {"op": "test", "path": "/metadata/uid", "value": "old-uid"},
            {"op": "test", "path": "/metadata/resourceVersion", "value": "1"}])

    def test_stale_resource_version_is_refreshed_without_retry_or_cause_claim(self):
        self.server.before_patch = lambda: self.server.node["metadata"].update(resourceVersion="7")
        result = self.fence()
        self.assertEqual(result["outcome"], "failed_observation_changed")
        self.assertEqual(result["beforeResourceVersion"], "1")
        self.assertEqual(result["afterResourceVersion"], "7")
        self.assertTrue(result["patchFailed"])
        self.assertFalse(result["failureCauseConfirmed"])
        self.assertIsNone(result["mutationApplied"])
        self.assertEqual(len(self.server.patches), 1)
        self.assertFalse(any(t["key"] == observer.OUT_OF_SERVICE for t in self.server.node["spec"]["taints"]))

    def test_delete_during_patch_resumes_read_only_observation(self):
        self.server.before_patch = lambda: setattr(self.server, "node", None)
        result = self.fence()
        self.assertEqual(result["outcome"], "node_absent")
        self.assertTrue(result["patchFailed"])
        self.assertEqual(len(self.server.patches), 1)

    def test_uid_replacement_during_patch_does_not_mutate_the_replacement(self):
        replacement = {"metadata": {"uid": "new-uid", "resourceVersion": "8"},
                       "spec": {"providerID": "aws:///az/i-new", "taints": []}}
        self.server.before_patch = lambda: setattr(self.server, "node", copy.deepcopy(replacement))
        result = self.fence()
        self.assertEqual(result["outcome"], "node_identity_changed")
        self.assertTrue(result["patchFailed"])
        self.assertEqual(self.server.node, replacement)
        self.assertEqual(len(self.server.patches), 1)

    def test_unchanged_rejection_is_not_classified_as_a_known_cas_failure(self):
        def reject():
            raise observer.MutationRejected("HTTP 422; unknown validation error")
        self.server.before_patch = reject
        result = self.fence()
        self.assertEqual(result["outcome"], "failed_unexplained")
        self.assertFalse(result["failureCauseConfirmed"])
        self.assertEqual(result["beforeResourceVersion"], result["afterResourceVersion"])
        self.assertEqual(len(self.server.patches), 1)

    def test_response_loss_does_not_claim_our_mutation_was_applied(self):
        def lose_response():
            raise observer.MutationRejected("response unavailable after server applied patch")
        self.server.after_patch = lose_response
        result = self.fence()
        self.assertEqual(result["outcome"], "out_of_service")
        self.assertIsNone(result["mutationApplied"])
        self.assertTrue(result["patchFailed"])
        self.assertEqual(len(self.server.patches), 1)

    def test_already_fenced_node_is_not_patched_again(self):
        self.server.node["spec"]["taints"].append({"key": observer.OUT_OF_SERVICE, "effect": "NoExecute"})
        before = copy.deepcopy(self.server.node)
        result = self.fence()
        self.assertEqual(result["outcome"], "already_out_of_service")
        self.assertEqual(self.server.node, before)
        self.assertEqual(self.server.patches, [])

    def test_noschedule_alone_does_not_establish_the_required_noexecute_fencing(self):
        existing = {"key": observer.OUT_OF_SERVICE, "effect": "NoSchedule"}
        self.server.node["spec"]["taints"].append(existing)
        result = self.fence()
        self.assertEqual(result["outcome"], "out_of_service")
        self.assertTrue(result["mutationApplied"])
        self.assertIn(existing, self.server.node["spec"]["taints"])
        self.assertIn({"key": observer.OUT_OF_SERVICE, "value": "fenced-terminated", "effect": "NoExecute"},
                      self.server.node["spec"]["taints"])


class PodServer:
    """Pod store honouring the UID precondition like the API server."""
    def __init__(self):
        self.pod = {"metadata": {"uid": "old-pod-uid", "deletionTimestamp": "2026-10-15T14:18:38Z"},
                    "spec": {"nodeName": "old-node"}}
        self.before_delete = lambda: None
        self.deletes = []

    def get(self, name):
        return copy.deepcopy(self.pod)

    def delete(self, name, uid):
        self.deletes.append(uid)
        self.before_delete()
        if self.pod is None:
            raise observer.MutationRejected("HTTP 404")
        if self.pod["metadata"]["uid"] != uid:
            raise observer.MutationRejected("HTTP 409 precondition failed")
        self.pod = None


class PodReleaseTests(unittest.TestCase):
    def setUp(self):
        self.nodes = NodeServer()
        self.nodes.node["spec"]["taints"].append({"key": observer.OUT_OF_SERVICE, "effect": "NoExecute"})
        self.pods = PodServer()

    def release(self, state=lambda _: "terminated", fencing="out_of_service"):
        return observer.release_fenced_pod(self.nodes.get, self.pods.get, self.pods.delete, state,
                                           {"outcome": fencing}, "old-node", "old-uid", "i-old",
                                           "narumq-2", "old-pod-uid")

    def test_fenced_terminated_node_releases_the_old_pod_once_with_its_uid(self):
        result = self.release()
        self.assertEqual(result["outcome"], "deleted")
        self.assertTrue(result["deleteApplied"])
        self.assertEqual(self.pods.deletes, ["old-pod-uid"])
        self.assertIsNone(self.pods.pod)

    def test_unfenced_outcomes_never_delete(self):
        for fencing in ("node_absent", "failed_unexplained", "mutation_unconfirmed", "failed_observation_changed"):
            with self.subTest(fencing=fencing):
                self.assertEqual(self.release(fencing=fencing)["outcome"], "skipped_not_fenced")
        self.assertEqual(self.pods.deletes, [])

    def test_nonterminated_instance_never_allows_pod_deletion(self):
        for states in (("running",), ("terminated", "running")):
            with self.subTest(states=states):
                sequence = iter(states)
                with self.assertRaises(observer.FencingUnsafe):
                    self.release(lambda _: next(sequence))
        self.assertEqual(self.pods.deletes, [])

    def test_node_must_still_exist_with_the_same_identity_and_taint(self):
        cases = {"node_absent": None,
                 "node_identity_changed": {"metadata": {"uid": "new-uid"}, "spec": {"providerID": "aws:///az/i-new"}},
                 "node_not_fenced": {"metadata": {"uid": "old-uid"}, "spec": {"providerID": "aws:///az/i-old",
                     "taints": [{"key": observer.OUT_OF_SERVICE, "effect": "NoSchedule"}]}}}
        for outcome, node in cases.items():
            with self.subTest(outcome=outcome):
                self.nodes.node = node
                self.assertEqual(self.release()["outcome"], outcome)
        self.assertEqual(self.pods.deletes, [])

    def test_replacement_or_moved_pod_is_never_deleted(self):
        for pod in ({"metadata": {"uid": "new-pod-uid"}, "spec": {"nodeName": "spare-node"}},
                    {"metadata": {"uid": "old-pod-uid"}, "spec": {"nodeName": "spare-node"}}):
            with self.subTest(pod=pod):
                self.pods.pod = copy.deepcopy(pod)
                result = self.release()
                self.assertEqual(result["outcome"], "pod_identity_changed")
                self.assertEqual(self.pods.pod, pod)
        self.pods.pod = None
        self.assertEqual(self.release()["outcome"], "pod_absent")
        self.assertEqual(self.pods.deletes, [])

    def test_replacement_created_during_delete_is_kept_and_attempt_is_not_retried(self):
        replacement = {"metadata": {"uid": "new-pod-uid"}, "spec": {"nodeName": "spare-node"}}
        self.pods.before_delete = lambda: setattr(self.pods, "pod", copy.deepcopy(replacement))
        result = self.release()
        self.assertEqual(result["outcome"], "deleted")
        self.assertTrue(result["deleteFailed"])
        self.assertIsNone(result["deleteApplied"])
        self.assertEqual(result["observedPodUID"], "new-pod-uid")
        self.assertEqual(self.pods.pod, replacement)
        self.assertEqual(self.pods.deletes, ["old-pod-uid"])

    def test_rejected_delete_with_old_pod_remaining_is_unexplained_without_retry(self):
        def reject():
            raise observer.MutationRejected("HTTP 403")
        self.pods.before_delete = reject
        result = self.release()
        self.assertEqual(result["outcome"], "failed_unexplained")
        self.assertEqual(self.pods.deletes, ["old-pod-uid"])
        self.assertIsNotNone(self.pods.pod)

    def test_accepted_delete_that_leaves_the_old_pod_is_unconfirmed(self):
        self.pods.delete = lambda name, uid: self.pods.deletes.append(uid)
        result = self.release()
        self.assertEqual(result["outcome"], "delete_unconfirmed")
        self.assertEqual(self.pods.deletes, ["old-pod-uid"])


if __name__ == "__main__":
    unittest.main()
