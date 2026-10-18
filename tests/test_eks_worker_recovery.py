from __future__ import annotations

import copy
import datetime
import importlib.util
import json
import contextlib
import http.server
import io
from pathlib import Path
import sys
import tempfile
import threading
import time
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
import az_outage
import durable_stream
import fixture as fixture_module
import pod_tunnel
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
            with self.subTest(key=key), patch.object(pod_tunnel.subprocess, "Popen") as process:
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
            with self.subTest(change=change), patch.object(pod_tunnel.subprocess, "Popen") as process:
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


ZONE_LABEL = "topology.kubernetes.io/zone"
BASE_TIME = datetime.datetime(2026, 10, 16, 3, 0, 0, tzinfo=datetime.timezone.utc)


def iso(seconds: float) -> str:
    return (BASE_TIME + datetime.timedelta(seconds=seconds)).isoformat()


def k8s_ready(ok=True):
    return {"conditions": [{"type": "Ready", "status": "True" if ok else "False"}]}


def make_pod(name, uid, node):
    return {"metadata": {"name": name, "uid": uid, "labels": {"app": "narumq"}}, "spec": {"nodeName": node},
            "status": k8s_ready()}


def make_node(name, uid, instance, zone):
    return {"metadata": {"name": name, "uid": uid, "labels": {ZONE_LABEL: zone}},
            "spec": {"providerID": f"aws:///{zone}/{instance}"}, "status": k8s_ready()}


def not_found(path):
    error = worker_main.urllib.error.HTTPError(path, 404, "missing", {}, io.BytesIO())
    error.close()
    return error


class RolePreflightFixture:
    """Read-only fixture for the fault preflight; `calls` counts every read."""

    run_id = "run1"

    def __init__(self, leader):
        self.leader = leader
        self.calls = 0
        self.nodes = {"w0": make_node("w0", "nu0", "i-0", "az-a"), "w1": make_node("w1", "nu1", "i-1", "az-b"),
                      "w2": make_node("w2", "nu2", "i-2", "az-c"),
                      "w-spare": make_node("w-spare", "nus", "i-spare", "az-c")}
        self.pods = [make_pod("narumq-0", "u0", "w0"), make_pod("narumq-1", "u1", "w1"),
                     make_pod("narumq-2", "u2", "w2")]

    def aws(self, *args):
        self.calls += 1
        if args[:2] == ("sts", "get-caller-identity"):
            return {"Account": "809446814769"}
        if args[:2] == ("ec2", "describe-snapshots"):
            return {"Snapshots": [{"State": "completed", "VolumeId": "vol-2",
                                   "Tags": [{"Key": "RunId", "Value": "run1"}]}]}
        raise AssertionError(args)

    def get(self, *args):
        self.calls += 1
        if args[0] == "namespace":
            return {"metadata": {"labels": {"narumq.io/acceptance-run": "run1"}}}
        if args[0] == "pods":
            return {"items": copy.deepcopy(self.pods)}
        if args[0] == "pvc":
            return {"metadata": {"uid": "pvcu"}, "spec": {"volumeName": "pv-2"}}
        if args[0] == "pv":
            return {"metadata": {"name": "pv-2"}, "spec": {"csi": {"volumeHandle": "vol-2"}}}
        raise AssertionError(args)

    def get_node(self, name):
        self.calls += 1
        return copy.deepcopy(self.nodes.get(name))

    def instance(self, instance_id):
        self.calls += 1
        return {"State": {"Name": "running"}, "VpcId": "vpc-1",
                "BlockDeviceMappings": [{"Ebs": {"VolumeId": "vol-2", "DeleteOnTermination": False}}]}

    def api(self, path, **kwargs):
        self.calls += 1
        if path == "/api/v1/cluster":
            return {"clusterId": "run1", "quorumAvailable": True, "leaderNodeId": self.leader}
        raise not_found(path)


class RolePreflightTests(unittest.TestCase):
    PLAN = {"runId": "run1", "clusterName": "narumq-ha-acceptance-run1", "vpcId": "vpc-1", "targetPod": "narumq-2",
            "targetNodeId": "3", "oldPodUID": "u2", "oldWorkerName": "w2", "oldNodeUID": "nu2",
            "oldInstanceId": "i-2", "pvcName": "data-narumq-2", "pvcUID": "pvcu", "pvName": "pv-2",
            "volumeId": "vol-2", "zone": "az-c", "spareWorkerName": "w-spare", "spareInstanceId": "i-spare",
            "snapshotId": "snap-1", "observerPod": "narumq-0", "observerWorkerName": "w0"}

    def run_preflight(self, role, leader, fixture=None, **changes):
        fixture = fixture or RolePreflightFixture(leader)
        return worker_main.preflight(fixture, {**self.PLAN, "targetRole": role, **changes})

    def test_leader_target_is_accepted_for_the_leader_role_with_the_observer_in_another_az(self):
        self.assertEqual(len(self.run_preflight("leader", "3")), 3)

    def test_follower_role_keeps_the_follower_rule(self):
        self.assertEqual(len(self.run_preflight("follower", "1")), 3)
        with self.assertRaisesRegex(ValueError, "current follower"):
            self.run_preflight("follower", "3")

    def test_leader_role_rejects_a_target_that_is_not_the_current_leader(self):
        with self.assertRaisesRegex(ValueError, "current leader"):
            self.run_preflight("leader", "1")

    def test_leader_role_rejects_an_observer_in_the_target_az(self):
        fixture = RolePreflightFixture("3")
        fixture.nodes["w0"]["metadata"]["labels"][ZONE_LABEL] = "az-c"
        with self.assertRaisesRegex(ValueError, "different AZ"):
            self.run_preflight("leader", "3", fixture=fixture)

    def test_leader_role_rejects_an_unreadable_observer_worker(self):
        with self.assertRaisesRegex(ValueError, "different AZ"):
            self.run_preflight("leader", "3", observerWorkerName="w-missing")

    def test_follower_role_does_not_require_a_different_observer_az(self):
        fixture = RolePreflightFixture("1")
        fixture.nodes["w0"]["metadata"]["labels"][ZONE_LABEL] = "az-c"
        self.assertEqual(len(self.run_preflight("follower", "1", fixture=fixture)), 3)

    def test_target_role_is_required_and_checked_before_any_cluster_read(self):
        for role in (None, "", "primary", "Leader"):
            with self.subTest(role=role):
                fixture = RolePreflightFixture("3")
                plan = {**self.PLAN}
                if role is not None:
                    plan["targetRole"] = role
                with self.assertRaises(ValueError):
                    worker_main.preflight(fixture, plan)
                self.assertEqual(fixture.calls, 0)


def probe_sample(t, ok=True, data=None, elapsed=1.0):
    sample = {"startedAt": iso(t), "startedSeconds": float(t), "ok": ok, "elapsedSeconds": elapsed,
              "finishedAt": iso(t + elapsed)}
    if ok:
        sample["data"] = data
    else:
        sample["errorCode"] = "TimeoutError"
    return sample


def streams(times=range(0, 100, 2), management_failing=(), mqtt_failing=(), new_leader_at=26, quorum_false=()):
    """Monitor samples every 2 s; the leader is member 3 until `new_leader_at`, then member 2."""
    management = [probe_sample(t, ok=t not in management_failing,
                               data={"quorumAvailable": t not in quorum_false,
                                     "leaderNodeId": "2" if new_leader_at is not None and t >= new_leader_at else "3"})
                  for t in times]
    mqtt = [probe_sample(t, ok=t not in mqtt_failing, data={"puback": True}) for t in times]
    return {"management": management, "mqtt": mqtt}


class LeaderContinuityTests(unittest.TestCase):
    FAULT = iso(10)

    def judge(self, **kwargs):
        return observer.leader_continuity(streams(**kwargs), self.FAULT, "3")

    def test_one_short_window_across_both_streams_passes(self):
        result = self.judge(management_failing={20, 22, 24}, mqtt_failing={22, 24})
        self.assertEqual(result["result"], "PASS", result["reasons"])
        self.assertEqual(len(result["union"]), 1)
        self.assertEqual(result["union"][0]["spanSeconds"], 7.0)
        self.assertEqual(result["newLeader"]["nodeId"], "2")

    def test_window_runs_from_first_failed_sample_start_to_next_success_end(self):
        window = self.judge(management_failing={20})["streams"]["management"]["windows"][0]
        self.assertEqual((window["start"], window["end"]), (20.0, 23.0))

    def test_window_longer_than_thirty_seconds_fails(self):
        result = self.judge(management_failing=set(range(20, 60, 2)))
        self.assertEqual(result["result"], "FAIL")
        self.assertTrue(any("longer than 30" in reason for reason in result["reasons"]))

    def test_span_of_exactly_thirty_seconds_passes_and_one_more_second_fails(self):
        sparse = [0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24]
        exact = self.judge(management_failing={20, 22, 24}, times=sparse + [49])
        self.assertEqual(exact["streams"]["management"]["windows"][0]["spanSeconds"], 30.0)
        self.assertEqual(exact["result"], "PASS", exact["reasons"])
        over = self.judge(management_failing={20, 22, 24}, times=sparse + [50])
        self.assertEqual(over["streams"]["management"]["windows"][0]["spanSeconds"], 31.0)
        self.assertEqual(over["result"], "FAIL")

    def test_two_windows_in_one_stream_fail(self):
        result = self.judge(management_failing={20, 40})
        self.assertEqual(result["result"], "FAIL")
        self.assertTrue(any("more than one failure window" in reason for reason in result["reasons"]))

    def test_disjoint_windows_in_different_streams_fail(self):
        result = self.judge(management_failing={20}, mqtt_failing={50})
        self.assertEqual(result["result"], "FAIL")
        self.assertTrue(any("not inside one contiguous window" in reason for reason in result["reasons"]))

    def test_overlapping_windows_whose_union_is_too_long_fail_although_each_is_short(self):
        result = self.judge(management_failing=set(range(12, 36, 2)), mqtt_failing=set(range(30, 54, 2)),
                            new_leader_at=40)
        for name in ("management", "mqtt"):
            self.assertLessEqual(result["streams"][name]["windows"][0]["spanSeconds"], 30)
        self.assertEqual(len(result["union"]), 1)
        self.assertGreater(result["union"][0]["spanSeconds"], 30)
        self.assertEqual(result["result"], "FAIL")

    def test_failure_that_never_recovers_leaves_an_open_window_and_fails(self):
        result = self.judge(mqtt_failing=set(range(90, 100, 2)))
        self.assertEqual(result["result"], "FAIL")
        self.assertIsNone(result["union"][0]["spanSeconds"])

    def test_failure_before_the_fault_request_fails_even_without_a_window_after_it(self):
        result = self.judge(management_failing={4})
        self.assertEqual(result["streams"]["management"]["failedBeforeFault"], 1)
        self.assertEqual(result["result"], "FAIL")

    def test_no_failure_at_all_passes_when_a_survivor_is_leader(self):
        result = self.judge()
        self.assertEqual(result["result"], "PASS", result["reasons"])
        self.assertEqual(result["union"], [])

    def test_survivor_leader_is_required_even_without_failures(self):
        result = self.judge(new_leader_at=None)
        self.assertEqual(result["result"], "FAIL")
        self.assertIsNone(result["newLeader"])

    def test_survivor_leader_without_quorum_does_not_count(self):
        result = self.judge(quorum_false=set(range(26, 100, 2)))
        self.assertEqual(result["result"], "FAIL")
        self.assertIsNone(result["newLeader"])

    def test_streams_without_samples_after_the_fault_cannot_pass(self):
        result = observer.leader_continuity(streams(times=range(0, 10, 2)), self.FAULT, "3")
        self.assertEqual(result["result"], "FAIL")
        self.assertTrue(any("no samples after" in reason for reason in result["reasons"]))

    def test_monitor_integrity_needs_closed_journals_and_samples_in_every_stream(self):
        good = {"workersStoppedAndJournalsClosed": True, "journalErrors": [], "shutdownDeadlineExceeded": False,
                "streams": {"management": {"samples": 3}, "mqtt": {"samples": 3}}}
        self.assertTrue(observer.monitor_integrity(good))
        for change in ({"workersStoppedAndJournalsClosed": False}, {"journalErrors": ["x"]},
                       {"shutdownDeadlineExceeded": True},
                       {"streams": {"management": {"samples": 0}, "mqtt": {"samples": 3}}}):
            with self.subTest(change=change):
                self.assertFalse(observer.monitor_integrity({**good, **change}))


SCN = "scn"


def journal_entry(seq, outcome, ack_seconds=None):
    return {"seq": seq, "sentAt": iso(seq), "outcome": outcome,
            "reasonCode": {"acked": 0, "rejected": 0x97, "unknown": None}[outcome],
            "ackAt": None if outcome == "unknown" else iso(seq + 0.1 if ack_seconds is None else ack_seconds)}


def got(label, *seqs):
    return [{"label": label, "seq": seq, "at": iso(0), "dup": False} for seq in seqs]


class DurableJudgeTests(unittest.TestCase):
    def test_every_acked_message_received_passes_and_counts_are_reported(self):
        journal = [journal_entry(1, "acked"), journal_entry(2, "acked"), journal_entry(3, "rejected"),
                   journal_entry(4, "unknown")]
        result = durable_stream.judge(journal, got("a", 1, 2))
        self.assertEqual(result["result"], "PASS")
        self.assertEqual((result["acked"], result["rejected"], result["unknown"]), (2, 1, 1))
        self.assertEqual(result["missingAcked"], [])

    def test_a_missing_acked_message_fails_and_is_listed(self):
        journal = [journal_entry(s, "acked") for s in (1, 2, 3)]
        result = durable_stream.judge(journal, got("a", 1, 3))
        self.assertEqual(result["result"], "FAIL")
        self.assertEqual(result["missingAcked"], [2])

    def test_a_delivered_rejected_message_fails(self):
        journal = [journal_entry(1, "acked"), journal_entry(2, "rejected")]
        result = durable_stream.judge(journal, got("a", 1, 2))
        self.assertEqual(result["result"], "FAIL")
        self.assertEqual(result["rejectedDelivered"], [2])

    def test_unknown_messages_are_reported_but_never_judged(self):
        journal = [journal_entry(1, "acked"), journal_entry(2, "unknown"), journal_entry(3, "unknown")]
        delivered = durable_stream.judge(journal, got("a", 1, 2))
        self.assertEqual(delivered["result"], "PASS")
        self.assertEqual(delivered["unknownDelivered"], [2])
        lost = durable_stream.judge(journal, got("a", 1))
        self.assertEqual(lost["result"], "PASS")
        self.assertEqual(lost["unknownDelivered"], [])

    def test_duplicates_are_counted_but_not_judged(self):
        result = durable_stream.judge([journal_entry(1, "acked")], got("a", 1) + got("b", 1, 1))
        self.assertEqual(result["result"], "PASS")
        self.assertEqual(result["duplicates"], 2)

    def test_received_messages_are_the_union_over_all_labels(self):
        journal = [journal_entry(s, "acked") for s in (1, 2, 3)]
        self.assertEqual(durable_stream.judge(journal, got("mid-outage", 1, 2))["result"], "FAIL")
        self.assertEqual(durable_stream.judge(journal, got("mid-outage", 1, 2) + got("after-restore", 3))["result"],
                         "PASS")

    def test_a_stream_without_any_acked_message_cannot_pass(self):
        self.assertEqual(durable_stream.judge([journal_entry(1, "unknown")], [])["result"], "FAIL")
        self.assertEqual(durable_stream.judge([], [])["result"], "FAIL")

    def test_cutoff_judges_only_messages_acked_up_to_it(self):
        journal = [journal_entry(1, "acked"), journal_entry(2, "acked"), journal_entry(3, "acked", ack_seconds=50)]
        through = datetime.datetime.fromisoformat(iso(10))
        self.assertEqual(durable_stream.judge(journal, got("a", 1, 2), through)["result"], "PASS")
        self.assertEqual(durable_stream.judge(journal, got("a", 1, 2))["result"], "FAIL")

    def test_payload_parsing_accepts_only_the_scenario_sequence_format(self):
        self.assertEqual(durable_stream.stream_payload("leader-loss", 7), b"leader-loss:7")
        self.assertEqual(durable_stream.parse_seq("leader-loss", b"leader-loss:7"), 7)
        for payload in (b"other:7", b"leader-loss:x", b"leader-loss", b"\xff"):
            self.assertIsNone(durable_stream.parse_seq("leader-loss", payload))

    def test_outcomes_between_selects_messages_sent_inside_the_window(self):
        entries = [journal_entry(s, "acked") for s in (1, 5, 9)]
        selected = durable_stream.outcomes_between(entries, iso(4), iso(9))
        self.assertEqual([e["seq"] for e in selected], [5, 9])


class FakeBroker:
    """Stands in for durable_stream.MqttSession; the factory object doubles as the session."""

    def __init__(self, deliver=(), session_present=True, outcome=lambda seq: 0, synchronous=False):
        self.deliver = deliver
        self.session_present = session_present
        self.outcome = outcome
        self.synchronous = synchronous
        self.created = []
        self.sent = []
        self.subscribed = []
        self.closed = False
        self.mid = 0
        self.on_puback = None
        self.on_disconnect = None

    def __call__(self, client_id, password, port, clean_start, session_expiry, on_message=None, on_puback=None,
                 on_disconnect=None):
        self.created.append({"clientId": client_id, "password": password, "port": port, "cleanStart": clean_start,
                             "sessionExpiry": session_expiry})
        self.on_puback, self.on_disconnect = on_puback, on_disconnect
        if on_message is not None:
            for seq in self.deliver:
                on_message(f"{SCN}:{seq}".encode(), False)
        return self

    def subscribe(self, topic):
        self.subscribed.append(topic)

    def publish(self, topic, payload):
        self.mid += 1
        mid = self.mid
        self.sent.append((topic, payload))
        code = self.outcome(int(payload.decode().rpartition(":")[2]))
        if code is not None:
            if self.synchronous:
                self.on_puback(mid, code)
            else:
                threading.Timer(0.01, self.on_puback, (mid, code)).start()
        return mid

    def close(self):
        self.closed = True


class DurableSubscriberTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="narumq-durable-test-")
        self.addCleanup(self.temporary.cleanup)
        self.private = Path(self.temporary.name) / "private"
        self.output = Path(self.temporary.name) / "output"
        self.private.mkdir()
        self.output.mkdir()
        (self.private / "mqtt.password").write_text("secret-pw\n", encoding="utf-8")

    def write_journal(self, entries):
        (self.output / f"durable-publish-{SCN}.jsonl").write_text(
            "".join(json.dumps(e) + "\n" for e in entries), encoding="utf-8")

    def verify(self, broker, label, **kwargs):
        return durable_stream.verify_subscriber(self.private, "run1", SCN, 23883, self.output, label,
                                                idle_seconds=0.1, session_factory=broker, **kwargs)

    def test_prepare_subscribes_with_clean_start_and_a_one_hour_session_then_disconnects(self):
        broker = FakeBroker()
        record = durable_stream.prepare_subscriber(self.private, "run1", SCN, 24883, self.output,
                                                   session_factory=broker)
        self.assertEqual(broker.created, [{"clientId": "durable-stream-subscriber", "password": "secret-pw",
                                           "port": 24883, "cleanStart": True, "sessionExpiry": 3600}])
        self.assertEqual(broker.subscribed, ["acceptance/run1/durable/scn"])
        self.assertTrue(broker.closed)
        self.assertEqual(record["result"], "PASS")
        saved = json.loads((self.output / "durable-prepare-scn.json").read_text(encoding="utf-8"))
        self.assertEqual(saved["subscribedQos"], 1)

    def test_failed_prepare_is_recorded_and_raised_without_leaking_the_error_text(self):
        def refuse(*args, **kwargs):
            raise RuntimeError("CONNACK secret-pw refused")
        with self.assertRaises(RuntimeError):
            durable_stream.prepare_subscriber(self.private, "run1", SCN, 24883, self.output, session_factory=refuse)
        text = (self.output / "durable-prepare-scn.json").read_text(encoding="utf-8")
        self.assertEqual(json.loads(text)["result"], "FAIL")
        self.assertNotIn("secret-pw", text)

    def test_verify_result_shows_the_published_total_stays_below_the_per_session_limit(self):
        self.write_journal([journal_entry(s, "acked") for s in range(1, 61)])
        result = self.verify(FakeBroker(deliver=range(1, 61)), "after-recovery")
        self.assertEqual(result["result"], "PASS")
        self.assertEqual((result["publishedTotal"], result["perSessionLimit"]), (60, 64))
        saved = json.loads((self.output / "durable-verify-scn-after-recovery.json").read_text(encoding="utf-8"))
        self.assertEqual((saved["publishedTotal"], saved["perSessionLimit"]), (60, 64))

    def test_verify_reconnects_with_clean_start_zero_and_judges_the_journal(self):
        self.write_journal([journal_entry(1, "acked"), journal_entry(2, "acked")])
        broker = FakeBroker(deliver=(1, 2))
        result = self.verify(broker, "after-recovery")
        self.assertEqual(broker.created, [{"clientId": "durable-stream-subscriber", "password": "secret-pw",
                                           "port": 23883, "cleanStart": False, "sessionExpiry": 3600}])
        self.assertEqual(result["result"], "PASS")
        self.assertTrue(result["sessionPresent"])
        self.assertTrue(broker.closed)
        received = (self.output / "durable-received-scn.jsonl").read_text(encoding="utf-8").splitlines()
        self.assertEqual([(json.loads(l)["label"], json.loads(l)["seq"]) for l in received],
                         [("after-recovery", 1), ("after-recovery", 2)])
        self.assertTrue((self.output / "durable-verify-scn-after-recovery.json").exists())

    def test_missing_acked_message_fails_verification(self):
        self.write_journal([journal_entry(1, "acked"), journal_entry(2, "acked")])
        result = self.verify(FakeBroker(deliver=(1,)), "after-recovery")
        self.assertEqual(result["result"], "FAIL")
        self.assertEqual(result["missingAcked"], [2])

    def test_delivered_rejected_message_fails_verification(self):
        self.write_journal([journal_entry(1, "acked"), journal_entry(2, "rejected")])
        result = self.verify(FakeBroker(deliver=(1, 2)), "after-recovery")
        self.assertEqual(result["result"], "FAIL")
        self.assertEqual(result["rejectedDelivered"], [2])

    def test_unknown_message_delivery_is_allowed_and_reported(self):
        self.write_journal([journal_entry(1, "acked"), journal_entry(2, "unknown")])
        result = self.verify(FakeBroker(deliver=(1, 2)), "after-recovery")
        self.assertEqual(result["result"], "PASS")
        self.assertEqual(result["unknownDelivered"], [2])

    def test_second_label_completes_what_the_first_label_missed(self):
        self.write_journal([journal_entry(s, "acked") for s in (1, 2, 3)])
        first = self.verify(FakeBroker(deliver=(1, 2)), "mid-outage")
        second = self.verify(FakeBroker(deliver=(3,)), "after-restore")
        self.assertEqual(first["result"], "FAIL")
        self.assertEqual(second["result"], "PASS")
        self.assertEqual(second["receivedThisLabel"], 1)
        self.assertEqual(second["receivedDistinct"], 3)

    def test_a_missing_session_fails_even_when_everything_was_delivered(self):
        self.write_journal([journal_entry(1, "acked")])
        result = self.verify(FakeBroker(deliver=(1,), session_present=False), "after-recovery")
        self.assertEqual(result["result"], "FAIL")
        self.assertFalse(result["sessionPresent"])
        self.assertEqual(result["errorCode"], "SessionNotPresent")

    def test_a_refused_connection_fails_and_is_recorded_without_the_error_text(self):
        self.write_journal([journal_entry(1, "acked")])
        def refuse(*args, **kwargs):
            raise RuntimeError("CONNACK secret-pw refused")
        result = durable_stream.verify_subscriber(self.private, "run1", SCN, 23883, self.output, "after-recovery",
                                                  idle_seconds=0.1, session_factory=refuse)
        self.assertEqual(result["result"], "FAIL")
        self.assertEqual(result["errorCode"], "RuntimeError")

    def test_cutoff_verification_ignores_messages_acked_after_the_cutoff(self):
        self.write_journal([journal_entry(1, "acked"), journal_entry(2, "acked"),
                            journal_entry(3, "acked", ack_seconds=50)])
        result = self.verify(FakeBroker(deliver=(1, 2)), "mid-outage", cutoff=iso(10))
        self.assertEqual(result["result"], "PASS")
        self.assertEqual(result["judgedThrough"], datetime.datetime.fromisoformat(iso(10)).isoformat())
        self.assertEqual(self.verify(FakeBroker(deliver=(1, 2)), "no-cutoff")["result"], "FAIL")

    def test_outputs_never_contain_the_password(self):
        self.write_journal([journal_entry(1, "acked")])
        self.verify(FakeBroker(deliver=(1,)), "after-recovery")
        durable_stream.prepare_subscriber(self.private, "run1", SCN, 24883, self.output, session_factory=FakeBroker())
        for path in self.output.iterdir():
            self.assertNotIn("secret-pw", path.read_text(encoding="utf-8"), path.name)


class DurablePublisherTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="narumq-publisher-test-")
        self.addCleanup(self.temporary.cleanup)
        self.private = Path(self.temporary.name) / "private"
        self.output = Path(self.temporary.name) / "output"
        self.private.mkdir()
        self.output.mkdir()
        (self.private / "mqtt.password").write_text("secret-pw\n", encoding="utf-8")

    def publisher(self, broker, **kwargs):
        return durable_stream.DurablePublisher(self.private, "run1", SCN, 23883, self.output, interval=0.01,
                                               session_factory=broker, **kwargs)

    def journal(self):
        path = self.output / "durable-publish-scn.jsonl"
        return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]

    def test_acked_and_rejected_outcomes_are_journaled_per_message(self):
        broker = FakeBroker(outcome=lambda seq: 0x97 if seq % 3 == 0 else 0)
        publisher = self.publisher(broker)
        publisher.start()
        self.assertTrue(publisher.wait_for_acked(5, 5))
        stats = publisher.stop()
        entries = self.journal()
        self.assertEqual(broker.created[0]["clientId"], "durable-stream-publisher")
        self.assertTrue(broker.created[0]["cleanStart"])
        self.assertEqual(broker.sent[0], ("acceptance/run1/durable/scn", b"scn:1"))
        self.assertEqual(len(entries), stats["published"])
        self.assertEqual(sorted(e["seq"] for e in entries), list(range(1, len(entries) + 1)))
        for entry in entries:
            self.assertEqual(set(entry), {"seq", "sentAt", "outcome", "reasonCode", "ackAt"})
            if entry["seq"] % 3 == 0:
                self.assertEqual((entry["outcome"], entry["reasonCode"]), ("rejected", 0x97))
            else:
                self.assertEqual((entry["outcome"], entry["reasonCode"]), ("acked", 0))
            self.assertIsNotNone(entry["ackAt"])
        self.assertEqual((stats["unknown"], stats["connectionLost"]), (0, False))
        self.assertTrue(broker.closed)

    def test_a_puback_that_arrives_before_publish_returns_is_not_lost(self):
        publisher = self.publisher(FakeBroker(synchronous=True))
        publisher.start()
        self.assertTrue(publisher.wait_for_acked(5, 5))
        stats = publisher.stop()
        self.assertEqual(stats["unknown"], 0)
        self.assertEqual(stats["acked"], stats["published"])

    def test_message_without_a_puback_in_time_is_unknown(self):
        with patch.object(durable_stream, "ACK_TIMEOUT_SECONDS", 0.2):
            publisher = self.publisher(FakeBroker(outcome=lambda seq: None))
            publisher.start()
            threading.Event().wait(0.5)
            stats = publisher.stop()
        entries = self.journal()
        self.assertGreater(len(entries), 0)
        self.assertTrue(all(e["outcome"] == "unknown" and e["ackAt"] is None and e["reasonCode"] is None
                            for e in entries))
        self.assertEqual((stats["acked"], stats["unknown"], stats["connectionLost"]), (0, len(entries), False))

    def test_connection_loss_stops_publishing_and_records_in_flight_messages_as_unknown(self):
        broker = FakeBroker(outcome=lambda seq: 0 if seq <= 3 else None)
        publisher = self.publisher(broker)
        publisher.start()
        deadline = time.monotonic() + 5
        while len(broker.sent) < 6 and time.monotonic() < deadline:
            time.sleep(0.01)
        broker.on_disconnect()
        publisher._thread.join(5)
        sent_at_loss = len(broker.sent)
        time.sleep(0.1)
        stats = publisher.stop()
        self.assertEqual(len(broker.sent), sent_at_loss)
        self.assertTrue(stats["connectionLost"])
        entries = self.journal()
        unknown = [e for e in entries if e["outcome"] == "unknown"]
        self.assertEqual({e["seq"] for e in unknown}, set(range(4, sent_at_loss + 1)))
        self.assertTrue(all(e["ackAt"] is None for e in unknown))
        self.assertEqual(stats["acked"], 3)

    def test_a_publish_the_client_refuses_is_recorded_as_unknown_and_stops_the_stream(self):
        broker = FakeBroker()
        calls = []
        def publish(topic, payload):
            calls.append(payload)
            raise RuntimeError("publish not accepted by client")
        broker.publish = publish
        publisher = self.publisher(broker)
        publisher.start()
        publisher._thread.join(5)
        stats = publisher.stop()
        self.assertEqual(len(calls), 1)
        self.assertEqual([e["outcome"] for e in self.journal()], ["unknown"])
        self.assertTrue(stats["connectionLost"])

    def test_wait_for_acked_gives_up_when_the_connection_is_lost(self):
        broker = FakeBroker(outcome=lambda seq: None)
        publisher = self.publisher(broker)
        publisher.start()
        broker.on_disconnect()
        self.assertFalse(publisher.wait_for_acked(1, 5))
        publisher.stop()

    def test_publisher_stops_sending_at_the_cap_and_reports_it(self):
        broker = FakeBroker()
        publisher = self.publisher(broker, max_messages=5)
        publisher.start()
        publisher._thread.join(5)
        self.assertFalse(publisher._thread.is_alive())
        stats = publisher.stop()
        entries = self.journal()
        self.assertEqual([e["seq"] for e in entries], [1, 2, 3, 4, 5])
        self.assertEqual(len(broker.sent), 5)
        self.assertTrue(all(e["outcome"] == "acked" for e in entries))
        self.assertTrue(stats["capReached"])
        self.assertIsNotNone(stats["cappedAt"])
        self.assertEqual((stats["maxMessages"], stats["interval"], stats["perSessionLimit"]), (5, 0.01, 64))
        self.assertEqual(stats["published"], 5)

    def test_a_stopped_publisher_below_the_cap_does_not_report_the_cap(self):
        publisher = self.publisher(FakeBroker(), max_messages=60)
        publisher.start()
        self.assertTrue(publisher.wait_for_acked(2, 5))
        stats = publisher.stop()
        self.assertLessEqual(stats["published"], 60)
        if stats["published"] < 60:
            self.assertFalse(stats["capReached"])
            self.assertIsNone(stats["cappedAt"])

    def test_messages_in_flight_at_the_cap_are_expired_to_unknown_without_more_sends(self):
        broker = FakeBroker(outcome=lambda seq: None)
        with patch.object(durable_stream, "ACK_TIMEOUT_SECONDS", 0.3):
            publisher = self.publisher(broker, max_messages=3)
            publisher.start()
            publisher._thread.join(5)
            self.assertFalse(publisher._thread.is_alive())
            stats = publisher.stop()
        self.assertEqual(len(broker.sent), 3)
        self.assertEqual([e["outcome"] for e in self.journal()], ["unknown"] * 3)
        self.assertTrue(stats["capReached"])

    def test_a_connection_lost_before_the_cap_does_not_report_the_cap(self):
        broker = FakeBroker(outcome=lambda seq: None)
        publisher = self.publisher(broker, max_messages=60)
        publisher.start()
        broker.on_disconnect()
        publisher._thread.join(5)
        stats = publisher.stop()
        self.assertTrue(stats["connectionLost"])
        self.assertFalse(stats["capReached"])

    def test_max_messages_is_limited_to_sixty_below_the_per_session_queue_limit(self):
        for value in (0, -1, 61, 64, 1515, 2.5, "60", None, True):
            with self.subTest(value=value), self.assertRaisesRegex(ValueError, "max_messages"):
                self.publisher(FakeBroker(), max_messages=value)
        for value in (1, 60):
            self.publisher(FakeBroker(), max_messages=value)
        self.assertLess(durable_stream.MAX_MESSAGES, durable_stream.PER_SESSION_LIMIT)

    def test_outcomes_between_reports_the_journaled_messages_of_a_window(self):
        publisher = self.publisher(FakeBroker())
        publisher.start()
        self.assertTrue(publisher.wait_for_acked(3, 5))
        stats = publisher.stop()
        everything = publisher.outcomes_between("1970-01-01T00:00:00+00:00", "2999-01-01T00:00:00+00:00")
        self.assertEqual(len(everything), stats["published"])
        self.assertEqual(publisher.outcomes_between("2999-01-01T00:00:00+00:00", "2999-01-02T00:00:00+00:00"), [])


class FixtureApiRequestTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="narumq-api-test-")
        self.addCleanup(self.temporary.cleanup)
        private = Path(self.temporary.name)
        (private / "admin.token").write_text("admin-secret\n", encoding="utf-8")
        self.fixture = fixture_module.Fixture(private, "run1")
        self.requests = []
        owner = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def respond(self):
                length = int(self.headers.get("Content-Length", 0))
                owner.requests.append({"method": self.command, "path": self.path, "body": self.rfile.read(length),
                                       "headers": dict(self.headers)})
                status, body = owner.reply
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(body)
            do_GET = do_POST = do_PUT = respond
            def log_message(self, *args):
                pass

        self.server = http.server.HTTPServer(("127.0.0.1", 0), Handler)
        self.addCleanup(self.server.server_close)
        thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(thread.join, 5)
        self.addCleanup(self.server.shutdown)
        self.port = self.server.server_address[1]

    def test_post_sends_bearer_json_and_idempotency_key_and_returns_the_envelope(self):
        self.reply = (200, b'{"data": {"ok": true}}')
        status, envelope = self.fixture.api_request("POST", "/api/v1/x", {"a": 1}, port=self.port,
                                                    idempotency_key="key-1")
        self.assertEqual((status, envelope), (200, {"data": {"ok": True}}))
        request = self.requests[0]
        self.assertEqual((request["method"], request["path"]), ("POST", "/api/v1/x"))
        self.assertEqual(json.loads(request["body"]), {"a": 1})
        self.assertEqual(request["headers"]["Authorization"], "Bearer admin-secret")
        self.assertEqual(request["headers"]["Content-Type"], "application/json")
        self.assertEqual(request["headers"]["Idempotency-Key"], "key-1")

    def test_http_error_statuses_are_returned_not_raised(self):
        for status in (404, 409, 503):
            with self.subTest(status=status):
                self.reply = (status, b'{"error": {"code": "conflict"}}')
                self.assertEqual(self.fixture.api_request("GET", "/api/v1/x", port=self.port),
                                 (status, {"error": {"code": "conflict"}}))

    def test_get_without_body_sends_no_content_type_or_idempotency_key(self):
        self.reply = (200, b'{"data": []}')
        self.fixture.api_request("GET", "/api/v1/x", port=self.port)
        headers = self.requests[0]["headers"]
        self.assertNotIn("Content-Type", headers)
        self.assertNotIn("Idempotency-Key", headers)

    def test_a_non_json_error_body_gives_no_envelope(self):
        self.reply = (502, b"bad gateway")
        self.assertEqual(self.fixture.api_request("GET", "/api/v1/x", port=self.port), (502, None))

    def test_transport_errors_raise(self):
        self.server.shutdown()
        self.server.server_close()
        with self.assertRaises(OSError):
            self.fixture.api_request("GET", "/api/v1/x", port=self.port)


class FakeProcess:
    def __init__(self, lines):
        self.stdout = io.StringIO("".join(lines))
        self.returncode = None
        self.terminated = False

    def poll(self):
        return self.returncode

    def terminate(self):
        self.terminated = True
        self.returncode = -15

    def kill(self):
        self.returncode = -9

    def wait(self, timeout=None):
        return self.returncode


class PodTunnelTests(unittest.TestCase):
    def setUp(self):
        self.pod = make_pod("narumq-2", "u2", "w2")
        owner = self

        class Fixture:
            kubectl = ["kubectl", "--context", "run1"]
            environment = {}
            def get(self, *args):
                return copy.deepcopy(owner.pod)
        self.fixture = Fixture()

    def tunnel(self, uid="u2"):
        return pod_tunnel.PodTunnel(self.fixture, "narumq-2", uid, 24880, 24883)

    def test_start_forwards_both_ports_of_the_exact_pod_and_reports_them(self):
        lines = ["Forwarding from 127.0.0.1:24880 -> 8080\n", "Forwarding from 127.0.0.1:24883 -> 1883\n"]
        with patch.object(pod_tunnel.subprocess, "Popen", return_value=FakeProcess(lines)) as popen:
            tunnel = self.tunnel()
            info = tunnel.start(timeout=5)
            tunnel.check()
            tunnel.close()
        command = popen.call_args.args[0]
        self.assertEqual(command, ["kubectl", "--context", "run1", "-n", "narumq", "port-forward", "pod/narumq-2",
                                   "--address=127.0.0.1", "24880:8080", "24883:1883"])
        self.assertEqual(info["ports"], {"management": 24880, "mqtt": 24883})
        self.assertEqual((info["pod"], info["podUID"]), ("narumq-2", "u2"))

    def test_identity_mismatch_notready_or_terminating_never_starts_a_process(self):
        changes = (lambda: self.pod["metadata"].update(uid="other"),
                   lambda: self.pod["status"].update(conditions=[]),
                   lambda: self.pod["metadata"].update(deletionTimestamp="now"),
                   lambda: self.pod["metadata"]["labels"].update(app="other"))
        for change in changes:
            self.setUp()
            with self.subTest(change=change), patch.object(pod_tunnel.subprocess, "Popen") as popen:
                change()
                with self.assertRaises(ValueError):
                    self.tunnel().start(timeout=1)
                popen.assert_not_called()

    def test_a_process_that_ends_is_detected_and_not_restarted(self):
        lines = ["Forwarding from 127.0.0.1:24880 -> 8080\n", "Forwarding from 127.0.0.1:24883 -> 1883\n"]
        process = FakeProcess(lines)
        with patch.object(pod_tunnel.subprocess, "Popen", return_value=process) as popen:
            tunnel = self.tunnel()
            tunnel.start(timeout=5)
            process.returncode = 1
            with self.assertRaises(RuntimeError):
                tunnel.check()
            tunnel.close()
        self.assertEqual(popen.call_count, 1)

    def test_ports_that_never_forward_time_out_and_the_process_is_stopped(self):
        process = FakeProcess(["Forwarding from 127.0.0.1:24880 -> 8080\n"])
        with patch.object(pod_tunnel.subprocess, "Popen", return_value=process):
            with self.assertRaises(TimeoutError):
                self.tunnel().start(timeout=0.3)
        self.assertTrue(process.terminated)

    def test_check_before_start_fails(self):
        with self.assertRaises(RuntimeError):
            self.tunnel().check()


AZ_PLAN = {"runId": "run1", "clusterName": "narumq-ha-acceptance-run1", "vpcId": "vpc-1", "zone": "az-c",
           "instanceIds": ["i-2", "i-x"], "asgNames": ["asg-c"], "holdSeconds": 60, "targetPod": "narumq-2",
           "targetNodeId": "3", "oldPodUID": "u2", "oldWorkerName": "w2", "oldInstanceId": "i-2",
           "pvcName": "data-narumq-2", "pvcUID": "pvcu", "pvName": "pv-2", "volumeId": "vol-2",
           "observerPod": "narumq-0", "observerPodUID": "u0", "observerWorkerName": "w0",
           "observerInstanceId": "i-0", "durableScenario": "az-outage", "postResumeSeconds": 60}
MUTATING = {"stop-instances", "start-instances", "suspend-processes", "resume-processes", "set-instance-health"}


def values_after(args, flag):
    rest = list(args[list(args).index(flag) + 1:])
    return rest[:next((i for i, a in enumerate(rest) if a.startswith("--")), len(rest))]


class AzFixture:
    """Scripted fixture for the AZ outage: three Pods in three AZs, two workers in the target AZ."""

    run_id = "run1"

    def __init__(self):
        self.events = []
        self.leader = "1"
        self.nodes = {"w0": make_node("w0", "nu0", "i-0", "az-a"), "w1": make_node("w1", "nu1", "i-1", "az-b"),
                      "w2": make_node("w2", "nu2", "i-2", "az-c"), "w-x": make_node("w-x", "nux", "i-x", "az-c")}
        self.pods = [make_pod("narumq-0", "u0", "w0"), make_pod("narumq-1", "u1", "w1"),
                     make_pod("narumq-2", "u2", "w2")]
        self.running_in_zone = ["i-2", "i-x"]
        self.instance_group = "asg-c"
        self.asg_tags = [{"Key": "eks:cluster-name", "Value": "narumq-ha-acceptance-run1"}]
        self.suspended = {"asg-c": set()}
        self.state = "running"
        self.account = "809446814769"
        self.stop_error = False
        self.suspend_error_group = None
        self.resume_error = False
        self.resume_noop = False
        self.describe_error = False
        self.volume = {"AvailabilityZone": "az-c", "Attachments": [{"InstanceId": "i-2", "State": "attached"}]}
        self.members = {"asg-c": [{"InstanceId": i, "HealthStatus": "Healthy", "LifecycleState": "InService"}
                                  for i in ("i-2", "i-x")]}
        self.activities = []
        self.after_resume = None

    def aws(self, *args):
        self.events.append(("aws",) + args)
        if args[:2] == ("sts", "get-caller-identity"):
            return {"Account": self.account}
        if args[:2] == ("ec2", "describe-instances"):
            if "--filters" in args:
                return {"Reservations": [{"Instances": [
                    {"InstanceId": i, "VpcId": "vpc-1", "State": {"Name": "running"},
                     "Tags": [{"Key": "aws:autoscaling:groupName", "Value": self.instance_group}]}
                    for i in self.running_in_zone]}]}
            return {"Reservations": [{"Instances": [{"InstanceId": i, "State": {"Name": self.state}}
                                                    for i in values_after(args, "--instance-ids")]}]}
        if args[:2] == ("ec2", "stop-instances"):
            if self.stop_error:
                raise fixture_module.CommandFailed("command exit 1")
            self.state = "stopped"
            return {"StoppingInstances": [{"InstanceId": i} for i in values_after(args, "--instance-ids")]}
        if args[:2] == ("ec2", "start-instances"):
            self.state = "running"
            return {"StartingInstances": [{"InstanceId": i} for i in values_after(args, "--instance-ids")]}
        if args[:2] == ("ec2", "describe-volumes"):
            return {"Volumes": [copy.deepcopy(self.volume)]}
        if args[:2] == ("autoscaling", "describe-scaling-activities"):
            return {"Activities": copy.deepcopy(self.activities)}
        if args[:2] == ("autoscaling", "describe-auto-scaling-groups"):
            if self.describe_error:
                raise fixture_module.CommandFailed("command exit 1")
            return {"AutoScalingGroups": [
                {"AutoScalingGroupName": g, "Tags": self.asg_tags, "Instances": copy.deepcopy(self.members.get(g, [])),
                 "SuspendedProcesses": [{"ProcessName": p} for p in sorted(self.suspended[g])]}
                for g in values_after(args, "--auto-scaling-group-names")]}
        raise AssertionError(args)

    def command(self, args, timeout=90, stdin=None):
        self.events.append(("command",) + tuple(args))
        if "set-instance-health" in args:
            instance = args[args.index("--instance-id") + 1]
            for members in self.members.values():
                for member in members:
                    if member["InstanceId"] == instance:
                        member["HealthStatus"] = "Healthy"
        if "suspend-processes" in args or "resume-processes" in args:
            group = args[args.index("--auto-scaling-group-name") + 1]
            processes = set(values_after(args, "--scaling-processes"))
            if "suspend-processes" in args:
                if group == self.suspend_error_group:
                    raise fixture_module.CommandFailed("command exit 1")
                self.suspended[group] |= processes
            else:
                if self.resume_error:
                    raise fixture_module.CommandFailed("command exit 1")
                if not self.resume_noop:
                    self.suspended[group] -= processes
                if self.after_resume is not None:
                    self.after_resume()
        return ""

    def get(self, *args):
        if args[0] == "namespace":
            return {"metadata": {"labels": {"narumq.io/acceptance-run": "run1"}}}
        if args[0] == "pods":
            return {"items": copy.deepcopy(self.pods)}
        if args[0] == "nodes":
            return {"items": copy.deepcopy(list(self.nodes.values()))}
        if args[0] == "pvc":
            return {"metadata": {"uid": "pvcu"}, "spec": {"volumeName": "pv-2"}}
        if args[0] == "pv":
            return {"metadata": {"name": "pv-2"}, "spec": {"csi": {"volumeHandle": "vol-2"}}}
        if args[0] == "volumeattachments":
            return {"items": [{"spec": {"source": {"persistentVolumeName": "pv-2"}, "nodeName": "w2"},
                               "status": {"attached": True}}]}
        raise AssertionError(args)

    def get_node(self, name):
        return copy.deepcopy(self.nodes.get(name))

    def api(self, path, **kwargs):
        if path == "/api/v1/cluster":
            return {"clusterId": "run1", "quorumAvailable": True, "leaderNodeId": self.leader,
                    "nodesByState": {"ready": 3}}
        raise not_found(path)

    def mutations(self):
        return [e for e in self.events if len(e) > 2 and (e[2] in MUTATING or e[1] in MUTATING)]


class AzOutagePlanTests(unittest.TestCase):
    def test_durable_settings_default_and_bounds(self):
        plan = {k: v for k, v in AZ_PLAN.items()}
        self.assertEqual(durable_stream.plan_settings(plan), (3.0, 60))
        self.assertEqual(durable_stream.plan_settings({**plan, "durableIntervalSeconds": 0.2,
                                                       "durableMaxMessages": 1}), (0.2, 1))
        self.assertEqual(durable_stream.plan_settings({**plan, "durableIntervalSeconds": 30,
                                                       "durableMaxMessages": 60}), (30.0, 60))
        for change in ({"durableIntervalSeconds": 0.19}, {"durableIntervalSeconds": 30.1},
                       {"durableIntervalSeconds": "3"}, {"durableIntervalSeconds": True},
                       {"durableMaxMessages": 0}, {"durableMaxMessages": 61}, {"durableMaxMessages": 64},
                       {"durableMaxMessages": 2.5}, {"durableMaxMessages": "60"}, {"durableMaxMessages": True}):
            with self.subTest(change=change):
                with self.assertRaisesRegex(ValueError, "durable"):
                    durable_stream.plan_settings({**plan, **change})
                with self.assertRaisesRegex(ValueError, "durable"):
                    az_outage.validate_plan({**plan, **change})

    def test_durable_run_passes_the_plan_settings_to_the_publisher_and_rejects_bad_ones_before_any_tunnel(self):
        plan = {**AZ_PLAN, "durableIntervalSeconds": 1.5, "durableMaxMessages": 7}
        run = worker_main.DurableRun(object(), Path("."), "run1", plan, Path("."))
        with patch.object(worker_main, "PodTunnel"), patch.object(worker_main, "prepare_subscriber"), \
             patch.object(worker_main, "DurablePublisher") as publisher_class:
            run.begin()
        self.assertEqual((publisher_class.call_args.kwargs["interval"], publisher_class.call_args.kwargs["max_messages"]),
                         (1.5, 7))
        with patch.object(worker_main, "PodTunnel") as tunnel_class:
            with self.assertRaises(ValueError):
                worker_main.DurableRun(object(), Path("."), "run1", {**plan, "durableMaxMessages": 61}, Path("."))
            tunnel_class.assert_not_called()

    def test_hold_seconds_default_and_bounds(self):
        plan = {k: v for k, v in AZ_PLAN.items() if k != "holdSeconds"}
        self.assertEqual(az_outage.validate_plan(plan), 480)
        for hold in (60, 1800, 481):
            self.assertEqual(az_outage.validate_plan({**AZ_PLAN, "holdSeconds": hold}), hold)
        for hold in (59, 1801, 0, -480, "480", 480.5, True, None):
            with self.subTest(hold=hold), self.assertRaisesRegex(ValueError, "holdSeconds"):
                az_outage.validate_plan({**AZ_PLAN, "holdSeconds": hold})

    def test_incomplete_or_inconsistent_plans_are_rejected(self):
        broken = [{"durableScenario": ""}, {"zone": None}, {"instanceIds": []}, {"instanceIds": ["i-2", "i-2"]},
                  {"asgNames": "asg-c"}, {"asgNames": []}, {"instanceIds": ["i-x"]},
                  {"observerInstanceId": "i-x"}, {"observerPod": "narumq-2"}]
        for change in broken:
            with self.subTest(change=change), self.assertRaises(ValueError):
                az_outage.validate_plan({**AZ_PLAN, **change})


class AzOutagePreflightTests(unittest.TestCase):
    def reject(self, fixture, pattern, **changes):
        with self.assertRaisesRegex(ValueError, pattern):
            az_outage.preflight(fixture, {**AZ_PLAN, **changes})
        self.assertEqual(fixture.mutations(), [])

    def test_planned_az_is_accepted_without_any_mutation(self):
        fixture = AzFixture()
        result = az_outage.preflight(fixture, AZ_PLAN)
        self.assertEqual(len(result["pods"]), 3)
        self.assertEqual(result["podZones"], {"narumq-0": "az-a", "narumq-1": "az-b", "narumq-2": "az-c"})
        self.assertEqual(fixture.mutations(), [])

    def test_a_leader_in_the_target_az_is_rejected(self):
        fixture = AzFixture()
        fixture.leader = "3"
        self.reject(fixture, "must be a follower")

    def test_a_leader_pod_in_the_target_az_is_rejected(self):
        fixture = AzFixture()
        fixture.pods[1]["spec"]["nodeName"] = "w-x"
        self.reject(fixture, "only the target follower")

    def test_an_observer_pod_in_the_target_az_is_rejected(self):
        fixture = AzFixture()
        fixture.nodes["w0"]["metadata"]["labels"][ZONE_LABEL] = "az-c"
        self.reject(fixture, "only the target follower")

    def test_a_pod_on_a_node_without_a_zone_is_rejected(self):
        fixture = AzFixture()
        del fixture.nodes["w1"]["metadata"]["labels"][ZONE_LABEL]
        self.reject(fixture, "only the target follower")

    def test_a_running_instance_in_the_az_that_the_plan_omits_is_rejected(self):
        fixture = AzFixture()
        fixture.running_in_zone.append("i-extra")
        self.reject(fixture, "running instances in the zone differ")

    def test_a_planned_instance_that_is_not_running_is_rejected(self):
        fixture = AzFixture()
        fixture.running_in_zone.remove("i-x")
        self.reject(fixture, "running instances in the zone differ")

    def test_another_ready_kubernetes_node_in_the_az_is_rejected_but_a_notready_one_is_not(self):
        fixture = AzFixture()
        fixture.nodes["w-y"] = make_node("w-y", "nuy", "i-y", "az-c")
        self.reject(fixture, "another Ready Kubernetes Node")
        fixture.nodes["w-y"]["status"] = k8s_ready(False)
        az_outage.preflight(fixture, AZ_PLAN)

    def test_the_target_must_run_in_the_planned_az_on_its_planned_worker(self):
        self.reject(AzFixture(), "not in the outage zone", zone="az-b")
        self.reject(AzFixture(), "target identity changed", oldPodUID="other")
        self.reject(AzFixture(), "target identity changed", oldInstanceId="i-x")

    def test_the_target_ebs_volume_must_be_attached_in_the_az_to_the_target_worker(self):
        fixture = AzFixture()
        fixture.volume["AvailabilityZone"] = "az-a"
        self.reject(fixture, "EBS volume")
        fixture = AzFixture()
        fixture.volume["Attachments"][0]["InstanceId"] = "i-x"
        self.reject(fixture, "EBS volume")

    def test_instances_must_belong_to_the_planned_auto_scaling_groups_of_this_cluster(self):
        fixture = AzFixture()
        fixture.instance_group = "asg-other"
        self.reject(fixture, "outside the fixture VPC or its Auto Scaling groups")
        fixture = AzFixture()
        fixture.asg_tags = [{"Key": "eks:cluster-name", "Value": "another-cluster"}]
        self.reject(fixture, "not owned by this cluster")

    def test_account_run_and_cluster_ownership_are_checked_first(self):
        fixture = AzFixture()
        fixture.account = "111111111111"
        self.reject(fixture, "AWS account")
        self.reject(AzFixture(), "disposable fixture", runId="other")
        self.reject(AzFixture(), "disposable fixture", clusterName="production")


class FakeTime:
    def __init__(self):
        self.now = 1000.0

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


class FakeDurable:
    def __init__(self, events, clock, results=None):
        self.events = events
        self.clock = clock
        self.results = results or {}
        self.verify_times = {}

    def verify(self, label, cutoff=None):
        self.events.append(("durable", "verify", label))
        self.verify_times[label] = self.clock.now
        return {"result": self.results.get(label, "PASS"), "cutoff": cutoff}

    def finish(self, label):
        self.events.append(("durable", "finish", label))
        return {"result": self.results.get(label, "PASS")}


class AzOutageExecuteTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="narumq-az-outage-test-")
        self.addCleanup(self.temporary.cleanup)
        self.output = Path(self.temporary.name)
        self.clock = FakeTime()
        patcher = patch.object(az_outage, "time", self.clock)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.fixture = AzFixture()
        self.durable = FakeDurable(self.fixture.events, self.clock)
        self.result = {}

    def execute(self, plan=None):
        az_outage.execute(self.fixture, plan or AZ_PLAN, self.output, self.fixture.pods, self.durable,
                          lambda: None, self.result)

    def commands(self, name):
        return [e for e in self.fixture.events if e[0] == "command" and name in e]

    def index(self, *prefix):
        return next(i for i, e in enumerate(self.fixture.events) if e[:len(prefix)] == prefix)

    def test_outage_runs_in_order_and_resumes_exactly_what_it_suspended(self):
        self.fixture.suspended["asg-c"] = {"AZRebalance"}
        self.execute()
        self.assertEqual(self.fixture.suspended, {"asg-c": {"AZRebalance"}})
        suspend, = self.commands("suspend-processes")
        resume, = self.commands("resume-processes")
        self.assertEqual(suspend[-4:], ("Launch", "Terminate", "ReplaceUnhealthy", "HealthCheck"))
        self.assertEqual(resume[-4:], ("Launch", "Terminate", "ReplaceUnhealthy", "HealthCheck"))
        stop = self.index("aws", "ec2", "stop-instances", "--instance-ids", "i-2", "i-x", "--skip-os-shutdown")
        start = self.index("aws", "ec2", "start-instances")
        order = [self.fixture.events.index(suspend), stop, self.index("durable", "verify", "mid-outage"), start,
                 self.index("durable", "finish", "after-restore"), self.fixture.events.index(resume)]
        self.assertEqual(order, sorted(order))
        self.assertEqual(len(set(order)), 6)

    def test_mid_outage_check_runs_halfway_through_the_hold_and_the_hold_is_kept(self):
        self.execute()
        timings = self.result["timings"]
        self.assertEqual(set(timings["at"]), {"stopRequested", "stopped", "startRequested", "running",
                                              "targetReady", "threeReady"})
        seconds = timings["secondsSinceStopRequest"]
        self.assertGreaterEqual(seconds["startRequested"] - seconds["stopped"], 60)
        self.assertLess(seconds["startRequested"] - seconds["stopped"], 62)
        mid = self.durable.verify_times["mid-outage"] - 1000.0
        self.assertGreaterEqual(mid, 30)
        self.assertLess(mid, 32)
        self.assertEqual(self.result["midOutage"]["result"], "PASS")
        self.assertEqual(self.result["afterRestore"]["result"], "PASS")
        self.assertTrue(self.result["recovery"]["samePVC_PV_EBS"])
        self.assertFalse(self.result["asg"]["resume"]["failed"])

    def test_every_planned_instance_is_stopped_and_started_together(self):
        self.execute()
        for operation in ("stop-instances", "start-instances"):
            call, = [e for e in self.fixture.events if e[:3] == ("aws", "ec2", operation)]
            self.assertEqual(values_after(call, "--instance-ids"), ["i-2", "i-x"])
        waits = [e[3:5] for e in self.fixture.events if e[:3] == ("command", "aws", "ec2") and e[3] == "wait"]
        self.assertEqual(waits, [("wait", "instance-stopped"), ("wait", "instance-running")])

    def test_a_stop_failure_still_resumes_the_suspended_processes_and_never_starts_instances(self):
        self.fixture.stop_error = True
        with self.assertRaises(fixture_module.CommandFailed):
            self.execute()
        self.assertEqual(self.fixture.suspended, {"asg-c": set()})
        resume, = self.commands("resume-processes")
        self.assertEqual(resume[-5:], az_outage.PROCESSES)
        self.assertEqual([e for e in self.fixture.events if e[:3] == ("aws", "ec2", "start-instances")], [])
        self.assertFalse(self.result["asg"]["resume"]["failed"])

    def test_a_failed_second_group_suspension_resumes_both_groups(self):
        self.fixture.suspended = {"asg-c": set(), "asg-d": set()}
        self.fixture.suspend_error_group = "asg-d"
        with self.assertRaises(fixture_module.CommandFailed):
            self.execute({**AZ_PLAN, "asgNames": ["asg-c", "asg-d"]})
        self.assertEqual(self.fixture.suspended, {"asg-c": set(), "asg-d": set()})
        self.assertEqual(sorted(c[c.index("--auto-scaling-group-name") + 1]
                                for c in self.commands("resume-processes")), ["asg-c", "asg-d"])
        self.assertEqual([e for e in self.fixture.events if e[:3] == ("aws", "ec2", "stop-instances")], [])

    def test_a_failed_resume_is_reported_as_failed(self):
        self.fixture.stop_error = True
        self.fixture.resume_error = True
        with self.assertRaises(fixture_module.CommandFailed):
            self.execute()
        resume = self.result["asg"]["resume"]
        self.assertTrue(resume["failed"])
        self.assertEqual(resume["groups"]["asg-c"]["errorCode"], "CommandFailed")

    def test_processes_that_stay_suspended_after_resume_are_a_failure(self):
        self.fixture.stop_error = True
        self.fixture.resume_noop = True
        with self.assertRaises(fixture_module.CommandFailed):
            self.execute()
        resume = self.result["asg"]["resume"]
        self.assertTrue(resume["failed"])
        self.assertEqual(resume["groups"]["asg-c"]["stillSuspended"], sorted(az_outage.PROCESSES))

    def test_an_unreadable_baseline_suspends_and_resumes_nothing(self):
        self.fixture.describe_error = True
        with self.assertRaises(fixture_module.CommandFailed):
            self.execute()
        self.assertEqual(self.fixture.mutations(), [])
        self.assertEqual(self.result["asg"]["suspendedByTool"], {})
        self.assertFalse(self.result["asg"]["resume"]["failed"])

    def test_recovery_timeout_fails_after_the_deadline_and_still_resumes(self):
        original = self.fixture.get
        def never_ready(*args):
            value = original(*args)
            if args[0] == "pods":
                for pod in value["items"]:
                    if pod["metadata"]["name"] == "narumq-2":
                        pod["status"] = {"conditions": []}
            return value
        self.fixture.get = never_ready
        with self.assertRaises(TimeoutError):
            self.execute()
        self.assertGreaterEqual(self.clock.now - 1000.0, 60 + az_outage.RECOVERY_DEADLINE_SECONDS)
        self.assertEqual(self.fixture.suspended, {"asg-c": set()})
        self.assertNotIn("targetReady", self.result["timings"]["at"])
        self.assertNotIn("afterRestore", self.result)
        self.assertNotIn("postResume", self.result)

    def test_recovery_on_a_different_ebs_attachment_fails_and_still_resumes(self):
        self.fixture.volume["Attachments"][0]["InstanceId"] = "i-9"
        with self.assertRaisesRegex(RuntimeError, "same PVC, PV and EBS"):
            self.execute()
        self.assertEqual(self.fixture.suspended, {"asg-c": set()})
        self.assertNotIn("afterRestore", self.result)
        self.assertNotIn("postResume", self.result)

    def set_health_calls(self):
        return [e for e in self.fixture.events if e[:3] == ("command", "aws", "autoscaling")
                and e[3] == "set-instance-health"]

    def test_health_check_is_suspended_with_the_other_processes_and_resumed_with_them(self):
        self.assertIn("HealthCheck", az_outage.PROCESSES)
        self.execute()
        suspend, = self.commands("suspend-processes")
        resume, = self.commands("resume-processes")
        self.assertEqual(suspend[-5:], az_outage.PROCESSES)
        self.assertEqual(resume[-5:], az_outage.PROCESSES)

    def test_a_health_check_suspended_before_the_run_is_left_suspended(self):
        self.fixture.suspended["asg-c"] = {"HealthCheck"}
        self.execute()
        suspend, = self.commands("suspend-processes")
        resume, = self.commands("resume-processes")
        self.assertNotIn("HealthCheck", suspend)
        self.assertNotIn("HealthCheck", resume)
        self.assertEqual(self.fixture.suspended, {"asg-c": {"HealthCheck"}})

    def test_healthy_instances_are_not_touched(self):
        self.execute()
        self.assertEqual(self.set_health_calls(), [])
        health = self.result["asg"]["health"]
        self.assertEqual((health["set"], health["unrestored"]), ([], []))
        self.assertEqual(health["instances"]["i-2"], {"state": "running", "health": "Healthy",
                                                      "lifecycle": "InService", "nodeReady": True})

    def test_unhealthy_running_instances_with_ready_nodes_are_set_healthy_before_the_resume(self):
        for member in self.fixture.members["asg-c"]:
            member["HealthStatus"] = "Unhealthy"
        self.execute()
        calls = self.set_health_calls()
        self.assertEqual([c[3:] for c in calls],
                         [("set-instance-health", "--instance-id", i, "--health-status", "Healthy",
                           "--no-should-respect-grace-period") for i in ("i-2", "i-x")])
        self.assertEqual([c[5] for c in calls], ["i-2", "i-x"])
        for call in calls:
            self.assertEqual(call[6:], ("--health-status", "Healthy", "--no-should-respect-grace-period"))
            self.assertLess(self.fixture.events.index(call),
                            self.fixture.events.index(self.commands("resume-processes")[0]))
        self.assertEqual(self.result["asg"]["health"]["set"], ["i-2", "i-x"])

    def test_an_unhealthy_instance_whose_node_is_not_ready_is_never_set_healthy(self):
        for member in self.fixture.members["asg-c"]:
            member["HealthStatus"] = "Unhealthy"
        self.fixture.nodes["w-x"]["status"] = k8s_ready(False)
        self.execute()
        self.assertEqual([c[5] for c in self.set_health_calls()], ["i-2"])
        health = self.result["asg"]["health"]
        self.assertEqual((health["set"], health["unrestored"]), (["i-2"], ["i-x"]))
        self.assertGreaterEqual(self.clock.now - 1000.0, 60 + az_outage.HEALTH_WAIT_SECONDS)

    def test_an_instance_that_is_not_running_is_never_set_healthy(self):
        for member in self.fixture.members["asg-c"]:
            member["HealthStatus"] = "Unhealthy"
        original = self.fixture.aws
        def stays_stopped(*args):
            value = original(*args)
            if args[:2] == ("ec2", "start-instances"):
                self.fixture.state = "stopped"
            return value
        self.fixture.aws = stays_stopped
        self.execute()
        self.assertEqual(self.set_health_calls(), [])
        self.assertEqual(self.result["asg"]["health"]["unrestored"], ["i-2", "i-x"])

    def test_a_failure_after_the_start_still_restores_health_before_the_resume(self):
        for member in self.fixture.members["asg-c"]:
            member["HealthStatus"] = "Unhealthy"
        self.fixture.volume["Attachments"][0]["InstanceId"] = "i-9"
        with self.assertRaisesRegex(RuntimeError, "same PVC, PV and EBS"):
            self.execute()
        self.assertEqual(len(self.set_health_calls()), 2)
        self.assertLess(self.fixture.events.index(self.set_health_calls()[-1]),
                        self.fixture.events.index(self.commands("resume-processes")[0]))

    def test_no_health_change_is_made_when_the_instances_were_never_started(self):
        for member in self.fixture.members["asg-c"]:
            member["HealthStatus"] = "Unhealthy"
        self.fixture.stop_error = True
        with self.assertRaises(fixture_module.CommandFailed):
            self.execute()
        self.assertEqual(self.set_health_calls(), [])
        self.assertNotIn("health", self.result["asg"])

    def test_post_resume_observation_passes_when_nothing_changes(self):
        self.execute()
        observed = self.result["postResume"]
        self.assertEqual(observed["result"], "PASS", observed["reasons"])
        self.assertEqual(observed["seconds"], 60)
        self.assertEqual(observed["samples"], 7)
        saved = json.loads((self.output / "az-outage-post-resume.json").read_text(encoding="utf-8"))
        self.assertEqual(len(saved["samples"]), 7)
        self.assertEqual(saved["samples"][0]["podUIDs"], {"narumq-0": "u0", "narumq-1": "u1", "narumq-2": "u2"})
        self.assertEqual({n["name"] for n in saved["samples"][0]["nodes"]}, {"w0", "w1", "w2", "w-x"})
        resume_at = self.fixture.events.index(self.commands("resume-processes")[0])
        self.assertEqual(self.fixture.events[resume_at + 1:].count(
            ("aws", "autoscaling", "describe-scaling-activities", "--auto-scaling-group-name", "asg-c",
             "--max-items", "50")), 7)

    def test_a_scaling_activity_after_the_resume_fails_the_observation(self):
        def terminate():
            when = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=1)
            self.fixture.activities.append({"ActivityId": "act-new", "StartTime": when.isoformat(),
                                            "StatusCode": "InProgress", "Description": "Terminating EC2 instance: i-2",
                                            "Cause": "EC2 health check indicating it has been terminated or stopped"})
        self.fixture.after_resume = terminate
        self.execute()
        observed = self.result["postResume"]
        self.assertEqual(observed["result"], "FAIL")
        self.assertEqual([a["activityId"] for a in observed["activities"]], ["act-new"])

    def test_activities_that_started_before_the_resume_are_ignored(self):
        self.fixture.activities.append({"ActivityId": "act-old", "StatusCode": "Successful",
                                        "StartTime": "2020-01-01T00:00:00+00:00"})
        self.execute()
        self.assertEqual(self.result["postResume"]["result"], "PASS")

    def test_a_replaced_asg_instance_or_a_changed_broker_pod_after_the_resume_fails(self):
        def replace_instance():
            self.fixture.members["asg-c"][0] = {"InstanceId": "i-new", "HealthStatus": "Healthy",
                                                "LifecycleState": "InService"}
        def replace_pod():
            self.fixture.pods[1]["metadata"]["uid"] = "u1-new"
        for change in (replace_instance, replace_pod):
            with self.subTest(change=change.__name__):
                self.setUp()
                self.fixture.after_resume = change
                self.execute()
                self.assertEqual(self.result["postResume"]["result"], "FAIL")

    def test_a_missing_health_baseline_fails_the_observation_without_observing(self):
        with patch.object(az_outage, "restore_health", side_effect=RuntimeError("boom")):
            self.execute()
        self.assertEqual(self.result["asg"]["health"], {"errorCode": "RuntimeError"})
        self.assertEqual(self.result["postResume"]["result"], "FAIL")
        self.assertFalse(self.result["asg"]["resume"]["failed"])
        self.assertEqual(self.fixture.suspended, {"asg-c": set()})


class JudgePostResumeTests(unittest.TestCase):
    BASELINE = {"asg-c": {"i-2", "i-x"}}
    UIDS = {"narumq-0": "u0", "narumq-1": "u1"}

    def sample(self, **changes):
        sample = {"at": iso(0), "instances": {"asg-c": [{"instanceId": "i-2", "lifecycle": "InService",
                                                          "health": "Healthy"},
                                                         {"instanceId": "i-x", "lifecycle": "InService",
                                                          "health": "Healthy"}]},
                  "activities": [], "nodes": [], "podUIDs": dict(self.UIDS)}
        sample.update(changes)
        return sample

    def judge(self, *samples):
        return az_outage.judge_post_resume(list(samples), self.BASELINE, self.UIDS)

    def test_unchanged_samples_pass(self):
        self.assertEqual(self.judge(self.sample(), self.sample())["result"], "PASS")

    def test_no_samples_cannot_pass(self):
        self.assertEqual(self.judge()["result"], "FAIL")

    def test_each_kind_of_change_fails_with_its_reason(self):
        launched = self.sample(instances={"asg-c": [
            {"instanceId": i, "lifecycle": "InService", "health": "Healthy"} for i in ("i-2", "i-x", "i-new")]})
        terminated = self.sample(instances={"asg-c": [
            {"instanceId": "i-2", "lifecycle": "InService", "health": "Healthy"}]})
        leaving = self.sample(instances={"asg-c": [
            {"instanceId": "i-2", "lifecycle": "Terminating", "health": "Unhealthy"},
            {"instanceId": "i-x", "lifecycle": "InService", "health": "Healthy"}]})
        activity = self.sample(activities=[{"activityId": "a1"}])
        pods = self.sample(podUIDs={"narumq-0": "u0", "narumq-1": "other"})
        gone = self.sample(podUIDs={"narumq-0": "u0"})
        unreadable = {"at": iso(0), "errorCode": "CommandFailed"}
        cases = {"instance set changed": (launched, terminated), "not InService": (leaving,),
                 "scaling activity": (activity,), "Pod UIDs changed": (pods, gone), "could not be read": (unreadable,)}
        for reason, samples in cases.items():
            for sample in samples:
                with self.subTest(reason=reason):
                    result = self.judge(self.sample(), sample)
                    self.assertEqual(result["result"], "FAIL")
                    self.assertTrue(any(reason in r for r in result["reasons"]), result["reasons"])

    def test_one_bad_sample_among_good_ones_fails(self):
        bad = self.sample(podUIDs={"narumq-0": "u0", "narumq-1": "other"})
        self.assertEqual(self.judge(self.sample(), bad, self.sample())["result"], "FAIL")

    def test_post_resume_seconds_default_and_bounds(self):
        self.assertEqual(az_outage.post_resume_seconds({}), 180)
        for value in (60, 900, 181):
            self.assertEqual(az_outage.post_resume_seconds({"postResumeSeconds": value}), value)
        for value in (59, 901, 0, "180", 180.5, True, None):
            with self.subTest(value=value), self.assertRaisesRegex(ValueError, "postResumeSeconds"):
                az_outage.validate_plan({**AZ_PLAN, "postResumeSeconds": value})


class JudgeHoldTests(unittest.TestCase):
    START, END = iso(20), iso(80)

    def judge(self, **kwargs):
        return az_outage.judge_hold(streams(**kwargs), self.START, self.END, "3")

    def test_quorum_survivor_leader_and_successful_mqtt_for_the_whole_hold_pass(self):
        result = self.judge(new_leader_at=0)
        self.assertEqual(result["result"], "PASS")
        self.assertEqual(result["management"]["samples"], 31)

    def test_a_leader_in_the_outage_az_fails(self):
        self.assertEqual(self.judge(new_leader_at=40)["result"], "FAIL")

    def test_a_missing_quorum_fails(self):
        self.assertEqual(self.judge(new_leader_at=0, quorum_false={50})["result"], "FAIL")

    def test_a_failed_management_or_mqtt_sample_fails(self):
        self.assertEqual(self.judge(new_leader_at=0, management_failing={50})["result"], "FAIL")
        self.assertEqual(self.judge(new_leader_at=0, mqtt_failing={50})["result"], "FAIL")

    def test_failures_outside_the_hold_do_not_count(self):
        result = self.judge(new_leader_at=0, management_failing={90}, mqtt_failing={10, 94})
        self.assertEqual(result["result"], "PASS")

    def test_a_hold_without_samples_cannot_pass(self):
        result = az_outage.judge_hold(streams(times=range(0, 10, 2)), self.START, self.END, "3")
        self.assertEqual(result["result"], "FAIL")


class FastMonitor(observer.IndependentMonitor):
    def __init__(self, *args, **kwargs):
        kwargs["interval"] = 0.02
        super().__init__(*args, **kwargs)


class ScenarioWiringTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="narumq-scenario-wiring-test-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def run_main(self, mode, name, plan, patches, extra=()):
        plan_path = self.root / f"{name}-plan.json"
        plan_path.write_text(json.dumps(plan), encoding="utf-8")
        with contextlib.ExitStack() as stack, contextlib.redirect_stdout(io.StringIO()):
            stack.enter_context(patch.object(worker_main, "IndependentMonitor", FastMonitor))
            for target, attribute, kwargs in patches:
                stack.enter_context(patch.object(target, attribute, **kwargs))
            code = worker_main.main([mode, "--private", str(self.root), "--output", str(self.root / name),
                                     "--plan", str(plan_path), "--execute", *extra])
        return code, json.loads((self.root / name / "summary.json").read_text(encoding="utf-8"))

    def run_fault(self, name, role, mqtt_failures=0, durable_result="PASS", leader_after="2", scenario="leader-loss"):
        state = {"faulted": False, "mqtt_failures": mqtt_failures}
        events = []
        plan = {"observerPod": "narumq-0", "targetRole": role, "targetNodeId": "3"}
        if scenario:
            plan["durableScenario"] = scenario

        def observe(fixture, plan, output, original):
            events.append("fault")
            state["faulted"] = True
            time.sleep(0.4)
            return {"fencing": {"outcome": "out_of_service"}, "podRelease": {"outcome": "deleted"}}

        def management():
            return {"quorumAvailable": True, "leaderNodeId": leader_after if state["faulted"] else "3",
                    "nodesByState": {"ready": 3}}

        def mqtt():
            if state["faulted"] and state["mqtt_failures"] > 0:
                state["mqtt_failures"] -= 1
                return {"puback": False}
            return {"puback": True}

        with patch.object(worker_main, "DurableRun") as durable_class:
            durable = durable_class.return_value
            durable.report = {"verifications": {"after-recovery": {"result": durable_result}}}
            durable.begin.side_effect = lambda: events.append("begin")
            durable.finish.side_effect = lambda label: events.append(("finish", label))
            with patch.object(worker_main, "Fixture") as fixture_class, \
                 patch.object(worker_main, "SurvivorTunnel") as tunnel_class, \
                 patch.object(worker_main, "preflight", return_value=[]), \
                 patch.object(worker_main, "MQTTProbe") as mqtt_class, \
                 patch.object(worker_main, "observe_fault", side_effect=observe):
                tunnel_class.return_value.start.return_value = {"localPort": 23880}
                fixture_class.return_value.nodes_probe.return_value = []
                fixture_class.return_value.management_probe.side_effect = management
                mqtt_class.return_value.probe.side_effect = mqtt
                code, summary = self.run_main("fault", name, plan, [])
            constructed = durable_class.call_count
        return code, summary, events, constructed

    def test_leader_role_passes_one_short_failure_window_that_the_follower_rule_rejects(self):
        code, summary, events, _ = self.run_fault("leader-short", "leader", mqtt_failures=1)
        self.assertEqual((code, summary["result"]), (0, "PASS"))
        self.assertEqual(summary["continuity"]["result"], "PASS", summary["continuity"]["reasons"])
        self.assertEqual(summary["monitor"]["streams"]["mqtt"]["failed"], 1)
        code, summary, _, _ = self.run_fault("follower-short", "follower", mqtt_failures=1)
        self.assertEqual((code, summary["result"]), (1, "FAIL"))
        self.assertNotIn("continuity", summary)

    def test_leader_role_without_a_survivor_leader_fails(self):
        code, summary, _, _ = self.run_fault("leader-unchanged", "leader", leader_after="3")
        self.assertEqual((code, summary["result"]), (1, "FAIL"))
        self.assertIsNone(summary["continuity"]["newLeader"])

    def test_follower_role_without_failures_still_passes(self):
        code, summary, _, _ = self.run_fault("follower-clean", "follower", leader_after="3")
        self.assertEqual((code, summary["result"]), (0, "PASS"))

    def test_durable_stream_starts_before_the_fault_and_is_verified_after_recovery(self):
        code, summary, events, constructed = self.run_fault("durable-ok", "leader")
        self.assertEqual(events, ["begin", "fault", ("finish", "after-recovery")])
        self.assertEqual((code, summary["result"], constructed), (0, "PASS", 1))
        self.assertEqual(summary["durable"]["verifications"]["after-recovery"]["result"], "PASS")

    def test_a_failed_durable_verification_fails_the_whole_run(self):
        code, summary, _, _ = self.run_fault("durable-fail", "leader", durable_result="FAIL")
        self.assertEqual((code, summary["result"]), (1, "FAIL"))
        self.assertEqual(summary["continuity"]["result"], "PASS")

    def test_without_a_durable_scenario_no_durable_stream_is_created(self):
        code, summary, events, constructed = self.run_fault("no-durable", "leader", scenario=None)
        self.assertEqual((code, summary["result"], constructed), (0, "PASS", 0))
        self.assertEqual(events, ["fault"])

    def run_az(self, name, mid="PASS", final="PASS", resume_failed=False, leader_in_hold="1", recovery=True,
               mqtt_failing_in_hold=False, mqtt_failing_after_hold=False, plan=None, post="PASS"):
        state = {"phase": "before"}

        def execute(fixture, plan, output, original, durable, check, result):
            result["timings"] = {"at": {"stopRequested": worker_main.timestamp()}}
            state["phase"] = "hold"
            time.sleep(0.4)
            result["timings"]["at"]["startRequested"] = worker_main.timestamp()
            state["phase"] = "after"
            time.sleep(0.2)
            result["midOutage"] = {"result": mid}
            result["afterRestore"] = {"result": final}
            if recovery:
                result["recovery"] = {"samePVC_PV_EBS": True}
            result["asg"] = {"resume": {"failed": resume_failed}}
            if post is not None:
                result["postResume"] = {"result": post}

        def management():
            leader = leader_in_hold if state["phase"] == "hold" else "1"
            return {"quorumAvailable": True, "leaderNodeId": leader, "nodesByState": {"ready": 3}}

        def mqtt():
            failing = ((state["phase"] == "hold" and mqtt_failing_in_hold)
                       or (state["phase"] == "after" and mqtt_failing_after_hold))
            return {"puback": not failing}

        with patch.object(worker_main, "Fixture") as fixture_class, \
             patch.object(worker_main, "SurvivorTunnel") as tunnel_class, \
             patch.object(worker_main, "MQTTProbe") as mqtt_class, \
             patch.object(worker_main, "DurableRun") as durable_class, \
             patch.object(worker_main, "require_drain_record_absent"), \
             patch.object(az_outage, "preflight", return_value={"pods": []}), \
             patch.object(az_outage, "execute", side_effect=execute) as execute_mock:
            tunnel_class.return_value.start.return_value = {"localPort": 23880}
            durable_class.return_value.report = {"scenario": "az-outage", "verifications": {}}
            fixture_class.return_value.nodes_probe.return_value = []
            fixture_class.return_value.management_probe.side_effect = management
            mqtt_class.return_value.probe.side_effect = mqtt
            code, summary = self.run_main("az-outage", name, plan or AZ_PLAN, [])
        return code, summary, tunnel_class, execute_mock

    def test_az_outage_passes_when_hold_durable_recovery_and_resume_are_all_good(self):
        code, summary, _, _ = self.run_az("az-ok")
        self.assertEqual((code, summary["result"]), (0, "PASS"), summary.get("hold"))
        self.assertEqual(summary["holdSeconds"], 60)
        self.assertEqual(summary["hold"]["result"], "PASS")

    def test_failures_after_the_hold_do_not_fail_the_az_outage_but_failures_inside_it_do(self):
        code, summary, _, _ = self.run_az("az-after", mqtt_failing_after_hold=True)
        self.assertEqual((code, summary["result"]), (0, "PASS"))
        self.assertEqual(summary["monitor"]["result"], "FAIL")
        code, summary, _, _ = self.run_az("az-inside", mqtt_failing_in_hold=True)
        self.assertEqual((code, summary["result"]), (1, "FAIL"))

    def test_az_outage_fails_for_each_unmet_condition(self):
        cases = {"mid": {"mid": "FAIL"}, "final": {"final": "FAIL"}, "resume": {"resume_failed": True},
                 "leader-in-zone": {"leader_in_hold": "3"}, "no-recovery": {"recovery": False},
                 "post-resume": {"post": "FAIL"}, "no-post-resume": {"post": None}}
        for name, kwargs in cases.items():
            with self.subTest(name=name):
                code, summary, _, _ = self.run_az("az-" + name, **kwargs)
                self.assertEqual((code, summary["result"]), (1, "FAIL"))

    def test_an_out_of_bounds_hold_is_refused_before_any_tunnel_or_mutation(self):
        for hold in (59, 1801):
            with self.subTest(hold=hold):
                code, summary, tunnel_class, execute_mock = self.run_az(f"az-hold-{hold}",
                                                                       plan={**AZ_PLAN, "holdSeconds": hold})
                self.assertEqual((code, summary["result"], summary["errorCode"]), (1, "FAIL", "ValueError"))
                tunnel_class.assert_not_called()
                execute_mock.assert_not_called()

    def test_az_outage_and_fault_require_execute_and_plan(self):
        for mode in ("fault", "az-outage"):
            with self.subTest(mode=mode), contextlib.redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit) as raised:
                    worker_main.main([mode, "--private", str(self.root), "--output", str(self.root / ("x-" + mode)),
                                      "--plan", str(self.root / "plan.json")])
                self.assertEqual(raised.exception.code, 2)
                self.assertFalse((self.root / ("x-" + mode)).exists())


if __name__ == "__main__":
    unittest.main()
