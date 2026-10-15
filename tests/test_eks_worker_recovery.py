from __future__ import annotations

import copy
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import threading
import unittest

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parents[1]
path = ROOT / "tests/chaos/eks-worker-recovery/observer.py"
spec = importlib.util.spec_from_file_location("eks_worker_observer", path)
observer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(observer)


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


if __name__ == "__main__":
    unittest.main()
