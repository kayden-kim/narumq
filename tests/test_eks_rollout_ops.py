"""Offline tests of the shared rollout helpers (tests/chaos/eks_rollout_ops.py) with a fake fixture; no cluster."""
from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tests/chaos"))
import eks_rollout_ops as ops  # noqa: E402

REPOSITORY = "123456789012.dkr.ecr.ap-northeast-2.amazonaws.com/narumq"


def metrics(**values: int) -> str:
    lines = ["# TYPE narumq_will_outcomes_total counter", "narumq_other_total 5"]
    for reason in ops.WILL_REASONS:
        lines.append(f'narumq_will_outcomes_total{{reason="{reason}"}} {values.get(reason, 0)}')
    return "\n".join(lines) + "\n"


def sample(pod: str, uid: str, **values: int) -> dict:
    return {"pod": pod, "uid": uid, "counters": ops.parse_will_outcomes(metrics(**values))}


class WillOutcomes(unittest.TestCase):
    def test_parse_reads_the_six_fixed_series(self):
        counters = ops.parse_will_outcomes(metrics(unexpected_supported_client=2, unplanned_failure=1))
        self.assertEqual(set(counters), set(ops.WILL_REASONS))
        self.assertEqual((counters["unexpected_supported_client"], counters["unplanned_failure"], counters["suppressed_normal_disconnect"]), (2, 1, 0))

    def test_missing_series_or_bad_value_raise(self):
        with self.assertRaises(ops.BrokerProbeError):
            ops.parse_will_outcomes("narumq_other_total 1\n")
        with self.assertRaises(ops.BrokerProbeError):
            ops.parse_will_outcomes(metrics().replace('reason="unplanned_failure"} 0', 'reason="unplanned_failure"} -1'))

    def test_accumulator_ignores_the_baseline_and_sums_growth(self):
        accumulator = ops.WillAccumulator()
        accumulator.start(sample("narumq-0", "u0", expected_mqtt311_server_disconnect=10))
        accumulator.add(sample("narumq-0", "u0", expected_mqtt311_server_disconnect=14))
        accumulator.add(sample("narumq-0", "u0", expected_mqtt311_server_disconnect=14))
        self.assertEqual(accumulator.totals["expected_mqtt311_server_disconnect"], 4)

    def test_new_process_generation_counts_from_zero(self):
        accumulator = ops.WillAccumulator()
        accumulator.start(sample("narumq-0", "u0", unexpected_supported_client=7))
        accumulator.add(sample("narumq-0", "u0", unexpected_supported_client=7))
        accumulator.add(sample("narumq-0", "u1", unexpected_supported_client=2))  # replaced Pod: the new process started at zero
        accumulator.add(sample("narumq-0", "u1", unexpected_supported_client=3))
        self.assertEqual(accumulator.totals["unexpected_supported_client"], 3)

    def test_counter_decrease_inside_one_generation_is_an_error(self):
        accumulator = ops.WillAccumulator()
        accumulator.start(sample("narumq-0", "u0", unplanned_failure=5))
        with self.assertRaises(ops.BrokerProbeError):
            accumulator.add(sample("narumq-0", "u0", unplanned_failure=4))

    def test_state_round_trips_through_json(self):
        accumulator = ops.WillAccumulator()
        accumulator.start(sample("narumq-0", "u0", unplanned_failure=1))
        accumulator.add(sample("narumq-0", "u0", unplanned_failure=3))
        restored = ops.WillAccumulator.from_json(json.loads(json.dumps(accumulator.to_json())))
        restored.add(sample("narumq-0", "u0", unplanned_failure=4))
        self.assertEqual(restored.totals["unplanned_failure"], 3)
        self.assertEqual(restored.baseline, accumulator.baseline)


class FakeFixture:
    def __init__(self, statefulset: dict | None = None, kubectl_output: str = "", statuses: list[dict] | None = None):
        self.statefulset, self.kubectl_output, self.statuses = statefulset, kubectl_output, list(statuses or [])
        self.mutations: list[tuple] = []
        self.applied: list = []
        self.calls: list[tuple] = []

    def kjson(self, *args, **kwargs):
        if args[:2] == ("get", "statefulset"):
            return self.statefulset
        if args[:2] == ("get", "narumqrollout"):
            return {"metadata": {"uid": "rollout-uid"}}
        raise AssertionError(args)

    def mutate(self, *args, **kwargs):
        self.mutations.append(args)
        return ""

    def apply(self, *objects, **kwargs):
        self.applied.extend(objects)

    def kubectl(self, *args, **kwargs):
        self.calls.append(args)
        return self.kubectl_output

    def status(self, name):
        return self.statuses.pop(0) if len(self.statuses) > 1 else self.statuses[0]

    def broker_pods(self):
        return {"narumq-0": {"uid": "u0", "ready": True, "deleting": None}, "narumq-1": {"uid": "u1", "ready": False, "deleting": None}}


def statefulset(broker_image: str, other_image: str = "runner@sha256:" + "c" * 64) -> dict:
    return {"spec": {"template": {"spec": {
        "initContainers": [{"name": "init", "image": broker_image}],
        "containers": [{"name": "narumq", "image": broker_image}, {"name": "capacity-jwks", "image": other_image}]}}}}


class StatefulSetPatch(unittest.TestCase):
    OLD = f"{REPOSITORY}@sha256:" + "a" * 64
    NEW = f"{REPOSITORY}@sha256:" + "b" * 64

    def test_repository_of(self):
        self.assertEqual(ops.repository_of(self.OLD), REPOSITORY)
        self.assertEqual(ops.repository_of("localhost:5000/narumq:1.2.0"), "localhost:5000/narumq")
        self.assertEqual(ops.repository_of("narumq"), "narumq")

    def test_only_broker_repository_containers_change(self):
        operations = ops.image_patch_operations(statefulset(self.OLD), REPOSITORY, self.NEW)
        self.assertEqual([(o["path"], o["value"]) for o in operations],
                         [("/spec/template/spec/initContainers/0/image", self.NEW), ("/spec/template/spec/containers/0/image", self.NEW)])

    def test_set_template_image_patches_and_refuses_a_no_op(self):
        fixture = FakeFixture(statefulset(self.OLD))
        ops.set_template_image(fixture, self.NEW)
        self.assertEqual(fixture.mutations[0][:3], ("patch", "statefulset", "narumq"))
        self.assertIn(self.NEW, fixture.mutations[0][-1])
        with self.assertRaisesRegex(RuntimeError, "replace nothing"):
            ops.set_template_image(FakeFixture(statefulset(self.NEW)), self.NEW)

    def test_digest_of_image_id(self):
        self.assertEqual(ops.digest_of_image_id(f"{REPOSITORY}@sha256:" + "a" * 64), "sha256:" + "a" * 64)
        self.assertIsNone(ops.digest_of_image_id(None))
        self.assertEqual(ops.digest_of_image_id("sha256:" + "a" * 64), "sha256:" + "a" * 64)
        self.assertEqual(ops.digest_of_image_id(f"docker-pullable://{REPOSITORY}@sha256:" + "b" * 64), "sha256:" + "b" * 64)


class BrokerReads(unittest.TestCase):
    def test_api_status_and_body_are_split(self):
        status, body = ops.broker_api(FakeFixture(kubectl_output='{"data":{"quorumAvailable":true}}\n200'), "narumq-0", "/api/v1/cluster")
        self.assertEqual((status, body["data"]["quorumAvailable"]), (200, True))

    def test_unsafe_path_is_refused(self):
        with self.assertRaises(ValueError):
            ops.broker_api(FakeFixture(), "narumq-0", "/api/v1/cluster; rm -rf /")

    def test_failed_exec_is_a_probe_error(self):
        class Failing(FakeFixture):
            def kubectl(self, *args, **kwargs):
                raise RuntimeError("pod is stopping")
        with self.assertRaises(ops.BrokerProbeError):
            ops.broker_api(Failing(), "narumq-0", "/api/v1/cluster")

    def test_ready_pods_exclude_unready(self):
        self.assertEqual(ops.ready_broker_pods(FakeFixture()), ["narumq-0"])

    def test_scrape_uses_the_pod_uid(self):
        scraped = ops.scrape_will_outcomes(FakeFixture(kubectl_output=metrics(unplanned_failure=2)), "narumq-0")
        self.assertEqual((scraped["pod"], scraped["uid"], scraped["counters"]["unplanned_failure"]), ("narumq-0", "u0", 2))
        with self.assertRaises(ops.BrokerProbeError):
            ops.scrape_will_outcomes(FakeFixture(), "narumq-9")


class FakeSession:
    rollout_name = "drn-test"

    def rollout_manifest(self, token: str) -> dict:
        return {"kind": "NaruMQRollout", "metadata": {"name": self.rollout_name}, "token": token}


class RolloutDriving(unittest.TestCase):
    def test_samples_until_terminal_and_merges_observations(self):
        statuses = [{"phase": "Running", "currentOrdinal": 2}, {"phase": "Running", "currentOrdinal": 1, "completedOrdinals": [2]},
                    {"phase": "Completed", "completedOrdinals": [2, 1, 0]}]
        fixture = FakeFixture(statuses=statuses)
        drive = ops.drive_rollout(fixture, FakeSession(), "tok", 30, 0.0, lambda status, sample: {"quorumAvailable": True})
        self.assertEqual((drive["uid"], drive["status"]["phase"], len(drive["samples"])), ("rollout-uid", "Completed", 3))
        self.assertTrue(all(s["quorumAvailable"] for s in drive["samples"]))
        self.assertEqual(fixture.applied[0]["token"], "tok")

    def test_timeout_raises_with_the_samples_taken(self):
        fixture = FakeFixture(statuses=[{"phase": "Running"}])
        with self.assertRaises(ops.RolloutTimeout) as raised:
            ops.drive_rollout(fixture, FakeSession(), "tok", 0.0, 0.0)
        self.assertEqual(len(raised.exception.samples), 1)

    def test_approvals_are_the_delete_approvals_of_one_rollout(self):
        events = [{"action": ops.APPROVAL_ACTION, "details": {"rolloutUid": "a", "ordinal": 2}},
                  {"action": ops.APPROVAL_ACTION, "details": {"rolloutUid": "b", "ordinal": 2}},
                  {"action": "rollout.started", "details": {"rolloutUid": "a"}}]
        self.assertEqual([e["details"]["ordinal"] for e in ops.approvals_of(events, "a")], [2])

    def test_delete_rollout_ignores_a_missing_object(self):
        fixture = FakeFixture()
        ops.delete_rollout(fixture, "drn-test")
        self.assertIn("--ignore-not-found", fixture.mutations[0])


if __name__ == "__main__":
    unittest.main()
