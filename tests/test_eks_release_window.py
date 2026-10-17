"""Offline tests of the DRN-SCN-020 judgment (published N-1/N pair, same safe-to-stop barrier in both directions).

No cluster and no registry: observed data are synthetic dictionaries and the registry is a fake that returns bytes.
"""
from __future__ import annotations

import argparse
import copy
import hashlib
import importlib.util
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parents[1]
RELEASE_DIR = ROOT / "tests/chaos/eks-release-window"
sys.path.insert(0, str(ROOT / "tests"))
sys.path.insert(0, str(ROOT / "tests/chaos"))
sys.path.insert(0, str(RELEASE_DIR))
import eks_rollout_ops as ops  # noqa: E402
import release_window_judge as judge  # noqa: E402

main_spec = importlib.util.spec_from_file_location("eks_release_window_main", RELEASE_DIR / "main.py")
release_main = importlib.util.module_from_spec(main_spec)
main_spec.loader.exec_module(release_main)

REPOSITORY = "123456789012.dkr.ecr.ap-northeast-2.amazonaws.com/narumq"
FEATURES = ["cluster-observability-v1", "session-metadata"]
NOW = 1_800_000_000.0


def manifest_bytes(tag: str) -> bytes:
    return json.dumps({"schemaVersion": 2, "mediaType": "application/vnd.oci.image.manifest.v1+json",
                       "config": {"digest": "sha256:" + hashlib.sha256(tag.encode()).hexdigest()}, "layers": []}).encode()


def digest_of(data: bytes) -> str:
    return "sha256:" + hashlib.sha256(data).hexdigest()


def config_for(version: str, features: str = ",".join(FEATURES), arch: str = "arm64") -> dict:
    return {"os": "linux", "architecture": arch,
            "config": {"Labels": {ops.VERSION_LABEL: version, ops.FEATURE_LABEL: features}}}


def image(version: str, features: list[str] | None = None) -> dict:
    data = manifest_bytes(version)
    digest = digest_of(data)
    return {"reference": f"{REPOSITORY}@{digest}", "repository": REPOSITORY, "digest": digest, "version": version,
            "features": features or list(FEATURES), "configDigest": None, "mediaType": None, "os": "linux", "architecture": "arm64"}


def record(img: dict, **overrides) -> dict:
    value = {"version": img["version"], "digest": img["digest"], "repository": REPOSITORY, "publishedAt": "2026-09-01T00:00:00Z",
             "source": {"type": "github-release", "url": f"https://github.com/example/narumq/releases/tag/v{img['version']}"}}
    value.update(overrides)
    return value


class FakeRegistry:
    def __init__(self, images: dict[str, tuple[bytes, dict]], tags: dict[str, bytes] | None = None):
        self.images, self.tags = images, tags or {}

    def verify(self, reference: str) -> dict:
        manifest, config = self.images[reference.split("@")[1]]
        return ops.verify_platform_image(reference, manifest, config)

    def manifest(self, reference: str) -> bytes:
        return self.tags[reference]


def approval(ordinal: int, uid: str, target: dict, **detail_overrides) -> dict:
    details = {"rollout": "r", "rolloutUid": "uid-1", "workload": "narumq", "ordinal": ordinal, "nodeId": ordinal + 1, "podUid": uid,
               "podResourceVersion": "10", "attemptKey": "k", "drainOperationId": "op", "targetImage": target["reference"],
               "targetReleaseVersion": target["version"], "drainPhase": "COMPLETED", "drainState": "SAFE_TO_STOP",
               "drainSafeToStop": True, "connectionsInitial": 12, "connectionsRemaining": 0, "sessionsReady": 12,
               "sessionsTakenOver": 12, "sessionsFailed": 0, "pendingWAL": 0, "underReplicated": 0, "drainFailureCode": "",
               "blockingReason": "", "safeToStopObservedAt": "2026-10-17T00:00:00.123456789Z"}
    details.update(detail_overrides)
    return {"action": ops.APPROVAL_ACTION, "result": "ALLOWED", "reason": "safe_to_stop", "occurredAt": "2026-10-17T00:00:01.5Z",
            "target": {"type": "broker_pod", "id": f"narumq-{ordinal}"}, "details": details}


def pods(image_: dict, prefix: str) -> dict:
    return {f"narumq-{o}": {"uid": f"{prefix}{o}", "image": image_["reference"], "imageID": f"{REPOSITORY}@{image_['digest']}",
                            "ready": True, "deleting": None} for o in range(3)}


def members(version: str, features: list[str]) -> list[dict]:
    return [{"nodeId": str(n), "releaseVersion": version, "features": list(features)} for n in (1, 2, 3)]


def good_direction(label: str, target: dict, before_prefix: str, after_prefix: str) -> dict:
    before = pods(image("1.1.0" if label == "upgrade" else "1.2.0"), before_prefix)
    after = pods(target, after_prefix)
    approvals = [approval(o, f"{before_prefix}{o}", target) for o in range(3)]
    status = {"phase": "Completed", "completedOrdinals": [0, 1, 2]}
    quorum = [{"quorumAvailable": True} for _ in range(5)]
    return judge.judge_direction(label, target, before, after, status, approvals,
                                 {"members": members(target["version"], target["features"])}, quorum)


class ReleaseRulesTest(unittest.TestCase):
    def test_parse_version_is_canonical(self):
        self.assertEqual(ops.parse_version("1.20.3"), (1, 20, 3))
        for bad in ("v1.0.0", "1.0", "1.0.0.0", "01.0.0", "1.0.0-rc1", "1.0.0+b", "", "1.-1.0", "4294967296.0.0", None):
            with self.assertRaises(ops.ReleaseError, msg=str(bad)):
                ops.parse_version(bad)

    def test_window_matches_domain_rule(self):
        self.assertTrue(ops.versions_compatible((1, 1, 9), (1, 2, 0)))
        self.assertTrue(ops.versions_compatible((1, 2, 0), (1, 1, 9)))
        self.assertFalse(ops.versions_compatible((1, 0, 0), (1, 2, 0)))
        self.assertFalse(ops.versions_compatible((1, 1, 0), (2, 1, 0)))

    def test_feature_label_must_be_sorted_unique(self):
        self.assertEqual(ops.parse_feature_label("a-b,c1"), ["a-b", "c1"])
        for bad in ("", "b,a", "a,a", "A", "a,,b", ",".join(f"f{chr(97 + i)}" for i in range(17))):
            with self.assertRaises(ops.ReleaseError, msg=bad):
                ops.parse_feature_label(bad)


class ImageVerificationTest(unittest.TestCase):
    def verify(self, manifest: bytes, config: dict, digest: str | None = None) -> dict:
        return ops.verify_platform_image(f"{REPOSITORY}@{digest or digest_of(manifest)}", manifest, config)

    def test_accepts_arm64_platform_manifest(self):
        result = self.verify(manifest_bytes("a"), config_for("1.2.0"))
        self.assertEqual((result["version"], result["features"]), ("1.2.0", FEATURES))

    def test_rejects_wrong_bytes_for_digest(self):
        with self.assertRaisesRegex(ops.ReleaseError, "hash to"):
            self.verify(manifest_bytes("a"), config_for("1.2.0"), digest="sha256:" + "0" * 64)

    def test_rejects_index_and_tag_references(self):
        index = json.dumps({"mediaType": ops.INDEX_MEDIA_TYPES[0], "manifests": []}).encode()
        with self.assertRaisesRegex(ops.ReleaseError, "index"):
            self.verify(index, config_for("1.2.0"))
        with self.assertRaises(ops.ReleaseError):
            ops.verify_platform_image(f"{REPOSITORY}:1.2.0", b"{}", config_for("1.2.0"))

    def test_rejects_other_platform_and_missing_labels(self):
        with self.assertRaisesRegex(ops.ReleaseError, "arm64"):
            self.verify(manifest_bytes("a"), config_for("1.2.0", arch="amd64"))
        config = config_for("1.2.0")
        del config["config"]["Labels"][ops.FEATURE_LABEL]
        with self.assertRaisesRegex(ops.ReleaseError, "io.narumq.features"):
            self.verify(manifest_bytes("a"), config)

    def test_buildx_single_platform_config_shapes_are_normalized(self):
        wrapped = {"linux/arm64": config_for("1.2.0")}
        self.assertEqual(ops.normalize_image_config(wrapped)["architecture"], "arm64")

    def test_tag_resolution_direct_and_through_index(self):
        data = manifest_bytes("a")
        self.assertTrue(ops.tag_resolves_to(data, digest_of(data)))
        index = json.dumps({"mediaType": ops.INDEX_MEDIA_TYPES[0], "manifests": [
            {"digest": digest_of(data), "platform": {"os": "linux", "architecture": "arm64"}}]}).encode()
        self.assertTrue(ops.tag_resolves_to(index, digest_of(data)))
        self.assertFalse(ops.tag_resolves_to(index, "sha256:" + "1" * 64))


class PublishedReleaseTest(unittest.TestCase):
    def setUp(self):
        self.previous, self.current = image("1.1.0"), image("1.2.0")

    def records(self, *items) -> dict:
        return {"schemaVersion": 1, "releases": list(items)}

    def test_binds_matching_record(self):
        bound = judge.bind_published(self.current, self.records(record(self.current)), NOW)
        self.assertEqual(bound["digest"], self.current["digest"])

    def test_missing_or_absent_record_names_what_is_missing(self):
        with self.assertRaisesRegex(ops.MissingPublishedRelease, "--release-record"):
            judge.load_release_records(None)
        with self.assertRaisesRegex(ops.MissingPublishedRelease, "does not exist"):
            judge.load_release_records(Path("no-such-record.json"))
        with self.assertRaisesRegex(ops.MissingPublishedRelease, "no published-release record for version 1.2.0"):
            judge.bind_published(self.current, self.records(record(self.previous)), NOW)

    def test_bad_schema_file_is_a_missing_release(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "record.json"
            path.write_text('{"schemaVersion": 2, "releases": []}', encoding="utf-8")
            with self.assertRaises(ops.MissingPublishedRelease):
                judge.load_release_records(path)
            path.write_text("{not json", encoding="utf-8")
            with self.assertRaises(ops.MissingPublishedRelease):
                judge.load_release_records(path)

    def test_digest_repository_and_tag_mismatches(self):
        with self.assertRaisesRegex(ops.MissingPublishedRelease, "digest"):
            judge.bind_published(self.current, self.records(record(self.current, digest="sha256:" + "2" * 64)), NOW)
        with self.assertRaisesRegex(ops.MissingPublishedRelease, "repository"):
            judge.bind_published(self.current, self.records(record(self.current, repository="other/narumq")), NOW)
        wrong = record(self.current, source={"type": "github-release", "url": "https://github.com/example/narumq/releases/tag/v1.9.9"})
        with self.assertRaisesRegex(ops.MissingPublishedRelease, "tag"):
            judge.bind_published(self.current, self.records(wrong), NOW)

    def test_incomplete_and_future_records(self):
        incomplete = record(self.current)
        del incomplete["publishedAt"]
        with self.assertRaisesRegex(ops.MissingPublishedRelease, "publishedAt"):
            judge.bind_published(self.current, self.records(incomplete), NOW)
        with self.assertRaisesRegex(ops.MissingPublishedRelease, "future"):
            judge.bind_published(self.current, self.records(record(self.current, publishedAt="2099-01-01T00:00:00Z")), NOW)
        with self.assertRaisesRegex(ops.MissingPublishedRelease, "source.type"):
            judge.bind_published(self.current, self.records(record(self.current, source={"type": "local-build"})), NOW)

    def test_registry_tag_source_must_resolve_to_the_digest(self):
        source = {"type": "registry-tag", "reference": f"{REPOSITORY}:1.2.0"}
        entry = self.records(record(self.current, source=source))
        data = manifest_bytes("1.2.0")
        self.assertEqual(self.current["digest"], digest_of(data))
        self.assertEqual(judge.bind_published(self.current, entry, NOW, data)["source"], source)
        with self.assertRaisesRegex(ops.MissingPublishedRelease, "not resolved"):
            judge.bind_published(self.current, entry, NOW, None)
        with self.assertRaisesRegex(ops.MissingPublishedRelease, "does not resolve"):
            judge.bind_published(self.current, entry, NOW, manifest_bytes("other"))

    def test_pair_rules(self):
        self.assertEqual(judge.judge_release_pair(self.previous, self.current), [])
        synthetic = judge.judge_release_pair(image("0.0.0"), image("0.0.1"))
        self.assertEqual(sum("synthetic" in p for p in synthetic), 2)
        self.assertTrue(any("later release" in p for p in judge.judge_release_pair(self.current, self.previous)))
        self.assertTrue(any("window" in p for p in judge.judge_release_pair(image("1.0.0"), image("1.2.0"))))
        self.assertTrue(any("window" in p for p in judge.judge_release_pair(image("1.2.0"), image("2.3.0"))))
        same = dict(self.current, version="1.3.0")
        self.assertTrue(any("same image digest" in p for p in judge.judge_release_pair(self.current, same)))
        other_repo = dict(self.current, repository="other/narumq")
        self.assertTrue(any("repositories" in p for p in judge.judge_release_pair(self.previous, other_repo)))

    def test_active_features_must_be_supported_by_both_releases(self):
        self.assertEqual(judge.check_active_features(["session-metadata"], self.previous, self.current), [])
        missing = judge.check_active_features(["proposal-recovery"], self.previous, self.current)
        self.assertEqual(len(missing), 2)

    def test_prepare_release_pair_end_to_end_with_fake_registry(self):
        previous_bytes, current_bytes = manifest_bytes("1.1.0"), manifest_bytes("1.2.0")
        registry = FakeRegistry({digest_of(previous_bytes): (previous_bytes, config_for("1.1.0")),
                                 digest_of(current_bytes): (current_bytes, config_for("1.2.0"))})
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "record.json"
            path.write_text(json.dumps(self.records(record(self.previous), record(self.current))), encoding="utf-8")
            args = argparse.Namespace(release_record=path, image_n_1=self.previous["reference"], image_n=self.current["reference"])
            pair = release_main.prepare_release_pair(args, registry, NOW)
            self.assertEqual((pair["previous"]["version"], pair["current"]["version"]), ("1.1.0", "1.2.0"))
            path.write_text(json.dumps(self.records(record(self.current))), encoding="utf-8")
            with self.assertRaisesRegex(ops.MissingPublishedRelease, "1.1.0"):
                release_main.prepare_release_pair(args, registry, NOW)

    def test_scenario_fails_without_release_record_before_touching_a_cluster(self):
        def no_cluster():
            raise AssertionError("the fixture must not be built before the published-release prerequisite holds")
        args = argparse.Namespace(release_record=None, image_n_1=self.previous["reference"], image_n=self.current["reference"],
                                  registry_tool="docker", token="", registry_secret="s", management_secret="m",
                                  helper_image="", timeout=1.0)
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {"NARUMQ_SCENARIO_RESULTS": str(Path(directory) / "r.jsonl")}):
            with patch.object(ops.RegistryClient, "__init__", lambda self, tool="auto": None):
                with self.assertRaises(ops.MissingPublishedRelease):
                    release_main.scenario_release_window(args, no_cluster)
            lines = (Path(directory) / "r.jsonl").read_text(encoding="utf-8").splitlines()
            self.assertEqual(json.loads(lines[-1])["status"], "fail")


class FixtureStartTest(unittest.TestCase):
    def test_start_state_requires_n_minus_1_everywhere(self):
        previous = image("1.1.0")
        self.assertEqual(judge.judge_start_state(members("1.1.0", FEATURES), pods(previous, "u"), previous), [])
        wrong_member = members("1.1.0", FEATURES)
        wrong_member[1]["releaseVersion"] = "1.2.0"
        self.assertTrue(judge.judge_start_state(wrong_member, pods(previous, "u"), previous))
        other = pods(previous, "u")
        other["narumq-2"]["imageID"] = f"{REPOSITORY}@sha256:" + "9" * 64
        self.assertTrue(judge.judge_start_state(members("1.1.0", FEATURES), other, previous))


class BarrierJudgmentTest(unittest.TestCase):
    def setUp(self):
        self.previous, self.current = image("1.1.0"), image("1.2.0")
        self.up = good_direction("upgrade", self.current, "a", "b")
        self.down = good_direction("rollback", self.previous, "b", "c")

    def test_both_directions_pass_and_compare_equal(self):
        self.assertTrue(self.up["passed"], self.up["problems"])
        self.assertTrue(self.down["passed"], self.down["problems"])
        self.assertEqual(judge.compare_directions(self.up, self.down), [])
        self.assertTrue(judge.judge_release_window(self.previous, self.current, self.up, self.down)["passed"])

    def run_direction(self, **overrides) -> dict:
        target = self.current
        before, after = pods(self.previous, "a"), pods(target, "b")
        approvals = overrides.pop("approvals", [approval(o, f"a{o}", target) for o in range(3)])
        status = overrides.pop("status", {"phase": "Completed", "completedOrdinals": [0, 1, 2]})
        features = overrides.pop("features", {"members": members(target["version"], target["features"])})
        quorum = overrides.pop("quorum", [{"quorumAvailable": True}] * 4)
        after = overrides.pop("after", after)
        return judge.judge_direction("upgrade", target, before, after, status, approvals, features, quorum)

    def test_barrier_condition_violations_fail(self):
        for field, value in (("connectionsRemaining", 1), ("pendingWAL", 2), ("underReplicated", 1), ("sessionsFailed", 3),
                             ("drainSafeToStop", False), ("drainState", "DRAINING"), ("drainPhase", "DRAINING"),
                             ("blockingReason", "pending_wal")):
            approvals = [approval(o, f"a{o}", self.current) for o in range(3)]
            approvals[1] = approval(1, "a1", self.current, **{field: value})
            verdict = self.run_direction(approvals=approvals)
            self.assertFalse(verdict["passed"], field)
            self.assertTrue(any("narumq-1" in p for p in verdict["problems"]), field)

    def test_approval_before_safe_to_stop_observation_fails(self):
        approvals = [approval(o, f"a{o}", self.current) for o in range(3)]
        approvals[0]["occurredAt"] = "2026-10-16T23:59:59Z"
        self.assertTrue(any("approvedAfterSafeToStop" in p for p in self.run_direction(approvals=approvals)["problems"]))

    def test_missing_evidence_field_and_wrong_pod_fail(self):
        approvals = [approval(o, f"a{o}", self.current) for o in range(3)]
        del approvals[2]["details"]["pendingWAL"]
        problems = self.run_direction(approvals=approvals)["problems"]
        self.assertTrue(any("lacks evidence fields" in p for p in problems))
        self.assertTrue(any("pendingWAL0" in p for p in problems))
        approvals = [approval(o, f"a{o}", self.current) for o in range(3)]
        approvals[0]["details"]["podUid"] = "someone-else"
        self.assertTrue(any("Pod UID" in p for p in self.run_direction(approvals=approvals)["problems"]))

    def test_wrong_approval_count_and_wrong_target(self):
        self.assertTrue(any("2 delete approvals" in p for p in self.run_direction(approvals=[approval(o, f"a{o}", self.current) for o in range(2)])["problems"]))
        wrong = [approval(o, f"a{o}", self.previous) for o in range(3)]
        self.assertTrue(any("names target" in p for p in self.run_direction(approvals=wrong)["problems"]))

    def test_unreplaced_not_ready_and_failed_rollout(self):
        after = pods(self.current, "b")
        after["narumq-1"]["uid"] = "a1"
        self.assertTrue(any("not replaced" in p for p in self.run_direction(after=after)["problems"]))
        after = pods(self.current, "b")
        after["narumq-2"]["ready"] = False
        self.assertTrue(any("not Ready" in p for p in self.run_direction(after=after)["problems"]))
        failed = {"phase": "Failed", "completedOrdinals": [0], "failureCode": "drain_timeout"}
        self.assertTrue(any("Failed" in p for p in self.run_direction(status=failed)["problems"]))

    def test_features_and_quorum_conditions(self):
        wrong = {"members": members("1.1.0", self.current["features"])}
        self.assertTrue(any("cluster features" in p for p in self.run_direction(features=wrong)["problems"]))
        quorum = [{"quorumAvailable": True}, {"quorumAvailable": False}, {"quorumAvailable": None}, {"quorumAvailable": True}]
        self.assertTrue(any("quorum was not reported available" in p for p in self.run_direction(quorum=quorum)["problems"]))
        self.assertTrue(any("quorum samples" in p for p in self.run_direction(quorum=[{"quorumAvailable": True}])["problems"]))

    def test_directions_with_different_barriers_do_not_match(self):
        extra = good_direction("rollback", self.previous, "b", "c")
        extra["evidenceKeys"] = extra["evidenceKeys"] + ["extra"]
        self.assertTrue(any("evidence field sets differ" in p for p in judge.compare_directions(self.up, extra)))
        reordered = copy.deepcopy(self.down)
        reordered["ordinalOrder"] = [2, 1, 0]
        self.assertTrue(any("order differs" in p for p in judge.compare_directions(self.up, reordered)))
        broken = copy.deepcopy(self.down)
        broken["conditions"][0]["pendingWAL0"] = False
        self.assertTrue(any("pendingWAL0" in p for p in judge.compare_directions(self.up, broken)))
        self.assertTrue(any("evidence field sets differ" in p for p in judge.compare_directions(self.up, {**self.down, "evidenceKeys": []})))

    def test_approval_time_preserves_nanosecond_order_and_accepts_short_fractions(self):
        event = approval(0, "a0", self.current)
        event["occurredAt"] = "2026-10-17T00:00:00.123456788Z"
        self.assertFalse(judge.approval_conditions(event)["approvedAfterSafeToStop"])
        event["occurredAt"] = "2026-10-17T00:00:00.123456789Z"
        self.assertTrue(judge.approval_conditions(event)["approvedAfterSafeToStop"])
        event["occurredAt"] = "2026-10-17T00:00:00.5Z"
        self.assertTrue(judge.approval_conditions(event)["approvedAfterSafeToStop"])
        event["occurredAt"] = "2026-10-17T01:00:00.123456788+01:00"
        self.assertFalse(judge.approval_conditions(event)["approvedAfterSafeToStop"])
        event["occurredAt"] = "2026-10-17T00:00:01"
        self.assertFalse(judge.approval_conditions(event)["approvedAfterSafeToStop"])


if __name__ == "__main__":
    unittest.main()
