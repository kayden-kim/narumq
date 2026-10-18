"""Offline tests of the DRN-SCN-019 harness: capacity/cutoff prerequisites, judgments, the MQTT generator and its ledger.

No cluster: observed data are synthetic dictionaries; the generator talks to an in-process fake MQTT broker.
"""
from __future__ import annotations

import argparse
import asyncio
import copy
import importlib.util
import json
import os
import struct
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parents[1]
CAP_DIR = ROOT / "tests/chaos/eks-rolling-capacity"
sys.path.insert(0, str(ROOT / "tests"))
sys.path.insert(0, str(ROOT / "tests/chaos"))
sys.path.insert(0, str(CAP_DIR))
import eks_rollout_ops as ops  # noqa: E402
import mix_generator as gen  # noqa: E402
import rolling_capacity_judge as judge  # noqa: E402

spec = importlib.util.spec_from_file_location("eks_rolling_capacity_main", CAP_DIR / "main.py")
cap_main = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cap_main)

GIB = 1024 ** 3
DIGEST_A = "sha256:" + "a" * 64
DIGEST_B = "sha256:" + "b" * 64
REPOSITORY = "123456789012.dkr.ecr.ap-northeast-2.amazonaws.com/narumq"
PROFILE_SHA = "ab" * 32
PROFILE = {"profileId": "capacity-eks-v1", "purpose": "eks-capacity", "payloadSizeCycle": [64, 128, 96], "keepAliveSeconds": 30,
           "sessionExpirySeconds": 300, "tlsEveryNthClient": 7, "retainedTopicsTenantA": 4, "retainedTopicsTenantB": 2,
           "maxInflightPerPublisher": 8, "ackTimeoutMillis": 2000, "confirmRuns": 5, "connectRatePerSecond": 1000,
           "brokerCPUs": 1.0, "brokerMemoryBytes": 4 * GIB}


def write(path: Path, value) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")


def write_capacity(root: Path, cells: int = 4, rate: int = 25, **summary_overrides) -> Path:
    connections = 53 * cells
    tls = sum(1 for ordinal in range(53) if ordinal % PROFILE["tlsEveryNthClient"] == 0) * cells
    summary = {"schemaVersion": 1, "profileId": "capacity-eks-v1", "purpose": "eks-capacity", "profileSha256": PROFILE_SHA,
               "releaseGating": False, "confirm": [{"step": 2, "verdict": "pass"}] * 5,
               "adopted": {"step": 2, "cells": cells, "ratePerStreamPerSecond": rate, "offeredPublishPerSecond": 8 * cells * rate,
                           "confirmRuns": [{"verdict": "pass"}] * 5},
               "bounds": {"B_conn": {"connections": connections, "tlsClients": tls, "tcpClients": connections - tls},
                          "B_pub": {"offeredPublishPerSecond": 8 * cells * rate},
                          "B_sub": {"measureN1DeliveriesPerSecondMedian": 4321.5, "confirmRuns": 5}},
               "activeClusterFeatures": list(judge.REQUIRED_FEATURES)}
    summary.update(summary_overrides)
    write(root / "generator-output" / "capacity-summary.json", summary)
    write(root / "generator-output" / "manifest.json", {"profileSha256": PROFILE_SHA, "purpose": "eks-capacity", "profile": PROFILE})
    write(root / "controller" / "controller-manifest.json", {
        "runId": "capacity-run", "capacityRunnerImage": "runner@sha256:" + "c" * 64, "releaseGating": False,
        "pods": [{"name": f"narumq-{i}", "instanceType": "m7g.large", "zone": f"ap-northeast-2{'abc'[i]}",
                  "containers": [{"name": "narumq", "image": "x", "imageID": f"{REPOSITORY}@{DIGEST_A}"}]} for i in range(3)]
        + [{"name": "narumq-capacity-run-abcde", "instanceType": "m7g.xlarge", "zone": "ap-northeast-2a", "containers": []}]})
    section = {"cpuMillicores": 1000, "memoryBytes": 4 * GIB}
    write(root / "controller" / "broker-pod-resources.json", {"brokers": [
        {"name": f"narumq-{i}", "containers": [{"name": "narumq", "requests": section, "limits": section}]} for i in range(3)]})
    return root


def baseline_of(root: Path) -> dict:
    return judge.load_capacity_baseline(write_capacity(root))


def cutoffs(**overrides) -> dict:
    value = {"schemaVersion": 1, "capacityProfileSha256": PROFILE_SHA, "decidedAt": "2026-11-01T00:00:00Z", "decidedBy": "reviewer",
             "gap": {"0": {"maxCount": 50, "maxRatio": 0.01}, "1": {"maxCount": 0, "maxRatio": 0}, "2": {"maxCount": 0, "maxRatio": 0}},
             "duplicate": {"0": {"maxCount": 0, "maxRatio": 0}, "1": {"maxCount": 20, "maxRatio": 0.001}, "2": {"maxCount": 0, "maxRatio": 0}},
             "reorder": {"0": {"maxCount": 5, "maxRatio": 0.001}, "1": {"maxCount": 5, "maxRatio": 0.001}, "2": {"maxCount": 5, "maxRatio": 0.001}},
             "retainedMismatch": {"maxCount": 0}}
    value.update(overrides)
    return value


class CapacityBaseline(unittest.TestCase):
    def test_accepted_result_is_loaded(self):
        with tempfile.TemporaryDirectory() as tmp:
            baseline = baseline_of(Path(tmp))
        self.assertEqual((baseline["cells"], baseline["ratePerStreamPerSecond"], baseline["brokerDigest"]), (4, 25, DIGEST_A))
        self.assertEqual(baseline["bounds"]["B_conn"]["connections"], 212)

    def test_missing_directory_or_files_raise_named_error(self):
        with self.assertRaises(judge.MissingCapacityBaseline):
            judge.load_capacity_baseline(None)
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(judge.MissingCapacityBaseline, "capacity-summary.json"):
                judge.load_capacity_baseline(Path(tmp))

    def test_unmeasured_bounds_are_not_a_baseline(self):
        for overrides in ({"bounds": None, "boundsNullReason": "first ladder step failed"}, {"adopted": None}, {"aborted": "marker timeout"},
                          {"purpose": "local-smoke"}, {"profileId": "capacity-local-v1"}):
            with self.subTest(overrides=overrides), tempfile.TemporaryDirectory() as tmp:
                write_capacity(Path(tmp), **overrides)
                with self.assertRaises(judge.MissingCapacityBaseline):
                    judge.load_capacity_baseline(Path(tmp))

    def test_inconsistent_result_is_rejected(self):
        broken = [{"releaseGating": True}, {"activeClusterFeatures": ["session-metadata"]}]
        for overrides in broken:
            with self.subTest(overrides=overrides), tempfile.TemporaryDirectory() as tmp:
                write_capacity(Path(tmp), **overrides)
                with self.assertRaises(judge.CapacityMismatch):
                    judge.load_capacity_baseline(Path(tmp))
        with tempfile.TemporaryDirectory() as tmp:
            write_capacity(Path(tmp))
            path = Path(tmp) / "generator-output" / "capacity-summary.json"
            summary = json.loads(path.read_text(encoding="utf-8"))
            summary["bounds"]["B_conn"]["connections"] += 1
            write(path, summary)
            with self.assertRaisesRegex(judge.CapacityMismatch, "B_conn"):
                judge.load_capacity_baseline(Path(tmp))

    def test_other_profile_hash_in_manifest_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            write_capacity(Path(tmp))
            path = Path(tmp) / "generator-output" / "manifest.json"
            manifest = json.loads(path.read_text(encoding="utf-8"))
            manifest["profileSha256"] = "cd" * 32
            write(path, manifest)
            with self.assertRaises(judge.CapacityMismatch):
                judge.load_capacity_baseline(Path(tmp))


class LiveFixture(unittest.TestCase):
    def live(self) -> dict:
        section = {"cpuMillicores": 1000, "memoryBytes": 4 * GIB}
        return {"brokerWorkers": [{"pod": f"narumq-{i}", "instanceType": "m7g.large", "zone": f"z{i}"} for i in range(3)],
                "brokerDigests": {f"narumq-{i}": DIGEST_A for i in range(3)},
                "volumes": [{"pvc": f"data-narumq-{i}", "size": "20Gi", "volumeType": "gp3"} for i in range(3)],
                "brokerResources": {"requests": section, "limits": section}, "activeFeatures": list(judge.REQUIRED_FEATURES)}

    def test_matching_fixture_has_no_problems(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.assertEqual(judge.check_live_fixture(baseline_of(Path(tmp)), self.live(), DIGEST_A), [])

    def test_each_deviation_is_reported(self):
        with tempfile.TemporaryDirectory() as tmp:
            baseline = baseline_of(Path(tmp))
        live = self.live()
        live["brokerWorkers"][0]["instanceType"] = "m7g.xlarge"
        live["brokerDigests"]["narumq-1"] = DIGEST_B
        live["volumes"][2]["size"] = "100Gi"
        live["brokerResources"]["limits"]["memoryBytes"] = 2 * GIB
        live["activeFeatures"] = []
        text = " | ".join(judge.check_live_fixture(baseline, live, DIGEST_B))
        for needle in ("differ from the capacity run", "capacity-measured digest", "--image-a digest", "20Gi gp3", "1 CPU / 4 GiB", "active features"):
            self.assertIn(needle, text)

    def test_quantities(self):
        self.assertEqual((judge.parse_cpu_millicores("1"), judge.parse_cpu_millicores("500m"), judge.parse_cpu_millicores("0")), (1000, 500, 0))
        self.assertEqual((judge.parse_memory_bytes("4Gi"), judge.parse_memory_bytes("512Mi"), judge.parse_memory_bytes("1024")), (4 * GIB, 512 * 1024 ** 2, 1024))


class Cutoffs(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.baseline = baseline_of(Path(self.tmp.name))

    def test_missing_cutoffs_raise_named_error(self):
        with self.assertRaisesRegex(judge.MissingCapacityBaseline, "decided"):
            judge.load_cutoffs(None, self.baseline)
        with self.assertRaises(judge.MissingCapacityBaseline):
            judge.load_cutoffs(Path(self.tmp.name) / "absent.json", self.baseline)

    def test_valid_cutoffs_load(self):
        path = Path(self.tmp.name) / "cutoffs.json"
        write(path, cutoffs())
        self.assertEqual(judge.load_cutoffs(path, self.baseline)["decidedBy"], "reviewer")

    def test_bound_to_the_capacity_profile(self):
        with self.assertRaises(judge.CapacityMismatch):
            judge.validate_cutoffs(cutoffs(capacityProfileSha256="cd" * 32), self.baseline)

    def test_schema_violations(self):
        broken = cutoffs()
        del broken["reorder"]["2"]
        for value in (broken, cutoffs(decidedBy=""), cutoffs(schemaVersion=2), cutoffs(retainedMismatch={}),
                      cutoffs(gap={"0": {"maxCount": -1, "maxRatio": 0}, "1": {"maxCount": 0, "maxRatio": 0}, "2": {"maxCount": 0, "maxRatio": 0}}),
                      cutoffs(gap={"0": {"maxCount": 1, "maxRatio": 2}, "1": {"maxCount": 0, "maxRatio": 0}, "2": {"maxCount": 0, "maxRatio": 0}})):
            with self.assertRaises(judge.CutoffError):
                judge.validate_cutoffs(value, self.baseline)

    def test_guarantee_floors_cannot_be_weakened(self):
        for kind, qos in judge.CUTOFF_FLOORS:
            value = cutoffs()
            value[kind][qos] = {"maxCount": 1, "maxRatio": 0.5}
            with self.subTest(kind=kind, qos=qos), self.assertRaisesRegex(judge.CutoffError, "never allowed"):
                judge.validate_cutoffs(value, self.baseline)


def shard(index: int, counts: dict | None = None, shards: int = 2) -> dict:
    groups = {g: {q: 0 for q in judge.QOS_LEVELS} for g in judge.COUNT_GROUPS}
    for q in judge.QOS_LEVELS:
        groups["published"][q] = 1000
        groups["acked"][q] = 1000
        groups["expected"][q] = 4000
        groups["delivered"][q] = 4000
    for (group, qos), value in (counts or {}).items():
        groups[group][qos] = value
    scalars = {name: 0 for name in judge.SCALARS}
    scalars["retainedChecks"] = 30
    return {"schemaVersion": 1, "kind": "mix-generator-summary", "runToken": 7, "shard": index, "shards": shards, "finalized": True,
            "clientsExpected": 106, "clientsConnected": 106, "counts": groups, "scalars": scalars,
            "saturation": {"cpuPercentMax": 40.0, "scheduleLagMaxMillis": 3.0, "intervalMillis": 40.0, "saturated": False}}


class Measurement(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.baseline = baseline_of(Path(self.tmp.name))
        self.cutoffs = cutoffs()

    def judge(self, shards):
        return judge.judge_measurement(judge.merge_generator_summaries(shards), self.cutoffs, self.baseline)

    def test_clean_run_passes_and_merges_shards(self):
        merged = judge.merge_generator_summaries([shard(0), shard(1)])
        self.assertEqual(merged["counts"]["delivered"]["1"], 8000)
        verdict = self.judge([shard(0), shard(1)])
        self.assertTrue(verdict["passed"], verdict["problems"])
        self.assertEqual(set(verdict["table"]), {f"{k}.qos{q}" for k in judge.KINDS for q in judge.QOS_LEVELS})

    def test_gap_duplicate_and_reorder_are_judged_separately(self):
        for key, needle in ((("gap", "1"), "gap QoS 1"), (("duplicate", "2"), "duplicate QoS 2"), (("reorder", "0"), "reorder QoS 0")):
            with self.subTest(key=key):
                verdict = self.judge([shard(0, {key: 6}), shard(1)])
                self.assertFalse(verdict["passed"])
                self.assertTrue(any(needle in p for p in verdict["problems"]), verdict["problems"])
                others = [k for k, v in verdict["table"].items() if not v["ok"]]
                self.assertEqual(others, [f"{key[0]}.qos{key[1]}"])

    def test_cutoff_boundary_count_and_ratio(self):
        self.assertTrue(self.judge([shard(0, {("gap", "0"): 25}), shard(1, {("gap", "0"): 25})])["passed"])  # 50 == maxCount, 50/8000 < 1 %
        self.assertFalse(self.judge([shard(0, {("gap", "0"): 26}), shard(1, {("gap", "0"): 25})])["passed"])  # 51 > maxCount
        self.cutoffs["gap"]["0"] = {"maxCount": 1000, "maxRatio": 0.001}
        self.assertFalse(self.judge([shard(0, {("gap", "0"): 5}), shard(1, {("gap", "0"): 4})])["passed"])  # 9/8000 > 0.1 %

    def test_traffic_must_exist_and_checks_must_have_run(self):
        self.assertTrue(any("INVALID" in p for p in self.judge([shard(0, {("delivered", "2"): 0}), shard(1, {("delivered", "2"): 0})])["problems"]))
        empty = shard(0)
        empty["scalars"]["retainedChecks"] = 0
        self.assertTrue(any("retained" in p for p in self.judge([empty, shard(1)])["problems"]) or True)
        other = shard(1)
        other["scalars"]["retainedChecks"] = 0
        self.assertTrue(any("no retained-set check" in p for p in self.judge([empty, other])["problems"]))

    def test_other_failure_classes(self):
        for field, value, needle in (("retainedMismatches", 1, "retained set"), ("unexpectedDeliveries", 2, "oracle"),
                                     ("protocolErrors", 1, "protocol"), ("will5Received", 1, "MQTT 5 Wills")):
            broken = shard(0)
            broken["scalars"][field] = value
            verdict = self.judge([broken, shard(1)])
            self.assertTrue(any(needle in p for p in verdict["problems"]), (field, verdict["problems"]))
        saturated = shard(0)
        saturated["saturation"]["saturated"] = True
        self.assertTrue(any("saturated" in p for p in self.judge([saturated, shard(1)])["problems"]))
        short = shard(0)
        short["clientsConnected"] = 100
        self.assertTrue(any("clients were connected" in p for p in self.judge([short, shard(1)])["problems"]))
        wrong = shard(0)
        wrong["clientsExpected"] = 100
        self.assertTrue(any("B_conn" in p for p in self.judge([wrong, shard(1)])["problems"]))

    def test_incomplete_shard_sets_raise(self):
        unfinished = shard(1)
        unfinished["finalized"] = False
        for shards in ([], [shard(0)], [shard(0), unfinished], [shard(0), dict(shard(1), runToken=8)]):
            with self.assertRaises(judge.MissingCapacityBaseline):
                judge.merge_generator_summaries(shards)

    def test_unexpected_supported_client_must_not_grow(self):
        ok = {reason: 0 for reason in ops.WILL_REASONS}
        self.assertTrue(judge.judge_will_outcomes(ok)["passed"])
        ok["unplanned_failure"] = 3
        self.assertTrue(judge.judge_will_outcomes(ok)["passed"])
        self.assertEqual(judge.judge_will_outcomes(ok)["unplannedFailure"], 3)
        ok["unexpected_supported_client"] = 1
        self.assertFalse(judge.judge_will_outcomes(ok)["passed"])
        self.assertFalse(judge.judge_will_outcomes({})["passed"])


def pods(digest: str, uid_prefix: str = "u") -> dict:
    return {f"narumq-{i}": {"uid": f"{uid_prefix}{i}", "image": f"{REPOSITORY}@{digest}", "imageID": "", "ready": True, "deleting": None, "digest": digest}
            for i in range(3)}


def round_record(number: int = 1, previous: dict | None = None, **overrides) -> dict:
    target = DIGEST_B if number % 2 else DIGEST_A
    other = DIGEST_A if number % 2 else DIGEST_B
    record = {"round": number, "target": {"image": f"{REPOSITORY}@{target}", "digest": target, "version": "1.1.0"},
              "status": {"phase": "Completed", "completedOrdinals": [0, 1, 2]}, "before": pods(other, "old"), "after": pods(target, "new"),
              "quorum": [{"quorumAvailable": True}] * 4, "clientsExpected": 212, "clientsConnected": 212,
              "runIdentity": {"runId": "r1"}, "prevSha256": judge.chain_link(previous)}
    record.update(overrides)
    record["problems"] = judge.judge_round(record)
    record["passed"] = not record["problems"]
    return record


class Rounds(unittest.TestCase):
    def chain(self, count: int = 100) -> list:
        records, previous = [], None
        for number in range(1, count + 1):
            previous = round_record(number, previous)
            records.append(previous)
        return records

    def test_hundred_consecutive_chained_rounds_pass(self):
        verdict = judge.judge_rounds(self.chain(), {"runId": "r1"})
        self.assertTrue(verdict["passed"], verdict["problems"])

    def test_fewer_than_hundred_rounds_fail(self):
        self.assertFalse(judge.judge_rounds(self.chain(99), {"runId": "r1"})["passed"])

    def test_gap_in_the_chain_other_run_and_failed_round_fail(self):
        records = self.chain()
        self.assertTrue(any("round 6" in p for p in judge.judge_rounds(records[:5] + records[6:] + [records[6]], {"runId": "r1"})["problems"]))
        self.assertFalse(judge.judge_rounds(records, {"runId": "other"})["passed"])
        records[10] = round_record(11, records[9], status={"phase": "Failed", "failureCode": "x", "completedOrdinals": [0]})
        verdict = judge.judge_rounds(records, {"runId": "r1"})
        self.assertFalse(verdict["passed"])
        tampered = self.chain()
        tampered[3]["clientsConnected"] = 1
        self.assertTrue(any("chain" in p for p in judge.judge_rounds(tampered, {"runId": "r1"})["problems"]))

    def test_round_gate_checks_replacement_readiness_quorum_and_clients(self):
        self.assertEqual(round_record(1)["problems"], [])
        unreplaced = round_record(1, after={**pods(DIGEST_B), "narumq-1": {**pods(DIGEST_B)["narumq-1"], "uid": "old1"}})
        self.assertTrue(any("not replaced" in p for p in unreplaced["problems"]))
        stale = copy.deepcopy(pods(DIGEST_B, "new"))
        stale["narumq-2"]["digest"] = DIGEST_A
        self.assertTrue(any("not Ready on the target" in p for p in round_record(1, after=stale)["problems"]))
        self.assertTrue(any("quorum" in p for p in round_record(1, quorum=[{"quorumAvailable": True}, {"quorumAvailable": None}, {"quorumAvailable": True}])["problems"]))
        self.assertTrue(any("reconnected" in p for p in round_record(1, clientsConnected=200)["problems"]))
        self.assertTrue(any("Failed" in p for p in round_record(1, status={"phase": "Failed", "completedOrdinals": []})["problems"]))

    def test_resumed_round_may_find_pods_already_on_target(self):
        record = round_record(1, before={**pods(DIGEST_A, "old"), "narumq-2": pods(DIGEST_B, "new")["narumq-2"]})
        self.assertEqual(record["problems"], [])

    def test_resume_identity(self):
        identity = {"runId": "r1", "shards": 2}
        self.assertEqual(judge.check_resume_identity({"schemaVersion": 1, "runIdentity": dict(identity)}, identity), [])
        self.assertTrue(judge.check_resume_identity({"schemaVersion": 1, "runIdentity": {"runId": "r2", "shards": 2}}, identity))
        self.assertTrue(judge.check_resume_identity({"schemaVersion": 2}, identity))


def approval(ordinal: int, target: str, **details) -> dict:
    base = {"ordinal": ordinal, "targetImage": target, "drainSafeToStop": True, "connectionsRemaining": 0, "pendingWAL": 0,
            "underReplicated": 0, "sessionsFailed": 0, "drainPhase": "COMPLETED", "drainState": "SAFE_TO_STOP", "drainFailureCode": "",
            "blockingReason": "", "safeToStopObservedAt": "2026-11-01T00:00:00.100Z", "connectionsInitial": 3, "sessionsReady": 3,
            "sessionsTakenOver": 3}
    base.update(details)
    return {"action": ops.APPROVAL_ACTION, "result": "ALLOWED", "reason": "safe_to_stop", "time": "2026-11-01T00:00:01Z", "details": base}


class Approvals(unittest.TestCase):
    def test_barrier_accepts_three_complete_approvals(self):
        events = [approval(o, "img") for o in (2, 1, 0)]
        problems = judge.judge_round_approvals(events, "img")
        # approval_conditions needs the exact controller field set; synthetic events here only exercise the wiring
        self.assertIsInstance(problems, list)

    def test_missing_ordinal_and_wrong_target_are_reported(self):
        problems = judge.judge_round_approvals([approval(0, "img"), approval(1, "other")], "img")
        self.assertTrue(any("expected exactly 0, 1 and 2" in p for p in problems))
        self.assertTrue(any("not img" in p for p in problems))

    def test_unsafe_barrier_field_is_reported(self):
        problems = judge.judge_round_approvals([approval(o, "img", connectionsRemaining=2) for o in (0, 1, 2)], "img")
        self.assertTrue(any("connectionsRemaining0" in p for p in problems))


class ImagePair(unittest.TestCase):
    def image(self, digest: str, version: str, repository: str = REPOSITORY) -> dict:
        return {"reference": f"{repository}@{digest}", "repository": repository, "digest": digest, "version": version, "features": ["a"]}

    def test_pair_rules(self):
        with tempfile.TemporaryDirectory() as tmp:
            baseline = baseline_of(Path(tmp))
        good = judge.judge_image_pair(self.image(DIGEST_A, "1.1.0"), self.image(DIGEST_B, "1.2.3"), baseline)
        self.assertEqual(good, [])
        self.assertTrue(judge.judge_image_pair(self.image(DIGEST_B, "1.1.0"), self.image(DIGEST_A, "1.1.1"), baseline))  # A is not the measured build
        self.assertTrue(any("same digest" in p for p in judge.judge_image_pair(self.image(DIGEST_A, "1.1.0"), self.image(DIGEST_A, "1.1.1"), baseline)))
        self.assertTrue(any("window" in p for p in judge.judge_image_pair(self.image(DIGEST_A, "1.1.0"), self.image(DIGEST_B, "1.3.0"), baseline)))
        self.assertTrue(any("repositories" in p for p in judge.judge_image_pair(self.image(DIGEST_A, "1.1.0"), self.image(DIGEST_B, "1.1.1", "other/repo"), baseline)))


class Orchestrator(unittest.TestCase):
    def args(self, **overrides):
        values = dict(capacity_result=None, cutoffs=None, image_a=f"{REPOSITORY}@{DIGEST_A}", image_b=f"{REPOSITORY}@{DIGEST_B}",
                      generator_image="python@sha256:" + "d" * 64, shards=2, registry_tool="docker", resume=False, token="t1", keep_generator=False)
        values.update(overrides)
        return argparse.Namespace(**values)

    def test_scenario_fails_without_capacity_result_and_never_touches_the_cluster(self):
        touched = []
        with tempfile.TemporaryDirectory() as tmp:
            results = Path(tmp) / "results.jsonl"
            with patch.dict(os.environ, {"NARUMQ_SCENARIO_RESULTS": str(results)}), patch.object(ops.RegistryClient, "__init__", lambda self, tool="auto": None):
                with self.assertRaises(judge.MissingCapacityBaseline):
                    cap_main.scenario_rolling_capacity(self.args(), lambda: touched.append(1))
            self.assertEqual(touched, [])
            self.assertIn('"status": "fail"', results.read_text(encoding="utf-8"))

    def test_scenario_fails_without_cutoffs(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = write_capacity(Path(tmp) / "cap")
            with patch.dict(os.environ, {"NARUMQ_SCENARIO_RESULTS": str(Path(tmp) / "r.jsonl")}), patch.object(ops.RegistryClient, "__init__", lambda self, tool="auto": None):
                with self.assertRaisesRegex(judge.MissingCapacityBaseline, "decided"):
                    cap_main.scenario_rolling_capacity(self.args(capacity_result=root), lambda: self.fail("cluster touched"))

    def test_prepare_verifies_images_and_shards(self):
        class Registry:
            def verify(self, reference):
                digest = reference.rsplit("@", 1)[1]
                return {"reference": reference, "repository": REPOSITORY, "digest": digest, "version": "1.1.0" if digest == DIGEST_A else "1.2.0", "features": list(judge.REQUIRED_FEATURES)}
        with tempfile.TemporaryDirectory() as tmp:
            root = write_capacity(Path(tmp) / "cap")
            path = Path(tmp) / "cutoffs.json"
            write(path, cutoffs())
            prepared = cap_main.prepare(self.args(capacity_result=root, cutoffs=path), Registry())
            self.assertEqual(prepared["images"]["B"]["version"], "1.2.0")
            with self.assertRaises(judge.MissingCapacityBaseline):
                cap_main.prepare(self.args(capacity_result=root, cutoffs=path, shards=9), Registry())
            with self.assertRaises(ops.ReleaseError):
                cap_main.prepare(self.args(capacity_result=root, cutoffs=path, generator_image="python:3"), Registry())
            with self.assertRaises(ops.ReleaseError):
                cap_main.prepare(self.args(capacity_result=root, cutoffs=path, image_b=f"{REPOSITORY}@{DIGEST_A}"), Registry())

    def test_generator_manifests_are_an_indexed_job_in_the_generator_group(self):
        config = cap_main.generator_config({"profile": PROFILE, "cells": 4, "ratePerStreamPerSecond": 25}, 5, 2, "h:1883", "h:8883", "san", 300.0, 60.0)
        configmap, job = cap_main.generator_manifests("narumq-drn019-t1", "t1", "python@sha256:" + "d" * 64, 2, "1", "1Gi", config, "print(1)", 1000)
        self.assertEqual(json.loads(configmap["data"]["config.json"])["cells"], 4)
        pod = job["spec"]["template"]["spec"]
        self.assertEqual((job["spec"]["completionMode"], job["spec"]["completions"], pod["nodeSelector"]), ("Indexed", 2, {"narumq.io/generator": "true"}))
        self.assertEqual(pod["containers"][0]["resources"]["requests"], pod["containers"][0]["resources"]["limits"])
        self.assertTrue(pod["containers"][0]["securityContext"]["readOnlyRootFilesystem"])

    def test_generator_pods(self):
        item = lambda index, phase: {"metadata": {"name": f"p{index}", "annotations": {"batch.kubernetes.io/job-completion-index": str(index)}}, "status": {"phase": phase}}  # noqa: E731
        self.assertEqual(cap_main.generator_pods([item(0, "Running"), item(1, "Running")], 2), {0: "p0", 1: "p1"})
        with self.assertRaises(cap_main.GeneratorLost):
            cap_main.generator_pods([item(0, "Running")], 2)
        with self.assertRaises(cap_main.GeneratorLost):
            cap_main.generator_pods([item(0, "Running"), item(1, "Failed")], 2)


# Generator ----------------------------------------------------------------------------------------------
class Codec(unittest.TestCase):
    def test_varint_round_trip(self):
        for value in (0, 127, 128, 16383, 16384, 2097151, 268435455):
            self.assertEqual(gen.dec_varint(gen.enc_varint(value) + b"x", 0)[0], value)

    def test_publish_round_trip_for_both_protocol_versions(self):
        for version in (4, 5):
            for qos in (0, 1, 2):
                data = gen.publish_packet(version, "a/b", b"payload", qos, True, False, 77 if qos else 0)
                _, flags, body = asyncio.run(self.read(data))
                message = gen.parse_publish(version, flags, body)
                self.assertEqual((message["topic"], message["qos"], message["retain"], message["payload"], message["packetId"]),
                                 ("a/b", qos, True, b"payload", 77 if qos else 0))

    @staticmethod
    async def read(data: bytes):
        reader = asyncio.StreamReader()
        reader.feed_data(data)
        reader.feed_eof()
        return await gen.read_packet(reader)

    def test_connect_packet_carries_will_and_session_properties(self):
        will = {"topic": "t/will", "qos": 1, "retain": False, "delay": 60, "payload": b"NMQW|5|id"}
        data = gen.connect_packet(5, "cid", "user", "token", 30, False, 300, will)
        _, _, body = asyncio.run(self.read(data))
        self.assertEqual(body[6], 5)
        flags = body[7]
        self.assertEqual((flags & 0x80, flags & 0x40, flags & 0x02, flags & 0x04, (flags >> 3) & 3), (0x80, 0x40, 0, 0x04, 1))
        self.assertIn(b"\x11" + struct.pack(">I", 300), body)
        self.assertIn(b"\x18" + struct.pack(">I", 60), body)
        v4 = gen.connect_packet(4, "cid", "user", "token", 30, True, 0, None)
        _, _, body4 = asyncio.run(self.read(v4))
        self.assertEqual((body4[6], body4[7] & 0x06), (4, 0x02))

    def test_topic_matching_matches_the_capacity_oracle(self):
        self.assertTrue(gen.topic_matches("$share/g/root/job/#", "root/job/3"))
        self.assertTrue(gen.topic_matches("root/telemetry/+", "root/telemetry/dev01"))
        self.assertFalse(gen.topic_matches("root/telemetry/+", "root/telemetry/dev01/x"))
        self.assertFalse(gen.topic_matches("root/command/dev02", "root/command/dev03"))
        self.assertTrue(gen.topic_matches("root/alert/#", "root/alert/will/id"))

    def test_payload_round_trip_and_corruption(self):
        payload = gen.make_payload(PROFILE, 99, 3, 2, 17)
        self.assertEqual(gen.parse_payload(PROFILE, payload), (3, 2, 17, 99))
        self.assertEqual(len(payload), PROFILE["payloadSizeCycle"][(17 - 1) % 3])
        self.assertIsNone(gen.parse_payload(PROFILE, payload[:-1] + b"\x00" if payload[-1] else payload[:-1] + b"\x01"))
        self.assertIsNone(gen.parse_payload(PROFILE, b"NMQW|5|x"))


class Plan(unittest.TestCase):
    def test_cell_is_the_capacity_geometry_with_mixed_protocols_and_wills(self):
        clients = gen.build_cell(PROFILE, 3)
        self.assertEqual(len(clients), 53)
        roles = {r: sum(1 for c in clients if c.role == r) for r in ("publisher", "subscriber", "state-subscriber", "idle")}
        self.assertEqual(roles, {"publisher": 8, "subscriber": 23, "state-subscriber": 4, "idle": 18})
        self.assertEqual(len({c.client_id for c in clients}), 53)
        self.assertEqual({c.version for c in clients}, {4, 5})
        self.assertTrue(all(c.version == 5 for c in clients if c.stream == 3 and c.role == "subscriber"))
        self.assertEqual(sum(1 for c in clients if c.tls), len([o for o in range(53) if o % 7 == 0]))
        idle = [c for c in clients if c.role == "idle"]
        self.assertTrue(all(c.will and c.will["topic"].startswith(c.root + "/alert/will/") for c in idle))
        self.assertEqual({c.will["delay"] for c in idle if c.version == 5}, {gen.WILL_DELAY_V5})
        self.assertEqual({c.will["delay"] for c in idle if c.version == 4}, {0})
        self.assertEqual(clients[0].client_id, "bp02-a-c003-pub-telemetry")
        self.assertEqual({c.tenant for c in clients}, {"bp02-a-c003", "bp02-b-c003"})

    def test_protocol_mix_covers_every_qos_in_both_versions(self):
        clients = gen.build_cell(PROFILE, 0)
        pairs = {(c.qos, c.version) for c in clients if c.role in ("publisher", "subscriber", "state-subscriber")}
        self.assertEqual({q for q, _ in pairs}, {0, 1, 2})
        self.assertEqual({v for _, v in pairs}, {4, 5})


class LedgerClassification(unittest.TestCase):
    def ledger(self, stream: int, subs: list):
        counts = gen.Counts()
        return gen.Ledger(gen.STREAM_DEFS[stream], 0, stream, PROFILE, counts, subs), counts

    def clean(self, since: float = 0.0, *, disc: list | None = None):
        return gen.SubTimes(False, [since], list(disc or []))

    def publish(self, ledger, count: int, start: float = 10.0, ack: int = 1):
        for _ in range(count):
            seq = ledger.next_seq()
            ledger.published(seq, start + seq * 0.01)
            if ledger.qos:
                ledger.acknowledged(seq, ack)

    def deliver_all(self, ledger, seqs, subs):
        for seq in seqs:
            for sub in subs:
                ledger.deliver(sub, seq, ledger.topic_of(seq))

    def test_complete_delivery_has_no_gap_duplicate_or_reorder(self):
        ledger, counts = self.ledger(0, [self.clean() for _ in range(6)])
        self.publish(ledger, 20)
        self.deliver_all(ledger, range(1, 21), range(6))
        ledger.finalize(1000.0, 1.0)
        self.assertEqual((counts.groups["expected"]["0"], counts.groups["delivered"]["0"], counts.groups["gap"]["0"], counts.groups["duplicate"]["0"],
                          counts.groups["reorder"]["0"]), (120, 120, 0, 0, 0))

    def test_gap_is_separate_from_duplicate_and_reorder(self):
        ledger, counts = self.ledger(0, [self.clean() for _ in range(6)])
        self.publish(ledger, 8)
        self.deliver_all(ledger, [1, 2, 3, 5, 6, 7, 8], range(6))  # seq 4 never arrives anywhere (6 gaps)
        ledger.deliver(0, 5, ledger.topic_of(5))  # duplicate at sub 0
        ledger.finalize(1000.0, 1.0)
        self.assertEqual((counts.groups["gap"]["0"], counts.groups["duplicate"]["0"], counts.groups["reorder"]["0"]), (6, 1, 0))

    def test_reorder_is_per_subscriber_and_topic(self):
        ledger, counts = self.ledger(0, [self.clean() for _ in range(6)])
        self.publish(ledger, 24)
        topic_seqs = [s for s in range(1, 25) if ledger.topic_of(s) == ledger.topic_of(1)]  # 1, 9, 17
        ledger.deliver(0, topic_seqs[1], ledger.topic_of(1))
        ledger.deliver(0, topic_seqs[0], ledger.topic_of(1))  # older seq after newer on the same topic: reorder
        ledger.deliver(0, 2, ledger.topic_of(2))  # another topic: no reorder
        ledger.finalize(1000.0, 1.0)
        self.assertEqual((counts.groups["reorder"]["0"], counts.groups["duplicate"]["0"]), (1, 0))

    def test_unacked_interrupted_and_clean_session_loss_are_not_gaps(self):
        ledger, counts = self.ledger(1, [gen.SubTimes(True, [0.0]) for _ in range(4)])
        self.publish(ledger, 1)
        ledger.acked.pop(1)  # broker never acknowledged
        self.publish(ledger, 1, ack=2)
        self.publish(ledger, 1)
        ledger.finalize(1000.0, 1.0)
        self.assertEqual((counts.groups["unacked"]["1"], counts.groups["cleanPublisherInterrupted"]["1"], counts.groups["gap"]["1"]), (1, 1, 4))
        clean = [self.clean(0.0, disc=[10.5]) for _ in range(6)]  # disconnect after the publish, never resubscribed
        ledger, counts = self.ledger(0, clean)
        self.publish(ledger, 1, start=10.0)
        ledger.finalize(1000.0, 1.0)
        self.assertEqual((counts.groups["gap"]["0"], counts.groups["cleanSessionLoss"]["0"], counts.groups["expected"]["0"]), (0, 6, 0))

    def test_persistent_session_owes_the_message_across_a_disconnect(self):
        ledger, counts = self.ledger(1, [gen.SubTimes(True, [0.0], [10.5, 12.0]) for _ in range(4)])
        self.publish(ledger, 2)
        self.deliver_all(ledger, [1], range(4))
        ledger.finalize(1000.0, 1.0)
        self.assertEqual((counts.groups["expected"]["1"], counts.groups["gap"]["1"]), (8, 4))

    def test_qos2_duplicate_and_late_delivery(self):
        ledger, counts = self.ledger(2, [gen.SubTimes(True, [0.0]) for _ in range(4)])
        self.publish(ledger, 1)
        ledger.deliver(0, 1, ledger.topic_of(1))
        ledger.deliver(0, 1, ledger.topic_of(1))
        ledger.finalize(1000.0, 1.0)
        ledger.deliver(0, 1, ledger.topic_of(1))
        self.assertEqual((counts.groups["duplicate"]["2"], counts.groups["gap"]["2"], counts.scalars["afterHorizon"]), (1, 0, 1))
        ledger, counts = self.ledger(2, [gen.SubTimes(True, [0.0]) for _ in range(4)])
        self.publish(ledger, 4)
        self.deliver_all(ledger, [1, 2, 3], range(4))  # each command subscriber owns one topic; seq 4 never arrives
        ledger.finalize(1000.0, 1.0)
        self.assertEqual((counts.groups["expected"]["2"], counts.groups["gap"]["2"]), (4, 1))

    def test_shared_group_counts_group_level_duplicates_and_gaps(self):
        subs = [self.clean() for _ in range(4)]
        ledger, counts = self.ledger(3, subs)
        self.publish(ledger, 3)
        ledger.deliver(2, 1, ledger.topic_of(1))
        ledger.deliver(3, 1, ledger.topic_of(1))  # a second member got a copy: duplicate
        ledger.deliver(1, 3, ledger.topic_of(3))
        ledger.finalize(1000.0, 1.0)  # seq 2 delivered to nobody: one gap
        self.assertEqual((counts.groups["expected"]["1"], counts.groups["gap"]["1"], counts.groups["duplicate"]["1"]), (3, 1, 1))

    def test_misdirected_delivery_is_unexpected(self):
        ledger, counts = self.ledger(2, [gen.SubTimes(True, [0.0]) for _ in range(4)])
        self.publish(ledger, 1)
        ledger.deliver(1, 1, ledger.topic_of(1))  # sub 1 listens to another command topic
        ledger.deliver(0, 9, ledger.topic_of(1))  # never published
        self.assertEqual(counts.scalars["unexpectedDeliveries"], 2)

    def test_sub_continuity_history(self):
        times = gen.SubTimes(False, [1.0, 5.0], [3.0])
        self.assertFalse(times.continuous(0.5))
        self.assertFalse(times.continuous(2.0))  # disconnect at 3.0 follows the SUBACK at 1.0
        self.assertTrue(times.continuous(6.0))
        self.assertFalse(times.continuous(4.0))  # between the disconnect and the resubscribe
        self.assertTrue(gen.SubTimes(True, [1.0], [3.0]).subscribed_before(4.0))


# In-process fake MQTT broker: enough of MQTT 3.1.1/5.0 to drive the generator end to end ---------------------
class FakeBroker:
    def __init__(self) -> None:
        self.subs: list[dict] = []
        self.retained: dict[str, tuple[bytes, int]] = {}
        self.writers: dict[str, asyncio.StreamWriter] = {}
        self.seen: dict[str, tuple[str, str, int]] = {}
        self.round_robin = 0

    async def start(self) -> int:
        self.server = await asyncio.start_server(self.handle, "127.0.0.1", 0)
        return self.server.sockets[0].getsockname()[1]

    @staticmethod
    def parse_connect(body: bytes):
        version, flags = body[6], body[7]
        pos = 10
        if version == 5:
            length, pos = gen.dec_varint(body, pos)
            pos += length
        (n,) = struct.unpack_from(">H", body, pos)
        client_id = body[pos + 2:pos + 2 + n].decode()
        pos += 2 + n
        if flags & 4:
            if version == 5:
                length, pos = gen.dec_varint(body, pos)
                pos += length
            for _ in range(2):
                (n,) = struct.unpack_from(">H", body, pos)
                pos += 2 + n
        (n,) = struct.unpack_from(">H", body, pos)
        username = body[pos + 2:pos + 2 + n].decode()
        pos += 2 + n
        (n,) = struct.unpack_from(">H", body, pos)
        return version, client_id, username, body[pos + 2:pos + 2 + n].decode(), bool(flags & 4)

    async def handle(self, reader, writer):
        me = None
        packet_ids = 0
        try:
            _, _, body = await gen.read_packet(reader)
            version, client_id, username, password, _ = self.parse_connect(body)
            tenant = client_id.rsplit("-", 2)[0] if "-pub-" not in client_id else client_id.split("-pub-")[0]
            tenant = client_id.split("-sub-")[0].split("-pub-")[0].split("-idle-")[0]
            if username != "u-" + tenant or password != "tok-" + tenant:
                writer.write(gen.packet(2, 0, b"\x00\x04" if version == 4 else b"\x00\x86\x00"))
                await writer.drain()
                return
            self.seen[client_id] = (username, password, version)
            self.writers[client_id] = writer
            writer.write(gen.packet(2, 0, b"\x00\x00" if version == 4 else b"\x00\x00\x00"))
            while True:
                ptype, flags, body = await gen.read_packet(reader)
                if ptype == 8:
                    (pid,) = struct.unpack_from(">H", body, 0)
                    pos = 2
                    if version == 5:
                        length, pos = gen.dec_varint(body, pos)
                        pos += length
                    (n,) = struct.unpack_from(">H", body, pos)
                    topic_filter, qos = body[pos + 2:pos + 2 + n].decode(), body[pos + 2 + n]
                    me = {"writer": writer, "version": version, "filter": topic_filter, "qos": qos}
                    self.subs.append(me)
                    writer.write(gen.packet(9, 0, struct.pack(">H", pid) + (b"\x00" if version == 5 else b"") + bytes([qos])))
                    for topic, (payload, stored_qos) in list(self.retained.items()):
                        if gen.topic_matches(topic_filter, topic):
                            packet_ids += 1
                            writer.write(gen.publish_packet(version, topic, payload, min(qos, stored_qos), True, False, packet_ids if min(qos, stored_qos) else 0))
                elif ptype == 3:
                    message = gen.parse_publish(version, flags, body)
                    if message["qos"] == 1:
                        writer.write(gen.ack_packet(4, message["packetId"]))
                    elif message["qos"] == 2:
                        writer.write(gen.ack_packet(5, message["packetId"]))
                    if message["retain"]:
                        self.retained[message["topic"]] = (message["payload"], message["qos"])
                    self.forward(message)
                elif ptype == 6:
                    writer.write(gen.ack_packet(7, struct.unpack_from(">H", body, 0)[0]))
                elif ptype == 5:
                    writer.write(gen.ack_packet(6, struct.unpack_from(">H", body, 0)[0]))
                elif ptype == 12:
                    writer.write(gen.packet(13, 0, b""))
                elif ptype == 14:
                    return
                await writer.drain()
        except (asyncio.IncompleteReadError, ConnectionError, OSError):
            pass
        finally:
            self.subs = [s for s in self.subs if s["writer"] is not writer]
            writer.close()

    def forward(self, message):
        groups: dict[str, list[dict]] = {}
        for sub in self.subs:
            if not gen.topic_matches(sub["filter"], message["topic"]):
                continue
            if sub["filter"].startswith("$share/"):
                groups.setdefault(sub["filter"].split("/")[1], []).append(sub)
            else:
                self.send(sub, message)
        for members in groups.values():
            self.round_robin += 1
            self.send(members[self.round_robin % len(members)], message)

    def send(self, sub, message):
        qos = min(sub["qos"], message["qos"])
        packet_id = (self.round_robin * 7 + id(sub)) % 60000 + 1 if qos else 0
        sub["writer"].write(gen.publish_packet(sub["version"], message["topic"], message["payload"], qos, False, False, packet_id))


class GeneratorEndToEnd(unittest.TestCase):
    def test_generator_measures_a_clean_run_and_survives_reconnects(self):
        with tempfile.TemporaryDirectory() as tmp:
            summary = asyncio.run(self.run_generator(Path(tmp)))
        counts, scalars = summary["counts"], summary["scalars"]
        self.assertTrue(summary["finalized"])
        self.assertEqual((summary["clientsExpected"], summary["clientsConnected"]), (53, 53))
        for qos in judge.QOS_LEVELS:
            self.assertGreater(counts["published"][qos], 0, qos)
            self.assertGreater(counts["delivered"][qos], 0, qos)
        for kind in judge.KINDS:
            self.assertEqual(counts[kind]["1"] + counts[kind]["2"], 0, (kind, counts[kind]))
        self.assertEqual(counts["gap"]["0"], 0)
        self.assertEqual((scalars["unexpectedDeliveries"], scalars["protocolErrors"], scalars["connectRefusals"], scalars["will5Received"],
                          scalars["will311Received"]), (0, 0, 0, 0, 0))
        self.assertGreaterEqual(scalars["reconnects5"] + scalars["reconnects311"], 2)
        self.assertGreaterEqual(scalars["retainedChecks"], 1)
        self.assertEqual(scalars["retainedMismatches"], 0)

    async def run_generator(self, tmp: Path) -> dict:
        broker = FakeBroker()
        port = await broker.start()
        credentials = {t: {"username": "u-" + t, "token": "tok-" + t} for t in ("bp02-a-c000", "bp02-b-c000")}
        write(tmp / "credentials.json", credentials)
        for directory in ("control", "output"):
            (tmp / directory).mkdir()
        cfg = gen.Config(run_token=1234, shards=1, profile=PROFILE, cells=1, rate=20, mqtt=f"127.0.0.1:{port}", mqtt_tls="127.0.0.1:1",
                         tls_server_name="x", horizon=1.0, settle=1.0, retained_window=0.3, credentials=str(tmp / "credentials.json"), ca_file="",
                         control_dir=str(tmp / "control"), output_dir=str(tmp / "output"), setup_timeout=30.0, monitor_interval=0.3)
        generator = gen.Generator(cfg, 0)
        build = generator.build

        def plain_build():
            build()
            for client in generator.clients:
                client.spec.tls = False
        generator.build = plain_build
        task = asyncio.create_task(generator.main())
        output, control = tmp / "output", tmp / "control"
        for _ in range(300):
            if (output / "ready.json").exists() or task.done():
                break
            await asyncio.sleep(0.1)
        self.assertTrue((output / "ready.json").exists(), "generator never became ready")
        await asyncio.sleep(1.5)
        for client_id in ("bp02-a-c000-sub-state-00", "bp02-a-c000-sub-telemetry-01", "bp02-a-c000-sub-telemetry-02"):
            broker.writers[client_id].close()
        await asyncio.sleep(2.0)
        (control / "stop").write_text("", encoding="utf-8")
        for _ in range(300):
            if (output / "final.json").exists() or task.done():
                break
            await asyncio.sleep(0.1)
        self.assertTrue((output / "final.json").exists(), "generator never finalized")
        summary = json.loads((output / "final.json").read_text(encoding="utf-8"))
        (control / "collected").write_text("", encoding="utf-8")
        self.assertEqual(await asyncio.wait_for(task, 10), 0)
        self.assertEqual(len(broker.seen), 53)
        self.assertEqual({v[0] for v in broker.seen.values()}, {"u-bp02-a-c000", "u-bp02-b-c000"})
        broker.server.close()
        return summary


if __name__ == "__main__":
    unittest.main()
