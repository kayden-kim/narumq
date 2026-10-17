"""Operator-policy tests for feature activation (Release contract §4.1, ADR-0032).

Each test writes an evidence directory in the exact format of the runbook's `Invoke-FeatureRequest`
(`<prefix>.result.json` with a UTF-8 BOM and PowerShell JSON keys, plus `<prefix>.body.json`) and runs the real
decision procedure `tests/chaos/feature-activation-runbook/decision.py` over it, the way an operator does with
`python decision.py --evidence-dir ...`. Scope: the written policy's state invariants over recorded evidence.
These tests do not start a cluster and cannot show that a person follows the policy; the broker does not check
approval records (ADR-0032 §1).
"""
from __future__ import annotations

import contextlib
import importlib.util
import io
import json
from pathlib import Path
import tempfile
import unittest

DECISION_PATH = Path(__file__).resolve().parent / "chaos" / "feature-activation-runbook" / "decision.py"
SPEC = importlib.util.spec_from_file_location("feature_activation_decision", DECISION_PATH)
decision = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(decision)

FEATURE = "session-metadata"
OTHER = "compatibility-probe"


def member(node_id, release="1.2.0", features=(FEATURE, OTHER)):
    return {"nodeId": str(node_id), "releaseVersion": release, "features": list(features)}


def features_body(members, active=()):
    return {"data": {"activeFeatures": list(active), "members": members}}


def approval_for(members, active=()):
    return {"clusterId": "cluster-a", "feature": FEATURE, "approvedMembers": members,
            "approvedActiveFeatures": list(active), "rollbackWaiver": decision.ROLLBACK_WAIVER,
            "approvedAtUtc": "2026-10-04T09:00:00.0000000Z", "approver": "release-owner", "executor": "release-owner"}


class Evidence:
    """Writes runbook-format evidence files with increasing UTC times."""

    def __init__(self, directory: Path):
        self.directory, self.tick = directory, 0

    def add(self, prefix, method, status, body=None, curl_exit=0):
        self.tick += 1
        stamp = f"2026-10-04T09:{self.tick:02d}:00.1234567Z"
        result = {"Method": method, "HttpStatus": status, "CurlExitCode": curl_exit,
                  "RequestId": f"req_{prefix}", "RequestedAtUtc": stamp, "ObservedAtUtc": stamp}
        # PowerShell 5.1 `ConvertTo-Json | Set-Content -Encoding UTF8` output carries a BOM.
        (self.directory / f"{prefix}.result.json").write_bytes(b"\xef\xbb\xbf" + json.dumps(result, indent=4).encode())
        if body is not None:
            (self.directory / f"{prefix}.body.json").write_text(json.dumps(body), encoding="utf-8")

    def features(self, prefix, members, active=()):
        self.add(prefix, "GET", "200", features_body(members, active))

    def summary(self, prefix, cluster_id="cluster-a"):
        self.add(prefix, "GET", "200", {"data": {"clusterId": cluster_id, "quorumAvailable": True, "underReplicated": 0}})

    def decide(self, approval):
        return decision.evaluate(FEATURE, approval, decision.load_observations(self.directory))


class FeatureActivationDecisionTests(unittest.TestCase):
    def setUp(self):
        self._temp = tempfile.TemporaryDirectory()
        self.addCleanup(self._temp.cleanup)
        self.evidence = Evidence(Path(self._temp.name))
        self.members = [member(1), member(2), member(3)]
        self.approval = approval_for(self.members)

    # Scenarios: cluster-session-and-delivery.md:CLU-SCN-210
    def test_missing_advertisement_on_one_member_holds_approval_and_post(self):
        lacking = [member(1), member(2), member(3, features=(OTHER,))]
        self.evidence.summary("recheck-cluster")
        self.evidence.features("recheck-features", lacking)
        for approval in (None, approval_for(lacking)):
            with self.subTest(approval=approval is not None):
                result = self.evidence.decide(approval)
                self.assertEqual(result["decision"], decision.HOLD)
                self.assertFalse(result["postAllowed"])
                self.assertIn(f"member 3 does not advertise {FEATURE}", result["reasons"])
                self.assertFalse(result["windowClosed"])
        # A member with no features at all, and an empty member list, also hold.
        for members in ([member(1), member(2), member(3, features=())], []):
            with self.subTest(members=len(members)):
                directory = Path(tempfile.mkdtemp(dir=self._temp.name))
                other = Evidence(directory)
                other.features("recheck-features", members)
                self.assertEqual(other.decide(None)["decision"], decision.HOLD)

    # Scenarios: cluster-session-and-delivery.md:CLU-SCN-211
    def test_complete_approval_before_post_does_not_close_the_window(self):
        self.evidence.summary("before-cluster")
        self.evidence.features("before-features", self.members)
        self.evidence.features("recheck-features", self.members)
        result = self.evidence.decide(self.approval)
        self.assertEqual(result["decision"], decision.POST_PENDING)
        self.assertTrue(result["approvalValid"])
        self.assertTrue(result["postAllowed"])
        self.assertFalse(result["windowClosed"])
        self.assertNotIn("firstCommitObservedAtUtc", result)
        # Elapsed time since approval never closes the window (§4.1 item 7).
        old = dict(self.approval, approvedAtUtc="2025-01-01T00:00:00.0000000Z")
        aged = self.evidence.decide(old)
        self.assertFalse(aged["windowClosed"])
        self.assertEqual(aged["decision"], decision.POST_PENDING)
        # No commit means no rollback decision is recorded as closed, and recovery follows the pre-activation rules.
        recovery = decision.evaluate_recovery(result, {"kind": "redeploy-build", "feature": FEATURE,
                                                       "build": {"features": [OTHER]}})
        self.assertFalse(recovery["windowClosed"])
        self.assertEqual(recovery["action"], "PRE_ACTIVATION_COMPATIBILITY_RULES")
        # An incomplete approval (no waiver statement) is not an approval.
        incomplete = decision.evaluate(FEATURE, dict(self.approval, rollbackWaiver="ok"),
                                       decision.load_observations(self.evidence.directory))
        self.assertEqual(incomplete["decision"], decision.APPROVAL_INCOMPLETE)
        self.assertFalse(incomplete["postAllowed"])
        # Identity and health gates: without a successful summary, with another cluster, or without quorum, no POST.
        for name, summaries in {
            "no summary": [],
            "quorum lost": [("s", {"data": {"clusterId": "cluster-a", "quorumAvailable": False, "underReplicated": 0}})],
            "under-replicated": [("s", {"data": {"clusterId": "cluster-a", "quorumAvailable": True, "underReplicated": 1}})],
            "disagreeing clusters": [("s1", {"data": {"clusterId": "cluster-a", "quorumAvailable": True, "underReplicated": 0}}),
                                     ("s2", {"data": {"clusterId": "cluster-b", "quorumAvailable": True, "underReplicated": 0}})],
            "malformed cluster id": [("s", {"data": {"clusterId": ["cluster-a"], "quorumAvailable": True, "underReplicated": 0}})],
        }.items():
            with self.subTest(gate=name):
                evidence = Evidence(Path(tempfile.mkdtemp(dir=self._temp.name)))
                for prefix, body in summaries:
                    evidence.add(prefix, "GET", "200", body)
                evidence.features("recheck-features", self.members)
                for approval in (None, self.approval):
                    held = evidence.decide(approval)
                    self.assertEqual(held["decision"], decision.HOLD_CLUSTER)
                    self.assertFalse(held["postAllowed"])
        # Malformed or hostile approval records fail closed instead of raising or allowing the POST.
        for name, bad in {
            "not an object": ["approval"],
            "empty object": {},
            "members not a list": dict(self.approval, approvedMembers="1,2,3"),
            "member without features": dict(self.approval, approvedMembers=[{"nodeId": "1", "releaseVersion": "1.2.0"}]),
            "member id not text": dict(self.approval, approvedMembers=[member(1) | {"nodeId": 1}]),
            "duplicate member": dict(self.approval, approvedMembers=[member(1), member(1)]),
            "active features not list": dict(self.approval, approvedActiveFeatures="none"),
            "cluster id not text": dict(self.approval, clusterId=7),
            "different feature": dict(self.approval, feature=OTHER),
            "bad time": dict(self.approval, approvedAtUtc="yesterday"),
        }.items():
            with self.subTest(approval=name):
                held = self.evidence.decide(bad)
                self.assertEqual(held["decision"], decision.APPROVAL_INCOMPLETE)
                self.assertFalse(held["postAllowed"])
                self.assertFalse(held["approvalValid"])

    # Scenarios: cluster-session-and-delivery.md:CLU-SCN-212
    def test_member_or_build_change_after_approval_invalidates_it(self):
        changes = {
            "member added": [member(1), member(2), member(3), member(4)],
            "member removed": [member(1), member(2)],
            "release changed": [member(1), member(2), member(3, release="1.3.0")],
            "features changed": [member(1), member(2), member(3, features=(FEATURE,))],
        }
        for name, current in changes.items():
            with self.subTest(change=name):
                directory = Path(tempfile.mkdtemp(dir=self._temp.name))
                evidence = Evidence(directory)
                evidence.summary("recheck-cluster")
                evidence.features("recheck-features", current)
                result = evidence.decide(self.approval)
                self.assertEqual(result["decision"], decision.REAPPROVAL)
                self.assertFalse(result["approvalValid"])
                self.assertFalse(result["postAllowed"])
                self.assertTrue(result["reasons"])
                # Re-approval for the new set makes the POST allowed again.
                renewed = evidence.decide(approval_for(current))
                self.assertEqual(renewed["decision"], decision.POST_PENDING)
        with self.subTest(change="activeFeatures changed"):
            directory = Path(tempfile.mkdtemp(dir=self._temp.name))
            evidence = Evidence(directory)
            evidence.summary("recheck-cluster")
            evidence.features("recheck-features", self.members, active=[OTHER])
            self.assertEqual(evidence.decide(self.approval)["decision"], decision.REAPPROVAL)
        with self.subTest(change="cluster changed"):
            directory = Path(tempfile.mkdtemp(dir=self._temp.name))
            evidence = Evidence(directory)
            evidence.summary("recheck-cluster", cluster_id="cluster-b")
            evidence.features("recheck-features", self.members)
            self.assertEqual(evidence.decide(self.approval)["decision"], decision.REAPPROVAL)

    # Scenarios: cluster-session-and-delivery.md:CLU-SCN-213
    def test_post_timeout_then_get_without_token_is_unknown_and_forbids_rollback(self):
        self.evidence.features("recheck-features", self.members)
        self.evidence.add("activation", "POST", "000", curl_exit=28)
        self.evidence.features("after-features", self.members)
        result = self.evidence.decide(self.approval)
        self.assertEqual(result["decision"], decision.UNKNOWN)
        self.assertFalse(result["windowClosed"])
        self.assertFalse(result["rollbackToUnsupportedBuildAllowed"])
        self.assertFalse(result["newTargetApprovalAllowed"])
        self.assertFalse(result["previousRequestsJudgedFailed"])
        self.assertTrue(any("does not prove" in reason for reason in result["reasons"]))
        recovery = decision.evaluate_recovery(result, {"kind": "redeploy-build", "feature": FEATURE,
                                                       "build": {"features": [OTHER]}})
        self.assertEqual(recovery["action"], "REFUSE_UNKNOWN_STATE")
        # The other unverifiable outcomes are unknown too.
        for status, body in (("500", None), ("503", {"error": {"code": "audit_unavailable"}}), ("200", {"data": {}}),
                             ("200", features_body(self.members))):
            with self.subTest(status=status):
                directory = Path(tempfile.mkdtemp(dir=self._temp.name))
                evidence = Evidence(directory)
                evidence.add("activation", "POST", status, body)
                self.assertEqual(evidence.decide(self.approval)["decision"], decision.UNKNOWN)

    # Scenarios: cluster-session-and-delivery.md:CLU-SCN-214
    def test_post_error_then_successful_get_with_token_confirms_commit(self):
        self.evidence.features("recheck-features", self.members)
        self.evidence.add("activation", "POST", "503", {"error": {"code": "cluster_unavailable", "retryable": True}})
        self.evidence.features("after-features", self.members, active=[FEATURE])
        result = self.evidence.decide(self.approval)
        self.assertEqual(result["decision"], decision.COMMITTED)
        self.assertTrue(result["windowClosed"])
        self.assertEqual(result["firstCommitObservedBy"], "GET after-features")
        self.assertEqual(result["firstCommitObservedAtUtc"], "2026-10-04T09:03:00.1234567Z")
        self.assertFalse(result["rollbackToUnsupportedBuildAllowed"])
        # A failed GET that mentions the token is not a confirmation.
        directory = Path(tempfile.mkdtemp(dir=self._temp.name))
        evidence = Evidence(directory)
        evidence.add("activation", "POST", "503", {"error": {"code": "cluster_unavailable"}})
        evidence.add("after-features", "GET", "503", features_body(self.members, active=[FEATURE]))
        self.assertEqual(evidence.decide(self.approval)["decision"], decision.UNKNOWN)

    # Scenarios: cluster-session-and-delivery.md:CLU-SCN-215
    def test_current_409_does_not_fail_an_earlier_unknown_post(self):
        self.evidence.features("recheck-features", self.members)
        self.evidence.add("activation-1", "POST", "000", curl_exit=28)
        self.evidence.add("activation-2", "POST", "409", {"error": {"code": "feature_not_supported_by_all_members",
                                                                    "retryable": False}})
        result = self.evidence.decide(self.approval)
        self.assertEqual(result["decision"], decision.UNKNOWN)
        self.assertEqual(result["thisRequestOutcome"], decision.POST_NOT_ACTIVATED)
        self.assertEqual(result["priorUnknownRequests"], 1)
        self.assertFalse(result["previousRequestsJudgedFailed"])
        self.assertFalse(result["rollbackToUnsupportedBuildAllowed"])
        self.assertFalse(result["newTargetApprovalAllowed"])
        # A first-ever 409 is the ordinary not-activated case that needs a fix and a new approval.
        directory = Path(tempfile.mkdtemp(dir=self._temp.name))
        evidence = Evidence(directory)
        evidence.add("activation", "POST", "409", {"error": {"code": "feature_not_supported_by_all_members"}})
        plain = evidence.decide(self.approval)
        self.assertEqual(plain["decision"], decision.NOT_ACTIVATED)
        self.assertEqual(plain["thisRequestOutcome"], decision.POST_NOT_ACTIVATED)
        self.assertFalse(plain["windowClosed"])

    # Scenarios: cluster-session-and-delivery.md:CLU-SCN-217
    def test_activation_then_unsupported_build_recovers_forward_and_backup_restore_is_separate_dr(self):
        self.evidence.features("recheck-features", self.members)
        self.evidence.add("activation", "POST", "200", features_body(self.members, active=[FEATURE]))
        committed = self.evidence.decide(self.approval)
        self.assertEqual(committed["decision"], decision.COMMITTED)
        self.assertTrue(committed["windowClosed"])
        old_build = {"releaseVersion": "1.1.0", "features": [OTHER]}
        refused = decision.evaluate_recovery(committed, {"kind": "redeploy-build", "feature": FEATURE, "build": old_build})
        self.assertEqual(refused["action"], "REFUSE_IN_PLACE_ROLLBACK")
        self.assertEqual(refused["recommended"], "FORWARD_RECOVERY_WITH_SUPPORTED_BUILD")
        self.assertFalse(refused["losslessInPlaceRollback"])
        # A recovery request for a different feature than the evaluated one is rejected, never judged.
        with self.assertRaises(ValueError):
            decision.evaluate_recovery(committed, {"kind": "redeploy-build", "feature": OTHER, "build": old_build})
        with self.assertRaises(ValueError):
            decision.evaluate_recovery(committed, {"kind": "restore-backup", "feature": FEATURE, "build": old_build})
        forward = decision.evaluate_recovery(committed, {"kind": "redeploy-build", "feature": FEATURE,
                                                         "build": {"releaseVersion": "1.2.0", "features": [FEATURE, OTHER]}})
        self.assertEqual(forward["action"], "SAME_VOLUME_REDEPLOY_ALLOWED")
        restore = decision.evaluate_recovery(committed, {"kind": "restore-backup", "feature": FEATURE, "build": old_build,
                                                         "backupCommit": "before-activation"})
        self.assertEqual(restore["action"], "DISASTER_RECOVERY_SEPARATE_APPROVAL_REQUIRED")
        self.assertTrue(restore["dataLossBoundary"])
        self.assertTrue(restore["separateDisasterRecoveryApprovalRequired"])
        self.assertFalse(restore["losslessInPlaceRollback"])
        # The same decision is reached from the CLI over the evidence directory.
        approval_path = self.evidence.directory / "approval.json"
        request_path = self.evidence.directory / "recovery.json"
        approval_path.write_text(json.dumps(self.approval), encoding="utf-8")
        request_path.write_text(json.dumps({"kind": "redeploy-build", "feature": FEATURE, "build": old_build}), encoding="utf-8")
        buffer = io.StringIO()
        with contextlib.redirect_stdout(buffer):
            code = decision.main(["--feature", FEATURE, "--evidence-dir", str(self.evidence.directory),
                                  "--approval", str(approval_path), "--recovery-request", str(request_path)])
        self.assertEqual(code, 0)
        output = json.loads(buffer.getvalue())
        self.assertEqual(output["decision"]["decision"], decision.COMMITTED)
        self.assertEqual(output["recovery"]["action"], "REFUSE_IN_PLACE_ROLLBACK")


if __name__ == "__main__":
    unittest.main()
