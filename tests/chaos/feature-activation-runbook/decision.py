"""Operator decision procedure for a feature activation (Release contract §4.1, ADR-0032).

This is the decision aid an operator runs over the evidence directory that the runbook's
`Invoke-FeatureRequest` writes (`<prefix>.result.json` + `<prefix>.body.json`) and over the external approval
record described in docs/how-to/cluster-feature-activation.md §3 step 5. It answers four questions and never
guesses: may the operator approve/POST, is the approval still valid, is the rollback window closed, and may
the operator roll back to a build that lacks the feature.

Scope: this evaluates recorded observations against the written policy. It cannot make a person follow the
policy, and the broker does not inspect approval records (ADR-0032 §1). The evidence is read as-is; nothing is
fetched, written, or sent to a cluster. `main()` prints the decision as JSON.
"""
from __future__ import annotations

import argparse
import datetime
import json
from pathlib import Path
import re
import sys
from typing import Any

FEATURE_TOKEN = re.compile(r"^[a-z][a-z0-9-]{0,62}$")
ROLLBACK_WAIVER = "이 feature를 지원하지 않는 이전 build로의 무손실 in-place rollback을 포기한다"
APPROVAL_FIELDS = ("clusterId", "feature", "approvedMembers", "approvedActiveFeatures", "rollbackWaiver",
                   "approvedAtUtc", "approver", "executor")

HOLD = "HOLD_APPROVAL_AND_POST"
HOLD_CLUSTER = "HOLD_CLUSTER_STATE"
NO_OBSERVATION = "HOLD_NO_VALID_OBSERVATION"
AWAIT_APPROVAL = "APPROVABLE_AWAIT_APPROVAL"
APPROVAL_INCOMPLETE = "APPROVAL_INCOMPLETE"
REAPPROVAL = "REAPPROVAL_REQUIRED"
POST_PENDING = "APPROVED_POST_PENDING"
UNKNOWN = "UNKNOWN_DO_NOT_ROLLBACK"
COMMITTED = "COMMITTED_WINDOW_CLOSED"
ALREADY_ACTIVE = "ALREADY_ACTIVE_WINDOW_CLOSED"
NOT_ACTIVATED = "NOT_ACTIVATED_REAPPROVAL_REQUIRED"
FIX_INPUT = "FIX_INPUT_OR_PERMISSION"
FIND_LEADER = "FIND_CURRENT_LEADER"

POST_COMMITTED, POST_UNKNOWN, POST_NOT_ACTIVATED, POST_INPUT_ERROR, POST_NOT_LEADER = (
    "COMMITTED", "UNKNOWN", "NOT_ACTIVATED_THIS_REQUEST", "INPUT_ERROR", "NOT_LEADER")


def parse_utc(value: Any) -> datetime.datetime:
    """Parse the runbook's `DateTime.ToString('o')` UTC text (seven fractional digits)."""
    if not isinstance(value, str):
        raise ValueError("timestamp must be text")
    text = re.sub(r"(\.\d{6})\d+", r"\1", value.strip())
    if not text.endswith("Z"):
        raise ValueError("timestamp must be UTC (Z)")
    return datetime.datetime.fromisoformat(text[:-1] + "+00:00")


def _read_json(path: Path) -> Any:
    # PowerShell 5.1 `Set-Content -Encoding UTF8` writes a BOM.
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _valid_features_body(body: Any) -> bool:
    data = body.get("data") if isinstance(body, dict) else None
    if not isinstance(data, dict) or not isinstance(data.get("activeFeatures"), list) \
            or not isinstance(data.get("members"), list):
        return False
    if not all(isinstance(item, str) for item in data["activeFeatures"]):
        return False
    return all(isinstance(m, dict) and isinstance(m.get("nodeId"), str) and isinstance(m.get("releaseVersion"), str)
               and isinstance(m.get("features"), list) and all(isinstance(f, str) for f in m["features"])
               for m in data["members"])


def load_observations(directory: Path) -> list[dict]:
    """Load every `<prefix>.result.json` (with its optional body) from an evidence directory, oldest first.

    A malformed result file raises ValueError: evidence that cannot be read must stop the operator, not be skipped."""
    observations = []
    for result_path in sorted(directory.glob("*.result.json")):
        prefix = result_path.name[: -len(".result.json")]
        try:
            result = _read_json(result_path)
            if not isinstance(result, dict):
                raise ValueError("not an object")
            requested, observed = result["RequestedAtUtc"], result["ObservedAtUtc"]
            parse_utc(requested)
            parse_utc(observed)
        except (ValueError, KeyError, UnicodeDecodeError) as error:
            raise ValueError(f"unreadable evidence {result_path.name}: {error}") from error
        body_path = directory / f"{prefix}.body.json"
        body = None
        if body_path.is_file():
            try:
                body = _read_json(body_path)
            except (ValueError, UnicodeDecodeError):
                body = None
        status = str(result.get("HttpStatus", "")).strip()
        observations.append({
            "prefix": prefix, "method": result.get("Method"),
            "httpStatus": int(status) if status.isdigit() else None,
            "curlExitCode": result.get("CurlExitCode"), "requestId": result.get("RequestId"),
            "requestedAtUtc": requested, "observedAtUtc": observed, "body": body})
    observations.sort(key=lambda o: (parse_utc(o["requestedAtUtc"]), o["prefix"]))
    return observations


def _succeeded(observation: dict) -> bool:
    return observation["curlExitCode"] == 0 and observation["httpStatus"] == 200


def classify_post(observation: dict, feature: str) -> str:
    """Outcome of one POST. Anything that does not prove commit or rejection is UNKNOWN (§4.1 item 10)."""
    status, body = observation["httpStatus"], observation["body"]
    if observation["curlExitCode"] != 0 or status is None:
        return POST_UNKNOWN  # timeout / connection closed: ActivateFeature may have committed before the error
    code = body.get("error", {}).get("code") if isinstance(body, dict) and isinstance(body.get("error"), dict) else None
    if status == 200:
        if _valid_features_body(body) and feature in body["data"]["activeFeatures"]:
            return POST_COMMITTED
        return POST_UNKNOWN  # success that cannot be verified
    if code == "not_leader":
        return POST_NOT_LEADER
    if status == 409 and code == "feature_not_supported_by_all_members":
        return POST_NOT_ACTIVATED
    if status in (400, 401, 403):
        return POST_INPUT_ERROR
    return POST_UNKNOWN  # 500, 503, other statuses and unreadable errors


def _advertisement_gaps(members: list[dict], feature: str) -> list[str]:
    if not members:
        return ["no member reported"]
    return [f"member {m['nodeId']} does not advertise {feature}" for m in members if feature not in m["features"]]


def _is_text(value: Any) -> bool:
    return isinstance(value, str) and bool(value)


def _approval_problems(approval: Any, feature: str) -> list[str]:
    """Every reason the approval record is not a complete, well-typed approval of exactly this feature."""
    if not isinstance(approval, dict):
        return ["approval record is not a JSON object"]
    problems = [f"missing {name}" for name in APPROVAL_FIELDS if name not in approval]
    if problems:
        return problems
    for name in ("clusterId", "feature", "approver", "executor"):
        if not _is_text(approval[name]):
            problems.append(f"{name} must be non-empty text")
    if approval["feature"] != feature:
        problems.append("approval is for a different feature token")
    if approval["rollbackWaiver"] != ROLLBACK_WAIVER:
        problems.append("rollback waiver statement is missing or altered")
    try:
        parse_utc(approval["approvedAtUtc"])
    except ValueError:
        problems.append("approvedAtUtc is not UTC time text")
    active = approval["approvedActiveFeatures"]
    if not isinstance(active, list) or not all(_is_text(item) for item in active):
        problems.append("approvedActiveFeatures must be a list of tokens")
    members = approval["approvedMembers"]
    if not isinstance(members, list) or not members:
        problems.append("approvedMembers must list every non-retired member")
        return problems
    seen = set()
    for member in members:
        valid = (isinstance(member, dict) and _is_text(member.get("nodeId")) and _is_text(member.get("releaseVersion"))
                 and isinstance(member.get("features"), list) and all(_is_text(item) for item in member["features"]))
        if not valid:
            problems.append("approvedMembers entries need text nodeId, releaseVersion and a features list")
            break
        if member["nodeId"] in seen:
            problems.append(f"approvedMembers repeats member {member['nodeId']}")
        seen.add(member["nodeId"])
    return problems


def _normalised_members(members: list[dict]) -> dict[str, tuple[str, tuple[str, ...]]]:
    return {m["nodeId"]: (m["releaseVersion"], tuple(sorted(m["features"]))) for m in members}


def _approval_differences(approval: dict, data: dict, cluster_id: str) -> list[str]:
    differences = []
    if cluster_id != approval["clusterId"]:
        differences.append(f"cluster ID {cluster_id} differs from approved {approval['clusterId']}")
    approved = _normalised_members(approval["approvedMembers"])
    current = _normalised_members(data["members"])
    for node_id in sorted(set(approved) | set(current)):
        if node_id not in current:
            differences.append(f"approved member {node_id} is no longer reported")
        elif node_id not in approved:
            differences.append(f"unexpected member {node_id}")
        elif approved[node_id] != current[node_id]:
            differences.append(f"member {node_id} build/features changed: approved {approved[node_id]}, now {current[node_id]}")
    if sorted(approval["approvedActiveFeatures"]) != sorted(data["activeFeatures"]):
        differences.append("activeFeatures differ from the approved set")
    return differences


def _cluster_identity(summaries: list[dict]) -> tuple[str | None, list[str]]:
    """The single clusterId of the successful summaries and the reasons the cluster state cannot be trusted."""
    if not summaries:
        return None, ["no successful GET /api/v1/cluster summary; the cluster identity is not established"]
    raw = [s["body"]["data"]["clusterId"] for s in summaries]
    if not all(isinstance(i, str) and i for i in raw) or len(set(raw)) != 1:
        return None, [f"cluster summaries disagree or are malformed: {raw!r}"]
    ids = set(raw)
    latest = max(summaries, key=lambda o: parse_utc(o["observedAtUtc"]))["body"]["data"]
    problems = []
    if latest.get("quorumAvailable") is not True:
        problems.append("quorumAvailable is not true")
    if latest.get("underReplicated") != 0 or isinstance(latest.get("underReplicated"), bool):
        problems.append("underReplicated is not 0")
    return next(iter(ids)), problems


def _decision(name: str, **fields: Any) -> dict:
    base = {"decision": name, "postAllowed": False, "approvalValid": None, "windowClosed": False,
            "rollbackToUnsupportedBuildAllowed": False, "newTargetApprovalAllowed": True,
            "previousRequestsJudgedFailed": False, "reasons": [], "nextActions": [], "warnings": []}
    base.update(fields)
    return base


def evaluate(feature: str, approval: dict | None, observations: list[dict]) -> dict:
    """Decide from the approval record and the ordered evidence. Pure function; raises ValueError on a bad token.

    postAllowed is true only when no POST exists yet and, in the same evidence, a successful features GET shows
    every non-retired member advertising the feature, a successful cluster summary reports one consistent
    clusterId with quorumAvailable=true and underReplicated=0, and a complete approval matches that clusterId and
    the member/release/features/activeFeatures set exactly. The result files carry no URL, so the caller must
    supply an evidence directory from one run against the approved leader endpoint.
    """
    if not isinstance(feature, str) or not FEATURE_TOKEN.fullmatch(feature):
        raise ValueError("invalid feature token")
    result = _evaluate(feature, approval, observations)
    result["feature"] = feature
    return result


def _evaluate(feature: str, approval: Any, observations: list[dict]) -> dict:
    posts = [o for o in observations if o["method"] == "POST"]
    outcomes = [(o, classify_post(o, feature)) for o in posts]
    features_gets = [o for o in observations if o["method"] == "GET" and _succeeded(o) and _valid_features_body(o["body"])]
    summaries = [o for o in observations if o["method"] == "GET" and _succeeded(o) and isinstance(o["body"], dict)
                 and isinstance(o["body"].get("data"), dict) and "clusterId" in o["body"]["data"]]

    # 1. Commit confirmation: HTTP 200 + token on POST, or a successful GET with the token (§4.1 item 9).
    confirmations = [(o, "POST") for o, outcome in outcomes if outcome == POST_COMMITTED]
    confirmations += [(o, "GET") for o in features_gets if feature in o["body"]["data"]["activeFeatures"]]
    if confirmations:
        first, source = min(confirmations, key=lambda c: parse_utc(c[0]["observedAtUtc"]))
        fields = {"windowClosed": True, "newTargetApprovalAllowed": True, "firstCommitObservedAtUtc": first["observedAtUtc"],
                  "firstCommitObservedBy": f"{source} {first['prefix']}",
                  "nextActions": ["record request, first observation and status in the approval record; observation time is not the commit time",
                                  "recover forward with a build that supports the feature"]}
        if not posts:
            return _decision(ALREADY_ACTIVE, reasons=["the token was already active before any POST; this is existing commit evidence, not a new activation"], **fields)
        return _decision(COMMITTED, reasons=["HTTP 200 with the token or a successful GET with the token confirms the commit"], **fields)

    # 2. A request whose result is unknown remains unknown; no other response proves it failed (§4.1 items 10, 11).
    unknown = [o for o, outcome in outcomes if outcome == POST_UNKNOWN]
    if unknown:
        latest, latest_outcome = outcomes[-1]
        reasons = [f"POST {o['prefix']} result unknown (curl={o['curlExitCode']}, HTTP={o['httpStatus']})" for o in unknown]
        if latest_outcome != POST_UNKNOWN:
            reasons.append(f"latest request {latest['prefix']} is {latest_outcome}; this describes that request only")
        if features_gets:
            reasons.append("a successful GET without the token does not prove that no proposal is pending")
        return _decision(UNKNOWN, reasons=reasons, thisRequestOutcome=latest_outcome, priorUnknownRequests=len(unknown),
                         newTargetApprovalAllowed=False,
                         nextActions=["restore quorum", "confirm the existing approval is still valid", "re-run the same-token POST",
                                      "escalate to the release owner if the commit cannot be confirmed"])

    # 3. Only definitive rejections.
    if outcomes:
        latest, latest_outcome = outcomes[-1]
        if latest_outcome == POST_NOT_ACTIVATED:
            return _decision(NOT_ACTIVATED, thisRequestOutcome=latest_outcome,
                             reasons=["409 feature_not_supported_by_all_members: this request did not activate the feature"],
                             nextActions=["fix the deployment", "obtain a new approval before the next POST"])
        if latest_outcome == POST_NOT_LEADER:
            return _decision(FIND_LEADER, thisRequestOutcome=latest_outcome, reasons=["not_leader"],
                             nextActions=["confirm the current leader endpoint"])
        return _decision(FIX_INPUT, thisRequestOutcome=latest_outcome, reasons=[f"HTTP {latest['httpStatus']}"],
                         nextActions=["fix the input or the credential permission"])

    # 4. No POST yet: gate on the latest successful feature observation.
    if not features_gets:
        return _decision(NO_OBSERVATION, reasons=["no successful, valid GET /api/v1/cluster/features observation"],
                         nextActions=["retrieve the features and keep the response"])
    latest_get = max(features_gets, key=lambda o: parse_utc(o["observedAtUtc"]))
    data = latest_get["body"]["data"]
    gaps = _advertisement_gaps(data["members"], feature)
    if gaps:
        return _decision(HOLD, approvalValid=False if approval is not None else None, reasons=gaps,
                         nextActions=["do not approve and do not POST", "fix the deployment so every non-retired member advertises the feature"])
    cluster_id, health_problems = _cluster_identity(summaries)
    if health_problems:
        return _decision(HOLD_CLUSTER, approvalValid=False if approval is not None else None, reasons=health_problems,
                         nextActions=["do not approve and do not POST", "retrieve a healthy cluster summary with clusterId, quorumAvailable=true and underReplicated=0"])
    if approval is None:
        return _decision(AWAIT_APPROVAL, reasons=["all members advertise the feature and the cluster summary is healthy; no approval record"],
                         nextActions=["the release owner records the approval externally"])
    problems = _approval_problems(approval, feature)
    if problems:
        return _decision(APPROVAL_INCOMPLETE, approvalValid=False, reasons=problems)
    differences = _approval_differences(approval, data, cluster_id)
    if differences:
        return _decision(REAPPROVAL, approvalValid=False, reasons=differences,
                         nextActions=["re-verify the deployment set", "obtain a new approval before the POST"])
    return _decision(POST_PENDING, postAllowed=True, approvalValid=True,
                     reasons=["approval matches the re-read cluster ID, member/build and activeFeatures set; no commit observed, so the rollback window is not closed"],
                     nextActions=["POST once with ClusterAdmin credentials and no body"])


def evaluate_recovery(decision: dict, request: dict) -> dict:
    """Judge a recovery request after `evaluate`. `request`: kind redeploy-build|restore-backup, feature,
    build.features (build to run) and, for restore-backup, backupCommit before-activation|after-activation."""
    kind, feature = request.get("kind"), request.get("feature")
    if kind not in ("redeploy-build", "restore-backup") or not isinstance(feature, str) or not FEATURE_TOKEN.fullmatch(feature):
        raise ValueError("recovery request needs kind and a feature token")
    if feature != decision.get("feature"):
        raise ValueError("recovery request feature differs from the evaluated feature")
    build = request.get("build")
    build_features = build.get("features") if isinstance(build, dict) else None
    if not isinstance(build_features, list) or not all(isinstance(item, str) for item in build_features):
        raise ValueError("recovery request needs build.features")
    if kind == "restore-backup" and request.get("backupCommit") not in ("before-activation", "after-activation"):
        raise ValueError("restore-backup needs backupCommit before-activation|after-activation")
    supports = feature in build_features
    closed = decision["decision"] in (COMMITTED, ALREADY_ACTIVE)
    result = {"windowClosed": closed, "losslessInPlaceRollback": False, "dataLossBoundary": False,
              "separateDisasterRecoveryApprovalRequired": False, "reasons": []}
    if decision["decision"] == UNKNOWN and not supports:
        return {**result, "action": "REFUSE_UNKNOWN_STATE", "reasons": ["commit result unknown; no rollback to a build without the feature"]}
    if not closed:
        return {**result, "action": "PRE_ACTIVATION_COMPATIBILITY_RULES",
                "reasons": ["no commit is confirmed; the pre-activation release compatibility rules apply"]}
    if kind == "redeploy-build":
        if supports:
            return {**result, "action": "SAME_VOLUME_REDEPLOY_ALLOWED",
                    "reasons": ["the build supports the active feature"]}
        return {**result, "action": "REFUSE_IN_PLACE_ROLLBACK", "recommended": "FORWARD_RECOVERY_WITH_SUPPORTED_BUILD",
                "reasons": ["the feature is active; a build that does not support it cannot read the committed state"]}
    if request.get("backupCommit") == "before-activation":
        return {**result, "action": "DISASTER_RECOVERY_SEPARATE_APPROVAL_REQUIRED", "dataLossBoundary": True,
                "separateDisasterRecoveryApprovalRequired": True,
                "reasons": ["restoring a pre-activation backup loses commits made after it; this is not a lossless rollback"]}
    if supports:
        return {**result, "action": "RESTORE_WITH_SUPPORTED_BUILD", "reasons": ["the backup already contains the active feature"]}
    return {**result, "action": "REFUSE_IN_PLACE_ROLLBACK", "recommended": "FORWARD_RECOVERY_WITH_SUPPORTED_BUILD",
            "reasons": ["the restore build does not support the active feature"]}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--feature", required=True)
    parser.add_argument("--evidence-dir", type=Path, required=True)
    parser.add_argument("--approval", type=Path, help="external approval record as JSON; omit when none exists")
    parser.add_argument("--recovery-request", type=Path, help="JSON recovery request judged after the decision")
    args = parser.parse_args(argv)
    approval = _read_json(args.approval) if args.approval else None
    decision = evaluate(args.feature, approval, load_observations(args.evidence_dir))
    output: dict[str, Any] = {"decision": decision}
    if args.recovery_request:
        output["recovery"] = evaluate_recovery(decision, _read_json(args.recovery_request))
    json.dump(output, sys.stdout, ensure_ascii=False, indent=2)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
