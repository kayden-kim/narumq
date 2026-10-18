"""Plan validation, Pod construction, log parsing and judgement for the EKS disposable-emptyDir bootstrap guard.

Spec: docs/specs/cluster-session-and-delivery.md CLU-SCN-136 (§11.20) and ADR-0022. A throw-away Pod runs the broker
image with member 3's real configuration, certificates and peer addresses but an `emptyDir` data directory. The
bootstrap guard must probe the running members, receive a PROGRESSED report (Raft term > 1 or commit index > 3),
refuse with `cluster bootstrap refused` and exit 1 without creating a file. The three real brokers and their PVCs are
never touched.

This module holds no run state. Functions take a fixture-like object (`tests/chaos/eks-worker-recovery/fixture.py`)
so the same code runs with fakes in `tests/test_eks_emptydir_guard.py`. Credentials are never returned in evidence.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import re
import sys
import time
from typing import Callable

RECOVERY_DIR = Path(__file__).resolve().parents[1] / "eks-worker-recovery"
if str(RECOVERY_DIR) not in sys.path:
    sys.path.append(str(RECOVERY_DIR))

from fixture import ready  # noqa: E402

NAMESPACE = "narumq"
STATEFULSET = "narumq"
CONFIGMAP = "narumq-cluster-configs"
SECRET = "narumq-cluster-secrets"
CONTAINER = "narumq"
BROKER_LABEL = "app=narumq"
BROKER_PODS = ("narumq-0", "narumq-1", "narumq-2")
HEADLESS = "narumq-headless"
PEER_PORT = 7000
DATA_MOUNT = "/data"
AUDIT_MOUNT = "/audit"
GUARD_MESSAGE = "cluster bootstrap refused"
RAFT_STATUS_MESSAGE = "raft node status changed"
# ADR-0022: a peer is PROGRESSED when Raft Term > 1 or CommitIndex > 3 (RawNode.Bootstrap ends at term 1, index 3).
INITIAL_TERM = 1
INITIAL_COMMIT_INDEX = 3
SCENARIO = "CLU-SCN-136"
SCENARIO_LABEL = "clu-scn-136"
MANAGED_BY = "eks-emptydir-guard"
GUARD_APP = "narumq-emptydir-guard"
POD_PREFIX = "narumq-emptydir-guard-"
LABEL_ACCEPTANCE_RUN = "narumq.io/acceptance-run"
LABEL_SCENARIO = "narumq.io/scenario"
LABEL_MANAGED_BY = "narumq.io/managed-by"
DISPOSABLE_PREFIXES = ("narumq-ha-acceptance-", "narumq-clu14-")
RUN_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,62}$")
DECIMAL_ID = re.compile(r"^[1-9][0-9]*$")
UINT64_MAX = 2 ** 64 - 1

PLAN_STRINGS = ("runId", "clusterName", "vpcId")
PLAN_KEYS = PLAN_STRINGS + ("targetMemberId", "peerMemberId", "expectedTerm", "expectedCommitIndex")

LISTING_SCRIPT = r"""list_data() {
  for entry in /data/* /data/.[!.]* /data/..?*; do
    if [ -e "$entry" ] || [ -L "$entry" ]; then printf '%s\n' "$entry"; fi
  done
}
if [ -d /data ]; then echo "DATA_DIRECTORY_PRESENT=1"; else echo "DATA_DIRECTORY_PRESENT=0"; fi
echo "EMPTYDIR_BEFORE_BEGIN"
list_data
echo "EMPTYDIR_BEFORE_END"
/narumq -cluster-config "/configs/node-__MEMBER__.json"
code=$?
echo "NARUMQ_EXIT_CODE=$code"
echo "EMPTYDIR_AFTER_BEGIN"
list_data
echo "EMPTYDIR_AFTER_END"
exit "$code"
"""


class GuardRejected(ValueError):
    """A precondition or plan input is not satisfied; `code` is a stable identifier without secrets."""

    def __init__(self, code: str, detail: str = ""):
        super().__init__(code if not detail else f"{code}: {detail}")
        self.code = code


def reject(code: str, detail: str = "") -> None:
    raise GuardRejected(code, detail)


def digest(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


# ---- plan ---------------------------------------------------------------------------------------------------------

def _member_id(value, name: str) -> int:
    if isinstance(value, bool):
        reject(f"plan_invalid:{name}", "must be a positive decimal member id")
    if isinstance(value, str):
        if not DECIMAL_ID.match(value):
            reject(f"plan_invalid:{name}", "must be a canonical positive decimal")
        value = int(value)
    if not isinstance(value, int) or not 1 <= value <= UINT64_MAX:
        reject(f"plan_invalid:{name}", "must be a positive decimal member id")
    return value


def _positive(value, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        reject(f"plan_invalid:{name}", "must be a positive integer")
    return value


def load_plan(raw, run_id: str) -> dict:
    """Validate the plan; member ids become ints. `expectedTerm`/`expectedCommitIndex` are optional plan-level
    expectations (the numbers the fixture itself produced, e.g. 6 and 100); the live values are always read."""
    if not isinstance(raw, dict):
        reject("plan_invalid:plan", "must be an object")
    unknown = sorted(set(raw) - set(PLAN_KEYS))
    if unknown:
        reject("plan_invalid:unknown_key", ",".join(unknown))
    plan = dict(raw)
    for key in PLAN_STRINGS:
        if not isinstance(plan.get(key), str) or not plan[key]:
            reject(f"plan_missing:{key}")
    for key in ("targetMemberId", "peerMemberId"):
        if key not in plan:
            reject(f"plan_missing:{key}")
    plan["targetMemberId"] = _member_id(plan["targetMemberId"], "targetMemberId")
    plan["peerMemberId"] = _member_id(plan["peerMemberId"], "peerMemberId")
    for key in ("expectedTerm", "expectedCommitIndex"):
        if key in plan:
            plan[key] = _positive(plan[key], key)
    if not RUN_ID.match(run_id) or plan["runId"] != run_id:
        reject("plan_invalid:runId", "plan does not identify this run")
    if not plan["clusterName"].startswith(DISPOSABLE_PREFIXES):
        reject("plan_invalid:clusterName", "plan does not identify a disposable acceptance fixture")
    if plan["targetMemberId"] == plan["peerMemberId"]:
        reject("plan_invalid:peerMemberId", "peer must differ from the target member")
    return plan


def guard_pod_name(run_id: str) -> str:
    return POD_PREFIX + digest(run_id)[:10]


def slot_dns(ordinal: int) -> str:
    return f"narumq-{ordinal}.{HEADLESS}.{NAMESPACE}.svc.cluster.local"


def slot_address(ordinal: int) -> str:
    return f"{slot_dns(ordinal)}:{PEER_PORT}"


# ---- live fixture reads -------------------------------------------------------------------------------------------

def slot_map(configmap: dict) -> dict[int, int]:
    """member id -> StatefulSet ordinal from the live `slot-<ordinal>` ConfigMap keys."""
    slots: dict[int, int] = {}
    for key, value in (configmap.get("data") or {}).items():
        match = re.fullmatch(r"slot-([0-9]+)", key)
        if not match:
            continue
        if not isinstance(value, str) or not DECIMAL_ID.match(value):
            reject("slot_invalid", key)
        slots[int(value)] = int(match.group(1))
    return slots


def member_config(configmap: dict, plan: dict) -> dict:
    """Validate member `targetMemberId`'s real config against the live slots and return the facts the Pod needs."""
    slots = slot_map(configmap)
    target, peer = plan["targetMemberId"], plan["peerMemberId"]
    if target not in slots:
        reject("target_member_slot_missing", str(target))
    if peer not in slots:
        reject("peer_member_slot_missing", str(peer))
    raw = (configmap.get("data") or {}).get(f"node-{target}.json")
    if not isinstance(raw, str):
        reject("target_member_config_missing", f"node-{target}.json")
    try:
        config = json.loads(raw)
    except ValueError:
        reject("target_member_config_invalid", "not JSON")
    if not isinstance(config, dict) or config.get("nodeId") != target:
        reject("target_member_config_invalid", "nodeId differs from targetMemberId")
    if config.get("role") != "bootstrap" or config.get("mode") != "cluster":
        reject("target_member_config_not_bootstrap")
    if config.get("directory") != DATA_MOUNT:
        reject("target_member_config_invalid", f"directory must be {DATA_MOUNT}")
    if (config.get("management") or {}).get("auditDirectory") != AUDIT_MOUNT:
        reject("target_member_config_invalid", f"auditDirectory must be {AUDIT_MOUNT}")
    tls = config.get("peerTLS") or {}
    if (tls.get("certFile"), tls.get("keyFile"), tls.get("caFile")) != (
            "/peer-tls/node.crt", "/peer-tls/node.key", "/peer-tls/ca.crt"):
        reject("target_member_config_invalid", "peerTLS paths differ from the StatefulSet layout")
    ordinal = slots[target]
    if config.get("advertisedAddress") != slot_address(ordinal):
        reject("target_member_config_invalid", "advertisedAddress differs from the slot address")
    members = {item.get("memberId"): item for item in config.get("bootstrapMembers") or [] if isinstance(item, dict)}
    if target not in members or peer not in members:
        reject("target_member_config_invalid", "bootstrapMembers lack the target or peer")
    if members[peer].get("address") != slot_address(slots[peer]):
        reject("peer_member_address_mismatch", str(peer))
    peers = [{"memberId": member_id, "address": item.get("address")}
             for member_id, item in sorted(members.items()) if member_id != target]
    return {"targetOrdinal": ordinal, "peerOrdinal": slots[peer], "peers": peers, "config": f"node-{target}.json"}


def pod_summary(pod: dict) -> dict:
    status = next((s for s in pod.get("status", {}).get("containerStatuses", []) if s.get("name") == CONTAINER), {})
    return {"name": pod["metadata"]["name"], "uid": pod["metadata"]["uid"], "nodeName": pod.get("spec", {}).get("nodeName"),
            "ready": ready(pod), "restartCount": status.get("restartCount"),
            "deleting": bool(pod["metadata"].get("deletionTimestamp"))}


def broker_pods(fixture) -> list[dict]:
    pods = (fixture.get("pods", "-n", NAMESPACE, "-l", BROKER_LABEL) or {}).get("items", [])
    return sorted((pod_summary(pod) for pod in pods), key=lambda item: item["name"])


def three_ready(pods: list[dict]) -> bool:
    return [pod["name"] for pod in pods] == list(BROKER_PODS) and all(
        pod["ready"] and not pod["deleting"] for pod in pods)


def unchanged(before: list[dict], after: list[dict]) -> bool:
    """Same Pod UIDs with the same restart counts: nothing was rescheduled or restarted by the guard run."""
    return [(p["name"], p["uid"], p["restartCount"]) for p in before] == [
        (p["name"], p["uid"], p["restartCount"]) for p in after]


def broker_image(statefulset: dict) -> str:
    spec = (statefulset or {}).get("spec", {})
    if spec.get("replicas") != 3:
        reject("statefulset_not_three_replicas")
    for container in spec.get("template", {}).get("spec", {}).get("containers", []):
        if container.get("name") == CONTAINER and isinstance(container.get("image"), str) and container["image"]:
            return container["image"]
    reject("broker_image_missing")


def instance_of(node: dict) -> str:
    return str(node.get("spec", {}).get("providerID", "")).rsplit("/", 1)[-1]


# ---- the guard Pod ------------------------------------------------------------------------------------------------

def guard_labels(run_id: str) -> dict:
    return {"app": GUARD_APP, LABEL_ACCEPTANCE_RUN: run_id, LABEL_SCENARIO: SCENARIO_LABEL, LABEL_MANAGED_BY: MANAGED_BY}


def build_pod(plan: dict, image: str, target_ordinal: int) -> dict:
    """The member's real config/certs/peers on an emptyDir `/data`. No PVC, no Service selection, no `app=narumq`."""
    member = plan["targetMemberId"]
    return {
        "apiVersion": "v1", "kind": "Pod",
        "metadata": {"name": guard_pod_name(plan["runId"]), "namespace": NAMESPACE, "labels": guard_labels(plan["runId"]),
                     "annotations": {"narumq.io/target-member-id": str(member),
                                     "narumq.io/peer-member-id": str(plan["peerMemberId"]),
                                     "narumq.io/acceptance-run-id": plan["runId"]}},
        "spec": {
            "restartPolicy": "Never", "terminationGracePeriodSeconds": 10, "automountServiceAccountToken": False,
            "securityContext": {"runAsUser": 10001, "runAsGroup": 10001, "fsGroup": 10001},
            "containers": [{
                "name": CONTAINER, "image": image, "imagePullPolicy": "IfNotPresent",
                "command": ["/bin/sh", "-c", LISTING_SCRIPT.replace("__MEMBER__", str(member))],
                "securityContext": {"allowPrivilegeEscalation": False, "readOnlyRootFilesystem": True},
                "resources": {"requests": {"cpu": "100m", "memory": "256Mi"},
                              "limits": {"cpu": "1", "memory": "1Gi"}},
                "volumeMounts": [
                    {"name": "configs", "mountPath": "/configs", "readOnly": True},
                    {"name": "cluster-secrets", "mountPath": "/secrets", "readOnly": True},
                    {"name": "peer-tls", "mountPath": "/peer-tls", "readOnly": True},
                    {"name": "data", "mountPath": DATA_MOUNT},
                    {"name": "audit", "mountPath": AUDIT_MOUNT},
                ],
            }],
            "volumes": [
                {"name": "configs", "configMap": {"name": CONFIGMAP, "defaultMode": 0o444}},
                {"name": "cluster-secrets", "secret": {"secretName": SECRET, "defaultMode": 0o440, "items": [
                    {"key": "admin.token", "path": "admin.token"},
                    {"key": "dashboard.key", "path": "dashboard.key"},
                    {"key": "security.json", "path": "security.json"}]}},
                {"name": "peer-tls", "secret": {"secretName": SECRET, "defaultMode": 0o440, "items": [
                    {"key": f"node-{target_ordinal}.crt", "path": "node.crt"},
                    {"key": f"node-{target_ordinal}.key", "path": "node.key"},
                    {"key": "ca.crt", "path": "ca.crt"}]}},
                {"name": "data", "emptyDir": {}},
                {"name": "audit", "emptyDir": {}},
            ],
        },
    }


def owned_guard_pod(pod: dict | None, run_id: str) -> bool:
    labels = ((pod or {}).get("metadata") or {}).get("labels") or {}
    return all(labels.get(key) == value for key, value in guard_labels(run_id).items())


def container_status(pod: dict | None) -> dict | None:
    if not pod:
        return None
    return next((s for s in pod.get("status", {}).get("containerStatuses", []) if s.get("name") == CONTAINER), None)


def terminated_state(pod: dict | None) -> dict | None:
    status = container_status(pod)
    return ((status or {}).get("state") or {}).get("terminated")


def waiting_reason(pod: dict | None) -> str | None:
    status = container_status(pod)
    return (((status or {}).get("state") or {}).get("waiting") or {}).get("reason")


# ---- log parsing --------------------------------------------------------------------------------------------------

REPORT = re.compile(r"member ([0-9]+) term ([0-9]+) commit index ([0-9]+)\)")
EXIT_MARKER = re.compile(r"^NARUMQ_EXIT_CODE=([0-9]+)$", re.MULTILINE)
TEXT_STATUS = re.compile(r"raft node status changed.*?node_id=([0-9]+).*?term=([0-9]+)")


def parse_refusals(logs: str) -> list[dict]:
    """Every refusal line with the peer report `(member M term T commit index C)` the guard appends."""
    found = []
    for line in logs.splitlines():
        if GUARD_MESSAGE not in line:
            continue
        match = REPORT.search(line)
        found.append({"line": line.strip()[:400], "memberId": int(match.group(1)) if match else None,
                      "term": int(match.group(2)) if match else None,
                      "commitIndex": int(match.group(3)) if match else None})
    return found


def parse_exit_marker(logs: str) -> int | None:
    markers = EXIT_MARKER.findall(logs)
    return int(markers[0]) if len(markers) == 1 else None


def parse_listing(logs: str, label: str) -> list[str] | None:
    """Entries between EMPTYDIR_<label>_BEGIN/END; None unless both markers appear exactly once, in order."""
    lines = [line.strip() for line in logs.splitlines()]
    begin, end = f"EMPTYDIR_{label}_BEGIN", f"EMPTYDIR_{label}_END"
    if lines.count(begin) != 1 or lines.count(end) != 1 or lines.index(begin) > lines.index(end):
        return None
    return [entry for entry in lines[lines.index(begin) + 1:lines.index(end)] if entry]


def data_directory_present(logs: str) -> bool | None:
    values = re.findall(r"^DATA_DIRECTORY_PRESENT=([01])$", logs, re.MULTILINE)
    return values[0] == "1" if len(values) == 1 else None


def latest_raft_term(logs: str, member_id: int) -> int | None:
    """Term of the last `raft node status changed` entry the member itself logged. The broker logs this whenever
    its role, leader or term changes, so the last entry is its current term."""
    term = None
    for line in logs.splitlines():
        if RAFT_STATUS_MESSAGE not in line:
            continue
        try:
            entry = json.loads(line)
        except ValueError:
            entry = None
        if isinstance(entry, dict):
            if entry.get("msg") == RAFT_STATUS_MESSAGE and str(entry.get("node_id")) == str(member_id) \
                    and isinstance(entry.get("term"), int):
                term = entry["term"]
            continue
        match = TEXT_STATUS.search(line)
        if match and int(match.group(1)) == member_id:
            term = int(match.group(2))
    return term


# ---- live reads through kubectl and the management API -----------------------------------------------------------

def fetch_logs(fixture, pod_name: str) -> str:
    return fixture.command(fixture.kubectl + ["-n", NAMESPACE, "logs", pod_name, "-c", CONTAINER], timeout=120)


def peer_applied_index(fixture, port: int, member_id: int) -> int:
    """`lastAppliedIndex` of the member as reported by its own management API (the API shows no commit index)."""
    nodes = fixture.api("/api/v1/nodes", port)
    if not isinstance(nodes, list):
        reject("peer_nodes_response_invalid")
    row = next((node for node in nodes if str(node.get("nodeId")) == str(member_id)), None)
    if row is None or row.get("queryStatus") != "OK":
        reject("peer_node_unavailable", str(member_id))
    index = row.get("lastAppliedIndex")
    if isinstance(index, bool) or not isinstance(index, int) or index < 0:
        reject("peer_applied_index_invalid", str(member_id))
    return index


def quorum_available(fixture, port: int) -> bool:
    return fixture.api("/api/v1/cluster", port).get("quorumAvailable") is True


def wait_for_termination(fixture, name: str, uid: str, timeout: float, interval: float = 3.0,
                         sleep: Callable = time.sleep, monotonic: Callable = time.monotonic) -> dict:
    """Poll the guard Pod until its container has terminated. Observation only. Raises GuardRejected on a vanished or
    replaced Pod and on timeout."""
    deadline = monotonic() + timeout
    last = None
    while True:
        pod = fixture.get_pod(name)
        if pod is None:
            reject("guard_pod_vanished")
        if pod["metadata"]["uid"] != uid:
            reject("guard_pod_replaced")
        if terminated_state(pod):
            return pod
        last = {"phase": pod.get("status", {}).get("phase"), "waitingReason": waiting_reason(pod)}
        if monotonic() >= deadline:
            reject("guard_timeout", json.dumps(last, sort_keys=True))
        sleep(interval)


def wait_for_applied(fixture, port: int, member_id: int, at_least: int, timeout: float, interval: float = 2.0,
                     sleep: Callable = time.sleep, monotonic: Callable = time.monotonic) -> int:
    """Read the member's applied index; poll until it has reached `at_least` (the commit index the guard saw)."""
    deadline = monotonic() + timeout
    while True:
        applied = peer_applied_index(fixture, port, member_id)
        if applied >= at_least or monotonic() >= deadline:
            return applied
        sleep(interval)


# ---- judgement ----------------------------------------------------------------------------------------------------

CRITERIA = (
    "plan.fixtureOwnedAndThreeReadyBefore", "quorum.availableBefore",
    "guard.exit1", "guard.refusalLogged", "guard.reportFromProgressedPeer", "guard.progressBoundaryExceeded",
    "guard.reportedTermEqualsLive", "guard.reportedCommitIndexMatchesLiveApplied", "guard.reportMatchesPlanExpectation",
    "emptyDir.listedBeforeAndAfter", "emptyDir.unchangedAndEmpty",
    "brokers.threeReadyUnchangedAfter", "quorum.availableAfter", "cleanup.guardPodDeleted",
)


def judge_guard(plan: dict, observation: dict) -> dict:
    """Judge one run. Returns `criteria` (each exactly True or False) and the facts they were derived from.

    observation keys: exitCode (container terminated state), logs, peer{termBefore,termAfter,appliedBefore,appliedAfter}.
    The termination state/logs/peer values may be None when they could not be read: the criterion is then False.
    """
    logs = observation.get("logs") or ""
    peer = observation.get("peer") or {}
    refusals = parse_refusals(logs)
    report = refusals[0] if refusals else None
    marker = parse_exit_marker(logs)
    before, after = parse_listing(logs, "BEFORE"), parse_listing(logs, "AFTER")
    term, commit = (report or {}).get("term"), (report or {}).get("commitIndex")
    applied_before, applied_after = peer.get("appliedBefore"), peer.get("appliedAfter")
    criteria = {
        "guard.exit1": observation.get("exitCode") == 1 and marker == 1,
        "guard.refusalLogged": len(refusals) >= 1,
        "guard.reportFromProgressedPeer": report is not None and report["memberId"] == plan["peerMemberId"],
        "guard.progressBoundaryExceeded": term is not None and commit is not None
        and (term > INITIAL_TERM or commit > INITIAL_COMMIT_INDEX),
        "guard.reportedTermEqualsLive": term is not None and peer.get("termBefore") == term == peer.get("termAfter"),
        "guard.reportedCommitIndexMatchesLiveApplied": commit is not None and applied_before is not None
        and applied_after is not None and applied_before <= commit <= applied_after,
        "guard.reportMatchesPlanExpectation": term is not None and commit is not None
        and plan.get("expectedTerm", term) == term and plan.get("expectedCommitIndex", commit) == commit,
        "emptyDir.listedBeforeAndAfter": before is not None and after is not None
        and data_directory_present(logs) is True,
        "emptyDir.unchangedAndEmpty": before == [] and after == [],
    }
    exact = (applied_before is not None and applied_before == applied_after == commit)
    return {"criteria": criteria,
            "facts": {"refusals": len(refusals), "report": report, "exitMarker": marker,
                      "emptyDirBefore": before, "emptyDirAfter": after, "peer": peer,
                      "commitIndexEqualsLiveAppliedExactly": exact}}


def aggregate(criteria: dict, failed_step: str | None = None, error_code: str | None = None) -> dict:
    """PASS only when every named criterion is exactly True and no step failed."""
    missing = [name for name in CRITERIA if name not in criteria]
    failed = [name for name in CRITERIA if criteria.get(name) is not True]
    passed = not missing and not failed and failed_step is None and error_code is None
    return {"result": "PASS" if passed else "FAIL", "failedCriteria": failed, "failedStep": failed_step,
            "errorCode": error_code}
