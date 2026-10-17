"""Pure decisions for the DEP-21 EBS disk pressure acceptance: plan checks, fill sizes, and judging.

Nothing here talks to Kubernetes, AWS, or MQTT, so every rule can be tested without a fixture.
"""
from __future__ import annotations

import datetime
import json
from pathlib import Path
import re

MIB = 1 << 20
GIB = 1 << 30

# Process configuration v1 default; the broker clears pressure at minFreeBytes + minFreeBytes/8.
MIN_FREE_BYTES = GIB
CLEAR_FREE_BYTES = MIN_FREE_BYTES + MIN_FREE_BYTES // 8
# Shrink target that leaves headroom above CLEAR_FREE_BYTES for broker writes during the recovery probe.
RECOVERY_FREE_BYTES = CLEAR_FREE_BYTES + 128 * MIB
D1_TARGET_FREE_BYTES = 512 * MIB
D1_FREE_TOLERANCE_BYTES = 128 * MIB
# Busybox df reports f_bavail (the non-root view the broker uses); at or below this the filesystem counts as full.
FULL_AVAILABLE_BYTES = MIB

PUBACK_QUOTA_EXCEEDED = 0x97
PUBACK_FAILURE_FLOOR = 0x80
D1_PUBACK_LIMIT_SECONDS = 10.0
D2_DETECTION_LIMIT_SECONDS = 60.0
D3_DETECTION_LIMIT_SECONDS = 120.0
RECOVERY_LIMIT_SECONDS = 900.0

SCENARIOS = ("D1", "D2", "D3")
STORAGE_FAILURE_CODES = ("storage_no_space", "storage_io_error")

POD_RE = re.compile(r"^narumq-[0-9]+$")
NODE_ID_RE = re.compile(r"^[1-9][0-9]*$")
SCENARIO_PREFIX_RE = re.compile(r"^[a-z0-9][a-z0-9-]{0,40}$")
IMAGE_RE = re.compile(r"^\S+@sha256:[0-9a-f]{64}$")

# A detached session holds at most 64 durable messages; the oracle stays below that so no target is Saturated.
DURABLE_MAX_MESSAGES = 60
DURABLE_INTERVAL_SECONDS = 3.0
# Retained QoS 1 write load after the fill: every publish appends its payload to each member's WAL.
# 2 topics x 128 KiB keep retained state at 256 KiB. A serialized Raft snapshot above 1 MiB cannot be sent to a
# restarted follower (cluster spec limit), so the retained state must stay small while the WAL grows.
LOAD_PAYLOAD_BYTES = 128 * 1024
LOAD_TOPICS = 2
LOAD_RETAINED_LIMIT_BYTES = 256 * 1024
LOAD_DEADLINE_SECONDS = 300.0

PLAN_KEYS = {"runId", "clusterName", "targetPod", "targetPodUID", "secondTargetPod", "secondTargetPodUID",
             "observerPod", "observerPodUID", "brokerImage", "durableScenario", "targetNodeId",
             "secondTargetNodeId", "observerNodeId", "selfRecoverySeconds", "d3HoldSeconds",
             "durableIntervalSeconds", "durableMaxMessages", "loadPayloadBytes", "loadTopics",
             "loadIntervalSeconds", "loadDeadlineSeconds"}


def timestamp_seconds(value: str) -> float:
    return datetime.datetime.fromisoformat(value).timestamp()


def write_json(output: Path, name: str, value) -> None:
    (output / name).write_text(json.dumps(value, indent=2), encoding="utf-8")


def pod_ready(obj: dict) -> bool:
    return any(c.get("type") == "Ready" and c.get("status") == "True"
               for c in (obj.get("status") or {}).get("conditions", []))


# --- plan and roles --------------------------------------------------------------------------

def parse_scenarios(value: str | None) -> list[str]:
    if not value:
        return list(SCENARIOS)
    names = [part.strip().upper() for part in value.split(",") if part.strip()]
    if not names or any(name not in SCENARIOS for name in names) or len(set(names)) != len(names):
        raise ValueError("--only must list distinct scenarios from D1,D2,D3")
    return [name for name in SCENARIOS if name in names]


def default_node_id(pod: str) -> str:
    """ADR-0061 initial slot manifest: ordinal N runs member N+1. A plan may override after a reseed."""
    return str(int(pod.rsplit("-", 1)[1]) + 1)


def validate_plan(plan: dict, scenarios: list[str], run_id: str) -> dict:
    if not isinstance(plan, dict):
        raise ValueError("plan must be a JSON object")
    unknown = set(plan) - PLAN_KEYS
    if unknown:
        raise ValueError("unknown plan keys: " + ",".join(sorted(unknown)))
    required = ["runId", "clusterName", "targetPod", "targetPodUID", "observerPod", "observerPodUID"]
    if "D3" in scenarios:
        required += ["secondTargetPod", "secondTargetPodUID"]
    if not all(isinstance(plan.get(key), str) and plan[key] for key in required):
        raise ValueError("incomplete storage pressure plan")
    if plan["runId"] != run_id or not plan["clusterName"].startswith("narumq-ha-acceptance-"):
        raise ValueError("plan does not identify this disposable fixture")
    for key in ("targetPod", "secondTargetPod", "observerPod"):
        if key in plan and not (isinstance(plan[key], str) and POD_RE.match(plan[key])):
            raise ValueError(f"{key} is not a narumq StatefulSet Pod name")
    normalized = dict(plan)
    prefix = plan.get("durableScenario", "storage")
    if not isinstance(prefix, str) or not SCENARIO_PREFIX_RE.match(prefix):
        raise ValueError("durableScenario prefix must match [a-z0-9][a-z0-9-]{0,40}")
    normalized["durableScenario"] = prefix
    for key, pod_key in (("targetNodeId", "targetPod"), ("secondTargetNodeId", "secondTargetPod"),
                         ("observerNodeId", "observerPod")):
        if key in plan:
            if not (isinstance(plan[key], str) and NODE_ID_RE.match(plan[key])):
                raise ValueError(f"{key} must be a positive decimal member id")
        elif pod_key in plan:
            normalized[key] = default_node_id(plan[pod_key])
    for key, low, high, default in (("selfRecoverySeconds", 1, 600, 120), ("d3HoldSeconds", 0, 300, 20),
                                    ("durableIntervalSeconds", 0.2, 30, DURABLE_INTERVAL_SECONDS),
                                    ("loadPayloadBytes", 1024, 512 * 1024, LOAD_PAYLOAD_BYTES),
                                    ("loadTopics", 1, 16, LOAD_TOPICS), ("loadIntervalSeconds", 0.1, 10, 1.0),
                                    ("loadDeadlineSeconds", 30, 900, LOAD_DEADLINE_SECONDS)):
        value = plan.get(key, default)
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not low <= value <= high:
            raise ValueError(f"{key} must be within [{low}, {high}]")
        if key in ("loadPayloadBytes", "loadTopics") and value != int(value):
            raise ValueError(f"{key} must be an integer")
        normalized[key] = int(value) if key in ("loadPayloadBytes", "loadTopics") else value
    retained = normalized["loadPayloadBytes"] * normalized["loadTopics"]
    if retained > LOAD_RETAINED_LIMIT_BYTES:
        raise ValueError(f"loadPayloadBytes * loadTopics must be at most {LOAD_RETAINED_LIMIT_BYTES} bytes of "
                         f"retained state, got {retained}")
    value = plan.get("durableMaxMessages", DURABLE_MAX_MESSAGES)
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= DURABLE_MAX_MESSAGES:
        raise ValueError(f"durableMaxMessages must be an integer within [1, {DURABLE_MAX_MESSAGES}]")
    normalized["durableMaxMessages"] = value
    if "brokerImage" in plan and not (isinstance(plan["brokerImage"], str) and IMAGE_RE.match(plan["brokerImage"])):
        raise ValueError("brokerImage must be an image reference with a sha256 digest")
    return normalized


def durable_scenario(plan: dict, scenario: str) -> str:
    return f"{plan['durableScenario']}-{scenario.lower()}"


def check_roles(plan: dict, leader_node_id: str, scenarios: list[str]) -> None:
    """Targets must be followers; the observer is never filled and is a different Pod from every target."""
    pods = [plan["targetPod"]]
    nodes = [plan["targetNodeId"]]
    if "D3" in scenarios:
        pods.append(plan["secondTargetPod"])
        nodes.append(plan["secondTargetNodeId"])
    if plan["observerPod"] in pods or plan["observerNodeId"] in nodes:
        raise ValueError("observer must not be a fill target")
    if len(set(pods)) != len(pods) or len(set(nodes)) != len(nodes):
        raise ValueError("fill targets must be distinct")
    leader = str(leader_node_id)
    if leader in nodes:
        raise ValueError("fill targets must be followers")
    if "D3" in scenarios and plan["observerNodeId"] != leader:
        raise ValueError("D3 fills both followers, so the observer must be the leader")


def recovery_problems(pods: list[dict], pvc_uids: dict[str, str], expected_pvc_uids: dict[str, str],
                      cluster: dict | None) -> list[str]:
    """Reasons the cluster has not recovered; an empty list means three Ready brokers on the same PVCs."""
    problems = []
    if len(pods) != 3:
        problems.append("pod_count")
    if not all(pod_ready(pod) and not pod["metadata"].get("deletionTimestamp") for pod in pods):
        problems.append("pod_not_ready")
    for pvc, uid in expected_pvc_uids.items():
        if pvc_uids.get(pvc) != uid:
            problems.append(f"pvc_changed:{pvc}")
    if not cluster or cluster.get("quorumAvailable") is not True or (cluster.get("nodesByState") or {}).get("ready") != 3:
        problems.append("cluster_not_converged")
    return problems


# --- df parsing and ballast sizing -----------------------------------------------------------

def parse_df(text: str) -> dict:
    """Parse busybox `df -k <path>` output; a wrapped filesystem name still yields six tokens."""
    lines = [line for line in text.splitlines() if line.strip()]
    start = next((i for i, line in enumerate(lines) if line.split()[:1] == ["Filesystem"]), None)
    if start is None:
        raise ValueError("df header not found")
    tokens = " ".join(lines[start + 1:]).split()
    if len(tokens) != 6 or not tokens[4].endswith("%"):
        raise ValueError("unexpected df row")
    blocks, used, available = (int(tokens[i]) for i in (1, 2, 3))
    if min(blocks, used, available) < 0 or available > blocks:
        raise ValueError("inconsistent df numbers")
    return {"totalBytes": blocks * 1024, "usedBytes": used * 1024, "availableBytes": available * 1024,
            "mount": tokens[5]}


def fill_bytes(available: int, target_free: int) -> int:
    """MiB-aligned bytes to add so that free space drops to target_free; zero when already at or below it."""
    if available <= target_free:
        return 0
    return (available - target_free) // MIB * MIB


def shrink_bytes(available: int, target_free: int, ballast_bytes: int) -> int:
    """MiB-aligned bytes to free so that free space reaches at least target_free."""
    if available >= target_free:
        return 0
    needed = -(-(target_free - available) // MIB) * MIB
    if needed > ballast_bytes:
        raise ValueError("ballast is smaller than the space that must be released")
    return needed


def adjust_action(available: int, target_free: int, tolerance: int, ballast_bytes: int) -> dict:
    """Decide how to bring free space within [target_free - tolerance, target_free + tolerance]."""
    if available > target_free + tolerance:
        amount = fill_bytes(available, target_free)
        return {"action": "fill", "bytes": amount} if amount else {"action": "none", "bytes": 0}
    if available < target_free - tolerance:
        return {"action": "shrink", "bytes": shrink_bytes(available, target_free, ballast_bytes)}
    return {"action": "none", "bytes": 0}


def release_action(available: int, minimum_free: int, ballast_bytes: int) -> dict:
    """Shrink only when free space is below the minimum; never adds ballast."""
    amount = shrink_bytes(available, minimum_free, ballast_bytes)
    return {"action": "shrink", "bytes": amount} if amount else {"action": "none", "bytes": 0}


def is_full(available: int) -> bool:
    return available <= FULL_AVAILABLE_BYTES


def dd_exit_code(output: str) -> int | None:
    match = re.search(r"DD_EXIT=([0-9]+)", output)
    return int(match.group(1)) if match else None


# --- judging ----------------------------------------------------------------------------------

def reason_counts(samples: list[dict]) -> dict:
    counts: dict[str, int] = {}
    for sample in samples:
        key = sample.get("outcome") if sample.get("reasonCode") is None else f"0x{sample['reasonCode']:02x}"
        counts[str(key)] = counts.get(str(key), 0) + 1
    return counts


def judge_d1_pressure(target_samples: list[dict], observer_samples: list[dict],
                      limit: float = D1_PUBACK_LIMIT_SECONDS) -> dict:
    """The target must answer a QoS 1 PUBLISH with PUBACK 0x97 within `limit` seconds of the first attempt."""
    rejection = next((s for s in target_samples if s.get("reasonCode") == PUBACK_QUOTA_EXCEEDED), None)
    seconds = None
    if rejection is not None and target_samples:
        seconds = rejection["finishedSeconds"] - target_samples[0]["startedSeconds"]
    observer_failed = [s for s in observer_samples if s.get("puback") is not True]
    reasons = []
    if seconds is None:
        reasons.append("no_0x97")
    elif seconds > limit:
        reasons.append("0x97_after_limit")
    if not observer_samples:
        reasons.append("observer_not_sampled")
    if observer_failed:
        reasons.append("observer_publish_failed")
    return {"result": "PASS" if not reasons else "FAIL", "reasons": reasons, "limitSeconds": limit,
            "firstQuotaRejectionSeconds": seconds, "targetOutcomes": reason_counts(target_samples),
            "observer": {"samples": len(observer_samples), "failed": len(observer_failed)}}


def judge_d1_recovery(samples: list[dict], zero_seconds: float, limit: float = D1_PUBACK_LIMIT_SECONDS) -> dict:
    """After free space reaches the clear threshold, a target publish must succeed within `limit` seconds."""
    success = next((s for s in samples if s.get("puback") is True and s["finishedSeconds"] >= zero_seconds), None)
    seconds = None if success is None else success["finishedSeconds"] - zero_seconds
    reasons = []
    if seconds is None:
        reasons.append("no_success_after_release")
    elif seconds > limit:
        reasons.append("success_after_limit")
    return {"result": "PASS" if not reasons else "FAIL", "reasons": reasons, "limitSeconds": limit,
            "firstSuccessSeconds": seconds}


def storage_failure_view(node: dict | None) -> dict:
    """Exact node fields that decide whether a storage failure is visible; absent node data is not visible."""
    fields = ("nodeId", "queryStatus", "queryErrorCode", "state", "ready", "readinessReason", "storagePressure",
              "replicationHealth", "lastErrorCode")
    if not isinstance(node, dict):
        return {"visible": False, "fields": None}
    view = {field: node.get(field) for field in fields}
    failed = view["storagePressure"] is True and "FAILED" in (view["state"], view["replicationHealth"])
    visible = view["queryStatus"] == "OK" and (view["lastErrorCode"] in STORAGE_FAILURE_CODES or failed)
    return {"visible": visible, "fields": view}


LOG_NEEDLE = "no space left on device"


def scan_log(text: str, needle: str = LOG_NEEDLE) -> tuple[bool, str | None]:
    """Whether any log line holds `needle`, and the `msg` field of the first such JSON line (never the line)."""
    for line in text.splitlines():
        if needle in line:
            try:
                value = json.loads(line)
            except ValueError:
                return True, None
            msg = value.get("msg") if isinstance(value, dict) else None
            return True, msg[:200] if isinstance(msg, str) else None
    return False, None


def judge_d2_detection(pod_not_ready_seconds: float | None, storage_failure_seconds: float | None,
                       limit: float = D2_DETECTION_LIMIT_SECONDS) -> dict:
    reasons = []
    for name, value in (("pod_not_ready", pod_not_ready_seconds), ("storage_failure_not_visible", storage_failure_seconds)):
        if value is None:
            reasons.append(name)
        elif value > limit:
            reasons.append(name + "_after_limit")
    return {"result": "PASS" if not reasons else "FAIL", "reasons": reasons, "limitSeconds": limit,
            "podNotReadySeconds": pod_not_ready_seconds, "storageFailureVisibleSeconds": storage_failure_seconds}


def read_publish_journal(path: Path) -> list[dict]:
    rows = []
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                rows.append(json.loads(line))
    return rows


def judge_d3_window(rows: list[dict], window_start: str | None, window_end: str | None) -> dict:
    """No message sent while both followers were failed may have been acknowledged.

    An empty window proves nothing, so it fails with its own reason instead of passing vacuously.
    """
    if window_start is None or window_end is None:
        return {"result": "FAIL", "reasons": ["window_not_established"], "windowStart": window_start,
                "windowEnd": window_end}
    start, end = timestamp_seconds(window_start), timestamp_seconds(window_end)
    if end < start:
        return {"result": "FAIL", "reasons": ["window_inverted"], "windowStart": window_start, "windowEnd": window_end}
    inside = [row for row in rows if start <= timestamp_seconds(row["sentAt"]) <= end]
    acked = [row["seq"] for row in inside if row.get("outcome") == "acked"]
    outcomes: dict[str, int] = {}
    for row in inside:
        outcomes[row.get("outcome")] = outcomes.get(row.get("outcome"), 0) + 1
    reasons = []
    if not inside:
        reasons.append("window_without_messages")
    if acked:
        reasons.append("acked_in_window")
    return {"result": "PASS" if not reasons else "FAIL", "reasons": reasons, "windowStart": window_start,
            "windowEnd": window_end, "sentInWindow": len(inside), "outcomesInWindow": outcomes,
            "ackedSeqInWindow": acked}


def load_summary(samples: list[dict]) -> dict:
    """Bytes and PUBACK outcomes of the retained write load; the load itself is never judged."""
    acked = [s for s in samples if s.get("puback") is True]
    return {"attempts": len(samples), "attemptedBytes": sum(s.get("payloadBytes", 0) for s in samples),
            "ackedMessages": len(acked), "ackedBytes": sum(s.get("payloadBytes", 0) for s in acked),
            "outcomes": reason_counts(samples)}


def error_location(exc: BaseException) -> str:
    """`file:line function` of the innermost traceback frame; no message text, so nothing secret is kept."""
    import traceback
    frames = traceback.extract_tb(exc.__traceback__)
    if not frames:
        return "unknown"
    frame = frames[-1]
    return f"{Path(frame.filename).name}:{frame.lineno} {frame.name}"
