"""Pure judgments for DRN-SCN-022: broker 0.0.0 -> 0.0.1 NaruMQRollout with a controller restart during a drain.

Nothing here talks to a cluster. ``main.py`` observes the disposable EKS fixture and passes plain dicts in;
every ``judge_*`` function returns ``(check name, ok, observed)`` rows and every ``check_*`` function raises
``ReplaceRestartPrerequisiteError`` when the fixture cannot even express the scenario (so the live run fails
loudly instead of passing a weaker scenario).
"""

from __future__ import annotations

import re
import sys
from datetime import datetime
from typing import Any

sys.dont_write_bytecode = True

OLD_RELEASE = "0.0.0"
NEW_RELEASE = "0.0.1"
POD_NAMES = ("narumq-0", "narumq-1", "narumq-2")
IMAGE_PATTERN = re.compile(r"^.+@sha256:[a-f0-9]{64}$")
BROKER_POD_PATTERN = re.compile(r"^narumq-\d+$")
MIN_LB_PROPAGATION_MILLIS = 60_000
MAX_DRAIN_TIMEOUT_MILLIS = 600_000  # controller DefaultDrainTimeout; a longer broker deadline would outlive the controller's.
TERMINAL_PHASES = ("Completed", "Failed", "Aborted")
# Barrier evidence the approval audit event must carry, from internal/application/rollout/service.go.
BARRIER_FIELDS = ("drainPhase", "drainState", "drainSafeToStop", "connectionsInitial", "connectionsRemaining", "sessionsReady",
                  "sessionsTakenOver", "sessionsFailed", "pendingWAL", "underReplicated", "safeToStopObservedAt")
ACCESS_RESOURCES = {("pods", "exec"), ("pods", "attach"), ("pods", "portforward"), ("pods", "proxy"), ("services", "proxy")}

Row = tuple[str, bool, Any]


class ReplaceRestartPrerequisiteError(RuntimeError):
    """The fixture, target or routing cannot express DRN-SCN-022; the scenario must fail, not weaken."""


class RestartConditionNotMetError(RuntimeError):
    """The controller could not be restarted while a drain was in progress."""


def timestamp(value: str) -> float:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


def digest_of(image: str) -> str:
    return image.split("@", 1)[1]


# Prerequisites ------------------------------------------------------------------------------------
def check_drain_params(lb_propagation_millis: int, timeout_millis: int) -> None:
    if not isinstance(lb_propagation_millis, int) or not isinstance(timeout_millis, int):
        raise ReplaceRestartPrerequisiteError("drain parameters must be integer milliseconds")
    if lb_propagation_millis < MIN_LB_PROPAGATION_MILLIS:
        raise ReplaceRestartPrerequisiteError(
            f"loadBalancerPropagationDelayMillis {lb_propagation_millis} < {MIN_LB_PROPAGATION_MILLIS}: the drain would not outlast a controller restart")
    if not lb_propagation_millis < timeout_millis <= MAX_DRAIN_TIMEOUT_MILLIS:
        raise ReplaceRestartPrerequisiteError(
            f"timeoutMillis must satisfy loadBalancerPropagationDelayMillis < timeoutMillis <= {MAX_DRAIN_TIMEOUT_MILLIS}")


def check_target(target: dict[str, Any]) -> None:
    """The Session target must be the synthetic ``0.0.1`` release, pinned by platform digest."""
    image = target.get("image")
    if not isinstance(image, str) or not IMAGE_PATTERN.match(image):
        raise ReplaceRestartPrerequisiteError(f"target image must be repository@sha256:<digest>, got {image!r}")
    if target.get("releaseVersion") != NEW_RELEASE:
        raise ReplaceRestartPrerequisiteError(
            f"DRN-SCN-022 replaces broker {OLD_RELEASE} with the synthetic {NEW_RELEASE}; target releaseVersion is {target.get('releaseVersion')!r}")
    features = target.get("features")
    if not isinstance(features, list) or not features or features != sorted(set(features)):
        raise ReplaceRestartPrerequisiteError(f"target features must be a non-empty sorted unique list, got {features!r}")


def check_fixture_before(workload: dict[str, Any], pods: dict[str, dict[str, Any]], features: tuple[int, Any], target: dict[str, Any]) -> None:
    """The fixture must be three Ready ``0.0.0`` brokers with the ``0.0.1`` image already in the OnDelete template."""
    digest = digest_of(target["image"])
    if workload.get("updateStrategy") != "OnDelete" or workload.get("replicas") != 3:
        raise ReplaceRestartPrerequisiteError(f"workload must be replicas=3 with updateStrategy OnDelete, got {workload}")
    if workload.get("image") != target["image"]:
        raise ReplaceRestartPrerequisiteError(
            "the StatefulSet narumq template image must already equal the target image (the controller never edits the workload); "
            f"template={workload.get('image')!r} target={target['image']!r}")
    if sorted(pods) != list(POD_NAMES):
        raise ReplaceRestartPrerequisiteError(f"fixture must have exactly {list(POD_NAMES)}, got {sorted(pods)}")
    for name, pod in pods.items():
        if not pod.get("ready") or pod.get("deleting"):
            raise ReplaceRestartPrerequisiteError(f"{name} is not Ready or is terminating")
        if pod.get("image") == target["image"] or str(pod.get("imageID") or "").endswith(digest):
            raise ReplaceRestartPrerequisiteError(f"{name} already runs the target image; the fixture is not at {OLD_RELEASE}")
    status, body = features
    members = (body or {}).get("data", {}).get("members") if status == 200 and isinstance(body, dict) else None
    versions = sorted({member.get("releaseVersion") for member in members or []})
    if not members or len(members) != 3 or versions != [OLD_RELEASE]:
        raise ReplaceRestartPrerequisiteError(
            f"Management API /api/v1/cluster/features must report exactly three members at {OLD_RELEASE} before the rollout; "
            f"http={status} versions={versions} members={len(members or [])}")


def routes_through_proxy(host_aliases: list[dict[str, Any]] | None, proxy_ip: str, namespace: str) -> bool:
    """True when the controller pins both management Service names to the fault proxy ClusterIP."""
    wanted = {f"narumq-management.{namespace}.svc", f"narumq-management.{namespace}.svc.cluster.local"}
    return any(alias.get("ip") == proxy_ip and wanted <= set(alias.get("hostnames", [])) for alias in host_aliases or [])


# Restart precondition ------------------------------------------------------------------------------
def judge_restart_precondition(status: dict[str, Any], broker: tuple[int, Any]) -> list[Row]:
    """The controller may only be restarted while its drain is active and the real broker drain is not SAFE_TO_STOP."""
    http, body = broker
    data = body.get("data") if http == 200 and isinstance(body, dict) else None
    data = data if isinstance(data, dict) else {}
    ordinal = status.get("currentOrdinal", 0)
    return [
        ("restart: rollout is Draining with a persisted operation and idempotency key",
         status.get("phase") == "Draining" and bool(status.get("drainOperationID")) and bool(status.get("attemptKey")),
         {k: status.get(k) for k in ("phase", "drainOperationID", "attemptKey", "currentOrdinal")}),
        ("restart: controller status does not yet hold SAFE_TO_STOP",
         status.get("drainSafeToStop") is not True and status.get("drainState") != "SAFE_TO_STOP" and not status.get("deleteIntent"),
         {k: status.get(k) for k in ("drainState", "drainSafeToStop", "deleteIntent")}),
        ("restart: real broker drain GET is the same operation, DRAINING and not safe to stop",
         http == 200 and data.get("operationId") == status.get("drainOperationID") and data.get("nodeId") == str(ordinal + 1)
         and data.get("state") == "DRAINING" and data.get("safeToStop") is False and data.get("phase") != "COMPLETED",
         {"http": http, **{k: data.get(k) for k in ("operationId", "nodeId", "state", "phase", "safeToStop")}}),
    ]


# Continuity ----------------------------------------------------------------------------------------
def ordinal_identity(samples: list[dict[str, Any]]) -> dict[int, dict[str, set[str]]]:
    """Drain operation IDs and idempotency keys seen per ordinal; a value carried over from an earlier ordinal is skipped."""
    identity: dict[int, dict[str, set[str]]] = {}
    seen_operations: dict[str, int] = {}
    for sample in samples:
        operation, key, ordinal = sample.get("operation"), sample.get("attemptKey"), sample.get("ordinal")
        if not operation or not key:
            continue
        if seen_operations.setdefault(operation, ordinal) != ordinal:
            continue
        entry = identity.setdefault(ordinal, {"operations": set(), "keys": set()})
        entry["operations"].add(operation)
        entry["keys"].add(key)
    return identity


def _drain_path(node: int) -> str:
    return f"/api/v1/nodes/{node}/drain"


def _posts(proxy_log: list[dict[str, Any]], node: int) -> list[dict[str, Any]]:
    return [r for r in proxy_log if r["method"] == "POST" and r["path"].split("?", 1)[0] == _drain_path(node)]


def judge_continuity(before: dict[str, Any], restart: dict[str, Any], samples: list[dict[str, Any]], proxy_log: list[dict[str, Any]] | None,
                     broker_after: tuple[int, Any]) -> list[Row]:
    """The restarted controller continues the same drain operation with the same idempotency key.

    ``proxy_log`` is the fault proxy request log since the scenario start, or ``None`` when the controller is not
    routed through the proxy; then the persisted key is judged from status samples and the approval audit only."""
    ordinal = before.get("currentOrdinal", 0)
    node = ordinal + 1
    operation, key = before["drainOperationID"], before["attemptKey"]
    old, new = restart["deleted"], restart["leaderPod"]
    after = [s for s in samples if s["at"] >= restart["leaderChangedAt"] and s["ordinal"] == ordinal and s.get("operation")]
    http, body = broker_after
    data = body.get("data") if http == 200 and isinstance(body, dict) else None
    data = data if isinstance(data, dict) else {}
    rows: list[Row] = [
        ("restart: a different controller Pod with a new UID took the leader Lease and is Ready",
         old["uid"] != new["uid"] and old["name"] != new["name"] and bool(new.get("ready")),
         {"deleted": old["uid"], "leader": {k: new.get(k) for k in ("name", "uid", "ready")}}),
        ("after restart the rollout status keeps the same drain operation and idempotency key on the same ordinal",
         bool(after) and all(s["operation"] == operation and s["attemptKey"] == key for s in after),
         {"samples": len(after), "operations": sorted({s["operation"] for s in after}), "keys": sorted({s["attemptKey"] for s in after})}),
        ("after restart the real broker still reports the same drain operation for the node (no second operation)",
         http == 200 and data.get("operationId") == operation and data.get("nodeId") == str(node),
         {"http": http, "operationId": data.get("operationId"), "expected": operation}),
    ]
    if proxy_log is not None:
        deleted_at = restart["deletedAt"]
        gets = [r for r in proxy_log if r["method"] == "GET" and r["path"].split("?", 1)[0] == _drain_path(node)
                and r["client"] == new["ip"] and r["at"] >= deleted_at]
        posts = _posts(proxy_log, node)
        late = [r for r in posts if r["at"] >= deleted_at]
        rows += [
            ("the restarted controller read the same node's drain through the proxy (GET first)", bool(gets), len(gets)),
            ("every drain POST for the node used the persisted idempotency key, and none was sent after the restart",
             bool(posts) and {p["idempotencyKey"] for p in posts} == {key} and not late,
             {"posts": [(p["client"], p["idempotencyKey"]) for p in posts], "afterRestart": len(late)}),
        ]
    return rows


def judge_single_operation_per_ordinal(samples: list[dict[str, Any]], proxy_log: list[dict[str, Any]] | None) -> list[Row]:
    """Each ordinal used exactly one drain operation and one key; distinct across ordinals; POST keys match when proxied."""
    identity = ordinal_identity(samples)
    complete = sorted(identity) == [0, 1, 2]
    single = complete and all(len(e["operations"]) == 1 and len(e["keys"]) == 1 for e in identity.values())
    operations = [next(iter(e["operations"])) for _, e in sorted(identity.items()) if len(e["operations"]) == 1]
    keys = [next(iter(e["keys"])) for _, e in sorted(identity.items()) if len(e["keys"]) == 1]
    rows: list[Row] = [
        ("each of the three ordinals used exactly one drain operation ID and one idempotency key, distinct per ordinal",
         single and len(set(operations)) == 3 and len(set(keys)) == 3,
         {o: {"operations": sorted(e["operations"]), "keys": sorted(e["keys"])} for o, e in sorted(identity.items())}),
    ]
    if proxy_log is not None and single:
        per_node = {o + 1: sorted({p["idempotencyKey"] for p in _posts(proxy_log, o + 1)}) for o in identity}
        rows.append(("drain POSTs reached each node only with that ordinal's persisted key",
                     all(per_node[o + 1] == sorted(identity[o]["keys"]) for o in identity), per_node))
    return rows


# Replacement ---------------------------------------------------------------------------------------
def uid_timeline(samples: list[dict[str, Any]]) -> tuple[dict[str, list[str]], dict[str, dict[str, float]]]:
    """Ordered distinct Pod UIDs per name and the first sighting time of each."""
    history: dict[str, list[str]] = {}
    first_seen: dict[str, dict[str, float]] = {}
    for sample in samples:
        for name, uid in sample.get("pods", {}).items():
            if uid not in history.setdefault(name, []):
                history[name].append(uid)
                first_seen.setdefault(name, {})[uid] = sample["at"]
    return history, first_seen


def judge_completion(status: dict[str, Any]) -> list[Row]:
    return [("rollout reached Completed with ordinals [0, 1, 2] and no failure",
             status.get("phase") == "Completed" and sorted(status.get("completedOrdinals", [])) == [0, 1, 2] and not status.get("failureCode"),
             {k: status.get(k) for k in ("phase", "completedOrdinals", "failureCode", "failureReason", "targetVersion", "targetImage")})]


def judge_replacement(initial: dict[str, dict[str, Any]], final: dict[str, dict[str, Any]], samples: list[dict[str, Any]],
                      deletes: list[dict[str, Any]], target: dict[str, Any], controller_user: str) -> list[Row]:
    """Every broker Pod UID was replaced exactly once, in ordinal order, and is Ready on the target digest."""
    digest = digest_of(target["image"])
    history, first_seen = uid_timeline(samples)
    exactly_once = sorted(initial) == sorted(final) == list(POD_NAMES) and all(
        history.get(name) == [initial[name]["uid"], final[name]["uid"]] and initial[name]["uid"] != final[name]["uid"] for name in POD_NAMES)
    arrival = [first_seen.get(name, {}).get(final[name]["uid"]) for name in POD_NAMES] if exactly_once else []
    successful = [r for r in deletes if r["pod"] in POD_NAMES and r["code"] in (200, 202)]
    controller = [r for r in successful if r["user"] == controller_user]
    approved = {(r.get("preconditions") or {}).get("uid") for r in controller}
    kubelet = [r for r in successful if r["user"].startswith("system:node:") and (r.get("preconditions") or {}).get("uid") in approved]
    others = [r for r in successful if r not in controller and r not in kubelet]
    return [
        ("every broker Pod UID was replaced exactly once (observed UID history is [initial, final])", exactly_once,
         {name: {"history": history.get(name), "initial": initial.get(name, {}).get("uid"), "final": final.get(name, {}).get("uid")}
          for name in sorted(set(initial) | set(final))}),
        ("the three replacements arrived in ordinal order", exactly_once and None not in arrival and arrival == sorted(arrival), arrival),
        ("all three replacement Pods are Ready, not terminating, and run the target image digest",
         sorted(final) == list(POD_NAMES) and all(p["ready"] and not p["deleting"] and p["image"] == target["image"]
                                                  and str(p["imageID"] or "").endswith(digest) for p in final.values()),
         {name: (p["ready"], p["deleting"], p["image"], p["imageID"]) for name, p in sorted(final.items())}),
        ("Kubernetes API audit: the controller deleted each broker Pod once with its initial UID precondition; only the kubelet completed those deletions",
         sorted(r["pod"] for r in controller) == list(POD_NAMES) and all(
             (r.get("preconditions") or {}).get("uid") == initial.get(r["pod"], {}).get("uid") for r in controller)
         and len(kubelet) <= 3 and not others,
         {"controller": [(r["pod"], r["code"], r.get("preconditions")) for r in controller],
          "kubelet": [(r["pod"], r["user"]) for r in kubelet], "others": others}),
    ]


def judge_release_after(features: tuple[int, Any], target: dict[str, Any]) -> list[Row]:
    status, body = features
    members = (body or {}).get("data", {}).get("members") if status == 200 and isinstance(body, dict) else None
    members = members or []
    return [("Management API reports all three voters at the target release and features after the rollout",
             len(members) == 3 and all(m.get("releaseVersion") == target["releaseVersion"]
                                       and sorted(m.get("features") or []) == sorted(target["features"]) for m in members),
             {"http": status, "members": [(m.get("nodeId"), m.get("releaseVersion")) for m in members]})]


# Approval audit ------------------------------------------------------------------------------------
def judge_approvals(events: list[dict[str, Any]], verify_exit_code: int, verify_output: str, rollout_uid: str,
                    initial: dict[str, dict[str, Any]], samples: list[dict[str, Any]], target: dict[str, Any],
                    restart: dict[str, Any]) -> list[Row]:
    """One complete SAFE_TO_STOP approval per original Pod UID, naming the operation and key the rollout used."""
    identity = ordinal_identity(samples)
    approvals = [e for e in events if e.get("action") == "rollout.pod.delete_approved" and (e.get("details") or {}).get("rolloutUid") == rollout_uid]
    by_uid: dict[str, list[dict[str, Any]]] = {}
    for event in approvals:
        by_uid.setdefault(event["details"].get("podUid"), []).append(event)
    uids = {name: initial[name]["uid"] for name in POD_NAMES if name in initial}
    problems: list[str] = []
    for ordinal, name in enumerate(POD_NAMES):
        found = by_uid.get(uids.get(name), [])
        if len(found) != 1:
            problems.append(f"{name}: {len(found)} approvals for UID {uids.get(name)}")
            continue
        event, details = found[0], found[0].get("details") or {}
        mine = identity.get(ordinal, {"operations": set(), "keys": set()})
        wrong = [field for field, ok in (
            ("result", event.get("result") == "ALLOWED"),
            ("reason", event.get("reason") == "safe_to_stop"),
            ("target", (event.get("target") or {}).get("type") == "broker_pod" and (event.get("target") or {}).get("id") == name),
            ("requestId", event.get("requestId") == event.get("eventId") and bool(event.get("eventId"))),
            ("producer", (event.get("producer") or {}).get("component") == "ROLLOUT_CONTROLLER"
             and (event.get("producer") or {}).get("instanceId") == restart["leaderPod"]["uid"]),
            ("actor", (event.get("actor") or {}).get("subject") == "narumq-rollout-controller"),
            ("barrier fields present", all(field in details and details[field] is not None for field in BARRIER_FIELDS)),
            ("drainSafeToStop", details.get("drainSafeToStop") is True),
            ("drainState", details.get("drainState") == "SAFE_TO_STOP"),
            ("drainPhase", details.get("drainPhase") == "COMPLETED"),
            ("connectionsRemaining", details.get("connectionsRemaining") == 0),
            ("pendingWAL", details.get("pendingWAL") == 0),
            ("underReplicated", details.get("underReplicated") == 0),
            ("sessionsFailed", details.get("sessionsFailed") == 0),
            ("no drain failure", not details.get("drainFailureCode") and not details.get("blockingReason")),
            ("ordinal/node", details.get("ordinal") == ordinal and details.get("nodeId") == ordinal + 1),
            ("operation", mine["operations"] == {details.get("drainOperationId")}),
            ("idempotency key", mine["keys"] == {details.get("attemptKey")}),
            ("target", details.get("targetImage") == target["image"] and details.get("targetReleaseVersion") == target["releaseVersion"]),
        ) if not ok]
        if wrong:
            problems.append(f"{name}: {', '.join(wrong)}")
    return [
        ("controller audit hash chain verifies with the broker Go verifier", verify_exit_code == 0, verify_output),
        ("each original Pod UID has exactly one complete SAFE_TO_STOP approval with the observed operation, key and a restarted-controller producer",
         not problems and len(approvals) == 3 and set(by_uid) == set(uids.values()),
         {"problems": problems, "approvals": len(approvals), "uids": sorted(by_uid, key=str)}),
    ]


# Startup without a manual readiness call -----------------------------------------------------------
def judge_startup(pods: list[dict[str, Any]], events: list[dict[str, Any]]) -> list[Row]:
    """Replacement Pods passed the Kubernetes startup probe on their own: started, no restart, no probe-kill events."""
    uids = {pod["metadata"]["uid"] for pod in pods}
    probe_ok, container_ok, observed = [], [], {}
    thresholds = {}
    for pod in pods:
        name = pod["metadata"]["name"]
        container = next((c for c in pod["spec"]["containers"] if c["name"] == "narumq"), {})
        probe = container.get("startupProbe") or {}
        probe_ok.append((probe.get("httpGet") or {}).get("path") == "/health/startup")
        thresholds[pod["metadata"]["uid"]] = probe.get("failureThreshold", 3)
        state = next((c for c in pod.get("status", {}).get("containerStatuses", []) if c["name"] == "narumq"), {})
        container_ok.append(state.get("started") is True and state.get("restartCount") == 0 and state.get("ready") is True)
        observed[name] = {"startupProbe": probe.get("httpGet"), "started": state.get("started"), "restartCount": state.get("restartCount"),
                          "ready": state.get("ready")}
    mine = [e for e in events if (e.get("involvedObject") or {}).get("uid") in uids]
    killed = [e for e in mine if e.get("reason") == "Killing" and "startup probe" in (e.get("message") or "").lower()]
    failing = [e for e in mine if e.get("reason") == "Unhealthy" and "startup probe failed" in (e.get("message") or "").lower()]
    exhausted = [e for e in failing if e.get("count", 1) >= thresholds.get(e["involvedObject"]["uid"], 3)]
    return [
        ("each replacement Pod declares the httpGet /health/startup startupProbe", bool(pods) and all(probe_ok), observed),
        ("each replacement Pod container started=true, restartCount=0 and Ready", len(pods) == 3 and all(container_ok), observed),
        ("no event shows a startup probe kill or an exhausted startup probe failure threshold for the replacement Pods",
         not killed and not exhausted,
         {"killing": [e.get("message") for e in killed], "startupProbeFailedEvents": [(e["involvedObject"].get("name"), e.get("count", 1)) for e in failing]}),
    ]


def judge_no_manual_access(records: list[dict[str, Any]], harness_user: str, allowed_forwards: int, window: tuple[float, float]) -> list[Row]:
    """No exec, attach or proxy call, and no port-forward beyond the harness's own, reached a broker Pod in the window.

    Records are Kubernetes API audit entries (``auditID``, ``user``, ``resource``, ``subresource``, ``name``, ``received``,
    ``completed``). The harness's own Management API forwards (kubectl port-forward processes it holds) are the only
    allowed access; the harness calls only /api/v1 paths."""
    start, end = window
    in_window, own_forwards = [], []
    for record in records:
        if (record["resource"], record["subresource"]) not in ACCESS_RESOURCES:
            continue
        name = record.get("name") or ""
        targets_broker = (record["resource"] == "pods" and bool(BROKER_POD_PATTERN.match(name))) or (
            record["resource"] == "services" and name.startswith("narumq"))
        received = timestamp(record["received"])
        finished = timestamp(record["completed"]) if record.get("completed") else end
        if not targets_broker or received > end or finished < start:
            continue
        if record["user"] == harness_user and record["resource"] == "pods" and record["subresource"] == "portforward":
            own_forwards.append(record)
        else:
            in_window.append(record)
    violations = in_window + (own_forwards if len(own_forwards) > allowed_forwards else [])
    return [("Kubernetes API audit shows no exec, attach, proxy or foreign port-forward call on a broker Pod during the rollout window",
             not violations,
             {"violations": violations, "harnessForwards": len(own_forwards), "allowedHarnessForwards": allowed_forwards})]
