"""Pure judgment of observed data for DRN-SCN-031 (readiness, EndpointSlice, NLB TargetGroup, new connections).

Every function takes decoded observations so tests/test_eks_nlb_drain.py needs no cluster, AWS account or broker.
Judges return a list of problem strings; an empty list means the scenario row is satisfied for that part.

Time basis: samples carry ``start`` and ``at`` (``time.monotonic()`` at request start and at completion). A transition is
dated by the completion time of the first sample that shows it, so a measured delay is an upper bound of the real
propagation and a measurement at or under the budget proves the real propagation was within the budget.
"""

from __future__ import annotations

import re
import sys
from typing import Any, Callable

import nlb_connect_probe as probe

sys.dont_write_bytecode = True

METRIC_CONNECTIONS = "narumq_connections_total"
METRIC_REJECTED = "narumq_mqtt_connect_rejected_total"
METRIC_ACTIVE = "narumq_connections_active"
MQTT_PORT = 1883
MAX_SAMPLE_GAP_SECONDS = 5.0
ROUTING_BOUNDARY = ("The NLB-routed CONNECT attempts were unauthenticated. Their outcomes and the metrics show that none was "
                    "accepted and that the drained node saw none of them, but they do not prove that the NLB routes "
                    "authenticated traffic to the healthy targets.")


# ---------------------------------------------------------------- AWS / Kubernetes identification
def judge_load_balancer(lb: dict[str, Any], tags: dict[str, str], hostname: str, cluster: str) -> list[str]:
    """The Service's NLB must be internal, of type network, and created for this cluster by the load balancer controller."""
    problems = []
    if lb.get("DNSName") != hostname:
        problems.append(f"load balancer DNS name {lb.get('DNSName')!r} != Service hostname {hostname!r}")
    if lb.get("Type") != "network":
        problems.append(f"load balancer type {lb.get('Type')!r}, want network")
    if lb.get("Scheme") != "internal":
        problems.append(f"load balancer scheme {lb.get('Scheme')!r}, want internal")
    if tags.get("elbv2.k8s.aws/cluster") != cluster:
        problems.append(f"load balancer tag elbv2.k8s.aws/cluster={tags.get('elbv2.k8s.aws/cluster')!r}, want {cluster!r}")
    return problems


def pick_target_group(groups: list[dict[str, Any]]) -> dict[str, Any]:
    """The single TCP target group serving the MQTT listener port."""
    found = [g for g in groups if g.get("Port") == MQTT_PORT and g.get("Protocol") == "TCP"]
    if len(found) != 1:
        raise LookupError(f"{len(found)} TCP target groups on port {MQTT_PORT} behind the load balancer, want exactly 1")
    return found[0]


def target_states(descriptions: list[dict[str, Any]]) -> dict[str, str]:
    """target IP -> state (healthy, draining, ...)."""
    return {d["Target"]["Id"]: d["TargetHealth"]["State"] for d in descriptions}


def judge_targets_are_pods(descriptions: list[dict[str, Any]], pod_ips: set[str]) -> list[str]:
    """The target group's targets must be exactly the broker Pod IPs on the MQTT port."""
    ids = [d["Target"]["Id"] for d in descriptions]
    problems = []
    if sorted(ids) != sorted(pod_ips) or len(ids) != len(set(ids)):
        problems.append(f"target IPs {sorted(ids)} != broker Pod IPs {sorted(pod_ips)}")
    wrong_port = sorted(d["Target"]["Id"] for d in descriptions if d["Target"].get("Port") != MQTT_PORT)
    if wrong_port:
        problems.append(f"targets not on port {MQTT_PORT}: {wrong_port}")
    return problems


def endpoint_for_pod(slices: list[dict[str, Any]], pod: str, pod_ip: str) -> dict[str, Any]:
    """Conditions of the endpoint that names ``pod`` (or holds its IP) in any EndpointSlice of the Service."""
    for item in slices:
        for endpoint in item.get("endpoints") or []:
            ref = endpoint.get("targetRef") or {}
            if ref.get("name") == pod or pod_ip in (endpoint.get("addresses") or []):
                conditions = endpoint.get("conditions") or {}
                return {"present": True, "ready": conditions.get("ready"), "serving": conditions.get("serving"),
                        "terminating": conditions.get("terminating")}
    return {"present": False, "ready": None, "serving": None, "terminating": None}


def judge_baseline(target_health: dict[str, str], pod_ips: dict[str, str], endpoints: dict[str, dict[str, Any]],
                   ready_status: int | None) -> list[str]:
    """Before the drain: every target healthy, every endpoint ready, the drain target's /health/ready is 200."""
    problems = [f"TargetGroup target {ip} is {state!r}, want healthy" for ip, state in sorted(target_health.items()) if state != "healthy"]
    problems += [f"EndpointSlice endpoint of {pod} ready={endpoints.get(pod, {}).get('ready')!r}, want true"
                 for pod in sorted(pod_ips) if endpoints.get(pod, {}).get("ready") is not True]
    if ready_status != 200:
        problems.append(f"/health/ready of the drain target is {ready_status!r}, want 200")
    return problems


# ---------------------------------------------------------------- drain lifecycle
def first_sample(samples: list[dict[str, Any]], since: float, predicate: Callable[[Any], bool]) -> dict[str, Any] | None:
    """First sample (by completion time) at or after ``since`` whose value satisfies ``predicate``."""
    for sample in sorted(samples, key=lambda s: s["at"]):
        if sample["at"] >= since and predicate(sample["value"]):
            return sample
    return None


def max_gap(samples: list[dict[str, Any]], begin: float, end: float) -> float | None:
    """Largest interval without a completed sample in [begin, end]; ``None`` without samples."""
    times = sorted(s["at"] for s in samples if begin <= s["at"] <= end)
    if not times:
        return None
    points = [begin, *times, end]
    return max(b - a for a, b in zip(points, points[1:]))


def judge_lifecycle(t0: float, t_ack: float, ready: list[dict[str, Any]], slices: list[dict[str, Any]],
                    groups: list[dict[str, Any]], budget_seconds: float) -> tuple[dict[str, list[str]], dict[str, Any]]:
    """Readiness, EndpointSlice and TargetGroup transitions measured from ``t0`` (drain POST sent) against the budget.

    Returns the problems per part (``ready``, ``endpointSlice``, ``targetGroup``, ``sampling``) and the measurements.
    """
    problems: dict[str, list[str]] = {"ready": [], "endpointSlice": [], "targetGroup": [], "sampling": []}
    first_ready_fail = first_sample(ready, t0, lambda v: isinstance(v.get("status"), int) and v["status"] != 200)
    after_ack = next((s for s in sorted(ready, key=lambda s: s["start"]) if s["start"] >= t_ack), None)
    first_slice_false = first_sample(slices, t0, lambda v: v.get("ready") is False)
    first_draining = first_sample(groups, t0, lambda v: v.get("state") == "draining")
    measurements: dict[str, Any] = {
        "T_ready_fail_seconds": None if first_ready_fail is None else round(first_ready_fail["at"] - t0, 3),
        "T1_endpoint_slice_unready_seconds": None if first_slice_false is None else round(first_slice_false["at"] - t0, 3),
        "T2_target_group_draining_seconds": None if first_draining is None else round(first_draining["at"] - t0, 3),
        "budgetSeconds": budget_seconds,
        "firstReadyAfterAck": None if after_ack is None else after_ack["value"],
    }
    if after_ack is None or after_ack["value"].get("status") != 503:
        problems["ready"].append("first /health/ready sample after the drain acknowledgement is "
                                 f"{None if after_ack is None else after_ack['value']}, want HTTP 503")
    if first_ready_fail is None:
        problems["ready"].append("/health/ready never failed")
    else:
        recovered = first_sample(ready, first_ready_fail["at"], lambda v: v.get("status") == 200)
        if recovered is not None:
            problems["ready"].append(f"/health/ready returned 200 again {recovered['at'] - t0:.3f}s after the drain start")
    for part, label, key, found in (("endpointSlice", "EndpointSlice endpoint ready=false", "T1_endpoint_slice_unready_seconds", first_slice_false),
                                    ("targetGroup", "TargetGroup target draining", "T2_target_group_draining_seconds", first_draining)):
        if found is None:
            problems[part].append(f"{label} was never observed")
        elif measurements[key] > budget_seconds:
            problems[part].append(f"{label} after {measurements[key]}s, over the {budget_seconds}s LB propagation budget")
    horizon = t0 + budget_seconds
    if first_slice_false is not None and first_draining is not None:
        horizon = min(horizon, max(first_slice_false["at"], first_draining["at"]))
    for name, samples in (("ready", ready), ("EndpointSlice", slices), ("TargetGroup", groups)):
        gap = max_gap(samples, t0, horizon)
        measurements[f"{name}_max_sample_gap_seconds"] = None if gap is None else round(gap, 3)
        if gap is None or gap > MAX_SAMPLE_GAP_SECONDS:
            problems["sampling"].append(f"{name} sampling gap {gap!r}s in the observation window exceeds {MAX_SAMPLE_GAP_SECONDS}s")
    measurements["withinBudget"] = (first_slice_false is not None and first_draining is not None
                                    and measurements["T1_endpoint_slice_unready_seconds"] <= budget_seconds
                                    and measurements["T2_target_group_draining_seconds"] <= budget_seconds)
    return problems, measurements


# ---------------------------------------------------------------- new connections
def judge_direct_connect(outcome: dict[str, Any], authenticated: bool) -> list[str]:
    """A CONNECT sent straight to the draining Pod must not be accepted.

    A refused, reset or closed connection and CONNACK 0x03 count as rejection. A timeout proves nothing. With valid
    credentials the rejection must not be an authentication one, otherwise it would not prove the drain closed admission.
    """
    kind = outcome.get("kind")
    if outcome.get("accepted"):
        return [f"drained Pod accepted a new connection: {outcome}"]
    if kind == "timeout":
        return [f"direct CONNECT timed out without CONNACK or close: {outcome}"]
    if authenticated and kind in ("bad_credentials", "not_authorized", "identifier_rejected", "malformed_response"):
        return [f"authenticated direct CONNECT rejected for {kind!r}, which does not prove the drain closed admission: {outcome}"]
    return []


def parse_metrics(text: str) -> list[tuple[str, dict[str, str], float]]:
    """Prometheus text exposition -> (name, labels, value)."""
    out = []
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        match = re.match(r'^([A-Za-z_:][A-Za-z0-9_:]*)(?:\{(.*)\})?\s+([^\s]+)(?:\s+\d+)?$', line)
        if not match:
            continue
        labels = {k: v for k, v in re.findall(r'([A-Za-z_][A-Za-z0-9_]*)="((?:[^"\\]|\\.)*)"', match.group(2) or "")}
        try:
            out.append((match.group(1), labels, float(match.group(3))))
        except ValueError:
            continue
    return out


def connect_landings(samples: list[tuple[str, dict[str, str], float]]) -> dict[str, Any]:
    """CONNECT packets this node processed: successes plus per-reason rejections (``attempts`` is their sum)."""
    success = sum(v for n, l, v in samples if n == METRIC_CONNECTIONS and l.get("result") == "success")
    rejected: dict[str, float] = {}
    for name, labels, value in samples:
        if name == METRIC_REJECTED:
            rejected[labels.get("reason", "")] = rejected.get(labels.get("reason", ""), 0.0) + value
    active = sum(v for n, _, v in samples if n == METRIC_ACTIVE)
    return {"success": success, "rejected": rejected, "attempts": success + sum(rejected.values()), "active": active}


def landings_delta(before: dict[str, Any], after: dict[str, Any]) -> dict[str, Any]:
    reasons = set(before["rejected"]) | set(after["rejected"])
    return {"success": after["success"] - before["success"], "attempts": after["attempts"] - before["attempts"],
            "rejected": {r: after["rejected"].get(r, 0.0) - before["rejected"].get(r, 0.0) for r in sorted(reasons)}}


def judge_direct_metrics(delta: dict[str, Any], outcome: dict[str, Any]) -> list[str]:
    """The drained node must have counted the direct CONNECT it rejected and accepted none."""
    problems = []
    if delta["success"] != 0:
        problems.append(f"drained node connection successes grew by {delta['success']} during the direct probe")
    if outcome.get("kind") == "server_unavailable" and delta["rejected"].get("server_unavailable", 0.0) < 1:
        problems.append(f"CONNACK 0x03 received but the drained node's server_unavailable rejection counter did not grow: {delta}")
    return problems


def judge_nlb_batch(drained: str, outcomes: list[dict[str, Any]], before: dict[str, dict[str, Any]],
                    after: dict[str, dict[str, Any]], authenticated: bool) -> tuple[list[str], dict[str, Any]]:
    """NLB-routed CONNECTs sent after the TargetGroup showed ``draining`` must never land on the drained node.

    ``before``/``after`` map node id -> ``connect_landings``. Every accepted CONNECT must be accounted for by a healthy
    node's success counter. With credentials at least one CONNECT must be accepted, which proves the NLB routes traffic.
    """
    deltas = {node: landings_delta(before[node], after[node]) for node in before}
    problems = []
    if drained not in deltas:
        return [f"no metrics for the drained node {drained}"], {"deltas": deltas}
    if deltas[drained]["attempts"] != 0:
        problems.append(f"drained node {drained} processed {deltas[drained]['attempts']} CONNECT packets of the NLB batch: {deltas[drained]}")
    accepted = sum(1 for o in outcomes if o.get("accepted"))
    healthy_success = sum(d["success"] for node, d in deltas.items() if node != drained)
    if not outcomes:
        problems.append("NLB batch produced no outcome")
    if healthy_success != accepted:
        problems.append(f"{accepted} accepted CONNECTs but healthy nodes' success counters grew by {healthy_success}")
    if authenticated and accepted < 1:
        problems.append("no authenticated NLB CONNECT was accepted after the target showed draining; routing is not proven")
    summary = {"deltas": deltas, "attempts": len(outcomes), "accepted": accepted,
               "kinds": {k: sum(1 for o in outcomes if o.get("kind") == k) for k in sorted({o.get("kind") for o in outcomes})},
               "healthySuccessDelta": healthy_success}
    return problems, summary


def judge_pre_drain_batch(outcomes: list[dict[str, Any]]) -> list[str]:
    """Before the drain the credentials must work through the NLB, otherwise the post-drain batch proves nothing."""
    accepted = sum(1 for o in outcomes if o.get("accepted"))
    return [] if accepted >= 1 else [f"no authenticated NLB CONNECT was accepted before the drain: kinds="
                                     f"{sorted({str(o.get('kind')) for o in outcomes})}"]


def routing_isolation(authenticated: bool, batch_problems: list[str], batch_summary: dict[str, Any]) -> dict[str, Any]:
    """``nlbRoutingIsolated`` is true only when authenticated NLB traffic was accepted by healthy nodes and none reached the drained node."""
    isolated = bool(authenticated and not batch_problems and batch_summary.get("accepted", 0) >= 1)
    return {"nlbRoutingIsolated": isolated, "authenticatedRouting": authenticated,
            "routingEvidenceBoundary": None if authenticated else ROUTING_BOUNDARY}


# ---------------------------------------------------------------- restoration
def pick_drain_target(roles: dict[str, str]) -> str:
    """Node id to drain: the lowest-numbered FOLLOWER; the leader only when no follower exists."""
    followers = sorted((n for n, r in roles.items() if r == "FOLLOWER"), key=int)
    if followers:
        return followers[0]
    leaders = sorted((n for n, r in roles.items() if r == "LEADER"), key=int)
    if not leaders:
        raise LookupError(f"no FOLLOWER or LEADER among nodes {roles}")
    return leaders[0]


def restore_action(drain: dict[str, Any] | None) -> str:
    """How to bring the drained node back: ``none``, ``cancel`` (not yet SAFE_TO_STOP) or ``restart`` (COMPLETED)."""
    if not drain:
        return "none"
    phase = drain.get("phase")
    if phase == "COMPLETED" and drain.get("safeToStop") is True:
        return "restart"
    if phase == "CANCELLED":
        return "none"
    return "cancel"
