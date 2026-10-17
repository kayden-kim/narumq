"""Live EKS acceptance of NLB lifecycle during a node drain (DRN-SCN-031; DEP-03 and DEP-22).

On the disposable fixture the harness drains one broker node (a follower when there is one) through the real
Management API and observes, from the drain POST (T0):

* ``/health/ready`` of that Pod (through ``kubectl port-forward``): HTTP 503 from the first request after the POST
  was acknowledged, and it stays failing;
* the EndpointSlice endpoint of the Pod behind Service ``narumq-nlb`` turning ``ready=false`` (T1);
* the AWS NLB TargetGroup target of the Pod IP turning ``draining`` (T2). The NLB is resolved from the Service status
  hostname with ``aws elbv2``, must be the internal network LB of this cluster, and its targets must be exactly the
  three broker Pod IPs;
* T1 and T2 against the drain POST ``loadBalancerPropagationDelayMillis`` (``--budget-millis``, default 30000);
* new connections: a hand-built MQTT 3.1.1 CONNECT straight to the drained Pod (right after the POST and again later)
  must not be accepted, and a batch of CONNECTs through the NLB, started from a helper Pod inside the VPC after the
  TargetGroup shows ``draining``, must never be processed by the drained node (its ``/metrics`` CONNECT success and
  rejection counters must not move; the healthy nodes' counters must account for every accepted CONNECT).

Each source is sampled by independent workers (about every 0.25 s for readiness, two staggered workers for the
slower kubectl and AWS calls). Transitions are dated by the completion of the first sample that shows them, so a result
within budget proves the real propagation was within budget. The maximum sampling gap is part of the result.

Authenticated routing proof: with ``--private/mqtt.password`` (user ``acceptance-device``, the synthetic fixture
identity used by the other harnesses) the NLB batch uses valid credentials. A pre-drain batch must be accepted
(proves routing and credentials), and after the drain every accepted CONNECT must be counted by a healthy node and
none by the drained node: ``nlbRoutingIsolated`` is then true. Without that file the batch is unauthenticated and the
result records ``nlbRoutingIsolated: false`` and the evidence boundary: only the scenario outputs proven by the
direct probe, the metrics and the sampled state are judged.

Mutations (all by the identity-guarded fixture): the drain POST on the target node; a helper Pod ``nlb-drain-probe``
in namespace ``narumq`` (labels do not match the Service selector); port-forward processes. Cleanup runs in
``finally``: the drain is cancelled with DELETE while not SAFE_TO_STOP, or after COMPLETED the Pod is deleted with a UID
precondition so it restarts on the same PVC; the helper Pod is deleted; the run then waits for three Ready Pods, three
Ready EndpointSlice endpoints and three healthy TargetGroup targets and records the result as a check.

Prerequisites: the fixture arguments of ``eks_fixture`` (``--private`` holds ``kubeconfig``, optional ``kubectl.exe`` and
optional ``mqtt.password``; AWS credentials in the environment with ``elasticloadbalancing:Describe*`` and
``sts:GetCallerIdentity``), Secret ``narumq-cluster-secrets`` key ``admin.token`` (read with kubectl, never printed or
saved), a Python image ``--helper-image`` that the cluster can pull (``python`` on PATH, e.g. a digest-pinned
``python:3.12-alpine`` mirrored into ECR; ``--pull-secret`` names an image pull Secret if needed). A missing
prerequisite raises ``PrerequisiteError``; the scenario is then recorded as failed, never skipped.

Run::

    python tests/chaos/eks-nlb-drain/main.py --run-id <run> --cluster <eks cluster> --private <dir> --evidence <dir> \\
        --helper-image <python image@sha256:...> [--budget-millis 30000] [--batch-size 30] [--pull-secret <name>]
"""

from __future__ import annotations

import argparse
import base64
import json
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path
from typing import Any, Callable

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent))  # tests/chaos: eks_fixture, eks_port_forward
sys.path.insert(0, str(HERE.parents[1]))  # tests: scenario_results

import nlb_connect_probe as probe  # noqa: E402
import nlb_drain_judge as judge  # noqa: E402
from eks_fixture import add_fixture_arguments, build_fixture, rollout_faults  # noqa: E402
from eks_port_forward import PortForward  # noqa: E402
from scenario_results import scenario_result  # noqa: E402

SERVICE = "narumq-nlb"
SECRET = "narumq-cluster-secrets"
SLOT_CONFIGMAP = "narumq-cluster-configs"
HELPER = "nlb-drain-probe"
MQTT_USER = "acceptance-device"
RESULT_NAME = "drn-scn-031-nlb-drain.json"
FAILURE_NAME = "drn-scn-031-nlb-drain-failure.json"
SAMPLE_INTERVAL = 0.25
OBSERVE_GRACE_SECONDS = 20.0
DRAIN_TIMEOUT_MILLIS = 300_000
HELPER_EXEC_TIMEOUT = 180.0


class PrerequisiteError(RuntimeError):
    """A prerequisite of the live acceptance is missing; the scenario fails instead of being skipped."""


# ---------------------------------------------------------------- HTTP helpers
def http_text(url: str, headers: dict[str, str] | None = None, timeout: float = 10.0) -> tuple[int, str]:
    request = urllib.request.Request(url, headers=headers or {})
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.status, response.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as error:
        with error:
            return error.code, error.read().decode("utf-8", "replace")


class Management:
    """Management API of one Pod over a local port. The bearer token is only ever put in a request header."""

    def __init__(self, port: int, token: str):
        self.port, self.token = port, token

    def call(self, method: str, path: str, body: Any = None, idempotency_key: str | None = None) -> tuple[int, Any]:
        headers = {"Authorization": "Bearer " + self.token}
        if idempotency_key:
            headers["Idempotency-Key"] = idempotency_key
        return rollout_faults().http_json(method, f"http://127.0.0.1:{self.port}{path}", body, headers, 15.0)

    def ready_status(self) -> int:
        return http_text(f"http://127.0.0.1:{self.port}/health/ready", timeout=3.0)[0]

    def landings(self) -> dict[str, Any]:
        status, text = http_text(f"http://127.0.0.1:{self.port}/metrics", {"Authorization": "Bearer " + self.token})
        if status != 200:
            raise RuntimeError(f"/metrics HTTP {status}")
        return judge.connect_landings(judge.parse_metrics(text))


# ---------------------------------------------------------------- cluster and AWS reads
def read_admin_token(fixture: Any) -> str:
    raw = fixture.kubectl("get", "secret", SECRET, "-o", r"jsonpath={.data.admin\.token}", check=False).strip()
    if not raw:
        raise PrerequisiteError(f"Secret {SECRET} key admin.token is missing or unreadable in namespace {fixture.namespace}")
    return base64.b64decode(raw).decode("utf-8").strip()


def read_mqtt_password(private: Path) -> str | None:
    path = private / "mqtt.password"
    return path.read_text(encoding="utf-8").strip() if path.is_file() else None


def resolve_nlb(fixture: Any, cluster: str) -> dict[str, Any]:
    """Load balancer, its single MQTT target group and the Service hostname, with the identity guards applied."""
    status = fixture.kjson("get", "service", SERVICE).get("status", {})
    ingress = (status.get("loadBalancer") or {}).get("ingress") or []
    hostname = (ingress[0] if ingress else {}).get("hostname")
    if not hostname:
        raise PrerequisiteError(f"Service {SERVICE} has no load balancer hostname yet")
    matches = [lb for lb in fixture.aws("elbv2", "describe-load-balancers")["LoadBalancers"] if lb.get("DNSName") == hostname]
    if len(matches) != 1:
        raise PrerequisiteError(f"{len(matches)} load balancers have DNS name {hostname}, want exactly 1")
    lb = matches[0]
    tags = {t["Key"]: t["Value"] for t in fixture.aws("elbv2", "describe-tags", "--resource-arns", lb["LoadBalancerArn"])["TagDescriptions"][0]["Tags"]}
    problems = judge.judge_load_balancer(lb, tags, hostname, cluster)
    if problems:
        raise rollout_faults().FixtureIdentityError("; ".join(problems))
    group = judge.pick_target_group(fixture.aws("elbv2", "describe-target-groups", "--load-balancer-arn", lb["LoadBalancerArn"])["TargetGroups"])
    return {"hostname": hostname, "loadBalancerArn": lb["LoadBalancerArn"], "targetGroupArn": group["TargetGroupArn"],
            "scheme": lb.get("Scheme"), "vpcId": lb.get("VpcId"), "healthCheckProtocol": group.get("HealthCheckProtocol"),
            "healthCheckIntervalSeconds": group.get("HealthCheckIntervalSeconds")}


def target_descriptions(fixture: Any, group_arn: str) -> list[dict[str, Any]]:
    return fixture.aws("elbv2", "describe-target-health", "--target-group-arn", group_arn)["TargetHealthDescriptions"]


def endpoint_slices(fixture: Any) -> list[dict[str, Any]]:
    return fixture.kjson("get", "endpointslices", "-l", f"kubernetes.io/service-name={SERVICE}")["items"]


def ready_broker_pods(fixture: Any) -> dict[str, dict[str, Any]]:
    pods = fixture.broker_pods()
    problems = [f"{n}: ready={p['ready']} deleting={p['deleting']} ip={p['podIP']}" for n, p in sorted(pods.items())
                if not p["ready"] or p["deleting"] or not p["podIP"]]
    if len(pods) != 3 or problems:
        raise PrerequisiteError(f"want 3 Ready broker Pods, got {len(pods)}: {problems}")
    return pods


def helper_manifest(fixture: Any, image: str, pull_secret: str | None) -> dict[str, Any]:
    labels = {"app.kubernetes.io/name": HELPER, "narumq.io/acceptance-run": fixture.run_id}  # not app=narumq: never a Service endpoint
    spec: dict[str, Any] = {
        "restartPolicy": "Never", "terminationGracePeriodSeconds": 0,
        "securityContext": {"runAsNonRoot": True, "runAsUser": 65532, "seccompProfile": {"type": "RuntimeDefault"}},
        "containers": [{"name": "probe", "image": image, "command": ["sleep", "86400"],
                        "securityContext": {"allowPrivilegeEscalation": False, "readOnlyRootFilesystem": True,
                                            "capabilities": {"drop": ["ALL"]}},
                        "resources": {"requests": {"cpu": "50m", "memory": "64Mi"}, "limits": {"cpu": "200m", "memory": "128Mi"}}}]}
    if pull_secret:
        spec["imagePullSecrets"] = [{"name": pull_secret}]
    return {"apiVersion": "v1", "kind": "Pod", "metadata": {"name": HELPER, "namespace": fixture.namespace, "labels": labels}, "spec": spec}


def run_batch(fixture: Any, host: str, count: int, username: str | None, password: str | None) -> dict[str, Any]:
    """Execute the self-contained prober in the helper Pod; the spec (with the password) goes over stdin only."""
    spec = {"host": host, "port": judge.MQTT_PORT, "count": count, "prefix": "nlbdrn", "username": username,
            "password": password, "timeout": 5.0, "workers": 8}
    source = (HERE / "nlb_connect_probe.py").read_text(encoding="utf-8")
    result = fixture.run(fixture.base() + ["-n", fixture.namespace, "exec", "-i", HELPER, "--", "python", "-c", source],
                         stdin=json.dumps(spec) + "\n", timeout=HELPER_EXEC_TIMEOUT)
    return json.loads(result.stdout)


# ---------------------------------------------------------------- sampling
class Sampler:
    """Independent workers that call ``probe`` repeatedly and record ``start``/``at`` (monotonic) and the value."""

    def __init__(self, probe_fn: Callable[[], Any], samples: list[dict[str, Any]], workers: int = 1, interval: float = SAMPLE_INTERVAL):
        self.probe_fn, self.samples, self.workers, self.interval = probe_fn, samples, workers, interval
        self.stop_event = threading.Event()
        self.threads: list[threading.Thread] = []

    def _loop(self, delay: float) -> None:
        if self.stop_event.wait(delay):
            return
        while not self.stop_event.is_set():
            start = time.monotonic()
            try:
                value = self.probe_fn()
            except Exception as error:  # recorded as an observation; it never satisfies a transition predicate
                value = {"error": f"{type(error).__name__}: {error}"}
            at = time.monotonic()
            self.samples.append({"start": start, "at": at, "value": value})
            self.stop_event.wait(max(0.0, self.interval - (at - start)))

    def start(self) -> None:
        for index in range(self.workers):
            thread = threading.Thread(target=self._loop, args=(index * self.interval / self.workers,), daemon=True)
            thread.start()
            self.threads.append(thread)

    def stop(self) -> None:
        self.stop_event.set()
        for thread in self.threads:
            thread.join(60)


def to_wall(samples: list[dict[str, Any]], mono0: float) -> list[dict[str, Any]]:
    return [{"offsetSeconds": round(s["at"] - mono0, 3), "durationSeconds": round(s["at"] - s["start"], 3), "value": s["value"]}
            for s in sorted(samples, key=lambda s: s["at"])]


# ---------------------------------------------------------------- restoration
def wait_until(probe_fn: Callable[[], Any], timeout: float, interval: float, what: str) -> Any:
    return rollout_faults().wait_for(probe_fn, timeout, interval, what)


def restore(fixture: Any, token: str, pod: str, uid: str, node_id: str, group_arn: str, timeout: float) -> dict[str, Any]:
    """Bring the drained node back; never raises. ``ok`` is true only when 3 Pods, 3 endpoints and 3 targets are healthy."""
    report: dict[str, Any] = {"ok": False, "steps": []}
    started = time.monotonic()
    try:
        for _ in range(3):
            current = fixture.kubectl("get", "pod", pod, "-o", "json", "--ignore-not-found")
            live = json.loads(current) if current.strip() else None
            if not live or live["metadata"]["uid"] != uid or live["metadata"].get("deletionTimestamp"):
                report["steps"].append({"step": "pod already replaced or terminating", "found": bool(live)})
                break
            with PortForward(fixture, fixture.namespace, f"pod/{pod}", 8080) as forward:
                api = Management(forward.local[8080], token)
                status, payload = api.call("GET", f"/api/v1/nodes/{node_id}/drain")
                drain = payload.get("data") if status == 200 and isinstance(payload, dict) else None
                action = judge.restore_action(drain)
                report["steps"].append({"step": "inspect drain", "http": status, "phase": (drain or {}).get("phase"), "action": action})
                if action == "none":
                    break
                if action == "cancel":
                    status, _ = api.call("DELETE", f"/api/v1/nodes/{node_id}/drain", idempotency_key=f"nlb-drain-restore-{uuid.uuid4()}")
                    report["steps"].append({"step": "cancel drain", "http": status})
                    if status in (200, 202):
                        break
                    time.sleep(3)
                    continue
            options = {"kind": "DeleteOptions", "apiVersion": "v1", "preconditions": {"uid": uid}}
            fixture.mutate("delete", "--raw", f"/api/v1/namespaces/{fixture.namespace}/pods/{pod}", "-f", "-", stdin=json.dumps(options))
            report["steps"].append({"step": "delete Pod with UID precondition after COMPLETED", "uid": uid})
            break

        def healthy() -> dict[str, Any] | None:
            pods = fixture.broker_pods()
            if len(pods) != 3 or any(not p["ready"] or p["deleting"] or not p["podIP"] for p in pods.values()):
                return None
            states = judge.target_states(target_descriptions(fixture, group_arn))
            ips = {p["podIP"] for p in pods.values()}
            if set(states) != ips or any(s != "healthy" for s in states.values()):
                return None
            endpoints = {n: judge.endpoint_for_pod(endpoint_slices(fixture), n, p["podIP"]) for n, p in pods.items()}
            if any(e["ready"] is not True for e in endpoints.values()):
                return None
            return {"pods": {n: {"uid": p["uid"], "ip": p["podIP"]} for n, p in pods.items()}, "targets": states}

        report["final"] = wait_until(healthy, timeout, 5.0, "3 Ready Pods, 3 ready endpoints and 3 healthy targets")
        report["ok"] = True
    except Exception as error:
        report["error"] = f"{type(error).__name__}: {error}"
    report["seconds"] = round(time.monotonic() - started, 1)
    return report


# ---------------------------------------------------------------- scenario
# Scenarios: drain-and-rolling-update.md:DRN-SCN-031
@scenario_result
def scenario_nlb_drain(fixture: Any, private: Path, cluster: str, helper_image: str, budget_millis: int = 30_000,
                       batch_size: int = 30, pull_secret: str | None = None, restore_timeout: float = 900.0) -> dict[str, Any]:
    """Drain one node and judge readiness, EndpointSlice, NLB TargetGroup and new-connection rejection."""
    if not helper_image:
        raise PrerequisiteError("--helper-image is required: the internal NLB is reachable only from inside the VPC")
    if budget_millis <= 0 or budget_millis >= DRAIN_TIMEOUT_MILLIS:
        raise ValueError(f"budget must be in (0, {DRAIN_TIMEOUT_MILLIS}) milliseconds")
    if batch_size < 1:
        raise ValueError("batch size must be positive")
    checks = rollout_faults().Checks()
    budget_seconds = budget_millis / 1000
    token = read_admin_token(fixture)
    password = read_mqtt_password(private)
    authenticated = password is not None
    username = MQTT_USER if authenticated else None
    nlb = resolve_nlb(fixture, cluster)
    pods = ready_broker_pods(fixture)
    slots = fixture.kjson("get", "configmap", SLOT_CONFIGMAP).get("data", {})
    node_of = {name: slots.get(f"slot-{name.rsplit('-', 1)[-1]}") for name in pods}
    if None in node_of.values() or len(set(node_of.values())) != 3:
        raise PrerequisiteError(f"cannot map Pods to node ids from {SLOT_CONFIGMAP}: {node_of}")
    pod_of = {node: name for name, node in node_of.items()}
    pod_ips = {name: pod["podIP"] for name, pod in pods.items()}
    evidence: dict[str, Any] = {"nlb": nlb, "budgetMillis": budget_millis, "authenticatedNlbProbe": authenticated,
                                "pods": {n: {"uid": p["uid"], "ip": p["podIP"], "node": p["node"], "nodeId": node_of[n]} for n, p in pods.items()}}
    forwards: dict[str, PortForward] = {}
    helper_started = False
    target: dict[str, Any] = {}
    samplers: list[Sampler] = []
    failure: BaseException | None = None
    restoration: dict[str, Any] | None = None
    try:
        for node, name in pod_of.items():
            forwards[node] = PortForward(fixture, fixture.namespace, f"pod/{name}", 8080)
            forwards[node].start()
        apis = {node: Management(forwards[node].local[8080], token) for node in forwards}
        status, payload = apis[min(apis, key=int)].call("GET", "/api/v1/nodes?limit=1000")
        rows = (payload or {}).get("data") if status == 200 and isinstance(payload, dict) else None
        if not rows or len(rows) != 3 or any(r.get("queryStatus") != "OK" for r in rows):
            raise PrerequisiteError(f"cluster node list is not complete and OK: HTTP {status} {rows}")
        roles = {str(r["nodeId"]): r["role"] for r in rows}
        drained = judge.pick_drain_target(roles)
        pod = pod_of[drained]
        target = {"node": drained, "pod": pod, "uid": pods[pod]["uid"], "ip": pod_ips[pod], "role": roles[drained]}
        evidence["target"] = target
        api = apis[drained]
        direct = PortForward(fixture, fixture.namespace, f"pod/{pod}", 1883)
        forwards["mqtt"] = direct
        mqtt_port = direct.start()[1883]

        fixture.mutate("delete", "pod", HELPER, "--ignore-not-found", "--wait=true", timeout=180)
        fixture.apply(helper_manifest(fixture, helper_image, pull_secret))
        helper_started = True
        fixture.mutate("wait", "--for=condition=Ready", f"pod/{HELPER}", "--timeout=240s", timeout=260)

        # Baseline ------------------------------------------------------------------------------
        descriptions = target_descriptions(fixture, nlb["targetGroupArn"])
        problems = judge.judge_targets_are_pods(descriptions, set(pod_ips.values()))
        checks.check("NLB TargetGroup targets are exactly the 3 broker Pod IPs on the MQTT port", not problems,
                     {"problems": problems, "targets": judge.target_states(descriptions)})
        slices = endpoint_slices(fixture)
        endpoints = {n: judge.endpoint_for_pod(slices, n, ip) for n, ip in pod_ips.items()}
        baseline = judge.judge_baseline(judge.target_states(descriptions), pod_ips, endpoints, api.ready_status())
        checks.check("baseline: 3 targets healthy, 3 endpoints ready, /health/ready 200", not baseline,
                     {"problems": baseline, "endpoints": endpoints})
        pre_outcomes: list[dict[str, Any]] = []
        if authenticated:
            pre = run_batch(fixture, nlb["hostname"], min(batch_size, 9), username, password)
            pre_outcomes = pre["outcomes"]
            problems = judge.judge_pre_drain_batch(pre_outcomes)
            checks.check("baseline: authenticated CONNECTs through the NLB are accepted before the drain", not problems,
                         {"problems": problems, "nlbAddresses": pre["addresses"], "accepted": sum(1 for o in pre_outcomes if o["accepted"]),
                          "attempts": len(pre_outcomes)})
        if not checks.passed:
            return fixture.save_result(RESULT_NAME, checks.result("DRN-SCN-031", fixture=fixture.identity, note="baseline unmet; no drain was started", **evidence))
        metrics0 = {node: apis[node].landings() for node in apis}

        # Drain and observe -----------------------------------------------------------------------
        ready_samples: list[dict[str, Any]] = []
        slice_samples: list[dict[str, Any]] = []
        group_samples: list[dict[str, Any]] = []
        target_ip = pod_ips[pod]
        samplers = [
            Sampler(lambda: {"status": api.ready_status()}, ready_samples, 1),
            Sampler(lambda: judge.endpoint_for_pod(endpoint_slices(fixture), pod, target_ip), slice_samples, 2, 0.5),
            Sampler(lambda: {"state": judge.target_states(target_descriptions(fixture, nlb["targetGroupArn"])).get(target_ip)},
                    group_samples, 2, 0.5)]
        for sampler in samplers:
            sampler.start()
        time.sleep(1.0)  # at least one pre-drain sample per source
        body = {"loadBalancerPropagationDelayMillis": budget_millis, "timeoutMillis": DRAIN_TIMEOUT_MILLIS}
        wall0, t0 = time.time(), time.monotonic()
        post_status, post_payload = api.call("POST", f"/api/v1/nodes/{drained}/drain", body, f"nlb-drain-{uuid.uuid4()}")
        t_ack = time.monotonic()
        direct_early = probe.connect_once("127.0.0.1", mqtt_port, "nlbdrn-direct-1", username, password, 5.0)
        post_data = (post_payload or {}).get("data") if isinstance(post_payload, dict) else None
        checks.check("drain POST was accepted (202) and the node is DRAINING", post_status == 202 and (post_data or {}).get("state") == "DRAINING",
                     {"http": post_status, "state": (post_data or {}).get("state"), "phase": (post_data or {}).get("phase"),
                      "operationId": (post_data or {}).get("operationId"), "seconds": round(t_ack - t0, 3)})
        if post_status not in (200, 202):
            raise RuntimeError(f"drain POST failed: HTTP {post_status}")

        deadline = t0 + budget_seconds + OBSERVE_GRACE_SECONDS
        while time.monotonic() < deadline:
            if (judge.first_sample(slice_samples, t0, lambda v: v.get("ready") is False)
                    and judge.first_sample(group_samples, t0, lambda v: v.get("state") == "draining")):
                break
            time.sleep(0.25)
        for sampler in samplers:
            sampler.stop()
        lifecycle, measured = judge.judge_lifecycle(t0, t_ack, ready_samples, slice_samples, group_samples, budget_seconds)
        checks.check("/health/ready fails immediately (HTTP 503 from the first request after the drain acknowledgement)",
                     not lifecycle["ready"], {"problems": lifecycle["ready"], "T_ready_fail_seconds": measured["T_ready_fail_seconds"],
                                              "firstReadyAfterAck": measured["firstReadyAfterAck"]})
        checks.check("EndpointSlice endpoint of the drained Pod becomes ready=false within the LB propagation budget",
                     not lifecycle["endpointSlice"], {"problems": lifecycle["endpointSlice"], "seconds": measured["T1_endpoint_slice_unready_seconds"], "budgetSeconds": budget_seconds})
        checks.check("TargetGroup target of the drained Pod becomes draining within the LB propagation budget",
                     not lifecycle["targetGroup"], {"problems": lifecycle["targetGroup"], "seconds": measured["T2_target_group_draining_seconds"], "budgetSeconds": budget_seconds})
        checks.check("sampling resolution allows the judgment (no source unobserved for long)", not lifecycle["sampling"],
                     {"problems": lifecycle["sampling"], **{k: v for k, v in measured.items() if k.endswith("_max_sample_gap_seconds")}})

        # New connections ---------------------------------------------------------------------------
        problems = judge.judge_direct_connect(direct_early, authenticated)
        metrics1 = {node: apis[node].landings() for node in apis}
        problems += judge.judge_direct_metrics(judge.landings_delta(metrics0[drained], metrics1[drained]), direct_early)
        checks.check("a CONNECT sent directly to the drained Pod right after the drain start is not accepted", not problems,
                     {"problems": problems, "outcome": direct_early, "metricsDelta": judge.landings_delta(metrics0[drained], metrics1[drained])})
        batch = run_batch(fixture, nlb["hostname"], batch_size, username, password)
        metrics2 = {node: apis[node].landings() for node in apis}
        batch_problems, batch_summary = judge.judge_nlb_batch(drained, batch["outcomes"], metrics1, metrics2, authenticated)
        checks.check("NLB-routed CONNECTs after the target shows draining never reach the drained node", not batch_problems,
                     {"problems": batch_problems, "nlbAddresses": batch["addresses"], **batch_summary})
        direct_late = probe.connect_once("127.0.0.1", mqtt_port, "nlbdrn-direct-2", username, password, 5.0)
        problems = judge.judge_direct_connect(direct_late, authenticated)
        checks.check("a CONNECT sent directly to the drained Pod after the NLB batch is still not accepted", not problems,
                     {"problems": problems, "outcome": direct_late})
        isolation = judge.routing_isolation(authenticated, batch_problems, batch_summary)

        drain_final: dict[str, Any] | None = None
        completion_deadline = time.monotonic() + budget_seconds + 150
        while time.monotonic() < completion_deadline:
            status, payload = api.call("GET", f"/api/v1/nodes/{drained}/drain")
            drain_final = (payload or {}).get("data") if status == 200 and isinstance(payload, dict) else None
            if drain_final and drain_final.get("phase") in ("COMPLETED", "FAILED", "CANCELLED"):
                break
            time.sleep(3)
        evidence.update({"startedAtUnix": wall0, "measurements": measured, "directProbes": {"early": direct_early, "late": direct_late},
                         "batchOutcomes": batch["outcomes"], "preDrainOutcomes": pre_outcomes,
                         "metrics": {"before": metrics0, "afterDirectProbe": metrics1, "afterNlbBatch": metrics2},
                         "drainFinal": drain_final, **isolation,
                         "samples": {"ready": to_wall(ready_samples, t0), "endpointSlice": to_wall(slice_samples, t0),
                                     "targetGroup": to_wall(group_samples, t0)}})
    except BaseException as error:
        failure = error
        raise
    finally:
        for sampler in samplers:
            sampler.stop()
        if target:
            restoration = restore(fixture, token, target["pod"], target["uid"], target["node"], nlb["targetGroupArn"], restore_timeout)
        for forward in forwards.values():
            forward.close()
        if helper_started:
            try:
                fixture.mutate("delete", "pod", HELPER, "--ignore-not-found", "--wait=true", timeout=180)
            except Exception as error:
                print(f"helper Pod cleanup failed: {error}", file=sys.stderr)
        if failure is not None:
            fixture.save(FAILURE_NAME, {"error": f"{type(failure).__name__}: {failure}", "restoration": restoration, **evidence})
    checks.check("restoration: 3 Ready Pods, 3 ready EndpointSlice endpoints and 3 healthy TargetGroup targets", bool(restoration and restoration["ok"]), restoration)
    return fixture.save_result(RESULT_NAME, checks.result("DRN-SCN-031", fixture=fixture.identity, restoration=restoration, **evidence))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    add_fixture_arguments(parser)
    parser.add_argument("--helper-image", required=True, help="Python image (digest-pinned) for the in-VPC NLB prober Pod")
    parser.add_argument("--pull-secret", help="image pull Secret for --helper-image")
    parser.add_argument("--budget-millis", type=int, default=30_000, help="drain POST loadBalancerPropagationDelayMillis and the judged budget")
    parser.add_argument("--batch-size", type=int, default=30, help="NLB-routed CONNECT attempts after the target shows draining")
    parser.add_argument("--restore-timeout", type=float, default=900.0)
    args = parser.parse_args(argv)
    fixture = build_fixture(args)
    result = scenario_nlb_drain(fixture, args.private, args.cluster, args.helper_image, args.budget_millis, args.batch_size,
                                args.pull_secret, args.restore_timeout)
    for item in result["checks"]:
        print(("PASS " if item["ok"] else "FAIL ") + item["check"])
    print(result["result"], "nlbRoutingIsolated=%s" % result.get("nlbRoutingIsolated"), "->", args.evidence / RESULT_NAME)
    return 0 if result["result"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
