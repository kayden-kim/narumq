"""EKS acceptance runner for the rollout controller fault matrix (DEP-11, DEP-18).

The runner drives a disposable EKS fixture through docs/contracts/rollout-controller-v1.md §3:
drain failure and deadline, GET timeout / malformed / incomplete evidence, audit sink failure,
re-approval of a SAFE_TO_STOP drain after a failure (§2.9), Kubernetes DELETE result unknown,
Pod resourceVersion change, deletionTimestamp, controller restart while a drain POST or a Pod
DELETE result is unknown, non-leader replicas, status CAS conflict, an Argo CD-applied broker
replacement, and a separate finalizer-only Argo CD sync check without Pod replacement.

Every mutating call first requires ``Fixture.verify_identity()``. It checks that the kubeconfig
context, the EKS cluster endpoint and tags, and the namespace label all name the same disposable
acceptance run. The runner refuses to touch any other cluster.

The scenario functions are called step by step from an operator session; each returns and saves
a JSON result whose ``result`` is ``PASS`` only when every recorded check holds.
"""

from __future__ import annotations

import base64
import json
import ssl
import subprocess
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

sys.dont_write_bytecode = True
sys.path.append(str(Path(__file__).resolve().parents[2]))  # tests/ holds scenario_results.py
from scenario_results import scenario_result  # noqa: E402

RUN_LABEL = "narumq.io/acceptance-run"
NAMESPACE = "narumq"
CONTROLLER = "narumq-rollout-controller"
STANDBY = f"{CONTROLLER}-standby"
CONTROLLER_USER = f"system:serviceaccount:{NAMESPACE}:{CONTROLLER}"
CONTROLLER_LEADER_LEASE = "narumq-rollout-controller.narumq.io"
PROXY = "rollout-fault-proxy"
WEBHOOK = "rollout-delete-delay"
HELPER = "rollout-audit-helper"
HOLD_FINALIZER = "narumq.io/acceptance-hold"
FAILED_DRAIN_PARAMS = {"loadBalancerPropagationDelayMillis": 999, "timeoutMillis": 1000}
LONG_DRAIN_PARAMS = {"loadBalancerPropagationDelayMillis": 1_500_000, "timeoutMillis": 1_800_000}
# A short broker deadline lets a SAFE_TO_STOP drain outlive it within one scenario.
SAFE_RETRY_DRAIN_PARAMS = {"loadBalancerPropagationDelayMillis": 1000, "timeoutMillis": 150_000}
# Contract §2.9: a SAFE_TO_STOP retry sets approvalDeadlineAt = now + DefaultDrainTimeout (10 minutes).
APPROVAL_WINDOW_SECONDS = 600
REQUIRED_TAGS = {"Purpose": "disposable-acceptance-fixture", "DataClass": "synthetic-test-data-only"}


class FixtureIdentityError(RuntimeError):
    pass


class Checks:
    def __init__(self) -> None:
        self.items: list[dict[str, Any]] = []

    def check(self, name: str, ok: bool, observed: Any = None) -> bool:
        self.items.append({"check": name, "ok": bool(ok), "observed": observed})
        return bool(ok)

    @property
    def passed(self) -> bool:
        return bool(self.items) and all(item["ok"] for item in self.items)

    def result(self, scenario: str, **extra: Any) -> dict[str, Any]:
        return {"scenario": scenario, "result": "PASS" if self.passed else "FAIL", "checks": self.items, **extra}


def wait_for(probe: Callable[[], Any], timeout: float, interval: float = 2.0, what: str = "condition") -> Any:
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        last = probe()
        if last:
            return last
        time.sleep(interval)
    raise TimeoutError(f"{what} not reached in {timeout:.0f}s; last={last!r}")


def observe_until(fixture: Fixture, rollout: str, done: Callable[[dict[str, Any]], bool], timeout: float,
                  interval: float = 3.0, what: str = "condition") -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Sample the rollout status until ``done`` holds; return the final status and every sample."""
    samples, deadline = [], time.monotonic() + timeout
    while time.monotonic() < deadline:
        status = fixture.status(rollout)
        samples.append({"at": time.time(), "phase": status.get("phase"), "failureCode": status.get("failureCode"),
                        "attemptKey": status.get("attemptKey"), "operation": status.get("drainOperationID"),
                        "approvalDeadlineAt": status.get("approvalDeadlineAt"), "deleteIntent": status.get("deleteIntent")})
        if done(status):
            return status, samples
        time.sleep(interval)
    raise TimeoutError(f"{what} not reached in {timeout:.0f}s; last={samples[-1] if samples else None!r}")


def rfc3339(value: str) -> float:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


def http_json(method: str, url: str, body: Any = None, headers: dict[str, str] | None = None,
              timeout: float = 15.0, context: ssl.SSLContext | None = None) -> tuple[int, Any]:
    data = None if body is None else json.dumps(body).encode("utf-8")
    request = urllib.request.Request(url, data=data, method=method, headers={"Content-Type": "application/json", **(headers or {})})
    try:
        with urllib.request.urlopen(request, timeout=timeout, context=context) as response:
            raw = response.read()
            return response.status, json.loads(raw) if raw else None
    except urllib.error.HTTPError as error:
        with error:
            raw = error.read()
        try:
            return error.code, json.loads(raw) if raw else None
        except json.JSONDecodeError:
            return error.code, raw.decode("utf-8", "replace")


@dataclass
class Fixture:
    run_id: str
    cluster: str
    kubectl_path: Path
    kubeconfig: Path
    evidence_dir: Path
    aws_env: dict[str, str]
    namespace: str = NAMESPACE
    identity: dict[str, Any] | None = field(default=None, init=False)
    controller_uids: set[str] = field(default_factory=set, init=False)

    def base(self) -> list[str]:
        return [str(self.kubectl_path), "--kubeconfig", str(self.kubeconfig), "--context", self.run_id]

    def run(self, args: list[str], stdin: str | None = None, timeout: float = 120, check: bool = True) -> subprocess.CompletedProcess:
        # AWS CLI v2 writes with the console code page on Windows (cp949 fails on "µs" in audit records).
        result = subprocess.run(args, input=stdin, capture_output=True, text=True, encoding="utf-8", timeout=timeout,
                                env={**self.aws_env, "PYTHONUTF8": "1"})
        if check and result.returncode != 0:
            raise RuntimeError(f"{Path(args[0]).name} {' '.join(args[1:4])}… failed ({result.returncode}): {result.stderr.strip()[:500]}")
        return result

    def kubectl(self, *args: str, stdin: str | None = None, timeout: float = 120, check: bool = True, namespace: str | None = None) -> str:
        return self.run(self.base() + ["-n", namespace or self.namespace, *args], stdin=stdin, timeout=timeout, check=check).stdout

    def kjson(self, *args: str, namespace: str | None = None) -> Any:
        return json.loads(self.kubectl(*args, "-o", "json", namespace=namespace))

    def mutate(self, *args: str, stdin: str | None = None, timeout: float = 120, check: bool = True, namespace: str | None = None) -> str:
        if self.identity is None:
            raise FixtureIdentityError("verify_identity() must pass before any mutation")
        return self.kubectl(*args, stdin=stdin, timeout=timeout, check=check, namespace=namespace)

    def apply(self, *objects: dict[str, Any], namespace: str | None = None) -> str:
        return self.mutate("apply", "-f", "-", stdin=json.dumps({"apiVersion": "v1", "kind": "List", "items": list(objects)}), namespace=namespace)

    def aws(self, *args: str) -> Any:
        return json.loads(self.run(["aws", *args, "--output", "json"]).stdout)

    def verify_identity(self) -> dict[str, Any]:
        config = json.loads(self.run(self.base() + ["config", "view", "-o", "json"]).stdout)
        contexts = [entry for entry in config.get("contexts", []) if entry["name"] == self.run_id]
        if len(contexts) != 1:
            raise FixtureIdentityError(f"kubeconfig has no single context named {self.run_id}")
        cluster_ref = contexts[0]["context"]["cluster"]
        servers = [entry["cluster"]["server"] for entry in config.get("clusters", []) if entry["name"] == cluster_ref]
        described = self.aws("eks", "describe-cluster", "--name", self.cluster)["cluster"]
        tags = described.get("tags", {})
        namespace = json.loads(self.run(self.base() + ["get", "namespace", self.namespace, "-o", "json"]).stdout)
        label = namespace["metadata"].get("labels", {}).get(RUN_LABEL)
        problems = []
        if servers != [described["endpoint"]]:
            problems.append("kubeconfig server is not the fixture EKS endpoint")
        if tags.get("AcceptanceRun") != self.run_id:
            problems.append("EKS AcceptanceRun tag does not match")
        problems += [f"EKS tag {key} != {value}" for key, value in REQUIRED_TAGS.items() if tags.get(key) != value]
        if label != self.run_id:
            problems.append(f"namespace label {RUN_LABEL} does not match")
        if problems:
            raise FixtureIdentityError("; ".join(problems))
        self.identity = {"verifiedAt": time.time(), "cluster": described["name"], "endpoint": described["endpoint"],
                         "clusterArn": described["arn"], "tags": {k: tags[k] for k in ("AcceptanceRun", *REQUIRED_TAGS)},
                         "namespaceLabel": label, "context": self.run_id}
        return self.identity

    def save(self, name: str, value: Any) -> Path:
        path = self.evidence_dir / name
        path.write_text(json.dumps(value, indent=2, default=str) + "\n", encoding="utf-8")
        return path

    def save_result(self, name: str, result: dict[str, Any]) -> dict[str, Any]:
        self.save(name, result)
        return result

    # Observations -------------------------------------------------------------------------
    def broker_pods(self) -> dict[str, dict[str, Any]]:
        pods = {}
        for item in self.kjson("get", "pods", "-l", "app=narumq")["items"]:
            meta, status = item["metadata"], item.get("status", {})
            container = next((c for c in status.get("containerStatuses", []) if c["name"] == "narumq"), {})
            pods[meta["name"]] = {
                "uid": meta["uid"], "resourceVersion": meta["resourceVersion"], "deleting": meta.get("deletionTimestamp"),
                "finalizers": meta.get("finalizers", []), "image": item["spec"]["containers"][0]["image"],
                "imageID": container.get("imageID"), "ready": container.get("ready", False),
                "node": item["spec"].get("nodeName"), "podIP": status.get("podIP"),
            }
        return pods

    def rollout(self, name: str) -> dict[str, Any] | None:
        out = self.kubectl("get", "narumqrollout", name, "-o", "json", "--ignore-not-found")
        return json.loads(out) if out.strip() else None

    def status(self, name: str) -> dict[str, Any]:
        return (self.rollout(name) or {}).get("status", {})

    def controller_pods(self) -> list[dict[str, Any]]:
        items = self.kjson("get", "pods", "-l", f"app.kubernetes.io/name={CONTROLLER}")["items"]
        self.controller_uids.update(i["metadata"]["uid"] for i in items)
        return [{"name": i["metadata"]["name"], "uid": i["metadata"]["uid"], "ip": i.get("status", {}).get("podIP"),
                 "node": i["spec"].get("nodeName"), "deleting": i["metadata"].get("deletionTimestamp"),
                 "ready": all(c.get("ready") for c in i.get("status", {}).get("containerStatuses", [{}]))} for i in items]

    def leader(self) -> dict[str, Any]:
        lease = self.kjson("get", "lease", CONTROLLER_LEADER_LEASE)
        return {"holder": lease["spec"].get("holderIdentity"), "transitions": lease["spec"].get("leaseTransitions"),
                "resourceVersion": lease["metadata"]["resourceVersion"]}

    def pod_delete_audit(self, start: float, end: float) -> list[dict[str, Any]]:
        """Kubernetes API audit records for Pod DELETE in the fixture namespace (CloudWatch)."""
        pattern = ('{ $.verb = "delete" && $.objectRef.resource = "pods" && $.objectRef.namespace = "' + self.namespace +
                   '" && $.stage = "ResponseComplete" }')
        records, token = [], None
        while True:
            args = ["logs", "filter-log-events", "--log-group-name", f"/aws/eks/{self.cluster}/cluster",
                    "--log-stream-name-prefix", "kube-apiserver-audit", "--start-time", str(int(start * 1000)),
                    "--end-time", str(int(end * 1000)), "--filter-pattern", pattern]
            page = self.aws(*args, *(["--next-token", token] if token else []))
            for event in page.get("events", []):
                entry = json.loads(event["message"])
                body = entry.get("requestObject") or {}
                records.append({"at": entry.get("requestReceivedTimestamp"), "user": entry["user"]["username"],
                                "pod": entry["objectRef"].get("name"), "code": (entry.get("responseStatus") or {}).get("code"),
                                "reason": (entry.get("responseStatus") or {}).get("reason"),
                                "preconditions": body.get("preconditions"), "sourceIPs": entry.get("sourceIPs")})
            token = page.get("nextToken")
            if not token:
                return sorted(records, key=lambda record: record["at"] or "")

    def audit_ingested_through(self, marker: str, timeout: float = 600) -> None:
        """Wait until CloudWatch holds the audit record of a sentinel DELETE issued now."""
        issued = time.time()
        self.mutate("delete", "pod", marker, "--ignore-not-found", "--wait=false")
        wait_for(lambda: any(r["pod"] == marker for r in self.pod_delete_audit(issued - 5, time.time() + 5)),
                 timeout, 15, f"CloudWatch audit ingestion of {marker}")


class PortForward:
    def __init__(self, fixture: Fixture, target: str, local: int, remote: int):
        self.fixture, self.target, self.local, self.remote = fixture, target, local, remote
        self.process: subprocess.Popen | None = None

    def ensure(self) -> None:
        if self.process and self.process.poll() is None:
            return
        self.process = subprocess.Popen(self.fixture.base() + ["-n", self.fixture.namespace, "port-forward", "--address", "127.0.0.1",
                                                               self.target, f"{self.local}:{self.remote}"],
                                        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        line = self.process.stdout.readline() if self.process.stdout else ""
        if "Forwarding from" not in line:
            self.close()
            raise RuntimeError(f"port-forward {self.target} failed: {line.strip()}")

    def close(self) -> None:
        if self.process and self.process.poll() is None:
            self.process.terminate()
            self.process.wait(10)
        self.process = None


class Endpoint:
    def __init__(self, forward: PortForward, scheme: str = "http", context: ssl.SSLContext | None = None,
                 headers: dict[str, str] | None = None):
        self.forward, self.scheme, self.context, self.headers = forward, scheme, context, headers or {}

    def call(self, method: str, path: str, body: Any = None, headers: dict[str, str] | None = None, timeout: float = 15) -> tuple[int, Any]:
        for attempt in range(2):
            self.forward.ensure()
            try:
                return http_json(method, f"{self.scheme}://127.0.0.1:{self.forward.local}{path}", body,
                                 {**self.headers, **(headers or {})}, timeout, self.context)
            except (urllib.error.URLError, ConnectionError, TimeoutError):
                self.forward.close()
                if attempt:
                    raise
        raise AssertionError("unreachable")


@dataclass
class Session:
    fixture: Fixture
    proxy: Endpoint
    management: Endpoint
    webhook: Endpoint
    rollout_name: str
    target: dict[str, Any]
    registry_secret: str = "narumq-registry"
    management_secret: str = "rollout-management"
    helper_image: str = ""
    secrets: tuple[str, ...] = ()
    initial_pods: dict[str, Any] = field(default_factory=dict)
    windows: list[dict[str, Any]] = field(default_factory=list)
    standbys: list[dict[str, Any]] = field(default_factory=list)

    # Fixture control ------------------------------------------------------------------------
    def proxy_set(self, **update: Any) -> dict[str, Any]:
        status, state = self.proxy.call("POST", "/control/state", update)
        if status != 200:
            raise RuntimeError(f"proxy control rejected: {state}")
        return state

    def proxy_requests(self, since: float = 0.0) -> list[dict[str, Any]]:
        return self.proxy.call("GET", f"/control/requests?since={since}")[1]["requests"]

    def drain(self, node: int) -> tuple[int, Any]:
        return self.management.call("GET", f"/api/v1/nodes/{node}/drain")

    def rollout_manifest(self, retry_token: str, abort: bool = False) -> dict[str, Any]:
        return {"apiVersion": "narumq.io/v1alpha1", "kind": "NaruMQRollout",
                "metadata": {"name": self.rollout_name, "namespace": self.fixture.namespace},
                "spec": {"workloadRef": {"name": "narumq"}, "target": self.target,
                         "imagePullSecretRef": {"name": self.registry_secret},
                         "management": {"endpoint": f"http://narumq-management.{self.fixture.namespace}.svc.cluster.local:8080",
                                        "credentialSecretRef": {"name": self.management_secret}},
                         "retryToken": retry_token, "abort": abort}}

    def set_retry_token(self, token: str) -> None:
        self.fixture.mutate("patch", "narumqrollout", self.rollout_name, "--type=merge", "-p",
                            json.dumps({"spec": {"retryToken": token}}))

    def restart_controller(self) -> dict[str, Any]:
        """Delete the leader Pod and wait for a different Pod to hold the leader Lease and be Ready."""
        fx = self.fixture
        leader = fx.leader()
        victim = next(p for p in fx.controller_pods() if leader["holder"] and leader["holder"].startswith(p["name"] + "_"))
        deleted_at = time.time()
        fx.mutate("delete", "pod", victim["name"], "--wait=true", timeout=180)
        new_leader = wait_for(lambda: (lambda l: l if l["holder"] and not l["holder"].startswith(victim["name"] + "_") else None)(fx.leader()),
                              300, 1, "new controller leader")
        changed_at = time.time()
        leader_pod = wait_for(lambda: next((p for p in fx.controller_pods() if new_leader["holder"].startswith(p["name"] + "_") and p["ready"]), None),
                              300, 2, "new leader Pod Ready")
        return {"deleted": victim, "deletedAt": deleted_at, "leaderBefore": leader, "leaderAfter": new_leader,
                "leaderChangedAt": changed_at, "leaderPod": leader_pod, "pods": fx.controller_pods()}

    def observe(self, seconds: float, interval: float = 3.0) -> list[dict[str, Any]]:
        samples, end = [], time.monotonic() + seconds
        while time.monotonic() < end:
            status = self.fixture.status(self.rollout_name)
            samples.append({"at": time.time(), "phase": status.get("phase"), "failureCode": status.get("failureCode"),
                            "currentOrdinal": status.get("currentOrdinal", 0), "completed": status.get("completedOrdinals", []),
                            "attemptKey": status.get("attemptKey"), "operation": status.get("drainOperationID"),
                            "podRV": status.get("currentPodResourceVersion")})
            time.sleep(interval)
        return samples

    def mark_window(self, scenario: str, start: float, end: float, allowed_pods: tuple[str, ...] = ()) -> None:
        self.windows.append({"scenario": scenario, "start": start, "end": end, "allowedDeletes": list(allowed_pods)})

    def hold_checks(self, checks: Checks, label: str, baseline: dict[str, Any], samples: list[dict[str, Any]],
                    phases: set[str], codes: set[str], ordinal: int, since: float) -> None:
        pods = self.fixture.broker_pods()
        unchanged = {name: (pods.get(name, {}).get("uid") == pod["uid"] and not pods.get(name, {}).get("deleting")) for name, pod in baseline.items()}
        checks.check(f"{label}: every broker Pod keeps its UID and has no deletionTimestamp", all(unchanged.values()), unchanged)
        checks.check(f"{label}: phase stays in {sorted(phases)}", all(s["phase"] in phases for s in samples),
                     sorted({s["phase"] for s in samples}))
        seen = {s["failureCode"] for s in samples}
        checks.check(f"{label}: blocker codes {sorted(codes)} observed and no other", bool(seen & codes) and seen <= codes | {None}, sorted(map(str, seen)))
        checks.check(f"{label}: ordinal stays {ordinal} with no completed ordinal added",
                     all(s["currentOrdinal"] == ordinal and len(s["completed"]) == len(samples[0]["completed"]) for s in samples),
                     sorted({(s["currentOrdinal"], len(s["completed"])) for s in samples}))
        other = [r for r in self.proxy_requests(since) if "/drain" in r["path"] and f"/nodes/{ordinal + 1}/" not in r["path"]]
        checks.check(f"{label}: no drain request for any other node", not other, other[:5])


# Fixture support objects ----------------------------------------------------------------------
def _script_deployment(name: str, image: str, script: str, port: int, env: list[dict[str, Any]],
                       volumes: list[dict[str, Any]], mounts: list[dict[str, Any]]) -> dict[str, Any]:
    labels = {"app.kubernetes.io/name": name}
    return {"apiVersion": "apps/v1", "kind": "Deployment", "metadata": {"name": name, "namespace": NAMESPACE, "labels": labels},
            "spec": {"replicas": 1, "selector": {"matchLabels": labels}, "template": {"metadata": {"labels": labels}, "spec": {
                "securityContext": {"runAsNonRoot": True, "runAsUser": 65532, "seccompProfile": {"type": "RuntimeDefault"}},
                "containers": [{"name": name, "image": image, "command": ["python", "-B", f"/scripts/{script}"], "env": env,
                                "ports": [{"containerPort": port}],
                                "securityContext": {"allowPrivilegeEscalation": False, "readOnlyRootFilesystem": True, "capabilities": {"drop": ["ALL"]}},
                                "volumeMounts": [{"name": "scripts", "mountPath": "/scripts", "readOnly": True}, *mounts]}],
                "volumes": [{"name": "scripts", "configMap": {"name": "rollout-fault-scripts"}}, *volumes]}}}}


def _service(name: str, port: int, target: int) -> dict[str, Any]:
    return {"apiVersion": "v1", "kind": "Service", "metadata": {"name": name, "namespace": NAMESPACE},
            "spec": {"selector": {"app.kubernetes.io/name": name}, "ports": [{"port": port, "targetPort": target}]}}


def support_objects(python_image: str, tls_cert: bytes, tls_key: bytes) -> list[dict[str, Any]]:
    """Proxy and webhook objects. Script sources come from this directory, unchanged."""
    here = Path(__file__).resolve().parent
    scripts = {name: (here / name).read_text(encoding="utf-8") for name in ("proxy.py", "delete_delay_webhook.py")}
    tls = {"tls.crt": base64.b64encode(tls_cert).decode(), "tls.key": base64.b64encode(tls_key).decode()}
    return [
        {"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "rollout-fault-scripts", "namespace": NAMESPACE}, "data": scripts},
        {"apiVersion": "v1", "kind": "Secret", "type": "kubernetes.io/tls", "metadata": {"name": f"{WEBHOOK}-tls", "namespace": NAMESPACE}, "data": tls},
        _script_deployment(PROXY, python_image, "proxy.py", 8080,
                           [{"name": "UPSTREAM_URL", "value": f"http://narumq-management.{NAMESPACE}.svc.cluster.local:8080"}], [], []),
        _service(PROXY, 8080, 8080),
        _script_deployment(WEBHOOK, python_image, "delete_delay_webhook.py", 8443,
                           [{"name": "TLS_CERT_FILE", "value": "/tls/tls.crt"}, {"name": "TLS_KEY_FILE", "value": "/tls/tls.key"},
                            {"name": "DELAY_SECONDS", "value": "15"}],
                           [{"name": "tls", "secret": {"secretName": f"{WEBHOOK}-tls"}}], [{"name": "tls", "mountPath": "/tls", "readOnly": True}]),
        _service(WEBHOOK, 443, 8443),
    ]


def route_controller_through_proxy(fixture: Fixture) -> dict[str, Any]:
    """Pin the controller's management Service names to the proxy ClusterIP; the proxy resolves the real Service."""
    proxy_ip = fixture.kjson("get", "service", PROXY)["spec"]["clusterIP"]
    hostnames = [f"narumq-management.{NAMESPACE}.svc", f"narumq-management.{NAMESPACE}.svc.cluster.local"]
    fixture.mutate("patch", "deployment", CONTROLLER, "--type=strategic", "-p",
                   json.dumps({"spec": {"template": {"spec": {"hostAliases": [{"ip": proxy_ip, "hostnames": hostnames}]}}}}))
    fixture.mutate("rollout", "status", f"deployment/{CONTROLLER}", "--timeout=300s", timeout=320)
    return {"proxyClusterIP": proxy_ip, "hostnames": hostnames}


# Scenario A: real broker drain FAILED -----------------------------------------------------------
# Scenarios: drain-and-rolling-update.md:DRN-SCN-026
@scenario_result
def scenario_drain_failed(session: Session, retry_token: str) -> dict[str, Any]:
    fx, checks, start = session.fixture, Checks(), time.time()
    baseline = fx.broker_pods()
    session.proxy_set(getMode="relay", postParams=FAILED_DRAIN_PARAMS, getDelaySeconds=0, postResponseDelaySeconds=0)
    fx.apply(session.rollout_manifest(retry_token))
    status = wait_for(lambda: (lambda s: s if s.get("phase") == "Failed" else None)(fx.status(session.rollout_name)), 300, 3, "Failed")
    # The controller deadline is the broker deadlineAt truncated to persisted seconds, so the controller can
    # fail before the broker marks its own operation FAILED; read the broker until it settles.
    real = session.drain(1)
    settle = time.monotonic() + 60
    while not (real[0] == 200 and real[1]["data"]["phase"] == "FAILED") and time.monotonic() < settle:
        time.sleep(2)
        real = session.drain(1)
    samples = session.observe(45)
    checks.check("failure code is drain_timeout", status.get("failureCode") == "drain_timeout", status.get("failureCode"))
    # The broker marks a timed-out drain FAILED at deadlineAt, which is also the controller's persisted
    # deadline; pollDrain checks that deadline first, so the controller fails on the persisted deadline.
    checks.check("controller kept the broker operation identity and failed on its persisted deadline",
                 bool(status.get("drainOperationID")) and "deadline" in status.get("failureReason", ""),
                 {k: status.get(k) for k in ("drainPhase", "drainFailureCode", "drainOperationID", "drainDeadlineAt")})
    checks.check("real broker GET reports the same operation FAILED with drain_timeout",
                 real[0] == 200 and real[1]["data"]["operationId"] == status.get("drainOperationID")
                 and real[1]["data"]["phase"] == "FAILED" and (real[1]["data"].get("failure") or {}).get("code") == "drain_timeout", real)
    reason = status.get("failureReason", "")
    checks.check("status names the safe operator action", "spec.retryToken" in reason and "spec.abort=true" in reason, reason)
    posts = [r for r in session.proxy_requests(start) if r["method"] == "POST"]
    checks.check("Failed attempt issued exactly one drain POST and no new operation", len(posts) == 1, posts)
    session.hold_checks(checks, "after FAILED", baseline, samples, {"Failed"}, {"drain_timeout"}, 0, start)
    end = time.time()
    session.mark_window("drain_failed", start, end)
    return fx.save_result("rollout-drain-failed.json", checks.result("drain_failed", status=status, realDrain=real, posts=posts,
                                                                     samples=samples, baseline=baseline, window=[start, end]))


# Scenario B: GET faults on a long real drain, then controller deadline ----------------------------
# Scenarios: drain-and-rolling-update.md:DRN-SCN-023
@scenario_result
def scenario_get_faults_and_deadline(session: Session, retry_token: str, hold: float = 75) -> dict[str, Any]:
    fx, checks, start = session.fixture, Checks(), time.time()
    baseline = fx.broker_pods()
    previous = fx.status(session.rollout_name)
    session.proxy_set(getMode="relay", postParams=LONG_DRAIN_PARAMS)
    session.set_retry_token(retry_token)
    status = wait_for(lambda: (lambda s: s if s.get("attemptKey") != previous.get("attemptKey") and s.get("drainOperationID")
                                and s.get("drainOperationID") != previous.get("drainOperationID") else None)(fx.status(session.rollout_name)),
                      240, 3, "retried drain operation")
    old = session.drain(1)
    checks.check("retry token started a new broker operation that the real broker reports for node 1",
                 old[0] == 200 and old[1]["data"]["operationId"] == status["drainOperationID"], old)
    phases: dict[str, Any] = {}
    for mode, codes in (("incomplete", {"drain_incomplete"}), ("malformed", {"drain_observation_unknown"}),
                        ("unavailable", {"drain_observation_unknown"}), ("hang", {"drain_observation_unknown"})):
        session.proxy_set(getMode=mode, hangSeconds=8)
        mode_start = time.time()
        time.sleep(6)
        samples = session.observe(hold)
        real = session.drain(1)
        checks.check(f"{mode}: real broker drain is still DRAINING (not safe)", real[0] == 200 and real[1]["data"]["safeToStop"] is False
                     and real[1]["data"]["state"] == "DRAINING", real)
        session.hold_checks(checks, mode, baseline, samples, {"Draining"}, codes, 0, mode_start)
        phases[mode] = {"start": mode_start, "samples": samples, "realDrain": real}
    deadline = fx.status(session.rollout_name).get("drainDeadlineAt")
    final = wait_for(lambda: (lambda s: s if s.get("phase") == "Failed" else None)(fx.status(session.rollout_name)), 900, 5, "deadline Failed")
    real = session.drain(1)
    checks.check("controller failed at its persisted deadline with drain_timeout", final.get("failureCode") == "drain_timeout"
                 and "deadline" in final.get("failureReason", ""), {k: final.get(k) for k in ("failureCode", "failureReason", "lastTransitionTime")})
    checks.check("failure happened at or after the persisted deadline", final.get("lastTransitionTime", "") >= (deadline or "~"),
                 {"deadline": deadline, "failedAt": final.get("lastTransitionTime")})
    checks.check("real broker drain was never safe", real[0] == 200 and real[1]["data"]["safeToStop"] is False, real)
    session.hold_checks(checks, "deadline", baseline, session.observe(15), {"Failed"}, {"drain_timeout"}, 0, start)
    session.proxy_set(getMode="relay", postParams=None)
    end = time.time()
    session.mark_window("get_faults_and_deadline", start, end)
    return fx.save_result("rollout-get-faults-deadline.json", checks.result("get_faults_and_deadline", modes=phases, final=final,
                                                                            finalRealDrain=real, deadline=deadline, window=[start, end]))


# Scenario C: durable audit sink failure blocks approval --------------------------------------------
def helper_pod(session: Session, node: str) -> dict[str, Any]:
    return {"apiVersion": "v1", "kind": "Pod", "metadata": {"name": HELPER, "namespace": session.fixture.namespace,
                                                            "labels": {"app.kubernetes.io/name": HELPER}},
            "spec": {"nodeName": node, "restartPolicy": "Never",
                     "containers": [{"name": "helper", "image": session.helper_image,
                                     "command": ["sh", "-c", "apk add --no-cache e2fsprogs-extra >/dev/null && touch /tmp/ready && sleep 86400"],
                                     "securityContext": {"runAsUser": 0, "capabilities": {"add": ["LINUX_IMMUTABLE"]}},
                                     "readinessProbe": {"exec": {"command": ["test", "-f", "/tmp/ready"]}, "periodSeconds": 2},
                                     "volumeMounts": [{"name": "audit", "mountPath": "/audit"}]}],
                     "volumes": [{"name": "audit", "persistentVolumeClaim": {"claimName": "narumq-rollout-audit"}}]}}


def start_helper(session: Session) -> None:
    fx = session.fixture
    node = next(p["node"] for p in fx.controller_pods() if not p["deleting"])
    fx.apply(helper_pod(session, node))
    fx.mutate("wait", "--for=condition=Ready", f"pod/{HELPER}", "--timeout=180s", timeout=200)


def stop_helper(session: Session) -> None:
    session.fixture.mutate("delete", "pod", HELPER, "--ignore-not-found", "--wait=true", timeout=180)


def helper_exec(session: Session, script: str, check: bool = True) -> str:
    return session.fixture.mutate("exec", HELPER, "--", "sh", "-c", script, check=check)


def read_audit_chain(session: Session) -> dict[str, Any]:
    """Verify the controller audit directory with the broker binary's Go verifier and return its events."""
    result = session.fixture.run(session.fixture.base() + ["-n", session.fixture.namespace, "exec", HELPER, "--",
                                                          "/narumq", "-audit-verify", "/audit"], check=False)
    text = helper_exec(session, "cat $(ls /audit/audit-*.jsonl 2>/dev/null | sort) /audit/audit.jsonl")
    return {"verifyExitCode": result.returncode, "verifyOutput": (result.stdout + result.stderr).strip(),
            "events": [json.loads(line)["event"] for line in text.splitlines() if line.strip()]}


# Scenarios: drain-and-rolling-update.md:DRN-SCN-026
@scenario_result
def scenario_audit_sink_failure(session: Session, retry_token: str, hold: float = 75) -> dict[str, Any]:
    fx, checks, start = session.fixture, Checks(), time.time()
    baseline = fx.broker_pods()
    start_helper(session)
    before = helper_exec(session, "sha256sum /audit/audit.jsonl && stat -c %s /audit/audit.jsonl")
    helper_exec(session, "chattr +i /audit/audit.jsonl && lsattr /audit/audit.jsonl")
    attrs = helper_exec(session, "lsattr /audit/audit.jsonl")
    checks.check("audit file is immutable before approval", attrs.split()[0].find("i") >= 0, attrs.strip())
    webhook_requests_before = len(session.webhook.call("GET", "/requests")[1]["requests"])
    session.proxy_set(getMode="relay", postParams=None)
    session.set_retry_token(retry_token)
    status = wait_for(lambda: (lambda s: s if s.get("failureCode") == "audit_unavailable" else None)(fx.status(session.rollout_name)),
                      300, 3, "audit_unavailable")
    samples = session.observe(hold)
    after = helper_exec(session, "sha256sum /audit/audit.jsonl && stat -c %s /audit/audit.jsonl")
    final = fx.status(session.rollout_name)
    checks.check("drain reached real SAFE_TO_STOP before approval", final.get("drainSafeToStop") is True and final.get("drainState") == "SAFE_TO_STOP",
                 {k: final.get(k) for k in ("drainState", "drainPhase", "drainSafeToStop")})
    checks.check("approval was not persisted (no delete intent)", not final.get("deleteIntent") and final.get("phase") == "DrainSafe",
                 {k: final.get(k) for k in ("phase", "deleteIntent", "failureReason")})
    checks.check("audit file bytes unchanged", before == after, {"before": before, "after": after})
    webhook_requests = session.webhook.call("GET", "/requests")[1]["requests"]
    checks.check("no Pod DELETE reached the API server admission chain", len(webhook_requests) == webhook_requests_before, len(webhook_requests))
    session.hold_checks(checks, "audit sink failure", baseline, samples, {"DrainSafe"}, {"audit_unavailable"}, 0, start)
    helper_exec(session, "chattr -i /audit/audit.jsonl")
    stop_helper(session)
    end = time.time()
    session.mark_window("audit_sink_failure", start, end)
    return fx.save_result("rollout-audit-sink-failure.json", checks.result("audit_sink_failure", status=status, final=final, samples=samples,
                                                                           auditBefore=before, auditAfter=after, window=[start, end]))


# Scenario H: retry after a failure at SAFE_TO_STOP re-approves the same drain ----------------------
# Scenarios: drain-and-rolling-update.md:DRN-SCN-027
@scenario_result
def scenario_safe_retry(session: Session, tokens: tuple[str, str, str], approval_timeout: float = 900) -> dict[str, Any]:
    """Contract §2.9 SAFE_TO_STOP retry on a real broker drain.

    The audit file is immutable, so approval blocks with ``audit_unavailable`` after a real SAFE_TO_STOP.
    1. The first token starts a drain with a short broker deadline; the rollout fails at that deadline.
    2. The second token, after the broker deadline, re-approves the same drain with a new 10-minute
       approval deadline; approval still blocks, and the rollout fails again at the approval deadline.
    3. With the audit file writable, the controller restarts, because the durable audit publisher stays
       unavailable after a write failure until the process restarts. The third token then re-approves
       again; the controller deletes the same Pod once and the rollout completes. No drain POST or
       cancel reaches node 1 after step 1.

    Time comparisons use cluster clocks (broker ``deadlineAt``, controller status times); the
    workstation clock is only used for observation windows."""
    fx, checks, start = session.fixture, Checks(), time.time()
    baseline = fx.broker_pods()
    previous = fx.status(session.rollout_name)
    start_helper(session)
    helper_exec(session, "chattr +i /audit/audit.jsonl")
    audit_before = helper_exec(session, "sha256sum /audit/audit.jsonl && stat -c %s /audit/audit.jsonl")
    session.proxy_set(getMode="relay", postParams=SAFE_RETRY_DRAIN_PARAMS, getDelaySeconds=0, postResponseDelaySeconds=0)
    session.set_retry_token(tokens[0])
    first, first_samples = observe_until(fx, session.rollout_name, lambda s: s.get("phase") == "Failed" and s.get("attemptKey") != previous.get("attemptKey"),
                                         600, what="failure at SAFE_TO_STOP")
    real = session.drain(1)
    drain = real[1]["data"] if real[0] == 200 else {}
    checks.check("1: real SAFE_TO_STOP was reached and approval blocked on the audit sink",
                 any(s["phase"] == "DrainSafe" and s["failureCode"] == "audit_unavailable" for s in first_samples),
                 sorted({(s["phase"], s["failureCode"]) for s in first_samples}, key=str))
    checks.check("1: rollout failed with drain_timeout while the last drain was SAFE_TO_STOP, without a re-approval deadline",
                 first.get("failureCode") == "drain_timeout" and first.get("drainSafeToStop") is True and not first.get("approvalDeadlineAt")
                 and not first.get("deleteIntent"), {k: first.get(k) for k in ("failureCode", "drainSafeToStop", "approvalDeadlineAt", "deleteIntent")})
    checks.check("1: real broker reports the same operation COMPLETED with safeToStop",
                 drain.get("operationId") == first.get("drainOperationID") and drain.get("phase") == "COMPLETED" and drain.get("safeToStop") is True, real)
    reason = first.get("failureReason", "")
    checks.check("1: status names the SAFE_TO_STOP operator action (re-approve or abort)",
                 "re-approve" in reason and "spec.retryToken" in reason and "spec.abort=true" in reason, reason)
    broker_deadline = rfc3339(drain["deadlineAt"]) if drain.get("deadlineAt") else None

    retried_at = time.time()
    session.set_retry_token(tokens[1])
    second, _ = observe_until(fx, session.rollout_name, lambda s: s.get("approvalDeadlineAt") and s.get("retryTokenHash") != first.get("retryTokenHash"),
                              180, what="SAFE_TO_STOP re-approval")
    checks.check("2: retry kept the drain idempotency key and broker operation",
                 second.get("attemptKey") == first.get("attemptKey") and second.get("drainOperationID") == first.get("drainOperationID"),
                 {k: (first.get(k), second.get(k)) for k in ("attemptKey", "drainOperationID")})
    # approvalDeadlineAt is the controller's re-approval time plus 10 minutes, truncated to seconds,
    # so approvalDeadlineAt - 10m is no later than the re-approval on the controller clock.
    reapproved_no_later = rfc3339(second["approvalDeadlineAt"]) - APPROVAL_WINDOW_SECONDS
    checks.check("2: re-approval was recorded after the broker drain deadline (cluster clocks)",
                 broker_deadline is not None and reapproved_no_later >= broker_deadline,
                 {"brokerDeadlineAt": drain.get("deadlineAt"), "approvalDeadlineAt": second["approvalDeadlineAt"],
                  "firstFailedAt": first.get("lastTransitionTime")})
    window = rfc3339(second["approvalDeadlineAt"]) - retried_at
    checks.check("2: approval deadline is about 10 minutes after the retry (workstation clock)", 590 <= window <= 640,
                 {"approvalDeadlineAt": second["approvalDeadlineAt"], "secondsAfterRetry": round(window, 1)})
    failed_again, second_samples = observe_until(fx, session.rollout_name, lambda s: s.get("phase") == "Failed", approval_timeout,
                                                 what="failure at the approval deadline")
    checks.check("2: approval stayed blocked on the audit sink in DrainSafe",
                 all(s["phase"] in ("DrainSafe", "Failed") for s in second_samples)
                 and any(s["phase"] == "DrainSafe" and s["failureCode"] == "audit_unavailable" for s in second_samples),
                 sorted({(s["phase"], s["failureCode"]) for s in second_samples}, key=str))
    checks.check("2: rollout failed with drain_timeout at or after the approval deadline",
                 failed_again.get("failureCode") == "drain_timeout"
                 and rfc3339(failed_again["lastTransitionTime"]) >= rfc3339(second["approvalDeadlineAt"]),
                 {k: failed_again.get(k) for k in ("failureCode", "lastTransitionTime", "approvalDeadlineAt")})
    audit_after = helper_exec(session, "sha256sum /audit/audit.jsonl && stat -c %s /audit/audit.jsonl")
    checks.check("1-2: audit file bytes unchanged while it was immutable", audit_before == audit_after, {"before": audit_before, "after": audit_after})
    pods = fx.broker_pods()
    checks.check("1-2: no broker Pod was deleted", all(pods.get(n, {}).get("uid") == p["uid"] and not pods.get(n, {}).get("deleting") for n, p in baseline.items()),
                 {n: pods.get(n, {}).get("uid") for n in baseline})
    blocked_end = time.time()
    session.mark_window("safe_retry_blocked", start, blocked_end)

    helper_exec(session, "chattr -i /audit/audit.jsonl")
    stop_helper(session)
    restart = session.restart_controller()
    checks.check("3: a new controller process holds the leader Lease before the re-approval",
                 restart["leaderAfter"]["holder"] != restart["leaderBefore"]["holder"],
                 {"before": restart["leaderBefore"]["holder"], "after": restart["leaderAfter"]["holder"]})
    session.proxy_set(postParams=None)
    session.set_retry_token(tokens[2])
    final, final_samples = observe_until(fx, session.rollout_name, lambda s: s.get("phase") in ("Completed", "Failed", "Aborted"), 2700, 5,
                                         "rollout completion")
    pods = fx.broker_pods()
    target_digest = session.target["image"].split("@", 1)[1]
    checks.check("3: rollout completed after the re-approval", final.get("phase") == "Completed" and sorted(final.get("completedOrdinals", [])) == [0, 1, 2],
                 {k: final.get(k) for k in ("phase", "completedOrdinals", "failureCode", "failureReason")})
    checks.check("3: every broker Pod was replaced with the target digest and is Ready",
                 all(pods[n]["uid"] != p["uid"] and (pods[n]["imageID"] or "").endswith(target_digest) and pods[n]["ready"] for n, p in baseline.items()),
                 {n: (p["uid"], pods.get(n, {}).get("uid"), pods.get(n, {}).get("imageID")) for n, p in baseline.items()})
    node1 = [r for r in session.proxy_requests(retried_at) if r["method"] in ("POST", "DELETE") and "/nodes/1/drain" in r["path"]]
    checks.check("2-3: no drain POST or cancel reached node 1 after the first failure", not node1, node1)
    end = time.time()
    session.mark_window("safe_retry_reapproved", blocked_end, end, allowed_pods=tuple(baseline))
    return fx.save_result("rollout-safe-retry.json", checks.result(
        "safe_retry", first=first, firstSamples=first_samples, realDrain=real, second=second, failedAgain=failed_again,
        secondSamples=second_samples, restart=restart, final=final, finalSamples=final_samples, baseline=baseline, after=pods,
        tokens=list(tokens), window=[start, end], retriedAt=retried_at, blockedEnd=blocked_end))


# Scenario D: DELETE result unknown, RV change, deletionTimestamp, restarts --------------------------
def webhook_configuration(session: Session, ca_bundle: bytes, pod: str, timeout_seconds: int) -> dict[str, Any]:
    return {"apiVersion": "admissionregistration.k8s.io/v1", "kind": "ValidatingWebhookConfiguration",
            "metadata": {"name": WEBHOOK, "labels": {RUN_LABEL: session.fixture.run_id}},
            "webhooks": [{"name": "delete-delay.acceptance.narumq.io", "admissionReviewVersions": ["v1"], "sideEffects": "None",
                          "failurePolicy": "Fail", "timeoutSeconds": timeout_seconds,
                          "clientConfig": {"service": {"name": WEBHOOK, "namespace": session.fixture.namespace, "path": "/validate", "port": 443},
                                           "caBundle": base64.b64encode(ca_bundle).decode()},
                          "rules": [{"operations": ["DELETE"], "apiGroups": [""], "apiVersions": ["v1"], "resources": ["pods"], "scope": "Namespaced"}],
                          "namespaceSelector": {"matchLabels": {"kubernetes.io/metadata.name": session.fixture.namespace}},
                          "objectSelector": {"matchLabels": {"statefulset.kubernetes.io/pod-name": pod}},
                          "matchConditions": [{"name": "controller-only", "expression": f"request.userInfo.username == '{CONTROLLER_USER}'"}]}]}


def webhook_requests(session: Session, since: float = 0.0) -> list[dict[str, Any]]:
    return [r for r in session.webhook.call("GET", "/requests")[1]["requests"] if r["receivedAt"] >= since]


# Scenarios: drain-and-rolling-update.md:DRN-SCN-026
@scenario_result
def scenario_delete_unknown(session: Session, ca_bundle: bytes, restart_first: bool = True) -> dict[str, Any]:
    """DELETE result unknown on ordinal 0. ``restart_first`` restarts the controller after an audit-sink failure;
    a fresh rollout skips it and reaches approval through its own drain."""
    fx, checks, start = session.fixture, Checks(), time.time()
    pod = "narumq-0"
    session.webhook.call("POST", "/control", {"delaySeconds": 15})
    fx.apply(webhook_configuration(session, ca_bundle, pod, 10))
    restart_audit = session.restart_controller() if restart_first else None
    status = wait_for(lambda: (lambda s: s if s.get("failureCode") == "delete_unconfirmed" and s.get("phase") == "DeleteApproved" else None)(
        fx.status(session.rollout_name)), 600, 3, "delete_unconfirmed")
    baseline = fx.broker_pods()
    approved = {"uid": status["currentPodUID"], "rv": status["currentPodResourceVersion"], "attemptKey": status["attemptKey"]}
    approved_precondition = {"uid": approved["uid"], "resourceVersion": approved["rv"]}
    samples = session.observe(40)
    attempts = webhook_requests(session, start)
    checks.check("controller retried the unknown DELETE with the controller identity", len(attempts) >= 2 and
                 all(a["user"] == CONTROLLER_USER and a["name"] == pod for a in attempts), attempts)
    checks.check("every retry used the same persisted UID/resourceVersion precondition",
                 all(a["preconditions"] == approved_precondition for a in attempts), [a["preconditions"] for a in attempts])
    session.hold_checks(checks, "DELETE timeout", baseline, samples, {"DeleteApproved"}, {"delete_unconfirmed"}, 0, start)
    # Restart while the DELETE result is unknown: the new leader keeps the same attempt and precondition.
    restart_delete = session.restart_controller()
    time.sleep(20)
    resumed = fx.status(session.rollout_name)
    checks.check("restart during unknown DELETE keeps attempt key and approved UID/RV",
                 (resumed.get("attemptKey"), resumed.get("currentPodUID"), resumed.get("currentPodResourceVersion")) ==
                 (approved["attemptKey"], approved["uid"], approved["rv"]), resumed)
    after_restart = webhook_requests(session, restart_delete["leaderChangedAt"])
    checks.check("the new leader retried the DELETE with the same precondition",
                 bool(after_restart) and all(a["preconditions"] == approved_precondition for a in after_restart), after_restart)
    # Pod resourceVersion change after approval: no stale DELETE, fresh evidence and re-approval.
    fx.mutate("patch", "pod", pod, "--type=merge", "-p", json.dumps({"metadata": {"finalizers": [HOLD_FINALIZER]}}))
    new_rv = fx.broker_pods()[pod]["resourceVersion"]
    reapproved = wait_for(lambda: (lambda s: s if s.get("currentPodResourceVersion") == new_rv and s.get("phase") == "DeleteApproved" else None)(
        fx.status(session.rollout_name)), 180, 3, "re-approval with new resourceVersion")
    checks.check("controller re-approved with the new Pod resourceVersion, not the stale one",
                 reapproved["currentPodResourceVersion"] == new_rv != approved["rv"], {"old": approved["rv"], "new": new_rv})
    # API-level precondition: the Pod changes while an allowed DELETE is inside admission.
    session.webhook.call("POST", "/control", {"delaySeconds": 6})
    race_since = time.time()
    first = wait_for(lambda: webhook_requests(session, race_since), 60, 0.3, "DELETE inside admission")[0]
    fx.mutate("label", "pod", pod, f"narumq.io/acceptance-race={int(time.time())}", "--overwrite")
    labelled_at = time.time()
    race_rv = fx.broker_pods()[pod]["resourceVersion"]
    # The re-approved DELETE succeeds; the finalizer keeps the Pod object with deletionTimestamp.
    deleting = wait_for(lambda: (lambda p: p if p.get("deleting") else None)(fx.broker_pods().get(pod, {})), 180, 2, "deletionTimestamp")
    awaiting = wait_for(lambda: (lambda s: s if s.get("phase") == "AwaitingReplacement" else None)(fx.status(session.rollout_name)), 120, 3, "AwaitingReplacement")
    admissions = webhook_requests(session, start)
    checks.check("the Pod changed while a DELETE with the re-approved precondition was inside admission",
                 first["preconditions"] == {"uid": approved["uid"], "resourceVersion": new_rv} and labelled_at < first["receivedAt"] + 6
                 and race_rv != new_rv, {"first": first, "labelledAt": labelled_at, "raceRV": race_rv})
    final = admissions[-1]
    checks.check("the DELETE that set deletionTimestamp carried a fresh precondition matching the object it deleted",
                 final["preconditions"]["uid"] == approved["uid"] and final["preconditions"]["resourceVersion"] not in (approved["rv"], new_rv)
                 and final["preconditions"]["resourceVersion"] == final["oldResourceVersion"], final)
    fx.audit_ingested_through(f"audit-sentinel-{int(time.time())}")
    api_records = [r for r in fx.pod_delete_audit(race_since - 5, time.time()) if r["user"] == CONTROLLER_USER and r["pod"] == pod]
    checks.check("the Kubernetes API answered the in-flight stale DELETE with 409 Conflict and the next DELETE succeeded",
                 any(r["code"] == 409 for r in api_records) and any(r["code"] in (200, 202) for r in api_records), api_records)
    deletes_at_deletion = len(webhook_requests(session, start))
    restart_awaiting = session.restart_controller()
    hold_start = time.time()
    samples_awaiting = session.observe(60)
    later = webhook_requests(session, hold_start)
    checks.check("no duplicate DELETE after deletionTimestamp (including after controller restart)", not later, later)
    checks.check("ordinal 0 is held in AwaitingReplacement and no node-2 drain starts",
                 all(s["phase"] == "AwaitingReplacement" and s["currentOrdinal"] == 0 for s in samples_awaiting) and
                 not [r for r in session.proxy_requests(hold_start) if "/nodes/2/drain" in r["path"] or "/nodes/3/drain" in r["path"]],
                 sorted({(s["phase"], s["currentOrdinal"]) for s in samples_awaiting}))
    fx.mutate("delete", "validatingwebhookconfiguration", WEBHOOK, "--ignore-not-found")
    end = time.time()
    session.mark_window("delete_unknown", start, end, allowed_pods=(pod,))
    return fx.save_result("rollout-delete-unknown.json", checks.result(
        "delete_unknown", approved=approved, baseline=baseline, samples=samples, restartDuringAuditRecovery=restart_audit,
        restartDuringUnknownDelete=restart_delete, resumed=resumed, reapproved=reapproved, raceRV=race_rv, raceFirstAdmission=first,
        admissions=admissions, finalAdmission=final, apiDeleteRecords=api_records, deleting=deleting, awaiting=awaiting, deletesAtDeletion=deletes_at_deletion,
        restartDuringAwaiting=restart_awaiting, samplesAwaiting=samples_awaiting, window=[start, end]))


def release_hold(session: Session, pod: str = "narumq-0") -> dict[str, Any]:
    fx = session.fixture
    fx.mutate("patch", "pod", pod, "--type=json", "-p", json.dumps([{"op": "remove", "path": "/metadata/finalizers"}]))
    return wait_for(lambda: (lambda s: s if 0 in s.get("completedOrdinals", []) else None)(fx.status(session.rollout_name)), 900, 5,
                    "ordinal 0 replacement completed")


# Non-leader controller process ----------------------------------------------------------------
def standby_deployment(fixture: Fixture) -> dict[str, Any]:
    """A second controller process from the live template (same image, ServiceAccount and proxy route).

    The product Deployment stays replicas=1/Recreate. The standby gets its own emptyDir audit
    directory because the FileAuditPublisher keeps the hash-chain head in memory; it is stopped
    before every leader restart so it can never take the leader Lease.
    """
    template = fixture.kjson("get", "deployment", CONTROLLER)["spec"]["template"]
    labels = {"app.kubernetes.io/name": STANDBY}
    template["metadata"] = {"labels": labels}
    template["spec"]["volumes"] = [{"name": v["name"], "emptyDir": {}} if "persistentVolumeClaim" in v else v
                                   for v in template["spec"].get("volumes", [])]
    return {"apiVersion": "apps/v1", "kind": "Deployment", "metadata": {"name": STANDBY, "namespace": fixture.namespace, "labels": labels},
            "spec": {"replicas": 1, "strategy": {"type": "Recreate"}, "selector": {"matchLabels": labels}, "template": template}}


def start_standby(session: Session) -> dict[str, Any]:
    fx = session.fixture
    fx.apply(standby_deployment(fx))
    fx.mutate("rollout", "status", f"deployment/{STANDBY}", "--timeout=300s", timeout=320)
    item = wait_for(lambda: next((i for i in fx.kjson("get", "pods", "-l", f"app.kubernetes.io/name={STANDBY}")["items"]
                                  if i["status"].get("podIP") and not i["metadata"].get("deletionTimestamp")
                                  and all(c.get("ready") for c in i["status"].get("containerStatuses", [{}]))), None), 300, 3, "standby Ready")
    standby = {"name": item["metadata"]["name"], "uid": item["metadata"]["uid"], "ip": item["status"]["podIP"],
               "node": item["spec"].get("nodeName"), "startedAt": time.time(), "leader": fx.leader()}
    if standby["leader"]["holder"].startswith(standby["name"] + "_"):
        raise RuntimeError("standby process took the leader Lease")
    return standby


def stop_standby(session: Session, standby: dict[str, Any], checks: Checks, label: str) -> dict[str, Any]:
    fx = session.fixture
    standby["leaderAtStop"] = fx.leader()
    fx.mutate("delete", "deployment", STANDBY, "--wait=true", timeout=180)
    wait_for(lambda: not fx.kjson("get", "pods", "-l", f"app.kubernetes.io/name={STANDBY}")["items"], 180, 2, "standby gone")
    standby["stoppedAt"] = time.time()
    window = [r for r in session.proxy_requests(standby["startedAt"]) if r["at"] <= standby["stoppedAt"]]
    own = [r for r in window if r["client"] == standby["ip"]]
    leader_drain = [r for r in window if r["client"] != standby["ip"] and "/drain" in r["path"]]
    checks.check(f"{label}: non-leader process made no management API call while the leader made drain calls",
                 not own and bool(leader_drain), {"standbyCalls": own[:5], "leaderDrainCalls": len(leader_drain)})
    checks.check(f"{label}: the leader Lease never moved to the non-leader process",
                 not standby["leaderAtStop"]["holder"].startswith(standby["name"] + "_"), standby["leaderAtStop"])
    session.standbys.append(standby)
    return standby


# Scenario E: drain POST unknown, restart at the request boundary, non-leader process ----------------
# Scenarios: drain-and-rolling-update.md:DRN-SCN-024
@scenario_result
def scenario_post_unknown_non_leader(session: Session) -> dict[str, Any]:
    fx, checks, start = session.fixture, Checks(), time.time()
    leader_before = fx.leader()
    leader_pod = next(p for p in fx.controller_pods() if leader_before["holder"].startswith(p["name"] + "_") and not p["deleting"])
    standby_a = start_standby(session)
    # Only the current leader's drain POST responses are withheld; the broker still executes them.
    session.proxy_set(getMode="relay", postResponseDelaySeconds=8, delayClients=[leader_pod["ip"]])
    release = release_hold(session)
    first_post = wait_for(lambda: [r for r in session.proxy_requests(start) if r["method"] == "POST" and "/nodes/2/drain" in r["path"]],
                          600, 1, "node-2 drain POST")
    unknown = wait_for(lambda: (lambda s: s if s.get("failureCode") == "drain_start_unconfirmed" else None)(fx.status(session.rollout_name)),
                       120, 2, "drain_start_unconfirmed")
    checks.check("restart happens while the drain start result is unknown (no adopted operation)",
                 not unknown.get("drainOperationID") and unknown.get("currentOrdinal") == 1,
                 {k: unknown.get(k) for k in ("drainOperationID", "currentOrdinal", "failureCode")})
    stop_standby(session, standby_a, checks, "POST unknown window")
    restart = session.restart_controller()
    resumed = wait_for(lambda: (lambda s: s if s.get("drainOperationID") else None)(fx.status(session.rollout_name)), 180, 3, "operation adopted")
    session.proxy_set(postResponseDelaySeconds=0, delayClients=None)
    standby_b = start_standby(session)
    posts = [r for r in session.proxy_requests(start) if r["method"] == "POST" and "/nodes/2/drain" in r["path"]]
    real = session.drain(2)
    checks.check("every POST used the persisted idempotency key", len(posts) >= 2 and {p["idempotencyKey"] for p in posts} == {unknown["attemptKey"]},
                 [(p["client"], p["action"], p["idempotencyKey"]) for p in posts])
    checks.check("new leader replayed the POST", any(p["client"] == restart["leaderPod"]["ip"] for p in posts), restart["leaderPod"])
    checks.check("new leader adopted the operation the broker started for the first POST",
                 real[0] == 200 and real[1]["data"]["operationId"] == resumed["drainOperationID"], {"real": real, "status": resumed.get("drainOperationID")})
    before_change = {r["client"] for r in session.proxy_requests(start) if r["at"] < restart["deletedAt"]}
    checks.check("before the leader change only the leader called the management API", before_change == {leader_pod["ip"]},
                 {"callers": sorted(before_change), "leader": leader_pod["ip"]})
    awaiting = wait_for(lambda: (lambda s: s if s.get("currentOrdinal") == 1 and s.get("phase") == "AwaitingReplacement" else None)(
        fx.status(session.rollout_name)), 900, 2, "ordinal 1 AwaitingReplacement")
    # Hold ordinal 2 before its first drain GET succeeds, so scenario F controls its approval write.
    session.proxy_set(getMode="unavailable")
    done = wait_for(lambda: (lambda s: s if 1 in s.get("completedOrdinals", []) else None)(fx.status(session.rollout_name)), 900, 5, "ordinal 1 completed")
    after_change = {r["client"] for r in session.proxy_requests(restart["leaderChangedAt"])}
    checks.check("after the leader change only the new leader called the management API", after_change == {restart["leaderPod"]["ip"]},
                 {"callers": sorted(after_change), "leader": restart["leaderPod"]["ip"], "standby": standby_b["ip"]})
    end = time.time()
    session.mark_window("post_unknown_non_leader", start, end, allowed_pods=("narumq-0", "narumq-1"))
    return fx.save_result("rollout-post-unknown-non-leader.json", checks.result(
        "post_unknown_non_leader", leaderBefore=leader_before, standbyA=standby_a, standbyB=standby_b, ordinal0=release, firstPost=first_post,
        unknown=unknown, restart=restart, resumed=resumed, posts=posts, realDrain=real, awaiting=awaiting, ordinal1=done, window=[start, end]))


# Scenario F: status CAS conflict at every write, including the approval write ---------------------
# Scenarios: drain-and-rolling-update.md:DRN-SCN-026
@scenario_result
def scenario_cas_conflict(session: Session, standby: dict[str, Any], after_safe: int = 3) -> dict[str, Any]:
    fx, checks, start = session.fixture, Checks(), time.time()
    held = wait_for(lambda: (lambda s: s if s.get("currentOrdinal") == 2 and s.get("failureCode") == "drain_observation_unknown" else None)(
        fx.status(session.rollout_name)), 300, 2, "ordinal 2 held before its first drain observation")
    session.proxy_set(getMode="relay", getDelaySeconds=3)
    injected, seen, safe_seen, deadline = [], set(), 0, time.monotonic() + 600
    while safe_seen < after_safe:
        if time.monotonic() > deadline:
            raise TimeoutError("broker did not reach SAFE_TO_STOP while CAS conflicts were injected")
        fresh = [r for r in session.proxy_requests(start) if r["method"] == "GET" and "/nodes/3/drain" in r["path"]
                 and (r["at"], r["client"]) not in seen]
        if not fresh:
            time.sleep(0.2)
            continue
        seen.update((r["at"], r["client"]) for r in fresh)
        fx.mutate("annotate", "narumqrollout", session.rollout_name, f"narumq.io/acceptance-cas={len(injected)}-{time.time():.3f}", "--overwrite")
        real = session.drain(3)
        safe = real[0] == 200 and real[1]["data"]["safeToStop"] is True
        safe_seen += 1 if safe else 0
        status = fx.status(session.rollout_name)
        injected.append({"getAt": fresh[-1]["at"], "annotatedAt": time.time(), "brokerSafe": safe, "phase": status.get("phase"),
                         "lastTransitionTime": status.get("lastTransitionTime"),
                         "deleteIntent": status.get("deleteIntent", False), "podDeleting": bool(fx.broker_pods()["narumq-2"]["deleting"])})
    session.proxy_set(getDelaySeconds=0)
    checks.check("while injected annotations raced status writes, no delete intent was persisted and narumq-2 was not deleted",
                 not any(i["deleteIntent"] or i["podDeleting"] for i in injected),
                 [(i["phase"], i["brokerSafe"], i["deleteIntent"], i["podDeleting"]) for i in injected])
    checks.check(f"injections covered {after_safe} drain GETs after the real broker reported SAFE_TO_STOP",
                 safe_seen >= after_safe, safe_seen)
    leader_pod = next(p for p in fx.controller_pods() if (fx.leader()["holder"] or "").startswith(p["name"] + "_"))
    logs = fx.kubectl("logs", leader_pod["name"], f"--since-time={time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(start))}")
    # The status adapter checks UID/resourceVersion itself and returns a Kubernetes Conflict, so the
    # conflict appears only in the controller log (no 409 in the API audit). Unchanged polls do not write.
    conflict_lines = [line for line in logs.splitlines() if "Operation cannot be fulfilled on narumqrollouts" in line]
    # save() sets lastTransitionTime from the controller clock, the same clock as the controller log.
    drain_safe_at = min((i["lastTransitionTime"] for i in injected if i["phase"] == "DrainSafe"), default=None)
    approval_conflicts = [line for line in conflict_lines if drain_safe_at and json.loads(line)["ts"] >= drain_safe_at]
    checks.check("controller logged status CAS conflicts on approval writes after DrainSafe and reconciled again",
                 len(approval_conflicts) >= 1, {"drainSafeAt": drain_safe_at, "approvalConflicts": approval_conflicts[:5],
                                                "allConflicts": len(conflict_lines)})
    completed = wait_for(lambda: (lambda s: s if s.get("phase") == "Completed" else None)(fx.status(session.rollout_name)), 900, 5, "Completed")
    stop_standby(session, standby, checks, "CAS and final DELETE window")
    end = time.time()
    session.mark_window("cas_conflict", start, end, allowed_pods=("narumq-2",))
    return fx.save_result("rollout-cas-conflict.json", checks.result("cas_conflict", held=held, injected=injected,
                                                                     conflictLog=conflict_lines[:20], completed=completed, window=[start, end]))


# Scenario G: the three controller audit actions in the real durable file ---------------------------
# Scenarios: drain-and-rolling-update.md:DRN-SCN-026
@scenario_result
def scenario_audit_events(session: Session, controller_version: str) -> dict[str, Any]:
    fx, checks = session.fixture, Checks()
    start_helper(session)
    chain = read_audit_chain(session)
    events = chain["events"]
    checks.check("broker Go verifier accepts the controller audit hash chain", chain["verifyExitCode"] == 0, chain["verifyOutput"])
    actions = {event["action"] for event in events}
    checks.check("approval, denial and terminal failure actions are all present",
                 {"rollout.pod.delete_approved", "rollout.pod.delete_denied", "rollout.failed"} <= actions, sorted(actions))
    checks.check("every event names the controller producer, version and a real controller Pod UID",
                 all(e["producer"]["component"] == "ROLLOUT_CONTROLLER" and e["producer"]["version"] == controller_version
                     and e["producer"]["instanceId"] in fx.controller_uids and e["actor"]["subject"] == CONTROLLER for e in events),
                 sorted({(e["producer"]["instanceId"], e["producer"]["version"]) for e in events}))
    checks.check("requestId equals eventId and event IDs are unique",
                 all(e["requestId"] == e["eventId"] for e in events) and len({e["eventId"] for e in events}) == len(events), len(events))
    approvals = [e for e in events if e["action"] == "rollout.pod.delete_approved"]
    checks.check("every approval carries full SAFE barrier evidence",
                 bool(approvals) and all(e["result"] == "ALLOWED" and e["reason"] == "safe_to_stop" and e["details"]["drainSafeToStop"] is True
                                         and e["details"]["connectionsRemaining"] == 0 and e["details"]["pendingWAL"] == 0
                                         and e["details"]["underReplicated"] == 0 and e["details"]["sessionsFailed"] == 0 for e in approvals),
                 len(approvals))
    # The chain is shared by every rollout in the fixture; the final rollout must approve exactly its original Pods.
    rollout_uid = fx.kjson("get", "narumqrollout", session.rollout_name)["metadata"]["uid"]
    approved_uids = {e["details"]["podUid"] for e in approvals if e["details"]["rolloutUid"] == rollout_uid}
    initial_uids = {pod["uid"] for pod in session.initial_pods.values()}
    checks.check("the final rollout's approvals cover exactly its three original broker Pod UIDs", approved_uids == initial_uids,
                 {"rolloutUid": rollout_uid, "approved": sorted(approved_uids), "initial": sorted(initial_uids)})
    failed = [e for e in events if e["action"] == "rollout.failed"]
    checks.check("terminal failures carry the failure cause and the fixed safe operator action",
                 bool(failed) and all(e["result"] == "FAILED" and e["details"].get("failureReason") and "spec.retryToken" in e["details"]["operatorAction"]
                                      and "spec.abort=true" in e["details"]["operatorAction"] for e in failed),
                 [(e["reason"], e["details"].get("failureReason")) for e in failed])
    denied = [e for e in events if e["action"] == "rollout.pod.delete_denied"]
    checks.check("denials name a stable blocker for a broker Pod", bool(denied) and all(e["result"] == "DENIED" and e["target"]["type"] == "broker_pod"
                                                                                       and e["details"]["observedBlocker"] == e["reason"] for e in denied),
                 [(e["target"]["id"], e["reason"]) for e in denied])
    raw = json.dumps(events)
    checks.check("no management token or registry credential appears in any event", not any(secret and secret in raw for secret in session.secrets),
                 f"{len(session.secrets)} secrets scanned")
    stop_helper(session)
    return fx.save_result("rollout-audit-events.json", checks.result("audit_events", verify=chain["verifyOutput"], events=events))


# Scenarios: drain-and-rolling-update.md:DRN-SCN-026, drain-and-rolling-update.md:DRN-SCN-027
@scenario_result
def verify_windows(session: Session) -> dict[str, Any]:
    """Cross-check all scenario windows against Kubernetes API audit DELETE records."""
    fx, checks = session.fixture, Checks()
    fx.audit_ingested_through(f"audit-sentinel-{int(time.time())}")
    report = []
    for window in session.windows:
        records = [r for r in fx.pod_delete_audit(window["start"], window["end"]) if r["user"] == CONTROLLER_USER]
        successful = [r for r in records if r["code"] in (200, 202)]
        bad = [r for r in successful if r["pod"] not in window["allowedDeletes"]]
        once = all(len([r for r in successful if r["pod"] == pod]) <= 1 for pod in window["allowedDeletes"])
        checks.check(f"{window['scenario']}: controller deleted only allowed Pods, each at most once", not bad and once,
                     {"successful": successful, "unexpected": bad})
        report.append({**window, "controllerDeletes": records})
    for standby in session.standbys:
        records = [r for r in fx.pod_delete_audit(standby["startedAt"], standby["stoppedAt"]) if standby["ip"] in (r["sourceIPs"] or [])]
        checks.check(f"non-leader process {standby['name']} sent no Pod DELETE", not records, records)
    return fx.save_result("rollout-kubernetes-audit-deletes.json", checks.result("kubernetes_audit_deletes", windows=report, standbys=session.standbys))


# Argo CD-applied broker replacement ------------------------------------------------------------
ARGO_NS = "argocd"
GIT = "rollout-git"
ARGO_APP = "narumq-broker"
ARGO_CLUSTER_KINDS = {"CustomResourceDefinition", "ClusterRole", "ClusterRoleBinding"}
ARGO_PERSIST_HEALTH_ENV = "ARGOCD_APPLICATION_CONTROLLER_PERSIST_RESOURCE_HEALTH"


def target_statefulset(statefulset: dict[str, Any], image: str, pull_secret: str) -> dict[str, Any]:
    """The broker StatefulSet with the target image in every broker container and the CR pull Secret."""
    desired = json.loads(json.dumps(statefulset))
    spec = desired["spec"]["template"]["spec"]
    for container in spec.get("initContainers", []) + spec["containers"]:
        container["image"] = image
    spec["imagePullSecrets"] = [{"name": pull_secret}]
    return desired


def git_repository_tar(files: dict[str, str]) -> tuple[bytes, str]:
    """Commit the files and return a dumb-HTTP-servable bare repository as a tar, plus the commit SHA."""
    import io
    import tarfile
    import tempfile
    with tempfile.TemporaryDirectory() as tmp:
        work, bare = Path(tmp) / "work", Path(tmp) / "narumq.git"
        work.mkdir()
        for name, text in files.items():
            (work / name).write_bytes(text.encode("utf-8"))

        def git(*args: str, cwd: Path) -> str:
            return subprocess.run(["git", "-c", "core.autocrlf=false", "-c", "user.name=narumq-acceptance",
                                   "-c", "user.email=acceptance@narumq.invalid", *args],
                                  cwd=cwd, check=True, capture_output=True, text=True).stdout.strip()
        git("init", "-q", "-b", "main", cwd=work)
        git("add", ".", cwd=work)
        git("commit", "-q", "-m", "narumq broker target release", cwd=work)
        revision = git("rev-parse", "HEAD", cwd=work)
        git("clone", "-q", "--bare", str(work), str(bare), cwd=Path(tmp))
        git("update-server-info", cwd=bare)
        buffer = io.BytesIO()
        with tarfile.open(fileobj=buffer, mode="w") as archive:
            archive.add(bare, arcname="narumq.git")
        return buffer.getvalue(), revision


def git_server_objects(nginx_image: str, repository_tar: bytes) -> list[dict[str, Any]]:
    labels = {"app.kubernetes.io/name": GIT}
    www = {"name": "www", "mountPath": "/usr/share/nginx/html"}
    return [
        {"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": GIT, "namespace": ARGO_NS},
         "binaryData": {"repo.tar": base64.b64encode(repository_tar).decode()}},
        {"apiVersion": "apps/v1", "kind": "Deployment", "metadata": {"name": GIT, "namespace": ARGO_NS, "labels": labels},
         "spec": {"replicas": 1, "selector": {"matchLabels": labels}, "template": {"metadata": {"labels": labels}, "spec": {
             "initContainers": [{"name": "unpack", "image": nginx_image, "command": ["sh", "-c", "tar -xf /src/repo.tar -C /usr/share/nginx/html"],
                                 "volumeMounts": [{"name": "src", "mountPath": "/src", "readOnly": True}, www]}],
             "containers": [{"name": "nginx", "image": nginx_image, "ports": [{"containerPort": 80}], "volumeMounts": [www],
                             "readinessProbe": {"httpGet": {"path": "/narumq.git/info/refs", "port": 80}, "periodSeconds": 2}}],
             "volumes": [{"name": "src", "configMap": {"name": GIT}}, {"name": "www", "emptyDir": {}}]}}}},
        {"apiVersion": "v1", "kind": "Service", "metadata": {"name": GIT, "namespace": ARGO_NS},
         "spec": {"selector": labels, "ports": [{"port": 80, "targetPort": 80}]}},
    ]


def argo_install_objects(core_install: bytes, argocd_image: str, redis_image: str, health_lua: str) -> list[dict[str, Any]]:
    """Pinned Argo CD core install with the NaruMQRollout health check and persisted resource health."""
    import yaml
    objects = [d for d in yaml.safe_load_all(core_install) if d]
    for obj in objects:
        if obj["kind"] not in ARGO_CLUSTER_KINDS:
            obj["metadata"]["namespace"] = ARGO_NS
        pod = obj.get("spec", {}).get("template", {}).get("spec") if obj["kind"] in ("Deployment", "StatefulSet") else None
        for container in (pod or {}).get("initContainers", []) + (pod or {}).get("containers", []):
            if "argoproj/argocd" in container["image"]:
                container["image"] = argocd_image
            elif "redis" in container["image"]:
                container["image"] = redis_image
            else:
                raise ValueError(f"unpinned Argo CD image {container['image']}")
        if obj["kind"] == "ConfigMap" and obj["metadata"]["name"] == "argocd-cm":
            obj["data"] = {**(obj.get("data") or {}), "resource.customizations.health.narumq.io_NaruMQRollout": health_lua}
        if obj["kind"] == "StatefulSet" and obj["metadata"]["name"] == "argocd-application-controller":
            container = next(c for c in pod["containers"] if c["name"] == "argocd-application-controller")
            container["env"] = [e for e in container.get("env", []) if e["name"] != ARGO_PERSIST_HEALTH_ENV] + \
                [{"name": ARGO_PERSIST_HEALTH_ENV, "value": "true"}]
    return objects


def argo_application_objects(repo_url: str, revision: str) -> list[dict[str, Any]]:
    project = {"apiVersion": "argoproj.io/v1alpha1", "kind": "AppProject", "metadata": {"name": "narumq-rollout", "namespace": ARGO_NS},
               "spec": {"sourceRepos": [repo_url], "destinations": [{"server": "https://kubernetes.default.svc", "namespace": NAMESPACE}],
                        "namespaceResourceWhitelist": [{"group": "apps", "kind": "StatefulSet"}, {"group": "narumq.io", "kind": "NaruMQRollout"}]}}
    app = {"apiVersion": "argoproj.io/v1alpha1", "kind": "Application", "metadata": {"name": ARGO_APP, "namespace": ARGO_NS},
           "spec": {"project": "narumq-rollout", "destination": {"server": "https://kubernetes.default.svc", "namespace": NAMESPACE},
                    "source": {"repoURL": repo_url, "targetRevision": revision, "path": ".", "directory": {"include": "*.json"}},
                    "syncPolicy": {"automated": {"prune": False, "selfHeal": False}}}}
    return [project, app]


# Scenarios: drain-and-rolling-update.md:DRN-SCN-026
@scenario_result
def scenario_argo_upgrade(session: Session, rollout_name: str, target: dict[str, Any], repo_url: str, revision: str,
                          statefulset_in_git: dict[str, Any], timeout: float = 2700) -> dict[str, Any]:
    """Argo CD syncs the target StatefulSet template and NaruMQRollout from Git; the controller replaces the brokers."""
    fx, checks, start = session.fixture, Checks(), time.time()
    before = fx.broker_pods()
    fx.apply(*argo_application_objects(repo_url, revision), namespace=ARGO_NS)
    timeline: list[dict[str, Any]] = []
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        app = fx.kjson("get", "application", ARGO_APP, namespace=ARGO_NS)
        status = fx.status(rollout_name)
        pods = fx.broker_pods()
        sample = {"at": time.time(), "sync": app.get("status", {}).get("sync", {}).get("status"),
                  "revision": app.get("status", {}).get("sync", {}).get("revision"),
                  "appHealth": app.get("status", {}).get("health", {}).get("status"),
                  "rolloutHealth": next((r.get("health", {}).get("status") for r in app.get("status", {}).get("resources", [])
                                         if r.get("kind") == "NaruMQRollout"), None),
                  "operation": app.get("status", {}).get("operationState", {}).get("phase"),
                  "phase": status.get("phase"), "completed": status.get("completedOrdinals", []), "failureCode": status.get("failureCode"),
                  "pods": {name: (p["uid"], p["imageID"], p["ready"]) for name, p in pods.items()}}
        timeline.append(sample)
        if sample["phase"] in ("Completed", "Failed", "Aborted") and sample["appHealth"] in ("Healthy", "Degraded"):
            break
        time.sleep(5)
    # The 2026-10-16 OutOfSync defect appeared only after completion, so hold the completed state for 60 s.
    settle: list[dict[str, Any]] = []
    settle_end = time.monotonic() + 60
    while time.monotonic() < settle_end:
        status = fx.kjson("get", "application", ARGO_APP, namespace=ARGO_NS).get("status", {})
        resource = next((r for r in status.get("resources", []) if r.get("kind") == "NaruMQRollout" and r.get("name") == rollout_name), {})
        settle.append({"at": time.time(), "revision": status.get("sync", {}).get("revision"), "sync": status.get("sync", {}).get("status"),
                       "rolloutSync": resource.get("status"), "appHealth": status.get("health", {}).get("status")})
        time.sleep(5)
    final_app = fx.kjson("get", "application", ARGO_APP, namespace=ARGO_NS)
    final = fx.status(rollout_name)
    after = fx.broker_pods()
    live_sts = fx.kjson("get", "statefulset", "narumq")
    features = session.management.call("GET", "/api/v1/cluster/features")
    end = time.time()
    digest = target["image"].split("@", 1)[1]
    checks.check("Argo CD synced the pinned Git revision successfully",
                 final_app["status"]["sync"]["revision"] == revision and final_app["status"]["sync"]["status"] == "Synced"
                 and final_app["status"].get("operationState", {}).get("phase") == "Succeeded", final_app["status"].get("operationState"))
    checks.check("live StatefulSet template equals the Git target (image and pull Secret)",
                 [c["image"] for c in live_sts["spec"]["template"]["spec"]["containers"]] ==
                 [c["image"] for c in statefulset_in_git["spec"]["template"]["spec"]["containers"]]
                 and live_sts["spec"]["template"]["spec"].get("imagePullSecrets") == statefulset_in_git["spec"]["template"]["spec"]["imagePullSecrets"],
                 live_sts["spec"]["template"]["spec"]["containers"][0]["image"])
    checks.check("NaruMQRollout from Git completed", final.get("phase") == "Completed" and sorted(final.get("completedOrdinals", [])) == [0, 1, 2],
                 {k: final.get(k) for k in ("phase", "completedOrdinals", "failureCode", "failureReason")})
    checks.check("every broker Pod was replaced once with the target digest and is Ready",
                 all(after[name]["uid"] != pod["uid"] and after[name]["imageID"].endswith(digest) and after[name]["ready"] for name, pod in before.items()),
                 {name: (pod["uid"], after.get(name, {}).get("uid"), after.get(name, {}).get("imageID")) for name, pod in before.items()})
    members = features[1]["data"]["members"] if features[0] == 200 else []
    checks.check("committed cluster profile reports the target release on every voter",
                 len(members) == 3 and all(m["releaseVersion"] == target["releaseVersion"] for m in members), features)
    # Before Argo's resource tree contains the NaruMQRollout (Missing during the first sync), the
    # Application health is Argo's own aggregate, not the NaruMQRollout health mapping.
    in_tree = [s for s in timeline if s["rolloutHealth"] not in (None, "Missing")]
    healthy_early = [s for s in in_tree if s["appHealth"] == "Healthy" and s["phase"] != "Completed" and s["revision"] == revision]
    checks.check("once the NaruMQRollout was in Argo's resource tree, Argo never reported Healthy before the rollout completed",
                 bool(in_tree) and not healthy_early, {"early": healthy_early[:3],
                                                       "beforeTree": [s for s in timeline if s not in in_tree][:3]})
    checks.check("Argo CD reported Progressing while the controller replaced brokers, then Healthy",
                 any(s["appHealth"] == "Progressing" and s["phase"] not in (None, "Completed") for s in timeline)
                 and final_app["status"]["health"]["status"] == "Healthy", sorted({(s["appHealth"], s["phase"]) for s in timeline}, key=str))
    checks.check("Argo CD kept the Application and the NaruMQRollout Synced and Healthy for 60 s after the rollout completed",
                 bool(settle) and all(s["revision"] == revision and s["sync"] == "Synced" and s["rolloutSync"] == "Synced"
                                      and s["appHealth"] == "Healthy" for s in settle),
                 sorted({(s["sync"], s["rolloutSync"], s["appHealth"]) for s in settle}, key=str))
    session.mark_window("argo_upgrade", start, end, allowed_pods=("narumq-0", "narumq-1", "narumq-2"))
    return fx.save_result("rollout-argo-upgrade.json", checks.result(
        "argo_upgrade", revision=revision, repoURL=repo_url, target=target, before=before, after=after, final=final,
        application=final_app.get("status"), clusterFeatures=features, timeline=timeline, settle=settle, window=[start, end]))


def verify_argo_deletes(session: Session, start: float, end: float) -> dict[str, Any]:
    """Kubernetes API audit: during the Argo stage only the controller deleted broker Pods, each once."""
    fx, checks = session.fixture, Checks()
    fx.audit_ingested_through(f"audit-sentinel-{int(time.time())}")
    records = fx.pod_delete_audit(start, end)
    brokers = [r for r in records if r["pod"] in ("narumq-0", "narumq-1", "narumq-2") and r["code"] in (200, 202)]
    by_controller = [r for r in brokers if r["user"] == CONTROLLER_USER]
    # After graceful termination the kubelet removes the Pod object with a UID-only DELETE; that completes
    # the controller's request and is not a second deletion decision.
    approved_uids = {r["preconditions"]["uid"] for r in by_controller if r.get("preconditions")}
    kubelet = [r for r in brokers if r["user"].startswith("system:node:") and (r.get("preconditions") or {}).get("uid") in approved_uids]
    others = [r for r in brokers if r not in by_controller and r not in kubelet]
    checks.check("each broker Pod was deleted exactly once by the rollout controller; only the kubelet completed those deletions",
                 sorted(r["pod"] for r in by_controller) == ["narumq-0", "narumq-1", "narumq-2"] and len(kubelet) <= 3 and not others,
                 {"controller": by_controller, "kubeletCompletion": kubelet, "others": others})
    checks.check("Argo CD never deleted a Pod", not [r for r in records if r["user"].startswith(f"system:serviceaccount:{ARGO_NS}:")], records)
    return fx.save_result("rollout-argo-kubernetes-audit.json", checks.result("argo_kubernetes_audit", records=records, window=[start, end]))


# Scenarios: drain-and-rolling-update.md:DRN-SCN-026
@scenario_result
def scenario_argo_finalizer_sync(session: Session, previous_rollout: str, rollout_name: str, repo_url: str,
                                 files: dict[str, str], nginx_image: str, timeout: float = 900) -> dict[str, Any]:
    """Controller finalizer writes keep a Git-applied NaruMQRollout Synced (explicit spec.abort=false survives).

    The previous Argo rollout is deleted first, so the controller releases its finalizer. A new Git revision then
    carries a rollout whose target already matches every broker: the controller installs its finalizer and completes
    every ordinal without a Pod replacement."""
    fx, checks, start = session.fixture, Checks(), time.time()
    before = fx.broker_pods()
    fx.mutate("delete", "narumqrollout", previous_rollout, "--wait=true", "--timeout=180s", timeout=200)
    checks.check("the controller released the previous Argo rollout's finalizer and the CR was removed",
                 fx.rollout(previous_rollout) is None, previous_rollout)
    repository_tar, revision = git_repository_tar(files)
    fx.apply(*git_server_objects(nginx_image, repository_tar), namespace=ARGO_NS)
    fx.mutate("rollout", "restart", f"deployment/{GIT}", namespace=ARGO_NS)
    fx.mutate("rollout", "status", f"deployment/{GIT}", "--timeout=180s", namespace=ARGO_NS, timeout=200)
    fx.apply(*argo_application_objects(repo_url, revision), namespace=ARGO_NS)
    timeline: list[dict[str, Any]] = []
    stable_since: float | None = None
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        status = fx.kjson("get", "application", ARGO_APP, namespace=ARGO_NS).get("status", {})
        resource = next((r for r in status.get("resources", []) if r.get("kind") == "NaruMQRollout" and r.get("name") == rollout_name), {})
        live = fx.rollout(rollout_name) or {}
        sample = {"at": time.time(), "revision": status.get("sync", {}).get("revision"), "sync": status.get("sync", {}).get("status"),
                  "operation": status.get("operationState", {}).get("phase"), "appHealth": status.get("health", {}).get("status"),
                  "rolloutSync": resource.get("status"), "rolloutHealth": resource.get("health", {}).get("status"),
                  "phase": live.get("status", {}).get("phase"), "finalizers": live.get("metadata", {}).get("finalizers"),
                  "liveSpecAbort": live.get("spec", {}).get("abort", "<absent>")}
        timeline.append(sample)
        settled = (sample["revision"] == revision and sample["sync"] == "Synced" and sample["rolloutSync"] == "Synced"
                   and sample["phase"] == "Completed" and sample["appHealth"] == "Healthy")
        stable_since = (stable_since or sample["at"]) if settled else None
        # Hold the settled state long enough for Argo to compare the CR after the finalizer and status writes.
        if stable_since is not None and sample["at"] - stable_since >= 60:
            break
        time.sleep(5)
    final_app = fx.kjson("get", "application", ARGO_APP, namespace=ARGO_NS)
    live = fx.rollout(rollout_name) or {}
    after = fx.broker_pods()
    end = time.time()
    fx.audit_ingested_through(f"audit-sentinel-{int(time.time())}")
    deletes = [r for r in fx.pod_delete_audit(start, end) if r["pod"] in before and r["code"] in (200, 202)]
    checks.check("Argo CD synced the pinned Git revision and kept the NaruMQRollout Synced for 60 s after it completed",
                 stable_since is not None and final_app["status"]["sync"]["revision"] == revision
                 and final_app["status"]["sync"]["status"] == "Synced" and final_app["status"].get("operationState", {}).get("phase") == "Succeeded",
                 {"operationState": final_app["status"].get("operationState"), "resources": final_app["status"].get("resources")})
    checks.check("the live CR keeps the Git spec.abort=false with the controller finalizer installed",
                 live.get("spec", {}).get("abort", "<absent>") is False and "narumq.io/rollout-protection" in live.get("metadata", {}).get("finalizers", []),
                 {"spec": live.get("spec"), "finalizers": live.get("metadata", {}).get("finalizers")})
    checks.check("the rollout completed every ordinal without replacing a broker Pod",
                 sorted(live.get("status", {}).get("completedOrdinals", [])) == [0, 1, 2]
                 and all(after.get(name, {}).get("uid") == pod["uid"] for name, pod in before.items()) and not deletes,
                 {"status": live.get("status"), "deletes": deletes})
    session.mark_window("argo_finalizer_sync", start, end)
    return fx.save_result("rollout-argo-sync-r3.json", checks.result(
        "argo_finalizer_sync", revision=revision, repoURL=repo_url, files=files, before=before, after=after,
        application=final_app.get("status"), timeline=timeline, window=[start, end]))
