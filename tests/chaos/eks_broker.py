"""Live-broker helpers shared by the strict-fixture EKS harnesses that drive broker Pods (DRN-SCN-030, DRN-SCN-033).

Everything here runs on a ``Fixture`` that already passed ``eks_fixture.build_fixture`` (kubeconfig context,
EKS endpoint and tags, namespace label). ``mutating_result`` refuses to run before that check and, unlike
``Fixture.mutate``, keeps the exit code and stderr of a command that is expected to fail.
"""

from __future__ import annotations

import base64
import json
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

sys.dont_write_bytecode = True
sys.path.append(str(Path(__file__).resolve().parent))
from eks_fixture import rollout_faults  # noqa: E402
from eks_port_forward import PortForward  # noqa: E402

BROKER_LABEL = "app=narumq"
SECRET_NAME = "narumq-cluster-secrets"
CONFIGMAP_NAME = "narumq-cluster-configs"
STATEFULSET = "narumq"


class PrerequisiteMissing(RuntimeError):
    """A fixture prerequisite is absent. The scenario cannot be judged, so the caller must record a FAIL."""


class AcceptanceFailure(RuntimeError):
    """Raised after the evidence is saved when a scenario check failed, so ``scenario_result`` records a FAIL."""


class ManagementError(RuntimeError):
    pass


def pod_ready(pod: dict[str, Any]) -> bool:
    return any(c.get("type") == "Ready" and c.get("status") == "True" for c in pod.get("status", {}).get("conditions", []))


def parse_time(value: str) -> float:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


def provider_instance_id(node: dict[str, Any]) -> str:
    """EC2 instance id from ``spec.providerID`` (``aws:///<zone>/i-...``)."""
    provider = node.get("spec", {}).get("providerID", "")
    instance = provider.rsplit("/", 1)[-1]
    if not provider.startswith("aws:///") or not instance.startswith("i-"):
        raise PrerequisiteMissing(f"node {node.get('metadata', {}).get('name')} has no AWS providerID: {provider!r}")
    return instance


def management_token(fixture: Any) -> str:
    """The broker admin token, read from the Secret that the broker Pods mount at /secrets/admin.token."""
    secret = fixture.kjson("get", "secret", SECRET_NAME)
    encoded = secret.get("data", {}).get("admin.token")
    if not encoded:
        raise PrerequisiteMissing(f"Secret {SECRET_NAME} has no admin.token")
    return base64.b64decode(encoded).decode("utf-8").strip()


def slot_map(fixture: Any) -> dict[int, str]:
    """StatefulSet ordinal -> Raft node id from the ``slot-N`` entries of the cluster ConfigMap."""
    data = fixture.kjson("get", "configmap", CONFIGMAP_NAME).get("data", {})
    slots = {int(key.removeprefix("slot-")): value.strip() for key, value in data.items()
             if key.startswith("slot-") and key.removeprefix("slot-").isdigit()}
    if not slots:
        raise PrerequisiteMissing(f"ConfigMap {CONFIGMAP_NAME} has no slot-N entries")
    return slots


def mutating_result(fixture: Any, *args: str, stdin: str | None = None, timeout: float = 120, namespace: str | None = None) -> subprocess.CompletedProcess:
    """kubectl in the fixture namespace that may fail; the caller judges the exit code and stderr."""
    if fixture.identity is None:
        raise rollout_faults().FixtureIdentityError("verify_identity() must pass before any mutation")
    return fixture.run(fixture.base() + ["-n", namespace or fixture.namespace, *args], stdin=stdin, timeout=timeout, check=False)


def delete_pod_with_uid(fixture: Any, name: str, uid: str) -> dict[str, Any]:
    """One graceful DELETE (the Pod's own grace period, so the live preStop hook runs) guarded by a UID precondition."""
    options = {"kind": "DeleteOptions", "apiVersion": "v1", "preconditions": {"uid": uid}}
    out = fixture.mutate("delete", "--raw", f"/api/v1/namespaces/{fixture.namespace}/pods/{name}", "-f", "-", stdin=json.dumps(options))
    return json.loads(out) if out.strip() else {}


class Management:
    """Authenticated Management API access to one broker Pod through an owned port-forward."""

    def __init__(self, fixture: Any, pod: str, token: str):
        self.fixture, self.pod, self.token = fixture, pod, token
        self.forward: PortForward | None = None
        self.port = 0

    def _ensure(self) -> None:
        if self.forward is not None and self.forward.alive():
            return
        self.close()
        forward = PortForward(self.fixture, self.fixture.namespace, f"pod/{self.pod}", 8080)
        self.port = forward.start()[8080]
        self.forward = forward

    def request(self, method: str, path: str, body: Any = None, headers: dict[str, str] | None = None, timeout: float = 8) -> tuple[int, Any]:
        module = rollout_faults()
        for attempt in range(2):
            self._ensure()
            try:
                return module.http_json(method, f"http://127.0.0.1:{self.port}{path}", body,
                                        {"Authorization": f"Bearer {self.token}", **(headers or {})}, timeout)
            except (OSError, ConnectionError, TimeoutError):
                self.close()
                if attempt:
                    raise
        raise AssertionError("unreachable")

    def data(self, path: str) -> Any:
        status, body = self.request("GET", path)
        if status != 200 or not isinstance(body, dict) or "data" not in body:
            raise ManagementError(f"GET {path} on {self.pod} returned {status}: {str(body)[:200]}")
        return body["data"]

    def close(self) -> None:
        forward, self.forward = self.forward, None
        if forward is not None:
            forward.close()


def wait_replaced_and_ready(fixture: Any, name: str, old_uid: str, timeout: float, interval: float = 2.0) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Wait until ``name`` is a new Pod (UID != old_uid, not deleting, Ready) and all broker Pods are Ready; return it with the samples."""
    samples: list[dict[str, Any]] = []
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        items = fixture.kjson("get", "pods", "-l", BROKER_LABEL)["items"]
        samples.append({"at": time.time(), "pods": [
            {"name": p["metadata"]["name"], "uid": p["metadata"]["uid"], "phase": p.get("status", {}).get("phase"),
             "node": p["spec"].get("nodeName"), "ready": pod_ready(p), "deleting": p["metadata"].get("deletionTimestamp")} for p in items]})
        target = next((p for p in items if p["metadata"]["name"] == name), None)
        if (target is not None and target["metadata"]["uid"] != old_uid and not target["metadata"].get("deletionTimestamp")
                and len(items) == 3 and all(pod_ready(p) and not p["metadata"].get("deletionTimestamp") for p in items)):
            return target, samples
        time.sleep(interval)
    raise TimeoutError(f"{name} was not replaced and Ready with all three brokers Ready in {timeout:.0f}s; last={samples[-1] if samples else None!r}")
