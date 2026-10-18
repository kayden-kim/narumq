"""Kubernetes objects and the stub management endpoint for the DRN-SCN-030 harness.

A probe Pod runs the broker image with the live StatefulSet's real preStop command. Beside it a stub container
answers on 127.0.0.1:8080 (the address the hook calls) with either HTTP 401 or a drain that never leaves
DRAINING. The stub replaces only the broker's management endpoint; the hook, its image and the kubelet
termination path are the real ones.
"""

from __future__ import annotations

import sys
from typing import Any

sys.dont_write_bytecode = True

RUN_LABEL = "narumq.io/acceptance-run"
PROBE_LABEL = "narumq.io/prestop-probe"
MODES = ("unauthorized", "draining")
STUB_TOKEN = "stub-token-not-a-credential"
# The probe name must end in -0: the hook derives its slot file from ${HOSTNAME##*-}.
PROBE_POD = {"unauthorized": "prestop-probe-fail-0", "draining": "prestop-probe-grace-0"}
CONFIG_MAP = "prestop-probe-config"
SECRET = "prestop-probe-token"
STUB_NODE_ID = "1"
# PID 1 must handle SIGTERM, otherwise the kernel ignores it and the container would outlive its hook.
MAIN_COMMAND = ["/bin/sh", "-c", "trap 'exit 0' TERM; while :; do sleep 1; done"]

STUB_SCRIPT = r'''
import http.server, json, os, signal, sys, time

MODE = sys.argv[1]
PORT = int(sys.argv[2]) if len(sys.argv) > 2 else 8080
signal.signal(signal.SIGTERM, lambda *_: os._exit(0))


class Handler(http.server.BaseHTTPRequestHandler):
    def reply(self):
        auth = self.headers.get("Authorization", "")
        print(json.dumps({"at": time.time(), "method": self.command, "path": self.path,
                          "bearer": auth.startswith("Bearer ") and len(auth) > 7,
                          "idempotencyKey": bool(self.headers.get("Idempotency-Key"))}), flush=True)
        if MODE == "unauthorized":
            code, body = 401, {"error": {"code": "unauthorized", "message": "stub: credential rejected"}}
        else:
            code, body = 200, {"data": {"nodeId": "1", "operationId": "stub-operation", "phase": "DRAINING",
                                        "state": "DRAINING", "safeToStop": False}}
        raw = json.dumps(body, separators=(",", ":")).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    do_GET = do_POST = do_DELETE = reply

    def log_message(self, *_):
        pass


http.server.ThreadingHTTPServer(("127.0.0.1", PORT), Handler).serve_forever()
'''


def probe_config_map(run_id: str, namespace: str) -> dict[str, Any]:
    return {"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": CONFIG_MAP, "namespace": namespace, "labels": {RUN_LABEL: run_id}},
            "data": {"slot-0": STUB_NODE_ID}}


def probe_secret(run_id: str, namespace: str) -> dict[str, Any]:
    return {"apiVersion": "v1", "kind": "Secret", "type": "Opaque",
            "metadata": {"name": SECRET, "namespace": namespace, "labels": {RUN_LABEL: run_id}},
            "stringData": {"admin.token": STUB_TOKEN}}


def probe_pod(mode: str, run_id: str, namespace: str, broker_image: str, stub_image: str, prestop_command: list[str], grace_seconds: int) -> dict[str, Any]:
    """A Pod that is not selected by any broker Service, PDB or StatefulSet (no ``app=narumq`` label)."""
    if mode not in MODES:
        raise ValueError(f"unknown probe mode {mode!r}")
    hardened = {"allowPrivilegeEscalation": False, "readOnlyRootFilesystem": True, "capabilities": {"drop": ["ALL"]}}
    resources = {"requests": {"cpu": "10m", "memory": "32Mi"}, "limits": {"cpu": "100m", "memory": "128Mi"}}
    return {"apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": PROBE_POD[mode], "namespace": namespace,
                         "labels": {RUN_LABEL: run_id, PROBE_LABEL: mode, "app.kubernetes.io/name": "prestop-probe"}},
            "spec": {"restartPolicy": "Never", "terminationGracePeriodSeconds": grace_seconds,
                     "securityContext": {"runAsNonRoot": True, "runAsUser": 10001, "runAsGroup": 10001, "seccompProfile": {"type": "RuntimeDefault"}},
                     "containers": [
                         {"name": "narumq", "image": broker_image, "imagePullPolicy": "IfNotPresent", "command": MAIN_COMMAND,
                          "lifecycle": {"preStop": {"exec": {"command": prestop_command}}},
                          "securityContext": hardened, "resources": resources,
                          "volumeMounts": [{"name": "configs", "mountPath": "/configs", "readOnly": True},
                                           {"name": "secrets", "mountPath": "/secrets", "readOnly": True}]},
                         {"name": "stub", "image": stub_image, "imagePullPolicy": "IfNotPresent",
                          "command": ["python3", "-c", STUB_SCRIPT, mode],
                          "env": [{"name": "PYTHONDONTWRITEBYTECODE", "value": "1"}],
                          "securityContext": hardened, "resources": resources}],
                     "volumes": [{"name": "configs", "configMap": {"name": CONFIG_MAP}},
                                 {"name": "secrets", "secret": {"secretName": SECRET}}]}}


def admission_statefulset(name: str, run_id: str, namespace: str, image: str, grace_seconds: int, annotations: dict[str, str] | None = None) -> dict[str, Any]:
    """A broker-shaped StatefulSet with zero replicas: nothing schedules, the API server only has to admit or reject it.

    The Pod template carries ``app=narumq`` so the policy treats it as a broker workload; the selector uses a
    distinct label so it can never match, adopt or be adopted by the live broker Pods.
    """
    selector = {"narumq.io/admission-probe": name}
    return {"apiVersion": "apps/v1", "kind": "StatefulSet",
            "metadata": {"name": name, "namespace": namespace, "labels": {RUN_LABEL: run_id, "app.kubernetes.io/name": "admission-probe"},
                         **({"annotations": dict(annotations)} if annotations else {})},
            "spec": {"replicas": 0, "serviceName": "narumq-headless", "updateStrategy": {"type": "OnDelete"},
                     "selector": {"matchLabels": selector},
                     "template": {"metadata": {"labels": {**selector, "app": "narumq", RUN_LABEL: run_id}},
                                  "spec": {"terminationGracePeriodSeconds": grace_seconds,
                                           "containers": [{"name": "narumq", "image": image}]}}}}
