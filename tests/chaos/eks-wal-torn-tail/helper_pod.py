"""Helper Pod that mounts a stopped broker's PVC, byte-exact file transfer, and worker cordon control.

The helper never runs while its broker runs: the caller cordons every worker in the target zone first, so
the StatefulSet's replacement Pod cannot start, then deletes the broker Pod and waits until it is gone.
The helper is pinned to the broker's former worker (`nodeName`); the volume is already attached there.
Output of `kubectl exec` is never logged; only sizes and hashes leave this module.
"""
from __future__ import annotations

import hashlib
import json
import subprocess
import time
from typing import Callable

NAMESPACE = "narumq"
CONTAINER = "helper"
HELPER_LABEL = "narumq-wal-helper"
MOUNT = "/volume"
WAL_PATH = "/volume/data/wal.bin"
IDLE_COMMAND = 'trap "exit 0" TERM INT; while :; do sleep 1 & wait $!; done'
EXEC_TIMEOUT_SECONDS = 180
RUNNING_WAIT_SECONDS = 180
GONE_WAIT_SECONDS = 120


class HelperError(RuntimeError):
    """A helper operation failed; `code` is a stable identifier without command output."""

    def __init__(self, code: str):
        super().__init__(code)
        self.code = code


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def helper_name(pod_name: str) -> str:
    return f"wal-helper-{pod_name}"


def manifest(run_id: str, pod_name: str, pvc_name: str, node_name: str, image: str, user: int, group: int) -> dict:
    """Pod that runs as the broker's user and group so it can read and write the broker's files."""
    return {
        "apiVersion": "v1", "kind": "Pod",
        "metadata": {"name": helper_name(pod_name), "namespace": NAMESPACE,
                     "labels": {"app": HELPER_LABEL, "narumq.io/acceptance-run": run_id}},
        "spec": {
            "nodeName": node_name, "restartPolicy": "Never", "terminationGracePeriodSeconds": 5,
            "securityContext": {"runAsUser": user, "runAsGroup": group, "fsGroup": group,
                                "fsGroupChangePolicy": "OnRootMismatch"},
            "containers": [{
                "name": CONTAINER, "image": image, "imagePullPolicy": "IfNotPresent",
                "command": ["/bin/sh", "-c", IDLE_COMMAND],
                "securityContext": {"allowPrivilegeEscalation": False, "readOnlyRootFilesystem": True,
                                    "capabilities": {"drop": ["ALL"]}},
                "resources": {"requests": {"cpu": "50m", "memory": "64Mi"}, "limits": {"cpu": "100m", "memory": "128Mi"}},
                "volumeMounts": [{"name": "data", "mountPath": MOUNT}]}],
            "volumes": [{"name": "data", "persistentVolumeClaim": {"claimName": pvc_name}}]}}


def container_running(pod: dict | None) -> bool:
    if not pod or pod["metadata"].get("deletionTimestamp"):
        return False
    status = next((s for s in pod.get("status", {}).get("containerStatuses", []) if s.get("name") == CONTAINER), None)
    return bool(status and (status.get("state") or {}).get("running"))


class HelperPod:
    def __init__(self, fixture, run_id: str, pod_name: str, pvc_name: str, node_name: str, image: str, user: int,
                 group: int, sleep: Callable = time.sleep):
        self.fixture = fixture
        self.run_id = run_id
        self.name = helper_name(pod_name)
        self.spec = manifest(run_id, pod_name, pvc_name, node_name, image, user, group)
        self.sleep = sleep
        self.created = False

    def current(self) -> dict | None:
        return self.fixture.get("pod", self.name, "-n", NAMESPACE, "--ignore-not-found")

    def _owned(self, pod: dict | None) -> bool:
        return bool(pod and pod["metadata"].get("labels", {}).get("app") == HELPER_LABEL
                    and pod["metadata"].get("labels", {}).get("narumq.io/acceptance-run") == self.run_id)

    def create(self) -> None:
        if self.current() is not None:
            raise HelperError("helper_pod_exists")
        self.created = True  # set first: an unconfirmed create must still be cleaned up
        try:
            self.fixture.command(self.fixture.kubectl + ["create", "-f", "-", "-o", "name"],
                                 stdin=json.dumps(self.spec))
        except Exception as exc:
            raise HelperError("helper_create_failed") from exc

    def wait_running(self, timeout: float = RUNNING_WAIT_SECONDS) -> None:
        deadline = time.monotonic() + timeout
        while True:
            pod = self.current()
            if pod is not None and not self._owned(pod):
                raise HelperError("helper_pod_not_owned")
            if container_running(pod):
                return
            if time.monotonic() >= deadline:
                raise HelperError("helper_not_running")
            self.sleep(3)

    def _exec(self, command: list[str], stdin: bytes | None = None) -> bytes:
        argv = self.fixture.kubectl + ["-n", NAMESPACE, "exec", *(["-i"] if stdin is not None else []), self.name,
                                       "-c", CONTAINER, "--", *command]
        try:
            result = subprocess.run(argv, env=self.fixture.environment, input=stdin, capture_output=True,
                                    timeout=EXEC_TIMEOUT_SECONDS)
        except subprocess.TimeoutExpired as exc:
            raise HelperError("exec_timeout") from exc
        if result.returncode:
            raise HelperError("exec_failed")
        return result.stdout

    def read(self, path: str = WAL_PATH) -> bytes:
        return self._exec(["cat", path])

    def write(self, data: bytes, path: str = WAL_PATH) -> None:
        """Truncate and rewrite the file in place, then sync. Callers verify by reading it back."""
        self._exec(["sh", "-c", f'cat > "{path}" && sync'], stdin=data)

    def delete(self) -> dict:
        """Delete the helper once if it exists and is ours; return what was observed."""
        pod = self.current()
        if pod is None:
            self.created = False
            return {"existed": False}
        if not self._owned(pod):
            raise HelperError("helper_pod_not_owned")
        try:
            self.fixture.command(self.fixture.kubectl + ["-n", NAMESPACE, "delete", "pod", self.name,
                                                         "--grace-period=5", "--wait=false", "-o", "name"])
        except Exception as exc:
            raise HelperError("helper_delete_failed") from exc
        deadline = time.monotonic() + GONE_WAIT_SECONDS
        while self.current() is not None:
            if time.monotonic() >= deadline:
                raise HelperError("helper_not_gone")
            self.sleep(2)
        self.created = False
        return {"existed": True, "gone": True}


def set_unschedulable(fixture, node_name: str, uid: str, value: bool) -> None:
    fixture.patch_node(node_name, [{"op": "test", "path": "/metadata/uid", "value": uid},
                                   {"op": "add", "path": "/spec/unschedulable", "value": value}])
