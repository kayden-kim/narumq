"""Docker harness for the opt-in capacity fault acceptance tests (PRF-SCN-056, 057, 059).

Everything the acceptance asserts about processes, signals, cgroups and exit codes comes from real
Docker containers. Only the Kubernetes API is emulated: ``DockerKube`` answers the few ``kubectl``
calls that ``tests/benchmark/capacity/eks/controller.py`` makes, from ``docker inspect`` and
``docker exec``. The controller code under test is the production controller module, unchanged.

Requires a native linux/amd64 Docker engine and Go on PATH. Nothing here talks to a cluster or AWS.
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path
from types import ModuleType

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parents[1]
CONTROLLER_PATH = ROOT / "tests/benchmark/capacity/eks/controller.py"
PROCESS_SCRIPT_DIR = ROOT / "deploy/kubernetes/eks-ec2-ha-capacity"
PROCESS_SCRIPT = "/capacity-process/broker-process.sh"
BASE_IMAGE = "alpine:3.20"
RUN_LABEL = "narumq.acceptance.run"
POD_LABEL = "narumq.acceptance.pod"
UID_LABEL = "narumq.acceptance.uid"
WORKER = "docker-worker-1"
MANAGEMENT_TOKEN = "acceptance-token"


def load_controller() -> ModuleType:
    spec = importlib.util.spec_from_file_location("eks_capacity_controller_acceptance", CONTROLLER_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def docker(*args: str, timeout: float = 120, check: bool = True, stdin: str | None = None) -> subprocess.CompletedProcess:
    result = subprocess.run(["docker", *args], input=stdin, text=True, encoding="utf-8", capture_output=True, timeout=timeout)
    if check and result.returncode:
        raise RuntimeError(f"docker {args[0]} exit {result.returncode}: {result.stderr.strip()[-400:]}")
    return result


def require_native_amd64_linux_docker() -> None:
    engine = docker("version", "--format", "{{.Server.Os}}/{{.Server.Arch}}").stdout.strip()
    if engine != "linux/amd64":
        raise RuntimeError(f"acceptance needs a native linux/amd64 Docker engine, got {engine}")


class Workspace:
    """Unique run prefix, scratch directory and cleanup of everything labelled with the prefix."""

    def __init__(self) -> None:
        self.prefix = "narumq-prf-" + uuid.uuid4().hex[:8]
        self.directory = Path(tempfile.mkdtemp(prefix=self.prefix + "-"))
        self.image = f"{self.prefix}-broker:acceptance"

    def build_broker_image(self) -> str:
        """Build ./cmd/narumq for the engine platform on an alpine base (busybox sh and wget are required)."""
        require_native_amd64_linux_docker()
        context = self.directory / "image"
        context.mkdir()
        subprocess.run(["go", "build", "-trimpath", "-o", str(context / "narumq"), "./cmd/narumq"], cwd=ROOT,
                       env={**os.environ, "CGO_ENABLED": "0", "GOOS": "linux", "GOARCH": "amd64"}, check=True, timeout=900)
        (context / "Dockerfile").write_text(f"FROM {BASE_IMAGE}\nCOPY narumq /narumq\n", encoding="utf-8", newline="\n")
        docker("build", "-q", "-t", self.image, str(context), timeout=900)
        secrets = self.directory / "secrets"
        secrets.mkdir()
        (secrets / "token").write_text(MANAGEMENT_TOKEN, encoding="utf-8")
        return self.image

    def container_ids(self) -> list[str]:
        return docker("ps", "-a", "-q", "--filter", f"label={RUN_LABEL}={self.prefix}").stdout.split()

    def cleanup(self) -> None:
        ids = self.container_ids()
        if ids:
            docker("rm", "-f", *ids, check=False)
        docker("rmi", "-f", self.image, check=False)
        shutil.rmtree(self.directory, ignore_errors=True)

    def labels(self, pod: str, uid: str, app: str = "narumq") -> list[str]:
        return ["--label", f"app={app}", "--label", f"{RUN_LABEL}={self.prefix}", "--label", f"{POD_LABEL}={pod}",
                "--label", f"{UID_LABEL}={uid}"]

    def start_broker(self, pod: str, uid: str, cpus: str = "1", memory: str = "256m") -> str:
        """Standalone broker under the capacity process wrapper, as the EKS StatefulSet runs it."""
        name = f"{self.prefix}-{pod}"
        docker("run", "-d", "--name", name, *self.labels(pod, uid), "-e", f"CAPACITY_POD_UID={uid}",
               "--read-only", "--tmpfs", "/capacity-runtime:size=64k", "--tmpfs", "/tmp:size=16m",
               "--mount", f"type=bind,source={PROCESS_SCRIPT_DIR},destination=/capacity-process,readonly",
               "--mount", f"type=bind,source={self.directory / 'secrets'},destination=/secrets,readonly",
               "--cpus", cpus, "--memory", memory, self.image,
               "/bin/sh", PROCESS_SCRIPT, "run", "/narumq", "-tcp-listen", "127.0.0.1:1883",
               "-allow-insecure-anonymous-mqtt", "-http-listen", "127.0.0.1:8080",
               "-http-auth-token-file", "/secrets/token")
        return name

    def start_plain(self, pod: str, uid: str, command: str, *limits: str) -> str:
        """A limited alpine container that runs a shell command (CPU burner, memory hog)."""
        name = f"{self.prefix}-{pod}"
        docker("run", "-d", "--name", name, *self.labels(pod, uid), *limits, BASE_IMAGE, "sh", "-c", command)
        return name


def http_ready(container: str, timeout_seconds: int = 2) -> bool:
    """Management readiness from inside the container; false on timeout, refusal or a non-UP body."""
    result = docker("exec", container, "wget", "-q", "-T", str(timeout_seconds), "-O-", "http://127.0.0.1:8080/health/ready",
                    check=False, timeout=timeout_seconds + 30)
    if result.returncode:
        return False
    try:
        return json.loads(result.stdout)["data"]["status"] == "UP"
    except (ValueError, KeyError, TypeError):
        return False


def wait_http_ready(container: str, timeout: float = 60) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if http_ready(container):
            return
        time.sleep(0.5)
    raise AssertionError(f"{container} was not HTTP Ready within {timeout}s")


def broker_identity(container: str) -> dict:
    return json.loads(docker("exec", container, "/bin/sh", PROCESS_SCRIPT, "identity").stdout)


def process_script(container: str, mode: str, *arguments: str) -> subprocess.CompletedProcess:
    return docker("exec", container, "/bin/sh", PROCESS_SCRIPT, mode, *arguments, check=False)


def thread_states(container: str) -> list[str]:
    """State letter of every broker thread, read from /proc inside the container."""
    script = ('pid="$(cat /capacity-runtime/broker.pid)"; for f in /proc/"$pid"/task/*/status; do '
              'while read -r key state rest; do if [ "$key" = State: ]; then echo "$state"; fi; done < "$f"; done')
    return docker("exec", container, "/bin/sh", "-c", script).stdout.split()


def inspect_container(container: str) -> dict:
    return json.loads(docker("inspect", container).stdout)[0]


class RecordingChannel:
    """Stands in for the generator Pod control-marker channel; markers are kept in memory."""

    def __init__(self) -> None:
        self.markers: list[tuple[str, dict]] = []

    def write_json(self, name: str, payload: dict) -> None:
        self.markers.append((name, payload))

    def states(self) -> list[str]:
        return [payload["state"] for _, payload in self.markers if "state" in payload]


class DockerKube:
    """The subset of ``Kubectl`` the controller uses, answered from Docker.

    A "Pod" is one labelled container. Its UID is the label value, its container ID changes with every
    start of the container (as in Kubernetes) and Ready means the broker answered /health/ready.
    ``delete_mode`` is ``accept`` (the request succeeds and the Pod stays, as when deletion stalls) or
    ``error`` (the request fails).
    """

    def __init__(self, controller: ModuleType, workspace: Workspace, delete_mode: str = "accept") -> None:
        self.controller = controller
        self.workspace = workspace
        self.delete_mode = delete_mode
        self.cordon_log: list[tuple[str, str]] = []
        self.delete_log: list[str] = []
        self._starts: dict[str, tuple[str, int]] = {}

    # -- Kubectl surface -------------------------------------------------------------------
    def run(self, args: list[str], *, stdin: str | None = None, timeout: float = 120, sensitive: bool = False,
            cwd: str | None = None) -> str:
        verb = args[0]
        if verb in ("cordon", "uncordon"):
            self.cordon_log.append((verb, args[1]))
            return ""
        if verb == "delete" and args[1] == "pod":
            self.delete_log.append(args[2])
            if self.delete_mode == "error":
                raise self.controller.CommandFailed("kubectl delete exit 1: injected deletion error")
            return ""
        if verb == "exec":
            return self._exec(args, stdin, timeout)
        raise self.controller.CommandFailed(f"kubectl {verb} is not available in the Docker harness")

    def json(self, args: list[str], **kwargs):
        if args[0] != "get":
            raise self.controller.CommandFailed(f"kubectl {args[0]} is not available in the Docker harness")
        if args[1] == "pods":
            selector = args[args.index("-l") + 1] if "-l" in args else None
            items = [self._pod(item, probe=True) for item in self._inspect_all()]
            if selector:
                key, _, value = selector.partition("=")
                items = [item for item in items if item["metadata"]["labels"].get(key) == value]
            return {"items": items}
        if args[1] == "pod":
            for item in self._inspect_all():
                if item["Config"]["Labels"].get(POD_LABEL) == args[2]:
                    return self._pod(item, probe=False)
            if "--ignore-not-found" in args:
                return None
            raise self.controller.CommandFailed(f"pod {args[2]} not found")
        raise self.controller.CommandFailed(f"kubectl get {args[1]} is not available in the Docker harness")

    # -- Docker side ------------------------------------------------------------------------
    def container_of(self, pod: str) -> str:
        return f"{self.workspace.prefix}-{pod}"

    def _exec(self, args: list[str], stdin: str | None, timeout: float) -> str:
        separator = args.index("--")
        pod = args[args.index("-n") + 2]
        command = args[separator + 1:]
        flags = ["-i"] if "-i" in args[:separator] else []
        result = docker("exec", *flags, self.container_of(pod), *command, check=False, stdin=stdin, timeout=timeout)
        if result.returncode:
            raise self.controller.CommandFailed(f"kubectl exec exit {result.returncode}")
        return result.stdout

    def _inspect_all(self) -> list[dict]:
        ids = self.workspace.container_ids()
        return json.loads(docker("inspect", *ids).stdout) if ids else []

    def _restart_count(self, item: dict) -> int:
        started = item["State"].get("StartedAt", "")
        previous = self._starts.get(item["Name"])
        if previous is None:
            count = 0
        elif previous[0] == started:
            count = previous[1]
        else:
            count = previous[1] + 1
        self._starts[item["Name"]] = (started, count)
        return count

    def _pod(self, item: dict, probe: bool) -> dict:
        labels = item["Config"]["Labels"]
        state = item["State"]
        restarts = self._restart_count(item)
        if state["Running"]:
            container_state = {"running": {"startedAt": state["StartedAt"]}}
        else:
            reason = "OOMKilled" if state.get("OOMKilled") else ("Completed" if state["ExitCode"] == 0 else "Error")
            container_state = {"terminated": {"reason": reason, "exitCode": state["ExitCode"],
                                              "startedAt": state["StartedAt"], "finishedAt": state["FinishedAt"]}}
        broker = "broker-process.sh" in " ".join(item["Config"].get("Cmd") or [])
        # Only broker Pods are probed, and only by list calls; a plain limited container is Ready while running.
        ready = bool(state["Running"]) and (http_ready(item["Name"].lstrip("/"), 1) if broker and probe else not broker)
        return {
            "metadata": {"name": labels[POD_LABEL], "uid": labels[UID_LABEL], "labels": {"app": labels.get("app", "")}},
            "spec": {"nodeName": WORKER},
            "status": {
                "phase": "Running" if state["Running"] else "Failed",
                "conditions": [{"type": "Ready", "status": "True" if ready else "False"}],
                "containerStatuses": [{
                    "name": "narumq", "containerID": f"docker://{item['Id']}#{restarts}", "restartCount": restarts,
                    "ready": ready, "state": container_state, "lastState": {},
                }],
            },
        }



def run_controller(controller: ModuleType, kube: DockerKube, artifacts: Path, scenario, ready_timeout_ms: int = 60000):
    """RunController whose job start and main loop are replaced by the scenario; run() and every method
    the scenario calls are the production ones, including the finally cleanup that resumes the broker."""

    class Harness(controller.RunController):
        def start_job(self) -> None:
            self.pod = "capacity-generator"
            self.channel = RecordingChannel()

        def write_manifest(self) -> None:
            pass

        def write_pod_resources(self) -> None:
            pass

        def loop(self) -> None:
            scenario(self)

    args = argparse.Namespace(artifacts=str(artifacts), sample_seconds=3600, job_timeout=600, poll=1, ack_timeout=30)
    profile = {"readyTimeoutMillis": ready_timeout_ms, "resyncTimeoutMillis": ready_timeout_ms}
    return Harness(args, kube, profile)


def read_jsonl(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
