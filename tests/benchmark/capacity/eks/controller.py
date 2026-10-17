"""BP-02 EKS capacity controller (docs/specs/performance-and-verification.md §3.9).

Runs on the operator workstation. It only talks to the cluster through `kubectl` and exchanges
control markers with the in-cluster `capacity run` Job through the Job's `toolbox` sidecar.
It never contacts AWS and never prints Secret values, tokens or stderr of Secret commands.

Subcommands: prepare (fixture Secrets), deploy (overlay), run (Job + failure injection + collection),
cleanup. Every subcommand needs an explicit --context so a wrong default cluster is never used.
"""
from __future__ import annotations

import argparse
import base64
import datetime
import json
import os
import queue
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import urllib.request
from pathlib import Path
from string import Template

NAMESPACE = "narumq"
BROKER_LABEL = "app=narumq"
BROKER_SERVICE_ACCOUNT_SECRET = "narumq-cluster-secrets"
MANAGEMENT_PORT = 8080
RUNNER_CONTAINER = "capacity-runner"
TOOLBOX_CONTAINER = "toolbox"
CONTROL_DIR = "/control"
OUTPUT_DIR = "/output"
BROKER_PROCESS_SCRIPT = "/capacity-process/broker-process.sh"
BROKER_REPLICAS = 3
GENERATOR_LABEL = "narumq.io/generator=true"
REPO_ROOT = Path(__file__).resolve().parents[4]
OVERLAY_DIR = REPO_ROOT / "deploy/kubernetes/eks-ec2-ha-capacity"
JOB_TEMPLATE = OVERLAY_DIR / "job-capacity-runner.template.yaml"
DEFAULT_CAPACITY_COMMAND = ["go", "run", "./tests/benchmark/capacity"]

SAFE_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
MARKER_NAME = re.compile(r"^(step-\d+-r\d+)-(ready|kill|kill-ack|n1-done|complete)\.json$")
DIGEST_IMAGE = re.compile(r"^[^\s@]+@sha256:[0-9a-f]{64}$")
RUN_ID = re.compile(r"^[a-z0-9]([a-z0-9-]{0,30}[a-z0-9])?$")
FORWARD_LINE = re.compile(r"Forwarding from 127\.0\.0\.1:(\d+) -> %d" % MANAGEMENT_PORT)
JOB_PLACEHOLDER = re.compile(r"\$\{([A-Z_]+)\}")
JOB_VALUES = ("RUN_ID", "CAPACITY_RUNNER_IMAGE", "TOOLBOX_IMAGE", "MQTT_ENDPOINT", "MQTT_TLS_ENDPOINT",
              "TLS_SERVER_NAME", "GENERATOR_CPU", "GENERATOR_MEMORY", "ACTIVE_DEADLINE_SECONDS")


class ControllerError(RuntimeError):
    pass


class CommandFailed(ControllerError):
    pass


def timestamp(now: datetime.datetime | None = None) -> str:
    now = now or datetime.datetime.now(datetime.timezone.utc)
    return now.astimezone(datetime.timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


# ---------------------------------------------------------------------------------------------
# Pure logic: marker names, step state machine, resync rule, parsing, rendering.
# ---------------------------------------------------------------------------------------------

def parse_control_listing(listing: str) -> tuple[dict[str, set[str]], set[str]]:
    """Split `ls -1 /control` output into per-step marker kinds and the run-level files."""
    steps: dict[str, set[str]] = {}
    run_level: set[str] = set()
    for line in listing.splitlines():
        name = line.strip()
        match = MARKER_NAME.match(name)
        if match:
            steps.setdefault(match.group(1), set()).add(match.group(2))
        elif name in ("run-complete.json", "collected.json", "finish.json"):
            run_level.add(name)
    return steps, run_level


def broker_pod_name(node_id: int) -> str:
    """StatefulSet ordinal n runs Raft node n+1 (deploy/kubernetes/eks-ec2-ha/statefulset.yaml)."""
    if node_id < 1 or node_id > BROKER_REPLICAS:
        raise ControllerError(f"leader node id {node_id} is outside 1..{BROKER_REPLICAS}")
    return f"narumq-{node_id - 1}"


def node_number(value) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def kill_marker(state: str, node_id: int, pod: str, worker: str, at: str, **extra) -> dict:
    if state not in ("killing", "down", "restoring", "restored", "failed"):
        raise ValueError(f"unknown kill marker state {state!r}")
    marker = {"state": state, "target": {"nodeId": str(node_id), "pod": pod, "worker": worker}, "at": at}
    marker.update(extra)
    return marker


class StepMachine:
    """Controller side of one step-run. The runner decides what runs next; this only executes.

    States: new -> awaiting-ack -> down -> restoring -> restored | failed -> complete.
    `next_action` advances the state when it returns an action; the caller reports the outcome of
    a restore with `finish_restore`.
    """

    def __init__(self, prefix: str, ack_timeout: float):
        self.prefix = prefix
        self.ack_timeout = ack_timeout
        self.state = "new"
        self.requested_at = 0.0
        self.failure = ""

    def next_action(self, kinds: set[str], now: float) -> str | None:
        if self.state == "new" and "ready" in kinds:
            self.state = "awaiting-ack"
            self.requested_at = now
            return "request-kill"
        if self.state == "awaiting-ack":
            if "kill-ack" in kinds:
                self.state = "down"
                return "kill"
            if now - self.requested_at > self.ack_timeout:
                self.state = "failed"
                self.failure = "kill acknowledgement timeout"
                return "ack-timeout"
        if self.state == "down" and "n1-done" in kinds:
            self.state = "restoring"
            return "restore"
        if self.state in ("restored", "failed") and "complete" in kinds:
            self.state = "complete"
            return "complete"
        return None

    def finish_restore(self, ok: bool, reason: str = "") -> None:
        if self.state != "restoring":
            raise ValueError("restore result without a restore in progress")
        self.state = "restored" if ok else "failed"
        self.failure = "" if ok else reason

    def fail(self, reason: str) -> None:
        self.state = "failed"
        self.failure = reason


def find_leader(rows: list[dict]) -> dict | None:
    for row in rows:
        if row.get("role") == "LEADER":
            return row
    return None


def evaluate_resync(rows: list[dict], target_id: int, watermark: int | None) -> dict:
    """§3.6 NaruMQ resync rule on the leader's own node page.

    The watermark is the leader's lastAppliedIndex at the first observation. The restored node is
    resynced once its index reaches it and its replication health is HEALTHY (the leader itself
    needs no health). Returns {"reached", "watermark", "leaderNodeId", "targetIndex"}.
    """
    leader = find_leader(rows)
    result = {"reached": False, "watermark": watermark, "leaderNodeId": None, "targetIndex": None}
    if leader is None:
        return result
    leader_id = node_number(leader.get("nodeId"))
    result["leaderNodeId"] = leader_id
    if watermark is None:
        watermark = int(leader.get("lastAppliedIndex", 0))
        result["watermark"] = watermark
    target = next((row for row in rows if node_number(row.get("nodeId")) == target_id), None)
    if target is None:
        return result
    index = int(target.get("lastAppliedIndex", 0))
    result["targetIndex"] = index
    result["reached"] = index >= watermark and (target_id == leader_id or target.get("replicationHealth") == "HEALTHY")
    return result


def pod_ready(pod: dict) -> bool:
    if pod.get("metadata", {}).get("deletionTimestamp"):
        return False
    return any(item.get("type") == "Ready" and item.get("status") == "True"
               for item in pod.get("status", {}).get("conditions", []))


def ready_broker_pods(pods: dict) -> dict[str, str]:
    """Ready, non-terminating broker Pods as {name: uid}."""
    result = {}
    for pod in pods.get("items", []):
        metadata = pod.get("metadata", {})
        if metadata.get("labels", {}).get("app") == "narumq" and pod_ready(pod):
            result[metadata["name"]] = metadata["uid"]
    return result


def restore_complete(ready: dict[str, str], killed_pod: str, killed_uid: str) -> bool:
    """Three ready brokers and the killed Pod name now has a replacement UID."""
    return len(ready) >= BROKER_REPLICAS and ready.get(killed_pod) not in (None, killed_uid)


_QUANTITY = {"Ki": 1024, "Mi": 1024 ** 2, "Gi": 1024 ** 3, "Ti": 1024 ** 4,
             "K": 1000, "k": 1000, "M": 1000 ** 2, "G": 1000 ** 3, "T": 1000 ** 4}


def parse_cpu_millicores(value) -> int:
    """Kubernetes CPU quantity ("500m", "1", "1.5", "250000000n") as integer millicores."""
    text = str(value).strip()
    if text.endswith("m"):
        return int(float(text[:-1]))
    if text.endswith("n"):
        return int(float(text[:-1])) // 1_000_000
    return int(round(float(text) * 1000))


def parse_memory_bytes(value) -> int:
    """Kubernetes memory quantity ("64Mi", "4Gi", "128974848", "1G") as integer bytes."""
    text = str(value).strip()
    for suffix in sorted(_QUANTITY, key=len, reverse=True):
        if text.endswith(suffix):
            return int(float(text[:-len(suffix)]) * _QUANTITY[suffix])
    return int(float(text))


def parse_top(output: str) -> list[dict]:
    """Parse `kubectl top pod --no-headers` into CPU millicores and memory bytes."""
    pods = []
    for line in output.splitlines():
        fields = line.split()
        if len(fields) < 3:
            continue
        pods.append({"name": fields[0], "cpuMillicores": parse_cpu_millicores(fields[1]),
                     "memoryBytes": parse_memory_bytes(fields[2])})
    return pods


def _container_resources(container: dict, kind: str, restartable: bool = False) -> dict:
    resources = container.get("resources", {})
    record = {"name": container["name"], "kind": kind, "restartable": restartable}
    for section in ("requests", "limits"):
        values = resources.get(section, {})
        record[section] = {
            "cpuMillicores": parse_cpu_millicores(values["cpu"]) if "cpu" in values else None,
            "memoryBytes": parse_memory_bytes(values["memory"]) if "memory" in values else None,
        }
    return record


def _sum_resources(records: list[dict], section: str) -> dict:
    total = {}
    for key in ("cpuMillicores", "memoryBytes"):
        values = [record[section][key] for record in records]
        total[key] = None if any(value is None for value in values) else sum(values)
    return total


def pod_resource_summary(pod: dict) -> dict:
    """Per-container requests/limits and Pod totals from a `kubectl get pod -o json` object.

    `sumAllContainers` adds every container and init container (the additive test-overlay cost).
    `steadyState` adds only containers plus restartable (native sidecar) init containers, i.e.
    what keeps running after startup. A value is None when any summed container omits it.
    """
    spec = pod.get("spec", {})
    records = [_container_resources(c, "container") for c in spec.get("containers", [])]
    records += [_container_resources(c, "initContainer", c.get("restartPolicy") == "Always")
                for c in spec.get("initContainers", [])]
    steady = [r for r in records if r["kind"] == "container" or r["restartable"]]
    return {
        "name": pod.get("metadata", {}).get("name"),
        "containers": records,
        "sumAllContainers": {s: _sum_resources(records, s) for s in ("requests", "limits")},
        "steadyState": {s: _sum_resources(steady, s) for s in ("requests", "limits")},
    }


def parse_cpu_stat(text: str) -> dict:
    """Normalize cgroup v1/v2 counters to periods and microseconds; missing is not zero."""
    version, *lines = text.strip().splitlines()
    if version not in ("v1", "v2"):
        raise ValueError("unsupported cgroup CPU accounting")
    values = dict(line.split() for line in lines if line.strip())
    time_key = "throttled_usec" if version == "v2" else "throttled_time"
    counters = {key: int(values[source]) for key, source in (
        ("nr_periods", "nr_periods"), ("nr_throttled", "nr_throttled"), ("throttled_usec", time_key))}
    if any(value < 0 for value in counters.values()):
        raise ValueError("negative CPU counter")
    if version == "v1":
        counters["throttled_usec"] //= 1000
    return {"cgroupVersion": version, **counters}


def pod_runtime_summary(pod: dict) -> dict:
    """Keep Kubernetes restart/termination evidence, including init and native sidecar status."""
    metadata, status = pod.get("metadata", {}), pod.get("status", {})
    containers = []
    for key, kind in (("containerStatuses", "container"), ("initContainerStatuses", "initContainer")):
        for item in status.get(key, []):
            containers.append({
                "name": item["name"], "kind": kind, "containerID": item.get("containerID"),
                "restartCount": item.get("restartCount"), "ready": item.get("ready"),
                "state": item.get("state", {}), "lastState": item.get("lastState", {}),
            })
    return {"name": metadata.get("name"), "uid": metadata.get("uid"),
            "worker": pod.get("spec", {}).get("nodeName"), "phase": status.get("phase"),
            "reason": status.get("reason"), "deletionTimestamp": metadata.get("deletionTimestamp"),
            "containers": containers}


# Read only the container's own cgroup. No host mounts, privileges or ephemeral containers.
CPU_STAT_COMMAND = [
    "sh", "-c",
    "if [ -f /sys/fs/cgroup/cgroup.controllers ]; then "
    "echo v2; cat /sys/fs/cgroup/cpu.stat; "
    "elif [ -f /sys/fs/cgroup/cpu/cpu.stat ]; then "
    "echo v1; cat /sys/fs/cgroup/cpu/cpu.stat; "
    "else echo v1; cat /sys/fs/cgroup/cpu,cpuacct/cpu.stat; fi",
]


def split_image_digest(reference: str) -> tuple[str, str]:
    """Return (repository, sha256:digest); a mutable tag is rejected."""
    if not DIGEST_IMAGE.match(reference):
        raise ControllerError("image must be <repository>@sha256:<64 hex>; tags are not accepted")
    repository, digest = reference.split("@", 1)
    return repository, digest


def render_kustomization(overlay: str, runner_image: str) -> str:
    repository, digest = split_image_digest(runner_image)
    return ("apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nresources:\n"
            f"  - {json.dumps(overlay)}\nimages:\n  - name: capacity-runner\n"
            f"    newName: {json.dumps(repository)}\n    digest: {digest}\n")


def render_job(template: str, values: dict[str, str]) -> str:
    required = set(JOB_PLACEHOLDER.findall(template))
    missing = required - set(values)
    if missing:
        raise ControllerError(f"Job template values missing: {sorted(missing)}")
    if not RUN_ID.match(values["RUN_ID"]):
        raise ControllerError("run id must be a lowercase DNS label of at most 32 characters")
    for key in ("CAPACITY_RUNNER_IMAGE", "TOOLBOX_IMAGE"):
        split_image_digest(values[key])
    for key, value in values.items():
        if re.search(r"[\s\"'\\]", str(value)):
            raise ControllerError(f"Job template value {key} contains an unsafe character")
    return Template(template).substitute(values)


def profile_timeouts(profile: dict) -> tuple[float, float]:
    """readyTimeoutMillis and resyncTimeoutMillis in seconds; both are required profile fields."""
    try:
        ready, resync = float(profile["readyTimeoutMillis"]) / 1000, float(profile["resyncTimeoutMillis"]) / 1000
    except (KeyError, TypeError, ValueError) as exc:
        raise ControllerError("profile needs numeric readyTimeoutMillis and resyncTimeoutMillis") from exc
    if ready <= 0 or resync <= 0:
        raise ControllerError("profile timeouts must be positive")
    return ready, resync


def kubectl_global_args(kubectl: str, context: str, kubeconfig: str | None) -> list[str]:
    if not context:
        raise ControllerError("--context is required")
    args = [kubectl]
    if kubeconfig:
        args += ["--kubeconfig", kubeconfig]
    return args + ["--context", context]


def cordon_args(worker: str, cordon: bool) -> list[str]:
    return ["cordon" if cordon else "uncordon", worker]


def force_delete_args(pod: str) -> list[str]:
    return ["delete", "pod", pod, "-n", NAMESPACE, "--grace-period=0", "--force", "--wait=false"]


def exec_args(pod: str, container: str, *command: str, stdin: bool = False) -> list[str]:
    return ["exec"] + (["-i"] if stdin else []) + ["-n", NAMESPACE, pod, "-c", container, "--", *command]


def target_matches_pod(target: dict, pod: dict | None) -> bool:
    """Reject signals across Pod replacement, worker movement or container restart."""
    if not pod or pod.get("metadata", {}).get("uid") != target.get("uid"):
        return False
    if pod.get("spec", {}).get("nodeName") != target.get("worker") or not target.get("containerID"):
        return False
    return any(c.get("name") == "narumq" and c.get("containerID") == target["containerID"]
               and "running" in c.get("state", {}) for c in pod.get("status", {}).get("containerStatuses", []))


def atomic_write_command(name: str) -> list[str]:
    """Shell command run in the toolbox: temp file in the same directory, then rename."""
    if not SAFE_NAME.match(name):
        raise ControllerError("unsafe control file name")
    return ["sh", "-c", f"cat > {CONTROL_DIR}/.{name}.tmp && mv {CONTROL_DIR}/.{name}.tmp {CONTROL_DIR}/{name}"]


# ---------------------------------------------------------------------------------------------
# kubectl access.
# ---------------------------------------------------------------------------------------------

class Kubectl:
    def __init__(self, base: list[str]):
        self.base = base

    def run(self, args: list[str], *, stdin: str | None = None, timeout: float = 120, sensitive: bool = False,
            cwd: str | None = None) -> str:
        result = subprocess.run(self.base + args, input=stdin, text=True, encoding="utf-8", capture_output=True,
                                timeout=timeout, cwd=cwd)
        if result.returncode:
            detail = "" if sensitive else ": " + result.stderr.strip()[:300]
            raise CommandFailed(f"kubectl {args[0]} exit {result.returncode}{detail}")
        return result.stdout

    def json(self, args: list[str], **kwargs):
        text = self.run(args + ["-o", "json"], **kwargs).strip()
        return json.loads(text) if text else None

    def popen(self, args: list[str]) -> subprocess.Popen:
        return subprocess.Popen(self.base + args, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True,
                                encoding="utf-8")


class PodChannel:
    """Control-marker channel through the toolbox sidecar of the generator Pod."""

    def __init__(self, kube: Kubectl, pod: str):
        self.kube = kube
        self.pod = pod

    def listing(self) -> str:
        return self.kube.run(exec_args(self.pod, TOOLBOX_CONTAINER, "ls", "-1", CONTROL_DIR), timeout=30)

    def read_json(self, name: str) -> dict:
        if not SAFE_NAME.match(name):
            raise ControllerError("unsafe control file name")
        return json.loads(self.kube.run(exec_args(self.pod, TOOLBOX_CONTAINER, "cat", f"{CONTROL_DIR}/{name}"), timeout=30))

    def write_json(self, name: str, payload: dict) -> None:
        self.kube.run(exec_args(self.pod, TOOLBOX_CONTAINER, *atomic_write_command(name), stdin=True),
                      stdin=json.dumps(payload, separators=(",", ":")), timeout=30)


class ManagementTunnel:
    """One owned `kubectl port-forward` to a single broker Pod; closed on drop or exit."""

    def __init__(self, kube: Kubectl, pod: str, wait: float = 20):
        self.pod = pod
        self.process = kube.popen(["port-forward", "-n", NAMESPACE, f"pod/{pod}", f":{MANAGEMENT_PORT}"])
        self.port = 0
        found = threading.Event()

        def pump():
            for line in self.process.stdout:
                match = FORWARD_LINE.search(line)
                if match and not self.port:
                    self.port = int(match.group(1))
                    found.set()

        threading.Thread(target=pump, daemon=True).start()
        if not found.wait(wait):
            self.close()
            raise CommandFailed(f"port-forward to {pod} did not become ready")

    def close(self) -> None:
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
        if self.process.stdout:
            self.process.stdout.close()


class Management:
    """Management API reads through owned port-forwards; the bearer token stays in memory."""

    def __init__(self, kube: Kubectl, token: str):
        self.kube = kube
        self.token = token
        self.tunnels: dict[str, ManagementTunnel] = {}

    def nodes(self, pod: str) -> list[dict]:
        tunnel = self.tunnels.get(pod) or self.tunnels.setdefault(pod, ManagementTunnel(self.kube, pod))
        request = urllib.request.Request(f"http://127.0.0.1:{tunnel.port}/api/v1/nodes",
                                         headers={"Authorization": "Bearer " + self.token})
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                page = json.load(response)
        except Exception:
            self.drop(pod)
            raise
        if page.get("page", {}).get("hasMore"):
            raise ControllerError("node page has more rows than one page")
        return page["data"]

    def drop(self, pod: str) -> None:
        tunnel = self.tunnels.pop(pod, None)
        if tunnel:
            tunnel.close()

    def close(self) -> None:
        for pod in list(self.tunnels):
            self.drop(pod)


class Journal:
    """Append-only JSONL under the local artifact directory; thread safe."""

    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self.lock = threading.Lock()

    def write(self, record: dict) -> None:
        with self.lock, self.path.open("a", encoding="utf-8", newline="\n") as handle:
            handle.write(json.dumps(record, separators=(",", ":")) + "\n")


class RuntimeDiagnostics:
    """Serialized snapshots; deltas never cross a Pod/container restart or counter reset."""

    def __init__(self, kube: Kubectl, journal: Journal, generator: str):
        self.kube = kube
        self.journal = journal
        self.generator = generator
        self.previous: dict[str, tuple[tuple, dict]] = {}
        self.lock = threading.Lock()

    def sample(self, boundary: str, step: str | None = None, requested_at: str | None = None) -> None:
        with self.lock:
            record = {"at": timestamp(), "requestedAt": requested_at, "boundary": boundary,
                      "step": step, "available": False}
            try:
                pods = (self.kube.json(["get", "pods", "-n", NAMESPACE], timeout=5) or {})["items"]
                selected = [p for p in pods if p["metadata"].get("labels", {}).get("app") == "narumq"
                            or p["metadata"]["name"] == self.generator]
                record["pods"] = [pod_runtime_summary(p) for p in selected]
                samples = []
                for pod in record["pods"]:
                    if pod["name"] == self.generator:
                        continue
                    container = next((c for c in pod["containers"] if c["name"] == "narumq"), {})
                    cpu = {"pod": pod["name"], "uid": pod["uid"],
                           "containerID": container.get("containerID"), "available": False, "delta": None}
                    if not container.get("containerID") or "running" not in container.get("state", {}):
                        cpu["reason"] = "broker container not running or identity unavailable"
                    else:
                        try:
                            text = self.kube.run(exec_args(pod["name"], "narumq", *CPU_STAT_COMMAND), timeout=5)
                            cpu["counters"] = parse_cpu_stat(text)
                            cpu["at"] = timestamp()
                            cpu["available"] = True
                        except (ControllerError, subprocess.TimeoutExpired, OSError, ValueError, KeyError) as exc:
                            cpu["reason"] = type(exc).__name__
                    samples.append((cpu, container.get("restartCount")))
                # Exec targets a name, not a UID: reject a sample read across replacement/restart.
                after = (self.kube.json(["get", "pods", "-n", NAMESPACE], timeout=5) or {})["items"]
                identities = {}
                for pod in after:
                    for container in pod.get("status", {}).get("containerStatuses", []):
                        if container["name"] == "narumq":
                            identities[pod["metadata"]["name"]] = (
                                pod["metadata"].get("uid"), container.get("containerID"), container.get("restartCount"))
                record["cpu"] = []
                for cpu, restarts in samples:
                    identity = (cpu["uid"], cpu["containerID"], restarts)
                    if cpu["available"] and identities.get(cpu["pod"]) != identity:
                        cpu.update(available=False, reason="container identity changed during collection")
                        cpu.pop("counters", None)
                    previous = self.previous.pop(cpu["pod"], None)
                    if cpu["available"]:
                        current = cpu["counters"]
                        keys = ("nr_periods", "nr_throttled", "throttled_usec")
                        if (previous and previous[0] == identity
                                and previous[1]["cgroupVersion"] == current["cgroupVersion"]
                                and all(current[k] >= previous[1][k] for k in keys)):
                            cpu["delta"] = {k: current[k] - previous[1][k] for k in keys}
                        self.previous[cpu["pod"]] = (identity, current)
                    record["cpu"].append(cpu)
                record["available"] = True
            except (ControllerError, subprocess.TimeoutExpired, OSError, ValueError, KeyError) as exc:
                record["reason"] = type(exc).__name__
                self.previous.clear()
            self.journal.write(record)


class ResourceSampler(threading.Thread):
    """Pod CPU/memory plus cgroup and restart evidence; unavailable samples stay explicit."""

    def __init__(self, kube: Kubectl, journal: Journal, interval: float, diagnostics: RuntimeDiagnostics):
        super().__init__(daemon=True)
        self.kube = kube
        self.journal = journal
        self.interval = interval
        self.requests = queue.Queue()
        self.diagnostics = diagnostics

    def request(self, boundary: str, step: str | None = None) -> None:
        self.requests.put((boundary, step, timestamp()))

    def run(self) -> None:
        while True:
            try:
                request = self.requests.get(timeout=self.interval)
                if request is None:
                    return
            except queue.Empty:
                request = ("periodic", None, None)
            record = {"at": timestamp()}
            try:
                record["available"] = True
                record["pods"] = parse_top(self.kube.run(["top", "pod", "-n", NAMESPACE, "--no-headers"], timeout=30))
            except Exception as exc:
                record = {"at": record["at"], "available": False, "reason": type(exc).__name__}
            self.journal.write(record)
            self.diagnostics.sample(*request)

    def stop(self) -> None:
        if self.ident is not None:
            self.requests.put(None)
            self.join()


# ---------------------------------------------------------------------------------------------
# Subcommands.
# ---------------------------------------------------------------------------------------------

def apply_generated(kube: Kubectl, create_args: list[str]) -> None:
    """Server-side apply avoids duplicating large credentials in the last-applied annotation."""
    manifest = kube.run(create_args + ["--dry-run=client", "-o", "yaml"], sensitive=True)
    kube.run(["apply", "--server-side", "--field-manager=narumq-capacity-fixture", "-f", "-"],
             stdin=manifest, sensitive=True)


def prepare(args, kube: Kubectl) -> int:
    profile = Path(args.profile).resolve()
    json.loads(profile.read_text(encoding="utf-8"))
    kube.run(["get", "namespace", NAMESPACE], timeout=30)
    with tempfile.TemporaryDirectory(prefix="narumq-capacity-fixture-") as directory:
        fixture = Path(directory)
        command = shlex.split(args.capacity_command) + ["fixture", "--profile", str(profile), "--out", str(fixture),
                                                        "--tls-server-name", args.server_name]
        result = subprocess.run(command, cwd=REPO_ROOT, capture_output=True, text=True, encoding="utf-8")
        if result.returncode:
            raise CommandFailed(f"capacity fixture exit {result.returncode}")
        for required in ("jwks.json", "credentials.json", "ca.pem", "server.crt", "server.key"):
            if not (fixture / required).is_file():
                raise ControllerError(f"capacity fixture did not write {required}")
        apply_generated(kube, ["create", "configmap", "narumq-capacity-profile", "-n", NAMESPACE,
                               f"--from-file=profile.json={profile}"])
        apply_generated(kube, ["create", "secret", "generic", "narumq-capacity-jwks", "-n", NAMESPACE,
                               f"--from-file=jwks.json={fixture / 'jwks.json'}"])
        apply_generated(kube, ["create", "secret", "generic", "narumq-capacity-mqtt-tls", "-n", NAMESPACE,
                               f"--from-file=tls.crt={fixture / 'server.crt'}", f"--from-file=tls.key={fixture / 'server.key'}"])
        apply_generated(kube, ["create", "secret", "generic", "narumq-capacity-fixture", "-n", NAMESPACE,
                               f"--from-file=ca.pem={fixture / 'ca.pem'}",
                               f"--from-file=credentials.json={fixture / 'credentials.json'}"])
    print("fixture objects applied: narumq-capacity-profile, -jwks, -mqtt-tls, -fixture")
    return 0


def deploy(args, kube: Kubectl) -> int:
    # kustomize rejects absolute base paths and a base on another Windows drive, so the temporary
    # root lives under the (git-ignored) repo .artifacts directory and references the overlay relatively.
    scratch = REPO_ROOT / ".artifacts"
    scratch.mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="narumq-capacity-overlay-", dir=scratch) as directory:
        root = Path(directory)
        relative = Path(os.path.relpath(OVERLAY_DIR, root)).as_posix()
        (root / "kustomization.yaml").write_text(
            render_kustomization(relative, args.capacity_image), encoding="utf-8", newline="\n")
        kube.run(["apply", "-k", str(root)], timeout=300)
    print("overlay applied; StatefulSet updateStrategy is OnDelete, so existing broker Pods need --roll")
    if args.roll:
        roll_brokers(kube, args.ready_timeout)
    return 0


def roll_brokers(kube: Kubectl, timeout: float) -> None:
    """Delete broker Pods one at a time so each picks up the OnDelete template change."""
    for ordinal in range(BROKER_REPLICAS):
        pod = f"narumq-{ordinal}"
        old = ready_broker_pods(kube.json(["get", "pods", "-n", NAMESPACE, "-l", BROKER_LABEL]) or {}).get(pod)
        kube.run(["delete", "pod", pod, "-n", NAMESPACE, "--wait=true"], timeout=timeout)
        wait_until(lambda: restore_complete(
            ready_broker_pods(kube.json(["get", "pods", "-n", NAMESPACE, "-l", BROKER_LABEL]) or {}), pod, old or ""),
            timeout, f"broker pod {pod} ready after roll")


def wait_until(predicate, timeout: float, what: str, interval: float = 2.0, clock=time.monotonic, sleep=time.sleep):
    deadline = clock() + timeout
    while True:
        value = predicate()
        if value:
            return value
        if clock() >= deadline:
            raise ControllerError(f"timeout waiting for {what}")
        sleep(interval)


class RunController:
    def __init__(self, args, kube: Kubectl, profile: dict):
        self.args = args
        self.kube = kube
        self.ready_timeout, self.resync_timeout = profile_timeouts(profile)
        self.artifacts = Path(args.artifacts)
        self.events = Journal(self.artifacts / "controller" / "events.jsonl")
        self.resources = Journal(self.artifacts / "controller" / "broker-resource.jsonl")
        self.pod = ""
        self.channel: PodChannel | None = None
        self.management: Management | None = None
        self.machines: dict[str, StepMachine] = {}
        self.targets: dict[str, dict] = {}
        self.cordoned: set[str] = set()
        self.sampler: ResourceSampler | None = None

    def event(self, name: str, **fields) -> None:
        self.events.write({"at": timestamp(), "event": name, **fields})
        print(f"{timestamp()} {name} " + " ".join(f"{k}={v}" for k, v in fields.items() if k in ("step", "pod", "worker")),
              flush=True)

    def admin_token(self) -> str:
        encoded = self.kube.run(["get", "secret", BROKER_SERVICE_ACCOUNT_SECRET, "-n", NAMESPACE, "-o",
                                 "jsonpath={.data.admin\\.token}"], sensitive=True)
        return base64.b64decode(encoded).decode("utf-8").strip()

    def start_job(self) -> None:
        args = self.args
        statefulset = self.kube.json(["get", "statefulset", "narumq", "-n", NAMESPACE])
        broker = next(c for c in statefulset["spec"]["template"]["spec"]["containers"] if c["name"] == "narumq")
        nlb = wait_until(lambda: self.kube.run(
            ["get", "service", "narumq-nlb", "-n", NAMESPACE, "-o",
             "jsonpath={.status.loadBalancer.ingress[0].hostname}"]).strip(), 600, "internal NLB hostname", interval=10)
        values = {
            "RUN_ID": args.run_id, "CAPACITY_RUNNER_IMAGE": args.capacity_image, "TOOLBOX_IMAGE": broker["image"],
            "MQTT_ENDPOINT": f"{nlb}:1883", "MQTT_TLS_ENDPOINT": f"{nlb}:8883", "TLS_SERVER_NAME": args.server_name,
            "GENERATOR_CPU": str(args.generator_cpu), "GENERATOR_MEMORY": args.generator_memory,
            "ACTIVE_DEADLINE_SECONDS": str(args.job_timeout),
        }
        manifest = render_job(JOB_TEMPLATE.read_text(encoding="utf-8"), values)
        self.kube.run(["apply", "-f", "-"], stdin=manifest)
        self.event("job-applied", run=args.run_id)
        label = f"narumq.io/capacity-run={args.run_id}"

        def toolbox_running():
            pods = (self.kube.json(["get", "pods", "-n", NAMESPACE, "-l", label]) or {}).get("items", [])
            for pod in pods:
                if pod.get("status", {}).get("phase") == "Failed":
                    raise ControllerError("generator Pod failed before start")
                for status in pod.get("status", {}).get("initContainerStatuses", []):
                    if status["name"] == TOOLBOX_CONTAINER and status.get("state", {}).get("running"):
                        return pod["metadata"]["name"]
            return None

        self.pod = wait_until(toolbox_running, 900, "generator Pod toolbox", interval=5)
        self.channel = PodChannel(self.kube, self.pod)

    def write_pod_resources(self) -> None:
        """Record requests/limits of every broker and the generator container as test-overlay conditions."""
        pods = self.kube.json(["get", "pods", "-n", NAMESPACE]) or {"items": []}
        brokers = sorted((p for p in pods["items"] if p["metadata"].get("labels", {}).get("app") == "narumq"),
                         key=lambda p: p["metadata"]["name"])
        generator = [p for p in pods["items"] if p["metadata"]["name"] == self.pod]
        if len(brokers) != BROKER_REPLICAS or len(generator) != 1:
            raise ControllerError("expected three broker Pods and one generator Pod for the resource record")
        directory = self.artifacts / "controller"
        directory.mkdir(parents=True, exist_ok=True)
        (directory / "broker-pod-resources.json").write_text(json.dumps({
            "recordedAt": timestamp(),
            "note": "broker container resources are the profile; init and sidecar containers are additive test-overlay conditions",
            "brokers": [pod_resource_summary(p) for p in brokers],
            "generator": pod_resource_summary(generator[0]),
        }, indent=2), encoding="utf-8")

    def write_manifest(self) -> None:
        pods = self.kube.json(["get", "pods", "-n", NAMESPACE]) or {"items": []}
        nodes = {n["metadata"]["name"]: n for n in (self.kube.json(["get", "nodes"]) or {"items": []})["items"]}
        records = []
        for pod in pods["items"]:
            node = nodes.get(pod.get("spec", {}).get("nodeName"), {})
            records.append({
                "name": pod["metadata"]["name"], "node": pod.get("spec", {}).get("nodeName"),
                "instanceType": node.get("metadata", {}).get("labels", {}).get("node.kubernetes.io/instance-type"),
                "zone": node.get("metadata", {}).get("labels", {}).get("topology.kubernetes.io/zone"),
                "containers": [{"name": s["name"], "image": s.get("image"), "imageID": s.get("imageID")}
                               for s in pod.get("status", {}).get("containerStatuses", [])
                               + pod.get("status", {}).get("initContainerStatuses", [])],
            })
        client = json.loads(self.kube.run(["version", "--client", "-o", "json"]))
        try:
            security = self.kube.run(exec_args("narumq-0", "narumq", "sha256sum", "/capacity-security/security.json")).split()[0]
        except CommandFailed:
            security = None
        self.artifacts.joinpath("controller").mkdir(parents=True, exist_ok=True)
        (self.artifacts / "controller" / "controller-manifest.json").write_text(json.dumps({
            "runId": self.args.run_id, "startedAt": timestamp(), "context": self.args.context, "namespace": NAMESPACE,
            "kubectlClient": client.get("clientVersion", {}).get("gitVersion"),
            "capacityRunnerImage": self.args.capacity_image, "pods": records,
            "securityConfigSha256": security,
            "tenantQuotas": "unset in cluster config; cluster-wide maximum defaults apply (per-cell tenants)",
            "releaseGating": False,
        }, indent=2), encoding="utf-8")

    def ensure_management(self) -> Management:
        if self.management is None:
            self.management = Management(self.kube, self.admin_token())
        return self.management

    def handle_request_kill(self, prefix: str, ready: dict) -> None:
        self.sampler.request("step-ready", prefix)
        node_id = node_number(ready.get("leaderNodeId"))
        pod = broker_pod_name(node_id)
        info = self.kube.json(["get", "pod", pod, "-n", NAMESPACE])
        worker = info["spec"]["nodeName"]
        self.targets[prefix] = {"nodeId": node_id, "pod": pod, "worker": worker, "uid": info["metadata"]["uid"]}
        target = self.targets[prefix]
        target["containerID"] = next(
            (c.get("containerID") for c in info.get("status", {}).get("containerStatuses", [])
             if c["name"] == "narumq"), None)
        if not target_matches_pod(target, info):
            raise ControllerError("target broker container is not running")
        process = json.loads(self.kube.run(
            exec_args(pod, "narumq", "/bin/sh", BROKER_PROCESS_SCRIPT, "identity"), timeout=10))
        if process["podUID"] != target["uid"] or process["pid"] <= 1 or not process["instance"]:
            raise ControllerError("target broker process identity is not the prepared Pod")
        target["processInstance"] = process["instance"]
        target["brokerPID"] = process["pid"]
        at = timestamp()
        self.channel.write_json(f"{prefix}-kill.json", kill_marker("killing", node_id, pod, worker, at))
        self.targets[prefix]["killingWrittenAt"] = at
        self.event("kill-requested", step=prefix, pod=pod, worker=worker)

    def write_failed(self, prefix: str, error: str) -> None:
        """Tell the runner the failure injection or restore failed so it marks the step invalid."""
        target = self.targets.get(prefix, {})
        try:
            self.channel.write_json(f"{prefix}-kill.json", kill_marker(
                "failed", target.get("nodeId", 0), target.get("pod", ""), target.get("worker", ""), timestamp(),
                error=error))
        except (CommandFailed, subprocess.TimeoutExpired):
            self.event("failed-marker-unwritable", step=prefix)

    def resume_target(self, prefix: str) -> None:
        target = self.targets[prefix]
        if not target.get("frozen"):
            return
        info = self.kube.json(["get", "pod", target["pod"], "-n", NAMESPACE, "--ignore-not-found"])
        if target_matches_pod(target, info):
            self.kube.run(exec_args(target["pod"], "narumq", "/bin/sh", BROKER_PROCESS_SCRIPT,
                                    "resume", target["processInstance"]), timeout=10)
            self.event("broker-resumed-on-abort", step=prefix, pod=target["pod"])
        else:
            self.event("frozen-target-replaced", step=prefix, pod=target["pod"])
        target["frozen"] = False

    def handle_kill(self, prefix: str) -> None:
        target = self.targets[prefix]
        target["ackObservedAt"] = timestamp()
        info = self.kube.json(["get", "pod", target["pod"], "-n", NAMESPACE])
        if not target_matches_pod(target, info):
            raise ControllerError("target Pod or broker container changed before injection")
        self.cordoned.add(target["worker"])
        self.kube.run(cordon_args(target["worker"], True))
        target["cordonedAt"] = timestamp()
        try:
            # Mark before exec: a lost exec response must still trigger guarded SIGCONT.
            target["frozen"] = True
            stopped = json.loads(self.kube.run(exec_args(
                target["pod"], "narumq", "/bin/sh", BROKER_PROCESS_SCRIPT, "freeze",
                target["processInstance"]), timeout=10))
            if stopped["pid"] != target["brokerPID"] or stopped["stoppedThreads"] < 1:
                raise ControllerError("broker process stop was not confirmed")
            target["frozenAt"] = timestamp()
            target["stoppedThreads"] = stopped["stoppedThreads"]
            self.event("broker-frozen", step=prefix, pod=target["pod"],
                       **{k: target[k] for k in ("uid", "containerID", "processInstance", "brokerPID",
                                                "stoppedThreads", "frozenAt")})
            self.kube.run(force_delete_args(target["pod"]))
            target["deletedAt"] = timestamp()
            # --wait=false confirms acceptance, not disappearance of the original Pod.
            # Keep guarded SIGCONT available if deletion stalls and this run aborts.
        except Exception:
            try:
                self.resume_target(prefix)
            except (ControllerError, subprocess.TimeoutExpired):
                self.event("broker-resume-failed", step=prefix, pod=target["pod"])
            raise
        self.channel.write_json(f"{prefix}-kill.json", kill_marker(
            "down", target["nodeId"], target["pod"], target["worker"], timestamp(),
            frozenAt=target["frozenAt"], cordonedAt=target["cordonedAt"], deletedAt=target["deletedAt"]))
        self.event("leader-killed", step=prefix, pod=target["pod"], worker=target["worker"], **{
            k: target[k] for k in ("killingWrittenAt", "ackObservedAt", "frozenAt", "cordonedAt", "deletedAt")})

    def handle_restore(self, prefix: str) -> tuple[bool, str]:
        target = self.targets[prefix]
        started = time.monotonic()
        self.channel.write_json(f"{prefix}-kill.json", kill_marker(
            "restoring", target["nodeId"], target["pod"], target["worker"], timestamp()))
        self.kube.run(cordon_args(target["worker"], False))
        self.cordoned.discard(target["worker"])
        target["uncordonedAt"] = timestamp()
        self.event("worker-uncordoned", step=prefix, worker=target["worker"], uncordonedAt=target["uncordonedAt"])
        try:
            wait_until(lambda: restore_complete(
                ready_broker_pods(self.kube.json(["get", "pods", "-n", NAMESPACE, "-l", BROKER_LABEL]) or {}),
                target["pod"], target["uid"]), self.ready_timeout, "three ready broker Pods", interval=2)
            target["frozen"] = False  # A Ready replacement UID was observed.
            target["readyAt"] = timestamp()
            resync = self.wait_resync(target, self.resync_timeout)
        except ControllerError as exc:
            self.event("restore-failed", step=prefix, reason=str(exc))
            return False, str(exc)
        target["resyncedAt"] = timestamp()
        target["resync"] = resync
        target["restoreSeconds"] = round(time.monotonic() - started, 3)
        self.channel.write_json(f"{prefix}-kill.json", kill_marker(
            "restored", target["nodeId"], target["pod"], target["worker"], target["resyncedAt"],
            readyAt=target["readyAt"], resync=resync))
        self.event("restored", step=prefix, **{"restoreSeconds": target["restoreSeconds"]})
        return True, ""

    def wait_resync(self, target: dict, timeout: float) -> dict:
        management = self.ensure_management()
        watermark = None
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                ready = ready_broker_pods(self.kube.json(["get", "pods", "-n", NAMESPACE, "-l", BROKER_LABEL]) or {})
                survivors = sorted(name for name in ready if name != target["pod"])
                if survivors:
                    rows = management.nodes(survivors[0])
                    leader = find_leader(rows)
                    if leader is not None:
                        leader_pod = broker_pod_name(node_number(leader.get("nodeId")))
                        if leader_pod not in ready:
                            raise ControllerError("leader Pod is not Ready")
                        if leader_pod != survivors[0]:
                            rows = management.nodes(leader_pod)
                        outcome = evaluate_resync(rows, target["nodeId"], watermark)
                        watermark = outcome["watermark"]
                        if outcome["reached"]:
                            return outcome
            except (ControllerError, OSError, ValueError, KeyError):
                pass
            time.sleep(2)
        raise ControllerError("resync timeout")

    def collect(self) -> None:
        destination = self.artifacts / "generator-output"
        if destination.exists():
            shutil.rmtree(destination)
        self.artifacts.mkdir(parents=True, exist_ok=True)
        # Relative destination + cwd: kubectl cp misreads Windows drive-letter paths as Pod specs.
        self.kube.run(["cp", "-c", TOOLBOX_CONTAINER, f"{NAMESPACE}/{self.pod}:{OUTPUT_DIR}", "generator-output"],
                      timeout=1800, cwd=str(self.artifacts))
        if not (destination / "manifest.json").is_file():
            raise ControllerError("collected output has no manifest.json")
        count = sum(1 for path in destination.rglob("*") if path.is_file())
        self.channel.write_json("collected.json", {"at": timestamp(), "files": count})
        self.event("collected", files=count)

    def save_runner_log(self) -> None:
        try:
            log = self.kube.run(["logs", self.pod, "-n", NAMESPACE, "-c", RUNNER_CONTAINER], timeout=120)
            (self.artifacts / "controller" / "runner.log").write_text(log, encoding="utf-8")
        except CommandFailed:
            self.event("runner-log-unavailable")

    def loop(self) -> None:
        deadline = time.monotonic() + self.args.job_timeout
        polls = 0
        failures = 0
        while time.monotonic() < deadline:
            try:
                steps, run_level = parse_control_listing(self.channel.listing())
                failures = 0
            except (CommandFailed, subprocess.TimeoutExpired):
                failures += 1
                if failures >= 5:
                    raise ControllerError("control listing failed five times in a row")
                time.sleep(self.args.poll)
                continue
            for prefix in sorted(steps):
                machine = self.machines.setdefault(prefix, StepMachine(prefix, self.args.ack_timeout))
                action = machine.next_action(steps[prefix], time.monotonic())
                if action in ("request-kill", "kill"):
                    try:
                        if action == "request-kill":
                            self.handle_request_kill(prefix, self.channel.read_json(f"{prefix}-ready.json"))
                        else:
                            self.handle_kill(prefix)
                    except (ControllerError, subprocess.TimeoutExpired, json.JSONDecodeError, KeyError) as exc:
                        machine.fail(f"{action} failed: {type(exc).__name__}")
                        self.event("kill-step-failed", step=prefix, action=action)
                        self.write_failed(prefix, machine.failure)
                        for worker in sorted(self.cordoned):
                            try:
                                self.kube.run(cordon_args(worker, False))
                                self.cordoned.discard(worker)
                            except CommandFailed:
                                self.event("worker-uncordon-failed", worker=worker)
                elif action == "ack-timeout":
                    self.event("kill-ack-timeout", step=prefix)
                    self.write_failed(prefix, machine.failure)
                elif action == "restore":
                    ok, reason = self.handle_restore(prefix)
                    machine.finish_restore(ok, reason)
                    if not ok:
                        self.write_failed(prefix, reason)
                elif action == "complete":
                    self.sampler.request("step-complete", prefix)
                    self.event("step-complete", step=prefix)
            if "run-complete.json" in run_level and "collected.json" not in run_level:
                self.collect()
                return
            polls += 1
            if polls % 30 == 0:
                phase = self.kube.run(["get", "pod", self.pod, "-n", NAMESPACE, "-o", "jsonpath={.status.phase}"])
                if phase == "Failed":
                    raise ControllerError("generator Pod failed")
            time.sleep(self.args.poll)
        raise ControllerError("run deadline exceeded before run-complete.json")

    def run(self) -> int:
        diagnostics = RuntimeDiagnostics(self.kube, Journal(
            self.artifacts / "controller" / "runtime-health.jsonl"), "")
        sampler = self.sampler = ResourceSampler(
            self.kube, self.resources, self.args.sample_seconds, diagnostics)
        try:
            self.start_job()
            diagnostics.generator = self.pod
            self.write_manifest()
            self.write_pod_resources()
            sampler.start()
            sampler.request("run-start")
            self.loop()
            self.save_runner_log()
            return 0
        finally:
            if sampler.ident is not None:
                sampler.request("run-end")
            try:
                for prefix in self.targets:
                    try:
                        self.resume_target(prefix)
                    except (ControllerError, subprocess.TimeoutExpired):
                        self.event("broker-resume-failed", step=prefix)
                for worker in sorted(self.cordoned):
                    try:
                        self.kube.run(cordon_args(worker, False))
                        self.event("worker-uncordoned-on-abort", worker=worker)
                    except CommandFailed:
                        self.event("worker-uncordon-failed", worker=worker)
                if self.management:
                    self.management.close()
            finally:
                sampler.stop()


# Scenarios: performance-and-verification.md:PRF-SCN-063, performance-and-verification.md:PRF-SCN-064
def run_command(args, kube: Kubectl) -> int:
    profile = json.loads(Path(args.profile).read_text(encoding="utf-8"))
    if not args.run_id:
        args.run_id = "cap-" + datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dt%H%M%Sz").lower()
    if not args.artifacts:
        args.artifacts = str(REPO_ROOT / ".artifacts/benchmark/bp02-capacity" / args.run_id)
    return RunController(args, kube, profile).run()


def cleanup(args, kube: Kubectl) -> int:
    kube.run(["delete", "job", "-n", NAMESPACE, "-l", "app=narumq-capacity-runner", "--ignore-not-found"])
    for kind, name in (("secret", "narumq-capacity-fixture"), ("secret", "narumq-capacity-mqtt-tls"),
                       ("secret", "narumq-capacity-jwks"), ("configmap", "narumq-capacity-profile")):
        kube.run(["delete", kind, name, "-n", NAMESPACE, "--ignore-not-found"])
    print("capacity Job and fixture objects deleted")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--context", required=True, help="kubectl context of the isolated capacity cluster")
    parser.add_argument("--kubeconfig")
    parser.add_argument("--kubectl", default="kubectl")
    sub = parser.add_subparsers(dest="command", required=True)

    prep = sub.add_parser("prepare", help="generate synthetic fixture and create the overlay's Secrets")
    prep.add_argument("--profile", required=True)
    prep.add_argument("--server-name", default="narumq-capacity.test")
    prep.add_argument("--capacity-command", default=" ".join(DEFAULT_CAPACITY_COMMAND))
    prep.set_defaults(handler=prepare)

    dep = sub.add_parser("deploy", help="apply the capacity overlay with the runner image pinned by digest")
    dep.add_argument("--capacity-image", required=True, help="<repository>@sha256:<digest> (ARM64)")
    dep.add_argument("--roll", action="store_true", help="delete broker Pods one by one (updateStrategy OnDelete)")
    dep.add_argument("--ready-timeout", type=float, default=900)
    dep.set_defaults(handler=deploy)

    run = sub.add_parser("run", help="start the Job, inject leader failures, collect artifacts")
    run.add_argument("--profile", required=True)
    run.add_argument("--capacity-image", required=True)
    run.add_argument("--server-name", default="narumq-capacity.test")
    run.add_argument("--run-id")
    run.add_argument("--artifacts")
    run.add_argument("--generator-cpu", type=int, default=3)
    run.add_argument("--generator-memory", default="6Gi")
    run.add_argument("--job-timeout", type=int, default=21600)
    run.add_argument("--poll", type=float, default=1.0)
    run.add_argument("--ack-timeout", type=float, default=10.0)
    run.add_argument("--sample-seconds", type=float, default=10.0)
    run.set_defaults(handler=run_command)

    clean = sub.add_parser("cleanup", help="delete the Job and fixture objects")
    clean.set_defaults(handler=cleanup)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    kube = Kubectl(kubectl_global_args(args.kubectl, args.context, args.kubeconfig))
    try:
        return args.handler(args, kube)
    except (ControllerError, subprocess.TimeoutExpired, OSError, json.JSONDecodeError) as exc:
        print(f"capacity controller failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
