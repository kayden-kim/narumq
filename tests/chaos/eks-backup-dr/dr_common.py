"""kubectl, AWS and ownership helpers shared by the backup/DR acceptance runs.

Every mutation runs once and is never retried. Failures carry a stable code (`StepFailed.code`); command output is
never copied into evidence because CLI stderr can contain credentials. Credentials are read from --private only.
"""
from __future__ import annotations

import json
from pathlib import Path
import re
import sys
import time
from typing import Callable

CHAOS_DIR = Path(__file__).resolve().parents[1]
REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
RECOVERY_DIR = CHAOS_DIR / "eks-worker-recovery"
VOLUME_RESEED_DIR = CHAOS_DIR / "eks-volume-reseed"
for _directory in (RECOVERY_DIR,):
    if str(_directory) not in sys.path:
        sys.path.append(str(_directory))

from fixture import CommandFailed, Fixture, ready, save  # noqa: E402,F401
from observer import timestamp  # noqa: E402,F401

NAMESPACE = "narumq"
CRONJOB = "narumq-backup"
BACKUP_PVC = "narumq-backup-pvc"
SCRIPTS_CONFIGMAP = "narumq-backup-scripts"
CONFIGS_CONFIGMAP = "narumq-cluster-configs"
STATEFULSET = "narumq"
BROKER_LABEL = "app=narumq"
DISPOSABLE_PREFIXES = ("narumq-ha-acceptance-", "narumq-clu14-")
RUN_LABEL = "narumq.io/acceptance-run"
OWNER_LABEL = "narumq.io/dr-acceptance"
APPROVED_SCHEDULE = "0 2 * * *"
APPROVED_RETENTION_DAYS = 30
APPROVED_RPO_SECONDS = 24 * 3600
APPROVED_RTO_SECONDS = 3600
DEPLOY_DIR = REPOSITORY_ROOT / "deploy" / "kubernetes"
BACKUP_SCRIPT = DEPLOY_DIR / "eks-ec2-ha" / "backup.sh"
S3_LIST_JOB = DEPLOY_DIR / "eks-ec2-ha-s3" / "restore-from-s3-list-job.yaml"
S3_SEED_JOBS = DEPLOY_DIR / "eks-ec2-ha-s3" / "restore-from-s3-seed-jobs.yaml"
RUN_ID = re.compile(r"^[a-z0-9][a-z0-9-]{5,62}$")


class PlanError(ValueError):
    """The plan or arguments are unusable; `args[0]` is a stable code."""


class StepFailed(RuntimeError):
    def __init__(self, step: str, code: str):
        super().__init__(f"{step}: {code}")
        self.step = step
        self.code = code


def bounded(plan: dict, key: str, default: int, bounds: tuple[int, int]) -> int:
    value = plan.get(key, default)
    if isinstance(value, bool) or not isinstance(value, int) or not bounds[0] <= value <= bounds[1]:
        raise PlanError(f"plan_{key}_out_of_range:{bounds[0]}..{bounds[1]}")
    return value


def require_strings(plan: dict, keys: tuple[str, ...]) -> None:
    for key in keys:
        if not isinstance(plan.get(key), str) or not plan[key]:
            raise PlanError(f"plan_key_missing:{key}")


def validate_fixture_identity(plan: dict, run_id: str) -> None:
    """Plan shape of the disposable fixture; reads nothing from the cluster."""
    if not isinstance(plan, dict):
        raise PlanError("plan_not_object")
    require_strings(plan, ("runId", "clusterName", "vpcId"))
    if plan["runId"] != run_id or not RUN_ID.fullmatch(run_id):
        raise PlanError("plan_run_id_mismatch")
    if not plan["clusterName"].startswith(DISPOSABLE_PREFIXES) or plan["clusterName"] == "narumq-eks":
        raise PlanError("plan_cluster_not_disposable")


def wait_until(predicate: Callable, timeout: float, interval: float, sleep: Callable = time.sleep,
               monotonic: Callable = time.monotonic):
    """Return the first truthy predicate value, or None at the deadline. The predicate runs at least once."""
    deadline = monotonic() + timeout
    while True:
        value = predicate()
        if value:
            return value
        if monotonic() >= deadline:
            return None
        sleep(interval)


# -- kubectl ----------------------------------------------------------------------------------------------

def kube(fixture, *args: str, timeout: float = 90, stdin: str | None = None) -> str:
    return fixture.command(fixture.kubectl + ["-n", NAMESPACE, *args], timeout=timeout, stdin=stdin)


def kube_json(fixture, *args: str, timeout: float = 90):
    value = kube(fixture, "get", *args, "-o", "json", timeout=timeout).strip()
    return json.loads(value) if value else None


def kube_create(fixture, manifest: dict, timeout: float = 120) -> dict:
    return json.loads(kube(fixture, "create", "-f", "-", "-o", "json", timeout=timeout, stdin=json.dumps(manifest)))


def kube_delete(fixture, kind: str, name: str, timeout: float = 180) -> None:
    kube(fixture, "delete", kind, name, "--ignore-not-found", "--wait=true", f"--timeout={int(timeout)}s",
         timeout=timeout + 30)


def broker_pods(fixture) -> list[dict]:
    return sorted(kube_json(fixture, "pods", "-l", BROKER_LABEL)["items"], key=lambda pod: pod["metadata"]["name"])


def three_brokers_ready(pods: list[dict]) -> bool:
    names = [pod["metadata"]["name"] for pod in pods]
    return names == ["narumq-0", "narumq-1", "narumq-2"] and all(
        ready(pod) and not pod["metadata"].get("deletionTimestamp") for pod in pods)


def kubectl_server(fixture) -> str:
    return json.loads(fixture.command(fixture.kubectl + ["config", "view", "--minify", "-o", "json"]))[
        "clusters"][0]["cluster"]["server"]


def tags_of(resource: dict) -> dict:
    return {tag["Key"]: tag["Value"] for tag in resource.get("Tags", [])}


def require_owned_cluster(fixture, plan: dict) -> dict:
    """Read-only: the EKS cluster and VPC carry this run's disposable tags and kubectl talks to that cluster."""
    expected = {"Purpose": "disposable-acceptance-fixture", "DataClass": "synthetic-test-data-only",
                "AcceptanceRun": plan["runId"]}
    cluster = fixture.aws("eks", "describe-cluster", "--name", plan["clusterName"])["cluster"]
    if cluster.get("resourcesVpcConfig", {}).get("vpcId") != plan["vpcId"] or any(
            cluster.get("tags", {}).get(key) != value for key, value in expected.items()):
        raise StepFailed("fixture", "cluster_not_owned_by_run")
    vpcs = fixture.aws("ec2", "describe-vpcs", "--vpc-ids", plan["vpcId"])["Vpcs"]
    if len(vpcs) != 1 or any(tags_of(vpcs[0]).get(key) != value for key, value in expected.items()):
        raise StepFailed("fixture", "vpc_not_owned_by_run")
    if kubectl_server(fixture).rstrip("/").lower() != str(cluster.get("endpoint", "")).rstrip("/").lower():
        raise StepFailed("fixture", "kubectl_context_is_not_the_planned_cluster")
    return {"clusterName": plan["clusterName"], "vpcId": plan["vpcId"], "runTags": expected,
            "endpointMatchesKubectlContext": True}


# -- Pods and Jobs ------------------------------------------------------------------------------------------

def helper_pod(name: str, run_id: str, image: str, script: str, *, env: dict[str, str] | None = None,
               backup_pvc_read_only: bool | None = None, configmaps: dict[str, str] | None = None,
               scratch: dict[str, str] | None = None, memory: str = "256Mi", cpu: str = "250m") -> dict:
    """A one-shot Pod with the backup Job's security context. `backup_pvc_read_only` None means no PVC."""
    mounts = [{"name": "tmp", "mountPath": "/tmp"}]
    volumes: list[dict] = [{"name": "tmp", "emptyDir": {"sizeLimit": "64Mi"}}]
    if backup_pvc_read_only is not None:
        mounts.append({"name": "backup-storage", "mountPath": "/backups", "readOnly": backup_pvc_read_only})
        volumes.append({"name": "backup-storage",
                        "persistentVolumeClaim": {"claimName": BACKUP_PVC, "readOnly": backup_pvc_read_only}})
    for volume_name, mount_path in (configmaps or {}).items():
        mounts.append({"name": volume_name, "mountPath": mount_path, "readOnly": True})
        volumes.append({"name": volume_name, "configMap": {"name": CONFIGS_CONFIGMAP, "defaultMode": 0o444}})
    for volume_name, mount_path in (scratch or {}).items():
        mounts.append({"name": volume_name, "mountPath": mount_path})
        volumes.append({"name": volume_name, "emptyDir": {"sizeLimit": "1Gi"}})
    return {
        "apiVersion": "v1", "kind": "Pod",
        "metadata": {"name": name, "namespace": NAMESPACE, "labels": {OWNER_LABEL: "true", RUN_LABEL: run_id}},
        "spec": {
            "restartPolicy": "Never", "automountServiceAccountToken": False, "terminationGracePeriodSeconds": 5,
            "securityContext": {"runAsUser": 10001, "runAsGroup": 10001, "fsGroup": 10001,
                                "fsGroupChangePolicy": "OnRootMismatch"},
            "containers": [{
                "name": "work", "image": image, "imagePullPolicy": "IfNotPresent",
                "command": ["/bin/sh", "-c", script],
                "env": [{"name": key, "value": value} for key, value in sorted((env or {}).items())],
                "volumeMounts": mounts,
                "resources": {"requests": {"cpu": cpu, "memory": memory}, "limits": {"cpu": "1", "memory": "2Gi"}},
                "securityContext": {"allowPrivilegeEscalation": False, "readOnlyRootFilesystem": True,
                                    "runAsNonRoot": True, "runAsUser": 10001, "runAsGroup": 10001,
                                    "capabilities": {"drop": ["ALL"]}},
            }],
            "volumes": volumes,
        },
    }


def run_pod(fixture, manifest: dict, timeout: float, sleep: Callable = time.sleep,
            monotonic: Callable = time.monotonic) -> dict:
    """Create a one-shot Pod, wait for Succeeded/Failed, read its log and always delete it."""
    name = manifest["metadata"]["name"]
    created = False
    try:
        pod = kube_create(fixture, manifest)
        created = True

        def finished():
            current = kube_json(fixture, "pod", name)
            return current if current["status"].get("phase") in ("Succeeded", "Failed") else None

        done = wait_until(finished, timeout, 3, sleep, monotonic)
        if done is None:
            raise StepFailed("pod", f"pod_timeout:{name}")
        logs = kube(fixture, "logs", name, "-c", "work", "--limit-bytes=134217728", timeout=300)
        return {"name": name, "uid": pod["metadata"]["uid"], "phase": done["status"]["phase"], "logs": logs,
                "node": done["spec"].get("nodeName")}
    finally:
        if created:
            kube_delete(fixture, "pod", name)


def create_job_from_cronjob(fixture, name: str) -> dict:
    return json.loads(kube(fixture, "create", "job", name, f"--from=cronjob/{CRONJOB}", "-o", "json"))


def wait_job(fixture, name: str, timeout: float, sleep: Callable = time.sleep,
             monotonic: Callable = time.monotonic) -> dict:
    """Return the Job once it has succeeded or failed; None at the deadline."""
    def finished():
        job = kube_json(fixture, "job", name)
        status = job.get("status", {})
        conditions = {c["type"] for c in status.get("conditions", []) if c.get("status") == "True"}
        if "Complete" in conditions or status.get("succeeded", 0) >= 1:
            return {"job": job, "succeeded": True}
        if "Failed" in conditions:
            return {"job": job, "succeeded": False}
        return None

    return wait_until(finished, timeout, 3, sleep, monotonic)


def job_logs(fixture, job: str, container: str) -> str:
    return kube(fixture, "logs", f"job/{job}", "-c", container, "--limit-bytes=4194304", timeout=180)
