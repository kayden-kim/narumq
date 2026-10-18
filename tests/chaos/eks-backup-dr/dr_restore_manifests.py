"""Manifest derivations for the S3 full-restore run (CLU-SCN-050). Pure functions over parsed JSON objects."""
from __future__ import annotations

import copy
import re

from dr_common import NAMESPACE, OWNER_LABEL, RUN_LABEL, PlanError, StepFailed

RESTORE_TEMPLATE = "restore-data"
ZONE_LABEL = "topology.kubernetes.io/zone"
LAST_APPLIED = "kubectl.kubernetes.io/last-applied-configuration"
CONFIG_PATH = re.compile(r"/configs/node-[0-9]+\.json")


def restore_claim_names(ordinals: int = 3) -> list[str]:
    return [f"{RESTORE_TEMPLATE}-narumq-{ordinal}" for ordinal in range(ordinals)]


def restore_pvc(template: dict, name: str, run_id: str) -> dict:
    """A new empty claim with the spec of the StatefulSet's `data` template (class, size, access modes)."""
    spec = copy.deepcopy(template["spec"])
    spec.pop("volumeName", None)
    return {"apiVersion": "v1", "kind": "PersistentVolumeClaim",
            "metadata": {"name": name, "namespace": NAMESPACE,
                         "labels": {"app.kubernetes.io/name": "narumq", "app.kubernetes.io/component": "restore-target",
                                    OWNER_LABEL: "true", RUN_LABEL: run_id}},
            "spec": spec}


def restore_statefulset(original: dict, replicas: int) -> dict:
    """The live StatefulSet with its `data` claim template renamed so the pods bind new `restore-data-*` claims."""
    templates = original["spec"].get("volumeClaimTemplates", [])
    if [t["metadata"]["name"] for t in templates] != ["data"]:
        raise StepFailed("restore-statefulset", "unexpected_volume_claim_templates")
    result = copy.deepcopy(original)
    metadata = result["metadata"]
    annotations = {k: v for k, v in metadata.get("annotations", {}).items() if k != LAST_APPLIED}
    result["metadata"] = {"name": metadata["name"], "namespace": metadata["namespace"],
                          "labels": metadata.get("labels", {}), **({"annotations": annotations} if annotations else {})}
    result.pop("status", None)
    result["spec"]["replicas"] = replicas
    result["spec"]["volumeClaimTemplates"][0]["metadata"] = {
        **{k: v for k, v in templates[0]["metadata"].items() if k in ("labels", "annotations")},
        "name": RESTORE_TEMPLATE}
    result["spec"]["volumeClaimTemplates"][0].pop("status", None)
    renamed = 0
    pod = result["spec"]["template"]["spec"]
    for group in ("containers", "initContainers"):
        for container in pod.get(group, []):
            for mount in container.get("volumeMounts", []):
                if mount["name"] == "data":
                    mount["name"] = RESTORE_TEMPLATE
                    renamed += 1
    if renamed < 3 or any(v.get("name") == "data" for v in pod.get("volumes", [])):
        raise StepFailed("restore-statefulset", "unexpected_data_volume_mounts")
    return result


def statefulset_retains_claims(statefulset: dict) -> bool:
    policy = statefulset["spec"].get("persistentVolumeClaimRetentionPolicy") or {}
    return policy.get("whenDeleted", "Retain") == "Retain" and policy.get("whenScaled", "Retain") == "Retain"


def seed_jobs(items: list[dict], claims: list[str], zones: list[str], node_ids: list[str], run_id: str) -> list[dict]:
    """The deployed seed Jobs, pointed at the new claims, the original zones and the live slot->member mapping."""
    if len(items) != 3 or not (len(claims) == len(zones) == len(node_ids) == 3):
        raise PlanError("seed_jobs_expected_three")
    jobs = []
    for ordinal, source in enumerate(sorted(items, key=lambda j: j["metadata"]["generateName"])):
        if source["metadata"].get("generateName") != f"narumq-backup-restore-node-{ordinal + 1}-":
            raise StepFailed("seed", "unexpected_seed_job_names")
        job = copy.deepcopy(source)
        labels = job["metadata"].setdefault("labels", {})
        labels.update({OWNER_LABEL: "true", RUN_LABEL: run_id})
        pod = job["spec"]["template"]["spec"]
        claimed = [v for v in pod["volumes"] if v.get("persistentVolumeClaim", {}).get("claimName") == f"data-narumq-{ordinal}"]
        if len(claimed) != 1:
            raise StepFailed("seed", "seed_job_claim_not_found")
        claimed[0]["persistentVolumeClaim"]["claimName"] = claims[ordinal]
        pod["nodeSelector"] = {ZONE_LABEL: zones[ordinal]}
        restore = [c for c in pod["containers"] if c["name"] == "restore"]
        if len(restore) != 1 or len(CONFIG_PATH.findall("\n".join(restore[0]["args"]))) != 1:
            raise StepFailed("seed", "seed_job_config_path_not_found")
        restore[0]["args"] = [CONFIG_PATH.sub(f"/configs/node-{node_ids[ordinal]}.json", arg) for arg in restore[0]["args"]]
        jobs.append(job)
    return jobs
