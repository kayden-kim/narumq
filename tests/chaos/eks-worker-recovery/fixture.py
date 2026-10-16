"""Command, Kubernetes, AWS and management API access for one disposable EKS fixture."""
from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import urllib.error
import urllib.request

from observer import MANAGEMENT_TIMEOUT_SECONDS, MutationRejected, node_diagnostic, timestamp


class CommandFailed(RuntimeError):
    pass


class Fixture:
    def __init__(self, private: Path, run_id: str):
        self.private = private
        self.run_id = run_id
        self.token = (private / "admin.token").read_text(encoding="utf-8").strip()
        self.environment = os.environ.copy()
        self.environment.update(AWS_PROFILE="kayden-sandbox-admin", AWS_REGION="ap-northeast-2", AWS_PAGER="")
        self.kubectl = [str(private / "kubectl.exe"), "--kubeconfig", str(private / "kubeconfig"), "--context", run_id]

    def command(self, args: list[str], timeout: float = 90, stdin: str | None = None) -> str:
        result = subprocess.run(args, env=self.environment, text=True, encoding="utf-8", input=stdin,
                                capture_output=True, timeout=timeout)
        if result.returncode:
            # CLI stderr can contain credentials. Structured observer results do not include it.
            raise CommandFailed(f"command exit {result.returncode}")
        return result.stdout

    def aws(self, *args) -> dict:
        return json.loads(self.command(["aws", *args, "--output", "json"]))

    def get(self, *args):
        value = self.command(self.kubectl + ["get", *args, "-o", "json"]).strip()
        return json.loads(value) if value else None

    def get_node(self, name: str):
        return self.get("node", name, "--ignore-not-found")

    def patch_node(self, name: str, operations: list[dict]):
        try:
            self.command(self.kubectl + ["patch", "node", name, "--type=json", "-p", json.dumps(operations), "-o", "json"])
        except (CommandFailed, subprocess.TimeoutExpired) as exc:
            raise MutationRejected("Kubernetes mutation command failed; application and failed operation unknown") from exc

    def get_pod(self, name: str):
        return self.get("pod", name, "-n", "narumq", "--ignore-not-found")

    def delete_pod(self, name: str, uid: str):
        """One DELETE with a UID precondition; kubectl has no flag for it, so send DeleteOptions raw."""
        options = {"kind": "DeleteOptions", "apiVersion": "v1", "gracePeriodSeconds": 0,
                   "preconditions": {"uid": uid}}
        try:
            self.command(self.kubectl + ["delete", "--raw", f"/api/v1/namespaces/narumq/pods/{name}", "-f", "-"],
                         stdin=json.dumps(options))
        except (CommandFailed, subprocess.TimeoutExpired) as exc:
            raise MutationRejected("Kubernetes delete command failed; application unknown") from exc

    def attachment_probe(self, pv_name: str, volume_id: str) -> dict:
        """Read-only VolumeAttachment and EBS attachment state; failures are recorded, not raised."""
        sample = {"at": timestamp()}
        try:
            listed = self.get("volumeattachments") or {"items": []}
            sample["volumeAttachments"] = [volume_attachment(item) for item in listed.get("items", [])
                                           if item.get("spec", {}).get("source", {}).get("persistentVolumeName") == pv_name]
        except Exception as exc:
            sample["volumeAttachmentErrorCode"] = type(exc).__name__
        try:
            volume = self.aws("ec2", "describe-volumes", "--volume-ids", volume_id)["Volumes"][0]
            sample["ebs"] = {"state": volume.get("State"),
                             "attachments": [{key: item.get(key) for key in ("InstanceId", "State", "AttachTime")}
                                             for item in volume.get("Attachments", [])]}
        except Exception as exc:
            sample["ebsErrorCode"] = type(exc).__name__
        return sample

    def instance(self, instance_id: str) -> dict:
        return self.aws("ec2", "describe-instances", "--instance-ids", instance_id)["Reservations"][0]["Instances"][0]

    def instance_state(self, instance_id: str) -> str:
        return self.instance(instance_id)["State"]["Name"]

    def api(self, path: str, port: int = 23880) -> dict:
        request = urllib.request.Request(f"http://127.0.0.1:{port}{path}",
                                        headers={"Authorization": "Bearer " + self.token})
        with urllib.request.urlopen(request, timeout=MANAGEMENT_TIMEOUT_SECONDS) as response:
            return json.load(response)["data"]

    def api_request(self, method: str, path: str, body=None, port: int = 23880,
                    idempotency_key: str | None = None) -> tuple[int, dict | None]:
        """One management API call. HTTP 4xx/5xx is returned as a status; transport errors raise."""
        headers = {"Authorization": "Bearer " + self.token}
        data = None
        if body is not None:
            data = json.dumps(body).encode("utf-8")
            headers["Content-Type"] = "application/json"
        if idempotency_key is not None:
            headers["Idempotency-Key"] = idempotency_key
        request = urllib.request.Request(f"http://127.0.0.1:{port}{path}", data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(request, timeout=MANAGEMENT_TIMEOUT_SECONDS) as response:
                status, raw = response.status, response.read()
        except urllib.error.HTTPError as exc:
            try:
                status, raw = exc.code, exc.read()
            finally:
                exc.close()
        try:
            envelope = json.loads(raw) if raw else None
        except ValueError:
            envelope = None
        return status, envelope if isinstance(envelope, dict) else None

    def management_probe(self) -> dict:
        cluster = self.api("/api/v1/cluster")
        return {key: cluster[key] for key in ("quorumAvailable", "leaderNodeId", "nodesByState")}

    def nodes_probe(self) -> list[dict]:
        nodes = self.api("/api/v1/nodes")
        if not isinstance(nodes, list):
            raise ValueError("node list is not a list")
        return [node_diagnostic(node) for node in nodes]


def ready(obj: dict) -> bool:
    return any(c["type"] == "Ready" and c["status"] == "True" for c in obj.get("status", {}).get("conditions", []))


def volume_attachment(item: dict) -> dict:
    status = item.get("status", {})
    return {"name": item["metadata"]["name"], "nodeName": item.get("spec", {}).get("nodeName"),
            "attached": status.get("attached"), "deletionTimestamp": item["metadata"].get("deletionTimestamp"),
            "attachErrorAt": (status.get("attachError") or {}).get("time"),
            "detachErrorAt": (status.get("detachError") or {}).get("time")}


def save(output: Path, name: str, value) -> None:
    (output / name).write_text(json.dumps(value, indent=2), encoding="utf-8")


def require_owned_fixture(fixture, plan: dict, instance_ids: list[str]) -> dict:
    """Read-only EKS/VPC/run-tag and worker ownership checks before any new CLU-14 mutation."""
    cluster_name, run_id, vpc_id = plan["clusterName"], plan["runId"], plan["vpcId"]
    if run_id != fixture.run_id:
        raise ValueError("fixture_run_mismatch")
    expected = {"Purpose": "disposable-acceptance-fixture", "DataClass": "synthetic-test-data-only",
                "AcceptanceRun": run_id}
    cluster = fixture.aws("eks", "describe-cluster", "--name", cluster_name)["cluster"]
    if cluster.get("resourcesVpcConfig", {}).get("vpcId") != vpc_id or any(
            cluster.get("tags", {}).get(key) != value for key, value in expected.items()):
        raise ValueError("cluster_not_owned_by_run")
    vpcs = fixture.aws("ec2", "describe-vpcs", "--vpc-ids", vpc_id)["Vpcs"]
    if len(vpcs) != 1 or vpcs[0].get("VpcId") != vpc_id:
        raise ValueError("fixture_vpc_missing")
    tags = {tag["Key"]: tag["Value"] for tag in vpcs[0].get("Tags", [])}
    if any(tags.get(key) != value for key, value in expected.items()):
        raise ValueError("vpc_not_owned_by_run")
    instances = [item for reservation in fixture.aws(
        "ec2", "describe-instances", "--instance-ids", *instance_ids)["Reservations"] for item in reservation["Instances"]]
    if {item["InstanceId"] for item in instances} != set(instance_ids):
        raise ValueError("fixture_instances_missing")
    groups = set()
    for item in instances:
        tags = {tag["Key"]: tag["Value"] for tag in item.get("Tags", [])}
        group = tags.get("aws:autoscaling:groupName", "")
        if item.get("VpcId") != vpc_id or not group.startswith((f"eks-{cluster_name}-", f"{cluster_name}-")):
            raise ValueError("instance_not_in_fixture_vpc_or_asg")
        groups.add(group)
    described = fixture.aws("autoscaling", "describe-auto-scaling-groups",
                            "--auto-scaling-group-names", *sorted(groups))["AutoScalingGroups"]
    if {group["AutoScalingGroupName"] for group in described} != groups:
        raise ValueError("fixture_asg_missing")
    for group in described:
        tags = {tag["Key"]: tag["Value"] for tag in group.get("Tags", [])}
        if tags.get("eks:cluster-name") != cluster_name and tags.get(f"kubernetes.io/cluster/{cluster_name}") != "owned":
            raise ValueError("asg_not_owned_by_fixture_cluster")
    return {"clusterName": cluster_name, "vpcId": vpc_id, "runTags": expected,
            "instanceIds": sorted(instance_ids), "asgNames": sorted(groups)}
