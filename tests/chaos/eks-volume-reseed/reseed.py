"""Decisions, checks and mutation helpers for the EKS slot reseed acceptance run.

Spec: docs/specs/cluster-session-and-delivery.md "EKS slot 재시드 기준 (2026-10-16)" and ADR-0061.
This module holds no run state. Functions take a fixture-like object (`tests/chaos/eks-worker-recovery/fixture.py`)
so the same code is exercised with fakes in `tests/test_eks_volume_reseed.py`.
Credentials are never returned in evidence; private key material is written only below the private directory.
"""
from __future__ import annotations

import base64
import copy
import datetime
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys
import time
from typing import Callable
import socket

RECOVERY_DIR = Path(__file__).resolve().parents[1] / "eks-worker-recovery"
if str(RECOVERY_DIR) not in sys.path:
    sys.path.append(str(RECOVERY_DIR))

from fixture import ready, require_owned_fixture  # noqa: E402
from observer import MutationRejected, timestamp  # noqa: E402

NAMESPACE = "narumq"
CONFIGMAP = "narumq-cluster-configs"
SECRET = "narumq-cluster-secrets"
ACCOUNT = "809446814769"
PEER_PORT = 7000
CONTAINER = "narumq"
GUARD_MESSAGE = "cluster bootstrap refused"
OBSERVER_PORTS = (23880, 23883)
TARGET_PORTS = (24880, 24883)
LEADER_PORTS = (22880, 22883)  # 25839-26038 can fall in a Windows excluded port range
ORDINALS = (0, 1, 2)
UINT64_MAX = 2 ** 64 - 1
CERTIFICATE_VALIDITY = datetime.timedelta(hours=24)
# 25 s x 60 messages = 1500 s of publisher coverage starting just before the fault; the stream is capped at 60
# messages by the per-session queue limit of the broker (durable_stream.PER_SESSION_LIMIT).
DEFAULT_STREAM_INTERVAL_SECONDS = 25.0

PLAN_STRINGS = ("runId", "clusterName", "vpcId", "targetPod", "oldPodUID", "oldWorkerName", "oldNodeUID",
                "oldInstanceId", "pvcName", "pvcUID", "pvName", "volumeId", "zone", "spareWorkerName",
                "spareInstanceId", "observerPod", "observerPodUID", "observerWorkerName", "observerInstanceId",
                "durableScenario")
SCENARIO = re.compile(r"^[a-z0-9][a-z0-9-]{0,39}$")
DECIMAL_ID = re.compile(r"^[1-9][0-9]*$")


class PlanError(ValueError):
    pass


class PreflightRejected(ValueError):
    """The fixture does not satisfy a precondition; `code` is a stable identifier, never a message with secrets."""

    def __init__(self, code: str, detail: str = ""):
        super().__init__(code if not detail else f"{code}: {detail}")
        self.code = code


def reject(code: str, detail: str = "") -> None:
    raise PreflightRejected(code, detail)


def canonical_id(value, name: str) -> str:
    """A positive canonical decimal uint64 given as int or string."""
    if isinstance(value, bool):
        raise PlanError(f"{name} must be a member id")
    if isinstance(value, int):
        value = str(value)
    if not isinstance(value, str) or not DECIMAL_ID.match(value) or int(value) > UINT64_MAX:
        raise PlanError(f"{name} must be a canonical positive decimal uint64")
    return value


def load_plan(raw: dict, run_id: str) -> dict:
    """Validate and normalise the plan; member ids become canonical decimal strings."""
    if not isinstance(raw, dict):
        raise PlanError("plan must be an object")
    plan = dict(raw)
    for key in PLAN_STRINGS:
        if not isinstance(plan.get(key), str) or not plan[key]:
            raise PlanError(f"plan key {key} is missing")
    ordinal = plan.get("slotOrdinal")
    if isinstance(ordinal, bool) or not isinstance(ordinal, int) or ordinal not in ORDINALS:
        raise PlanError("slotOrdinal must be 0, 1 or 2")
    plan["targetNodeId"] = canonical_id(plan.get("targetNodeId"), "targetNodeId")
    plan["newNodeId"] = canonical_id(plan.get("newNodeId"), "newNodeId")
    if plan["runId"] != run_id:
        raise PlanError("plan does not identify this run")
    if not plan["clusterName"].startswith(("narumq-ha-acceptance-", "narumq-clu14-")):
        raise PlanError("plan does not identify a disposable acceptance fixture")
    if plan["targetPod"] != f"narumq-{ordinal}" or plan["pvcName"] != f"data-narumq-{ordinal}":
        raise PlanError("targetPod and pvcName must match slotOrdinal")
    if int(plan["newNodeId"]) <= int(plan["targetNodeId"]):
        raise PlanError("newNodeId must be greater than the lost member id")
    if not SCENARIO.match(plan["durableScenario"]):
        raise PlanError("durableScenario must be a short lowercase token")
    plan.setdefault("durableIntervalSeconds", DEFAULT_STREAM_INTERVAL_SECONDS)
    import durable_stream
    try:
        durable_stream.plan_settings(plan)
    except ValueError as exc:
        raise PlanError(str(exc)) from exc
    if not re.fullmatch(r"narumq-[0-2]", plan["observerPod"]) or plan["observerPod"] == plan["targetPod"]:
        raise PlanError("observerPod must be another broker Pod")
    if plan["observerWorkerName"] == plan["oldWorkerName"] or plan["observerInstanceId"] == plan["oldInstanceId"]:
        raise PlanError("observer must use an independent surviving worker")
    if plan["spareWorkerName"] in (plan["oldWorkerName"], plan["observerWorkerName"]) \
            or plan["spareInstanceId"] in (plan["oldInstanceId"], plan["observerInstanceId"]):
        raise PlanError("spare must be independent of the lost and observer workers")
    apply_large_snapshot(plan)
    return plan


def apply_large_snapshot(plan: dict) -> None:
    """Validate `largeSnapshot` and store the normalised settings back into the plan."""
    settings = large_snapshot_settings(plan)
    if settings is not None:
        plan["largeSnapshot"] = settings


def slot_dns(ordinal: int) -> str:
    return f"narumq-{ordinal}.narumq-headless.{NAMESPACE}.svc.cluster.local"


def slot_address(ordinal: int) -> str:
    return f"{slot_dns(ordinal)}:{PEER_PORT}"


def instance_of(node: dict) -> str:
    return str(node.get("spec", {}).get("providerID", "")).rsplit("/", 1)[-1]


def pod_ordinal(pod_name: str) -> int:
    return int(pod_name.rsplit("-", 1)[1])


# ---- slot ConfigMap -------------------------------------------------------------------------------------------

def parse_slots(configmap_data: dict) -> dict[int, str]:
    """Ordinal -> member id from `slot-<ordinal>`; values must be canonical and distinct."""
    slots: dict[int, str] = {}
    for ordinal in ORDINALS:
        value = configmap_data.get(f"slot-{ordinal}")
        if not isinstance(value, str) or not DECIMAL_ID.match(value):
            reject("slot_invalid", f"slot-{ordinal}")
        slots[ordinal] = value
    if len(set(slots.values())) != len(slots):
        reject("slot_duplicate")
    return slots


def derive_joiner_config(old_config: dict, plan: dict, slots: dict[int, str]) -> dict:
    """The joiner configuration for the new id: same slot address and files, joiner role, seeds = other two slots."""
    ordinal = plan["slotOrdinal"]
    if old_config.get("nodeId") != int(plan["targetNodeId"]):
        reject("old_config_node_id_mismatch")
    if old_config.get("advertisedAddress") != slot_address(ordinal) or old_config.get("localDnsName") != slot_dns(ordinal):
        reject("old_config_address_mismatch")
    config = copy.deepcopy(old_config)
    config["nodeId"] = int(plan["newNodeId"])
    config["role"] = "joiner"
    config["bootstrapMembers"] = []
    config["seedAddresses"] = [slot_address(other) for other in sorted(slots) if other != ordinal]
    return config


def config_text(config: dict) -> str:
    return json.dumps(config, indent=2) + "\n"


def configmap_with_reseed(configmap: dict, plan: dict, joiner_text: str) -> dict:
    """Add `node-<new>.json` and point the slot at the new id; the slot must still name the lost id."""
    ordinal, old, new = plan["slotOrdinal"], plan["targetNodeId"], plan["newNodeId"]
    result = copy.deepcopy(configmap)
    data = result["data"]
    if data.get(f"slot-{ordinal}") != old:
        reject("slot_mismatch", f"slot-{ordinal}")
    if f"node-{new}.json" in data:
        reject("joiner_config_exists")
    if f"node-{old}.json" not in data:
        reject("old_config_missing")
    data[f"node-{new}.json"] = joiner_text
    data[f"slot-{ordinal}"] = new
    return result


def configmap_without_old(configmap: dict, plan: dict) -> dict:
    ordinal, old, new = plan["slotOrdinal"], plan["targetNodeId"], plan["newNodeId"]
    result = copy.deepcopy(configmap)
    data = result["data"]
    if data.get(f"slot-{ordinal}") != new:
        reject("slot_mismatch", f"slot-{ordinal}")
    if f"node-{new}.json" not in data:
        reject("joiner_config_missing")
    if data.pop(f"node-{old}.json", None) is None:
        reject("old_config_missing")
    return result


def secret_with_certificate(secret: dict, ordinal: int, cert_pem: bytes, key_pem: bytes) -> tuple[dict, dict]:
    """Replace the slot certificate and key. Returns the new Secret and the previous base64 values (private only)."""
    result = copy.deepcopy(secret)
    data = result["data"]
    crt, key = f"node-{ordinal}.crt", f"node-{ordinal}.key"
    if crt not in data or key not in data:
        reject("secret_slot_keys_missing")
    previous = {crt: data[crt], key: data[key]}
    data[crt] = base64.b64encode(cert_pem).decode("ascii")
    data[key] = base64.b64encode(key_pem).decode("ascii")
    return result, previous


def digest(value: str | bytes) -> str:
    return hashlib.sha256(value.encode("utf-8") if isinstance(value, str) else value).hexdigest()


# ---- peer certificate -----------------------------------------------------------------------------------------

def generate_peer_certificate(ca_cert_pem: bytes, ca_key_pem: bytes, dns_name: str,
                              now: datetime.datetime | None = None) -> dict:
    """P-256 leaf signed by the fixture peer CA. SAN is exactly `dns_name`; EKU serverAuth and clientAuth."""
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

    ca_cert = x509.load_pem_x509_certificate(ca_cert_pem)
    ca_key = serialization.load_pem_private_key(ca_key_pem, password=None)
    if not isinstance(ca_key, ec.EllipticCurvePrivateKey):
        raise ValueError("fixture peer CA key is not an EC key")
    now = now or datetime.datetime.now(datetime.timezone.utc)
    key = ec.generate_private_key(ec.SECP256R1())
    certificate = (x509.CertificateBuilder()
                   .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, dns_name)]))
                   .issuer_name(ca_cert.subject)
                   .public_key(key.public_key())
                   .serial_number(x509.random_serial_number())
                   .not_valid_before(now - datetime.timedelta(minutes=1))
                   .not_valid_after(now + CERTIFICATE_VALIDITY)
                   .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
                   .add_extension(x509.KeyUsage(digital_signature=True, content_commitment=False,
                                                key_encipherment=False, data_encipherment=False, key_agreement=False,
                                                key_cert_sign=False, crl_sign=False, encipher_only=False,
                                                decipher_only=False), critical=True)
                   .add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH,
                                                         ExtendedKeyUsageOID.CLIENT_AUTH]), critical=False)
                   .add_extension(x509.SubjectAlternativeName([x509.DNSName(dns_name)]), critical=False)
                   .add_extension(x509.SubjectKeyIdentifier.from_public_key(key.public_key()), critical=False)
                   .add_extension(x509.AuthorityKeyIdentifier.from_issuer_public_key(ca_cert.public_key()),
                                  critical=False)
                   .sign(ca_key, hashes.SHA256()))
    der = certificate.public_bytes(serialization.Encoding.DER)
    return {"certificatePem": certificate.public_bytes(serialization.Encoding.PEM),
            "keyPem": key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.TraditionalOpenSSL,
                                        serialization.NoEncryption()),
            "sha256": hashlib.sha256(der).hexdigest(), "dnsNames": [dns_name],
            "notBefore": certificate.not_valid_before_utc.isoformat(),
            "notAfter": certificate.not_valid_after_utc.isoformat()}


def write_private(private: Path, name: str, content: bytes) -> Path:
    """Create a new file below the private directory; never overwrites."""
    path = private / name
    with path.open("xb") as stream:
        stream.write(content)
    return path


def write_private_material(private: Path, plan: dict, material: dict, previous_secret: dict,
                           suffix: str = "") -> dict:
    """Keep the new key/cert and the previous Secret values only in the private directory.

    A resumed run passes a `suffix` so its files never collide with the interrupted run's.
    """
    stem = f"reseed-{plan['runId']}-slot{plan['slotOrdinal']}"
    names = {"certificate": f"{stem}-node{plan['newNodeId']}{suffix}.crt",
             "key": f"{stem}-node{plan['newNodeId']}{suffix}.key",
             "rollbackSecret": f"{stem}-rollback-secret{suffix}.json"}
    write_private(private, names["certificate"], material["certificatePem"])
    write_private(private, names["key"], material["keyPem"])
    write_private(private, names["rollbackSecret"], json.dumps(previous_secret, indent=2).encode("utf-8"))
    return names


# ---- membership and preflight checks --------------------------------------------------------------------------

def node_rows(nodes: list) -> dict[str, dict]:
    rows: dict[str, dict] = {}
    for node in nodes:
        node_id = node.get("nodeId")
        if not isinstance(node_id, str) or not DECIMAL_ID.match(node_id) or node_id in rows:
            reject("member_list_invalid")
        rows[node_id] = node
    return rows


def check_members(nodes: list, slots: dict[int, str], plan: dict, leader_id) -> dict:
    """Exactly three OK voters on the slot ids; target is a follower; the new id is unused and above every id."""
    rows = node_rows(nodes)
    active = {node_id for node_id, row in rows.items() if row.get("membershipPhase") != "RETIRED"}
    if active != set(slots.values()) or len(active) != 3:
        reject("not_three_voters_on_slots")
    for node_id in active:
        row = rows[node_id]
        if row.get("membershipPhase") != "VOTER" or row.get("queryStatus") != "OK":
            reject("voter_not_observed", node_id)
    if plan["newNodeId"] in rows:
        reject("new_id_in_use", plan["newNodeId"])
    highest = max(int(node_id) for node_id in rows)
    if int(plan["newNodeId"]) <= highest:
        reject("new_id_not_above_committed_ids", str(highest))
    target = rows.get(plan["targetNodeId"])
    if target is None or str(leader_id) == plan["targetNodeId"] or target.get("role") != "FOLLOWER":
        reject("target_is_leader")
    return {"voters": sorted(active, key=int), "retired": sorted((i for i in rows if i not in active), key=int),
            "highestCommittedMemberId": highest, "leaderNodeId": str(leader_id)}


def check_slot_target(slots: dict[int, str], plan: dict) -> None:
    if slots.get(plan["slotOrdinal"]) != plan["targetNodeId"]:
        reject("slot_mismatch", f"slot-{plan['slotOrdinal']}")


def check_spare(spare: dict | None, instance_state: str, pods: list[dict], plan: dict) -> dict:
    """An independent Ready, schedulable worker in the same AZ that carries no broker Pod."""
    if not spare:
        reject("spare_missing")
    metadata = spare.get("metadata", {})
    if (not ready(spare) or spare.get("spec", {}).get("unschedulable", False) or metadata.get("deletionTimestamp")
            or instance_state != "running"):
        reject("spare_not_ready")
    if instance_of(spare) != plan["spareInstanceId"] or plan["spareInstanceId"] == plan["oldInstanceId"]:
        reject("spare_identity_mismatch")
    if metadata.get("labels", {}).get("topology.kubernetes.io/zone") != plan["zone"]:
        reject("spare_zone_mismatch")
    if any(pod.get("spec", {}).get("nodeName") == plan["spareWorkerName"] for pod in pods):
        reject("spare_hosts_broker")
    return {"nodeName": plan["spareWorkerName"], "nodeUID": metadata.get("uid"), "instanceId": plan["spareInstanceId"],
            "zone": plan["zone"]}


def check_ebs_mapping(instance: dict, plan: dict) -> dict:
    if instance.get("State", {}).get("Name") != "running" or instance.get("VpcId") != plan["vpcId"]:
        reject("target_instance_not_running_in_vpc")
    mapping = next((m for m in instance.get("BlockDeviceMappings", []) if m.get("Ebs", {}).get("VolumeId") == plan["volumeId"]),
                   None)
    if mapping is None:
        reject("volume_not_attached_to_target")
    if mapping["Ebs"].get("DeleteOnTermination") is not False:
        reject("volume_deleted_on_termination")
    return {"deviceName": mapping.get("DeviceName"), "deleteOnTermination": False}


def release_version(features: dict, survivor_ids: list[str]) -> str:
    """The survivors' common release version, copied from the committed member profiles."""
    members = {item.get("nodeId"): item.get("releaseVersion") for item in features.get("members", [])}
    versions = {members.get(node_id) for node_id in survivor_ids}
    if len(versions) != 1 or None in versions or not re.fullmatch(r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)",
                                                                 next(iter(versions))):
        reject("survivor_release_versions_differ")
    return versions.pop()


def run_preflight(fixture, plan: dict, port: int = OBSERVER_PORTS[0]) -> tuple[dict, dict]:
    """Read-only ownership and safety checks. Returns (evidence, context for later steps)."""
    ordinal = plan["slotOrdinal"]
    cluster = check_ownership(fixture, plan, port)
    nodes = fixture.api("/api/v1/nodes?limit=1000", port)
    configmap = fixture.get("configmap", CONFIGMAP, "-n", NAMESPACE)
    slots = parse_slots(configmap["data"])
    check_slot_target(slots, plan)
    members = check_members(nodes, slots, plan, cluster["leaderNodeId"])
    survivors = [node_id for slot, node_id in sorted(slots.items()) if slot != ordinal]
    pods = fixture.get("pods", "-n", NAMESPACE, "-l", "app=narumq")["items"]
    if len(pods) != 3 or not all(ready(pod) for pod in pods):
        reject("three_ready_brokers_required")
    target = next((pod for pod in pods if pod["metadata"]["name"] == plan["targetPod"]), None)
    old_node = fixture.get_node(plan["oldWorkerName"])
    if (target is None or target["metadata"]["uid"] != plan["oldPodUID"]
            or target["spec"].get("nodeName") != plan["oldWorkerName"] or not old_node
            or old_node["metadata"]["uid"] != plan["oldNodeUID"] or instance_of(old_node) != plan["oldInstanceId"]):
        reject("target_identity_changed")
    instance = fixture.instance(plan["oldInstanceId"])
    ebs = check_ebs_mapping(instance, plan)
    volume = fixture.aws("ec2", "describe-volumes", "--volume-ids", plan["volumeId"])["Volumes"][0]
    attachments = [item.get("InstanceId") for item in volume.get("Attachments", [])]
    if volume.get("AvailabilityZone") != plan["zone"] or attachments != [plan["oldInstanceId"]]:
        reject("volume_zone_or_attachment_mismatch")
    pvc = fixture.get("pvc", plan["pvcName"], "-n", NAMESPACE)
    pv = fixture.get("pv", pvc["spec"]["volumeName"])
    claim = pv["spec"].get("claimRef", {})
    if (pvc["metadata"]["uid"] != plan["pvcUID"] or pv["metadata"]["name"] != plan["pvName"]
            or pv["spec"]["csi"]["volumeHandle"] != plan["volumeId"]
            or claim.get("name") != plan["pvcName"] or claim.get("uid") != plan["pvcUID"]):
        reject("data_identity_mismatch")
    spare_node = fixture.get_node(plan["spareWorkerName"])
    spare_state = fixture.instance_state(plan["spareInstanceId"]) if spare_node else "absent"
    spare = check_spare(spare_node, spare_state, pods, plan)
    old_config_text = configmap["data"].get(f"node-{plan['targetNodeId']}.json")
    if old_config_text is None:
        reject("old_config_missing")
    old_config = json.loads(old_config_text)
    joiner = derive_joiner_config(old_config, plan, slots)
    features = fixture.api("/api/v1/cluster/features", port)
    version = release_version(features, survivors)
    for key in ("clusterProtocolMajor", "clusterProtocolMinor", "stateSchemaMajor", "stateSchemaMinor"):
        if not isinstance(old_config.get(key), int) or isinstance(old_config.get(key), bool):
            reject("old_config_profile_invalid", key)
    evidence = {"at": timestamp(), "account": ACCOUNT, "clusterName": plan["clusterName"], "members": members,
                "slots": {str(slot): node_id for slot, node_id in sorted(slots.items())},
                "targetRole": "FOLLOWER", "ebs": ebs, "volume": {"zone": plan["zone"], "attachedTo": attachments[0],
                                                                  "tags": volume.get("Tags", [])},
                "pvc": {"uid": plan["pvcUID"], "pv": plan["pvName"]}, "spare": spare,
                "releaseVersion": version, "joinerSeedAddresses": joiner["seedAddresses"]}
    context = {"slots": slots, "survivors": survivors, "oldConfig": old_config, "joinerConfig": joiner,
               "releaseVersion": version, "spareNodeUID": spare["nodeUID"], "configmapResourceVersion":
                   configmap["metadata"].get("resourceVersion")}
    return evidence, context


# ---- Kubernetes mutations -------------------------------------------------------------------------------------

def replace_object(fixture, obj: dict) -> None:
    """One `kubectl replace` with the object read earlier: resourceVersion makes a stale write fail."""
    try:
        fixture.command(fixture.kubectl + ["replace", "-f", "-", "-o", "name"], stdin=json.dumps(obj))
    except (RuntimeError, subprocess.TimeoutExpired) as exc:
        raise MutationRejected("Kubernetes replace failed; application unknown") from exc


def delete_with_uid(fixture, resource_path: str, uid: str) -> None:
    """One DELETE with a UID precondition; the path is an API path such as /api/v1/namespaces/narumq/persistentvolumeclaims/x."""
    options = {"kind": "DeleteOptions", "apiVersion": "v1", "preconditions": {"uid": uid}}
    try:
        fixture.command(fixture.kubectl + ["delete", "--raw", resource_path, "-f", "-"], stdin=json.dumps(options))
    except (RuntimeError, subprocess.TimeoutExpired) as exc:
        raise MutationRejected("Kubernetes delete failed; application unknown") from exc


def set_unschedulable(fixture, node_name: str, uid: str, value: bool) -> None:
    fixture.patch_node(node_name, [{"op": "test", "path": "/metadata/uid", "value": uid},
                                   {"op": "add", "path": "/spec/unschedulable", "value": value}])


# ---- waiting --------------------------------------------------------------------------------------------------

def wait_until(probe: Callable, timeout: float, interval: float = 3.0, sleep: Callable = time.sleep,
               monotonic: Callable = time.monotonic):
    """Call `probe` until it returns a truthy value; raise TimeoutError with the last falsy result kept on the error."""
    deadline = monotonic() + timeout
    last = None
    while True:
        last = probe()
        if last:
            return last
        if monotonic() >= deadline:
            error = TimeoutError("condition not met before deadline")
            error.last = last
            raise error
        sleep(interval)


# ---- guard ----------------------------------------------------------------------------------------------------

def container_status(pod: dict | None, name: str = CONTAINER) -> dict | None:
    if not pod:
        return None
    return next((s for s in pod.get("status", {}).get("containerStatuses", []) if s.get("name") == name), None)


def classify_guard(status: dict | None, logs: str | None) -> dict:
    """Judge the refused start: the broker container must have ended with exit code 1 and the refusal message.

    `waiting` = no terminated state seen yet; any other exit code, or a Ready container, fails.
    """
    if status is None:
        return {"verdict": "waiting", "reason": "container_status_absent"}
    result = {"restartCount": status.get("restartCount"), "ready": bool(status.get("ready"))}
    terminated = (status.get("state") or {}).get("terminated") or (status.get("lastState") or {}).get("terminated")
    if result["ready"]:
        return {**result, "verdict": "fail", "reason": "container_became_ready"}
    if not terminated:
        return {**result, "verdict": "waiting", "reason": "not_terminated"}
    result.update(exitCode=terminated.get("exitCode"), terminationReason=terminated.get("reason"),
                  finishedAt=terminated.get("finishedAt"))
    if terminated.get("exitCode") != 1:
        return {**result, "verdict": "fail", "reason": "unexpected_exit_code"}
    if logs is None:
        return {**result, "verdict": "waiting", "reason": "logs_unavailable"}
    line = next((entry for entry in logs.splitlines() if GUARD_MESSAGE in entry), None)
    result.update(logSha256=digest(logs), logMatched=line is not None)
    if line is None:
        return {**result, "verdict": "fail", "reason": "refusal_message_absent"}
    return {**result, "verdict": "refused", "reason": "exit_1_with_refusal", "logLine": line.strip()[:300]}


def fetch_logs(fixture, pod_name: str, restarted: bool) -> str | None:
    """Container log through kubectl; `--previous` after a restart. Failures return None."""
    previous = [["--previous"]] if restarted else []
    for extra in previous + [[]]:
        try:
            return fixture.command(fixture.kubectl + ["-n", NAMESPACE, "logs", pod_name, "-c", CONTAINER, *extra],
                                   timeout=60)
        except (RuntimeError, subprocess.TimeoutExpired):
            continue
    return None


def pod_claims(pod: dict) -> list[str]:
    """PersistentVolumeClaim names the Pod mounts."""
    return [volume["persistentVolumeClaim"]["claimName"] for volume in pod.get("spec", {}).get("volumes", [])
            if volume.get("persistentVolumeClaim")]


def watch_guard(fixture, plan: dict, timeout: float = 600, interval: float = 3.0, sleep: Callable = time.sleep,
                monotonic: Callable = time.monotonic) -> dict:
    """Watch the recreated slot Pod until the broker container ends. Observation only; no mutation.

    Returns the classification with the Pod identity, restart count and a compact observation list.
    `verdict` is `refused` (exit 1 + message), `fail` (other exit code, message absent, Ready) or `timeout`.
    """
    deadline = monotonic() + timeout
    observations: list[dict] = []
    last_key = None
    while True:
        pod = fixture.get_pod(plan["targetPod"])
        result = None
        if pod and pod["metadata"]["uid"] != plan["oldPodUID"] and not pod["metadata"].get("deletionTimestamp"):
            status = container_status(pod)
            ended = status is not None and bool((status.get("state") or {}).get("terminated")
                                                or (status.get("lastState") or {}).get("terminated"))
            restarted = ended and (status.get("restartCount") or 0) > 0
            logs = fetch_logs(fixture, plan["targetPod"], restarted) if ended else None
            verdict = classify_guard(status, logs)
            key = (pod["metadata"]["uid"], verdict["verdict"], verdict["reason"], verdict.get("restartCount"))
            if key != last_key:
                observations.append({"at": timestamp(), "podUID": pod["metadata"]["uid"],
                                     "nodeName": pod["spec"].get("nodeName"), "phase": pod.get("status", {}).get("phase"),
                                     "verdict": verdict["verdict"], "reason": verdict["reason"],
                                     "restartCount": verdict.get("restartCount")})
                last_key = key
            if verdict["verdict"] in ("refused", "fail"):
                result = {**verdict, "podUID": pod["metadata"]["uid"], "nodeName": pod["spec"].get("nodeName"),
                          "claims": pod_claims(pod)}
        if result is not None:
            return {**result, "observations": observations}
        if monotonic() >= deadline:
            return {"verdict": "timeout", "reason": "no_terminated_container_before_deadline",
                    "observations": observations}
        sleep(interval)


# ---- monitor windows and journal ------------------------------------------------------------------------------

def _instant(value: str) -> datetime.datetime:
    return datetime.datetime.fromisoformat(value)


def window_judgment(samples: dict, start_iso: str, end_iso: str) -> dict:
    """All survivor samples that started inside [start, end] must be ok; quorum true and PUBACK success."""
    start, end = _instant(start_iso), _instant(end_iso)
    fields = {"management": "quorumAvailable", "mqtt": "puback"}
    streams = {}
    for name, field in fields.items():
        inside = [s for s in samples.get(name, []) if start <= _instant(s["startedAt"]) <= end]
        good = [s for s in inside if s.get("ok") and s.get("data", {}).get(field) is True]
        streams[name] = {"samples": len(inside), "successful": len(good), "failed": len(inside) - len(good)}
    passed = all(item["samples"] > 0 and item["failed"] == 0 for item in streams.values())
    return {"result": "PASS" if passed else "FAIL", "from": start_iso, "to": end_iso, "streams": streams}


def journal_counts(entries: list[dict], start_iso: str | None = None, end_iso: str | None = None) -> dict:
    selected = entries
    if start_iso is not None and end_iso is not None:
        start, end = _instant(start_iso), _instant(end_iso)
        selected = [e for e in entries if start <= _instant(e["sentAt"]) <= end]
    return {name: sum(e["outcome"] == name for e in selected) for name in ("acked", "rejected", "unknown")}


# ---- membership operations and stage body ---------------------------------------------------------------------

def original_certificate_sha256(old_config: dict, member_id: str) -> str:
    """The lost member's committed certificate fingerprint, from the bootstrap members of its own config."""
    for member in old_config.get("bootstrapMembers", []):
        if member.get("memberId") == int(member_id):
            return member["certificateSha256"]
    reject("old_certificate_fingerprint_missing")


def stage_body(plan: dict, member_id: str, certificate_sha256: str, context: dict) -> dict:
    profile = context["oldConfig"]
    return {"memberId": int(member_id), "address": slot_address(plan["slotOrdinal"]),
            "dnsSan": slot_dns(plan["slotOrdinal"]), "certificateSha256": certificate_sha256,
            "releaseVersion": context["releaseVersion"],
            "clusterProtocolMajor": profile["clusterProtocolMajor"], "clusterProtocolMinor": profile["clusterProtocolMinor"],
            "stateSchemaMajor": profile["stateSchemaMajor"], "stateSchemaMinor": profile["stateSchemaMinor"]}


def idempotency_key(run_id: str, action: str, member_id: str) -> str:
    """Printable ASCII without spaces, at most 128 bytes."""
    key = f"{run_id}-{action}-{member_id}"
    return key if len(key) <= 128 and re.fullmatch(r"[!-~]+", key) else f"{action}-{member_id}-{digest(run_id)[:32]}"


def error_code(envelope: dict | None) -> str | None:
    error = (envelope or {}).get("error")
    return error.get("code") if isinstance(error, dict) else None


def operation_summary(envelope: dict | None) -> dict:
    data = (envelope or {}).get("data")
    if not isinstance(data, dict):
        return {}
    return {key: data.get(key) for key in ("operationId", "action", "memberId", "phase", "committedIndex")}


# ---- judgement ------------------------------------------------------------------------------------------------

CRITERIA = (
    "loss.instanceTerminated", "loss.volumeDeleted", "loss.pvcRecreated",
    "guard.exit1AndRefusalMessage", "guard.survivorSamplesAllOk", "guard.twoReadyBrokers",
    "reseed.newIdVoter", "reseed.oldIdRetired", "reseed.threeReadyAndQuorum", "reseed.oldIdStage409MemberIdReserved",
    "durable.appliedIndexAtLeastBaseline", "durable.streamOracle", "durable.retainedMarkerSha256",
)
# Added when the plan has `largeSnapshot` (R1): the snapshot stream carries more than 1 MiB to the new member.
LARGE_CRITERIA = ("stream.featureActive", "stream.largeRetainedWritten", "stream.logCompletedOverLimit",
                  "stream.receiverLogged", "stream.retainedReadableOnNewMember")


def criteria_for(plan: dict | None) -> tuple[str, ...]:
    return CRITERIA + (LARGE_CRITERIA if plan and plan.get("largeSnapshot") else ())


def aggregate(criteria: dict, failed_step: str | None = None, plan: dict | None = None) -> dict:
    """PASS only when every named criterion is exactly True and no step failed."""
    verdicts = {name: criteria.get(name) is True for name in criteria_for(plan)}
    failed = [name for name, ok in verdicts.items() if not ok]
    passed = not failed and failed_step is None
    return {"result": "PASS" if passed else "FAIL", "criteria": verdicts, "failedCriteria": failed,
            "failedStep": failed_step}


# ---- local tunnel ports ---------------------------------------------------------------------------------------

def tunnel_ports() -> list[int]:
    return [*OBSERVER_PORTS, *TARGET_PORTS, *LEADER_PORTS]


def check_local_ports(ports: list[int] | None = None) -> list[int]:
    """Bind every local tunnel port on 127.0.0.1 and release it at once; any failure rejects the run.

    Windows can exclude whole port ranges ("access forbidden"), so this runs before any mutation.
    """
    checked = list(tunnel_ports() if ports is None else ports)
    for port in checked:
        probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            probe.bind(("127.0.0.1", port))
        except OSError:
            reject(f"local_port_unavailable:{port}")
        finally:
            probe.close()
    return checked


# ---- resume ---------------------------------------------------------------------------------------------------

RESUME_FROM_STEPS = ("reseed", "new-pod", "verify")
# Third-party modules any mode needs (MQTT client for the durable oracle and marker, certificate generation).
REQUIRED_MODULES = ("paho.mqtt.client", "paho.mqtt.packettypes", "paho.mqtt.properties", "cryptography.x509",
                    "cryptography.hazmat.primitives.asymmetric.ec")


def check_dependencies(modules: tuple[str, ...] | None = None, importer: Callable | None = None) -> list[str]:
    """Import every third-party module the run needs before any live call; a failure is `dependency_missing:<module>`."""
    import importlib
    importer = importer or importlib.import_module
    checked = list(REQUIRED_MODULES if modules is None else modules)
    for module in checked:
        try:
            importer(module)
        except ImportError as exc:
            reject(f"dependency_missing:{module}", type(exc).__name__)
    return checked


CARRIED_SOURCES = {"loss": "reseed-loss.json", "guard": "reseed-guard.json"}
# Records written by the step that must not exist yet: they would mean `reseed` already mutated something.
RESEED_MUTATION_KEYS = ("stageObserved", "configMapReseed", "secretReseed", "podDelete", "newPod", "newVoter",
                        "removeRequest")


def mismatch(field: str, detail: str = "") -> None:
    reject(f"resume_state_mismatch:{field}", detail)


def file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def hash_records(directory: Path, skip: Path | None = None, recursive: bool = True) -> dict[str, str]:
    """SHA-256 of every file below `directory` (keys are POSIX paths relative to it), except below `skip`.

    `recursive=False` hashes only the files directly inside, i.e. the original run's own records.
    """
    files = directory.rglob("*") if recursive else directory.iterdir()
    return {item.relative_to(directory).as_posix(): file_sha256(item) for item in sorted(files)
            if item.is_file() and not (skip is not None and skip in item.parents)}


def resume_dirs(original: Path) -> list[Path]:
    """`resume-<n>` directories below the original run directory, oldest first."""
    found = [(int(item.name.split("-", 1)[1]), item) for item in original.glob("resume-*")
             if item.is_dir() and re.fullmatch(r"resume-\d+", item.name)]
    return [item for _, item in sorted(found)]


def read_record(path: Path) -> dict | None:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return value if isinstance(value, dict) else None


def check_no_stage_attempt(original: Path) -> None:
    """`--from reseed` stages with a fresh certificate; an earlier stage request under the same idempotency key
    would be replayed with a different body, so any earlier resume that sent one forbids it."""
    for directory in resume_dirs(original):
        record = read_record(directory / "reseed-reseed.json")
        if record is not None and "stageRequest" in record:
            mismatch("stage_already_attempted", directory.name)


def committed_stage_source(original: Path) -> tuple[Path, dict] | None:
    """Latest resume whose `reseed` record shows a committed stage (202/200 with operationId), the applied
    ConfigMap and Secret, and the slot Pod delete. Resumes that only continued from it do not qualify."""
    for directory in reversed(resume_dirs(original)):
        record = read_record(directory / "reseed-reseed.json")
        if record is None:
            continue
        response = record.get("stageResponse") or {}
        delete = record.get("podDelete")
        sha = (record.get("certificate") or {}).get("sha256")
        if (response.get("httpStatus") in (200, 202) and isinstance(response.get("operationId"), str)
                and response["operationId"]
                and (record.get("configMapReseed") or {}).get("appliedAfterFreshRead") is True
                and (record.get("secretReseed") or {}).get("appliedAfterFreshRead") is True
                and isinstance(delete, dict) and isinstance(delete.get("podUID"), str) and delete["podUID"]
                and (not delete.get("commandFailed") or delete.get("appliedAfterFreshRead") is True)
                and isinstance(sha, str) and re.fullmatch(r"[0-9a-f]{64}", sha)):
            return directory, record
    return None


def load_original_records(original: Path) -> dict[str, dict]:
    records = {}
    for name in ("summary", "reseed-loss", "reseed-baseline", "reseed-guard", "reseed-reseed"):
        path = original / f"{name}.json"
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            mismatch(f"records.{name}")
        if not isinstance(value, dict):
            mismatch(f"records.{name}")
        records[name] = value
    return records


def carried_criteria(records: dict[str, dict]) -> dict[str, dict]:
    """loss.* and guard.* criteria that the interrupted run already proved, with their source file."""
    carried = {}
    for name, value in records["summary"].get("criteria", {}).items():
        source = CARRIED_SOURCES.get(name.split(".", 1)[0])
        if source is not None:
            carried[name] = {"value": value is True, "source": f"../{source}"}
    return carried


def validate_records(records: dict[str, dict], plan: dict) -> dict:
    """Checks of the interrupted run's own records. Returns values the resume continues from."""
    summary, loss, baseline, guard, stage = (records[k] for k in ("summary", "reseed-loss", "reseed-baseline",
                                                                  "reseed-guard", "reseed-reseed"))
    if summary.get("runId") != plan["runId"] or summary.get("failedStep") != "reseed":
        mismatch("summary.failedStep")
    carried = carried_criteria(records)
    for name in ("loss.instanceTerminated", "loss.volumeDeleted", "loss.pvcRecreated",
                 "guard.exit1AndRefusalMessage", "guard.survivorSamplesAllOk", "guard.twoReadyBrokers"):
        if not carried.get(name, {}).get("value"):
            mismatch(f"summary.criteria.{name}")
    new_pvc_uid = (loss.get("pvcReplaced") or {}).get("pvcUID")
    if not isinstance(new_pvc_uid, str) or not new_pvc_uid or new_pvc_uid == plan["pvcUID"]:
        mismatch("loss.newPvcUID")
    fresh = guard.get("freshPvc") or {}
    if guard.get("verdict") != "refused" or fresh.get("uid") != new_pvc_uid:
        mismatch("guard.record")
    applied = (baseline.get("baseline") or {}).get("appliedIndexLowerBound")
    marker = (baseline.get("marker") or {}).get("sha256")
    if isinstance(applied, bool) or not isinstance(applied, int) or applied < 0:
        mismatch("baseline.appliedIndex")
    if not isinstance(marker, str) or not re.fullmatch(r"[0-9a-f]{64}", marker) \
            or (baseline.get("markerOnTarget") or {}).get("sha256") != marker:
        mismatch("baseline.marker")
    key = (stage.get("stageRequest") or {}).get("idempotencyKey")
    if key != idempotency_key(plan["runId"], "stage", plan["newNodeId"]):
        mismatch("reseed.stageKey")
    if "httpStatus" in (stage.get("stageResponse") or {}) or any(name in stage for name in RESEED_MUTATION_KEYS):
        mismatch("reseed.alreadyMutated")
    return {"newPvcUID": new_pvc_uid, "appliedIndex": applied, "markerSha256": marker,
            "stageIdempotencyKey": key, "carried": carried}


def certificate_sha256_of_secret_value(value: str) -> str:
    from cryptography import x509
    from cryptography.hazmat.primitives import serialization
    pem = base64.b64decode(value, validate=True)
    return hashlib.sha256(x509.load_pem_x509_certificate(pem).public_bytes(serialization.Encoding.DER)).hexdigest()


def check_ownership(fixture, plan: dict, port: int) -> dict:
    """Account, EKS VPC, namespace run label, cluster id and quorum; returns the cluster summary."""
    if fixture.aws("sts", "get-caller-identity")["Account"] != ACCOUNT:
        reject("account_mismatch")
    if plan.get("largeSnapshot") is not None:
        require_owned_fixture(fixture, plan, [plan["oldInstanceId"], plan["observerInstanceId"], plan["spareInstanceId"]])
    eks = fixture.aws("eks", "describe-cluster", "--name", plan["clusterName"])["cluster"]
    if eks["resourcesVpcConfig"]["vpcId"] != plan["vpcId"]:
        reject("cluster_vpc_mismatch")
    namespace = fixture.get("namespace", NAMESPACE)
    if namespace["metadata"].get("labels", {}).get("narumq.io/acceptance-run") != plan["runId"]:
        reject("namespace_not_owned")
    cluster = fixture.api("/api/v1/cluster", port)
    if cluster["clusterId"] != plan["runId"] or cluster["quorumAvailable"] is not True:
        reject("cluster_not_owned_or_no_quorum")
    return cluster


def pods_list(fixture) -> list[dict]:
    return fixture.get("pods", "-n", NAMESPACE, "-l", "app=narumq")["items"]


def resume_validate(fixture, plan: dict, records: dict[str, dict], private: Path, port: int = OBSERVER_PORTS[0],
                    logs: Callable | None = None) -> tuple[dict, dict]:
    """Read-only check that the live fixture is exactly the state the interrupted run stopped in.

    Every failure is `resume_state_mismatch:<field>` and happens before any mutation.
    Returns (evidence, context); the context also carries `baseline` and `carried` recovered from the records.
    """
    logs = logs or fetch_logs
    ordinal, old, new = plan["slotOrdinal"], plan["targetNodeId"], plan["newNodeId"]
    recovered = validate_records(records, plan)
    try:
        cluster = check_ownership(fixture, plan, port)
    except PreflightRejected as exc:
        mismatch(f"ownership.{exc.code}")
    rows = node_rows(fixture.api("/api/v1/nodes?limit=1000", port))
    configmap = fixture.get("configmap", CONFIGMAP, "-n", NAMESPACE)
    try:
        slots = parse_slots(configmap["data"])
    except PreflightRejected as exc:
        mismatch(f"configmap.{exc.code}")
    if slots.get(ordinal) != old:
        mismatch("configmap.slot")
    if f"node-{new}.json" in configmap["data"]:
        mismatch("configmap.joinerConfigPresent")
    old_text = configmap["data"].get(f"node-{old}.json")
    if old_text is None:
        mismatch("configmap.oldConfig")
    survivors = [node_id for slot, node_id in sorted(slots.items()) if slot != ordinal]
    active = {node_id for node_id, row in rows.items() if row.get("membershipPhase") != "RETIRED"}
    if active != set(slots.values()):
        mismatch("members.active")
    if new in rows:
        mismatch("members.newIdPresent")
    if int(new) <= max(int(node_id) for node_id in rows):
        mismatch("members.newIdNotAboveCommitted")
    if (rows[old].get("membershipPhase") != "VOTER"
            or any(rows[node_id].get("membershipPhase") != "VOTER" or rows[node_id].get("queryStatus") != "OK"
                   for node_id in survivors)):
        mismatch("members.phases")
    leader = cluster.get("leaderNodeId")
    if str(leader) not in survivors:
        mismatch("members.leader")
    try:
        old_config = json.loads(old_text)
        joiner = derive_joiner_config(old_config, plan, slots)
    except (ValueError, PreflightRejected) as exc:
        mismatch("configmap.oldConfig", getattr(exc, "code", type(exc).__name__))
    for key in ("clusterProtocolMajor", "clusterProtocolMinor", "stateSchemaMajor", "stateSchemaMinor"):
        if not isinstance(old_config.get(key), int) or isinstance(old_config.get(key), bool):
            mismatch("configmap.oldConfigProfile", key)
    try:
        version = release_version(fixture.api("/api/v1/cluster/features", port), survivors)
    except PreflightRejected as exc:
        mismatch(f"features.{exc.code}")
    pods = {p["metadata"]["name"]: p for p in pods_list(fixture)}
    if any(f"narumq-{other}" not in pods or not ready(pods[f"narumq-{other}"])
           or pods[f"narumq-{other}"]["metadata"].get("deletionTimestamp")
           for other in ORDINALS if other != ordinal):
        mismatch("pods.survivorsReady")
    guard_pod = pods.get(plan["targetPod"])
    if guard_pod is None or guard_pod["metadata"].get("deletionTimestamp"):
        mismatch("pods.guardPod")
    pvc = fixture.get("pvc", plan["pvcName"], "-n", NAMESPACE, "--ignore-not-found")
    if not pvc or pvc["metadata"]["uid"] != recovered["newPvcUID"] or pvc["metadata"].get("deletionTimestamp"):
        mismatch("pvc.uid")
    if plan["pvcName"] not in pod_claims(guard_pod):
        mismatch("pods.guardPodClaim")
    status = container_status(guard_pod)
    ended = status is not None and bool((status.get("state") or {}).get("terminated")
                                        or (status.get("lastState") or {}).get("terminated"))
    restarted = status is not None and (status.get("restartCount") or 0) > 0
    verdict = classify_guard(status, logs(fixture, plan["targetPod"], restarted) if ended else None)
    if verdict["verdict"] != "refused":
        mismatch("pods.guardRefusal", verdict["reason"])
    spare = fixture.get_node(plan["spareWorkerName"])
    if not spare or instance_of(spare) != plan["spareInstanceId"] or not spare.get("metadata", {}).get("uid"):
        mismatch("spare.node")
    secret = fixture.get("secret", SECRET, "-n", NAMESPACE)
    crt_key, key_key = f"node-{ordinal}.crt", f"node-{ordinal}.key"
    if crt_key not in secret["data"] or key_key not in secret["data"]:
        mismatch("secret.slotKeys")
    snapshot_path = private / f"reseed-{plan['runId']}-slot{ordinal}-rollback-secret.json"
    snapshot = None
    if snapshot_path.exists():
        snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
        if any(secret["data"][name] != snapshot.get(name) for name in (crt_key, key_key)):
            mismatch("secret.slotCertificate")
    try:
        slot_sha = certificate_sha256_of_secret_value(secret["data"][crt_key])
    except Exception:
        mismatch("secret.slotCertificate", "unparseable")
    try:
        bootstrap_sha = original_certificate_sha256(old_config, old)
    except PreflightRejected:
        bootstrap_sha = None
    first_run = sorted(item.name for item in private.glob(f"reseed-{plan['runId']}-slot{ordinal}-*")
                       if item.is_file())
    evidence = {"at": timestamp(), "recovered": {"newPvcUID": recovered["newPvcUID"],
                                                 "appliedIndexLowerBound": recovered["appliedIndex"],
                                                 "markerSha256": recovered["markerSha256"],
                                                 "stageIdempotencyKey": recovered["stageIdempotencyKey"]},
                "members": {"voters": sorted(active, key=int), "leaderNodeId": str(leader),
                            "newIdAbsent": True},
                "stageOperationLookup": "operations cannot be listed; the new id is absent from /api/v1/nodes and the "
                                        "stage re-POST uses the original idempotency key (response status recorded)",
                "slots": {str(slot): node_id for slot, node_id in sorted(slots.items())},
                "guardPod": {"podUID": guard_pod["metadata"]["uid"], "restartCount": verdict.get("restartCount"),
                             "exitCode": verdict.get("exitCode"), "logSha256": verdict.get("logSha256"),
                             "podUIDDiffersFromGuardRecord": guard_pod["metadata"]["uid"]
                             != records["reseed-guard"].get("podUID")},
                "secret": {"slotCertificateSha256": slot_sha, "rollbackSnapshotPresent": snapshot is not None,
                           "matchesBootstrapConfig": slot_sha == bootstrap_sha},
                "spare": {"nodeName": plan["spareWorkerName"], "nodeUID": spare["metadata"]["uid"]},
                "releaseVersion": version, "joinerSeedAddresses": joiner["seedAddresses"],
                "firstRunPrivateFiles": first_run,
                "firstRunPrivateFilesNote": "unused certificate material from the interrupted run stays in private"}
    context = {"slots": slots, "survivors": survivors, "oldConfig": old_config, "joinerConfig": joiner,
               "releaseVersion": version, "spareNodeUID": spare["metadata"]["uid"],
               "guardPodUID": guard_pod["metadata"]["uid"], "newPvcUID": recovered["newPvcUID"],
               "baseline": {"appliedIndex": recovered["appliedIndex"], "markerSha256": recovered["markerSha256"]},
               "carried": recovered["carried"]}
    return evidence, context


OPERATION_ENDED_PHASES = ("FAILED", "CANCELLED", "ABORTED")


def resume_validate_new_pod(fixture, plan: dict, records: dict[str, dict], original: Path,
                            port: int = OBSERVER_PORTS[0]) -> tuple[dict, dict]:
    """Read-only check for `resume --from new-pod`: the stage is committed, ConfigMap and Secret carry the new id and
    certificate, and only the new slot Pod (and what follows) is left. Failures are `resume_state_mismatch:<field>`."""
    ordinal, old, new = plan["slotOrdinal"], plan["targetNodeId"], plan["newNodeId"]
    recovered = validate_records(records, plan)
    source = committed_stage_source(original)
    if source is None:
        mismatch("resume.source")
    source_dir, source_record = source
    operation_id = source_record["stageResponse"]["operationId"]
    certificate_sha = source_record["certificate"]["sha256"]
    guard_uid = source_record["podDelete"]["podUID"]
    try:
        cluster = check_ownership(fixture, plan, port)
    except PreflightRejected as exc:
        mismatch(f"ownership.{exc.code}")
    rows = node_rows(fixture.api("/api/v1/nodes?limit=1000", port))
    configmap = fixture.get("configmap", CONFIGMAP, "-n", NAMESPACE)
    try:
        slots = parse_slots(configmap["data"])
    except PreflightRejected as exc:
        mismatch(f"configmap.{exc.code}")
    if slots.get(ordinal) != new:
        mismatch("configmap.slot")
    old_text, joiner_text = configmap["data"].get(f"node-{old}.json"), configmap["data"].get(f"node-{new}.json")
    if old_text is None:
        mismatch("configmap.oldConfig")
    if joiner_text is None:
        mismatch("configmap.joinerConfig")
    try:
        old_config, joiner = json.loads(old_text), json.loads(joiner_text)
    except ValueError:
        mismatch("configmap.unparseable")
    if not isinstance(joiner, dict) or joiner.get("nodeId") != int(new) or joiner.get("role") != "joiner":
        mismatch("configmap.joinerConfig", "nodeId or role")
    survivors = [node_id for slot, node_id in sorted(slots.items()) if slot != ordinal]
    secret = fixture.get("secret", SECRET, "-n", NAMESPACE)
    crt_key = f"node-{ordinal}.crt"
    try:
        slot_sha = certificate_sha256_of_secret_value(secret["data"][crt_key])
    except Exception:
        mismatch("secret.slotCertificate", "missing or unparseable")
    if slot_sha != certificate_sha:
        mismatch("secret.slotCertificate", "differs from the certificate recorded by the resume that applied it")
    status, envelope = fixture.api_request("GET", f"/api/v1/cluster/membership-operations/{operation_id}", port=port)
    stage = operation_summary(envelope) if status == 200 else {}
    if (status != 200 or stage.get("operationId") != operation_id or stage.get("memberId") != int(new)
            or stage.get("action") != "ADD" or stage.get("phase") in OPERATION_ENDED_PHASES or not stage.get("phase")):
        mismatch("stage.operation", f"httpStatus {status}")
    if (rows.get(old) or {}).get("membershipPhase") != "VOTER":
        mismatch("members.oldVoter")
    if any((rows.get(node_id) or {}).get("membershipPhase") != "VOTER" for node_id in survivors):
        mismatch("members.survivorVoters")
    leader = cluster.get("leaderNodeId")
    if str(leader) not in survivors:
        mismatch("members.leader")
    try:
        version = release_version(fixture.api("/api/v1/cluster/features", port), survivors)
    except PreflightRejected as exc:
        mismatch(f"features.{exc.code}")
    evidence = {"at": timestamp(),
                "source": {"directory": source_dir.name, "operationId": operation_id, "podDeletedUID": guard_uid,
                           "certificateSha256": certificate_sha},
                "recovered": {"newPvcUID": recovered["newPvcUID"], "appliedIndexLowerBound": recovered["appliedIndex"],
                              "markerSha256": recovered["markerSha256"]},
                "slots": {str(slot): node_id for slot, node_id in sorted(slots.items())},
                "secret": {"slotCertificateSha256": slot_sha, "matchesRecordedCertificate": True,
                           "certificateGenerated": False},
                "stageOperation": {key: stage.get(key) for key in ("operationId", "memberId", "action", "phase")},
                "members": {"oldPhase": rows[old]["membershipPhase"], "survivors": survivors,
                            "newRow": rows.get(new), "leaderNodeId": str(leader)},
                "releaseVersion": version, "stageReposted": False}
    context = {"slots": slots, "survivors": survivors, "oldConfig": old_config, "joinerConfig": joiner,
               "releaseVersion": version, "guardPodUID": guard_uid, "newPvcUID": recovered["newPvcUID"],
               "operationId": operation_id, "certificateSha256": certificate_sha,
               "baseline": {"appliedIndex": recovered["appliedIndex"], "markerSha256": recovered["markerSha256"]},
               "carried": recovered["carried"]}
    return evidence, context


# ---- resume --from verify -------------------------------------------------------------------------------------

def verify_source(original: Path) -> tuple[Path, dict] | None:
    """Latest resume whose `reseed` record finished the whole reseed: new voter, stage SUCCEEDED, old id retired
    and observed, and the lost id's ConfigMap entry removed."""
    for directory in reversed(resume_dirs(original)):
        record = read_record(directory / "reseed-reseed.json")
        if (record is not None and isinstance(record.get("newVoter"), dict) and isinstance(record.get("stageSucceeded"), dict)
                and isinstance(record.get("removeObserved"), dict)
                and (record.get("configMapCleanup") or {}).get("appliedAfterFreshRead") is True):
            return directory, record
    return None


def verify_attempt_dirs(original: Path, current: Path | None) -> list[Path]:
    """The original directory and every resume directory except the one being written."""
    return [original] + [item for item in resume_dirs(original) if current is None or item != current]


def check_durable_session_untouched(original: Path, scenario: str, current: Path | None = None) -> list[str]:
    """An earlier verify that connected the persistent subscriber may have consumed the queued messages.

    Any `durable-verify-<scenario>-*.json` with a SessionPresent value or received messages (or one that cannot be
    read) rejects the run. Returns the files that were checked, all untouched."""
    checked = []
    for directory in verify_attempt_dirs(original, current):
        for path in sorted(directory.glob(f"durable-verify-{scenario}-*.json")):
            record = read_record(path)
            distinct = None if record is None else record.get("receivedDistinct", 0)
            untouched = (record is not None and record.get("sessionPresent") is None
                         and isinstance(distinct, int) and not isinstance(distinct, bool) and distinct == 0)
            checked.append(f"{directory.name}/{path.name}")
            if not untouched:
                mismatch("durable_session_touched", f"{directory.name}/{path.name}")
    return checked


def carried_restage(original: Path, current: Path | None) -> dict | None:
    """The old-id restage record of an earlier verify attempt when it already got 409 member_id_reserved, so the
    POST under the same idempotency key is not sent a second time. Newest attempt first."""
    for directory in reversed(verify_attempt_dirs(original, current)):
        record = read_record(directory / "reseed-verify.json")
        restage = (record or {}).get("restageOldId")
        if isinstance(restage, dict) and restage.get("httpStatus") == 409 \
                and restage.get("errorCode") == "member_id_reserved":
            return {**restage, "carriedFrom": restage.get("carriedFrom") or f"{directory.name}/reseed-verify.json"}
    return None


def resume_validate_verify(fixture, plan: dict, records: dict[str, dict], original: Path,
                           current: Path | None = None, port: int = OBSERVER_PORTS[0]) -> tuple[dict, dict]:
    """Read-only check for `resume --from verify`: the reseed finished and nothing consumed the durable session.
    Failures are `resume_state_mismatch:<field>`, before any mutation."""
    ordinal, old, new = plan["slotOrdinal"], plan["targetNodeId"], plan["newNodeId"]
    recovered = validate_records(records, plan)
    source = verify_source(original)
    if source is None:
        mismatch("resume.source")
    source_dir, source_record = source
    guard_uid = (source_record.get("continuedFrom") or {}).get("deletedPodUID") \
        or (source_record.get("podDelete") or {}).get("podUID")
    if not isinstance(guard_uid, str) or not guard_uid:
        mismatch("resume.guardPodUID")
    untouched = check_durable_session_untouched(original, plan["durableScenario"], current)
    carried_record = carried_restage(original, current)
    try:
        cluster = check_ownership(fixture, plan, port)
    except PreflightRejected as exc:
        mismatch(f"ownership.{exc.code}")
    rows = node_rows(fixture.api("/api/v1/nodes?limit=1000", port))
    configmap = fixture.get("configmap", CONFIGMAP, "-n", NAMESPACE)
    try:
        slots = parse_slots(configmap["data"])
    except PreflightRejected as exc:
        mismatch(f"configmap.{exc.code}")
    if slots.get(ordinal) != new:
        mismatch("configmap.slot")
    if f"node-{old}.json" in configmap["data"]:
        mismatch("configmap.oldConfigPresent")
    try:
        joiner = json.loads(configmap["data"][f"node-{new}.json"])
    except (KeyError, ValueError):
        mismatch("configmap.joinerConfig")
    if not isinstance(joiner, dict) or joiner.get("nodeId") != int(new):
        mismatch("configmap.joinerConfig", "nodeId")
    survivors = [node_id for slot, node_id in sorted(slots.items()) if slot != ordinal]
    voters = {node_id for node_id, row in rows.items() if row.get("membershipPhase") == "VOTER"}
    active = {node_id for node_id, row in rows.items() if row.get("membershipPhase") != "RETIRED"}
    if voters != set(survivors) | {new} or active != voters:
        mismatch("members.voters")
    if (rows.get(old) or {}).get("membershipPhase") != "RETIRED":
        mismatch("members.oldRetired")
    if str(cluster.get("leaderNodeId")) not in survivors + [new]:
        mismatch("members.leader")
    pod = fixture.get_pod(plan["targetPod"])
    if (pod is None or pod["metadata"].get("deletionTimestamp") or not ready(pod)
            or pod["metadata"]["uid"] == guard_uid):
        mismatch("pods.newPod", "missing, deleting, not Ready or still the deleted guard Pod")
    # The old-id restage in verify needs the lost member's committed certificate and profile; node-<old>.json is
    # gone, so they are rebuilt from a survivor's bootstrap config (which lists all three members).
    old_config = None
    try:
        base = json.loads(configmap["data"][f"node-{survivors[0]}.json"])
        old_config = {**base, "nodeId": int(old)}
        original_certificate_sha256(old_config, old)
        version = release_version(fixture.api("/api/v1/cluster/features", port), survivors)
    except (KeyError, ValueError, PreflightRejected):
        old_config, version = None, None
    if old_config is None and carried_record is None:
        mismatch("restage.oldCertificate", "no earlier restage record and the lost member's certificate is not recoverable")
    if old_config is None:
        old_config = {key: joiner[key] for key in ("clusterProtocolMajor", "clusterProtocolMinor",
                                                   "stateSchemaMajor", "stateSchemaMinor") if key in joiner}
    evidence = {"at": timestamp(),
                "source": {"directory": source_dir.name, "podDeletedUID": guard_uid},
                "recovered": {"newPvcUID": recovered["newPvcUID"], "appliedIndexLowerBound": recovered["appliedIndex"],
                              "markerSha256": recovered["markerSha256"]},
                "durableSession": {"untouched": True, "verifyRecordsChecked": untouched},
                "slots": {str(slot): node_id for slot, node_id in sorted(slots.items())},
                "members": {"voters": sorted(voters, key=int), "oldPhase": "RETIRED", "leaderNodeId": str(cluster.get("leaderNodeId"))},
                "newPod": {"podUID": pod["metadata"]["uid"], "differsFromDeletedGuardPod": True},
                "restage": {"carriedFrom": carried_record["carriedFrom"] if carried_record else None,
                            "willPost": carried_record is None},
                "releaseVersion": version}
    context = {"slots": slots, "survivors": survivors, "oldConfig": old_config, "joinerConfig": joiner,
               "releaseVersion": version, "guardPodUID": guard_uid, "newPodUID": pod["metadata"]["uid"],
               "newPvcUID": recovered["newPvcUID"],
               "baseline": {"appliedIndex": recovered["appliedIndex"], "markerSha256": recovered["markerSha256"]},
               "carried": recovered["carried"]}
    return evidence, context


# ---- large snapshot (R1) --------------------------------------------------------------------------------------

SNAPSHOT_STREAM_FEATURE = "snapshot-stream"
# Cluster spec: retained values are limited to 1,000,000 bytes each and 16 MiB in total.
RETAINED_PAYLOAD_LIMIT = 1_000_000
RETAINED_TOTAL_LIMIT = 8_000_000
SNAPSHOT_STREAM_LIMIT_BYTES = 1_048_576
LARGE_TOPIC_PREFIX = "reseed/large/t"
STREAM_COMPLETED = "raft snapshot stream completed"
STREAM_FAILED = "raft snapshot stream failed"
STREAM_RECEIVED = "raft snapshot stream received"
LARGE_SNAPSHOT_KEYS = {"feature", "retainedTopics", "retainedPayloadBytes", "padEntries", "minStreamBytes",
                       "totalRetainedBytes"}


def large_snapshot_settings(plan: dict) -> dict | None:
    """Validated `largeSnapshot` options, or None when the plan asks for no R1 load. Reads nothing live."""
    raw = plan.get("largeSnapshot")
    if raw is None:
        return None
    if not isinstance(raw, dict) or not set(raw) <= LARGE_SNAPSHOT_KEYS:
        raise PlanError(f"largeSnapshot must be an object with keys from {sorted(LARGE_SNAPSHOT_KEYS)}")

    def integer(key: str, default: int, low: int, high: int) -> int:
        value = raw.get(key, default)
        if isinstance(value, bool) or not isinstance(value, int) or not low <= value <= high:
            raise PlanError(f"largeSnapshot.{key} must be an integer within [{low}, {high}]")
        return value

    feature = raw.get("feature", SNAPSHOT_STREAM_FEATURE)
    if feature != SNAPSHOT_STREAM_FEATURE:
        raise PlanError("largeSnapshot.feature must be snapshot-stream")
    settings = {"feature": feature,
                "retainedTopics": integer("retainedTopics", 8, 1, 64),
                "retainedPayloadBytes": integer("retainedPayloadBytes", 524_288, 1024, RETAINED_PAYLOAD_LIMIT),
                "padEntries": integer("padEntries", 1100, 1000, 5000),
                "minStreamBytes": integer("minStreamBytes", SNAPSHOT_STREAM_LIMIT_BYTES, SNAPSHOT_STREAM_LIMIT_BYTES,
                                          RETAINED_TOTAL_LIMIT)}
    total = settings["retainedTopics"] * settings["retainedPayloadBytes"]
    if total > RETAINED_TOTAL_LIMIT:
        raise PlanError(f"largeSnapshot retained load {total} bytes exceeds {RETAINED_TOTAL_LIMIT}")
    if total <= settings["minStreamBytes"]:
        raise PlanError("largeSnapshot retained load must exceed minStreamBytes, or the snapshot cannot exceed it")
    settings["totalRetainedBytes"] = total
    return settings


def large_topic(run_id: str, index: int) -> str:
    return f"acceptance/{run_id}/{LARGE_TOPIC_PREFIX}/{index:02d}"


def large_topic_filter(run_id: str) -> str:
    return f"acceptance/{run_id}/{LARGE_TOPIC_PREFIX}/+"


def large_pad_topic(run_id: str) -> str:
    return f"acceptance/{run_id}/reseed/large-pad"


def feature_state(data: dict | None) -> dict:
    """Normalised `/api/v1/cluster/features` data: active tokens and per-member advertised tokens."""
    data = data if isinstance(data, dict) else {}
    members = [{"nodeId": str(m.get("nodeId")), "releaseVersion": m.get("releaseVersion"),
                "features": [f for f in (m.get("features") or []) if isinstance(f, str)]}
               for m in data.get("members") or [] if isinstance(m, dict)]
    return {"activeFeatures": [f for f in (data.get("activeFeatures") or []) if isinstance(f, str)], "members": members}


def large_payload(run_id: str, index: int, size: int) -> bytes:
    """Deterministic, incompressible-looking payload of exactly `size` bytes; the hash alone proves the content."""
    return hashlib.shake_256(f"{run_id}:large:{index}".encode("ascii")).digest(size)


def parse_log_records(text: str | None) -> list[dict]:
    """Structured records of broker log text: JSON lines (slog JSON) or `key=value` lines (slog text)."""
    records: list[dict] = []
    for line in (text or "").splitlines():
        line = line.strip()
        if not line:
            continue
        if line.startswith("{"):
            try:
                value = json.loads(line)
            except ValueError:
                continue
            if isinstance(value, dict):
                records.append(value)
            continue
        pairs = re.findall(r'([A-Za-z_][A-Za-z0-9_]*)=("(?:[^"\\]|\\.)*"|\S*)', line)
        if pairs:
            record = {}
            for key, raw in pairs:
                record[key] = json.loads(raw) if raw.startswith('"') else raw
            records.append(record)
    return records


def _integer(value) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.isdigit():
        return int(value)
    return None


def stream_events(records: list[dict], message: str, peer_key: str, peer_id: str) -> list[dict]:
    """Records with exactly this message about this peer, reduced to the fields the judgement uses."""
    events = []
    for record in records:
        if record.get("msg") == message and str(record.get(peer_key)) == str(peer_id):
            events.append({"snapshotIndex": _integer(record.get("snapshot_index")),
                           "totalBytes": _integer(record.get("total_bytes")), "chunks": _integer(record.get("chunks")),
                           "reason": record.get("reason") if isinstance(record.get("reason"), str) else None})
    return events


def judge_stream_logs(sender_records: list[dict], receiver_records: list[dict], new_id: str, survivor_ids: list[str],
                      min_bytes: int) -> dict:
    """The stream must have completed for the new member above `min_bytes`, and the new member must log the receipt."""
    completed = stream_events(sender_records, STREAM_COMPLETED, "peer_id", new_id)
    failed = stream_events(sender_records, STREAM_FAILED, "peer_id", new_id)
    big = [event for event in completed if isinstance(event["totalBytes"], int) and event["totalBytes"] > min_bytes]
    received = [event for record in receiver_records if record.get("msg") == STREAM_RECEIVED
                and str(record.get("from_peer_id")) in {str(item) for item in survivor_ids}
                for event in [{"snapshotIndex": _integer(record.get("snapshot_index")),
                               "totalBytes": _integer(record.get("total_bytes"))}]]
    big_received = [event for event in received if isinstance(event["totalBytes"], int) and event["totalBytes"] > min_bytes]
    return {"completedOverLimit": bool(big), "receiverLogged": bool(big_received), "minStreamBytes": min_bytes,
            "completed": completed[:5], "failedAttempts": failed[:5], "received": received[:5],
            "largestCompletedBytes": max((e["totalBytes"] or 0 for e in completed), default=0)}


def judge_large_retained(expected: dict[str, str], seen: list[dict]) -> dict:
    """Every written topic must arrive retained with the written SHA-256; no extra topic may appear."""
    by_topic: dict[str, set[str]] = {}
    for message in seen:
        if message.get("retain"):
            by_topic.setdefault(message["topic"], set()).add(message["sha256"])
    mismatched = sorted(topic for topic, digest_value in expected.items() if by_topic.get(topic) != {digest_value})
    unexpected = sorted(set(by_topic) - set(expected))
    return {"result": "PASS" if expected and not mismatched and not unexpected else "FAIL", "expectedTopics": len(expected),
            "receivedTopics": len(by_topic), "mismatchedTopics": mismatched, "unexpectedTopics": unexpected}
