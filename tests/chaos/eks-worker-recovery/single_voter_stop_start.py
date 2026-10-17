"""Single-voter EC2 stop/start recovery on a disposable EKS 3-voter fixture (CLU-SCN-137, CLU-14).

ONE voter's worker EC2 instance is stopped by `stop-instances --skip-os-shutdown`, held stopped and started again.
The other two voters keep running. Acceptance row: docs/specs/cluster-session-and-delivery.md CLU-SCN-137.
Compute-side power-loss model only: EBS-internal write-cache loss and a real AZ power failure are not reproduced.
`power_loss.py` stops ALL voters in one request; that is a different scenario and is only reused for its helpers.

Order of one run (nothing is stopped unless every baseline step succeeded):
  1. preflight: account, disposable-fixture ownership, three Ready voters, quorum, volume/instance/image identity;
  2. one retained QoS 1 marker is published and its `PUBACK` (reason code 0) is required, then the marker must be
     readable from all three voters;
  3. three baseline EBS snapshots (one per voter volume) must be `completed`: either the plan lists their ids
     (`snapshotIds`, verified via `describe-snapshots`) or `createSnapshots: true` creates them after the marker
     (tagged with the run id, never deleted by this tool);
  4. ASG processes are suspended; the target's data directory is inventoried read-only through `kubectl exec`
     (two identical inventories a few seconds apart prove the files are quiescent);
  5. ONE `stop-instances --skip-os-shutdown` for the target instance; only the target may be `stopped`;
  6. hold, `start-instances`, wait until three Pods are Ready within `readyDeadlineSeconds` and the management
     API reports quorum and three READY nodes; the target must be a READY VOTER whose applied index reached the
     pre-fault maximum. restartCount, container termination, node state, readinessReason and the drain record are
     recorded (a FAILED drain record fails the run; `/health/ready` is never called, nothing is deleted);
  7. the same EBS volume must be attached to the Pod's node (an ASG replacement instance only counts when the
     volume id, PV, PVC and volume creation time are unchanged; the instance ids are recorded);
  8. the data directory is inventoried again and compared with the baseline (definition below);
  9. the marker SHA-256 is read from ALL THREE voters, including the restarted one;
 10. ASG processes this run suspended are resumed and no scaling activity or Pod change may follow.

"Existing WAL and files unchanged by recovery" (replay only) is judged strictly per file under the data roots
(`/data` by default):
  * a file that existed in the baseline must still exist with the SAME INODE (a rewrite via temp file + rename or
    a delete + create is a replacement and fails);
  * `wal.bin` is append-only: it may only grow, and the first <baseline size> bytes must hash to the baseline
    SHA-256 (shrinking, i.e. a tail cut, or any changed byte fails);
  * every other pre-existing file (cluster.meta, snapshot-N.bin, ...) must keep size, SHA-256 and mtime;
  * files that did not exist in the baseline (for example a newer snapshot) are allowed and listed;
  * the baseline WAL must be non-empty and the broker log must not say "raft wal torn tail truncated".
Files outside the data roots (for example `/audit`) are not part of this definition.

Evidence is written under --output (`single-voter-*.json`; the inherited ASG steps keep their
`power-loss-asg-*.json` names). Credentials are read from --private and never written. The executing function
`SingleVoterRun.execute` carries the scenario link and records its result through `@scenario_result`.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time
from typing import Callable
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.append(str(Path(__file__).resolve().parents[2]))  # tests/ holds scenario_results.py

import az_outage  # noqa: E402
import power_loss  # noqa: E402
from fixture import Fixture, ready, save  # noqa: E402
from observer import timestamp  # noqa: E402
from pod_tunnel import PodTunnel  # noqa: E402
from scenario_results import scenario_result  # noqa: E402

PowerLossError = power_loss.PowerLossError
ACCOUNT = power_loss.ACCOUNT
NAMESPACE = "narumq"
CONTAINER = "narumq"
# Local ports: observer (marker publish, baseline API reads) and, after the stop, the status tunnel to a survivor.
OBSERVER_PORTS = power_loss.PUBLISHER_PORTS
# One voter at a time for the marker reads.
READ_PORTS = power_loss.SUBSCRIBER_PORTS
HOLD_DEFAULT_SECONDS, HOLD_BOUNDS = 120, (30, 900)
READY_DEADLINE_DEFAULT_SECONDS, READY_DEADLINE_BOUNDS = 900, (120, 1800)
SNAPSHOT_WAIT_DEFAULT_SECONDS, SNAPSHOT_WAIT_BOUNDS = 1800, (60, 3600)
SNAPSHOT_POLL_SECONDS = 15
INVENTORY_ATTEMPTS = 5
QUIESCENCE_GAP_SECONDS = 3
EXEC_TIMEOUT_SECONDS = 300
MARKER_WAIT_SECONDS = 15
DATA_ROOTS_DEFAULT = ["/data"]
APPEND_ONLY_NAMES = ("wal.bin",)
STRING_KEYS = ("runId", "clusterName", "vpcId", "targetPod", "observerPod")
SNAPSHOT_ID = re.compile(r"snap-[0-9a-f]{8,17}")
SAFE_PATH = re.compile(r"/[A-Za-z0-9._-]+(?:/[A-Za-z0-9._-]+)*")
HEX64 = re.compile(r"[0-9a-f]{64}")
PRIVATE_FILES = ("admin.token", "kubeconfig", "kubectl.exe", "mqtt.password")
CRITERIA = ("baseline.markerPuback", "baseline.markerReadableAllVoters", "baseline.snapshotsCompleted",
            "baseline.imageDigestRecorded", "baseline.volumeIdentityRecorded", "baseline.dataInventoryQuiescent",
            "fault.singleStopRequest", "fault.onlyTargetStopped",
            "recovery.threeReadyWithinBound", "recovery.quorumAndThreeReady", "recovery.targetVoterReady",
            "recovery.targetAppliedIndexAtLeastBaseline", "recovery.sameVolumeReattached",
            "recovery.imageDigestUnchanged", "recovery.voterDiagnosticsRecorded", "recovery.noFailedDrainRecord",
            "recovery.drainRecordsCleared", "recovery.dataFilesUnchanged", "recovery.noWalTailCutLogged",
            "durable.retainedMarkerSha256AllVoters", "asg.resumed", "asg.noScalingAfterResume")

# Read-only inventory: inode, size, mtime and SHA-256 of the first <size> bytes of every regular file under $@.
INVENTORY_SCRIPT = """set -eu
for root in "$@"; do
  [ -d "$root" ] || { echo NOROOT; exit 3; }
  find "$root" -type f | sort | while IFS= read -r f; do
    i=$(stat -c %i "$f")
    s=$(stat -c %s "$f")
    m=$(stat -c %Y "$f")
    h=$(head -c "$s" "$f" | sha256sum | cut -d' ' -f1)
    printf '%s %s %s %s %s\\n' "$i" "$s" "$m" "$h" "$f"
  done
done
echo END
"""

# Read-only prefix hash of named files: the paths and baseline sizes are embedded as a quoted here-document.
PREFIX_SCRIPT = """set -eu
while read -r p s; do
  if [ ! -f "$p" ]; then printf 'MISSING %s\\n' "$p"; continue; fi
  i=$(stat -c %i "$p")
  c=$(stat -c %s "$p")
  m=$(stat -c %Y "$p")
  h=$(head -c "$s" "$p" | sha256sum | cut -d' ' -f1)
  printf 'FILE %s %s %s %s %s\\n' "$i" "$c" "$m" "$h" "$p"
done <<'NARUMQ_LIST'
{pairs}
NARUMQ_LIST
echo END
"""


def _bounded(plan: dict, key: str, default: int, bounds: tuple[int, int]) -> int:
    return power_loss._bounded(plan, key, default, bounds)


def validate_plan(plan: dict, run_id: str | None = None) -> dict:
    """Check the plan shape. Returns the numeric settings; reads nothing from the cluster."""
    if not isinstance(plan, dict) or not all(isinstance(plan.get(key), str) and plan[key] for key in STRING_KEYS):
        raise ValueError("incomplete single-voter plan")
    if run_id is not None and plan["runId"] != run_id:
        raise ValueError("plan does not identify this run")
    if not plan["clusterName"].startswith(("narumq-ha-acceptance-", "narumq-clu14-")):
        raise ValueError("plan does not identify a disposable acceptance fixture")
    voters = plan.get("voters")
    if not isinstance(voters, list) or len(voters) != 3 or not all(
            isinstance(v, dict) and all(isinstance(v.get(key), str) and v[key] for key in power_loss.VOTER_KEYS)
            for v in voters):
        raise ValueError(f"voters must list exactly three voters with {list(power_loss.VOTER_KEYS)}")
    for key in ("pod", "nodeId", "workerName", "instanceId", "volumeId", "pvcName"):
        if len({v[key] for v in voters}) != 3:
            raise ValueError(f"voter {key} values must be distinct")
    pods = {v["pod"] for v in voters}
    if plan["targetPod"] not in pods or plan["observerPod"] not in pods or plan["targetPod"] == plan["observerPod"]:
        raise ValueError("targetPod and observerPod must be two different voters")
    asgs = plan.get("asgNames")
    if (not isinstance(asgs, list) or not asgs or len(set(asgs)) != len(asgs)
            or not all(isinstance(item, str) and item for item in asgs)):
        raise ValueError("asgNames must be a non-empty list of distinct strings")
    ids, create = plan.get("snapshotIds"), plan.get("createSnapshots")
    if (ids is None) == (create is None) or (create is not None and create is not True):
        raise ValueError("plan needs exactly one of snapshotIds (three existing ids) or createSnapshots: true")
    if ids is not None and (not isinstance(ids, list) or len(ids) != 3 or len(set(ids)) != 3 or not all(
            isinstance(item, str) and SNAPSHOT_ID.fullmatch(item) for item in ids)):
        raise ValueError("snapshotIds must list exactly three distinct snap-<hex> ids")
    roots = plan.setdefault("dataRoots", list(DATA_ROOTS_DEFAULT))
    if (not isinstance(roots, list) or not roots or len(set(roots)) != len(roots)
            or not all(isinstance(item, str) and SAFE_PATH.fullmatch(item) for item in roots)):
        raise ValueError("dataRoots must be a non-empty list of distinct simple absolute paths")
    if plan.get("imageDigest") is not None and not (
            isinstance(plan["imageDigest"], str) and power_loss.DIGEST.fullmatch(plan["imageDigest"])):
        raise ValueError("imageDigest must be sha256:<64 hex>")
    expected_ids = sorted(v["instanceId"] for v in voters)
    if "instanceIds" in plan and plan["instanceIds"] != expected_ids:
        raise ValueError("instanceIds differ from planned voters")
    plan["instanceIds"] = expected_ids
    return {"holdSeconds": _bounded(plan, "holdSeconds", HOLD_DEFAULT_SECONDS, HOLD_BOUNDS),
            "readyDeadlineSeconds": _bounded(plan, "readyDeadlineSeconds", READY_DEADLINE_DEFAULT_SECONDS,
                                             READY_DEADLINE_BOUNDS),
            "snapshotWaitSeconds": _bounded(plan, "snapshotWaitSeconds", SNAPSHOT_WAIT_DEFAULT_SECONDS,
                                            SNAPSHOT_WAIT_BOUNDS),
            "postResumeSeconds": az_outage.post_resume_seconds(plan)}


def aggregate(criteria: dict, failed_step: str | None) -> dict:
    """PASS only when every named criterion is exactly True and no step failed."""
    verdicts = {name: criteria.get(name) is True for name in CRITERIA}
    failed = [name for name, ok in verdicts.items() if not ok]
    return {"result": "PASS" if not failed and failed_step is None else "FAIL", "criteria": verdicts,
            "failedCriteria": failed, "failedStep": failed_step}


# -- data directory inventory (pure) ----------------------------------------------------------------------------

def is_append_only(path: str) -> bool:
    return path.rsplit("/", 1)[-1] in APPEND_ONLY_NAMES


def _entry(inode: str, size: str, mtime: str, sha: str) -> dict:
    if not (inode.isdigit() and size.isdigit() and mtime.isdigit() and HEX64.fullmatch(sha)):
        raise PowerLossError("inventory_unparseable")
    return {"inode": int(inode), "size": int(size), "mtime": int(mtime), "sha256": sha}


def _report_lines(output: str) -> list[str]:
    lines = [line.rstrip("\r") for line in output.splitlines()]
    if not lines or lines[-1] != "END":
        raise PowerLossError("inventory_incomplete")
    return lines[:-1]


def parse_inventory(output: str) -> dict[str, dict]:
    """`INVENTORY_SCRIPT` output -> {path: {inode, size, mtime, sha256}}. A missing END line is an error."""
    files: dict[str, dict] = {}
    for line in _report_lines(output):
        parts = line.split(" ", 4)
        if len(parts) != 5 or not SAFE_PATH.fullmatch(parts[4]) or parts[4] in files:
            raise PowerLossError("inventory_unparseable")
        files[parts[4]] = _entry(*parts[:4])
    return files


def prefix_script(pairs: dict[str, int]) -> str:
    for path, size in pairs.items():
        if not SAFE_PATH.fullmatch(path) or isinstance(size, bool) or not isinstance(size, int) or size < 0:
            raise PowerLossError("prefix_request_invalid")
    return PREFIX_SCRIPT.replace("{pairs}", "\n".join(f"{path} {size}" for path, size in sorted(pairs.items())))


def parse_prefix_report(output: str) -> dict[str, dict | None]:
    """`PREFIX_SCRIPT` output -> {path: entry or None when the file is missing}. `sha256` hashes the baseline-size prefix."""
    found: dict[str, dict | None] = {}
    for line in _report_lines(output):
        kind, _, rest = line.partition(" ")
        if kind == "MISSING" and SAFE_PATH.fullmatch(rest):
            found[rest] = None
        elif kind == "FILE":
            parts = rest.split(" ", 4)
            if len(parts) != 5 or not SAFE_PATH.fullmatch(parts[4]):
                raise PowerLossError("inventory_unparseable")
            found[parts[4]] = _entry(*parts[:4])
        else:
            raise PowerLossError("inventory_unparseable")
    return found


def judge_data_files(baseline: dict[str, dict], after: dict[str, dict],
                     prefix: dict[str, dict | None] | None = None) -> dict:
    """Strict per-file comparison; see the module docstring for the definition of "unchanged"."""
    prefix = prefix or {}
    violations: list[dict] = []
    growth: dict[str, int] = {}
    if not any(is_append_only(path) and entry["size"] > 0 for path, entry in baseline.items()):
        violations.append({"path": "", "reason": "baseline_wal_missing_or_empty"})
    for path, was in sorted(baseline.items()):
        now = after.get(path)
        if now is None:
            violations.append({"path": path, "reason": "removed"})
        elif now["inode"] != was["inode"]:
            violations.append({"path": path, "reason": "replaced_inode_changed"})
        elif is_append_only(path):
            if now["size"] < was["size"]:
                violations.append({"path": path, "reason": "truncated"})
            elif now["size"] == was["size"]:
                if now["sha256"] != was["sha256"]:
                    violations.append({"path": path, "reason": "prefix_altered"})
            else:
                grown = prefix.get(path)
                if grown is None or grown["inode"] != was["inode"] or grown["sha256"] != was["sha256"]:
                    violations.append({"path": path, "reason": "prefix_altered_or_unverified"})
                growth[path] = now["size"] - was["size"]
        elif now["size"] != was["size"] or now["sha256"] != was["sha256"]:
            violations.append({"path": path, "reason": "modified"})
        elif now["mtime"] != was["mtime"]:
            violations.append({"path": path, "reason": "mtime_changed"})
    return {"result": "FAIL" if violations else "PASS", "violations": violations, "checked": len(baseline),
            "walGrowthBytes": growth, "added": sorted(set(after) - set(baseline))}


# -- snapshot and volume judges (pure) --------------------------------------------------------------------------

def describe_volume(raw: dict) -> dict:
    return {"volumeId": raw.get("VolumeId"), "az": raw.get("AvailabilityZone"), "sizeGiB": raw.get("Size"),
            "encrypted": raw.get("Encrypted"), "createTime": raw.get("CreateTime"), "state": raw.get("State"),
            "attachments": [{"instanceId": a.get("InstanceId"), "state": a.get("State"),
                             "attachTime": a.get("AttachTime"), "device": a.get("Device")}
                            for a in raw.get("Attachments", [])]}


def judge_snapshots(described: list[dict], voters: list[dict], volumes: dict[str, dict]) -> dict:
    """Exactly one completed snapshot (100%, this account) per voter volume."""
    problems: list[str] = []
    wanted = {v["volumeId"] for v in voters}
    per_volume: dict = {}
    for snapshot in described:
        per_volume.setdefault(snapshot.get("VolumeId"), []).append(snapshot)
    if len(described) != 3:
        problems.append("snapshot_count_not_three")
    if set(per_volume) != wanted or any(len(items) != 1 for items in per_volume.values()):
        problems.append("snapshots_not_one_per_voter_volume")
    for snapshot in described:
        name = snapshot.get("SnapshotId")
        if snapshot.get("State") != "completed" or snapshot.get("Progress") != "100%":
            problems.append(f"not_completed:{name}")
        if snapshot.get("OwnerId") != ACCOUNT:
            problems.append(f"foreign_owner:{name}")
        volume = volumes.get(snapshot.get("VolumeId"))
        if volume is not None and bool(snapshot.get("Encrypted")) != bool(volume.get("encrypted")):
            problems.append(f"encryption_differs_from_volume:{name}")
    return {"ok": not problems, "problems": problems,
            "snapshots": [{"snapshotId": s.get("SnapshotId"), "volumeId": s.get("VolumeId"), "state": s.get("State"),
                           "progress": s.get("Progress"), "startTime": s.get("StartTime"),
                           "encrypted": s.get("Encrypted")} for s in described]}


def judge_same_volume(baseline_volume: dict, planned: dict, current: dict, fleet: dict) -> dict:
    """The planned PVC/PV/EBS volume is attached to the node the recovered Pod runs on.

    `current`: pvcUID, pvName, volumeHandle, volume (describe_volume), nodeInstanceId, instance {state, vpcId, group}.
    An instance other than the original one (ASG replacement) is allowed only with unchanged volume identity.
    """
    problems: list[str] = []
    volume = current.get("volume") or {}
    if (current.get("pvcUID") != planned["pvcUID"] or current.get("pvName") != planned["pvName"]
            or current.get("volumeHandle") != planned["volumeId"]):
        problems.append("pvc_or_pv_identity_changed")
    if volume.get("volumeId") != planned["volumeId"]:
        problems.append("volume_id_changed")
    for key in ("createTime", "az", "sizeGiB", "encrypted"):
        if volume.get(key) != baseline_volume.get(key):
            problems.append(f"volume_{key}_changed")
    attachments = volume.get("attachments") or []
    if volume.get("state") != "in-use" or len(attachments) != 1 or attachments[0].get("state") != "attached":
        problems.append("volume_not_in_use_with_one_attached_attachment")
    elif attachments[0].get("instanceId") != current.get("nodeInstanceId"):
        problems.append("volume_not_attached_to_pod_node_instance")
    instance = current.get("instance") or {}
    if instance.get("state") != "running":
        problems.append("node_instance_not_running")
    if instance.get("vpcId") != fleet["vpcId"] or instance.get("group") not in fleet["asgNames"]:
        problems.append("node_instance_outside_fixture_vpc_or_planned_asgs")
    return {"ok": not problems, "problems": problems, "volumeId": volume.get("volumeId"),
            "originalInstanceId": planned["instanceId"], "attachedInstanceId": (attachments[0].get("instanceId")
                                                                                if len(attachments) == 1 else None),
            "podNodeInstanceId": current.get("nodeInstanceId"),
            "instanceReplaced": current.get("nodeInstanceId") != planned["instanceId"],
            "attachment": attachments}


def marker_messages_ok(messages: list[dict], expected_sha: str) -> bool:
    return bool(messages) and all(m.get("sha256") == expected_sha and m.get("retain") is True and m.get("qos") == 1
                                  for m in messages)


def read_marker(client_factory: Callable, port: int, topic: str) -> list[dict]:
    """Subscribe with a clean reader and return what the broker delivers for the marker topic."""
    reader = client_factory("single-voter-reader", port, True, 0)
    try:
        reader.subscribe(topic)
        if not reader.received.wait(MARKER_WAIT_SECONDS):
            raise AssertionError("retained marker timeout")
        return [dict(m) for m in reader.messages if m["topic"] == topic]
    finally:
        reader.close()


def default_client_factory(private: Path) -> Callable:
    import payload_oracle  # paho-mqtt is only needed for a real run
    password = (private / "mqtt.password").read_text(encoding="utf-8").strip()
    return lambda identity, port, clean, expiry: payload_oracle.Client(identity, password, port, clean, expiry)


def kubectl_exec(fixture, pod: str, script: str, args: tuple = (), timeout: float = EXEC_TIMEOUT_SECONDS) -> str:
    """Run a fixed read-only shell script in the broker container; the script travels as UTF-8 bytes on stdin."""
    argv = fixture.kubectl + ["-n", NAMESPACE, "exec", "-i", pod, "-c", CONTAINER, "--", "sh", "-s", "--", *args]
    try:
        result = subprocess.run(argv, env=fixture.environment, input=script.encode("utf-8"), capture_output=True,
                                timeout=timeout)
    except subprocess.TimeoutExpired as exc:
        raise PowerLossError("data_exec_timeout") from exc
    if result.returncode != 0:
        raise PowerLossError("data_exec_failed")
    return result.stdout.decode("utf-8", errors="replace")


class SingleVoterRun(power_loss.PowerLossRun):
    """Reuses the diagnostics, ASG suspend/resume and post-resume steps of the all-voter power-loss run."""

    def __init__(self, fixture, plan: dict, private: Path, output: Path, sleep: Callable = time.sleep,
                 tunnel_factory=PodTunnel, client_factory: Callable | None = None,
                 exec_runner: Callable | None = None):
        self.fixture = fixture
        self.plan = plan
        self.private = private
        self.output = output
        self.sleep = sleep
        self.settings = validate_plan(plan, fixture.run_id)
        self.tunnel_factory = tunnel_factory
        self.client_factory = client_factory or (
            lambda identity, port, clean, expiry: default_client_factory(private)(identity, port, clean, expiry))
        self.exec_runner = exec_runner or (lambda pod, script, args=(): kubectl_exec(fixture, pod, script, args))
        self.target = power_loss.voter(plan, plan["targetPod"])
        self.criteria: dict[str, bool] = {}
        self.evidence: dict = {"voterPods": [v["pod"] for v in plan["voters"]], "targetPod": plan["targetPod"]}
        self.voters_saved = False
        self.step = "init"
        self.publisher_tunnel = None
        self.subscriber_tunnel = None
        self.publisher = None
        self.marks: dict[str, float] = {}
        self.times: dict[str, str] = {}
        self.suspended_by_tool: dict[str, list[str]] = {}
        self.baseline: dict = {}
        self.restart_baseline: dict[str, int] = {}
        self.diag_tunnel = None
        self.drain_seen: dict[str, str | None] = {}
        self.failed_drain: dict | None = None
        self.timeline: list[dict] = []
        self.last_signature = None
        self.resumed_from: str | None = None
        self.image_digest: str | None = None
        self.volume_baseline: dict[str, dict] = {}
        self.data_baseline: dict[str, dict] = {}
        self.marker: dict = {}
        self.created_snapshots: list[str] = []
        self.teardown: dict = {}

    # -- inherited helpers adapted to this scenario -----------------------------------------------------------

    def record(self, name: str, value) -> None:
        self.evidence[name] = value
        save(self.output, f"single-voter-{name}.json", value)

    def diagnostics_available(self, pods: dict[str, dict]) -> bool:
        """Status tunnel to a Ready voter, survivors first so the restarted target never carries the diagnostics."""
        if self.diag_tunnel is not None:
            try:
                self.diag_tunnel.check()
                return True
            except Exception:
                self.close_diag()
        for planned in sorted(self.plan["voters"], key=lambda v: v["pod"] == self.target["pod"]):
            pod = pods.get(planned["pod"])
            if not pod or not ready(pod) or pod["metadata"].get("deletionTimestamp"):
                continue
            tunnel = self.tunnel_factory(self.fixture, planned["pod"], pod["metadata"]["uid"], *OBSERVER_PORTS)
            try:
                tunnel.start()
            except Exception:
                continue
            self.diag_tunnel = tunnel
            return True
        return False

    def status_api(self, path: str):
        """One management API read through the status tunnel; None when no tunnel or the read failed."""
        if not self.diagnostics_available(self.pods()):
            return None
        try:
            return self.fixture.api(path, OBSERVER_PORTS[0])
        except Exception:
            self.close_diag()
            return None

    def node_rows(self, port: int | None = None) -> dict[str, dict] | None:
        try:
            data = self.fixture.api("/api/v1/nodes?limit=1000", port) if port else self.status_api("/api/v1/nodes?limit=1000")
        except Exception:
            return None
        return None if data is None else {str(row.get("nodeId")): row for row in data}

    # -- baseline ----------------------------------------------------------------------------------------------

    def open_observer_tunnel(self) -> None:
        self.step = "observer_tunnel"
        planned = power_loss.voter(self.plan, self.plan["observerPod"])
        self.publisher_tunnel = self.tunnel_factory(self.fixture, planned["pod"], planned["podUID"], *OBSERVER_PORTS)
        self.record("observer-endpoint", self.publisher_tunnel.start())

    def close_observer_tunnel(self) -> None:
        self.step = "observer_tunnel_close"
        tunnel, self.publisher_tunnel = self.publisher_tunnel, None
        if tunnel is not None:
            tunnel.close()

    def step_preflight(self) -> None:
        self.step = "preflight"
        result = power_loss.preflight(self.fixture, self.plan, OBSERVER_PORTS[0])
        self.restart_baseline = {pod: n for pod, n in result["restartCounts"].items() if isinstance(n, int)}
        self.image_digest = result["image"]["digest"]
        self.criteria["baseline.imageDigestRecorded"] = bool(self.image_digest)
        volumes = {}
        for planned in self.plan["voters"]:
            raw = self.fixture.aws("ec2", "describe-volumes", "--volume-ids", planned["volumeId"])["Volumes"][0]
            volumes[planned["volumeId"]] = describe_volume(raw)
        self.volume_baseline = volumes
        self.criteria["baseline.volumeIdentityRecorded"] = all(
            v["volumeId"] == planned["volumeId"] and v["createTime"] and v["state"] == "in-use"
            and [(a["instanceId"], a["state"]) for a in v["attachments"]] == [(planned["instanceId"], "attached")]
            for planned in self.plan["voters"] for v in [volumes[planned["volumeId"]]])
        rows = self.node_rows(OBSERVER_PORTS[0]) or {}
        result["target"] = {"pod": self.target["pod"], "nodeId": self.target["nodeId"],
                            "instanceId": self.target["instanceId"], "volumeId": self.target["volumeId"],
                            "role": rows.get(self.target["nodeId"], {}).get("role")}
        result["volumes"] = volumes
        self.record("preflight", result)
        if not self.criteria["baseline.volumeIdentityRecorded"]:
            raise PowerLossError("volume_identity_not_recorded")

    def read_marker_all(self) -> dict[str, dict]:
        """The marker SHA-256 as delivered by each voter's own broker."""
        pods = self.pods()
        reads: dict[str, dict] = {}
        for planned in self.plan["voters"]:
            entry: dict = {"pod": planned["pod"]}
            try:
                pod = pods.get(planned["pod"])
                if pod is None:
                    raise PowerLossError("pod_missing")
                tunnel = self.tunnel_factory(self.fixture, planned["pod"], pod["metadata"]["uid"], *READ_PORTS)
                try:
                    entry["tunnel"] = tunnel.start()
                    messages = read_marker(self.client_factory, READ_PORTS[1], self.marker["topic"])
                finally:
                    tunnel.close()
                entry.update(messages=len(messages), sha256s=sorted({m["sha256"] for m in messages}),
                             retain=all(m["retain"] is True for m in messages),
                             qos1=all(m["qos"] == 1 for m in messages),
                             ok=marker_messages_ok(messages, self.marker["sha256"]))
            except Exception as exc:
                entry.update(ok=False, errorCode=type(exc).__name__)
            reads[planned["nodeId"]] = entry
        return reads

    def step_marker(self) -> None:
        """Retained QoS 1 marker with a PUBACK, then readable from all three voters before anything is stopped."""
        self.step = "marker"
        run_id = self.plan["runId"]
        payload = f"{run_id}:single-voter-marker:{uuid.uuid4().hex[:16]}".encode()
        self.marker = {"topic": f"acceptance/{run_id}/single-voter-stop-start/marker",
                       "sha256": hashlib.sha256(payload).hexdigest()}
        publisher = None
        try:
            publisher = self.client_factory("single-voter-publisher", OBSERVER_PORTS[1], True, 0)
            publisher.publish(self.marker["topic"], payload, retain=True)
            acks = [dict(a) for a in publisher.acks]
        except AssertionError as exc:
            raise PowerLossError("marker_puback_missing") from exc
        finally:
            if publisher is not None:
                publisher.close()
        self.marker["publishedAt"] = timestamp()
        self.criteria["baseline.markerPuback"] = len(acks) == 1 and acks[0].get("reasonValue") == 0
        reads = self.read_marker_all()
        self.criteria["baseline.markerReadableAllVoters"] = all(item["ok"] for item in reads.values()) and len(reads) == 3
        self.record("marker-baseline", {"topic": self.marker["topic"], "sha256": self.marker["sha256"],
                                        "publishedAt": self.marker["publishedAt"], "qos": 1, "retain": True,
                                        "pubacks": acks, "readsByNodeId": reads})
        if not self.criteria["baseline.markerPuback"]:
            raise PowerLossError("marker_not_acknowledged")
        if not self.criteria["baseline.markerReadableAllVoters"]:
            raise PowerLossError("marker_not_readable_before_fault")

    def create_snapshots(self) -> list[str]:
        run_id = self.plan["runId"]
        tags = f"ResourceType=snapshot,Tags=[{{Key=AcceptanceRun,Value={run_id}}},{{Key=Purpose,Value=single-voter-stop-start-baseline}}]"
        for planned in self.plan["voters"]:
            response = self.fixture.aws("ec2", "create-snapshot", "--volume-id", planned["volumeId"], "--description",
                                        f"narumq single-voter baseline {run_id} {planned['pod']}",
                                        "--tag-specifications", tags)
            self.created_snapshots.append(response["SnapshotId"])
            save(self.output, "single-voter-snapshots-created.json",
                 {"snapshotIds": list(self.created_snapshots), "retained": True,
                  "note": "baseline snapshots are intentionally never deleted by this tool"})
        return list(self.created_snapshots)

    def wait_snapshots(self, ids: list[str]) -> list[dict]:
        deadline = time.monotonic() + self.settings["snapshotWaitSeconds"]
        while True:
            described = self.fixture.aws("ec2", "describe-snapshots", "--snapshot-ids", *ids)["Snapshots"]
            if any(s.get("State") == "error" for s in described):
                raise PowerLossError("snapshot_failed")
            if len(described) == len(ids) and all(s.get("State") == "completed" for s in described):
                return described
            if time.monotonic() >= deadline:
                raise PowerLossError("snapshots_not_completed_within_bound")
            self.sleep(SNAPSHOT_POLL_SECONDS)

    def step_snapshots(self) -> None:
        self.step = "snapshots"
        created = self.plan.get("snapshotIds") is None
        ids = self.create_snapshots() if created else list(self.plan["snapshotIds"])
        verdict = judge_snapshots(self.wait_snapshots(ids), self.plan["voters"], self.volume_baseline)
        verdict.update(created=created, snapshotIds=ids, markerPublishedAt=self.marker["publishedAt"])
        self.criteria["baseline.snapshotsCompleted"] = verdict["ok"]
        self.record("snapshots", verdict)
        if not verdict["ok"]:
            raise PowerLossError("baseline_snapshots_not_completed")

    def read_inventory(self, pod: str) -> dict[str, dict]:
        roots = self.plan["dataRoots"]
        inventory = parse_inventory(self.exec_runner(pod, INVENTORY_SCRIPT, tuple(roots)))
        if any(not any(path.startswith(root + "/") for root in roots) for path in inventory):
            raise PowerLossError("inventory_outside_data_roots")
        return inventory

    def capture_quiescent_inventory(self, pod: str) -> tuple[dict[str, dict], int]:
        """Two identical inventories prove the files are not being written while the baseline is taken."""
        for attempt in range(1, INVENTORY_ATTEMPTS + 1):
            first = self.read_inventory(pod)
            self.sleep(QUIESCENCE_GAP_SECONDS)
            second = self.read_inventory(pod)
            if first == second:
                return second, attempt
            self.sleep(QUIESCENCE_GAP_SECONDS)
        raise PowerLossError("data_dir_not_quiescent")

    def step_data_baseline(self) -> None:
        self.step = "data_baseline"
        inventory, attempts = self.capture_quiescent_inventory(self.target["pod"])
        self.data_baseline = inventory
        has_wal = any(is_append_only(path) and entry["size"] > 0 for path, entry in inventory.items())
        self.criteria["baseline.dataInventoryQuiescent"] = has_wal
        rows = self.node_rows(OBSERVER_PORTS[0])
        applied = {node_id: row.get("lastAppliedIndex") for node_id, row in (rows or {}).items()}
        if not applied or not all(isinstance(v, int) and not isinstance(v, bool) for v in applied.values()):
            raise PowerLossError("applied_index_unavailable")
        self.baseline = {"lastAppliedIndex": applied, "maxLastAppliedIndex": max(applied.values())}
        self.record("data-baseline", {"pod": self.target["pod"], "roots": self.plan["dataRoots"], "attempts": attempts,
                                      "appendOnlyNames": list(APPEND_ONLY_NAMES), "files": inventory,
                                      "baseline": self.baseline})
        if not has_wal:
            raise PowerLossError("baseline_wal_missing_or_empty")

    # -- fault -------------------------------------------------------------------------------------------------

    def stop_target(self) -> None:
        """ONE stop call for the target instance; the other two voters must keep running."""
        self.step = "stop"
        ids = [self.target["instanceId"]]
        self.mark("stopRequested")
        response = self.fixture.aws("ec2", "stop-instances", "--instance-ids", *ids, "--skip-os-shutdown")
        self.criteria["fault.singleStopRequest"] = {i["InstanceId"] for i in response["StoppingInstances"]} == set(ids)
        save(self.output, "single-voter-stop-request.json",
             {"at": self.times["stopRequested"], "skipOsShutdown": True, "instanceIds": ids, "oneCall": True,
              "targetPod": self.target["pod"], "volumeId": self.target["volumeId"]})
        if not self.criteria["fault.singleStopRequest"]:
            raise PowerLossError("unexpected_stop_response")
        self.fixture.command(["aws", "ec2", "wait", "instance-stopped", "--instance-ids", *ids],
                             timeout=power_loss.WAIT_TIMEOUT_SECONDS)
        states = {i["InstanceId"]: i["State"]["Name"] for r in
                  self.fixture.aws("ec2", "describe-instances", "--instance-ids", *self.plan["instanceIds"])["Reservations"]
                  for i in r["Instances"]}
        expected = {i: "stopped" if i == self.target["instanceId"] else "running" for i in self.plan["instanceIds"]}
        self.criteria["fault.onlyTargetStopped"] = states == expected
        self.record("states-while-stopped", {"instances": states,
                                             "volume": self.fixture.attachment_probe(self.target["pvName"],
                                                                                     self.target["volumeId"])})
        if not self.criteria["fault.onlyTargetStopped"]:
            raise PowerLossError("instances_not_in_expected_state")
        self.mark("stopped")

    def hold_and_start(self) -> None:
        self.step = "hold"
        end = self.marks["stopped"] + self.settings["holdSeconds"]
        while time.monotonic() < end:
            self.sleep(1)
        self.step = "start"
        ids = [self.target["instanceId"]]
        self.mark("startRequested")
        response = self.fixture.aws("ec2", "start-instances", "--instance-ids", *ids)
        if {i["InstanceId"] for i in response["StartingInstances"]} != set(ids):
            raise PowerLossError("unexpected_start_response")
        self.fixture.command(["aws", "ec2", "wait", "instance-running", "--instance-ids", *ids],
                             timeout=power_loss.WAIT_TIMEOUT_SECONDS)
        self.mark("running")

    # -- recovery ----------------------------------------------------------------------------------------------

    def step_recovery(self) -> None:
        self.step = "recovery"
        deadline = self.settings["readyDeadlineSeconds"]
        names = {v["pod"] for v in self.plan["voters"]}

        def three_ready():
            pods = self.pods()
            self.note(self.snapshot(pods))
            if self.failed_drain is not None:
                raise PowerLossError(f"drain_record_failed:{self.failed_drain['pod']}")
            return set(pods) == names and all(ready(p) and not p["metadata"].get("deletionTimestamp")
                                              for p in pods.values())

        try:
            elapsed = time.monotonic() - self.marks["startRequested"]
            ok = self.wait_until(three_ready, max(deadline - elapsed, 0), power_loss.DIAGNOSTIC_INTERVAL_SECONDS)
            within = bool(ok) and time.monotonic() - self.marks["startRequested"] <= deadline
            self.criteria["recovery.threeReadyWithinBound"] = within
            if ok:
                self.mark("threeReady")
            if not within:
                raise PowerLossError("three_voters_not_ready_within_bound")

            def quorum():
                cluster = self.status_api("/api/v1/cluster")
                if isinstance(cluster, dict) and cluster.get("quorumAvailable") is True \
                        and cluster.get("nodesByState", {}).get("ready") == 3:
                    return cluster
                return None

            remaining = max(deadline - (time.monotonic() - self.marks["startRequested"]), 0)
            cluster = self.wait_until(quorum, remaining, 5)
            self.criteria["recovery.quorumAndThreeReady"] = cluster is not None
            pods = self.pods()
            final = self.snapshot(pods, force_drain=True)
            self.note(final)
            self.criteria["recovery.voterDiagnosticsRecorded"] = power_loss.diagnostics_complete(final)
            self.criteria["recovery.drainRecordsCleared"] = bool(final["apiReachable"]) and all(
                (v.get("drainRecord") or {}).get("present") is False for v in final["voters"].values())
            target_node = (final["voters"].get(self.target["nodeId"]) or {}).get("node") or {}
            self.criteria["recovery.targetVoterReady"] = (
                target_node.get("state") == "READY" and target_node.get("ready") is True
                and target_node.get("membershipPhase") == "VOTER" and target_node.get("queryStatus") == "OK")

            def applied():
                rows = self.node_rows()
                value = (rows or {}).get(self.target["nodeId"], {}).get("lastAppliedIndex")
                ok_value = isinstance(value, int) and not isinstance(value, bool) \
                    and value >= self.baseline["maxLastAppliedIndex"]
                return {"targetLastAppliedIndex": value} if ok_value else None

            reached = self.wait_until(applied, power_loss.APPLIED_INDEX_WAIT_SECONDS, 5)
            self.criteria["recovery.targetAppliedIndexAtLeastBaseline"] = reached is not None
            digests = {name: power_loss.image_digest(pod) for name, pod in pods.items()}
            self.criteria["recovery.imageDigestUnchanged"] = bool(self.image_digest) and all(
                digest == self.image_digest for digest in digests.values()) and set(digests) == names
            recovery = {"podUIDs": {name: p["metadata"]["uid"] for name, p in pods.items()},
                        "podUIDChanged": {v["pod"]: pods[v["pod"]]["metadata"]["uid"] != v["podUID"]
                                          for v in self.plan["voters"]},
                        "restartCounts": {name: power_loss.broker_status(p).get("restartCount") for name, p in pods.items()},
                        "restartCountsBefore": self.restart_baseline,
                        "targetContainer": power_loss.container_diagnostics(pods[self.target["pod"]]),
                        "imageDigests": digests, "imageDigestBefore": self.image_digest,
                        "targetLastAppliedIndex": (reached or {}).get("targetLastAppliedIndex"),
                        "baselineMaxLastAppliedIndex": self.baseline["maxLastAppliedIndex"],
                        "secondsFromStartRequest": round(time.monotonic() - self.marks["startRequested"], 1),
                        "cluster": None if cluster is None else {"quorumAvailable": True,
                                                                  "nodesByState": cluster["nodesByState"]}}
            self.record("recovery", recovery)
            self.save_voters(final)
            if cluster is None:
                raise PowerLossError("quorum_not_restored_within_bound")
        finally:
            self.criteria["recovery.noFailedDrainRecord"] = self.failed_drain is None
            if not self.voters_saved:
                self.save_voters(None)

    def step_same_volume(self) -> None:
        self.step = "same_volume"
        target = self.target
        pod = self.pods()[target["pod"]]
        node_name = pod["spec"].get("nodeName")
        node = self.fixture.get_node(node_name) if node_name else None
        instance_id = power_loss._instance_id(node)
        pvc = self.fixture.get("pvc", target["pvcName"], "-n", NAMESPACE)
        pv = self.fixture.get("pv", pvc["spec"]["volumeName"])
        volume = describe_volume(self.fixture.aws("ec2", "describe-volumes", "--volume-ids", target["volumeId"])["Volumes"][0])
        raw = self.fixture.instance(instance_id) if instance_id else {}
        group = next((t["Value"] for t in raw.get("Tags", []) if t["Key"] == "aws:autoscaling:groupName"), None)
        current = {"pvcUID": pvc["metadata"]["uid"], "pvName": pv["metadata"]["name"],
                   "volumeHandle": pv["spec"]["csi"]["volumeHandle"], "volume": volume, "nodeInstanceId": instance_id,
                   "instance": {"state": (raw.get("State") or {}).get("Name"), "vpcId": raw.get("VpcId"), "group": group}}
        verdict = judge_same_volume(self.volume_baseline[target["volumeId"]], target, current,
                                    {"vpcId": self.plan["vpcId"], "asgNames": self.plan["asgNames"]})
        verdict["attachmentProbe"] = self.fixture.attachment_probe(target["pvName"], target["volumeId"])
        verdict["podUID"] = {"before": target["podUID"], "after": pod["metadata"]["uid"]}
        self.criteria["recovery.sameVolumeReattached"] = verdict["ok"]
        self.record("same-volume", verdict)

    def step_data_verify(self) -> None:
        self.step = "data_verify"
        pod = self.pods()[self.target["pod"]]
        if not ready(pod):
            raise PowerLossError("target_pod_not_ready_for_data_check")
        after = self.read_inventory(self.target["pod"])
        grown = {path: entry["size"] for path, entry in self.data_baseline.items()
                 if is_append_only(path) and path in after and after[path]["size"] > entry["size"]}
        prefix = parse_prefix_report(self.exec_runner(self.target["pod"], prefix_script(grown), ())) if grown else {}
        verdict = judge_data_files(self.data_baseline, after, prefix)
        log = power_loss.torn_tail_log(power_loss.fetch_log(self.fixture, self.target["pod"], self.times["stopRequested"]))
        self.criteria["recovery.dataFilesUnchanged"] = verdict["result"] == "PASS"
        self.criteria["recovery.noWalTailCutLogged"] = log["logRead"] and not log["logged"]
        self.record("data-verify", {**verdict, "definition": "see module docstring of single_voter_stop_start.py",
                                    "afterFiles": after, "prefixChecked": sorted(grown), "tornTailLog": log})

    def step_marker_verify(self) -> None:
        self.step = "marker_verify"
        reads = self.read_marker_all()
        self.criteria["durable.retainedMarkerSha256AllVoters"] = len(reads) == 3 and all(
            item["ok"] for item in reads.values())
        self.record("marker-after-recovery", {"expectedSha256": self.marker["sha256"], "topic": self.marker["topic"],
                                              "readsByNodeId": reads, "republished": False})

    # Scenarios: cluster-session-and-delivery.md:CLU-SCN-137
    @scenario_result
    def execute(self) -> dict:
        try:
            power_loss.check_ports([*OBSERVER_PORTS, *READ_PORTS])
            self.open_observer_tunnel()
            self.step_preflight()
            self.step_marker()
            self.step_snapshots()
            try:
                self.suspend_autoscaling()
                self.step_data_baseline()
                self.close_observer_tunnel()
                self.stop_target()
                self.hold_and_start()
                self.step_recovery()
                self.step_same_volume()
                self.step_data_verify()
                self.step_marker_verify()
            finally:
                self.finish_autoscaling()
        finally:
            self.cleanup(self.teardown)
        judged = aggregate(self.criteria, None)
        if self.teardown.get("teardownFailed"):
            judged["result"] = "FAIL"
            judged["teardownErrors"] = self.teardown.get("teardownErrors")
        return judged


def check_prerequisites(private: Path) -> None:
    """External prerequisites fail with a stable code; nothing is skipped."""
    for name in PRIVATE_FILES:
        if not (private / name).is_file():
            raise PowerLossError(f"prerequisite_missing:{name}")
    if shutil.which("aws") is None:
        raise PowerLossError("prerequisite_missing:aws")
    try:
        import paho.mqtt.client  # noqa: F401
    except ImportError as exc:
        raise PowerLossError("dependency_missing:paho-mqtt") from exc


def run_cli(args) -> int:
    """Entry point of `main.py single-voter-stop-start`: the output directory is created here."""
    args.output.mkdir(parents=True, exist_ok=False)
    run_id = args.output.name
    report = {"result": "FAIL", "mode": "single-voter-stop-start", "runId": run_id, "startedAt": timestamp()}
    run, judged, step = None, None, "prerequisites"
    try:
        check_prerequisites(args.private)
        step = "plan"
        try:
            plan = json.loads(args.plan.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise PowerLossError("plan_unreadable") from exc
        fixture = Fixture(args.private, run_id)
        run = SingleVoterRun(fixture, plan, args.private, args.output,
                             client_factory=default_client_factory(args.private))
        report["settings"] = run.settings
        judged = run.execute()
    except PowerLossError as exc:
        report["errorCode"] = exc.code
    except Exception as exc:
        report["errorCode"] = type(exc).__name__
    finally:
        if run is not None:
            report["timings"] = run.evidence.get("timings")
            report["failedStep"] = run.step if "errorCode" in report else None
            if judged is None:
                judged = aggregate(run.criteria, report["failedStep"] or "execute")
        else:
            report["failedStep"] = step
            judged = aggregate({}, step)
        report.update(judged)
        report["target"] = None if run is None else {"pod": run.target["pod"], "instanceId": run.target["instanceId"],
                                                      "volumeId": run.target["volumeId"]}
        report["scope"] = ("compute power-loss model: one stop-instances --skip-os-shutdown call for ONE voter; "
                           "EBS internal write-cache loss is not reproduced")
        report["finishedAt"] = timestamp()
        save(args.output, "summary.json", report)
    print(json.dumps(report))
    return 0 if report["result"] == "PASS" else 1


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--private", type=Path, required=True, help="directory with kubeconfig, tokens and passwords")
    parser.add_argument("--output", type=Path, required=True,
                        help="new evidence directory; its name must equal the fixture run id (kube context)")
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--execute", action="store_true",
                        help="allow stop/start of the planned voter instance of the checked fixture")
    args = parser.parse_args(argv)
    if not args.execute:
        parser.error("single-voter-stop-start requires --execute")
    return run_cli(args)


if __name__ == "__main__":
    raise SystemExit(main())
