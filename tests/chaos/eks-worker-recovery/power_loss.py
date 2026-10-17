"""Full-voter power-loss model for a disposable EKS fixture (CLU-14 P1).

All three voter worker EC2 instances are stopped by ONE `stop-instances --skip-os-shutdown` call while a durable
QoS 1 publisher is running, held stopped, and started again with the same instances and EBS volumes. EBS-internal
write-cache loss and a real AZ power failure are not reproduced; this is the compute-side power-loss model of
the az-outage tool (DEP-20) applied to every voter.

Acceptance: docs/specs/cluster-session-and-delivery.md "EKS 전체 voter 전원 손실·torn-tail·큰 snapshot 재시드 기준",
criterion P1. Evidence is written under --output; credentials are read from --private and never written.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import re
import socket
import subprocess
import time
from typing import Callable

import az_outage
import durable_stream
from fixture import Fixture, ready, require_owned_fixture, save
from observer import timestamp
from pod_tunnel import PodTunnel

ACCOUNT = az_outage.ACCOUNT
PROCESSES = az_outage.PROCESSES
PUBLISHER_PORTS = (23880, 23883)
SUBSCRIBER_PORTS = (24880, 24883)
HOLD_DEFAULT_SECONDS, HOLD_BOUNDS = 120, (30, 900)
READY_DEADLINE_DEFAULT_SECONDS, READY_DEADLINE_BOUNDS = 900, (120, 1800)
FAULT_AFTER_ACKS_DEFAULT, FAULT_AFTER_ACKS_BOUNDS = 5, (1, 30)
DURABLE_INTERVAL_DEFAULT_SECONDS = 0.5
APPLIED_INDEX_WAIT_SECONDS = 120
WAIT_TIMEOUT_SECONDS = 600
VOTER_KEYS = ("pod", "podUID", "nodeId", "workerName", "instanceId", "volumeId", "pvcName", "pvcUID", "pvName")
STRING_KEYS = ("runId", "clusterName", "vpcId", "durableScenario", "publisherPod", "subscriberPod")
# The broker logs this INFO line when it cuts a torn WAL tail on open (path, valid_bytes, dropped_bytes, reason).
TORN_TAIL_MESSAGE = "raft wal torn tail truncated"
TORN_TAIL_FALLBACK = re.compile(r"wal.*torn.*(truncat|cut|drop)|torn.*wal.*(truncat|cut|drop)", re.IGNORECASE)
TORN_TAIL_FIELDS = ("path", "valid_bytes", "dropped_bytes", "reason")
CRITERIA = ("fault.singleStopRequest", "fault.allStopped", "fault.publishingUnderLoad", "baseline.marker",
            "baseline.imageDigestRecorded", "recovery.threeReadyWithinBound", "recovery.quorumAndThreeReady",
            "recovery.voterDiagnosticsRecorded", "recovery.noFailedDrainRecord", "recovery.drainRecordsCleared",
            "durable.streamOracle", "durable.retainedMarkerSha256", "durable.offlineSession",
            "observe.tornTailRecordedPerNode", "observe.appliedIndexAtLeastBaseline", "asg.resumed",
            "asg.noScalingAfterResume")


class PowerLossError(RuntimeError):
    """A run step failed; `code` is a stable identifier without command output."""

    def __init__(self, code: str):
        super().__init__(code)
        self.code = code


def _bounded(plan: dict, key: str, default: int, bounds: tuple[int, int]) -> int:
    value = plan.get(key, default)
    if isinstance(value, bool) or not isinstance(value, int) or not bounds[0] <= value <= bounds[1]:
        raise ValueError(f"{key} must be an integer within {list(bounds)}")
    return value


def validate_plan(plan: dict, run_id: str | None = None) -> dict:
    """Check the plan shape. Returns the numeric settings; reads nothing from the cluster."""
    if not isinstance(plan, dict) or not all(isinstance(plan.get(key), str) and plan[key] for key in STRING_KEYS):
        raise ValueError("incomplete power-loss plan")
    if run_id is not None and plan["runId"] != run_id:
        raise ValueError("plan does not identify this run")
    if not plan["clusterName"].startswith(("narumq-ha-acceptance-", "narumq-clu14-")):
        raise ValueError("plan does not identify a disposable acceptance fixture")
    if not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,39}", plan["durableScenario"]):
        raise ValueError("durableScenario must be a short lowercase token")
    voters = plan.get("voters")
    if not isinstance(voters, list) or len(voters) != 3 or not all(
            isinstance(v, dict) and all(isinstance(v.get(key), str) and v[key] for key in VOTER_KEYS) for v in voters):
        raise ValueError(f"voters must list exactly three voters with {list(VOTER_KEYS)}")
    for key in ("pod", "nodeId", "workerName", "instanceId", "volumeId", "pvcName"):
        if len({v[key] for v in voters}) != 3:
            raise ValueError(f"voter {key} values must be distinct")
    pods = {v["pod"] for v in voters}
    if plan["publisherPod"] not in pods or plan["subscriberPod"] not in pods or plan["publisherPod"] == plan["subscriberPod"]:
        raise ValueError("publisherPod and subscriberPod must be two different voters")
    asgs = plan.get("asgNames")
    if (not isinstance(asgs, list) or not asgs or len(set(asgs)) != len(asgs)
            or not all(isinstance(item, str) and item for item in asgs)):
        raise ValueError("asgNames must be a non-empty list of distinct strings")
    plan.setdefault("durableIntervalSeconds", DURABLE_INTERVAL_DEFAULT_SECONDS)
    durable_stream.plan_settings(plan)
    if plan.get("imageDigest") is not None and not (isinstance(plan["imageDigest"], str) and DIGEST.fullmatch(plan["imageDigest"])):
        raise ValueError("imageDigest must be sha256:<64 hex>")
    ids = sorted(v["instanceId"] for v in voters)
    if "instanceIds" in plan and plan["instanceIds"] != ids:
        raise ValueError("instanceIds differ from planned voters")
    plan["instanceIds"] = ids
    return {"holdSeconds": _bounded(plan, "holdSeconds", HOLD_DEFAULT_SECONDS, HOLD_BOUNDS),
            "readyDeadlineSeconds": _bounded(plan, "readyDeadlineSeconds", READY_DEADLINE_DEFAULT_SECONDS,
                                             READY_DEADLINE_BOUNDS),
            "faultAfterAcks": _bounded(plan, "faultAfterAcks", FAULT_AFTER_ACKS_DEFAULT, FAULT_AFTER_ACKS_BOUNDS),
            "postResumeSeconds": az_outage.post_resume_seconds(plan)}


def voter(plan: dict, pod: str) -> dict:
    return next(v for v in plan["voters"] if v["pod"] == pod)


def check_ports(ports: list[int]) -> list[int]:
    """Bind every local tunnel port on 127.0.0.1; Windows can exclude whole ranges."""
    for port in ports:
        probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            probe.bind(("127.0.0.1", port))
        except OSError as exc:
            raise PowerLossError(f"local_port_unavailable:{port}") from exc
        finally:
            probe.close()
    return ports


def _instance_id(node: dict | None) -> str:
    return str((node or {}).get("spec", {}).get("providerID", "")).rsplit("/", 1)[-1]


DIGEST = re.compile(r"sha256:[0-9a-f]{64}")
DRAIN_FAILED = "FAILED"
TIMELINE_LIMIT = 120
DIAGNOSTIC_INTERVAL_SECONDS = 5


def broker_status(pod: dict | None) -> dict:
    return next((s for s in (pod or {}).get("status", {}).get("containerStatuses", []) if s.get("name") == "narumq"), {})


def image_digest(pod: dict | None) -> str | None:
    match = DIGEST.search(str(broker_status(pod).get("imageID", "")))
    return match.group(0) if match else None


def container_diagnostics(pod: dict) -> dict:
    """Pod UID, restart count and the last container termination (reason, exit code, time) of the broker container."""
    status = broker_status(pod)
    last = (status.get("lastState") or {}).get("terminated") or {}
    state = status.get("state") or {}
    return {"podUID": pod["metadata"]["uid"], "restartCount": status.get("restartCount"),
            "containerReady": status.get("ready"), "podReady": ready(pod),
            "lastTermination": {"reason": last.get("reason"), "exitCode": last.get("exitCode"),
                                "startedAt": last.get("startedAt"), "finishedAt": last.get("finishedAt")} if last else None,
            "runningSince": (state.get("running") or {}).get("startedAt"),
            "waitingReason": (state.get("waiting") or {}).get("reason")}


def node_diagnostics(row: dict | None) -> dict | None:
    """The status fields of one `/api/v1/nodes` row; None when the API did not list the node."""
    if not isinstance(row, dict):
        return None
    return {key: row.get(key) for key in ("state", "ready", "readinessReason", "uptimeMillis", "role",
                                          "membershipPhase", "queryStatus", "lastAppliedIndex")}


def read_drain_record(fixture, node_id: str, port: int) -> dict:
    """`GET /api/v1/nodes/{id}/drain`: 404 is no record. Only phase and state are kept."""
    try:
        data = fixture.api(f"/api/v1/nodes/{node_id}/drain", port)
    except Exception as exc:
        if getattr(exc, "code", None) == 404:
            return {"present": False}
        return {"present": None, "errorCode": type(exc).__name__}
    data = data if isinstance(data, dict) else {}
    return {"present": True, "phase": data.get("phase"), "state": data.get("state"),
            "hasFailure": bool(data.get("failure"))}


def diagnostics_complete(snapshot: dict) -> bool:
    """Every voter has Pod UID, restart count and node state, readiness reason key and uptime."""
    if not snapshot.get("apiReachable"):
        return False
    for entry in snapshot["voters"].values():
        container, node = entry.get("container"), entry.get("node")
        if not container or not isinstance(container.get("restartCount"), int) or not container.get("podUID"):
            return False
        if not node or not isinstance(node.get("state"), str) or not isinstance(node.get("uptimeMillis"), int):
            return False
    return True


def preflight(fixture, plan: dict, port: int = PUBLISHER_PORTS[0]) -> dict:
    """Read-only checks. Nothing is stopped unless exactly the three planned voters, workers and volumes are healthy."""
    if fixture.aws("sts", "get-caller-identity")["Account"] != ACCOUNT:
        raise PowerLossError("account_mismatch")
    ownership = require_owned_fixture(fixture, plan, plan["instanceIds"])
    if fixture.get("namespace", "narumq")["metadata"].get("labels", {}).get("narumq.io/acceptance-run") != fixture.run_id:
        raise PowerLossError("namespace_not_owned")
    cluster = fixture.api("/api/v1/cluster", port)
    if (cluster.get("clusterId") != fixture.run_id or cluster.get("quorumAvailable") is not True
            or cluster.get("nodesByState", {}).get("ready") != 3):
        raise PowerLossError("cluster_identity_quorum_or_three_ready")
    pods = {p["metadata"]["name"]: p for p in fixture.get("pods", "-n", "narumq", "-l", "app=narumq")["items"]}
    nodes = {n["metadata"]["name"]: n for n in fixture.get("nodes")["items"]}
    if set(pods) != {v["pod"] for v in plan["voters"]}:
        raise PowerLossError("broker_pods_differ_from_plan")
    for planned in plan["voters"]:
        pod, node = pods[planned["pod"]], nodes.get(planned["workerName"])
        if (pod["metadata"]["uid"] != planned["podUID"] or pod["spec"].get("nodeName") != planned["workerName"]
                or pod["metadata"].get("deletionTimestamp") or not ready(pod) or not node or not ready(node)
                or _instance_id(node) != planned["instanceId"]):
            raise PowerLossError(f"voter_identity_changed:{planned['pod']}")
        pvc = fixture.get("pvc", planned["pvcName"], "-n", "narumq")
        pv = fixture.get("pv", pvc["spec"]["volumeName"])
        if (pvc["metadata"]["uid"] != planned["pvcUID"] or pv["metadata"]["name"] != planned["pvName"]
                or pv["spec"]["csi"]["volumeHandle"] != planned["volumeId"]):
            raise PowerLossError(f"data_identity_mismatch:{planned['pod']}")
        volume = fixture.aws("ec2", "describe-volumes", "--volume-ids", planned["volumeId"])["Volumes"][0]
        attached = [a.get("InstanceId") for a in volume.get("Attachments", [])]
        if attached != [planned["instanceId"]]:
            raise PowerLossError(f"volume_attachment_mismatch:{planned['pod']}")
        try:
            fixture.api(f"/api/v1/nodes/{planned['nodeId']}/drain", port)
        except Exception as exc:
            if getattr(exc, "code", None) != 404:
                raise PowerLossError(f"drain_record_unreadable:{planned['pod']}") from exc
        else:
            raise PowerLossError(f"drain_record_present:{planned['pod']}")
    described = fixture.aws("ec2", "describe-instances", "--instance-ids", *plan["instanceIds"])
    instances = [i for r in described["Reservations"] for i in r["Instances"]]
    if {i["InstanceId"] for i in instances} != set(plan["instanceIds"]) or not all(
            i["State"]["Name"] == "running" and i["VpcId"] == plan["vpcId"] for i in instances):
        raise PowerLossError("voter_instances_not_running_in_fixture_vpc")
    for instance in instances:
        group = next((t["Value"] for t in instance.get("Tags", []) if t["Key"] == "aws:autoscaling:groupName"), None)
        if group not in plan["asgNames"]:
            raise PowerLossError("voter_instance_outside_planned_asgs")
    groups = fixture.aws("autoscaling", "describe-auto-scaling-groups",
                         "--auto-scaling-group-names", *plan["asgNames"])["AutoScalingGroups"]
    if {g["AutoScalingGroupName"] for g in groups} != set(plan["asgNames"]) or not all(
            any(t["Key"] == "eks:cluster-name" and t["Value"] == plan["clusterName"] for t in g.get("Tags", []))
            for g in groups):
        raise PowerLossError("asg_missing_or_not_owned")
    digests = {p["pod"]: image_digest(pods[p["pod"]]) for p in plan["voters"]}
    if not all(digests.values()):
        raise PowerLossError("image_digest_unavailable")
    if len(set(digests.values())) != 1 or plan.get("imageDigest") not in (None, next(iter(digests.values()))):
        raise PowerLossError("image_digest_differs_between_voters_or_from_plan")
    return {"at": timestamp(), "account": ACCOUNT, "clusterName": plan["clusterName"],
            "ownership": ownership,
            "voters": [{k: v[k] for k in ("pod", "nodeId", "workerName", "instanceId")} for v in plan["voters"]],
            "asgNames": plan["asgNames"], "image": {"digests": digests, "digest": next(iter(digests.values())),
                                                    "note": "images built from commit 8096f30 or later carry the startup fix"},
            "restartCounts": {p["pod"]: broker_status(pods[p["pod"]]).get("restartCount") for p in plan["voters"]}}


def torn_tail_log(logs: str | None) -> dict:
    """What the broker log says about a WAL tail cut; the whole log is never kept."""
    if logs is None:
        return {"logRead": False, "logged": False, "lines": []}
    lines, fields = [], {}
    for line in logs.splitlines():
        if TORN_TAIL_MESSAGE in line or TORN_TAIL_FALLBACK.search(line):
            lines.append(line.strip()[:300])
            if not fields:
                try:
                    record = json.loads(line)
                except ValueError:
                    record = {}
                if isinstance(record, dict):
                    fields = {key: record[key] for key in TORN_TAIL_FIELDS if key in record}
    return {"logRead": True, "logged": bool(lines), "lines": lines[:3], "fields": fields,
            "exactMessage": any(TORN_TAIL_MESSAGE in line for line in lines)}


def fetch_log(fixture, pod: str, since: str) -> str | None:
    args = fixture.kubectl + ["-n", "narumq", "logs", pod, "-c", "narumq", "--since-time", since]
    try:
        current = fixture.command(args, timeout=60)
    except (RuntimeError, subprocess.TimeoutExpired):
        return None
    try:
        previous = fixture.command(args + ["--previous"], timeout=60)
    except (RuntimeError, subprocess.TimeoutExpired):
        previous = ""
    return current + "\n" + previous


def aggregate(criteria: dict, failed_step: str | None) -> dict:
    """PASS only when every named criterion is exactly True and no step failed."""
    verdicts = {name: criteria.get(name) is True for name in CRITERIA}
    failed = [name for name, ok in verdicts.items() if not ok]
    return {"result": "PASS" if not failed and failed_step is None else "FAIL", "criteria": verdicts,
            "failedCriteria": failed, "failedStep": failed_step}


class PowerLossRun:
    def __init__(self, fixture, plan: dict, private: Path, output: Path, sleep: Callable = time.sleep,
                 tunnel_factory=PodTunnel, streams=durable_stream, oracle_factory: Callable | None = None):
        self.fixture = fixture
        self.plan = plan
        self.private = private
        self.output = output
        self.sleep = sleep
        self.settings = validate_plan(plan, fixture.run_id)
        self.tunnel_factory = tunnel_factory
        self.streams = streams
        self.oracle_factory = oracle_factory or self._oracle
        self.criteria: dict[str, bool] = {}
        self.evidence: dict = {"voterPods": [v["pod"] for v in plan["voters"]]}
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

    def _oracle(self):
        import payload_oracle
        return payload_oracle.Oracle(self.plan["runId"], self.private, self.output)

    def mark(self, name: str) -> None:
        self.marks[name] = time.monotonic()
        self.times[name] = timestamp()
        self.evidence["timings"] = {"at": dict(self.times),
                                    "secondsSinceStopRequest": {k: round(v - self.marks["stopRequested"], 3)
                                                                for k, v in self.marks.items()
                                                                if "stopRequested" in self.marks}}

    def record(self, name: str, value) -> None:
        self.evidence[name] = value
        save(self.output, f"power-loss-{name}.json", value)

    def wait_until(self, probe: Callable, timeout: float, interval: float):
        deadline = time.monotonic() + timeout
        while True:
            value = probe()
            if value:
                return value
            if time.monotonic() >= deadline:
                return None
            self.sleep(interval)

    def pods(self) -> dict[str, dict]:
        return {p["metadata"]["name"]: p for p in self.fixture.get("pods", "-n", "narumq", "-l", "app=narumq")["items"]}

    # -- steps --------------------------------------------------------------------------------------------

    def open_publisher_tunnel(self) -> None:
        self.step = "publisher_tunnel"
        planned = voter(self.plan, self.plan["publisherPod"])
        self.publisher_tunnel = self.tunnel_factory(self.fixture, planned["pod"], planned["podUID"], *PUBLISHER_PORTS)
        self.record("publisher-endpoint", self.publisher_tunnel.start())

    def step_preflight(self) -> None:
        self.step = "preflight"
        result = preflight(self.fixture, self.plan)
        self.restart_baseline = {pod: count for pod, count in result["restartCounts"].items() if isinstance(count, int)}
        self.criteria["baseline.imageDigestRecorded"] = bool(result["image"]["digest"])
        self.record("preflight", result)

    def step_baseline(self) -> None:
        """Persistent session, retained marker and the durable subscriber exist before anything is stopped."""
        self.step = "baseline"
        plan = self.plan
        subscriber = voter(plan, plan["subscriberPod"])
        tunnel = self.tunnel_factory(self.fixture, subscriber["pod"], subscriber["podUID"], *SUBSCRIBER_PORTS)
        evidence = {"startedAt": timestamp()}
        oracle = self.oracle_factory()
        try:
            evidence["subscriberTunnel"] = tunnel.start()
            oracle.prepare(SUBSCRIBER_PORTS[1], PUBLISHER_PORTS[1])
            evidence["durablePrepare"] = self.streams.prepare_subscriber(
                self.private, plan["runId"], plan["durableScenario"], SUBSCRIBER_PORTS[1], self.output)
        finally:
            tunnel.close()
        self.criteria["baseline.marker"] = True
        evidence["retainedSha256"] = hashlib.sha256(oracle.retained).hexdigest()
        rows = {row["nodeId"]: row for row in self.fixture.api("/api/v1/nodes?limit=1000", PUBLISHER_PORTS[0])}
        applied = {node_id: row.get("lastAppliedIndex") for node_id, row in rows.items()}
        if not all(isinstance(value, int) and not isinstance(value, bool) for value in applied.values()):
            raise PowerLossError("applied_index_unavailable")
        self.baseline = {"lastAppliedIndex": applied, "maxLastAppliedIndex": max(applied.values()),
                         "retainedSha256": evidence["retainedSha256"]}
        evidence["baseline"] = self.baseline
        self.record("baseline", evidence)

    def suspend_autoscaling(self) -> None:
        self.step = "suspend_autoscaling"
        asg = {"baselineSuspended": {}, "suspendedByTool": self.suspended_by_tool}
        self.evidence["asg"] = asg
        for group in self.plan["asgNames"]:
            asg["baselineSuspended"][group] = sorted(az_outage.suspended_processes(self.fixture, group))
        for group in self.plan["asgNames"]:
            ours = [p for p in PROCESSES if p not in asg["baselineSuspended"][group]]
            # Recorded before the request so the resume covers a request whose outcome is unknown.
            self.suspended_by_tool[group] = ours
            if ours:
                self.fixture.command(["aws", "autoscaling", "suspend-processes", "--auto-scaling-group-name", group,
                                      "--scaling-processes", *ours])
            if not set(PROCESSES) <= az_outage.suspended_processes(self.fixture, group):
                raise PowerLossError("autoscaling_not_suspended")

    def start_publisher(self) -> None:
        self.step = "publisher"
        plan = self.plan
        interval, max_messages = durable_stream.plan_settings(plan)
        self.publisher = self.streams.DurablePublisher(self.private, plan["runId"], plan["durableScenario"],
                                                       PUBLISHER_PORTS[1], self.output, interval=interval,
                                                       max_messages=max_messages)
        self.publisher.start()
        self.evidence["publisherStartedAt"] = timestamp()
        if not self.publisher.wait_for_acked(self.settings["faultAfterAcks"], 60):
            raise PowerLossError("publisher_has_no_acked_baseline")

    def stop_all(self) -> None:
        """ONE stop call for all three instances, requested while the publisher is still sending."""
        self.step = "stop"
        ids = self.plan["instanceIds"]
        acked = sum(e["outcome"] == "acked" for e in list(self.publisher.entries))
        capped = self.publisher.capped_at is not None
        self.publisher_tunnel.check()
        self.mark("stopRequested")
        response = self.fixture.aws("ec2", "stop-instances", "--instance-ids", *ids, "--skip-os-shutdown")
        self.criteria["fault.singleStopRequest"] = {i["InstanceId"] for i in response["StoppingInstances"]} == set(ids)
        save(self.output, "power-loss-stop-request.json",
             {"at": self.times["stopRequested"], "skipOsShutdown": True, "instanceIds": ids, "oneCall": True,
              "ackedBeforeRequest": acked, "publisherCappedBeforeRequest": capped})
        self.criteria["fault.publishingUnderLoad"] = acked >= self.settings["faultAfterAcks"] and not capped
        if not self.criteria["fault.singleStopRequest"]:
            raise PowerLossError("unexpected_stop_response")
        self.fixture.command(["aws", "ec2", "wait", "instance-stopped", "--instance-ids", *ids], timeout=WAIT_TIMEOUT_SECONDS)
        states = {i["InstanceId"]: i["State"]["Name"] for r in
                  self.fixture.aws("ec2", "describe-instances", "--instance-ids", *ids)["Reservations"]
                  for i in r["Instances"]}
        self.criteria["fault.allStopped"] = states == {i: "stopped" for i in ids}
        if not self.criteria["fault.allStopped"]:
            raise PowerLossError("instances_not_all_stopped")
        self.mark("stopped")
        self.evidence["publisher"] = self.publisher.stop()
        self.publisher_tunnel.close()

    def hold_and_start(self) -> None:
        self.step = "hold"
        end = self.marks["stopped"] + self.settings["holdSeconds"]
        while time.monotonic() < end:
            self.sleep(1)
        self.step = "start"
        ids = self.plan["instanceIds"]
        self.mark("startRequested")
        response = self.fixture.aws("ec2", "start-instances", "--instance-ids", *ids)
        if {i["InstanceId"] for i in response["StartingInstances"]} != set(ids):
            raise PowerLossError("unexpected_start_response")
        self.fixture.command(["aws", "ec2", "wait", "instance-running", "--instance-ids", *ids], timeout=WAIT_TIMEOUT_SECONDS)
        self.mark("running")

    def diagnostics_available(self, pods: dict[str, dict]) -> bool:
        """Keep a status tunnel to any Ready voter. It only reads /api/v1; nothing here calls /health."""
        if self.diag_tunnel is not None:
            try:
                self.diag_tunnel.check()
                return True
            except Exception:
                self.close_diag()
        for planned in self.plan["voters"]:
            pod = pods.get(planned["pod"])
            if not pod or not ready(pod) or pod["metadata"].get("deletionTimestamp"):
                continue
            tunnel = self.tunnel_factory(self.fixture, planned["pod"], pod["metadata"]["uid"], *PUBLISHER_PORTS)
            try:
                tunnel.start()
            except Exception:
                continue
            self.diag_tunnel = tunnel
            return True
        return False

    def close_diag(self) -> None:
        tunnel, self.diag_tunnel = self.diag_tunnel, None
        if tunnel is not None:
            try:
                tunnel.close()
            except Exception:
                pass

    def snapshot(self, pods: dict[str, dict], force_drain: bool = False) -> dict:
        """Per voter: Pod and container facts, node state from the status API, and the drain record when it matters.

        The drain record is read when the container restarted since preflight, the node is DRAINING, a record was
        seen earlier, or `force_drain` is set. COMPLETED is only waited on; FAILED is remembered and fails the run.
        The record is never deleted and /health/ready is never called.
        """
        api = self.diagnostics_available(pods)
        rows: dict[str, dict] = {}
        if api:
            try:
                rows = {str(row.get("nodeId")): row for row in self.fixture.api("/api/v1/nodes?limit=1000", PUBLISHER_PORTS[0])}
            except Exception:
                api = False
        voters = {}
        for planned in self.plan["voters"]:
            pod = pods.get(planned["pod"])
            entry = {"pod": planned["pod"], "container": container_diagnostics(pod) if pod else None,
                     "node": node_diagnostics(rows.get(planned["nodeId"]))}
            count = (entry["container"] or {}).get("restartCount")
            base = self.restart_baseline.get(planned["pod"])
            entry["restartedSincePreflight"] = isinstance(count, int) and isinstance(base, int) and count > base
            draining = (entry["node"] or {}).get("state") == "DRAINING"
            if api and (entry["restartedSincePreflight"] or draining or planned["nodeId"] in self.drain_seen or force_drain):
                record = read_drain_record(self.fixture, planned["nodeId"], PUBLISHER_PORTS[0])
                entry["drainRecord"] = record
                if record.get("present") is True:
                    self.drain_seen[planned["nodeId"]] = record.get("phase")
                    if record.get("phase") == DRAIN_FAILED and self.failed_drain is None:
                        self.failed_drain = {"pod": planned["pod"], "nodeId": planned["nodeId"], "at": timestamp()}
                elif record.get("present") is False:
                    self.drain_seen.pop(planned["nodeId"], None)
            voters[planned["nodeId"]] = entry
        return {"at": timestamp(), "apiReachable": api, "voters": voters}

    def note(self, snapshot: dict) -> None:
        """Keep the first snapshot and every change; the list is bounded."""
        signature = [[node_id, v["container"] and [v["container"]["podUID"], v["container"]["restartCount"],
                                                    v["container"]["podReady"]],
                      v["node"] and [v["node"]["state"], v["node"]["readinessReason"]],
                      (v.get("drainRecord") or {}).get("phase")] for node_id, v in snapshot["voters"].items()]
        if signature == self.last_signature:
            return
        self.last_signature = signature
        self.timeline.append(snapshot)
        if len(self.timeline) > TIMELINE_LIMIT:
            del self.timeline[1]

    def save_voters(self, final: dict | None) -> None:
        self.voters_saved = True
        self.record("voters", {"timeline": self.timeline, "final": final, "drainRecordsSeen": self.drain_seen,
                               "failedDrainRecord": self.failed_drain, "healthReadyCalled": False,
                               "note": "container and node facts are read-only; the drain record is never deleted"})

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
            ok = self.wait_until(three_ready, max(deadline - elapsed, 0), DIAGNOSTIC_INTERVAL_SECONDS)
            within = bool(ok) and time.monotonic() - self.marks["startRequested"] <= deadline
            self.criteria["recovery.threeReadyWithinBound"] = within
            if ok:
                self.mark("threeReady")
            if not within:
                raise PowerLossError("three_voters_not_ready_within_bound")
            subscriber = self.pods()[self.plan["subscriberPod"]]
            self.subscriber_tunnel = self.tunnel_factory(self.fixture, self.plan["subscriberPod"],
                                                         subscriber["metadata"]["uid"], *SUBSCRIBER_PORTS)
            self.subscriber_tunnel.start()

            def quorum():
                cluster = self.fixture.api("/api/v1/cluster", SUBSCRIBER_PORTS[0])
                return cluster if cluster["quorumAvailable"] is True and cluster["nodesByState"].get("ready") == 3 else None

            remaining = max(deadline - (time.monotonic() - self.marks["startRequested"]), 0)
            cluster = self.wait_until(quorum, remaining, 5)
            self.criteria["recovery.quorumAndThreeReady"] = cluster is not None
            final = self.snapshot(self.pods(), force_drain=True)
            self.note(final)
            self.criteria["recovery.voterDiagnosticsRecorded"] = diagnostics_complete(final)
            self.criteria["recovery.drainRecordsCleared"] = bool(final["apiReachable"]) and all(
                (v.get("drainRecord") or {}).get("present") is False for v in final["voters"].values())
            recovery = {"podUIDs": {name: p["metadata"]["uid"] for name, p in self.pods().items()},
                        "podUIDChanged": {v["pod"]: self.pods()[v["pod"]]["metadata"]["uid"] != v["podUID"]
                                          for v in self.plan["voters"]},
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

    def step_verify(self) -> None:
        self.step = "verify"
        plan = self.plan
        result = self.streams.verify_subscriber(self.private, plan["runId"], plan["durableScenario"],
                                                SUBSCRIBER_PORTS[1], self.output, "after-power-loss")
        stats = self.evidence.get("publisher", {})
        self.criteria["durable.streamOracle"] = (
            result.get("result") == "PASS" and result.get("sessionPresent") is True
            and not result.get("missingAcked") and result.get("acked", 0) >= self.settings["faultAfterAcks"])
        self.record("durable", {key: result.get(key) for key in (
            "result", "label", "sessionPresent", "acked", "rejected", "unknown", "missingAcked", "rejectedDelivered",
            "unknownDelivered", "duplicates", "errorCode", "timedOut")} | {"publisher": stats})
        oracle = self.oracle_factory()
        evidence = {"expectedRetainedSha256": self.baseline["retainedSha256"]}
        try:
            oracle.verify(SUBSCRIBER_PORTS[1])
            evidence["sessionAndOfflineMessages"] = True
            self.criteria["durable.offlineSession"] = True
            self.criteria["durable.retainedMarkerSha256"] = True
        except Exception as exc:
            evidence.update(sessionAndOfflineMessages=False, errorCode=type(exc).__name__)
            self.criteria["durable.offlineSession"] = False
            self.criteria["durable.retainedMarkerSha256"] = False
        self.record("oracle", evidence)

    def step_observe(self) -> None:
        """Per node: was a WAL tail cut logged, and did it catch up to the pre-loss applied index."""
        self.step = "observe"
        nodes = {}
        for planned in self.plan["voters"]:
            pod = self.pods()[planned["pod"]]
            status = next((s for s in pod.get("status", {}).get("containerStatuses", []) if s.get("name") == "narumq"), {})
            nodes[planned["nodeId"]] = {"pod": planned["pod"], "restartCount": status.get("restartCount"),
                                        "tornTail": torn_tail_log(fetch_log(self.fixture, planned["pod"], self.times["stopRequested"]))}
        self.criteria["observe.tornTailRecordedPerNode"] = all(n["tornTail"]["logRead"] for n in nodes.values())

        def applied():
            rows = {row["nodeId"]: row for row in self.fixture.api("/api/v1/nodes?limit=1000", SUBSCRIBER_PORTS[0])}
            values = {node_id: rows.get(node_id, {}).get("lastAppliedIndex") for node_id in nodes}
            ok = all(isinstance(v, int) and not isinstance(v, bool) and v >= self.baseline["maxLastAppliedIndex"]
                     for v in values.values())
            return values if ok else None

        reached = self.wait_until(applied, APPLIED_INDEX_WAIT_SECONDS, 5)
        self.criteria["observe.appliedIndexAtLeastBaseline"] = reached is not None
        self.record("nodes", {"nodes": nodes, "tornTailLogMessage": TORN_TAIL_MESSAGE,
                              "anyTornTailLogged": any(n["tornTail"]["logged"] for n in nodes.values()),
                              "baselineMaxLastAppliedIndex": self.baseline["maxLastAppliedIndex"],
                              "lastAppliedIndexAfter": reached,
                              "note": "no log line means no cut was logged; a broker without this log cannot say"})

    # Scenarios: cluster-session-and-delivery.md:CLU-SCN-036, cluster-session-and-delivery.md:CLU-SCN-140, cluster-session-and-delivery.md:CLU-SCN-141, cluster-session-and-delivery.md:CLU-SCN-142
    def execute(self) -> None:
        check_ports([*PUBLISHER_PORTS, *SUBSCRIBER_PORTS])
        self.open_publisher_tunnel()
        self.step_preflight()
        self.step_baseline()
        try:
            self.suspend_autoscaling()
            self.start_publisher()
            self.stop_all()
            self.hold_and_start()
            self.step_recovery()
            self.step_verify()
            self.step_observe()
        finally:
            self.finish_autoscaling()

    def finish_autoscaling(self) -> None:
        """Mark restarted instances Healthy, resume exactly what this run suspended, then watch for scaling."""
        asg = self.evidence.setdefault("asg", {"suspendedByTool": self.suspended_by_tool})
        ids = self.plan["instanceIds"]
        if "stopRequested" in self.marks and "startRequested" not in self.marks:
            # Resuming Auto Scaling while the voters are stopped would let it replace them: start them first.
            try:
                self.mark("startRequested")
                self.fixture.aws("ec2", "start-instances", "--instance-ids", *ids)
                self.fixture.command(["aws", "ec2", "wait", "instance-running", "--instance-ids", *ids],
                                     timeout=WAIT_TIMEOUT_SECONDS)
                asg["startedByCleanup"] = True
            except Exception as exc:
                asg["startedByCleanup"] = False
                asg["cleanupStartErrorCode"] = type(exc).__name__
                self.criteria["asg.resumed"] = False
                self.criteria["asg.noScalingAfterResume"] = False
                asg["resume"] = {"failed": list(self.suspended_by_tool), "retainedForSafety": True}
                save(self.output, "power-loss-asg-resume.json", asg)
                return
        if "stopRequested" in self.marks:
            try:
                states = {item["InstanceId"]: item["State"]["Name"] for reservation in
                          self.fixture.aws("ec2", "describe-instances", "--instance-ids", *ids)["Reservations"]
                          for item in reservation["Instances"]}
                all_running = states == {instance: "running" for instance in ids}
            except Exception:
                all_running = False
            if not all_running:
                self.criteria["asg.resumed"] = False
                self.criteria["asg.noScalingAfterResume"] = False
                asg["resume"] = {"failed": list(self.suspended_by_tool), "retainedForSafety": True,
                                 "reason": "planned instances are not confirmed running"}
                save(self.output, "power-loss-asg-resume.json", asg)
                return
        self.resumed_from = timestamp()
        if "startRequested" in self.marks:
            try:
                asg["health"] = az_outage.restore_health(self.fixture, self.plan, ids)
                save(self.output, "power-loss-asg-health.json", asg["health"])
            except Exception as exc:
                asg["health"] = {"errorCode": type(exc).__name__}
        if asg.get("health", {}).get("unrestored") or asg.get("health", {}).get("errorCode"):
            self.criteria["asg.resumed"] = False
            self.criteria["asg.noScalingAfterResume"] = False
            asg["resume"] = {"failed": list(self.suspended_by_tool), "retainedForSafety": True}
            save(self.output, "power-loss-asg-resume.json", asg)
            return
        asg["resume"] = az_outage.resume_suspended(self.fixture, self.suspended_by_tool)
        self.criteria["asg.resumed"] = not asg["resume"]["failed"]
        save(self.output, "power-loss-asg-resume.json", asg)
        health = asg.get("health", {})
        if "asgInstances" not in health or "recovery" not in self.evidence:
            self.criteria["asg.noScalingAfterResume"] = False
            asg["postResume"] = {"result": "FAIL", "reasons": ["no ASG instance baseline or recovery point"]}
            return
        asg["postResume"] = az_outage.observe_post_resume(
            self.fixture, self.plan, self.settings["postResumeSeconds"], self.resumed_from,
            {group: set(items) for group, items in health["asgInstances"].items()},
            self.evidence["recovery"]["podUIDs"], self.output)
        self.criteria["asg.noScalingAfterResume"] = asg["postResume"]["result"] == "PASS"

    def cleanup(self, report: dict) -> None:
        for name, action in (("publisher", lambda: self.publisher and self.publisher.stop()),
                             ("publisherTunnel", lambda: self.publisher_tunnel and self.publisher_tunnel.close()),
                             ("subscriberTunnel", lambda: self.subscriber_tunnel and self.subscriber_tunnel.close()),
                             ("statusTunnel", self.close_diag)):
            try:
                action()
            except Exception as exc:
                report.setdefault("teardownErrors", {})[name] = type(exc).__name__
                report["teardownFailed"] = True


def run_cli(args) -> int:
    """Entry point of `main.py power-loss`: the output directory is created here."""
    args.output.mkdir(parents=True, exist_ok=False)
    run_id = args.output.name
    report = {"result": "FAIL", "mode": "power-loss", "runId": run_id, "startedAt": timestamp()}
    run = None
    try:
        fixture = Fixture(args.private, run_id)
        plan = json.loads(args.plan.read_text(encoding="utf-8"))
        run = PowerLossRun(fixture, plan, args.private, args.output)
        report["settings"] = run.settings
        run.execute()
    except PowerLossError as exc:
        report["errorCode"] = exc.code
    except Exception as exc:
        report["errorCode"] = type(exc).__name__
    finally:
        if run is not None:
            run.cleanup(report)
            report["timings"] = run.evidence.get("timings")
            report["failedStep"] = run.step if "errorCode" in report else None
            judged = aggregate(run.criteria, report["failedStep"])
        else:
            judged = aggregate({}, "plan")
        if report.get("teardownFailed"):
            judged["result"] = "FAIL"
        report.update(judged)
        report["scope"] = ("compute power-loss model: one stop-instances --skip-os-shutdown call for all three voters; "
                           "EBS internal write-cache loss is not reproduced")
        report["finishedAt"] = timestamp()
        save(args.output, "summary.json", report)
    print(json.dumps(report))
    return 0 if report["result"] == "PASS" else 1
