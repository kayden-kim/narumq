"""Single-AZ compute power interruption model for a disposable EKS fixture (DEP-20).

The model stops every worker EC2 of one AZ without an OS shutdown, holds them stopped, then starts
the same instances. AZ network, EBS service and EKS control plane failures are not modelled.
"""
from __future__ import annotations

import datetime
from pathlib import Path
import time
from typing import Callable

from durable_stream import plan_settings
from fixture import ready, save
from observer import timestamp

ACCOUNT = "809446814769"
PROCESSES = ("Launch", "Terminate", "ReplaceUnhealthy", "AZRebalance", "HealthCheck")
POST_RESUME_DEFAULT_SECONDS = 180
POST_RESUME_MIN_SECONDS = 60
POST_RESUME_MAX_SECONDS = 900
POST_RESUME_INTERVAL_SECONDS = 10
HEALTH_WAIT_SECONDS = 120
HOLD_DEFAULT_SECONDS = 480
HOLD_MIN_SECONDS = 60
HOLD_MAX_SECONDS = 1800
RECOVERY_DEADLINE_SECONDS = 900
ZONE_LABEL = "topology.kubernetes.io/zone"
STRING_KEYS = ("runId", "clusterName", "vpcId", "zone", "targetPod", "targetNodeId", "oldPodUID", "oldWorkerName",
               "oldInstanceId", "pvcName", "pvcUID", "pvName", "volumeId", "observerPod", "observerPodUID",
               "observerWorkerName", "observerInstanceId", "durableScenario")
LIST_KEYS = ("instanceIds", "asgNames")


def validate_plan(plan: dict) -> int:
    """Check the plan shape and return the hold duration in seconds."""
    if not all(isinstance(plan.get(key), str) and plan[key] for key in STRING_KEYS):
        raise ValueError("incomplete az-outage plan")
    for key in LIST_KEYS:
        value = plan.get(key)
        if (not isinstance(value, list) or not value or len(set(value)) != len(value)
                or not all(isinstance(item, str) and item for item in value)):
            raise ValueError(f"{key} must be a non-empty list of distinct strings")
    hold = plan.get("holdSeconds", HOLD_DEFAULT_SECONDS)
    if isinstance(hold, bool) or not isinstance(hold, int) or not HOLD_MIN_SECONDS <= hold <= HOLD_MAX_SECONDS:
        raise ValueError(f"holdSeconds must be an integer within [{HOLD_MIN_SECONDS}, {HOLD_MAX_SECONDS}]")
    if plan["oldInstanceId"] not in plan["instanceIds"]:
        raise ValueError("instanceIds must contain the target worker instance")
    if plan["observerPod"] == plan["targetPod"] or plan["observerInstanceId"] in plan["instanceIds"]:
        raise ValueError("observer must be outside the target AZ instances")
    plan_settings(plan)
    post_resume_seconds(plan)
    return hold


def post_resume_seconds(plan: dict) -> int:
    seconds = plan.get("postResumeSeconds", POST_RESUME_DEFAULT_SECONDS)
    if (isinstance(seconds, bool) or not isinstance(seconds, int)
            or not POST_RESUME_MIN_SECONDS <= seconds <= POST_RESUME_MAX_SECONDS):
        raise ValueError(f"postResumeSeconds must be an integer within "
                         f"[{POST_RESUME_MIN_SECONDS}, {POST_RESUME_MAX_SECONDS}]")
    return seconds


def _instance_id(node: dict) -> str:
    return str(node.get("spec", {}).get("providerID", "")).rsplit("/", 1)[-1]


def _zone(node: dict | None) -> str | None:
    return None if node is None else node.get("metadata", {}).get("labels", {}).get(ZONE_LABEL)


def preflight(fixture, plan: dict) -> dict:
    """Refuse to stop anything unless the AZ holds exactly the planned workers and one follower."""
    validate_plan(plan)
    if plan["runId"] != fixture.run_id or not plan["clusterName"].startswith("narumq-ha-acceptance-"):
        raise ValueError("plan does not identify this disposable fixture")
    if fixture.aws("sts", "get-caller-identity")["Account"] != ACCOUNT:
        raise ValueError("AWS account differs from acceptance account")
    namespace = fixture.get("namespace", "narumq")
    if namespace["metadata"].get("labels", {}).get("narumq.io/acceptance-run") != fixture.run_id:
        raise ValueError("namespace is not owned by this synthetic fixture")
    cluster = fixture.api("/api/v1/cluster")
    if (cluster["clusterId"] != fixture.run_id or cluster["quorumAvailable"] is not True
            or str(cluster["leaderNodeId"]) == plan["targetNodeId"]):
        raise ValueError("cluster must have quorum and the target must be a follower")
    pods = fixture.get("pods", "-n", "narumq", "-l", "app=narumq")["items"]
    if len(pods) != 3 or not all(ready(p) for p in pods):
        raise ValueError("three Ready voters are required before the outage")
    nodes = {n["metadata"]["name"]: n for n in fixture.get("nodes")["items"]}
    zones = {p["metadata"]["name"]: _zone(nodes.get(p["spec"].get("nodeName"))) for p in pods}
    target = next((p for p in pods if p["metadata"]["name"] == plan["targetPod"]), None)
    if (target is None or target["metadata"]["uid"] != plan["oldPodUID"]
            or target["spec"].get("nodeName") != plan["oldWorkerName"]
            or _instance_id(nodes.get(plan["oldWorkerName"]) or {}) != plan["oldInstanceId"]):
        raise ValueError("outage target identity changed")
    if zones[plan["targetPod"]] != plan["zone"]:
        raise ValueError("target Pod is not in the outage zone")
    in_zone = sorted(name for name, zone in zones.items() if zone is None or zone == plan["zone"])
    if in_zone != [plan["targetPod"]]:
        # The leader is not the target, so the leader and the observer are the other two Pods.
        raise ValueError("only the target follower may run in the outage zone; leader and observer must be elsewhere")
    observer = next((p for p in pods if p["metadata"]["name"] == plan["observerPod"]), None)
    if (observer is None or observer["metadata"]["uid"] != plan["observerPodUID"]
            or observer["spec"].get("nodeName") != plan["observerWorkerName"]):
        raise ValueError("observer Pod identity changed")
    running = fixture.aws("ec2", "describe-instances", "--filters", f"Name=tag:eks:cluster-name,Values={plan['clusterName']}",
                          f"Name=availability-zone,Values={plan['zone']}", "Name=instance-state-name,Values=running")
    instances = [i for r in running["Reservations"] for i in r["Instances"]]
    if {i["InstanceId"] for i in instances} != set(plan["instanceIds"]):
        raise ValueError("running instances in the zone differ from the planned instanceIds")
    for instance in instances:
        group = next((t["Value"] for t in instance.get("Tags", []) if t["Key"] == "aws:autoscaling:groupName"), None)
        if instance["VpcId"] != plan["vpcId"] or group not in plan["asgNames"]:
            raise ValueError("planned instance is outside the fixture VPC or its Auto Scaling groups")
    extra = sorted(n["metadata"]["name"] for n in nodes.values()
                   if _zone(n) == plan["zone"] and ready(n) and _instance_id(n) not in plan["instanceIds"])
    if extra:
        raise ValueError("another Ready Kubernetes Node is running in the outage zone")
    groups = fixture.aws("autoscaling", "describe-auto-scaling-groups",
                         "--auto-scaling-group-names", *plan["asgNames"])["AutoScalingGroups"]
    if {g["AutoScalingGroupName"] for g in groups} != set(plan["asgNames"]) or not all(
            any(t["Key"] == "eks:cluster-name" and t["Value"] == plan["clusterName"] for t in g.get("Tags", []))
            for g in groups):
        raise ValueError("Auto Scaling groups are missing or not owned by this cluster")
    pvc = fixture.get("pvc", plan["pvcName"], "-n", "narumq")
    pv = fixture.get("pv", pvc["spec"]["volumeName"])
    if (pvc["metadata"]["uid"] != plan["pvcUID"] or pv["metadata"]["name"] != plan["pvName"]
            or pv["spec"]["csi"]["volumeHandle"] != plan["volumeId"]):
        raise ValueError("data identity differs from the outage plan")
    volume = fixture.aws("ec2", "describe-volumes", "--volume-ids", plan["volumeId"])["Volumes"][0]
    if (volume["AvailabilityZone"] != plan["zone"] or len(volume["Attachments"]) != 1
            or volume["Attachments"][0]["InstanceId"] != plan["oldInstanceId"]):
        raise ValueError("target EBS volume is not attached in the outage zone to the target worker")
    return {"pods": pods, "podZones": zones, "instanceIds": sorted(plan["instanceIds"])}


def suspended_processes(fixture, group: str) -> set[str]:
    described = fixture.aws("autoscaling", "describe-auto-scaling-groups", "--auto-scaling-group-names", group)
    return {p["ProcessName"] for p in described["AutoScalingGroups"][0].get("SuspendedProcesses", [])}


def resume_suspended(fixture, suspended_by_tool: dict[str, list[str]]) -> dict:
    """Resume exactly the processes this run suspended; report every group, never raise."""
    outcome = {"groups": {}, "failed": False}
    for group, processes in suspended_by_tool.items():
        entry = {"resumed": list(processes), "ok": True}
        try:
            fixture.command(["aws", "autoscaling", "resume-processes", "--auto-scaling-group-name", group,
                             "--scaling-processes", *processes])
            still = suspended_processes(fixture, group) & set(processes)
            if still:
                entry.update(ok=False, stillSuspended=sorted(still))
        except Exception as exc:
            entry.update(ok=False, errorCode=type(exc).__name__)
        outcome["failed"] = outcome["failed"] or not entry["ok"]
        outcome["groups"][group] = entry
    outcome["at"] = timestamp()
    return outcome


def asg_instances(fixture, groups: list[str]) -> dict[str, list[dict]]:
    """Per Auto Scaling group: member instance ids with lifecycle and health status."""
    described = fixture.aws("autoscaling", "describe-auto-scaling-groups", "--auto-scaling-group-names", *groups)
    return {g["AutoScalingGroupName"]: [{"instanceId": i["InstanceId"], "lifecycle": i.get("LifecycleState"),
                                         "health": i.get("HealthStatus")} for i in g.get("Instances", [])]
            for g in described["AutoScalingGroups"]}


def _node_ready_by_instance(fixture) -> dict[str, bool]:
    return {_instance_id(n): ready(n) for n in fixture.get("nodes")["items"]}


def restore_health(fixture, plan: dict, ids: list[str]) -> dict:
    """With the ASG processes still suspended, mark restarted planned instances Healthy.

    Only a planned instance that is running, Unhealthy in its ASG, and whose Kubernetes Node is Ready is
    changed. Instances that stay unfit are recorded as `unrestored`; nothing else is touched.
    """
    record = {"at": timestamp(), "set": [], "unrestored": [], "instances": {}}
    deadline = time.monotonic() + HEALTH_WAIT_SECONDS
    while True:
        states = {i["InstanceId"]: i["State"]["Name"] for r in
                  fixture.aws("ec2", "describe-instances", "--instance-ids", *ids)["Reservations"]
                  for i in r["Instances"]}
        listing = asg_instances(fixture, plan["asgNames"])
        health = {i["instanceId"]: i for members in listing.values() for i in members}
        node_ready = _node_ready_by_instance(fixture)
        pending = []
        for instance in ids:
            info = {"state": states.get(instance), "health": health.get(instance, {}).get("health"),
                    "lifecycle": health.get(instance, {}).get("lifecycle"),
                    "nodeReady": node_ready.get(instance, False)}
            record["instances"][instance] = info
            if info["health"] != "Unhealthy":
                continue
            if info["state"] == "running" and info["nodeReady"]:
                fixture.command(["aws", "autoscaling", "set-instance-health", "--instance-id", instance,
                                 "--health-status", "Healthy", "--no-should-respect-grace-period"])
                record["set"].append(instance)
                info["health"] = "Healthy"
            else:
                pending.append(instance)
        if not pending or time.monotonic() >= deadline:
            break
        time.sleep(5)
    record["unrestored"] = pending
    record["asgInstances"] = {group: [i["instanceId"] for i in members] for group, members in listing.items()}
    return record


def _activities(fixture, groups: list[str], since: datetime.datetime) -> list[dict]:
    found = []
    for group in groups:
        described = fixture.aws("autoscaling", "describe-scaling-activities", "--auto-scaling-group-name", group,
                                "--max-items", "50")
        for activity in described.get("Activities", []):
            if datetime.datetime.fromisoformat(activity["StartTime"]) >= since:
                found.append({"group": group, "activityId": activity["ActivityId"],
                              "status": activity.get("StatusCode"), "startTime": activity["StartTime"],
                              "description": activity.get("Description"), "cause": activity.get("Cause")})
    return found


def post_resume_sample(fixture, plan: dict, resumed_at: datetime.datetime) -> dict:
    sample = {"at": timestamp()}
    try:
        sample["instances"] = asg_instances(fixture, plan["asgNames"])
        sample["activities"] = _activities(fixture, plan["asgNames"], resumed_at)
        sample["nodes"] = sorted(({"name": n["metadata"]["name"], "instanceId": _instance_id(n), "ready": ready(n)}
                                  for n in fixture.get("nodes")["items"]), key=lambda n: n["name"])
        sample["podUIDs"] = {p["metadata"]["name"]: p["metadata"]["uid"] for p in
                             fixture.get("pods", "-n", "narumq", "-l", "app=narumq")["items"]}
    except Exception as exc:
        sample["errorCode"] = type(exc).__name__
    return sample


def judge_post_resume(samples: list[dict], baseline: dict[str, set], recovery_uids: dict[str, str]) -> dict:
    """PASS only if, after the ASG processes resumed, nothing was launched or terminated and no broker Pod changed."""
    reasons: list[str] = []
    activities: dict[str, dict] = {}
    if not samples:
        reasons.append("no post-resume samples")
    for sample in samples:
        if "errorCode" in sample:
            reasons.append("a post-resume sample could not be read")
            continue
        for activity in sample["activities"]:
            activities[activity["activityId"]] = activity
        for group, members in sample["instances"].items():
            if {m["instanceId"] for m in members} != set(baseline.get(group, ())):
                reasons.append("ASG instance set changed after resume")
            if any(m["lifecycle"] != "InService" for m in members):
                reasons.append("an ASG instance is not InService after resume")
        if set(sample["instances"]) != set(baseline):
            reasons.append("ASG group set differs from the baseline")
        if sample["podUIDs"] != recovery_uids:
            reasons.append("broker Pod UIDs changed after the recovery point")
    if activities:
        reasons.append("ASG scaling activity started after resume")
    return {"result": "FAIL" if reasons else "PASS", "samples": len(samples),
            "activities": list(activities.values()), "reasons": sorted(set(reasons))}


def observe_post_resume(fixture, plan: dict, seconds: int, resumed_from: str, baseline: dict[str, set],
                        recovery_uids: dict[str, str], output: Path) -> dict:
    """Observe the ASG, Nodes and broker Pods for `seconds` after the ASG processes were resumed."""
    since = datetime.datetime.fromisoformat(resumed_from)
    samples = []
    started = time.monotonic()
    while True:
        samples.append(post_resume_sample(fixture, plan, since))
        save(output, "az-outage-post-resume.json", {"resumedFrom": resumed_from, "seconds": seconds,
                                                    "samples": samples})
        if time.monotonic() - started >= seconds:
            break
        time.sleep(POST_RESUME_INTERVAL_SECONDS)
    verdict = judge_post_resume(samples, baseline, recovery_uids)
    verdict.update(resumedFrom=resumed_from, seconds=seconds)
    return verdict


def _recovery_checks(fixture, plan: dict, original: list[dict]) -> dict:
    pods = fixture.get("pods", "-n", "narumq", "-l", "app=narumq")["items"]
    target = next(p for p in pods if p["metadata"]["name"] == plan["targetPod"])
    for previous in original:
        if previous["metadata"]["name"] != plan["targetPod"]:
            current = next(p for p in pods if p["metadata"]["name"] == previous["metadata"]["name"])
            if current["metadata"]["uid"] != previous["metadata"]["uid"]:
                raise RuntimeError("another broker was replaced")
    node = fixture.get_node(target["spec"]["nodeName"])
    instance = _instance_id(node)
    pvc = fixture.get("pvc", plan["pvcName"], "-n", "narumq")
    pv = fixture.get("pv", pvc["spec"]["volumeName"])
    volume = fixture.aws("ec2", "describe-volumes", "--volume-ids", plan["volumeId"])["Volumes"][0]
    csi = [v for v in fixture.get("volumeattachments")["items"]
           if v["spec"]["source"].get("persistentVolumeName") == plan["pvName"]]
    if (instance not in plan["instanceIds"] or _zone(node) != plan["zone"]
            or pvc["metadata"]["uid"] != plan["pvcUID"] or pv["metadata"]["name"] != plan["pvName"]
            or pv["spec"]["csi"]["volumeHandle"] != plan["volumeId"] or len(volume["Attachments"]) != 1
            or volume["Attachments"][0]["InstanceId"] != instance or volume["Attachments"][0]["State"] != "attached"
            or len(csi) != 1 or csi[0]["status"]["attached"] is not True
            or csi[0]["spec"]["nodeName"] != target["spec"]["nodeName"]):
        raise RuntimeError("target slot did not return with the same PVC, PV and EBS on a planned instance")
    return {"targetInstanceId": instance, "targetPodUID": target["metadata"]["uid"],
            "podUIDChanged": target["metadata"]["uid"] != plan["oldPodUID"], "samePVC_PV_EBS": True,
            "survivorPodUIDsUnchanged": True, "podUIDs": {p["metadata"]["name"]: p["metadata"]["uid"] for p in pods}}


# Scenarios: drain-and-rolling-update.md:DRN-SCN-037
def execute(fixture, plan: dict, output: Path, original: list[dict], durable, check: Callable[[], None],
            result: dict) -> None:
    """Run the outage. `result` is filled as steps complete so a failure keeps what was observed.

    The processes this run suspended are always resumed, also after an exception.
    """
    hold = validate_plan(plan)
    ids = sorted(plan["instanceIds"])
    marks: dict[str, float] = {}
    times: dict[str, str] = {}

    def mark(name: str) -> None:
        marks[name] = time.monotonic()
        times[name] = timestamp()
        result["timings"] = {"at": dict(times),
                             "secondsSinceStopRequest": {k: round(v - marks["stopRequested"], 3)
                                                         for k, v in marks.items() if "stopRequested" in marks}}

    suspended_by_tool: dict[str, list[str]] = {}
    result["asg"] = {"baselineSuspended": {}, "suspendedByTool": suspended_by_tool}
    try:
        for group in plan["asgNames"]:
            result["asg"]["baselineSuspended"][group] = sorted(suspended_processes(fixture, group))
        for group in plan["asgNames"]:
            ours = [p for p in PROCESSES if p not in result["asg"]["baselineSuspended"][group]]
            # Recorded before the request so the resume covers a request whose outcome is unknown.
            suspended_by_tool[group] = ours
            if ours:
                fixture.command(["aws", "autoscaling", "suspend-processes", "--auto-scaling-group-name", group,
                                 "--scaling-processes", *ours])
            if not set(PROCESSES) <= suspended_processes(fixture, group):
                raise RuntimeError("Auto Scaling processes are not suspended")
        check()
        mark("stopRequested")
        response = fixture.aws("ec2", "stop-instances", "--instance-ids", *ids, "--skip-os-shutdown")
        if {i["InstanceId"] for i in response["StoppingInstances"]} != set(ids):
            raise RuntimeError("unexpected stop response")
        save(output, "az-outage-stop-request.json", {"at": times["stopRequested"], "skipOsShutdown": True,
                                                     "instanceIds": ids})
        fixture.command(["aws", "ec2", "wait", "instance-stopped", "--instance-ids", *ids], timeout=600)
        states = {i["InstanceId"]: i["State"]["Name"] for r in
                  fixture.aws("ec2", "describe-instances", "--instance-ids", *ids)["Reservations"]
                  for i in r["Instances"]}
        if states != {i: "stopped" for i in ids}:
            raise RuntimeError("instances are not all stopped")
        mark("stopped")
        hold_end = marks["stopped"] + hold
        mid_at = marks["stopped"] + hold / 2
        while time.monotonic() < hold_end:
            check()
            if "midOutage" not in result and time.monotonic() >= mid_at:
                result["midOutage"] = durable.verify("mid-outage", cutoff=timestamp())
            time.sleep(1)
        if "midOutage" not in result:
            result["midOutage"] = durable.verify("mid-outage", cutoff=timestamp())
        mark("startRequested")
        response = fixture.aws("ec2", "start-instances", "--instance-ids", *ids)
        if {i["InstanceId"] for i in response["StartingInstances"]} != set(ids):
            raise RuntimeError("unexpected start response")
        fixture.command(["aws", "ec2", "wait", "instance-running", "--instance-ids", *ids], timeout=600)
        mark("running")
        deadline = marks["startRequested"] + RECOVERY_DEADLINE_SECONDS
        while True:
            check()
            pods = fixture.get("pods", "-n", "narumq", "-l", "app=narumq")["items"]
            target = next((p for p in pods if p["metadata"]["name"] == plan["targetPod"]), None)
            target_ready = (target is not None and ready(target)
                            and not target["metadata"].get("deletionTimestamp"))
            if target_ready and "targetReady" not in marks:
                mark("targetReady")
            if target_ready and len(pods) == 3 and all(ready(p) for p in pods):
                cluster = fixture.api("/api/v1/cluster")
                if cluster["quorumAvailable"] is True and cluster["nodesByState"]["ready"] == 3:
                    mark("threeReady")
                    break
            if time.monotonic() >= deadline:
                raise TimeoutError("target slot did not recover within the deadline")
            time.sleep(5)
        result["recovery"] = _recovery_checks(fixture, plan, original)
        result["afterRestore"] = durable.finish("after-restore")
    finally:
        resumed_from = timestamp()
        if "startRequested" in marks:
            # Before ASG processes resume: restarted instances must not be left marked Unhealthy.
            try:
                result["asg"]["health"] = restore_health(fixture, plan, ids)
                save(output, "az-outage-asg-health.json", result["asg"]["health"])
            except Exception as exc:
                result["asg"]["health"] = {"errorCode": type(exc).__name__}
        result["asg"]["resume"] = resume_suspended(fixture, suspended_by_tool)
        save(output, "az-outage-asg-resume.json", result["asg"])
    health = result["asg"].get("health", {})
    if "asgInstances" not in health:
        result["postResume"] = {"result": "FAIL", "reasons": ["no ASG instance baseline before resume"]}
        return
    result["postResume"] = observe_post_resume(
        fixture, plan, post_resume_seconds(plan), resumed_from,
        {group: set(instances) for group, instances in health["asgInstances"].items()},
        result["recovery"]["podUIDs"], output)


def judge_hold(samples: dict[str, list[dict]], stop_requested_at: str, start_requested_at: str,
               target_node_id: str) -> dict:
    """During the outage every management sample needs quorum and a leader outside the zone."""
    low = datetime.datetime.fromisoformat(stop_requested_at)
    high = datetime.datetime.fromisoformat(start_requested_at)
    inside = {name: [s for s in samples[name] if low <= datetime.datetime.fromisoformat(s["startedAt"]) <= high]
              for name in ("management", "mqtt")}

    def bad_management(sample: dict) -> bool:
        data = sample.get("data", {}) if sample.get("ok") else {}
        return not (data.get("quorumAvailable") is True and data.get("leaderNodeId") not in (None, "")
                    and str(data["leaderNodeId"]) != str(target_node_id))

    management = {"samples": len(inside["management"]),
                  "failed": sum(bad_management(s) for s in inside["management"])}
    mqtt = {"samples": len(inside["mqtt"]),
            "failed": sum(not (s.get("ok") and s.get("data", {}).get("puback") is True) for s in inside["mqtt"])}
    passed = all(v["samples"] > 0 and v["failed"] == 0 for v in (management, mqtt))
    return {"result": "PASS" if passed else "FAIL", "management": management, "mqtt": mqtt,
            "scope": "samples started between the stop request and the start request"}
