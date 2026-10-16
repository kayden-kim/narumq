"""Observe a prepared synthetic fixture; AWS mutation requires the fault, az-outage or power-loss subcommand."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import threading
import time
import urllib.error

import az_outage
import power_loss
from durable_stream import DurablePublisher, plan_settings, prepare_subscriber, verify_subscriber
from fixture import Fixture, ready, save
from observer import (IndependentMonitor, fence_node, fenced, leader_continuity, monitor_integrity,
                      release_fenced_pod, timestamp)
from pod_tunnel import PodTunnel


# Bounded wait for the first nodes sample before a fault; diagnostics never gate the fault or the result.
DIAGNOSTIC_BASELINE_WAIT_SECONDS = 20
# Fixed local ports: observer (survivor) 23880/23883, target Pod 24880/24883.
OBSERVER_MANAGEMENT_PORT, OBSERVER_MQTT_PORT = 23880, 23883
TARGET_MANAGEMENT_PORT, TARGET_MQTT_PORT = 24880, 24883
# Acked messages required before a fault so the stream has a pre-fault baseline.
DURABLE_BASELINE_ACKS = 3
TARGET_ROLES = ("follower", "leader")


class SurvivorTunnel:
    """Own both local ports for one identity-checked surviving Pod, without retry."""

    def __init__(self, fixture: Fixture, plan: dict):
        self.fixture = fixture
        self.plan = plan
        self.tunnel = None

    def identity(self) -> dict:
        plan = self.plan
        required = ("observerPod", "observerPodUID", "observerWorkerName", "observerInstanceId",
                    "targetPod", "oldWorkerName", "oldInstanceId")
        if not all(isinstance(plan.get(key), str) and plan[key] for key in required):
            raise ValueError("missing survivor identity")
        if (plan["observerPod"] == plan["targetPod"] or plan["observerWorkerName"] == plan["oldWorkerName"]
                or plan["observerInstanceId"] == plan["oldInstanceId"]):
            raise ValueError("observer must use an independent surviving worker")
        pod = self.fixture.get("pod", plan["observerPod"], "-n", "narumq")
        node = self.fixture.get_node(plan["observerWorkerName"])
        if (not pod or not node or pod["metadata"]["uid"] != plan["observerPodUID"]
                or pod["metadata"].get("deletionTimestamp")
                or pod["metadata"].get("labels", {}).get("app") != "narumq"
                or pod["spec"].get("nodeName") != plan["observerWorkerName"] or not ready(pod)
                or node["spec"].get("providerID", "").rsplit("/", 1)[-1] != plan["observerInstanceId"]
                or not ready(node) or node["metadata"].get("deletionTimestamp")
                or self.fixture.instance_state(plan["observerInstanceId"]) != "running"):
            raise ValueError("surviving Pod or worker identity is not current and Ready")
        return {key: plan[key] for key in required[:4]}

    def start(self, timeout: float = 20) -> dict:
        identity = self.identity()
        self.tunnel = PodTunnel(self.fixture, self.plan["observerPod"], self.plan["observerPodUID"],
                                OBSERVER_MANAGEMENT_PORT, OBSERVER_MQTT_PORT)
        try:
            self.tunnel.start(timeout)
            self.identity()
        except Exception:
            self.close()
            raise
        return {**identity, "at": timestamp(), "ports": {"management": OBSERVER_MANAGEMENT_PORT,
                                                         "mqtt": OBSERVER_MQTT_PORT},
                "routing": "owned kubectl port-forward to exact Pod; no service selection or reconnect"}

    def check(self) -> None:
        if self.tunnel is None:
            raise RuntimeError("survivor tunnel is not running")
        self.tunnel.check()

    def close(self) -> None:
        if self.tunnel is not None:
            self.tunnel.close()


class MQTTProbe:
    def __init__(self, private: Path, run_id: str):
        import paho.mqtt.client as mqtt
        self.mqtt = mqtt
        self.connected = threading.Event()
        self.connection_failed = False
        self.acks = {}
        self.sequence = 0
        self.topic = f"acceptance/{run_id}/continuity"
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="ebs-worker-publisher",
                                  protocol=mqtt.MQTTv5, reconnect_on_failure=False)
        self.client.username_pw_set("acceptance-device", (private / "mqtt.password").read_text(encoding="utf-8").strip())
        def connect(client, userdata, flags, reason, properties):
            self.connection_failed = reason.is_failure
            self.connected.set()
        def publish(client, userdata, mid, reason, properties):
            self.acks[mid] = {"reasonCode": reason.value, "success": not reason.is_failure}
        self.client.on_connect = connect
        self.client.on_publish = publish
        self.client.connect("127.0.0.1", 23883, 30, clean_start=True)
        self.client.loop_start()
        if not self.connected.wait(15) or self.connection_failed:
            self.close()
            raise RuntimeError("MQTT connection did not succeed")

    def probe(self) -> dict:
        sequence = self.sequence
        self.sequence += 1
        published = self.client.publish(self.topic, str(sequence).encode(), qos=1)
        published.wait_for_publish(timeout=15)
        ack = self.acks.pop(published.mid, {})
        return {"sequence": sequence, "puback": published.is_published()
                and published.rc == self.mqtt.MQTT_ERR_SUCCESS and ack.get("success") is True,
                "reasonCode": ack.get("reasonCode")}

    def close(self) -> None:
        self.client.disconnect()
        self.client.loop_stop()


class DurableRun:
    """Durable stream of one fault scenario: subscriber prepared on the target, publisher on the observer."""

    def __init__(self, fixture: Fixture, private: Path, run_id: str, plan: dict, output: Path):
        self.fixture = fixture
        self.private = private
        self.run_id = run_id
        self.plan = plan
        self.output = output
        self.scenario = plan["durableScenario"]
        self.publisher = None
        self.report = {"scenario": self.scenario, "verifications": {}}
        plan_settings(plan)

    def begin(self) -> None:
        """Subscribe on the target Pod, then start publishing through the observer; call before the fault."""
        tunnel = PodTunnel(self.fixture, self.plan["targetPod"], self.plan["oldPodUID"],
                           TARGET_MANAGEMENT_PORT, TARGET_MQTT_PORT)
        try:
            self.report["targetTunnel"] = tunnel.start()
            self.report["prepare"] = prepare_subscriber(self.private, self.run_id, self.scenario,
                                                        TARGET_MQTT_PORT, self.output)
        finally:
            tunnel.close()
        interval, max_messages = plan_settings(self.plan)
        self.publisher = DurablePublisher(self.private, self.run_id, self.scenario, OBSERVER_MQTT_PORT, self.output,
                                          interval=interval, max_messages=max_messages)
        self.publisher.start()
        self.report["publisherStartedAt"] = timestamp()
        if not self.publisher.wait_for_acked(DURABLE_BASELINE_ACKS, 30):
            raise RuntimeError("durable publisher has no acked baseline; fault not injected")

    def verify(self, label: str, cutoff: str | None = None) -> dict:
        result = verify_subscriber(self.private, self.run_id, self.scenario, OBSERVER_MQTT_PORT, self.output,
                                   label, cutoff=cutoff)
        self.report["verifications"][label] = result
        return result

    def finish(self, label: str) -> dict:
        """Stop publishing after recovery, then verify; a lost publisher connection fails the stream."""
        stats = self.publisher.stop()
        self.report["publisher"] = stats
        result = self.verify(label)
        if stats["connectionLost"]:
            result["result"] = "FAIL"
            result["publisherConnectionLost"] = True
        return result

    def abort(self) -> None:
        if self.publisher is not None:
            self.report["publisher"] = self.publisher.stop()


def require_drain_record_absent(fixture, target_node_id: str) -> None:
    """Read the cluster-wide drain record through the owned surviving-Pod tunnel."""
    try:
        fixture.api(f"/api/v1/nodes/{target_node_id}/drain")
    except urllib.error.HTTPError as exc:
        if exc.code != 404:
            raise
    else:
        raise ValueError("pre-fault drain record must be absent")


def preflight(fixture: Fixture, plan: dict) -> list[dict]:
    required = ("runId", "clusterName", "vpcId", "targetPod", "targetNodeId", "oldPodUID", "oldWorkerName",
                "oldNodeUID", "oldInstanceId", "pvcName", "pvcUID", "pvName", "volumeId", "zone",
                "spareWorkerName", "spareInstanceId", "snapshotId", "targetRole")
    if not all(isinstance(plan.get(key), str) and plan[key] for key in required):
        raise ValueError("incomplete fixture plan")
    if plan["targetRole"] not in TARGET_ROLES:
        raise ValueError("targetRole must be follower or leader")
    if plan["runId"] != fixture.run_id or not plan["clusterName"].startswith("narumq-ha-acceptance-"):
        raise ValueError("plan does not identify this disposable fixture")
    if fixture.aws("sts", "get-caller-identity")["Account"] != "809446814769":
        raise ValueError("AWS account differs from acceptance account")
    ns = fixture.get("namespace", "narumq")
    if ns["metadata"].get("labels", {}).get("narumq.io/acceptance-run") != fixture.run_id:
        raise ValueError("namespace is not owned by this synthetic fixture")
    cluster = fixture.api("/api/v1/cluster")
    leader_is_target = str(cluster["leaderNodeId"]) == plan["targetNodeId"]
    if (cluster["clusterId"] != fixture.run_id or cluster["quorumAvailable"] is not True
            or leader_is_target != (plan["targetRole"] == "leader")):
        raise ValueError(f"cluster must have quorum and the target must be the current {plan['targetRole']}")
    pods = fixture.get("pods", "-n", "narumq", "-l", "app=narumq")["items"]
    if len(pods) != 3 or not all(ready(p) for p in pods):
        raise ValueError("three Ready voters are required before the fault")
    target = next(p for p in pods if p["metadata"]["name"] == plan["targetPod"])
    old_node = fixture.get_node(plan["oldWorkerName"])
    spare = fixture.get_node(plan["spareWorkerName"])
    instance = fixture.instance(plan["oldInstanceId"])
    if (target["metadata"]["uid"] != plan["oldPodUID"] or target["spec"]["nodeName"] != plan["oldWorkerName"]
            or not old_node or old_node["metadata"]["uid"] != plan["oldNodeUID"]
            or old_node["spec"]["providerID"].rsplit("/", 1)[-1] != plan["oldInstanceId"]
            or instance["State"]["Name"] != "running" or instance["VpcId"] != plan["vpcId"]):
        raise ValueError("fault target identity changed")
    if plan["targetRole"] == "leader":
        observer = fixture.get_node(plan["observerWorkerName"])
        if not observer or (observer["metadata"]["labels"]["topology.kubernetes.io/zone"]
                            == old_node["metadata"]["labels"]["topology.kubernetes.io/zone"]):
            raise ValueError("leader fault requires the observer in a different AZ from the target")
    if (not spare or not ready(spare) or spare["spec"].get("unschedulable", False)
            or spare["spec"]["providerID"].rsplit("/", 1)[-1] != plan["spareInstanceId"]
            or plan["spareInstanceId"] == plan["oldInstanceId"]
            or spare["metadata"]["labels"]["topology.kubernetes.io/zone"] != plan["zone"]):
        raise ValueError("independent same-AZ Ready spare is required")
    mapping = next(b for b in instance["BlockDeviceMappings"] if b["Ebs"]["VolumeId"] == plan["volumeId"])
    if mapping["Ebs"]["DeleteOnTermination"] is not False:
        raise ValueError("data volume would be deleted by termination")
    pvc = fixture.get("pvc", plan["pvcName"], "-n", "narumq")
    pv = fixture.get("pv", pvc["spec"]["volumeName"])
    if (pvc["metadata"]["uid"] != plan["pvcUID"] or pv["metadata"]["name"] != plan["pvName"]
            or pv["spec"]["csi"]["volumeHandle"] != plan["volumeId"]):
        raise ValueError("data identity differs from fault plan")
    snapshot = fixture.aws("ec2", "describe-snapshots", "--snapshot-ids", plan["snapshotId"])["Snapshots"][0]
    if (snapshot["State"] != "completed" or snapshot["VolumeId"] != plan["volumeId"]
            or not any(t["Key"] == "RunId" and t["Value"] == fixture.run_id for t in snapshot.get("Tags", []))):
        raise ValueError("completed run-owned baseline snapshot is required")
    require_drain_record_absent(fixture, plan["targetNodeId"])
    return pods


def observe_fault(fixture: Fixture, plan: dict, output: Path, original: list[dict]) -> dict:
    start = time.monotonic()
    response = fixture.aws("ec2", "terminate-instances", "--instance-ids", plan["oldInstanceId"], "--skip-os-shutdown")
    if response["TerminatingInstances"][0]["InstanceId"] != plan["oldInstanceId"]:
        raise RuntimeError("unexpected termination response")
    save(output, "fault-request.json", {"at": timestamp(), "skipOsShutdown": True, "response": response})
    fixture.command(["aws", "ec2", "wait", "instance-terminated", "--instance-ids", plan["oldInstanceId"]], timeout=360)
    if fixture.instance_state(plan["oldInstanceId"]) != "terminated":
        raise RuntimeError("old EC2 fencing not established")
    save(output, "terminated-proof.json", {"at": timestamp(), "instanceId": plan["oldInstanceId"], "state": "terminated"})
    fencing = fence_node(fixture.get_node, fixture.patch_node, fixture.instance_state,
                         plan["oldWorkerName"], plan["oldNodeUID"], plan["oldInstanceId"])
    save(output, "fencing-observation.json", fencing)
    release = release_fenced_pod(fixture.get_node, fixture.get_pod, fixture.delete_pod, fixture.instance_state,
                                 fencing, plan["oldWorkerName"], plan["oldNodeUID"], plan["oldInstanceId"],
                                 plan["targetPod"], plan["oldPodUID"])
    save(output, "pod-release-observation.json", release)
    observations = []
    while time.monotonic() - start < 900:
        pods = fixture.get("pods", "-n", "narumq", "-l", "app=narumq")["items"]
        old_node = fixture.get_node(plan["oldWorkerName"])
        observations.append({"at": timestamp(), "secondsAfterFault": time.monotonic() - start, "pods": [
            {"name": p["metadata"]["name"], "uid": p["metadata"]["uid"], "node": p["spec"].get("nodeName"), "ready": ready(p),
             "deletionTimestamp": p["metadata"].get("deletionTimestamp")} for p in pods],
            "oldNode": None if old_node is None else {"uid": old_node["metadata"]["uid"], "fenced": fenced(old_node),
                                                      "ready": ready(old_node)},
            "attachment": fixture.attachment_probe(plan["pvName"], plan["volumeId"])})
        save(output, "recovery-observations.json", observations)
        replacement = next((p for p in pods if p["metadata"]["name"] == plan["targetPod"]), None)
        if replacement and replacement["metadata"]["uid"] != plan["oldPodUID"] and len(pods) == 3 and all(ready(p) for p in pods):
            ready_seconds = time.monotonic() - start
            break
        time.sleep(3)
    else:
        raise TimeoutError("replacement did not recover within fault observation deadline")
    for previous in original:
        if previous["metadata"]["name"] != plan["targetPod"]:
            current = next(p for p in pods if p["metadata"]["name"] == previous["metadata"]["name"])
            if current["metadata"]["uid"] != previous["metadata"]["uid"]:
                raise RuntimeError("another broker was replaced")
    node = fixture.get_node(replacement["spec"]["nodeName"])
    new_instance = node["spec"]["providerID"].rsplit("/", 1)[-1]
    if new_instance == plan["oldInstanceId"] or node["metadata"]["labels"]["topology.kubernetes.io/zone"] != plan["zone"]:
        raise RuntimeError("replacement is not on an independent same-AZ worker")
    pvc = fixture.get("pvc", plan["pvcName"], "-n", "narumq")
    pv = fixture.get("pv", pvc["spec"]["volumeName"])
    volume = fixture.aws("ec2", "describe-volumes", "--volume-ids", plan["volumeId"])["Volumes"][0]
    csi = [v for v in fixture.get("volumeattachments")["items"] if v["spec"]["source"].get("persistentVolumeName") == plan["pvName"]]
    if (pvc["metadata"]["uid"] != plan["pvcUID"] or pv["metadata"]["name"] != plan["pvName"]
            or pv["spec"]["csi"]["volumeHandle"] != plan["volumeId"] or not volume["Encrypted"]
            or volume["AvailabilityZone"] != plan["zone"] or len(volume["Attachments"]) != 1
            or volume["Attachments"][0]["InstanceId"] != new_instance or volume["Attachments"][0]["State"] != "attached"
            or len(csi) != 1 or csi[0]["status"]["attached"] is not True
            or csi[0]["spec"]["nodeName"] != replacement["spec"]["nodeName"]):
        raise RuntimeError("persistent volume or attachment identity differs after recovery")
    final = fixture.api("/api/v1/cluster")
    if final["quorumAvailable"] is not True or final["nodesByState"]["ready"] != 3:
        raise RuntimeError("cluster did not converge to three Ready brokers")
    return {"readyObservedSecondsAfterFault": ready_seconds, "fencing": fencing, "podRelease": release,
            "oldInstanceId": plan["oldInstanceId"], "newInstanceId": new_instance,
            "samePVC_PV_EBS": True, "survivorPodUIDsUnchanged": True,
            "scope": "physical recovery and sampled continuity; the durable stream oracle is judged separately in report['durable']"}


def judge_fault(report: dict, plan: dict, monitor, durable) -> bool:
    """Overall verdict of fault mode: continuity rule of the target role, explained mutations, durable stream."""
    summary = report["monitor"]
    explained = (report["recovery"]["fencing"]["outcome"] not in ("failed_unexplained", "mutation_unconfirmed")
                 and report["recovery"]["podRelease"]["outcome"] not in ("failed_unexplained", "delete_unconfirmed"))
    if plan.get("targetRole") == "leader":
        report["continuity"] = leader_continuity(monitor.snapshot(), report["faultRequestedAt"], plan["targetNodeId"])
        continuous = report["continuity"]["result"] == "PASS" and monitor_integrity(summary)
    else:
        continuous = summary["result"] == "PASS"
    stream = durable is None or durable.report.get("verifications", {}).get("after-recovery", {}).get("result") == "PASS"
    return continuous and explained and stream


def judge_az_outage(report: dict, plan: dict, monitor) -> bool:
    """Overall verdict of az-outage mode; the whole-run monitor result stays informational."""
    outage = report["outage"]
    times = outage["timings"]["at"]
    report["hold"] = az_outage.judge_hold(monitor.snapshot(), times["stopRequested"], times["startRequested"],
                                          plan["targetNodeId"])
    return (report["hold"]["result"] == "PASS" and monitor_integrity(report["monitor"])
            and outage["midOutage"]["result"] == "PASS" and outage["afterRestore"]["result"] == "PASS"
            and "recovery" in outage and not outage["asg"]["resume"]["failed"]
            and outage.get("postResume", {}).get("result") == "PASS")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("monitor", "fault", "az-outage", "power-loss"))
    parser.add_argument("--private", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--seconds", type=float, default=12, help="read-only monitor duration")
    parser.add_argument("--plan", type=Path)
    parser.add_argument("--execute", action="store_true",
                        help="allow the AWS mutation of the checked synthetic fixture (fault, az-outage, power-loss)")
    args = parser.parse_args(argv)
    acting = args.mode != "monitor"
    if acting and (not args.execute or args.plan is None):
        parser.error(f"{args.mode} requires --execute and --plan")
    if args.mode == "power-loss":
        return power_loss.run_cli(args)
    if not 0 < args.seconds <= 3600:
        parser.error("--seconds must be within (0, 3600]")
    args.output.mkdir(parents=True, exist_ok=False)
    monitor = None
    mqtt = None
    tunnel = None
    durable = None
    report = {"result": "FAIL", "mode": args.mode, "startedAt": timestamp()}
    try:
        fixture = Fixture(args.private, args.output.name)
        plan = json.loads(args.plan.read_text(encoding="utf-8")) if acting else None
        if args.mode == "az-outage":
            report["holdSeconds"] = az_outage.validate_plan(plan)
        if plan:
            tunnel = SurvivorTunnel(fixture, plan)
            report["observerEndpoint"] = tunnel.start()
            save(args.output, "observer-endpoint.json", report["observerEndpoint"])
        if args.mode == "az-outage":
            original = az_outage.preflight(fixture, plan)["pods"]
            require_drain_record_absent(fixture, plan["targetNodeId"])
        else:
            original = preflight(fixture, plan) if plan else None
        mqtt = MQTTProbe(args.private, args.output.name)
        def management_probe():
            if tunnel:
                tunnel.check()
            return fixture.management_probe()
        def mqtt_probe():
            if tunnel:
                tunnel.check()
            return mqtt.probe()
        def nodes_probe():
            if tunnel:
                tunnel.check()
            return fixture.nodes_probe()
        diagnostics = {"nodes": nodes_probe} if acting else None
        monitor = IndependentMonitor(management_probe, mqtt_probe, args.output, diagnostics=diagnostics)
        monitor.start()
        if not monitor.wait_initial():
            raise TimeoutError("initial probes did not complete")
        initial = monitor.snapshot()
        if acting and not all(initial[name][0]["ok"] and initial[name][0]["data"].get(field) is True
                              for name, field in (("management", "quorumAvailable"), ("mqtt", "puback"))):
            raise RuntimeError("initial probes are not successful; fault not injected")
        if acting:
            monitor.wait_diagnostic_baseline("nodes", DIAGNOSTIC_BASELINE_WAIT_SECONDS)
            tunnel.identity()
            tunnel.check()
            if plan.get("durableScenario"):
                durable = DurableRun(fixture, args.private, args.output.name, plan, args.output)
                report["durable"] = durable.report
                durable.begin()
            report["faultRequestedAt"] = timestamp()
            if args.mode == "fault":
                report["recovery"] = observe_fault(fixture, plan, args.output, original)
                if durable:
                    durable.finish("after-recovery")
            else:
                report["outage"] = {}
                az_outage.execute(fixture, plan, args.output, original, durable, tunnel.check, report["outage"])
        else:
            time.sleep(args.seconds)
        monitor.stop()
        if tunnel:
            tunnel.check()
        report["monitor"] = monitor.summary()
        if args.mode == "monitor":
            passed = report["monitor"]["result"] == "PASS"
        elif args.mode == "fault":
            passed = judge_fault(report, plan, monitor, durable)
        else:
            passed = judge_az_outage(report, plan, monitor)
        report["result"] = "PASS" if passed else "FAIL"
    except Exception as exc:
        report["errorCode"] = type(exc).__name__
    finally:
        if monitor:
            try:
                monitor.stop()
            except Exception as exc:
                report.update(result="FAIL", stopErrorCode=type(exc).__name__)
            report["monitor"] = monitor.summary()
        if durable:
            try:
                durable.abort()
            except Exception as exc:
                report.update(result="FAIL", durableStopErrorCode=type(exc).__name__)
        if mqtt:
            try:
                mqtt.close()
            except Exception as exc:
                report.update(result="FAIL", mqttStopErrorCode=type(exc).__name__)
        if tunnel:
            try:
                tunnel.close()
            except Exception as exc:
                report.update(result="FAIL", tunnelStopErrorCode=type(exc).__name__)
        report["finishedAt"] = timestamp()
        save(args.output, "summary.json", report)
    print(json.dumps(report))
    return 0 if report["result"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
