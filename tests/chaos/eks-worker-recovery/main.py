"""Observe a prepared synthetic fixture; AWS mutation requires the fault subcommand."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess
import threading
import time
import urllib.error
import urllib.request

from observer import IndependentMonitor, MANAGEMENT_TIMEOUT_SECONDS, MutationRejected, fence_node, timestamp


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

    def command(self, args: list[str], timeout: float = 90) -> str:
        result = subprocess.run(args, env=self.environment, text=True, encoding="utf-8",
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

    def instance(self, instance_id: str) -> dict:
        return self.aws("ec2", "describe-instances", "--instance-ids", instance_id)["Reservations"][0]["Instances"][0]

    def instance_state(self, instance_id: str) -> str:
        return self.instance(instance_id)["State"]["Name"]

    def api(self, path: str, port: int = 23880) -> dict:
        request = urllib.request.Request(f"http://127.0.0.1:{port}{path}",
                                        headers={"Authorization": "Bearer " + self.token})
        with urllib.request.urlopen(request, timeout=MANAGEMENT_TIMEOUT_SECONDS) as response:
            return json.load(response)["data"]

    def management_probe(self) -> dict:
        cluster = self.api("/api/v1/cluster")
        return {key: cluster[key] for key in ("quorumAvailable", "leaderNodeId", "nodesByState")}


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


def ready(obj: dict) -> bool:
    return any(c["type"] == "Ready" and c["status"] == "True" for c in obj.get("status", {}).get("conditions", []))


def save(output: Path, name: str, value) -> None:
    (output / name).write_text(json.dumps(value, indent=2), encoding="utf-8")


def preflight(fixture: Fixture, plan: dict) -> list[dict]:
    required = ("runId", "clusterName", "vpcId", "targetPod", "targetNodeId", "oldPodUID", "oldWorkerName",
                "oldNodeUID", "oldInstanceId", "pvcName", "pvcUID", "pvName", "volumeId", "zone",
                "spareWorkerName", "spareInstanceId", "snapshotId")
    if not all(isinstance(plan.get(key), str) and plan[key] for key in required):
        raise ValueError("incomplete fixture plan")
    if plan["runId"] != fixture.run_id or not plan["clusterName"].startswith("narumq-ha-acceptance-"):
        raise ValueError("plan does not identify this disposable fixture")
    if fixture.aws("sts", "get-caller-identity")["Account"] != "809446814769":
        raise ValueError("AWS account differs from acceptance account")
    ns = fixture.get("namespace", "narumq")
    if ns["metadata"].get("labels", {}).get("narumq.io/acceptance-run") != fixture.run_id:
        raise ValueError("namespace is not owned by this synthetic fixture")
    cluster = fixture.api("/api/v1/cluster")
    if (cluster["clusterId"] != fixture.run_id or cluster["quorumAvailable"] is not True
            or str(cluster["leaderNodeId"]) == plan["targetNodeId"]):
        raise ValueError("cluster must have quorum and the target must be a follower")
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
    try:
        fixture.api(f"/api/v1/nodes/{plan['targetNodeId']}/drain", port=28880)
    except urllib.error.HTTPError as exc:
        if exc.code != 404:
            raise
    else:
        raise ValueError("pre-fault drain record must be absent")
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
    observations = []
    while time.monotonic() - start < 900:
        pods = fixture.get("pods", "-n", "narumq", "-l", "app=narumq")["items"]
        observations.append({"secondsAfterFault": time.monotonic() - start, "pods": [
            {"name": p["metadata"]["name"], "uid": p["metadata"]["uid"], "node": p["spec"].get("nodeName"), "ready": ready(p)} for p in pods]})
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
    return {"readyObservedSecondsAfterFault": ready_seconds, "fencing": fencing,
            "oldInstanceId": plan["oldInstanceId"], "newInstanceId": new_instance,
            "samePVC_PV_EBS": True, "survivorPodUIDsUnchanged": True,
            "scope": "physical recovery and sampled continuity; persistent payload oracle remains a separate scenario"}


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("monitor", "fault"))
    parser.add_argument("--private", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--seconds", type=float, default=12, help="read-only monitor duration")
    parser.add_argument("--plan", type=Path)
    parser.add_argument("--execute", action="store_true", help="allow termination of the checked synthetic follower")
    args = parser.parse_args(argv)
    if args.mode == "fault" and (not args.execute or args.plan is None):
        parser.error("fault requires --execute and --plan")
    if not 0 < args.seconds <= 3600:
        parser.error("--seconds must be within (0, 3600]")
    args.output.mkdir(parents=True, exist_ok=False)
    monitor = None
    mqtt = None
    report = {"result": "FAIL", "mode": args.mode, "startedAt": timestamp()}
    try:
        fixture = Fixture(args.private, args.output.name)
        plan = json.loads(args.plan.read_text(encoding="utf-8")) if args.mode == "fault" else None
        original = preflight(fixture, plan) if plan else None
        mqtt = MQTTProbe(args.private, args.output.name)
        monitor = IndependentMonitor(fixture.management_probe, mqtt.probe, args.output)
        monitor.start()
        if not monitor.wait_initial():
            raise TimeoutError("initial probes did not complete")
        initial = monitor.snapshot()
        if args.mode == "fault" and not all(initial[name][0]["ok"] and initial[name][0]["data"].get(field) is True
                                            for name, field in (("management", "quorumAvailable"), ("mqtt", "puback"))):
            raise RuntimeError("initial probes are not successful; fault not injected")
        if args.mode == "fault":
            report["recovery"] = observe_fault(fixture, plan, args.output, original)
        else:
            time.sleep(args.seconds)
        monitor.stop()
        report["monitor"] = monitor.summary()
        explained = args.mode != "fault" or report["recovery"]["fencing"]["outcome"] not in ("failed_unexplained", "mutation_unconfirmed")
        report["result"] = "PASS" if report["monitor"]["result"] == "PASS" and explained else "FAIL"
    except Exception as exc:
        report["errorCode"] = type(exc).__name__
    finally:
        if monitor:
            try:
                monitor.stop()
            except Exception as exc:
                report.update(result="FAIL", stopErrorCode=type(exc).__name__)
            report["monitor"] = monitor.summary()
        if mqtt:
            try:
                mqtt.close()
            except Exception as exc:
                report.update(result="FAIL", mqttStopErrorCode=type(exc).__name__)
        report["finishedAt"] = timestamp()
        save(args.output, "summary.json", report)
    print(json.dumps(report))
    return 0 if report["result"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
