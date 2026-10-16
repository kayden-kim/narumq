"""Inject WAL torn tails and corruptions into one stopped broker's real EBS volume (CLU-14 T1/T2).

Acceptance: docs/specs/cluster-session-and-delivery.md "EKS 전체 voter 전원 손실·torn-tail·큰 snapshot 재시드 기준",
ADR-0023 (tail rules), `internal/adapters/raft/stable_store.go` (`readWALMode`). This is a model, not a natural
torn write: the bytes are written by the tool into a cleanly stopped broker's `wal.bin`.

Per case: the broker Pod is stopped, a helper Pod mounts the PVC, the WAL is backed up (private directory),
the case bytes are written and read back, the helper is removed and the broker restarts. T1 cases must
start, cut the tail and keep the data. T2 cases must refuse to start without changing the file; the backup
bytes are then written back and the broker must start. Each mutation runs once and is never retried.
Evidence is written under --output; WAL backups only under --private. Secrets are never written.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import re
import subprocess
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(1, str(Path(__file__).resolve().parents[1] / "eks-volume-reseed"))

import helper_pod  # noqa: E402
import judge  # noqa: E402
import wal_frames  # noqa: E402
import reseed  # noqa: E402  (also puts tests/chaos/eks-worker-recovery on sys.path)
from fixture import CommandFailed, Fixture, ready, require_owned_fixture, save  # noqa: E402
from observer import MutationRejected, timestamp  # noqa: E402
from pod_tunnel import PodTunnel  # noqa: E402

NAMESPACE = reseed.NAMESPACE
BROKER = reseed.CONTAINER
BROKER_WAL_PATH = "/data/wal.bin"
OBSERVER_PORTS = reseed.OBSERVER_PORTS
TARGET_PORTS = reseed.TARGET_PORTS
READY_DEADLINE_DEFAULT = 900
READY_DEADLINE_BOUNDS = (60, 1800)
REFUSE_WAIT_DEFAULT = 300
REFUSE_WAIT_BOUNDS = (30, 900)
REFUSE_HOLD_DEFAULT = 120
REFUSE_HOLD_BOUNDS = (30, 600)
# terminationGracePeriodSeconds is 660 and the preStop drain waits up to 600 s.
POD_STOP_DEADLINE_DEFAULT = 780
POD_STOP_DEADLINE_BOUNDS = (60, 1800)
DEPENDENCIES = ("paho.mqtt.client", "paho.mqtt.packettypes", "paho.mqtt.properties")
PLAN_STRINGS = ("runId", "clusterName", "vpcId", "zone", "targetPod", "targetPodUID", "targetNodeId",
                "targetWorkerName", "targetNodeUID", "targetInstanceId", "pvcName", "pvcUID", "pvName", "volumeId",
                "observerPod", "observerPodUID", "observerWorkerName", "observerInstanceId")
BACKUP_NAME = re.compile(r"^wal-backup-[A-Za-z0-9._-]+\.bin$")


class PlanError(ValueError):
    pass


class StepFailed(RuntimeError):
    def __init__(self, step: str, code: str):
        super().__init__(f"{step}: {code}")
        self.step = step
        self.code = code


def _bounded(plan: dict, key: str, default: int, bounds: tuple[int, int]) -> int:
    value = plan.get(key, default)
    if isinstance(value, bool) or not isinstance(value, int) or not bounds[0] <= value <= bounds[1]:
        raise PlanError(f"{key} must be an integer within {list(bounds)}")
    return value


def load_plan(raw: dict, run_id: str) -> dict:
    """Validate and normalise the plan. Nothing is read from the cluster here."""
    if not isinstance(raw, dict):
        raise PlanError("plan must be an object")
    plan = dict(raw)
    for key in PLAN_STRINGS:
        if not isinstance(plan.get(key), str) or not plan[key]:
            raise PlanError(f"plan key {key} is missing")
    if plan["runId"] != run_id:
        raise PlanError("plan does not identify this run")
    if not plan["clusterName"].startswith(("narumq-ha-acceptance-", "narumq-clu14-")):
        raise PlanError("plan does not identify a disposable acceptance fixture")
    if not re.fullmatch(r"narumq-[0-2]", plan["targetPod"]) or plan["pvcName"] != f"data-{plan['targetPod']}":
        raise PlanError("targetPod must be narumq-0..2 and pvcName must be data-<targetPod>")
    if not re.fullmatch(r"narumq-[0-2]", plan["observerPod"]) or plan["observerPod"] == plan["targetPod"]:
        raise PlanError("observerPod must be another broker Pod")
    if plan["observerWorkerName"] == plan["targetWorkerName"] or plan["observerInstanceId"] == plan["targetInstanceId"]:
        raise PlanError("observer must use an independent surviving worker")
    workers = plan.get("zoneWorkers")
    if (not isinstance(workers, list) or not workers
            or not all(isinstance(w, dict) and isinstance(w.get("name"), str) and w["name"]
                       and isinstance(w.get("uid"), str) and w["uid"] for w in workers)):
        raise PlanError("zoneWorkers must list every worker of the target zone as {name, uid}")
    if len({w["name"] for w in workers}) != len(workers) or plan["targetWorkerName"] not in {w["name"] for w in workers}:
        raise PlanError("zoneWorkers must be distinct and contain the target worker")
    if plan["observerWorkerName"] in {w["name"] for w in workers}:
        raise PlanError("the observer worker must be outside the target zone")
    cases = plan.get("cases", [*wal_frames.DEFAULT_T1, *wal_frames.DEFAULT_T2])
    if (not isinstance(cases, list) or not cases or len(set(cases)) != len(cases)
            or not all(isinstance(c, str) and c in wal_frames.CASES for c in cases)):
        raise PlanError(f"cases must be distinct names from {sorted(wal_frames.CASES)}")
    plan["cases"] = cases
    image = plan.get("helperImage")
    if image is not None and (not isinstance(image, str) or not image):
        raise PlanError("helperImage must be a non-empty string")
    plan["readyDeadlineSeconds"] = _bounded(plan, "readyDeadlineSeconds", READY_DEADLINE_DEFAULT, READY_DEADLINE_BOUNDS)
    plan["refuseWaitSeconds"] = _bounded(plan, "refuseWaitSeconds", REFUSE_WAIT_DEFAULT, REFUSE_WAIT_BOUNDS)
    plan["refuseHoldSeconds"] = _bounded(plan, "refuseHoldSeconds", REFUSE_HOLD_DEFAULT, REFUSE_HOLD_BOUNDS)
    plan["podStopDeadlineSeconds"] = _bounded(plan, "podStopDeadlineSeconds", POD_STOP_DEADLINE_DEFAULT,
                                              POD_STOP_DEADLINE_BOUNDS)
    return plan


def instance_of(node: dict) -> str:
    return str(node.get("spec", {}).get("providerID", "")).rsplit("/", 1)[-1]


def read_via_exec(fixture, pod: str, container: str, path: str, timeout: float = 180) -> bytes | None:
    """Read-only `cat` of a file inside a running container; failures return None."""
    argv = fixture.kubectl + ["-n", NAMESPACE, "exec", pod, "-c", container, "--", "cat", path]
    try:
        result = subprocess.run(argv, env=fixture.environment, capture_output=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        return None
    return result.stdout if result.returncode == 0 else None


def delete_pod_graceful(fixture, name: str, uid: str) -> None:
    """One DELETE with a UID precondition and the Pod's own grace period (preStop drain runs)."""
    options = {"kind": "DeleteOptions", "apiVersion": "v1", "preconditions": {"uid": uid}}
    try:
        fixture.command(fixture.kubectl + ["delete", "--raw", f"/api/v1/namespaces/{NAMESPACE}/pods/{name}", "-f", "-"],
                        stdin=json.dumps(options))
    except (CommandFailed, subprocess.TimeoutExpired) as exc:
        raise MutationRejected("Kubernetes delete command failed; application unknown") from exc


def preflight(fixture, plan: dict, port: int = OBSERVER_PORTS[0]) -> tuple[dict, dict]:
    """Read-only ownership and safety checks. Returns (evidence, context for later steps)."""
    reject = reseed.reject
    if fixture.aws("sts", "get-caller-identity")["Account"] != reseed.ACCOUNT:
        reject("account_mismatch")
    ownership = require_owned_fixture(fixture, plan, [plan["targetInstanceId"], plan["observerInstanceId"]])
    namespace = fixture.get("namespace", NAMESPACE)
    if namespace["metadata"].get("labels", {}).get("narumq.io/acceptance-run") != fixture.run_id:
        reject("namespace_not_owned")
    cluster = fixture.api("/api/v1/cluster", port)
    if cluster.get("clusterId") != fixture.run_id or cluster.get("quorumAvailable") is not True:
        reject("cluster_identity_or_quorum")
    if str(cluster.get("leaderNodeId")) == plan["targetNodeId"]:
        reject("target_is_leader")
    pods = fixture.get("pods", "-n", NAMESPACE, "-l", "app=narumq")["items"]
    if len(pods) != 3 or not all(ready(p) for p in pods):
        reject("three_ready_brokers_required")
    target = next((p for p in pods if p["metadata"]["name"] == plan["targetPod"]), None)
    node = fixture.get_node(plan["targetWorkerName"])
    if (target is None or target["metadata"]["uid"] != plan["targetPodUID"]
            or target["spec"].get("nodeName") != plan["targetWorkerName"] or not node
            or node["metadata"]["uid"] != plan["targetNodeUID"] or instance_of(node) != plan["targetInstanceId"]
            or node["metadata"].get("labels", {}).get("topology.kubernetes.io/zone") != plan["zone"]):
        reject("target_identity_changed")
    live = {n["metadata"]["name"]: n for n in fixture.get("nodes")["items"]
            if n["metadata"].get("labels", {}).get("topology.kubernetes.io/zone") == plan["zone"]}
    if {name: n["metadata"]["uid"] for name, n in live.items()} != {w["name"]: w["uid"] for w in plan["zoneWorkers"]}:
        reject("zone_workers_differ")
    if not all(ready(n) and not n["spec"].get("unschedulable", False) and not n["metadata"].get("deletionTimestamp")
               for n in live.values()):
        reject("zone_worker_not_ready_or_already_cordoned")
    if fixture.instance_state(plan["targetInstanceId"]) != "running":
        reject("target_instance_not_running")
    volume = fixture.aws("ec2", "describe-volumes", "--volume-ids", plan["volumeId"])["Volumes"][0]
    attachments = [item.get("InstanceId") for item in volume.get("Attachments", [])]
    if volume.get("AvailabilityZone") != plan["zone"] or attachments != [plan["targetInstanceId"]]:
        reject("volume_zone_or_attachment_mismatch")
    pvc = fixture.get("pvc", plan["pvcName"], "-n", NAMESPACE)
    pv = fixture.get("pv", pvc["spec"]["volumeName"])
    if (pvc["metadata"]["uid"] != plan["pvcUID"] or pv["metadata"]["name"] != plan["pvName"]
            or pv["spec"]["csi"]["volumeHandle"] != plan["volumeId"]):
        reject("data_identity_mismatch")
    observer = next((p for p in pods if p["metadata"]["name"] == plan["observerPod"]), None)
    observer_node = fixture.get_node(plan["observerWorkerName"])
    if (observer is None or observer["metadata"]["uid"] != plan["observerPodUID"]
            or observer["spec"].get("nodeName") != plan["observerWorkerName"] or not observer_node
            or instance_of(observer_node) != plan["observerInstanceId"] or not ready(observer_node)):
        reject("observer_identity_not_current")
    try:
        fixture.api(f"/api/v1/nodes/{plan['targetNodeId']}/drain", port)
    except Exception as exc:
        if getattr(exc, "code", None) != 404:
            reject("drain_record_unreadable")
    else:
        reject("drain_record_present")
    if fixture.get("pod", helper_pod.helper_name(plan["targetPod"]), "-n", NAMESPACE, "--ignore-not-found") is not None:
        reject("helper_pod_exists")
    container = next((c for c in target["spec"]["containers"] if c["name"] == BROKER), None)
    image = plan.get("helperImage") or (container or {}).get("image")
    if not image:
        reject("helper_image_unknown")
    security = target["spec"].get("securityContext", {})
    user, group = security.get("runAsUser"), security.get("runAsGroup", security.get("fsGroup"))
    if not isinstance(user, int) or not isinstance(group, int):
        reject("broker_user_unknown")
    evidence = {"at": timestamp(), "account": reseed.ACCOUNT, "clusterName": plan["clusterName"],
                "ownership": ownership,
                "target": {"pod": plan["targetPod"], "nodeId": plan["targetNodeId"], "role": "FOLLOWER"},
                "zone": plan["zone"], "zoneWorkers": sorted(live), "volume": {"zone": plan["zone"],
                                                                              "attachedTo": attachments[0]},
                "helper": {"image": image, "runAsUser": user, "runAsGroup": group, "pinnedByDigest": "@sha256:" in image},
                "cases": plan["cases"], "walPathInVolume": helper_pod.WAL_PATH}
    return evidence, {"image": image, "user": user, "group": group}


class Run:
    def __init__(self, fixture, plan: dict, output: Path, private: Path, sleep=time.sleep, oracle_factory=None,
                 tunnel_factory=None, helper_factory=None, broker_wal_reader=None):
        self.fixture = fixture
        self.plan = plan
        self.output = output
        self.private = private
        self.sleep = sleep
        self.oracle_factory = oracle_factory or self._oracle
        self.tunnel_factory = tunnel_factory or PodTunnel
        self.helper_factory = helper_factory or helper_pod.HelperPod
        self.broker_wal_reader = broker_wal_reader or (lambda pod: read_via_exec(fixture, pod, BROKER, BROKER_WAL_PATH))
        self.criteria: dict[str, bool] = {}
        self.evidence: dict[str, dict] = {}
        self.context: dict = {}
        self.step = "init"
        self.observer = None
        self.helper = None
        self.cordoned: dict[str, str] = {}
        self.last_pod_uid = plan["targetPodUID"]
        self.restore_required: dict | None = None
        self.state: dict = {"stage": "init", "restoreRequired": None}

    # -- plumbing -----------------------------------------------------------------------------------------

    def _oracle(self):
        import payload_oracle
        return payload_oracle.Oracle(self.plan["runId"], self.private, self.output)

    def record(self, name: str, value: dict) -> None:
        self.evidence[name] = value
        save(self.output, f"torn-{name}.json", value)

    def journal(self, stage: str, **extra) -> None:
        self.state.update(stage=stage, at=timestamp(), restoreRequired=self.restore_required,
                          cordoned=sorted(self.cordoned), lastPodUID=self.last_pod_uid, **extra)
        save(self.output, "state.json", self.state)

    def fail(self, code: str):
        raise StepFailed(self.step, code)

    def pods(self) -> list[dict]:
        return self.fixture.get("pods", "-n", NAMESPACE, "-l", "app=narumq")["items"]

    def api(self, path: str) -> dict:
        return self.fixture.api(path, OBSERVER_PORTS[0])

    def new_target_pod(self, require_ready: bool) -> dict | None:
        pod = self.fixture.get_pod(self.plan["targetPod"])
        if (pod and pod["metadata"]["uid"] != self.last_pod_uid and not pod["metadata"].get("deletionTimestamp")
                and (ready(pod) or not require_ready)):
            return pod
        return None

    def cluster_recovered(self) -> dict | None:
        pods = self.pods()
        cluster = self.api("/api/v1/cluster")
        ok = (len(pods) == 3 and all(ready(p) for p in pods) and cluster["quorumAvailable"] is True
              and cluster["nodesByState"].get("ready") == 3)
        return {"readyPods": sorted(p["metadata"]["name"] for p in pods), "nodesByState": cluster["nodesByState"]} if ok else None

    # -- steps --------------------------------------------------------------------------------------------

    def step_ports(self) -> None:
        self.step = "ports"
        self.record("ports", {"at": timestamp(), "checked": reseed.check_local_ports([*OBSERVER_PORTS, *TARGET_PORTS]),
                              "bind": "127.0.0.1"})

    def step_observer(self) -> None:
        self.step = "observer"
        self.observer = self.tunnel_factory(self.fixture, self.plan["observerPod"], self.plan["observerPodUID"],
                                            *OBSERVER_PORTS)
        endpoint = self.observer.start()
        self.record("observer", {**{k: self.plan[k] for k in ("observerPod", "observerPodUID", "observerWorkerName",
                                                              "observerInstanceId")}, **endpoint})

    def step_preflight(self) -> None:
        self.step = "preflight"
        evidence, context = preflight(self.fixture, self.plan)
        self.context.update(context)
        self.record("preflight", evidence)

    def step_baseline(self) -> None:
        """Persistent session and retained marker through the survivor; the target Pod sees them before the stop."""
        self.step = "baseline"
        plan = self.plan
        oracle = self.oracle_factory()
        target = self.tunnel_factory(self.fixture, plan["targetPod"], plan["targetPodUID"], *TARGET_PORTS)
        evidence = {"startedAt": timestamp()}
        try:
            evidence["targetTunnel"] = target.start()
            oracle.prepare(TARGET_PORTS[1], OBSERVER_PORTS[1])
        except Exception as exc:
            evidence["errorCode"] = type(exc).__name__
            self.record("baseline", evidence)
            self.fail("baseline_prepare_failed")
        finally:
            target.close()
        evidence["retainedSha256"] = judge.digest(oracle.retained)
        rows = reseed.node_rows(self.api("/api/v1/nodes?limit=1000"))
        evidence["lastAppliedIndex"] = {node_id: row.get("lastAppliedIndex") for node_id, row in sorted(rows.items())}
        self.criteria["baseline.markerPublished"] = True
        self.session_started = time.monotonic()
        self.record("baseline", evidence)

    def marker_intact(self, pod_uid: str) -> bool:
        tunnel = self.tunnel_factory(self.fixture, self.plan["targetPod"], pod_uid, *TARGET_PORTS)
        try:
            tunnel.start()
            self.oracle_factory().check_retained(TARGET_PORTS[1])
            return True
        except Exception:
            return False
        finally:
            tunnel.close()

    # -- stop and start -----------------------------------------------------------------------------------

    def cordon_zone(self, evidence: dict) -> None:
        for worker in self.plan["zoneWorkers"]:
            self.cordoned[worker["name"]] = worker["uid"]  # set first: an unconfirmed patch must still be undone
            self.journal("cordoning")
            try:
                helper_pod.set_unschedulable(self.fixture, worker["name"], worker["uid"], True)
            except MutationRejected:
                evidence.setdefault("cordonCommandFailed", []).append(worker["name"])
            node = self.fixture.get_node(worker["name"])
            if not node or node["spec"].get("unschedulable") is not True:
                self.fail("cordon_unconfirmed")
        evidence["cordoned"] = {"at": timestamp(), "workers": sorted(self.cordoned)}

    def uncordon_zone(self, evidence: dict | None = None) -> None:
        for name, uid in list(self.cordoned.items()):
            try:
                helper_pod.set_unschedulable(self.fixture, name, uid, False)
            except MutationRejected:
                pass
            node = self.fixture.get_node(name)
            if node is not None and node["spec"].get("unschedulable", False):
                if evidence is not None:
                    evidence["uncordonUnconfirmed"] = name
                self.fail("uncordon_unconfirmed")
            del self.cordoned[name]
        self.journal("uncordoned")
        if evidence is not None:
            evidence["uncordoned"] = {"at": timestamp()}

    def stop_broker(self, label: str) -> dict:
        """Cordon every worker of the zone, delete the broker Pod once, wait until it is gone."""
        self.step = f"stop:{label}"
        evidence: dict = {"startedAt": timestamp()}
        pod = self.fixture.get_pod(self.plan["targetPod"])
        if pod is not None and pod["metadata"].get("labels", {}).get("app") != "narumq":
            self.fail("target_pod_not_a_broker")
        self.cordon_zone(evidence)
        if pod is not None:
            uid = pod["metadata"]["uid"]
            self.last_pod_uid = uid
            self.journal("stopping")
            evidence["podDelete"] = {"at": timestamp(), "podUID": uid, "gracefulWithPreStopDrain": True}
            try:
                delete_pod_graceful(self.fixture, self.plan["targetPod"], uid)
            except MutationRejected:
                evidence["podDelete"]["commandFailed"] = True
            try:
                reseed.wait_until(lambda: (lambda p: p is None or p["metadata"]["uid"] != uid)(
                    self.fixture.get_pod(self.plan["targetPod"])), self.plan["podStopDeadlineSeconds"], 5.0, self.sleep)
            except TimeoutError:
                self.record(f"stop-{label}", evidence)
                self.fail("broker_pod_not_stopped")
            evidence["podGone"] = {"at": timestamp()}
        replacement = self.fixture.get_pod(self.plan["targetPod"])
        if replacement is not None:
            # The replacement Pod keeps its UID when it starts later, so `last_pod_uid` stays the deleted Pod's.
            started = replacement["spec"].get("nodeName") or replacement.get("status", {}).get("phase") not in (None, "Pending")
            evidence["replacementPod"] = {"podUID": replacement["metadata"]["uid"],
                                          "phase": replacement.get("status", {}).get("phase"),
                                          "scheduled": bool(replacement["spec"].get("nodeName"))}
            if started:
                self.record(f"stop-{label}", evidence)
                self.fail("replacement_pod_not_blocked")
        self.journal("stopped")
        self.record(f"stop-{label}", evidence)
        return evidence

    def open_helper(self) -> None:
        plan, context = self.plan, self.context
        self.helper = self.helper_factory(self.fixture, plan["runId"], plan["targetPod"], plan["pvcName"],
                                          plan["targetWorkerName"], context["image"], context["user"], context["group"],
                                          self.sleep)
        self.journal("helper_creating")
        self.helper.create()
        self.helper.wait_running()
        mounts = self.helper.read("/proc/mounts").decode("utf-8", errors="strict")
        mounted = [line.split() for line in mounts.splitlines() if len(line.split()) >= 3
                   and line.split()[1] == helper_pod.MOUNT]
        if len(mounted) != 1 or mounted[0][2] != "ext4":
            self.fail("helper_volume_is_not_ext4")
        self.record("filesystem", {"mount": helper_pod.MOUNT, "type": mounted[0][2], "pvc": plan["pvcName"]})

    def close_helper(self) -> dict:
        if self.helper is None:
            return {"existed": False}
        removed = self.helper.delete()
        self.helper = None
        return removed

    def write_backup(self, name: str, data: bytes) -> tuple[str, str]:
        file_name = f"wal-backup-{self.plan['runId']}-{name}.bin"
        with (self.private / file_name).open("xb") as stream:
            stream.write(data)
        return file_name, helper_pod.sha256(data)

    def start_broker(self, evidence: dict) -> None:
        self.uncordon_zone(evidence)

    def wait_ready(self, evidence: dict, key: str) -> dict | None:
        """New broker Pod Ready, then three Ready brokers with quorum. Records the outcome, returns the Pod or None."""
        started = time.monotonic()
        try:
            pod = reseed.wait_until(lambda: self.new_target_pod(True), self.plan["readyDeadlineSeconds"], 5.0, self.sleep)
        except TimeoutError:
            pod = self.new_target_pod(False)
            evidence[key] = {"ready": False, "podUID": pod["metadata"]["uid"] if pod else None,
                             "waitedSeconds": round(time.monotonic() - started, 1)}
            return None
        self.last_pod_uid = pod["metadata"]["uid"]
        try:
            recovered = reseed.wait_until(self.cluster_recovered, self.plan["readyDeadlineSeconds"], 5.0, self.sleep)
        except TimeoutError:
            evidence[key] = {"ready": True, "threeReadyAndQuorum": False, "podUID": self.last_pod_uid}
            return None
        evidence[key] = {"ready": True, "threeReadyAndQuorum": True, "podUID": self.last_pod_uid,
                         "readySeconds": round(time.monotonic() - started, 1), **recovered}
        return pod

    # -- one case -----------------------------------------------------------------------------------------

    def inject(self, name: str, evidence: dict) -> tuple[bytes, bytes, dict]:
        """With the broker stopped: back up, write the case bytes, read them back. Returns (backup, injected, info)."""
        self.open_helper()
        before = self.helper.read()
        clean = wal_frames.classify(before)
        evidence["before"] = {"bytes": len(before), "sha256": helper_pod.sha256(before), "frames": clean["frames"],
                              "classifier": clean["outcome"]}
        if clean["outcome"] != "ok":
            self.record(f"case-{name}", evidence)
            self.fail("wal_before_injection_not_clean")
        backup_file, backup_sha = self.write_backup(name, before)
        evidence["backup"] = {"file": f"private/{backup_file}", "sha256": backup_sha}
        try:
            injected, info = wal_frames.craft(name, before)
        except wal_frames.CraftError:
            self.record(f"case-{name}", evidence)
            self.fail("no_byte_variant_for_alignment")
        evidence["crafted"] = {**info, "sha256": helper_pod.sha256(injected)}
        self.restore_required = {"case": name, "backupFile": backup_file, "backupSha256": backup_sha,
                                 "injectedSha256": helper_pod.sha256(injected)}
        self.journal(f"injecting:{name}")
        self.helper.write(injected)
        after = self.helper.read()
        self.criteria[f"{name}.injectedBytesVerified"] = after == injected
        evidence["written"] = {"bytes": len(after), "sha256": helper_pod.sha256(after), "matchesCrafted": after == injected}
        if after != injected:
            self.record(f"case-{name}", evidence)
            self.fail("injected_bytes_mismatch")
        evidence["helperRemoved"] = self.close_helper()
        self.journal(f"injected:{name}")
        return before, injected, info

    def run_truncate_case(self, name: str, last: bool) -> None:
        evidence: dict = {"case": name, "group": "T1", "startedAt": timestamp()}
        self.step = f"case:{name}"
        before, injected, info = self.inject(name, evidence)
        self.start_broker(evidence)
        pod = self.wait_ready(evidence, "start")
        self.criteria[f"{name}.readyAfterInjection"] = pod is not None and evidence["start"].get("threeReadyAndQuorum") is True
        if pod is None:
            self.record(f"case-{name}", evidence)
            self.fail("not_ready_after_injection")
        logs = reseed.fetch_logs(self.fixture, self.plan["targetPod"], False)
        logged = judge.find_torn_tail_log(logs)
        file_evidence = judge.truncation_evidence(injected, self.broker_wal_reader(self.plan["targetPod"]),
                                                  info["validBytes"])
        evidence["tornTailLog"] = logged
        evidence["walAfterStart"] = file_evidence
        evidence["validBytesExpected"] = info["validBytes"]
        self.criteria[f"{name}.tailCut"] = bool(file_evidence.get("cut") or logged.get("logged"))
        if self.criteria[f"{name}.tailCut"]:
            self.restore_required = None
            self.journal(f"truncated:{name}")
        self.criteria[f"{name}.markerIntact"] = self.marker_intact(self.last_pod_uid)
        evidence["finishedAt"] = timestamp()
        self.record(f"case-{name}", evidence)
        if not last:
            self.stop_broker(f"after-{name}")

    def observe_refusal(self, name: str, evidence: dict) -> bool:
        """Watch the restarted broker until it refuses (non-zero exit with the corruption message); it must never be Ready."""
        plan = self.plan
        reason = wal_frames.CASES[name]["reason"]
        observations: list[dict] = []
        ever_ready = False
        outcome: dict = {}

        def poll():
            nonlocal ever_ready, outcome
            pod = self.new_target_pod(False)
            if pod is None:
                return None
            ever_ready = ever_ready or ready(pod)
            status = reseed.container_status(pod)
            ended = status is not None and bool((status.get("state") or {}).get("terminated")
                                                or (status.get("lastState") or {}).get("terminated"))
            logs = reseed.fetch_logs(self.fixture, plan["targetPod"], (status or {}).get("restartCount", 0) > 0) if ended else None
            verdict = judge.classify_refusal(status, logs, reason, self.last_pod_uid, pod["metadata"]["uid"])
            observations.append({"at": timestamp(), "podUID": pod["metadata"]["uid"], "verdict": verdict["verdict"],
                                 "reason": verdict["reason"], "restartCount": verdict.get("restartCount")})
            del observations[:-20]
            if verdict["verdict"] in ("refused", "fail"):
                outcome = {**verdict, "podUID": pod["metadata"]["uid"]}
                return True
            return None

        try:
            reseed.wait_until(poll, plan["refuseWaitSeconds"], 3.0, self.sleep)
        except TimeoutError:
            outcome = {"verdict": "timeout", "reason": "no_terminated_container_before_deadline"}
        hold_until = time.monotonic() + (plan["refuseHoldSeconds"] if outcome.get("verdict") == "refused" else 0)
        while time.monotonic() < hold_until and not ever_ready:
            pod = self.new_target_pod(False)
            ever_ready = ever_ready or bool(pod and ready(pod))
            self.sleep(5)
        evidence["refusal"] = {**outcome, "everReady": ever_ready, "observations": observations}
        self.criteria[f"{name}.refusedStart"] = outcome.get("verdict") == "refused" and outcome.get("exitCode") not in (0, None)
        self.criteria[f"{name}.neverReady"] = not ever_ready
        if outcome.get("podUID"):
            self.last_pod_uid = outcome["podUID"]
        return self.criteria[f"{name}.refusedStart"] and not ever_ready

    def run_refusal_case(self, name: str, last: bool) -> None:
        evidence: dict = {"case": name, "group": "T2", "startedAt": timestamp()}
        self.step = f"case:{name}"
        backup, injected, info = self.inject(name, evidence)
        self.start_broker(evidence)
        refused = self.observe_refusal(name, evidence)
        self.record(f"case-{name}", evidence)
        # The crash-looping Pod is removed whatever the verdict; the helper then checks and restores the file.
        self.stop_broker(f"after-{name}")
        self.open_helper()
        current = self.helper.read()
        evidence["walAfterRefusedStart"] = {"bytes": len(current), "sha256": helper_pod.sha256(current),
                                           "unchangedFromInjected": current == injected}
        self.criteria[f"{name}.walUnchangedByRefusedStart"] = current == injected
        self.helper.write(backup)
        restored = self.helper.read()
        sha_ok = restored == backup
        evidence["restored"] = {"bytes": len(restored), "sha256": helper_pod.sha256(restored),
                                "expectedSha256": helper_pod.sha256(backup), "matches": sha_ok}
        self.criteria[f"{name}.restoredShaMatches"] = sha_ok
        if not sha_ok:
            self.record(f"case-{name}", evidence)
            self.fail("restored_bytes_mismatch")
        self.restore_required = None
        self.journal(f"restored:{name}")
        evidence["helperRemoved"] = self.close_helper()
        self.start_broker(evidence)
        pod = self.wait_ready(evidence, "startAfterRestore")
        self.criteria[f"{name}.readyAfterRestore"] = pod is not None and evidence["startAfterRestore"].get("threeReadyAndQuorum") is True
        if pod is None:
            self.record(f"case-{name}", evidence)
            self.fail("not_ready_after_restore")
        self.criteria[f"{name}.markerIntact"] = self.marker_intact(self.last_pod_uid)
        evidence["finishedAt"] = timestamp()
        self.record(f"case-{name}", evidence)
        if not refused:
            self.fail("broker_did_not_refuse")
        if not last:
            self.stop_broker(f"after-{name}")

    def step_cases(self) -> None:
        self.stop_broker("initial")
        cases = self.plan["cases"]
        for position, name in enumerate(cases):
            last = position == len(cases) - 1
            if wal_frames.CASES[name]["group"] == "T1":
                self.run_truncate_case(name, last)
            else:
                self.run_refusal_case(name, last)

    def step_final(self) -> None:
        self.step = "final"
        evidence: dict = {"at": timestamp()}
        evidence["sessionAgeSeconds"] = round(time.monotonic() - self.session_started, 1) if hasattr(self, "session_started") else None
        try:
            recovered = reseed.wait_until(self.cluster_recovered, self.plan["readyDeadlineSeconds"], 5.0, self.sleep)
            evidence["recovered"] = recovered
            self.criteria["final.threeReadyAndQuorum"] = True
        except TimeoutError:
            self.criteria["final.threeReadyAndQuorum"] = False
        pod = self.fixture.get_pod(self.plan["targetPod"])
        tunnel = self.tunnel_factory(self.fixture, self.plan["targetPod"], pod["metadata"]["uid"] if pod else "",
                                     *TARGET_PORTS)
        try:
            evidence["targetTunnel"] = tunnel.start()
            self.oracle_factory().verify(TARGET_PORTS[1])
            self.criteria["final.durableSession"] = True
        except Exception as exc:
            evidence["durableErrorCode"] = type(exc).__name__
            self.criteria["final.durableSession"] = False
        finally:
            tunnel.close()
        self.record("final", evidence)

    def execute(self) -> None:
        for step in (self.step_ports, self.step_observer, self.step_preflight, self.step_baseline, self.step_cases,
                     self.step_final):
            step()

    # -- teardown -----------------------------------------------------------------------------------------

    def cleanup(self, report: dict) -> None:
        """Remove the helper; uncordon only when no corruption is left in the WAL."""
        try:
            removed = self.close_helper()
            self.criteria["cleanup.helperRemoved"] = True
            report["helperAtCleanup"] = removed
        except Exception as exc:
            self.criteria["cleanup.helperRemoved"] = False
            report.setdefault("teardownErrors", {})["helper"] = getattr(exc, "code", type(exc).__name__)
            report["teardownFailed"] = True
        if self.restore_required is not None:
            # The WAL still holds the injected corruption: starting the broker would only crash-loop it.
            self.criteria["cleanup.workersUncordoned"] = False
            report["restoreRequired"] = self.restore_required
            report["restoreHint"] = "run `main.py restore --execute` with this output directory and the same plan"
            report["teardownFailed"] = True
        else:
            try:
                self.uncordon_zone(None)
                self.criteria["cleanup.workersUncordoned"] = True
            except Exception as exc:
                self.criteria["cleanup.workersUncordoned"] = False
                report.setdefault("teardownErrors", {})["uncordon"] = getattr(exc, "code", type(exc).__name__)
                report["teardownFailed"] = True
        try:
            if self.observer is not None:
                self.observer.close()
        except Exception as exc:
            report.setdefault("teardownErrors", {})["observerTunnel"] = type(exc).__name__
            report["teardownFailed"] = True
        self.journal("finished")

    # -- restore ------------------------------------------------------------------------------------------

    def restore(self, required: dict, out: Path) -> dict:
        """Write the private backup back into the stopped broker's WAL and start the broker. Idempotent."""
        self.step = "restore"
        name = required["backupFile"]
        if not BACKUP_NAME.match(name):
            self.fail("backup_file_name_invalid")
        backup = (self.private / name).read_bytes()
        if helper_pod.sha256(backup) != required["backupSha256"]:
            self.fail("backup_file_sha_mismatch")
        evidence: dict = {"startedAt": timestamp(), "backupSha256": required["backupSha256"]}
        self.stop_broker("restore")
        self.open_helper()
        current = self.helper.read()
        sha = helper_pod.sha256(current)
        evidence["before"] = {"sha256": sha, "isInjected": sha == required["injectedSha256"],
                              "isBackup": sha == required["backupSha256"]}
        if sha not in (required["injectedSha256"], required["backupSha256"]):
            self.fail("wal_matches_neither_injected_nor_backup")
        if sha != required["backupSha256"]:
            self.helper.write(backup)
        restored = self.helper.read()
        evidence["restored"] = {"sha256": helper_pod.sha256(restored), "matches": restored == backup}
        if restored != backup:
            self.fail("restored_bytes_mismatch")
        self.restore_required = None
        self.journal("restored")
        evidence["helperRemoved"] = self.close_helper()
        self.uncordon_zone(evidence)
        pod = self.wait_ready(evidence, "start")
        evidence["ready"] = pod is not None and evidence["start"].get("threeReadyAndQuorum") is True
        save(out, "torn-restore.json", evidence)
        return evidence


def finish_report(report: dict, run: "Run | None", cases: list[str]) -> None:
    if run is not None:
        run.cleanup(report)
        report["steps"] = sorted(run.evidence)
    judged = judge.aggregate(run.criteria if run else {}, cases, report.get("failedStep"))
    if report.get("teardownFailed"):
        judged["result"] = "FAIL"
    report.update(judged)
    report["scope"] = ("model: tool-written WAL bytes on a stopped broker's real EBS volume; not a natural torn write "
                       "and not an EBS write-cache loss")
    report["finishedAt"] = timestamp()


def execute_run(args) -> int:
    args.output.mkdir(parents=True, exist_ok=False)
    run_id = args.output.name
    report = {"result": "FAIL", "runId": run_id, "startedAt": timestamp()}
    run = None
    cases: list[str] = []
    try:
        reseed.check_dependencies(DEPENDENCIES)
        fixture = Fixture(args.private, run_id)
        plan = load_plan(json.loads(args.plan.read_text(encoding="utf-8")), run_id)
        cases = plan["cases"]
        run = Run(fixture, plan, args.output, args.private)
        run.execute()
    except reseed.PreflightRejected as exc:
        report["errorCode"] = exc.code
        report["failedStep"] = run.step if run else ("dependencies" if exc.code.startswith("dependency_missing:") else "plan")
    except StepFailed as exc:
        report["errorCode"] = exc.code
        report["failedStep"] = exc.step
    except helper_pod.HelperError as exc:
        report["errorCode"] = exc.code
        report["failedStep"] = run.step if run else "plan"
    except Exception as exc:
        report["errorCode"] = type(exc).__name__
        report["failedStep"] = run.step if run else "plan"
    finally:
        finish_report(report, run, cases)
        save(args.output, "summary.json", report)
    print(json.dumps(report))
    return 0 if report["result"] == "PASS" else 1


def execute_restore(args) -> int:
    """Finish an interrupted run: write the private backup back and start the broker. Original records stay untouched."""
    original = args.output
    run_id = original.name
    number = 1
    while (original / f"restore-{number}").exists():
        number += 1
    out = original / f"restore-{number}"
    out.mkdir()
    report = {"result": "FAIL", "runId": run_id, "mode": "restore", "restoreNumber": number, "startedAt": timestamp()}
    run = None
    try:
        reseed.check_dependencies(DEPENDENCIES)
        state = json.loads((original / "state.json").read_text(encoding="utf-8"))
        required = state.get("restoreRequired")
        if not isinstance(required, dict) or not {"backupFile", "backupSha256", "injectedSha256"} <= set(required):
            raise reseed.PreflightRejected("nothing_to_restore")
        fixture = Fixture(args.private, run_id)
        plan = load_plan(json.loads(args.plan.read_text(encoding="utf-8")), run_id)
        if fixture.aws("sts", "get-caller-identity")["Account"] != reseed.ACCOUNT:
            raise reseed.PreflightRejected("account_mismatch")
        ownership = require_owned_fixture(fixture, plan, [plan["targetInstanceId"], plan["observerInstanceId"]])
        namespace = fixture.get("namespace", NAMESPACE)
        if namespace["metadata"].get("labels", {}).get("narumq.io/acceptance-run") != run_id:
            raise reseed.PreflightRejected("namespace_not_owned")
        pvc = fixture.get("pvc", plan["pvcName"], "-n", NAMESPACE)
        pv = fixture.get("pv", plan["pvName"])
        node = fixture.get_node(plan["targetWorkerName"])
        if (pvc["metadata"]["uid"] != plan["pvcUID"] or pvc["spec"]["volumeName"] != plan["pvName"]
                or pv["spec"]["csi"]["volumeHandle"] != plan["volumeId"] or node is None
                or node["metadata"]["uid"] != plan["targetNodeUID"] or instance_of(node) != plan["targetInstanceId"]):
            raise reseed.PreflightRejected("restore_target_identity_changed")
        previous = json.loads((original / "torn-preflight.json").read_text(encoding="utf-8"))
        helper = previous["helper"]
        run = Run(fixture, plan, out, args.private)
        run.context = {"image": helper["image"], "user": helper["runAsUser"], "group": helper["runAsGroup"]}
        run.restore_required = required
        run.record("restore-preflight", {"ownership": ownership, "pvcUID": plan["pvcUID"], "volumeId": plan["volumeId"]})
        # A crashed controller can leave its helper behind. Delete only the helper owned by this run.
        run.helper = helper_pod.HelperPod(fixture, run_id, plan["targetPod"], plan["pvcName"],
                                         plan["targetWorkerName"], helper["image"], helper["runAsUser"],
                                         helper["runAsGroup"])
        run.close_helper()
        run.step_observer()
        evidence = run.restore(required, out)
        report["restore"] = evidence
        report["result"] = "PASS" if evidence.get("ready") else "FAIL"
    except reseed.PreflightRejected as exc:
        report["errorCode"] = exc.code
    except StepFailed as exc:
        report.update(errorCode=exc.code, failedStep=exc.step)
    except helper_pod.HelperError as exc:
        report.update(errorCode=exc.code, failedStep=run.step if run else "plan")
    except Exception as exc:
        report.update(errorCode=type(exc).__name__, failedStep=run.step if run else "plan")
    finally:
        if run is not None:
            try:
                run.close_helper()
                if run.observer is not None:
                    run.observer.close()
            except Exception as exc:
                report["teardownError"] = type(exc).__name__
                report["result"] = "FAIL"
        report["finishedAt"] = timestamp()
        save(out, "summary.json", report)
    print(json.dumps(report))
    return 0 if report["result"] == "PASS" else 1


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="mode", required=True)
    run_parser = sub.add_parser("run", help="stop one follower, inject WAL tails case by case, restart and judge")
    restore_parser = sub.add_parser("restore", help="write the private WAL backup back after an interrupted T2 case")
    for command in (run_parser, restore_parser):
        command.add_argument("--private", type=Path, required=True)
        command.add_argument("--output", type=Path, required=True,
                             help="run output directory" if command is run_parser
                             else "the existing output directory of the interrupted run")
        command.add_argument("--plan", type=Path, required=True)
        command.add_argument("--execute", action="store_true",
                             help="allow Pod deletion, node cordon and WAL writes on the checked fixture")
    args = parser.parse_args(argv)
    if not args.execute:
        parser.error(f"{args.mode} requires --execute")
    if args.mode == "restore":
        if not args.output.is_dir():
            parser.error("restore --output must be the existing output directory of the interrupted run")
        return execute_restore(args)
    return execute_run(args)


if __name__ == "__main__":
    raise SystemExit(main())
