"""DEP-21 EBS disk pressure and ENOSPC acceptance (D1, D2, D3) for a disposable synthetic EKS fixture.

The only mutation is a ballast file written by an ephemeral container into the target Pod's PVC filesystem, plus
a UID-preconditioned Pod delete when a failed broker does not restart by itself after the ballast is removed.
The ballast is always removed in `finally` before the process ends.
"""
from __future__ import annotations

import argparse
import contextlib
import json
from pathlib import Path
import sys
import threading
import time
from types import SimpleNamespace

HERE = Path(__file__).resolve().parent
RECOVERY_DIR = HERE.parent / "eks-worker-recovery"
for _directory in (str(HERE), str(RECOVERY_DIR)):
    if _directory not in sys.path:
        sys.path.insert(0, _directory)

import pressure_ballast as ballast_module  # noqa: E402
import pressure_logic as logic  # noqa: E402
from observer import IndependentMonitor, timestamp  # noqa: E402

ACCOUNT = "809446814769"
OBSERVER_PORTS = (23880, 23883)
TARGET_PORTS = (24880, 24883)
D1_PRESSURE_WINDOW_SECONDS = 15
D1_RECOVERY_WINDOW_SECONDS = 20
BASELINE_SEND_SECONDS = 5
POLL_SECONDS = 1
RETAINED_CLEANUP_SECONDS = 60
RETAINED_CLEANUP_RETRY_SECONDS = 5
RECOVERY_POLL_SECONDS = 3
LOAD_PORTS = (27880, 27883)
SECOND_TARGET_PORTS = (22880, 22883)  # 25839-26038 can fall in a Windows excluded port range
TUNNEL_RETRY_SECONDS = 3
PREVIOUS_LOG_WAIT_SECONDS = 45
PREFLIGHT_SETTLE_SECONDS = 120
NODE_FIELDS = ("nodeId", "queryStatus", "queryErrorCode", "state", "ready", "readinessReason", "storagePressure",
               "replicationHealth", "lastErrorCode")


def load_runtime() -> SimpleNamespace:
    """Import the shared fixture modules lazily so pure logic and tests do not need paho or kubectl."""
    import durable_stream
    import fixture
    import pod_tunnel
    import pressure_probe
    return SimpleNamespace(Fixture=fixture.Fixture, PodTunnel=pod_tunnel.PodTunnel,
                           prepare_subscriber=durable_stream.prepare_subscriber,
                           DurablePublisher=durable_stream.DurablePublisher,
                           verify_subscriber=durable_stream.verify_subscriber, QoS1Probe=pressure_probe.QoS1Probe,
                           ProbeLoop=pressure_probe.ProbeLoop)


def main_container_restarts(pod: dict | None) -> int | None:
    if not pod:
        return None
    status = next((s for s in (pod.get("status") or {}).get("containerStatuses", []) if s.get("name") == "narumq"), None)
    return None if status is None else status.get("restartCount")


class ScenarioError(RuntimeError):
    """A scenario stopped for a reason this tool names; the message is fixed text and safe to record."""


class ObserverTunnelLost(ScenarioError):
    """The observer port-forward ended; sampled results after this moment prove nothing about the product."""


def container_view(pod: dict | None) -> dict | None:
    """Container `narumq` status fields that explain restarts: readiness, restart count, last termination."""
    status = next((s for s in ((pod or {}).get("status") or {}).get("containerStatuses", [])
                   if s.get("name") == "narumq"), None)
    if status is None:
        return None
    terminated = (status.get("lastState") or {}).get("terminated")
    return {"ready": status.get("ready"), "restartCount": status.get("restartCount"),
            "state": next(iter(status.get("state") or {}), None),
            "lastTerminated": None if not terminated else {key: terminated.get(key) for key in
                                                          ("exitCode", "reason", "finishedAt")}}


class ManagedTunnel:
    """A PodTunnel to one exact Pod that may be re-established to the SAME Pod UID after it ends.

    A restarted container drops the port-forward; PodTunnel only starts for a Ready Pod, so re-establishing can
    fail for a while. Every loss, re-establish and the failed attempts in between are recorded.
    """

    def __init__(self, run: "StoragePressureRun", name: str, pod: str, ports: tuple[int, int]):
        self.run, self.name, self.pod, self.ports = run, name, pod, ports
        self.uid = run.uids[pod]
        self.tunnel = None
        self.events: list[dict] = []
        self.failed_attempts = 0
        self.lost_at: float | None = None
        self.last_attempt: float | None = None
        self.reconnects = 0

    def _new(self):
        return self.run.runtime.PodTunnel(self.run.fixture, self.pod, self.uid, *self.ports)

    def start(self) -> dict:
        tunnel = self._new()
        evidence = tunnel.start()
        self.tunnel = tunnel
        self.events.append({"event": "established", "at": timestamp(), "initial": True})
        return evidence

    def alive(self) -> bool:
        if self.tunnel is None:
            return False
        try:
            self.tunnel.check()
            return True
        except Exception:
            return False

    def ensure(self) -> bool:
        """True when a live tunnel exists; otherwise at most one re-establish attempt per retry interval."""
        if self.alive():
            return True
        now = self.run.clock()
        if self.tunnel is not None:
            self._close_quietly(self.tunnel)
            self.tunnel = None
            self.lost_at = now
            self.events.append({"event": "lost", "at": timestamp()})
        if self.last_attempt is not None and now - self.last_attempt < TUNNEL_RETRY_SECONDS:
            return False
        self.last_attempt = now
        tunnel = self._new()
        try:
            tunnel.start()
        except Exception as exc:
            self.failed_attempts += 1
            if not self.events or self.events[-1]["event"] != "attempt_failed":
                self.events.append({"event": "attempt_failed", "at": timestamp(), "errorCode": type(exc).__name__})
            self._close_quietly(tunnel)
            return False
        self.tunnel = tunnel
        self.reconnects += 1
        gap = None if self.lost_at is None else round(now - self.lost_at, 3)
        self.events.append({"event": "established", "at": timestamp(), "initial": False, "gapSeconds": gap})
        self.lost_at = None
        return True

    @staticmethod
    def _close_quietly(tunnel) -> None:
        try:
            tunnel.close()
        except Exception:
            pass

    def close(self) -> None:
        tunnel, self.tunnel = self.tunnel, None
        if tunnel is not None:
            tunnel.close()

    def summary(self) -> dict:
        return {"pod": self.pod, "podUID": self.uid, "ports": list(self.ports), "reconnects": self.reconnects,
                "failedAttempts": self.failed_attempts, "events": list(self.events)}


class TunneledProbe:
    """Load probe that re-establishes its own tunnel before an attempt and records an unavailable tunnel."""

    def __init__(self, tunnel: ManagedTunnel, probe, epoch: float):
        self.tunnel, self.probe, self.epoch = tunnel, probe, epoch

    def attempt(self) -> dict:
        if not self.tunnel.ensure():
            now = time.monotonic() - self.epoch
            return {"at": timestamp(), "startedSeconds": now, "finishedSeconds": now, "outcome": "tunnel_unavailable",
                    "reasonCode": None, "puback": False, "payloadBytes": 0}
        return self.probe.attempt()

    def clear_retained(self) -> list[dict]:
        self.tunnel.ensure()
        return self.probe.clear_retained()

    def close(self) -> None:
        self.probe.close()


# Scenarios: drain-and-rolling-update.md:DRN-SCN-038
class StoragePressureRun:
    def __init__(self, fixture, plan: dict, output: Path, private: Path, runtime, scenarios: list[str],
                 sleep=time.sleep, clock=time.monotonic):
        self.fixture = fixture
        self.plan = plan
        self.output = output
        self.private = private
        self.runtime = runtime
        self.scenarios = scenarios
        self.sleep = sleep
        self.clock = clock
        self.epoch = time.monotonic()
        self.uids = {plan["targetPod"]: plan["targetPodUID"], plan["observerPod"]: plan["observerPodUID"]}
        if "secondTargetPod" in plan and "secondTargetPodUID" in plan:
            self.uids[plan["secondTargetPod"]] = plan["secondTargetPodUID"]
        self.ballast = ballast_module.BallastController(fixture, plan["brokerImage"], sleep=sleep, clock=clock)
        self.observer = None
        self.closers: list[tuple[str, object]] = []
        self.cleanup_errors: list[str] = []
        self.current_step = "init"
        self.partial: dict = {}
        self.observer_lost_at = None
        self.observer_lost_seconds = None
        self.publisher_results: dict[int, dict] = {}
        self.loads: dict[str, dict] = {}

    # -- evidence and resources --

    def save(self, name: str, value) -> None:
        logic.write_json(self.output, name, value)

    def elapsed(self) -> float:
        return time.monotonic() - self.epoch

    def track(self, name: str, closer) -> None:
        self.closers.append((name, closer))

    def release_resources(self) -> None:
        while self.closers:
            name, closer = self.closers.pop()
            try:
                closer()
            except Exception as exc:
                self.cleanup_errors.append(f"{name}:{type(exc).__name__}")

    @contextlib.contextmanager
    def target_tunnel(self, pod: str):
        tunnel = self.runtime.PodTunnel(self.fixture, pod, self.uids[pod], *TARGET_PORTS)
        evidence = tunnel.start()
        try:
            yield tunnel, evidence
        finally:
            try:
                tunnel.close()
            except Exception as exc:
                self.cleanup_errors.append(f"target-tunnel:{type(exc).__name__}")

    # -- cluster reads --

    def observer_api(self, path: str):
        return self.fixture.api(path, OBSERVER_PORTS[0])

    def node_record(self, node_id: str) -> dict:
        try:
            return {"ok": True, "node": self.observer_api(f"/api/v1/nodes/{node_id}")}
        except Exception as exc:
            return {"ok": False, "errorCode": type(exc).__name__}

    def local_node_record(self, pod: str, node_id: str) -> dict:
        """The node's own measurement, read inside its broker container; the token never leaves the Pod."""
        script = ('curl --silent --max-time 5 -H "Authorization: Bearer $(cat /secrets/admin.token)" '
                  f"http://127.0.0.1:8080/api/v1/nodes/{node_id}")
        try:
            output = self.fixture.command(self.fixture.kubectl + ["-n", "narumq", "exec", pod, "-c", "narumq", "--",
                                                                  "/bin/sh", "-c", script], timeout=30)
            return {"ok": True, "node": json.loads(output)["data"]}
        except Exception as exc:
            return {"ok": False, "errorCode": type(exc).__name__}

    def pods(self) -> list[dict]:
        return self.fixture.get("pods", "-n", "narumq", "-l", "app=narumq")["items"]

    def pvc_uids(self, pods: list[str]) -> dict[str, str]:
        result = {}
        for pod in pods:
            pvc = self.fixture.get("pvc", f"data-{pod}", "-n", "narumq")
            if (pvc.get("status") or {}).get("phase") != "Bound":
                raise ValueError(f"PVC data-{pod} is not Bound")
            result[f"data-{pod}"] = pvc["metadata"]["uid"]
        return result

    def check_cluster(self, scenarios: list[str]) -> dict:
        """Cluster, roles and identities that must hold right before a fault; raises ValueError otherwise."""
        plan = self.plan
        cluster = self.observer_api("/api/v1/cluster")
        if cluster["clusterId"] != self.fixture.run_id or cluster["quorumAvailable"] is not True:
            raise ValueError("cluster must belong to this run and have quorum")
        logic.check_roles(plan, str(cluster["leaderNodeId"]), scenarios)
        items = self.pods()
        if len(items) != 3 or not all(logic.pod_ready(p) for p in items):
            raise ValueError("three Ready voters are required before the fault")
        by_name = {p["metadata"]["name"]: p for p in items}
        for name, uid in self.uids.items():
            pod = by_name.get(name)
            if not pod or pod["metadata"]["uid"] != uid or pod["metadata"].get("deletionTimestamp"):
                raise ValueError(f"Pod {name} identity differs from the expected UID")
            image = next((c["image"] for c in pod["spec"]["containers"] if c["name"] == "narumq"), None)
            if image != plan["brokerImage"]:
                raise ValueError(f"Pod {name} does not run the planned broker image")
        nodes = self.observer_api("/api/v1/nodes")
        wanted = {plan["targetNodeId"], plan["observerNodeId"]} | (
            {plan["secondTargetNodeId"]} if "secondTargetNodeId" in plan else set())
        by_id = {str(n.get("nodeId")): n for n in nodes} if isinstance(nodes, list) else {}
        if not wanted <= set(by_id) or not all(by_id[i].get("queryStatus") == "OK" and by_id[i].get("ready") is True
                                               for i in wanted):
            raise ValueError("planned member ids are not all queryable and ready")
        return {"leaderNodeId": str(cluster["leaderNodeId"]), "nodesByState": cluster.get("nodesByState"),
                "podUIDs": {name: pod["metadata"]["uid"] for name, pod in by_name.items()}}

    # -- run lifecycle --

    def open_observer(self) -> dict:
        plan = self.plan
        self.observer = self.runtime.PodTunnel(self.fixture, plan["observerPod"], plan["observerPodUID"], *OBSERVER_PORTS)
        self.track("observer-tunnel", self.observer.close)
        self.observer_lost_at = None
        return self.observer.start()

    def close_observer(self) -> None:
        keep, close = [], []
        for item in self.closers:
            (close if item[0] == "observer-tunnel" else keep).append(item)
        self.closers = keep
        for name, closer in close:
            try:
                closer()
            except Exception as exc:
                self.cleanup_errors.append(f"{name}:{type(exc).__name__}")
        self.observer = None

    def require_observer(self) -> None:
        """The observer port-forward carries the monitor and the durable publisher; without it nothing is judged."""
        if self.observer is None:
            return
        try:
            self.observer.check()
        except Exception as exc:
            if self.observer_lost_at is None:
                self.observer_lost_at = timestamp()
                self.observer_lost_seconds = self.elapsed()
            raise ObserverTunnelLost("observer tunnel lost") from exc

    def start(self) -> dict:
        """Open the observer tunnel, then check ownership and roles before any ballast exists."""
        plan = self.plan
        endpoint = self.open_observer()
        self.save("observer-endpoint.json", endpoint)
        if self.fixture.aws("sts", "get-caller-identity")["Account"] != ACCOUNT:
            raise ValueError("AWS account differs from acceptance account")
        namespace = self.fixture.get("namespace", "narumq")
        if namespace["metadata"].get("labels", {}).get("narumq.io/acceptance-run") != self.fixture.run_id:
            raise ValueError("namespace is not owned by this synthetic fixture")
        cluster = self.check_cluster(self.scenarios)
        evidence = {"at": timestamp(), "scenarios": self.scenarios, "targetPod": plan["targetPod"],
                    "secondTargetPod": plan.get("secondTargetPod"), "observerPod": plan["observerPod"],
                    "cluster": cluster}
        self.save("preflight.json", evidence)
        return evidence

    def fresh_preflight(self, name: str) -> dict:
        """Before every later scenario: drop the old observer tunnel, open a new one and check the cluster again.

        The previous scenario may have restarted brokers, so the check is retried for a bounded settle window and
        every attempt is recorded.
        """
        self.step(f"{name.lower()}-preflight")
        self.close_observer()
        deadline = self.clock() + PREFLIGHT_SETTLE_SECONDS
        attempts = []
        while True:
            try:
                endpoint = self.open_observer()
                cluster = self.check_cluster([name])
                break
            except Exception as exc:
                attempts.append({"at": timestamp(), "errorCode": type(exc).__name__})
                self.close_observer()
                if self.clock() >= deadline:
                    self.save(f"{name.lower()}-preflight.json", {"result": "FAIL", "attempts": attempts})
                    raise ScenarioError("fresh preflight did not pass within the settle window") from exc
                self.sleep(5)
        evidence = {"at": timestamp(), "observerEndpoint": endpoint, "cluster": cluster, "failedAttempts": attempts}
        self.save(f"{name.lower()}-preflight.json", evidence)
        return evidence

    def record_cluster_state(self) -> dict:
        """Cluster and Pod state read through a fresh observer tunnel; every failure is recorded, none raised."""
        state = {"at": timestamp()}
        try:
            self.close_observer()
            state["observerEndpoint"] = self.open_observer()
            cluster = self.observer_api("/api/v1/cluster")
            state["cluster"] = {key: cluster.get(key) for key in ("quorumAvailable", "leaderNodeId", "nodesByState")}
            nodes = self.observer_api("/api/v1/nodes")
            state["nodes"] = [{field: node.get(field) for field in NODE_FIELDS} for node in nodes
                              if isinstance(node, dict)] if isinstance(nodes, list) else None
        except Exception as exc:
            state["errorCode"] = type(exc).__name__
        try:
            state["pods"] = [{"name": p["metadata"]["name"], "uid": p["metadata"]["uid"], "ready": logic.pod_ready(p),
                              "container": container_view(p)} for p in self.pods()]
        except Exception as exc:
            state["podsErrorCode"] = type(exc).__name__
        return state

    def step(self, name: str) -> None:
        self.current_step = name

    def run_scenarios(self) -> dict:
        results = {}
        for index, name in enumerate(self.scenarios):
            method = {"D1": self.scenario_d1, "D2": self.scenario_d2, "D3": self.scenario_d3}[name]
            started = timestamp()
            self.partial = {}
            self.current_step = name.lower() + "-start"
            try:
                if index:
                    self.fresh_preflight(name)
                result = method()
            except ObserverTunnelLost:
                result = {"result": "FAIL", "outcome": "inconclusive_observer_tunnel_lost",
                          "reasons": ["inconclusive_observer_tunnel_lost"], "observerTunnelLostAt": self.observer_lost_at,
                          "observerTunnelLostSecondsIntoRun": self.observer_lost_seconds,
                          "errorStep": self.current_step, "partial": self.partial}
            except Exception as exc:
                result = {"result": "FAIL", "reasons": ["exception"], "errorCode": type(exc).__name__,
                          "errorStep": self.current_step, "errorLocation": logic.error_location(exc),
                          # Only messages written by this tool are kept; library messages can hold URLs or tokens.
                          "errorMessage": exc.args[0] if isinstance(exc, (ScenarioError, ballast_module.BallastError))
                          and exc.args and isinstance(exc.args[0], str) else "unclassified",
                          "partial": self.partial}
            finally:
                self.release_resources_except_observer()
            if result["result"] != "PASS" or "errorCode" in result:
                # A failed scenario must not leave ballast behind while later scenarios decide whether to run.
                result["ballastRemovalAfterScenario"] = self.ballast.remove_all()
            if self.loads:
                # Ballast is gone now, so a recovering cluster can accept the delete; the scenario may have ended early.
                result["retainedCleanup"] = {key: self.cleanup_retained(key) for key in list(self.loads)}
            if result.get("outcome") == "inconclusive_observer_tunnel_lost":
                result["clusterStateAfterLoss"] = self.record_cluster_state()
            result["startedAt"], result["finishedAt"] = started, timestamp()
            self.save(f"{name.lower()}-result.json", result)
            results[name] = result
        return results

    def release_resources_except_observer(self) -> None:
        keep = [item for item in self.closers if item[0] == "observer-tunnel"]
        self.closers = [item for item in self.closers if item[0] != "observer-tunnel"]
        self.release_resources()
        self.closers = keep

    def finish(self) -> dict:
        """`finally` path: stop background work, remove every ballast, and report leftover Pod state."""
        report = {}
        self.release_resources()
        report["ballastRemoval"] = self.ballast.remove_all()
        report["ballastContainerNote"] = ("ephemeral containers cannot be deleted; the idle `ballast` container stays "
                                          "until its Pod is replaced")
        report["retainedCleanup"] = {name: self.cleanup_retained(name) for name in list(self.loads)}
        report["finalClusterState"] = self.record_cluster_state()
        self.release_resources()
        try:
            report["finalPods"] = [{"name": p["metadata"]["name"], "uid": p["metadata"]["uid"],
                                    "ready": logic.pod_ready(p), "restarts": main_container_restarts(p)}
                                   for p in self.pods()]
        except Exception as exc:
            report["finalPodsErrorCode"] = type(exc).__name__
        report["cleanupErrors"] = list(self.cleanup_errors)
        return report

    # -- recovery shared by D2 and D3 --

    def delete_pod(self, pod: str, uid: str) -> None:
        """One DELETE with a UID precondition and the Pod's default grace period (preStop runs normally)."""
        options = {"kind": "DeleteOptions", "apiVersion": "v1", "preconditions": {"uid": uid}}
        self.fixture.command(self.fixture.kubectl + ["delete", "--raw", f"/api/v1/namespaces/narumq/pods/{pod}",
                                                     "-f", "-"], stdin=json.dumps(options))

    def observe_recovery(self, targets: list[str], expected_pvcs: dict[str, str]) -> dict:
        self.require_observer()
        items = self.pods()
        for pod in items:
            name = pod["metadata"]["name"]
            if name not in targets and name in self.uids and pod["metadata"]["uid"] != self.uids[name]:
                raise ScenarioError(f"Pod {name} was replaced although it was not a fault target")
        try:
            pvcs = {f"data-{name}": self.fixture.get("pvc", f"data-{name}", "-n", "narumq")["metadata"]["uid"]
                    for name in targets}
        except Exception:
            pvcs = {}
        try:
            cluster = self.observer_api("/api/v1/cluster")
            cluster = {key: cluster.get(key) for key in ("quorumAvailable", "leaderNodeId", "nodesByState")}
        except Exception:
            cluster = None
        problems = logic.recovery_problems(items, pvcs, expected_pvcs, cluster)
        return {"at": timestamp(), "problems": problems,
                "pods": [{"name": p["metadata"]["name"], "uid": p["metadata"]["uid"], "ready": logic.pod_ready(p),
                          "restarts": main_container_restarts(p),
                          "deletionTimestamp": p["metadata"].get("deletionTimestamp")} for p in items],
                "cluster": cluster}

    def recover(self, targets: list[str], expected_pvcs: dict[str, str], removal_clock: float, name: str) -> dict:
        """After ballast removal: wait for a self restart, else delete the Pod (UID-checked); then wait <= 900 s."""
        started_restarts = {p["metadata"]["name"]: main_container_restarts(p) for p in self.pods()}
        observations: list[dict] = []
        result = {"selfRecoveryWindowSeconds": self.plan["selfRecoverySeconds"], "path": None, "deletes": []}
        deadline = removal_clock + self.plan["selfRecoverySeconds"]
        while self.clock() < deadline and result["path"] is None:
            observation = self.observe_recovery(targets, expected_pvcs)
            observations.append(observation)
            if not observation["problems"]:
                result["path"] = "broker_recovered_without_pod_delete"
            else:
                self.sleep(RECOVERY_POLL_SECONDS)
        if result["path"] is None:
            current = {p["metadata"]["name"]: p for p in self.pods()}
            for pod in targets:
                live = current.get(pod)
                if live and live["metadata"]["uid"] == self.uids[pod] and not logic.pod_ready(live):
                    self.delete_pod(pod, self.uids[pod])
                    result["deletes"].append({"pod": pod, "uid": self.uids[pod], "at": timestamp(),
                                              "gracePeriod": "pod default", "precondition": "uid"})
            result["path"] = "pod_deleted_after_self_recovery_window" if result["deletes"] else "no_delete_needed"
        recovered = False
        while self.clock() - removal_clock <= logic.RECOVERY_LIMIT_SECONDS:
            observation = self.observe_recovery(targets, expected_pvcs)
            observations.append(observation)
            replaced = {p["name"]: p["uid"] for p in observation["pods"]}
            deleted_ready = all(replaced.get(d["pod"]) not in (None, d["uid"]) for d in result["deletes"])
            if not observation["problems"] and deleted_ready:
                recovered = True
                break
            self.sleep(RECOVERY_POLL_SECONDS)
        result.update(recovered=recovered, secondsAfterBallastRemoval=self.clock() - removal_clock,
                      limitSeconds=logic.RECOVERY_LIMIT_SECONDS, finalProblems=observations[-1]["problems"],
                      restartCountsBefore={t: started_restarts.get(t) for t in targets},
                      restartCountsAfter={p["name"]: p["restarts"] for p in observations[-1]["pods"]
                                          if p["name"] in targets})
        if recovered:
            current = {p["metadata"]["name"]: p for p in self.pods()}
            for pod in targets:
                self.uids[pod] = current[pod]["metadata"]["uid"]
        self.save(f"{name}-recovery-observations.json", observations)
        return result

    def verify_durable(self, pod: str, scenario: str) -> dict:
        with self.target_tunnel(pod):
            return self.runtime.verify_subscriber(self.private, self.fixture.run_id, scenario, TARGET_PORTS[1],
                                                  self.output, "after-recovery")

    def start_publisher(self, scenario: str):
        """Start the durable publisher; callers start it right before the write load so it covers the failure."""
        publisher = self.runtime.DurablePublisher(self.private, self.fixture.run_id, scenario, OBSERVER_PORTS[1],
                                                  self.output, interval=self.plan["durableIntervalSeconds"],
                                                  max_messages=self.plan["durableMaxMessages"])
        publisher.start()
        self.track(f"publisher-{scenario}", lambda: self.stop_publisher(publisher))
        return publisher

    def stop_publisher(self, publisher) -> dict:
        """Stop once; later calls (the `finally` path) return the first result."""
        key = id(publisher)
        if key not in self.publisher_results:
            self.publisher_results[key] = publisher.stop()
        return self.publisher_results[key]

    # -- D1 --

    def scenario_d1(self) -> dict:
        plan, target = self.plan, self.plan["targetPod"]
        topic = f"acceptance/{self.fixture.run_id}/storage-pressure/d1"
        report = self.partial = {}
        self.step("d1-roles")
        report["roles"] = self.check_cluster(["D1"])
        self.step("d1-ballast")
        report["ballast"] = self.ballast.ensure(target, self.uids[target])
        report["baselineDf"] = self.ballast.release_to(target, logic.RECOVERY_FREE_BYTES)
        self.step("d1-probe")
        tunnel, evidence = self._open_tunnel(target)
        report["targetEndpoint"] = evidence
        target_probe = self.runtime.QoS1Probe(self.private, self.fixture.run_id, "storage-pressure-probe",
                                              TARGET_PORTS[1], topic, self.epoch)
        observer_probe = self.runtime.QoS1Probe(self.private, self.fixture.run_id, "ebs-worker-publisher",
                                                OBSERVER_PORTS[1], topic, self.epoch)
        self.track("d1-target-probe", target_probe.close)
        self.track("d1-observer-probe", observer_probe.close)
        report["pressure"] = self.ballast.adjust_free(target, logic.D1_TARGET_FREE_BYTES, logic.D1_FREE_TOLERANCE_BYTES)
        available = report["pressure"]["df"]["availableBytes"]
        target_loop, observer_loop = self.runtime.ProbeLoop(target_probe), self.runtime.ProbeLoop(observer_probe)
        self.track("d1-target-loop", target_loop.stop)
        self.track("d1-observer-loop", observer_loop.stop)
        target_loop.start()
        observer_loop.start()
        end = self.clock() + D1_PRESSURE_WINDOW_SECONDS
        while self.clock() < end:
            tunnel.check()
            self.sleep(POLL_SECONDS)
        release_started = self.elapsed()
        held = self.ballast.df(target)
        report["pressureHeldDf"] = held
        released = self.ballast.release_to(target, logic.RECOVERY_FREE_BYTES)
        zero = self.elapsed()
        report["release"] = released
        end = self.clock() + D1_RECOVERY_WINDOW_SECONDS
        while self.clock() < end:
            tunnel.check()
            if any(s.get("puback") is True and s["finishedSeconds"] >= zero for s in list(target_loop.samples)):
                break
            self.sleep(POLL_SECONDS)
        target_samples = target_loop.stop()
        observer_samples = observer_loop.stop()
        pressure_samples = [s for s in target_samples if s["finishedSeconds"] <= release_started]
        self.save("d1-target-probe-samples.json", target_samples)
        self.save("d1-observer-probe-samples.json", observer_samples)
        pressure = logic.judge_d1_pressure(pressure_samples, observer_samples)
        recovery = logic.judge_d1_recovery(target_samples, zero)
        reasons = []
        if not 0 < held["availableBytes"] < logic.MIN_FREE_BYTES or not 0 < available < logic.MIN_FREE_BYTES:
            reasons.append("free_space_not_in_pressure_range")
        if released["df"]["availableBytes"] < logic.CLEAR_FREE_BYTES:
            reasons.append("release_below_clear_threshold")
        reasons += [f"pressure:{r}" for r in pressure["reasons"]] + [f"recovery:{r}" for r in recovery["reasons"]]
        report.update(pressureJudgement=pressure, recoveryJudgement=recovery, reasons=reasons,
                      result="PASS" if not reasons else "FAIL",
                      note="ballast left in place with free space above the clear threshold; removed in finally")
        return report

    def _open_tunnel(self, pod: str):
        tunnel = self.runtime.PodTunnel(self.fixture, pod, self.uids[pod], *TARGET_PORTS)
        evidence = tunnel.start()
        self.track(f"target-tunnel-{pod}", tunnel.close)
        return tunnel, evidence

    # -- write load after the fill --

    def start_load(self, scenario: str):
        """Retained QoS 1 publishes of a large payload through the observer; each one is appended to every WAL.

        A full filesystem alone fails nothing: the WAL is one append-only file without preallocation, so a broker
        only hits ENOSPC when new bytes arrive. Plain QoS 1 publishes without a subscriber write nothing to Raft
        (ADR-0019), so the load must be retained. The load has its own port-forward to the observer Pod, so heavy
        traffic or an abrupt connection close can never take down the observer tunnel that carries the monitor and
        the durable publisher.
        """
        plan = self.plan
        topic = f"acceptance/{self.fixture.run_id}/storage-load/{scenario}"
        tunnel = ManagedTunnel(self, f"load-{scenario}", plan["observerPod"], LOAD_PORTS)
        self.track(f"load-tunnel-{scenario}", tunnel.close)
        tunnel.start()
        inner = self.runtime.QoS1Probe(self.private, self.fixture.run_id, "storage-pressure-probe", LOAD_PORTS[1],
                                       topic, self.epoch, payload_bytes=plan["loadPayloadBytes"],
                                       topic_count=plan["loadTopics"])
        probe = TunneledProbe(tunnel, inner, self.epoch)
        loop = self.runtime.ProbeLoop(probe, plan["loadIntervalSeconds"])
        self.track(f"load-probe-{scenario}", probe.close)
        self.track(f"load-loop-{scenario}", loop.stop)
        self.loads[scenario] = {"topic": topic, "probe": probe, "tunnel": tunnel}
        loop.start()
        return probe, loop, {"topic": topic, "payloadBytes": plan["loadPayloadBytes"], "topics": plan["loadTopics"],
                             "intervalSeconds": plan["loadIntervalSeconds"],
                             "deadlineSeconds": plan["loadDeadlineSeconds"], "startedAt": timestamp(),
                             "_tunnel": tunnel}

    def stop_load(self, scenario: str, loop, info: dict) -> dict:
        samples = loop.stop()
        self.save(f"{scenario}-load-samples.json", samples)
        info = dict(info)
        tunnel = info.pop("_tunnel")
        return {**info, "stoppedAt": timestamp(), "tunnel": tunnel.summary(), **logic.load_summary(samples)}

    def cleanup_retained(self, scenario: str) -> dict | None:
        """Delete the retained load messages through a fresh load tunnel and probe; None when no load started.

        Runs after recovery on the normal path and in the failure paths too, so a FAIL or inconclusive scenario never
        leaves retained state behind (it enlarges the Raft snapshot). The old probe is closed first because one
        client id may hold one connection. Attempts repeat until every topic is acknowledged or the window ends;
        each topic's last outcome is recorded, never judged.
        """
        entry = self.loads.pop(scenario, None)
        if entry is None:
            return None
        plan = self.plan
        for closer in (entry["probe"].close, entry["tunnel"].close):
            try:
                closer()
            except Exception as exc:
                self.cleanup_errors.append(f"retained-cleanup-old:{type(exc).__name__}")
        tunnel = ManagedTunnel(self, f"cleanup-{scenario}", plan["observerPod"], LOAD_PORTS)
        probe = self.runtime.QoS1Probe(self.private, self.fixture.run_id, "storage-pressure-probe", LOAD_PORTS[1],
                                       entry["topic"], self.epoch, payload_bytes=plan["loadPayloadBytes"],
                                       topic_count=plan["loadTopics"])
        deadline = self.clock() + RETAINED_CLEANUP_SECONDS
        result = {"topic": entry["topic"], "topics": None, "attempts": 0, "complete": False,
                  "tunnelOpened": False}
        try:
            while True:
                result["attempts"] += 1
                if tunnel.ensure():
                    result["tunnelOpened"] = True
                    try:
                        result["topics"] = probe.clear_retained()
                    except Exception as exc:
                        result["topics"] = [{"outcome": "error:" + type(exc).__name__}]
                    if all(t.get("outcome") == "puback" and t.get("reasonCode") == 0 for t in result["topics"]):
                        result["complete"] = True
                        break
                if self.clock() >= deadline:
                    break
                self.sleep(RETAINED_CLEANUP_RETRY_SECONDS)
        finally:
            for closer in (probe.close, tunnel.close):
                try:
                    closer()
                except Exception as exc:
                    self.cleanup_errors.append(f"retained-cleanup:{type(exc).__name__}")
        result["tunnel"] = tunnel.summary()
        return result

    # -- target observation through each target's own management API --

    def open_target_tunnel(self, pod: str, ports: tuple[int, int]) -> ManagedTunnel:
        tunnel = ManagedTunnel(self, f"target-{pod}", pod, ports)
        self.track(f"target-tunnel-{pod}", tunnel.close)
        tunnel.start()
        return tunnel

    def restart_baseline(self, pods: list[str]) -> dict[str, int | None]:
        current = {p["metadata"]["name"]: p for p in self.pods()}
        return {pod: main_container_restarts(current.get(pod)) for pod in pods}

    def sample_target(self, tunnel: ManagedTunnel, pod: str, node_id: str) -> dict:
        """One reading of a target: Kubernetes container status plus the node's own management API."""
        sample = {"at": timestamp(), "pod": pod}
        live = None
        ready = None
        try:
            live = self.fixture.get_pod(pod)
            ready = bool(live) and logic.pod_ready(live)
        except Exception as exc:
            sample["podReadErrorCode"] = type(exc).__name__
        sample.update(podReady=ready, container=container_view(live))
        node, api = None, {"source": "target_api"}
        if tunnel.ensure():
            try:
                node = self.fixture.api(f"/api/v1/nodes/{node_id}", tunnel.ports[0])
            except Exception as exc:
                api["errorCode"] = type(exc).__name__
        else:
            api["errorCode"] = "tunnel_down"
        view = logic.storage_failure_view(node)
        if node is None:
            # The broker container is still running while its API port-forward is gone, so read from inside it.
            local = self.local_node_record(pod, node_id)
            if local["ok"]:
                local_view = logic.storage_failure_view(local["node"])
                api["source"] = "local_exec"
                if local_view["visible"] or view["fields"] is None:
                    view = local_view
        sample.update(api=api, storageFailure=view)
        return sample

    def read_log_evidence(self, pod: str, previous: bool) -> dict:
        """Only a boolean and the matching `msg` field are kept, never the log line."""
        args = ["-n", "narumq", "logs", pod, "-c", "narumq", "--tail=500"] + (["--previous"] if previous else [])
        result = {"previous": previous, "at": timestamp()}
        try:
            text = self.fixture.command(self.fixture.kubectl + args, timeout=60)
        except Exception as exc:
            result["errorCode"] = type(exc).__name__
            return result
        found, msg = logic.scan_log(text)
        result.update(containsNoSpace=found, msg=msg)
        return result

    def hold_for_log_evidence(self, pods: list[str], baselines: dict, min_hold: float, name: str) -> dict:
        """Keep the failure in place until each target's previous container log was read, or the wait ends.

        The previous log exists only after the kubelet restarted the container, so the wait is bounded and the
        outcome (restart seen or not) is recorded either way. Cluster state is sampled through the observer.
        """
        started = self.clock()
        evidence: dict[str, dict] = {}
        samples: list[dict] = []
        while True:
            self.require_observer()
            current = {}
            try:
                current = {p["metadata"]["name"]: p for p in self.pods()}
            except Exception as exc:
                samples.append({"at": timestamp(), "podListErrorCode": type(exc).__name__})
            for pod in pods:
                restarts = main_container_restarts(current.get(pod))
                base = baselines.get(pod)
                if pod not in evidence and restarts is not None and base is not None and restarts > base:
                    evidence[pod] = self.read_log_evidence(pod, True)
            sample = {"at": timestamp()}
            try:
                sample["cluster"] = self.fixture.management_probe()
            except Exception as exc:
                sample["clusterErrorCode"] = type(exc).__name__
            samples.append(sample)
            now = self.clock()
            held = now - started >= min_hold
            if held and len(evidence) == len(pods):
                break
            if held and now - started >= PREVIOUS_LOG_WAIT_SECONDS:
                for pod in pods:
                    if pod not in evidence:
                        evidence[pod] = {"previous": None, "outcome": "no_container_restart_observed",
                                         "current": self.read_log_evidence(pod, False)}
                break
            self.sleep(POLL_SECONDS)
        self.save(f"{name}-hold-samples.json", samples)
        return evidence

    # -- D2 --

    def watch_d2_detection(self, pod: str, node_id: str, tunnel: ManagedTunnel, anchor: float,
                           deadline_seconds: float) -> dict:
        samples, not_ready, visible = [], None, None
        deadline = anchor + deadline_seconds
        while True:
            self.require_observer()
            now = self.clock()
            sample = self.sample_target(tunnel, pod, node_id)
            sample["secondsAfterLoadStart"] = round(now - anchor, 3)
            samples.append(sample)
            seconds = max(0.0, now - anchor)
            if not_ready is None and sample["podReady"] is False:
                not_ready = seconds
            if visible is None and sample["storageFailure"]["visible"]:
                visible = seconds
            if (not_ready is not None and visible is not None) or now >= deadline:
                break
            self.sleep(POLL_SECONDS)
        self.save("d2-detection-samples.json", samples)
        verdict = logic.judge_d2_detection(not_ready, visible)
        verdict["watchDeadlineSeconds"] = deadline_seconds
        verdict["source"] = "target's own management API (per-target tunnel)"
        verdict["firstStorageFailureFields"] = next((s["storageFailure"]["fields"] for s in samples
                                                     if s["storageFailure"]["visible"]), None)
        return verdict

    def scenario_d2(self) -> dict:
        plan, target = self.plan, self.plan["targetPod"]
        scenario = logic.durable_scenario(plan, "D2")
        report = self.partial = {"durableScenario": scenario}
        self.step("d2-roles")
        report["roles"] = self.check_cluster(["D2"])
        pvcs = self.pvc_uids([target])
        report["pvcUIDs"] = pvcs
        self.step("d2-ballast-create")
        report["ballast"] = self.ballast.ensure(target, self.uids[target])
        report["baselineDf"] = self.ballast.release_to(target, logic.RECOVERY_FREE_BYTES)
        self.step("d2-prepare-subscriber")
        with self.target_tunnel(target) as (_, endpoint):
            report["prepare"] = self.runtime.prepare_subscriber(self.private, self.fixture.run_id, scenario,
                                                                TARGET_PORTS[1], self.output)
        continuity = self.runtime.QoS1Probe(self.private, self.fixture.run_id, "ebs-worker-publisher",
                                            OBSERVER_PORTS[1], f"acceptance/{self.fixture.run_id}/continuity", self.epoch)
        self.track("d2-continuity-probe", continuity.close)

        def management():
            self.observer.check()
            return self.fixture.management_probe()

        def mqtt():
            self.observer.check()
            return continuity.attempt()

        self.step("d2-monitor-start")
        monitor = IndependentMonitor(management, mqtt, self.output / "d2-observer")
        self.track("d2-monitor", monitor.stop)
        monitor.start()
        if not monitor.wait_initial():
            raise ScenarioError("initial observer probes did not complete")
        initial = monitor.snapshot()
        report["initialProbes"] = {name: [{key: s.get(key) for key in ("ok", "errorCode")} | (
            {"outcome": s["data"].get("outcome"), "reasonCode": s["data"].get("reasonCode")}
            if s.get("ok") and isinstance(s.get("data"), dict) and "outcome" in s["data"] else {})
            for s in initial[name][:1]] for name in initial}
        if not (initial["management"][0]["ok"] and initial["mqtt"][0]["ok"] and initial["mqtt"][0]["data"]["puback"]):
            raise ScenarioError("initial observer probes were not successful; ballast not written")
        self.step("d2-target-tunnel")
        baselines = self.restart_baseline([target])
        tunnel = self.open_target_tunnel(target, TARGET_PORTS)
        self.sleep(BASELINE_SEND_SECONDS)
        self.step("d2-fill")
        report["fill"] = self.ballast.fill_to_enospc(target)
        report["fillCompletedAt"] = timestamp()
        self.require_observer()
        self.step("d2-load")
        publisher = self.start_publisher(scenario)
        load_probe, load_loop, load_info = self.start_load("d2")
        anchor = self.clock()
        self.step("d2-detection")
        report["detection"] = self.watch_d2_detection(target, plan["targetNodeId"], tunnel, anchor,
                                                      plan["loadDeadlineSeconds"])
        report["load"] = self.stop_load("d2", load_loop, load_info)
        self.step("d2-log-evidence")
        report["logEvidence"] = self.hold_for_log_evidence([target], baselines, 0, "d2")
        report["targetTunnel"] = tunnel.summary()
        self.step("d2-ballast-remove")
        report["ballastRemovalStartedAt"] = timestamp()
        removal_clock = self.clock()
        report["ballastRemoval"] = self.ballast.remove(target)
        self.step("d2-recovery")
        report["recovery"] = self.recover([target], pvcs, removal_clock, "d2")
        report["publisher"] = self.stop_publisher(publisher)
        monitor.stop()
        report["observerMonitor"] = monitor.summary()
        continuity.close()
        self.require_observer()
        tunnel.close()
        if report["recovery"]["recovered"]:
            self.step("d2-retained-cleanup")
            report["load"]["retainedCleanup"] = self.cleanup_retained("d2")
            self.step("d2-durable-verify")
            report["durable"] = self.verify_durable(target, scenario)
        else:
            report["durable"] = {"result": "SKIPPED", "reason": "cluster did not recover"}
        reasons = [f"detection:{r}" for r in report["detection"]["reasons"]]
        if report["observerMonitor"]["result"] != "PASS":
            reasons.append("observer_samples_failed")
        if not report["recovery"]["recovered"]:
            reasons.append("not_recovered_within_limit")
        if report["durable"].get("result") != "PASS":
            reasons.append("durable_stream_failed")
        report.update(reasons=reasons, result="PASS" if not reasons else "FAIL")
        return report

    # -- D3 --

    def watch_d3_detection(self, targets: list[tuple[str, str]], tunnels: dict[str, ManagedTunnel], anchor: float,
                           deadline_seconds: float) -> dict:
        """A follower counts as failed when its own API shows the storage failure or its Pod is NotReady.

        The liveness kill can take the API away before a sample reads it, so NotReady is accepted as the other
        evidence. Both followers must be failed in the same poll round for the window to start.
        """
        seen: dict[str, dict] = {}
        samples = []
        window_start = None
        deadline = anchor + deadline_seconds
        while True:
            self.require_observer()
            round_started = timestamp()
            now = self.clock()
            for pod, node_id in targets:
                sample = self.sample_target(tunnels[pod], pod, node_id)
                sample["secondsAfterLoadStart"] = round(now - anchor, 3)
                samples.append(sample)
                if pod in seen:
                    continue
                view = sample["storageFailure"]
                if view["visible"] or sample["podReady"] is False:
                    seen[pod] = {"nodeId": node_id, "seconds": max(0.0, now - anchor),
                                 "evidence": "storage_failure_visible" if view["visible"] else "pod_not_ready",
                                 "source": sample["api"]["source"], "fields": view["fields"]}
            if len(seen) == len(targets):
                window_start = round_started
                break
            if now >= deadline:
                break
            self.sleep(POLL_SECONDS)
        self.save("d3-detection-samples.json", samples)
        return {"windowStart": window_start, "seen": seen, "limitSeconds": logic.D3_DETECTION_LIMIT_SECONDS,
                "watchDeadlineSeconds": deadline_seconds}

    def scenario_d3(self) -> dict:
        plan = self.plan
        first, second = plan["targetPod"], plan["secondTargetPod"]
        pairs = [(first, plan["targetNodeId"]), (second, plan["secondTargetNodeId"])]
        scenario = logic.durable_scenario(plan, "D3")
        report = self.partial = {"durableScenario": scenario}
        self.step("d3-roles")
        report["roles"] = self.check_cluster(["D3"])
        pvcs = self.pvc_uids([first, second])
        report["pvcUIDs"] = pvcs
        self.step("d3-ballast-create")
        report["ballast"] = [self.ballast.ensure(pod, self.uids[pod]) for pod, _ in pairs]
        report["baselineDf"] = [self.ballast.release_to(pod, logic.RECOVERY_FREE_BYTES) for pod, _ in pairs]
        self.step("d3-prepare-subscriber")
        with self.target_tunnel(first):
            report["prepare"] = self.runtime.prepare_subscriber(self.private, self.fixture.run_id, scenario,
                                                                TARGET_PORTS[1], self.output)
        self.step("d3-target-tunnels")
        baselines = self.restart_baseline([first, second])
        tunnels = {first: self.open_target_tunnel(first, TARGET_PORTS),
                   second: self.open_target_tunnel(second, SECOND_TARGET_PORTS)}
        self.step("d3-fill")
        fills = {}
        failures = {}

        def fill(pod):
            try:
                fills[pod] = self.ballast.fill_to_enospc(pod)
            except Exception as exc:
                failures[pod] = type(exc).__name__
        threads = [threading.Thread(target=fill, args=(pod,)) for pod, _ in pairs]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        report["fill"] = {"results": fills, "failures": failures}
        if failures:
            raise ScenarioError("a follower did not reach ENOSPC")
        report["fillCompletedAt"] = timestamp()
        self.require_observer()
        self.step("d3-load")
        publisher = self.start_publisher(scenario)
        load_probe, load_loop, load_info = self.start_load("d3")
        anchor = self.clock()
        self.step("d3-detection")
        detection = self.watch_d3_detection(pairs, tunnels, anchor, plan["loadDeadlineSeconds"])
        report["detection"] = detection
        report["load"] = self.stop_load("d3", load_loop, load_info)
        self.step("d3-window")
        if detection["windowStart"]:
            report["logEvidence"] = self.hold_for_log_evidence([first, second], baselines, plan["d3HoldSeconds"], "d3")
        report["targetTunnels"] = {pod: tunnel.summary() for pod, tunnel in tunnels.items()}
        self.require_observer()
        window_end = timestamp() if detection["windowStart"] else None
        self.step("d3-ballast-remove")
        removal_clock = self.clock()
        removals = {}
        removal_errors = {}
        for pod, _ in pairs:
            try:
                removals[pod] = self.ballast.remove(pod)
            except Exception as exc:
                removal_errors[pod] = type(exc).__name__
        report["ballastRemoval"] = {"firstRemovalStartedAt": window_end, "results": removals, "errors": removal_errors}
        if removal_errors:
            raise ScenarioError("ballast removal failed on a follower")
        self.step("d3-recovery")
        report["recovery"] = self.recover([first, second], pvcs, removal_clock, "d3")
        report["publisher"] = self.stop_publisher(publisher)
        self.require_observer()
        for tunnel in tunnels.values():
            tunnel.close()
        rows = logic.read_publish_journal(self.output / f"durable-publish-{scenario}.jsonl")
        report["window"] = logic.judge_d3_window(rows, detection["windowStart"], window_end)
        if report["recovery"]["recovered"]:
            self.step("d3-retained-cleanup")
            report["load"]["retainedCleanup"] = self.cleanup_retained("d3")
            self.step("d3-durable-verify")
            report["durable"] = self.verify_durable(first, scenario)
        else:
            report["durable"] = {"result": "SKIPPED", "reason": "cluster did not recover"}
        reasons = []
        if detection["windowStart"] is None:
            reasons.append("both_storage_failures_not_visible")
        reasons += [f"window:{r}" for r in report["window"]["reasons"]]
        if not report["recovery"]["recovered"]:
            reasons.append("not_recovered_within_limit")
        if report["durable"].get("result") != "PASS":
            reasons.append("durable_stream_failed")
        report.update(reasons=reasons, result="PASS" if not reasons else "FAIL")
        return report


def load_plan(path: Path, private: Path, scenarios: list[str], run_id: str) -> dict:
    plan = logic.validate_plan(json.loads(path.read_text(encoding="utf-8")), scenarios, run_id)
    if "brokerImage" not in plan:
        image = (private / "broker-image.txt").read_text(encoding="utf-8").strip()
        plan = logic.validate_plan({**plan, "brokerImage": image}, scenarios, run_id)
    return plan


def main(argv=None, runtime=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="mode", required=True)
    run = sub.add_parser("run", help="run D1, D2 and D3 against the synthetic fixture")
    run.add_argument("--private", type=Path, required=True)
    run.add_argument("--output", type=Path, required=True)
    run.add_argument("--plan", type=Path, required=True)
    run.add_argument("--execute", action="store_true", help="allow writing ballast into the checked follower PVCs")
    run.add_argument("--only", help="comma-separated subset of D1,D2,D3")
    args = parser.parse_args(argv)
    if not args.execute:
        parser.error("run requires --execute")
    try:
        scenarios = logic.parse_scenarios(args.only)
    except ValueError as exc:
        parser.error(str(exc))
    args.output.mkdir(parents=True, exist_ok=False)
    report = {"result": "FAIL", "mode": "run", "scenarios": {}, "startedAt": timestamp(), "selected": scenarios}
    run_state = None
    try:
        runtime = runtime or load_runtime()
        fixture = runtime.Fixture(args.private, args.output.name)
        plan = load_plan(args.plan, args.private, scenarios, args.output.name)
        run_state = StoragePressureRun(fixture, plan, args.output, args.private, runtime, scenarios)
        report["preflight"] = run_state.start()
        report["scenarios"] = run_state.run_scenarios()
        report["result"] = "PASS" if all(r["result"] == "PASS" for r in report["scenarios"].values()) else "FAIL"
    except Exception as exc:
        report["errorCode"] = type(exc).__name__
        report["errorLocation"] = logic.error_location(exc)
        report["errorStep"] = run_state.current_step if run_state is not None else "setup"
    finally:
        if run_state is not None:
            cleanup = run_state.finish()
            report["cleanup"] = cleanup
            leftover = [pod for pod, r in cleanup["ballastRemoval"].items()
                        if r.get("removed") is False and r.get("outcome") == "failed"]
            if leftover or cleanup["cleanupErrors"]:
                report["result"] = "FAIL"
                report["ballastLeftOn"] = leftover
        report["finishedAt"] = timestamp()
        logic.write_json(args.output, "summary.json", report)
    print(json.dumps(report))
    return 0 if report["result"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
