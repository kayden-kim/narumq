"""Run the EKS disposable-emptyDir bootstrap guard acceptance (CLU-SCN-136, ADR-0022) on one disposable fixture.

A throw-away Pod runs the broker image with member 3's real config, certificates and peer addresses on an `emptyDir`
data directory. The running members answer its bootstrap probe with PROGRESSED, so the broker must refuse with
`cluster bootstrap refused`, exit 1 and leave the emptyDir empty, while the three real brokers stay Ready and keep
quorum. The management API shows no commit index or term, so the harness reads member 1's own `lastAppliedIndex`
through its management API and its own last `raft node status changed` log term before and after the run and requires
the guard's report to agree with those live values (see emptydir_guard.judge_guard).

The run directory name is the kubectl context and the plan's runId. `--private` holds `kubectl.exe`, `kubeconfig` and
`admin.token`; AWS credentials come from the environment (kubeconfig exec plugin, ownership checks). Run it only
against a disposable fixture. Evidence is written under --output; credentials are never written.

Plan (JSON): {"runId", "clusterName", "vpcId", "targetMemberId": 3, "peerMemberId": 1,
              "expectedTerm": 6, "expectedCommitIndex": 100}
The scenario requires exactly these member IDs and progress values. Omitted expected values default to 6 and 100.

Error codes (summary.json `errorCode`): plan_invalid:*, plan_missing:*, private_file_missing:*, dependency_missing:aws,
statefulset_*, configmap_missing, secret_missing, secret_key_missing:*, target_member_*, peer_member_*,
brokers_not_three_ready, guard_pod_exists, peer_pod_missing, peer_term_unobservable, guard_pod_create_failed,
guard_pod_not_owned, guard_timeout, guard_pod_vanished, guard_pod_replaced, or the exception class name.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import shutil
import sys
import time

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
sys.path.append(str(HERE))
sys.path.append(str(HERE.parents[1]))  # tests/ holds scenario_results.py

import emptydir_guard as guard  # noqa: E402  (also puts tests/chaos/eks-worker-recovery on sys.path)
from fixture import Fixture, require_owned_fixture, save  # noqa: E402
from observer import timestamp  # noqa: E402
from pod_tunnel import PodTunnel  # noqa: E402
from scenario_results import scenario_result  # noqa: E402

MANAGEMENT_PORT = 21880
MQTT_PORT = 21883
GUARD_SECONDS = 300
SETTLE_SECONDS = 30
CLEANUP_SECONDS = 120
PRIVATE_FILES = ("kubectl.exe", "kubeconfig", "admin.token")
SECRET_KEYS = ("admin.token", "dashboard.key", "security.json", "ca.crt")


def check_private(private: Path) -> None:
    for name in PRIVATE_FILES:
        if not (private / name).is_file():
            guard.reject(f"private_file_missing:{name}")


def check_dependencies(which=shutil.which) -> None:
    if which("aws") is None:
        guard.reject("dependency_missing:aws")


class GuardRun:
    def __init__(self, fixture, plan: dict, output: Path, tunnel_factory=PodTunnel, ownership=require_owned_fixture,
                 ports: tuple[int, int] = (MANAGEMENT_PORT, MQTT_PORT), guard_seconds: float = GUARD_SECONDS,
                 settle_seconds: float = SETTLE_SECONDS, cleanup_seconds: float = CLEANUP_SECONDS,
                 sleep=time.sleep, monotonic=time.monotonic):
        self.fixture = fixture
        self.plan = plan
        self.output = output
        self.tunnel_factory = tunnel_factory
        self.ownership = ownership
        self.ports = ports
        self.guard_seconds = guard_seconds
        self.settle_seconds = settle_seconds
        self.cleanup_seconds = cleanup_seconds
        self.sleep = sleep
        self.monotonic = monotonic
        self.step = "plan"
        self.criteria: dict[str, bool] = {}
        self.evidence: dict = {}
        self.name = guard.guard_pod_name(plan["runId"])
        self.pod_uid: str | None = None

    # -- steps ---------------------------------------------------------------------------------------------------

    def preflight(self) -> dict:
        self.step = "preflight"
        fixture, plan = self.fixture, self.plan
        image = guard.broker_image(fixture.get("statefulset", guard.STATEFULSET, "-n", guard.NAMESPACE))
        configmap = fixture.get("configmap", guard.CONFIGMAP, "-n", guard.NAMESPACE)
        if not configmap:
            guard.reject("configmap_missing")
        config = guard.member_config(configmap, plan)
        secret = fixture.get("secret", guard.SECRET, "-n", guard.NAMESPACE)
        if not secret:
            guard.reject("secret_missing")
        keys = set(secret.get("data") or {})
        for key in SECRET_KEYS + (f"node-{config['targetOrdinal']}.crt", f"node-{config['targetOrdinal']}.key"):
            if key not in keys:
                guard.reject(f"secret_key_missing:{key}")
        brokers = guard.broker_pods(fixture)
        if not guard.three_ready(brokers):
            guard.reject("brokers_not_three_ready", json.dumps([(b["name"], b["ready"]) for b in brokers]))
        instance_ids = []
        for broker in brokers:
            node = fixture.get_node(broker["nodeName"])
            if not node or not guard.instance_of(node):
                guard.reject("broker_node_missing", broker["name"])
            instance_ids.append(guard.instance_of(node))
        self.evidence["ownership"] = self.ownership(fixture, plan, sorted(set(instance_ids)))
        if fixture.get_pod(self.name) is not None:
            guard.reject("guard_pod_exists", self.name)
        peer_name = f"narumq-{config['peerOrdinal']}"
        peer_pod = next((b for b in brokers if b["name"] == peer_name), None)
        if peer_pod is None:
            guard.reject("peer_pod_missing", peer_name)
        self.criteria["plan.fixtureOwnedAndThreeReadyBefore"] = True
        self.evidence["preflight"] = {"image": image, "configuration": config, "brokersBefore": brokers,
                                      "guardPod": self.name, "peerPod": peer_pod}
        return {"image": image, "config": config, "brokers": brokers, "peerPod": peer_pod}

    def read_peer(self, port: int, peer_pod: dict, applied: bool = True) -> dict:
        """Member `peerMemberId`'s live term (own log) and applied index (own management API)."""
        member = self.plan["peerMemberId"]
        term = guard.latest_raft_term(guard.fetch_logs(self.fixture, peer_pod["name"]), member)
        if term is None:
            guard.reject("peer_term_unobservable", str(member))
        return {"term": term, "applied": guard.peer_applied_index(self.fixture, port, member) if applied else None}

    def create_pod(self, context: dict) -> str:
        self.step = "create"
        pod = guard.build_pod(self.plan, context["image"], context["config"]["targetOrdinal"])
        try:
            self.fixture.command(self.fixture.kubectl + ["create", "-f", "-"], timeout=60, stdin=json.dumps(pod))
        except Exception as exc:
            guard.reject("guard_pod_create_failed", type(exc).__name__)
        created = self.fixture.get_pod(self.name)
        if created is None or not guard.owned_guard_pod(created, self.plan["runId"]):
            guard.reject("guard_pod_not_owned", self.name)
        self.pod_uid = created["metadata"]["uid"]
        self.evidence["guardPodCreated"] = {"at": timestamp(), "uid": self.pod_uid}
        return self.pod_uid

    def cleanup(self) -> bool:
        """Delete only this run's guard Pod (ownership labels and UID) and confirm it is gone."""
        fixture = self.fixture
        pod = fixture.get_pod(self.name)
        if pod is None:
            self.evidence["cleanup"] = {"guardPodDeleted": True, "alreadyAbsent": True}
            return True
        if not guard.owned_guard_pod(pod, self.plan["runId"]) or (
                self.pod_uid is not None and pod["metadata"]["uid"] != self.pod_uid):
            self.evidence["cleanup"] = {"guardPodDeleted": False, "reason": "not_owned_by_run"}
            return False
        fixture.command(fixture.kubectl + ["-n", guard.NAMESPACE, "delete", "pod", self.name, "--ignore-not-found",
                                           "--wait=true", f"--timeout={int(self.cleanup_seconds)}s"],
                        timeout=self.cleanup_seconds + 30)
        deadline = self.monotonic() + self.cleanup_seconds
        while fixture.get_pod(self.name) is not None:
            if self.monotonic() >= deadline:
                self.evidence["cleanup"] = {"guardPodDeleted": False, "reason": "still_present"}
                return False
            self.sleep(2.0)
        self.evidence["cleanup"] = {"guardPodDeleted": True, "alreadyAbsent": False, "at": timestamp()}
        return True

    # -- run -----------------------------------------------------------------------------------------------------

    def execute(self) -> dict:
        started = timestamp()
        error_code = None
        failed_step = None
        observation: dict = {"peer": {}}
        tunnel = None
        context = None
        try:
            context = self.preflight()
            self.step = "peer-before"
            tunnel = self.tunnel_factory(self.fixture, context["peerPod"]["name"], context["peerPod"]["uid"], *self.ports)
            self.evidence["peerTunnel"] = tunnel.start()
            self.criteria["quorum.availableBefore"] = guard.quorum_available(self.fixture, self.ports[0])
            before = self.read_peer(self.ports[0], context["peerPod"])
            observation["peer"].update(termBefore=before["term"], appliedBefore=before["applied"])
            uid = self.create_pod(context)
            self.step = "guard"
            pod = guard.wait_for_termination(self.fixture, self.name, uid, self.guard_seconds,
                                             sleep=self.sleep, monotonic=self.monotonic)
            terminated = guard.terminated_state(pod)
            logs = guard.fetch_logs(self.fixture, self.name)
            (self.output / "guard-pod.log").write_text(logs, encoding="utf-8")
            observation.update(exitCode=terminated.get("exitCode"), logs=logs)
            self.evidence["guardPod"] = {"phase": pod.get("status", {}).get("phase"), "nodeName": pod["spec"].get("nodeName"),
                                         "exitCode": terminated.get("exitCode"), "reason": terminated.get("reason"),
                                         "startedAt": terminated.get("startedAt"), "finishedAt": terminated.get("finishedAt"),
                                         "restartCount": (guard.container_status(pod) or {}).get("restartCount"),
                                         "logSha256": guard.digest(logs), "volumes": sorted(
                                             (v["name"], next(kind for kind in v if kind != "name"))
                                             for v in pod["spec"]["volumes"])}
            self.step = "peer-after"
            refusals = guard.parse_refusals(logs)
            commit = refusals[0]["commitIndex"] if refusals and refusals[0]["commitIndex"] is not None else 0
            applied_after = guard.wait_for_applied(self.fixture, self.ports[0], self.plan["peerMemberId"], commit,
                                                   self.settle_seconds, sleep=self.sleep, monotonic=self.monotonic)
            after = self.read_peer(self.ports[0], context["peerPod"], applied=False)
            observation["peer"].update(termAfter=after["term"], appliedAfter=applied_after)
            brokers_after = guard.broker_pods(self.fixture)
            self.criteria["brokers.threeReadyUnchangedAfter"] = (
                guard.three_ready(brokers_after) and guard.unchanged(context["brokers"], brokers_after))
            self.evidence["brokersAfter"] = brokers_after
            self.criteria["quorum.availableAfter"] = guard.quorum_available(self.fixture, self.ports[0])
        except guard.GuardRejected as exc:
            error_code, failed_step = exc.code, self.step
        except Exception as exc:
            error_code, failed_step = type(exc).__name__, self.step
        finally:
            try:
                self.criteria["cleanup.guardPodDeleted"] = self.cleanup()
            except Exception as exc:
                self.criteria["cleanup.guardPodDeleted"] = False
                self.evidence["cleanup"] = {"guardPodDeleted": False, "error": type(exc).__name__}
                error_code = error_code or type(exc).__name__
                failed_step = failed_step or "cleanup"
            if tunnel is not None:
                try:
                    tunnel.close()
                except Exception as exc:
                    self.evidence["peerTunnelCloseError"] = type(exc).__name__
        judged = guard.judge_guard(self.plan, observation)
        criteria = {**{name: False for name in guard.CRITERIA}, **judged["criteria"], **self.criteria}
        if "logs" not in observation:  # the guard Pod never ended: the judged values are not evidence
            criteria.update({name: False for name in judged["criteria"]})
        verdict = guard.aggregate(criteria, failed_step, error_code)
        report = {"scenario": guard.SCENARIO, "runId": self.plan["runId"], "startedAt": started, "finishedAt": timestamp(),
                  **verdict, "criteria": criteria, "facts": judged["facts"],
                  "planExpectation": {key: self.plan[key] for key in ("expectedTerm", "expectedCommitIndex")
                                      if key in self.plan},
                  "evidence": self.evidence}
        save(self.output, "guard-evidence.json", report)
        return report


# Scenarios: cluster-session-and-delivery.md:CLU-SCN-136
@scenario_result
def scenario_emptydir_bootstrap_guard(private: Path, out: Path, plan_path: Path,
                                      ports: tuple[int, int] = (MANAGEMENT_PORT, MQTT_PORT),
                                      guard_seconds: float = GUARD_SECONDS) -> dict:
    run_id = out.name
    report = {"result": "FAIL", "scenario": guard.SCENARIO, "runId": run_id, "startedAt": timestamp()}
    try:
        plan = guard.load_plan(json.loads(plan_path.read_text(encoding="utf-8")), run_id)
        for key, expected in (("targetMemberId", 3), ("peerMemberId", 1),
                              ("expectedTerm", 6), ("expectedCommitIndex", 100)):
            if plan.setdefault(key, expected) != expected:
                guard.reject(f"scenario_input_mismatch:{key}")
        check_dependencies()
        check_private(private)
        report = GuardRun(Fixture(private, run_id), plan, out, ports=ports, guard_seconds=guard_seconds).execute()
    except guard.GuardRejected as exc:
        report.update(errorCode=exc.code, failedStep="plan")
    except Exception as exc:
        report.update(errorCode=type(exc).__name__, failedStep="plan")
    finally:
        report["finishedAt"] = timestamp()
        save(out, "summary.json", report)
    return report


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--private", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True, help="new run directory; its name is the run id and kubectl context")
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--execute", action="store_true", help="create and delete the throw-away guard Pod")
    parser.add_argument("--management-port", type=int, default=MANAGEMENT_PORT)
    parser.add_argument("--mqtt-port", type=int, default=MQTT_PORT)
    parser.add_argument("--guard-seconds", type=float, default=GUARD_SECONDS)
    args = parser.parse_args(argv)
    if not args.execute:
        parser.error("this acceptance requires --execute")
    args.output.mkdir(parents=True, exist_ok=False)
    report = scenario_emptydir_bootstrap_guard(args.private, args.output, args.plan,
                                               (args.management_port, args.mqtt_port), args.guard_seconds)
    print(json.dumps({key: report.get(key) for key in ("result", "scenario", "runId", "errorCode", "failedStep",
                                                         "failedCriteria")}))
    return 0 if report["result"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
