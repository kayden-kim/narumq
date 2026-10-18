"""EKS acceptance harness for DRN-SCN-030 (DEP-13, DEP-14): preStop failure, duplicate calls and too-short grace period.

Status: prepared, not yet executed live. It needs a disposable EKS fixture that does not exist at the time of writing.

The scenario row (docs/specs/drain-and-rolling-update.md section 10) asks for four real observations:

1. preStop hook failure. The harness copies the real ``preStop`` script and the broker image from the live
   ``narumq`` StatefulSet into a throwaway probe Pod (label ``narumq.io/acceptance-run``, no ``app=narumq`` label so
   no Service, PDB or StatefulSet selects it). A stub container answers HTTP 401 on 127.0.0.1:8080, so the hook fails
   with ``Drain query failed: HTTP 401``. The harness runs the identical command with ``kubectl exec`` to record the
   stderr failure reason, deletes the Pod, and requires that the Pod disappears inside its grace period and that the
   kubelet recorded a ``FailedPreStopHook`` Event. (Current kubelets write only a generic Event message so that no hook
   output leaks; the harness records which form it saw.)
2. Grace expiry. A second probe Pod (grace 20 s) whose stub always answers DRAINING keeps the real hook in its wait
   loop. The Pod must be force-terminated at about the grace period, not earlier and not much later.
3. Duplicate invocation. The real hook runs twice concurrently with ``kubectl exec`` against a live follower broker
   (a real drain, no stub). The Management API must show exactly one drain operation id; each exit code and stderr is
   recorded. The follower is then restored in ``finally``: Pod deleted with a UID precondition after COMPLETED, three
   Ready brokers, drain record CLEAR.
4. Admission. The harness applies ``deploy/kubernetes/eks-ec2-ha-admission`` (a ValidatingAdmissionPolicy with the
   section 7 formula, ``Deny``), then submits zero-replica broker-shaped StatefulSets: grace 30, 659 and (with a
   1 800 000 ms drain-timeout annotation) 660 must be rejected by the API server naming the policy; 660 and 1860 must
   be accepted. Policy objects are removed and verified gone afterwards.

Prerequisites (a missing one raises ``PrerequisiteMissing`` and the scenario is recorded as FAIL, never skipped)
  * Disposable EKS fixture that passes ``eks_fixture.build_fixture`` (context == run id, EKS tags, namespace label).
  * Kubernetes with the ValidatingAdmissionPolicy API (admissionregistration.k8s.io/v1, 1.30+).
  * The three-broker ``narumq`` StatefulSet Ready, quorum available, no drain record on the chosen follower.
  * ``--stub-image``: an image with ``python3`` that the nodes can pull (no default; for example a mirrored
    ``python:3.12-alpine``).
  * No leftover probe objects and no existing ``narumq-statefulset-termination-grace`` policy.

Mutations: probe Pods/ConfigMap/Secret and zero-replica StatefulSets in the fixture namespace, the cluster-scoped
admission policy and binding, one real drain plus one Pod deletion of a follower. Cleanup runs in ``finally`` and is
verified; a failed cleanup fails the scenario.

Run (PowerShell; AWS_PROFILE and AWS_REGION select the credentials):
  python tests/chaos/eks-prestop-faults/main.py --run-id RUN --cluster CLUSTER --private DIR --evidence DIR --stub-image IMAGE
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any, Callable

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
for _directory in (HERE, HERE.parent, HERE.parents[1]):  # this dir, tests/chaos, tests (scenario_results)
    if str(_directory) not in sys.path:
        sys.path.append(str(_directory))

import eks_broker  # noqa: E402
import prestop_judge as judge  # noqa: E402
import prestop_objects as objects  # noqa: E402
from eks_broker import AcceptanceFailure, Management, PrerequisiteMissing  # noqa: E402
from eks_fixture import add_fixture_arguments, build_fixture, rollout_faults  # noqa: E402
from scenario_results import scenario_result  # noqa: E402

REPOSITORY_ROOT = HERE.parents[2]
ADMISSION_DIR = REPOSITORY_ROOT / "deploy" / "kubernetes" / "eks-ec2-ha-admission"
ADMISSION_PREFIX = "prestop-admission-"
GRACE_SHORT_SECONDS = 20
GRACE_FAIL_PROBE_SECONDS = 30
LONG_TIMEOUT_ANNOTATION = {judge.TIMEOUT_ANNOTATION: "1800000"}
ADMISSION_CASES = (
    {"name": "short-30", "grace": 30, "annotations": None, "accepted": False},
    {"name": "boundary-659", "grace": 659, "annotations": None, "accepted": False},
    {"name": "timeout-1800000-grace-660", "grace": 660, "annotations": LONG_TIMEOUT_ANNOTATION, "accepted": False},
    {"name": "control-660", "grace": 660, "annotations": None, "accepted": True},
    {"name": "timeout-1800000-grace-1860", "grace": 1860, "annotations": LONG_TIMEOUT_ANNOTATION, "accepted": True},
)
DUPLICATE_HOOK_TIMEOUT_SECONDS = 720
REPLACEMENT_TIMEOUT_SECONDS = 900


def feed(checks: Any, rows: list[judge.Check]) -> None:
    for name, ok, observed in rows:
        checks.check(name, ok, observed)


def get_or_none(fixture: Any, *args: str) -> dict[str, Any] | None:
    out = fixture.kubectl("get", *args, "--ignore-not-found", "-o", "json")
    return json.loads(out) if out.strip() else None


# Preflight ---------------------------------------------------------------------------------------
def preflight(fixture: Any, stub_image: str) -> dict[str, Any]:
    """Verify every prerequisite before the first mutation; raise ``PrerequisiteMissing`` otherwise."""
    if not stub_image.strip():
        raise PrerequisiteMissing("STUB_IMAGE_MISSING: --stub-image (an image with python3) is required")
    statefulset = fixture.kjson("get", "statefulset", eks_broker.STATEFULSET)
    command, broker_image = judge.extract_prestop_command(statefulset)
    pods = fixture.broker_pods()
    if len(pods) != 3 or not all(pod["ready"] and not pod["deleting"] for pod in pods.values()):
        raise PrerequisiteMissing(f"THREE_READY_BROKERS_REQUIRED: {sorted((n, p['ready']) for n, p in pods.items())}")
    try:
        groups = json.loads(fixture.kubectl("get", "--raw", "/apis/admissionregistration.k8s.io/v1"))
    except (RuntimeError, ValueError) as error:
        raise PrerequisiteMissing(f"ADMISSION_API_UNAVAILABLE: {error}") from error
    served = {resource["name"] for resource in groups.get("resources", [])}
    if not {"validatingadmissionpolicies", "validatingadmissionpolicybindings"} <= served:
        raise PrerequisiteMissing(f"ADMISSION_API_UNAVAILABLE: ValidatingAdmissionPolicy is not served ({sorted(served)})")
    if not (ADMISSION_DIR / "kustomization.yaml").is_file():
        raise PrerequisiteMissing(f"ADMISSION_PACKAGE_MISSING: {ADMISSION_DIR}")
    leftovers = leftover_objects(fixture)
    if leftovers:
        raise PrerequisiteMissing(f"LEFTOVER_PROBE_OBJECTS: remove {leftovers} before the run")
    return {"command": command, "brokerImage": broker_image, "stubImage": stub_image, "token": eks_broker.management_token(fixture),
            "statefulset": statefulset, "slots": eks_broker.slot_map(fixture)}


def leftover_objects(fixture: Any) -> list[str]:
    found = []
    for name in objects.PROBE_POD.values():
        if get_or_none(fixture, "pod", name):
            found.append(f"pod/{name}")
    for kind, name in (("configmap", objects.CONFIG_MAP), ("secret", objects.SECRET)):
        if get_or_none(fixture, kind, name):
            found.append(f"{kind}/{name}")
    for item in fixture.kjson("get", "statefulsets", "-l", "app.kubernetes.io/name=admission-probe")["items"]:
        found.append(f"statefulset/{item['metadata']['name']}")
    for kind in ("validatingadmissionpolicy", "validatingadmissionpolicybinding"):
        if fixture.kubectl("get", kind, judge.POLICY_NAME, "--ignore-not-found", "-o", "name").strip():
            found.append(f"{kind}/{judge.POLICY_NAME}")
    return found


# Probe Pods --------------------------------------------------------------------------------------
def start_probe(fixture: Any, plan: dict[str, Any], mode: str, grace: int) -> dict[str, Any]:
    name = objects.PROBE_POD[mode]
    fixture.apply(objects.probe_config_map(fixture.run_id, fixture.namespace), objects.probe_secret(fixture.run_id, fixture.namespace),
                  objects.probe_pod(mode, fixture.run_id, fixture.namespace, plan["brokerImage"], plan["stubImage"], plan["command"], grace))
    fatal = {"ImagePullBackOff", "InvalidImageName", "CreateContainerConfigError"}

    def ready() -> dict[str, Any] | None:
        pod = get_or_none(fixture, "pod", name)
        statuses = (pod or {}).get("status", {}).get("containerStatuses", [])
        for status in statuses:
            reason = (status.get("state", {}).get("waiting") or {}).get("reason")
            if reason in fatal:
                raise PrerequisiteMissing(f"PROBE_IMAGE_UNUSABLE: {status.get('image')} {reason}")
        if pod and pod["status"].get("phase") == "Running" and len(statuses) == 2 and all(s.get("ready") for s in statuses):
            return pod
        return None

    pod = rollout_faults().wait_for(ready, 300, 3, f"probe Pod {name} Running with both containers ready")
    return {"name": name, "uid": pod["metadata"]["uid"], "node": pod["spec"].get("nodeName"), "pod": pod}


def stub_reply(fixture: Any, name: str) -> dict[str, Any]:
    """One request from inside the broker container to the stub, as the hook would send it."""
    def attempt() -> dict[str, Any] | None:
        result = eks_broker.mutating_result(fixture, "exec", name, "-c", "narumq", "--", "curl", "--silent", "--max-time", "5",
                                            "--write-out", "\nHTTP:%{http_code}", "http://127.0.0.1:8080/api/v1/nodes/1/drain")
        body, _, code = result.stdout.rpartition("\nHTTP:")
        return {"status": code.strip(), "body": body.strip()} if result.returncode == 0 and code.strip().isdigit() and code.strip() != "000" else None

    return rollout_faults().wait_for(attempt, 60, 2, f"stub endpoint of {name}")


def run_hook(fixture: Any, name: str, command: list[str]) -> dict[str, Any]:
    started = time.monotonic()
    result = eks_broker.mutating_result(fixture, "exec", name, "-c", "narumq", "--", *command, timeout=120)
    return {"exitCode": result.returncode, "stdout": result.stdout, "stderr": result.stderr, "elapsedSeconds": round(time.monotonic() - started, 3)}


def delete_and_watch(fixture: Any, name: str, uid: str) -> dict[str, Any]:
    """DELETE with a UID precondition and the Pod's own grace period, then observe until the object is gone."""
    response = eks_broker.delete_pod_with_uid(fixture, name, uid)
    meta = response.get("metadata", {})
    started = time.monotonic()
    grace = meta.get("deletionGracePeriodSeconds")
    deadline = started + (grace or 30) + 180
    samples: list[dict[str, Any]] = []
    observation: dict[str, Any] = {"gone": False, "elapsedSeconds": None, "deletionGracePeriodSeconds": grace,
                                   "deletionTimestamp": meta.get("deletionTimestamp"), "samples": samples}
    while time.monotonic() < deadline:
        pod = get_or_none(fixture, "pod", name)
        at = round(time.monotonic() - started, 2)
        if pod is None:
            observation.update(gone=True, elapsedSeconds=at)
            return observation
        samples.append({"at": at, "phase": pod["status"].get("phase"), "containers": [
            {"name": s["name"], "state": next(iter(s.get("state", {})), None),
             "exitCode": (s.get("state", {}).get("terminated") or {}).get("exitCode")} for s in pod["status"].get("containerStatuses", [])]})
        time.sleep(0.5)
    return observation


def collect_events(fixture: Any, name: str, uid: str, wanted: Callable[[list[dict[str, Any]]], bool], timeout: float = 90) -> list[dict[str, Any]]:
    def read() -> list[dict[str, Any]]:
        return judge.summarize_events(fixture.kjson("get", "events", "--field-selector", f"involvedObject.name={name}")["items"], uid)

    deadline = time.monotonic() + timeout
    events = read()
    while not wanted(events) and time.monotonic() < deadline:
        time.sleep(3)
        events = read()
    time.sleep(5)  # let late kubelet Events (for example FailedPreStopHook after Killing) arrive
    return read()


def remove_probe(fixture: Any, name: str) -> None:
    fixture.mutate("delete", "pod", name, "--ignore-not-found", "--force", "--grace-period=0", "--wait=true", timeout=120)
    fixture.mutate("delete", "configmap", objects.CONFIG_MAP, "--ignore-not-found")
    fixture.mutate("delete", "secret", objects.SECRET, "--ignore-not-found")


# Part 1: failing hook ---------------------------------------------------------------------------
def failed_hook_part(fixture: Any, plan: dict[str, Any], checks: Any) -> dict[str, Any]:
    name = objects.PROBE_POD["unauthorized"]
    try:
        probe = start_probe(fixture, plan, "unauthorized", GRACE_FAIL_PROBE_SECONDS)
        feed(checks, judge.judge_probe_pod_command(probe["pod"], plan["command"]))
        reply = stub_reply(fixture, probe["name"])
        checks.check("stub management endpoint answers HTTP 401", reply["status"] == "401", reply)
        executed = run_hook(fixture, probe["name"], plan["command"])
        feed(checks, judge.judge_failed_hook_exec(executed, judge.UNAUTHORIZED_REASON))
        deletion = delete_and_watch(fixture, probe["name"], probe["uid"])
        feed(checks, judge.judge_termination(deletion, GRACE_FAIL_PROBE_SECONDS, expect_forced=False))
        events = collect_events(fixture, probe["name"], probe["uid"], lambda e: any(x["reason"] == judge.PRESTOP_EVENT_REASON for x in e))
        feed(checks, judge.judge_failed_prestop_event(events, judge.UNAUTHORIZED_REASON))
        return {"probe": {k: probe[k] for k in ("name", "uid", "node")}, "stubReply": reply, "hookExec": executed,
                "deletion": deletion, "events": events, "failureReason": judge.failure_reason(executed["stderr"])}
    finally:
        remove_probe(fixture, name)


# Part 2: grace expiry ---------------------------------------------------------------------------
def grace_expiry_part(fixture: Any, plan: dict[str, Any], checks: Any) -> dict[str, Any]:
    name = objects.PROBE_POD["draining"]
    try:
        probe = start_probe(fixture, plan, "draining", GRACE_SHORT_SECONDS)
        feed(checks, judge.judge_probe_pod_command(probe["pod"], plan["command"]))
        reply = stub_reply(fixture, probe["name"])
        checks.check("stub management endpoint reports DRAINING and never COMPLETED",
                     reply["status"] == "200" and '"phase":"DRAINING"' in reply["body"] and "COMPLETED" not in reply["body"], reply)
        deletion = delete_and_watch(fixture, probe["name"], probe["uid"])
        feed(checks, judge.judge_termination(deletion, GRACE_SHORT_SECONDS, expect_forced=True))
        events = collect_events(fixture, probe["name"], probe["uid"], lambda e: any(x["reason"] == "Killing" for x in e))
        feed(checks, judge.judge_killing_event(events))
        killed = [c for sample in deletion["samples"] for c in sample["containers"] if c["name"] == "narumq" and c["exitCode"] is not None]
        return {"probe": {k: probe[k] for k in ("name", "uid", "node")}, "stubReply": reply, "deletion": deletion, "events": events,
                "failedPreStopHookEvents": [e for e in events if e["reason"] == judge.PRESTOP_EVENT_REASON],
                "observedBrokerContainerExitCodes": sorted({c["exitCode"] for c in killed})}
    finally:
        remove_probe(fixture, name)


# Part 3: duplicate invocation on a live follower ------------------------------------------------
class DrainPoller(threading.Thread):
    """Reads the follower's drain operation every two seconds while the hooks run."""

    def __init__(self, management: Management, path: str):
        super().__init__(daemon=True)
        self.management, self.path, self.rows, self.stop = management, path, [], threading.Event()

    def run(self) -> None:
        while not self.stop.is_set():
            try:
                status, body = self.management.request("GET", self.path, timeout=5)
                data = body.get("data") if isinstance(body, dict) else None
                self.rows.append({"at": time.time(), "status": status, "operationId": (data or {}).get("operationId"), "phase": (data or {}).get("phase")})
            except Exception as error:  # a transient port-forward error is recorded, not fatal
                self.rows.append({"at": time.time(), "error": f"{type(error).__name__}: {error}"})
            self.stop.wait(2)


def pick_follower(fixture: Any, plan: dict[str, Any]) -> tuple[str, str, str]:
    """(pod name, node id, anchor pod name) with a follower target and a different pod as observer."""
    token = plan["token"]
    pods = fixture.broker_pods()
    observer = Management(fixture, "narumq-0", token)
    try:
        cluster = observer.data("/api/v1/cluster")
    finally:
        observer.close()
    if cluster.get("quorumAvailable") is not True or (cluster.get("nodesByState") or {}).get("ready") != 3:
        raise PrerequisiteMissing(f"CLUSTER_NOT_READY: {cluster}")
    leader = str(cluster["leaderNodeId"])
    follower = next((f"narumq-{ordinal}" for ordinal, node in sorted(plan["slots"].items()) if node != leader and f"narumq-{ordinal}" in pods), None)
    if follower is None:
        raise PrerequisiteMissing(f"NO_FOLLOWER: leader {leader}, slots {plan['slots']}")
    anchor = next(name for name in sorted(pods) if name != follower)
    return follower, next(node for ordinal, node in plan["slots"].items() if f"narumq-{ordinal}" == follower), anchor


def duplicate_hook_part(fixture: Any, plan: dict[str, Any], checks: Any) -> dict[str, Any]:
    token = plan["token"]
    follower, node_id, anchor = pick_follower(fixture, plan)
    pod = fixture.broker_pods()[follower]
    drain_path = f"/api/v1/nodes/{node_id}/drain"
    evidence: dict[str, Any] = {"follower": follower, "nodeId": node_id, "podUID": pod["uid"], "anchor": anchor}
    management = Management(fixture, follower, token)
    drain_started = False
    poller: DrainPoller | None = None
    try:
        before_status, before_body = management.request("GET", drain_path)
        evidence["drainBefore"] = {"status": before_status, "body": before_body}
        if before_status != 404:
            raise PrerequisiteMissing(f"FOLLOWER_HAS_DRAIN_RECORD: GET {drain_path} returned {before_status}")
        poller = DrainPoller(management, drain_path)
        command = fixture.base() + ["-n", fixture.namespace, "exec", follower, "-c", "narumq", "--", *plan["command"]]
        env = {**fixture.aws_env, "PYTHONUTF8": "1"}
        drain_started = True  # from the first hook start a drain record may exist
        poller.start()
        origin = time.monotonic()
        processes = []
        for _ in range(2):
            processes.append((time.monotonic() - origin, subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                                                          text=True, encoding="utf-8", env=env)))
        runs = []
        for started, process in processes:
            try:
                stdout, stderr = process.communicate(timeout=DUPLICATE_HOOK_TIMEOUT_SECONDS)
            except subprocess.TimeoutExpired:
                process.kill()
                stdout, stderr = process.communicate()
                stderr += "\nharness: hook did not return within the harness timeout"
            runs.append({"startedAtSeconds": round(started, 3), "finishedAtSeconds": round(time.monotonic() - origin, 3),
                         "exitCode": process.returncode, "stdout": stdout, "stderr": stderr})
        poller.stop.set()
        poller.join(15)
        after_status, after_body = management.request("GET", drain_path)
        after = after_body.get("data") if after_status == 200 and isinstance(after_body, dict) else None
        evidence.update(runs=runs, polled=poller.rows, drainAfter={"status": after_status, "data": after})
        feed(checks, judge.judge_duplicate_hooks(before_status, runs, [r for r in poller.rows if r.get("status") == 200], after))
        evidence["failureReasons"] = [judge.failure_reason(run["stderr"]) for run in runs]
        return evidence
    finally:
        if poller is not None:
            poller.stop.set()
        if drain_started:  # never touch a drain record this run did not create
            try:
                evidence["restore"] = restore_follower(fixture, plan, follower, pod["uid"], node_id, anchor)
                checks.check("follower restored: Pod replaced after COMPLETED, three brokers Ready, drain record CLEAR",
                             evidence["restore"]["restored"], evidence["restore"])
            except Exception as error:
                checks.check("follower restored: Pod replaced after COMPLETED, three brokers Ready, drain record CLEAR", False, f"{type(error).__name__}: {error}")
        management.close()


def restore_follower(fixture: Any, plan: dict[str, Any], pod_name: str, pod_uid: str, node_id: str, anchor: str) -> dict[str, Any]:
    """Return the follower to a clean READY state: replace it after COMPLETED, cancel an unfinished drain, then verify."""
    token, drain_path = plan["token"], f"/api/v1/nodes/{node_id}/drain"
    record: dict[str, Any] = {"restored": False}
    management = Management(fixture, pod_name, token)
    try:
        status, body = management.request("GET", drain_path)
        data = body.get("data") if isinstance(body, dict) else None
        phase = (data or {}).get("phase")
        record["drainState"] = {"status": status, "phase": phase}
        if status == 404:
            record["action"] = "none: no drain record"
        elif phase == "COMPLETED":
            started = time.monotonic()
            current = fixture.broker_pods().get(pod_name)
            if current is None or current["uid"] != pod_uid:
                raise RuntimeError(f"{pod_name} no longer has UID {pod_uid}; not deleting")
            eks_broker.delete_pod_with_uid(fixture, pod_name, pod_uid)
            replacement, samples = eks_broker.wait_replaced_and_ready(fixture, pod_name, pod_uid, REPLACEMENT_TIMEOUT_SECONDS)
            record.update(action="deleted after COMPLETED", replacementUID=replacement["metadata"]["uid"],
                          replacementSeconds=round(time.monotonic() - started, 1), podSamples=len(samples))
        elif phase in ("DRAINING", "FAILED"):
            cancelled, cancel_body = management.request("DELETE", drain_path, headers={"Idempotency-Key": f"prestop-restore-{pod_uid}"})
            record.update(action="cancelled the unfinished drain", cancel={"status": cancelled, "body": cancel_body})
            if cancelled != 200:
                raise RuntimeError(f"drain cancel returned {cancelled}")
        else:
            record["action"] = f"none: phase {phase}"
    finally:
        management.close()
    observer = Management(fixture, anchor, token)
    refreshed = Management(fixture, pod_name, token)
    try:
        def converged() -> dict[str, Any] | None:
            try:
                cluster = observer.data("/api/v1/cluster")
                nodes = {str(n.get("nodeId")): n for n in observer.data("/api/v1/nodes")}
            except (eks_broker.ManagementError, OSError, ValueError):
                return None
            row = nodes.get(node_id, {})
            ok = cluster.get("quorumAvailable") is True and (cluster.get("nodesByState") or {}).get("ready") == 3 and row.get("ready") is True
            return {"cluster": cluster.get("nodesByState"), "node": {k: row.get(k) for k in ("nodeId", "state", "ready")}} if ok else None

        record["converged"] = rollout_faults().wait_for(converged, 300, 3, "three READY nodes after the follower restore")
        status, body = refreshed.request("GET", drain_path)
        phase = ((body or {}).get("data") or {}).get("phase") if isinstance(body, dict) else None
        record["drainAfterRestore"] = {"status": status, "phase": phase}
        pods = fixture.broker_pods()
        record["restored"] = (status == 404 or phase == "CANCELLED") and len(pods) == 3 and all(p["ready"] for p in pods.values())
    finally:
        observer.close()
        refreshed.close()
    return record


# Part 4: admission ------------------------------------------------------------------------------
def admission_part(fixture: Any, plan: dict[str, Any], checks: Any) -> dict[str, Any]:
    evidence: dict[str, Any] = {"cases": []}
    names = [ADMISSION_PREFIX + case["name"] for case in ADMISSION_CASES]
    applied = False
    try:
        fixture.mutate("apply", "-k", str(ADMISSION_DIR))
        applied = True
        for kind in ("validatingadmissionpolicy", "validatingadmissionpolicybinding"):
            fixture.mutate("label", kind, judge.POLICY_NAME, f"{objects.RUN_LABEL}={fixture.run_id}", "--overwrite")

        def manifest(case: dict[str, Any]) -> str:
            return json.dumps(objects.admission_statefulset(ADMISSION_PREFIX + case["name"], fixture.run_id, fixture.namespace,
                                                            plan["brokerImage"], case["grace"], case["annotations"]))

        def denying() -> dict[str, Any] | None:  # the binding takes a few seconds to take effect; a dry run creates nothing
            result = eks_broker.mutating_result(fixture, "apply", "--dry-run=server", "-f", "-", stdin=manifest(ADMISSION_CASES[0]))
            return {"returncode": result.returncode, "stderr": result.stderr.strip()} if result.returncode != 0 else None

        evidence["firstDenial"] = rollout_faults().wait_for(denying, 90, 3, "admission policy to start denying")
        policy = fixture.kjson("get", "validatingadmissionpolicy", judge.POLICY_NAME)
        feed(checks, judge.judge_policy_status(policy))
        for case in ADMISSION_CASES:
            name = ADMISSION_PREFIX + case["name"]
            minimum = judge.minimum_grace_seconds(judge.drain_timeout_millis(case["annotations"]))
            result = eks_broker.mutating_result(fixture, "apply", "-f", "-", stdin=manifest(case))
            row = {"case": case["name"], "grace": case["grace"], "minimum": minimum, "returncode": result.returncode,
                   "stdout": result.stdout.strip(), "stderr": result.stderr.strip()}
            existing = get_or_none(fixture, "statefulset", name)
            if case["accepted"]:
                feed(checks, [(f"{case['name']}: {n}", ok, o) for n, ok, o in judge.judge_admission_accepted(result.returncode, result.stdout, result.stderr)])
                checks.check(f"{case['name']}: accepted object carries the submitted grace",
                             existing is not None and existing["spec"]["template"]["spec"]["terminationGracePeriodSeconds"] == case["grace"],
                             None if existing is None else existing["spec"]["template"]["spec"]["terminationGracePeriodSeconds"])
            else:
                feed(checks, [(f"{case['name']}: {n}", ok, o) for n, ok, o in judge.judge_admission_rejected(result.returncode, result.stderr, case["grace"], minimum)])
                checks.check(f"{case['name']}: rejected object was not stored", existing is None, None if existing is None else existing["metadata"]["name"])
            evidence["cases"].append(row)
        live = fixture.kjson("get", "statefulset", eks_broker.STATEFULSET)
        checks.check("live broker StatefulSet was not touched by the admission cases",
                     live["metadata"]["resourceVersion"] == plan["statefulset"]["metadata"]["resourceVersion"], live["metadata"]["resourceVersion"])
        return evidence
    finally:
        problems = []
        for name in names:
            try:
                fixture.mutate("delete", "statefulset", name, "--ignore-not-found", "--wait=true", timeout=180)
            except Exception as error:
                problems.append(f"{name}: {error}")
        if applied:
            for kind in ("validatingadmissionpolicybinding", "validatingadmissionpolicy"):
                try:
                    fixture.mutate("delete", kind, judge.POLICY_NAME, "--ignore-not-found", "--wait=true")
                except Exception as error:
                    problems.append(f"{kind}: {error}")
        remaining = leftover_objects(fixture)
        evidence["cleanup"] = {"problems": problems, "remaining": remaining}
        checks.check("admission objects and zero-replica StatefulSets were removed and verified gone", not problems and not remaining, evidence["cleanup"])


# Scenario ---------------------------------------------------------------------------------------
# Scenarios: drain-and-rolling-update.md:DRN-SCN-030
@scenario_result
def scenario_prestop_failure_duplicate_and_short_grace(fixture: Any, stub_image: str) -> dict[str, Any]:
    module = rollout_faults()
    checks, started = module.Checks(), time.time()
    plan = preflight(fixture, stub_image)
    feed(checks, judge.judge_live_statefulset(plan["statefulset"]))
    evidence: dict[str, Any] = {"liveStatefulSet": {"image": plan["brokerImage"], "resourceVersion": plan["statefulset"]["metadata"]["resourceVersion"]},
                                "preStopSha256": hashlib.sha256(plan["command"][2].encode("utf-8")).hexdigest(), "parts": {}}
    parts = (("admission", admission_part), ("failedHook", failed_hook_part), ("graceExpiry", grace_expiry_part), ("duplicateHook", duplicate_hook_part))
    for name, part in parts:
        try:
            evidence["parts"][name] = part(fixture, plan, checks)
        except Exception as error:  # evidence of the other parts is still worth collecting
            checks.check(f"{name}: part ran to completion", False, f"{type(error).__name__}: {error}")
    leftovers = leftover_objects(fixture)
    checks.check("no probe object, policy or zero-replica StatefulSet is left in the fixture", not leftovers, leftovers)
    result = checks.result("prestop_failure_duplicate_and_short_grace", startedAt=started, finishedAt=time.time(), **evidence)
    fixture.save_result("prestop-faults-result.json", result)
    if result["result"] != "PASS":
        failing = [item["check"] for item in result["checks"] if not item["ok"]]
        raise AcceptanceFailure(f"DRN-SCN-030 failed {len(failing)} check(s): {failing}")
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    add_fixture_arguments(parser)
    parser.add_argument("--stub-image", required=True, help="image with python3 that the fixture nodes can pull (stub management endpoint)")
    args = parser.parse_args(argv)
    fixture = build_fixture(args)
    scenario_prestop_failure_duplicate_and_short_grace(fixture, args.stub_image)
    print(f"DRN-SCN-030 PASS; evidence in {args.evidence}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
