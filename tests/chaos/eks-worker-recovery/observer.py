"""Local observation and fencing decisions for disposable EKS worker fixtures."""
from __future__ import annotations

import copy
import datetime
import json
from pathlib import Path
import threading
import time
from typing import Callable

MANAGEMENT_TIMEOUT_SECONDS = 10
OUT_OF_SERVICE = "node.kubernetes.io/out-of-service"


def timestamp() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


DIAGNOSTIC_NODE_FIELDS = ("nodeId", "membershipPhase", "queryStatus", "queryErrorCode", "state", "ready", "readinessReason")


def node_diagnostic(node: dict) -> dict:
    """Keep only the per-node fields used for diagnosis; other fields are dropped."""
    return {field: node[field] for field in DIAGNOSTIC_NODE_FIELDS}


class IndependentMonitor:
    """Journal management and MQTT probes without a dependency between them; diagnostics never gate PASS."""

    def __init__(self, management: Callable, mqtt: Callable, output: Path, interval: float = 2.0,
                 diagnostics: dict[str, Callable] | None = None, diagnostic_interval: float = 4.0):
        if interval <= 0 or diagnostic_interval <= 0:
            raise ValueError("probe interval must be positive")
        self.probes = {"management": management, "mqtt": mqtt}
        self.diagnostics = dict(diagnostics or {})
        if set(self.diagnostics) & set(self.probes):
            raise ValueError("diagnostic stream name collides with an acceptance stream")
        self._all = {**self.probes, **self.diagnostics}
        self.output = Path(output)
        self.interval = interval
        self.diagnostic_interval = diagnostic_interval
        self._stop = threading.Event()
        self._first = {name: threading.Event() for name in self._all}
        self._lock = threading.Lock()
        self._samples: dict[str, list[dict]] = {name: [] for name in self._all}
        self._errors: list[str] = []
        self._baseline: dict[str, dict] = {}
        self._threads: list[threading.Thread] = []
        self._streams = {}
        self._started = False
        self._closed = False
        self._shutdown_failed = False
        self._epoch = 0.0

    def start(self) -> None:
        if self._started:
            raise RuntimeError("monitor already started")
        self.output.mkdir(parents=True, exist_ok=True)
        try:
            for name in self._all:
                self._streams[name] = (self.output / f"{name}-samples.jsonl").open("x", encoding="utf-8")
        except Exception:
            for stream in self._streams.values():
                stream.close()
            raise
        self._started = True
        self._epoch = time.monotonic()
        for name, probe in self._all.items():
            interval = self.diagnostic_interval if name in self.diagnostics else self.interval
            thread = threading.Thread(target=self._run, args=(name, probe, interval), name=f"observer-{name}", daemon=True)
            self._threads.append(thread)
            thread.start()

    def _run(self, name: str, probe: Callable, interval: float) -> None:
        try:
            while not self._stop.is_set():
                started = time.monotonic()
                sample = {"startedAt": timestamp(), "startedSeconds": started - self._epoch}
                try:
                    sample.update(ok=True, data=probe())
                except Exception as exc:
                    # Exception text can include credentials, URLs or payloads.
                    sample.update(ok=False, errorCode=type(exc).__name__)
                sample.update(finishedAt=timestamp(), elapsedSeconds=time.monotonic() - started)
                self._streams[name].write(json.dumps(sample, separators=(",", ":")) + "\n")
                self._streams[name].flush()
                with self._lock:
                    self._samples[name].append(sample)
                self._first[name].set()
                self._stop.wait(interval)
        except Exception as exc:
            with self._lock:
                self._errors.append(f"{name}:{type(exc).__name__}")
            self._first[name].set()
            self._stop.set()

    def wait_initial(self, timeout: float = 30) -> bool:
        deadline = time.monotonic() + timeout
        return all(self._first[name].wait(max(0, deadline - time.monotonic())) for name in self.probes)

    def wait_diagnostic_baseline(self, name: str, limit: float) -> dict:
        """Wait at most `limit` seconds for the first completed diagnostic sample before a fault is requested."""
        if name not in self.diagnostics:
            raise ValueError(f"{name} is not a diagnostic stream")
        if limit <= 0:
            raise ValueError("baseline limit must be positive")
        started = time.monotonic()
        self._first[name].wait(limit)
        waited = time.monotonic() - started
        with self._lock:
            samples = list(self._samples[name])
        first = samples[0] if samples else None
        baseline = {"limitSeconds": limit, "waitedSeconds": round(waited, 3),
                    "available": first is not None and first["ok"],
                    "firstSampleOk": None if first is None else first["ok"],
                    "firstSampleStartedAt": None if first is None else first["startedAt"],
                    "firstSampleFinishedAt": None if first is None else first["finishedAt"],
                    "samplesAtWaitEnd": len(samples),
                    "firstSampleMayOverlapFault": first is None}
        self._baseline[name] = baseline
        return dict(baseline)

    def snapshot(self) -> dict[str, list[dict]]:
        with self._lock:
            return copy.deepcopy(self._samples)

    def stop(self, timeout: float = 20) -> None:
        if not self._started or self._closed:
            return
        self._stop.set()
        deadline = time.monotonic() + timeout
        for thread in self._threads:
            thread.join(max(0, deadline - time.monotonic()))
        if any(thread.is_alive() for thread in self._threads):
            self._shutdown_failed = True
            raise TimeoutError("probe did not stop within its deadline")
        for stream in self._streams.values():
            stream.close()
        self._closed = True
        if self._errors:
            raise RuntimeError("probe journal failed: " + ",".join(self._errors))

    def summary(self) -> dict:
        samples = self.snapshot()
        fields = {"management": "quorumAvailable", "mqtt": "puback"}
        streams = {}
        for name, field in fields.items():
            successful = sum(s["ok"] and s.get("data", {}).get(field) is True for s in samples[name])
            streams[name] = {"samples": len(samples[name]), "successful": successful,
                             "failed": len(samples[name]) - successful}
        passed = self._closed and not self._errors and not self._shutdown_failed and all(s["samples"] > 0 and s["failed"] == 0 for s in streams.values())
        report = {"result": "PASS" if passed else "FAIL", "streams": streams,
                  "workersStoppedAndJournalsClosed": self._closed, "journalErrors": list(self._errors),
                  "shutdownDeadlineExceeded": self._shutdown_failed}
        if self.diagnostics:
            report["diagnostics"] = {name: {"samples": len(samples[name]),
                                            "ok": sum(s["ok"] for s in samples[name]),
                                            "failed": sum(not s["ok"] for s in samples[name])}
                                     for name in self.diagnostics}
            for name, baseline in self._baseline.items():
                report["diagnostics"][name]["baseline"] = dict(baseline)
        report["scope"] = "sampled observations only, not continuous availability between samples"
        return report


class FencingUnsafe(RuntimeError):
    pass


class MutationRejected(RuntimeError):
    """The mutation command failed; neither its cause nor application is inferred."""


def _instance(node: dict) -> str:
    return str(node.get("spec", {}).get("providerID", "")).rsplit("/", 1)[-1]


def fenced(node: dict) -> bool:
    return any(t.get("key") == OUT_OF_SERVICE and t.get("effect") == "NoExecute"
               for t in node.get("spec", {}).get("taints", []))


def fence_node(get_node: Callable, patch_node: Callable, get_instance_state: Callable,
               node_name: str, expected_uid: str, expected_instance: str) -> dict:
    """Attempt at most one identity-guarded patch, then classify a fresh read.

    A rejected mutation is never retried or attributed to a specific JSON Patch
    operation. The caller keeps observing recovery without changing another Node.
    """
    if not expected_uid or not expected_instance:
        raise FencingUnsafe("missing expected node identity")
    if get_instance_state(expected_instance) != "terminated":
        raise FencingUnsafe("EC2 termination is not established")
    before = get_node(node_name)
    result = {"at": timestamp(), "expectedNodeUID": expected_uid, "expectedInstanceId": expected_instance,
              "mutationAttempted": False, "mutationApplied": False, "patchFailed": False,
              "failureCauseConfirmed": False}
    if before is None:
        return {**result, "outcome": "node_absent"}
    if before["metadata"]["uid"] != expected_uid or _instance(before) != expected_instance:
        return {**result, "outcome": "node_identity_changed", "observedNodeUID": before["metadata"]["uid"]}
    if fenced(before):
        return {**result, "outcome": "already_out_of_service"}
    rv = before["metadata"].get("resourceVersion")
    if not isinstance(rv, str) or not rv:
        raise FencingUnsafe("missing node resourceVersion")
    taints = copy.deepcopy(before.get("spec", {}).get("taints", []))
    taints.append({"key": OUT_OF_SERVICE, "value": "fenced-terminated", "effect": "NoExecute"})
    patch = [{"op": "test", "path": "/metadata/uid", "value": expected_uid},
             {"op": "test", "path": "/metadata/resourceVersion", "value": rv},
             {"op": "add", "path": "/spec/taints", "value": taints}]
    if get_instance_state(expected_instance) != "terminated":
        raise FencingUnsafe("EC2 fencing confirmation changed")
    result.update(mutationAttempted=True, beforeResourceVersion=rv)
    try:
        patch_node(node_name, patch)
    except MutationRejected:
        result.update(patchFailed=True, mutationApplied=None)
    after = get_node(node_name)
    if after is None:
        return {**result, "outcome": "node_absent"}
    result.update(afterResourceVersion=after["metadata"].get("resourceVersion"),
                  observedNodeUID=after["metadata"]["uid"])
    if after["metadata"]["uid"] != expected_uid or _instance(after) != expected_instance:
        return {**result, "outcome": "node_identity_changed"}
    if fenced(after):
        return {**result, "outcome": "out_of_service",
                "mutationApplied": None if result["patchFailed"] else True}
    if result["patchFailed"]:
        changed = result["afterResourceVersion"] != rv
        return {**result, "outcome": "failed_observation_changed" if changed else "failed_unexplained"}
    return {**result, "outcome": "mutation_unconfirmed"}


FENCED_OUTCOMES = ("out_of_service", "already_out_of_service")


def release_fenced_pod(get_node: Callable, get_pod: Callable, delete_pod: Callable, get_instance_state: Callable,
                       fencing: dict, node_name: str, expected_node_uid: str, expected_instance: str,
                       pod_name: str, expected_pod_uid: str) -> dict:
    """Delete the old Pod once, UID-guarded with grace 0, while its terminated Node still carries the taint.

    PodGC force-deletes such a Pod only while the Node is both NotReady and tainted; after an EC2 terminate
    the cloud controller may delete the Node (and the taint) first, which leaves detach to the force-detach
    timeout. Every identity is re-read before the single attempt; a replacement Pod is never deleted.
    """
    if not all((expected_node_uid, expected_instance, expected_pod_uid)):
        raise FencingUnsafe("missing expected Pod or Node identity")
    result = {"at": timestamp(), "expectedPodUID": expected_pod_uid, "fencingOutcome": fencing.get("outcome"),
              "deleteAttempted": False, "deleteApplied": False, "deleteFailed": False}
    if fencing.get("outcome") not in FENCED_OUTCOMES:
        return {**result, "outcome": "skipped_not_fenced"}
    if get_instance_state(expected_instance) != "terminated":
        raise FencingUnsafe("EC2 termination is not established")
    node = get_node(node_name)
    if node is None:
        return {**result, "outcome": "node_absent"}
    if node["metadata"]["uid"] != expected_node_uid or _instance(node) != expected_instance:
        return {**result, "outcome": "node_identity_changed"}
    if not fenced(node):
        return {**result, "outcome": "node_not_fenced"}
    pod = get_pod(pod_name)
    if pod is None:
        return {**result, "outcome": "pod_absent"}
    if pod["metadata"]["uid"] != expected_pod_uid or pod.get("spec", {}).get("nodeName") != node_name:
        return {**result, "outcome": "pod_identity_changed", "observedPodUID": pod["metadata"]["uid"]}
    if get_instance_state(expected_instance) != "terminated":
        raise FencingUnsafe("EC2 fencing confirmation changed")
    result.update(deleteAttempted=True, deleteRequestedAt=timestamp())
    try:
        delete_pod(pod_name, expected_pod_uid)
    except MutationRejected:
        result.update(deleteFailed=True, deleteApplied=None)
    after = get_pod(pod_name)
    result["observedAt"] = timestamp()
    if after is None or after["metadata"]["uid"] != expected_pod_uid:
        if after is not None:
            result["observedPodUID"] = after["metadata"]["uid"]
        return {**result, "outcome": "deleted", "deleteApplied": None if result["deleteFailed"] else True}
    return {**result, "outcome": "failed_unexplained" if result["deleteFailed"] else "delete_unconfirmed"}


STREAM_FIELDS = {"management": "quorumAvailable", "mqtt": "puback"}


def _failed(sample: dict, field: str) -> bool:
    return not (sample.get("ok") and sample.get("data", {}).get(field) is True)


def _at(value: str) -> datetime.datetime:
    return datetime.datetime.fromisoformat(value)


def failure_windows(samples: list[dict], field: str, fault_requested_at: str) -> dict:
    """Group the failed samples of one stream into runs of consecutive failures.

    A window starts at the first failed sample's start and ends when the next successful sample finishes
    (monitor-relative seconds). A run without a later success stays open. Failures that started before
    the fault request are counted separately because no window may cover them.
    """
    fault = _at(fault_requested_at)
    ordered = sorted(samples, key=lambda s: s["startedSeconds"])
    before = sum(_failed(s, field) for s in ordered if _at(s["startedAt"]) < fault)
    after = [s for s in ordered if _at(s["startedAt"]) >= fault]
    windows: list[dict] = []
    current = None
    for sample in after:
        if _failed(sample, field):
            if current is None:
                current = {"start": sample["startedSeconds"], "end": None, "failedSamples": 0}
                windows.append(current)
            current["failedSamples"] += 1
        elif current is not None:
            current["end"] = sample["startedSeconds"] + sample["elapsedSeconds"]
            current = None
    return {"samplesAfterFault": len(after), "failedBeforeFault": before,
            "failedAfterFault": sum(w["failedSamples"] for w in windows), "windows": windows}


def _span(window: dict) -> float | None:
    return None if window["end"] is None else window["end"] - window["start"]


def merge_windows(windows: list[dict]) -> list[dict]:
    """Union of windows across streams; an open window extends to infinity."""
    merged: list[dict] = []
    for window in sorted(windows, key=lambda w: w["start"]):
        if merged and (merged[-1]["end"] is None or window["start"] <= merged[-1]["end"]):
            last = merged[-1]
            last["end"] = None if last["end"] is None or window["end"] is None else max(last["end"], window["end"])
        else:
            merged.append({"start": window["start"], "end": window["end"]})
    return merged


def leader_continuity(samples: dict[str, list[dict]], fault_requested_at: str, target_node_id: str,
                      limit_seconds: float = 30.0) -> dict:
    """Judge a leader loss: failures only inside ONE closed window of at most `limit_seconds`, and a
    management sample that shows a surviving member as leader with quorum after the fault request."""
    reasons: list[str] = []
    streams = {}
    every = []
    for name, field in STREAM_FIELDS.items():
        stream = failure_windows(samples[name], field, fault_requested_at)
        for window in stream["windows"]:
            window["spanSeconds"] = _span(window)
            every.append(window)
        if stream["samplesAfterFault"] == 0:
            reasons.append(f"{name}: no samples after the fault request")
        if stream["failedBeforeFault"]:
            reasons.append(f"{name}: failure before the fault request")
        if len(stream["windows"]) > 1:
            reasons.append(f"{name}: more than one failure window")
        streams[name] = stream
    union = merge_windows([{"start": w["start"], "end": w["end"]} for w in every])
    for window in union:
        window["spanSeconds"] = _span(window)
        if window["spanSeconds"] is None:
            reasons.append("failure window is still open at the last sample")
        elif window["spanSeconds"] > limit_seconds:
            reasons.append(f"failure window longer than {limit_seconds:g} s")
    if len(union) > 1:
        reasons.append("failures are not inside one contiguous window")
    fault = _at(fault_requested_at)
    new_leader = next(({"nodeId": str(s["data"]["leaderNodeId"]), "at": s["startedAt"]}
                       for s in samples["management"]
                       if s.get("ok") and _at(s["startedAt"]) >= fault and s["data"].get("quorumAvailable") is True
                       and s["data"].get("leaderNodeId") not in (None, "")
                       and str(s["data"]["leaderNodeId"]) != str(target_node_id)), None)
    if new_leader is None:
        reasons.append("no management sample shows a surviving member as leader with quorum")
    return {"result": "FAIL" if reasons else "PASS", "limitSeconds": limit_seconds, "streams": streams,
            "union": union, "newLeader": new_leader, "reasons": reasons}


def monitor_integrity(summary: dict) -> bool:
    """Workers stopped, journals closed without error, and every acceptance stream has samples."""
    return bool(summary["workersStoppedAndJournalsClosed"] and not summary["journalErrors"]
                and not summary["shutdownDeadlineExceeded"]
                and all(stream["samples"] > 0 for stream in summary["streams"].values()))
