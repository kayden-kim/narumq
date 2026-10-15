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


class IndependentMonitor:
    """Journal management and MQTT probes without a dependency between them."""

    def __init__(self, management: Callable, mqtt: Callable, output: Path, interval: float = 2.0):
        if interval <= 0:
            raise ValueError("probe interval must be positive")
        self.probes = {"management": management, "mqtt": mqtt}
        self.output = Path(output)
        self.interval = interval
        self._stop = threading.Event()
        self._first = {name: threading.Event() for name in self.probes}
        self._lock = threading.Lock()
        self._samples: dict[str, list[dict]] = {name: [] for name in self.probes}
        self._errors: list[str] = []
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
            for name in self.probes:
                self._streams[name] = (self.output / f"{name}-samples.jsonl").open("x", encoding="utf-8")
        except Exception:
            for stream in self._streams.values():
                stream.close()
            raise
        self._started = True
        self._epoch = time.monotonic()
        for name, probe in self.probes.items():
            thread = threading.Thread(target=self._run, args=(name, probe), name=f"observer-{name}", daemon=True)
            self._threads.append(thread)
            thread.start()

    def _run(self, name: str, probe: Callable) -> None:
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
                self._stop.wait(self.interval)
        except Exception as exc:
            with self._lock:
                self._errors.append(f"{name}:{type(exc).__name__}")
            self._first[name].set()
            self._stop.set()

    def wait_initial(self, timeout: float = 30) -> bool:
        deadline = time.monotonic() + timeout
        return all(event.wait(max(0, deadline - time.monotonic())) for event in self._first.values())

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
        return {"result": "PASS" if passed else "FAIL", "streams": streams,
                "workersStoppedAndJournalsClosed": self._closed, "journalErrors": list(self._errors),
                "shutdownDeadlineExceeded": self._shutdown_failed,
                "scope": "sampled observations only, not continuous availability between samples"}


class FencingUnsafe(RuntimeError):
    pass


class MutationRejected(RuntimeError):
    """The mutation command failed; neither its cause nor application is inferred."""


def _instance(node: dict) -> str:
    return str(node.get("spec", {}).get("providerID", "")).rsplit("/", 1)[-1]


def _fenced(node: dict) -> bool:
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
    if _fenced(before):
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
    if _fenced(after):
        return {**result, "outcome": "out_of_service",
                "mutationApplied": None if result["patchFailed"] else True}
    if result["patchFailed"]:
        changed = result["afterResourceVersion"] != rv
        return {**result, "outcome": "failed_observation_changed" if changed else "failed_unexplained"}
    return {**result, "outcome": "mutation_unconfirmed"}
