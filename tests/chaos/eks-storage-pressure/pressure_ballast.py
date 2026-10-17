"""Ballast file control through an ephemeral container that shares the broker Pod's PVC filesystem.

The broker container and its configuration are never touched. Every command runs in the `ballast` ephemeral
container (broker image, busybox applets `df`, `dd`, `truncate`, `stat`, `killall`, `sync`, `rm`).
"""
from __future__ import annotations

import json
import time

import pressure_logic as logic

CONTAINER = "ballast"
NAMESPACE = "narumq"
VOLUME_NAME = "data"
MOUNT = "/volume"
FILE = "/volume/chaos-ballast.bin"
MAX_ROUNDS = 4
RUNNING_WAIT_SECONDS = 60
# Assumed worst-case sequential write rate used only to size the kubectl exec timeout.
ASSUMED_MIB_PER_SECOND = 10


class BallastError(RuntimeError):
    pass


# PID 1 ignores SIGTERM without a handler, which would make Pod deletion wait the whole grace period.
IDLE_COMMAND = 'trap "exit 0" TERM INT; while :; do sleep 1 & wait $!; done'


def ephemeral_container(image: str) -> dict:
    return {"name": CONTAINER, "image": image, "imagePullPolicy": "IfNotPresent",
            "command": ["/bin/sh", "-c", IDLE_COMMAND],
            "securityContext": {"runAsUser": 0, "runAsGroup": 0, "allowPrivilegeEscalation": False,
                                "readOnlyRootFilesystem": True},
            "volumeMounts": [{"name": VOLUME_NAME, "mountPath": MOUNT}]}


def ephemeral_status(pod: dict) -> dict | None:
    return next((s for s in (pod.get("status") or {}).get("ephemeralContainerStatuses", [])
                 if s.get("name") == CONTAINER), None)


def ephemeral_running(pod: dict) -> bool:
    status = ephemeral_status(pod)
    return bool(status and (status.get("state") or {}).get("running"))


class BallastController:
    """Create, grow, shrink and remove one ballast per Pod; `registry` remembers every Pod that may hold one."""

    def __init__(self, fixture, image: str, registry: dict | None = None, sleep=time.sleep, clock=time.monotonic):
        self.fixture = fixture
        self.image = image
        self.registry = registry if registry is not None else {}
        self.sleep = sleep
        self.clock = clock

    # -- container lifecycle --

    def ensure(self, pod: str, uid: str) -> dict:
        """Create the ephemeral container, or reuse a running one left by an earlier scenario of this run."""
        current = self.fixture.get_pod(pod)
        if not current or current["metadata"]["uid"] != uid or current["metadata"].get("deletionTimestamp"):
            raise BallastError("Pod is missing, replaced or terminating")
        existing = [c for c in current["spec"].get("ephemeralContainers", []) if c["name"] == CONTAINER]
        # Registered before the request so cleanup still runs when the patch outcome is unknown.
        entry = self.registry.setdefault(pod, {"uid": uid, "reused": False, "removed": None, "lostWithPod": False})
        entry["uid"] = uid
        entry["removed"] = None
        if existing:
            if not ephemeral_running(current):
                raise BallastError("existing ballast container is not running")
            entry["reused"] = True
        else:
            patch = {"spec": {"ephemeralContainers": [ephemeral_container(self.image)]}}
            self.fixture.command(self.fixture.kubectl + [
                "-n", NAMESPACE, "patch", "pod", pod, "--subresource=ephemeralcontainers", "--type=strategic",
                "-p", json.dumps(patch), "-o", "json"], timeout=60)
            deadline = self.clock() + RUNNING_WAIT_SECONDS
            while True:
                latest = self.fixture.get_pod(pod)
                if latest and latest["metadata"]["uid"] == uid and ephemeral_running(latest):
                    break
                if self.clock() >= deadline:
                    raise BallastError("ballast container did not start")
                self.sleep(1)
        return {"pod": pod, "uid": uid, "created": not existing, "reused": bool(existing)}

    def exec(self, pod: str, script: str, timeout: float = 90) -> str:
        return self.fixture.command(self.fixture.kubectl + [
            "-n", NAMESPACE, "exec", pod, "-c", CONTAINER, "--", "/bin/sh", "-c", script], timeout=timeout)

    # -- measurements --

    def df(self, pod: str) -> dict:
        return {**logic.parse_df(self.exec(pod, f"df -k {MOUNT}")), "readAt": time.time()}

    def size(self, pod: str) -> int:
        value = self.exec(pod, f"stat -c %s {FILE} 2>/dev/null || echo 0").strip()
        if not value.isdigit():
            raise BallastError("ballast size is not numeric")
        return int(value)

    # -- mutations --

    def _dd(self, pod: str, count_mib: int | None, block_size: str = "1M") -> dict:
        count = "" if count_mib is None else f" count={count_mib}"
        timeout = 3600 if count_mib is None else min(3600, max(300, count_mib // ASSUMED_MIB_PER_SECOND + 60))
        try:
            output = self.exec(pod, f"dd if=/dev/zero of={FILE} bs={block_size}{count} oflag=append "
                                    "conv=notrunc,fsync 2>&1; echo DD_EXIT=$?; sync", timeout=timeout)
            return {"ddExit": logic.dd_exit_code(output), "interrupted": False}
        except Exception as exc:
            # The remote dd may outlive a dropped kubectl exec; stop it before measuring again.
            self.stop_writers(pod)
            return {"ddExit": None, "interrupted": True, "errorCode": type(exc).__name__}

    def stop_writers(self, pod: str) -> None:
        try:
            self.exec(pod, "killall dd 2>/dev/null; true", timeout=30)
        except Exception:
            pass

    def fill(self, pod: str, nbytes: int) -> dict:
        if nbytes <= 0 or nbytes % logic.MIB:
            raise ValueError("fill size must be a positive MiB multiple")
        return self._dd(pod, nbytes // logic.MIB)

    def shrink(self, pod: str, nbytes: int) -> dict:
        size = self.size(pod)
        if nbytes <= 0 or nbytes > size:
            raise BallastError("shrink size is outside the ballast file")
        self.exec(pod, f"truncate -s {size - nbytes} {FILE} && sync")
        return {"before": size, "after": self.size(pod)}

    def fill_to_enospc(self, pod: str) -> dict:
        """Write until the filesystem is full; a non-zero dd exit is the expected end, df proves it."""
        rounds = []
        for _ in range(MAX_ROUNDS):
            result = self._dd(pod, None)
            reading = self.df(pod)
            rounds.append({**result, "availableBytes": reading["availableBytes"]})
            if logic.is_full(reading["availableBytes"]):
                # The 1 MiB-block dd can stop with a few hundred KiB left; 4 KiB blocks take most of the rest.
                tail = self._dd(pod, None, "4096")
                reading = self.df(pod)
                rounds.append({**tail, "blockSize": 4096, "availableBytes": reading["availableBytes"]})
                return {"full": logic.is_full(reading["availableBytes"]), "rounds": rounds, "df": reading,
                        "ballastBytes": self.size(pod)}
        raise BallastError("filesystem did not reach ENOSPC")

    def adjust_free(self, pod: str, target_free: int, tolerance: int) -> dict:
        """Move free space into the tolerance band around target_free, filling or shrinking as needed."""
        rounds = []
        for _ in range(MAX_ROUNDS):
            reading = self.df(pod)
            decision = logic.adjust_action(reading["availableBytes"], target_free, tolerance, self.size(pod))
            rounds.append({"availableBytes": reading["availableBytes"], **decision})
            if decision["action"] == "none":
                return {"ok": True, "targetFreeBytes": target_free, "toleranceBytes": tolerance, "rounds": rounds,
                        "df": reading}
            rounds[-1]["result"] = (self.fill if decision["action"] == "fill" else self.shrink)(pod, decision["bytes"])
        raise BallastError("free space did not settle near the target")

    def release_to(self, pod: str, minimum_free: int) -> dict:
        """Shrink the ballast until free space is at least minimum_free; an absent ballast leaves it unchanged."""
        reading = self.df(pod)
        decision = logic.release_action(reading["availableBytes"], minimum_free, self.size(pod))
        result = None
        if decision["action"] == "shrink":
            result = self.shrink(pod, decision["bytes"])
            reading = self.df(pod)
        if reading["availableBytes"] < minimum_free:
            raise BallastError("free space is still below the required minimum after release")
        return {"minimumFreeBytes": minimum_free, **decision, "shrink": result, "df": reading}

    def remove(self, pod: str) -> dict:
        """Delete the ballast file and prove it is gone and free space returned."""
        self.exec(pod, f"killall dd 2>/dev/null; rm -f {FILE} && sync", timeout=60)
        remaining = self.size(pod)
        reading = self.df(pod)
        if remaining:
            raise BallastError("ballast file still present after removal")
        result = {"removed": True, "df": reading}
        if pod in self.registry:
            self.registry[pod]["removed"] = result
        return result

    def remove_all(self) -> dict:
        """Cleanup for `finally`: try every registered Pod, never raise, and report each outcome."""
        report = {}
        for pod, entry in list(self.registry.items()):
            if entry.get("removed") and entry["removed"].get("removed") is True:
                report[pod] = {"alreadyRemoved": True}
                continue
            try:
                current = self.fixture.get_pod(pod)
                if not current or current["metadata"]["uid"] != entry["uid"]:
                    entry["lostWithPod"] = True
                    report[pod] = {"removed": False, "outcome": "pod_replaced_ballast_lost_with_pod"}
                    continue
                entry["removed"] = self.remove(pod)
                report[pod] = {"removed": True, "df": entry["removed"]["df"]}
            except Exception as exc:
                report[pod] = {"removed": False, "outcome": "failed", "errorCode": type(exc).__name__}
        return report
