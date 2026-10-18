"""Identity-checked kubectl port-forward to one exact broker Pod, without retry or reconnect."""
from __future__ import annotations

import subprocess
import threading
import time

from fixture import ready
from observer import timestamp


class PodTunnel:
    """Own two local ports for one Pod identified by name and UID."""

    def __init__(self, fixture, pod: str, pod_uid: str, management_port: int, mqtt_port: int):
        self.fixture = fixture
        self.pod = pod
        self.pod_uid = pod_uid
        self.management_port = management_port
        self.mqtt_port = mqtt_port
        self.process = None
        self.reader = None
        self.bound: set[int] = set()
        self.lock = threading.Lock()

    def identity(self) -> dict:
        if not (isinstance(self.pod, str) and self.pod and isinstance(self.pod_uid, str) and self.pod_uid):
            raise ValueError("missing Pod identity")
        pod = self.fixture.get("pod", self.pod, "-n", "narumq")
        if (not pod or pod["metadata"]["uid"] != self.pod_uid or pod["metadata"].get("deletionTimestamp")
                or pod["metadata"].get("labels", {}).get("app") != "narumq" or not ready(pod)):
            raise ValueError("Pod identity is not current and Ready")
        return {"pod": self.pod, "podUID": self.pod_uid}

    def start(self, timeout: float = 20) -> dict:
        identity = self.identity()
        ports = (self.management_port, self.mqtt_port)
        self.process = subprocess.Popen(
            self.fixture.kubectl + ["-n", "narumq", "port-forward", "pod/" + self.pod, "--address=127.0.0.1",
                                    f"{self.management_port}:8080", f"{self.mqtt_port}:1883"],
            env=self.fixture.environment, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, encoding="utf-8", errors="replace")

        def drain():
            for line in self.process.stdout:
                for port in ports:
                    if line.startswith(f"Forwarding from 127.0.0.1:{port} ->"):
                        with self.lock:
                            self.bound.add(port)

        self.reader = threading.Thread(target=drain, daemon=True)
        self.reader.start()
        try:
            deadline = time.monotonic() + timeout
            while time.monotonic() < deadline:
                self.check()
                with self.lock:
                    established = self.bound == set(ports)
                if established:
                    self.identity()
                    self.check()
                    return {**identity, "at": timestamp(),
                            "ports": {"management": self.management_port, "mqtt": self.mqtt_port},
                            "routing": "owned kubectl port-forward to exact Pod; no service selection or reconnect"}
                time.sleep(0.05)
            raise TimeoutError("Pod tunnel ports did not become ready")
        except Exception:
            self.close()
            raise

    def check(self) -> None:
        if self.process is None or self.process.poll() is not None:
            raise RuntimeError("Pod tunnel is not running")

    def close(self) -> None:
        if self.process is not None:
            if self.process.poll() is None:
                self.process.terminate()
                try:
                    self.process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    self.process.kill()
                    self.process.wait(timeout=5)
            if self.reader is not None:
                self.reader.join(timeout=5)
                if self.reader.is_alive():
                    raise TimeoutError("Pod tunnel output worker did not stop")
            self.process.stdout.close()
