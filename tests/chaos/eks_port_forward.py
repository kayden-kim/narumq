"""Owned ``kubectl port-forward`` processes for the strict-fixture harnesses under tests/chaos.

``eks_fixture.build_fixture`` proves the kubeconfig context names the disposable acceptance run, and every
forward here uses that same context, so a forward cannot reach another cluster. The forward only opens a TCP
path; it mutates nothing. Local ports are chosen free at start, the output is drained so a long run never blocks
kubectl on a full pipe, and ``close`` always terminates the process.
"""

from __future__ import annotations

import socket
import subprocess
import sys
import threading
import time
from typing import Any

sys.dont_write_bytecode = True


class PortForwardError(RuntimeError):
    """The port-forward process failed to bind every requested port."""


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def forward_command(base: list[str], namespace: str, target: str, mapping: dict[int, int]) -> list[str]:
    """``kubectl ... port-forward TARGET LOCAL:REMOTE...``; ``mapping`` is remote port -> local port."""
    if not mapping:
        raise ValueError("port-forward needs at least one port")
    return [*base, "-n", namespace, "port-forward", "--address", "127.0.0.1", target,
            *[f"{local}:{remote}" for remote, local in sorted(mapping.items())]]


class PortForward:
    """``kubectl port-forward`` to one Pod or Service. Use as a context manager or call ``close`` in ``finally``."""

    def __init__(self, fixture: Any, namespace: str, target: str, *remote_ports: int):
        self.fixture, self.namespace, self.target = fixture, namespace, target
        self.remote_ports = tuple(remote_ports)
        self.local: dict[int, int] = {}
        self.process: subprocess.Popen | None = None
        self._bound: set[int] = set()
        self._lines: list[str] = []
        self._lock = threading.Lock()
        self._reader: threading.Thread | None = None

    def start(self, timeout: float = 30.0) -> dict[int, int]:
        self.local = {remote: free_port() for remote in self.remote_ports}
        command = forward_command(self.fixture.base(), self.namespace, self.target, self.local)
        self.process = subprocess.Popen(command, env=self.fixture.aws_env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                        text=True, encoding="utf-8", errors="replace")

        def drain() -> None:
            assert self.process is not None and self.process.stdout is not None
            for line in self.process.stdout:
                with self._lock:
                    self._lines = (self._lines + [line.strip()])[-20:]
                    for remote, local in self.local.items():
                        if line.startswith(f"Forwarding from 127.0.0.1:{local} ->"):
                            self._bound.add(remote)

        self._reader = threading.Thread(target=drain, daemon=True)
        self._reader.start()
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            with self._lock:
                if self._bound == set(self.remote_ports):
                    return dict(self.local)
            if self.process.poll() is not None:
                break
            time.sleep(0.05)
        with self._lock:
            tail = list(self._lines)
        self.close()
        raise PortForwardError(f"port-forward {self.target} in {self.namespace} did not bind {self.remote_ports}: {tail}")

    def alive(self) -> bool:
        return self.process is not None and self.process.poll() is None

    def close(self) -> None:
        process, self.process = self.process, None
        if process is None:
            return
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(10)
        if self._reader is not None:
            self._reader.join(5)
        if process.stdout is not None:
            process.stdout.close()

    def __enter__(self) -> "PortForward":
        self.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()
