"""Controller-only fault proxy for the rollout controller EKS acceptance matrix.

The proxy sits between the rollout controller and the real broker management Service.
Every fault is derived from a real broker response or a real broker request:

- ``postParams`` merges fixed drain parameters (for example a short ``timeoutMillis``) into
  the controller's drain POST body before it reaches the real broker. Keep the value fixed for
  one drain attempt: the broker rejects an idempotent replay whose parameters differ.
- ``getMode`` alters only drain GET responses. ``incomplete`` and ``malformed`` start from the
  real broker observation and keep its operation ID, node ID and timestamps. ``hang`` delays
  past the controller's 5 s HTTP timeout without calling the broker. ``unavailable`` returns 503.
- ``getDelaySeconds`` delays a relayed drain GET response (status CAS race window).
  ``postResponseDelaySeconds`` relays a drain POST to the real broker and then withholds the
  response past the controller's timeout, so the broker starts the drain but the result is unknown.
  ``delayClients`` limits both delays to the listed client IPs (for example the current leader).

All other paths, including ``/api/v1/cluster/features`` and drain DELETE, are relayed unchanged.
``GET /control/requests?since=<unix seconds>`` returns the recorded requests received at or after ``since``.
"""

from __future__ import annotations

import copy
import json
import logging
import os
import threading
import time
import urllib.error
import urllib.request
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit
from typing import Any

logger = logging.getLogger("rollout_fault_proxy")

GET_MODES = ("relay", "incomplete", "malformed", "hang", "unavailable")
HOP_BY_HOP = {"connection", "keep-alive", "proxy-authenticate", "proxy-authorization", "te", "trailer",
              "transfer-encoding", "upgrade", "host", "content-length"}
# A controller that re-reconciles on its own status writes can poll several times per second;
# keep a full scenario window (tens of minutes) rather than the last few minutes.
MAX_REQUEST_LOG = 200_000


def is_drain_path(path: str) -> bool:
    parts = path.split("?", 1)[0].strip("/").split("/")
    return len(parts) == 5 and parts[:3] == ["api", "v1", "nodes"] and parts[4] == "drain"


def safe_looking(observation: dict[str, Any]) -> dict[str, Any]:
    """Return a copy of a real observation rewritten to claim a completed safe drain."""
    forged = copy.deepcopy(observation)
    forged["state"], forged["phase"], forged["safeToStop"], forged["failure"] = "SAFE_TO_STOP", "COMPLETED", True, None
    forged["connections"] = {"initial": forged.get("connections", {}).get("initial", 0), "remaining": 0}
    forged["sessions"] = {"stateReadyForTakeover": 0, "takenOver": 0, "failed": 0}
    forged["replication"] = {"pendingWal": 0, "underReplicated": 0}
    return forged


def mutate_get(mode: str, envelope: dict[str, Any]) -> dict[str, Any]:
    """Apply a GET fault to a real ``{"data": observation}`` envelope."""
    observation = envelope.get("data")
    if not isinstance(observation, dict):
        raise ValueError("broker drain response is not a data envelope")
    forged = safe_looking(observation)
    if mode == "incomplete":
        forged["connections"]["remaining"] = 1
    elif mode == "malformed":
        del forged["replication"]
    else:
        raise ValueError(f"mode {mode!r} does not mutate a response")
    return {"data": forged}


class FaultProxyHandler(BaseHTTPRequestHandler):
    server: FaultProxyServer
    protocol_version = "HTTP/1.1"

    def log_message(self, format: str, *args: object) -> None:
        logger.info("%s - " + format, self.address_string(), *args)

    def do_GET(self) -> None:
        self.handle_request("GET")

    def do_POST(self) -> None:
        self.handle_request("POST")

    def do_DELETE(self) -> None:
        self.handle_request("DELETE")

    def handle_request(self, method: str) -> None:
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length) if length > 0 else b""
        if self.path == "/healthz":
            self.send_json(200, {"status": "ok"})
            return
        if self.path.startswith("/control/"):
            self.handle_control(method, body)
            return
        config = self.server.snapshot()
        action = "relay"
        if is_drain_path(self.path) and method == "POST" and config["postParams"]:
            payload = json.loads(body.decode("utf-8") or "{}")
            payload.update(config["postParams"])
            body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
            action = "post_params"
        if is_drain_path(self.path) and method == "GET" and config["getMode"] != "relay":
            action = config["getMode"]
        self.server.record(self.client_address[0], method, self.path, action, self.headers.get("Idempotency-Key"))
        if action == "hang":
            time.sleep(config["hangSeconds"])
            self.send_json(504, {"error": "fault_injected_hang"})
            return
        if action == "unavailable":
            self.send_json(503, {"error": "fault_injected_unavailable"})
            return
        status, headers, response = self.relay(method, body)
        if action in ("incomplete", "malformed") and status == 200:
            response = json.dumps(mutate_get(action, json.loads(response))).encode("utf-8")
            headers = {"Content-Type": "application/json"}
        delay = 0.0
        if is_drain_path(self.path) and method == "GET":
            delay = config["getDelaySeconds"]
        elif is_drain_path(self.path) and method == "POST":
            delay = config["postResponseDelaySeconds"]
        clients = config["delayClients"]
        if delay > 0 and (clients is None or self.client_address[0] in clients):
            # The broker has already executed the request; only the controller's view is delayed.
            self.server.record(self.client_address[0], method, self.path, "response_delayed", self.headers.get("Idempotency-Key"))
            time.sleep(delay)
        self.send_raw(status, headers, response)

    def handle_control(self, method: str, body: bytes) -> None:
        url = urlsplit(self.path)
        if url.path == "/control/state" and method == "GET":
            self.send_json(200, self.server.snapshot())
        elif url.path == "/control/state" and method == "POST":
            try:
                self.server.configure(json.loads(body.decode("utf-8")))
            except (ValueError, TypeError) as error:
                self.send_json(400, {"error": "invalid_control", "message": str(error)})
                return
            self.send_json(200, self.server.snapshot())
        elif url.path == "/control/requests" and method == "GET":
            try:
                since = float(parse_qs(url.query).get("since", ["0"])[0])
            except ValueError:
                self.send_json(400, {"error": "invalid_control", "message": "since must be unix seconds"})
                return
            self.send_json(200, {"requests": self.server.requests(since)})
        else:
            self.send_json(404, {"error": "unknown_control"})

    def relay(self, method: str, body: bytes) -> tuple[int, dict[str, str], bytes]:
        request = urllib.request.Request(self.server.upstream_base_url.rstrip("/") + self.path,
                                         data=body if method != "GET" else None, method=method)
        for name, value in self.headers.items():
            if name.lower() not in HOP_BY_HOP:
                request.add_header(name, value)
        try:
            with urllib.request.urlopen(request, timeout=self.server.upstream_timeout) as response:
                return response.status, {"Content-Type": response.headers.get("Content-Type", "application/json")}, response.read()
        except urllib.error.HTTPError as error:
            return error.code, {"Content-Type": error.headers.get("Content-Type", "application/json")}, error.read()
        except (urllib.error.URLError, OSError) as error:
            logger.error("upstream relay failed for %s %s: %s", method, self.path, error)
            return 502, {"Content-Type": "application/json"}, b'{"error":"upstream_unreachable"}'

    def send_json(self, status: int, data: object) -> None:
        self.send_raw(status, {"Content-Type": "application/json"}, json.dumps(data).encode("utf-8"))

    def send_raw(self, status: int, headers: dict[str, str], body: bytes) -> None:
        try:
            self.send_response(status)
            for name, value in headers.items():
                self.send_header(name, value)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except ConnectionError:
            logger.info("client closed before %s %s response was sent", self.command, self.path)


class FaultProxyServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, host: str, port: int, upstream_base_url: str, upstream_timeout: float = 10.0):
        super().__init__((host, port), FaultProxyHandler)
        self.upstream_base_url = upstream_base_url
        self.upstream_timeout = upstream_timeout
        self._lock = threading.Lock()
        self._config: dict[str, Any] = {"getMode": "relay", "hangSeconds": 8.0, "postParams": None,
                                        "getDelaySeconds": 0.0, "postResponseDelaySeconds": 0.0, "delayClients": None}
        self._requests: deque[dict[str, Any]] = deque(maxlen=MAX_REQUEST_LOG)

    def configure(self, update: dict[str, Any]) -> None:
        if not isinstance(update, dict) or not set(update) <= set(self._config):
            raise ValueError(f"control accepts only {sorted(self._config)}")
        merged = {**self.snapshot(), **update}
        if merged["getMode"] not in GET_MODES:
            raise ValueError(f"unknown getMode {merged['getMode']!r}")
        merged["hangSeconds"] = float(merged["hangSeconds"])
        if merged["hangSeconds"] <= 0:
            raise ValueError("hangSeconds must be positive")
        for key in ("getDelaySeconds", "postResponseDelaySeconds"):
            merged[key] = float(merged[key])
            if merged[key] < 0:
                raise ValueError(f"{key} must not be negative")
        if merged["postParams"] is not None and not isinstance(merged["postParams"], dict):
            raise ValueError("postParams must be an object or null")
        merged["postParams"] = merged["postParams"] or None
        clients = merged["delayClients"]
        if clients is not None and (not isinstance(clients, list) or not all(isinstance(ip, str) and ip for ip in clients)):
            raise ValueError("delayClients must be a list of client IPs or null")
        with self._lock:
            self._config = merged

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return copy.deepcopy(self._config)

    def record(self, client: str, method: str, path: str, action: str, idempotency_key: str | None) -> None:
        entry = {"at": time.time(), "client": client, "method": method, "path": path, "action": action,
                 "idempotencyKey": idempotency_key}
        with self._lock:
            self._requests.append(entry)

    def requests(self, since: float = 0.0) -> list[dict[str, Any]]:
        with self._lock:
            return [entry for entry in self._requests if entry["at"] >= since]


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    server = FaultProxyServer(os.environ.get("PROXY_HOST", "0.0.0.0"), int(os.environ.get("PROXY_PORT", "8080")),
                              os.environ.get("UPSTREAM_URL", "http://narumq-management.narumq.svc.cluster.local:8080"))
    logger.info("fault proxy listening on %s:%d upstream %s", *server.server_address[:2], server.upstream_base_url)
    try:
        server.serve_forever()
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
