"""Validating admission webhook that delays selected Pod DELETE requests.

The rollout controller EKS acceptance registers this webhook with a timeout shorter than
``DELAY_SECONDS`` and ``failurePolicy: Fail``. The Kubernetes API server then returns a real
webhook-timeout error to the controller's DELETE, and the Pod is not deleted. The webhook
itself always answers ``allowed: true``; selection (Pod, user) is done by the registration.
"""

from __future__ import annotations

import json
import logging
import os
import ssl
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit

logger = logging.getLogger("delete_delay_webhook")


def admission_response(review: dict) -> dict:
    request = review.get("request") or {}
    uid = request.get("uid")
    if not uid:
        raise ValueError("AdmissionReview request.uid is required")
    return {"apiVersion": review.get("apiVersion", "admission.k8s.io/v1"), "kind": "AdmissionReview",
            "response": {"uid": uid, "allowed": True}}


class WebhookHandler(BaseHTTPRequestHandler):
    server: WebhookServer

    def log_message(self, format: str, *args: object) -> None:
        logger.info("%s - " + format, self.address_string(), *args)

    def do_GET(self) -> None:
        path = urlsplit(self.path).path
        if path == "/healthz":
            self.reply(200, {"status": "ok"})
        elif path == "/requests":
            with self.server.lock:
                self.reply(200, {"requests": list(self.server.seen)})
        else:
            self.reply(404, {"error": "not_found"})

    def do_POST(self) -> None:
        body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        # The API server calls the registered path with a query, for example /validate?timeout=10s.
        path = urlsplit(self.path).path
        if path == "/control":
            delay = float(json.loads(body)["delaySeconds"])
            if delay < 0:
                self.reply(400, {"error": "delaySeconds must not be negative"})
                return
            with self.server.lock:
                self.server.delay_seconds = delay
            self.reply(200, {"delaySeconds": delay})
            return
        if path != "/validate":
            self.reply(404, {"error": "not_found"})
            return
        review = json.loads(body)
        request = review.get("request") or {}
        old = (request.get("oldObject") or {}).get("metadata") or {}
        entry = {"receivedAt": time.time(), "uid": request.get("uid"), "operation": request.get("operation"),
                 "name": request.get("name"), "user": (request.get("userInfo") or {}).get("username"),
                 "preconditions": (request.get("options") or {}).get("preconditions"),
                 "oldUid": old.get("uid"), "oldResourceVersion": old.get("resourceVersion")}
        with self.server.lock:
            self.server.seen.append(entry)
            delay = self.server.delay_seconds
        time.sleep(delay)
        self.reply(200, admission_response(review))

    def reply(self, status: int, data: object) -> None:
        body = json.dumps(data).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


class WebhookServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, address: tuple[str, int], delay_seconds: float):
        super().__init__(address, WebhookHandler)
        self.delay_seconds = delay_seconds
        self.lock = threading.Lock()
        self.seen: list[dict] = []


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    server = WebhookServer(("0.0.0.0", int(os.environ.get("WEBHOOK_PORT", "8443"))), float(os.environ.get("DELAY_SECONDS", "15")))
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(os.environ["TLS_CERT_FILE"], os.environ["TLS_KEY_FILE"])
    server.socket = context.wrap_socket(server.socket, server_side=True)
    logger.info("delete delay webhook listening with delay %.1fs", server.delay_seconds)
    server.serve_forever()


if __name__ == "__main__":
    main()
