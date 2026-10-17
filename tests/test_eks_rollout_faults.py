from __future__ import annotations

import json
import sys
import threading
import time
import unittest
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parent / "chaos/eks-rollout-faults"))
import main as runner  # noqa: E402
from delete_delay_webhook import WebhookServer, admission_response  # noqa: E402
from proxy import FaultProxyServer  # noqa: E402

REAL_OBSERVATION = {
    "operationId": "op-real", "nodeId": "1", "state": "DRAINING", "phase": "EXCLUDING_TRAFFIC",
    "startedAt": "2026-10-16T00:00:00Z", "deadlineAt": "2026-10-16T00:30:00Z",
    "connections": {"initial": 4, "remaining": 4}, "sessions": {"stateReadyForTakeover": 0, "takenOver": 0, "failed": 0},
    "replication": {"pendingWal": 2, "underReplicated": 0}, "safeToStop": False, "failure": None,
}


class RecordingBroker(BaseHTTPRequestHandler):
    received: list[dict] = []

    def log_message(self, format, *args):
        pass

    def respond(self, method: str) -> None:
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length) if length else b""
        RecordingBroker.received.append({"method": method, "path": self.path, "headers": self.headers, "body": body})
        if self.path == "/api/v1/cluster/features":
            data = {"activeFeatures": ["a"], "members": []}
        elif self.path == "/api/v1/nodes/1/drain":
            data = dict(REAL_OBSERVATION, phase="CANCELLED" if method == "DELETE" else REAL_OBSERVATION["phase"])
        else:
            self.send_response(404)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        encoded = json.dumps({"data": data}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def do_GET(self):
        self.respond("GET")

    def do_POST(self):
        self.respond("POST")

    def do_DELETE(self):
        self.respond("DELETE")


class FaultProxyTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.broker = ThreadingHTTPServer(("127.0.0.1", 0), RecordingBroker)
        threading.Thread(target=cls.broker.serve_forever, daemon=True).start()
        cls.proxy = FaultProxyServer("127.0.0.1", 0, f"http://127.0.0.1:{cls.broker.server_address[1]}")
        threading.Thread(target=cls.proxy.serve_forever, daemon=True).start()
        cls.url = f"http://127.0.0.1:{cls.proxy.server_address[1]}"

    @classmethod
    def tearDownClass(cls):
        for server in (cls.proxy, cls.broker):
            server.shutdown()
            server.server_close()

    def setUp(self):
        RecordingBroker.received.clear()
        self.control({"getMode": "relay", "hangSeconds": 8, "postParams": None, "getDelaySeconds": 0,
                      "postResponseDelaySeconds": 0, "delayClients": None})

    def control(self, update: dict) -> dict:
        return self.call("POST", "/control/state", json.dumps(update).encode())[1]

    def call(self, method: str, path: str, body: bytes | None = None, headers: dict | None = None):
        request = urllib.request.Request(self.url + path, data=body, method=method, headers=headers or {})
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                return response.status, json.loads(response.read() or b"null")
        except urllib.error.HTTPError as error:
            with error:
                return error.code, json.loads(error.read() or b"null")

    def test_post_params_merge_into_real_request_with_controller_headers(self):
        self.control({"postParams": {"timeoutMillis": 1000, "loadBalancerPropagationDelayMillis": 999}})
        status, _ = self.call("POST", "/api/v1/nodes/1/drain", b"{}",
                              {"X-API-Key": "k", "Idempotency-Key": "rollout-key", "Content-Type": "application/json"})
        self.assertEqual(status, 200)
        upstream = RecordingBroker.received[-1]
        self.assertEqual(json.loads(upstream["body"]), {"timeoutMillis": 1000, "loadBalancerPropagationDelayMillis": 999})
        self.assertEqual(upstream["headers"]["X-API-Key"], "k")
        self.assertEqual(upstream["headers"]["Idempotency-Key"], "rollout-key")
        logged = self.call("GET", "/control/requests")[1]["requests"][-1]
        self.assertEqual((logged["action"], logged["idempotencyKey"], logged["client"]), ("post_params", "rollout-key", "127.0.0.1"))

    def test_post_response_delay_executes_on_broker_before_client_timeout(self):
        self.control({"postResponseDelaySeconds": 1.0})
        request = urllib.request.Request(self.url + "/api/v1/nodes/1/drain", data=b"{}", method="POST",
                                         headers={"Idempotency-Key": "k1"})
        with self.assertRaises(TimeoutError):
            urllib.request.urlopen(request, timeout=0.4)
        self.assertEqual([(r["method"], r["headers"]["Idempotency-Key"]) for r in RecordingBroker.received], [("POST", "k1")])
        actions = [r["action"] for r in self.call("GET", "/control/requests")[1]["requests"] if r["idempotencyKey"] == "k1"]
        self.assertEqual(actions[-2:], ["relay", "response_delayed"])

    def test_delay_limited_to_other_client_answers_this_client_promptly(self):
        self.control({"postResponseDelaySeconds": 2.0, "delayClients": ["10.0.0.9"]})
        started = time.monotonic()
        self.assertEqual(self.call("POST", "/api/v1/nodes/1/drain", b"{}")[0], 200)
        self.assertLess(time.monotonic() - started, 1.5)
        self.assertEqual(self.call("POST", "/control/state", json.dumps({"delayClients": "10.0.0.9"}).encode())[0], 400)

    def test_incomplete_keeps_real_identity_and_withholds_one_connection(self):
        self.control({"getMode": "incomplete"})
        status, envelope = self.call("GET", "/api/v1/nodes/1/drain")
        data = envelope["data"]
        self.assertEqual(status, 200)
        self.assertEqual((data["operationId"], data["nodeId"], data["deadlineAt"]), ("op-real", "1", REAL_OBSERVATION["deadlineAt"]))
        self.assertEqual((data["phase"], data["state"], data["safeToStop"]), ("COMPLETED", "SAFE_TO_STOP", True))
        self.assertEqual(data["connections"]["remaining"], 1)
        self.assertEqual(data["replication"], {"pendingWal": 0, "underReplicated": 0})
        self.assertEqual(RecordingBroker.received[-1]["method"], "GET")

    def test_malformed_is_safe_looking_but_omits_replication_evidence(self):
        self.control({"getMode": "malformed"})
        data = self.call("GET", "/api/v1/nodes/1/drain")[1]["data"]
        self.assertTrue(data["safeToStop"])
        self.assertNotIn("replication", data)
        self.assertEqual(data["operationId"], "op-real")

    def test_hang_and_unavailable_never_reach_broker(self):
        self.control({"getMode": "hang", "hangSeconds": 0.3})
        started = time.monotonic()
        self.assertEqual(self.call("GET", "/api/v1/nodes/1/drain")[0], 504)
        self.assertGreaterEqual(time.monotonic() - started, 0.3)
        self.control({"getMode": "unavailable"})
        self.assertEqual(self.call("GET", "/api/v1/nodes/1/drain")[0], 503)
        self.assertEqual(RecordingBroker.received, [])

    def test_get_faults_do_not_touch_features_delete_or_post(self):
        self.control({"getMode": "unavailable"})
        self.assertEqual(self.call("GET", "/api/v1/cluster/features")[1]["data"]["activeFeatures"], ["a"])
        self.assertEqual(self.call("DELETE", "/api/v1/nodes/1/drain", headers={"Idempotency-Key": "k"})[1]["data"]["phase"], "CANCELLED")
        self.assertEqual(self.call("POST", "/api/v1/nodes/1/drain", b"{}")[0], 200)
        self.assertEqual(json.loads(RecordingBroker.received[-1]["body"]), {})
        self.assertEqual([r["method"] for r in RecordingBroker.received], ["GET", "DELETE", "POST"])

    def test_control_rejects_unknown_mode_and_keeps_previous_state(self):
        self.control({"getMode": "incomplete"})
        status, _ = self.call("POST", "/control/state", json.dumps({"getMode": "drain_failed"}).encode())
        self.assertEqual(status, 400)
        self.assertEqual(self.call("GET", "/control/state")[1]["getMode"], "incomplete")

    def test_request_log_since_filter_returns_only_later_requests(self):
        self.call("GET", "/api/v1/cluster/features")
        boundary = time.time()
        time.sleep(0.05)
        self.call("GET", "/api/v1/nodes/2/drain")
        later = self.call("GET", f"/control/requests?since={boundary}")[1]["requests"]
        self.assertEqual([r["path"] for r in later], ["/api/v1/nodes/2/drain"])
        self.assertEqual(self.call("GET", "/control/requests?since=x")[0], 400)


class FixtureIdentityTests(unittest.TestCase):
    RUN = "eks-rollout-test-run"

    def fixture(self, server="https://fixture.eks", tags=None, label=RUN):
        fx = runner.Fixture(self.RUN, "fixture-cluster", Path("kubectl"), Path("kubeconfig"), Path("."), {})
        tags = {"AcceptanceRun": self.RUN, **runner.REQUIRED_TAGS} if tags is None else tags
        replies = {
            "config": {"contexts": [{"name": self.RUN, "context": {"cluster": "c1"}}], "clusters": [{"name": "c1", "cluster": {"server": server}}]},
            "eks": {"cluster": {"name": "fixture-cluster", "endpoint": "https://fixture.eks", "arn": "arn:fixture", "tags": tags}},
            "namespace": {"metadata": {"labels": {runner.RUN_LABEL: label} if label else {}}},
        }
        calls = []

        def fake_run(args, stdin=None, timeout=120, check=True):
            calls.append(args)
            key = "eks" if args[0] == "aws" else "config" if "config" in args else "namespace" if "namespace" in args else None
            if key is None:
                raise AssertionError(f"unexpected command {args}")
            return type("Result", (), {"stdout": json.dumps(replies[key]), "returncode": 0})()

        fx.run = fake_run
        return fx, calls

    def test_mutation_is_refused_before_identity_verification(self):
        fx, calls = self.fixture()
        with self.assertRaises(runner.FixtureIdentityError):
            fx.mutate("delete", "pod", "narumq-0")
        self.assertEqual(calls, [])

    def test_identity_rejects_other_cluster_wrong_tags_or_unlabelled_namespace(self):
        for case in ({"server": "https://production.eks"}, {"tags": {"AcceptanceRun": self.RUN}},
                     {"tags": {"AcceptanceRun": "another-run", **runner.REQUIRED_TAGS}}, {"label": None}):
            fx, _ = self.fixture(**case)
            with self.subTest(case=case), self.assertRaises(runner.FixtureIdentityError):
                fx.verify_identity()
            self.assertIsNone(fx.identity)

    def test_matching_fixture_is_verified(self):
        fx, _ = self.fixture()
        self.assertEqual(fx.verify_identity()["tags"]["AcceptanceRun"], self.RUN)


class WebhookResponseTests(unittest.TestCase):
    def test_allows_and_echoes_request_uid(self):
        response = admission_response({"apiVersion": "admission.k8s.io/v1", "request": {"uid": "u-1", "operation": "DELETE"}})
        self.assertEqual(response["response"], {"uid": "u-1", "allowed": True})
        self.assertEqual(response["kind"], "AdmissionReview")

    def test_rejects_review_without_uid(self):
        with self.assertRaises(ValueError):
            admission_response({"request": {}})

    def test_validate_path_with_api_server_timeout_query_is_admitted_and_recorded(self):
        server = WebhookServer(("127.0.0.1", 0), 0.0)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        try:
            review = {"apiVersion": "admission.k8s.io/v1", "request": {
                "uid": "u-2", "operation": "DELETE", "name": "narumq-0",
                "options": {"preconditions": {"uid": "pod-uid", "resourceVersion": "7"}},
                "oldObject": {"metadata": {"uid": "pod-uid", "resourceVersion": "8"}}}}
            request = urllib.request.Request(f"http://127.0.0.1:{server.server_address[1]}/validate?timeout=10s",
                                             data=json.dumps(review).encode(), method="POST")
            with urllib.request.urlopen(request, timeout=5) as response:
                self.assertEqual(json.loads(response.read())["response"], {"uid": "u-2", "allowed": True})
            self.assertEqual([(s["preconditions"], s["oldResourceVersion"]) for s in server.seen],
                             [({"uid": "pod-uid", "resourceVersion": "7"}, "8")])
        finally:
            server.shutdown()
            server.server_close()


if __name__ == "__main__":
    unittest.main()
