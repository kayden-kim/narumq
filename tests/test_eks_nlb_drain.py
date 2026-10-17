from __future__ import annotations

import base64
import importlib.util
import json
import socket
import sys
import tempfile
import threading
import unittest
import unittest.mock
from pathlib import Path

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parents[1]
NLB_DIR = ROOT / "tests" / "chaos" / "eks-nlb-drain"
CHAOS_DIR = ROOT / "tests" / "chaos"
for directory in (str(NLB_DIR), str(CHAOS_DIR)):
    if directory not in sys.path:
        sys.path.insert(0, directory)

import nlb_connect_probe as probe  # noqa: E402
import nlb_drain_judge as judge  # noqa: E402

spec = importlib.util.spec_from_file_location("eks_nlb_drain_main", NLB_DIR / "main.py")
nlb_main = importlib.util.module_from_spec(spec)
spec.loader.exec_module(nlb_main)

T0, ACK, BUDGET = 1000.0, 1000.4, 30.0


def dense(start, end, value, step=0.5):
    """Samples every ``step`` seconds in [start, end]; ``value`` may be a function of the completion time."""
    out, t = [], start
    while t <= end + 1e-9:
        out.append({"start": t - 0.05, "at": t, "value": value(t) if callable(value) else value})
        t += step
    return out


def lifecycle(ready_flip=T0 + 0.2, slice_flip=T0 + 11.5, group_flip=T0 + 12.6, end=T0 + 14.0):
    ready = dense(T0 - 1, end, lambda t: {"status": 503 if t >= ready_flip else 200}, 0.25)
    slices = dense(T0 - 1, end, lambda t: {"present": True, "ready": False if t >= slice_flip else True}, 0.5)
    groups = dense(T0 - 1, end, lambda t: {"state": "draining" if t >= group_flip else "healthy"}, 0.5)
    return ready, slices, groups


def judged(**kw):
    budget = kw.pop("budget", BUDGET)
    ready, slices, groups = lifecycle(**kw)
    return judge.judge_lifecycle(T0, ACK, ready, slices, groups, budget)


def metrics_text(success=0, rejected=None, active=0):
    lines = ["# HELP narumq_connections_total x", f'narumq_connections_total{{protocol="MQTT_3_1_1",transport="tcp",result="success"}} {success}',
             'narumq_connections_total{protocol="MQTT_3_1_1",transport="tcp",result="closed"} 7', f"narumq_connections_active {active}"]
    for reason, value in (rejected or {}).items():
        lines.append(f'narumq_mqtt_connect_rejected_total{{protocol="MQTT_3_1_1",reason="{reason}"}} {value}')
    return "\n".join(lines) + "\n"


def landings(success=0, rejected=None):
    return judge.connect_landings(judge.parse_metrics(metrics_text(success, rejected)))


class FakeBroker:
    """TCP server answering every CONNECT with a fixed reply; ``None`` closes without a CONNACK."""

    def __init__(self, reply):
        self.reply, self.received = reply, []
        self.server = socket.socket()
        self.server.bind(("127.0.0.1", 0))
        self.server.listen(8)
        self.port = self.server.getsockname()[1]
        self.thread = threading.Thread(target=self.serve, daemon=True)
        self.thread.start()

    def serve(self):
        while True:
            try:
                conn, _ = self.server.accept()
            except OSError:
                return
            with conn:
                conn.settimeout(2)
                try:
                    self.received.append(conn.recv(1024))
                    if self.reply is not None:
                        conn.sendall(self.reply)
                except OSError:
                    pass

    def close(self):
        self.server.close()


class PacketCodec(unittest.TestCase):
    def test_connect_without_credentials_is_byte_exact(self):
        self.assertEqual(probe.build_connect("c"), b"\x10\x0d\x00\x04MQTT\x04\x02\x00\x1e\x00\x01c")

    def test_connect_with_credentials_sets_username_and_password_flags(self):
        packet = probe.build_connect("c", "u", "pw")
        self.assertEqual(packet[:1], b"\x10")
        self.assertEqual(packet[2 + 7], 0xC2)  # flags byte after length, protocol name and level
        self.assertTrue(packet.endswith(b"\x00\x01c\x00\x01u\x00\x02pw"))

    def test_password_without_username_is_refused(self):
        with self.assertRaises(ValueError):
            probe.build_connect("c", None, "pw")

    def test_remaining_length_uses_continuation_bytes(self):
        self.assertEqual(probe.encode_length(127), b"\x7f")
        self.assertEqual(probe.encode_length(128), b"\x80\x01")
        self.assertEqual(probe.encode_length(16384), b"\x80\x80\x01")

    def test_connack_classification(self):
        cases = {"20020000": (True, "accepted", 0), "20020100": (True, "accepted", 0), "20020003": (False, "server_unavailable", 3),
                 "20020004": (False, "bad_credentials", 4), "20020005": (False, "not_authorized", 5), "20020002": (False, "identifier_rejected", 2)}
        for text, (accepted, kind, code) in cases.items():
            outcome = probe.classify(bytes.fromhex(text))
            self.assertEqual((outcome["accepted"], outcome["kind"], outcome["returnCode"], outcome["hex"]), (accepted, kind, code, text))

    def test_no_or_malformed_response_is_never_accepted(self):
        self.assertEqual(probe.classify(b"", None)["kind"], "closed_without_connack")
        self.assertEqual(probe.classify(b"", "timeout")["kind"], "timeout")
        self.assertFalse(probe.classify(b"\x30\x00\x00\x00")["accepted"])
        self.assertFalse(probe.classify(b"\x20\x02\x02\x00")["accepted"])  # reserved flag bits set


class ProbeAgainstFakeBroker(unittest.TestCase):
    def outcome(self, reply):
        broker = FakeBroker(reply)
        try:
            return probe.connect_once("127.0.0.1", broker.port, "cid", "u", "pw", 2.0), broker
        finally:
            broker.close()

    def test_server_unavailable_is_the_documented_drain_rejection(self):
        outcome, broker = self.outcome(bytes.fromhex("20020003"))
        self.assertEqual((outcome["kind"], outcome["hex"], outcome["accepted"]), ("server_unavailable", "20020003", False))
        self.assertTrue(broker.received[0].startswith(b"\x10"))

    def test_accepted_connect(self):
        outcome, _ = self.outcome(bytes.fromhex("20020000"))
        self.assertTrue(outcome["accepted"])

    def test_close_without_connack_and_refused_port(self):
        outcome, _ = self.outcome(None)
        self.assertEqual((outcome["accepted"], outcome["kind"]), (False, "closed_without_connack"))
        for error, kind in ((ConnectionRefusedError(), "connection_refused"), (ConnectionResetError(), "connection_reset"),
                            (socket.timeout(), "timeout")):
            with unittest.mock.patch.object(probe.socket, "create_connection", side_effect=error):
                result = probe.connect_once("127.0.0.1", 1, "cid", timeout=1.0)
            self.assertEqual((result["accepted"], result["kind"]), (False, kind))

    def test_batch_runs_the_requested_attempts_with_unique_client_ids(self):
        broker = FakeBroker(bytes.fromhex("20020005"))
        try:
            result = probe.run_batch({"host": "127.0.0.1", "port": broker.port, "count": 6, "prefix": "nlbdrn", "timeout": 2.0, "workers": 3})
        finally:
            broker.close()
        self.assertEqual(result["addresses"], ["127.0.0.1"])
        self.assertEqual(len(result["outcomes"]), 6)
        self.assertEqual(len({o["clientId"] for o in result["outcomes"]}), 6)
        self.assertTrue(all(o["kind"] == "not_authorized" for o in result["outcomes"]))


class LifecycleJudgment(unittest.TestCase):
    def test_past_run_timings_pass_within_a_30s_budget(self):
        problems, measured = judged()
        self.assertEqual(problems, {"ready": [], "endpointSlice": [], "targetGroup": [], "sampling": []})
        self.assertTrue(measured["withinBudget"])
        self.assertLessEqual(measured["T_ready_fail_seconds"], 0.5)
        self.assertAlmostEqual(measured["T1_endpoint_slice_unready_seconds"], 11.5, delta=0.6)  # sampled every 0.5 s
        self.assertAlmostEqual(measured["T2_target_group_draining_seconds"], 12.6, delta=0.6)

    def test_exactly_at_the_budget_passes_and_just_over_fails(self):
        problems, measured = judged(slice_flip=T0 + 30.0, group_flip=T0 + 30.0, end=T0 + 31.0)
        self.assertEqual(measured["T1_endpoint_slice_unready_seconds"], 30.0)
        self.assertEqual((problems["endpointSlice"], problems["targetGroup"]), ([], []))
        problems, measured = judged(slice_flip=T0 + 30.25, group_flip=T0 + 29.0, end=T0 + 31.0)
        self.assertEqual(measured["T1_endpoint_slice_unready_seconds"], 30.5)
        self.assertTrue(any("over the 30.0s" in p for p in problems["endpointSlice"]))
        self.assertEqual(problems["targetGroup"], [])
        self.assertFalse(measured["withinBudget"])
        problems, _ = judged(slice_flip=T0 + 5.0, group_flip=T0 + 30.25, end=T0 + 31.0)
        self.assertTrue(any("over the 30.0s" in p for p in problems["targetGroup"]))

    def test_the_budget_is_the_requested_value(self):
        problems, _ = judged(budget=10.0, slice_flip=T0 + 11.5, group_flip=T0 + 12.6)
        self.assertTrue(problems["endpointSlice"] and problems["targetGroup"])

    def test_wrong_target_group_state_fails(self):
        ready, slices, _ = lifecycle()
        groups = dense(T0 - 1, T0 + 14, {"state": "unhealthy"}, 0.5)
        problems, _ = judge.judge_lifecycle(T0, ACK, ready, slices, groups, BUDGET)
        self.assertTrue(any("never observed" in p for p in problems["targetGroup"]))

    def test_endpoint_that_stays_ready_or_is_absent_fails(self):
        ready, _, groups = lifecycle()
        stays = dense(T0 - 1, T0 + 14, {"present": True, "ready": True}, 0.5)
        absent = dense(T0 - 1, T0 + 14, {"present": False, "ready": None}, 0.5)
        for slices in (stays, absent):
            problems, _ = judge.judge_lifecycle(T0, ACK, ready, slices, groups, BUDGET)
            self.assertTrue(any("never observed" in p for p in problems["endpointSlice"]))

    def test_readiness_that_stays_200_after_the_acknowledgement_fails(self):
        problems, _ = judged(ready_flip=T0 + 5.0)
        self.assertTrue(any("first /health/ready sample after the drain acknowledgement" in p for p in problems["ready"]))
        ready = dense(T0 - 1, T0 + 14, {"status": 200}, 0.25)
        _, slices, groups = lifecycle()
        problems, _ = judge.judge_lifecycle(T0, ACK, ready, slices, groups, BUDGET)
        self.assertTrue(any("never failed" in p for p in problems["ready"]))

    def test_readiness_that_recovers_fails(self):
        ready = dense(T0 - 1, T0 + 14, lambda t: {"status": 200 if T0 + 0.2 > t or t > T0 + 8 else 503}, 0.25)
        _, slices, groups = lifecycle()
        problems, _ = judge.judge_lifecycle(T0, ACK, ready, slices, groups, BUDGET)
        self.assertTrue(any("returned 200 again" in p for p in problems["ready"]))

    def test_connection_errors_do_not_count_as_a_readiness_failure(self):
        ready = dense(T0 - 1, T0 + 14, lambda t: {"status": 200} if t < T0 + 0.2 else {"error": "ConnectionResetError"}, 0.25)
        _, slices, groups = lifecycle()
        problems, _ = judge.judge_lifecycle(T0, ACK, ready, slices, groups, BUDGET)
        self.assertTrue(problems["ready"])

    def test_a_request_in_flight_across_the_acknowledgement_does_not_count(self):
        ready, slices, groups = lifecycle()
        ready.append({"start": ACK - 0.2, "at": ACK + 0.01, "value": {"status": 200}})
        problems, _ = judge.judge_lifecycle(T0, ACK, [s for s in ready if not (ACK - 0.2 < s["start"] < ACK)] + ready[-1:], slices, groups, BUDGET)
        self.assertEqual(problems["ready"], [])

    def test_sparse_sampling_cannot_support_the_judgment(self):
        ready, _, groups = lifecycle()
        sparse = [s for s in dense(T0 - 1, T0 + 14, {"present": True, "ready": False}, 0.5) if s["at"] < T0 + 0.6 or s["at"] > T0 + 9]
        problems, measured = judge.judge_lifecycle(T0, ACK, ready, sparse, groups, BUDGET)
        self.assertTrue(any("EndpointSlice sampling gap" in p for p in problems["sampling"]))
        self.assertGreater(measured["EndpointSlice_max_sample_gap_seconds"], judge.MAX_SAMPLE_GAP_SECONDS)

    def test_missing_samples_fail(self):
        problems, _ = judge.judge_lifecycle(T0, ACK, [], [], [], BUDGET)
        self.assertTrue(problems["ready"] and problems["endpointSlice"] and problems["targetGroup"] and problems["sampling"])


class DirectConnectJudgment(unittest.TestCase):
    def test_rejections_pass(self):
        for kind in ("server_unavailable", "closed_without_connack", "connection_refused", "connection_reset"):
            self.assertEqual(judge.judge_direct_connect({"kind": kind, "accepted": False}, True), [], kind)

    def test_accepted_connection_fails(self):
        self.assertTrue(judge.judge_direct_connect({"kind": "accepted", "accepted": True, "hex": "20020000"}, True))
        self.assertTrue(judge.judge_direct_connect({"kind": "accepted", "accepted": True}, False))

    def test_timeout_proves_nothing(self):
        self.assertTrue(judge.judge_direct_connect({"kind": "timeout", "accepted": False}, False))

    def test_authentication_rejection_does_not_prove_the_drain_when_credentials_were_sent(self):
        self.assertTrue(judge.judge_direct_connect({"kind": "not_authorized", "accepted": False}, True))
        self.assertEqual(judge.judge_direct_connect({"kind": "not_authorized", "accepted": False}, False), [])


class MetricsJudgment(unittest.TestCase):
    def test_parse_and_aggregate(self):
        counters = landings(5, {"server_unavailable": 2, "not_authorized": 1})
        self.assertEqual((counters["success"], counters["attempts"], counters["rejected"]), (5.0, 8.0, {"server_unavailable": 2.0, "not_authorized": 1.0}))

    def test_parse_ignores_comments_and_malformed_lines(self):
        samples = judge.parse_metrics('# TYPE x counter\nbroken line\nnarumq_connections_active 3\nlabelled{a="b\\"c",d="e"} 1.5 1700000000\n')
        self.assertEqual(samples, [("narumq_connections_active", {}, 3.0), ("labelled", {"a": 'b\\"c', "d": "e"}, 1.5)])

    def test_direct_probe_metrics(self):
        before, after = landings(0, {"server_unavailable": 4}), landings(0, {"server_unavailable": 5})
        outcome = {"kind": "server_unavailable"}
        self.assertEqual(judge.judge_direct_metrics(judge.landings_delta(before, after), outcome), [])
        self.assertTrue(judge.judge_direct_metrics(judge.landings_delta(before, before), outcome))
        self.assertTrue(judge.judge_direct_metrics(judge.landings_delta(landings(0), landings(1)), {"kind": "closed_without_connack"}))

    def accepted(self, n):
        return [{"kind": "accepted", "accepted": True} for _ in range(n)]

    def test_authenticated_batch_accepted_by_healthy_nodes_only(self):
        before = {"1": landings(0), "2": landings(0), "3": landings(0)}
        after = {"1": landings(6), "2": landings(4), "3": landings(0)}
        problems, summary = judge.judge_nlb_batch("3", self.accepted(10), before, after, True)
        self.assertEqual(problems, [])
        self.assertEqual((summary["accepted"], summary["healthySuccessDelta"]), (10, 10))
        self.assertTrue(judge.routing_isolation(True, problems, summary)["nlbRoutingIsolated"])

    def test_drained_node_touched_by_the_batch_fails(self):
        before = {"1": landings(0), "2": landings(0), "3": landings(0)}
        for after_three in (landings(1), landings(0, {"server_unavailable": 1})):
            after = {"1": landings(5), "2": landings(4), "3": after_three}
            problems, _ = judge.judge_nlb_batch("3", self.accepted(9), before, after, True)
            self.assertTrue(any("drained node 3 processed" in p for p in problems))

    def test_authenticated_batch_with_no_accepted_connect_fails(self):
        before = {n: landings(0) for n in "123"}
        outcomes = [{"kind": "timeout", "accepted": False}] * 5
        problems, summary = judge.judge_nlb_batch("3", outcomes, before, before, True)
        self.assertTrue(any("routing is not proven" in p for p in problems))
        self.assertFalse(judge.routing_isolation(True, problems, summary)["nlbRoutingIsolated"])

    def test_accepted_connects_must_be_accounted_for_by_healthy_nodes(self):
        before = {n: landings(0) for n in "123"}
        after = {"1": landings(3), "2": landings(3), "3": landings(0)}
        problems, _ = judge.judge_nlb_batch("3", self.accepted(10), before, after, True)
        self.assertTrue(any("success counters grew by 6" in p for p in problems))

    def test_unauthenticated_batch_records_the_boundary_and_is_not_isolated(self):
        before = {n: landings(0) for n in "123"}
        after = {"1": landings(0, {"not_authorized": 8}), "2": landings(0, {"not_authorized": 7}), "3": landings(0)}
        outcomes = [{"kind": "not_authorized", "accepted": False}] * 15
        problems, summary = judge.judge_nlb_batch("3", outcomes, before, after, False)
        self.assertEqual(problems, [])
        isolation = judge.routing_isolation(False, problems, summary)
        self.assertFalse(isolation["nlbRoutingIsolated"])
        self.assertIn("unauthenticated", isolation["routingEvidenceBoundary"])

    def test_missing_drained_node_metrics_and_empty_batch_fail(self):
        problems, _ = judge.judge_nlb_batch("9", [], {"1": landings(0)}, {"1": landings(0)}, False)
        self.assertTrue(any("no metrics for the drained node" in p for p in problems))
        before = {n: landings(0) for n in "123"}
        problems, _ = judge.judge_nlb_batch("3", [], before, before, False)
        self.assertIn("NLB batch produced no outcome", problems)

    def test_pre_drain_batch_needs_an_accepted_connect(self):
        self.assertEqual(judge.judge_pre_drain_batch(self.accepted(1)), [])
        self.assertTrue(judge.judge_pre_drain_batch([{"kind": "not_authorized", "accepted": False}]))


class IdentificationJudgment(unittest.TestCase):
    HOST = "k8s-narumq-abc.elb.ap-northeast-2.amazonaws.com"
    LB = {"DNSName": HOST, "Type": "network", "Scheme": "internal"}

    def test_load_balancer_identity(self):
        self.assertEqual(judge.judge_load_balancer(self.LB, {"elbv2.k8s.aws/cluster": "c"}, self.HOST, "c"), [])
        self.assertTrue(judge.judge_load_balancer(self.LB, {"elbv2.k8s.aws/cluster": "other"}, self.HOST, "c"))
        self.assertTrue(judge.judge_load_balancer(self.LB, {}, self.HOST, "c"))
        self.assertTrue(judge.judge_load_balancer({**self.LB, "Scheme": "internet-facing"}, {"elbv2.k8s.aws/cluster": "c"}, self.HOST, "c"))
        self.assertTrue(judge.judge_load_balancer({**self.LB, "Type": "application"}, {"elbv2.k8s.aws/cluster": "c"}, self.HOST, "c"))
        self.assertTrue(judge.judge_load_balancer({**self.LB, "DNSName": "x"}, {"elbv2.k8s.aws/cluster": "c"}, self.HOST, "c"))

    def test_target_group_selection(self):
        groups = [{"Port": 1883, "Protocol": "TCP", "TargetGroupArn": "a"}, {"Port": 8080, "Protocol": "TCP"}]
        self.assertEqual(judge.pick_target_group(groups)["TargetGroupArn"], "a")
        with self.assertRaises(LookupError):
            judge.pick_target_group(groups[1:])
        with self.assertRaises(LookupError):
            judge.pick_target_group([groups[0], groups[0]])

    def descriptions(self, **states):
        return [{"Target": {"Id": ip, "Port": 1883}, "TargetHealth": {"State": state}} for ip, state in states.items()]

    def test_targets_must_be_exactly_the_pod_ips(self):
        good = self.descriptions(**{"10.0.0.1": "healthy", "10.0.0.2": "healthy", "10.0.0.3": "healthy"})
        ips = {"10.0.0.1", "10.0.0.2", "10.0.0.3"}
        self.assertEqual(judge.judge_targets_are_pods(good, ips), [])
        self.assertTrue(judge.judge_targets_are_pods(good[:2], ips))
        self.assertTrue(judge.judge_targets_are_pods(good, {"10.0.0.1", "10.0.0.2", "10.0.0.9"}))
        self.assertTrue(judge.judge_targets_are_pods([*good, *self.descriptions(**{"10.0.0.4": "healthy"})], ips))
        good[0]["Target"]["Port"] = 9999
        self.assertTrue(any("not on port" in p for p in judge.judge_targets_are_pods(good, ips)))
        self.assertEqual(judge.target_states(good)["10.0.0.2"], "healthy")

    def test_endpoint_lookup_and_baseline(self):
        slices = [{"endpoints": [{"addresses": ["10.0.0.1"], "targetRef": {"name": "narumq-0"}, "conditions": {"ready": True, "serving": True}},
                                 {"addresses": ["10.0.0.2"], "targetRef": {"name": "narumq-1"}, "conditions": {"ready": False, "serving": False}}]}]
        self.assertEqual(judge.endpoint_for_pod(slices, "narumq-1", "10.0.0.2")["ready"], False)
        self.assertEqual(judge.endpoint_for_pod(slices, "x", "10.0.0.1")["ready"], True)
        self.assertEqual(judge.endpoint_for_pod(slices, "narumq-2", "10.0.0.3"), {"present": False, "ready": None, "serving": None, "terminating": None})
        ips = {"narumq-0": "10.0.0.1", "narumq-1": "10.0.0.2"}
        endpoints = {n: judge.endpoint_for_pod(slices, n, ip) for n, ip in ips.items()}
        problems = judge.judge_baseline({"10.0.0.1": "healthy", "10.0.0.2": "unhealthy"}, ips, endpoints, 503)
        self.assertEqual(len(problems), 3)
        ready = {n: {"ready": True} for n in ips}
        self.assertEqual(judge.judge_baseline({"10.0.0.1": "healthy"}, ips, ready, 200), [])

    def test_drain_target_and_restore_action(self):
        self.assertEqual(judge.pick_drain_target({"1": "LEADER", "2": "FOLLOWER", "3": "FOLLOWER"}), "2")
        self.assertEqual(judge.pick_drain_target({"1": "FOLLOWER", "2": "LEADER", "10": "FOLLOWER"}), "1")
        self.assertEqual(judge.pick_drain_target({"1": "LEADER"}), "1")
        with self.assertRaises(LookupError):
            judge.pick_drain_target({"1": None})
        self.assertEqual(judge.restore_action(None), "none")
        self.assertEqual(judge.restore_action({"phase": "COMPLETED", "safeToStop": True}), "restart")
        self.assertEqual(judge.restore_action({"phase": "CANCELLED"}), "none")
        for phase in ("EXCLUDING_TRAFFIC", "DRAINING", "FAILED"):
            self.assertEqual(judge.restore_action({"phase": phase, "safeToStop": False}), "cancel")
        self.assertEqual(judge.restore_action({"phase": "COMPLETED", "safeToStop": False}), "cancel")


class HarnessSupport(unittest.TestCase):
    class Fixture:
        namespace, run_id = "narumq", "run"

        def __init__(self, token=""):
            self.token, self.calls = token, []

        def base(self):
            return ["kubectl"]

        def kubectl(self, *args, **kwargs):
            return self.token

        def run(self, args, stdin=None, timeout=0):
            self.calls.append((args, stdin))

            class Result:
                stdout = json.dumps({"addresses": ["10.0.0.9"], "outcomes": []})

            return Result()

    def test_admin_token_is_decoded_and_a_missing_one_raises(self):
        encoded = base64.b64encode(b"secret-token\n").decode()
        self.assertEqual(nlb_main.read_admin_token(self.Fixture(encoded)), "secret-token")
        with self.assertRaises(nlb_main.PrerequisiteError):
            nlb_main.read_admin_token(self.Fixture(""))

    def test_mqtt_password_is_optional(self):
        with tempfile.TemporaryDirectory() as directory:
            self.assertIsNone(nlb_main.read_mqtt_password(Path(directory)))
            (Path(directory) / "mqtt.password").write_text("pw\n", encoding="utf-8")
            self.assertEqual(nlb_main.read_mqtt_password(Path(directory)), "pw")

    def test_batch_credentials_travel_over_stdin_only(self):
        fixture = self.Fixture()
        result = nlb_main.run_batch(fixture, "nlb.internal", 5, "acceptance-device", "s3cret")
        args, stdin = fixture.calls[0]
        self.assertNotIn("s3cret", " ".join(args))
        self.assertIn("s3cret", stdin)
        self.assertEqual(args[-3:-1], ["python", "-c"])
        self.assertIn("def build_connect", args[-1])
        self.assertEqual(json.loads(stdin)["host"], "nlb.internal")
        self.assertEqual(result["addresses"], ["10.0.0.9"])

    def test_helper_pod_is_not_selected_by_the_nlb_service(self):
        manifest = nlb_main.helper_manifest(self.Fixture(), "python@sha256:" + "a" * 64, "pull")
        self.assertNotIn("app", manifest["metadata"]["labels"])
        self.assertEqual(manifest["spec"]["imagePullSecrets"], [{"name": "pull"}])
        self.assertNotIn("imagePullSecrets", nlb_main.helper_manifest(self.Fixture(), "python@sha256:" + "a" * 64, None)["spec"])

    def test_sampler_records_values_and_errors(self):
        samples, calls = [], []

        def fn():
            calls.append(1)
            if len(calls) == 2:
                raise ConnectionError("boom")
            return {"status": 200}

        sampler = nlb_main.Sampler(fn, samples, workers=2, interval=0.01)
        sampler.start()
        deadline = __import__("time").monotonic() + 5
        while len(samples) < 4 and __import__("time").monotonic() < deadline:
            __import__("time").sleep(0.01)
        sampler.stop()
        self.assertGreaterEqual(len(samples), 4)
        self.assertTrue(all(s["at"] >= s["start"] for s in samples))
        self.assertTrue(any("ConnectionError" in str(s["value"].get("error")) for s in samples))

    def test_scenario_requires_a_helper_image_and_a_sane_budget(self):
        scenario = nlb_main.scenario_nlb_drain.__wrapped__  # call without recording a result
        with self.assertRaises(nlb_main.PrerequisiteError):
            scenario(None, Path("."), "c", "")
        with self.assertRaises(ValueError):
            scenario(None, Path("."), "c", "img", budget_millis=0)
        with self.assertRaises(ValueError):
            scenario(None, Path("."), "c", "img", budget_millis=300_000)

    def test_restore_never_raises_and_reports_the_failure(self):
        class Broken(self.Fixture):
            def kubectl(self, *args, **kwargs):
                raise RuntimeError("kubectl failed")

        report = nlb_main.restore(Broken(), "t", "narumq-1", "uid", "2", "arn", 1.0)
        self.assertFalse(report["ok"])
        self.assertIn("kubectl failed", report["error"])


if __name__ == "__main__":
    unittest.main()
