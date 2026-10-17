from __future__ import annotations

import copy
import importlib.util
import json
import sys
import unittest
from pathlib import Path

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parents[1]
DASHBOARD_DIR = ROOT / "tests" / "chaos" / "eks-dashboard"
CHAOS_DIR = ROOT / "tests" / "chaos"
for directory in (str(DASHBOARD_DIR), str(CHAOS_DIR)):
    if directory not in sys.path:
        sys.path.insert(0, directory)

import dashboard_judge as judge  # noqa: E402
import eks_port_forward  # noqa: E402

spec = importlib.util.spec_from_file_location("eks_dashboard_main", DASHBOARD_DIR / "main.py")
dashboard_main = importlib.util.module_from_spec(spec)
spec.loader.exec_module(dashboard_main)

DASHBOARD = json.loads((ROOT / "deploy/observability/grafana/dashboards/narumq-operations.json").read_text(encoding="utf-8"))
EXPECTED = {"1": "narumq-0", "2": "narumq-1", "3": "narumq-2"}
NOW_MS = 1_800_000_000_000


def frame(*fields):
    """fields: (name, type, values[, labels])"""
    return {"schema": {"fields": [{"name": f[0], "type": f[1], "labels": f[3] if len(f) > 3 else None} for f in fields]},
            "data": {"values": [f[2] for f in fields]}}


def topology_result(**overrides):
    rows = {"Node": ["1", "2", "3"], "Membership": ["VOTER"] * 3, "Query status": ["OK"] * 3, "Query error": [None] * 3,
            "Observed at": ["2026-10-17T00:00:00Z"] * 3, "Role": ["LEADER", "FOLLOWER", "FOLLOWER"], "Version": ["0.0.1"] * 3,
            "Uptime (ms)": [1000, 2000, 3000], "State": ["READY"] * 3, "Ready": [True] * 3,
            "Readiness reason": [None] * 3, "Replication": ["HEALTHY"] * 3}
    rows.update(overrides)
    return {"status": 200, "frames": [frame(*[(name, "string", values) for name, values in rows.items()])]}


def series_frame(values=(3.0, 3.0)):
    return frame(("Time", "time", [1, 2][: len(values)]), ("Connections", "number", list(values)))


def panel_response(panel, **frames):
    return {"results": {t["refId"]: {"status": 200, "frames": frames.get(t["refId"], [series_frame()])} for t in panel["targets"]}}


def target(node_id, health="up", pod=None, error="", job="narumq-management"):
    pod = pod or EXPECTED[node_id]
    return {"labels": {"job": job, "node_id": node_id}, "health": health, "lastError": error,
            "scrapeUrl": f"http://{pod}.narumq-headless.narumq.svc.cluster.local:8080/metrics"}


def pod_item(name, zone_node, ready=True, deleting=None):
    return {"metadata": {"name": name, "deletionTimestamp": deleting}, "spec": {"nodeName": zone_node},
            "status": {"conditions": [{"type": "Ready", "status": "True" if ready else "False"}],
                       "containerStatuses": [{"name": "narumq", "ready": ready}]}}


class QueryConstruction(unittest.TestCase):
    def test_panel_seven_queries_are_the_provisioned_targets_scoped_to_a_node(self):
        panel = judge.find_panel(DASHBOARD, 7)
        path, body = judge.build_query_request(panel, "2", NOW_MS)
        self.assertEqual(path, "/api/ds/query?ds_type=prometheus")
        self.assertEqual([q["refId"] for q in body["queries"]], ["A", "B", "C"])
        self.assertEqual(body["queries"][0]["expr"], 'sum(narumq_connections_active{node_id=~"2"})')
        self.assertTrue(all("$node" not in q["expr"] and q["datasource"]["uid"] == "narumq-prometheus" for q in body["queries"]))
        self.assertEqual((body["from"], body["to"]), (str(NOW_MS - 300_000), str(NOW_MS)))

    def test_topology_panel_uses_the_management_datasource_without_prometheus_param(self):
        panel = judge.find_panel(DASHBOARD, 6)
        path, body = judge.build_query_request(panel, ".*", NOW_MS)
        query = body["queries"][0]
        self.assertEqual(path, "/api/ds/query")
        self.assertEqual((query["datasource"]["uid"], query["url"], query["url_options"], query["parser"]),
                         ("narumq-management", "/api/v1/nodes?limit=1000", {"method": "GET"}, "backend"))
        self.assertIn("Node", [c["text"] for c in query["columns"]])

    def test_unknown_datasource_and_missing_panel_raise(self):
        panel = copy.deepcopy(judge.find_panel(DASHBOARD, 7))
        panel["targets"][0]["datasource"]["uid"] = "other"
        with self.assertRaises(ValueError):
            judge.build_query_request(panel, ".*", NOW_MS)
        with self.assertRaises(LookupError):
            judge.find_panel(DASHBOARD, 999)

    def test_query_problems(self):
        panel = judge.find_panel(DASHBOARD, 7)
        self.assertEqual(judge.query_problems(panel, panel_response(panel)), [])
        self.assertIn("query B: no result", judge.query_problems(panel, {"results": {"A": {"status": 200}, "C": {"status": 200}}}))
        failed = {"results": {"A": {"status": 500, "error": "boom"}, "B": {"status": 200}, "C": {"status": 200}}}
        self.assertTrue(any("status=500" in p for p in judge.query_problems(panel, failed)))
        errored = {"results": {t["refId"]: {"status": 200, "error": "bad"} for t in panel["targets"]}}
        self.assertEqual(len(judge.query_problems(panel, errored)), 3)


class PrometheusJudgment(unittest.TestCase):
    def test_all_three_targets_up_passes(self):
        targets = [target(n) for n in EXPECTED] + [target("1", job="prometheus")]
        self.assertEqual(judge.judge_prometheus_targets(targets, EXPECTED), [])

    def test_down_target_fails(self):
        problems = judge.judge_prometheus_targets([target("1"), target("2", "down", error="x509"), target("3")], EXPECTED)
        self.assertTrue(any("node_id=2" in p and "down" in p for p in problems))

    def test_missing_duplicate_extra_and_misrouted_targets_fail(self):
        self.assertTrue(any("node_id=3: 0" in p for p in judge.judge_prometheus_targets([target("1"), target("2")], EXPECTED)))
        self.assertTrue(any("node_id=1: 2" in p for p in judge.judge_prometheus_targets([target(n) for n in EXPECTED] + [target("1")], EXPECTED)))
        extra = {"labels": {"job": "narumq-management", "node_id": "9"}, "health": "up", "lastError": "", "scrapeUrl": "http://x:8080/m"}
        self.assertTrue(any("unexpected" in p for p in judge.judge_prometheus_targets([target(n) for n in EXPECTED] + [extra], EXPECTED)))
        wrong = judge.judge_prometheus_targets([target("1", pod="narumq-2"), target("2"), target("3")], EXPECTED)
        self.assertTrue(any("node_id=1" in p and "narumq-0" in p for p in wrong))

    def test_unknown_health_and_last_error_fail(self):
        self.assertTrue(judge.judge_prometheus_targets([target("1", "unknown"), target("2"), target("3")], EXPECTED))
        self.assertTrue(judge.judge_prometheus_targets([target("1", error="boom"), target("2"), target("3")], EXPECTED))

    def test_node_series(self):
        series = {"status": "success", "data": {"result": [{"metric": {"node_id": n}} for n in "123"]}}
        self.assertEqual(judge.judge_node_series(series, {"1", "2", "3"}, "m"), [])
        series["data"]["result"].pop()
        self.assertIn("m: no series for node_id ['3']", judge.judge_node_series(series, {"1", "2", "3"}, "m"))
        self.assertTrue(judge.judge_node_series({"status": "error"}, {"1"}, "m"))


class DatasourceJudgment(unittest.TestCase):
    LISTING = [{"uid": "narumq-prometheus", "type": "prometheus"}, {"uid": "narumq-management", "type": "yesoreyeram-infinity-datasource"}]
    OK = {"narumq-prometheus": {"status": "OK"}, "narumq-management": {"status": "OK"}}

    def test_both_ok_passes(self):
        self.assertEqual(judge.judge_datasources(self.LISTING, self.OK), [])

    def test_unhealthy_missing_or_extra_datasource_fails(self):
        bad = {**self.OK, "narumq-management": {"status": "ERROR", "message": "401"}}
        self.assertTrue(any("narumq-management" in p for p in judge.judge_datasources(self.LISTING, bad)))
        self.assertTrue(judge.judge_datasources(self.LISTING[:1], self.OK))
        self.assertTrue(judge.judge_datasources(self.LISTING + [{"uid": "x", "type": "loki"}], self.OK))
        self.assertTrue(judge.judge_datasources(self.LISTING, {"narumq-prometheus": {"status": "OK"}}))


class TopologyJudgment(unittest.TestCase):
    def test_healthy_three_node_topology_passes(self):
        self.assertEqual(judge.judge_topology(topology_result(), {"1", "2", "3"}), [])

    def test_wrong_state_or_not_ready_fails(self):
        self.assertTrue(any("state='SAFE_TO_STOP'" in p for p in judge.judge_topology(
            topology_result(State=["READY", "READY", "SAFE_TO_STOP"], Ready=[True, True, False]), {"1", "2", "3"})))
        self.assertTrue(judge.judge_topology(topology_result(Ready=[True, "false", True]), {"1", "2", "3"}))

    def test_missing_node_fails(self):
        two = topology_result(**{k: v[:2] for k, v in {
            "Node": ["1", "2", "3"], "Membership": ["VOTER"] * 3, "Query status": ["OK"] * 3, "Query error": [None] * 3,
            "Observed at": ["t"] * 3, "Role": ["LEADER", "FOLLOWER", "FOLLOWER"], "Version": ["v"] * 3, "Uptime (ms)": [1, 2, 3],
            "State": ["READY"] * 3, "Ready": [True] * 3, "Readiness reason": [None] * 3, "Replication": ["HEALTHY"] * 3}.items()})
        self.assertTrue(any("2 values, want 3" in p for p in judge.judge_topology(two, {"1", "2", "3"})))
        self.assertTrue(any("node ids" in p for p in judge.judge_topology(topology_result(Node=["1", "2", "7"]), {"1", "2", "3"})))
        self.assertTrue(any("absent" in p for p in judge.judge_topology({"frames": []}, {"1", "2", "3"})))

    def test_leader_count_membership_query_status_and_uptime(self):
        self.assertTrue(any("0 LEADER" in p for p in judge.judge_topology(topology_result(Role=["FOLLOWER"] * 3), {"1", "2", "3"})))
        self.assertTrue(any("2 LEADER" in p for p in judge.judge_topology(topology_result(Role=["LEADER", "LEADER", "FOLLOWER"]), {"1", "2", "3"})))
        self.assertTrue(judge.judge_topology(topology_result(Membership=["VOTER", "LEARNER", "VOTER"]), {"1", "2", "3"}))
        self.assertTrue(judge.judge_topology(topology_result(**{"Query status": ["OK", "UNREACHABLE", "OK"]}), {"1", "2", "3"}))
        self.assertTrue(judge.judge_topology(topology_result(**{"Uptime (ms)": [1000, 0, 3000]}), {"1", "2", "3"}))
        self.assertTrue(judge.judge_topology(topology_result(**{"Uptime (ms)": [1000, None, 3000]}), {"1", "2", "3"}))
        self.assertTrue(judge.judge_topology(topology_result(Version=["0.0.1", "", "0.0.1"]), {"1", "2", "3"}))
        self.assertTrue(judge.judge_topology(topology_result(**{"Query error": [None, "node_unreachable", None]}), {"1", "2", "3"}))


class TimeSeriesJudgment(unittest.TestCase):
    PANEL = judge.find_panel(DASHBOARD, 7)

    def test_frames_with_time_and_numeric_samples_pass(self):
        self.assertEqual(judge.judge_timeseries(self.PANEL, panel_response(self.PANEL), "node=1"), [])

    def test_empty_frame_fails(self):
        empty = {"results": {t["refId"]: {"status": 200, "frames": []} for t in self.PANEL["targets"]}}
        problems = judge.judge_timeseries(self.PANEL, empty, "node=1")
        self.assertEqual(len([p for p in problems if "empty result" in p]), 3)
        hollow = panel_response(self.PANEL, A=[frame(("Time", "time", []), ("Connections", "number", []))])
        problems = judge.judge_timeseries(self.PANEL, hollow, "node=1")
        self.assertTrue(any("no time field with samples" in p for p in problems))
        self.assertTrue(any("has no sample" in p for p in problems))

    def test_frame_without_numeric_or_time_field_fails(self):
        only_time = panel_response(self.PANEL, B=[frame(("Time", "time", [1, 2]))])
        self.assertTrue(any("no numeric field" in p for p in judge.judge_timeseries(self.PANEL, only_time, "s")))
        no_time = panel_response(self.PANEL, C=[frame(("Connections", "number", [1, 2]))])
        self.assertTrue(any("no time field" in p for p in judge.judge_timeseries(self.PANEL, no_time, "s")))

    def test_non_numeric_values_do_not_count_as_samples(self):
        text = panel_response(self.PANEL, A=[frame(("Time", "time", [1]), ("Connections", "number", [None]))])
        self.assertTrue(any("has no sample" in p for p in judge.judge_timeseries(self.PANEL, text, "s")))

    def test_query_error_fails(self):
        response = panel_response(self.PANEL)
        response["results"]["A"]["status"] = 400
        response["results"]["A"]["error"] = "parse error"
        self.assertTrue(any("status=400" in p for p in judge.judge_timeseries(self.PANEL, response, "s")))

    def test_summary_reports_samples(self):
        summary = judge.timeseries_summary(panel_response(self.PANEL))
        self.assertEqual(summary["A"]["frames"][0]["numericSamples"], 2)
        self.assertEqual(summary["A"]["frames"][0]["last"], 3.0)


class KubernetesCrossCheck(unittest.TestCase):
    SLOTS = {"slot-0": "1", "slot-1": "2", "slot-2": "3"}
    ZONES = {"w0": "a", "w1": "b", "w2": "c"}

    def pods(self, **kw):
        return [pod_item(f"narumq-{i}", f"w{i}", **kw.get(f"narumq-{i}", {})) for i in range(3)]

    def test_three_ready_pods_in_three_zones_map_to_node_ids(self):
        problems, mapping = judge.judge_pods(self.pods(), self.ZONES, self.SLOTS)
        self.assertEqual((problems, mapping), ([], EXPECTED))

    def test_shared_zone_not_ready_missing_pod_and_missing_slot_fail(self):
        problems, _ = judge.judge_pods(self.pods(), {**self.ZONES, "w2": "a"}, self.SLOTS)
        self.assertTrue(any("3 distinct zones" in p for p in problems))
        problems, _ = judge.judge_pods(self.pods(**{"narumq-1": {"ready": False}}), self.ZONES, self.SLOTS)
        self.assertTrue(any("narumq-1: not Ready" in p for p in problems))
        problems, _ = judge.judge_pods(self.pods(**{"narumq-1": {"deleting": "2026-10-17T00:00:00Z"}}), self.ZONES, self.SLOTS)
        self.assertTrue(any("narumq-1: not Ready" in p for p in problems))
        problems, _ = judge.judge_pods(self.pods()[:2], self.ZONES, self.SLOTS)
        self.assertTrue(any("2 broker Pods" in p for p in problems))
        problems, _ = judge.judge_pods(self.pods(), self.ZONES, {"slot-0": "1", "slot-1": "2"})
        self.assertTrue(any("slot-2" in p for p in problems))


class HarnessSupport(unittest.TestCase):
    def test_settled_retries_until_the_problems_clear(self):
        calls = []

        def observe():
            calls.append(1)
            return len(calls)

        result = dashboard_main.settled(observe, lambda n: [] if n >= 3 else ["not yet"], timeout=30, interval=0)
        self.assertEqual((result["problems"], result["attempts"], result["observed"]), ([], 3, 3))

    def test_settled_returns_the_last_problems_after_the_timeout(self):
        result = dashboard_main.settled(lambda: "x", lambda _: ["still down"], timeout=0, interval=0)
        self.assertEqual((result["problems"], result["attempts"]), (["still down"], 1))

    def test_settled_records_an_observation_failure_as_a_problem(self):
        def broken():
            raise ConnectionError("refused")

        result = dashboard_main.settled(broken, lambda _: [], timeout=0, interval=0)
        self.assertEqual(result["observed"], None)
        self.assertTrue(result["problems"][0].startswith("observation failed: ConnectionError"))

    def test_a_missing_prerequisite_is_raised_not_retried(self):
        def missing():
            raise dashboard_main.PrerequisiteError("login rejected")

        with self.assertRaises(dashboard_main.PrerequisiteError):
            dashboard_main.settled(missing, lambda _: [], timeout=30, interval=0)

    def test_require_stack_raises_for_a_missing_namespace_or_deployment(self):
        class Fixture:
            def __init__(self, namespace_present, deployments):
                self.namespace_present, self.deployments = namespace_present, deployments

            def kubectl(self, *args, namespace=None):
                if args[1] == "namespace":
                    return "namespace/x" if self.namespace_present else ""
                return json.dumps(self.deployments[args[2]]) if args[2] in self.deployments else ""

        up = {"status": {"replicas": 1, "availableReplicas": 1}}
        with self.assertRaises(dashboard_main.PrerequisiteError):
            dashboard_main.require_stack(Fixture(False, {}), "ns")
        with self.assertRaises(dashboard_main.PrerequisiteError):
            dashboard_main.require_stack(Fixture(True, {"prometheus": up}), "ns")
        with self.assertRaises(dashboard_main.PrerequisiteError):
            dashboard_main.require_stack(Fixture(True, {"prometheus": up, "grafana": {"status": {"replicas": 1}}}), "ns")
        self.assertEqual(sorted(dashboard_main.require_stack(Fixture(True, {"prometheus": up, "grafana": up}), "ns")), ["grafana", "prometheus"])

    def test_forward_command(self):
        command = eks_port_forward.forward_command(["kubectl", "--context", "run"], "ns", "svc/grafana", {3000: 41000})
        self.assertEqual(command, ["kubectl", "--context", "run", "-n", "ns", "port-forward", "--address", "127.0.0.1", "svc/grafana", "41000:3000"])
        with self.assertRaises(ValueError):
            eks_port_forward.forward_command([], "ns", "svc/x", {})
        self.assertGreater(eks_port_forward.free_port(), 0)


if __name__ == "__main__":
    unittest.main()
