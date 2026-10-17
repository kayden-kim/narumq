"""Live EKS acceptance of the operations dashboard stack (MGT-SCN-043; UI-01 to UI-05 and UI-10).

The harness is read-only. Against the disposable fixture it checks, in this order:

1. The three broker Pods in namespace ``narumq`` are Ready in three distinct zones and the cluster ConfigMap
   ``slot-N`` entries give their node ids (the same mapping the Prometheus targets file uses).
2. Prometheus (Service ``prometheus:9090``): every ``narumq-management`` node target is ``up`` without
   ``lastError`` and scrapes the Pod behind its ``node_id`` label; ``narumq_connections_active`` has a series per
   node id.
3. Grafana (Service ``grafana:3000``): exactly the two provisioned datasources (``narumq-prometheus``,
   ``narumq-management``) and each reports health ``OK``.
4. Topology: the panel 6 targets are read from the provisioned dashboard ``narumq-operations`` and queried through
   Grafana ``/api/ds/query`` (Infinity datasource over the real Management API ``/api/v1/nodes``). The table must hold
   three ``VOTER`` rows with queryStatus ``OK``, State ``READY``, Ready true, a version, a positive uptime and exactly
   one ``LEADER``.
5. Time series: every panel 7 target is queried the same way for all nodes and for each node id alone. Every result has
   status 200 without error and a frame with a time field and a numeric field that holds a sample. An empty frame
   fails.

Prometheus scrapes every 15 s, so each observation is retried up to ``--settle-seconds`` and the last observation is
judged. Mutations: none. Only ``kubectl port-forward`` processes are started (read access to Pods, Nodes, the
ConfigMap, Deployments) and they are terminated in ``finally``.

Prerequisites: the disposable fixture from ``eks_fixture`` (``--run-id``, ``--cluster``, ``--private`` holding
``kubeconfig`` and ``kubectl.exe``, AWS credentials in the environment), the stack from
``deploy/observability/kubernetes/dashboard-stack.yaml`` applied in ``narumq-observability``, and the Grafana admin
login. ``NARUMQ_GRAFANA_ADMIN_USER`` (default ``admin``) and ``NARUMQ_GRAFANA_ADMIN_PASSWORD`` (default ``admin``,
the Grafana default the manifest leaves unchanged) are read from the environment and never printed or saved.

Run::

    python tests/chaos/eks-dashboard/main.py --run-id <run> --cluster <eks cluster> --private <dir> --evidence <dir>

A missing prerequisite raises ``PrerequisiteError``; the scenario is then recorded as failed, never skipped.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Callable

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent))  # tests/chaos: eks_fixture, eks_port_forward
sys.path.insert(0, str(HERE.parents[1]))  # tests: scenario_results

import dashboard_judge as judge  # noqa: E402
from eks_fixture import add_fixture_arguments, build_fixture, rollout_faults  # noqa: E402
from eks_port_forward import PortForward  # noqa: E402
from scenario_results import scenario_result  # noqa: E402

OBSERVABILITY_NAMESPACE = "narumq-observability"
BROKER_NAMESPACE = "narumq"
SLOT_CONFIGMAP = "narumq-cluster-configs"
RESULT_NAME = "mgt-scn-043-dashboard.json"
NODE_SCOPES = ("1", "2", "3")


class PrerequisiteError(RuntimeError):
    """A prerequisite of the live acceptance is missing; the scenario fails instead of being skipped."""


def settled(observe: Callable[[], Any], problems_of: Callable[[Any], list[str]], timeout: float,
            interval: float = 5.0) -> dict[str, Any]:
    """Observe until ``problems_of`` is empty or ``timeout`` passes; the last observation is the judged one."""
    deadline, attempts = time.monotonic() + timeout, 0
    while True:
        attempts += 1
        try:
            observed = observe()
            problems = problems_of(observed)
        except PrerequisiteError:
            raise
        except Exception as error:  # a connection or decode failure is an unmet condition, retried until the deadline
            observed, problems = None, [f"observation failed: {type(error).__name__}: {error}"]
        if not problems or time.monotonic() >= deadline:
            return {"observed": observed, "problems": problems, "attempts": attempts}
        time.sleep(interval)


class Http:
    """JSON over a local port-forward, optionally with Grafana basic auth. The credential is never stored in output."""

    def __init__(self, port: int, credentials: tuple[str, str] | None = None):
        self.port = port
        self.headers: dict[str, str] = {}
        if credentials:
            token = base64.b64encode(f"{credentials[0]}:{credentials[1]}".encode()).decode()
            self.headers["Authorization"] = "Basic " + token

    def call(self, method: str, path: str, body: Any = None) -> tuple[int, Any]:
        status, payload = rollout_faults().http_json(method, f"http://127.0.0.1:{self.port}{path}", body, self.headers, 20.0)
        if status == 401:
            raise PrerequisiteError(f"{method} {path} answered 401: the Grafana admin login was rejected "
                                    "(set NARUMQ_GRAFANA_ADMIN_USER / NARUMQ_GRAFANA_ADMIN_PASSWORD)")
        return status, payload


def require_stack(fixture: Any, namespace: str) -> dict[str, Any]:
    """The observability namespace and both Deployments must exist and be available."""
    if not fixture.kubectl("get", "namespace", namespace, "--ignore-not-found", "-o", "name", namespace=BROKER_NAMESPACE).strip():
        raise PrerequisiteError(f"namespace {namespace} does not exist: apply deploy/observability/kubernetes/dashboard-stack.yaml")
    state = {}
    for name in ("prometheus", "grafana"):
        raw = fixture.kubectl("get", "deployment", name, "--ignore-not-found", "-o", "json", namespace=namespace)
        if not raw.strip():
            raise PrerequisiteError(f"Deployment {name} is missing in {namespace}")
        status = json.loads(raw).get("status", {})
        state[name] = {"replicas": status.get("replicas"), "availableReplicas": status.get("availableReplicas")}
        if not status.get("availableReplicas"):
            raise PrerequisiteError(f"Deployment {name} in {namespace} has no available replica: {state[name]}")
    return state


def broker_cross_check(fixture: Any) -> tuple[list[str], dict[str, str], dict[str, Any]]:
    pods = fixture.kjson("get", "pods", "-l", "app=narumq", namespace=BROKER_NAMESPACE)["items"]
    nodes = fixture.kjson("get", "nodes", namespace=BROKER_NAMESPACE)["items"]
    zones = {node["metadata"]["name"]: node["metadata"].get("labels", {}).get(judge.ZONE_LABEL, "") for node in nodes}
    slots = fixture.kjson("get", "configmap", SLOT_CONFIGMAP, namespace=BROKER_NAMESPACE).get("data", {})
    problems, mapping = judge.judge_pods(pods, zones, slots)
    evidence = {"pods": [{"name": p["metadata"]["name"], "worker": p["spec"].get("nodeName"),
                          "zone": zones.get(p["spec"].get("nodeName", ""))} for p in pods],
                "nodeIdToPod": mapping}
    return problems, mapping, evidence


# Scenarios: management-api-and-observability.md:MGT-SCN-043
@scenario_result
def scenario_dashboard_stack(fixture: Any, grafana_user: str, grafana_password: str, settle_seconds: float = 180.0,
                             observability_namespace: str = OBSERVABILITY_NAMESPACE) -> dict[str, Any]:
    """Prometheus targets UP, Grafana datasources OK, real topology and the panel 7 time-series frames."""
    checks = rollout_faults().Checks()
    stack = require_stack(fixture, observability_namespace)
    pod_problems, mapping, pod_evidence = broker_cross_check(fixture)
    checks.check("kubectl: 3 broker Pods Ready in 3 distinct zones with node ids from the cluster slots", not pod_problems,
                 {"problems": pod_problems, **pod_evidence})
    expected_ids = set(mapping) or {"1", "2", "3"}
    evidence: dict[str, Any] = {"stack": stack, "brokers": pod_evidence}
    prometheus = PortForward(fixture, observability_namespace, "svc/prometheus", 9090)
    grafana = PortForward(fixture, observability_namespace, "svc/grafana", 3000)
    try:
        prom = Http(prometheus.start()[9090])
        graf = Http(grafana.start()[3000], (grafana_user, grafana_password))

        def targets() -> dict[str, Any]:
            status, payload = prom.call("GET", "/api/v1/targets?state=active")
            if status != 200:
                raise RuntimeError(f"Prometheus targets HTTP {status}")
            _, series = prom.call("GET", "/api/v1/query?query=narumq_connections_active")
            return {"targets": payload["data"]["activeTargets"], "series": series}

        result = settled(targets, lambda o: judge.judge_prometheus_targets(o["targets"], mapping)
                         + judge.judge_node_series(o["series"], expected_ids, "narumq_connections_active"),
                         settle_seconds)
        observed = result["observed"] or {"targets": []}
        checks.check("Prometheus: every narumq-management node target is UP and series exist for all node ids",
                     not result["problems"], {"problems": result["problems"], "attempts": result["attempts"],
                                              "targets": [{"nodeId": (t.get("labels") or {}).get("node_id"), "health": t.get("health"),
                                                           "scrapeUrl": t.get("scrapeUrl"), "lastError": t.get("lastError"),
                                                           "lastScrape": t.get("lastScrape")}
                                                          for t in observed["targets"] if (t.get("labels") or {}).get("job") == judge.JOB]})

        def datasources() -> dict[str, Any]:
            status, listing = graf.call("GET", "/api/datasources")
            if status != 200:
                raise RuntimeError(f"Grafana datasource list HTTP {status}")
            health = {uid: graf.call("GET", f"/api/datasources/uid/{uid}/health")[1] for uid in judge.DATASOURCES}
            return {"listing": listing, "health": {uid: h if isinstance(h, dict) else {"status": str(h)[:80]} for uid, h in health.items()}}

        result = settled(datasources, lambda o: judge.judge_datasources(o["listing"], o["health"]), settle_seconds)
        observed = result["observed"] or {"listing": [], "health": {}}
        checks.check("Grafana: both provisioned datasources exist and report health OK", not result["problems"],
                     {"problems": result["problems"], "attempts": result["attempts"],
                      "datasources": {i.get("uid"): i.get("type") for i in observed["listing"]},
                      "health": {uid: h.get("status") for uid, h in observed["health"].items()}})

        def dashboard() -> dict[str, Any]:
            status, payload = graf.call("GET", f"/api/dashboards/uid/{judge.DASHBOARD_UID}")
            if status != 200:
                raise RuntimeError(f"Grafana dashboard {judge.DASHBOARD_UID} HTTP {status}")
            return payload["dashboard"]

        loaded = settled(dashboard, lambda d: [], settle_seconds)
        if loaded["problems"]:
            raise PrerequisiteError(f"dashboard {judge.DASHBOARD_UID} is not provisioned: {loaded['problems']}")
        definition = loaded["observed"]
        topology_panel = judge.find_panel(definition, judge.TOPOLOGY_PANEL)
        series_panel = judge.find_panel(definition, judge.TIMESERIES_PANEL)
        checks.check("dashboard narumq-operations is provisioned with panel 6 (table) and panel 7 (timeseries)",
                     topology_panel.get("type") == "table" and series_panel.get("type") == "timeseries",
                     {"panel6": topology_panel.get("type"), "panel7": series_panel.get("type")})

        def query(panel: dict[str, Any], selector: str) -> dict[str, Any]:
            path, body = judge.build_query_request(panel, selector, int(time.time() * 1000))
            status, payload = graf.call("POST", path, body)
            if status != 200:
                raise RuntimeError(f"Grafana /api/ds/query panel {panel['id']} HTTP {status}")
            return payload

        def topology_problems(payload: dict[str, Any]) -> list[str]:
            return judge.query_problems(topology_panel, payload) + judge.judge_topology(
                (payload.get("results") or {}).get("A") or {}, expected_ids)

        result = settled(lambda: query(topology_panel, ".*"), topology_problems, settle_seconds)
        topology = ((result["observed"] or {}).get("results") or {}).get("A") or {}
        rows = {name: judge.column_values(topology, name) for name in ("Node", "Role", "State", "Ready", "Query status", "Version")}
        checks.check("Grafana panel 6: real topology - 3 VOTER rows, OK, READY/true, one LEADER, version, positive uptime",
                     not result["problems"], {"problems": result["problems"], "attempts": result["attempts"], "columns": rows})

        def series_problems(selector: str) -> Callable[[dict[str, Any]], list[str]]:
            return lambda payload: judge.judge_timeseries(series_panel, payload, f"node={selector}")

        for selector in (".*", *NODE_SCOPES):
            result = settled(lambda s=selector: query(series_panel, s), series_problems(selector), settle_seconds)
            label = "all nodes" if selector == ".*" else f"node_id={selector}"
            checks.check(f"Grafana panel 7: time-series frames for {label} (status 200, time + numeric field, samples)",
                         not result["problems"], {"problems": result["problems"], "attempts": result["attempts"],
                                                  "frames": judge.timeseries_summary(result["observed"])})
    finally:
        grafana.close()
        prometheus.close()
    return fixture.save_result(RESULT_NAME, checks.result("MGT-SCN-043", fixture=fixture.identity, **evidence))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    add_fixture_arguments(parser)
    parser.add_argument("--settle-seconds", type=float, default=180.0, help="bounded wait per observation for a Prometheus scrape")
    parser.add_argument("--observability-namespace", default=OBSERVABILITY_NAMESPACE)
    args = parser.parse_args(argv)
    fixture = build_fixture(args)
    result = scenario_dashboard_stack(fixture, os.environ.get("NARUMQ_GRAFANA_ADMIN_USER", "admin"),
                                      os.environ.get("NARUMQ_GRAFANA_ADMIN_PASSWORD", "admin"), args.settle_seconds,
                                      args.observability_namespace)
    for item in result["checks"]:
        print(("PASS " if item["ok"] else "FAIL ") + item["check"])
    print(result["result"], "->", args.evidence / RESULT_NAME)
    return 0 if result["result"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
