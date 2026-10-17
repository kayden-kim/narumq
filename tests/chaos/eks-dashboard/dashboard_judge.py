"""Pure judgment of observed Prometheus, Grafana and Kubernetes data for MGT-SCN-043.

Nothing here touches a cluster: every function takes already-decoded JSON, so the unit tests in
tests/test_eks_dashboard.py need no fixture. The query construction and frame helpers are ports of the helpers in
tests/integration/dashboard_stack_test.go (``queryGrafanaPanel``, ``dashboardStackResultColumnValues``,
``dashboardStackNumber``, ``dashboardStackResultMaximum`` and ``assertGrafanaNodeTopologyPanel``). Judges return a
list of problem strings; an empty list means the observation satisfies the scenario row.
"""

from __future__ import annotations

import sys
from typing import Any

sys.dont_write_bytecode = True

JOB = "narumq-management"
DASHBOARD_UID = "narumq-operations"
PROMETHEUS_UID = "narumq-prometheus"
MANAGEMENT_UID = "narumq-management"
DATASOURCES = {PROMETHEUS_UID: "prometheus", MANAGEMENT_UID: "yesoreyeram-infinity-datasource"}
TOPOLOGY_PANEL, TIMESERIES_PANEL = 6, 7
TOPOLOGY_COLUMNS = ("Node", "Membership", "Query status", "Query error", "Observed at", "Role", "Version",
                    "Uptime (ms)", "State", "Ready", "Readiness reason", "Replication")
ZONE_LABEL = "topology.kubernetes.io/zone"


# ---------------------------------------------------------------- frames (ports of the Go helpers)
def number(value: Any) -> float | None:
    """``dashboardStackNumber``: numbers and numeric strings, never booleans."""
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    return None


def frame_fields(frame: dict[str, Any]) -> list[dict[str, Any]]:
    """Schema fields of one Grafana frame joined with their value columns."""
    fields = (frame.get("schema") or {}).get("fields") or []
    columns = (frame.get("data") or {}).get("values") or []
    return [{"name": field.get("name"), "type": field.get("type"), "labels": field.get("labels") or {},
             "values": columns[index] if index < len(columns) else None} for index, field in enumerate(fields)]


def column_values(result: dict[str, Any], name: str) -> list[Any] | None:
    """``dashboardStackResultColumnValues``: first frame column called ``name``; ``None`` when absent."""
    for frame in result.get("frames") or []:
        for field in frame_fields(frame):
            if field["name"] == name and field["values"] is not None:
                return field["values"]
    return None


# ---------------------------------------------------------------- query construction
def find_panel(dashboard: dict[str, Any], panel_id: int) -> dict[str, Any]:
    """Return panel ``panel_id`` from the provisioned dashboard definition; raise when it is not provisioned."""
    for panel in dashboard.get("panels") or []:
        if panel.get("id") == panel_id and panel.get("type") != "row":
            return panel
    raise LookupError(f"provisioned dashboard lacks panel {panel_id}")


def build_panel_queries(panel: dict[str, Any], node_selector: str, rate_interval: str = "1m",
                        drain_node: str = "") -> tuple[list[dict[str, Any]], bool]:
    """Queries Grafana sends for ``panel`` as in ``queryGrafanaPanel``; second value says a Prometheus target exists."""
    queries: list[dict[str, Any]] = []
    prometheus = False
    for target in panel.get("targets") or []:
        datasource = target.get("datasource") or {}
        uid = datasource.get("uid")
        query: dict[str, Any] = {"datasource": {"type": datasource.get("type"), "uid": uid},
                                 "refId": target.get("refId"), "maxDataPoints": 1000, "intervalMs": 15000}
        if uid == PROMETHEUS_UID:
            prometheus = True
            expression = (target.get("expr", "").replace("$node", node_selector)
                          .replace("$__rate_interval", rate_interval).replace("$rebalance_max_deviation", "100"))
            query.update({"editorMode": target.get("editorMode"), "expr": expression, "format": target.get("format"),
                          "instant": bool(target.get("instant", False)), "legendFormat": target.get("legendFormat"),
                          "range": bool(target.get("range", False))})
        elif uid == MANAGEMENT_UID:
            query.update({"type": target.get("type"), "source": target.get("source"),
                          "url": target.get("url", "").replace("${drain_node}", drain_node),
                          "url_options": {"method": (target.get("url_options") or {}).get("method")},
                          "parser": target.get("parser"), "format": target.get("format"),
                          "root_selector": target.get("root_selector"), "columns": target.get("columns")})
        else:
            raise ValueError(f"panel {panel.get('id')} uses unexpected datasource {uid!r}")
        queries.append(query)
    return queries, prometheus


def build_query_request(panel: dict[str, Any], node_selector: str, now_ms: int, rate_interval: str = "1m",
                        drain_node: str = "") -> tuple[str, dict[str, Any]]:
    """Return ``(path, body)`` for POST /api/ds/query covering the last five minutes."""
    queries, prometheus = build_panel_queries(panel, node_selector, rate_interval, drain_node)
    body = {"from": str(now_ms - 5 * 60 * 1000), "to": str(now_ms), "timezone": "utc", "queries": queries}
    return "/api/ds/query" + ("?ds_type=prometheus" if prometheus else ""), body


def query_problems(panel: dict[str, Any], response: dict[str, Any] | None) -> list[str]:
    """Every target needs a result with status 200 and no error (the Go ``queryGrafanaPanel`` failure rule)."""
    problems = []
    results = (response or {}).get("results") or {}
    if not panel.get("targets"):
        problems.append(f"panel {panel.get('id')} has no targets")
    for target in panel.get("targets") or []:
        result = results.get(target.get("refId"))
        if result is None:
            problems.append(f"query {target.get('refId')}: no result")
        elif result.get("status") != 200 or result.get("error"):
            problems.append(f"query {target.get('refId')}: status={result.get('status')} error={result.get('error')!r}")
    return problems


# ---------------------------------------------------------------- Prometheus
def judge_prometheus_targets(active_targets: list[dict[str, Any]], expected: dict[str, str]) -> list[str]:
    """``expected`` maps node_id -> Pod name. Every node target of job narumq-management must be ``up``."""
    problems = []
    targets = [t for t in active_targets if (t.get("labels") or {}).get("job") == JOB]
    by_node: dict[str, list[dict[str, Any]]] = {}
    for target in targets:
        by_node.setdefault((target.get("labels") or {}).get("node_id", ""), []).append(target)
    for node_id, pod in sorted(expected.items()):
        found = by_node.get(node_id, [])
        if len(found) != 1:
            problems.append(f"node_id={node_id}: {len(found)} {JOB} targets, want exactly 1")
            continue
        target = found[0]
        host = (target.get("scrapeUrl") or "").split("//", 1)[-1].split("/", 1)[0]
        if target.get("health") != "up":
            problems.append(f"node_id={node_id}: health={target.get('health')!r} lastError={target.get('lastError')!r}")
        if target.get("lastError"):
            problems.append(f"node_id={node_id}: lastError={target.get('lastError')!r}")
        if host.split(".", 1)[0] != pod:
            problems.append(f"node_id={node_id}: scrapes {host!r}, the Pod behind this node id is {pod}")
    extra = sorted(set(by_node) - set(expected))
    if extra:
        problems.append(f"unexpected node_id targets: {extra}")
    return problems


def judge_node_series(instant_response: dict[str, Any], expected_ids: set[str], metric: str) -> list[str]:
    """Prometheus instant query result must hold a sample for every node id."""
    if instant_response.get("status") != "success":
        return [f"{metric}: Prometheus query status={instant_response.get('status')!r}"]
    ids = {(item.get("metric") or {}).get("node_id") for item in (instant_response.get("data") or {}).get("result") or []}
    missing = sorted(expected_ids - ids)
    return [f"{metric}: no series for node_id {missing}"] if missing else []


# ---------------------------------------------------------------- Grafana datasources
def judge_datasources(listing: list[dict[str, Any]], health: dict[str, dict[str, Any] | None]) -> list[str]:
    """Exactly the two provisioned datasources, with their types, each health ``OK``."""
    problems = []
    got = {item.get("uid"): item.get("type") for item in listing}
    if got != DATASOURCES:
        problems.append(f"datasources {got}, want exactly {DATASOURCES}")
    for uid in DATASOURCES:
        status = (health.get(uid) or {}).get("status")
        if status != "OK":
            problems.append(f"datasource {uid} health status={status!r} message={(health.get(uid) or {}).get('message')!r}")
    return problems


# ---------------------------------------------------------------- panel 6: topology
def judge_topology(result: dict[str, Any], expected_ids: set[str]) -> list[str]:
    """Panel 6 table: one row per node, VOTER, queryStatus OK, READY/true, positive uptime, exactly one LEADER."""
    problems = []
    columns: dict[str, list[Any]] = {}
    for name in TOPOLOGY_COLUMNS:
        values = column_values(result, name)
        if values is None:
            problems.append(f"column {name!r} is absent")
        elif len(values) != len(expected_ids):
            problems.append(f"column {name!r} has {len(values)} values, want {len(expected_ids)}")
        else:
            columns[name] = values
    if problems:
        return problems
    seen = [str(value) for value in columns["Node"]]
    if sorted(seen) != sorted(expected_ids):
        problems.append(f"node ids {sorted(seen)}, want {sorted(expected_ids)}")
    leaders = 0
    for index, node in enumerate(seen):
        row = {name: values[index] for name, values in columns.items()}
        if row["Membership"] != "VOTER" or row["Query status"] != "OK":
            problems.append(f"node {node}: membership={row['Membership']!r} queryStatus={row['Query status']!r}, want VOTER/OK")
        if row["Query error"] not in (None, ""):
            problems.append(f"node {node}: query error {row['Query error']!r}")
        if row["Observed at"] in (None, "") or not str(row["Version"] or ""):
            problems.append(f"node {node}: observedAt={row['Observed at']!r} version={row['Version']!r}, want both measured")
        if row["State"] != "READY" or str(row["Ready"]).lower() != "true":
            problems.append(f"node {node}: state={row['State']!r} ready={row['Ready']!r}, want READY/true")
        uptime = number(row["Uptime (ms)"])
        if uptime is None or uptime <= 0:
            problems.append(f"node {node}: uptime={row['Uptime (ms)']!r}, want positive")
        if row["Role"] == "LEADER":
            leaders += 1
        elif row["Role"] != "FOLLOWER":
            problems.append(f"node {node}: role={row['Role']!r}, want LEADER or FOLLOWER")
    if leaders != 1:
        problems.append(f"{leaders} LEADER rows, want exactly 1")
    return problems


# ---------------------------------------------------------------- panel 7: time series
def judge_timeseries(panel: dict[str, Any], response: dict[str, Any] | None, scope: str) -> list[str]:
    """Every target returns at least one frame; every frame has a time field and a numeric field with samples."""
    problems = [f"{scope}: {problem}" for problem in query_problems(panel, response)]
    results = (response or {}).get("results") or {}
    for target in panel.get("targets") or []:
        ref = target.get("refId")
        frames = (results.get(ref) or {}).get("frames") or []
        if not frames:
            problems.append(f"{scope}/{ref}: empty result, no frame")
            continue
        for number_of_frame, frame in enumerate(frames):
            fields = frame_fields(frame)
            time_fields = [f for f in fields if f["type"] == "time"]
            numeric = [f for f in fields if f["type"] == "number"]
            if not time_fields or not (time_fields[0]["values"] or []):
                problems.append(f"{scope}/{ref}/frame {number_of_frame}: no time field with samples")
            if not numeric:
                problems.append(f"{scope}/{ref}/frame {number_of_frame}: no numeric field")
                continue
            for field in numeric:
                samples = [v for v in (field["values"] or []) if number(v) is not None]
                if not samples:
                    problems.append(f"{scope}/{ref}/frame {number_of_frame}: numeric field {field['name']!r} has no sample")
    return problems


def timeseries_summary(response: dict[str, Any] | None) -> dict[str, Any]:
    """Evidence summary of a panel 7 response: frames, samples and the latest numeric value per target."""
    summary: dict[str, Any] = {}
    for ref, result in ((response or {}).get("results") or {}).items():
        frames = []
        for frame in result.get("frames") or []:
            fields = frame_fields(frame)
            numeric = [f for f in fields if f["type"] == "number"]
            samples = [number(v) for f in numeric for v in (f["values"] or []) if number(v) is not None]
            frames.append({"fields": [(f["name"], f["type"]) for f in fields],
                           "timeSamples": len(next((f["values"] or [] for f in fields if f["type"] == "time"), [])),
                           "numericSamples": len(samples), "last": samples[-1] if samples else None})
        summary[ref] = {"status": result.get("status"), "error": result.get("error") or None, "frames": frames}
    return summary


# ---------------------------------------------------------------- Kubernetes cross-check
def judge_pods(pods: list[dict[str, Any]], node_zones: dict[str, str], slots: dict[str, str]) -> tuple[list[str], dict[str, str]]:
    """Cross-check ``kubectl get pods -l app=narumq``.

    ``slots`` is the cluster ConfigMap data (``slot-N`` -> node id). Returns the problems and node id -> Pod name.
    """
    problems = []
    mapping: dict[str, str] = {}
    if len(pods) != 3:
        problems.append(f"{len(pods)} broker Pods, want 3")
    zones = []
    for pod in pods:
        meta, status = pod["metadata"], pod.get("status", {})
        name = meta["name"]
        ordinal = name.rsplit("-", 1)[-1]
        node_id = slots.get(f"slot-{ordinal}")
        if node_id is None:
            problems.append(f"{name}: no slot-{ordinal} entry in the cluster ConfigMap")
        else:
            mapping[node_id] = name
        container = next((c for c in status.get("containerStatuses", []) if c.get("name") == "narumq"), {})
        ready = any(c.get("type") == "Ready" and c.get("status") == "True" for c in status.get("conditions", []))
        if meta.get("deletionTimestamp") or not ready or not container.get("ready"):
            problems.append(f"{name}: not Ready (deleting={meta.get('deletionTimestamp')!r})")
        zone = node_zones.get(pod["spec"].get("nodeName", ""))
        if not zone:
            problems.append(f"{name}: worker {pod['spec'].get('nodeName')!r} has no {ZONE_LABEL} label")
        zones.append(zone)
    if len({z for z in zones if z}) != 3:
        problems.append(f"Pods occupy zones {sorted(z or '?' for z in zones)}, want 3 distinct zones")
    if len(mapping) != 3:
        problems.append(f"node ids from slots {sorted(mapping)}, want 3 distinct")
    return problems, mapping
