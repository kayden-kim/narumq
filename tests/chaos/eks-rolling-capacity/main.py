"""Live EKS acceptance of 100 consecutive rolling updates at the measured maximum traffic (DRN-SCN-019; DEP-06/07/10/12).

Opt-in, long (hours): NOT part of the local gate. Prepared, not executed against a live cluster.

What it proves (the row's expected result): with the real accepted capacity load running (the adopted step's
``B_conn`` connections and ``B_pub`` publish rate, mixed MQTT 3.1.1/5.0, QoS 0/1/2, retained, Will and persistent
sessions), 100 consecutive controller-driven rolling updates complete, and the separately measured gap, duplicate and
reorder counts stay within the decided absolute cutoffs and ``unexpected_supported_client`` grows by 0.

Prerequisites (any missing one raises a named error and ``scenario_result`` records FAIL; nothing is skipped or
simulated):

* ``--capacity-result``: a real accepted EKS capacity run (tests/benchmark/capacity/eks/controller.py output with
  ``bounds`` not null, ``adopted`` confirmed, three active features) - ``MissingCapacityBaseline`` otherwise.
* ``--cutoffs``: the absolute gap/duplicate/reorder cutoffs decided after B_* (the spec leaves them open), bound to the
  capacity profile hash - see ``rolling_capacity_judge.validate_cutoffs``. QoS 1/2 gap and QoS 2 duplicate must be 0.
* ``--image-a`` / ``--image-b``: two different ``repository@sha256:<platform manifest>`` builds inside the ADR-0018
  window, both ``linux/arm64`` with the release labels. A must be the broker digest the capacity run measured. The
  controller replaces only Pods whose digest differs from the target, so the 100 rounds alternate B, A, B, A ... (the
  100th round returns to A).
* ``--generator-image``: a digest-pinned ``python3`` (3.10+) image for the load generator Job, plus Nodes labelled and
  tainted ``narumq.io/generator=true`` (as in the capacity fixture), the Secret ``narumq-capacity-fixture`` and the
  internal NLB Service ``narumq-nlb``.
* The disposable fixture of ADR-0007 (three ``m7g.large`` workers in three AZs, broker 1 CPU / 4 GiB, 20 GiB gp3) with
  the rollout controller, its CRD, ``narumq-registry`` and ``rollout-management`` Secrets, a StatefulSet ``narumq``
  (OnDelete, 3 replicas) at image A, and the capacity fixture's active features. ``--registry-tool`` needs ``crane``
  or ``docker`` for registry reads. Fixture identity and the AWS account guard come from tests/chaos/eks_fixture.py.

Environment: ``AWS_PROFILE`` / ``AWS_REGION`` (or the usual AWS credential variables) for the fixture identity check;
no other environment variable is read.

Mutates (inside the verified fixture only): the broker StatefulSet template image (A <-> B), 100 ``NaruMQRollout``
objects (one per round, deleted after the round), a ConfigMap ``narumq-drn019-<token>`` and an Indexed Job
``narumq-drn019-<token>`` (the generator, in the generator node group), and a short-lived audit reader Pod.

Cleanup (``finally``): rollouts of this run, the generator Job and ConfigMap are deleted; the StatefulSet template is
left on the last image (A after a complete run). Ctrl-C keeps the Job and ConfigMap so ``--resume`` can continue.

Evidence in the fixture's evidence directory: ``drn-scn-019-rolling-capacity.json`` (result), ``drn019-state.json``
(resume state), ``drn019-rounds/round-NNN.json`` (hash-chained per-round records), ``drn019-generator-final.json``.

Run (from the repository root, PowerShell):
    python tests/chaos/eks-rolling-capacity/main.py --run-id <run> --cluster <eks cluster> --private <dir> `
        --evidence <dir> --capacity-result <capacity run dir> --cutoffs <cutoffs.json> `
        --image-a <repo@sha256:...> --image-b <repo@sha256:...> --generator-image <python@sha256:...> `
        --server-name <TLS SAN> --shards <N> [--resume]
Offline judgment tests: ``python -m unittest tests.test_eks_rolling_capacity``.

Resume: state is saved after every round and every Will scrape. ``--resume`` continues a run with the same identity at
the next round; an interrupted round is continued by the controller, and a run resumed in the middle of a round is
judged FAIL because the Will outcome counters were not observed continuously.

Known limits: the generator reconnects through the NLB and ignores the MQTT 5 Server Reference; Will counts that
happen between the last scrape of a Pod and its termination are unobservable (scrapes run every ~3 s and every
rollout sample); the one generator process per shard is single-threaded and saturation (CPU or schedule lag) makes
the run INVALID rather than passed.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Callable

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent))  # tests/chaos: eks_fixture, eks_rollout_ops
sys.path.insert(0, str(HERE.parents[1]))  # tests: scenario_results

import eks_rollout_ops as ops  # noqa: E402
import rolling_capacity_judge as judge  # noqa: E402
from eks_fixture import add_fixture_arguments, build_fixture, rollout_faults  # noqa: E402
from scenario_results import scenario_result  # noqa: E402

RESULT_NAME = "drn-scn-019-rolling-capacity.json"
STATE_NAME = "drn019-state.json"
ROUND_DIR = "drn019-rounds"
FINAL_NAME = "drn019-generator-final.json"
CRD = "narumqrollouts.narumq.io"
FIXTURE_SECRET = "narumq-capacity-fixture"
NLB_SERVICE = "narumq-nlb"
GENERATOR_CONTAINER = "generator"
GENERATOR_LABEL = "narumq.io/drn019-run"
SCRIPT = HERE / "mix_generator.py"


class GeneratorLost(RuntimeError):
    """A generator Pod is missing, failed or reported a failed setup."""


class RunStateError(RuntimeError):
    """The saved resume state is absent when ``--resume`` is set, present when it is not, or does not match this run."""


# Pure builders ----------------------------------------------------------------------------------------
def generator_config(baseline: dict[str, Any], token: int, shards: int, mqtt: str, mqtt_tls: str, server_name: str,
                     horizon: float, settle: float) -> dict[str, Any]:
    """Configuration file of ``mix_generator.py``: the capacity profile, the adopted step and the endpoints."""
    return {"runToken": token, "shards": shards, "profile": baseline["profile"], "cells": baseline["cells"],
            "rate": baseline["ratePerStreamPerSecond"], "mqtt": mqtt, "mqttTls": mqtt_tls, "tlsServerName": server_name,
            "horizonSeconds": horizon, "settleSeconds": settle}


def generator_manifests(name: str, token: str, image: str, shards: int, cpu: str, memory: str, config: dict[str, Any],
                        script: str, deadline_seconds: int) -> list[dict[str, Any]]:
    """ConfigMap (script + config) and Indexed Job that runs one generator shard per completion index."""
    labels = {"app": "narumq-drn019-generator", GENERATOR_LABEL: token}
    configmap = {"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": name, "labels": labels},
                 "data": {"mix_generator.py": script, "config.json": json.dumps(config)}}
    resources = {"requests": {"cpu": cpu, "memory": memory}, "limits": {"cpu": cpu, "memory": memory}}
    job = {"apiVersion": "batch/v1", "kind": "Job", "metadata": {"name": name, "labels": labels},
           "spec": {"completionMode": "Indexed", "completions": shards, "parallelism": shards, "backoffLimit": 0,
                    "activeDeadlineSeconds": deadline_seconds,
                    "template": {"metadata": {"labels": labels},
                                 "spec": {"restartPolicy": "Never",
                                          "securityContext": {"runAsNonRoot": True, "runAsUser": 10001, "runAsGroup": 10001, "fsGroup": 10001},
                                          "nodeSelector": {"narumq.io/generator": "true"},
                                          "tolerations": [{"key": "narumq.io/generator", "operator": "Equal", "value": "true", "effect": "NoSchedule"}],
                                          "containers": [{"name": GENERATOR_CONTAINER, "image": image, "imagePullPolicy": "IfNotPresent",
                                                          "command": ["python3", "-u", "/app/mix_generator.py", "--config", "/app/config.json"],
                                                          "env": [{"name": "PYTHONDONTWRITEBYTECODE", "value": "1"}],
                                                          "securityContext": {"allowPrivilegeEscalation": False, "readOnlyRootFilesystem": True},
                                                          "resources": resources,
                                                          "volumeMounts": [{"name": "app", "mountPath": "/app", "readOnly": True},
                                                                           {"name": "fixture", "mountPath": "/fixture", "readOnly": True},
                                                                           {"name": "control", "mountPath": "/control"},
                                                                           {"name": "output", "mountPath": "/output"}]}],
                                          "volumes": [{"name": "app", "configMap": {"name": name, "defaultMode": 0o444}},
                                                      {"name": "fixture", "secret": {"secretName": FIXTURE_SECRET, "defaultMode": 0o440}},
                                                      {"name": "control", "emptyDir": {}}, {"name": "output", "emptyDir": {}}]}}}}
    return [configmap, job]


def generator_pods(items: list[dict[str, Any]], shards: int) -> dict[int, str]:
    """Running generator Pod per completion index; a Failed Pod or a missing index raises ``GeneratorLost``."""
    pods: dict[int, str] = {}
    for item in items:
        phase = item.get("status", {}).get("phase")
        index = int(item["metadata"].get("annotations", {}).get("batch.kubernetes.io/job-completion-index", -1))
        if phase == "Failed":
            raise GeneratorLost(f"generator Pod {item['metadata']['name']} (shard {index}) failed")
        if phase == "Running":
            pods[index] = item["metadata"]["name"]
    if sorted(pods) != list(range(shards)):
        raise GeneratorLost(f"generator shards running: {sorted(pods)} of {shards}")
    return pods


def totals_of(summaries: list[dict[str, Any]]) -> dict[str, Any]:
    return {"clientsExpected": sum(s["clientsExpected"] for s in summaries), "clientsConnected": sum(s["clientsConnected"] for s in summaries),
            "saturated": any(s["saturation"]["saturated"] for s in summaries),
            "cpuPercentMax": max(s["saturation"]["cpuPercentMax"] for s in summaries),
            "scheduleLagMaxMillis": max(s["saturation"]["scheduleLagMaxMillis"] for s in summaries)}


def trim_quorum(probe: dict[str, Any]) -> dict[str, Any]:
    return {"at": probe.get("at"), "pod": probe.get("pod"), "quorumAvailable": probe.get("quorumAvailable"),
            "leaderNodeId": probe.get("leaderNodeId"), "error": probe.get("error")}


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, default=str) + "\n", encoding="utf-8")
    os.replace(temporary, path)


# Run context ---------------------------------------------------------------------------------------------
class Run:
    """State of one execution: fixture handle, verified inputs, Will accumulator and the saved resume state."""

    def __init__(self, fixture: Any, args: argparse.Namespace, baseline: dict[str, Any], cutoffs: dict[str, Any],
                 images: dict[str, dict[str, Any]], token: str):
        self.fixture, self.args, self.baseline, self.cutoffs, self.images, self.token = fixture, args, baseline, cutoffs, images, token
        self.job = f"narumq-drn019-{token}"
        self.will = ops.WillAccumulator()
        self.scrape_errors: list[dict[str, Any]] = []
        self.resumed_mid_round: list[int] = []
        self.identity = {"runId": fixture.run_id, "namespace": fixture.namespace, "capacityProfileSha256": baseline["profileSha256"],
                         "imageA": images["A"]["reference"], "imageB": images["B"]["reference"], "generatorImage": args.generator_image,
                         "shards": args.shards, "cells": baseline["cells"], "rate": baseline["ratePerStreamPerSecond"],
                         "cutoffsDecidedAt": cutoffs["decidedAt"]}
        self.evidence = Path(fixture.evidence_dir)
        self.state_path = self.evidence / STATE_NAME
        self.rounds_dir = self.evidence / ROUND_DIR

    # persisted state ----------------------------------------------------------------------------------
    def save_state(self, **extra: Any) -> None:
        state = {"schemaVersion": 1, "runIdentity": self.identity, "token": self.token, "job": self.job,
                 "will": self.will.to_json(), "scrapeErrors": self.scrape_errors, "resumedMidRound": self.resumed_mid_round,
                 "savedAt": time.time()}
        previous = json.loads(self.state_path.read_text(encoding="utf-8")) if self.state_path.is_file() else {}
        state["roundInProgress"] = extra.get("roundInProgress", previous.get("roundInProgress"))
        state["startedAt"] = previous.get("startedAt", time.time())
        write_json(self.state_path, state)

    def load_rounds(self) -> list[dict[str, Any]]:
        files = sorted(self.rounds_dir.glob("round-*.json")) if self.rounds_dir.is_dir() else []
        return [json.loads(f.read_text(encoding="utf-8")) for f in files]

    # generator ----------------------------------------------------------------------------------------
    def pods(self) -> dict[int, str]:
        items = self.fixture.kjson("get", "pods", "-l", f"job-name={self.job}")["items"]
        return generator_pods(items, self.args.shards)

    def read_file(self, pod: str, name: str) -> dict[str, Any] | None:
        code = f"import os,sys;p='/output/{name}';sys.stdout.write(open(p).read()) if os.path.exists(p) else sys.exit(3)"
        text = self.fixture.kubectl("exec", pod, "-c", GENERATOR_CONTAINER, "--", "python3", "-c", code, check=False, timeout=60)
        return json.loads(text) if text.strip() else None

    def summaries(self, name: str = "summary.json") -> list[dict[str, Any]]:
        result = []
        for index, pod in sorted(self.pods().items()):
            failed = self.read_file(pod, "failed.json")
            if failed:
                raise GeneratorLost(f"generator shard {index} setup failed: {failed.get('error')}")
            summary = self.read_file(pod, name)
            if summary is None:
                raise GeneratorLost(f"generator shard {index} has no {name} yet")
            result.append(summary)
        return result

    def touch(self, name: str) -> None:
        for pod in self.pods().values():
            self.fixture.mutate("exec", pod, "-c", GENERATOR_CONTAINER, "--", "python3", "-c", f"open('/control/{name}','w').close()", timeout=60)

    def wait_connected(self, timeout: float, what: str) -> dict[str, Any]:
        """Poll until every client of every shard is connected (and subscribed); saturation is an invalid run."""
        deadline, seen = time.monotonic() + timeout, {}
        while True:
            try:
                seen = totals_of(self.summaries())
            except GeneratorLost:
                if what == "ready" and time.monotonic() < deadline:
                    time.sleep(10)
                    continue
                raise
            if seen["saturated"]:
                raise judge.GeneratorSaturated(f"generator saturated while waiting for {what}: {seen}")
            if seen["clientsConnected"] == seen["clientsExpected"] and (what != "ready" or self.ready_everywhere()):
                return seen
            if time.monotonic() > deadline:
                raise judge.RoundFailed(0, f"{seen['clientsConnected']} of {seen['clientsExpected']} clients connected after {timeout:.0f} s ({what})")
            time.sleep(10)

    def ready_everywhere(self) -> bool:
        return all(self.read_file(pod, "ready.json") is not None for pod in self.pods().values())

    # Will outcomes ------------------------------------------------------------------------------------
    def scrape_all(self, baseline: bool = False) -> None:
        for name, pod in sorted(self.fixture.broker_pods().items()):
            try:
                sample = ops.scrape_will_outcomes(self.fixture, name)
            except ops.BrokerProbeError as error:
                self.scrape_errors.append({"at": time.time(), "pod": name, "ready": bool(pod["ready"] and not pod["deleting"]), "error": str(error)[:300]})
                continue
            if baseline:
                self.will.start(sample)
            else:
                self.will.add(sample)
        self.save_state()


# Cluster ---------------------------------------------------------------------------------------------------
def gather_live_fixture(fixture: Any) -> dict[str, Any]:
    """Observed profile of the live fixture, shaped for ``judge.check_live_fixture``."""
    items = fixture.kjson("get", "pods", "-l", "app=narumq")["items"]
    workers, digests = [], {}
    for item in items:
        node = fixture.kjson("get", "node", item["spec"]["nodeName"])["metadata"]["labels"]
        name = item["metadata"]["name"]
        workers.append({"pod": name, "instanceType": node.get("node.kubernetes.io/instance-type"), "zone": node.get("topology.kubernetes.io/zone")})
        status = next((c for c in item.get("status", {}).get("containerStatuses", []) if c["name"] == ops.BROKER_CONTAINER), {})
        digests[name] = ops.digest_of_image_id(status.get("imageID"))
    volumes = []
    for claim in fixture.kjson("get", "pvc")["items"]:
        if claim["metadata"]["name"].startswith("data-narumq-"):
            storage_class = claim["spec"].get("storageClassName")
            volumes.append({"pvc": claim["metadata"]["name"], "size": claim["spec"]["resources"]["requests"]["storage"],
                            "volumeType": fixture.kjson("get", "storageclass", storage_class).get("parameters", {}).get("type")})
    statefulset = fixture.kjson("get", "statefulset", ops.STATEFULSET)
    broker = next(c for c in statefulset["spec"]["template"]["spec"]["containers"] if c["name"] == ops.BROKER_CONTAINER)
    resources = {section: {"cpuMillicores": judge.parse_cpu_millicores(broker.get("resources", {}).get(section, {}).get("cpu", "0")),
                           "memoryBytes": judge.parse_memory_bytes(broker.get("resources", {}).get(section, {}).get("memory", "0"))}
                 for section in ("requests", "limits")}
    return {"brokerWorkers": workers, "brokerDigests": digests, "volumes": volumes, "brokerResources": resources,
            "activeFeatures": ops.features_probe(fixture)["activeFeatures"]}


def require_cluster(fixture: Any, args: argparse.Namespace, baseline: dict[str, Any], image_a: dict[str, Any]) -> dict[str, Any]:
    rf = rollout_faults()
    for name in (args.registry_secret, args.management_secret, FIXTURE_SECRET):
        if not fixture.kubectl("get", "secret", name, "--ignore-not-found", "-o", "name").strip():
            raise judge.MissingCapacityBaseline(f"secret {name} is missing in namespace {fixture.namespace}")
    if not fixture.kubectl("get", "crd", CRD, "--ignore-not-found", "-o", "name").strip():
        raise judge.MissingCapacityBaseline(f"CRD {CRD} is not installed")
    controller = fixture.kubectl("get", "deployment", rf.CONTROLLER, "--ignore-not-found", "-o", "json")
    if not controller.strip() or not json.loads(controller).get("status", {}).get("availableReplicas"):
        raise judge.MissingCapacityBaseline(f"rollout controller Deployment {rf.CONTROLLER} is missing or has no available replica")
    statefulset = fixture.kjson("get", "statefulset", ops.STATEFULSET)
    if statefulset["spec"].get("updateStrategy", {}).get("type") != "OnDelete" or statefulset["spec"].get("replicas") != ops.BROKER_REPLICAS:
        raise judge.MissingCapacityBaseline("StatefulSet narumq must be OnDelete with 3 replicas")
    unfinished = [r["metadata"]["name"] for r in fixture.kjson("get", "narumqrollout")["items"] if r.get("status", {}).get("phase") not in ops.TERMINAL_PHASES]
    if unfinished:
        raise judge.MissingCapacityBaseline(f"unfinished NaruMQRollout objects exist: {unfinished}")
    nodes = fixture.kjson("get", "nodes", "-l", "narumq.io/generator=true")["items"]
    if len(nodes) < 1:
        raise judge.MissingCapacityBaseline("no Node is labelled narumq.io/generator=true for the load generator")
    nlb = fixture.kubectl("get", "service", NLB_SERVICE, "-o", "jsonpath={.status.loadBalancer.ingress[0].hostname}").strip()
    if not nlb:
        raise judge.MissingCapacityBaseline(f"Service {NLB_SERVICE} has no load balancer hostname")
    live = gather_live_fixture(fixture)
    problems = judge.check_live_fixture(baseline, live, image_a["digest"])
    if problems:
        raise judge.CapacityMismatch("the live fixture is not the capacity-measured profile: " + "; ".join(problems))
    return {"live": live, "nlb": nlb}


def template_digest_image(fixture: Any) -> str:
    statefulset = fixture.kjson("get", "statefulset", ops.STATEFULSET)
    return next(c for c in statefulset["spec"]["template"]["spec"]["containers"] if c["name"] == ops.BROKER_CONTAINER)["image"]


def summarize_pods(fixture: Any) -> dict[str, dict[str, Any]]:
    pods = fixture.broker_pods()
    summary = ops.pod_summary(pods)
    for name, pod in pods.items():
        summary[name]["digest"] = ops.digest_of_image_id(pod["imageID"])
    return summary


# Rounds ------------------------------------------------------------------------------------------------------
def run_round(run: Run, number: int, previous: dict[str, Any] | None, resumed: bool) -> dict[str, Any]:
    """One rolling update under load; raises ``RoundFailed`` / ``GeneratorSaturated`` and stops the run at the first failure."""
    fixture, args = run.fixture, run.args
    image = run.images["B" if number % 2 else "A"]
    target = {"image": image["reference"], "digest": image["digest"], "version": image["version"]}
    name = f"drn019-r{number:03d}-{run.token}"
    started = time.time()
    run.save_state(roundInProgress=number)
    run.scrape_all()
    before = summarize_pods(fixture)
    if template_digest_image(fixture) != image["reference"]:
        ops.set_template_image(fixture, image["reference"])
    session = ops.rollout_session(fixture, name, ops.rollout_target(image), args.registry_secret, args.management_secret)
    quorum: list[dict[str, Any]] = []
    last_generator = [0.0]

    def observe(status: dict[str, Any], sample: dict[str, Any]) -> dict[str, Any]:
        try:
            probe = ops.cluster_probe(fixture)
        except ops.BrokerProbeError as error:
            probe = {"at": time.time(), "pod": None, "quorumAvailable": None, "error": str(error)}
        quorum.append(trim_quorum(probe))
        run.scrape_all()
        if time.monotonic() - last_generator[0] > 60:
            last_generator[0] = time.monotonic()
            seen = totals_of(run.summaries())
            if seen["saturated"]:
                raise judge.GeneratorSaturated(f"generator saturated during round {number}: {seen}")
        return {"quorumAvailable": probe["quorumAvailable"]}

    try:
        drive = ops.drive_rollout(fixture, session, f"{name}-1", args.round_timeout, 3.0, observe)
    except ops.RolloutTimeout as error:
        raise judge.RoundFailed(number, str(error)) from error
    finally:
        try:
            run.scrape_all()
        except Exception as error:  # the scenario outcome must not be hidden by a late scrape
            print(f"round {number}: final scrape failed: {error}", file=sys.stderr)
        ops.delete_rollout(fixture, name)
    seen = run.wait_connected(args.settle_timeout, f"round {number} reconnect")
    run.scrape_all()
    record = {"round": number, "target": target, "rolloutName": name, "rolloutUid": drive["uid"], "status": drive["status"],
              "before": before, "after": summarize_pods(fixture), "quorum": quorum, "clientsExpected": seen["clientsExpected"],
              "clientsConnected": seen["clientsConnected"], "generator": seen, "will": dict(run.will.totals), "resumed": resumed,
              "startedAt": started, "finishedAt": time.time(), "runIdentity": run.identity, "prevSha256": judge.chain_link(previous)}
    record["problems"] = judge.judge_round(record)
    record["passed"] = not record["problems"]
    write_json(run.rounds_dir / f"round-{number:03d}.json", record)
    if not record["passed"]:
        raise judge.RoundFailed(number, "; ".join(record["problems"]))
    run.save_state(roundInProgress=None)
    return record


def read_audit(run: Run) -> dict[str, Any]:
    rf = rollout_faults()
    session = ops.rollout_session(run.fixture, f"drn019-audit-{run.token}", ops.rollout_target(run.images["A"]), run.args.registry_secret,
                                  run.args.management_secret, helper_image=run.args.helper_image or run.images["A"]["reference"])
    try:
        rf.start_helper(session)
        return rf.read_audit_chain(session)
    finally:
        rf.stop_helper(session)


def collect_generator(run: Run) -> list[dict[str, Any]]:
    """Stop the load, wait for every shard to finalize its ledger and read the final summaries."""
    run.touch("stop")
    deadline = time.monotonic() + run.args.final_timeout
    while True:
        finals = []
        for pod in run.pods().values():
            final = run.read_file(pod, "final.json")
            if final is not None:
                finals.append(final)
        if len(finals) == run.args.shards:
            break
        if time.monotonic() > deadline:
            raise GeneratorLost(f"{len(finals)} of {run.args.shards} generator shards finalized after {run.args.final_timeout:.0f} s")
        time.sleep(10)
    write_json(run.evidence / FINAL_NAME, finals)
    run.touch("collected")
    return finals


def prepare(args: argparse.Namespace, registry: Any) -> dict[str, Any]:
    """Everything that can be decided without a cluster; raises before any cluster access."""
    baseline = judge.load_capacity_baseline(args.capacity_result)
    cutoffs = judge.load_cutoffs(args.cutoffs, baseline)
    if args.shards < 1 or args.shards > baseline["cells"]:
        raise judge.MissingCapacityBaseline(f"--shards must be within 1..{baseline['cells']} cells")
    ops.split_image_reference(args.generator_image)
    images = {"A": registry.verify(args.image_a), "B": registry.verify(args.image_b)}
    problems = judge.judge_image_pair(images["A"], images["B"], baseline)
    if problems:
        raise ops.ReleaseError("; ".join(problems))
    missing = [f for f in baseline["activeFeatures"] if any(f not in i["features"] for i in images.values())]
    if missing:
        raise ops.ReleaseError(f"active features {missing} are not supported by both images")
    return {"baseline": baseline, "cutoffs": cutoffs, "images": images}


# Scenarios: drain-and-rolling-update.md:DRN-SCN-019
@scenario_result
def scenario_rolling_capacity(args: argparse.Namespace, fixture_factory: Callable[[], Any], registry: Any | None = None) -> dict[str, Any]:
    """100 consecutive rolling updates under the measured maximum mixed MQTT traffic; gap, duplicate and reorder judged separately."""
    rf = rollout_faults()
    checks, started = rf.Checks(), time.time()
    registry = registry or ops.RegistryClient(args.registry_tool)
    prepared = prepare(args, registry)  # MissingCapacityBaseline / CutoffError / ReleaseError before any cluster access
    baseline, cutoffs, images = prepared["baseline"], prepared["cutoffs"], prepared["images"]
    checks.check("accepted capacity result with measured B_conn/B_pub/B_sub is present and consistent", True,
                 {"profileId": baseline["profileId"], "profileSha256": baseline["profileSha256"], "bounds": baseline["bounds"], "step": baseline["step"]})
    checks.check("absolute gap/duplicate/reorder cutoffs are decided for this capacity result", True, cutoffs)
    checks.check("image A is the capacity-measured build and image B is a different build inside the release window", True,
                 {"A": images["A"], "B": images["B"]})
    fixture = fixture_factory()
    resume = args.resume
    state_path = Path(fixture.evidence_dir) / STATE_NAME
    token = args.token or time.strftime("%y%m%d%H%M%S", time.gmtime(started))
    saved: dict[str, Any] = {}
    if resume:
        if not state_path.is_file():
            raise RunStateError(f"--resume needs {state_path}")
        saved = json.loads(state_path.read_text(encoding="utf-8"))
        token = saved["token"]
    elif state_path.is_file():
        raise RunStateError(f"{state_path} exists: continue it with --resume or use a new evidence directory")
    run = Run(fixture, args, baseline, cutoffs, images, token)
    interrupted = False
    try:
        cluster = require_cluster(fixture, args, baseline, images["A"])
        checks.check("live fixture is the capacity-measured profile (m7g.large x3 in three AZs, 1 CPU / 4 GiB, 20 GiB gp3, image A)", True, cluster["live"])
        rounds: list[dict[str, Any]] = []
        if resume:
            problems = judge.check_resume_identity(saved, run.identity)
            if problems:
                raise RunStateError("; ".join(problems))
            run.will = ops.WillAccumulator.from_json(saved["will"])
            run.scrape_errors, run.resumed_mid_round = list(saved["scrapeErrors"]), list(saved["resumedMidRound"])
            rounds = run.load_rounds()
            if saved.get("roundInProgress") is not None:
                run.resumed_mid_round.append(saved["roundInProgress"])
            run.pods()  # the generator Job must still be running
        else:
            shard_count = args.shards
            config = generator_config(baseline, int(started) & 0xFFFFFFFF, shard_count, f"{cluster['nlb']}:1883", f"{cluster['nlb']}:8883",
                                      args.server_name, args.horizon, args.settle)
            fixture.apply(*generator_manifests(run.job, token, args.generator_image, shard_count, args.generator_cpu, args.generator_memory,
                                               config, SCRIPT.read_text(encoding="utf-8"), args.job_timeout))
            run.scrape_all(baseline=True)
        ready = run.wait_connected(args.setup_timeout, "ready")
        checks.check(f"load generator holds B_conn: {ready['clientsExpected']} clients connected and subscribed", ready["clientsExpected"] == 53 * baseline["cells"], ready)
        previous = rounds[-1] if rounds else None
        interrupted_round = saved.get("roundInProgress")
        for number in range(len(rounds) + 1, judge.ROUNDS + 1):
            previous = run_round(run, number, previous, resumed=number == interrupted_round)
            rounds.append(previous)
            print(f"round {number}/{judge.ROUNDS} passed", flush=True)
        verdict_rounds = judge.judge_rounds(rounds, run.identity)
        checks.check("100 consecutive rolling updates completed, each with three Ready replaced Pods, quorum and reconnected clients",
                     verdict_rounds["passed"], verdict_rounds)
        finals = collect_generator(run)
        merged = judge.merge_generator_summaries(finals)
        measurement = judge.judge_measurement(merged, cutoffs, baseline)
        checks.check("gap, duplicate and reorder are within the decided absolute cutoffs, measured separately per QoS; retained and Will checks hold",
                     measurement["passed"], {"table": measurement["table"], "problems": measurement["problems"], "counts": merged["counts"], "scalars": merged["scalars"]})
        will = judge.judge_will_outcomes(run.will.totals)
        checks.check("unexpected_supported_client delta is 0 (unplanned_failure reported)", will["passed"], will)
        unobserved = [e for e in run.scrape_errors if e["ready"]]
        checks.check("Will outcome counters were observed continuously (no scrape failed on a Ready Pod, no round resumed mid-way)",
                     not unobserved and not run.resumed_mid_round, {"scrapeErrors": run.scrape_errors, "resumedMidRound": run.resumed_mid_round})
        audit = read_audit(run)
        checks.check("broker Go verifier accepts the controller audit hash chain", audit["verifyExitCode"] == 0, audit["verifyOutput"])
        barrier = {}
        for record in rounds:
            approvals = ops.approvals_of(audit["events"], record["rolloutUid"])
            barrier[record["round"]] = judge.judge_round_approvals(approvals, record["target"]["image"])
        failed = {number: problems for number, problems in barrier.items() if problems}
        checks.check("every round's three deletions passed the safe-to-stop barrier (audit approvals)", not failed, failed)
        result = checks.result("drn_scn_019", runId=fixture.run_id, startedAt=started, finishedAt=time.time(), baseline=baseline, cutoffs=cutoffs,
                               images=images, rounds=[{k: r[k] for k in ("round", "target", "rolloutUid", "problems", "passed", "will", "startedAt", "finishedAt")} for r in rounds],
                               measurement=measurement, merged=merged, willOutcomes=run.will.totals)
        return fixture.save_result(RESULT_NAME, result)
    except KeyboardInterrupt:
        interrupted = True
        raise
    finally:
        cleanup(run, keep_generator=interrupted or args.keep_generator)


def cleanup(run: Run, keep_generator: bool) -> None:
    fixture = run.fixture
    try:
        for item in fixture.kjson("get", "narumqrollout")["items"]:
            if item["metadata"]["name"].startswith("drn019-") and item["metadata"]["name"].endswith(f"-{run.token}"):
                ops.delete_rollout(fixture, item["metadata"]["name"])
    except Exception as error:  # cleanup must not hide the scenario outcome
        print(f"cleanup: deleting NaruMQRollout objects failed: {error}", file=sys.stderr)
    if keep_generator:
        print(f"cleanup: generator Job {run.job} kept for --resume or inspection", file=sys.stderr)
        return
    for kind in ("job", "configmap"):
        try:
            fixture.mutate("delete", kind, run.job, "--ignore-not-found", "--wait=true", "--timeout=300s", timeout=330, check=False)
        except Exception as error:
            print(f"cleanup: deleting {kind} {run.job} failed: {error}", file=sys.stderr)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    add_fixture_arguments(parser)
    parser.add_argument("--capacity-result", type=Path, help="accepted EKS capacity run directory (generator-output/ and controller/)")
    parser.add_argument("--cutoffs", type=Path, help="decided absolute gap/duplicate/reorder cutoffs bound to the capacity profile hash")
    parser.add_argument("--image-a", required=True, help="capacity-measured broker image, repository@sha256:<platform manifest digest>")
    parser.add_argument("--image-b", required=True, help="second broker build inside the release window, repository@sha256:<digest>")
    parser.add_argument("--generator-image", required=True, help="python3 image pinned by digest for the load generator Job")
    parser.add_argument("--server-name", required=True, help="SAN of the capacity fixture's TLS server certificate")
    parser.add_argument("--shards", type=int, required=True, help="generator Pods; cells are split across them (each is one single-threaded process)")
    parser.add_argument("--generator-cpu", default="1")
    parser.add_argument("--generator-memory", default="1Gi")
    parser.add_argument("--registry-tool", choices=("auto", "crane", "docker"), default="auto")
    parser.add_argument("--registry-secret", default="narumq-registry")
    parser.add_argument("--management-secret", default="rollout-management")
    parser.add_argument("--helper-image", default="", help="broker image for the audit reader Pod (default: image A)")
    parser.add_argument("--round-timeout", type=float, default=2700.0, help="seconds one rolling update may take")
    parser.add_argument("--settle-timeout", type=float, default=900.0, help="seconds all clients may take to reconnect after a round")
    parser.add_argument("--setup-timeout", type=float, default=1800.0, help="seconds the generator may take to connect every client")
    parser.add_argument("--final-timeout", type=float, default=900.0, help="seconds the shards may take to finalize their ledgers")
    parser.add_argument("--horizon", type=float, default=300.0, help="seconds after which a missing delivery is decided as a gap")
    parser.add_argument("--settle", type=float, default=60.0, help="seconds the generator keeps receiving after the publishers stop")
    parser.add_argument("--job-timeout", type=int, default=345600, help="activeDeadlineSeconds of the generator Job")
    parser.add_argument("--token", default="", help="DNS-label suffix of the generator and rollout names (default: UTC timestamp)")
    parser.add_argument("--resume", action="store_true", help="continue the run saved in the evidence directory")
    parser.add_argument("--keep-generator", action="store_true", help="leave the generator Job and ConfigMap in place")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = scenario_rolling_capacity(args, lambda: build_fixture(args))
    print(f"DRN-SCN-019 {result['result']}: " + "; ".join(c["check"] for c in result["checks"] if not c["ok"]))
    return 0 if result["result"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
