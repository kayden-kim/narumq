"""Live EKS acceptance of upgrade and rollback between a published N-1 and N release (DRN-SCN-020; CLU-11, NFR-15, DEP-18).

Question answered: do both directions pass the same safe-to-stop barrier? Per direction the harness drives one real
``NaruMQRollout`` through the rollout controller and judges, from the controller's durable audit hash chain, the
three per-Pod delete approvals: identical evidence field set and conditions in both directions
(``drainSafeToStop`` true, ``connectionsRemaining`` 0, ``pendingWAL`` 0, ``underReplicated`` 0, ``sessionsFailed`` 0,
``drainPhase`` COMPLETED / ``drainState`` SAFE_TO_STOP, approval only after the SAFE_TO_STOP observation), all three
Pods replaced and Ready, the committed cluster profile reporting the expected release on all voters at the end of the
direction, and quorum reported available in every sample taken during the direction.

Published-release prerequisite (never synthesized, GitHub is never called):

* ``--image-n-1`` and ``--image-n`` are digest-pinned ``repository@sha256:...`` OCI platform manifests. Each is read
  from the registry (``crane`` as in the development guide, or ``docker buildx imagetools``): the manifest bytes must
  hash to the digest, be a single ``linux/arm64`` platform manifest (an index is rejected) and carry the OCI config
  labels ``org.opencontainers.image.version`` and ``io.narumq.features``.
* ``--release-record`` is a JSON file ``{"schemaVersion": 1, "releases": [{"version", "digest", "repository",
  "publishedAt", "source": {"type": "github-release", "url": ".../releases/tag/vX.Y.Z"} | {"type": "registry-tag",
  "reference": "repository:tag"}}]}``. Version, digest and repository must equal the image; a registry-tag source is
  resolved in the registry and must lead to the digest. Absent or mismatching evidence raises
  ``MissingPublishedRelease`` naming what is missing.
* The release rules of ADR-0018 apply (same major, minor distance 0 or 1, N later than N-1), and the synthetic ``0.0.x``
  fixture builds are rejected.

Fixture prerequisites: the disposable EKS fixture of ``eks_fixture`` (``--run-id``, ``--cluster``, ``--private`` with
``kubeconfig`` and ``kubectl.exe``, AWS credentials in the environment), the rollout controller Deployment and CRD, the
broker StatefulSet ``narumq`` (``OnDelete``, 3 replicas) running the N-1 digest with all three voters reporting N-1
in ``/api/v1/cluster/features``, the Secrets named by ``--registry-secret`` (default ``narumq-registry``) and
``--management-secret`` (default ``rollout-management``), and no unfinished NaruMQRollout. Every active cluster
feature must be supported by both releases, otherwise the controller (rightly) refuses the rollback.

Mutations: the StatefulSet pod-template image (N, then N-1, as the GitOps path would), two NaruMQRollout objects
(``drn020-<token>-up`` / ``-down``), and one audit-reader helper Pod. Cleanup in ``finally``: the helper Pod and both
NaruMQRollout objects are deleted. The fixture ends where it started, at N-1.

Run (about 2 x 15-45 minutes)::

    python tests/chaos/eks-release-window/main.py --run-id <run> --cluster <eks cluster> --private <dir> \\
        --evidence <dir> --image-n-1 <repo@sha256:...> --image-n <repo@sha256:...> --release-record <file.json>

Offline judgment is covered by ``python -m unittest tests.test_eks_release_window``; nothing in that test starts a
cluster or contacts a registry.
"""

from __future__ import annotations

import argparse
import json
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
import release_window_judge as judge  # noqa: E402
from eks_fixture import add_fixture_arguments, build_fixture, rollout_faults  # noqa: E402
from scenario_results import scenario_result  # noqa: E402

RESULT_NAME = "drn-scn-020-release-window.json"
CRD = "narumqrollouts.narumq.io"


def prepare_release_pair(args: argparse.Namespace, registry: Any, now: float) -> dict[str, Any]:
    """Offline-checkable half: verify both images in the registry and bind each to its published-release record."""
    records = judge.load_release_records(args.release_record)
    images: dict[str, dict[str, Any]] = {}
    published: dict[str, dict[str, Any]] = {}
    for label, reference in (("N-1", args.image_n_1), ("N", args.image_n)):
        image = registry.verify(reference)
        record = next((r for r in records["releases"] if isinstance(r, dict) and r.get("version") == image["version"]), None)
        source = record.get("source") if isinstance(record, dict) else None
        tag_manifest = registry.manifest(source["reference"]) if isinstance(source, dict) and source.get("type") == "registry-tag" \
            and source.get("reference") else None
        published[label] = judge.bind_published(image, records, now, tag_manifest)
        images[label] = image
    problems = judge.judge_release_pair(images["N-1"], images["N"])
    if problems:
        raise judge.ReleasePrerequisiteError("; ".join(problems))
    return {"previous": images["N-1"], "current": images["N"], "published": published}


def require_cluster(fixture: Any, args: argparse.Namespace, pair: dict[str, Any]) -> dict[str, Any]:
    """The fixture starts at N-1 with a working controller and nothing else rolling."""
    rf = rollout_faults()
    for kind, name in (("secret", args.registry_secret), ("secret", args.management_secret)):
        if not fixture.kubectl("get", kind, name, "--ignore-not-found", "-o", "name").strip():
            raise judge.ReleasePrerequisiteError(f"{kind} {name} is missing in namespace {fixture.namespace}")
    if not fixture.kubectl("get", "crd", CRD, "--ignore-not-found", "-o", "name").strip():
        raise judge.ReleasePrerequisiteError(f"CRD {CRD} is not installed")
    controller = fixture.kubectl("get", "deployment", rf.CONTROLLER, "--ignore-not-found", "-o", "json")
    if not controller.strip() or not json.loads(controller).get("status", {}).get("availableReplicas"):
        raise judge.ReleasePrerequisiteError(f"rollout controller Deployment {rf.CONTROLLER} is missing or has no available replica")
    statefulset = fixture.kjson("get", "statefulset", ops.STATEFULSET)
    if statefulset["spec"].get("updateStrategy", {}).get("type") != "OnDelete" or statefulset["spec"].get("replicas") != ops.BROKER_REPLICAS:
        raise judge.ReleasePrerequisiteError("StatefulSet narumq must be OnDelete with 3 replicas")
    unfinished = [r["metadata"]["name"] for r in fixture.kjson("get", "narumqrollout")["items"]
                  if r.get("status", {}).get("phase") not in ops.TERMINAL_PHASES]
    if unfinished:
        raise judge.ReleasePrerequisiteError(f"unfinished NaruMQRollout objects exist: {unfinished}")
    features = ops.features_probe(fixture)
    pods = fixture.broker_pods()
    problems = judge.judge_start_state(features["members"], pods, pair["previous"])
    problems += judge.check_active_features(features["activeFeatures"], pair["previous"], pair["current"])
    if problems:
        raise judge.ReleasePrerequisiteError("fixture does not start at N-1: " + "; ".join(problems))
    return {"features": features, "pods": ops.pod_summary(pods)}


def run_direction(fixture: Any, args: argparse.Namespace, label: str, name: str, target: dict[str, Any]) -> dict[str, Any]:
    """One direction: template image, NaruMQRollout, sampled to a terminal phase; observations only."""
    before = fixture.broker_pods()
    ops.set_template_image(fixture, target["reference"])
    session = ops.rollout_session(fixture, name, ops.rollout_target(target), args.registry_secret, args.management_secret)
    quorum: list[dict[str, Any]] = []

    def observe(status: dict[str, Any], sample: dict[str, Any]) -> dict[str, Any]:
        try:
            probe = ops.cluster_probe(fixture)
        except ops.BrokerProbeError as error:
            probe = {"at": time.time(), "pod": None, "quorumAvailable": None, "error": str(error)}
        quorum.append(probe)
        return {"quorumAvailable": probe["quorumAvailable"]}

    drive = ops.drive_rollout(fixture, session, f"{name}-1", args.timeout, 3.0, observe)
    return {"label": label, "name": name, "before": ops.pod_summary(before), "after": ops.pod_summary(fixture.broker_pods()),
            "features": ops.features_probe(fixture), "quorum": quorum, "drive": drive}


def read_audit(fixture: Any, args: argparse.Namespace, target: dict[str, Any]) -> dict[str, Any]:
    """Verify the controller audit hash chain with the broker binary and return its events (helper Pod, always removed)."""
    rf = rollout_faults()
    session = ops.rollout_session(fixture, "drn020-audit", ops.rollout_target(target), args.registry_secret, args.management_secret,
                                  helper_image=args.helper_image or target["reference"])
    try:
        rf.start_helper(session)
        return rf.read_audit_chain(session)
    finally:
        rf.stop_helper(session)


# Scenarios: drain-and-rolling-update.md:DRN-SCN-020
@scenario_result
def scenario_release_window(args: argparse.Namespace, fixture_factory: Callable[[], Any], registry: Any | None = None) -> dict[str, Any]:
    """Upgrade N-1 -> N and rollback N -> N-1 through the rollout controller; both must pass the same barrier."""
    rf = rollout_faults()
    checks, started = rf.Checks(), time.time()
    registry = registry or ops.RegistryClient(args.registry_tool)
    pair = prepare_release_pair(args, registry, started)  # raises MissingPublishedRelease / ReleaseError before any cluster access
    previous, current = pair["previous"], pair["current"]
    checks.check("N-1 and N are published releases: registry images verified and bound to the release record",
                 True, {"N-1": {"image": previous, "published": pair["published"]["N-1"]},
                        "N": {"image": current, "published": pair["published"]["N"]}})
    fixture = fixture_factory()
    token = args.token or time.strftime("%y%m%d%H%M%S", time.gmtime(started))
    names = {"upgrade": f"drn020-{token}-up", "rollback": f"drn020-{token}-down"}
    runs: dict[str, dict[str, Any]] = {}
    audit: dict[str, Any] = {}
    start_state: dict[str, Any] = {}
    try:
        start_state = require_cluster(fixture, args, pair)
        checks.check("fixture starts at N-1 (three voters report N-1, three Ready Pods on the N-1 digest)", True, start_state)
        runs["upgrade"] = run_direction(fixture, args, "upgrade", names["upgrade"], current)
        runs["rollback"] = run_direction(fixture, args, "rollback", names["rollback"], previous)
        audit = read_audit(fixture, args, current)
        checks.check("broker Go verifier accepts the controller audit hash chain", audit["verifyExitCode"] == 0, audit["verifyOutput"])
        verdicts = {}
        for label, target in (("upgrade", current), ("rollback", previous)):
            run = runs[label]
            approvals = ops.approvals_of(audit["events"], run["drive"]["uid"])
            verdicts[label] = judge.judge_direction(label, target, run["before"], run["after"], run["drive"]["status"], approvals,
                                                    run["features"], run["quorum"])
            checks.check(f"{label}: Pods replaced and Ready, release reported on all voters, quorum available, "
                         "three approvals with the full SAFE_TO_STOP barrier", verdicts[label]["passed"], verdicts[label]["problems"])
        problems = judge.compare_directions(verdicts["upgrade"], verdicts["rollback"])
        checks.check("both directions pass the same safe-to-stop barrier (same evidence fields, conditions and order)", not problems,
                     {"problems": problems, "evidenceKeys": verdicts["upgrade"]["evidenceKeys"]})
        result = checks.result("drn_scn_020", runId=fixture.run_id, startedAt=started, finishedAt=time.time(), pair=pair,
                               startState=start_state, directions={k: {"drive": v["drive"], "before": v["before"], "after": v["after"],
                                                                      "features": v["features"], "quorum": v["quorum"]} for k, v in runs.items()},
                               verdicts=verdicts, auditEvents=audit.get("events", []))
        return fixture.save_result(RESULT_NAME, result)
    finally:
        for name in names.values():
            try:
                ops.delete_rollout(fixture, name)
            except Exception as error:  # cleanup must not hide the scenario outcome
                print(f"cleanup: deleting NaruMQRollout {name} failed: {error}", file=sys.stderr)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    add_fixture_arguments(parser)
    parser.add_argument("--image-n-1", required=True, help="published N-1 broker image, repository@sha256:<platform manifest digest>")
    parser.add_argument("--image-n", required=True, help="published N broker image, repository@sha256:<platform manifest digest>")
    parser.add_argument("--release-record", type=Path, help="JSON published-release record for both images (required)")
    parser.add_argument("--registry-tool", choices=("auto", "crane", "docker"), default="auto")
    parser.add_argument("--registry-secret", default="narumq-registry")
    parser.add_argument("--management-secret", default="rollout-management")
    parser.add_argument("--helper-image", default="", help="broker image used for the audit reader Pod (default: the N image)")
    parser.add_argument("--timeout", type=float, default=2700.0, help="seconds each direction may take")
    parser.add_argument("--token", default="", help="DNS-label suffix for the two rollout names (default: UTC timestamp)")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = scenario_release_window(args, lambda: build_fixture(args))
    print(f"DRN-SCN-020 {result['result']}: " + "; ".join(c["check"] for c in result["checks"] if not c["ok"]))
    return 0 if result["result"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
