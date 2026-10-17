"""Opt-in EKS acceptance for PRF-SCN-060 and PRF-SCN-061.

Not part of the CI unit list. It needs the strict disposable EKS fixture that the other tests/chaos harnesses use
(see tests/chaos/eks_fixture.py): the kubeconfig context, the EKS endpoint and tags and the namespace label must all name
the same acceptance run, and AWS credentials must belong to the approved account. PRF-SCN-060 reads the three running
broker Pods (namespace ``narumq``, app=narumq). PRF-SCN-061 creates one Secret with a unique name and deletes it again.
Nothing else in the cluster is changed.

    set NARUMQ_ACCEPT_EKS_RUN_ID=<kubeconfig context and AcceptanceRun tag>
    set NARUMQ_ACCEPT_EKS_CLUSTER=<EKS cluster name>
    set NARUMQ_ACCEPT_EKS_PRIVATE=<directory with kubeconfig and kubectl.exe>
    python tests/run_scenario_unittest.py tests/acceptance_eks_capacity.py

Optional: NARUMQ_ACCEPT_EKS_EVIDENCE (JSON result directory, default a temporary one), NARUMQ_ACCEPT_EKS_ACCOUNT and
AWS_REGION. Without the three required variables every test is skipped. No AWS resource is created or changed.
"""
from __future__ import annotations

import argparse
import base64
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
import uuid
from pathlib import Path

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE / "chaos"))
import capacity_docker_harness as harness  # noqa: E402  (load_controller only)
import eks_fixture  # noqa: E402

ROOT = HERE.parent
PROFILE = ROOT / "tests/benchmark/capacity/testdata/eks-capacity-v1.json"
REQUIRED = ("NARUMQ_ACCEPT_EKS_RUN_ID", "NARUMQ_ACCEPT_EKS_CLUSTER", "NARUMQ_ACCEPT_EKS_PRIVATE")
ENABLED = all(os.environ.get(name) for name in REQUIRED)
CELL_KEY = re.compile(r"^bp02-[ab]-c(\d{3})$")
LAST_APPLIED = "kubectl.kubernetes.io/last-applied-configuration"


class FixtureKube:
    """``Kubectl`` for the controller code, routed through the identity-verified fixture.

    Reads (get, exec, top) and client-side ``create --dry-run`` pass through. The only mutations are one
    server-side apply and one delete, and only for ``owned_secret``.
    """

    def __init__(self, controller, fixture, owned_secret: str | None = None) -> None:
        self.controller = controller
        self.fixture = fixture
        self.owned_secret = owned_secret

    def _authorize(self, args: list[str], stdin: str | None) -> bool:
        verb = args[0]
        if verb in ("get", "exec", "top"):
            return False
        if verb == "create" and "--dry-run=client" in args:
            return False
        owned = self.owned_secret
        if owned and verb == "apply" and "--server-side" in args and stdin and f"name: {owned}\n" in stdin:
            return True
        if owned and verb == "delete" and args[1:3] == ["secret", owned]:
            return True
        raise self.controller.CommandFailed(f"kubectl {verb} is not allowed by this acceptance harness")

    def run(self, args: list[str], *, stdin: str | None = None, timeout: float = 120, sensitive: bool = False,
            cwd: str | None = None) -> str:
        mutating = self._authorize(args, stdin)
        if mutating and self.fixture.identity is None:
            raise self.controller.CommandFailed("fixture identity was not verified before a mutation")
        result = self.fixture.run(self.fixture.base() + args, stdin=stdin, timeout=timeout, check=False)
        if result.returncode:
            detail = "" if sensitive else ": " + result.stderr.strip()[:300]
            raise self.controller.CommandFailed(f"kubectl {args[0]} exit {result.returncode}{detail}")
        return result.stdout

    def json(self, args: list[str], **kwargs):
        text = self.run(args + ["-o", "json"], **kwargs).strip()
        return json.loads(text) if text else None


@unittest.skipUnless(ENABLED, "set NARUMQ_ACCEPT_EKS_RUN_ID, _CLUSTER and _PRIVATE to run the EKS capacity acceptance")
class EksCapacityAcceptance(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.controller = harness.load_controller()
        evidence = os.environ.get("NARUMQ_ACCEPT_EKS_EVIDENCE")
        cls.evidence = Path(evidence) if evidence else Path(tempfile.mkdtemp(prefix="narumq-prf-eks-evidence-"))
        arguments = argparse.Namespace(
            run_id=os.environ["NARUMQ_ACCEPT_EKS_RUN_ID"], cluster=os.environ["NARUMQ_ACCEPT_EKS_CLUSTER"],
            private=Path(os.environ["NARUMQ_ACCEPT_EKS_PRIVATE"]), evidence=cls.evidence,
            account=os.environ.get("NARUMQ_ACCEPT_EKS_ACCOUNT", eks_fixture.DEFAULT_ACCOUNT),
            region=os.environ.get("AWS_REGION", eks_fixture.DEFAULT_REGION))
        cls.fixture = eks_fixture.build_fixture(arguments)
        assert cls.fixture.namespace == cls.controller.NAMESPACE

    # Scenarios: performance-and-verification.md:PRF-SCN-060
    def test_cgroup_v2_counters_and_deltas_are_read_from_all_three_broker_pods(self):
        controller = self.controller
        kube = FixtureKube(controller, self.fixture)
        directory = Path(tempfile.mkdtemp(prefix="narumq-prf-060-"))
        self.addCleanup(shutil.rmtree, directory, ignore_errors=True)
        journal = controller.Journal(directory / "runtime-health.jsonl")
        diagnostics = controller.RuntimeDiagnostics(kube, journal, "")
        diagnostics.sample("acceptance-first")
        time.sleep(20)
        diagnostics.sample("acceptance-second")
        records = harness.read_jsonl(directory / "runtime-health.jsonl")
        self.assertEqual([record["boundary"] for record in records], ["acceptance-first", "acceptance-second"])
        brokers = [f"narumq-{ordinal}" for ordinal in range(controller.BROKER_REPLICAS)]
        keys = ("nr_periods", "nr_throttled", "throttled_usec")
        for record in records:
            self.assertTrue(record["available"], record.get("reason"))
            self.assertEqual(sorted(item["pod"] for item in record["cpu"]), brokers)
            for item in record["cpu"]:
                self.assertTrue(item["available"], f"{item['pod']}: {item.get('reason')}")
                self.assertEqual(item["counters"]["cgroupVersion"], "v2")
                self.assertTrue(all(isinstance(item["counters"][key], int) and item["counters"][key] >= 0 for key in keys))
        first, second = records
        for item in second["cpu"]:
            before = next(entry for entry in first["cpu"] if entry["pod"] == item["pod"])
            self.assertEqual(item["delta"], {key: item["counters"][key] - before["counters"][key] for key in keys},
                             f"{item['pod']}: no delta between the two samples")
        self.fixture.save_result("prf-scn-060.json", {
            "scenario": "PRF-SCN-060", "brokers": brokers,
            "samples": [{"boundary": record["boundary"], "cpu": record["cpu"]} for record in records]})

    # Scenarios: performance-and-verification.md:PRF-SCN-061
    def test_512_cell_credentials_are_server_side_applied_without_last_applied_and_stay_out_of_output(self):
        controller, fixture = self.controller, self.fixture
        name = f"narumq-prf061-{uuid.uuid4().hex[:8]}"
        kube = FixtureKube(controller, fixture, owned_secret=name)
        directory = Path(tempfile.mkdtemp(prefix="narumq-prf-061-"))
        self.addCleanup(shutil.rmtree, directory, ignore_errors=True)
        generated = subprocess.run(
            ["go", "run", "./tests/benchmark/capacity", "fixture", "--profile", str(PROFILE), "--out", str(directory),
             "--tls-server-name", "narumq-capacity.test"], cwd=ROOT, capture_output=True, text=True, encoding="utf-8",
            timeout=900)
        self.assertEqual(generated.returncode, 0, generated.stderr[-300:])
        credentials = directory / "credentials.json"
        payload = credentials.read_bytes()
        cells = {match.group(1) for key in json.loads(payload) if (match := CELL_KEY.match(key))}
        self.assertEqual(len(cells), 512)
        self.assertTrue(570_000 <= len(payload) <= 600_000, f"credentials.json is {len(payload)} bytes, not about 586 KB")
        secret_values = [value for entry in json.loads(payload).values() for value in entry.values() if isinstance(value, str)]
        self.assertTrue(secret_values)

        def delete_secret():
            kube.run(["delete", "secret", name, "-n", controller.NAMESPACE, "--ignore-not-found"], timeout=120)

        self.addCleanup(delete_secret)
        controller.apply_generated(kube, ["create", "secret", "generic", name, "-n", controller.NAMESPACE,
                                          f"--from-file=credentials.json={credentials}"])
        stored = kube.json(["get", "secret", name, "-n", controller.NAMESPACE], sensitive=True)
        annotations = stored["metadata"].get("annotations") or {}
        self.assertNotIn(LAST_APPLIED, annotations)
        managers = [(entry["manager"], entry["operation"]) for entry in stored["metadata"].get("managedFields", [])]
        self.assertIn(("narumq-capacity-fixture", "Apply"), managers)
        self.assertEqual(base64.b64decode(stored["data"]["credentials.json"]), payload)

        summary = {"scenario": "PRF-SCN-061", "secret": name, "credentialBytes": len(payload), "cells": len(cells),
                   "lastAppliedAnnotation": LAST_APPLIED in annotations, "managedFields": managers}
        fixture.save_result("prf-scn-061.json", summary)
        public = [artifact.read_text(encoding="utf-8") for artifact in fixture.evidence_dir.glob("*.json")]
        for value in secret_values:
            self.assertFalse(any(value in text for text in public), "a credential value reached a public artifact")


if __name__ == "__main__":
    unittest.main()
