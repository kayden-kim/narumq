"""Shared access to the strict disposable-EKS ``Fixture`` for the acceptance harnesses under tests/chaos.

``eks-rollout-faults/main.py`` owns ``Fixture``. Its ``verify_identity()`` proves that the kubeconfig context,
the EKS endpoint and tags, and the namespace label all name the same disposable acceptance run, and every
mutation (``Fixture.mutate``) refuses to run before that check passes. Sibling harnesses load that class
instead of copying it, so no harness can touch a cluster the others would refuse.

The run id is the kubeconfig context name. ``--private`` holds ``kubectl.exe`` (or ``kubectl`` on PATH is used)
and ``kubeconfig``. AWS credentials come from the environment (``AWS_PROFILE``, ``AWS_REGION``).
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import sys
from pathlib import Path
from typing import Any

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
ROLLOUT_FAULTS = HERE / "eks-rollout-faults" / "main.py"
MODULE_NAME = "narumq_eks_rollout_faults"
DEFAULT_ACCOUNT = "809446814769"
DEFAULT_REGION = "ap-northeast-2"


def rollout_faults():
    """Import ``eks-rollout-faults/main.py`` once under a unique module name."""
    module = sys.modules.get(MODULE_NAME)
    if module is not None:
        return module
    spec = importlib.util.spec_from_file_location(MODULE_NAME, ROLLOUT_FAULTS)
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot load {ROLLOUT_FAULTS}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[MODULE_NAME] = module
    spec.loader.exec_module(module)
    return module


def add_fixture_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--run-id", required=True, help="kubeconfig context name and the AcceptanceRun tag of the fixture")
    parser.add_argument("--cluster", required=True, help="EKS cluster name of the disposable fixture")
    parser.add_argument("--private", required=True, type=Path, help="directory with kubeconfig and kubectl.exe")
    parser.add_argument("--evidence", required=True, type=Path, help="directory that receives JSON results")
    parser.add_argument("--account", default=DEFAULT_ACCOUNT, help="the only AWS account the harness may use")
    parser.add_argument("--region", default=os.environ.get("AWS_REGION", DEFAULT_REGION))


def build_fixture(args: argparse.Namespace):
    """Build a Fixture and pass identity verification plus the AWS account guard; raises otherwise."""
    module = rollout_faults()
    kubectl = args.private / "kubectl.exe"
    env = {**os.environ, "AWS_PAGER": "", "AWS_REGION": args.region, "AWS_DEFAULT_REGION": args.region}
    args.evidence.mkdir(parents=True, exist_ok=True)
    fixture = module.Fixture(run_id=args.run_id, cluster=args.cluster,
                             kubectl_path=kubectl if kubectl.exists() else Path("kubectl"),
                             kubeconfig=args.private / "kubeconfig", evidence_dir=args.evidence, aws_env=env)
    account = fixture.aws("sts", "get-caller-identity")["Account"]
    if account != args.account:
        raise module.FixtureIdentityError(f"AWS account {account} is not the approved acceptance account {args.account}")
    fixture.verify_identity()
    return fixture


def dump(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, indent=2, default=str)
