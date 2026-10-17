"""Backup, retention and disaster-recovery acceptance on a disposable EKS fixture.

Subcommands (each needs --execute; every one mutates the fixture it names):

  backup-retention             CLU-SCN-046 + CLU-SCN-047: deployed CronJob policy, manual Job, NMQBKUP3 integrity and
                               commit index, clean-directory three-voter offline seed timing, then one synthetic-aged
                               probe on the real backup PVC. Needs the local (PVC-only) deploy/kubernetes/eks-ec2-ha
                               backup variant. It does NOT claim the full-cluster RTO.
  s3-full-restore              CLU-SCN-050: real S3/IRSA list/download, three NEW claims, three Ready voters with
                               quorum, MQTT session/retained recovery, RTO. Needs the eks-ec2-ha-s3 overlay and
                               consumes the fixture (the cluster ends on restore-data-* claims).
  production-storage-retention CLU-SCN-159: S3 daily cadence / 30-day retention audit + power-loss + EBS volume
                               loss/reseed + s3-full-restore RTO <= 1 h, all in one run with plans from the caller.

The output directory is created by the tool, must not exist and its NAME is the fixture run ID (the kubectl context
in --private/kubeconfig, like the sibling tools). Evidence goes under --output; credentials are read from --private
(admin.token, mqtt.password, kubeconfig, kubectl.exe) and never written. AWS credentials come from the process
environment/profile of the Fixture helper.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.append(str(Path(__file__).resolve().parents[2]))  # tests/ holds scenario_results.py

import dr_production  # noqa: E402
import dr_retention  # noqa: E402
import dr_s3_restore  # noqa: E402
from dr_common import Fixture, PlanError, save, timestamp  # noqa: E402
from scenario_results import scenario_result  # noqa: E402


def _read_json(path: Path, code: str) -> dict:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise PlanError(code) from exc
    return value


def _failed(mode: str, run_id: str, started: str, code: str) -> dict:
    return {"result": "FAIL", "mode": mode, "runId": run_id, "startedAt": started, "errorCode": code,
            "failedStep": "plan", "criteria": {}, "failedCriteria": [], "finishedAt": timestamp()}


def _execute(args, mode: str, runner) -> dict:
    """Create the output directory, build the Fixture and plan, run, and write summary.json."""
    args.output.mkdir(parents=True, exist_ok=False)
    run_id, started = args.output.name, timestamp()
    try:
        raw = _read_json(args.plan, "plan_unreadable")
        fixture = Fixture(args.private, run_id)
        report = runner(args, raw, run_id, fixture)
    except PlanError as exc:
        report = _failed(mode, run_id, started, str(exc))
    except OSError:
        report = _failed(mode, run_id, started, "private_directory_incomplete")
    except Exception as exc:
        report = _failed(mode, run_id, started, type(exc).__name__)
    save(args.output, "summary.json", report)
    print(json.dumps(report))
    return report


def _backup_retention(args, raw: dict, run_id: str, fixture) -> dict:
    plan = dr_retention.load_plan(raw, run_id)
    if plan["expectedCommitIndex"] != 100:
        raise PlanError("scenario_requires_commit_index_100")
    return dr_retention.run(fixture, plan, args.output, args.private)


# Scenarios: cluster-session-and-delivery.md:CLU-SCN-046, cluster-session-and-delivery.md:CLU-SCN-047
@scenario_result
def scenario_backup_retention(args) -> dict:
    """Run the live CronJob policy, online backup, offline seed and synthetic-aged probe checks."""
    return _execute(args, "backup-retention", _backup_retention)


# Scenarios: cluster-session-and-delivery.md:CLU-SCN-050
@scenario_result
def scenario_s3_full_restore(args) -> dict:
    """Run the real S3/IRSA full restore onto new PVCs and judge Ready, quorum, MQTT recovery and RTO."""
    return _execute(args, "s3-full-restore", lambda a, raw, run_id, fixture: dr_s3_restore.run(
        fixture, dr_s3_restore.load_plan(raw, run_id), a.output, a.private))


def _production(args, raw: dict, run_id: str, fixture) -> dict:
    plan = dr_production.load_plan(raw, run_id)
    stage_plans = dr_production.validate_inputs(
        plan, _read_json(args.restore_plan, "restore_plan_unreadable"),
        _read_json(args.power_loss_plan, "power_loss_plan_unreadable"),
        _read_json(args.reseed_plan, "reseed_plan_unreadable"), run_id)
    files = {"power": args.power_loss_plan.resolve(), "reseed": args.reseed_plan.resolve()}
    return dr_production.run(fixture, plan, stage_plans, files, args.private.resolve(), args.output.resolve())


# Scenarios: cluster-session-and-delivery.md:CLU-SCN-159
@scenario_result
def scenario_production_storage_retention(args) -> dict:
    """Judge S3 retention/RPO, all-voter power loss, EBS loss/reseed and the full-restore RTO in one run."""
    return _execute(args, "production-storage-retention", _production)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="mode", required=True)
    for name in ("backup-retention", "s3-full-restore", "production-storage-retention"):
        command = sub.add_parser(name)
        command.add_argument("--private", type=Path, required=True, help="directory with kubeconfig, kubectl.exe, tokens")
        command.add_argument("--output", type=Path, required=True,
                             help="new directory; its name is the fixture run ID")
        command.add_argument("--plan", type=Path, required=True)
        command.add_argument("--execute", action="store_true", help="allow the mutations of the checked fixture")
        if name == "production-storage-retention":
            command.add_argument("--restore-plan", type=Path, required=True, help="s3-full-restore plan (same S3 prefix)")
            command.add_argument("--power-loss-plan", type=Path, required=True, help="worker-recovery power-loss plan")
            command.add_argument("--reseed-plan", type=Path, required=True, help="eks-volume-reseed run plan")
    return parser


def main(argv=None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not args.execute:
        parser.error(f"{args.mode} requires --execute")
    scenario = {"backup-retention": scenario_backup_retention, "s3-full-restore": scenario_s3_full_restore,
                "production-storage-retention": scenario_production_storage_retention}[args.mode]
    return 0 if scenario(args)["result"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
