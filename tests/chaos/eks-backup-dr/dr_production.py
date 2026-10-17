"""CLU-SCN-159: production storage and backup retention as one composite run.

PASS only when, inside this one run: (1) the real S3 backup prefix shows daily cadence, RPO and >= 30 days of
observed retention under the ADR-0058 bucket controls, (2) the all-voter power-loss run (worker-recovery
`power-loss`, P1) passes, (3) the EBS volume-loss/reseed run (`eks-volume-reseed run`) passes, and (4) the S3 full
restore (CLU-SCN-050 harness) passes with RTO <= 1 hour. The delegated runs are separate processes whose exit code
and summary.json criteria are judged; nothing is accepted from an earlier artifact. Stages run in this order so the
fixture-consuming restore is last; the first unmet stage stops the run and later stages are reported NOT_RUN.
"""
from __future__ import annotations

import datetime
import hashlib
import json
from pathlib import Path
import subprocess
import sys
from typing import Callable

import dr_common as common
import dr_judges as judges
import dr_s3_restore
from dr_common import PlanError, StepFailed, save, timestamp

UNREADABLE_ERRORS = ("CommandFailed", "TimeoutExpired", "JSONDecodeError")  # missing control or empty listing
AUDIT_CRITERIA = ("audit.archivesPresent", "audit.everyGapWithinDaily", "audit.newestWithinRpo",
                  "audit.observedRetentionDays", "audit.publicAccessBlocked", "audit.sseS3", "audit.tlsOnlyPolicy",
                  "audit.lifecycleExpiresAfter30Days", "audit.incompleteMultipartAbortAfter1Day",
                  "audit.sameRegionBucket")
STAGE_CRITERIA = ("stage.powerLossPassed", "stage.volumeReseedPassed", "restore.fullRestorePassed",
                  "restore.rtoWithinApprovedHour")
REQUIRED = AUDIT_CRITERIA + STAGE_CRITERIA
STAGES = ("retention-audit", "power-loss", "volume-reseed", "s3-full-restore")
APPROVED_POLICY = {"backup": "daily", "retentionDays": common.APPROVED_RETENTION_DAYS,
                   "rpoSeconds": common.APPROVED_RPO_SECONDS, "fullClusterRtoSeconds": common.APPROVED_RTO_SECONDS,
                   "source": "ADR-0058, docs/specs/cluster-session-and-delivery.md section 10"}
AWS_READS = {
    "publicAccessBlock": ("s3api", "get-public-access-block"),
    "encryption": ("s3api", "get-bucket-encryption"),
    "lifecycle": ("s3api", "get-bucket-lifecycle-configuration"),
    "policy": ("s3api", "get-bucket-policy"),
    "location": ("s3api", "get-bucket-location"),
}


def load_plan(raw: dict, run_id: str) -> dict:
    """The retention-audit plan; also the carrier of the delegated stage timeout."""
    if not isinstance(raw, dict):
        raise PlanError("plan_not_object")
    common.require_strings(raw, ("runId", "bucket", "prefix", "region"))
    if raw["runId"] != run_id or not common.RUN_ID.fullmatch(run_id):
        raise PlanError("plan_run_id_mismatch")
    plan = dict(raw)
    for key, pattern in (("bucket", dr_s3_restore.BUCKET), ("prefix", dr_s3_restore.PREFIX),
                         ("region", dr_s3_restore.REGION)):
        if not pattern.fullmatch(plan[key]):
            raise PlanError("plan_s3_location_invalid")
    if ".." in plan["prefix"] or "//" in plan["prefix"] or plan["prefix"].endswith("/"):
        raise PlanError("plan_s3_location_invalid")
    plan["minObservedDays"] = common.bounded(plan, "minObservedDays", common.APPROVED_RETENTION_DAYS, (30, 400))
    plan["maxGapSeconds"] = common.bounded(plan, "maxGapSeconds", common.APPROVED_RPO_SECONDS, (3600, 86400))
    plan["gapToleranceSeconds"] = common.bounded(plan, "gapToleranceSeconds", 0, (0, 1800))
    plan["lifecycleSlackSeconds"] = common.bounded(plan, "lifecycleSlackSeconds", 86400, (0, 86400))
    plan["stageTimeoutSeconds"] = common.bounded(plan, "stageTimeoutSeconds", 14400, (600, 43200))
    return plan


def stage_contracts():
    """Late import of the delegated tools' own plan validators and criteria lists."""
    for directory in (common.RECOVERY_DIR, common.VOLUME_RESEED_DIR):
        if str(directory) not in sys.path:
            sys.path.append(str(directory))
    try:
        import power_loss
        import reseed
    except ImportError as exc:
        raise PlanError(f"dependency_missing:{exc.name}") from exc
    return power_loss, reseed


def validate_inputs(plan: dict, restore_raw: dict, power_raw: dict, reseed_raw: dict, run_id: str,
                    contracts=None) -> dict:
    """Validate every stage input before any mutation. Returns the normalised stage plans."""
    if contracts is None:
        power_loss, reseed = stage_contracts()
        try:
            reseed.check_dependencies()
        except reseed.PreflightRejected as exc:
            raise PlanError(exc.code) from exc
    else:
        power_loss, reseed = contracts
    restore = dr_s3_restore.load_plan(restore_raw, run_id)
    if (restore["bucket"], restore["prefix"], restore["region"]) != (plan["bucket"], plan["prefix"], plan["region"]):
        raise PlanError("restore_plan_differs_from_audited_s3_prefix")
    try:
        power_loss.validate_plan(power_raw, run_id)
        reseed_plan = reseed.load_plan(reseed_raw, run_id)
    except (ValueError, reseed.PlanError) as exc:
        raise PlanError(f"stage_plan_invalid:{exc}") from exc
    names = {restore["clusterName"], power_raw.get("clusterName"), reseed_plan.get("clusterName")}
    vpcs = {restore["vpcId"], power_raw.get("vpcId"), reseed_plan.get("vpcId")}
    if len(names) != 1 or len(vpcs) != 1:
        raise PlanError("stage_plans_name_different_fixtures")
    return {"restore": restore, "power": power_raw, "reseed": reseed_plan,
            "powerCriteria": tuple(power_loss.CRITERIA), "reseedCriteria": tuple(reseed.criteria_for(reseed_plan))}


def default_runner(argv: list[str], timeout: float, log_path: Path) -> int | None:
    """Run a delegated tool; stdout/stderr go to a file (the tools print no credentials). None means timeout."""
    with log_path.open("wb") as log:
        process = subprocess.Popen(argv, stdout=log, stderr=subprocess.STDOUT)
        try:
            return process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=30)
            return None


def audit_s3(fixture, plan: dict, now) -> tuple[dict, dict]:
    """Read-only S3 audit with the operator's AWS credentials. Unreadable controls are unmet, not skipped."""
    bucket = plan["bucket"]
    config: dict = {}
    unreadable: list[str] = []
    for name, (service, command) in AWS_READS.items():
        try:
            config[name] = fixture.aws(service, command, "--bucket", bucket)
        except Exception as exc:
            if type(exc).__name__ not in UNREADABLE_ERRORS:
                raise
            config[name] = None
            unreadable.append(name)
    try:
        listed = fixture.aws("s3api", "list-objects-v2", "--bucket", bucket, "--prefix", plan["prefix"] + "/")
        objects = listed.get("Contents") or []
    except Exception as exc:
        if type(exc).__name__ not in UNREADABLE_ERRORS:
            raise
        objects = []
        unreadable.append("listObjects")
    cadence, cadence_evidence = judges.audit_cadence(objects, now, plan)
    controls, control_evidence = judges.audit_bucket(config, plan["prefix"], bucket, plan["region"])
    criteria = {**cadence, **controls}
    return criteria, {"bucket": bucket, "prefix": plan["prefix"], "unreadable": unreadable, "cadence": cadence_evidence,
                      "controls": control_evidence, "approvedPolicy": APPROVED_POLICY}


class ProductionRun:
    def __init__(self, fixture, plan: dict, stage_plans: dict, plan_files: dict[str, Path], private: Path,
                 output: Path, runner: Callable = default_runner, restore_runner: Callable | None = None,
                 audit: Callable = audit_s3, now: Callable | None = None):
        self.fixture = fixture
        self.plan = plan
        self.stage_plans = stage_plans
        self.plan_files = plan_files
        self.private = private
        self.output = output
        self.runner = runner
        self.restore_runner = restore_runner or dr_s3_restore.run
        self.audit = audit
        self.now = now or (lambda: datetime.datetime.now(datetime.timezone.utc))
        self.run_id = plan["runId"]
        self.criteria: dict[str, bool] = {}
        self.stages: dict[str, dict] = {name: {"result": "NOT_RUN"} for name in STAGES}
        self.step = "init"

    def stage_audit(self) -> None:
        self.step = "retention-audit"
        criteria, evidence = self.audit(self.fixture, self.plan, self.now())
        self.criteria.update(criteria)
        save(self.output, "production-retention-audit.json", {**evidence, "criteria": criteria})
        unmet = [name for name in AUDIT_CRITERIA if criteria.get(name) is not True]
        self.stages["retention-audit"] = {"result": "FAIL" if unmet else "PASS", "unmetCriteria": unmet}
        if unmet:
            raise StepFailed(self.step, "retention_audit_unmet:" + ",".join(unmet))

    def delegated(self, stage: str, script: Path, mode: str, plan_file: Path, required: tuple[str, ...],
                  criterion: str) -> None:
        self.step = stage
        directory = self.output / stage
        directory.mkdir(parents=True, exist_ok=False)
        child = directory / self.run_id
        started = timestamp()
        argv = [sys.executable, str(script), mode, "--private", str(self.private), "--output", str(child),
                "--plan", str(plan_file), "--execute"]
        code = self.runner(argv, self.plan["stageTimeoutSeconds"], directory / "process.log")
        summary_path = child / "summary.json"
        summary = json.loads(summary_path.read_text(encoding="utf-8")) if summary_path.is_file() else None
        ok, reason = judges.judge_stage_summary(code, summary, required)
        if ok and (summary.get("runId") != self.run_id or judges.parse_time(summary.get("startedAt", "1970-01-01T00:00:00Z"))
                   < judges.parse_time(started)):
            ok, reason = False, "stage_summary_not_from_this_run"
        self.criteria[criterion] = ok
        digest = hashlib.sha256(summary_path.read_bytes()).hexdigest() if summary_path.is_file() else None
        self.stages[stage] = {"result": "PASS" if ok else "FAIL", "exitCode": code, "reason": reason,
                              "summary": str(summary_path.relative_to(self.output)), "summarySha256": digest,
                              "failedCriteria": (summary or {}).get("failedCriteria"),
                              "errorCode": (summary or {}).get("errorCode"), "startedAt": started}
        if not ok:
            raise StepFailed(stage, reason or "stage_failed")

    def stage_restore(self) -> None:
        self.step = "s3-full-restore"
        directory = self.output / "s3-full-restore"
        directory.mkdir(parents=True, exist_ok=False)
        report = self.restore_runner(self.fixture, self.stage_plans["restore"], directory, self.private)
        save(directory, "summary.json", report)
        passed = report.get("result") == "PASS"
        rto = report.get("rtoSeconds")
        self.criteria["restore.fullRestorePassed"] = passed
        self.criteria["restore.rtoWithinApprovedHour"] = (
            passed and isinstance(rto, (int, float)) and not isinstance(rto, bool) and rto <= common.APPROVED_RTO_SECONDS)
        self.stages["s3-full-restore"] = {"result": "PASS" if passed else "FAIL", "rtoSeconds": rto,
                                          "failedCriteria": report.get("failedCriteria"),
                                          "errorCode": report.get("errorCode"), "summary": "s3-full-restore/summary.json"}
        if not passed or not self.criteria["restore.rtoWithinApprovedHour"]:
            raise StepFailed(self.step, report.get("errorCode") or "restore_unmet")

    def execute(self) -> None:
        self.stage_audit()
        self.delegated("power-loss", common.RECOVERY_DIR / "main.py", "power-loss", self.plan_files["power"],
                       self.stage_plans["powerCriteria"], "stage.powerLossPassed")
        self.delegated("volume-reseed", common.VOLUME_RESEED_DIR / "main.py", "run", self.plan_files["reseed"],
                       self.stage_plans["reseedCriteria"], "stage.volumeReseedPassed")
        self.stage_restore()


def run(fixture, plan: dict, stage_plans: dict, plan_files: dict[str, Path], private: Path, output: Path,
        **kwargs) -> dict:
    report: dict = {"result": "FAIL", "mode": "production-storage-retention", "runId": plan["runId"],
                    "startedAt": timestamp(), "approvedPolicy": APPROVED_POLICY,
                    "scope": ("CLU-SCN-159 composite: real S3 daily cadence/30-day retention audit, all-voter power "
                              "loss, EBS volume loss/reseed and S3 full restore RTO <= 1 h in one run")}
    state = ProductionRun(fixture, plan, stage_plans, plan_files, private, output, **kwargs)
    failed_step = None
    try:
        state.execute()
    except StepFailed as exc:
        report["errorCode"], failed_step = exc.code, exc.step
    except Exception as exc:
        report["errorCode"], failed_step = type(exc).__name__, state.step
    report.update(judges.aggregate(state.criteria, REQUIRED, failed_step))
    report["stages"] = state.stages
    report["finishedAt"] = timestamp()
    return report
