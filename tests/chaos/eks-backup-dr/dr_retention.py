"""CLU-SCN-046 and CLU-SCN-047 on one disposable EKS fixture with the local (PVC) backup variant.

Steps: ownership, deployed CronJob and script policy, manual Job from the CronJob, NMQBKUP3 integrity and commit
index of the new archive, clean-directory three-voter offline seed from the cluster ConfigMaps (timed), then one
empty synthetic-aged `backup-*.nrbak` probe on the real backup PVC and a second Job that must delete only it.
The full-cluster RTO is not claimed here: the seed timing stops before any voter starts.
"""
from __future__ import annotations

import datetime
import hashlib
from pathlib import Path
import time

import dr_archive
import dr_common as common
import dr_judges as judges
from dr_common import PlanError, StepFailed, kube_json, save, timestamp

PROBE_ARCHIVE_NAME_PREFIX = "backup-"
EXPECTED_COMMIT_INDEX_DEFAULT = 100
REQUIRED = (
    "fixture.ownedDisposable",
    "cronjob.schedule", "cronjob.notSuspended", "cronjob.runsDeployedScript", "cronjob.usesBackupPvc",
    "cronjob.localOnlyVariant", "cronjob.scriptMatchesRepository", "cronjob.localMtimePlus30Cleanup",
    "backup.jobSucceeded", "archive.nmqbkup3Integrity", "archive.formatProtocolSchema",
    "archive.commitIndexAsPlanned", "archive.logMatchesManifest",
    "seed.podSucceeded", "seed.threeNodesSeeded", "seed.cleanDirectories", "seed.expectedFilesPresent",
    "seed.noRestoreMarker", "seed.snapshotAtCommitIndex", "seed.withinRtoBudget",
    "probe.createdEmptyWithSyntheticMtime", "probe.singleAgedArchiveBeforeJob",
    "retention.jobSucceeded", "retention.probeDeleted", "retention.existingArchivesPreserved",
    "retention.newArchiveCreated", "retention.newArchiveIntegrity", "retention.noTemporaryLeftovers",
    "cleanup.ownedResourcesRemoved",
)

INVENTORY_SCRIPT = r'''
set -eu
echo "NOW $(date -u +%s)"
for path in /backups/* /backups/.[!.]* /backups/..?*; do
  [ -e "$path" ] || [ -L "$path" ] || continue
  printf 'ENTRY|%s|%s|%s|%s\n' "${path##*/}" "$(stat -c %F "$path")" "$(stat -c %s "$path")" "$(stat -c %Y "$path")"
done
for path in $(find /backups -maxdepth 1 -type f -name 'backup-*.nrbak' -mtime +30); do
  echo "AGED|${path##*/}"
done
if [ -n "${DUMP_NAME:-}" ]; then
  case "$DUMP_NAME" in
    backup-[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9].nrbak) ;;
    *) echo "invalid DUMP_NAME" >&2; exit 2 ;;
  esac
  [ -f "/backups/$DUMP_NAME" ] || { echo "archive missing" >&2; exit 3; }
  [ "$(stat -c %s "/backups/$DUMP_NAME")" -le "$MAX_DUMP_BYTES" ] || { echo "archive too large" >&2; exit 4; }
  echo "ARCHIVE-BEGIN $DUMP_NAME"
  base64 "/backups/$DUMP_NAME"
  echo "ARCHIVE-END"
fi
'''

PROBE_SCRIPT = r'''
set -eu
export TZ=UTC
case "$PROBE_AGE_DAYS" in ''|*[!0-9]*) echo "invalid age" >&2; exit 2 ;; esac
NOW=$(date -u +%s)
SYN=$((NOW - PROBE_AGE_DAYS * 86400))
NAME="backup-$(date -u -d "@$SYN" +%Y%m%d%H%M%S).nrbak"
TARGET="/backups/$NAME"
[ ! -e "$TARGET" ] && [ ! -L "$TARGET" ] || { echo "probe path exists" >&2; exit 3; }
( set -C; : > "$TARGET" )
touch -t "$(date -u -d "@$SYN" +%Y%m%d%H%M.%S)" "$TARGET"
echo "PROBE name=$NAME size=$(wc -c < "$TARGET" | tr -d ' ') mtime=$(stat -c %Y "$TARGET") expected=$SYN now=$NOW"
'''

REMOVE_PROBE_SCRIPT = r'''
set -eu
case "$PROBE_NAME" in
  backup-[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9].nrbak) ;;
  *) echo "invalid probe name" >&2; exit 2 ;;
esac
if [ -f "/backups/$PROBE_NAME" ] && [ ! -s "/backups/$PROBE_NAME" ]; then
  rm -f "/backups/$PROBE_NAME"
  echo "PROBE-REMOVED $PROBE_NAME"
else
  echo "probe is missing or not empty; left untouched" >&2
  exit 4
fi
'''

SEED_SCRIPT = r'''
set -u
case "$ARCHIVE_NAME" in
  backup-[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9].nrbak) ;;
  *) echo "invalid ARCHIVE_NAME" >&2; exit 2 ;;
esac
ARCHIVE="/backups/$ARCHIVE_NAME"
[ -f "$ARCHIVE" ] || { echo "archive missing" >&2; exit 3; }
uptime_s() { read -r up _ < /proc/uptime; echo "$up"; }
status=0
for n in $NODE_IDS; do
  base="/work/node$n"
  mkdir -p "$base/data" "$base/audit"
  clean=yes
  [ -z "$(ls -A "$base/data")" ] || clean=no
  sed -e "s#\"directory\": \"/data\"#\"directory\": \"$base/data\"#" \
      -e "s#\"auditDirectory\": \"/audit\"#\"auditDirectory\": \"$base/audit\"#" \
      "/configs/node-$n.json" > "/work/config-$n.json"
  if ! grep -q "\"directory\": \"$base/data\"" "/work/config-$n.json" \
      || ! grep -q "\"auditDirectory\": \"$base/audit\"" "/work/config-$n.json"; then
    echo "config rewrite failed for node $n" >&2; status=1; continue
  fi
  start=$(uptime_s)
  /narumq -cluster-config "/work/config-$n.json" -cluster-restore "$ARCHIVE" > "/work/restore-$n.log" 2>&1
  code=$?
  end=$(uptime_s)
  echo "SEED node=$n pre-clean=$clean exit=$code start=$start end=$end"
  [ "$code" = 0 ] || { status=1; echo "restore failed for node $n: $(head -c 300 "/work/restore-$n.log" | tr '\n' ' ')" >&2; }
  for f in "$base"/data/* "$base"/data/.[!.]* ; do
    [ -e "$f" ] && echo "FILE node=$n name=${f##*/} size=$(wc -c < "$f" | tr -d ' ')"
  done
done
exit $status
'''


def load_plan(raw: dict, run_id: str) -> dict:
    common.validate_fixture_identity(raw, run_id)
    plan = dict(raw)
    commit = plan.get("expectedCommitIndex", EXPECTED_COMMIT_INDEX_DEFAULT)
    if isinstance(commit, bool) or not isinstance(commit, int) or commit < 1:
        raise PlanError("plan_expectedCommitIndex_invalid")
    plan["expectedCommitIndex"] = commit
    plan["probeAgeDays"] = common.bounded(plan, "probeAgeDays", 32, (judges.MIN_PROBE_AGE_DAYS, 400))
    plan["seedBudgetSeconds"] = common.bounded(plan, "seedBudgetSeconds", common.APPROVED_RTO_SECONDS,
                                               (1, common.APPROVED_RTO_SECONDS))
    plan["jobTimeoutSeconds"] = common.bounded(plan, "jobTimeoutSeconds", 900, (60, 3600))
    plan["podTimeoutSeconds"] = common.bounded(plan, "podTimeoutSeconds", 600, (60, 1800))
    plan["maxArchiveBytes"] = common.bounded(plan, "maxArchiveBytes", 16 << 20, (1024, 64 << 20))
    plan["scheduleGuardMinutes"] = common.bounded(plan, "scheduleGuardMinutes", 15, (0, 120))
    return plan


def minutes_from_schedule(now: datetime.datetime) -> float:
    """Distance in minutes from `now` (UTC) to the nearest 02:00 UTC schedule tick."""
    today = now.replace(hour=2, minute=0, second=0, microsecond=0)
    return min(abs((now - candidate).total_seconds()) for candidate in (
        today - datetime.timedelta(days=1), today, today + datetime.timedelta(days=1))) / 60


class RetentionRun:
    def __init__(self, fixture, plan: dict, output: Path, private: Path, sleep=time.sleep, monotonic=time.monotonic,
                 utcnow=None, repository_script: str | None = None):
        self.fixture = fixture
        self.plan = plan
        self.output = output
        self.private = private
        self.sleep = sleep
        self.monotonic = monotonic
        self.utcnow = utcnow or (lambda: datetime.datetime.now(datetime.timezone.utc))
        self.repository_script = (repository_script if repository_script is not None
                                  else common.BACKUP_SCRIPT.read_text(encoding="utf-8"))
        self.suffix = hashlib.sha256(plan["runId"].encode()).hexdigest()[:8]
        self.criteria: dict[str, bool] = {}
        self.evidence: dict[str, dict] = {}
        self.step = "init"
        self.image = ""
        self.jobs: list[str] = []
        self.probe_name: str | None = None
        self.probe_confirmed_deleted = False
        self.pod_counter = 0
        self.log_commit = 0
        self.commit_index = 0
        self.archive_name = ""
        self.preflight_inventory: dict = {}

    # -- plumbing --------------------------------------------------------------------------------------

    def record(self, name: str, value: dict) -> None:
        self.evidence[name] = value
        save(self.output, f"retention-{name}.json", value)

    def fail(self, code: str):
        raise StepFailed(self.step, code)

    def pod(self, label: str, script: str, *, env: dict | None = None, read_only: bool = True,
            configmaps: dict | None = None, scratch: dict | None = None, memory: str = "256Mi") -> dict:
        self.pod_counter += 1
        name = f"narumq-dr-{label}-{self.suffix}-{self.pod_counter}"
        manifest = common.helper_pod(name, self.plan["runId"], self.image, script, env=env,
                                     backup_pvc_read_only=read_only, configmaps=configmaps, scratch=scratch,
                                     memory=memory)
        return common.run_pod(self.fixture, manifest, self.plan["podTimeoutSeconds"], self.sleep, self.monotonic)

    def inventory(self, label: str, dump: str | None = None) -> dict:
        env = {"DUMP_NAME": dump or "", "MAX_DUMP_BYTES": str(self.plan["maxArchiveBytes"])}
        result = self.pod(label, INVENTORY_SCRIPT, env=env)
        if result["phase"] != "Succeeded":
            self.fail("inventory_pod_failed")
        return judges.parse_inventory(result["logs"])

    def backup_job(self, number: int) -> tuple[dict, bool]:
        name = f"narumq-backup-dr-{self.suffix}-{number}"
        common.create_job_from_cronjob(self.fixture, name)
        self.jobs.append(name)
        outcome = common.wait_job(self.fixture, name, self.plan["jobTimeoutSeconds"], self.sleep, self.monotonic)
        if outcome is None:
            self.fail(f"backup_job_timeout:{name}")
        logs = common.job_logs(self.fixture, name, "backup") if outcome["succeeded"] else ""
        return {"job": name, "logs": logs, "succeeded": outcome["succeeded"],
                "status": outcome["job"].get("status", {})}, outcome["succeeded"]

    # -- steps -----------------------------------------------------------------------------------------

    def step_fixture(self) -> None:
        self.step = "fixture"
        self.record("fixture", common.require_owned_cluster(self.fixture, self.plan))
        self.criteria["fixture.ownedDisposable"] = True

    def step_policy(self) -> None:
        self.step = "policy"
        cronjob = kube_json(self.fixture, "cronjob", common.CRONJOB)
        scripts = kube_json(self.fixture, "configmap", common.SCRIPTS_CONFIGMAP)
        criteria, evidence = judges.cronjob_policy(cronjob, scripts, self.repository_script, common.APPROVED_SCHEDULE)
        self.criteria.update(criteria)
        self.image = evidence["image"] or ""
        self.record("policy", {**evidence, "criteria": criteria})
        if not all(criteria.values()) or not self.image:
            self.fail("cronjob_policy_unmet")

    def step_preflight(self) -> None:
        self.step = "preflight"
        if not common.three_brokers_ready(common.broker_pods(self.fixture)):
            self.fail("brokers_not_three_ready")
        pvc = kube_json(self.fixture, "pvc", common.BACKUP_PVC)
        if pvc["status"].get("phase") != "Bound":
            self.fail("backup_pvc_not_bound")
        active = [job["metadata"]["name"] for job in kube_json(self.fixture, "jobs")["items"]
                  if job.get("status", {}).get("active")]
        if active:
            self.fail("backup_job_already_active")
        using = [pod["metadata"]["name"] for pod in kube_json(self.fixture, "pods")["items"]
                 if pod["status"].get("phase") in ("Pending", "Running") and any(
                     v.get("persistentVolumeClaim", {}).get("claimName") == common.BACKUP_PVC
                     for v in pod["spec"].get("volumes", []))]
        if using:
            self.fail("backup_pvc_in_use")
        distance = minutes_from_schedule(self.utcnow())
        if distance < self.plan["scheduleGuardMinutes"]:
            self.fail("scheduled_backup_window_too_close")
        before = self.inventory("before")
        self.record("preflight", {"pvc": pvc["metadata"]["name"], "pvcUID": pvc["metadata"]["uid"],
                                  "minutesFromScheduleTick": round(distance, 1),
                                  "entries": before["entries"], "agedArchives": before["aged"]})
        if before["aged"]:
            self.fail("preexisting_aged_archives")
        self.preflight_inventory = before

    def step_backup(self) -> None:
        self.step = "backup"
        result, ok = self.backup_job(1)
        self.criteria["backup.jobSucceeded"] = ok
        self.record("backup", {"job": result["job"], "succeeded": ok, "log": result["logs"][-2000:]})
        if not ok:
            self.fail("backup_job_failed")
        try:
            self.log_commit, self.archive_name = judges.parse_local_backup_log(result["logs"])
        except ValueError as exc:
            self.fail(str(exc))

    def step_archive(self) -> None:
        self.step = "archive"
        inventory = self.inventory("archive", dump=self.archive_name)
        if inventory["archive"] is None:
            self.fail("archive_not_returned")
        try:
            facts = dr_archive.parse_archive(inventory["archive"])
            self.criteria["archive.nmqbkup3Integrity"] = True
        except dr_archive.ArchiveError as exc:
            self.criteria["archive.nmqbkup3Integrity"] = False
            self.record("archive", {"name": self.archive_name, "errorCode": str(exc)})
            self.fail(str(exc))
        self.criteria["archive.formatProtocolSchema"] = (
            (facts["formatMajor"], facts["formatMinor"], facts["clusterProtocol"], facts["stateSchema"]) == (3, 0, "2.0", "1.0"))
        self.criteria["archive.commitIndexAsPlanned"] = facts["commitIndex"] == self.plan["expectedCommitIndex"]
        self.criteria["archive.logMatchesManifest"] = facts["commitIndex"] == self.log_commit
        self.commit_index = facts["commitIndex"]
        self.record("archive", {"name": self.archive_name, "expectedCommitIndex": self.plan["expectedCommitIndex"],
                                "logCommitIndex": self.log_commit, **facts})

    def step_seed(self) -> None:
        self.step = "seed"
        slots = kube_json(self.fixture, "configmap", common.CONFIGS_CONFIGMAP)["data"]
        node_ids = [slots.get(f"slot-{ordinal}", "") for ordinal in range(3)]
        if not all(value.isdigit() and not value.startswith("0") for value in node_ids) or len(set(node_ids)) != 3:
            self.fail("cluster_configmap_slots_invalid")
        result = self.pod("seed", SEED_SCRIPT, read_only=True,
                          env={"ARCHIVE_NAME": self.archive_name, "NODE_IDS": " ".join(node_ids)},
                          configmaps={"configs": "/configs"}, scratch={"work": "/work"}, memory="1Gi")
        nodes = judges.parse_seed_logs(result["logs"])
        criteria, evidence = judges.judge_seed(nodes, [int(v) for v in node_ids], self.commit_index,
                                               self.plan["seedBudgetSeconds"])
        criteria["seed.podSucceeded"] = result["phase"] == "Succeeded"
        self.criteria.update(criteria)
        self.record("seed", {**evidence, "pod": result["name"], "podPhase": result["phase"],
                             "configSource": f"configmap/{common.CONFIGS_CONFIGMAP}", "archive": self.archive_name,
                             "nodeIds": node_ids})

    def step_probe(self) -> None:
        self.step = "probe"
        before = self.inventory("pre-probe")
        probe_pod = self.pod("probe", PROBE_SCRIPT, env={"PROBE_AGE_DAYS": str(self.plan["probeAgeDays"])},
                             read_only=False)
        if probe_pod["phase"] != "Succeeded":
            self.fail("probe_creation_failed")
        probe = judges.parse_probe_line(probe_pod["logs"])
        self.probe_name = probe["name"]
        with_probe = self.inventory("with-probe")
        creation = judges.judge_probe_creation(probe, with_probe, before, self.plan["probeAgeDays"])
        self.criteria.update(creation)
        self.record("probe", {**probe, "criteria": creation, "agedBeforeJob": with_probe["aged"]})
        if not all(creation.values()):
            self.fail("probe_preconditions_unmet")
        result, ok = self.backup_job(2)
        self.record("retention-job", {"job": result["job"], "succeeded": ok, "log": result["logs"][-2000:]})
        logged = None
        if ok:
            try:
                _, logged = judges.parse_local_backup_log(result["logs"])
            except ValueError:
                logged = None
        after = self.inventory("after", dump=logged)
        valid = False
        if after["archive"] is not None:
            try:
                valid = dr_archive.parse_archive(after["archive"])["magic"] == "NMQBKUP3"
            except dr_archive.ArchiveError:
                valid = False
        criteria, evidence = judges.judge_retention(before, probe["name"], after, logged, ok, valid)
        self.criteria.update(criteria)
        self.probe_confirmed_deleted = probe["name"] not in after["entries"]
        self.record("retention", {**evidence, "criteria": criteria, "entriesAfter": after["entries"],
                                  "newArchive": logged})

    # -- teardown --------------------------------------------------------------------------------------

    def cleanup(self, report: dict) -> None:
        errors: dict[str, str] = {}
        if self.probe_name and not self.probe_confirmed_deleted and self.image:
            try:
                result = self.pod("remove-probe", REMOVE_PROBE_SCRIPT, env={"PROBE_NAME": self.probe_name}, read_only=False)
                report["probeRemovedByCleanup"] = result["phase"] == "Succeeded"
                if result["phase"] != "Succeeded":
                    errors["probe"] = "remove_pod_failed"
            except Exception as exc:
                errors["probe"] = type(exc).__name__
        for name in self.jobs:
            try:
                common.kube_delete(self.fixture, "job", name)
            except Exception as exc:
                errors[f"job:{name}"] = type(exc).__name__
        self.criteria["cleanup.ownedResourcesRemoved"] = not errors
        if errors:
            report["teardownErrors"] = errors
            report["teardownFailed"] = True

    def execute(self) -> None:
        for step in (self.step_fixture, self.step_policy, self.step_preflight, self.step_backup, self.step_archive,
                     self.step_seed, self.step_probe):
            step()


def run(fixture, plan: dict, output: Path, private: Path, **kwargs) -> dict:
    """Execute and judge one retention run; the report is returned, never raised."""
    report: dict = {"result": "FAIL", "mode": "backup-retention", "runId": plan["runId"], "startedAt": timestamp(),
                    "scope": ("CLU-SCN-046 live EKS backup + offline seed (no voter start, no full-cluster RTO) and "
                              "CLU-SCN-047 single synthetic-aged probe on the real backup PVC")}
    run_state = RetentionRun(fixture, plan, output, private, **kwargs)
    failed_step = None
    try:
        run_state.execute()
    except StepFailed as exc:
        report["errorCode"], failed_step = exc.code, exc.step
    except Exception as exc:
        report["errorCode"], failed_step = type(exc).__name__, run_state.step
    finally:
        run_state.cleanup(report)
    judged = judges.aggregate(run_state.criteria, REQUIRED, failed_step)
    if report.get("teardownFailed"):
        judged["result"] = "FAIL"
    report.update(judged)
    report["steps"] = sorted(run_state.evidence)
    report["finishedAt"] = timestamp()
    return report
