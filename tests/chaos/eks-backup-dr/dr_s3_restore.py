"""CLU-SCN-050 on one disposable EKS fixture with the S3/IRSA backup overlay.

Sequence: persistent MQTT session + retained marker on the source cluster, a fresh S3 backup Job from the deployed
CronJob, source brokers stopped (the simulated disaster), then the timed restore: the deployed list Job, the exact
object key, three NEW claims seeded by the deployed seed Jobs (existing `data-narumq-*` claims are never touched),
the StatefulSet re-created on `restore-data-*` claims, three Ready voters with quorum and MQTT session/retained
recovery without publishing again. RTO is measured from the list Job to the verified MQTT recovery.
The fixture is consumed: afterwards the cluster runs on the restored claims.
"""
from __future__ import annotations

import datetime
import hashlib
import json
from pathlib import Path
import re
import time

import dr_common as common
import dr_judges as judges
import dr_restore_manifests as manifests
from dr_common import PlanError, StepFailed, kube, kube_json, save, timestamp
from dr_retention import minutes_from_schedule

REQUIRED = (
    "fixture.ownedDisposable", "config.s3WiredAsPlanned", "source.sessionAndRetainedPrepared", "backup.uploadedToS3",
    "source.stopped", "list.exactKeyListedUnderPrefix", "targets.threeNewClaimsBound", "seed.threeJobsSucceeded",
    "download.exactKeyVerified", "oldData.claimsPreserved", "voters.threeReadyOnNewClaims", "quorum.threeReadyVoters",
    "mqtt.sessionPresent", "mqtt.retainedSha256", "rto.withinBudget", "cronjob.resumedAfterVerification",
    "cleanup.ownedResourcesRemoved",
)
OBSERVER_PORTS = (23880, 23883)
SESSION_CLIENT = "dr-restore-subscriber"
PUBLISHER_CLIENT = "dr-restore-publisher"
READER_CLIENT = "dr-restore-reader"
SESSION_EXPIRY_SECONDS = 86400
BUCKET = re.compile(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]")
PREFIX = re.compile(r"[A-Za-z0-9][A-Za-z0-9._/-]*")
REGION = re.compile(r"[a-z0-9-]+")
S3_CONFIGMAP = "narumq-backup-config"
REQUEST_CONFIGMAP = "narumq-backup-restore-request"
OLD_CLAIMS = [f"data-narumq-{ordinal}" for ordinal in range(3)]


def load_plan(raw: dict, run_id: str) -> dict:
    common.validate_fixture_identity(raw, run_id)
    plan = dict(raw)
    common.require_strings(plan, ("bucket", "prefix", "region"))
    if (not BUCKET.fullmatch(plan["bucket"]) or not PREFIX.fullmatch(plan["prefix"]) or ".." in plan["prefix"]
            or "//" in plan["prefix"] or plan["prefix"].endswith("/") or not REGION.fullmatch(plan["region"])):
        raise PlanError("plan_s3_location_invalid")
    plan["rtoBudgetSeconds"] = common.bounded(plan, "rtoBudgetSeconds", common.APPROVED_RTO_SECONDS,
                                              (60, common.APPROVED_RTO_SECONDS))
    plan["jobTimeoutSeconds"] = common.bounded(plan, "jobTimeoutSeconds", 900, (60, 3600))
    plan["sourceStopSeconds"] = common.bounded(plan, "sourceStopSeconds", 900, (60, 1800))
    plan["seedTimeoutSeconds"] = common.bounded(plan, "seedTimeoutSeconds", 1800, (120, 3600))
    plan["readyDeadlineSeconds"] = common.bounded(plan, "readyDeadlineSeconds", 900, (120, 1800))
    plan["scheduleGuardMinutes"] = common.bounded(plan, "scheduleGuardMinutes", 15, (0, 120))
    uids = plan.get("dataPvcUids")
    if uids is not None and (not isinstance(uids, dict) or sorted(uids) != OLD_CLAIMS
                             or not all(isinstance(v, str) and v for v in uids.values())):
        raise PlanError("plan_dataPvcUids_invalid")
    return plan


# -- MQTT session and retained marker -----------------------------------------------------------------------

def marker_topic(run_id: str) -> str:
    return f"acceptance/{run_id}/dr/retained"


def marker_payload(run_id: str) -> bytes:
    return f"{run_id}:dr-restore-marker".encode("ascii")


def _client_factory():
    import payload_oracle  # imported late: paho is only needed for a live run
    return payload_oracle.Client


def mqtt_prepare(private: Path, run_id: str, port: int, factory=None) -> dict:
    """Persistent session with one subscription, then a retained QoS 1 marker; PUBACK is required."""
    factory = factory or _client_factory()
    password = (private / "mqtt.password").read_text(encoding="utf-8").strip()
    topic, payload = marker_topic(run_id), marker_payload(run_id)
    subscriber = factory(SESSION_CLIENT, password, port, True, SESSION_EXPIRY_SECONDS)
    try:
        subscriber.subscribe(topic)
    finally:
        subscriber.close()
    publisher = factory(PUBLISHER_CLIENT, password, port, True, 0)
    try:
        publisher.publish(topic, payload, retain=True)
        acks = list(publisher.acks)
    finally:
        publisher.close()
    if len(acks) != 1 or acks[0]["reasonValue"] >= 0x80:
        raise StepFailed("markers", "marker_puback_missing_or_refused")
    return {"topic": topic, "sha256": hashlib.sha256(payload).hexdigest(), "sessionClientId": SESSION_CLIENT,
            "sessionExpirySeconds": SESSION_EXPIRY_SECONDS, "pubackReasonCode": acks[0]["reasonValue"]}


def mqtt_verify(private: Path, run_id: str, port: int, expected_sha256: str, factory=None) -> dict:
    """Resume the persistent session and read the retained marker. Nothing is published."""
    factory = factory or _client_factory()
    password = (private / "mqtt.password").read_text(encoding="utf-8").strip()
    topic = marker_topic(run_id)
    session_present = False
    resumed = factory(SESSION_CLIENT, password, port, False, SESSION_EXPIRY_SECONDS)
    try:
        session_present = resumed.session_present is True
    finally:
        resumed.close()
    reader = factory(READER_CLIENT, password, port, True, 0)
    try:
        reader.subscribe(topic)
        arrived = reader.received.wait(15)
        found = [m for m in reader.messages if m["topic"] == topic and m["retain"]]
    finally:
        reader.close()
    hashes = sorted({m["sha256"] for m in found})
    return {"sessionPresent": session_present, "retainedArrived": bool(arrived and found),
            "retainedSha256": hashes[0] if len(hashes) == 1 else None, "retainedMessages": len(found),
            "retainedMatches": bool(found) and hashes == [expected_sha256]}


# -- run ----------------------------------------------------------------------------------------------------------

def pod_claims(pod: dict) -> dict[str, str]:
    return {v["name"]: v["persistentVolumeClaim"]["claimName"] for v in pod["spec"].get("volumes", [])
            if "persistentVolumeClaim" in v}


class S3RestoreRun:
    def __init__(self, fixture, plan: dict, output: Path, private: Path, sleep=time.sleep, monotonic=time.monotonic,
                 utcnow=None, client_factory=None, tunnel_factory=None, list_job_path: Path | None = None,
                 seed_jobs_path: Path | None = None):
        self.original_template: dict | None = None
        self.fixture = fixture
        self.plan = plan
        self.output = output
        self.private = private
        self.sleep = sleep
        self.monotonic = monotonic
        self.utcnow = utcnow or (lambda: datetime.datetime.now(datetime.timezone.utc))
        self.client_factory = client_factory
        if tunnel_factory is None:
            from pod_tunnel import PodTunnel
            tunnel_factory = PodTunnel
        self.tunnel_factory = tunnel_factory
        self.list_job_path = list_job_path or common.S3_LIST_JOB
        self.seed_jobs_path = seed_jobs_path or common.S3_SEED_JOBS
        self.suffix = hashlib.sha256(plan["runId"].encode()).hexdigest()[:8]
        self.criteria: dict[str, bool] = {}
        self.evidence: dict[str, dict] = {}
        self.step = "init"
        self.jobs: list[str] = []
        self.request_created = False
        self.tunnel = None
        self.cron_suspend_original = None
        self.cron_suspended_by_run = False
        self.timings: dict[str, float] = {}
        self.restore_started: float | None = None
        self.restore_started_at: str | None = None
        self.original_sts: dict | None = None
        self.baseline: dict = {}
        self.zones: list[str] = []
        self.node_ids: list[str] = []
        self.backup: dict = {}
        self.old_pod_uids: dict[str, str] = {}
        self.marker: dict = {}

    def record(self, name: str, value: dict) -> None:
        self.evidence[name] = value
        save(self.output, f"s3-restore-{name}.json", value)

    def fail(self, code: str):
        raise StepFailed(self.step, code)

    def open_tunnel(self, pod_name: str) -> None:
        pod = kube_json(self.fixture, "pod", pod_name)
        self.tunnel = self.tunnel_factory(self.fixture, pod_name, pod["metadata"]["uid"], *OBSERVER_PORTS)
        self.tunnel.start()

    def close_tunnel(self) -> None:
        if self.tunnel is not None:
            tunnel, self.tunnel = self.tunnel, None
            tunnel.close()

    def wait_jobs(self, names: list[str], timeout: float) -> dict[str, bool] | None:
        def settled():
            states = {}
            for name in names:
                status = kube_json(self.fixture, "job", name).get("status", {})
                conditions = {c["type"] for c in status.get("conditions", []) if c.get("status") == "True"}
                if "Failed" in conditions:
                    states[name] = False
                elif "Complete" in conditions or status.get("succeeded", 0) >= 1:
                    states[name] = True
            return states if len(states) == len(names) else None

        return common.wait_until(settled, timeout, 3, self.sleep, self.monotonic)

    # -- steps -----------------------------------------------------------------------------------------

    def step_fixture(self) -> None:
        self.step = "fixture"
        self.record("fixture", common.require_owned_cluster(self.fixture, self.plan))
        self.criteria["fixture.ownedDisposable"] = True

    def step_config(self) -> None:
        self.step = "config"
        plan = self.plan
        settings = kube_json(self.fixture, "configmap", S3_CONFIGMAP)["data"]
        scripts = kube_json(self.fixture, "configmap", common.SCRIPTS_CONFIGMAP)["data"]
        accounts = {}
        for name in ("narumq-backup", "narumq-backup-restore"):
            account = kube_json(self.fixture, "serviceaccount", name)
            accounts[name] = bool(account["metadata"].get("annotations", {}).get("eks.amazonaws.com/role-arn"))
        cronjob = kube_json(self.fixture, "cronjob", common.CRONJOB)
        init = cronjob["spec"]["jobTemplate"]["spec"]["template"]["spec"].get("initContainers", [])
        existing = []
        for kind, name in [("configmap", REQUEST_CONFIGMAP), *[("pvc", claim) for claim in manifests.restore_claim_names()]]:
            if kube(self.fixture, "get", kind, name, "--ignore-not-found", "-o", "name").strip():
                existing.append(f"{kind}/{name}")
        ok = (settings.get("BACKUP_S3_BUCKET") == plan["bucket"] and settings.get("BACKUP_S3_PREFIX") == plan["prefix"]
              and settings.get("AWS_REGION") == plan["region"]
              and {"restore-from-s3.py", "s3_uploader.py", "backup.sh"} <= set(scripts)
              and all(accounts.values()) and any(c["name"] == "s3-uploader" for c in init) and not existing)
        self.criteria["config.s3WiredAsPlanned"] = ok
        self.record("config", {"bucket": settings.get("BACKUP_S3_BUCKET"), "prefix": settings.get("BACKUP_S3_PREFIX"),
                               "region": settings.get("AWS_REGION"), "serviceAccountsHaveRoleArn": accounts,
                               "uploaderSidecar": any(c["name"] == "s3-uploader" for c in init),
                               "alreadyExisting": existing, "cronJobSuspend": cronjob["spec"].get("suspend")})
        if not ok:
            self.fail("s3_configuration_not_as_planned")
        self.cron_suspend_original = bool(cronjob["spec"].get("suspend"))

    def step_preflight(self) -> None:
        self.step = "preflight"
        pods = common.broker_pods(self.fixture)
        if not common.three_brokers_ready(pods):
            self.fail("brokers_not_three_ready")
        sts = kube_json(self.fixture, "statefulset", common.STATEFULSET)
        if not manifests.statefulset_retains_claims(sts):
            self.fail("statefulset_claim_retention_would_delete_data")
        if any("argocd" in key for key in {**sts["metadata"].get("labels", {}), **sts["metadata"].get("annotations", {})}):
            self.fail("statefulset_managed_by_gitops")
        manifests.restore_statefulset(sts, 3)  # shape check before any mutation
        template = sts["spec"]["volumeClaimTemplates"][0]
        baseline, zones = {}, []
        for ordinal, pod in enumerate(pods):
            claim = OLD_CLAIMS[ordinal]
            if pod_claims(pod).get("data") != claim:
                self.fail("pod_does_not_use_expected_data_claim")
            pvc = kube_json(self.fixture, "pvc", claim)
            if pvc["status"].get("phase") != "Bound":
                self.fail("data_claim_not_bound")
            expected = (self.plan.get("dataPvcUids") or {}).get(claim)
            if expected is not None and expected != pvc["metadata"]["uid"]:
                self.fail("data_claim_uid_mismatch")
            baseline[claim] = {"uid": pvc["metadata"]["uid"], "volumeName": pvc["spec"].get("volumeName")}
            node = self.fixture.get_node(pod["spec"]["nodeName"])
            zone = (node or {}).get("metadata", {}).get("labels", {}).get(manifests.ZONE_LABEL)
            if not zone:
                self.fail("worker_zone_unknown")
            zones.append(zone)
            self.old_pod_uids[pod["metadata"]["name"]] = pod["metadata"]["uid"]
        slots = kube_json(self.fixture, "configmap", common.CONFIGS_CONFIGMAP)["data"]
        node_ids = [slots.get(f"slot-{ordinal}", "") for ordinal in range(3)]
        if not all(v.isdigit() and not v.startswith("0") for v in node_ids) or len(set(node_ids)) != 3:
            self.fail("cluster_configmap_slots_invalid")
        if [job["metadata"]["name"] for job in kube_json(self.fixture, "jobs")["items"] if job.get("status", {}).get("active")]:
            self.fail("job_already_active")
        distance = minutes_from_schedule(self.utcnow())
        if distance < self.plan["scheduleGuardMinutes"]:
            self.fail("scheduled_backup_window_too_close")
        self.baseline, self.zones, self.node_ids, self.original_template = baseline, zones, node_ids, template
        self.original_sts = sts
        save(self.output, "s3-restore-original-statefulset.json", sts)
        self.record("preflight", {"dataClaims": baseline, "zones": zones, "nodeIds": node_ids,
                                  "originalPodUIDs": self.old_pod_uids, "minutesFromScheduleTick": round(distance, 1),
                                  "statefulSetSavedAs": "s3-restore-original-statefulset.json"})

    def step_markers(self) -> None:
        self.step = "markers"
        self.open_tunnel("narumq-0")
        try:
            self.marker = mqtt_prepare(self.private, self.plan["runId"], OBSERVER_PORTS[1], self.client_factory)
            self.criteria["source.sessionAndRetainedPrepared"] = True
            self.record("markers", {**self.marker, "clusterBefore": self.fixture.management_probe()})
        finally:
            self.close_tunnel()

    def step_backup(self) -> None:
        self.step = "backup"
        kube(self.fixture, "patch", "cronjob", common.CRONJOB, "--type=merge", "-p", json.dumps({"spec": {"suspend": True}}))
        self.cron_suspended_by_run = not self.cron_suspend_original
        name = f"narumq-backup-dr-{self.suffix}-s3"
        common.create_job_from_cronjob(self.fixture, name)
        self.jobs.append(name)
        outcome = common.wait_job(self.fixture, name, self.plan["jobTimeoutSeconds"], self.sleep, self.monotonic)
        if outcome is None or not outcome["succeeded"]:
            self.criteria["backup.uploadedToS3"] = False
            self.fail("s3_backup_job_failed" if outcome else f"s3_backup_job_timeout:{name}")
        try:
            self.backup = judges.parse_s3_backup_logs(common.job_logs(self.fixture, name, "backup"),
                                                      common.job_logs(self.fixture, name, "s3-uploader"),
                                                      self.plan["prefix"])
        except ValueError as exc:
            self.criteria["backup.uploadedToS3"] = False
            self.fail(str(exc))
        self.criteria["backup.uploadedToS3"] = True
        self.record("backup", {"job": name, **self.backup})

    def step_stop_source(self) -> None:
        self.step = "stop-source"
        kube(self.fixture, "scale", f"statefulset/{common.STATEFULSET}", "--replicas=0")
        gone = common.wait_until(lambda: not common.broker_pods(self.fixture), self.plan["sourceStopSeconds"], 5,
                                 self.sleep, self.monotonic)
        self.criteria["source.stopped"] = bool(gone)
        self.record("stop-source", {"brokerPodsRemaining": len(common.broker_pods(self.fixture)) if not gone else 0})
        if not gone:
            self.fail("source_brokers_did_not_stop")

    def step_list(self) -> None:
        self.step = "list"
        self.restore_started, self.restore_started_at = self.monotonic(), timestamp()
        job = json.loads(kube(self.fixture, "create", "-f", str(self.list_job_path), "-o", "json"))
        name = job["metadata"]["name"]
        self.jobs.append(name)
        states = self.wait_jobs([name], self.plan["jobTimeoutSeconds"])
        if not states or not states[name]:
            self.fail("list_job_failed")
        listing = judges.parse_listing(common.job_logs(self.fixture, name, "list"), self.plan["prefix"])
        keys = [item["key"] for item in listing]
        self.criteria["list.exactKeyListedUnderPrefix"] = self.backup["key"] in keys
        self.record("list", {"job": name, "listed": len(keys), "selectedKey": self.backup["key"],
                             "selectedListed": self.backup["key"] in keys})
        if self.backup["key"] not in keys:
            self.fail("uploaded_key_not_listed")
        self.timings["list"] = self.monotonic() - self.restore_started

    def step_request(self) -> None:
        self.step = "request"
        kube(self.fixture, "create", "configmap", REQUEST_CONFIGMAP, f"--from-literal=BACKUP_S3_KEY={self.backup['key']}")
        self.request_created = True

    def step_targets_and_seed(self) -> None:
        self.step = "targets"
        run_id = self.plan["runId"]
        claims = manifests.restore_claim_names()
        for claim in claims:
            common.kube_create(self.fixture, manifests.restore_pvc(self.original_template, claim, run_id))
        self.step = "seed"
        converted = json.loads(kube(self.fixture, "create", "-f", str(self.seed_jobs_path), "--dry-run=client", "-o", "json"))
        items = converted.get("items") if converted.get("kind") == "List" else None
        if not items:
            self.fail("seed_jobs_manifest_not_a_list")
        names = []
        for job in manifests.seed_jobs(items, claims, self.zones, self.node_ids, run_id):
            created = common.kube_create(self.fixture, job)
            names.append(created["metadata"]["name"])
            self.jobs.append(created["metadata"]["name"])
        states = self.wait_jobs(names, self.plan["seedTimeoutSeconds"])
        self.criteria["seed.threeJobsSucceeded"] = bool(states) and all(states.values())
        downloads = {}
        for name in names:
            try:
                downloads[name] = common.job_logs(self.fixture, name, "download-from-s3").strip().splitlines()[-1:]
            except Exception:
                downloads[name] = []
        expected = f"/restore/{judges.archive_file_name(self.backup['key'])}"
        self.criteria["download.exactKeyVerified"] = bool(states) and all(d == [expected] for d in downloads.values())
        bound = {claim: kube_json(self.fixture, "pvc", claim)["status"].get("phase") for claim in claims}
        self.criteria["targets.threeNewClaimsBound"] = all(phase == "Bound" for phase in bound.values())
        self.timings["seed"] = self.monotonic() - self.restore_started
        self.record("seed", {"jobs": names, "states": states, "downloadedPaths": downloads, "expectedPath": expected,
                             "claims": bound, "zones": self.zones, "nodeIds": self.node_ids})
        if not (self.criteria["seed.threeJobsSucceeded"] and self.criteria["download.exactKeyVerified"]
                and self.criteria["targets.threeNewClaimsBound"]):
            self.fail("seed_jobs_not_successful")

    def step_swap(self) -> None:
        self.step = "swap"
        restored = manifests.restore_statefulset(self.original_sts, 3)
        kube(self.fixture, "delete", "statefulset", common.STATEFULSET, "--cascade=orphan", "--wait=true", timeout=180)
        common.kube_create(self.fixture, restored)
        self.timings["swap"] = self.monotonic() - self.restore_started

    def step_ready(self) -> None:
        self.step = "ready"
        claims = manifests.restore_claim_names()

        def voters_ready():
            pods = common.broker_pods(self.fixture)
            if not common.three_brokers_ready(pods):
                return None
            for ordinal, pod in enumerate(pods):
                if pod_claims(pod).get(manifests.RESTORE_TEMPLATE) != claims[ordinal] or pod[
                        "metadata"]["uid"] == self.old_pod_uids.get(pod["metadata"]["name"]):
                    return None
            return pods

        pods = common.wait_until(voters_ready, self.plan["readyDeadlineSeconds"], 5, self.sleep, self.monotonic)
        self.criteria["voters.threeReadyOnNewClaims"] = bool(pods)
        self.timings["voters"] = self.monotonic() - self.restore_started
        if not pods:
            self.fail("restored_voters_not_ready")
        self.open_tunnel("narumq-0")

        def quorum():
            summary = self.fixture.management_probe()
            return summary if summary.get("quorumAvailable") is True and summary.get("nodesByState", {}).get("ready") == 3 else None

        summary = common.wait_until(lambda: self._safe(quorum), self.plan["readyDeadlineSeconds"], 5, self.sleep,
                                    self.monotonic)
        self.criteria["quorum.threeReadyVoters"] = summary is not None
        self.timings["quorum"] = self.monotonic() - self.restore_started
        self.record("ready", {"podUIDs": {p["metadata"]["name"]: p["metadata"]["uid"] for p in pods},
                              "claims": {p["metadata"]["name"]: pod_claims(p) for p in pods}, "management": summary})
        if summary is None:
            self.fail("quorum_not_reached")

    def _safe(self, probe):
        try:
            return probe()
        except Exception:
            return None

    def step_mqtt(self) -> None:
        self.step = "mqtt"
        result = mqtt_verify(self.private, self.plan["runId"], OBSERVER_PORTS[1], self.marker["sha256"],
                             self.client_factory)
        self.criteria["mqtt.sessionPresent"] = result["sessionPresent"]
        self.criteria["mqtt.retainedSha256"] = result["retainedMatches"]
        finished = self.monotonic()
        self.timings["mqtt"] = finished - self.restore_started
        rto = finished - self.restore_started
        self.criteria["rto.withinBudget"] = rto <= self.plan["rtoBudgetSeconds"]
        self.record("mqtt", {**result, "expectedRetainedSha256": self.marker["sha256"], "published": False})
        self.record("rto", {"restoreStartedAt": self.restore_started_at, "serviceVerifiedAt": timestamp(),
                            "rtoSeconds": round(rto, 2), "rtoBudgetSeconds": self.plan["rtoBudgetSeconds"],
                            "phaseSeconds": {k: round(v, 2) for k, v in self.timings.items()},
                            "scope": "list Job start to verified three-voter Ready, quorum and MQTT session/retained "
                                     "recovery; source stop and S3 backup upload precede the clock"})
        self.close_tunnel()

    def step_preserved(self) -> None:
        self.step = "preserved"
        pods = kube_json(self.fixture, "pods")["items"]
        in_use = {claim for pod in pods for claim in pod_claims(pod).values()}
        preserved = True
        after = {}
        for claim, before in self.baseline.items():
            pvc = kube_json(self.fixture, "pvc", claim)
            after[claim] = {"uid": pvc["metadata"]["uid"], "phase": pvc["status"].get("phase"),
                            "volumeName": pvc["spec"].get("volumeName"), "mountedByPod": claim in in_use}
            preserved = preserved and after[claim]["uid"] == before["uid"] and after[claim]["phase"] == "Bound" \
                and after[claim]["volumeName"] == before["volumeName"] and claim not in in_use
        self.criteria["oldData.claimsPreserved"] = preserved
        self.record("preserved", {"before": self.baseline, "after": after})

    def step_resume(self) -> None:
        self.step = "resume"
        if not self.cron_suspended_by_run:
            self.criteria["cronjob.resumedAfterVerification"] = True
            return
        if not all(self.criteria.get(name) is True for name in REQUIRED if name not in (
                "cronjob.resumedAfterVerification", "cleanup.ownedResourcesRemoved")):
            self.criteria["cronjob.resumedAfterVerification"] = False
            return
        kube(self.fixture, "patch", "cronjob", common.CRONJOB, "--type=merge", "-p", json.dumps({"spec": {"suspend": False}}))
        self.cron_suspended_by_run = False
        self.criteria["cronjob.resumedAfterVerification"] = True

    # -- teardown --------------------------------------------------------------------------------------

    def cleanup(self, report: dict) -> None:
        errors: dict[str, str] = {}
        try:
            self.close_tunnel()
        except Exception as exc:
            errors["tunnel"] = type(exc).__name__
        for name in self.jobs:
            try:
                common.kube_delete(self.fixture, "job", name)
            except Exception as exc:
                errors[f"job:{name}"] = type(exc).__name__
        if self.request_created:
            try:
                common.kube_delete(self.fixture, "configmap", REQUEST_CONFIGMAP)
            except Exception as exc:
                errors["requestConfigMap"] = type(exc).__name__
        self.criteria["cleanup.ownedResourcesRemoved"] = not errors
        if errors:
            report["teardownErrors"] = errors
            report["teardownFailed"] = True
        report["fixtureState"] = ("source claims preserved; cluster running on restore-data-* claims"
                                  if self.criteria.get("voters.threeReadyOnNewClaims") else
                                  "restore did not complete; inspect s3-restore-original-statefulset.json before "
                                  "changing anything; the CronJob stays suspended")

    def execute(self) -> None:
        for step in (self.step_fixture, self.step_config, self.step_preflight, self.step_markers, self.step_backup,
                     self.step_stop_source, self.step_list, self.step_request, self.step_targets_and_seed,
                     self.step_swap, self.step_ready, self.step_mqtt, self.step_preserved, self.step_resume):
            step()


def run(fixture, plan: dict, output: Path, private: Path, **kwargs) -> dict:
    report: dict = {"result": "FAIL", "mode": "s3-full-restore", "runId": plan["runId"], "startedAt": timestamp(),
                    "scope": "CLU-SCN-050 isolated EKS real S3/IRSA full restore on new PVCs with MQTT recovery"}
    state = S3RestoreRun(fixture, plan, output, private, **kwargs)
    failed_step = None
    try:
        state.execute()
    except StepFailed as exc:
        report["errorCode"], failed_step = exc.code, exc.step
    except Exception as exc:
        report["errorCode"], failed_step = type(exc).__name__, state.step
    finally:
        state.cleanup(report)
    judged = judges.aggregate(state.criteria, REQUIRED, failed_step)
    if report.get("teardownFailed"):
        judged["result"] = "FAIL"
    report.update(judged)
    rto = state.evidence.get("rto")
    report["rtoSeconds"] = rto["rtoSeconds"] if rto else None
    report["steps"] = sorted(state.evidence)
    report["finishedAt"] = timestamp()
    return report
