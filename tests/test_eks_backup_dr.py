"""Unit tests of the backup/DR acceptance harness (tests/chaos/eks-backup-dr) against a simulated cluster.

These are harness-logic tests. They run no kubectl or AWS and are not acceptance evidence for CLU-SCN-046/047/050/159.
"""
from __future__ import annotations

import datetime
import hashlib
import json
from pathlib import Path
import base64
import re
import struct
import sys
import tempfile
import types
import unittest

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parents[1]
DIR = ROOT / "tests/chaos/eks-backup-dr"
sys.path.insert(0, str(DIR))

import dr_archive  # noqa: E402
import dr_common as common  # noqa: E402
import dr_judges as judges  # noqa: E402
import dr_production  # noqa: E402
import dr_restore_manifests as manifests  # noqa: E402
import dr_retention  # noqa: E402
import dr_s3_restore  # noqa: E402

RUN = "narumq-ha-acceptance-20261017"
VPC = "vpc-0123456789abcdef0"
NOW_EPOCH = 1_792_000_000  # 2026-10-14T...Z, cluster clock of the simulation
UTC_NOW = datetime.datetime(2026, 10, 17, 10, 0, tzinfo=datetime.timezone.utc)
SHA = "ab" * 32


# -- synthetic archive ----------------------------------------------------------------------------------------------

def varint(value: int) -> bytes:
    out = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        out.append(byte | (0x80 if value else 0))
        if not value:
            return bytes(out)


def field_varint(number: int, value: int) -> bytes:
    return varint(number << 3) + varint(value)


def field_bytes(number: int, value: bytes) -> bytes:
    return varint(number << 3 | 2) + varint(len(value)) + value


def frame(payload: bytes) -> bytes:
    return struct.pack(">II", len(payload), dr_archive.crc32c(payload)) + payload


def make_archive(commit: int = 100, entries: int = 2, voters=(1, 2, 3), snapshot: int = 50, minor: int = 0) -> bytes:
    manifest = b"".join([
        field_varint(1, 3), field_varint(2, minor), field_bytes(3, b"narumq-cluster-1"), field_varint(4, 1),
        field_bytes(5, b"0.0.1"), field_varint(6, 2), field_varint(7, 0), field_varint(8, 1), field_varint(9, 0),
        field_varint(11, snapshot), field_varint(12, 1), field_varint(13, commit), field_varint(14, 1),
        field_bytes(15, b"".join(varint(v) for v in voters)), field_varint(17, entries)])
    body = b"NMQBKUP3" + frame(manifest) + frame(b"snapshot") + b"".join(frame(b"entry%d" % i) for i in range(entries))
    return body + hashlib.sha256(body).digest()


def reseal(body: bytes) -> bytes:
    return body + hashlib.sha256(body).digest()


class ArchiveTests(unittest.TestCase):
    def test_crc32c_standard_vector(self):
        self.assertEqual(dr_archive.crc32c(b"123456789"), 0xE3069283)

    def test_valid_archive_reports_manifest_facts(self):
        data = make_archive(commit=100)
        facts = dr_archive.parse_archive(data)
        self.assertEqual((facts["commitIndex"], facts["snapshotIndex"], facts["entryCount"]), (100, 50, 2))
        self.assertEqual((facts["formatMajor"], facts["formatMinor"], facts["clusterProtocol"], facts["stateSchema"]),
                         (3, 0, "2.0", "1.0"))
        self.assertEqual(facts["voterIds"], [1, 2, 3])
        self.assertEqual(facts["frames"], 4)
        self.assertEqual(facts["fileSha256"], hashlib.sha256(data).hexdigest())

    def test_rejections_have_stable_codes(self):
        good = make_archive()
        corrupt_trailer = good[:-1] + bytes([good[-1] ^ 1])
        flipped = bytearray(good[:-32])
        flipped[30] ^= 0xFF  # inside the manifest payload: CRC fails, trailer is resealed
        v2 = b"NMQBKUP2" + good[8:]
        cases = {
            "archive_trailer_sha256_mismatch": corrupt_trailer,
            "archive_size_out_of_range": b"NMQBKUP3",
            "archive_magic_mismatch": reseal(b"NMQBKUPX" + good[8:-32]),
            "archive_format_unsupported": v2,
            "frame_0_checksum_mismatch": reseal(bytes(flipped)),
            "archive_trailing_bytes": reseal(good[:-32] + b"x"),
            "archive_format_version_unsupported": make_archive(minor=1),
            "archive_commit_boundary_invalid": make_archive(commit=10, snapshot=50),
            "archive_voters_invalid": make_archive(voters=(3, 1, 2)),
        }
        for code, data in cases.items():
            with self.subTest(code=code):
                with self.assertRaises(dr_archive.ArchiveError) as caught:
                    dr_archive.parse_archive(data)
                self.assertEqual(str(caught.exception), code)

    def test_missing_entry_frame_is_rejected(self):
        good = make_archive(entries=2)
        body = good[:-32]
        shorter = reseal(body[:-len(frame(b"entry1"))])
        with self.assertRaises(dr_archive.ArchiveError):
            dr_archive.parse_archive(shorter)


# -- judges ----------------------------------------------------------------------------------------------------------

REPOSITORY_SCRIPT = (ROOT / "deploy/kubernetes/eks-ec2-ha/backup.sh").read_text(encoding="utf-8")


def cronjob(**changes) -> dict:
    spec = {
        "schedule": "0 2 * * *", "concurrencyPolicy": "Forbid",
        "jobTemplate": {"spec": {"template": {"spec": {
            "containers": [{"name": "backup", "image": "registry/narumq@sha256:" + SHA,
                            "command": ["/bin/sh", "/scripts/backup.sh"]}],
            "volumes": [{"name": "backup-storage", "persistentVolumeClaim": {"claimName": "narumq-backup-pvc"}}]}}}}}
    spec.update(changes)
    return {"spec": spec}


class PolicyTests(unittest.TestCase):
    def judge(self, cron=None, script=REPOSITORY_SCRIPT):
        return judges.cronjob_policy(cron or cronjob(), {"data": {"backup.sh": script}}, REPOSITORY_SCRIPT, "0 2 * * *")

    def test_repository_script_meets_local_policy(self):
        criteria, evidence = self.judge()
        self.assertTrue(all(criteria.values()), criteria)
        self.assertEqual(evidence["schedule"], "0 2 * * *")

    def test_each_policy_deviation_is_detected(self):
        self.assertFalse(self.judge(cronjob(schedule="0 3 * * *"))[0]["cronjob.schedule"])
        self.assertFalse(self.judge(cronjob(suspend=True))[0]["cronjob.notSuspended"])
        self.assertFalse(self.judge(script=REPOSITORY_SCRIPT.replace("-mtime +30", "-mtime +90"))[0][
            "cronjob.scriptMatchesRepository"])
        weakened = REPOSITORY_SCRIPT.replace("-mtime +30 -delete", "-mtime +30 -print")
        criteria = judges.cronjob_policy({"spec": cronjob()["spec"]}, {"data": {"backup.sh": weakened}}, weakened,
                                         "0 2 * * *")[0]
        self.assertFalse(criteria["cronjob.localMtimePlus30Cleanup"])
        s3 = cronjob()
        s3["spec"]["jobTemplate"]["spec"]["template"]["spec"]["initContainers"] = [{"name": "s3-uploader"}]
        self.assertFalse(self.judge(s3)[0]["cronjob.localOnlyVariant"])
        self.assertFalse(judges.cronjob_policy(cronjob(), {}, REPOSITORY_SCRIPT, "0 2 * * *")[0][
            "cronjob.scriptMatchesRepository"])


def inventory(entries: dict, aged=(), now=NOW_EPOCH, archive=None, name=None) -> dict:
    return {"now": now, "entries": entries, "aged": list(aged), "archive": archive, "archiveName": name}


def entry(size=100, mtime=NOW_EPOCH - 60, kind="regular file") -> dict:
    return {"type": kind, "size": size, "mtime": mtime}


class InventoryAndRetentionTests(unittest.TestCase):
    def test_inventory_round_trip_with_archive(self):
        data = make_archive()
        encoded = base64.encodebytes(data).decode()
        logs = ("NOW 1792000000\nENTRY|lost+found|directory|16384|1700000000\n"
                "ENTRY|backup-20261014000000.nrbak|regular file|1789|1791999000\nAGED|backup-20200101000000.nrbak\n"
                "ARCHIVE-BEGIN backup-20261014000000.nrbak\n" + encoded + "ARCHIVE-END\n")
        parsed = judges.parse_inventory(logs)
        self.assertEqual(parsed["now"], 1792000000)
        self.assertEqual(parsed["archive"], data)
        self.assertEqual(judges.archive_names(parsed), {"backup-20261014000000.nrbak"})
        self.assertEqual(parsed["aged"], ["backup-20200101000000.nrbak"])
        with self.assertRaises(ValueError):
            judges.parse_inventory("NOW 1\nARCHIVE-BEGIN backup-20261014000000.nrbak\nAAAA\n")
        with self.assertRaises(ValueError):
            judges.parse_inventory("ENTRY|x|regular file|1|1\n")

    def scenario(self):
        old = "backup-20261014000000.nrbak"
        probe = "backup-20260915000000.nrbak"
        new = "backup-20261017000000.nrbak"
        before = inventory({old: entry(), "lost+found": entry(kind="directory", size=16384)})
        probe_info = {"name": probe, "size": 0, "mtime": NOW_EPOCH - 32 * 86400, "expectedMtime": NOW_EPOCH - 32 * 86400,
                      "now": NOW_EPOCH}
        with_probe = inventory({**before["entries"], probe: entry(0, NOW_EPOCH - 32 * 86400)}, aged=[probe])
        after = inventory({**before["entries"], new: entry(1789)})
        return before, probe_info, with_probe, after, probe, new

    def test_probe_creation_judge(self):
        before, probe_info, with_probe, _, probe, _ = self.scenario()
        self.assertTrue(all(judges.judge_probe_creation(probe_info, with_probe, before, 32).values()))
        young = dict(probe_info, mtime=NOW_EPOCH - 10 * 86400, expectedMtime=NOW_EPOCH - 10 * 86400)
        self.assertFalse(judges.judge_probe_creation(young, with_probe, before, 32)["probe.createdEmptyWithSyntheticMtime"])
        non_empty = dict(probe_info, size=5)
        self.assertFalse(judges.judge_probe_creation(non_empty, with_probe, before, 32)[
            "probe.createdEmptyWithSyntheticMtime"])
        two_aged = inventory(with_probe["entries"], aged=[probe, "backup-20200101000000.nrbak"])
        self.assertFalse(judges.judge_probe_creation(probe_info, two_aged, before, 32)["probe.singleAgedArchiveBeforeJob"])
        preexisting = inventory(before["entries"], aged=["backup-20200101000000.nrbak"])
        self.assertFalse(judges.judge_probe_creation(probe_info, with_probe, preexisting, 32)[
            "probe.singleAgedArchiveBeforeJob"])

    def test_retention_judge_pass_and_each_failure(self):
        before, _, _, after, probe, new = self.scenario()
        criteria, evidence = judges.judge_retention(before, probe, after, new, True, True)
        self.assertTrue(all(criteria.values()), criteria)
        self.assertEqual(evidence["createdArchives"], [new])
        old = "backup-20261014000000.nrbak"
        still_there = inventory({**after["entries"], probe: entry(0, NOW_EPOCH - 32 * 86400)}, aged=[probe])
        self.assertFalse(judges.judge_retention(before, probe, still_there, new, True, True)[0]["retention.probeDeleted"])
        lost = inventory({k: v for k, v in after["entries"].items() if k != old})
        self.assertFalse(judges.judge_retention(before, probe, lost, new, True, True)[0][
            "retention.existingArchivesPreserved"])
        touched = inventory({**after["entries"], old: entry(size=1)})
        self.assertFalse(judges.judge_retention(before, probe, touched, new, True, True)[0][
            "retention.existingArchivesPreserved"])
        none_new = inventory(before["entries"])
        self.assertFalse(judges.judge_retention(before, probe, none_new, None, True, True)[0][
            "retention.newArchiveCreated"])
        extra = inventory({**after["entries"], "backup-20261017000100.nrbak": entry()})
        self.assertFalse(judges.judge_retention(before, probe, extra, new, True, True)[0]["retention.newArchiveCreated"])
        leftovers = inventory({**after["entries"], ".backup-abc": entry()})
        self.assertFalse(judges.judge_retention(before, probe, leftovers, new, True, True)[0][
            "retention.noTemporaryLeftovers"])
        self.assertFalse(judges.judge_retention(before, probe, after, new, False, True)[0]["retention.jobSucceeded"])
        self.assertFalse(judges.judge_retention(before, probe, after, new, True, False)[0]["retention.newArchiveIntegrity"])

    def test_local_backup_log_parser(self):
        self.assertEqual(judges.parse_local_backup_log("backup: archive commit=100 saved locally as backup-20261017000000.nrbak\n"),
                         (100, "backup-20261017000000.nrbak"))
        for bad in ("", "backup: archive commit=x saved locally as y\n"):
            with self.assertRaises(ValueError):
                judges.parse_local_backup_log(bad)


class SeedTests(unittest.TestCase):
    LOGS = "".join(
        f"SEED node={n} pre-clean=yes exit=0 start=100.00 end=100.0{n}\nFILE node={n} name=cluster.meta size=98\n"
        f"FILE node={n} name=snapshot-{100:020d}.bin size=1117\nFILE node={n} name=wal.bin size=48\n" for n in (1, 2, 3))

    def test_parse_and_judge_pass(self):
        nodes = judges.parse_seed_logs(self.LOGS)
        criteria, evidence = judges.judge_seed(nodes, [1, 2, 3], 100, 3600)
        self.assertTrue(all(criteria.values()), criteria)
        self.assertEqual(evidence["totalSeedSeconds"], 0.06)

    def test_each_failure_is_detected(self):
        nodes = judges.parse_seed_logs(self.LOGS)
        self.assertFalse(judges.judge_seed(nodes, [1, 2, 3], 101, 3600)[0]["seed.snapshotAtCommitIndex"])
        self.assertFalse(judges.judge_seed(nodes, [1, 2, 4], 100, 3600)[0]["seed.threeNodesSeeded"])
        self.assertFalse(judges.judge_seed(nodes, [1, 2, 3], 100, 0)[0]["seed.withinRtoBudget"])
        dirty = judges.parse_seed_logs(self.LOGS.replace("node=2 pre-clean=yes", "node=2 pre-clean=no"))
        self.assertFalse(judges.judge_seed(dirty, [1, 2, 3], 100, 3600)[0]["seed.cleanDirectories"])
        failed = judges.parse_seed_logs(self.LOGS.replace("node=3 pre-clean=yes exit=0", "node=3 pre-clean=yes exit=1"))
        self.assertFalse(judges.judge_seed(failed, [1, 2, 3], 100, 3600)[0]["seed.threeNodesSeeded"])
        marker = judges.parse_seed_logs(self.LOGS + "FILE node=1 name=restore-incomplete size=1\n")
        self.assertFalse(judges.judge_seed(marker, [1, 2, 3], 100, 3600)[0]["seed.noRestoreMarker"])
        self.assertFalse(any(judges.judge_seed({}, [1, 2, 3], 100, 3600)[0].values()))


PREFIX = "eks-run-1"
KEY = f"{PREFIX}/2026/10/17/backup-20261017T000000Z-52-{SHA}.nrbak"


class S3JudgeTests(unittest.TestCase):
    def test_backup_logs_and_key(self):
        backup = f"backup: archive commit=52 sha256={SHA} uploaded\n"
        uploader = f"backup-uploader: uploaded key={KEY} commit=52 sha256={SHA}\n"
        self.assertEqual(judges.parse_s3_backup_logs(backup, uploader, PREFIX)["key"], KEY)
        with self.assertRaises(ValueError):
            judges.parse_s3_backup_logs(backup, uploader.replace("commit=52", "commit=53"), PREFIX)
        with self.assertRaises(ValueError):
            judges.parse_s3_backup_logs(backup, uploader, "other-prefix")
        with self.assertRaises(ValueError):
            judges.parse_s3_backup_logs("", uploader, PREFIX)

    def test_listing(self):
        listing = f"LastModified\tSizeBytes\tKey\n2026-10-17T00:00:00+00:00\t5452\t{KEY}\n"
        self.assertEqual([i["key"] for i in judges.parse_listing(listing, PREFIX)], [KEY])
        with self.assertRaises(ValueError):
            judges.parse_listing("x\t1\tother/key\n", PREFIX)
        self.assertEqual(judges.archive_file_name(KEY), f"backup-20261017T000000Z-52-{SHA}.nrbak")


def daily_objects(days: int, gap_hours: float = 24, newest_age_hours: float = 20) -> list[dict]:
    newest = UTC_NOW - datetime.timedelta(hours=newest_age_hours)
    items = []
    for index in range(days):
        stamp = newest - datetime.timedelta(hours=gap_hours * index)
        key = f"{PREFIX}/{stamp:%Y/%m/%d}/backup-{stamp:%Y%m%dT%H%M%SZ}-{index + 1}-{SHA}.nrbak"
        items.append({"Key": key, "LastModified": stamp.isoformat()})
    return items


AUDIT_SETTINGS = {"prefix": PREFIX, "maxGapSeconds": 86400, "gapToleranceSeconds": 0, "minObservedDays": 30,
                  "lifecycleSlackSeconds": 86400}
BUCKET = "narumq-acc-bucket"
GOOD_CONFIG = {
    "publicAccessBlock": {"PublicAccessBlockConfiguration": {k: True for k in (
        "BlockPublicAcls", "IgnorePublicAcls", "BlockPublicPolicy", "RestrictPublicBuckets")}},
    "encryption": {"ServerSideEncryptionConfiguration": {"Rules": [{"ApplyServerSideEncryptionByDefault": {
        "SSEAlgorithm": "AES256"}}]}},
    "lifecycle": {"Rules": [{"ID": "expire", "Status": "Enabled", "Filter": {"Prefix": PREFIX + "/"},
                             "Expiration": {"Days": 30}, "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 1}}]},
    "policy": {"Policy": json.dumps({"Version": "2012-10-17", "Statement": [{
        "Sid": "DenyInsecureTransport", "Effect": "Deny", "Principal": "*", "Action": "s3:*",
        "Resource": [f"arn:aws:s3:::{BUCKET}", f"arn:aws:s3:::{BUCKET}/*"],
        "Condition": {"Bool": {"aws:SecureTransport": "false"}}}]})},
    "location": {"LocationConstraint": "ap-northeast-2"},
}


class AuditTests(unittest.TestCase):
    def cadence(self, objects):
        return judges.audit_cadence(objects, UTC_NOW, AUDIT_SETTINGS)[0]

    def test_thirty_daily_backups_pass(self):
        criteria = self.cadence(daily_objects(31))
        self.assertTrue(all(criteria.values()), criteria)

    def test_each_cadence_failure(self):
        self.assertFalse(self.cadence(daily_objects(31, gap_hours=25))["audit.everyGapWithinDaily"])
        self.assertFalse(self.cadence(daily_objects(20))["audit.observedRetentionDays"])
        self.assertFalse(self.cadence(daily_objects(31, newest_age_hours=30))["audit.newestWithinRpo"])
        self.assertFalse(self.cadence([])["audit.archivesPresent"])
        self.assertFalse(self.cadence(daily_objects(1))["audit.archivesPresent"])
        other = [dict(item, Key="elsewhere/" + item["Key"]) for item in daily_objects(31)]
        self.assertFalse(self.cadence(other)["audit.archivesPresent"])
        tolerant = dict(AUDIT_SETTINGS, gapToleranceSeconds=3600)
        slightly_late = daily_objects(31, gap_hours=24.5)
        self.assertTrue(judges.audit_cadence(slightly_late, UTC_NOW, tolerant)[0]["audit.everyGapWithinDaily"])

    def controls(self, **changes):
        config = {**GOOD_CONFIG, **changes}
        return judges.audit_bucket(config, PREFIX, BUCKET, "ap-northeast-2")[0]

    def test_bucket_controls(self):
        self.assertTrue(all(self.controls().values()))
        self.assertFalse(self.controls(publicAccessBlock=None)["audit.publicAccessBlocked"])
        self.assertFalse(self.controls(encryption={"ServerSideEncryptionConfiguration": {"Rules": [{
            "ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "aws:kms"}}]}})["audit.sseS3"])
        self.assertFalse(self.controls(policy=None)["audit.tlsOnlyPolicy"])
        shorter = {"Rules": [{"Status": "Enabled", "Filter": {"Prefix": PREFIX + "/"}, "Expiration": {"Days": 29},
                              "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 1}}]}
        self.assertFalse(self.controls(lifecycle=shorter)["audit.lifecycleExpiresAfter30Days"])
        no_abort = {"Rules": [{"Status": "Enabled", "Filter": {"Prefix": PREFIX + "/"}, "Expiration": {"Days": 30}}]}
        self.assertFalse(self.controls(lifecycle=no_abort)["audit.incompleteMultipartAbortAfter1Day"])
        disabled = {"Rules": [dict(GOOD_CONFIG["lifecycle"]["Rules"][0], Status="Disabled")]}
        self.assertFalse(self.controls(lifecycle=disabled)["audit.lifecycleExpiresAfter30Days"])
        other_prefix = {"Rules": [dict(GOOD_CONFIG["lifecycle"]["Rules"][0], Filter={"Prefix": "elsewhere/"})]}
        self.assertFalse(self.controls(lifecycle=other_prefix)["audit.lifecycleExpiresAfter30Days"])
        self.assertFalse(self.controls(location={"LocationConstraint": "us-west-2"})["audit.sameRegionBucket"])
        whole_bucket = {"Rules": [dict(GOOD_CONFIG["lifecycle"]["Rules"][0], Filter={"Prefix": ""})]}
        self.assertTrue(self.controls(lifecycle=whole_bucket)["audit.lifecycleExpiresAfter30Days"])


class StageSummaryTests(unittest.TestCase):
    REQUIRED = ("a.one", "b.two")

    def test_pass_requires_exit_zero_summary_pass_and_every_criterion(self):
        good = {"result": "PASS", "failedStep": None, "failedCriteria": [], "criteria": {"a.one": True, "b.two": True}}
        self.assertEqual(judges.judge_stage_summary(0, good, self.REQUIRED), (True, None))
        self.assertEqual(judges.judge_stage_summary(1, good, self.REQUIRED)[1], "stage_exit_code_nonzero")
        self.assertEqual(judges.judge_stage_summary(None, good, self.REQUIRED)[1], "stage_timeout")
        self.assertEqual(judges.judge_stage_summary(0, None, self.REQUIRED)[1], "stage_summary_missing")
        self.assertEqual(judges.judge_stage_summary(0, dict(good, result="FAIL"), self.REQUIRED)[1], "stage_summary_not_pass")
        self.assertEqual(judges.judge_stage_summary(0, dict(good, failedStep="x"), self.REQUIRED)[1], "stage_summary_not_pass")
        missing = dict(good, criteria={"a.one": True})
        self.assertEqual(judges.judge_stage_summary(0, missing, self.REQUIRED)[1], "stage_criteria_unmet:b.two")
        self.assertFalse(judges.judge_stage_summary(0, dict(good, criteria={"a.one": True, "b.two": "yes"}), self.REQUIRED)[0])


# -- manifests -------------------------------------------------------------------------------------------------------

def statefulset() -> dict:
    return {"apiVersion": "apps/v1", "kind": "StatefulSet", "status": {"replicas": 0},
            "metadata": {"name": "narumq", "namespace": "narumq", "uid": "x", "resourceVersion": "9",
                         "labels": {"app": "narumq"},
                         "annotations": {manifests.LAST_APPLIED: "{}"}},
            "spec": {"replicas": 0, "persistentVolumeClaimRetentionPolicy": {"whenDeleted": "Retain", "whenScaled": "Retain"},
                     "volumeClaimTemplates": [{"metadata": {"name": "data"}, "status": {"phase": "Pending"},
                                               "spec": {"accessModes": ["ReadWriteOnce"], "storageClassName": "gp3",
                                                        "resources": {"requests": {"storage": "20Gi"}}}}],
                     "template": {"spec": {
                         "initContainers": [{"name": "init", "volumeMounts": [{"name": "data", "mountPath": "/volume"}]}],
                         "containers": [{"name": "narumq", "volumeMounts": [
                             {"name": "configs", "mountPath": "/configs"}, {"name": "data", "mountPath": "/data"},
                             {"name": "data", "mountPath": "/audit"}]}],
                         "volumes": [{"name": "configs", "configMap": {"name": "x"}}]}}}}


def seed_job_items() -> list[dict]:
    items = []
    for ordinal in range(3):
        items.append({"apiVersion": "batch/v1", "kind": "Job",
                      "metadata": {"generateName": f"narumq-backup-restore-node-{ordinal + 1}-", "labels": {"a": "b"}},
                      "spec": {"template": {"spec": {
                          "containers": [{"name": "restore", "args": [
                              f"exec /narumq -cluster-config /configs/node-{ordinal + 1}.json -cluster-restore \"$1\""]}],
                          "volumes": [{"name": "data", "persistentVolumeClaim": {"claimName": f"data-narumq-{ordinal}"}}]}}}})
    return items


class ManifestTests(unittest.TestCase):
    def test_restore_statefulset_renames_template_and_mounts(self):
        restored = manifests.restore_statefulset(statefulset(), 3)
        spec = restored["spec"]
        self.assertEqual(spec["replicas"], 3)
        self.assertEqual(spec["volumeClaimTemplates"][0]["metadata"]["name"], "restore-data")
        names = [m["name"] for c in spec["template"]["spec"]["containers"] + spec["template"]["spec"]["initContainers"]
                 for m in c["volumeMounts"]]
        self.assertEqual(sorted(names), ["configs", "restore-data", "restore-data", "restore-data"])
        self.assertNotIn("status", restored)
        self.assertNotIn("uid", restored["metadata"])
        self.assertNotIn("annotations", restored["metadata"])
        self.assertEqual(statefulset()["spec"]["volumeClaimTemplates"][0]["metadata"]["name"], "data")

    def test_restore_statefulset_rejects_unexpected_shapes(self):
        odd = statefulset()
        odd["spec"]["volumeClaimTemplates"].append({"metadata": {"name": "extra"}})
        with self.assertRaises(common.StepFailed):
            manifests.restore_statefulset(odd, 3)
        fewer = statefulset()
        del fewer["spec"]["template"]["spec"]["initContainers"]
        with self.assertRaises(common.StepFailed):
            manifests.restore_statefulset(fewer, 3)

    def test_retention_policy(self):
        self.assertTrue(manifests.statefulset_retains_claims(statefulset()))
        scaled = statefulset()
        scaled["spec"]["persistentVolumeClaimRetentionPolicy"]["whenScaled"] = "Delete"
        self.assertFalse(manifests.statefulset_retains_claims(scaled))

    def test_restore_pvc_copies_spec_without_volume(self):
        template = statefulset()["spec"]["volumeClaimTemplates"][0]
        template["spec"]["volumeName"] = "pv-old"
        pvc = manifests.restore_pvc(template, "restore-data-narumq-0", RUN)
        self.assertEqual(pvc["spec"]["storageClassName"], "gp3")
        self.assertNotIn("volumeName", pvc["spec"])
        self.assertEqual(pvc["metadata"]["name"], "restore-data-narumq-0")
        self.assertEqual(manifests.restore_claim_names(), [f"restore-data-narumq-{i}" for i in range(3)])

    def test_seed_jobs_use_new_claims_zones_and_live_member_ids(self):
        claims = manifests.restore_claim_names()
        jobs = manifests.seed_jobs(seed_job_items(), claims, ["a", "b", "c"], ["1", "2", "4"], RUN)
        self.assertEqual([j["spec"]["template"]["spec"]["volumes"][0]["persistentVolumeClaim"]["claimName"] for j in jobs],
                         claims)
        self.assertEqual([j["spec"]["template"]["spec"]["nodeSelector"][manifests.ZONE_LABEL] for j in jobs], ["a", "b", "c"])
        self.assertIn("/configs/node-4.json", jobs[2]["spec"]["template"]["spec"]["containers"][0]["args"][0])
        self.assertEqual(jobs[0]["metadata"]["labels"][common.RUN_LABEL], RUN)
        original = seed_job_items()
        manifests.seed_jobs(original, claims, ["a", "b", "c"], ["1", "2", "3"], RUN)
        self.assertEqual(original, seed_job_items())  # the input is not modified

    def test_seed_jobs_reject_drift(self):
        items = seed_job_items()
        items[1]["spec"]["template"]["spec"]["volumes"][0]["persistentVolumeClaim"]["claimName"] = "other"
        with self.assertRaises(common.StepFailed):
            manifests.seed_jobs(items, manifests.restore_claim_names(), ["a", "b", "c"], ["1", "2", "3"], RUN)
        with self.assertRaises(common.PlanError):
            manifests.seed_jobs(items[:2], manifests.restore_claim_names(), ["a", "b", "c"], ["1", "2", "3"], RUN)


class PlanTests(unittest.TestCase):
    def base(self, **extra):
        return {"runId": RUN, "clusterName": RUN, "vpcId": VPC, **extra}

    def test_retention_plan_defaults_and_bounds(self):
        for changes in ({"runId": "other-run-0000"}, {"clusterName": "narumq-eks"}, {"clusterName": "prod"},
                        {"probeAgeDays": 30}, {"probeAgeDays": True}, {"seedBudgetSeconds": 3601},
                        {"expectedCommitIndex": 0}, {"expectedCommitIndex": "100"}, {"maxArchiveBytes": 10}):
            with self.subTest(changes=changes):
                with self.assertRaises(common.PlanError):
                    dr_retention.load_plan({**self.base(), **changes}, RUN)

    def s3_plan(self, **extra):
        return self.base(**{"bucket": BUCKET, "prefix": PREFIX, "region": "ap-northeast-2", **extra})

    def test_s3_plan_validation(self):
        plan = dr_s3_restore.load_plan(self.s3_plan(), RUN)
        self.assertEqual(plan["rtoBudgetSeconds"], 3600)
        for changes in ({"bucket": "UPPER"}, {"prefix": "a/"}, {"prefix": "a//b"}, {"prefix": "../x"},
                        {"region": "AP"}, {"rtoBudgetSeconds": 7200}, {"dataPvcUids": {"data-narumq-0": "u"}}):
            with self.subTest(changes=changes):
                with self.assertRaises(common.PlanError):
                    dr_s3_restore.load_plan(self.s3_plan(**changes), RUN)
        uids = {f"data-narumq-{i}": f"u{i}" for i in range(3)}
        self.assertEqual(dr_s3_restore.load_plan(self.s3_plan(dataPvcUids=uids), RUN)["dataPvcUids"], uids)

    def test_production_plan_cannot_weaken_the_approved_policy(self):
        raw = {"runId": RUN, "bucket": BUCKET, "prefix": PREFIX, "region": "ap-northeast-2"}
        plan = dr_production.load_plan(raw, RUN)
        self.assertEqual((plan["minObservedDays"], plan["maxGapSeconds"], plan["gapToleranceSeconds"]), (30, 86400, 0))
        for changes in ({"minObservedDays": 29}, {"maxGapSeconds": 90000}, {"gapToleranceSeconds": 7200},
                        {"lifecycleSlackSeconds": 100000}, {"runId": "x"}):
            with self.subTest(changes=changes):
                with self.assertRaises(common.PlanError):
                    dr_production.load_plan({**raw, **changes}, RUN)

    def test_minutes_from_schedule(self):
        at = lambda h, m: datetime.datetime(2026, 10, 17, h, m, tzinfo=datetime.timezone.utc)
        self.assertEqual(dr_retention.minutes_from_schedule(at(2, 10)), 10)
        self.assertEqual(dr_retention.minutes_from_schedule(at(1, 50)), 10)
        self.assertEqual(dr_retention.minutes_from_schedule(at(23, 55)), 125)
        self.assertGreater(dr_retention.minutes_from_schedule(at(10, 0)), 15)


# -- simulated cluster -----------------------------------------------------------------------------------------------

class SimulatedCluster:
    """Answers the exact kubectl/aws calls of the harness and applies the effect of the deployed scripts."""

    def __init__(self, variant: str = "local", backup_deletes_aged: bool = True, commit: int = 100,
                 seeded_exit: int = 0):
        self.variant = variant
        self.backup_deletes_aged = backup_deletes_aged
        self.commit = commit
        self.seeded_exit = seeded_exit
        self.clock = NOW_EPOCH
        self.pvc: dict[str, dict] = {"lost+found": {"type": "directory", "size": 16384, "mtime": 1_700_000_000, "data": b""}}
        self.pods: dict[str, dict] = {}
        self.pod_logs: dict[str, str] = {}
        self.jobs: dict[str, dict] = {}
        self.calls: list[list[str]] = []
        self.counter = 0
        self.suspend = False
        # S3 variant state
        self.brokers = self.make_brokers("old")
        self.claims = {f"data-narumq-{i}": {"uid": f"claim-uid-{i}", "volume": f"pv-{i}", "phase": "Bound"} for i in range(3)}
        self.configmaps: set[str] = set()
        self.sts = statefulset()
        self.sts["spec"]["replicas"] = 3
        self.sts_exists = True
        self.s3_key = KEY
        self.created: list[str] = []
        self.fail_backup_job = False

    # -- helpers --
    def uid(self, label: str) -> str:
        self.counter += 1
        return f"{label}-{self.counter}"

    def make_brokers(self, tag: str, restored: bool = False) -> list[dict]:
        pods = []
        for ordinal in range(3):
            claim = f"restore-data-narumq-{ordinal}" if restored else f"data-narumq-{ordinal}"
            volume_name = "restore-data" if restored else "data"
            pods.append({"metadata": {"name": f"narumq-{ordinal}", "uid": f"{tag}-pod-{ordinal}"},
                         "spec": {"nodeName": f"node-{ordinal}",
                                  "volumes": [{"name": volume_name, "persistentVolumeClaim": {"claimName": claim}}]},
                         "status": {"phase": "Running", "conditions": [{"type": "Ready", "status": "True"}]}})
        return pods

    def archive(self) -> bytes:
        return make_archive(commit=self.commit)

    def archives(self) -> list[str]:
        return [n for n, e in self.pvc.items() if judges.LEGACY_ARCHIVE.fullmatch(n) and e["type"] == "regular file"]

    def stamp(self, epoch: int) -> str:
        return datetime.datetime.fromtimestamp(epoch, datetime.timezone.utc).strftime("%Y%m%d%H%M%S")

    # -- script effects --
    def inventory_logs(self, env: dict) -> str:
        lines = [f"NOW {self.clock}"]
        for name, item in sorted(self.pvc.items()):
            lines.append(f"ENTRY|{name}|{item['type']}|{item['size']}|{item['mtime']}")
        for name in sorted(self.archives()):
            if (self.clock - self.pvc[name]["mtime"]) // 86400 > 30:
                lines.append(f"AGED|{name}")
        dump = env.get("DUMP_NAME")
        if dump:
            lines += [f"ARCHIVE-BEGIN {dump}", base64.encodebytes(self.pvc[dump]["data"]).decode().rstrip("\n"), "ARCHIVE-END"]
        return "\n".join(lines) + "\n"

    def probe_logs(self, env: dict) -> str:
        synthetic = self.clock - int(env["PROBE_AGE_DAYS"]) * 86400
        name = f"backup-{self.stamp(synthetic)}.nrbak"
        self.pvc[name] = {"type": "regular file", "size": 0, "mtime": synthetic, "data": b""}
        return f"PROBE name={name} size=0 mtime={synthetic} expected={synthetic} now={self.clock}\n"

    def seed_logs(self, env: dict) -> str:
        facts = dr_archive.parse_archive(self.pvc[env["ARCHIVE_NAME"]]["data"])
        lines = []
        for index, node in enumerate(env["NODE_IDS"].split(), start=1):
            code = self.seeded_exit if index == 3 else 0
            lines.append(f"SEED node={node} pre-clean=yes exit={code} start=10.00 end=10.0{index}")
            lines += [f"FILE node={node} name=cluster.meta size=98",
                      f"FILE node={node} name=snapshot-{facts['commitIndex']:020d}.bin size=1117",
                      f"FILE node={node} name=wal.bin size=48"]
        return "\n".join(lines) + "\n"

    def run_backup_job(self, name: str) -> None:
        self.clock += 600
        if self.backup_deletes_aged:
            for archive in list(self.archives()):
                if (self.clock - self.pvc[archive]["mtime"]) // 86400 > 30:
                    del self.pvc[archive]
        new = f"backup-{self.stamp(self.clock)}.nrbak"
        data = self.archive()
        self.pvc[new] = {"type": "regular file", "size": len(data), "mtime": self.clock, "data": data}
        self.jobs[name] = {"complete": not self.fail_backup_job, "logs": {
            "backup": f"backup: archive commit={self.commit} saved locally as {new}\n"}}

    # -- kubectl --
    def command(self, args, timeout=90, stdin=None):
        self.calls.append(list(args))
        tokens = list(args)
        if "config" in tokens and "view" in tokens:
            return json.dumps({"clusters": [{"cluster": {"server": "https://cluster.example/"}}]})
        index = tokens.index("-n")
        verb, rest = tokens[index + 2], tokens[index + 3:]
        handler = getattr(self, "verb_" + verb.replace("-", "_"), None)
        if handler is None:
            raise AssertionError(f"unsimulated kubectl verb {args}")
        return handler(rest, stdin)

    def verb_get(self, rest, stdin):
        kind = rest[0]
        name = rest[1] if len(rest) > 1 and not rest[1].startswith("-") else None
        as_name = rest[-2:] == ["-o", "name"]
        if as_name:
            exists = (kind == "configmap" and name in self.configmaps) or (kind == "pvc" and (
                name in self.claims))
            return f"{kind}/{name}\n" if exists else ""
        if kind == "cronjob":
            value = cronjob()
            value["spec"]["suspend"] = self.suspend
            if self.variant == "s3":
                value["spec"]["jobTemplate"]["spec"]["template"]["spec"]["initContainers"] = [{"name": "s3-uploader"}]
            return json.dumps(value)
        if kind == "configmap":
            if name == common.SCRIPTS_CONFIGMAP:
                data = {"backup.sh": REPOSITORY_SCRIPT}
                if self.variant == "s3":
                    data.update({"restore-from-s3.py": "x", "s3_uploader.py": "y"})
                return json.dumps({"data": data})
            if name == common.CONFIGS_CONFIGMAP:
                return json.dumps({"data": {"slot-0": "1", "slot-1": "2", "slot-2": "3"}})
            if name == "narumq-backup-config":
                return json.dumps({"data": {"BACKUP_S3_BUCKET": BUCKET, "BACKUP_S3_PREFIX": PREFIX,
                                            "AWS_REGION": "ap-northeast-2"}})
        if kind == "serviceaccount":
            return json.dumps({"metadata": {"annotations": {"eks.amazonaws.com/role-arn": "arn"}}})
        if kind == "pods" and "-l" in rest:
            return json.dumps({"items": self.brokers})
        if kind == "pods":
            items = list(self.brokers) + [p for p in self.pods.values()]
            return json.dumps({"items": items})
        if kind == "pvc":
            if name == common.BACKUP_PVC:
                return json.dumps({"metadata": {"name": name, "uid": "backup-pvc-uid"}, "status": {"phase": "Bound"}})
            claim = self.claims[name]
            return json.dumps({"metadata": {"uid": claim["uid"]}, "status": {"phase": claim["phase"]},
                               "spec": {"volumeName": claim["volume"]}})
        if kind == "jobs":
            return json.dumps({"items": []})
        if kind == "job":
            job = self.jobs[name]
            return json.dumps({"status": {"conditions": [{"type": "Complete" if job["complete"] else "Failed",
                                                          "status": "True"}], "succeeded": int(job["complete"])}})
        if kind == "pod":
            if name in self.pods:
                return json.dumps({"metadata": {"uid": self.pods[name]["metadata"]["uid"]},
                                   "status": {"phase": "Succeeded"}, "spec": {"nodeName": "node-0"}})
            broker = next(p for p in self.brokers if p["metadata"]["name"] == name)
            return json.dumps(broker)
        if kind == "statefulset":
            return json.dumps(self.sts)
        raise AssertionError(f"unsimulated get {rest}")

    def verb_create(self, rest, stdin):
        if rest[0] == "job":
            name = rest[1]
            self.created.append(f"job/{name}")
            if self.variant == "s3":
                self.clock += 60
                self.jobs[name] = {"complete": not self.fail_backup_job, "logs": {
                    "backup": f"backup: archive commit=52 sha256={SHA} uploaded\n",
                    "s3-uploader": f"backup-uploader: uploaded key={self.s3_key} commit=52 sha256={SHA}\n"}}
            else:
                self.run_backup_job(name)
            return json.dumps({"metadata": {"name": name}})
        if rest[0] == "configmap":
            self.configmaps.add(rest[1])
            return ""
        if rest[0] == "-f" and rest[1] != "-":
            if "--dry-run=client" in rest:
                return json.dumps({"kind": "List", "items": seed_job_items()})
            name = f"narumq-backup-list-{self.uid('x')}"
            self.jobs[name] = {"complete": True, "logs": {
                "list": f"LastModified\tSizeBytes\tKey\n2026-10-17T00:00:00+00:00\t5452\t{self.s3_key}\n"}}
            return json.dumps({"metadata": {"name": name}})
        manifest = json.loads(stdin)
        kind = manifest["kind"]
        if kind == "Pod":
            return self.create_pod(manifest)
        if kind == "PersistentVolumeClaim":
            name = manifest["metadata"]["name"]
            self.claims[name] = {"uid": self.uid("restore-claim"), "volume": f"pv-{name}", "phase": "Pending"}
            return json.dumps({"metadata": {"name": name}})
        if kind == "Job":
            name = f"{manifest['metadata']['generateName']}{self.uid('j')}"
            claim = manifest["spec"]["template"]["spec"]["volumes"][0]["persistentVolumeClaim"]["claimName"]
            self.claims[claim]["phase"] = "Bound"
            self.jobs[name] = {"complete": True, "logs": {
                "download-from-s3": f"/restore/{judges.archive_file_name(self.s3_key)}\n"}}
            return json.dumps({"metadata": {"name": name}})
        if kind == "StatefulSet":
            self.sts_exists = True
            self.brokers = self.make_brokers("new", restored=True)
            return json.dumps({"metadata": {"name": "narumq"}})
        raise AssertionError(f"unsimulated create {kind}")

    def create_pod(self, manifest) -> str:
        name = manifest["metadata"]["name"]
        container = manifest["spec"]["containers"][0]
        env = {item["name"]: item["value"] for item in container["env"]}
        script = container["command"][2]
        logs = {dr_retention.INVENTORY_SCRIPT: self.inventory_logs, dr_retention.PROBE_SCRIPT: self.probe_logs,
                dr_retention.SEED_SCRIPT: self.seed_logs}
        if script == dr_retention.REMOVE_PROBE_SCRIPT:
            del self.pvc[env["PROBE_NAME"]]
            text = f"PROBE-REMOVED {env['PROBE_NAME']}\n"
        else:
            text = logs[script](env)
        self.pods[name] = {"metadata": {"uid": self.uid("helper"), "name": name},
                           "spec": {"volumes": manifest["spec"]["volumes"]}, "status": {"phase": "Succeeded"}}
        self.pod_logs[name] = text
        return json.dumps({"metadata": {"name": name, "uid": self.pods[name]["metadata"]["uid"]}})

    def verb_logs(self, rest, stdin):
        target, container = rest[0], rest[rest.index("-c") + 1]
        if target.startswith("job/"):
            return self.jobs[target[4:]]["logs"][container]
        return self.pod_logs[target]

    def verb_delete(self, rest, stdin):
        kind, name = rest[0], rest[1]
        if kind == "pod":
            self.pods.pop(name, None)
        elif kind == "job":
            self.jobs.pop(name, None)
        elif kind == "configmap":
            self.configmaps.discard(name)
        elif kind == "statefulset":
            self.sts_exists = False
        return ""

    def verb_scale(self, rest, stdin):
        self.brokers = []
        return ""

    def verb_patch(self, rest, stdin):
        self.suspend = json.loads(rest[rest.index("-p") + 1])["spec"]["suspend"]
        return ""


class SimulatedFixture:
    def __init__(self, cluster: SimulatedCluster):
        self.cluster = cluster
        self.run_id = RUN
        self.kubectl = ["kubectl", "--context", RUN]

    def command(self, args, timeout=90, stdin=None):
        return self.cluster.command(args, timeout, stdin)

    def aws(self, *args):
        if args[:2] == ("eks", "describe-cluster"):
            return {"cluster": {"endpoint": "https://cluster.example", "resourcesVpcConfig": {"vpcId": VPC},
                                "tags": {"Purpose": "disposable-acceptance-fixture", "DataClass": "synthetic-test-data-only",
                                         "AcceptanceRun": RUN}}}
        if args[:2] == ("ec2", "describe-vpcs"):
            return {"Vpcs": [{"Tags": [{"Key": "Purpose", "Value": "disposable-acceptance-fixture"},
                                       {"Key": "DataClass", "Value": "synthetic-test-data-only"},
                                       {"Key": "AcceptanceRun", "Value": RUN}]}]}
        raise AssertionError(f"unsimulated aws {args}")

    def get_node(self, name):
        return {"metadata": {"labels": {manifests.ZONE_LABEL: "ap-northeast-2" + "abc"[int(name[-1])]}}}

    def management_probe(self):
        return {"quorumAvailable": True, "leaderNodeId": "1", "nodesByState": {"ready": 3}}


def no_sleep(_seconds):
    return None


class Ticker:
    def __init__(self, step: float = 1.0):
        self.value = 0.0
        self.step = step

    def __call__(self):
        self.value += self.step
        return self.value


class FakeEvent:
    """A non-blocking stand-in for threading.Event."""

    def __init__(self):
        self.flag = False

    def set(self):
        self.flag = True

    def wait(self, timeout=None):
        return self.flag
        return self.value


class RetentionRunTests(unittest.TestCase):
    def execute(self, cluster, **plan_changes):
        plan = dr_retention.load_plan({"runId": RUN, "clusterName": RUN, "vpcId": VPC, **plan_changes}, RUN)
        with tempfile.TemporaryDirectory() as directory:
            report = dr_retention.run(SimulatedFixture(cluster), plan, Path(directory), Path(directory), sleep=no_sleep,
                                      monotonic=Ticker(), utcnow=lambda: UTC_NOW, repository_script=REPOSITORY_SCRIPT)
            saved = sorted(p.name for p in Path(directory).iterdir())
        return report, saved

    def seeded_cluster(self, **kwargs) -> SimulatedCluster:
        cluster = SimulatedCluster(**kwargs)
        older = f"backup-{cluster.stamp(NOW_EPOCH - 3 * 86400)}.nrbak"
        data = cluster.archive()
        cluster.pvc[older] = {"type": "regular file", "size": len(data), "mtime": NOW_EPOCH - 3 * 86400, "data": data}
        return cluster

    def test_happy_path_passes_every_criterion_and_cleans_up(self):
        cluster = self.seeded_cluster()
        report, saved = self.execute(cluster)
        self.assertEqual(report["result"], "PASS", report)
        self.assertEqual(sorted(report["criteria"]), sorted(dr_retention.REQUIRED))
        self.assertEqual(report["failedCriteria"], [])
        self.assertIn("retention-seed.json", saved)
        self.assertFalse(cluster.jobs)
        self.assertFalse(cluster.pods)
        self.assertEqual(len(cluster.archives()), 3)  # preexisting, 046 backup, 047 backup; probe deleted

    def test_probe_not_deleted_fails_and_cleanup_removes_it(self):
        cluster = self.seeded_cluster(backup_deletes_aged=False)
        report, _ = self.execute(cluster)
        self.assertEqual(report["result"], "FAIL")
        self.assertIn("retention.probeDeleted", report["failedCriteria"])
        self.assertTrue(report["probeRemovedByCleanup"])
        self.assertFalse([n for n in cluster.archives() if cluster.pvc[n]["mtime"] < NOW_EPOCH - 30 * 86400])

    def test_commit_index_must_equal_the_plan(self):
        report, _ = self.execute(self.seeded_cluster(commit=52))
        self.assertEqual(report["result"], "FAIL")
        self.assertIn("archive.commitIndexAsPlanned", report["failedCriteria"])
        self.assertNotIn("archive.nmqbkup3Integrity", report["failedCriteria"])
        ok, _ = self.execute(self.seeded_cluster(commit=52), expectedCommitIndex=52)
        self.assertEqual(ok["result"], "PASS")

    def test_failed_seed_node_fails_the_run(self):
        report, _ = self.execute(self.seeded_cluster(seeded_exit=1))
        self.assertEqual(report["result"], "FAIL")
        self.assertIn("seed.threeNodesSeeded", report["failedCriteria"])
        self.assertIn("seed.podSucceeded", report["criteria"])

    def test_s3_variant_is_rejected_before_any_mutation(self):
        cluster = self.seeded_cluster(variant="s3")
        report, _ = self.execute(cluster)
        self.assertEqual(report["result"], "FAIL")
        self.assertEqual((report["failedStep"], report["errorCode"]), ("policy", "cronjob_policy_unmet"))
        self.assertFalse(any(call[:1] == ["kubectl"] and "create" in call for call in cluster.calls))

    def test_aged_archive_before_the_run_is_rejected(self):
        cluster = self.seeded_cluster()
        data = cluster.archive()
        cluster.pvc["backup-20200101000000.nrbak"] = {"type": "regular file", "size": len(data),
                                                      "mtime": NOW_EPOCH - 90 * 86400, "data": data}
        report, _ = self.execute(cluster)
        self.assertEqual(report["errorCode"], "preexisting_aged_archives")
        self.assertEqual(report["result"], "FAIL")

    def test_scheduled_window_guard(self):
        plan = dr_retention.load_plan({"runId": RUN, "clusterName": RUN, "vpcId": VPC}, RUN)
        with tempfile.TemporaryDirectory() as directory:
            report = dr_retention.run(SimulatedFixture(self.seeded_cluster()), plan, Path(directory), Path(directory),
                                      sleep=no_sleep, monotonic=Ticker(),
                                      utcnow=lambda: datetime.datetime(2026, 10, 17, 2, 5, tzinfo=datetime.timezone.utc),
                                      repository_script=REPOSITORY_SCRIPT)
        self.assertEqual(report["errorCode"], "scheduled_backup_window_too_close")


class FakeMqttClient:
    store: dict = {}

    def __init__(self, identity, password, port, clean, expiry):
        self.identity, self.clean = identity, clean
        self.session_present = (not clean) and identity in FakeMqttClient.store.get("sessions", set())
        self.received = FakeEvent()
        self.messages: list[dict] = []
        self.acks: list[dict] = []
        if identity == "dr-restore-subscriber" and clean:
            FakeMqttClient.store.setdefault("sessions", set()).add(identity)

    def subscribe(self, topic):
        self.topic = topic
        retained = FakeMqttClient.store.get("retained", {}).get(topic)
        if retained is not None:
            self.messages.append({"topic": topic, "sha256": hashlib.sha256(retained).hexdigest(), "retain": True, "qos": 1})
            self.received.set()

    def publish(self, topic, payload, retain=False):
        FakeMqttClient.store.setdefault("retained", {})[topic] = payload
        self.acks.append({"mid": 1, "reason": "Success", "reasonValue": 0})

    def close(self):
        return None


class FakeTunnel:
    started: list[str] = []

    def __init__(self, fixture, pod, uid, management, mqtt):
        self.pod = pod

    def start(self):
        FakeTunnel.started.append(self.pod)

    def close(self):
        return None


class S3RestoreRunTests(unittest.TestCase):
    def execute(self, cluster, mqtt_store=None, tick=1.0, **plan_changes):
        FakeMqttClient.store = mqtt_store if mqtt_store is not None else {}
        plan = dr_s3_restore.load_plan({"runId": RUN, "clusterName": RUN, "vpcId": VPC, "bucket": BUCKET,
                                        "prefix": PREFIX, "region": "ap-northeast-2", **plan_changes}, RUN)
        with tempfile.TemporaryDirectory() as directory:
            (Path(directory) / "mqtt.password").write_text("secret", encoding="utf-8")
            output = Path(directory) / "out"
            output.mkdir()
            report = dr_s3_restore.run(SimulatedFixture(cluster), plan, output, Path(directory), sleep=no_sleep,
                                       monotonic=Ticker(tick), utcnow=lambda: UTC_NOW, client_factory=FakeMqttClient,
                                       tunnel_factory=FakeTunnel)
            saved = sorted(p.name for p in output.iterdir())
            written = "".join(p.read_text(encoding="utf-8") for p in output.iterdir())
        return report, saved, written

    def test_happy_path(self):
        cluster = SimulatedCluster(variant="s3")
        report, saved, written = self.execute(cluster)
        self.assertEqual(report["result"], "PASS", report)
        self.assertEqual(sorted(report["criteria"]), sorted(dr_s3_restore.REQUIRED))
        self.assertIsNotNone(report["rtoSeconds"])
        self.assertIn("s3-restore-rto.json", saved)
        self.assertIn("s3-restore-original-statefulset.json", saved)
        self.assertNotIn("secret", written)
        self.assertEqual(cluster.suspend, False)  # resumed after verification
        self.assertEqual({c: v["uid"] for c, v in cluster.claims.items() if c.startswith("data-")},
                         {f"data-narumq-{i}": f"claim-uid-{i}" for i in range(3)})
        self.assertEqual(sorted(c for c in cluster.claims if c.startswith("restore-data-")), manifests.restore_claim_names())
        self.assertFalse(cluster.jobs)
        self.assertNotIn("narumq-backup-restore-request", cluster.configmaps)
        deletes = [c for c in cluster.calls if "delete" in c]
        self.assertTrue(any("--cascade=orphan" in c for c in deletes))
        self.assertFalse(any(c for c in deletes if "pvc" in c))

    def test_missing_retained_message_after_restore_fails(self):
        cluster = SimulatedCluster(variant="s3")
        original = FakeMqttClient.subscribe

        def drop_retained(self, topic):
            if self.identity == "dr-restore-reader":
                return None
            return original(self, topic)

        FakeMqttClient.subscribe = drop_retained
        try:
            report, _, _ = self.execute(cluster)
        finally:
            FakeMqttClient.subscribe = original
        self.assertEqual(report["result"], "FAIL")
        self.assertIn("mqtt.retainedSha256", report["failedCriteria"])

    def test_missing_session_after_restore_fails(self):
        cluster = SimulatedCluster(variant="s3")
        FakeMqttClient.store = {}
        original = FakeMqttClient.__init__

        def forget_session(self, identity, password, port, clean, expiry):
            original(self, identity, password, port, clean, expiry)
            if not clean:
                self.session_present = False

        FakeMqttClient.__init__ = forget_session
        try:
            report, _, _ = self.execute(cluster)
        finally:
            FakeMqttClient.__init__ = original
        self.assertIn("mqtt.sessionPresent", report["failedCriteria"])

    def test_rto_over_budget_fails(self):
        report, _, _ = self.execute(SimulatedCluster(variant="s3"), tick=100.0, rtoBudgetSeconds=60)
        self.assertEqual(report["result"], "FAIL")
        self.assertIn("rto.withinBudget", report["failedCriteria"])

    def test_unlisted_key_stops_before_any_new_claim(self):
        cluster = SimulatedCluster(variant="s3")
        original = SimulatedCluster.verb_create

        def other_key(self, rest, stdin):
            if rest[:1] == ["-f"] and rest[1] != "-" and "--dry-run=client" not in rest:
                key = f"{PREFIX}/2026/10/16/backup-20261016T000000Z-9-{'cd' * 32}.nrbak"
                name = f"narumq-backup-list-{self.uid('x')}"
                self.jobs[name] = {"complete": True, "logs": {"list": f"LastModified\tSizeBytes\tKey\nt\t1\t{key}\n"}}
                return json.dumps({"metadata": {"name": name}})
            return original(self, rest, stdin)

        SimulatedCluster.verb_create = other_key
        try:
            report, _, _ = self.execute(cluster)
        finally:
            SimulatedCluster.verb_create = original
        self.assertEqual(report["errorCode"], "uploaded_key_not_listed")
        self.assertFalse([c for c in cluster.claims if c.startswith("restore-data-")])
        self.assertTrue(cluster.suspend)  # not resumed after a failed restore

    def test_wrong_s3_wiring_is_rejected_first(self):
        cluster = SimulatedCluster(variant="s3")
        report, _, _ = self.execute(cluster, bucket="other-bucket")
        self.assertEqual(report["errorCode"], "s3_configuration_not_as_planned")
        self.assertFalse(any("scale" in c for c in cluster.calls))

    def test_local_variant_is_not_an_s3_fixture(self):
        report, _, _ = self.execute(SimulatedCluster(variant="local"))
        self.assertEqual(report["result"], "FAIL")
        self.assertEqual(report["failedStep"], "config")

    def test_failed_backup_job_leaves_source_running(self):
        cluster = SimulatedCluster(variant="s3")
        cluster.fail_backup_job = True
        report, _, _ = self.execute(cluster)
        self.assertEqual(report["errorCode"], "s3_backup_job_failed")
        self.assertEqual(len(cluster.brokers), 3)

    def test_mqtt_helpers_require_puback_and_resume(self):
        with tempfile.TemporaryDirectory() as directory:
            (Path(directory) / "mqtt.password").write_text("secret", encoding="utf-8")
            FakeMqttClient.store = {}
            marker = dr_s3_restore.mqtt_prepare(Path(directory), RUN, 1, FakeMqttClient)
            result = dr_s3_restore.mqtt_verify(Path(directory), RUN, 1, marker["sha256"], FakeMqttClient)
            self.assertTrue(result["sessionPresent"] and result["retainedMatches"])
            wrong = dr_s3_restore.mqtt_verify(Path(directory), RUN, 1, "0" * 64, FakeMqttClient)
            self.assertFalse(wrong["retainedMatches"])


# -- composite -------------------------------------------------------------------------------------------------------

class FakeContracts:
    CRITERIA = ("p.one", "p.two")

    class PlanError(ValueError):
        pass

    @classmethod
    def validate_plan(cls, plan, run_id):
        if plan.get("runId") != run_id:
            raise ValueError("plan does not identify this run")

    @classmethod
    def load_plan(cls, plan, run_id):
        if plan.get("runId") != run_id:
            raise cls.PlanError("plan does not identify this run")
        return dict(plan)

    @staticmethod
    def criteria_for(plan):
        return ("r.one",)


def contracts():
    power = types.SimpleNamespace(validate_plan=FakeContracts.validate_plan, CRITERIA=FakeContracts.CRITERIA)
    reseed = types.SimpleNamespace(load_plan=FakeContracts.load_plan, criteria_for=FakeContracts.criteria_for,
                                   PlanError=FakeContracts.PlanError)
    return power, reseed


class ProductionTests(unittest.TestCase):
    def inputs(self, **changes):
        plan = dr_production.load_plan({"runId": RUN, "bucket": BUCKET, "prefix": PREFIX, "region": "ap-northeast-2"}, RUN)
        restore = {"runId": RUN, "clusterName": RUN, "vpcId": VPC, "bucket": BUCKET, "prefix": PREFIX,
                   "region": "ap-northeast-2"}
        power = {"runId": RUN, "clusterName": RUN, "vpcId": VPC}
        reseed = {"runId": RUN, "clusterName": RUN, "vpcId": VPC}
        return plan, {**restore, **changes.get("restore", {})}, {**power, **changes.get("power", {})}, {
            **reseed, **changes.get("reseed", {})}

    def test_validation_rejects_missing_or_inconsistent_inputs_before_any_mutation(self):
        plan, restore, power, reseed = self.inputs()
        stage = dr_production.validate_inputs(plan, restore, power, reseed, RUN, contracts())
        self.assertEqual((stage["powerCriteria"], stage["reseedCriteria"]), (("p.one", "p.two"), ("r.one",)))
        cases = [
            {"restore": {"prefix": "another-prefix"}},
            {"restore": {"bucket": "another-bucket"}},
            {"power": {"runId": "other-run-12345"}},
            {"reseed": {"runId": "other-run-12345"}},
            {"power": {"clusterName": "narumq-ha-acceptance-other"}},
            {"reseed": {"vpcId": "vpc-other"}},
            {"restore": {"clusterName": "narumq-eks"}},
        ]
        for changes in cases:
            with self.subTest(changes=changes):
                with self.assertRaises(common.PlanError):
                    dr_production.validate_inputs(*self.inputs(**changes), RUN, contracts())

    def run_composite(self, audit_ok=True, power_code=0, power_result="PASS", reseed_code=0, restore_result="PASS",
                      rto=120.0, stale=False, other_run=False):
        plan, restore_raw, power_raw, reseed_raw = self.inputs()
        stage_plans = dr_production.validate_inputs(plan, restore_raw, power_raw, reseed_raw, RUN, contracts())
        calls: list[str] = []

        def audit(fixture, audit_plan, now):
            calls.append("audit")
            criteria = {name: audit_ok for name in dr_production.AUDIT_CRITERIA}
            return criteria, {"fake": True}

        def runner(argv, timeout, log_path):
            mode = argv[2]
            calls.append(mode)
            child = Path(argv[argv.index("--output") + 1])
            child.mkdir(parents=True)
            code = power_code if mode == "power-loss" else reseed_code
            required = ("p.one", "p.two") if mode == "power-loss" else ("r.one",)
            result = power_result if mode == "power-loss" else "PASS"
            started = "2020-01-01T00:00:00+00:00" if stale else timestamp_now()
            summary = {"result": result, "runId": "other-run-12345" if other_run else RUN, "startedAt": started,
                       "failedStep": None, "failedCriteria": [], "criteria": {name: True for name in required}}
            (child / "summary.json").write_text(json.dumps(summary), encoding="utf-8")
            log_path.write_text("", encoding="utf-8")
            return code

        def restore(fixture, restore_plan, output, private):
            calls.append("restore")
            return {"result": restore_result, "rtoSeconds": rto, "failedCriteria": [], "errorCode": None}

        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            files = {"power": output / "power.json", "reseed": output / "reseed.json"}
            (output / "run-output").mkdir()
            report = dr_production.run(object(), plan, stage_plans, files, output, output / "run-output",
                                       runner=runner, restore_runner=restore, audit=audit, now=lambda: UTC_NOW)
        return report, calls

    def test_all_stages_passing_passes(self):
        report, calls = self.run_composite()
        self.assertEqual(report["result"], "PASS", report)
        self.assertEqual(calls, ["audit", "power-loss", "run", "restore"])
        self.assertEqual(sorted(report["criteria"]), sorted(dr_production.REQUIRED))
        self.assertTrue(all(stage["result"] == "PASS" for stage in report["stages"].values()))
        self.assertEqual(report["approvedPolicy"]["fullClusterRtoSeconds"], 3600)

    def test_first_unmet_stage_stops_the_run(self):
        report, calls = self.run_composite(audit_ok=False)
        self.assertEqual((report["result"], calls), ("FAIL", ["audit"]))
        self.assertEqual(report["stages"]["power-loss"]["result"], "NOT_RUN")
        self.assertTrue(report["errorCode"].startswith("retention_audit_unmet:"))
        report, calls = self.run_composite(power_code=1)
        self.assertEqual((report["result"], calls), ("FAIL", ["audit", "power-loss"]))
        self.assertEqual(report["errorCode"], "stage_exit_code_nonzero")
        report, calls = self.run_composite(power_result="FAIL")
        self.assertEqual(report["errorCode"], "stage_summary_not_pass")
        report, calls = self.run_composite(reseed_code=1)
        self.assertEqual((report["result"], calls), ("FAIL", ["audit", "power-loss", "run"]))

    def test_restore_failure_or_rto_over_an_hour_fails(self):
        report, _ = self.run_composite(restore_result="FAIL")
        self.assertEqual(report["result"], "FAIL")
        self.assertFalse(report["criteria"]["restore.fullRestorePassed"])
        report, _ = self.run_composite(rto=3601.0)
        self.assertEqual(report["result"], "FAIL")
        self.assertIn("restore.rtoWithinApprovedHour", report["failedCriteria"])

    def test_a_summary_from_another_run_or_time_is_not_accepted(self):
        report, _ = self.run_composite(stale=True)
        self.assertEqual(report["errorCode"], "stage_summary_not_from_this_run")
        report, _ = self.run_composite(other_run=True)
        self.assertEqual(report["errorCode"], "stage_summary_not_from_this_run")

    def test_default_runner_reports_timeout_as_none(self):
        with tempfile.TemporaryDirectory() as directory:
            log = Path(directory) / "p.log"
            self.assertEqual(dr_production.default_runner([sys.executable, "-c", "import sys; sys.exit(3)"], 30, log), 3)
            self.assertIsNone(dr_production.default_runner([sys.executable, "-c", "import time; time.sleep(30)"], 0.5, log))

    def test_aws_audit_reads_and_unreadable_controls_are_unmet(self):
        class Aws:
            def __init__(self, fail=()):
                self.fail, self.calls = fail, []

            def aws(self, *args):
                self.calls.append(args[1])
                if args[1] in self.fail:
                    raise common.CommandFailed("command exit 254")
                return {"get-public-access-block": GOOD_CONFIG["publicAccessBlock"],
                        "get-bucket-encryption": GOOD_CONFIG["encryption"],
                        "get-bucket-lifecycle-configuration": GOOD_CONFIG["lifecycle"],
                        "get-bucket-policy": GOOD_CONFIG["policy"], "get-bucket-location": GOOD_CONFIG["location"],
                        "list-objects-v2": {"Contents": daily_objects(31)}}[args[1]]

        plan = dr_production.load_plan({"runId": RUN, "bucket": BUCKET, "prefix": PREFIX, "region": "ap-northeast-2"}, RUN)
        criteria, evidence = dr_production.audit_s3(Aws(), plan, UTC_NOW)
        self.assertTrue(all(criteria.values()), criteria)
        self.assertEqual(evidence["unreadable"], [])
        criteria, evidence = dr_production.audit_s3(Aws(fail=("get-bucket-lifecycle-configuration",)), plan, UTC_NOW)
        self.assertFalse(criteria["audit.lifecycleExpiresAfter30Days"])
        self.assertEqual(evidence["unreadable"], ["lifecycle"])
        criteria, _ = dr_production.audit_s3(Aws(fail=("list-objects-v2",)), plan, UTC_NOW)
        self.assertFalse(criteria["audit.archivesPresent"])


def timestamp_now() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


class CliTests(unittest.TestCase):
    def test_every_mode_requires_execute_and_plan(self):
        import importlib.util
        spec = importlib.util.spec_from_file_location("eks_backup_dr_main", DIR / "main.py")
        main = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(main)
        for argv in (["backup-retention", "--private", "p", "--output", "o", "--plan", "f"],
                     ["s3-full-restore", "--private", "p", "--output", "o", "--plan", "f"],
                     ["production-storage-retention", "--private", "p", "--output", "o", "--plan", "f", "--restore-plan", "r",
                      "--power-loss-plan", "a", "--reseed-plan", "b"]):
            with self.subTest(argv=argv[0]):
                with self.assertRaises(SystemExit):
                    main.main(argv)
        with self.assertRaises(SystemExit):
            main.main(["production-storage-retention", "--private", "p", "--output", "o", "--plan", "f", "--execute"])

    def test_backup_scenario_rejects_a_substituted_commit_index(self):
        import importlib.util
        spec = importlib.util.spec_from_file_location("eks_backup_dr_commit_boundary", DIR / "main.py")
        main = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(main)
        raw = {"runId": RUN, "clusterName": RUN, "vpcId": VPC, "expectedCommitIndex": 101}
        with self.assertRaisesRegex(common.PlanError, "scenario_requires_commit_index_100"):
            main._backup_retention(None, raw, RUN, None)

    def test_missing_private_directory_fails_with_a_stable_code(self):
        import importlib.util
        spec = importlib.util.spec_from_file_location("eks_backup_dr_main2", DIR / "main.py")
        main = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(main)
        with tempfile.TemporaryDirectory() as directory:
            plan = Path(directory) / "plan.json"
            plan.write_text(json.dumps({"runId": RUN, "clusterName": RUN, "vpcId": VPC}), encoding="utf-8")
            output = Path(directory) / RUN
            args = types.SimpleNamespace(private=Path(directory) / "absent", output=output, plan=plan)
            report = main._execute(args, "backup-retention", lambda a, raw, run_id, fixture: {})
            self.assertEqual((report["result"], report["errorCode"]), ("FAIL", "private_directory_incomplete"))
            self.assertTrue((output / "summary.json").is_file())
            unreadable = types.SimpleNamespace(private=Path(directory), output=Path(directory) / "second",
                                               plan=Path(directory) / "none.json")
            self.assertEqual(main._execute(unreadable, "backup-retention", lambda *a: {})["errorCode"], "plan_unreadable")


if __name__ == "__main__":
    unittest.main()
