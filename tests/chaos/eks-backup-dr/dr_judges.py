"""Pure parsers and judges of the backup/DR acceptance runs. Nothing here touches a cluster or AWS."""
from __future__ import annotations

import base64
import binascii
import datetime
import json
import re

LEGACY_ARCHIVE = re.compile(r"^backup-[0-9]{14}\.nrbak$")
LOCAL_BACKUP_LOG = re.compile(r"^backup: archive commit=([0-9]+) saved locally as (backup-[0-9]{14}\.nrbak)$", re.M)
S3_BACKUP_LOG = re.compile(r"^backup: archive commit=([0-9]+) sha256=([0-9a-f]{64}) uploaded$", re.M)
UPLOADER_LOG = re.compile(r"^backup-uploader: uploaded key=(\S+) commit=([0-9]+) sha256=([0-9a-f]{64})$", re.M)
TEMPORARY_PREFIXES = (".backup-", ".headers-", ".http-status-", ".sha256-", ".staging-")
MIN_PROBE_AGE_DAYS = 31  # `find -mtime +30` needs an age of at least 31 whole days


def aggregate(criteria: dict, required: tuple[str, ...], failed_step: str | None) -> dict:
    """PASS only when every required criterion is exactly True and no step failed."""
    verdicts = {name: criteria.get(name) is True for name in required}
    failed = [name for name, ok in verdicts.items() if not ok]
    return {"result": "PASS" if not failed and failed_step is None else "FAIL", "criteria": verdicts,
            "failedCriteria": failed, "failedStep": failed_step}


# -- CronJob and retention policy (CLU-SCN-046) -------------------------------------------------------------

def normalized_script(text: str) -> str:
    return text.replace("\r\n", "\n")


def cronjob_policy(cronjob: dict, scripts: dict, repository_script: str, schedule: str) -> tuple[dict, dict]:
    """Judge the deployed CronJob and script against the approved policy. Returns (criteria, evidence)."""
    spec = cronjob.get("spec", {})
    template = spec.get("jobTemplate", {}).get("spec", {}).get("template", {})
    pod = template.get("spec", {})
    containers = pod.get("containers", [])
    backup = containers[0] if len(containers) == 1 else {}
    deployed = normalized_script((scripts.get("data") or {}).get("backup.sh", ""))
    volumes = {v.get("name"): v for v in pod.get("volumes", [])}
    s3_environment = [item.get("name") for item in backup.get("env", []) if str(item.get("name", "")).startswith("BACKUP_S3")]
    criteria = {
        "cronjob.schedule": spec.get("schedule") == schedule,
        "cronjob.notSuspended": spec.get("suspend") in (None, False),
        "cronjob.runsDeployedScript": backup.get("command") == ["/bin/sh", "/scripts/backup.sh"],
        "cronjob.usesBackupPvc": volumes.get("backup-storage", {}).get("persistentVolumeClaim", {}).get(
            "claimName") == "narumq-backup-pvc",
        "cronjob.localOnlyVariant": not pod.get("initContainers") and not backup.get("envFrom") and not s3_environment,
        "cronjob.scriptMatchesRepository": bool(deployed) and deployed == normalized_script(repository_script),
        "cronjob.localMtimePlus30Cleanup": bool(re.search(r"find\s+\"\$archive\"\s+-type\s+f\s+-mtime\s+\+30\s+-delete",
                                                          deployed)),
    }
    evidence = {"schedule": spec.get("schedule"), "suspend": spec.get("suspend"),
                "concurrencyPolicy": spec.get("concurrencyPolicy"), "image": backup.get("image"),
                "scriptConfigMapKeys": sorted((scripts.get("data") or {}).keys()),
                "deployedScriptLines": deployed.count("\n"), "s3Environment": s3_environment}
    return criteria, evidence


# -- PVC inventory ------------------------------------------------------------------------------------------

def parse_inventory(logs: str) -> dict:
    """Parse the inventory Pod log: NOW, ENTRY|name|type|size|mtime, AGED|name, optional base64 archive."""
    inventory = {"now": None, "entries": {}, "aged": [], "archive": None, "archiveName": None}
    capturing: list[str] | None = None
    for raw in logs.splitlines():
        line = raw.rstrip("\r")
        if capturing is not None:
            if line == "ARCHIVE-END":
                try:
                    inventory["archive"] = base64.b64decode("".join(capturing), validate=True)
                except (binascii.Error, ValueError) as exc:
                    raise ValueError("inventory_archive_base64_invalid") from exc
                capturing = None
            else:
                capturing.append(line.strip())
        elif line.startswith("ARCHIVE-BEGIN "):
            inventory["archiveName"] = line.split(" ", 1)[1]
            capturing = []
        elif line.startswith("NOW "):
            inventory["now"] = int(line.split(" ", 1)[1])
        elif line.startswith("ENTRY|"):
            _, name, kind, size, mtime = line.split("|", 4)
            inventory["entries"][name] = {"type": kind, "size": int(size), "mtime": int(mtime)}
        elif line.startswith("AGED|"):
            inventory["aged"].append(line.split("|", 1)[1])
    if capturing is not None:
        raise ValueError("inventory_archive_truncated")
    if inventory["now"] is None:
        raise ValueError("inventory_clock_missing")
    return inventory


def archive_names(inventory: dict) -> set[str]:
    return {name for name, entry in inventory["entries"].items()
            if LEGACY_ARCHIVE.fullmatch(name) and entry["type"] == "regular file"}


def parse_probe_line(logs: str) -> dict:
    match = re.search(r"^PROBE name=(\S+) size=([0-9]+) mtime=([0-9]+) expected=([0-9]+) now=([0-9]+)$", logs, re.M)
    if match is None:
        raise ValueError("probe_line_missing")
    return {"name": match.group(1), "size": int(match.group(2)), "mtime": int(match.group(3)),
            "expectedMtime": int(match.group(4)), "now": int(match.group(5))}


def judge_probe_creation(probe: dict, with_probe: dict, before: dict, age_days: int) -> dict:
    return {
        "probe.createdEmptyWithSyntheticMtime": (
            bool(LEGACY_ARCHIVE.fullmatch(probe["name"])) and probe["size"] == 0
            and probe["mtime"] == probe["expectedMtime"]
            and probe["now"] - probe["mtime"] >= max(age_days, MIN_PROBE_AGE_DAYS) * 86400
            and probe["name"] not in before["entries"]
            and with_probe["entries"].get(probe["name"], {}).get("mtime") == probe["expectedMtime"]),
        "probe.singleAgedArchiveBeforeJob": before["aged"] == [] and with_probe["aged"] == [probe["name"]],
    }


def judge_retention(before: dict, probe_name: str, after: dict, logged_archive: str | None, job_ok: bool,
                    new_archive_valid: bool) -> tuple[dict, dict]:
    """Judge the Job after the probe: probe gone, every earlier archive untouched, exactly one new archive."""
    before_archives, after_archives = archive_names(before), archive_names(after)
    preserved = all(name in after["entries"] and after["entries"][name]["type"] == entry["type"]
                    and (name not in before_archives or after["entries"][name] == entry)
                    for name, entry in before["entries"].items())
    created = sorted(after_archives - before_archives - {probe_name})
    leftovers = sorted(name for name in after["entries"] if name.startswith(TEMPORARY_PREFIXES))
    criteria = {
        "retention.jobSucceeded": job_ok,
        "retention.probeDeleted": probe_name not in after["entries"] and probe_name not in after["aged"]
        and after["aged"] == [],
        "retention.existingArchivesPreserved": preserved and bool(before_archives),
        "retention.newArchiveCreated": logged_archive is not None and created == [logged_archive],
        "retention.newArchiveIntegrity": new_archive_valid,
        "retention.noTemporaryLeftovers": not leftovers,
    }
    return criteria, {"preservedArchives": sorted(before_archives), "createdArchives": created,
                      "probeName": probe_name, "agedAfter": after["aged"], "temporaryLeftovers": leftovers}


def parse_local_backup_log(logs: str) -> tuple[int, str]:
    matches = LOCAL_BACKUP_LOG.findall(logs)
    if len(matches) != 1:
        raise ValueError("backup_log_archive_line_missing")
    return int(matches[0][0]), matches[0][1]


# -- offline seed (CLU-SCN-046) -------------------------------------------------------------------------------

def parse_seed_logs(logs: str) -> dict[int, dict]:
    """Per-node result of the in-cluster clean-directory seed Pod: exit code, /proc/uptime seconds, files."""
    nodes: dict[int, dict] = {}
    for line in logs.splitlines():
        match = re.fullmatch(r"SEED node=(\d+) pre-clean=(yes|no) exit=(\d+) start=([0-9.]+) end=([0-9.]+)", line.strip())
        if match:
            nodes[int(match.group(1))] = {
                "preClean": match.group(2) == "yes", "exit": int(match.group(3)),
                "seconds": round(float(match.group(5)) - float(match.group(4)), 2), "files": {}}
            continue
        match = re.fullmatch(r"FILE node=(\d+) name=(\S+) size=(\d+)", line.strip())
        if match and int(match.group(1)) in nodes:
            nodes[int(match.group(1))]["files"][match.group(2)] = int(match.group(3))
    return nodes


def judge_seed(nodes: dict[int, dict], expected_nodes: list[int], commit_index: int, budget_seconds: int) -> tuple[dict, dict]:
    def snapshot_index(files: dict) -> int | None:
        found = [int(m.group(1)) for name in files if (m := re.fullmatch(r"snapshot-([0-9]{20})\.bin", name))]
        return found[0] if len(found) == 1 else None

    ids = sorted(nodes)
    total = round(sum(node["seconds"] for node in nodes.values()), 2)
    criteria = {
        "seed.threeNodesSeeded": ids == sorted(expected_nodes) and len(ids) == 3
        and all(node["exit"] == 0 for node in nodes.values()),
        "seed.cleanDirectories": ids == sorted(expected_nodes) and all(node["preClean"] for node in nodes.values()),
        "seed.expectedFilesPresent": bool(nodes) and all(
            "cluster.meta" in node["files"] and "wal.bin" in node["files"]
            and snapshot_index(node["files"]) is not None for node in nodes.values()),
        "seed.noRestoreMarker": bool(nodes) and all(not any("restore" in name for name in node["files"])
                                                    for node in nodes.values()),
        "seed.snapshotAtCommitIndex": bool(nodes) and all(snapshot_index(node["files"]) == commit_index
                                                          for node in nodes.values()),
        "seed.withinRtoBudget": bool(nodes) and total <= budget_seconds,
    }
    return criteria, {"perNodeSeconds": {str(i): nodes[i]["seconds"] for i in ids}, "totalSeedSeconds": total,
                      "maxNodeSeconds": max((n["seconds"] for n in nodes.values()), default=None),
                      "files": {str(i): nodes[i]["files"] for i in ids}, "budgetSeconds": budget_seconds,
                      "scope": "three sequential offline -cluster-restore seeds only; no voter start, no quorum"}


# -- S3 restore (CLU-SCN-050) ---------------------------------------------------------------------------------

def key_pattern(prefix: str) -> re.Pattern:
    return re.compile(re.escape(prefix) + r"/[0-9]{4}/[0-9]{2}/[0-9]{2}/backup-[0-9]{8}T[0-9]{6}Z-([0-9]+)-([0-9a-f]{64})\.nrbak")


def parse_s3_backup_logs(backup_log: str, uploader_log: str, prefix: str) -> dict:
    """The key and digest the S3 CronJob run reports for its new archive; both logs must agree."""
    sha_lines, key_lines = S3_BACKUP_LOG.findall(backup_log), UPLOADER_LOG.findall(uploader_log)
    if len(sha_lines) != 1 or len(key_lines) != 1:
        raise ValueError("s3_backup_log_lines_missing")
    commit, sha = sha_lines[0]
    key, key_commit, key_sha = key_lines[0]
    pattern = key_pattern(prefix)
    match = pattern.fullmatch(key)
    if match is None or match.group(1) != commit or match.group(2) != sha or key_commit != commit or key_sha != sha:
        raise ValueError("s3_backup_key_does_not_match_logged_commit_and_sha")
    return {"key": key, "commitIndex": int(commit), "sha256": sha}


def parse_listing(output: str, prefix: str) -> list[dict]:
    """restore-from-s3.py list output: a tab header, then LastModified, size and key per line."""
    items = []
    for line in output.splitlines():
        parts = line.rstrip("\r").split("\t")
        if len(parts) != 3 or parts[0] == "LastModified":
            continue
        if not parts[2].startswith(prefix + "/"):
            raise ValueError("listing_key_outside_prefix")
        items.append({"lastModified": parts[0], "size": int(parts[1]), "key": parts[2]})
    return items


def archive_file_name(key: str) -> str:
    return key.rsplit("/", 1)[-1]


# -- S3 retention audit (CLU-SCN-159) --------------------------------------------------------------------------

def parse_time(value: str) -> datetime.datetime:
    parsed = datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=datetime.timezone.utc)


def audit_cadence(objects: list[dict], now: datetime.datetime, settings: dict) -> tuple[dict, dict]:
    """Daily cadence, RPO and observed retention over the objects of one exact prefix."""
    pattern = key_pattern(settings["prefix"])
    archives = sorted((parse_time(item["LastModified"]), item["Key"]) for item in objects if pattern.fullmatch(item["Key"]))
    limit = settings["maxGapSeconds"] + settings["gapToleranceSeconds"]
    gaps = [(b[0] - a[0]).total_seconds() for a, b in zip(archives, archives[1:])]
    newest_age = (now - archives[-1][0]).total_seconds() if archives else None
    observed = (now - archives[0][0]).total_seconds() if archives else None
    required = settings["minObservedDays"] * 86400 - settings["lifecycleSlackSeconds"]
    criteria = {
        "audit.archivesPresent": len(archives) >= 2,
        "audit.everyGapWithinDaily": bool(gaps) and all(gap <= limit for gap in gaps),
        "audit.newestWithinRpo": newest_age is not None and newest_age <= limit,
        "audit.observedRetentionDays": observed is not None and observed >= required,
    }
    return criteria, {
        "archiveCount": len(archives), "nonArchiveKeys": len(objects) - len(archives),
        "oldest": archives[0][0].isoformat() if archives else None,
        "newest": archives[-1][0].isoformat() if archives else None,
        "maxGapSeconds": max(gaps) if gaps else None, "gapLimitSeconds": limit,
        "newestAgeSeconds": newest_age, "observedSeconds": observed, "requiredObservedSeconds": required,
        "lifecycleSlackSeconds": settings["lifecycleSlackSeconds"],
        "slackNote": "S3 expires at the next UTC midnight after the lifecycle days, so the oldest surviving "
                     "object can be up to one day younger than the approved retention",
    }


def _covers(prefix: str, rule: dict) -> bool:
    rule_filter = rule.get("Filter")
    if rule_filter is None:
        rule_prefix = rule.get("Prefix", "")
    elif set(rule_filter) <= {"Prefix"}:
        rule_prefix = rule_filter.get("Prefix", "")
    else:
        return False
    return (prefix + "/").startswith(rule_prefix)


def audit_bucket(config: dict, prefix: str, bucket: str, region: str) -> tuple[dict, dict]:
    """ADR-0058 bucket controls from the raw `aws s3api` responses (a missing response is False)."""
    block = (config.get("publicAccessBlock") or {}).get("PublicAccessBlockConfiguration", {})
    rules = (config.get("encryption") or {}).get("ServerSideEncryptionConfiguration", {}).get("Rules", [])
    lifecycle = [r for r in (config.get("lifecycle") or {}).get("Rules", []) if r.get("Status") == "Enabled"
                 and _covers(prefix, r)]
    expirations = [r["Expiration"].get("Days") for r in lifecycle if "Expiration" in r]
    aborts = [r["AbortIncompleteMultipartUpload"].get("DaysAfterInitiation") for r in lifecycle
              if "AbortIncompleteMultipartUpload" in r]
    try:
        policy = json.loads((config.get("policy") or {}).get("Policy", "null"))
    except ValueError:
        policy = None
    statements = (policy or {}).get("Statement", []) if isinstance(policy, dict) else []
    arns = {f"arn:aws:s3:::{bucket}", f"arn:aws:s3:::{bucket}/*"}

    def tls_only(statement: dict) -> bool:
        resources = statement.get("Resource")
        resources = {resources} if isinstance(resources, str) else set(resources or [])
        actions = statement.get("Action")
        actions = {actions} if isinstance(actions, str) else set(actions or [])
        condition = statement.get("Condition", {}).get("Bool", {})
        return (statement.get("Effect") == "Deny" and statement.get("Principal") in ("*", {"AWS": "*"})
                and bool(actions & {"s3:*", "*"}) and arns <= resources
                and str(condition.get("aws:SecureTransport")).lower() == "false")

    location = (config.get("location") or {}).get("LocationConstraint")
    criteria = {
        "audit.publicAccessBlocked": bool(block) and all(block.get(key) is True for key in (
            "BlockPublicAcls", "IgnorePublicAcls", "BlockPublicPolicy", "RestrictPublicBuckets")),
        "audit.sseS3": bool(rules) and all(
            r.get("ApplyServerSideEncryptionByDefault", {}).get("SSEAlgorithm") == "AES256" for r in rules),
        "audit.tlsOnlyPolicy": any(tls_only(statement) for statement in statements),
        "audit.lifecycleExpiresAfter30Days": bool(expirations) and set(expirations) == {30},
        "audit.incompleteMultipartAbortAfter1Day": bool(aborts) and set(aborts) == {1},
        "audit.sameRegionBucket": (location or "us-east-1") == region and bool(config.get("location") is not None),
    }
    return criteria, {"expirationDays": expirations, "abortDays": aborts, "bucketRegion": location,
                      "policyStatementSids": [s.get("Sid") for s in statements if isinstance(s, dict)]}


# -- sub-run judging (CLU-SCN-159) -------------------------------------------------------------------------------

def judge_stage_summary(exit_code: int | None, summary: dict | None, required: tuple[str, ...]) -> tuple[bool, str | None]:
    """A delegated acceptance run passes only with exit 0 and a summary that is PASS with every criterion True."""
    if exit_code != 0:
        return False, "stage_exit_code_nonzero" if exit_code is not None else "stage_timeout"
    if not isinstance(summary, dict):
        return False, "stage_summary_missing"
    criteria = summary.get("criteria")
    if summary.get("result") != "PASS" or summary.get("failedStep") is not None or not isinstance(criteria, dict):
        return False, "stage_summary_not_pass"
    missing = [name for name in required if criteria.get(name) is not True]
    if missing or summary.get("failedCriteria"):
        return False, "stage_criteria_unmet:" + ",".join(missing)
    return True, None
