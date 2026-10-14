#!/usr/bin/env python3
"""Upload durable request-backed NaruMQ backup files with the pod's IRSA identity."""

from __future__ import annotations

import base64
import json
import os
import re
import stat
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

MAX_ARCHIVE_BYTES = 1 << 30
MAX_REQUEST_BYTES = 512
REQUEST_SUFFIX = ".request.json"


def fail(message: str) -> None:
    raise ValueError(message)


def required(name: str) -> str:
    value = os.environ.get(name, "")
    if not value:
        fail(f"required setting {name} is missing")
    return value


def validate_config() -> tuple[str, str, str, str]:
    bucket = required("BACKUP_S3_BUCKET")
    prefix = required("BACKUP_S3_PREFIX")
    region = required("AWS_REGION")
    run_id = required("BACKUP_RUN_ID")
    if not re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", bucket):
        fail("BACKUP_S3_BUCKET is invalid")
    if (
        not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._/-]*", prefix)
        or ".." in prefix
        or "//" in prefix
        or prefix.endswith("/")
    ):
        fail("BACKUP_S3_PREFIX is invalid")
    if not re.fullmatch(r"[a-z0-9-]+", region):
        fail("AWS_REGION is invalid")
    if not re.fullmatch(r"[a-f0-9-]{36}", run_id):
        fail("BACKUP_RUN_ID must be the pod UID")
    return bucket, prefix, region, run_id


def atomic_write(path: Path, contents: str) -> None:
    temporary = path.with_name(path.name + f".tmp.{os.getpid()}")
    temporary.write_text(contents, encoding="utf-8")
    os.replace(temporary, path)


def read_request(request: Path, root: Path) -> tuple[Path, str, str, str, datetime]:
    request_info = request.lstat()
    if not stat.S_ISREG(request_info.st_mode) or request_info.st_size > MAX_REQUEST_BYTES:
        fail("upload request must be a regular file no larger than 512 bytes")
    if request.parent.resolve() != root.resolve():
        fail("upload request is outside the backup staging directory")

    payload = json.loads(request.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or set(payload) != {"utcTimestamp", "commitIndex", "sha256", "filename"}:
        fail("upload request fields are invalid")
    stamp = payload["utcTimestamp"]
    commit_index = payload["commitIndex"]
    full_sha = payload["sha256"]
    filename = payload["filename"]
    if not isinstance(stamp, str) or not re.fullmatch(r"[0-9]{8}T[0-9]{6}Z", stamp):
        fail("upload request UTC timestamp is invalid")
    if not isinstance(commit_index, str) or not re.fullmatch(r"[0-9]+", commit_index):
        fail("upload request commit index is invalid")
    if not isinstance(full_sha, str) or not re.fullmatch(r"[0-9a-f]{64}", full_sha):
        fail("upload request full-file SHA-256 is invalid")
    expected_name = f"backup-{stamp}-{commit_index}-{full_sha}.nrbak"
    if not isinstance(filename, str) or filename != expected_name:
        fail("upload request staged filename does not match its metadata")
    expected_request = root / f"{expected_name}{REQUEST_SUFFIX}"
    if request != expected_request:
        fail("upload request path does not match its staged filename")
    archive = root / filename
    archive_info = archive.lstat()
    if not stat.S_ISREG(archive_info.st_mode):
        fail("staged archive must be a regular file")
    if archive_info.st_size <= 0 or archive_info.st_size > MAX_ARCHIVE_BYTES:
        fail("staged archive size is outside the supported range")
    if archive.parent.resolve() != root.resolve():
        fail("staged archive is outside the backup staging directory")
    created_at = datetime.strptime(stamp, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
    return archive, stamp, commit_index, full_sha, created_at


def put_archive(path: Path, bucket: str, prefix: str, region: str, stamp: str, commit_index: str, full_sha: str, created_at: datetime) -> tuple[str, str]:
    checksum = base64.b64encode(bytes.fromhex(full_sha)).decode("ascii")
    key = (
        f"{prefix}/{created_at:%Y/%m/%d}/backup-{stamp}-"
        f"{commit_index}-{full_sha}.nrbak"
    )
    command = [
        "aws", "s3api", "put-object",
        "--bucket", bucket,
        "--key", key,
        "--body", str(path),
        "--checksum-algorithm", "SHA256",
        "--checksum-sha256", checksum,
        "--server-side-encryption", "AES256",
        "--region", region,
        "--no-cli-pager",
        "--query", "ChecksumSHA256",
        "--output", "text",
    ]
    try:
        result = subprocess.run(
            command,
            check=True,
            capture_output=True,
            text=True,
            timeout=3300,
        )
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError("S3 PutObject timed out; staged archive and request retained") from exc
    except subprocess.CalledProcessError as exc:
        detail = (exc.stderr or "").strip().replace("\n", " ")[:1000]
        raise RuntimeError(f"S3 PutObject failed; staged archive and request retained: {detail}") from exc
    returned_checksum = result.stdout.strip().strip('"')
    if returned_checksum != checksum:
        raise RuntimeError("S3 PutObject checksum response did not match; staged archive and request retained")
    return key, checksum


def finish_success(request: Path, archive: Path, key: str, checksum: str) -> None:
    receipt = request.with_name(request.name + ".uploaded")
    atomic_write(
        receipt,
        json.dumps(
            {"key": key, "checksumSHA256": checksum},
            separators=(",", ":"),
        ) + "\n",
    )
    archive.unlink(missing_ok=True)
    request.unlink(missing_ok=True)
    receipt.unlink(missing_ok=True)


def finish_prior_success(receipt: Path, root: Path, prefix: str) -> None:
    receipt_info = receipt.lstat()
    if not stat.S_ISREG(receipt_info.st_mode) or receipt_info.st_size > MAX_REQUEST_BYTES:
        fail("upload receipt must be a regular file no larger than 512 bytes")
    if receipt.parent.resolve() != root.resolve():
        fail("upload receipt is outside the backup staging directory")

    request_name = receipt.name.removesuffix(".uploaded")
    if not request_name.endswith(REQUEST_SUFFIX):
        fail("upload receipt path is invalid")
    filename = request_name.removesuffix(REQUEST_SUFFIX)
    match = re.fullmatch(
        r"backup-(?P<stamp>[0-9]{8}T[0-9]{6}Z)-(?P<commit>[0-9]+)-(?P<sha>[0-9a-f]{64})\.nrbak",
        filename,
    )
    if match is None:
        fail("upload receipt staged filename is invalid")

    payload = json.loads(receipt.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or set(payload) != {"key", "checksumSHA256"}:
        fail("upload receipt fields are invalid")
    stamp = match.group("stamp")
    commit_index = match.group("commit")
    full_sha = match.group("sha")
    created_at = datetime.strptime(stamp, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
    expected_key = (
        f"{prefix}/{created_at:%Y/%m/%d}/backup-{stamp}-{commit_index}-{full_sha}.nrbak"
    )
    expected_checksum = base64.b64encode(bytes.fromhex(full_sha)).decode("ascii")
    if payload["key"] != expected_key or payload["checksumSHA256"] != expected_checksum:
        fail("upload receipt key or checksum does not match the staged filename")

    archive = root / filename
    request = root / f"{filename}{REQUEST_SUFFIX}"
    archive.unlink(missing_ok=True)
    request.unlink(missing_ok=True)
    receipt.unlink(missing_ok=True)


def status_path(state: Path, filename: str, result: str) -> Path:
    return state / "status" / f"{filename}.{result}"


def process_request(request: Path, root: Path, state: Path, bucket: str, prefix: str, region: str) -> None:
    filename = request.name.removesuffix(REQUEST_SUFFIX)
    if not re.fullmatch(r"backup-[0-9]{8}T[0-9]{6}Z-[0-9]+-[0-9a-f]{64}\.nrbak", filename):
        fail("upload request filename is invalid")
    archive, stamp, commit_index, full_sha, created_at = read_request(request, root)
    key, checksum = put_archive(archive, bucket, prefix, region, stamp, commit_index, full_sha, created_at)
    finish_success(request, archive, key, checksum)
    atomic_write(status_path(state, filename, "uploaded"), key + "\n")
    print(f"backup-uploader: uploaded key={key} commit={commit_index} sha256={full_sha}")


def main() -> int:
    bucket, prefix, region, run_id = validate_config()
    root = Path(os.environ.get("BACKUP_DIR", "/backups"))
    state = Path("/run/narumq-backup") / run_id
    (state / "status").mkdir(parents=True, exist_ok=True)
    atomic_write(state / "uploader.ready", "ready\n")

    while not (state / "main.done").exists():
        for receipt in sorted(root.glob(f"backup-*{REQUEST_SUFFIX}.uploaded")):
            try:
                finish_prior_success(receipt, root, prefix)
            except Exception as exc:
                filename = receipt.name.removesuffix(REQUEST_SUFFIX + ".uploaded")
                message = str(exc).replace("\n", " ")[:2000]
                atomic_write(status_path(state, filename, "failed"), message + "\n")
                print(f"backup-uploader: cleanup failed for {filename}: {message}", file=sys.stderr)
        for request in sorted(root.glob(f"backup-*{REQUEST_SUFFIX}")):
            filename = request.name.removesuffix(REQUEST_SUFFIX)
            if status_path(state, filename, "uploaded").exists() or status_path(state, filename, "failed").exists():
                continue
            try:
                process_request(request, root, state, bucket, prefix, region)
            except Exception as exc:
                message = str(exc).replace("\n", " ")[:2000]
                atomic_write(status_path(state, filename, "failed"), message + "\n")
                print(f"backup-uploader: upload failed for {filename}: {message}", file=sys.stderr)
        time.sleep(1)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"backup-uploader: startup failed: {exc}", file=sys.stderr)
        raise SystemExit(1)
