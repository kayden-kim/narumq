#!/usr/bin/env python3
"""List cluster backups or download one exact key with SHA-256 verification."""

from __future__ import annotations

import base64
import hashlib
import json
import os
import re
import subprocess
import sys
import tempfile
from datetime import datetime
from pathlib import Path

MIN_ARCHIVE_BYTES = 40
MAX_ARCHIVE_BYTES = 1 << 30


def fail(message: str) -> None:
    raise ValueError(message)


def required(name: str) -> str:
    value = os.environ.get(name, "")
    if not value:
        fail(f"required setting {name} is missing")
    return value


def configuration() -> tuple[str, str, str]:
    bucket = required("BACKUP_S3_BUCKET")
    prefix = required("BACKUP_S3_PREFIX")
    region = required("AWS_REGION")
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
    return bucket, prefix, region


def aws(args: list[str], timeout: int = 3300) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            ["aws", "--region", required("AWS_REGION"), "--no-cli-pager", *args],
            check=True,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError("AWS CLI operation timed out") from exc
    except subprocess.CalledProcessError as exc:
        detail = (exc.stderr or "").strip().replace("\n", " ")[:1000]
        raise RuntimeError(f"AWS CLI operation failed: {detail}") from exc


def list_backups(bucket: str, prefix: str) -> None:
    response = aws([
        "s3api", "list-objects-v2",
        "--bucket", bucket,
        "--prefix", prefix + "/",
        "--output", "json",
    ])
    payload = json.loads(response.stdout)
    contents = payload.get("Contents", [])
    if not contents:
        print(f"No backup objects found under {prefix}/")
        return
    print("LastModified\tSizeBytes\tKey")
    for item in sorted(contents, key=lambda value: value.get("LastModified", ""), reverse=True):
        key = item.get("Key", "")
        if not key.startswith(prefix + "/"):
            fail("S3 list response included a key outside the configured cluster prefix")
        key_metadata(key, prefix)
        print(f"{item.get('LastModified', '')}\t{item.get('Size', '')}\t{key}")


def key_metadata(key: str, prefix: str) -> tuple[str, str, str]:
    pattern = (
        re.escape(prefix)
        + r"/(?P<year>[0-9]{4})/(?P<month>[0-9]{2})/(?P<day>[0-9]{2})/"
        + r"backup-(?P<stamp>[0-9]{8}T[0-9]{6}Z)-(?P<commit>[0-9]+)-(?P<sha>[0-9a-f]{64})\.nrbak"
    )
    match = re.fullmatch(pattern, key)
    if match is None:
        fail("BACKUP_S3_KEY must be one exact content-addressed key under BACKUP_S3_PREFIX")
    stamp = match.group("stamp")
    try:
        created = datetime.strptime(stamp, "%Y%m%dT%H%M%SZ")
    except ValueError as exc:
        raise ValueError("backup key timestamp is invalid") from exc
    if (created.strftime("%Y"), created.strftime("%m"), created.strftime("%d")) != (
        match.group("year"), match.group("month"), match.group("day")
    ):
        fail("backup key date path does not match its UTC timestamp")
    return stamp, match.group("commit"), match.group("sha")


def head_archive(bucket: str, key: str, expected_sha: str) -> int:
    expected_checksum = base64.b64encode(bytes.fromhex(expected_sha)).decode("ascii")
    response = aws([
        "s3api", "head-object",
        "--bucket", bucket,
        "--key", key,
        "--checksum-mode", "ENABLED",
        "--query", "{ContentLength: ContentLength, ChecksumSHA256: ChecksumSHA256}",
        "--output", "json",
    ])
    payload = json.loads(response.stdout)
    if not isinstance(payload, dict) or set(payload) != {"ContentLength", "ChecksumSHA256"}:
        fail("S3 HeadObject response fields are invalid")
    content_length = payload["ContentLength"]
    if type(content_length) is not int:
        fail("S3 HeadObject content length is invalid")
    if content_length < MIN_ARCHIVE_BYTES or content_length > MAX_ARCHIVE_BYTES:
        fail("S3 archive size is outside the supported NMQBKUP3 range")
    if not isinstance(payload["ChecksumSHA256"], str) or payload["ChecksumSHA256"] != expected_checksum:
        fail("S3 HeadObject checksum does not match the SHA-256 in the exact key")
    return content_length


def download_backup(bucket: str, prefix: str, key: str, destination_dir: str) -> Path:
    stamp, commit_index, expected_sha = key_metadata(key, prefix)
    directory = Path(destination_dir).resolve()
    if not directory.is_dir():
        fail("destination directory must already exist")
    destination = directory / f"backup-{stamp}-{commit_index}-{expected_sha}.nrbak"
    if destination.exists():
        fail(f"destination already exists: {destination}")

    content_length = head_archive(bucket, key, expected_sha)

    descriptor, temporary_name = tempfile.mkstemp(prefix=".narumq-restore-", dir=directory)
    os.close(descriptor)
    temporary = Path(temporary_name)
    try:
        response = aws([
            "s3api", "get-object",
            "--bucket", bucket,
            "--key", key,
            "--checksum-mode", "ENABLED",
            "--query", "ChecksumSHA256",
            "--output", "text",
            str(temporary),
        ])
        if temporary.stat().st_size != content_length:
            fail("downloaded archive size differs from S3 HeadObject")

        expected_checksum = base64.b64encode(bytes.fromhex(expected_sha)).decode("ascii")
        returned_checksum = response.stdout.strip().strip('"')
        if returned_checksum != expected_checksum:
            fail("S3 GetObject checksum response does not match the SHA-256 in the exact key")
        actual_hash = hashlib.sha256()
        with temporary.open("rb") as archive:
            for chunk in iter(lambda: archive.read(1024 * 1024), b""):
                actual_hash.update(chunk)
        if actual_hash.hexdigest() != expected_sha:
            fail("downloaded archive SHA-256 does not match the exact key")
        os.chmod(temporary, 0o600)
        os.replace(temporary, destination)
        print(destination)
        return destination
    except Exception:
        temporary.unlink(missing_ok=True)
        raise


def main(arguments: list[str]) -> int:
    bucket, prefix, _ = configuration()
    if len(arguments) == 1 and arguments[0] == "list":
        list_backups(bucket, prefix)
        return 0
    if len(arguments) == 3 and arguments[0] == "download":
        key = arguments[1]
        destination_dir = arguments[2]
        download_backup(bucket, prefix, key, destination_dir)
        return 0
    fail("usage: restore-from-s3.py list | download <exact-s3-key> <existing-directory>")


if __name__ == "__main__":
    try:
        raise SystemExit(main(sys.argv[1:]))
    except Exception as exc:
        print(f"backup-restore: {exc}", file=sys.stderr)
        raise SystemExit(1)
