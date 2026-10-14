from __future__ import annotations

import base64
import contextlib
import hashlib
import importlib.util
import io
import json
import os
import stat
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
S3_DEPLOY = ROOT / "deploy" / "kubernetes" / "eks-ec2-ha-s3"
sys.dont_write_bytecode = True


def load_script(name: str):
    path = S3_DEPLOY / name
    spec = importlib.util.spec_from_file_location(path.stem.replace("-", "_"), path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load helper {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


uploader = load_script("s3_uploader.py")
restore = load_script("restore-from-s3.py")


class S3TransferTests(unittest.TestCase):
    bucket = "narumq-backup-fixture-20261014"
    prefix = "clusters/narumq-fixture"
    region = "ap-northeast-2"
    run_id = "12345678-1234-1234-1234-123456789abc"
    stamp = "20261014T120000Z"
    commit_index = "42"

    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(prefix="narumq-s3-tests-")
        self.addCleanup(self.temporary.cleanup)
        self.temp = Path(self.temporary.name)
        self.fake_config: dict[str, object] = {}
        self.fake_objects: dict[str, str] = {}
        self.fake_calls: list[dict[str, list[str]]] = []
        self.put_count = 0
        self.environment = patch.dict(
            os.environ,
            {
                "BACKUP_S3_BUCKET": self.bucket,
                "BACKUP_S3_PREFIX": self.prefix,
                "AWS_REGION": self.region,
                "BACKUP_RUN_ID": self.run_id,
            },
        )
        self.environment.start()
        self.addCleanup(self.environment.stop)
        self.aws_patch = patch.object(uploader.subprocess, "run", side_effect=self.fake_aws)
        self.aws_patch.start()
        self.addCleanup(self.aws_patch.stop)

    def fake_aws(self, args: list[str], **kwargs) -> subprocess.CompletedProcess[str]:
        args = list(args)
        call_args = args[1:]
        self.fake_calls.append({"args": call_args})
        operation = call_args[call_args.index("s3api") + 1]

        def option(name: str) -> str:
            return call_args[call_args.index(name) + 1]

        def completed(returncode: int, stdout: str = "", stderr: str = ""):
            result = subprocess.CompletedProcess(args, returncode, stdout, stderr)
            if returncode and kwargs.get("check"):
                raise subprocess.CalledProcessError(
                    returncode, args, output=stdout, stderr=stderr
                )
            return result

        if operation == "put-object":
            self.put_count += 1
            key = option("--key")
            body = Path(option("--body")).read_bytes()
            actual = base64.b64encode(hashlib.sha256(body).digest()).decode("ascii")
            if option("--checksum-sha256") != actual:
                return completed(2, stderr="BadDigest")
            previous = self.fake_objects.get(key)
            if previous is not None and previous != actual:
                return completed(3, stderr="same key has different content")
            self.fake_objects[key] = actual
            if self.fake_config.get("fail_first_put") and self.put_count == 1:
                return completed(4, stderr="simulated response loss after commit")
            return completed(0, str(self.fake_config.get("put_response", actual)) + "\n")

        if operation == "head-object":
            key = option("--key")
            if key != self.fake_config.get("get_key"):
                return completed(5, stderr="unexpected exact key")
            body = base64.b64decode(str(self.fake_config["get_body_base64"]))
            actual = base64.b64encode(hashlib.sha256(body).digest()).decode("ascii")
            return completed(0, json.dumps({
                "ContentLength": self.fake_config.get("head_content_length", len(body)),
                "ChecksumSHA256": self.fake_config.get("head_response", actual),
            }) + "\n")

        if operation == "list-objects-v2":
            response = self.fake_config.get("list_response", {"Contents": []})
            return completed(0, json.dumps(response) + "\n")

        if operation == "get-object":
            key = option("--key")
            if key != self.fake_config.get("get_key"):
                return completed(5, stderr="unexpected exact key")
            body = base64.b64decode(str(self.fake_config["get_body_base64"]))
            Path(call_args[-1]).write_bytes(body)
            actual = base64.b64encode(hashlib.sha256(body).digest()).decode("ascii")
            checksum = str(self.fake_config.get("get_response", actual))
            return completed(0, checksum + "\n")

        return completed(6, stderr=f"unexpected operation {operation}")

    def set_fake_config(self, **values) -> None:
        self.fake_config = values

    def calls(self) -> list[dict[str, list[str]]]:
        return self.fake_calls

    def make_staged_request(self, directory: Path, contents: bytes) -> tuple[Path, Path, str, str]:
        directory.mkdir(parents=True, exist_ok=True)
        full_sha = hashlib.sha256(contents).hexdigest()
        filename = f"backup-{self.stamp}-{self.commit_index}-{full_sha}.nrbak"
        archive = directory / filename
        request = directory / f"{filename}{uploader.REQUEST_SUFFIX}"
        archive.write_bytes(contents)
        request.write_text(
            json.dumps(
                {
                    "utcTimestamp": self.stamp,
                    "commitIndex": self.commit_index,
                    "sha256": full_sha,
                    "filename": filename,
                },
                separators=(",", ":"),
            )
            + "\n",
            encoding="utf-8",
        )
        return archive, request, filename, full_sha

    def expected_key(self, full_sha: str) -> str:
        return (
            f"{self.prefix}/2026/10/14/backup-{self.stamp}-"
            f"{self.commit_index}-{full_sha}.nrbak"
        )

    def test_put_uses_request_metadata_and_removes_only_after_matching_checksum(self) -> None:
        root = self.temp / "backups"
        contents = b"deterministic request-backed archive payload"
        archive, request, filename, full_sha = self.make_staged_request(root, contents)
        state = self.temp / "run"
        (state / "status").mkdir(parents=True)

        uploader.process_request(request, root, state, self.bucket, self.prefix, self.region)

        self.assertFalse(archive.exists())
        self.assertFalse(request.exists())
        self.assertTrue((state / "status" / f"{filename}.uploaded").exists())
        args = self.calls()[0]["args"]
        self.assertEqual(args[args.index("--key") + 1], self.expected_key(full_sha))
        self.assertEqual(
            args[args.index("--checksum-sha256") + 1],
            base64.b64encode(bytes.fromhex(full_sha)).decode("ascii"),
        )
        self.assertEqual(args[args.index("--server-side-encryption") + 1], "AES256")
        self.assertNotIn("If-None-Match", args)

    def test_put_checksum_mismatch_preserves_staged_archive_and_request(self) -> None:
        root = self.temp / "backups"
        archive, request, filename, _ = self.make_staged_request(
            root, b"staging survives a mismatched S3 checksum response"
        )
        state = self.temp / "run"
        (state / "status").mkdir(parents=True)
        self.set_fake_config(
            put_response=base64.b64encode(bytes.fromhex("0" * 64)).decode("ascii")
        )

        with self.assertRaises(RuntimeError):
            uploader.process_request(request, root, state, self.bucket, self.prefix, self.region)

        self.assertTrue(archive.exists())
        self.assertTrue(request.exists())
        self.assertFalse((state / "status" / f"{filename}.uploaded").exists())

    def test_response_loss_retry_reuses_same_key_and_preserves_staging_until_success(self) -> None:
        root = self.temp / "backups"
        contents = b"same bytes survive an ambiguous PutObject response"
        archive, request, _, full_sha = self.make_staged_request(root, contents)
        state = self.temp / "run"
        (state / "status").mkdir(parents=True)
        self.set_fake_config(fail_first_put=True)

        with self.assertRaises(RuntimeError):
            uploader.process_request(request, root, state, self.bucket, self.prefix, self.region)
        self.assertTrue(archive.exists())
        self.assertTrue(request.exists())

        uploader.process_request(request, root, state, self.bucket, self.prefix, self.region)

        self.assertFalse(archive.exists())
        self.assertFalse(request.exists())
        calls = self.calls()
        self.assertEqual(len(calls), 2)
        keys = [call["args"][call["args"].index("--key") + 1] for call in calls]
        checksums = [
            call["args"][call["args"].index("--checksum-sha256") + 1]
            for call in calls
        ]
        self.assertEqual(keys, [self.expected_key(full_sha)] * 2)
        self.assertEqual(checksums[0], checksums[1])
        self.assertEqual(len(self.fake_objects), 1)
        self.assertTrue(all("If-None-Match" not in call["args"] for call in calls))

    def test_prior_success_receipt_finishes_cleanup_after_pod_replacement(self) -> None:
        root = self.temp / "backups"
        archive, request, _, full_sha = self.make_staged_request(
            root, b"committed object with interrupted local cleanup"
        )
        receipt = request.with_name(request.name + ".uploaded")
        receipt.write_text(
            json.dumps(
                {
                    "key": self.expected_key(full_sha),
                    "checksumSHA256": base64.b64encode(bytes.fromhex(full_sha)).decode("ascii"),
                }
            ),
            encoding="utf-8",
        )

        uploader.finish_prior_success(receipt, root, self.prefix)

        self.assertFalse(archive.exists())
        self.assertFalse(request.exists())
        self.assertFalse(receipt.exists())
        self.assertEqual(self.calls(), [])

    def test_corrupt_prior_success_receipt_preserves_archive_and_request(self) -> None:
        root = self.temp / "backups"
        archive, request, _, full_sha = self.make_staged_request(
            root, b"corrupt receipt must not erase a staged archive"
        )
        receipt = request.with_name(request.name + ".uploaded")
        receipt.write_text(
            json.dumps(
                {
                    "key": self.expected_key(full_sha),
                    "checksumSHA256": base64.b64encode(bytes.fromhex("0" * 64)).decode("ascii"),
                }
            ),
            encoding="utf-8",
        )

        with self.assertRaises(ValueError):
            uploader.finish_prior_success(receipt, root, self.prefix)

        self.assertTrue(archive.exists())
        self.assertTrue(request.exists())
        self.assertTrue(receipt.exists())
        self.assertEqual(self.calls(), [])

    def test_invalid_request_is_rejected_without_aws_or_staging_deletion(self) -> None:
        root = self.temp / "backups"
        archive, request, _, _ = self.make_staged_request(root, b"archive bytes")
        request.write_text("x" * (uploader.MAX_REQUEST_BYTES + 1), encoding="utf-8")

        with self.assertRaises(ValueError):
            uploader.read_request(request, root)

        self.assertTrue(archive.exists())
        self.assertTrue(request.exists())
        self.assertEqual(self.calls(), [])

    def test_unsafe_request_filename_is_rejected_without_aws_or_deletion(self) -> None:
        root = self.temp / "backups"
        archive, request, _, _ = self.make_staged_request(root, b"request path must be derived safely")
        payload = json.loads(request.read_text(encoding="utf-8"))
        payload["filename"] = "../outside.nrbak"
        request.write_text(json.dumps(payload), encoding="utf-8")

        with self.assertRaises(ValueError):
            uploader.read_request(request, root)

        self.assertTrue(archive.exists())
        self.assertTrue(request.exists())
        self.assertEqual(self.calls(), [])

    def test_writer_configuration_fails_closed_when_bucket_is_missing(self) -> None:
        with patch.dict(os.environ, {"BACKUP_S3_BUCKET": ""}):
            with self.assertRaises(ValueError):
                uploader.validate_config()

    def test_restore_lists_only_configured_prefix(self) -> None:
        key = self.expected_key("a" * 64)
        self.set_fake_config(
            list_response={
                "Contents": [
                    {"Key": key, "LastModified": "2026-10-14T12:00:00Z", "Size": 123}
                ]
            }
        )
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            restore.list_backups(self.bucket, self.prefix)

        self.assertIn(key, output.getvalue())
        args = self.calls()[0]["args"]
        self.assertEqual(args[args.index("--prefix") + 1], self.prefix + "/")

    def test_restore_listing_rejects_untrusted_key_text(self) -> None:
        unsafe_key = self.prefix + "/not-a-backup" + chr(9) + "terminal-content"
        self.set_fake_config(
            list_response={
                "Contents": [
                    {"Key": unsafe_key, "LastModified": "2026-10-14T12:00:00Z", "Size": 1}
                ]
            }
        )
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            with self.assertRaises(ValueError):
                restore.list_backups(self.bucket, self.prefix)

        self.assertNotIn(unsafe_key, output.getvalue())

    def test_restore_downloads_exact_key_and_verifies_both_checksums(self) -> None:
        contents = b"exact S3 object content for offline restore"
        full_sha = hashlib.sha256(contents).hexdigest()
        key = self.expected_key(full_sha)
        expected_checksum = base64.b64encode(bytes.fromhex(full_sha)).decode("ascii")
        self.set_fake_config(
            get_key=key,
            get_body_base64=base64.b64encode(contents).decode("ascii"),
            get_response=expected_checksum,
        )
        destination = self.temp / "restore"
        destination.mkdir()

        archive = restore.download_backup(self.bucket, self.prefix, key, str(destination))

        self.assertEqual(archive.read_bytes(), contents)
        if os.name == "posix":
            self.assertEqual(stat.S_IMODE(archive.stat().st_mode), 0o600)
        calls = self.calls()
        self.assertEqual(len(calls), 2)
        head_args = calls[0]["args"]
        get_args = calls[1]["args"]
        self.assertIn("head-object", head_args)
        self.assertEqual(head_args[head_args.index("--key") + 1], key)
        self.assertIn("get-object", get_args)
        self.assertEqual(get_args[get_args.index("--key") + 1], key)
        self.assertIn("--checksum-mode", get_args)
        self.assertEqual(len(list(destination.iterdir())), 1)

    def test_restore_rejects_out_of_range_object_size_before_get(self) -> None:
        contents = b"archive size metadata is checked before download"
        full_sha = hashlib.sha256(contents).hexdigest()
        key = self.expected_key(full_sha)
        encoded_contents = base64.b64encode(contents).decode("ascii")

        for advertised_size in (restore.MIN_ARCHIVE_BYTES - 1, restore.MAX_ARCHIVE_BYTES + 1):
            with self.subTest(advertised_size=advertised_size):
                self.set_fake_config(
                    get_key=key,
                    get_body_base64=encoded_contents,
                    head_content_length=advertised_size,
                )
                destination = self.temp / f"restore-{advertised_size}"
                destination.mkdir()
                first_call = len(self.calls())

                with self.assertRaises(ValueError):
                    restore.download_backup(self.bucket, self.prefix, key, str(destination))

                calls = self.calls()[first_call:]
                self.assertEqual(len(calls), 1)
                self.assertIn("head-object", calls[0]["args"])
                self.assertEqual(list(destination.iterdir()), [])

    def test_restore_rejects_head_checksum_mismatch_before_get(self) -> None:
        contents = b"head checksum must match the key before download"
        full_sha = hashlib.sha256(contents).hexdigest()
        key = self.expected_key(full_sha)
        self.set_fake_config(
            get_key=key,
            get_body_base64=base64.b64encode(contents).decode("ascii"),
            head_response=base64.b64encode(bytes.fromhex("0" * 64)).decode("ascii"),
        )
        destination = self.temp / "restore"
        destination.mkdir()

        with self.assertRaises(ValueError):
            restore.download_backup(self.bucket, self.prefix, key, str(destination))

        self.assertEqual(len(self.calls()), 1)
        self.assertIn("head-object", self.calls()[0]["args"])
        self.assertEqual(list(destination.iterdir()), [])

    def test_restore_rejects_bad_key_before_calling_aws(self) -> None:
        destination = self.temp / "restore"
        destination.mkdir()
        invalid_key = f"other-prefix/{self.expected_key('a' * 64)}"

        with self.assertRaises(ValueError):
            restore.download_backup(self.bucket, self.prefix, invalid_key, str(destination))

        self.assertEqual(self.calls(), [])
        self.assertEqual(list(destination.iterdir()), [])

    def test_restore_s3_checksum_mismatch_removes_partial_file(self) -> None:
        contents = b"download must be discarded on checksum mismatch"
        full_sha = hashlib.sha256(contents).hexdigest()
        key = self.expected_key(full_sha)
        wrong_checksum = base64.b64encode(bytes.fromhex("0" * 64)).decode("ascii")
        self.set_fake_config(
            get_key=key,
            get_body_base64=base64.b64encode(contents).decode("ascii"),
            get_response=wrong_checksum,
        )
        destination = self.temp / "restore"
        destination.mkdir()

        with self.assertRaises(ValueError):
            restore.download_backup(self.bucket, self.prefix, key, str(destination))

        self.assertEqual(list(destination.iterdir()), [])
        self.assertEqual(len(self.calls()), 2)

    def test_restore_full_file_hash_mismatch_removes_partial_file(self) -> None:
        body = b"download bytes do not match content-addressed key"
        key_sha = hashlib.sha256(b"different key bytes").hexdigest()
        key = self.expected_key(key_sha)
        self.set_fake_config(
            get_key=key,
            get_body_base64=base64.b64encode(body).decode("ascii"),
            head_response=base64.b64encode(bytes.fromhex(key_sha)).decode("ascii"),
            get_response=base64.b64encode(bytes.fromhex(key_sha)).decode("ascii"),
        )
        destination = self.temp / "restore"
        destination.mkdir()

        with self.assertRaises(ValueError):
            restore.download_backup(self.bucket, self.prefix, key, str(destination))
        self.assertEqual(list(destination.iterdir()), [])
        self.assertEqual(len(self.calls()), 2)

    def test_restore_rejects_missing_configuration(self) -> None:
        with patch.dict(os.environ, {"BACKUP_S3_PREFIX": ""}):
            with self.assertRaises(ValueError):
                restore.configuration()


if __name__ == "__main__":
    unittest.main()
