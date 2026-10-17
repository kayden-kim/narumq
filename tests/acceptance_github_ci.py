"""Opt-in GitHub acceptance for PRF-SCN-104 .. PRF-SCN-109 (remote CI and `main` protection).

Not part of the CI unit list and never run by ``go run ./tools/verify``. It talks to the real GitHub API through the
authenticated ``gh`` CLI, so the account behind ``gh auth status`` must be a repository administrator. Nothing here
replays a recorded result: every check reads live GitHub state or a run that this test started.

Read only (needs NARUMQ_ACCEPT_GITHUB=1):
    PRF-SCN-105  Branch Protection API of ``main``.

Starts a workflow run (needs NARUMQ_ACCEPT_GITHUB=1 and NARUMQ_ACCEPT_GITHUB_DISPATCH_REF=<pushed branch>):
    PRF-SCN-104  workflow_dispatch of verify.yml on that branch; the branch head must equal the local HEAD.

Creates and removes a branch and a pull request, and attempts a merge that must be refused
(needs NARUMQ_ACCEPT_GITHUB_PROBES=1 and NARUMQ_ACCEPT_GITHUB_REPO=<owner>/<repo>):
    PRF-SCN-106 .. PRF-SCN-109  one probe pull request each.

Guards for the destructive probes:
  * NARUMQ_ACCEPT_GITHUB_REPO must equal the ``origin`` remote of this checkout.
  * ``main`` protection must already satisfy PRF-SCN-105 and the token must be an administrator, otherwise nothing is
    created.
  * Only branches named ``prf-probe/...`` created by this run are touched. The merge is attempted only after the
    ``verify`` check of the probe head failed and ``mergeable_state`` is ``blocked``, and it pins the probe head SHA.
  * ``main`` must have the same SHA before and after, and the pull request must not be merged. A merge that succeeds
    fails the test loudly; it is never undone automatically.
  * Cleanup closes the probe pull requests and deletes the probe branches even when an assertion fails.

    python tests/run_scenario_unittest.py tests/acceptance_github_ci.py
"""
from __future__ import annotations

import base64
import datetime
import io
import json
import os
import re
import subprocess
import sys
import time
import unittest
import uuid
import zipfile
from pathlib import Path

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parents[1]
ENABLED = os.environ.get("NARUMQ_ACCEPT_GITHUB") == "1"
PROBES_ENABLED = ENABLED and os.environ.get("NARUMQ_ACCEPT_GITHUB_PROBES") == "1"
PROBE_PREFIX = "prf-probe/"
CHECK_NAME = "verify"
RUN_TIMEOUT = 60 * 60
MATRIX_PATH = "tests/conformance/mqtt-compliance-v1.json"


class GitHubError(AssertionError):
    pass


def gh(args, body=None, timeout=300):
    return subprocess.run(["gh", *args], input=body, capture_output=True, timeout=timeout)


def api(method, path, body=None, expect=(200, 201, 202, 204)):
    """One GitHub REST call through gh. Returns (status, parsed JSON or None). Unexpected statuses raise."""
    command = ["api", "--include", "-H", "Accept: application/vnd.github+json", "-X", method, path]
    payload = None
    if body is not None:
        command += ["--input", "-"]
        payload = json.dumps(body).encode("utf-8")
    result = gh(command, payload)
    text = result.stdout.decode("utf-8", "replace")
    head, _, rest = text.replace("\r\n", "\n").partition("\n\n")
    match = re.match(r"HTTP/\S+\s+(\d{3})", head)
    if not match:
        raise GitHubError(f"gh api {method} {path} returned no HTTP status: {result.stderr.decode('utf-8', 'replace')[:300]}")
    status = int(match.group(1))
    try:
        parsed = json.loads(rest) if rest.strip() else None
    except ValueError:
        parsed = None
    if status not in expect:
        raise GitHubError(f"{method} {path} -> HTTP {status}: {(parsed or {}).get('message', rest[:200])}")
    return status, parsed


def get(path):
    return api("GET", path)[1]


def parse_time(value):
    return datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))


def origin_repository():
    url = subprocess.run(["git", "remote", "get-url", "origin"], cwd=ROOT, capture_output=True, text=True, check=True).stdout.strip()
    match = re.search(r"[:/]([^/:]+/[^/]+?)(?:\.git)?$", url)
    if not match:
        raise GitHubError(f"cannot read owner/repo from origin remote {url}")
    return match.group(1)


def local_head():
    return subprocess.run(["git", "rev-parse", "HEAD"], cwd=ROOT, capture_output=True, text=True, check=True).stdout.strip()


def wait_for(probe, timeout, interval, what):
    deadline = time.monotonic() + timeout
    while True:
        value = probe()
        if value:
            return value
        if time.monotonic() >= deadline:
            raise GitHubError(f"timeout waiting for {what}")
        time.sleep(interval)


def job_log(repo, job_id):
    result = gh(["api", f"repos/{repo}/actions/jobs/{job_id}/logs"])
    if result.returncode:
        raise GitHubError(f"job {job_id} log is not readable: {result.stderr.decode('utf-8', 'replace')[:200]}")
    return result.stdout.decode("utf-8", "replace")


def verify_job(repo, run_id):
    jobs = get(f"repos/{repo}/actions/runs/{run_id}/jobs")["jobs"]
    matching = [job for job in jobs if job["name"] == CHECK_NAME]
    if len(matching) != 1:
        raise GitHubError(f"run {run_id} has {len(matching)} jobs named {CHECK_NAME}")
    return matching[0]


class GitHubSupport:
    """Shared by the acceptance classes below; not a test case itself."""

    @classmethod
    def setUpClass(cls):
        cls.repo = os.environ.get("NARUMQ_ACCEPT_GITHUB_REPO") or origin_repository()
        if cls.repo != origin_repository():
            raise GitHubError(f"NARUMQ_ACCEPT_GITHUB_REPO {cls.repo} is not the origin remote {origin_repository()}")

    def assert_main_protection(self):
        protection = get(f"repos/{self.repo}/branches/main/protection")
        checks = protection.get("required_status_checks") or {}
        contexts = sorted(set(checks.get("contexts") or []) | {item["context"] for item in checks.get("checks") or []})
        self.assertEqual(contexts, [CHECK_NAME])
        self.assertIs(checks.get("strict"), True)
        self.assertNotIn("required_pull_request_reviews", protection)
        self.assertIs((protection.get("enforce_admins") or {}).get("enabled"), True)
        self.assertIs((protection.get("allow_force_pushes") or {}).get("enabled"), False)
        self.assertIs((protection.get("allow_deletions") or {}).get("enabled"), False)
        self.assertFalse(protection.get("restrictions"), "main has a push restriction")
        return protection


@unittest.skipUnless(ENABLED, "set NARUMQ_ACCEPT_GITHUB=1 to run the GitHub CI acceptance")
class GitHubCiAcceptance(GitHubSupport, unittest.TestCase):
    # Scenarios: performance-and-verification.md:PRF-SCN-105
    def test_main_branch_protection_matches_the_required_settings(self):
        self.assert_main_protection()

    # Scenarios: performance-and-verification.md:PRF-SCN-104
    @unittest.skipUnless(os.environ.get("NARUMQ_ACCEPT_GITHUB_DISPATCH_REF"), "set NARUMQ_ACCEPT_GITHUB_DISPATCH_REF to a pushed branch")
    def test_verify_workflow_dispatch_runs_ci_gate_and_keeps_artifacts_for_30_days(self):
        repo, ref = self.repo, os.environ["NARUMQ_ACCEPT_GITHUB_DISPATCH_REF"]
        sha = get(f"repos/{repo}/git/ref/heads/{ref}")["object"]["sha"]
        self.assertEqual(sha, local_head(), "the dispatched branch is not the code in this checkout")
        started = datetime.datetime.now(datetime.timezone.utc)
        api("POST", f"repos/{repo}/actions/workflows/verify.yml/dispatches", {"ref": ref})

        def new_run():
            runs = get(f"repos/{repo}/actions/workflows/verify.yml/runs?event=workflow_dispatch&head_sha={sha}&per_page=10")
            fresh = [run for run in runs["workflow_runs"] if parse_time(run["created_at"]) >= started - datetime.timedelta(seconds=10)]
            return max(fresh, key=lambda run: run["created_at"]) if fresh else None

        run = wait_for(new_run, 300, 5, "the dispatched workflow run")
        run = wait_for(lambda: (lambda current: current if current["status"] == "completed" else None)(
            get(f"repos/{repo}/actions/runs/{run['id']}")), RUN_TIMEOUT, 30, "the workflow run to finish")
        self.assertEqual((run["event"], run["conclusion"], run["head_sha"]), ("workflow_dispatch", "success", sha))

        job = verify_job(repo, run["id"])
        self.assertEqual(job["conclusion"], "success")
        self.assertIn("ubuntu-24.04", job["labels"])
        steps = {step["name"]: step["conclusion"] for step in job["steps"]}
        self.assertEqual(steps.get("Run CI gate"), "success")
        log = job_log(repo, job["id"])
        self.assertIn("ci: PASS", log)
        self.assertIn("coverage: PASS", log)
        self.assertIn("diff base: ", log)
        self.assertNotIn("DATA RACE", log)
        self.assertNotIn("--- FAIL", log)
        self.assertIn("fuzz: ", log, "the fuzz stage left no output in the job log")
        checks = get(f"repos/{repo}/commits/{sha}/check-runs?check_name={CHECK_NAME}")["check_runs"]
        self.assertTrue(any(item["conclusion"] == "success" and item["id"] for item in checks))

        artifacts = get(f"repos/{repo}/actions/runs/{run['id']}/artifacts")["artifacts"]
        named = [item for item in artifacts if item["name"] == f"verification-{run['id']}-{run['run_attempt']}"]
        self.assertEqual(len(named), 1)
        artifact = named[0]
        self.assertFalse(artifact["expired"])
        days = (parse_time(artifact["expires_at"]) - parse_time(artifact["created_at"])).total_seconds() / 86400
        self.assertAlmostEqual(days, 30, delta=0.1)
        download = gh(["api", f"repos/{repo}/actions/artifacts/{artifact['id']}/zip"])
        self.assertEqual(download.returncode, 0, download.stderr[:200])
        members = {name for name in zipfile.ZipFile(io.BytesIO(download.stdout)).namelist()}
        for expected in ("coverage.out", "coverage-summary.json", "branch/manifest.json"):
            self.assertIn(expected, members)


def replace_statement_list(text):
    """Append MQTT-4.9.0-1 with an empty tests array to the matrix without reformatting the file."""
    statement = '{"id": "MQTT-4.9.0-1", "requirementIds": ["TEST-10"], "tests": []}'
    updated, count = re.subn(r"(\s*)\]\s*\}\s*$", lambda match: ",\n    " + statement + match.group(1) + "]\n}\n", text, count=1)
    if count != 1:
        raise GitHubError("compliance matrix does not end with the statements array")
    parsed = json.loads(updated)["statements"]
    if parsed[-1]["id"] != "MQTT-4.9.0-1" or parsed[-1]["tests"] != [] or len(parsed) != 62:
        raise GitHubError("probe statement was not appended as statement[61]")
    return updated


def add_unmapped_p0_row(text):
    lines = text.splitlines(keepends=True)
    rows = [index for index, line in enumerate(lines) if re.match(r"^\| API-\d+ \|", line)]
    if not rows:
        raise GitHubError("PRD has no API requirement rows")
    ending = "\r\n" if lines[rows[-1]].endswith("\r\n") else "\n"
    row = "| API-99 | P0 | 분기 보호 probe용 미매핑 요구사항이다. | probe 전용이며 시험도 예외도 없다. | 미작성 |" + ending
    lines.insert(rows[-1] + 1, row)
    return "".join(lines)


PROBES = {
    "test-failure": {
        "title": "probe: intentional failing test",
        "files": {
            "tools/verify/branch_protection_probe_test.go": (
                "package main\n\nimport \"testing\"\n\n"
                "func TestBranchProtectionProbe(t *testing.T) {\n"
                "\tt.Fatal(\"TEST-10 probe: intentional test failure\")\n}\n")},
        "expect": ["--- FAIL: TestBranchProtectionProbe", "TEST-10 probe: intentional test failure"],
    },
    "coverage-drop": {
        "title": "probe: function without a test",
        "files": {
            "internal/domain/mqtt/branch_protection_probe.go": (
                "package mqtt\n\n// BranchProtectionProbe exists only for the branch protection acceptance probe.\n"
                "func BranchProtectionProbe(value int) int {\n\tif value > 0 {\n\t\treturn value\n\t}\n\treturn -value\n}\n")},
        "expect": ["coverage gate failed:", "internal/domain/mqtt function ratchet", "diff line coverage 0.00% is below",
                   "diff function coverage 0.00% is below"],
    },
    "unmapped-p0": {
        "title": "probe: P0 requirement without a test or exception",
        "edits": {"docs/requirements/PRD.md": add_unmapped_p0_row},
        "expect": ["traceability: P0 requirement API-99 has no Traceability test and no exception in "
                   "tests/conformance/p0-requirement-exceptions-v1.json"],
    },
    "unmapped-mqtt-statement": {
        "title": "probe: MQTT statement with an empty test list",
        "edits": {MATRIX_PATH: replace_statement_list},
        "expect": ["traceability: invalid compliance matrix tests/conformance/mqtt-compliance-v1.json:",
                   "statement[61].tests must not be empty"],
    },
}


@unittest.skipUnless(PROBES_ENABLED, "set NARUMQ_ACCEPT_GITHUB=1, NARUMQ_ACCEPT_GITHUB_PROBES=1 and NARUMQ_ACCEPT_GITHUB_REPO to run the probe pull requests")
class BranchProtectionProbeAcceptance(GitHubSupport, unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not os.environ.get("NARUMQ_ACCEPT_GITHUB_REPO"):
            raise GitHubError("NARUMQ_ACCEPT_GITHUB_REPO must name the repository explicitly for probe pull requests")
        super().setUpClass()
        permissions = get(f"repos/{cls.repo}")["permissions"]
        if not permissions.get("admin"):
            raise GitHubError("the gh account is not an administrator of the repository")

    def close_pull_request(self, number, branch):
        pull = get(f"repos/{self.repo}/pulls/{number}")
        if pull["head"]["ref"] == branch and pull["head"]["repo"]["full_name"] == self.repo and pull["base"]["ref"] == "main":
            if pull["state"] == "open":
                api("PATCH", f"repos/{self.repo}/pulls/{number}", {"state": "closed"})

    def delete_branch(self, branch):
        if branch.startswith(PROBE_PREFIX):
            api("DELETE", f"repos/{self.repo}/git/refs/heads/{branch}", expect=(204, 404, 422))

    def put_file(self, branch, path, content=None, edit=None):
        existing_sha = None
        status, current = api("GET", f"repos/{self.repo}/contents/{path}?ref={branch}", expect=(200, 404))
        if status == 200:
            existing_sha = current["sha"]
            if edit is not None:
                content = edit(base64.b64decode(current["content"]).decode("utf-8"))
        if content is None:
            raise GitHubError(f"{path} does not exist and has no content to create")
        body = {"message": f"probe: {path}", "content": base64.b64encode(content.encode("utf-8")).decode("ascii"), "branch": branch}
        if existing_sha:
            body["sha"] = existing_sha
        api("PUT", f"repos/{self.repo}/contents/{path}", body)

    def run_probe(self, kind):
        spec, repo = PROBES[kind], self.repo
        self.assert_main_protection()
        main_before = get(f"repos/{repo}/git/ref/heads/main")["object"]["sha"]
        branch = f"{PROBE_PREFIX}{kind}-{int(time.time())}-{uuid.uuid4().hex[:6]}"
        api("POST", f"repos/{repo}/git/refs", {"ref": f"refs/heads/{branch}", "sha": main_before})
        self.addCleanup(self.delete_branch, branch)
        for path, content in spec.get("files", {}).items():
            self.put_file(branch, path, content=content)
        for path, edit in spec.get("edits", {}).items():
            self.put_file(branch, path, edit=edit)
        _, pull = api("POST", f"repos/{repo}/pulls", {
            "title": spec["title"], "head": branch, "base": "main", "body": "Branch protection acceptance probe. Never merge."})
        number = pull["number"]
        self.addCleanup(self.close_pull_request, number, branch)
        head_sha = get(f"repos/{repo}/pulls/{number}")["head"]["sha"]

        def finished_check():
            items = get(f"repos/{repo}/commits/{head_sha}/check-runs?check_name={CHECK_NAME}")["check_runs"]
            return items[0] if items and items[0]["status"] == "completed" else None

        check = wait_for(finished_check, RUN_TIMEOUT, 30, f"the {CHECK_NAME} check of the probe")
        self.assertEqual(check["conclusion"], "failure")
        run_id = re.search(r"/runs/(\d+)", check["details_url"]).group(1)
        log = job_log(repo, verify_job(repo, run_id)["id"])
        for needle in spec["expect"]:
            self.assertIn(needle, log)

        def blocked():
            state = get(f"repos/{repo}/pulls/{number}")["mergeable_state"]
            return state if state != "unknown" else None

        self.assertEqual(wait_for(blocked, 300, 5, "mergeable_state"), "blocked")
        status, refusal = api("PUT", f"repos/{repo}/pulls/{number}/merge", {"sha": head_sha, "merge_method": "squash"}, expect=(200, 405))
        self.assertEqual(status, 405, "the administrator merge of a failing probe was ACCEPTED; main is now modified")
        self.assertIn('Required status check "verify" is failing.', refusal["message"])
        self.assertFalse(get(f"repos/{repo}/pulls/{number}")["merged"])
        self.assertEqual(get(f"repos/{repo}/git/ref/heads/main")["object"]["sha"], main_before)

    # Scenarios: performance-and-verification.md:PRF-SCN-106
    def test_failing_test_probe_blocks_the_merge(self):
        self.run_probe("test-failure")

    # Scenarios: performance-and-verification.md:PRF-SCN-107
    def test_untested_function_probe_blocks_the_merge(self):
        self.run_probe("coverage-drop")

    # Scenarios: performance-and-verification.md:PRF-SCN-108
    def test_unmapped_p0_requirement_probe_blocks_the_merge(self):
        self.run_probe("unmapped-p0")

    # Scenarios: performance-and-verification.md:PRF-SCN-109
    def test_unmapped_mqtt_statement_probe_blocks_the_merge(self):
        self.run_probe("unmapped-mqtt-statement")


if __name__ == "__main__":
    unittest.main()
