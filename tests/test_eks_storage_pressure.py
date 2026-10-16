from __future__ import annotations

import copy
import datetime
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import types
import unittest
import unittest.mock

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parents[1]
PRESSURE_DIR = ROOT / "tests/chaos/eks-storage-pressure"
RECOVERY_DIR = ROOT / "tests/chaos/eks-worker-recovery"
for directory in (str(PRESSURE_DIR), str(RECOVERY_DIR)):
    if directory not in sys.path:
        sys.path.insert(0, directory)

import pressure_ballast as ballast_module  # noqa: E402
import pressure_logic as logic  # noqa: E402

spec = importlib.util.spec_from_file_location("eks_storage_pressure_main", PRESSURE_DIR / "main.py")
pressure_main = importlib.util.module_from_spec(spec)
spec.loader.exec_module(pressure_main)

MIB, GIB = logic.MIB, logic.GIB
IMAGE = "809446814769.dkr.ecr.ap-northeast-2.amazonaws.com/narumq@sha256:" + "a" * 64
RUN_ID = "eks-storage-pressure-test"


def plan(**overrides) -> dict:
    value = {"runId": RUN_ID, "clusterName": "narumq-ha-acceptance-test", "targetPod": "narumq-1",
             "targetPodUID": "uid-narumq-1", "secondTargetPod": "narumq-2", "secondTargetPodUID": "uid-narumq-2",
             "observerPod": "narumq-0", "observerPodUID": "uid-narumq-0", "brokerImage": IMAGE}
    value.update(overrides)
    return value


def df_text(avail_bytes: int, total: int = 20 * GIB) -> str:
    return ("Filesystem           1K-blocks      Used Available Use% Mounted on\n"
            f"/dev/nvme1n1      {total // 1024:>12} {(total - avail_bytes) // 1024:>9} {avail_bytes // 1024:>9}  5% /volume\n")


class PlanTests(unittest.TestCase):
    def test_minimal_plan_gets_defaults_and_ordinal_node_ids(self):
        normalized = logic.validate_plan(plan(), ["D1", "D2", "D3"], RUN_ID)
        self.assertEqual(normalized["durableScenario"], "storage")
        self.assertEqual((normalized["targetNodeId"], normalized["secondTargetNodeId"], normalized["observerNodeId"]),
                         ("2", "3", "1"))
        self.assertEqual(logic.durable_scenario(normalized, "D2"), "storage-d2")

    def test_plan_rejections(self):
        cases = [("unknown key", plan(extra=1), ["D1"]),
                 ("run id", plan(runId="other"), ["D1"]),
                 ("cluster", plan(clusterName="production"), ["D1"]),
                 ("pod name", plan(targetPod="web-1"), ["D1"]),
                 ("image", plan(brokerImage="narumq:latest"), ["D1"]),
                 ("node id", plan(targetNodeId="02"), ["D1"]),
                 ("prefix", plan(durableScenario="Bad Prefix"), ["D1"]),
                 ("recovery seconds", plan(selfRecoverySeconds=0), ["D1"])]
        for name, value, scenarios in cases:
            with self.subTest(name), self.assertRaises(ValueError):
                logic.validate_plan(value, scenarios, RUN_ID)
        incomplete = plan()
        del incomplete["secondTargetPod"]
        with self.assertRaises(ValueError):
            logic.validate_plan(incomplete, ["D3"], RUN_ID)
        logic.validate_plan(incomplete, ["D1", "D2"], RUN_ID)

    def test_durable_and_load_plan_keys(self):
        normalized = logic.validate_plan(plan(), ["D2"], RUN_ID)
        self.assertEqual((normalized["durableIntervalSeconds"], normalized["durableMaxMessages"]), (3.0, 60))
        self.assertEqual((normalized["loadPayloadBytes"], normalized["loadTopics"]), (131072, 2))
        custom = logic.validate_plan(plan(durableIntervalSeconds=0.5, durableMaxMessages=10, loadPayloadBytes=2048),
                                     ["D2"], RUN_ID)
        self.assertEqual((custom["durableIntervalSeconds"], custom["durableMaxMessages"]), (0.5, 10))
        for bad in ({"durableMaxMessages": 61}, {"durableMaxMessages": 0}, {"durableMaxMessages": 5.5},
                    {"durableIntervalSeconds": 0.1}, {"loadPayloadBytes": 600 * 1024}, {"loadTopics": 0},
                    {"loadPayloadBytes": 2048.5}, {"loadPayloadBytes": 262145}, {"loadTopics": 3}):
            with self.subTest(bad), self.assertRaises(ValueError):
                logic.validate_plan(plan(**bad), ["D2"], RUN_ID)
        with self.assertRaisesRegex(ValueError, "at most 262144 bytes"):
            logic.validate_plan(plan(loadPayloadBytes=131072, loadTopics=3), ["D2"], RUN_ID)
        edge = logic.validate_plan(plan(loadPayloadBytes=65536, loadTopics=4), ["D2"], RUN_ID)
        self.assertEqual(edge["loadPayloadBytes"] * edge["loadTopics"], 262144)

    def test_scenario_selection(self):
        self.assertEqual(logic.parse_scenarios(None), ["D1", "D2", "D3"])
        self.assertEqual(logic.parse_scenarios("d3,D1"), ["D1", "D3"])
        for bad in ("D4", "D1,D1", ","):
            with self.subTest(bad), self.assertRaises(ValueError):
                logic.parse_scenarios(bad)

    def test_roles_require_follower_targets_and_unfilled_observer(self):
        good = logic.validate_plan(plan(), ["D3"], RUN_ID)
        logic.check_roles(good, "1", ["D1", "D2", "D3"])
        with self.assertRaises(ValueError):  # target is the leader
            logic.check_roles(good, "2", ["D1"])
        with self.assertRaises(ValueError):  # second target is the leader, only checked for D3
            logic.check_roles(good, "3", ["D3"])
        logic.check_roles(good, "3", ["D1"])
        with self.assertRaises(ValueError):  # D3 observer must be the leader
            logic.check_roles(good, "9", ["D3"])
        same_pod = logic.validate_plan(plan(observerPod="narumq-1", observerPodUID="x"), ["D1"], RUN_ID)
        with self.assertRaises(ValueError):
            logic.check_roles(same_pod, "3", ["D1"])
        same_node = logic.validate_plan(plan(observerNodeId="2"), ["D1"], RUN_ID)
        with self.assertRaises(ValueError):
            logic.check_roles(same_node, "3", ["D1"])
        twins = logic.validate_plan(plan(secondTargetPod="narumq-1"), ["D3"], RUN_ID)
        with self.assertRaises(ValueError):
            logic.check_roles(twins, "1", ["D3"])

    def test_recovery_problems(self):
        ready = {"metadata": {}, "status": {"conditions": [{"type": "Ready", "status": "True"}]}}
        cluster = {"quorumAvailable": True, "nodesByState": {"ready": 3}}
        self.assertEqual(logic.recovery_problems([ready] * 3, {"data-narumq-1": "u"}, {"data-narumq-1": "u"}, cluster), [])
        unready = {"metadata": {}, "status": {"conditions": [{"type": "Ready", "status": "False"}]}}
        problems = logic.recovery_problems([ready, ready, unready], {"data-narumq-1": "v"}, {"data-narumq-1": "u"}, None)
        self.assertEqual(problems, ["pod_not_ready", "pvc_changed:data-narumq-1", "cluster_not_converged"])


class SizingTests(unittest.TestCase):
    def test_parse_df_plain_and_wrapped(self):
        parsed = logic.parse_df(df_text(512 * MIB))
        self.assertEqual(parsed["availableBytes"], 512 * MIB)
        self.assertEqual(parsed["mount"], "/volume")
        wrapped = ("Filesystem           1K-blocks      Used Available Use% Mounted on\n"
                   "a-very-long-device-name\n   20971520   1024  20970496   1% /volume\n")
        self.assertEqual(logic.parse_df(wrapped)["availableBytes"], 20970496 * 1024)
        for bad in ("", "Filesystem 1K-blocks\n", df_text(1).replace("/volume", ""), "no header\n1 2 3 4 5% /x"):
            with self.subTest(bad), self.assertRaises(ValueError):
                logic.parse_df(bad)

    def test_fill_size_boundaries(self):
        target = 512 * MIB
        self.assertEqual(logic.fill_bytes(target - 1, target), 0)  # already below the target
        self.assertEqual(logic.fill_bytes(target, target), 0)  # exactly at the threshold
        self.assertEqual(logic.fill_bytes(target + 1, target), 0)  # less than one MiB of work
        self.assertEqual(logic.fill_bytes(target + MIB, target), MIB)
        self.assertEqual(logic.fill_bytes(target + 3 * MIB // 2, target), MIB)
        self.assertEqual(logic.fill_bytes(19 * GIB, target), 19 * GIB - target)

    def test_shrink_size_boundaries(self):
        minimum = logic.RECOVERY_FREE_BYTES
        self.assertEqual(logic.shrink_bytes(minimum, minimum, 10 * GIB), 0)
        self.assertEqual(logic.shrink_bytes(minimum + 1, minimum, 10 * GIB), 0)
        self.assertEqual(logic.shrink_bytes(minimum - 1, minimum, 10 * GIB), MIB)
        self.assertEqual(logic.shrink_bytes(0, minimum, 2 * GIB), minimum)
        with self.assertRaises(ValueError):
            logic.shrink_bytes(0, minimum, minimum - MIB)

    def test_adjust_and_release_actions(self):
        target, tolerance = logic.D1_TARGET_FREE_BYTES, logic.D1_FREE_TOLERANCE_BYTES
        self.assertEqual(logic.adjust_action(target + tolerance, target, tolerance, 0)["action"], "none")
        self.assertEqual(logic.adjust_action(target - tolerance, target, tolerance, 0)["action"], "none")
        over = logic.adjust_action(target + tolerance + MIB, target, tolerance, 0)
        self.assertEqual(over, {"action": "fill", "bytes": tolerance + MIB})
        under = logic.adjust_action(target - tolerance - 1, target, tolerance, 5 * GIB)
        self.assertEqual(under["action"], "shrink")
        self.assertEqual(logic.release_action(logic.RECOVERY_FREE_BYTES, logic.RECOVERY_FREE_BYTES, 0)["action"], "none")
        self.assertEqual(logic.release_action(0, GIB, 2 * GIB), {"action": "shrink", "bytes": GIB})

    def test_full_detection_and_dd_exit(self):
        self.assertTrue(logic.is_full(0))
        self.assertTrue(logic.is_full(logic.FULL_AVAILABLE_BYTES))
        self.assertFalse(logic.is_full(logic.FULL_AVAILABLE_BYTES + 1))
        self.assertEqual(logic.dd_exit_code("dd: No space left on device\nDD_EXIT=1\n"), 1)
        self.assertIsNone(logic.dd_exit_code("garbage"))
        self.assertEqual(logic.CLEAR_FREE_BYTES, GIB + 128 * MIB)


def sample(started: float, finished: float, code: int | None, outcome: str = "puback") -> dict:
    return {"startedSeconds": started, "finishedSeconds": finished, "reasonCode": code, "outcome": outcome,
            "puback": code is not None and code < 0x80}


class JudgeTests(unittest.TestCase):
    def test_d1_pressure_needs_0x97_within_ten_seconds(self):
        observer = [sample(0, 0.1, 0), sample(1, 1.1, 0)]
        within = [sample(0, 0.2, 0), sample(1, 9.9, 0x97)]
        self.assertEqual(logic.judge_d1_pressure(within, observer)["result"], "PASS")
        exactly = [sample(0, 10.0, 0x97)]
        self.assertEqual(logic.judge_d1_pressure(exactly, observer)["result"], "PASS")
        late = logic.judge_d1_pressure([sample(0, 0.2, 0), sample(1, 10.5, 0x97)], observer)
        self.assertEqual((late["result"], late["reasons"]), ("FAIL", ["0x97_after_limit"]))
        none = logic.judge_d1_pressure([sample(0, 1, 0x80), sample(1, 2, None, "puback_timeout")], observer)
        self.assertEqual(none["reasons"], ["no_0x97"])
        self.assertEqual(none["targetOutcomes"], {"0x80": 1, "puback_timeout": 1})

    def test_d1_observer_path_must_succeed(self):
        target = [sample(0, 1, 0x97)]
        failing = logic.judge_d1_pressure(target, [sample(0, 1, 0), sample(1, 2, 0x97)])
        self.assertEqual(failing["reasons"], ["observer_publish_failed"])
        self.assertEqual(logic.judge_d1_pressure(target, [])["reasons"], ["observer_not_sampled"])

    def test_d1_recovery_window(self):
        samples = [sample(10, 10.5, 0x97), sample(11, 14.9, 0), sample(15, 15.2, 0)]
        self.assertEqual(logic.judge_d1_recovery(samples, 5.0)["result"], "PASS")
        self.assertEqual(logic.judge_d1_recovery(samples, 5.0)["firstSuccessSeconds"], 14.9 - 5.0)
        slow = logic.judge_d1_recovery(samples, 4.8)
        self.assertEqual(slow["reasons"], ["success_after_limit"])
        self.assertEqual(logic.judge_d1_recovery([sample(10, 10.5, 0x97)], 5.0)["reasons"], ["no_success_after_release"])
        early = [sample(1, 2, 0)]  # a success that finished before the release does not count
        self.assertEqual(logic.judge_d1_recovery(early, 5.0)["reasons"], ["no_success_after_release"])

    def test_storage_failure_view(self):
        failed = {"nodeId": "2", "queryStatus": "OK", "state": "UNREADY", "ready": False, "readinessReason": "x",
                  "storagePressure": True, "replicationHealth": "FAILED", "lastErrorCode": "storage_no_space"}
        self.assertTrue(logic.storage_failure_view(failed)["visible"])
        self.assertEqual(logic.storage_failure_view(failed)["fields"]["lastErrorCode"], "storage_no_space")
        io_error = {**failed, "lastErrorCode": "storage_io_error", "storagePressure": False}
        self.assertTrue(logic.storage_failure_view(io_error)["visible"])
        pressure_only = {**failed, "lastErrorCode": None, "replicationHealth": "FAILED"}
        self.assertTrue(logic.storage_failure_view(pressure_only)["visible"])
        slow = {**failed, "lastErrorCode": "storage_slow", "replicationHealth": "DEGRADED"}
        self.assertFalse(logic.storage_failure_view(slow)["visible"])
        unreachable = {**failed, "queryStatus": "UNREACHABLE"}
        self.assertFalse(logic.storage_failure_view(unreachable)["visible"])
        self.assertEqual(logic.storage_failure_view(None), {"visible": False, "fields": None})

    def test_d2_detection_limit(self):
        self.assertEqual(logic.judge_d2_detection(5.0, 60.0)["result"], "PASS")
        self.assertEqual(logic.judge_d2_detection(61.0, 3.0)["reasons"], ["pod_not_ready_after_limit"])
        self.assertEqual(logic.judge_d2_detection(None, None)["reasons"],
                         ["pod_not_ready", "storage_failure_not_visible"])

    def test_d3_window_has_no_acked_message(self):
        def at(seconds: float) -> str:
            base = datetime.datetime(2026, 10, 16, 6, 0, 0, tzinfo=datetime.timezone.utc)
            return (base + datetime.timedelta(seconds=seconds)).isoformat()
        rows = [{"seq": 1, "sentAt": at(0), "outcome": "acked"},
                {"seq": 2, "sentAt": at(10), "outcome": "unknown"},
                {"seq": 3, "sentAt": at(12), "outcome": "rejected"},
                {"seq": 4, "sentAt": at(30), "outcome": "acked"}]
        verdict = logic.judge_d3_window(rows, at(5), at(20))
        self.assertEqual((verdict["result"], verdict["sentInWindow"]), ("PASS", 2))
        self.assertEqual(verdict["outcomesInWindow"], {"unknown": 1, "rejected": 1})
        edge = logic.judge_d3_window(rows, at(0), at(20))  # sentAt exactly at the start is inside
        self.assertEqual(edge["ackedSeqInWindow"], [1])
        self.assertEqual(edge["reasons"], ["acked_in_window"])
        self.assertEqual(logic.judge_d3_window(rows, at(40), at(50))["reasons"], ["window_without_messages"])
        self.assertEqual(logic.judge_d3_window(rows, None, at(5))["reasons"], ["window_not_established"])
        self.assertEqual(logic.judge_d3_window(rows, at(20), at(5))["reasons"], ["window_inverted"])


class FakeFixture:
    """A scripted kubectl/management surface that tracks free space per Pod."""

    run_id = RUN_ID
    kubectl = ["kubectl"]

    def __init__(self):
        self.commands: list[tuple[list[str], str | None]] = []
        self.total = 20 * GIB
        self.avail = {f"narumq-{i}": 19 * GIB for i in range(3)}
        self.ballast = {f"narumq-{i}": 0 for i in range(3)}
        self.pods = {f"narumq-{i}": {
            "metadata": {"name": f"narumq-{i}", "uid": f"uid-narumq-{i}", "labels": {"app": "narumq"}},
            "spec": {"containers": [{"name": "narumq", "image": IMAGE}]},
            "status": {"conditions": [{"type": "Ready", "status": "True"}], "containerStatuses": [
                {"name": "narumq", "restartCount": 0}]}} for i in range(3)}
        self.leader = "1"
        self.quorum = True
        self.ready_nodes = 3
        self.exec_failures: set[str] = set()
        self.on_pods_read = None
        self.pvc_uid_suffix = ""
        self.deleted: list[tuple[str, dict]] = []
        self.failed: dict[str, bool] = {}
        self.recover_reads: dict[str, int] = {}
        self.clock = None
        self.outage_start = None
        self.outages: list[tuple[float, float]] = []
        self.api_fails_for_nodes = False
        self.local_api_fails = False
        self.liveness_restart = False
        self.killed: set[str] = set()

    # kubectl / aws surface
    def aws(self, *args):
        return {"Account": "809446814769"}

    def get(self, *args):
        if args[0] == "namespace":
            return {"metadata": {"labels": {"narumq.io/acceptance-run": RUN_ID}}}
        if args[0] == "pods":
            self.sync()
            if self.on_pods_read:
                self.on_pods_read(self)
            return {"items": copy.deepcopy(list(self.pods.values()))}
        if args[0] == "pod":
            self.sync()
            return copy.deepcopy(self.pods.get(args[1]))
        if args[0] == "pvc":
            return {"metadata": {"uid": "pvc-uid-" + args[1] + self.pvc_uid_suffix}, "status": {"phase": "Bound"}}
        raise AssertionError(args)

    def get_pod(self, name):
        self.sync()
        return copy.deepcopy(self.pods.get(name))

    def api(self, path, port=23880):
        self.sync()
        if path == "/api/v1/cluster":
            unhealthy = self.unhealthy_count()
            return {"clusterId": RUN_ID, "quorumAvailable": self.quorum and unhealthy < 2,
                    "leaderNodeId": self.leader, "nodesByState": {"ready": 3 - unhealthy}}
        if path == "/api/v1/nodes":
            return [{"nodeId": str(i), "queryStatus": "OK", "ready": True} for i in (1, 2, 3)]
        if path.startswith("/api/v1/nodes/"):
            if self.api_fails_for_nodes:
                raise TimeoutError("management API unavailable")
            return self.node_record(int(path.rsplit("/", 1)[1]))
        raise AssertionError(path)

    def management_probe(self):
        unhealthy = self.unhealthy_count()
        return {"quorumAvailable": self.quorum and unhealthy < 2, "leaderNodeId": self.leader,
                "nodesByState": {"ready": 3 - unhealthy}}

    def unhealthy(self, pod: str) -> bool:
        return self.failed.get(pod, False) or self.avail[pod] == 0

    def unhealthy_count(self) -> int:
        return sum(self.unhealthy(pod) for pod in self.avail)

    def node_record(self, node_id: int) -> dict:
        pod = f"narumq-{node_id - 1}"
        record = {"nodeId": str(node_id), "queryStatus": "OK", "state": "READY", "ready": True,
                  "readinessReason": None, "storagePressure": False, "replicationHealth": "HEALTHY",
                  "lastErrorCode": None, "queryErrorCode": None}
        if self.unhealthy(pod):
            record.update(state="UNREADY", ready=False, readinessReason="storage_failed", storagePressure=True,
                          replicationHealth="FAILED", lastErrorCode="storage_no_space")
        return record

    def write_load(self, nbytes: int) -> None:
        """Retained load: every filled member appends the payload to its WAL until no space is left."""
        for pod in self.avail:
            if self.ballast[pod] > 0 and self.avail[pod] <= MIB:
                self.avail[pod] = max(0, self.avail[pod] - nbytes)

    def sync(self) -> None:
        """Broker model: ENOSPC fails a broker until space returns and three reads pass (a self restart)."""
        for pod, obj in self.pods.items():
            ready = obj["status"]["conditions"][0]
            if self.avail[pod] == 0:
                self.failed[pod] = True
                self.recover_reads[pod] = 0
                ready["status"] = "False"
                if self.liveness_restart and pod not in self.killed:
                    self.killed.add(pod)
                    obj["status"]["containerStatuses"][0]["restartCount"] += 1
            elif self.failed.get(pod):
                self.recover_reads[pod] += 1
                if self.recover_reads[pod] >= 3:
                    self.failed[pod] = False
                    ready["status"] = "True"
                    obj["status"]["containerStatuses"][0]["restartCount"] += 1
        count = self.unhealthy_count()
        now = self.clock.now if self.clock else 0.0
        if count >= 2 and self.outage_start is None:
            self.outage_start = now
        elif count < 2 and self.outage_start is not None:
            self.outages.append((self.outage_start, now))
            self.outage_start = None

    def command(self, args, timeout=90, stdin=None):
        self.commands.append((list(args), stdin))
        if "patch" in args and "--subresource=ephemeralcontainers" in args:
            pod = self.pods[args[args.index("patch") + 2]]
            container = json.loads(args[args.index("-p") + 1])["spec"]["ephemeralContainers"][0]
            pod["spec"].setdefault("ephemeralContainers", []).append(container)
            pod["status"].setdefault("ephemeralContainerStatuses", []).append(
                {"name": container["name"], "state": {"running": {"startedAt": "2026-10-16T06:00:00Z"}}})
            return "{}"
        if "delete" in args:
            name = args[-3].rsplit("/", 1)[-1]
            self.deleted.append((name, json.loads(stdin)))
            return ""
        if "logs" in args:
            name = args[args.index("logs") + 1]
            if "--previous" in args and self.pods[name]["status"]["containerStatuses"][0]["restartCount"] == 0:
                raise RuntimeError("previous terminated container not found")
            return ('{"level":"INFO","msg":"started"}\n'
                    '{"level":"ERROR","msg":"persist snapshot failed","error":"write: no space left on device"}\n')
        if "exec" in args:
            name = args[args.index("exec") + 1]
            script = args[-1]
            if name in self.exec_failures:
                raise RuntimeError("exec failed")
            if args[args.index("-c") + 1] == "narumq":
                if self.local_api_fails:
                    raise RuntimeError("curl failed")
                return json.dumps({"data": self.node_record(int(name.rsplit("-", 1)[1]) + 1)})
            return self._exec(name, script)
        raise AssertionError(args)

    def _exec(self, pod: str, script: str) -> str:
        if script.startswith("df -k"):
            return df_text(self.avail[pod], self.total)
        if script.startswith("stat -c %s"):
            return f"{self.ballast[pod]}\n"
        if script.startswith("dd if=/dev/zero"):
            if " count=" in script:
                count = int(script.split(" count=")[1].split()[0]) * MIB
                count = min(count, self.avail[pod])
                self.ballast[pod] += count
                self.avail[pod] -= count
                return "DD_EXIT=0\n"
            # dd stops at ENOSPC with a little space left, like the real filesystem did (about 540 KiB free).
            left = 128 * 1024 if "bs=4096" in script else 512 * 1024
            self.ballast[pod] += max(0, self.avail[pod] - left)
            self.avail[pod] = min(self.avail[pod], left)
            return "dd: error writing '/volume/chaos-ballast.bin': No space left on device\nDD_EXIT=1\n"
        if script.startswith("truncate -s"):
            size = int(script.split()[2])
            self.avail[pod] += self.ballast[pod] - size
            self.ballast[pod] = size
            return ""
        if "rm -f" in script:
            self.avail[pod] += self.ballast[pod]
            self.ballast[pod] = 0
            return ""
        if script.startswith("killall dd"):
            return ""
        raise AssertionError(script)

    def exec_scripts(self, pod: str) -> list[str]:
        return [args[-1] for args, _ in self.commands if "exec" in args and args[args.index("exec") + 1] == pod]


class BallastTests(unittest.TestCase):
    def setUp(self):
        self.fixture = FakeFixture()
        self.controller = ballast_module.BallastController(self.fixture, IMAGE, sleep=lambda s: None)

    def test_ensure_creates_ephemeral_container_with_volume_and_root(self):
        result = self.controller.ensure("narumq-1", "uid-narumq-1")
        self.assertTrue(result["created"])
        patch = next(args for args, _ in self.fixture.commands if "patch" in args)
        self.assertIn("--subresource=ephemeralcontainers", patch)
        container = json.loads(patch[patch.index("-p") + 1])["spec"]["ephemeralContainers"][0]
        self.assertEqual(container["name"], "ballast")
        self.assertEqual(container["image"], IMAGE)
        self.assertEqual(container["command"][:2], ["/bin/sh", "-c"])
        self.assertIn('trap "exit 0" TERM', container["command"][2])
        self.assertEqual(container["command"][2], ballast_module.IDLE_COMMAND)
        self.assertEqual(container["securityContext"]["runAsUser"], 0)
        self.assertEqual(container["volumeMounts"], [{"name": "data", "mountPath": "/volume"}])
        self.assertIn("narumq-1", self.controller.registry)

    def test_ensure_reuses_running_container_and_rejects_replaced_pod(self):
        self.controller.ensure("narumq-1", "uid-narumq-1")
        again = self.controller.ensure("narumq-1", "uid-narumq-1")
        self.assertEqual((again["created"], again["reused"]), (False, True))
        self.assertEqual(sum("patch" in args for args, _ in self.fixture.commands), 1)
        with self.assertRaises(ballast_module.BallastError):
            self.controller.ensure("narumq-2", "stale-uid")
        self.assertNotIn("narumq-2", self.controller.registry)

    def test_adjust_free_fills_to_target_band_and_release_shrinks(self):
        self.controller.ensure("narumq-1", "uid-narumq-1")
        result = self.controller.adjust_free("narumq-1", logic.D1_TARGET_FREE_BYTES, logic.D1_FREE_TOLERANCE_BYTES)
        self.assertTrue(result["ok"])
        self.assertEqual(self.fixture.avail["narumq-1"], logic.D1_TARGET_FREE_BYTES)
        released = self.controller.release_to("narumq-1", logic.RECOVERY_FREE_BYTES)
        self.assertEqual(released["action"], "shrink")
        self.assertGreaterEqual(self.fixture.avail["narumq-1"], logic.RECOVERY_FREE_BYTES)
        self.assertLess(self.fixture.avail["narumq-1"], logic.RECOVERY_FREE_BYTES + MIB)
        noop = self.controller.release_to("narumq-1", logic.RECOVERY_FREE_BYTES)
        self.assertEqual(noop["action"], "none")

    def test_adjust_free_already_below_target_shrinks(self):
        self.controller.ensure("narumq-1", "uid-narumq-1")
        self.fixture.ballast["narumq-1"] = 19 * GIB
        self.fixture.avail["narumq-1"] = 64 * MIB
        self.controller.adjust_free("narumq-1", logic.D1_TARGET_FREE_BYTES, logic.D1_FREE_TOLERANCE_BYTES)
        self.assertLessEqual(abs(self.fixture.avail["narumq-1"] - logic.D1_TARGET_FREE_BYTES),
                             logic.D1_FREE_TOLERANCE_BYTES)

    def test_fill_to_enospc_expects_nonzero_dd_exit_and_verifies_df(self):
        self.controller.ensure("narumq-1", "uid-narumq-1")
        result = self.controller.fill_to_enospc("narumq-1")
        self.assertTrue(result["full"])
        self.assertEqual(result["rounds"][0]["ddExit"], 1)
        self.assertLessEqual(self.fixture.avail["narumq-1"], logic.FULL_AVAILABLE_BYTES)
        self.assertEqual(result["rounds"][-1]["blockSize"], 4096)

    def test_fill_to_enospc_fails_when_df_never_reaches_zero(self):
        self.controller.ensure("narumq-1", "uid-narumq-1")
        original = self.fixture._exec

        def never_full(pod, script):
            if script.startswith("dd if=/dev/zero"):
                return "DD_EXIT=0\n"
            return original(pod, script)
        self.fixture._exec = never_full
        with self.assertRaises(ballast_module.BallastError):
            self.controller.fill_to_enospc("narumq-1")

    def test_remove_all_continues_after_failure_and_notes_replaced_pod(self):
        for pod in ("narumq-1", "narumq-2"):
            self.controller.ensure(pod, f"uid-{pod}")
            self.fixture.ballast[pod] = GIB
            self.fixture.avail[pod] = 0
        self.controller.ensure("narumq-0", "uid-narumq-0")
        self.fixture.exec_failures.add("narumq-1")
        self.fixture.pods["narumq-0"]["metadata"]["uid"] = "replaced"
        report = self.controller.remove_all()
        self.assertEqual(report["narumq-1"]["outcome"], "failed")
        self.assertEqual(report["narumq-2"]["removed"], True)
        self.assertEqual(self.fixture.ballast["narumq-2"], 0)
        self.assertEqual(report["narumq-0"]["outcome"], "pod_replaced_ballast_lost_with_pod")
        again = self.controller.remove_all()
        self.assertTrue(again["narumq-2"]["alreadyRemoved"])


class FakePaho:
    """Just enough of paho-mqtt 2.x for QoS1Probe."""

    def __init__(self):
        self.connect_codes = []
        self.puback_codes = []
        self.events = []
        self.published = []
        outer = self

        class Reason:
            def __init__(self, value):
                self.value = value
                self.is_failure = value >= 0x80

        class Info:
            def __init__(self, mid):
                self.mid = mid
                self.rc = 0

            def wait_for_publish(self, timeout=None):
                return None

        class Client:
            def __init__(self, *args, **kwargs):
                self.client_id = kwargs["client_id"]
                self.mid = 0
                self.pending = None
                self.loop_alive = False

            def username_pw_set(self, *args):
                pass

            def loop_start(self):
                outer.events.append("loop_start")
                # Like paho with reconnect_on_failure=False: a loop started without a connected socket exits.
                self.loop_alive = self.pending is not None
                self.deliver()

            def loop_stop(self):
                outer.events.append("loop_stop")
                self.loop_alive = False

            def disconnect(self):
                pass

            def deliver(self):
                if self.loop_alive and self.pending is not None:
                    code, self.pending = self.pending, None
                    self.on_connect(self, None, {}, Reason(code), None)

            def connect(self, host, port, keepalive, clean_start=True):
                outer.events.append("connect")
                self.pending = outer.connect_codes.pop(0)
                self.deliver()

            def publish(self, topic, payload, qos=0, retain=False):
                outer.published.append({"topic": topic, "size": len(payload), "retain": retain})
                self.mid += 1
                code = outer.puback_codes.pop(0)
                if code is not None:
                    self.on_publish(self, None, self.mid, Reason(code), None)
                return Info(self.mid)

        mqtt = types.ModuleType("paho.mqtt")
        client = types.ModuleType("paho.mqtt.client")
        client.Client = Client
        client.CallbackAPIVersion = types.SimpleNamespace(VERSION2=2)
        client.MQTTv5 = 5
        client.MQTT_ERR_SUCCESS = 0
        mqtt.client = client
        paho = types.ModuleType("paho")
        paho.mqtt = mqtt
        self.modules = {"paho": paho, "paho.mqtt": mqtt, "paho.mqtt.client": client}


class ProbeTests(unittest.TestCase):
    def setUp(self):
        self.paho = FakePaho()
        self.saved = {name: sys.modules.get(name) for name in self.paho.modules}
        sys.modules.update(self.paho.modules)
        self.directory = tempfile.TemporaryDirectory()
        (Path(self.directory.name) / "mqtt.password").write_text("secret-password", encoding="utf-8")
        import pressure_probe
        self.probe = pressure_probe.QoS1Probe(Path(self.directory.name), RUN_ID, "storage-pressure-probe", 24883,
                                              "acceptance/x/storage-pressure/d1")

    def tearDown(self):
        for name, module in self.saved.items():
            if module is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = module
        self.directory.cleanup()

    def test_records_exact_reason_codes_without_secrets(self):
        self.paho.connect_codes = [0]
        self.paho.puback_codes = [0x97, 0x00]
        first, second = self.probe.attempt(), self.probe.attempt()
        self.assertEqual((first["outcome"], first["reasonCode"], first["puback"]), ("puback", 0x97, False))
        self.assertEqual((second["reasonCode"], second["puback"]), (0, True))
        self.assertLessEqual(first["startedSeconds"], first["finishedSeconds"])
        self.assertNotIn("secret-password", json.dumps([first, second]))

    def test_refused_connect_and_missing_puback_are_samples(self):
        self.paho.connect_codes = [0x97, 0]
        refused = self.probe.attempt()
        self.assertEqual((refused["outcome"], refused["reasonCode"], refused["puback"]), ("connect_refused", 0x97, False))
        self.paho.puback_codes = [None]
        missing = self.probe.attempt()
        self.assertEqual((missing["outcome"], missing["reasonCode"]), ("puback_timeout", None))

    def test_connect_precedes_loop_start_on_first_connect_and_every_reconnect(self):
        self.paho.connect_codes = [0, 0]
        self.paho.puback_codes = [0, 0]
        self.assertEqual(self.probe.attempt()["puback"], True)
        self.assertEqual(self.paho.events, ["connect", "loop_start"])  # no loop before the first connect
        self.probe.client.on_disconnect(self.probe.client, None, None, None)  # link lost: next attempt reconnects
        self.assertEqual(self.probe.attempt()["puback"], True)
        self.assertEqual(self.paho.events, ["connect", "loop_start", "loop_stop", "connect", "loop_start"])
        self.probe.close()
        self.assertEqual(self.paho.events[-1], "loop_stop")

    def test_retained_load_publishes_large_retained_payloads_over_rotating_topics(self):
        import pressure_probe
        load = pressure_probe.QoS1Probe(Path(self.directory.name), RUN_ID, "storage-pressure-probe", 23883,
                                        "acceptance/x/storage-load/d2", payload_bytes=131072, topic_count=3)
        self.paho.connect_codes = [0]
        self.paho.puback_codes = [0, 0, 0, 0]
        samples = [load.attempt() for _ in range(4)]
        self.assertTrue(all(s["puback"] and s["payloadBytes"] == 131072 for s in samples))
        self.assertEqual([p["topic"] for p in self.paho.published],
                         [f"acceptance/x/storage-load/d2/{n}" for n in (0, 1, 2, 0)])
        self.assertTrue(all(p["retain"] and p["size"] == 131072 for p in self.paho.published))
        self.paho.puback_codes = [0, 0, 0]
        cleared = load.clear_retained()
        self.assertEqual([c["reasonCode"] for c in cleared], [0, 0, 0])
        self.assertEqual([p["size"] for p in self.paho.published[-3:]], [0, 0, 0])
        self.assertTrue(all(p["retain"] for p in self.paho.published[-3:]))

    def test_connect_failure_is_a_sample_with_loop_stopped_before_retry(self):
        self.paho.connect_codes = [0x97, 0]
        self.probe.attempt()
        self.paho.puback_codes = [0]
        self.probe.attempt()
        self.assertEqual(self.paho.events, ["connect", "loop_start", "loop_stop", "connect", "loop_start"])


class FakeTunnel:
    started: list[tuple[str, int, int]] = []
    instances: list["FakeTunnel"] = []

    def __init__(self, fixture, pod, uid, management_port, mqtt_port):
        self.args = (pod, management_port, mqtt_port)
        self.pod = pod
        self.uid = uid
        self.fixture = fixture
        self.dead = False
        FakeTunnel.instances.append(self)

    def start(self, timeout=20):
        live = self.fixture.get_pod(self.pod)
        if not live or live["metadata"]["uid"] != self.uid or live["status"]["conditions"][0]["status"] != "True":
            raise ValueError("Pod identity is not current and Ready")
        FakeTunnel.started.append(self.args)
        return {"pod": self.pod}

    def check(self):
        if self.dead:
            raise RuntimeError("tunnel ended")

    def close(self):
        return None


BASE_TIME = datetime.datetime(2026, 10, 16, 6, 0, 0, tzinfo=datetime.timezone.utc)
OBSERVER_MQTT = 23883


def iso(seconds: float) -> str:
    return (BASE_TIME + datetime.timedelta(seconds=seconds)).isoformat()


class Clock:
    def __init__(self):
        self.now = 0.0
        self.hooks = []

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds
        for hook in self.hooks:
            hook()


def fake_runtime(fixture: FakeFixture, clock: Clock, explode_on_probe: bool = False):
    loops = []

    class FakeProbe:
        def __init__(self, private, run_id, client_id, port, topic, epoch, payload_bytes=None, topic_count=1):
            if explode_on_probe:
                raise RuntimeError("probe cannot start")
            self.client_id, self.epoch, self.port, self.payload_bytes = client_id, epoch, port, payload_bytes

        def attempt(self):
            import time
            now = time.monotonic() - self.epoch
            size = 0
            if self.payload_bytes:
                # Retained load: the payload is appended to every filled member's WAL until its disk is exhausted.
                size = self.payload_bytes
                fixture.write_load(size)
                code = 0
            elif self.port == OBSERVER_MQTT:
                code = options.get("observer_probe_code", 0)
            else:
                code = 0x97 if fixture.avail["narumq-1"] < logic.MIN_FREE_BYTES else 0
            return {"startedSeconds": now, "finishedSeconds": now, "outcome": "puback", "reasonCode": code,
                    "puback": code < 0x80, "payloadBytes": size}

        def clear_retained(self):
            return [{"topic": "t", "outcome": "puback", "reasonCode": 0}]

        def close(self):
            return None

    class FakeLoop:
        def __init__(self, probe, interval=1.0):
            self.probe, self.samples, self.active = probe, [], False
            loops.append(self)

        def start(self):
            self.active = True

        def stop(self, timeout=30):
            self.active = False
            return list(self.samples)

    clock.hooks.append(lambda: [loop.samples.append(loop.probe.attempt()) for loop in loops if loop.active])
    fixture.clock = clock
    calls = []
    options = {"ack_during_outage": False}

    def prepare_subscriber(private, run_id, scenario, port, output):
        calls.append(("prepare", scenario, port))
        return {"result": "PASS", "scenario": scenario}

    class FakeDurablePublisher:
        """Journals one message per `interval` s of fake time, at most `max_messages`; messages sent during a
        quorum outage are never acked."""

        def __init__(self, private, run_id, scenario, port, output, interval=0.2, max_messages=60):
            self.scenario, self.output, self.started, self.result = scenario, Path(output), None, None
            self.interval, self.max_messages = interval, max_messages
            calls.append(("publisher-args", scenario, interval, max_messages))

        def start(self):
            self.started = clock.now
            calls.append(("publisher", self.scenario, OBSERVER_MQTT))

        def stop(self):
            if self.result is None:
                rows, step = [], 0
                while step < self.max_messages and self.started + 0.05 + self.interval * step <= clock.now:
                    sent = self.started + 0.05 + self.interval * step
                    outage = any(a <= sent <= b for a, b in fixture.outages)
                    acked = options["ack_during_outage"] or not outage
                    rows.append({"seq": step + 1, "sentAt": iso(sent), "outcome": "acked" if acked else "unknown",
                                 "reasonCode": 0 if acked else None, "ackAt": iso(sent + 0.01) if acked else None})
                    step += 1
                (self.output / f"durable-publish-{self.scenario}.jsonl").write_text(
                    "".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")
                self.result = {"published": len(rows)}
            return self.result

    def verify_subscriber(private, run_id, scenario, port, output, label):
        calls.append(("verify", scenario, port, label))
        return {"result": "PASS", "label": label}

    return types.SimpleNamespace(Fixture=lambda private, run_id: fixture, PodTunnel=FakeTunnel, QoS1Probe=FakeProbe,
                                 ProbeLoop=FakeLoop, prepare_subscriber=prepare_subscriber,
                                 DurablePublisher=FakeDurablePublisher, verify_subscriber=verify_subscriber,
                                 calls=calls, options=options)


class RunTestBase(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.root = Path(self.directory.name)
        self.output = self.root / RUN_ID
        self.output.mkdir()
        self.fixture = FakeFixture()
        self.clock = Clock()
        FakeTunnel.started = []
        FakeTunnel.instances = []
        patcher = unittest.mock.patch.object(pressure_main, "timestamp", lambda: iso(self.clock.now))
        patcher.start()
        self.addCleanup(patcher.stop)

    def tearDown(self):
        self.directory.cleanup()

    def run_object(self, scenarios, explode=False, **plan_overrides):
        normalized = logic.validate_plan(plan(**plan_overrides), scenarios, RUN_ID)
        self.runtime = fake_runtime(self.fixture, self.clock, explode)
        return pressure_main.StoragePressureRun(self.fixture, normalized, self.output, self.root, self.runtime,
                                                scenarios, sleep=self.clock.sleep, clock=self.clock.monotonic)

    def ballast_commands(self):
        return [args for args, _ in self.fixture.commands if "patch" in args or "exec" in args]


class RunTests(RunTestBase):

    def test_preflight_rejects_leader_target_before_any_ballast(self):
        self.fixture.leader = "2"
        run = self.run_object(["D1"])
        with self.assertRaises(ValueError):
            run.start()
        self.assertEqual(self.ballast_commands(), [])

    def test_preflight_rejects_unready_cluster_and_wrong_image(self):
        self.fixture.pods["narumq-2"]["status"]["conditions"][0]["status"] = "False"
        with self.assertRaises(ValueError):
            self.run_object(["D1"]).start()
        self.fixture.pods["narumq-2"]["status"]["conditions"][0]["status"] = "True"
        self.fixture.pods["narumq-1"]["spec"]["containers"][0]["image"] = "other@sha256:" + "b" * 64
        with self.assertRaises(ValueError):
            self.run_object(["D1"]).start()
        self.assertEqual(self.ballast_commands(), [])

    def test_preflight_rejects_observer_equal_to_target(self):
        run = self.run_object(["D1"], observerPod="narumq-1", observerPodUID="uid-narumq-1")
        with self.assertRaises(ValueError):
            run.start()
        self.assertEqual(self.ballast_commands(), [])

    def test_d1_end_to_end_with_fakes_passes_and_keeps_evidence(self):
        run = self.run_object(["D1"])
        run.start()
        results = run.run_scenarios()
        d1 = results["D1"]
        self.assertEqual((d1["result"], d1["reasons"]), ("PASS", []))
        self.assertEqual(d1["pressureJudgement"]["result"], "PASS")
        self.assertEqual(d1["recoveryJudgement"]["result"], "PASS")
        self.assertIn((("narumq-1", 24880, 24883)), FakeTunnel.started)
        self.assertGreaterEqual(self.fixture.avail["narumq-1"], logic.CLEAR_FREE_BYTES)
        self.assertGreater(self.fixture.ballast["narumq-1"], 0)  # left for the next scenario
        report = run.finish()
        self.assertEqual(self.fixture.ballast["narumq-1"], 0)
        self.assertTrue(report["ballastRemoval"]["narumq-1"]["removed"])
        self.assertTrue((self.output / "d1-result.json").exists())
        self.assertTrue((self.output / "d1-target-probe-samples.json").exists())

    def test_cleanup_runs_after_scenario_exception(self):
        run = self.run_object(["D1"], explode=True)
        run.start()
        results = run.run_scenarios()
        self.assertEqual(results["D1"]["result"], "FAIL")
        self.assertEqual(results["D1"]["errorCode"], "RuntimeError")
        self.assertTrue(results["D1"]["ballastRemovalAfterScenario"]["narumq-1"]["removed"])
        self.assertEqual(self.fixture.ballast["narumq-1"], 0)
        self.assertIn("rm -f", " ".join(self.fixture.exec_scripts("narumq-1")))

    def test_exception_records_step_location_and_fixed_message_without_library_text(self):
        run = self.run_object(["D1"], explode=True)
        run.start()
        result = run.run_scenarios()["D1"]
        self.assertEqual(result["errorStep"], "d1-probe")
        self.assertIn("test_eks_storage_pressure.py", result["errorLocation"])
        self.assertEqual(result["errorMessage"], "unclassified")  # RuntimeError text from a library is never kept
        self.assertIn("ballast", result["partial"])

    def test_d2_records_initial_probe_failure_with_named_step(self):
        run = self.run_object(["D2"])
        self.runtime.options["observer_probe_code"] = 0x97
        run.start()
        result = run.run_scenarios()["D2"]
        self.assertEqual(result["result"], "FAIL")
        self.assertEqual(result["errorStep"], "d2-monitor-start")
        self.assertEqual(result["errorMessage"], "initial observer probes were not successful; ballast not written")
        self.assertEqual(result["partial"]["initialProbes"]["mqtt"][0]["reasonCode"], 0x97)
        self.assertEqual([c for c in self.runtime.calls if c[0] == "publisher"], [])

    def test_finish_removes_ballast_even_when_run_raised_midway(self):
        run = self.run_object(["D1", "D2"])
        run.start()
        run.ballast.ensure("narumq-1", "uid-narumq-1")
        run.ballast.fill_to_enospc("narumq-1")
        self.assertLessEqual(self.fixture.avail["narumq-1"], logic.FULL_AVAILABLE_BYTES)

        def explode():
            raise RuntimeError("boom")
        run.scenario_d1 = explode
        run.scenario_d2 = explode
        run.run_scenarios()
        report = run.finish()
        self.assertEqual(self.fixture.avail["narumq-1"], 19 * GIB)
        self.assertEqual(self.fixture.ballast["narumq-1"], 0)
        self.assertIn("narumq-1", report["ballastRemoval"])

    def test_main_always_writes_summary_and_removes_ballast(self):
        private = self.root / "private"
        private.mkdir()
        plan_path = self.root / "plan.json"
        plan_path.write_text(json.dumps(plan()), encoding="utf-8")
        output = self.root / RUN_ID
        output.rmdir()
        runtime = fake_runtime(self.fixture, self.clock, explode_on_probe=True)
        import contextlib
        import io
        with contextlib.redirect_stdout(io.StringIO()):
            code = pressure_main.main(["run", "--private", str(private), "--output", str(output), "--plan",
                                       str(plan_path), "--execute", "--only", "D1"], runtime=runtime)
        self.assertEqual(code, 1)
        summary = json.loads((output / "summary.json").read_text(encoding="utf-8"))
        self.assertEqual(summary["result"], "FAIL")
        self.assertEqual(summary["scenarios"]["D1"]["errorCode"], "RuntimeError")
        self.assertEqual(self.fixture.ballast["narumq-1"], 0)
        self.assertNotIn("ballastLeftOn", summary)

    def test_main_requires_execute_and_valid_only(self):
        import contextlib
        import io
        private = self.root / "private"
        private.mkdir()
        base = ["run", "--private", str(private), "--output", str(self.root / "x"), "--plan", str(self.root / "plan.json")]
        with contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit):
                pressure_main.main(base)
            with self.assertRaises(SystemExit):
                pressure_main.main(base + ["--execute", "--only", "D9"])
        self.assertFalse((self.root / "x").exists())

    # -- recovery paths shared by D2 and D3 --

    def test_recover_without_delete_when_broker_restarts_itself(self):
        run = self.run_object(["D2"])
        pvcs = run.pvc_uids(["narumq-1"])
        reads = {"count": 0}
        target = self.fixture.pods["narumq-1"]
        target["status"]["conditions"][0]["status"] = "False"

        def hook(fixture):
            reads["count"] += 1
            if reads["count"] == 4:
                target["status"]["conditions"][0]["status"] = "True"
                target["status"]["containerStatuses"][0]["restartCount"] = 1
        self.fixture.on_pods_read = hook
        result = run.recover(["narumq-1"], pvcs, self.clock.monotonic(), "d2")
        self.assertTrue(result["recovered"])
        self.assertEqual(result["path"], "broker_recovered_without_pod_delete")
        self.assertEqual(self.fixture.deleted, [])
        self.assertEqual(result["restartCountsAfter"], {"narumq-1": 1})

    def test_recover_deletes_pod_with_uid_precondition_after_self_recovery_window(self):
        run = self.run_object(["D2"], selfRecoverySeconds=30)
        pvcs = run.pvc_uids(["narumq-1"])
        target = self.fixture.pods["narumq-1"]
        target["status"]["conditions"][0]["status"] = "False"
        original_command = self.fixture.command

        def command(args, timeout=90, stdin=None):
            output = original_command(args, timeout, stdin)
            if "delete" in args:
                target["metadata"]["uid"] = "uid-replacement"
                target["status"]["conditions"][0]["status"] = "True"
            return output
        self.fixture.command = command
        result = run.recover(["narumq-1"], pvcs, self.clock.monotonic(), "d2")
        self.assertTrue(result["recovered"])
        self.assertEqual(result["path"], "pod_deleted_after_self_recovery_window")
        self.assertEqual(len(self.fixture.deleted), 1)
        name, options = self.fixture.deleted[0]
        self.assertEqual(name, "narumq-1")
        self.assertEqual(options["preconditions"], {"uid": "uid-narumq-1"})
        self.assertNotIn("gracePeriodSeconds", options)
        self.assertEqual(run.uids["narumq-1"], "uid-replacement")

    def test_recover_reports_failure_when_pvc_identity_changed(self):
        run = self.run_object(["D2"], selfRecoverySeconds=3)
        pvcs = run.pvc_uids(["narumq-1"])
        self.fixture.pvc_uid_suffix = "-new"
        result = run.recover(["narumq-1"], pvcs, self.clock.monotonic(), "d2")
        self.assertFalse(result["recovered"])
        self.assertIn("pvc_changed:data-narumq-1", result["finalProblems"])
        self.assertGreater(result["secondsAfterBallastRemoval"], logic.RECOVERY_LIMIT_SECONDS)

    def test_recover_aborts_when_a_non_target_pod_is_replaced(self):
        run = self.run_object(["D2"])
        pvcs = run.pvc_uids(["narumq-1"])
        self.fixture.pods["narumq-0"]["metadata"]["uid"] = "replaced-observer"
        with self.assertRaises(RuntimeError):
            run.recover(["narumq-1"], pvcs, self.clock.monotonic(), "d2")


class ScenarioTests(RunTestBase):
    """D2 and D3 end to end against a fake broker that fails at ENOSPC and restarts itself once space returns."""

    def finish(self, run):
        report = run.finish()
        self.assertEqual(report["cleanupErrors"], [])
        for pod in ("narumq-1", "narumq-2"):
            self.assertEqual(self.fixture.ballast[pod], 0)
        return report

    def test_d2_passes_and_removes_ballast_before_recovery(self):
        run = self.run_object(["D2"])
        run.start()
        result = run.run_scenarios()["D2"]
        self.assertEqual((result["result"], result["reasons"]), ("PASS", []))
        self.assertEqual(result["detection"]["firstStorageFailureFields"]["lastErrorCode"], "storage_no_space")
        self.assertGreater(result["detection"]["podNotReadySeconds"], 0.0)
        self.assertEqual(result["recovery"]["path"], "broker_recovered_without_pod_delete")
        self.assertEqual(result["recovery"]["restartCountsAfter"], {"narumq-1": 1})
        self.assertEqual(result["observerMonitor"]["result"], "PASS")
        self.assertEqual(self.fixture.deleted, [])
        kinds = [call[0] for call in self.runtime.calls]
        self.assertEqual(kinds, ["prepare", "publisher-args", "publisher", "verify"])
        self.assertEqual(self.runtime.calls[0][2], 24883)  # subscriber prepared on the target tunnel
        self.assertEqual(self.runtime.calls[1][2:], (3.0, 60))  # plan defaults: 3 s x 60 messages
        self.assertEqual(self.runtime.calls[3][3], "after-recovery")
        load = result["load"]
        self.assertGreater(load["ackedBytes"], 0)
        self.assertEqual(load["payloadBytes"], 128 * 1024)
        self.assertEqual(load["retainedCleanup"]["topics"][0]["reasonCode"], 0)
        self.assertTrue(load["retainedCleanup"]["complete"])
        self.assertTrue((self.output / "d2-load-samples.json").exists())
        scripts = self.fixture.exec_scripts("narumq-1")
        removal = next(i for i, script in enumerate(scripts) if "rm -f" in script)
        self.assertTrue(any(script.startswith("dd if=/dev/zero") and " count=" not in script
                            for script in scripts[:removal]))
        self.assertTrue((self.output / "d2-detection-samples.json").exists())
        self.assertTrue((self.output / "d2-recovery-observations.json").exists())
        self.finish(run)

    def test_d2_fails_when_storage_failure_is_never_visible(self):
        self.fixture.api_fails_for_nodes = True
        self.fixture.local_api_fails = True
        run = self.run_object(["D2"])
        run.start()
        result = run.run_scenarios()["D2"]
        self.assertEqual(result["result"], "FAIL")
        self.assertIn("detection:storage_failure_not_visible", result["reasons"])
        self.finish(run)

    def test_d3_passes_when_nothing_is_acked_inside_the_window(self):
        run = self.run_object(["D3"])
        run.start()
        result = run.run_scenarios()["D3"]
        self.assertEqual((result["result"], result["reasons"]), ("PASS", []))
        window = result["window"]
        self.assertEqual(window["ackedSeqInWindow"], [])
        self.assertGreaterEqual(window["sentInWindow"], 3)
        self.assertEqual(sorted(result["detection"]["seen"]), ["narumq-1", "narumq-2"])
        self.assertEqual(result["recovery"]["path"], "broker_recovered_without_pod_delete")
        self.assertEqual(self.runtime.calls[0], ("prepare", "storage-d3", 24883))
        self.assertEqual(self.runtime.calls[-1][3], "after-recovery")
        self.assertEqual(self.runtime.calls[1][2:], (3.0, 60))
        load = result["load"]
        self.assertGreater(load["ackedBytes"], 0)
        self.assertTrue(all(v["seconds"] > 0 for v in result["detection"]["seen"].values()))
        self.assertTrue((self.output / "d3-load-samples.json").exists())
        self.finish(run)

    def test_d3_fails_when_a_message_is_acked_inside_the_window(self):
        run = self.run_object(["D3"])
        self.runtime.options["ack_during_outage"] = True
        run.start()
        result = run.run_scenarios()["D3"]
        self.assertEqual(result["result"], "FAIL")
        self.assertIn("window:acked_in_window", result["reasons"])
        self.finish(run)

    def test_d3_without_messages_in_the_window_cannot_pass(self):
        run = self.run_object(["D3"], d3HoldSeconds=0, durableMaxMessages=1)
        run.start()
        result = run.run_scenarios()["D3"]
        self.assertEqual(result["result"], "FAIL")
        self.assertIn("window:window_without_messages", result["reasons"])
        self.finish(run)

    def test_d3_uses_the_nodes_own_view_when_the_observer_api_is_unavailable(self):
        self.fixture.api_fails_for_nodes = True
        run = self.run_object(["D3"])
        run.start()
        result = run.run_scenarios()["D3"]
        self.assertEqual(result["result"], "PASS")
        self.assertEqual({v["source"] for v in result["detection"]["seen"].values()}, {"local_exec"})
        self.finish(run)

    def test_d3_window_also_starts_on_not_ready_when_the_api_is_gone(self):
        self.fixture.api_fails_for_nodes = True
        self.fixture.local_api_fails = True
        run = self.run_object(["D3"])
        run.start()
        result = run.run_scenarios()["D3"]
        seen = result["detection"]["seen"]
        self.assertEqual({v["evidence"] for v in seen.values()}, {"pod_not_ready"})
        self.assertIsNotNone(result["detection"]["windowStart"])
        self.assertEqual(result["result"], "PASS")
        self.finish(run)

    def test_d3_target_tunnels_are_per_pod_on_separate_ports(self):
        run = self.run_object(["D3"])
        run.start()
        result = run.run_scenarios()["D3"]
        self.assertIn(("narumq-1", 24880, 24883), FakeTunnel.started)
        self.assertIn(("narumq-2", 22880, 22883), FakeTunnel.started)
        self.assertEqual(set(result["targetTunnels"]), {"narumq-1", "narumq-2"})
        self.finish(run)

    def test_d2_reads_the_targets_own_api_with_container_status_and_log_evidence(self):
        run = self.run_object(["D2"])
        run.start()
        result = run.run_scenarios()["D2"]
        samples = json.loads((self.output / "d2-detection-samples.json").read_text(encoding="utf-8"))
        self.assertTrue(all(s["api"]["source"] == "target_api" for s in samples))
        self.assertIn("restartCount", samples[0]["container"])
        self.assertIn(("narumq-1", 24880, 24883), FakeTunnel.started)
        evidence = result["logEvidence"]["narumq-1"]
        self.assertEqual(evidence["outcome"], "no_container_restart_observed")
        self.assertEqual((evidence["current"]["containsNoSpace"], evidence["current"]["msg"]),
                         (True, "persist snapshot failed"))
        self.assertNotIn("error", json.dumps(evidence))  # the log line itself is never stored
        self.finish(run)

    def test_d2_reads_previous_container_log_after_a_liveness_restart(self):
        self.fixture.liveness_restart = True
        run = self.run_object(["D2"])
        run.start()
        result = run.run_scenarios()["D2"]
        evidence = result["logEvidence"]["narumq-1"]
        self.assertEqual((evidence["previous"], evidence["containsNoSpace"]), (True, True))
        self.assertTrue(any("--previous" in args for args, _ in self.fixture.commands))
        self.finish(run)

    def test_d2_keeps_sampling_by_exec_when_the_target_tunnel_dies(self):
        run = self.run_object(["D2"])
        run.start()
        killed = []

        def kill_target_tunnel():
            if run.current_step == "d2-detection" and not killed:
                killed.append(True)
                for tunnel in FakeTunnel.instances:
                    if tunnel.pod == "narumq-1" and tunnel.args[1] == 24880:
                        tunnel.dead = True
        self.clock.hooks.append(kill_target_tunnel)
        result = run.run_scenarios()["D2"]
        samples = json.loads((self.output / "d2-detection-samples.json").read_text(encoding="utf-8"))
        self.assertTrue(any(s["api"].get("errorCode") == "tunnel_down" and s["api"]["source"] == "local_exec"
                            for s in samples))
        self.assertIn("lost", [e["event"] for e in result["targetTunnel"]["events"]])
        self.assertEqual(result["result"], "PASS")
        self.finish(run)

    def test_load_has_its_own_tunnel_apart_from_the_observer_monitor_tunnel(self):
        run = self.run_object(["D2"])
        run.start()
        result = run.run_scenarios()["D2"]
        self.assertEqual(FakeTunnel.started.count(("narumq-0", 23880, 23883)), 1)
        self.assertEqual(FakeTunnel.started.count(("narumq-0", 27880, 27883)), 2)  # load tunnel + fresh cleanup tunnel
        self.assertEqual(result["load"]["tunnel"]["ports"], [27880, 27883])
        self.assertEqual(result["load"]["tunnel"]["reconnects"], 0)
        self.finish(run)

    def test_load_tunnel_reconnects_are_recorded_and_unavailable_attempts_are_samples(self):
        run = self.run_object(["D2"])
        run.start()
        killed = []

        def kill_load_tunnel():
            if run.current_step == "d2-detection" and not killed:
                killed.append(True)
                for tunnel in FakeTunnel.instances:
                    if tunnel.args[1] == 27880:
                        tunnel.dead = True
        self.clock.hooks.insert(0, kill_load_tunnel)
        result = run.run_scenarios()["D2"]
        self.assertEqual(result["load"]["tunnel"]["reconnects"], 1)
        self.assertEqual(result["load"]["tunnel"]["events"][1]["event"], "lost")
        self.assertEqual(result["result"], "PASS")
        self.finish(run)

    def test_observer_tunnel_loss_is_inconclusive_and_cleanup_still_runs(self):
        run = self.run_object(["D2"])
        run.start()
        killed = []

        def kill_observer_tunnel():
            if run.current_step == "d2-detection" and not killed:
                killed.append(True)
                for tunnel in FakeTunnel.instances:
                    if tunnel.args[1] == 23880:
                        tunnel.dead = True
        self.clock.hooks.append(kill_observer_tunnel)
        result = run.run_scenarios()["D2"]
        self.assertEqual(result["result"], "FAIL")
        self.assertEqual(result["outcome"], "inconclusive_observer_tunnel_lost")
        self.assertEqual(result["reasons"], ["inconclusive_observer_tunnel_lost"])
        self.assertEqual(result["errorStep"], "d2-detection")
        self.assertIsNotNone(result["observerTunnelLostAt"])
        self.assertLess(self.clock.now, 120)  # no 900 s recovery wait
        self.assertTrue(result["ballastRemovalAfterScenario"]["narumq-1"]["removed"])
        self.assertEqual(self.fixture.ballast["narumq-1"], 0)
        state = result["clusterStateAfterLoss"]
        self.assertEqual(state["cluster"]["leaderNodeId"], "1")
        self.assertEqual(len(state["pods"]), 3)
        self.finish(run)

    def test_retained_cleanup_runs_on_inconclusive_result_with_a_fresh_load_tunnel(self):
        run = self.run_object(["D2"])
        run.start()
        killed = []

        def kill_observer_and_load_tunnels():
            if run.current_step == "d2-detection" and not killed:
                killed.append(True)
                for tunnel in FakeTunnel.instances:
                    if tunnel.args[1] in (23880, 27880):
                        tunnel.dead = True
        self.clock.hooks.append(kill_observer_and_load_tunnels)
        result = run.run_scenarios()["D2"]
        self.assertEqual(result["outcome"], "inconclusive_observer_tunnel_lost")
        cleanup = result["retainedCleanup"]["d2"]
        self.assertTrue(cleanup["complete"])
        self.assertEqual(cleanup["topics"][0]["reasonCode"], 0)
        self.assertEqual(FakeTunnel.started.count(("narumq-0", 27880, 27883)), 2)  # load tunnel + cleanup tunnel
        self.assertNotIn("d2", run.loads)
        self.finish(run)

    def test_retained_cleanup_runs_after_failed_recovery_but_is_bounded(self):
        run = self.run_object(["D2"], selfRecoverySeconds=3)
        run.start()

        def break_pvc_after_load():
            if run.current_step == "d2-recovery":
                self.fixture.pvc_uid_suffix = "-new"
        self.clock.hooks.append(break_pvc_after_load)
        original_clear = self.runtime.QoS1Probe.clear_retained
        self.runtime.QoS1Probe.clear_retained = lambda probe: [{"topic": "t", "outcome": "puback_timeout",
                                                                 "reasonCode": None}]
        result = run.run_scenarios()["D2"]
        self.runtime.QoS1Probe.clear_retained = original_clear
        self.assertEqual(result["result"], "FAIL")
        cleanup = result["retainedCleanup"]["d2"]
        self.assertFalse(cleanup["complete"])
        self.assertGreater(cleanup["attempts"], 1)
        self.assertLessEqual(cleanup["attempts"], pressure_main.RETAINED_CLEANUP_SECONDS
                             // pressure_main.RETAINED_CLEANUP_RETRY_SECONDS + 2)
        self.assertEqual(cleanup["topics"][0]["outcome"], "puback_timeout")
        self.finish(run)

    def test_retained_cleanup_without_a_tunnel_is_recorded_not_raised(self):
        run = self.run_object(["D2"])
        run.start()
        run.start_load("d2")
        original = pressure_main.ManagedTunnel.ensure
        pressure_main.ManagedTunnel.ensure = lambda tunnel: False
        try:
            cleanup = run.cleanup_retained("d2")
        finally:
            pressure_main.ManagedTunnel.ensure = original
        self.assertEqual((cleanup["complete"], cleanup["tunnelOpened"], cleanup["topics"]), (False, False, None))
        self.assertGreaterEqual(self.clock.now, pressure_main.RETAINED_CLEANUP_SECONDS)

    def test_finish_cleans_retained_state_of_a_load_that_was_never_cleaned(self):
        run = self.run_object(["D2"])
        run.start()
        run.start_load("d2")
        report = run.finish()
        self.assertTrue(report["retainedCleanup"]["d2"]["complete"])
        self.assertEqual(run.loads, {})

    def test_cleanup_is_none_when_no_load_started(self):
        run = self.run_object(["D2"])
        self.assertIsNone(run.cleanup_retained("d2"))

    def test_later_scenario_starts_with_fresh_preflight_and_fresh_observer_tunnel(self):
        run = self.run_object(["D2", "D3"])
        run.start()
        results = run.run_scenarios()
        self.assertEqual(results["D3"]["result"], "PASS")
        self.assertEqual(FakeTunnel.started.count(("narumq-0", 23880, 23883)), 2)
        preflight = json.loads((self.output / "d3-preflight.json").read_text(encoding="utf-8"))
        self.assertEqual(preflight["failedAttempts"], [])
        self.assertFalse((self.output / "d2-preflight.json").exists())
        self.finish(run)

    def test_fresh_preflight_is_bounded_and_fails_with_a_fixed_message(self):
        run = self.run_object(["D2", "D3"])
        run.start()
        self.fixture.quorum = False
        before = self.clock.now
        with self.assertRaises(pressure_main.ScenarioError):
            run.fresh_preflight("D3")
        self.assertGreaterEqual(self.clock.now - before, pressure_main.PREFLIGHT_SETTLE_SECONDS)
        saved = json.loads((self.output / "d3-preflight.json").read_text(encoding="utf-8"))
        self.assertEqual(saved["result"], "FAIL")
        self.assertGreater(len(saved["attempts"]), 1)


class TunnelAndLogTests(RunTestBase):
    def test_managed_tunnel_reestablishes_same_uid_and_records_gap_and_failed_attempts(self):
        run = self.run_object(["D2"])
        tunnel = pressure_main.ManagedTunnel(run, "target", "narumq-1", (24880, 24883))
        tunnel.start()
        self.assertTrue(tunnel.ensure())
        FakeTunnel.instances[-1].dead = True
        self.clock.now += 2
        self.assertTrue(tunnel.ensure())
        self.assertEqual(tunnel.reconnects, 1)
        self.assertEqual(tunnel.events[-1]["initial"], False)
        self.assertEqual(FakeTunnel.instances[-1].uid, "uid-narumq-1")
        FakeTunnel.instances[-1].dead = True
        self.fixture.avail["narumq-1"] = 0  # Pod becomes NotReady: PodTunnel refuses to start
        self.clock.now += pressure_main.TUNNEL_RETRY_SECONDS
        self.assertFalse(tunnel.ensure())
        self.assertEqual(tunnel.failed_attempts, 1)
        self.assertFalse(tunnel.ensure())  # retry interval not over: no new attempt
        self.assertEqual(tunnel.failed_attempts, 1)
        self.clock.now += pressure_main.TUNNEL_RETRY_SECONDS
        self.assertFalse(tunnel.ensure())
        self.assertEqual(tunnel.failed_attempts, 2)
        self.assertEqual([e["event"] for e in tunnel.events],
                         ["established", "lost", "established", "lost", "attempt_failed"])
        self.fixture.avail["narumq-1"] = 19 * GIB
        for _ in range(4):
            self.fixture.get_pod("narumq-1")
        self.clock.now += pressure_main.TUNNEL_RETRY_SECONDS
        self.assertTrue(tunnel.ensure())
        self.assertEqual(tunnel.events[-1]["event"], "established")
        self.assertGreaterEqual(tunnel.events[-1]["gapSeconds"], 3)

    def test_scan_log_keeps_only_a_boolean_and_the_msg_field(self):
        text = ('{"level":"INFO","msg":"started"}\n'
                '{"level":"ERROR","msg":"persist snapshot failed","error":"write: no space left on device"}\n')
        self.assertEqual(logic.scan_log(text), (True, "persist snapshot failed"))
        self.assertEqual(logic.scan_log("plain: no space left on device\n"), (True, None))
        self.assertEqual(logic.scan_log('{"msg":"fine"}\n'), (False, None))

    def test_storage_failure_view_accepts_state_failed_with_pressure(self):
        node = {"nodeId": "2", "queryStatus": "OK", "state": "FAILED", "storagePressure": True,
                "replicationHealth": None, "lastErrorCode": None}
        self.assertTrue(logic.storage_failure_view(node)["visible"])
        self.assertFalse(logic.storage_failure_view({**node, "storagePressure": False})["visible"])

    def test_container_view_reports_restart_and_last_termination(self):
        pod = {"status": {"containerStatuses": [{"name": "narumq", "ready": False, "restartCount": 2,
                                                 "state": {"waiting": {}}, "lastState": {"terminated": {
                                                     "exitCode": 137, "reason": "Error", "finishedAt": "t",
                                                     "message": "dropped"}}}]}}
        view = pressure_main.container_view(pod)
        self.assertEqual(view["lastTerminated"], {"exitCode": 137, "reason": "Error", "finishedAt": "t"})
        self.assertEqual((view["restartCount"], view["state"]), (2, "waiting"))
        self.assertIsNone(pressure_main.container_view(None))


if __name__ == "__main__":
    unittest.main()
