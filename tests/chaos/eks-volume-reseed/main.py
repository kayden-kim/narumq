"""Run the EKS slot reseed acceptance (real EBS loss, bootstrap guard, new-ID reseed) on one disposable fixture.

Acceptance criteria: docs/specs/cluster-session-and-delivery.md "EKS slot 재시드 기준 (2026-10-16)", ADR-0061.
Every mutation runs once and is never retried. After a failed or unconfirmed mutation the tool reads the state
again, records it and stops. Evidence is written under --output; key material only under --private.
"""
from __future__ import annotations

import argparse
import contextlib
import json
from pathlib import Path
import shutil
import subprocess
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parent))

import reseed  # noqa: E402  (also puts tests/chaos/eks-worker-recovery on sys.path)
import mqtt_helpers  # noqa: E402
import durable_stream  # noqa: E402
from fixture import Fixture, ready, save  # noqa: E402
from observer import IndependentMonitor, MutationRejected, fence_node, release_fenced_pod, timestamp  # noqa: E402
from pod_tunnel import PodTunnel  # noqa: E402

BASELINE_ACK_WAIT_SECONDS = 30
BASELINE_MIN_ACKED = 2
EC2_TERMINATE_WAIT_SECONDS = 360
VOLUME_AVAILABLE_SECONDS = 600
VOLUME_DELETE_CONFIRM_SECONDS = 180
OLD_POD_GONE_SECONDS = 300
PVC_REPLACED_SECONDS = 300
PVC_POD_DELETE_AFTER_SECONDS = 20
GUARD_SECONDS = 600
STAGE_VISIBLE_SECONDS = 60
NEW_POD_READY_SECONDS = 900
NEW_VOTER_SECONDS = 600
OPERATION_SECONDS = 900
RECOVERED_SECONDS = 300
RETAINED_VISIBLE_SECONDS = 30


class StepFailed(RuntimeError):
    def __init__(self, step: str, code: str):
        super().__init__(f"{step}: {code}")
        self.step = step
        self.code = code


def pods_of(fixture) -> list[dict]:
    return fixture.get("pods", "-n", reseed.NAMESPACE, "-l", "app=narumq")["items"]


class Run:
    def __init__(self, fixture, plan: dict, output: Path, private: Path, sleep=time.sleep, original: Path | None = None):
        self.fixture = fixture
        self.plan = plan
        self.output = output
        self.private = private
        self.sleep = sleep
        self.evidence: dict[str, dict] = {}
        self.criteria: dict[str, bool] = {}
        self.context: dict = {}
        self.step = "init"
        self.observer = None
        self.monitor = None
        self.publisher = None
        self.probe = None
        self.spare_cordoned = False
        self.fault_requested_at = None
        self.baseline: dict = {}
        self.certificate: dict = {}
        self.private_suffix = ""
        self.original = original
        self.records: dict | None = None
        self.carried: dict = {}
        self.large: dict | None = None

    # -- plumbing -----------------------------------------------------------------------------------------

    def record(self, name: str, value: dict) -> None:
        self.evidence[name] = value
        save(self.output, f"reseed-{name}.json", value)

    def fail(self, code: str):
        raise StepFailed(self.step, code)

    def survivor_ids(self) -> list[str]:
        return self.context["survivors"]

    def nodes(self) -> dict[str, dict]:
        return reseed.node_rows(self.fixture.api("/api/v1/nodes?limit=1000", reseed.OBSERVER_PORTS[0]))

    def operation(self, operation_id: str) -> dict:
        status, envelope = self.fixture.api_request("GET", f"/api/v1/cluster/membership-operations/{operation_id}",
                                                    port=reseed.OBSERVER_PORTS[0])
        return reseed.operation_summary(envelope) if status == 200 else {"httpStatus": status}

    # -- step: local ports --------------------------------------------------------------------------------

    def step_ports(self) -> None:
        self.step = "ports"
        self.record("ports", {"at": timestamp(), "checked": reseed.check_local_ports(), "bind": "127.0.0.1"})

    # -- step: resume state -------------------------------------------------------------------------------

    def step_resume(self) -> None:
        self.step = "resume_validate"
        records = reseed.load_original_records(self.original)
        self.records = records
        reseed.check_no_stage_attempt(self.original)
        evidence, context = reseed.resume_validate(self.fixture, self.plan, records, self.private,
                                                   reseed.OBSERVER_PORTS[0])
        self.adopt_original(evidence, context)

    def step_resume_verify(self) -> None:
        """`resume --from verify`: the reseed is finished; only the verify step runs, with the Pod UID taken live."""
        self.step = "resume_validate"
        records = reseed.load_original_records(self.original)
        self.records = records
        evidence, context = reseed.resume_validate_verify(self.fixture, self.plan, records, self.original, self.output,
                                                          reseed.OBSERVER_PORTS[0])
        self.adopt_original(evidence, context)

    def step_resume_new_pod(self) -> None:
        self.step = "resume_validate"
        records = reseed.load_original_records(self.original)
        self.records = records
        evidence, context = reseed.resume_validate_new_pod(self.fixture, self.plan, records, self.original,
                                                           reseed.OBSERVER_PORTS[0])
        self.adopt_original(evidence, context)

    def step_new_pod(self) -> None:
        """No certificate, no stage POST, no ConfigMap or Secret write: the earlier resume already did them."""
        source = self.context["operationId"]
        self.catch_up({"continuedFrom": {"operationId": source, "certificateSha256": self.context["certificateSha256"],
                                         "deletedPodUID": self.context["guardPodUID"]}}, source)

    def adopt_original(self, evidence: dict, context: dict) -> None:
        """Take the baseline, carried criteria and durable journal from the original run into this resume."""
        baseline, carried = context.pop("baseline"), context.pop("carried")
        context["ca"] = ((self.private / "ca.crt").read_bytes(), (self.private / "ca.key").read_bytes())
        scenario = self.plan["durableScenario"]
        name = f"durable-publish-{scenario}.jsonl"
        source = self.original / name
        if not source.is_file():
            reseed.mismatch("durable.journal")
        entries = durable_stream.read_jsonl(source)
        counts = reseed.journal_counts(entries)
        if counts["acked"] < 1:
            reseed.mismatch("durable.journalNoAcked")
        shutil.copy2(source, self.output / name)
        if reseed.file_sha256(source) != reseed.file_sha256(self.output / name):
            reseed.mismatch("durable.journalCopy")
        evidence["durableJournal"] = {"file": name, "sha256": reseed.file_sha256(source), "entries": len(entries),
                                      **counts, "copiedTo": f"{self.output.name}/{name}",
                                      "note": "original journal and session; prepare and publisher are not rerun"}
        self.context.update(context)
        self.baseline.update(appliedIndex=baseline["appliedIndex"], markerSha256=baseline["markerSha256"])
        self.carried = carried
        self.criteria.update({criterion: item["value"] for criterion, item in carried.items()})
        if self.plan.get("largeSnapshot"):
            feature = reseed.read_record(self.original / "reseed-feature.json")
            load = reseed.read_record(self.original / "reseed-large-load.json")
            if not (feature and feature.get("confirmedActive") is True and load and load.get("writtenOk") is True
                    and isinstance(load.get("sha256"), dict)):
                reseed.mismatch("largeSnapshot.originalRecords")
            self.large = dict(load["sha256"])
            self.criteria.update({"stream.featureActive": True, "stream.largeRetainedWritten": True})
        self.record("resume-state", evidence)

    def finish_streams(self) -> dict:
        """Stop the live publisher and monitor; a resumed run has none and judges the original journal."""
        if self.publisher is not None:
            publish = self.publisher.stop()
        else:
            entries = durable_stream.read_jsonl(self.output / f"durable-publish-{self.plan['durableScenario']}.jsonl")
            publish = {"source": "journal of the interrupted run", "published": len(entries),
                       **reseed.journal_counts(entries)}
        if self.monitor is not None:
            self.monitor.stop()
        return publish

    # -- step: observer -----------------------------------------------------------------------------------

    def observer_identity(self) -> dict:
        plan, fixture = self.plan, self.fixture
        pod = fixture.get("pod", plan["observerPod"], "-n", reseed.NAMESPACE)
        node = fixture.get_node(plan["observerWorkerName"])
        if (not pod or not node or pod["metadata"]["uid"] != plan["observerPodUID"]
                or pod["metadata"].get("deletionTimestamp") or pod["metadata"].get("labels", {}).get("app") != "narumq"
                or pod["spec"].get("nodeName") != plan["observerWorkerName"] or not ready(pod)
                or reseed.instance_of(node) != plan["observerInstanceId"] or not ready(node)
                or node["metadata"].get("deletionTimestamp")
                or fixture.instance_state(plan["observerInstanceId"]) != "running"):
            raise reseed.PreflightRejected("observer_identity_not_current")
        return {key: plan[key] for key in ("observerPod", "observerPodUID", "observerWorkerName", "observerInstanceId")}

    def step_observer(self) -> None:
        self.step = "observer"
        identity = self.observer_identity()
        self.observer = PodTunnel(self.fixture, self.plan["observerPod"], self.plan["observerPodUID"],
                                  *reseed.OBSERVER_PORTS)
        endpoint = self.observer.start()
        self.observer_identity()
        self.record("observer", {**identity, **endpoint})

    # -- step: preflight ----------------------------------------------------------------------------------

    def step_preflight(self) -> None:
        self.step = "preflight"
        evidence, context = reseed.run_preflight(self.fixture, self.plan, reseed.OBSERVER_PORTS[0])
        self.context.update(context)
        ca_cert = (self.private / "ca.crt").read_bytes()
        ca_key = (self.private / "ca.key").read_bytes()
        self.context["ca"] = (ca_cert, ca_key)
        self.record("preflight", evidence)

    # -- step: baseline -----------------------------------------------------------------------------------

    def step_baseline(self) -> None:
        self.step = "baseline"
        plan, fixture = self.plan, self.fixture
        evidence = {"startedAt": timestamp()}
        target = PodTunnel(fixture, plan["targetPod"], plan["oldPodUID"], *reseed.TARGET_PORTS)
        try:
            evidence["targetTunnel"] = target.start()
            evidence["prepare"] = durable_stream.prepare_subscriber(
                self.private, plan["runId"], plan["durableScenario"], reseed.TARGET_PORTS[1], self.output)
            marker = mqtt_helpers.publish_marker(self.private, plan["runId"], plan["durableScenario"],
                                                 reseed.OBSERVER_PORTS[1])
            evidence["marker"] = marker

            def visible_on_target():
                target.check()
                seen = mqtt_helpers.read_marker(self.private, plan["runId"], reseed.TARGET_PORTS[1])
                return seen if seen["sha256"] == marker["sha256"] else None

            try:
                evidence["markerOnTarget"] = reseed.wait_until(visible_on_target, RETAINED_VISIBLE_SECONDS, 3.0,
                                                               self.sleep)
            except TimeoutError:
                self.fail("marker_not_visible_on_target")
        finally:
            target.close()
        self.baseline["markerSha256"] = marker["sha256"]
        self.probe = mqtt_helpers.SurvivorProbe(self.private, plan["runId"], reseed.OBSERVER_PORTS[1])

        def management_probe():
            self.observer.check()
            return fixture.management_probe()

        def mqtt_probe():
            self.observer.check()
            return self.probe.probe()

        self.monitor = IndependentMonitor(management_probe, mqtt_probe, self.output)
        self.monitor.start()
        if not self.monitor.wait_initial():
            self.fail("initial_probes_incomplete")
        initial = self.monitor.snapshot()
        if not (initial["management"][0]["ok"] and initial["management"][0]["data"].get("quorumAvailable") is True
                and initial["mqtt"][0]["ok"] and initial["mqtt"][0]["data"].get("puback") is True):
            self.fail("initial_probes_failed")
        interval, max_messages = durable_stream.plan_settings(plan)
        self.publisher = durable_stream.DurablePublisher(self.private, plan["runId"], plan["durableScenario"],
                                                         reseed.OBSERVER_PORTS[1], self.output, interval=interval,
                                                         max_messages=max_messages)
        self.publisher.start()

        def first_ack():
            if self.publisher.connection_lost:
                self.fail("publisher_connection_lost_before_fault")
            return sum(e["outcome"] == "acked" for e in list(self.publisher.entries)) >= BASELINE_MIN_ACKED

        try:
            reseed.wait_until(first_ack, BASELINE_ACK_WAIT_SECONDS + interval * BASELINE_MIN_ACKED, 1.0, self.sleep)
        except TimeoutError:
            self.fail("publisher_not_acked_before_fault")
        acked = sum(e["outcome"] == "acked" for e in list(self.publisher.entries))
        evidence["publishedAckedBeforeFault"] = acked
        evidence["publisher"] = {"intervalSeconds": interval, "maxMessages": max_messages,
                                 "coverageSeconds": interval * max_messages}
        rows = self.nodes()
        applied = {}
        for node_id in self.survivor_ids():
            value = rows[node_id].get("lastAppliedIndex")
            if not isinstance(value, int) or isinstance(value, bool):
                self.fail("survivor_applied_index_unavailable")
            applied[node_id] = value
        self.baseline.update(survivorLastAppliedIndex=applied, appliedIndex=max(applied.values()))
        evidence["baseline"] = {"at": timestamp(), "survivorLastAppliedIndex": applied,
                                "appliedIndexLowerBound": self.baseline["appliedIndex"],
                                "note": "management API exposes lastAppliedIndex only; it never exceeds the commit index"}
        self.record("baseline", evidence)

    # -- steps: large snapshot (R1) ---------------------------------------------------------------------------

    def step_feature(self) -> None:
        """R1: activate the snapshot-stream feature once. It cannot be rolled back; an unknown result stops the run."""
        settings = self.plan.get("largeSnapshot")
        if not settings:
            return
        self.step = "feature"
        token = settings["feature"]
        evidence: dict = {"feature": token, "startedAt": timestamp(), "confirmedActive": False}
        state = reseed.feature_state(self.fixture.api("/api/v1/cluster/features", reseed.OBSERVER_PORTS[0]))
        evidence["before"] = {"activeFeatures": state["activeFeatures"],
                              "members": [{"nodeId": m["nodeId"], "releaseVersion": m["releaseVersion"],
                                           "advertises": token in m["features"]} for m in state["members"]]}
        if not state["members"] or not all(token in m["features"] for m in state["members"]):
            self.record("feature", evidence)
            self.fail("feature_not_advertised_by_all_members")
        if token in state["activeFeatures"]:
            evidence["alreadyActive"] = True
        else:
            try:
                status, envelope, leader = self.mutate_member("POST", f"/api/v1/cluster/features/{token}", None, None)
            except Exception as exc:
                evidence["activateRequest"] = {"at": timestamp(), "transportError": type(exc).__name__}
                self.record("feature", evidence)
                self.fail("feature_activation_result_unknown")
            evidence["activateRequest"] = {"at": timestamp(), "httpStatus": status, "servedByLeader": leader,
                                           "errorCode": reseed.error_code(envelope)}
            if status not in (200, 202):
                self.record("feature", evidence)
                self.fail("feature_activation_rejected")

        def active():
            current = reseed.feature_state(self.fixture.api("/api/v1/cluster/features", reseed.OBSERVER_PORTS[0]))
            return current if token in current["activeFeatures"] else None

        try:
            after = reseed.wait_until(active, 120, 3.0, self.sleep)
        except TimeoutError:
            self.record("feature", evidence)
            self.fail("feature_not_active")
        evidence["after"] = {"activeFeatures": after["activeFeatures"]}
        evidence["confirmedActive"] = True
        self.criteria["stream.featureActive"] = True
        self.record("feature", evidence)

    def step_large_load(self) -> None:
        """R1: retained payloads above 1 MiB in total, then padding entries so a snapshot that holds them exists."""
        settings = self.plan.get("largeSnapshot")
        if not settings:
            return
        self.step = "large_load"
        evidence: dict = {"startedAt": timestamp(), "writtenOk": False, "settings": dict(settings)}
        before = self.survivor_applied()
        try:
            result = mqtt_helpers.publish_large_load(self.private, self.plan["runId"], settings, reseed.OBSERVER_PORTS[1])
        except Exception as exc:
            evidence["errorCode"] = type(exc).__name__
            self.record("large-load", evidence)
            self.fail("large_load_not_acknowledged")
        evidence.update(result)
        evidence["survivorAppliedIndex"] = {"before": before, "after": self.survivor_applied()}
        evidence["writtenOk"] = result["topics"] == settings["retainedTopics"] \
            and result["totalRetainedBytes"] > settings["minStreamBytes"] \
            and result["padPublished"] == settings["padEntries"]
        self.large = dict(result["sha256"])
        self.criteria["stream.largeRetainedWritten"] = evidence["writtenOk"]
        self.record("large-load", evidence)
        if not evidence["writtenOk"]:
            self.fail("large_load_incomplete")

    def survivor_applied(self) -> int | None:
        rows = self.nodes()
        values = [(rows.get(node_id) or {}).get("lastAppliedIndex") for node_id in self.survivor_ids()]
        ints = [v for v in values if isinstance(v, int) and not isinstance(v, bool)]
        return max(ints) if ints else None

    def broker_logs(self, pod_name: str) -> str | None:
        """Current and previous container log of one broker Pod; None when neither could be read."""
        texts = []
        for extra in ([], ["--previous"]):
            try:
                texts.append(self.fixture.command(self.fixture.kubectl + [
                    "-n", reseed.NAMESPACE, "logs", pod_name, "-c", reseed.CONTAINER, *extra], timeout=120))
            except (RuntimeError, subprocess.TimeoutExpired):
                continue
        return "\n".join(texts) if texts else None

    # Scenarios: cluster-session-and-delivery.md:CLU-SCN-145
    def verify_large(self, evidence: dict) -> None:
        """R1 judgement on the new member: retained payloads identical, and the leader log shows the stream."""
        plan, settings = self.plan, self.plan["largeSnapshot"]
        new = plan["newNodeId"]
        seen = mqtt_helpers.read_large_retained(self.private, plan["runId"], settings, reseed.TARGET_PORTS[1])
        judged = reseed.judge_large_retained(self.large or {}, seen)
        evidence["largeRetained"] = judged
        self.criteria["stream.retainedReadableOnNewMember"] = judged["result"] == "PASS"
        sender_logs = [self.broker_logs(f"narumq-{ordinal}") for ordinal, node_id in self.context["slots"].items()
                       if node_id in self.survivor_ids()]
        receiver_logs = self.broker_logs(plan["targetPod"])
        logs = reseed.judge_stream_logs(
            [r for text in sender_logs for r in reseed.parse_log_records(text)], reseed.parse_log_records(receiver_logs),
            new, self.survivor_ids(), settings["minStreamBytes"])
        logs["logsRead"] = {"senders": [text is not None for text in sender_logs], "receiver": receiver_logs is not None}
        evidence["streamLogs"] = logs
        self.criteria["stream.logCompletedOverLimit"] = logs["completedOverLimit"]
        self.criteria["stream.receiverLogged"] = logs["receiverLogged"]

    # -- step: loss ---------------------------------------------------------------------------------------

    def step_loss(self) -> None:
        self.step = "loss"
        plan, fixture = self.plan, self.fixture
        evidence = {}
        # Cordon the spare first: the StatefulSet recreates the Pod as soon as the old one is released, and the
        # attach-detach controller would otherwise re-attach the still-existing volume to the spare.
        self.spare_cordoned = True  # set first: an unconfirmed patch must still be undone at teardown
        try:
            reseed.set_unschedulable(fixture, plan["spareWorkerName"], self.context["spareNodeUID"], True)
            evidence["spareCordonCommandFailed"] = False
        except MutationRejected:
            evidence["spareCordonCommandFailed"] = True
        spare = fixture.get_node(plan["spareWorkerName"])
        if not spare or spare["spec"].get("unschedulable") is not True:
            self.record("loss", evidence)
            self.fail("spare_cordon_unconfirmed")
        evidence["spareCordoned"] = {"at": timestamp(), "nodeName": plan["spareWorkerName"]}
        self.fault_requested_at = timestamp()
        try:
            response = fixture.aws("ec2", "terminate-instances", "--instance-ids", plan["oldInstanceId"],
                                   "--skip-os-shutdown")
        except (RuntimeError, OSError, ValueError) as exc:
            evidence["terminateRequest"] = {"at": self.fault_requested_at, "commandFailed": True,
                                            "errorCode": type(exc).__name__,
                                            "instanceStateAfterFreshRead": fixture.instance_state(plan["oldInstanceId"])}
            self.record("loss", evidence)
            self.fail("terminate_request_failed")
        if response["TerminatingInstances"][0]["InstanceId"] != plan["oldInstanceId"]:
            self.fail("unexpected_termination_response")
        evidence["terminateRequest"] = {"at": self.fault_requested_at, "skipOsShutdown": True,
                                        "instanceId": plan["oldInstanceId"]}
        self.record("loss", evidence)
        fixture.command(["aws", "ec2", "wait", "instance-terminated", "--instance-ids", plan["oldInstanceId"]],
                        timeout=EC2_TERMINATE_WAIT_SECONDS)
        if fixture.instance_state(plan["oldInstanceId"]) != "terminated":
            self.fail("instance_not_terminated")
        self.criteria["loss.instanceTerminated"] = True
        evidence["terminated"] = {"at": timestamp(), "state": "terminated"}
        fencing = fence_node(fixture.get_node, fixture.patch_node, fixture.instance_state, plan["oldWorkerName"],
                             plan["oldNodeUID"], plan["oldInstanceId"])
        evidence["fencing"] = fencing
        release = release_fenced_pod(fixture.get_node, fixture.get_pod, fixture.delete_pod, fixture.instance_state,
                                     fencing, plan["oldWorkerName"], plan["oldNodeUID"], plan["oldInstanceId"],
                                     plan["targetPod"], plan["oldPodUID"])
        evidence["podRelease"] = release
        self.record("loss", evidence)

        def old_pod_gone():
            pod = fixture.get_pod(plan["targetPod"])
            return pod is None or pod["metadata"]["uid"] != plan["oldPodUID"]

        try:
            reseed.wait_until(old_pod_gone, OLD_POD_GONE_SECONDS, 3.0, self.sleep)
        except TimeoutError:
            self.fail("old_pod_not_released")

        def volume_available():
            volume = fixture.aws("ec2", "describe-volumes", "--volume-ids", plan["volumeId"])["Volumes"][0]
            return volume if volume["State"] == "available" and not volume.get("Attachments") else None

        try:
            volume = reseed.wait_until(volume_available, VOLUME_AVAILABLE_SECONDS, 5.0, self.sleep)
        except TimeoutError:
            self.fail("volume_not_available")
        evidence["volumeAvailable"] = {"at": timestamp(), "state": volume["State"]}
        try:
            fixture.command(["aws", "ec2", "delete-volume", "--volume-id", plan["volumeId"]])
            evidence["deleteVolume"] = {"at": timestamp(), "commandFailed": False}
        except (RuntimeError, OSError) as exc:
            evidence["deleteVolume"] = {"at": timestamp(), "commandFailed": True, "errorCode": type(exc).__name__}
        self.record("loss", evidence)

        def volume_deleted():
            found = fixture.aws("ec2", "describe-volumes", "--filters", f"Name=volume-id,Values={plan['volumeId']}")
            volumes = found.get("Volumes", [])
            return {"found": bool(volumes), "state": volumes[0]["State"] if volumes else None} \
                if not volumes or volumes[0]["State"] == "deleted" else None

        try:
            evidence["volumeDeleted"] = {"at": timestamp(),
                                         **reseed.wait_until(volume_deleted, VOLUME_DELETE_CONFIRM_SECONDS, 5.0,
                                                             self.sleep)}
        except TimeoutError:
            self.fail("volume_deletion_unconfirmed")
        self.criteria["loss.volumeDeleted"] = True
        pvc = fixture.get("pvc", plan["pvcName"], "-n", reseed.NAMESPACE)
        if pvc["metadata"]["uid"] != plan["pvcUID"]:
            self.fail("pvc_identity_changed")
        try:
            reseed.delete_with_uid(fixture, f"/api/v1/namespaces/{reseed.NAMESPACE}/persistentvolumeclaims/{plan['pvcName']}",
                                   plan["pvcUID"])
            evidence["deletePvc"] = {"at": timestamp(), "commandFailed": False}
        except MutationRejected:
            current = fixture.get("pvc", plan["pvcName"], "-n", reseed.NAMESPACE, "--ignore-not-found")
            applied = current is None or current["metadata"]["uid"] != plan["pvcUID"] \
                or bool(current["metadata"].get("deletionTimestamp"))
            evidence["deletePvc"] = {"at": timestamp(), "commandFailed": True, "appliedAfterFreshRead": applied}
            self.record("loss", evidence)
            if not applied:
                self.fail("pvc_delete_failed")
        self.record("loss", evidence)
        started = time.monotonic()
        deleted_pods: set[str] = set()
        observations: list[dict] = []

        def pvc_replaced():
            current = fixture.get("pvc", plan["pvcName"], "-n", reseed.NAMESPACE, "--ignore-not-found")
            pod = fixture.get_pod(plan["targetPod"])
            old_pvc_present = current is not None and current["metadata"]["uid"] == plan["pvcUID"]
            if old_pvc_present and pod and pod["metadata"]["uid"] != plan["oldPodUID"] \
                    and pod["metadata"]["uid"] not in deleted_pods \
                    and time.monotonic() - started >= PVC_POD_DELETE_AFTER_SECONDS:
                # The Pod created by the StatefulSet holds the terminating PVC; free it once.
                deleted_pods.add(pod["metadata"]["uid"])
                fixture.delete_pod(plan["targetPod"], pod["metadata"]["uid"])
                observations.append({"at": timestamp(), "deletedReplacementPodUID": pod["metadata"]["uid"]})
            if (current is not None and current["metadata"]["uid"] != plan["pvcUID"]
                    and not current["metadata"].get("deletionTimestamp") and pod is not None
                    and pod["metadata"]["uid"] != plan["oldPodUID"]):
                return {"pvcUID": current["metadata"]["uid"], "podUID": pod["metadata"]["uid"]}
            return None

        try:
            evidence["pvcReplaced"] = {"at": timestamp(), **reseed.wait_until(pvc_replaced, PVC_REPLACED_SECONDS, 3.0,
                                                                              self.sleep),
                                       "podsDeletedByTool": sorted(deleted_pods)}
        except TimeoutError:
            evidence["pvcReplaced"] = {"podsDeletedByTool": sorted(deleted_pods), "observations": observations}
            self.record("loss", evidence)
            self.fail("pvc_not_recreated")
        self.criteria["loss.pvcRecreated"] = True
        self.context["newPvcUID"] = evidence["pvcReplaced"]["pvcUID"]
        self.uncordon(evidence)
        self.record("loss", evidence)

    def uncordon(self, evidence: dict | None = None) -> None:
        if not self.spare_cordoned:
            return
        reseed.set_unschedulable(self.fixture, self.plan["spareWorkerName"], self.context["spareNodeUID"], False)
        spare = self.fixture.get_node(self.plan["spareWorkerName"])
        schedulable = bool(spare) and not spare["spec"].get("unschedulable", False)
        self.spare_cordoned = not schedulable
        if evidence is not None:
            evidence["spareUncordoned"] = {"at": timestamp(), "schedulable": schedulable}
        if not schedulable:
            self.fail("spare_uncordon_unconfirmed")

    # -- step: guard --------------------------------------------------------------------------------------

    def step_guard(self) -> None:
        self.step = "guard"
        plan, fixture = self.plan, self.fixture
        result = reseed.watch_guard(fixture, plan, GUARD_SECONDS, 3.0, self.sleep)
        concluded = timestamp()
        evidence = {"concludedAt": concluded, **result}
        window = reseed.window_judgment(self.monitor.snapshot(), self.fault_requested_at, concluded)
        evidence["survivorWindow"] = window
        cluster = fixture.api("/api/v1/cluster", reseed.OBSERVER_PORTS[0])
        pods = pods_of(fixture)
        ready_pods = sorted(p["metadata"]["name"] for p in pods if ready(p))
        evidence["atConclusion"] = {"quorumAvailable": cluster["quorumAvailable"], "nodesByState": cluster["nodesByState"],
                                    "readyPods": ready_pods}
        pvc = fixture.get("pvc", plan["pvcName"], "-n", reseed.NAMESPACE)
        evidence["freshPvc"] = {"uid": pvc["metadata"]["uid"], "differsFromLost": pvc["metadata"]["uid"] != plan["pvcUID"],
                                "mountedByPod": plan["pvcName"] in result.get("claims", [])}
        self.criteria["guard.exit1AndRefusalMessage"] = (result["verdict"] == "refused"
                                                         and evidence["freshPvc"]["differsFromLost"]
                                                         and evidence["freshPvc"]["mountedByPod"])
        self.criteria["guard.survivorSamplesAllOk"] = window["result"] == "PASS" and cluster["quorumAvailable"] is True
        self.criteria["guard.twoReadyBrokers"] = (len(ready_pods) == 2 and plan["targetPod"] not in ready_pods
                                                  and cluster["nodesByState"].get("ready") == 2)
        self.context["guardPodUID"] = result.get("podUID")
        self.record("guard", evidence)
        if result["verdict"] != "refused":
            self.fail("guard_" + result["verdict"])

    # -- step: reseed -------------------------------------------------------------------------------------

    @contextlib.contextmanager
    def leader_endpoint(self):
        """Management port of the current leader: the observer, or a tunnel to the leader Pod."""
        fixture, plan = self.fixture, self.plan
        cluster = fixture.api("/api/v1/cluster", reseed.OBSERVER_PORTS[0])
        leader = cluster.get("leaderNodeId")
        configmap = fixture.get("configmap", reseed.CONFIGMAP, "-n", reseed.NAMESPACE)
        slots = reseed.parse_slots(configmap["data"])
        ordinal = next((slot for slot, node_id in slots.items() if node_id == str(leader)), None)
        if leader is None or ordinal is None:
            self.fail("leader_not_mapped_to_slot")
        if ordinal == reseed.pod_ordinal(plan["observerPod"]):
            yield reseed.OBSERVER_PORTS[0], str(leader)
            return
        pod = fixture.get_pod(f"narumq-{ordinal}")
        if not pod or not ready(pod) or pod["metadata"].get("deletionTimestamp"):
            self.fail("leader_pod_not_ready")
        tunnel = PodTunnel(fixture, pod["metadata"]["name"], pod["metadata"]["uid"], *reseed.LEADER_PORTS)
        try:
            tunnel.start()
            yield reseed.LEADER_PORTS[0], str(leader)
        finally:
            tunnel.close()

    def mutate_member(self, method: str, path: str, body, key: str) -> tuple[int, dict | None, str]:
        with self.leader_endpoint() as (port, leader):
            status, envelope = self.fixture.api_request(method, path, body, port, key)
        return status, envelope, leader

    def read_configmap(self) -> dict:
        return self.fixture.get("configmap", reseed.CONFIGMAP, "-n", reseed.NAMESPACE)

    def step_reseed(self) -> None:
        self.step = "reseed"
        plan, fixture = self.plan, self.fixture
        ordinal, old, new = plan["slotOrdinal"], plan["targetNodeId"], plan["newNodeId"]
        evidence: dict = {}
        # a. key and certificate, kept only in the private directory
        material = reseed.generate_peer_certificate(*self.context["ca"], reseed.slot_dns(ordinal))
        self.certificate = material
        evidence["certificate"] = {"sha256": material["sha256"], "dnsNames": material["dnsNames"],
                                   "notBefore": material["notBefore"], "notAfter": material["notAfter"]}
        secret = fixture.get("secret", reseed.SECRET, "-n", reseed.NAMESPACE)
        new_secret, previous = reseed.secret_with_certificate(secret, ordinal, material["certificatePem"],
                                                              material["keyPem"])
        private_files = reseed.write_private_material(self.private, plan, material, previous, self.private_suffix)
        evidence["privateMaterial"] = {"directory": "private", "files": sorted(private_files.values()),
                                       "keyInOutputDirectory": False}
        # b. stage
        body = reseed.stage_body(plan, new, material["sha256"], self.context)
        key = reseed.idempotency_key(plan["runId"], "stage", new)
        evidence["stageRequest"] = {"at": timestamp(), "idempotencyKey": key,
                                    "body": {**body, "certificateSha256": material["sha256"]}}
        self.record("reseed", evidence)
        try:
            status, envelope, leader = self.mutate_member("POST", "/api/v1/cluster/members", body, key)
        except Exception as exc:
            evidence["stageResponse"] = {"transportError": type(exc).__name__, "freshMemberRows": sorted(self.nodes())}
            self.record("reseed", evidence)
            self.fail("stage_response_unknown")
        stage = reseed.operation_summary(envelope)
        evidence["stageResponse"] = {"at": timestamp(), "httpStatus": status, "servedByLeader": leader,
                                     "errorCode": reseed.error_code(envelope), **stage}
        evidence["stageResponse"]["disposition"] = {202: "created", 200: "replayed existing operation"}.get(status,
                                                                                                            "rejected")
        self.record("reseed", evidence)
        if status not in (200, 202) or stage.get("phase") not in ("STAGED", "LEARNER", "CATCHING_UP", "PROMOTING",
                                                                  "SUCCEEDED") or stage.get("memberId") != int(new):
            self.fail("stage_rejected")
        operation_id = stage["operationId"]

        def staged_visible():
            summary = self.operation(operation_id)
            return summary if summary.get("phase") in ("STAGED", "LEARNER", "CATCHING_UP", "PROMOTING", "SUCCEEDED") else None

        try:
            evidence["stageObserved"] = reseed.wait_until(staged_visible, STAGE_VISIBLE_SECONDS, 2.0, self.sleep)
        except TimeoutError:
            self.fail("stage_not_observed")
        # c. ConfigMap then Secret
        configmap = self.read_configmap()
        updated = reseed.configmap_with_reseed(configmap, plan, reseed.config_text(self.context["joinerConfig"]))
        self.apply(evidence, "configMapReseed", updated,
                   lambda current: current["data"].get(f"slot-{ordinal}") == new
                   and f"node-{new}.json" in current["data"], "configmap", reseed.CONFIGMAP)
        crt_value = new_secret["data"][f"node-{ordinal}.crt"]
        self.apply(evidence, "secretReseed", new_secret,
                   lambda current: current["data"].get(f"node-{ordinal}.crt") == crt_value
                   and current["data"].get(f"node-{ordinal}.key") == new_secret["data"][f"node-{ordinal}.key"],
                   "secret", reseed.SECRET)
        # d. recreate the slot Pod
        guard_uid = self.context["guardPodUID"]
        evidence["podDelete"] = {"at": timestamp(), "podUID": guard_uid}
        try:
            fixture.delete_pod(plan["targetPod"], guard_uid)
        except MutationRejected:
            evidence["podDelete"]["commandFailed"] = True
            current = fixture.get_pod(plan["targetPod"])
            evidence["podDelete"]["appliedAfterFreshRead"] = current is None or current["metadata"]["uid"] != guard_uid
            self.record("reseed", evidence)
            if not evidence["podDelete"]["appliedAfterFreshRead"]:
                self.fail("slot_pod_delete_failed")

        self.catch_up(evidence, operation_id)

    def catch_up(self, evidence: dict, operation_id: str) -> None:
        """Recreated slot Pod ready, new voter, stage SUCCEEDED, old id removed, ConfigMap cleanup.

        Also the entry point of `resume --from new-pod`; the Pod may be any UID other than the deleted guard Pod."""
        self.step = "reseed"
        plan, fixture = self.plan, self.fixture
        ordinal, old, new = plan["slotOrdinal"], plan["targetNodeId"], plan["newNodeId"]
        guard_uid = self.context["guardPodUID"]

        def new_pod_ready():
            pod = fixture.get_pod(plan["targetPod"])
            if pod and pod["metadata"]["uid"] != guard_uid and not pod["metadata"].get("deletionTimestamp") and ready(pod):
                return {"podUID": pod["metadata"]["uid"], "nodeName": pod["spec"].get("nodeName")}
            return None

        try:
            evidence["newPod"] = {"at": timestamp(), **reseed.wait_until(new_pod_ready, NEW_POD_READY_SECONDS, 5.0,
                                                                         self.sleep)}
        except TimeoutError:
            self.record("reseed", evidence)
            self.fail("new_pod_not_ready")
        self.context["newPodUID"] = evidence["newPod"]["podUID"]
        self.record("reseed", evidence)

        def new_is_voter():
            row = self.nodes().get(new)
            return row if row and row.get("membershipPhase") == "VOTER" and row.get("queryStatus") == "OK" \
                and row.get("ready") is True else None

        try:
            row = reseed.wait_until(new_is_voter, NEW_VOTER_SECONDS, 5.0, self.sleep)
        except TimeoutError:
            evidence["newMemberRow"] = self.nodes().get(new)
            self.record("reseed", evidence)
            self.fail("new_member_not_voter")
        evidence["newVoter"] = {"at": timestamp(), "nodeId": new, "membershipPhase": row["membershipPhase"],
                                "role": row.get("role"), "lastAppliedIndex": row.get("lastAppliedIndex")}
        self.criteria["reseed.newIdVoter"] = True

        def stage_succeeded():
            summary = self.operation(operation_id)
            return summary if summary.get("phase") == "SUCCEEDED" else None

        try:
            evidence["stageSucceeded"] = reseed.wait_until(stage_succeeded, OPERATION_SECONDS, 5.0, self.sleep)
        except TimeoutError:
            self.fail("stage_operation_not_succeeded")
        self.record("reseed", evidence)
        # e. remove the lost id
        removal_key = reseed.idempotency_key(plan["runId"], "remove", old)
        evidence["removeRequest"] = {"at": timestamp(), "idempotencyKey": removal_key, "memberId": old}
        try:
            status, envelope, leader = self.mutate_member("DELETE", f"/api/v1/cluster/members/{old}", None, removal_key)
        except Exception as exc:
            evidence["removeResponse"] = {"transportError": type(exc).__name__, "freshMemberRows": sorted(self.nodes())}
            self.record("reseed", evidence)
            self.fail("remove_response_unknown")
        removal = reseed.operation_summary(envelope)
        evidence["removeResponse"] = {"at": timestamp(), "httpStatus": status, "servedByLeader": leader,
                                      "errorCode": reseed.error_code(envelope), **removal}
        self.record("reseed", evidence)
        if status not in (200, 202) or removal.get("action") != "REMOVE" or removal.get("memberId") != int(old):
            self.fail("remove_rejected")
        removal_id = removal["operationId"]

        def removal_done():
            summary = self.operation(removal_id)
            row = self.nodes().get(old)
            done = summary.get("phase") == "SUCCEEDED" and row is not None and row.get("membershipPhase") == "RETIRED"
            return {"operation": summary, "oldMembershipPhase": row.get("membershipPhase")} if done else None

        try:
            evidence["removeObserved"] = {"at": timestamp(), **reseed.wait_until(removal_done, OPERATION_SECONDS, 5.0,
                                                                                self.sleep)}
        except TimeoutError:
            evidence["removeObserved"] = {"operation": self.operation(removal_id),
                                          "oldMembershipPhase": (self.nodes().get(old) or {}).get("membershipPhase")}
            self.record("reseed", evidence)
            self.fail("old_member_not_retired")
        self.criteria["reseed.oldIdRetired"] = True
        # f. drop the lost id's config
        configmap = self.read_configmap()
        self.apply(evidence, "configMapCleanup", reseed.configmap_without_old(configmap, plan),
                   lambda current: f"node-{old}.json" not in current["data"]
                   and current["data"].get(f"slot-{ordinal}") == new, "configmap", reseed.CONFIGMAP)
        self.record("reseed", evidence)

    def apply(self, evidence: dict, name: str, desired: dict, applied, kind: str, object_name: str) -> None:
        """One replace; a failed command is followed by a fresh read that decides whether it applied."""
        entry = {"at": timestamp(), "kind": kind}
        try:
            reseed.replace_object(self.fixture, desired)
            entry["commandFailed"] = False
        except MutationRejected:
            entry["commandFailed"] = True
        current = self.fixture.get(kind, object_name, "-n", reseed.NAMESPACE)
        entry["appliedAfterFreshRead"] = bool(applied(current))
        evidence[name] = entry
        self.record("reseed", evidence)
        if not entry["appliedAfterFreshRead"]:
            self.fail(f"{name}_not_applied")

    # -- step: verify -------------------------------------------------------------------------------------

    def step_verify(self) -> None:
        self.step = "verify"
        plan, fixture = self.plan, self.fixture
        old, new = plan["targetNodeId"], plan["newNodeId"]
        evidence: dict = {}

        def recovered():
            pods = pods_of(fixture)
            cluster = fixture.api("/api/v1/cluster", reseed.OBSERVER_PORTS[0])
            ok = len(pods) == 3 and all(ready(p) for p in pods) and cluster["quorumAvailable"] is True \
                and cluster["nodesByState"].get("ready") == 3
            return {"readyPods": sorted(p["metadata"]["name"] for p in pods), "quorumAvailable": True,
                    "nodesByState": cluster["nodesByState"]} if ok else None

        try:
            evidence["recovered"] = {"at": timestamp(), **reseed.wait_until(recovered, RECOVERED_SECONDS, 5.0, self.sleep)}
            self.criteria["reseed.threeReadyAndQuorum"] = True
        except TimeoutError:
            self.criteria["reseed.threeReadyAndQuorum"] = False
        rows = self.nodes()
        active = {i for i, row in rows.items() if row.get("membershipPhase") != "RETIRED"}
        expected = set(self.survivor_ids()) | {new}
        voters = sorted(i for i, row in rows.items() if row.get("membershipPhase") == "VOTER" and row.get("queryStatus") == "OK")
        evidence["members"] = {"voters": sorted(voters, key=int), "expected": sorted(expected, key=int),
                               "oldMembershipPhase": (rows.get(old) or {}).get("membershipPhase")}
        self.criteria["reseed.newIdVoter"] = set(voters) == expected and active == expected
        self.criteria["reseed.oldIdRetired"] = (rows.get(old) or {}).get("membershipPhase") == "RETIRED"
        # the lost id cannot be staged again, even with its original address and certificate
        carried = reseed.carried_restage(self.original, self.output) if self.original is not None else None
        if carried is not None:
            # an earlier verify attempt already got 409 member_id_reserved; no second POST under the same key
            evidence["restageOldId"] = carried
            self.criteria["reseed.oldIdStage409MemberIdReserved"] = True
        else:
            original = reseed.original_certificate_sha256(self.context["oldConfig"], old)
            body = reseed.stage_body(plan, old, original, self.context)
            key = reseed.idempotency_key(plan["runId"], "restage", old)
            try:
                status, envelope, leader = self.mutate_member("POST", "/api/v1/cluster/members", body, key)
                evidence["restageOldId"] = {"at": timestamp(), "httpStatus": status, "servedByLeader": leader,
                                            "errorCode": reseed.error_code(envelope)}
                self.criteria["reseed.oldIdStage409MemberIdReserved"] = status == 409 \
                    and reseed.error_code(envelope) == "member_id_reserved"
            except Exception as exc:
                evidence["restageOldId"] = {"transportError": type(exc).__name__}
                self.criteria["reseed.oldIdStage409MemberIdReserved"] = False
        applied = (rows.get(new) or {}).get("lastAppliedIndex")
        evidence["appliedIndex"] = {"newMember": applied, "baselineSurvivorLastApplied": self.baseline["appliedIndex"]}
        self.criteria["durable.appliedIndexAtLeastBaseline"] = isinstance(applied, int) and not isinstance(applied, bool) \
            and applied >= self.baseline["appliedIndex"]
        self.record("verify", evidence)
        # durable oracle and retained marker on the new member Pod
        publish = self.finish_streams()
        evidence["publisher"] = publish
        tunnel = PodTunnel(fixture, plan["targetPod"], self.context["newPodUID"], *reseed.TARGET_PORTS)
        try:
            evidence["newMemberTunnel"] = tunnel.start()
            durable = durable_stream.verify_subscriber(self.private, plan["runId"], plan["durableScenario"],
                                                       reseed.TARGET_PORTS[1], self.output, "after-reseed")
            evidence["durable"] = {key: durable.get(key) for key in (
                "result", "label", "sessionPresent", "acked", "rejected", "unknown", "missingAcked",
                "rejectedDelivered", "unknownDelivered", "duplicates", "errorCode", "timedOut")}
            self.criteria["durable.streamOracle"] = durable.get("result") == "PASS" and publish.get("acked", 0) > 0
            tunnel.check()
            marker = mqtt_helpers.read_marker(self.private, plan["runId"], reseed.TARGET_PORTS[1])
            evidence["retainedMarker"] = {"expectedSha256": self.baseline["markerSha256"], **marker}
            self.criteria["durable.retainedMarkerSha256"] = marker["sha256"] == self.baseline["markerSha256"]
            if plan.get("largeSnapshot"):
                tunnel.check()
                self.verify_large(evidence)
        finally:
            tunnel.close()
        self.record("verify", evidence)

    # -- teardown -----------------------------------------------------------------------------------------

    def cleanup(self, report: dict) -> None:
        steps = (("publisher", lambda: self.publisher and self.publisher.stop()),
                 ("monitor", lambda: self.monitor and self.monitor.stop()),
                 ("probe", lambda: self.probe and self.probe.close()),
                 ("spareUncordon", lambda: self.uncordon(None)),
                 ("observerTunnel", lambda: self.observer and self.observer.close()))
        for name, action in steps:
            try:
                action()
            except Exception as exc:
                report.setdefault("teardownErrors", {})[name] = type(exc).__name__
                report["teardownFailed"] = True
        if self.monitor is not None:
            report["monitor"] = self.monitor.summary()
            report["monitor"]["scope"] = ("whole run, informational; the guard criterion uses the "
                                          "fault-to-guard window only (reseed-guard.json)")

    # Scenarios: cluster-session-and-delivery.md:CLU-SCN-135, cluster-session-and-delivery.md:CLU-SCN-139
    def execute(self) -> None:
        steps = [self.step_ports, self.step_observer, self.step_preflight, self.step_feature, self.step_baseline,
                 self.step_large_load, self.step_loss, self.step_guard, self.step_reseed, self.step_verify]
        for step in steps:
            step()

    def execute_resume(self, from_step: str = "reseed") -> None:
        """Re-validate the interrupted run's end state, then continue at `reseed` (stage, apply, recreate Pod, ...)
        or at `new-pod` (stage already committed and applied; wait for the recreated Pod, ...). Verify is unchanged."""
        middle = {"reseed": (self.step_resume, self.step_reseed),
                  "new-pod": (self.step_resume_new_pod, self.step_new_pod),
                  "verify": (self.step_resume_verify,)}[from_step]
        for step in (self.step_ports, self.step_observer, *middle, self.step_verify):
            step()


def finish_report(report: dict, run: "Run | None") -> None:
    if run is not None:
        run.cleanup(report)
        report["steps"] = sorted(run.evidence)
    judged = reseed.aggregate(run.criteria if run else {}, report.get("failedStep"), run.plan if run else None)
    if report.get("teardownFailed"):
        judged["result"] = "FAIL"
    report.update(judged)
    report["finishedAt"] = timestamp()


def execute_run(args) -> int:
    args.output.mkdir(parents=True, exist_ok=False)
    run_id = args.output.name
    report = {"result": "FAIL", "runId": run_id, "startedAt": timestamp()}
    run = None
    try:
        reseed.check_dependencies()
        fixture = Fixture(args.private, run_id)
        plan = reseed.load_plan(json.loads(args.plan.read_text(encoding="utf-8")), run_id)
        run = Run(fixture, plan, args.output, args.private)
        run.execute()
    except reseed.PreflightRejected as exc:
        report["errorCode"] = exc.code
        report["failedStep"] = run.step if run else ("dependencies" if exc.code.startswith("dependency_missing:") else "plan")
    except StepFailed as exc:
        report["errorCode"] = exc.code
        report["failedStep"] = exc.step
    except Exception as exc:
        report["errorCode"] = type(exc).__name__
        report["failedStep"] = run.step if run else "plan"
    finally:
        finish_report(report, run)
        save(args.output, "summary.json", report)
    print(json.dumps(report))
    return 0 if report["result"] == "PASS" else 1


def execute_resume(args) -> int:
    """Continue an interrupted run at `reseed`. Original records are never written; see Run.execute_resume."""
    original = args.output
    run_id = original.name
    existing = reseed.resume_dirs(original)
    number = int(existing[-1].name.split("-", 1)[1]) + 1 if existing else 1
    out = original / f"resume-{number}"
    out.mkdir()
    before = reseed.hash_records(original, skip=out)
    report = {"result": "FAIL", "runId": run_id, "mode": "resume", "resumeNumber": number, "from": args.from_step,
              "startedAt": timestamp()}
    run = None
    try:
        reseed.check_dependencies()
        fixture = Fixture(args.private, run_id)
        plan = reseed.load_plan(json.loads(args.plan.read_text(encoding="utf-8")), run_id)
        run = Run(fixture, plan, out, args.private, original=original)
        run.private_suffix = f"-resume{number}"
        run.execute_resume(args.from_step)
    except reseed.PreflightRejected as exc:
        report["errorCode"] = exc.code
        report["failedStep"] = run.step if run else ("dependencies" if exc.code.startswith("dependency_missing:") else "plan")
    except StepFailed as exc:
        report["errorCode"] = exc.code
        report["failedStep"] = exc.step
    except Exception as exc:
        report["errorCode"] = type(exc).__name__
        report["failedStep"] = run.step if run else "plan"
    finally:
        finish_report(report, run)
        after = reseed.hash_records(original, skip=out)
        changed = sorted(name for name in set(before) | set(after) if before.get(name) != after.get(name))
        original_summary = (run.records or {}).get("summary", {}) if run else {}
        report["original"] = {"summary": "../summary.json", "summarySha256": before.get("summary.json"),
                              "result": original_summary.get("result"), "failedStep": original_summary.get("failedStep"),
                              "errorCode": original_summary.get("errorCode"), "recordsUnchanged": not changed,
                              "changedRecords": changed}
        report["carriedCriteria"] = run.carried if run else {}
        if changed:
            report["result"] = "FAIL"
        save(out, "summary.json", report)
    print(json.dumps(report))
    return 0 if report["result"] == "PASS" else 1


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="mode", required=True)
    run_parser = sub.add_parser("run", help="destroy the planned follower volume and reseed its slot")
    resume_parser = sub.add_parser("resume", help="continue an interrupted run from its reseed, new-pod or verify step")
    for command in (run_parser, resume_parser):
        command.add_argument("--private", type=Path, required=True)
        command.add_argument("--output", type=Path, required=True,
                             help="run output directory" if command is run_parser
                             else "the existing output directory of the interrupted run")
        command.add_argument("--plan", type=Path, required=True)
    run_parser.add_argument("--execute", action="store_true", help="allow termination, volume and PVC deletion")
    resume_parser.add_argument("--execute", action="store_true", help="allow the reseed mutations")
    resume_parser.add_argument("--from", dest="from_step", choices=reseed.RESUME_FROM_STEPS, required=True)
    args = parser.parse_args(argv)
    if not args.execute:
        parser.error(f"{args.mode} requires --execute")
    if args.mode == "resume":
        if not args.output.is_dir():
            parser.error("resume --output must be the existing output directory of the interrupted run")
        return execute_resume(args)
    return execute_run(args)


if __name__ == "__main__":
    raise SystemExit(main())
