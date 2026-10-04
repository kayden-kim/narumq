#!/usr/bin/env python3
"""Deterministic Paho MQTT client used only by the Phase 1 evidence harness."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import queue
import random
import socket
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import ipaddress

import paho.mqtt.client as mqtt
from paho.mqtt.properties import Properties
from paho.mqtt.packettypes import PacketTypes

SCHEMA_VERSION = 1
SDK_VERSION = "2.1.0"
READINESS_SCHEMA_VERSION = 1
READINESS_INPUT_PATH = "/input/readiness.json"

class ProfileError(ValueError):
    pass


def strict_object_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise ProfileError(f"duplicate JSON key: {key}")
        value[key] = item
    return value


def load_profile(path: str) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as stream:
        profile = json.load(stream, object_pairs_hook=strict_object_pairs)
    if not isinstance(profile, dict):
        raise ProfileError("profile root must be an object")
    allowed = {
        "schemaVersion",
        "connectTimeoutMillis",
        "operationTimeoutMillis",
        "sessionExpirySeconds",
        "willDelaySeconds",
        "reconnectWithinMillis",
        "reconnectBaseMillis",
        "reconnectCapMillis",
        "maxReconnectAttempts",
        "jitterSeed",
        "expectedCases",
    }
    unknown = sorted(set(profile) - allowed)
    if unknown:
        raise ProfileError(f"unknown profile fields: {unknown}")
    if profile.get("schemaVersion") != SCHEMA_VERSION:
        raise ProfileError("unsupported profile schemaVersion")
    cases = profile.get("expectedCases")
    if not isinstance(cases, list) or not cases:
        raise ProfileError("expectedCases must be a non-empty array")
    for item in cases:
        if not isinstance(item, dict):
            raise ProfileError("expectedCases entries must be objects")
        unknown_case = sorted(set(item) - {"caseId", "protocol", "profile", "proxy", "expectedResults"})
        if unknown_case:
            raise ProfileError(f"unknown case fields: {unknown_case}")
    return profile

def subscriber_sdk(profile: dict[str, Any], case_id: str) -> str:
    for item in profile["expectedCases"]:
        if item["caseId"] != case_id:
            continue
        for expected in item["expectedResults"]:
            if expected["role"] == "subscriber":
                return expected["sdk"]
    raise ProfileError(f"subscriber role is missing for {case_id!r}")


def write_readiness(output: str, case_id: str, protocol: str) -> None:
    target = Path(output, "ready.json")
    temporary = target.with_name(".narumq-compat-readiness.tmp")
    try:
        temporary.write_text(
            json.dumps(
                {
                    "schemaVersion": READINESS_SCHEMA_VERSION,
                    "caseId": case_id,
                    "sdk": "python",
                    "role": "subscriber",
                    "protocol": protocol,
                    "ready": True,
                },
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        temporary.replace(target)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def require_readiness(case_id: str, protocol: str, expected_sdk: str) -> None:
    try:
        with open(READINESS_INPUT_PATH, "r", encoding="utf-8") as stream:
            value = json.load(stream, object_pairs_hook=strict_object_pairs)
    except Exception as exc:
        raise RuntimeError(f"read publisher readiness input: {exc}") from exc
    if not isinstance(value, dict):
        raise RuntimeError("publisher readiness input must be an object")
    if set(value) != {"schemaVersion", "caseId", "sdk", "role", "protocol", "ready"}:
        raise RuntimeError("publisher readiness input has unexpected fields")
    if type(value["schemaVersion"]) is not int or value["schemaVersion"] != READINESS_SCHEMA_VERSION:
        raise RuntimeError("publisher readiness input has an invalid schema version")
    if type(value["caseId"]) is not str or value["caseId"] != case_id:
        raise RuntimeError("publisher readiness input has an invalid case ID")
    if type(value["sdk"]) is not str or value["sdk"] != expected_sdk:
        raise RuntimeError("publisher readiness input has an invalid subscriber SDK")
    if type(value["role"]) is not str or value["role"] != "subscriber":
        raise RuntimeError("publisher readiness input has an invalid role")
    if type(value["protocol"]) is not str or value["protocol"] != protocol:
        raise RuntimeError("publisher readiness input has an invalid protocol")
    if type(value["ready"]) is not bool or not value["ready"]:
        raise RuntimeError("publisher readiness input is not ready")


def find_case(profile: dict[str, Any], case_id: str, role: str) -> tuple[dict[str, Any], dict[str, Any]]:
    for item in profile["expectedCases"]:
        if item["caseId"] != case_id:
            continue
        for expected in item["expectedResults"]:
            if expected["role"] == role and expected["sdk"] == "python":
                return item, expected
        raise ProfileError(f"role {role!r} is not a Python role for {case_id!r}")
    raise ProfileError(f"unknown case: {case_id}")


def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def fixture(case_id: str, role: str) -> dict[str, Any]:
    case_token = hashlib.sha256(f"narumq-compatibility/{case_id}".encode()).hexdigest()
    role_token = hashlib.sha256(f"narumq-compatibility/{case_id}/{role}".encode()).hexdigest()
    payload = f"mqtt-client-compatibility/{case_token[:24]}".encode()
    return {
        "client_id": f"nq-{role_token[:12]}",
        "topic": f"compat/{case_token[:12]}/message",
        "will_topic": f"compat/{case_token[:12]}/will",
        "payload": payload,
        "hash": hashlib.sha256(payload).hexdigest(),
    }


def wait_event(event: threading.Event, timeout: float, what: str) -> None:
    if not event.wait(timeout):
        raise RuntimeError(f"timed out waiting for {what}")


def session_present(flags: Any) -> bool:
    value = getattr(flags, "session_present", None)
    if value is not None:
        return bool(value)
    if isinstance(flags, dict):
        return bool(flags.get("session present", flags.get("session_present", False)))
    return False


class ClientHarness:
    def __init__(self, address: str, client_id: str, protocol: int, clean: bool, profile: dict[str, Any], reconnect_on_failure: bool = True):
        host, port_text = address.rsplit(":", 1)
        self.host = host
        self.port = int(port_text)
        self.protocol = protocol
        self.profile = profile
        self.reconnect_on_failure = reconnect_on_failure
        self.connected = threading.Event()
        self.connect_complete = threading.Event()
        self.lost = threading.Event()
        self.messages: queue.Queue[Any] = queue.Queue()
        self.connect_flags: Any = None
        self.connect_reason: Any = None
        self.connect_error: str | None = None
        self.subscribed = threading.Event()
        self.subscribe_mid: int | None = None
        self.subscribe_acks: set[int] = set()
        self.subscribe_lock = threading.Lock()
        kwargs: dict[str, Any] = {
            "callback_api_version": mqtt.CallbackAPIVersion.VERSION2,
            "client_id": client_id,
            "protocol": protocol,
            "reconnect_on_failure": reconnect_on_failure,
        }
        if protocol in (mqtt.MQTTv31, mqtt.MQTTv311):
            kwargs["clean_session"] = clean
        self.client = mqtt.Client(**kwargs)
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_subscribe = self._on_subscribe
        self.client.on_message = self._on_message

    def _on_connect(self, _client: mqtt.Client, _userdata: Any, flags: Any, reason_code: Any, _properties: Any) -> None:
        self.connect_flags = flags
        self.connect_reason = reason_code
        try:
            failed = bool(reason_code.is_failure)
        except AttributeError:
            failed = int(reason_code) != 0
        if failed:
            self.connect_error = str(reason_code)
        else:
            self.connected.set()
        self.connect_complete.set()

    def _on_subscribe(self, _client: mqtt.Client, _userdata: Any, mid: int, _reason_codes: Any, _properties: Any) -> None:
        with self.subscribe_lock:
            self.subscribe_acks.add(mid)
            active = self.subscribe_mid == mid
        if active:
            self.subscribed.set()

    def _on_disconnect(self, _client: mqtt.Client, _userdata: Any, _disconnect_flags: Any, _reason_code: Any, _properties: Any) -> None:
        self.lost.set()

    def _on_message(self, _client: mqtt.Client, _userdata: Any, message: Any) -> None:
        self.messages.put(message)
    def connect(self, clean_start: bool | None = None, will: tuple[str, bytes, int, bool, Any] | None = None) -> None:
        if will is not None:
            topic, payload, qos, retain, properties = will
            self.client.will_set(topic, payload=payload, qos=qos, retain=retain, properties=properties)
        if self.protocol == mqtt.MQTTv5:
            properties = Properties(PacketTypes.CONNECT)
            properties.SessionExpiryInterval = int(self.profile["sessionExpirySeconds"])
            kwargs: dict[str, Any] = {"properties": properties}
            if clean_start is not None:
                kwargs["clean_start"] = clean_start
            rc = self.client.connect(self.host, self.port, keepalive=5, **kwargs)
        else:
            rc = self.client.connect(self.host, self.port, keepalive=5)
        if rc != mqtt.MQTT_ERR_SUCCESS:
            raise RuntimeError(f"CONNECT returned {rc}")
        self.client.loop_start()
        deadline = time.monotonic() + self.profile["operationTimeoutMillis"] / 1000
        while not self.connect_complete.is_set() and not self.lost.is_set():
            if time.monotonic() >= deadline:
                raise RuntimeError("timed out waiting for CONNECT")
            time.sleep(0.01)
        if self.lost.is_set() and not self.connect_complete.is_set():
            raise RuntimeError("connection closed before CONNACK")
        if self.connect_error:
            raise RuntimeError(self.connect_error)
    def observe_rejected_close(self, timeout: float) -> None:
        rc = self.client.connect(self.host, self.port, keepalive=5)
        if rc != mqtt.MQTT_ERR_SUCCESS:
            raise RuntimeError(f"CONNECT returned {rc}")
        deadline = time.monotonic() + timeout
        rejection_observed = False
        while time.monotonic() < deadline:
            if self.connected.is_set():
                raise RuntimeError("MQTT 3.1.0 CONNECT was accepted")
            if self.lost.is_set():
                return
            if self.connect_error:
                rejection_observed = True
            remaining = max(0.01, min(0.1, deadline - time.monotonic()))
            self.client.loop(timeout=remaining)
        if rejection_observed:
            raise RuntimeError("rejected CONNACK observed but connection did not close")
        raise RuntimeError("connection did not close after rejected CONNECT")

    def disconnect(self) -> None:
        try:
            self.client.disconnect()
        finally:
            self.client.loop_stop()

    def close_socket(self) -> None:
        sock = getattr(self.client, "_sock", None)
        if sock is not None:
            sock.close()


    def subscribe(self, topic: str, qos: int) -> None:
        with self.subscribe_lock:
            self.subscribed.clear()
            self.subscribe_mid = None
        result, mid = self.client.subscribe(topic, qos=qos)
        if result != mqtt.MQTT_ERR_SUCCESS:
            raise RuntimeError(f"SUBSCRIBE returned {result}")
        with self.subscribe_lock:
            self.subscribe_mid = mid
            acknowledged = mid in self.subscribe_acks
            if acknowledged:
                self.subscribe_acks.remove(mid)
        if acknowledged:
            self.subscribed.set()
        else:
            wait_event(self.subscribed, self.profile["operationTimeoutMillis"] / 1000, "SUBACK")
    def publish(self, topic: str, payload: bytes, qos: int) -> Any:
        info = self.client.publish(topic, payload=payload, qos=qos, retain=False)
        if info.rc != mqtt.MQTT_ERR_SUCCESS:
            raise RuntimeError(f"PUBLISH returned {info.rc}")
        return info

    def wait_payload(self, expected_topic: str, expected_hash: str, timeout: float) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                message = self.messages.get(timeout=max(0.01, deadline - time.monotonic()))
            except queue.Empty:
                break
            if hashlib.sha256(bytes(message.payload)).hexdigest() == expected_hash:
                if str(message.topic) != expected_topic:
                    raise RuntimeError(f"payload arrived on topic {message.topic!r}, want {expected_topic!r}")
                return
        raise RuntimeError("expected payload was not delivered")


def connect_client(address: str, client_id: str, protocol: int, clean: bool, profile: dict[str, Any], clean_start: bool | None = None) -> ClientHarness:
    client = ClientHarness(address, client_id, protocol, clean, profile)
    client.connect(clean_start=clean_start)
    return client


def basic(address: str, f: dict[str, Any], profile: dict[str, Any], protocol: int) -> dict[str, Any]:
    client = connect_client(address, f["client_id"], protocol, True, profile, clean_start=True if protocol == mqtt.MQTTv5 else None)
    try:
        client.subscribe(f["topic"], 2)
        observed = {}
        for qos in (0, 1, 2):
            info = client.publish(f["topic"], f["payload"], qos)
            if qos:
                info.wait_for_publish(profile["operationTimeoutMillis"] / 1000)
            client.wait_payload(f["topic"], f["hash"], profile["operationTimeoutMillis"] / 1000)
            observed[f"qos{qos}RoundTrip"] = True
        observed["disconnectReason"] = "normal"
        return observed
    finally:
        client.disconnect()


def persistent(address: str, f: dict[str, Any], profile: dict[str, Any], protocol: int) -> dict[str, Any]:
    first = connect_client(address, f["client_id"], protocol, False, profile, clean_start=True if protocol == mqtt.MQTTv5 else None)
    try:
        first.subscribe(f["topic"], 1)
    finally:
        first.disconnect()
    publisher = connect_client(address, f["client_id"] + "-p", protocol, True, profile, clean_start=True if protocol == mqtt.MQTTv5 else None)
    try:
        info = publisher.publish(f["topic"], f["payload"], 1)
        info.wait_for_publish(profile["operationTimeoutMillis"] / 1000)
    finally:
        publisher.disconnect()
    resumed = connect_client(address, f["client_id"], protocol, False, profile, clean_start=False if protocol == mqtt.MQTTv5 else None)
    try:
        present = session_present(resumed.connect_flags)
        resumed.wait_payload(f["topic"], f["hash"], profile["operationTimeoutMillis"] / 1000)
        return {"sessionPresent": present, "offlineDelivery": True}
    finally:
        resumed.disconnect()


def jitter(profile: dict[str, Any], attempt: int, rng: random.Random) -> dict[str, Any]:
    upper = profile["reconnectBaseMillis"]
    for _ in range(attempt):
        upper = min(profile["reconnectCapMillis"], upper * 2)
    return {"attempt": attempt, "upper": upper, "chosen": rng.randint(0, upper)}


def proxy_observations(expected_id: int, qos: int) -> tuple[bool, bool]:
    trace_path = Path("/proxy-output/proxy-trace.jsonl")
    deadline = time.monotonic() + 2
    events: list[dict[str, Any]] = []
    publishes: list[dict[str, Any]] = []
    while time.monotonic() < deadline:
        if trace_path.exists():
            try:
                events = [json.loads(line) for line in trace_path.read_text(encoding="utf-8").splitlines() if line.strip()]
            except (OSError, json.JSONDecodeError):
                events = []
            publishes = [
                event.get("metadata", {})
                for event in events
                if event.get("direction") == "client_to_broker"
                and event.get("metadata", {}).get("packetType") == 3
                and event.get("metadata", {}).get("qos") == qos
            ]
            duplicate = any(item.get("dup") for item in publishes[1:])
            same_id = bool(publishes) and all(item.get("packetId") == publishes[0].get("packetId") for item in publishes)
            final_type = 4 if qos == 1 else 7
            final_ack = any(
                event.get("direction") == "broker_to_client"
                and event.get("metadata", {}).get("packetType") == final_type
                for event in events
            )
            if publishes and duplicate and same_id and final_ack:
                break
        time.sleep(0.05)
    if not publishes:
        raise RuntimeError("proxy trace has no matching PUBLISH")
    if expected_id and publishes[0].get("packetId") != expected_id:
        raise RuntimeError("proxy trace packet identifier mismatch")
    duplicate = any(item.get("dup") for item in publishes[1:])
    same_id = all(item.get("packetId") == publishes[0].get("packetId") for item in publishes)
    if not duplicate or not same_id:
        raise RuntimeError("proxy trace has no complete DUP retransmission proof")
    return duplicate, same_id


def retransmit(address: str, f: dict[str, Any], profile: dict[str, Any], protocol: int, qos: int) -> dict[str, Any]:
    observer = connect_client(address, f["client_id"] + "-o", protocol, True, profile, clean_start=True if protocol == mqtt.MQTTv5 else None)
    observer.subscribe(f["topic"], qos)
    publisher = connect_client(address, f["client_id"], protocol, False, profile, clean_start=True if protocol == mqtt.MQTTv5 else None)
    try:
        info = publisher.publish(f["topic"], f["payload"], qos)
        time.sleep(0.25)
        publisher.close_socket()
        wait_event(publisher.lost, profile["operationTimeoutMillis"] / 1000, "connection loss")
        publisher.client.loop_stop()
        rng = random.Random(profile["jitterSeed"])
        selected = None
        for attempt in range(profile["maxReconnectAttempts"]):
            selected = jitter(profile, attempt, rng)
            time.sleep(selected["chosen"] / 1000)
            publisher.connected.clear()
            publisher.lost.clear()
            try:
                rc = publisher.client.reconnect()
            except Exception:
                continue
            if rc != mqtt.MQTT_ERR_SUCCESS:
                continue
            publisher.client.loop_start()
            try:
                wait_event(publisher.connected, profile["operationTimeoutMillis"] / 1000, "reconnect")
            except RuntimeError:
                publisher.client.loop_stop()
                continue
            break
        if selected is None or not publisher.connected.is_set():
            raise RuntimeError("reconnect attempts exhausted")
        info.wait_for_publish(profile["operationTimeoutMillis"] / 1000)
        if not info.is_published():
            raise RuntimeError("retransmitted PUBLISH was not acknowledged")
        observer.wait_payload(f["topic"], f["hash"], profile["operationTimeoutMillis"] / 1000)
        duplicate, same_id = proxy_observations(info.mid, qos)
        return {
            "samePacketIdentifier": same_id,
            "dupObserved": duplicate,
            "completion": True,
            "subscriberDeliveries": 1,
            "reconnectAttempt": selected["attempt"] + 1,
        }
    finally:
        publisher.disconnect()
        observer.disconnect()


def abrupt_will(address: str, f: dict[str, Any], profile: dict[str, Any], protocol: int) -> dict[str, Any]:
    observer = connect_client(address, f["client_id"] + "-o", protocol, True, profile, clean_start=True if protocol == mqtt.MQTTv5 else None)
    observer.subscribe(f["will_topic"], 0)
    publisher = ClientHarness(address, f["client_id"], protocol, True, profile)
    publisher.connect(clean_start=True if protocol == mqtt.MQTTv5 else None, will=(f["will_topic"], f["payload"], 0, False, None))
    try:
        publisher.close_socket()
        publisher.client.loop_stop()
        observer.wait_payload(f["will_topic"], f["hash"], profile["operationTimeoutMillis"] / 1000)
    finally:
        try:
            publisher.client.loop_stop()
        finally:
            observer.disconnect()
    return {"willReceived": True, "willClassification": "expected_mqtt311_server_disconnect"}


def will_delay(address: str, f: dict[str, Any], profile: dict[str, Any]) -> dict[str, Any]:
    observer = connect_client(address, f["client_id"] + "-o", mqtt.MQTTv5, True, profile, clean_start=True)
    observer.subscribe(f["will_topic"], 0)
    will_properties = Properties(PacketTypes.WILLMESSAGE)
    will_properties.WillDelayInterval = int(profile["willDelaySeconds"])
    publisher = ClientHarness(address, f["client_id"], mqtt.MQTTv5, False, profile)
    publisher.connect(clean_start=True, will=(f["will_topic"], f["payload"], 0, False, will_properties))
    publisher.close_socket()
    publisher.client.loop_stop()
    time.sleep(1)
    resumed = connect_client(address, f["client_id"], mqtt.MQTTv5, False, profile, clean_start=False)
    try:
        present = session_present(resumed.connect_flags)
        time.sleep(profile["willDelaySeconds"] + 0.5)
        if not observer.messages.empty():
            raise RuntimeError("delayed Will was delivered after session resume")
        return {"willReceived": False, "sessionPresent": present, "willDelaySeconds": profile["willDelaySeconds"]}
    finally:
        resumed.disconnect()
        observer.disconnect()


def rejected(address: str, f: dict[str, Any], profile: dict[str, Any]) -> dict[str, Any]:
    client = ClientHarness(address, f["client_id"], mqtt.MQTTv31, True, profile, reconnect_on_failure=False)
    try:
        client.observe_rejected_close(profile["operationTimeoutMillis"] / 1000)
        return {"connackAccepted": False, "connectionClosed": True}
    finally:
        try:
            client.client.loop_stop()
        except Exception:
            pass

def cross(address: str, case_id: str, protocol_name: str, f: dict[str, Any], profile: dict[str, Any], protocol: int, role: str, output: str, expected_subscriber_sdk: str) -> dict[str, Any]:
    client = connect_client(address, f["client_id"], protocol, True, profile, clean_start=True if protocol == mqtt.MQTTv5 else None)
    try:
        if role == "subscriber":
            client.subscribe(f["topic"], 1)
            write_readiness(output, case_id, protocol_name)
            client.wait_payload(f["topic"], f["hash"], profile["operationTimeoutMillis"] / 1000)
            return {"readinessSignaled": True, "topic": f["topic"], "payloadHash": f["hash"], "qos": 1, "deliveryCompleted": True}
        require_readiness(case_id, protocol_name, expected_subscriber_sdk)
        info = client.publish(f["topic"], f["payload"], 1)
        info.wait_for_publish(profile["operationTimeoutMillis"] / 1000)
        if not info.is_published():
            raise RuntimeError("cross-SDK PUBLISH was not acknowledged")
        return {"topic": f["topic"], "payloadHash": f["hash"], "qos": 1, "ackCompleted": True}
    finally:
        client.disconnect()

def execute(case: dict[str, Any], role: dict[str, Any], f: dict[str, Any], profile: dict[str, Any], address: str, output: str) -> dict[str, Any]:
    case_id = case["caseId"]
    if role["role"] in ("publisher", "subscriber"):
        return cross(address, case_id, case["protocol"], f, profile, mqtt.MQTTv5 if case["protocol"] == "mqtt5" else mqtt.MQTTv311, role["role"], output, subscriber_sdk(profile, case_id))
    if case_id == "python-mqtt31-rejected":
        return rejected(address, f, profile)
    if case_id.endswith("basic-roundtrip"):
        return basic(address, f, profile, mqtt.MQTTv5 if case["protocol"] == "mqtt5" else mqtt.MQTTv311)
    if case_id.endswith("persistent-resume"):
        return persistent(address, f, profile, mqtt.MQTTv5 if case["protocol"] == "mqtt5" else mqtt.MQTTv311)
    if case_id.endswith("qos1-retransmit"):
        return retransmit(address, f, profile, mqtt.MQTTv5 if case["protocol"] == "mqtt5" else mqtt.MQTTv311, 1)
    if case_id.endswith("qos2-retransmit"):
        return retransmit(address, f, profile, mqtt.MQTTv5 if case["protocol"] == "mqtt5" else mqtt.MQTTv311, 2)
    if case_id.endswith("abrupt-will"):
        return abrupt_will(address, f, profile, mqtt.MQTTv311)
    if case_id.endswith("will-delay-resume"):
        return will_delay(address, f, profile)
    raise RuntimeError(f"unsupported case {case_id}")


def write_result(output: str, value: dict[str, Any]) -> None:
    path = Path(output) / "result.json"
    if path.exists():
        raise RuntimeError("result.json already exists")
    path.write_text(json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def validServerReference(ref: Any) -> bool:
    if not isinstance(ref, str) or any(ch.isspace() or ch in ",/?#@[]" for ch in ref):
        return False
    if ":" not in ref:
        return False
    host, port = ref.rsplit(":", 1)
    if not port or any(ch not in "0123456789" for ch in port):
        return False
    if not 1 <= int(port) <= 65535 or len(host) > 253:
        return False
    try:
        ipaddress.ip_address(host)
        return False
    except ValueError:
        pass
    for label in host.split("."):
        if not 1 <= len(label) <= 63 or label.startswith("-") or label.endswith("-"):
            return False
        if any(ch not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-" for ch in label):
            return False
    return True


def load_drain_profile(path: str) -> dict[str, Any]:
    with open(path, encoding="utf-8") as stream:
        p = json.load(stream, object_pairs_hook=strict_object_pairs)
    timing = {
        "schemaVersion": 1, "connectTimeoutMillis": 10000, "operationTimeoutMillis": 45000,
        "sessionExpirySeconds": 60, "willDelaySeconds": 5, "reconnectWithinMillis": 2000,
        "reconnectBaseMillis": 100, "reconnectCapMillis": 2000, "maxReconnectAttempts": 5,
        "jitterSeed": 20260921,
    }
    if not isinstance(p, dict) or set(p) != set(timing) | {"cluster", "fakeServer", "clientIds", "expectedCases"}:
        raise ProfileError("invalid drain profile keys")
    for key, value in timing.items():
        if type(p[key]) is not int or p[key] != value:
            raise ProfileError(f"invalid drain timing {key}")
    nested = {
        "cluster": {"commonEndpoint", "serverReference", "observerEndpoint", "drainNodeId",
                    "connectionRatePerSecond", "loadBalancerPropagationDelayMillis", "drainTimeoutMillis"},
        "fakeServer": {"endpoint", "trapEndpoint"},
        "clientIds": {"client", "observer", "publisher"},
    }
    for key, keys in nested.items():
        if not isinstance(p[key], dict) or set(p[key]) != keys:
            raise ProfileError(f"invalid drain {key} keys")
    if not isinstance(p["expectedCases"], list):
        raise ProfileError("invalid drain case array")
    for case in p["expectedCases"]:
        if set(case) != {"caseId", "protocol", "profile", "kind", "vector", "expectedResults"}:
            raise ProfileError("invalid drain case keys")
        for role in case["expectedResults"]:
            if set(role) != {"sdk", "role", "requiredObservations"}:
                raise ProfileError("invalid drain result keys")
    return p


class DrainSafeClient:
    """Owns the manual SDK loop and preserves the first disconnect classification."""

    def __init__(self, endpoint: str, client_id: str, profile: dict[str, Any],
                 credentials: dict[str, str], will: tuple[str, bytes] | None = None):
        self.permanent_endpoint = endpoint
        self.client_id = client_id
        self.profile = profile
        self.credentials = credentials
        self.will = will
        self.stop = threading.Event()
        self.up: queue.Queue[dict[str, Any]] = queue.Queue()
        self.messages: queue.Queue[Any] = queue.Queue()
        self.disconnected = threading.Event()
        self.decision: dict[str, Any] | None = None
        self.failure: Exception | None = None
        self.client: mqtt.Client | None = None
        self.current_down = threading.Event()
        self.ack_lock = threading.Lock()
        self.subscribe_acks: dict[int, Any] = {}
        self.random = random.Random(profile["jitterSeed"])
        self.thread = threading.Thread(target=self._run, name=f"drain-{client_id}")
        self.thread.start()

    def _classify(self, surface: str, reason: int | None = None, reference: Any = None,
                  error: str | None = None) -> None:
        if self.decision is None:
            self.decision = {
                "disconnectSurface": surface,
                "serverReferenceAccepted": surface == "server_disconnect" and reason == 156 and validServerReference(reference),
                "serverReference": reference,
                "at": time.monotonic(),
            }
            if reason is not None:
                self.decision["disconnectReasonCode"] = reason
            if error is not None:
                self.decision["sdkError"] = error
            self.disconnected.set()

    def _on_disconnect(self, _client: mqtt.Client, _userdata: Any, flags: Any,
                       reason: Any, properties: Any) -> None:
        if not self.stop.is_set():
            if flags.is_disconnect_packet_from_server:
                self._classify("server_disconnect", reason.value, getattr(properties, "ServerReference", None))
            else:
                self._classify("connection_lost", reason.value)
        self.current_down.set()

    def _on_subscribe(self, _client: mqtt.Client, _userdata: Any, mid: int, reasons: Any, _properties: Any) -> None:
        with self.ack_lock:
            self.subscribe_acks[mid] = reasons

    def _connect(self, target: str, initial: bool) -> None:
        self.current_down.clear()
        connected = threading.Event()
        error: list[str] = []
        host, port = target.rsplit(":", 1)
        client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                             client_id=self.client_id, protocol=mqtt.MQTTv5, reconnect_on_failure=False)
        self.client = client
        client.username_pw_set(self.credentials["username"], self.credentials["password"])
        if self.will is not None:
            properties = Properties(PacketTypes.WILLMESSAGE)
            properties.WillDelayInterval = self.profile["willDelaySeconds"]
            client.will_set(self.will[0], self.will[1], qos=0, properties=properties)
        def on_connect(_client: mqtt.Client, _userdata: Any, flags: Any, reason: Any, _properties: Any) -> None:
            if reason.is_failure:
                error.append(str(reason))
            else:
                self.up.put({"endpoint": target, "sessionPresent": session_present(flags), "at": time.monotonic()})
            connected.set()
        client.on_connect = on_connect
        client.on_disconnect = self._on_disconnect
        client.on_subscribe = self._on_subscribe
        client.on_message = lambda _client, _userdata, message: self.messages.put(message)
        properties = Properties(PacketTypes.CONNECT)
        properties.SessionExpiryInterval = self.profile["sessionExpirySeconds"]
        client.connect_timeout = self.profile["connectTimeoutMillis"] / 1000
        rc = client.connect(host, int(port), keepalive=5, clean_start=initial, properties=properties)
        if rc != mqtt.MQTT_ERR_SUCCESS:
            raise RuntimeError(f"CONNECT returned {rc}")
        deadline = time.monotonic() + self.profile["connectTimeoutMillis"] / 1000
        while not connected.is_set() and not self.stop.is_set():
            if time.monotonic() >= deadline or self.current_down.is_set():
                raise RuntimeError("connection closed or timed out before CONNACK")
            rc = client.loop(timeout=0.1)
            if rc != mqtt.MQTT_ERR_SUCCESS:
                raise RuntimeError(f"CONNACK loop returned {rc}; rejection={error}")
        if error:
            raise RuntimeError(error[0])

    def _close_socket(self) -> None:
        if self.client is not None:
            sock = self.client.socket()
            if sock is not None:
                sock.close()

    def _run(self) -> None:
        try:
            self._connect(self.permanent_endpoint, True)
            attempts = 0
            while not self.stop.is_set():
                try:
                    rc = self.client.loop(timeout=0.1)
                    if rc != mqtt.MQTT_ERR_SUCCESS or self.current_down.is_set():
                        self._classify("connection_lost")
                    else:
                        continue
                except Exception as exc:
                    self._classify("sdk_exception", error=repr(exc))
                self._close_socket()
                while not self.stop.is_set():
                    if attempts >= self.profile["maxReconnectAttempts"]:
                        raise RuntimeError("maximum reconnect attempts exceeded")
                    upper = min(self.profile["reconnectCapMillis"], self.profile["reconnectBaseMillis"] * 2 ** attempts)
                    if self.stop.wait(self.random.uniform(0, upper) / 1000):
                        return
                    target = self.permanent_endpoint
                    if attempts == 0 and self.decision and self.decision["serverReferenceAccepted"]:
                        target = self.decision["serverReference"]
                    attempts += 1
                    try:
                        self._connect(target, False)
                        break
                    except Exception:
                        self._close_socket()
        except Exception as exc:
            self.failure = exc
        finally:
            if self.stop.is_set() and self.client is not None:
                try:
                    self.client.disconnect()
                    self.client.loop_write()
                except Exception:
                    pass
            self._close_socket()

    def await_up(self, deadline: float) -> dict[str, Any]:
        while time.monotonic() < deadline:
            try:
                return self.up.get(timeout=0.05)
            except queue.Empty:
                if self.failure is not None:
                    raise self.failure
        raise RuntimeError("timed out waiting for drain CONNACK")

    def subscribe(self, topic: str, qos: int, deadline: float) -> None:
        rc, mid = self.client.subscribe(topic, qos)
        if rc != mqtt.MQTT_ERR_SUCCESS:
            raise RuntimeError(f"SUBSCRIBE returned {rc}")
        while time.monotonic() < deadline:
            with self.ack_lock:
                reasons = self.subscribe_acks.get(mid)
            if reasons is not None:
                if any(reason.is_failure for reason in reasons):
                    raise RuntimeError(f"SUBACK rejected: {reasons}")
                return
            if self.failure is not None:
                raise self.failure
            time.sleep(0.01)
        raise RuntimeError("timed out waiting for drain SUBACK")

    def close(self) -> None:
        self.stop.set()
        self.thread.join(timeout=self.profile["connectTimeoutMillis"] / 1000 + 1)
        if self.thread.is_alive():
            self._close_socket()
            self.thread.join(timeout=2)
        if self.thread.is_alive():
            raise RuntimeError("drain SDK loop did not terminate")


def execute_drain(args: Any, case: dict[str, Any], p: dict[str, Any],
                  credentials: dict[str, str], observations: dict[str, Any]) -> None:
    deadline = time.monotonic() + p["operationTimeoutMillis"] / 1000
    clients: list[DrainSafeClient] = []
    topic = f"compat/drain/{args.case}/message"
    will_topic = f"compat/drain/{args.case}/will"
    payload = f"drain-message/{args.case}".encode()
    cluster = case["kind"] == "cluster_drain"
    observations["permanentEndpoint"] = args.address
    try:
        if cluster:
            observer = DrainSafeClient(p["cluster"]["observerEndpoint"], p["clientIds"]["observer"], p, credentials)
            clients.append(observer)
            observer.await_up(deadline)
            observer.subscribe(will_topic, 0, deadline)
        client = DrainSafeClient(args.address, p["clientIds"]["client"], p, credentials,
                                 (will_topic, f"drain-will/{args.case}".encode()) if cluster else None)
        clients.append(client)
        client.await_up(deadline)
        if cluster:
            client.subscribe(topic, 1, deadline)
            ready = {"schemaVersion": 1, "caseId": args.case, "sdk": "python", "role": "client", "protocol": "mqtt5", "ready": True}
            target = Path(args.output, "drain-ready.json")
            temporary = target.with_suffix(".tmp")
            temporary.write_text(json.dumps(ready) + "\n", encoding="utf-8")
            temporary.replace(target)
        while not client.disconnected.wait(0.05):
            if client.failure is not None:
                raise client.failure
            if time.monotonic() >= deadline:
                raise RuntimeError("timed out waiting for drain disconnect")
        decision = client.decision
        observations.update({key: value for key, value in decision.items() if key != "at" and value is not None})
        if not cluster:
            observations["vector"] = case["vector"]
        ack = client.await_up(deadline)
        observations["reconnectEndpoint"] = ack["endpoint"]
        if not cluster:
            time.sleep(0.5)
            return
        observations["reconnectElapsedMillis"] = (ack["at"] - decision["at"]) * 1000
        observations["sessionPresent"] = ack["sessionPresent"]
        publisher = DrainSafeClient(p["cluster"]["observerEndpoint"], p["clientIds"]["publisher"], p, credentials)
        clients.append(publisher)
        publisher.await_up(deadline)
        published = publisher.client.publish(topic, payload, qos=1)
        published.wait_for_publish(timeout=max(0.01, deadline - time.monotonic()))
        if not published.is_published():
            raise RuntimeError("drain publish ACK missing")
        while time.monotonic() < deadline:
            try:
                message = client.messages.get(timeout=0.05)
                if message.topic == topic and message.payload == payload:
                    observations["postRedirectDelivery"] = True
                    break
            except queue.Empty:
                pass
        else:
            raise RuntimeError("post-redirect payload missing")
        will_deadline = decision["at"] + p["willDelaySeconds"] + 1
        observations["willReceived"] = False
        while True:
            remaining = will_deadline - time.monotonic()
            try:
                message = observer.messages.get(timeout=max(0, remaining))
                if message.topic == will_topic:
                    observations["willReceived"] = True
                    raise RuntimeError("unexpected drain Will")
            except queue.Empty:
                break
        observations["willClassification"] = "suppressed_reconnected_within_delay"
    finally:
        for client in reversed(clients):
            client.close()


def drain_main(args: Any) -> int:
    started = now()
    value = {"schemaVersion": 1, "caseId": args.case, "sdk": "python", "sdkVersion": SDK_VERSION,
             "protocol": "mqtt5", "profile": "MQTT5_DRAIN_SAFE", "role": args.role,
             "status": "failed", "startedAt": started, "finishedAt": started, "observations": {}}
    try:
        p = load_drain_profile(args.profile)
        case, _expected = find_case(p, args.case, args.role)
        with open("/input/credentials.json", encoding="utf-8") as stream:
            credentials = json.load(stream, object_pairs_hook=strict_object_pairs)
        if set(credentials) != {"username", "password"} or any(type(v) is not str for v in credentials.values()):
            raise ProfileError("invalid drain credentials")
        execute_drain(args, case, p, credentials, value["observations"])
        value["status"] = "passed"
    except Exception as exc:
        value["error"] = str(exc)
    value["finishedAt"] = now()
    try:
        write_result(args.output, value)
    except Exception as exc:
        print(f"unable to write result: {exc}", file=sys.stderr)
        return 1
    return 0 if value["status"] == "passed" else 1


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument("--case", required=True)
    parser.add_argument("--role", required=True)
    parser.add_argument("--address", required=True)
    parser.add_argument("--profile", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--suite", required=True, choices=["phase1", "drain"])
    args = parser.parse_args(argv)
    if args.suite == "drain":
        return drain_main(args)
    started = now()
    profile: dict[str, Any] | None = None
    case: dict[str, Any] | None = None
    expected: dict[str, Any] | None = None
    value: dict[str, Any] = {
        "schemaVersion": SCHEMA_VERSION,
        "caseId": args.case,
        "sdk": "python",
        "sdkVersion": SDK_VERSION,
        "protocol": "",
        "profile": "",
        "role": args.role,
        "status": "failed",
        "startedAt": started,
        "finishedAt": started,
        "observations": {},
    }
    try:
        if Path(args.output, "result.json").exists():
            raise RuntimeError("result.json already exists")
        profile = load_profile(args.profile)
        case, expected = find_case(profile, args.case, args.role)
        value["protocol"] = case["protocol"]
        value["profile"] = case["profile"]
        value["observations"] = execute(case, expected, fixture(args.case, args.role), profile, args.address, args.output)
        value["status"] = "passed"
    except Exception as exc:
        value["error"] = str(exc)
    value["finishedAt"] = now()
    try:
        write_result(args.output, value)
    except Exception as exc:
        print(f"unable to write result: {exc}", file=sys.stderr)
        return 1
    return 0 if value["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
