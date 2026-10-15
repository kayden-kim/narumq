"""Synthetic persistent-session, offline QoS 1 and retained payload oracle.

`prepare` runs before a fault. It creates a persistent MQTT 5 subscription on the
target Pod, disconnects it, then publishes one retained marker and three offline
QoS 1 messages through a surviving Pod. `verify` runs after recovery against the
recovered target Pod without publishing again. Results store payload hashes only.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import threading
import time
from pathlib import Path

import paho.mqtt.client as mqtt
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

SUBSCRIBER_ID = "ebs-worker-subscriber"
PUBLISHER_ID = "ebs-worker-publisher"
READER_ID = "ebs-worker-reader"
USERNAME = "acceptance-device"


def digest(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


class Oracle:
    def __init__(self, run_id: str, private: Path, output: Path):
        self.run_id = run_id
        self.output = output
        self.password = (private / "mqtt.password").read_text(encoding="utf-8").strip()
        base = "acceptance/" + run_id
        self.queue_topic = base + "/queue"
        self.retained_topic = base + "/retained"
        self.queued = [(run_id + ":offline-qos1:" + str(i)).encode() for i in range(3)]
        self.retained = (run_id + ":retained-marker").encode()

    def save(self, name: str, value: dict) -> None:
        self.output.mkdir(parents=True, exist_ok=True)
        (self.output / name).write_text(json.dumps(value, indent=2), encoding="utf-8")

    def client(self, identity: str, port: int, clean: bool = True, expiry: int = 0) -> "Client":
        return Client(identity, self.password, port, clean, expiry)

    def check_retained(self, port: int) -> list[dict]:
        reader = self.client(READER_ID, port)
        try:
            reader.subscribe(self.retained_topic)
            if not reader.received.wait(15):
                raise AssertionError("retained payload timeout")
            found = [m for m in reader.messages if m["topic"] == self.retained_topic]
            if not found or not all(m["sha256"] == digest(self.retained) and m["retain"] and m["qos"] == 1
                                    for m in found):
                raise AssertionError(found)
            return found
        finally:
            reader.close()

    def prepare(self, target_port: int, publisher_port: int) -> None:
        subscriber = self.client(SUBSCRIBER_ID, target_port, clean=True, expiry=3600)
        subscriber.subscribe(self.queue_topic)
        subscriber.close()
        publisher = self.client(PUBLISHER_ID, publisher_port)
        try:
            publisher.publish(self.retained_topic, self.retained, retain=True)
            for value in self.queued:
                publisher.publish(self.queue_topic, value)
            if len(publisher.acks) != 4 or any(a["reasonValue"] != 0 for a in publisher.acks):
                raise AssertionError(publisher.acks)
            pubacks = publisher.acks
        finally:
            publisher.close()
        self.save("mqtt-before-fault.json", {
            "result": "PASS", "runId": self.run_id, "pubacks": pubacks,
            "retained": self.check_retained(target_port), "offlineSessionClientId": SUBSCRIBER_ID,
            "offlinePayloadHashes": [digest(p) for p in self.queued], "retainedHash": digest(self.retained),
            "sessionExpirySeconds": 3600})

    def verify(self, target_port: int) -> None:
        subscriber = self.client(SUBSCRIBER_ID, target_port, clean=False, expiry=3600)
        try:
            if not subscriber.session_present:
                raise AssertionError("persistent session not resumed")
            expected = {digest(p) for p in self.queued}
            deadline = time.monotonic() + 20
            while time.monotonic() < deadline and {m["sha256"] for m in subscriber.messages} != expected:
                time.sleep(0.1)
            if {m["sha256"] for m in subscriber.messages} != expected:
                raise AssertionError(subscriber.messages)
            if not all(m["topic"] == self.queue_topic and m["qos"] == 1 for m in subscriber.messages):
                raise AssertionError(subscriber.messages)
            messages = list(subscriber.messages)
        finally:
            subscriber.close()
        self.save("mqtt-after-recovery.json", {
            "result": "PASS", "runId": self.run_id, "sessionPresent": True, "offlineMessages": messages,
            "allExpectedOfflineHashesReceived": True, "qos1DuplicateCount": len(messages) - len(self.queued),
            "retained": self.check_retained(target_port), "noRepublishDuringRecovery": True})


class Client:
    def __init__(self, identity: str, password: str, port: int, clean: bool, expiry: int):
        self.connected = threading.Event()
        self.disconnected = threading.Event()
        self.subscribed = threading.Event()
        self.received = threading.Event()
        self.messages: list[dict] = []
        self.errors: list[str] = []
        self.session_present = False
        self.acks: list[dict] = []
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=identity,
                                  protocol=mqtt.MQTTv5, reconnect_on_failure=False)
        self.client.username_pw_set(USERNAME, password)

        def connect(client, userdata, flags, reason_code, properties):
            self.session_present = flags.session_present
            if reason_code.is_failure:
                self.errors.append("CONNACK " + str(reason_code))
            self.connected.set()

        def subscribe(client, userdata, mid, reasons, properties):
            if any(r.is_failure for r in reasons):
                self.errors.append("SUBACK failure")
            self.subscribed.set()

        def message(client, userdata, msg):
            self.messages.append({"topic": msg.topic, "sha256": digest(msg.payload), "qos": msg.qos,
                                  "retain": msg.retain, "dup": msg.dup})
            self.received.set()

        def publish(client, userdata, mid, reason_code, properties):
            self.acks.append({"mid": mid, "reason": str(reason_code), "reasonValue": reason_code.value})
            if reason_code.is_failure:
                self.errors.append("PUBACK failure " + str(reason_code))

        self.client.on_connect = connect
        self.client.on_subscribe = subscribe
        self.client.on_message = message
        self.client.on_publish = publish
        self.client.on_disconnect = lambda *args: self.disconnected.set()
        properties = Properties(PacketTypes.CONNECT)
        properties.SessionExpiryInterval = expiry
        self.client.connect("127.0.0.1", port, 30, clean_start=clean, properties=properties)
        self.client.loop_start()
        if not self.connected.wait(15):
            self.close()
            raise AssertionError("CONNACK timeout")
        if self.errors:
            self.close()
            raise AssertionError(self.errors)

    def subscribe(self, topic: str) -> None:
        self.client.subscribe(topic, qos=1)
        if not self.subscribed.wait(15) or self.errors:
            raise AssertionError(self.errors or "SUBACK timeout")

    def publish(self, topic: str, payload: bytes, retain: bool = False) -> None:
        published = self.client.publish(topic, payload, qos=1, retain=retain)
        published.wait_for_publish(timeout=15)
        if not published.is_published() or published.rc != mqtt.MQTT_ERR_SUCCESS or self.errors:
            raise AssertionError(self.errors or "PUBACK not received")

    def close(self) -> None:
        self.client.disconnect()
        self.disconnected.wait(5)
        self.client.loop_stop()


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("prepare", "verify"))
    parser.add_argument("--private", type=Path, required=True, help="directory containing mqtt.password")
    parser.add_argument("--run-id", required=True, help="fixture cluster ID; used only to derive synthetic topics")
    parser.add_argument("--output", type=Path, required=True, help="directory for hash-only result JSON")
    parser.add_argument("--target-port", type=int, required=True, help="local port to the target Pod MQTT listener")
    parser.add_argument("--publisher-port", type=int, help="local port to a surviving Pod; required for prepare")
    args = parser.parse_args(argv)
    if args.mode == "prepare" and args.publisher_port is None:
        parser.error("prepare requires --publisher-port")
    oracle = Oracle(args.run_id, args.private, args.output)
    try:
        if args.mode == "prepare":
            oracle.prepare(args.target_port, args.publisher_port)
            print("PASS: retained plus three offline QoS 1 messages received successful PUBACKs before fault")
        else:
            oracle.verify(args.target_port)
            print("PASS: persistent session and all three QoS 1 payloads resumed; retained payload unchanged; no republish")
    except Exception as exc:
        oracle.save("mqtt-" + args.mode + "-failure.json", {"result": "FAIL", "runId": args.run_id, "error": str(exc)})
        raise
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
