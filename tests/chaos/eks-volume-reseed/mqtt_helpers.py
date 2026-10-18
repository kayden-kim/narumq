"""Retained marker and survivor PUBACK probe for the reseed run. paho is imported only when a client is created."""
from __future__ import annotations

import hashlib
from pathlib import Path
import threading

import reseed  # noqa: F401  (puts tests/chaos/eks-worker-recovery on sys.path)

USERNAME = "acceptance-device"
PUBLISHER_ID = "ebs-worker-publisher"
READER_ID = "ebs-worker-reader"


def marker_topic(run_id: str) -> str:
    return f"acceptance/{run_id}/reseed/retained"


def marker_payload(run_id: str, scenario: str) -> bytes:
    return f"{run_id}:reseed-marker:{scenario}".encode("ascii")


def _password(private: Path) -> str:
    return (private / "mqtt.password").read_text(encoding="utf-8").strip()


def publish_marker(private: Path, run_id: str, scenario: str, port: int, client_factory=None) -> dict:
    """Publish the retained QoS 1 marker and require a PUBACK with reason code below 0x80."""
    if client_factory is None:
        import payload_oracle
        client_factory = payload_oracle.Client
    payload = marker_payload(run_id, scenario)
    client = client_factory(PUBLISHER_ID, _password(private), port, True, 0)
    try:
        client.publish(marker_topic(run_id), payload, retain=True)
        acks = list(client.acks)
    finally:
        client.close()
    if len(acks) != 1 or acks[0]["reasonValue"] >= 0x80:
        raise AssertionError("marker PUBACK missing or refused")
    return {"topic": marker_topic(run_id), "sha256": hashlib.sha256(payload).hexdigest(),
            "pubackReasonCode": acks[0]["reasonValue"], "retain": True, "qos": 1}


def read_marker(private: Path, run_id: str, port: int, client_factory=None) -> dict:
    """Read the retained marker from the Pod behind `port`; returns the SHA-256 of what arrived."""
    if client_factory is None:
        import payload_oracle
        client_factory = payload_oracle.Client
    reader = client_factory(READER_ID, _password(private), port, True, 0)
    try:
        reader.subscribe(marker_topic(run_id))
        if not reader.received.wait(15):
            return {"found": False, "sha256": None}
        found = [m for m in reader.messages if m["topic"] == marker_topic(run_id) and m["retain"]]
    finally:
        reader.close()
    hashes = sorted({m["sha256"] for m in found})
    return {"found": bool(found), "sha256": hashes[0] if len(hashes) == 1 else None, "messages": len(found),
            "distinctHashes": len(hashes)}


class SurvivorProbe:
    """QoS 1 publish to the observer Pod; one sample is one PUBACK check, like the worker-recovery probe."""

    def __init__(self, private: Path, run_id: str, port: int):
        import paho.mqtt.client as mqtt
        self.mqtt = mqtt
        self.connected = threading.Event()
        self.connection_failed = False
        self.acks: dict[int, dict] = {}
        self.sequence = 0
        self.topic = f"acceptance/{run_id}/reseed/continuity"
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=PUBLISHER_ID,
                                  protocol=mqtt.MQTTv5, reconnect_on_failure=False)
        self.client.username_pw_set(USERNAME, _password(private))

        def connect(client, userdata, flags, reason, properties):
            self.connection_failed = reason.is_failure
            self.connected.set()

        def publish(client, userdata, mid, reason, properties):
            self.acks[mid] = {"reasonCode": reason.value, "success": not reason.is_failure}

        self.client.on_connect = connect
        self.client.on_publish = publish
        self.client.connect("127.0.0.1", port, 30, clean_start=True)
        self.client.loop_start()
        if not self.connected.wait(15) or self.connection_failed:
            self.close()
            raise RuntimeError("MQTT connection did not succeed")

    def probe(self) -> dict:
        sequence = self.sequence
        self.sequence += 1
        published = self.client.publish(self.topic, str(sequence).encode(), qos=1)
        published.wait_for_publish(timeout=15)
        ack = self.acks.pop(published.mid, {})
        return {"sequence": sequence, "puback": published.is_published()
                and published.rc == self.mqtt.MQTT_ERR_SUCCESS and ack.get("success") is True,
                "reasonCode": ack.get("reasonCode")}

    def close(self) -> None:
        self.client.disconnect()
        self.client.loop_stop()


LARGE_PUBLISHER_ID = "large-load-publisher"
LARGE_READER_ID = "large-load-reader"
PAD_DEADLINE_SECONDS = 600
READ_DEADLINE_SECONDS = 90
READ_GRACE_SECONDS = 3.0


def publish_large_load(private: Path, run_id: str, settings: dict, port: int, client_factory=None,
                       monotonic=None) -> dict:
    """Write the large retained topics, then `padEntries` small retained updates, all QoS 1 with a PUBACK each.

    The padding makes the leader pass its snapshot cadence after the large state was written, so a snapshot that
    contains the large state exists before the member is lost. Returns SHA-256 per topic; payloads are not kept.
    """
    import time
    monotonic = monotonic or time.monotonic
    if client_factory is None:
        import payload_oracle
        client_factory = payload_oracle.Client
    client = client_factory(LARGE_PUBLISHER_ID, _password(private), port, True, 0)
    hashes: dict[str, str] = {}
    padded = 0
    try:
        for index in range(settings["retainedTopics"]):
            payload = reseed.large_payload(run_id, index, settings["retainedPayloadBytes"])
            topic = reseed.large_topic(run_id, index)
            client.publish(topic, payload, retain=True)
            hashes[topic] = hashlib.sha256(payload).hexdigest()
        deadline = monotonic() + PAD_DEADLINE_SECONDS
        pad_topic = reseed.large_pad_topic(run_id)
        while padded < settings["padEntries"] and monotonic() < deadline:
            client.publish(pad_topic, f"pad:{padded}".encode("ascii"), retain=True)
            padded += 1
        acks = list(client.acks)
    finally:
        client.close()
    expected = settings["retainedTopics"] + padded
    if len(acks) != expected or any(ack["reasonValue"] >= 0x80 for ack in acks):
        raise AssertionError("large load PUBACK missing or refused")
    return {"topics": len(hashes), "payloadBytes": settings["retainedPayloadBytes"],
            "totalRetainedBytes": settings["retainedTopics"] * settings["retainedPayloadBytes"],
            "padPublished": padded, "padRequested": settings["padEntries"], "sha256": hashes}


def read_large_retained(private: Path, run_id: str, settings: dict, port: int, client_factory=None,
                        monotonic=None, sleep=None) -> list[dict]:
    """Subscribe to the large topics on the Pod behind `port` and return what arrived (hashes only)."""
    import time
    monotonic = monotonic or time.monotonic
    sleep = sleep or time.sleep
    if client_factory is None:
        import payload_oracle
        client_factory = payload_oracle.Client
    reader = client_factory(LARGE_READER_ID, _password(private), port, True, 0)
    try:
        reader.subscribe(reseed.large_topic_filter(run_id))
        deadline = monotonic() + READ_DEADLINE_SECONDS
        while monotonic() < deadline and len({m["topic"] for m in reader.messages}) < settings["retainedTopics"]:
            sleep(0.2)
        sleep(READ_GRACE_SECONDS)
        return [dict(m) for m in reader.messages]
    finally:
        reader.close()
