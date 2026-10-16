"""MQTT 5 QoS 1 publish probe that records the exact PUBACK reason code of every attempt."""
from __future__ import annotations

from pathlib import Path
import threading
import time

from observer import timestamp

CONNECT_WAIT_SECONDS = 5
PUBACK_WAIT_SECONDS = 10
# Fixed payload prefix keeps retained messages recognisable; the rest is filler of the requested size.
FILLER = b"x"


class QoS1Probe:
    """One MQTT 5 connection through a local tunnel port; paho never reconnects by itself.

    paho's network loop thread exits at once when it starts without a connected socket and automatic
    reconnect is off, so every (re)connect is: stop the old loop, `connect()`, then `loop_start()`.
    `attempt()` reconnects lazily when the previous attempt lost the connection, so a refused CONNECT or a
    dropped link is itself a recorded sample instead of an exception.

    With `payload_bytes` the probe publishes retained messages of that size, rotating over `topic_count`
    topics `<topic>/<n>`; a retained QoS 1 publish is replicated through Raft and therefore writes to every
    member's WAL, which a plain QoS 1 publish without subscribers does not.
    """

    def __init__(self, private: Path, run_id: str, client_id: str, port: int, topic: str, epoch: float | None = None,
                 payload_bytes: int | None = None, topic_count: int = 1):
        import paho.mqtt.client as mqtt
        if topic_count < 1 or (payload_bytes is not None and payload_bytes < 1):
            raise ValueError("topic_count and payload_bytes must be positive")
        self.mqtt = mqtt
        self.port = port
        self.topic = topic
        self.payload_bytes = payload_bytes
        self.topic_count = topic_count
        self.epoch = time.monotonic() if epoch is None else epoch
        self.sequence = 0
        self.connected = threading.Event()
        self.alive = False
        self.loop_running = False
        self.connect_reason = None
        self.acks: dict[int, int] = {}
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=client_id, protocol=mqtt.MQTTv5,
                                  reconnect_on_failure=False)
        self.client.username_pw_set("acceptance-device", (private / "mqtt.password").read_text(encoding="utf-8").strip())

        def on_connect(client, userdata, flags, reason, properties):
            self.connect_reason = reason.value
            self.alive = not reason.is_failure
            self.connected.set()

        def on_disconnect(client, userdata, flags, reason, properties=None):
            self.alive = False

        def on_publish(client, userdata, mid, reason, properties):
            self.acks[mid] = reason.value

        self.client.on_connect = on_connect
        self.client.on_disconnect = on_disconnect
        self.client.on_publish = on_publish

    def topics(self) -> list[str]:
        if self.topic_count == 1:
            return [self.topic]
        return [f"{self.topic}/{n}" for n in range(self.topic_count)]

    def _stop_loop(self) -> None:
        if self.loop_running:
            self.client.loop_stop()
            self.loop_running = False

    def _connect(self) -> str | None:
        """Return an outcome code when the connection could not be established, else None."""
        self._stop_loop()
        self.connected.clear()
        self.connect_reason = None
        try:
            self.client.connect("127.0.0.1", self.port, 30, clean_start=True)
        except OSError:
            return "transport_error"
        self.client.loop_start()
        self.loop_running = True
        if not self.connected.wait(CONNECT_WAIT_SECONDS):
            return "connect_timeout"
        return None if self.alive else "connect_refused"

    def _payload(self, sequence: int) -> bytes:
        head = f"{sequence}:".encode()
        if self.payload_bytes is None:
            return head[:-1]
        return head + FILLER * max(0, self.payload_bytes - len(head))

    def _publish(self, topic: str, payload: bytes, retain: bool) -> dict:
        info = self.client.publish(topic, payload, qos=1, retain=retain)
        sample: dict = {}
        if info.rc != self.mqtt.MQTT_ERR_SUCCESS:
            sample["outcome"] = "not_connected"
            self.alive = False
            return sample
        info.wait_for_publish(timeout=PUBACK_WAIT_SECONDS)
        code = self.acks.pop(info.mid, None)
        if code is None:
            sample["outcome"] = "connection_lost" if not self.alive else "puback_timeout"
        else:
            sample.update(outcome="puback", reasonCode=code, puback=code < 0x80)
        return sample

    def attempt(self) -> dict:
        started = time.monotonic()
        sample = {"at": timestamp(), "startedSeconds": started - self.epoch, "outcome": None, "reasonCode": None,
                  "puback": False, "payloadBytes": 0}
        try:
            failure = None if self.alive else self._connect()
            if failure:
                sample.update(outcome=failure, reasonCode=self.connect_reason)
            else:
                sequence = self.sequence
                self.sequence += 1
                topic = self.topics()[sequence % self.topic_count]
                payload = self._payload(sequence)
                sample["payloadBytes"] = len(payload)
                sample.update(self._publish(topic, payload, self.payload_bytes is not None))
        except Exception as exc:
            # Exception text can carry URLs or payloads; keep the type only.
            sample["outcome"] = "error:" + type(exc).__name__
            self.alive = False
        sample["finishedSeconds"] = time.monotonic() - self.epoch
        return sample

    def clear_retained(self) -> list[dict]:
        """Publish an empty retained message to each topic, which deletes the retained state."""
        results = []
        for topic in self.topics():
            started = time.monotonic()
            try:
                failure = None if self.alive else self._connect()
                outcome = {"outcome": failure} if failure else self._publish(topic, b"", True)
            except Exception as exc:
                outcome = {"outcome": "error:" + type(exc).__name__}
            results.append({"topic": topic, "outcome": outcome.get("outcome"), "reasonCode": outcome.get("reasonCode"),
                            "seconds": round(time.monotonic() - started, 3)})
        return results

    def close(self) -> None:
        try:
            self.client.disconnect()
        except Exception:
            pass
        finally:
            self._stop_loop()


class ProbeLoop:
    """Run `probe.attempt()` every `interval` seconds on a thread and keep every sample."""

    def __init__(self, probe, interval: float = 1.0):
        self.probe = probe
        self.interval = interval
        self.samples: list[dict] = []
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self.error: str | None = None

    def _run(self):
        try:
            while not self._stop.is_set():
                started = time.monotonic()
                self.samples.append(self.probe.attempt())
                self._stop.wait(max(0.0, self.interval - (time.monotonic() - started)))
        except Exception as exc:
            self.error = type(exc).__name__

    def start(self) -> None:
        self._thread.start()

    def stop(self, timeout: float = 30) -> list[dict]:
        self._stop.set()
        if self._thread.is_alive():
            self._thread.join(timeout)
        if self._thread.is_alive():
            raise TimeoutError("probe loop did not stop")
        if self.error:
            raise RuntimeError("probe loop failed: " + self.error)
        return list(self.samples)
