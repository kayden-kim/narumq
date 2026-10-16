"""Durable stream oracle: PUBACKed messages must survive a fault (RPO 0).

A persistent subscriber is prepared on the fault target before the fault. A publisher sends numbered
QoS 1 messages through a surviving Pod and journals every outcome. After the fault the subscriber
reconnects with Clean Start 0 and the received sequence numbers are judged against the journal.
Payloads are `<scenario>:<seq>`; credentials are read from the private directory and never written.
"""
from __future__ import annotations

import datetime
import json
from pathlib import Path
import threading
import time
from typing import Callable

from observer import timestamp

USERNAME = "acceptance-device"
SUBSCRIBER_ID = "durable-stream-subscriber"
PUBLISHER_ID = "durable-stream-publisher"
SESSION_EXPIRY_SECONDS = 3600
ACK_TIMEOUT_SECONDS = 15
IDLE_SECONDS = 10
VERIFY_MAX_SECONDS = 120
# A detached session's durable queue holds at most 64 messages; later matches are Saturated and skipped
# while the publish still succeeds (cluster-session-and-delivery.md). The oracle stays below that limit.
PER_SESSION_LIMIT = 64
MAX_MESSAGES = 60
# Plan defaults: 3 s x 60 messages = 180 s of coverage starting just before the fault.
DEFAULT_INTERVAL_SECONDS = 3.0
INTERVAL_BOUNDS = (0.2, 30.0)


def plan_settings(plan: dict) -> tuple[float, int]:
    """Validated `durableIntervalSeconds` (0.2..30, default 3.0) and `durableMaxMessages` (1..60, default 60)."""
    interval = plan.get("durableIntervalSeconds", DEFAULT_INTERVAL_SECONDS)
    if (isinstance(interval, bool) or not isinstance(interval, (int, float))
            or not INTERVAL_BOUNDS[0] <= interval <= INTERVAL_BOUNDS[1]):
        raise ValueError(f"durableIntervalSeconds must be a number within {list(INTERVAL_BOUNDS)}")
    count = plan.get("durableMaxMessages", MAX_MESSAGES)
    if isinstance(count, bool) or not isinstance(count, int) or not 1 <= count <= MAX_MESSAGES:
        raise ValueError(f"durableMaxMessages must be an integer within [1, {MAX_MESSAGES}]")
    return float(interval), count


def stream_topic(run_id: str, scenario: str) -> str:
    return f"acceptance/{run_id}/durable/{scenario}"


def stream_payload(scenario: str, seq: int) -> bytes:
    return f"{scenario}:{seq}".encode("ascii")


def parse_seq(scenario: str, payload: bytes) -> int | None:
    """Return the sequence number of a payload of this scenario, or None for any other payload."""
    try:
        text = payload.decode("ascii")
    except UnicodeDecodeError:
        return None
    prefix, separator, number = text.rpartition(":")
    if not separator or prefix != scenario or not number.isdigit():
        return None
    return int(number)


def _time(value: str) -> datetime.datetime:
    return datetime.datetime.fromisoformat(value)


def read_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def outcomes_between(entries: list[dict], start_iso: str, end_iso: str) -> list[dict]:
    """Journal entries whose message was sent within [start, end]."""
    start, end = _time(start_iso), _time(end_iso)
    return [dict(entry) for entry in entries if start <= _time(entry["sentAt"]) <= end]


class MqttSession:
    """One MQTT 5 connection through paho; the only place that touches paho."""

    def __init__(self, client_id: str, password: str, port: int, clean_start: bool, session_expiry: int,
                 on_message: Callable[[bytes, bool], None] | None = None,
                 on_puback: Callable[[int, int], None] | None = None,
                 on_disconnect: Callable[[], None] | None = None):
        import paho.mqtt.client as mqtt
        from paho.mqtt.packettypes import PacketTypes
        from paho.mqtt.properties import Properties
        self.mqtt = mqtt
        self.session_present = False
        self._connected = threading.Event()
        self._subscribed = threading.Event()
        self._failure: str | None = None
        self._closing = False
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=client_id,
                                  protocol=mqtt.MQTTv5, reconnect_on_failure=False)
        self.client.username_pw_set(USERNAME, password)

        def connect(client, userdata, flags, reason, properties):
            self.session_present = bool(flags.session_present)
            if reason.is_failure:
                self._failure = "CONNACK refused"
            self._connected.set()

        def subscribe(client, userdata, mid, reasons, properties):
            if any(r.is_failure for r in reasons):
                self._failure = "SUBACK refused"
            self._subscribed.set()

        def disconnect(client, userdata, flags, reason, properties):
            if not self._closing and on_disconnect is not None:
                on_disconnect()

        self.client.on_connect = connect
        self.client.on_subscribe = subscribe
        self.client.on_disconnect = disconnect
        if on_message is not None:
            self.client.on_message = lambda client, userdata, msg: on_message(bytes(msg.payload), bool(msg.dup))
        if on_puback is not None:
            self.client.on_publish = lambda client, userdata, mid, reason, properties: on_puback(mid, reason.value)
        properties = Properties(PacketTypes.CONNECT)
        properties.SessionExpiryInterval = session_expiry
        self.client.connect("127.0.0.1", port, 30, clean_start=clean_start, properties=properties)
        self.client.loop_start()
        if not self._connected.wait(15) or self._failure:
            self.close()
            raise RuntimeError(self._failure or "CONNACK timeout")

    def subscribe(self, topic: str) -> None:
        self.client.subscribe(topic, qos=1)
        if not self._subscribed.wait(15) or self._failure:
            raise RuntimeError(self._failure or "SUBACK timeout")

    def publish(self, topic: str, payload: bytes) -> int:
        info = self.client.publish(topic, payload, qos=1)
        if info.rc != self.mqtt.MQTT_ERR_SUCCESS:
            raise RuntimeError("publish not accepted by client")
        return info.mid

    def close(self) -> None:
        self._closing = True
        self.client.disconnect()
        self.client.loop_stop()


def _password(private: Path) -> str:
    return (private / "mqtt.password").read_text(encoding="utf-8").strip()


def _write(output: Path, name: str, value: dict) -> None:
    (output / name).write_text(json.dumps(value, indent=2), encoding="utf-8")


def prepare_subscriber(private: Path, run_id: str, scenario: str, port: int, output: Path,
                       session_factory=MqttSession) -> dict:
    """Create the persistent QoS 1 subscription on the Pod behind `port`, then disconnect normally."""
    name = f"durable-prepare-{scenario}.json"
    record = {"scenario": scenario, "clientId": SUBSCRIBER_ID, "topic": stream_topic(run_id, scenario),
              "sessionExpirySeconds": SESSION_EXPIRY_SECONDS, "subscribedQos": 1, "at": timestamp()}
    try:
        session = session_factory(SUBSCRIBER_ID, _password(private), port, True, SESSION_EXPIRY_SECONDS)
        try:
            session.subscribe(record["topic"])
        finally:
            session.close()
    except Exception as exc:
        _write(output, name, {**record, "result": "FAIL", "errorCode": type(exc).__name__})
        raise
    record["result"] = "PASS"
    _write(output, name, record)
    return record


class DurablePublisher:
    """Publish numbered QoS 1 messages through one connection and journal every outcome."""

    def __init__(self, private: Path, run_id: str, scenario: str, port: int, output: Path, interval: float = 0.2,
                 session_factory=MqttSession, max_messages: int = MAX_MESSAGES):
        if interval <= 0:
            raise ValueError("publish interval must be positive")
        if isinstance(max_messages, bool) or not isinstance(max_messages, int) or not 1 <= max_messages <= MAX_MESSAGES:
            raise ValueError(f"max_messages must be an integer within [1, {MAX_MESSAGES}]")
        self.max_messages = max_messages
        self.capped_at: str | None = None
        self.private = private
        self.scenario = scenario
        self.topic = stream_topic(run_id, scenario)
        self.port = port
        self.interval = interval
        self.session_factory = session_factory
        self.journal_name = f"durable-publish-{scenario}.jsonl"
        self.journal_path = Path(output) / self.journal_name
        self.entries: list[dict] = []
        self.connection_lost = False
        self._pending: dict[int, dict] = {}
        self._early: dict[int, tuple[int, float]] = {}
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._closing = False
        self._thread: threading.Thread | None = None
        self._session = None
        self._stream = None
        self._result: dict | None = None
        self._next = 1

    def start(self) -> None:
        if self._thread is not None:
            raise RuntimeError("publisher already started")
        self._stream = self.journal_path.open("x", encoding="utf-8")
        try:
            self._session = self.session_factory(PUBLISHER_ID, _password(self.private), self.port, True, 0,
                                                 on_puback=self._acked, on_disconnect=self._lost)
        except Exception:
            self._stream.close()
            raise
        self._thread = threading.Thread(target=self._run, name="durable-publisher", daemon=True)
        self._thread.start()

    def _record(self, entry: dict, outcome: str, reason: int | None, ack_at: str | None) -> None:
        entry.update(outcome=outcome, reasonCode=reason, ackAt=ack_at)
        self.entries.append(entry)
        self._stream.write(json.dumps(entry, separators=(",", ":")) + "\n")
        self._stream.flush()

    def _acked(self, mid: int, reason: int) -> None:
        with self._lock:
            entry = self._pending.pop(mid, None)
            if entry is None:
                self._early[mid] = (reason, time.monotonic())
                return
            self._record({k: v for k, v in entry.items() if k != "sentMonotonic"},
                         "rejected" if reason >= 0x80 else "acked", reason, timestamp())

    def _lost(self) -> None:
        with self._lock:
            if not self._closing:
                self.connection_lost = True
        self._stop.set()

    def _expire(self, everything: bool = False) -> None:
        now = time.monotonic()
        with self._lock:
            for mid in [m for m, e in self._pending.items()
                        if everything or now - e["sentMonotonic"] >= ACK_TIMEOUT_SECONDS]:
                entry = self._pending.pop(mid)
                self._record({k: v for k, v in entry.items() if k != "sentMonotonic"}, "unknown", None, None)

    def _publish_one(self) -> None:
        seq = self._next
        self._next += 1
        entry = {"seq": seq, "sentAt": timestamp(), "sentMonotonic": time.monotonic()}
        try:
            mid = self._session.publish(self.topic, stream_payload(self.scenario, seq))
        except Exception:
            with self._lock:
                self.connection_lost = True
                self._record({k: v for k, v in entry.items() if k != "sentMonotonic"}, "unknown", None, None)
            self._stop.set()
            return
        with self._lock:
            early = self._early.pop(mid, None)
            if early is not None and early[1] >= entry["sentMonotonic"]:
                self._record({k: v for k, v in entry.items() if k != "sentMonotonic"},
                             "rejected" if early[0] >= 0x80 else "acked", early[0], timestamp())
            else:
                self._pending[mid] = entry

    def _run(self) -> None:
        while not self._stop.is_set():
            self._publish_one()
            if self._next > self.max_messages and not self._stop.is_set():
                self.capped_at = timestamp()
                break
            self._expire()
            self._stop.wait(self.interval)
        # After the cap no more messages are sent; keep expiring until every sent message has an outcome.
        while self.capped_at is not None and not self._stop.is_set():
            with self._lock:
                if not self._pending:
                    break
            self._expire()
            self._stop.wait(0.2)
        if self.connection_lost:
            self._expire(everything=True)

    def wait_for_acked(self, count: int, timeout: float) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            with self._lock:
                if sum(e["outcome"] == "acked" for e in self.entries) >= count:
                    return True
            if self.connection_lost:
                return False
            time.sleep(0.05)
        return False

    def outcomes_between(self, start_iso: str, end_iso: str) -> list[dict]:
        with self._lock:
            return outcomes_between(self.entries, start_iso, end_iso)

    def stop(self) -> dict:
        if self._result is not None:
            return self._result
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=ACK_TIMEOUT_SECONDS + 5)
        deadline = time.monotonic() + ACK_TIMEOUT_SECONDS + 1
        while time.monotonic() < deadline and not self.connection_lost:
            with self._lock:
                if not self._pending:
                    break
            self._expire()
            time.sleep(0.05)
        self._expire(everything=True)
        with self._lock:
            self._closing = True
        try:
            if self._session is not None:
                self._session.close()
        finally:
            if self._stream is not None:
                self._stream.close()
        counts = {name: sum(e["outcome"] == name for e in self.entries) for name in ("acked", "rejected", "unknown")}
        self._result = {"scenario": self.scenario, "topic": self.topic, "published": len(self.entries),
                        **counts, "connectionLost": self.connection_lost, "journal": self.journal_name,
                        "maxMessages": self.max_messages, "interval": self.interval,
                        "perSessionLimit": PER_SESSION_LIMIT, "capReached": self.capped_at is not None,
                        "cappedAt": self.capped_at, "stoppedAt": timestamp()}
        return self._result


def judge(journal: list[dict], received: list[dict], through: datetime.datetime | None = None) -> dict:
    """PASS iff every acked seq was received at least once and no rejected seq was received.

    `received` is the union over all verification labels. With `through`, only messages acked at or
    before that instant must have been received (the publisher may still be running). Unknown outcomes
    and QoS 1 duplicates are reported, never judged. A stream without any acked message cannot pass.
    """
    outcomes = {entry["seq"]: entry for entry in journal}
    seen: dict[int, int] = {}
    for item in received:
        seen[item["seq"]] = seen.get(item["seq"], 0) + 1
    acked = [s for s, e in outcomes.items() if e["outcome"] == "acked"
             and (through is None or _time(e["ackAt"]) <= through)]
    rejected = [s for s, e in outcomes.items() if e["outcome"] == "rejected"]
    unknown = [s for s, e in outcomes.items() if e["outcome"] == "unknown"]
    missing = sorted(s for s in acked if s not in seen)
    rejected_delivered = sorted(s for s in rejected if s in seen)
    return {"result": "PASS" if acked and not missing and not rejected_delivered else "FAIL",
            "acked": len(acked), "rejected": len(rejected), "unknown": len(unknown),
            "missingAcked": missing, "rejectedDelivered": rejected_delivered,
            "unknownDelivered": sorted(s for s in unknown if s in seen),
            "duplicates": sum(count - 1 for count in seen.values()),
            "receivedDistinct": len(seen), "judgedThrough": None if through is None else through.isoformat()}


def verify_subscriber(private: Path, run_id: str, scenario: str, port: int, output: Path, label: str,
                      cutoff: str | None = None, idle_seconds: float = IDLE_SECONDS,
                      max_seconds: float = VERIFY_MAX_SECONDS, session_factory=MqttSession) -> dict:
    """Reconnect with Clean Start 0 (SessionPresent must be 1), receive until idle, and judge the union.

    With `cutoff` (ISO time) the publisher may still be running: the check also ends once every message
    acked up to the cutoff was received, and only those messages are judged. Without it, the
    publisher must already be stopped, and the check ends after `idle_seconds` without a new message.
    """
    output = Path(output)
    journal_path = output / f"durable-publish-{scenario}.jsonl"
    received_path = output / f"durable-received-{scenario}.jsonl"
    through = None if cutoff is None else _time(cutoff)
    result = {"label": label, "scenario": scenario, "startedAt": timestamp(), "sessionPresent": None,
              "idleSeconds": idle_seconds, "timedOut": False}
    lock = threading.Lock()
    received: list[dict] = []
    state = {"last": time.monotonic(), "lost": False, "unexpected": 0}

    def on_message(payload: bytes, dup: bool) -> None:
        seq = parse_seq(scenario, payload)
        with lock:
            if seq is None:
                state["unexpected"] += 1
            else:
                received.append({"label": label, "seq": seq, "at": timestamp(), "dup": dup})
            state["last"] = time.monotonic()

    def on_disconnect() -> None:
        state["lost"] = True

    def all_judged_received() -> bool:
        with lock:
            have = {item["seq"] for item in received}
        have |= {item["seq"] for item in read_jsonl(received_path)}
        return all(e["seq"] in have for e in read_jsonl(journal_path) if e["outcome"] == "acked"
                   and _time(e["ackAt"]) <= through)

    session = None
    try:
        session = session_factory(SUBSCRIBER_ID, _password(private), port, False, SESSION_EXPIRY_SECONDS,
                                  on_message=on_message, on_disconnect=on_disconnect)
        result["sessionPresent"] = bool(session.session_present)
        if result["sessionPresent"]:
            begin = time.monotonic()
            state["last"] = begin
            last_check = 0.0
            while True:
                time.sleep(0.05)
                now = time.monotonic()
                with lock:
                    idle = now - state["last"]
                if state["lost"]:
                    result["errorCode"] = "ConnectionLost"
                    break
                past_cutoff = through is None or datetime.datetime.now(datetime.timezone.utc) >= through
                if through is not None and past_cutoff and now - last_check >= 0.5:
                    last_check = now
                    if all_judged_received():
                        break
                if past_cutoff and idle >= idle_seconds:
                    break
                if now - begin >= max_seconds:
                    result["timedOut"] = True
                    break
        else:
            result["errorCode"] = "SessionNotPresent"
    except Exception as exc:
        result["errorCode"] = type(exc).__name__
    finally:
        if session is not None:
            try:
                session.close()
            except Exception as exc:
                result.setdefault("errorCode", type(exc).__name__)
        with received_path.open("a", encoding="utf-8") as stream:
            for item in received:
                stream.write(json.dumps(item, separators=(",", ":")) + "\n")
    verdict = judge(read_jsonl(journal_path), read_jsonl(received_path), through)
    result.update(verdict)
    result["perSessionLimit"] = PER_SESSION_LIMIT
    result["publishedTotal"] = len({e["seq"] for e in read_jsonl(journal_path)})
    result["receivedThisLabel"] = len(received)
    result["unexpectedPayloads"] = state["unexpected"]
    if result["sessionPresent"] is not True or "errorCode" in result:
        result["result"] = "FAIL"
    result["finishedAt"] = timestamp()
    _write(output, f"durable-verify-{scenario}-{label}.json", result)
    return result
