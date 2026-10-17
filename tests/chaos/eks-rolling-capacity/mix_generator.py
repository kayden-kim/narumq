"""Mixed MQTT workload for DRN-SCN-019 (runs inside the EKS fixture; Python standard library only).

Why this exists: the BP-02 capacity runner (tests/benchmark/capacity, Go) hard-codes MQTT 5, has no Will, no QoS 0 gap
accounting separate from QoS 1/2, no reorder measurement, and its step machine ends every step with a leader kill. It
cannot hold the measured B_* load while a rolling update runs. This generator keeps the runner's cell geometry so the
same capacity fixture applies (53 clients per cell, 8 streams, tenants ``bp02-{a,b}-cNNN`` with exact client-ID lists
and ACLs from ``capacity fixture`` / ``security-config``, TLS on every ``tlsEveryNthClient``-th client, the same
topics, payload size cycle and retained topic counts) and adds what the row demands:

* MQTT 3.1.1 and 5.0 clients in one run (publishers and subscribers alternate; shared subscriptions are MQTT 5);
* QoS 0, 1 and 2, retained state streams, persistent sessions (clean_session=0 / clean start false + Session Expiry);
* Will: every idle client carries a Will on ``<root>/alert/will/<id>`` (MQTT 5 with Will Delay 60 s, MQTT 3.1.1
  immediate); alert subscribers observe them;
* a per-stream monotonic sequence number in every payload, from which gap, duplicate and reorder are measured
  separately (definitions below).

Measurement definitions (one ledger per cell and stream, so one process owns every observation of a stream):

* ``gap``      an owed delivery that never arrived. Owed: QoS 1/2 messages the broker acknowledged (PUBACK, or PUBREC with
               reason < 0x80) to a persistent-session subscriber whose subscription existed at publish time, or to a
               clean-session subscriber that stayed connected and subscribed from publish time on; QoS 0 messages to a
               subscriber that stayed connected and subscribed. A shared-group message is owed only while every member
               stayed connected. Unacknowledged publishes are ``unacked``; QoS 2 messages whose publisher lost the
               connection between PUBREC and PUBCOMP are ``cleanPublisherInterrupted``; clean-session subscribers
               that dropped after publish are ``cleanSessionLoss`` (the capacity spec §3.9 gap rule). Deliveries after
               ``horizonSeconds`` count as gap (decided at the horizon) and as ``afterHorizon``.
* ``duplicate`` a second copy of one sequence number at the same subscriber (shared group: a second copy in the group),
               after MQTT packet-ID de-duplication of QoS 2.
* ``reorder``  a first copy whose sequence number is lower than the highest already received on the same topic at the
               same subscriber (MQTT orders per topic).
* ``retainedMismatches`` a clean subscriber's resubscribe must deliver, as RETAIN=1 copies, every state topic the
               publisher had published at SUBACK time and none it had not published by the end of the window.

Configuration is a JSON file (``--config``): see ``Config`` below. Control files: ``<controlDir>/stop`` starts the
shutdown, ``<controlDir>/collected`` releases the process after the harness read ``<outputDir>/final.json``. Progress is
``<outputDir>/summary.json`` (rewritten every 5 s), readiness ``<outputDir>/ready.json``, a failed setup
``<outputDir>/failed.json``.
"""

from __future__ import annotations

import asyncio
import bisect
import json
import os
import ssl
import struct
import sys
import time
import traceback
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

sys.dont_write_bytecode = True
CELL_STREAMS = 8
IDLE_A, IDLE_B = 12, 6
PAYLOAD_MAGIC = b"NMQ1"
WILL_MAGIC = b"NMQW|"
HEADER = struct.Struct(">4sQIIB")
QOS_KEYS = ("0", "1", "2")
GROUPS = ("published", "acked", "expected", "delivered", "gap", "duplicate", "reorder", "cleanSessionLoss", "unacked",
          "cleanPublisherInterrupted")
SCALARS = ("afterHorizon", "unexpectedDeliveries", "retainedChecks", "retainedMismatches", "protocolErrors", "ackTimeouts",
           "connectRefusals", "sessionNotPresent", "reconnects311", "reconnects5", "will311Received", "will5Received",
           "connectAttempts")
WILL_DELAY_V5 = 60
MASK64 = (1 << 64) - 1


class ProtocolError(Exception):
    pass


# Codec ---------------------------------------------------------------------------------------------
def enc_varint(value: int) -> bytes:
    out = bytearray()
    while True:
        byte, value = value & 0x7F, value >> 7
        out.append(byte | (0x80 if value else 0))
        if not value:
            return bytes(out)


def dec_varint(data: bytes, pos: int) -> tuple[int, int]:
    value, shift = 0, 0
    for _ in range(4):
        byte = data[pos]
        pos += 1
        value |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return value, pos
        shift += 7
    raise ProtocolError("malformed variable byte integer")


def enc_str(text: str) -> bytes:
    raw = text.encode("utf-8")
    return struct.pack(">H", len(raw)) + raw


def enc_bin(raw: bytes) -> bytes:
    return struct.pack(">H", len(raw)) + raw


def packet(ptype: int, flags: int, body: bytes) -> bytes:
    return bytes([(ptype << 4) | flags]) + enc_varint(len(body)) + body


def connect_packet(version: int, client_id: str, username: str, password: str, keepalive: int, clean: bool,
                   session_expiry: int, will: dict[str, Any] | None) -> bytes:
    flags = 0x80 | 0x40 | (0x02 if clean else 0)
    if will:
        flags |= 0x04 | (will["qos"] << 3) | (0x20 if will.get("retain") else 0)
    body = enc_str("MQTT") + bytes([version]) + bytes([flags]) + struct.pack(">H", keepalive)
    if version == 5:
        props = (b"\x11" + struct.pack(">I", session_expiry)) if session_expiry else b""
        body += enc_varint(len(props)) + props
    body += enc_str(client_id)
    if will:
        if version == 5:
            props = (b"\x18" + struct.pack(">I", will["delay"])) if will.get("delay") else b""
            body += enc_varint(len(props)) + props
        body += enc_str(will["topic"]) + enc_bin(will["payload"])
    body += enc_str(username) + enc_bin(password.encode("utf-8"))
    return packet(1, 0, body)


def subscribe_packet(version: int, packet_id: int, topic_filter: str, qos: int) -> bytes:
    body = struct.pack(">H", packet_id) + (b"\x00" if version == 5 else b"") + enc_str(topic_filter) + bytes([qos])
    return packet(8, 2, body)


def publish_packet(version: int, topic: str, payload: bytes, qos: int, retain: bool, dup: bool, packet_id: int) -> bytes:
    body = enc_str(topic) + (struct.pack(">H", packet_id) if qos else b"") + (b"\x00" if version == 5 else b"") + payload
    return packet(3, (0x08 if dup else 0) | (qos << 1) | (1 if retain else 0), body)


def ack_packet(ptype: int, packet_id: int) -> bytes:
    return packet(ptype, 2 if ptype == 6 else 0, struct.pack(">H", packet_id))


def disconnect_packet() -> bytes:
    return packet(14, 0, b"")


def parse_publish(version: int, flags: int, body: bytes) -> dict[str, Any]:
    qos = (flags >> 1) & 3
    if qos == 3 or len(body) < 2:
        raise ProtocolError("malformed PUBLISH")
    (length,) = struct.unpack_from(">H", body, 0)
    pos = 2 + length
    topic = body[2:pos].decode("utf-8")
    packet_id = 0
    if qos:
        (packet_id,) = struct.unpack_from(">H", body, pos)
        pos += 2
    if version == 5:
        props, pos = dec_varint(body, pos)
        pos += props
    return {"topic": topic, "qos": qos, "retain": bool(flags & 1), "dup": bool(flags & 8), "packetId": packet_id, "payload": body[pos:]}


def parse_connack(version: int, body: bytes) -> tuple[bool, int]:
    if len(body) < 2:
        raise ProtocolError("malformed CONNACK")
    return bool(body[0] & 1), body[1]


def parse_suback(version: int, body: bytes) -> tuple[int, list[int]]:
    (packet_id,) = struct.unpack_from(">H", body, 0)
    pos = 2
    if version == 5:
        props, pos = dec_varint(body, pos)
        pos += props
    return packet_id, list(body[pos:])


def parse_ack(body: bytes) -> tuple[int, int]:
    (packet_id,) = struct.unpack_from(">H", body, 0)
    return packet_id, (body[2] if len(body) > 2 else 0)


async def read_packet(reader: asyncio.StreamReader) -> tuple[int, int, bytes]:
    first = (await reader.readexactly(1))[0]
    length, shift = 0, 0
    for _ in range(4):
        byte = (await reader.readexactly(1))[0]
        length |= (byte & 0x7F) << shift
        if not byte & 0x80:
            break
        shift += 7
    else:
        raise ProtocolError("malformed remaining length")
    return first >> 4, first & 15, (await reader.readexactly(length)) if length else b""


# Plan (port of tests/benchmark/capacity/plan.go) ---------------------------------------------------------
@dataclass(frozen=True)
class StreamDef:
    name: str
    kind: str
    qos: int
    subscribers: int
    persistent: bool = False
    shared: bool = False
    retained: bool = False
    segment: str = ""
    cycle: int = 0


STREAM_DEFS = (
    StreamDef("a_telemetry", "a", 0, 6, segment="telemetry", cycle=8),
    StreamDef("a_alert", "a", 1, 4, persistent=True, segment="alert", cycle=4),
    StreamDef("a_command", "a", 2, 4, persistent=True, segment="command", cycle=4),
    StreamDef("a_job", "a", 1, 4, shared=True, segment="job", cycle=4),
    StreamDef("b_telemetry", "b", 0, 3, segment="telemetry", cycle=8),
    StreamDef("b_alert", "b", 1, 2, persistent=True, segment="alert", cycle=4),
    StreamDef("a_state", "a", 1, 2, retained=True, segment="state"),
    StreamDef("b_state", "b", 1, 2, retained=True, segment="state"),
)


def tenant_id(kind: str, cell: int) -> str:
    return f"bp02-{kind}-c{cell:03d}"


def topic_root(kind: str, cell: int) -> str:
    return f"bp02/{kind}/c{cell:03d}"


def stream_cycle(profile: dict[str, Any], stream: int) -> int:
    definition = STREAM_DEFS[stream]
    if definition.cycle:
        return definition.cycle
    return profile["retainedTopicsTenantA"] if stream == 6 else profile["retainedTopicsTenantB"]


def stream_topic(cell: int, stream: int, index: int) -> str:
    definition = STREAM_DEFS[stream]
    root = topic_root(definition.kind, cell)
    if definition.segment in ("telemetry", "command", "state"):
        return f"{root}/{definition.segment}/dev{index:02d}"
    if definition.segment == "alert":
        return f"{root}/alert/zone{index}"
    return f"{root}/job/{index}"


def subscriber_filter(cell: int, stream: int, index: int) -> str:
    definition = STREAM_DEFS[stream]
    root = topic_root(definition.kind, cell)
    if stream == 2:
        return stream_topic(cell, stream, index)
    if definition.shared:
        return f"$share/bp02a-c{cell:03d}/{root}/job/#"
    if definition.segment == "telemetry":
        return f"{root}/telemetry/+"
    return f"{root}/{definition.segment}/#"


def topic_matches(topic_filter: str, topic: str) -> bool:
    if topic_filter.startswith("$share/"):
        parts = topic_filter.split("/", 2)
        topic_filter = parts[2] if len(parts) == 3 else ""
    filter_levels, topic_levels = topic_filter.split("/"), topic.split("/")
    for index, level in enumerate(filter_levels):
        if level == "#":
            return index == len(filter_levels) - 1
        if index >= len(topic_levels) or (level != "+" and level != topic_levels[index]):
            return False
    return len(filter_levels) == len(topic_levels)


@dataclass
class ClientSpec:
    client_id: str
    tenant: str
    role: str  # publisher | subscriber | state-subscriber | idle
    cell: int
    stream: int
    index: int
    ordinal: int
    filter: str = ""
    qos: int = 0
    persistent: bool = False
    tls: bool = False
    version: int = 5
    will: dict[str, Any] | None = None
    root: str = ""


def protocol_version(role: str, stream: int, index: int, ordinal: int) -> int:
    """MQTT 5 and 3.1.1 alternate inside every role; shared subscriptions are an MQTT 5 feature."""
    if role == "publisher":
        return 5 if ordinal % 2 == 0 else 4
    if STREAM_DEFS[stream].shared if stream >= 0 else False:
        return 5
    return 5 if index % 2 == 0 else 4


def build_cell(profile: dict[str, Any], cell: int) -> list[ClientSpec]:
    """The 53 clients of one cell in the capacity runner's order, with protocol version and Will assigned."""
    clients: list[ClientSpec] = []

    def add(spec: ClientSpec) -> None:
        spec.cell, spec.ordinal = cell, len(clients)
        spec.tls = spec.ordinal % profile["tlsEveryNthClient"] == 0
        spec.version = protocol_version(spec.role, spec.stream, spec.index, spec.ordinal)
        spec.root = topic_root(STREAM_DEFS[spec.stream].kind, cell) if spec.stream >= 0 else topic_root(spec.tenant[5], cell)
        clients.append(spec)

    def publisher(stream: int) -> None:
        definition = STREAM_DEFS[stream]
        add(ClientSpec(f"bp02-{definition.kind}-c{cell:03d}-pub-{definition.segment}", tenant_id(definition.kind, cell), "publisher",
                       cell, stream, 0, 0, qos=definition.qos))

    def subscribers(stream: int, role: str) -> None:
        definition = STREAM_DEFS[stream]
        for index in range(definition.subscribers):
            add(ClientSpec(f"bp02-{definition.kind}-c{cell:03d}-sub-{definition.segment}-{index:02d}", tenant_id(definition.kind, cell), role,
                           cell, stream, index, 0, filter=subscriber_filter(cell, stream, index), qos=definition.qos,
                           persistent=definition.persistent))

    for stream in range(6):
        publisher(stream)
    publisher(6)
    publisher(7)
    for stream in range(6):
        subscribers(stream, "subscriber")
    for kind, count in (("a", IDLE_A), ("b", IDLE_B)):
        for index in range(count):
            spec = ClientSpec(f"bp02-{kind}-c{cell:03d}-idle-{index:03d}", tenant_id(kind, cell), "idle", cell, -1, index, 0, persistent=True)
            add(spec)
            delay = WILL_DELAY_V5 if spec.version == 5 else 0
            spec.will = {"topic": f"{topic_root(kind, cell)}/alert/will/{spec.client_id}", "qos": 1, "retain": False, "delay": delay,
                         "payload": WILL_MAGIC + str(spec.version).encode() + b"|" + spec.client_id.encode()}
    subscribers(6, "state-subscriber")
    subscribers(7, "state-subscriber")
    if len(clients) != 53:
        raise ValueError(f"cell has {len(clients)} clients, expected 53")
    return clients


# Payload -----------------------------------------------------------------------------------------------
def payload_size(profile: dict[str, Any], seq: int) -> int:
    cycle = profile["payloadSizeCycle"]
    return cycle[(seq - 1) % len(cycle)]


def _body(token: int, cell: int, stream: int, seq: int, length: int) -> bytes:
    block = struct.pack(">Q", ((seq * 0x9E3779B97F4A7C15) ^ (cell << 32) ^ (stream << 8) ^ token) & MASK64)
    return (block * (length // 8 + 1))[:length]


def make_payload(profile: dict[str, Any], token: int, cell: int, stream: int, seq: int) -> bytes:
    size = payload_size(profile, seq)
    return HEADER.pack(PAYLOAD_MAGIC, seq, token, cell, stream) + _body(token, cell, stream, seq, size - HEADER.size)


def parse_payload(profile: dict[str, Any], data: bytes) -> tuple[int, int, int, int] | None:
    """(cell, stream, seq, token) of a valid generated payload; ``None`` when it is not one (or its body is corrupt)."""
    if len(data) < HEADER.size or data[:4] != PAYLOAD_MAGIC:
        return None
    _, seq, token, cell, stream = HEADER.unpack_from(data)
    if seq == 0 or stream >= CELL_STREAMS or len(data) != payload_size(profile, seq) or data[HEADER.size:] != _body(token, cell, stream, seq, len(data) - HEADER.size):
        return None
    return cell, stream, seq, token


# Ledger --------------------------------------------------------------------------------------------------
class Counts:
    def __init__(self) -> None:
        self.groups = {group: {q: 0 for q in QOS_KEYS} for group in GROUPS}
        self.scalars = {name: 0 for name in SCALARS}

    def add(self, group: str, qos: int, amount: int = 1) -> None:
        self.groups[group][str(qos)] += amount

    def bump(self, name: str, amount: int = 1) -> None:
        self.scalars[name] += amount


@dataclass
class SubTimes:
    """Connectivity history of one subscriber client (monotonic seconds)."""
    persistent: bool
    suback_times: list[float] = field(default_factory=list)
    disc_times: list[float] = field(default_factory=list)

    def subscribed_before(self, moment: float) -> bool:
        return bisect.bisect_right(self.suback_times, moment) > 0

    def continuous(self, moment: float) -> bool:
        """Connected and subscribed from ``moment`` on: the latest SUBACK before it is not followed by a disconnect."""
        index = bisect.bisect_right(self.suback_times, moment)
        if index == 0:
            return False
        return bisect.bisect_left(self.disc_times, self.suback_times[index - 1]) == len(self.disc_times)


class Ledger:
    """Observations of one (cell, stream): publishes, broker acknowledgements and deliveries per subscriber."""

    def __init__(self, definition: StreamDef, cell: int, stream: int, profile: dict[str, Any], counts: Counts, subs: list[SubTimes]):
        self.definition, self.cell, self.stream, self.counts, self.subs = definition, cell, stream, counts, subs
        cycle = stream_cycle(profile, stream)
        self.topics = [stream_topic(cell, stream, i) for i in range(cycle)]
        filters = [subscriber_filter(cell, stream, i) for i in range(definition.subscribers)] if definition.subscribers else []
        self.recipients = [tuple(i for i, f in enumerate(filters) if topic_matches(f, topic)) for topic in self.topics]
        self.qos = definition.qos
        self.max_seq = 0
        self.next_final = 1
        self.pub_time: dict[int, float] = {}
        self.acked: dict[int, int] = {}
        self.recv: dict[int, dict[int, int]] = {}
        self.last_by_topic: dict[tuple[int, str], int] = {}

    def next_seq(self) -> int:
        self.max_seq += 1
        return self.max_seq

    def topic_of(self, seq: int) -> str:
        return self.topics[(seq - 1) % len(self.topics)]

    def published_topics(self) -> set[str]:
        return {self.topics[(s - 1) % len(self.topics)] for s in range(1, min(self.max_seq, len(self.topics)) + 1)}

    def published(self, seq: int, moment: float) -> None:
        self.pub_time[seq] = moment
        self.counts.add("published", self.qos)

    def acknowledged(self, seq: int, state: int) -> None:
        """State 1: broker acknowledged. State 2: acknowledged, then the publisher lost the connection before PUBCOMP."""
        if seq in self.pub_time:
            self.acked[seq] = state
        if state == 1:
            self.counts.add("acked", self.qos)

    def deliver(self, sub: int, seq: int, topic: str) -> None:
        counts = self.counts
        if seq < self.next_final:
            counts.bump("afterHorizon")
            counts.add("delivered", self.qos)
            return
        if seq > self.max_seq or topic != self.topic_of(seq) or sub not in self.recipients[(seq - 1) % len(self.topics)]:
            counts.bump("unexpectedDeliveries")
            return
        copies = self.recv.setdefault(seq, {})
        copies[sub] = copies.get(sub, 0) + 1
        counts.add("delivered", self.qos)
        if (sum(copies.values()) > 1) if self.definition.shared else (copies[sub] > 1):
            counts.add("duplicate", self.qos)
            return
        if self.definition.shared:
            return
        last = self.last_by_topic.get((sub, topic))
        if last is not None and seq < last:
            counts.add("reorder", self.qos)
        else:
            self.last_by_topic[(sub, topic)] = seq

    def finalize(self, now: float, horizon: float, force: bool = False) -> None:
        """Decide every sequence older than the horizon (all of them when ``force``) and drop it from memory."""
        counts, q = self.counts, self.qos
        while self.next_final <= self.max_seq:
            seq = self.next_final
            moment = self.pub_time.get(seq)
            if moment is not None and not force and moment + horizon > now:
                break
            self.next_final += 1
            copies = self.recv.pop(seq, {})
            if moment is None:
                continue
            ack = self.acked.pop(seq, 0)
            del self.pub_time[seq]
            if q > 0 and ack == 0:
                counts.add("unacked", q)
                continue
            if q > 0 and ack == 2:
                counts.add("cleanPublisherInterrupted", q)
                continue
            recipients = self.recipients[(seq - 1) % len(self.topics)]
            if self.definition.shared:
                if not recipients:
                    continue
                if all(self.subs[i].continuous(moment) for i in recipients):
                    counts.add("expected", q)
                    if not copies:
                        counts.add("gap", q)
                elif not copies:
                    counts.add("cleanSessionLoss", q)
                continue
            for sub in recipients:
                times = self.subs[sub]
                owed = times.continuous(moment) or (q > 0 and times.persistent and times.subscribed_before(moment))
                if owed:
                    counts.add("expected", q)
                    if sub not in copies:
                        counts.add("gap", q)
                elif sub not in copies:
                    counts.add("cleanSessionLoss", q)
        stale = [key for key, last in self.last_by_topic.items() if last < self.next_final - len(self.topics) * 4]
        for key in stale:
            del self.last_by_topic[key]


# Client --------------------------------------------------------------------------------------------------
@dataclass
class Config:
    run_token: int
    shards: int
    profile: dict[str, Any]
    cells: int
    rate: int
    mqtt: str
    mqtt_tls: str
    tls_server_name: str
    horizon: float = 300.0
    settle: float = 60.0
    retained_window: float = 3.0
    cpu_saturation_percent: float = 80.0
    credentials: str = "/fixture/credentials.json"
    ca_file: str = "/fixture/ca.pem"
    control_dir: str = "/control"
    output_dir: str = "/output"
    setup_timeout: float = 900.0
    monitor_interval: float = 5.0

    @classmethod
    def load(cls, path: str) -> "Config":
        raw = json.loads(Path(path).read_text(encoding="utf-8"))
        return cls(run_token=raw["runToken"], shards=raw["shards"], profile=raw["profile"], cells=raw["cells"], rate=raw["rate"],
                   mqtt=raw["mqtt"], mqtt_tls=raw["mqttTls"], tls_server_name=raw["tlsServerName"], horizon=raw.get("horizonSeconds", 300.0),
                   settle=raw.get("settleSeconds", 60.0), retained_window=raw.get("retainedWindowSeconds", 3.0),
                   cpu_saturation_percent=raw.get("cpuSaturationPercent", 80.0), credentials=raw.get("credentials", "/fixture/credentials.json"),
                   ca_file=raw.get("caFile", "/fixture/ca.pem"), control_dir=raw.get("controlDir", "/control"),
                   output_dir=raw.get("outputDir", "/output"), setup_timeout=raw.get("setupTimeoutSeconds", 900.0),
                   monitor_interval=raw.get("monitorSeconds", 5.0))


class Client:
    """One MQTT client for the whole run: reconnects with persistent or clean sessions and feeds the ledger."""

    def __init__(self, gen: "Generator", spec: ClientSpec, ledger: Ledger | None, times: SubTimes | None):
        self.gen, self.spec, self.ledger, self.times = gen, spec, ledger, times
        self.writer: asyncio.StreamWriter | None = None
        self.connected = False
        self.subscribed = False
        self.first_connect = True
        self.epoch = 0
        self.inbound_qos2: set[int] = set()
        self.inflight: dict[int, tuple[int, str]] = {}
        self.credit = asyncio.Event()
        self.credit.set()
        self.next_packet_id = 1
        self.suback: asyncio.Future | None = None
        self.retained_got: set[str] = set()

    # connection ---------------------------------------------------------------------------------------
    async def open(self, clean: bool, expiry: int) -> tuple[asyncio.StreamReader, asyncio.StreamWriter, bool]:
        gen, spec = self.gen, self.spec
        host, _, port = (gen.cfg.mqtt_tls if spec.tls else gen.cfg.mqtt).rpartition(":")
        context = gen.tls_context if spec.tls else None
        gen.counts.bump("connectAttempts")
        reader, writer = await asyncio.wait_for(asyncio.open_connection(
            host, int(port), ssl=context, server_hostname=gen.cfg.tls_server_name if spec.tls else None), 10)
        credential = gen.credentials[spec.tenant]
        writer.write(connect_packet(spec.version, spec.client_id, credential["username"], credential["token"],
                                    gen.cfg.profile["keepAliveSeconds"], clean, expiry, spec.will))
        await writer.drain()
        ptype, _, body = await asyncio.wait_for(read_packet(reader), 10)
        if ptype != 2:
            writer.close()
            raise ProtocolError(f"expected CONNACK, got packet {ptype}")
        present, code = parse_connack(spec.version, body)
        if code != 0 and (spec.version == 4 or code >= 0x80):
            writer.close()
            self.gen.counts.bump("connectRefusals")
            raise ConnectionRefusedError(f"CONNACK reason {code}")
        return reader, writer, present

    async def discard_session(self) -> None:
        """Clean-start connect and DISCONNECT so no session of an earlier run survives (as the capacity cleanup does)."""
        reader, writer, _ = await self.open(True, 0)
        writer.write(disconnect_packet())
        await writer.drain()
        writer.close()

    async def run(self) -> None:
        gen, spec = self.gen, self.spec
        persistent = spec.persistent
        expiry = gen.cfg.profile["sessionExpirySeconds"] if persistent and spec.version == 5 else 0
        backoff = 0.1
        discarded = not persistent
        while not gen.closing:
            try:
                if not discarded:
                    await gen.connect_gate()
                    await self.discard_session()
                    discarded = True
                await gen.connect_gate()
                reader, writer, present = await self.open(clean=not persistent, expiry=expiry)
                if persistent and not self.first_connect and not present:
                    gen.counts.bump("sessionNotPresent")
                if not present or not persistent:
                    self.inbound_qos2.clear()
                self.writer, self.connected, self.epoch = writer, True, self.epoch + 1
                backoff = 0.1
                if not self.first_connect:
                    gen.counts.bump("reconnects5" if spec.version == 5 else "reconnects311")
                resubscribe = spec.role in ("subscriber", "state-subscriber") and (not persistent or not present)
                self.first_connect = False
                if spec.role in ("subscriber", "state-subscriber") and not resubscribe:
                    self.subscribed = True
                await self.serve(reader, writer, resubscribe)
            except asyncio.CancelledError:
                raise
            except ProtocolError:
                gen.counts.bump("protocolErrors")
            except (OSError, asyncio.IncompleteReadError, asyncio.TimeoutError, ssl.SSLError):
                pass
            except Exception:  # a defect in one client must stay visible, not silently end its task
                gen.counts.bump("protocolErrors")
                traceback.print_exc()
            finally:
                self.on_drop()
            if gen.closing:
                break
            await asyncio.sleep(backoff * (0.5 + gen.jitter()))
            backoff = min(backoff * 2, 2.0)

    def on_drop(self) -> None:
        if self.connected and self.times is not None:
            self.times.disc_times.append(time.monotonic())
        was_connected, self.connected, self.subscribed = self.connected, False, False
        writer, self.writer = self.writer, None
        if writer is not None:
            try:
                writer.close()
            except Exception:
                pass
        if self.spec.role == "publisher" and was_connected:
            for packet_id, (seq, stage) in list(self.inflight.items()):
                if stage == "rel" and self.ledger is not None:
                    self.ledger.acknowledged(seq, 2)
            self.inflight.clear()
            self.credit.set()
        if self.suback is not None and not self.suback.done():
            self.suback.cancel()

    async def serve(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, resubscribe: bool) -> None:
        keepalive = self.gen.cfg.profile["keepAliveSeconds"]
        pinger = asyncio.create_task(self.ping_loop(writer, keepalive))
        try:
            if resubscribe:
                asyncio.create_task(self.subscribe(writer))
            while not self.gen.closing:
                ptype, flags, body = await asyncio.wait_for(read_packet(reader), keepalive * 1.5)
                await self.handle(ptype, flags, body, writer)
        finally:
            pinger.cancel()

    async def ping_loop(self, writer: asyncio.StreamWriter, keepalive: int) -> None:
        while True:
            await asyncio.sleep(keepalive / 2)
            writer.write(packet(12, 0, b""))
            await writer.drain()

    async def subscribe(self, writer: asyncio.StreamWriter) -> None:
        spec, epoch = self.spec, self.epoch
        self.suback = asyncio.get_running_loop().create_future()
        # Retained copies can arrive right behind the SUBACK, so the collection starts before the SUBSCRIBE leaves.
        self.retained_got = set()
        started = self.ledger.published_topics() if spec.role == "state-subscriber" else set()
        writer.write(subscribe_packet(spec.version, 1, spec.filter, spec.qos))
        await writer.drain()
        try:
            await asyncio.wait_for(self.suback, 10)
        except (asyncio.CancelledError, asyncio.TimeoutError):
            return
        if epoch != self.epoch or not self.connected:
            return
        self.subscribed = True
        if self.times is not None:
            self.times.suback_times.append(time.monotonic())
        if spec.role == "state-subscriber" and self.gen.steady:
            asyncio.create_task(self.check_retained(epoch, started))

    async def check_retained(self, epoch: int, started: set[str]) -> None:
        await asyncio.sleep(self.gen.cfg.retained_window)
        if epoch != self.epoch or not self.connected:
            return
        got, ended = set(self.retained_got), self.ledger.published_topics()
        self.gen.counts.bump("retainedChecks")
        if started - got or got - ended:
            self.gen.counts.bump("retainedMismatches")

    # packets --------------------------------------------------------------------------------------------
    async def handle(self, ptype: int, flags: int, body: bytes, writer: asyncio.StreamWriter) -> None:
        spec, gen = self.spec, self.gen
        if ptype == 3:
            message = parse_publish(spec.version, flags, body)
            if message["qos"] == 1:
                writer.write(ack_packet(4, message["packetId"]))
            elif message["qos"] == 2:
                writer.write(ack_packet(5, message["packetId"]))
                if message["packetId"] in self.inbound_qos2:
                    return
                self.inbound_qos2.add(message["packetId"])
            self.on_message(message)
        elif ptype == 6:
            packet_id, _ = parse_ack(body)
            self.inbound_qos2.discard(packet_id)
            writer.write(ack_packet(7, packet_id))
        elif ptype == 9:
            _, codes = parse_suback(spec.version, body)
            if self.suback is not None and not self.suback.done():
                if not codes or any(c >= 0x80 for c in codes):
                    gen.counts.bump("protocolErrors")
                self.suback.set_result(codes)
        elif ptype == 4:
            self.on_puback(*parse_ack(body))
        elif ptype == 5:
            self.on_pubrec(*parse_ack(body), writer)
        elif ptype == 7:
            self.inflight.pop(parse_ack(body)[0], None)
            self.credit.set()
        elif ptype in (12, 11):
            pass
        elif ptype == 14:
            raise ConnectionResetError("server DISCONNECT")
        else:
            raise ProtocolError(f"unexpected packet type {ptype}")

    def on_message(self, message: dict[str, Any]) -> None:
        gen, spec = self.gen, self.spec
        payload = message["payload"]
        if payload.startswith(WILL_MAGIC):
            gen.counts.bump("will5Received" if payload[len(WILL_MAGIC):len(WILL_MAGIC) + 1] == b"5" else "will311Received")
            return
        parsed = parse_payload(gen.cfg.profile, payload)
        if parsed is None or parsed[3] != gen.cfg.run_token or parsed[0] != spec.cell or parsed[1] != spec.stream:
            gen.counts.bump("unexpectedDeliveries")
            return
        if message["retain"] and spec.role == "state-subscriber":
            self.retained_got.add(message["topic"])
            return
        self.ledger.deliver(spec.index, parsed[2], message["topic"])

    def on_puback(self, packet_id: int, reason: int) -> None:
        entry = self.inflight.pop(packet_id, None)
        if entry is None:
            return
        if reason >= 0x80:
            self.gen.counts.bump("protocolErrors")
        else:
            self.ledger.acknowledged(entry[0], 1)
        self.credit.set()

    def on_pubrec(self, packet_id: int, reason: int, writer: asyncio.StreamWriter) -> None:
        entry = self.inflight.get(packet_id)
        if entry is None:
            return
        if reason >= 0x80:
            self.inflight.pop(packet_id, None)
            self.gen.counts.bump("protocolErrors")
            self.credit.set()
            return
        self.ledger.acknowledged(entry[0], 1)
        self.inflight[packet_id] = (entry[0], "rel")
        writer.write(ack_packet(6, packet_id))

    # publishing -----------------------------------------------------------------------------------------
    def allocate_packet_id(self) -> int:
        for _ in range(65535):
            packet_id, self.next_packet_id = self.next_packet_id, self.next_packet_id % 65535 + 1
            if packet_id not in self.inflight:
                return packet_id
        raise ProtocolError("no free packet identifier")

    async def publish_loop(self) -> None:
        gen, spec, ledger = self.gen, self.spec, self.ledger
        definition = STREAM_DEFS[spec.stream]
        interval = 1.0 / gen.cfg.rate
        loop = asyncio.get_running_loop()
        scheduled = loop.time() + (spec.cell * CELL_STREAMS + spec.stream) % 997 / 997 * interval
        limit = gen.cfg.profile["maxInflightPerPublisher"]
        ack_timeout = gen.cfg.profile["ackTimeoutMillis"] / 1000
        while not gen.stopping:
            await asyncio.sleep(max(0.0, scheduled - loop.time()))
            woke = loop.time()
            writer = self.writer
            if not self.connected or writer is None:
                scheduled = woke + interval
                continue
            waited = False
            if definition.qos and len(self.inflight) >= limit:
                waited = True
                self.credit.clear()
                try:
                    await asyncio.wait_for(self.credit.wait(), ack_timeout)
                except asyncio.TimeoutError:
                    gen.counts.bump("ackTimeouts")
                    scheduled = loop.time() + interval
                    continue
                if not self.connected or self.writer is None:
                    scheduled = loop.time() + interval
                    continue
                writer = self.writer
            if gen.steady and not waited:
                gen.note_lag((woke - scheduled) * 1000, interval * 1000)
            scheduled += interval
            seq = ledger.next_seq()
            payload = make_payload(gen.cfg.profile, gen.cfg.run_token, spec.cell, spec.stream, seq)
            packet_id = self.allocate_packet_id() if definition.qos else 0
            ledger.published(seq, time.monotonic())
            if definition.qos:
                self.inflight[packet_id] = (seq, "pub")
            try:
                writer.write(publish_packet(spec.version, ledger.topic_of(seq), payload, definition.qos, definition.retained, False, packet_id))
                await writer.drain()
            except (OSError, ConnectionError):
                self.inflight.pop(packet_id, None)


# Generator ---------------------------------------------------------------------------------------------
class Generator:
    def __init__(self, cfg: Config, shard: int):
        self.cfg, self.shard = cfg, shard
        self.counts = Counts()
        self.stopping = False  # publishers stop; clients keep receiving through the settle period
        self.closing = False  # final.json is collected: clients disconnect
        self.steady = False
        self.credentials: dict[str, dict[str, str]] = {}
        self.tls_context: ssl.SSLContext | None = None
        self.clients: list[Client] = []
        self.ledgers: list[Ledger] = []
        self.cells = [c for c in range(cfg.cells) if c % cfg.shards == shard]
        self.cpu_max = 0.0
        self.lag_max = 0.0
        self.interval_ms = 1000.0 / cfg.rate
        self.started = time.time()
        self.next_connect = 0.0
        self.random_state = (cfg.run_token * 2654435761 + shard) & 0xFFFFFFFF

    def jitter(self) -> float:
        self.random_state = (self.random_state * 1664525 + 1013904223) & 0xFFFFFFFF
        return self.random_state / 0xFFFFFFFF

    def note_lag(self, lag_ms: float, interval_ms: float) -> None:
        self.lag_max = max(self.lag_max, lag_ms)
        self.interval_ms = min(self.interval_ms, interval_ms)

    async def connect_gate(self) -> None:
        """Connect rate limit: the profile rate split over the shards."""
        spacing = self.cfg.shards / self.cfg.profile["connectRatePerSecond"]
        now = asyncio.get_running_loop().time()
        self.next_connect = max(self.next_connect, now) + spacing
        await asyncio.sleep(max(0.0, self.next_connect - spacing - now))

    def build(self) -> None:
        credentials = json.loads(Path(self.cfg.credentials).read_text(encoding="utf-8"))
        self.credentials = credentials
        self.tls_context = ssl.create_default_context(cafile=self.cfg.ca_file or None)
        for cell in self.cells:
            specs = build_cell(self.cfg.profile, cell)
            by_stream: dict[int, list[Client]] = {}
            ledgers: dict[int, Ledger] = {}
            times_by_stream: dict[int, list[SubTimes]] = {s: [] for s in range(CELL_STREAMS)}
            for spec in specs:
                if spec.role in ("subscriber", "state-subscriber"):
                    times_by_stream[spec.stream].append(SubTimes(spec.persistent))
            for stream in range(CELL_STREAMS):
                ledgers[stream] = Ledger(STREAM_DEFS[stream], cell, stream, self.cfg.profile, self.counts, times_by_stream[stream])
                self.ledgers.append(ledgers[stream])
            for spec in specs:
                times = times_by_stream[spec.stream][spec.index] if spec.role in ("subscriber", "state-subscriber") else None
                ledger = ledgers[spec.stream] if spec.stream >= 0 else None
                client = Client(self, spec, ledger, times)
                by_stream.setdefault(spec.stream, []).append(client)
                self.clients.append(client)

    def connected_count(self) -> int:
        return sum(1 for c in self.clients if c.connected and (c.spec.role not in ("subscriber", "state-subscriber") or c.subscribed))

    def summary(self, finalized: bool) -> dict[str, Any]:
        saturated = self.cpu_max > self.cfg.cpu_saturation_percent or self.lag_max >= self.interval_ms
        return {"schemaVersion": 1, "kind": "mix-generator-summary", "runToken": self.cfg.run_token, "shard": self.shard,
                "shards": self.cfg.shards, "finalized": finalized, "clientsExpected": len(self.clients),
                "clientsConnected": self.connected_count(), "counts": self.counts.groups, "scalars": self.counts.scalars,
                "saturation": {"cpuPercentMax": round(self.cpu_max, 1), "scheduleLagMaxMillis": round(self.lag_max, 1),
                               "intervalMillis": round(self.interval_ms, 1), "saturated": saturated},
                "startedAt": self.started, "updatedAt": time.time()}

    def write(self, name: str, value: dict[str, Any]) -> None:
        directory = Path(self.cfg.output_dir)
        directory.mkdir(parents=True, exist_ok=True)
        temporary = directory / (name + ".tmp")
        temporary.write_text(json.dumps(value), encoding="utf-8")
        os.replace(temporary, directory / name)

    async def monitor(self) -> None:
        """Every ``monitor_interval`` (5 s): finalize old sequences, track CPU, rewrite the progress summary, watch the stop file."""
        last_wall, last_cpu = time.monotonic(), time.process_time()
        stop_file = Path(self.cfg.control_dir) / "stop"
        while not self.stopping:
            await asyncio.sleep(self.cfg.monitor_interval)
            wall, cpu = time.monotonic(), time.process_time()
            if self.steady:
                self.cpu_max = max(self.cpu_max, (cpu - last_cpu) / max(wall - last_wall, 1e-6) * 100)
            last_wall, last_cpu = wall, cpu
            for ledger in self.ledgers:
                ledger.finalize(time.monotonic(), self.cfg.horizon)
            self.write("summary.json", self.summary(False))
            if stop_file.exists():
                self.stopping = True

    async def wait_connected(self) -> None:
        deadline = time.monotonic() + self.cfg.setup_timeout
        while self.connected_count() < len(self.clients):
            if time.monotonic() > deadline:
                raise TimeoutError(f"{self.connected_count()} of {len(self.clients)} clients connected after {self.cfg.setup_timeout:.0f} s")
            await asyncio.sleep(1)

    async def main(self) -> int:
        self.build()
        loop_tasks = [asyncio.create_task(client.run()) for client in self.clients]
        monitor = asyncio.create_task(self.monitor())
        publishers = []
        try:
            await self.wait_connected()
            await asyncio.sleep(2)
            self.steady = True
            for client in self.clients:
                if client.spec.role == "publisher":
                    publishers.append(asyncio.create_task(client.publish_loop()))
            self.write("ready.json", self.summary(False))
            while not self.stopping:
                await asyncio.sleep(1)
            await asyncio.gather(*publishers, return_exceptions=True)
            await asyncio.sleep(self.cfg.settle)
        except Exception as error:
            self.write("failed.json", {"error": f"{type(error).__name__}: {error}", "summary": self.summary(False)})
            raise
        finally:
            monitor.cancel()
        for ledger in self.ledgers:
            ledger.finalize(time.monotonic(), self.cfg.horizon, force=True)
        self.write("final.json", self.summary(True))
        collected = Path(self.cfg.control_dir) / "collected"
        while not collected.exists():
            await asyncio.sleep(2)
        self.closing = True
        for client in self.clients:
            if client.writer is not None:
                try:
                    client.writer.write(disconnect_packet())
                except Exception:
                    pass
        for task in loop_tasks:
            task.cancel()
        return 0


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    if len(args) != 2 or args[0] != "--config":
        print("usage: mix_generator.py --config <config.json>", file=sys.stderr)
        return 2
    cfg = Config.load(args[1])
    shard = int(os.environ.get("JOB_COMPLETION_INDEX", "0"))
    if not 0 <= shard < cfg.shards:
        print(f"JOB_COMPLETION_INDEX {shard} is outside 0..{cfg.shards - 1}", file=sys.stderr)
        return 2
    return asyncio.run(Generator(cfg, shard).main())


if __name__ == "__main__":
    raise SystemExit(main())
