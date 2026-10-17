"""Self-contained MQTT 3.1.1 CONNECT prober (standard library only).

The same source runs in two places: imported by the harness for the direct probe through a Pod port-forward, and passed
as ``python -c`` text into the helper Pod inside the VPC for the NLB-routed batch (the Service NLB is ``internal``, so the
workstation cannot reach it). It reads one JSON spec line from stdin, so credentials never appear in an argument list,
and prints one JSON document. It never echoes the password.

Packets are built by hand: CONNECT = fixed header 0x10, protocol name ``MQTT``, level 4, clean session, optional
username and password; CONNACK = 0x20 0x02 <flags> <return code>. A successful connect is followed by DISCONNECT.
"""

import json
import socket
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

RETURN_CODES = {0: "accepted", 1: "unacceptable_protocol_version", 2: "identifier_rejected",
                3: "server_unavailable", 4: "bad_credentials", 5: "not_authorized"}


def encode_length(value):
    out = bytearray()
    while True:
        digit, value = value % 128, value // 128
        out.append(digit | 0x80 if value else digit)
        if not value:
            return bytes(out)


def utf8(text):
    data = text.encode("utf-8")
    return len(data).to_bytes(2, "big") + data


def build_connect(client_id, username=None, password=None, keepalive=30):
    """MQTT 3.1.1 CONNECT with clean session; a password requires a username."""
    if password is not None and username is None:
        raise ValueError("MQTT 3.1.1 cannot send a password without a username")
    flags = 0x02 | (0x80 if username is not None else 0) | (0x40 if password is not None else 0)
    body = utf8("MQTT") + b"\x04" + bytes([flags]) + int(keepalive).to_bytes(2, "big") + utf8(client_id)
    if username is not None:
        body += utf8(username)
    if password is not None:
        body += utf8(password)
    return b"\x10" + encode_length(len(body)) + body


def classify(received, error=None):
    """Outcome of one CONNECT: ``accepted`` is true only for CONNACK return code 0."""
    if len(received) >= 4 and received[0] == 0x20 and received[1] == 2:
        code = received[3]
        return {"kind": RETURN_CODES.get(code, "return_code_%d" % code), "accepted": code == 0 and received[2] in (0, 1),
                "returnCode": code, "hex": received[:4].hex(), "error": None}
    if received:
        return {"kind": "malformed_response", "accepted": False, "returnCode": None, "hex": received[:16].hex(), "error": error}
    return {"kind": error or "closed_without_connack", "accepted": False, "returnCode": None, "hex": "", "error": error}


def connect_once(address, port, client_id, username=None, password=None, timeout=5.0):
    started = time.monotonic()
    received, error = b"", None
    try:
        with socket.create_connection((address, port), timeout) as conn:
            conn.settimeout(timeout)
            conn.sendall(build_connect(client_id, username, password))
            while len(received) < 4:
                chunk = conn.recv(4 - len(received))
                if not chunk:
                    break
                received += chunk
            if classify(received)["accepted"]:
                try:
                    conn.sendall(b"\xe0\x00")
                except OSError:
                    pass
    except ConnectionRefusedError:
        error = "connection_refused"
    except socket.timeout:
        error = "timeout"
    except ConnectionResetError:
        error = "connection_reset"
    except OSError as exc:
        error = "os_error_%s" % type(exc).__name__
    outcome = classify(received, error)
    outcome.update({"address": address, "clientId": client_id, "elapsedMs": round((time.monotonic() - started) * 1000)})
    return outcome


def run_batch(spec):
    """Connect ``spec['count']`` times, round-robin over every A record of ``spec['host']`` (one per NLB zone)."""
    infos = socket.getaddrinfo(spec["host"], spec["port"], socket.AF_INET, socket.SOCK_STREAM)
    addresses = sorted({info[4][0] for info in infos})
    tag = uuid.uuid4().hex[:8]

    def one(index):
        return connect_once(addresses[index % len(addresses)], spec["port"], "%s-%s-%d" % (spec.get("prefix", "nlb"), tag, index),
                            spec.get("username"), spec.get("password"), float(spec.get("timeout", 5.0)))

    with ThreadPoolExecutor(max_workers=int(spec.get("workers", 8))) as pool:
        outcomes = list(pool.map(one, range(int(spec["count"]))))
    return {"addresses": addresses, "outcomes": outcomes}


def main():
    spec = json.loads(sys.stdin.readline())
    json.dump(run_batch(spec), sys.stdout)
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()
