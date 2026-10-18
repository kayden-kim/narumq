"""WAL frame model for the torn-tail injection: byte crafting and the ADR-0023 tail classifier.

`classify` mirrors `readWALMode` in `internal/adapters/raft/stable_store.go`. It checks the frame
structure and CRC only; it cannot decode records. The committed vectors
(`internal/adapters/raft/testdata/wal-tail-vectors.json`) are produced here from frames that the Go
decoder accepts, and `TestWALTailVectorsMatchGoReader` replays them against the real reader.

File layout: `NMQWAL01`, then frames. A frame is `length (u32 BE) | CRC-32C of payload (u32 BE) | payload`.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import re
import struct

MAGIC = b"NMQWAL01"
HEADER_SIZE = 8
SECTOR = 512
MAX_FRAME_PAYLOAD = 2 * 1024 * 1024
MAX_DYNAMIC_FRAME_PAYLOAD = 64 * 1024 * 1024 + 2 * 1024 * 1024 + 1024
VECTOR_FORMAT = 1
VECTORS_PATH = (Path(__file__).resolve().parents[3] / "internal" / "adapters" / "raft" / "testdata"
                / "wal-tail-vectors.json")

# Stable reason strings; they equal the Go error text after the `corrupt raft WAL: ` prefix.
REASON_NO_EVIDENCE = "final WAL frame checksum mismatch without torn-write evidence"
REASON_MIDDLE = "middle WAL frame checksum mismatch"
REASON_ZERO_LENGTH = "zero-length WAL frame"
REASON_OVERSIZED = "exceeds"
GO_ERROR_NAME = "corrupt raft WAL"


class CraftError(ValueError):
    """No byte variant of the case gives the outcome ADR-0023 requires for this WAL."""


def _crc32c_table() -> list[int]:
    table = []
    for value in range(256):
        for _ in range(8):
            value = (value >> 1) ^ 0x82F63B78 if value & 1 else value >> 1
        table.append(value)
    return table


_TABLE = _crc32c_table()


def crc32c(data: bytes) -> int:
    crc = 0xFFFFFFFF
    for byte in data:
        crc = _TABLE[(crc ^ byte) & 0xFF] ^ (crc >> 8)
    return crc ^ 0xFFFFFFFF


def frame(payload: bytes) -> bytes:
    return struct.pack(">II", len(payload), crc32c(payload)) + payload


def bad_crc_frame(payload: bytes) -> bytes:
    """A frame whose stored checksum is the true checksum with every bit flipped."""
    return struct.pack(">II", len(payload), crc32c(payload) ^ 0xFFFFFFFF) + payload


# ---- valid records the Go decoder accepts --------------------------------------------------------------------

def _varint(value: int) -> bytes:
    out = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        if value:
            out.append(byte | 0x80)
        else:
            out.append(byte)
            return bytes(out)


def hard_state_payload(term: int, vote: int, commit: int) -> bytes:
    """Version 1, flags 1 (HardState), two reserved bytes, three u64, zero entries."""
    return b"\x01\x01\x00\x00" + struct.pack(">QQQ", term, vote, commit) + struct.pack(">I", 0)


def entry_payload(index: int, term: int, data: bytes) -> bytes:
    """Version 1, no flags, one protobuf `Entry{Type: Normal, Term, Index, Data}`."""
    entry = b"\x08\x00" + b"\x10" + _varint(term) + b"\x18" + _varint(index) + b"\x22" + _varint(len(data)) + data
    return b"\x01\x00\x00\x00" + struct.pack(">I", 1) + struct.pack(">I", len(entry)) + entry


def base_wal(frames: int = 4, align_to: int | None = None) -> bytes:
    """A valid WAL: `frames` HardState frames. With `align_to`, one more entry frame is sized so that the file
    length modulo 512 equals `align_to`; the frame start of anything appended then has that alignment."""
    data = bytearray(MAGIC)
    for number in range(frames):
        data += frame(hard_state_payload(1, 1, number))
    if align_to is None:
        return bytes(data)
    for size in range(0, 2 * SECTOR):
        candidate = frame(entry_payload(1000, 1, b"x" * size))
        if (len(data) + len(candidate)) % SECTOR == align_to % SECTOR:
            return bytes(data) + candidate
    raise CraftError("alignment unreachable")


# ---- classifier (mirror of readWALMode) -----------------------------------------------------------------------

def _all_zero(data: bytes, start: int, end: int) -> bool:
    return not any(data[start:end])


def _frame_has_zero_sector(data: bytes, start: int, end: int) -> bool:
    while start < end:
        boundary = min((start // SECTOR + 1) * SECTOR, end)
        if not any(data[start:boundary]):
            return True
        start = boundary
    return False


def classify(data: bytes, dynamic: bool = True) -> dict:
    """`outcome` is `ok` (nothing to cut), `truncate` (the reader cuts at `validBytes`) or `fail` (it refuses)."""
    size = len(data)
    if size == 0:
        return {"outcome": "ok", "validBytes": 0, "frames": 0, "reason": ""}
    if size < len(MAGIC):
        return {"outcome": "fail", "validBytes": 0, "frames": 0, "reason": "truncated WAL magic"}
    if data[:len(MAGIC)] != MAGIC:
        return {"outcome": "fail", "validBytes": 0, "frames": 0, "reason": "invalid WAL magic"}
    limit = MAX_DYNAMIC_FRAME_PAYLOAD if dynamic else MAX_FRAME_PAYLOAD
    offset = valid = len(MAGIC)
    frames = 0

    def result(outcome: str, reason: str) -> dict:
        return {"outcome": outcome, "validBytes": valid, "frames": frames, "reason": reason}

    while offset < size:
        if size - offset < HEADER_SIZE:
            return result("truncate", "tail shorter than a frame header")
        length, checksum = struct.unpack_from(">II", data, offset)
        if length == 0:
            if _all_zero(data, offset, size):
                return result("truncate", "zero-filled tail")
            return result("fail", REASON_ZERO_LENGTH)
        if length > limit:
            return result("fail", f"WAL frame length {length} exceeds {limit}")
        end = offset + HEADER_SIZE + length
        if end > size:
            return result("truncate", "frame payload shorter than its length")
        if crc32c(data[offset + HEADER_SIZE:end]) != checksum:
            final = end == size or _all_zero(data, end, size)
            if not final:
                return result("fail", REASON_MIDDLE)
            if _frame_has_zero_sector(data, offset, end):
                return result("truncate", "final frame checksum mismatch with a zero sector")
            return result("fail", REASON_NO_EVIDENCE)
        valid = offset = end
        frames += 1
    return result("ok", "")


def frame_offsets(data: bytes) -> list[tuple[int, int]]:
    """(start, end) of every complete CRC-valid frame before the first anomaly."""
    spans: list[tuple[int, int]] = []
    offset = len(MAGIC)
    while offset + HEADER_SIZE <= len(data):
        length, checksum = struct.unpack_from(">II", data, offset)
        end = offset + HEADER_SIZE + length
        if length == 0 or end > len(data) or crc32c(data[offset + HEADER_SIZE:end]) != checksum:
            break
        spans.append((offset, end))
        offset = end
    return spans


# ---- cases ----------------------------------------------------------------------------------------------------

NONZERO = bytes((index % 251) + 1 for index in range(4096))  # never contains a zero byte


def _bogus_payload(length: int) -> bytes:
    return NONZERO[:length]


def _flip(data: bytes, position: int) -> bytes:
    changed = bytearray(data)
    changed[position] ^= 0xFF
    return bytes(changed)


def _append_short_header(base: bytes) -> bytes:
    return base + b"\x00\x00\x00\x20\xaa"


def _append_partial_payload(base: bytes) -> bytes:
    payload = _bogus_payload(96)
    return base + frame(payload)[:HEADER_SIZE + 40]


def _append_zero_tail(base: bytes) -> bytes:
    return base + bytes(4096)


def _append_crc_zero_sector(base: bytes) -> bytes:
    payload = _bogus_payload(600) + bytes(600)
    return base + bad_crc_frame(payload)


def _append_crc_zero_sector_padded(base: bytes) -> bytes:
    return _append_crc_zero_sector(base) + bytes(1024)


def _append_no_evidence(base: bytes) -> bytes:
    return base + bad_crc_frame(_bogus_payload(1200))


def _flip_last_frame(base: bytes) -> bytes:
    start, end = frame_offsets(base)[-1]
    return _flip(base, end - 1)


def _append_no_evidence_padded(base: bytes) -> bytes:
    return _append_no_evidence(base) + bytes(1024)


def _flip_last_frame_padded(base: bytes) -> bytes:
    return _flip_last_frame(base) + bytes(1024)


def _flip_middle_frame(base: bytes) -> bytes:
    spans = frame_offsets(base)
    if len(spans) < 2:
        raise CraftError("middle corruption needs at least two frames")
    return _flip(base, spans[-2][1] - 1)


def _append_zero_length_nonzero(base: bytes) -> bytes:
    return base + bytes(8) + b"\x01"


def _append_oversized(base: bytes) -> bytes:
    return base + struct.pack(">II", MAX_DYNAMIC_FRAME_PAYLOAD + 1, 0)


# name -> (group, expected outcome, expected reason, byte variants tried in order)
CASES: dict[str, dict] = {
    "short-header": {"group": "T1", "expect": "truncate", "reason": "", "variants": [_append_short_header],
                     "describe": "5 bytes after the last frame, shorter than a frame header"},
    "partial-payload": {"group": "T1", "expect": "truncate", "reason": "", "variants": [_append_partial_payload],
                        "describe": "frame header (length 96) followed by only 40 payload bytes"},
    "zero-tail": {"group": "T1", "expect": "truncate", "reason": "", "variants": [_append_zero_tail],
                  "describe": "4096 zero bytes after the last frame"},
    "crc-final-zero-sector": {"group": "T1", "expect": "truncate", "reason": "",
                              "variants": [_append_crc_zero_sector],
                              "describe": "final frame with a wrong CRC whose last 600 payload bytes are zero"},
    "crc-final-zero-sector-padded": {"group": "T1", "expect": "truncate", "reason": "",
                                     "variants": [_append_crc_zero_sector_padded],
                                     "describe": "the same frame followed by 1024 zero padding bytes"},
    "final-crc-no-zero-evidence": {"group": "T2", "expect": "fail", "reason": REASON_NO_EVIDENCE,
                                   "variants": [_append_no_evidence, _flip_last_frame],
                                   "describe": "final frame with a wrong CRC and no zero sector in it"},
    "crc-zero-padding-no-evidence": {"group": "T2", "expect": "fail", "reason": REASON_NO_EVIDENCE,
                                     "variants": [_append_no_evidence_padded, _flip_last_frame_padded],
                                     "describe": "wrong-CRC final frame without a zero sector, then zero padding"},
    "middle-crc": {"group": "T2", "expect": "fail", "reason": REASON_MIDDLE, "variants": [_flip_middle_frame],
                   "describe": "one byte of the second-to-last frame flipped; valid frames follow it"},
    "zero-length-frame": {"group": "T2", "expect": "fail", "reason": REASON_ZERO_LENGTH,
                          "variants": [_append_zero_length_nonzero],
                          "describe": "zero-length frame header followed by a non-zero byte"},
    "oversized-frame": {"group": "T2", "expect": "fail", "reason": REASON_OVERSIZED, "variants": [_append_oversized],
                        "describe": "frame length above the dynamic WAL frame limit"},
}
DEFAULT_T1 = ("short-header", "zero-tail")
DEFAULT_T2 = ("final-crc-no-zero-evidence", "middle-crc")


def craft(name: str, base: bytes, dynamic: bool = True) -> tuple[bytes, dict]:
    """Return the new WAL bytes and a description. The first variant that gives the case's outcome wins."""
    case = CASES[name]
    if classify(base, dynamic)["outcome"] != "ok" or len(base) <= len(MAGIC):
        raise CraftError("the base WAL must be a non-empty, fully valid WAL")
    tried = []
    for variant in case["variants"]:
        try:
            changed = variant(base)
        except CraftError:
            continue
        verdict = classify(changed, dynamic)
        tried.append({"variant": variant.__name__.lstrip("_"), "outcome": verdict["outcome"]})
        if verdict["outcome"] == case["expect"] and case["reason"] in verdict["reason"]:
            return changed, {"case": name, "group": case["group"], "expect": case["expect"],
                             "reason": case["reason"], "variant": variant.__name__.lstrip("_"),
                             "baseBytes": len(base), "newBytes": len(changed), "validBytes": verdict["validBytes"],
                             "classifierReason": verdict["reason"], "tried": tried}
    raise CraftError(f"no byte variant of {name} gives {case['expect']} for this WAL: {tried}")


# ---- committed Go cross-check vectors -------------------------------------------------------------------------

VECTOR_ALIGNMENTS = (None, 0, 255, 509, 510, 511)
ZERO_RUN = 16


def encode_parts(data: bytes) -> list[dict]:
    """Split into `{"hex": ...}` runs and `{"zeros": n}` runs of at least ZERO_RUN zero bytes (keeps the file small)."""
    parts: list[dict] = []
    position = 0
    for match in re.finditer(rb"\x00{%d,}" % ZERO_RUN, data):
        if match.start() > position:
            parts.append({"hex": data[position:match.start()].hex()})
        parts.append({"zeros": match.end() - match.start()})
        position = match.end()
    if position < len(data):
        parts.append({"hex": data[position:].hex()})
    return parts


def decode_parts(parts: list[dict]) -> bytes:
    return b"".join(bytes(part["zeros"]) if "zeros" in part else bytes.fromhex(part["hex"]) for part in parts)


def vectors() -> dict:
    """Deterministic vectors: every case over several base alignments, plus the untouched base."""
    items = []
    for align in VECTOR_ALIGNMENTS:
        base = base_wal(3, align)
        verdict = classify(base)
        items.append({"name": f"valid/align-{align}", "wal": encode_parts(base), "outcome": verdict["outcome"],
                      "validBytes": verdict["validBytes"], "frames": verdict["frames"], "reason": ""})
        for name in CASES:
            changed, info = craft(name, base)
            verdict = classify(changed)
            items.append({"name": f"{name}/align-{align}", "wal": encode_parts(changed), "outcome": verdict["outcome"],
                          "validBytes": verdict["validBytes"], "frames": verdict["frames"],
                          "reason": CASES[name]["reason"]})
    return {"format": VECTOR_FORMAT,
            "note": "Generated by tests/chaos/eks-wal-torn-tail/wal_frames.py; do not edit by hand.",
            "vectors": items}


def render_vectors() -> str:
    return json.dumps(vectors(), indent=1, sort_keys=True) + "\n"


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--write-vectors", action="store_true", help=f"rewrite {VECTORS_PATH}")
    args = parser.parse_args(argv)
    if args.write_vectors:
        VECTORS_PATH.parent.mkdir(parents=True, exist_ok=True)
        VECTORS_PATH.write_text(render_vectors(), encoding="utf-8", newline="\n")
        print(f"wrote {VECTORS_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
