"""Independent NMQBKUP3 archive reader for the backup/DR acceptance runs.

The reader mirrors the container checks of `records.DecodeBackupArchive` (magic, trailer SHA-256, per-frame
CRC32C, manifest identity fields, entry count, no trailing bytes). It does not decode the typed snapshot or the
entries; the broker's own restore CLI does that during the offline seed stage. Only hashes and numbers are
returned, never payload bytes.
"""
from __future__ import annotations

import hashlib
import struct

MAGIC = b"NMQBKUP3"
TRAILER_BYTES = 32
MIN_ARCHIVE_BYTES = 40
MAX_ARCHIVE_BYTES = 1 << 30
MAX_MANIFEST_BYTES = 64 * 1024
FRAME_HEADER_BYTES = 8


class ArchiveError(ValueError):
    """The archive is not a valid NMQBKUP3 container; the message is a stable lowercase code."""


def _crc_table() -> tuple[int, ...]:
    table = []
    for value in range(256):
        for _ in range(8):
            value = (value >> 1) ^ 0x82F63B78 if value & 1 else value >> 1
        table.append(value)
    return tuple(table)


_TABLE = _crc_table()


def crc32c(data: bytes) -> int:
    value = 0xFFFFFFFF
    for byte in data:
        value = _TABLE[(value ^ byte) & 0xFF] ^ (value >> 8)
    return value ^ 0xFFFFFFFF


def _varint(data: bytes, offset: int) -> tuple[int, int]:
    result = shift = 0
    while True:
        if offset >= len(data) or shift > 63:
            raise ArchiveError("manifest_varint_truncated")
        byte = data[offset]
        offset += 1
        result |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return result, offset
        shift += 7


def decode_message(payload: bytes) -> dict[int, list]:
    """Decode one protobuf message into {field number: [values]}; varints are ints, others bytes."""
    fields: dict[int, list] = {}
    offset = 0
    while offset < len(payload):
        key, offset = _varint(payload, offset)
        number, wire = key >> 3, key & 7
        if number == 0:
            raise ArchiveError("manifest_field_zero")
        if wire == 0:
            value, offset = _varint(payload, offset)
        elif wire == 2:
            length, offset = _varint(payload, offset)
            if offset + length > len(payload):
                raise ArchiveError("manifest_field_truncated")
            value, offset = payload[offset:offset + length], offset + length
        elif wire == 1:
            if offset + 8 > len(payload):
                raise ArchiveError("manifest_field_truncated")
            value, offset = payload[offset:offset + 8], offset + 8
        elif wire == 5:
            if offset + 4 > len(payload):
                raise ArchiveError("manifest_field_truncated")
            value, offset = payload[offset:offset + 4], offset + 4
        else:
            raise ArchiveError("manifest_wire_type_unsupported")
        fields.setdefault(number, []).append(value)
    return fields


def _scalar(fields: dict, number: int) -> int:
    values = fields.get(number, [])
    if not values:
        return 0
    if len(values) != 1 or not isinstance(values[0], int):
        raise ArchiveError(f"manifest_field_{number}_invalid")
    return values[0]


def _text(fields: dict, number: int) -> str:
    values = fields.get(number, [])
    if len(values) != 1 or not isinstance(values[0], bytes):
        raise ArchiveError(f"manifest_field_{number}_invalid")
    try:
        return values[0].decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ArchiveError(f"manifest_field_{number}_invalid") from exc


def _ids(fields: dict, number: int) -> list[int]:
    """A repeated uint64 field, packed (proto3 default) or unpacked."""
    result: list[int] = []
    for value in fields.get(number, []):
        if isinstance(value, int):
            result.append(value)
            continue
        offset = 0
        while offset < len(value):
            item, offset = _varint(value, offset)
            result.append(item)
    return result


def _frame(data: bytes, offset: int, end: int, maximum: int, index: int) -> tuple[bytes, int]:
    if end - offset < FRAME_HEADER_BYTES:
        raise ArchiveError(f"frame_{index}_truncated")
    length, checksum = struct.unpack(">II", data[offset:offset + FRAME_HEADER_BYTES])
    offset += FRAME_HEADER_BYTES
    if length > maximum or offset + length > end:
        raise ArchiveError(f"frame_{index}_length_invalid")
    payload = data[offset:offset + length]
    if crc32c(payload) != checksum:
        raise ArchiveError(f"frame_{index}_checksum_mismatch")
    return payload, offset + length


def parse_archive(data: bytes) -> dict:
    """Validate the container and return manifest facts. Raises ArchiveError with a stable code."""
    if data[:8] in (b"NMQBKUP1", b"NMQBKUP2"):
        raise ArchiveError("archive_format_unsupported")
    if len(data) < MIN_ARCHIVE_BYTES or len(data) > MAX_ARCHIVE_BYTES:
        raise ArchiveError("archive_size_out_of_range")
    if data[:8] != MAGIC:
        raise ArchiveError("archive_magic_mismatch")
    end = len(data) - TRAILER_BYTES
    body_sha = hashlib.sha256(data[:end]).digest()
    if data[end:] != body_sha:
        raise ArchiveError("archive_trailer_sha256_mismatch")
    manifest_bytes, offset = _frame(data, 8, end, MAX_MANIFEST_BYTES, 0)
    manifest = decode_message(manifest_bytes)
    facts = {
        "formatMajor": _scalar(manifest, 1), "formatMinor": _scalar(manifest, 2),
        "clusterId": _text(manifest, 3), "sourceMemberId": _scalar(manifest, 4),
        "producerRelease": _text(manifest, 5),
        "clusterProtocol": f"{_scalar(manifest, 6)}.{_scalar(manifest, 7)}",
        "stateSchema": f"{_scalar(manifest, 8)}.{_scalar(manifest, 9)}",
        "snapshotIndex": _scalar(manifest, 11), "snapshotTerm": _scalar(manifest, 12),
        "commitIndex": _scalar(manifest, 13), "commitTerm": _scalar(manifest, 14),
        "voterIds": _ids(manifest, 15), "learnerIds": _ids(manifest, 16), "entryCount": _scalar(manifest, 17),
    }
    if (facts["formatMajor"], facts["formatMinor"]) != (3, 0):
        raise ArchiveError("archive_format_version_unsupported")
    if facts["clusterProtocol"] != "2.0" or facts["stateSchema"] != "1.0":
        raise ArchiveError("archive_protocol_or_schema_unsupported")
    if not facts["snapshotIndex"] or facts["snapshotIndex"] > facts["commitIndex"] or not facts["snapshotTerm"] \
            or facts["snapshotTerm"] > facts["commitTerm"]:
        raise ArchiveError("archive_commit_boundary_invalid")
    if not facts["voterIds"] or facts["voterIds"] != sorted(set(facts["voterIds"])):
        raise ArchiveError("archive_voters_invalid")
    _, offset = _frame(data, offset, end, MAX_ARCHIVE_BYTES, 1)  # typed snapshot: container check only
    for number in range(facts["entryCount"]):
        _, offset = _frame(data, offset, end, MAX_ARCHIVE_BYTES, 2 + number)
    if offset != end:
        raise ArchiveError("archive_trailing_bytes")
    facts.update(sizeBytes=len(data), bodySha256=body_sha.hex(), fileSha256=hashlib.sha256(data).hexdigest(),
                 frames=2 + facts["entryCount"], magic=MAGIC.decode("ascii"))
    return facts
