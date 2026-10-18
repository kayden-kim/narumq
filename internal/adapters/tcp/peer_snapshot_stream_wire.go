package tcp

import (
	"crypto/sha256"
	"encoding/binary"
	"hash/crc32"
)

// Raft snapshot stream frames (docs/contracts/raft-snapshot-stream-v1.md). Every frame is a
// length-prefixed payload whose first byte is the frame kind.
const (
	dynamicSnapshotMagic = "NMQSNAP1"

	snapshotFrameManifest byte = 0x01
	snapshotFrameChunk    byte = 0x02
	snapshotFrameDone     byte = 0x03
	snapshotFrameAccept   byte = 0x81
	snapshotFrameAck      byte = 0x82
	snapshotFrameComplete byte = 0x83
	snapshotFrameError    byte = 0x8F

	snapshotManifestSize    = 1 + 8 + 8 + 8 + sha256.Size
	snapshotChunkHeaderSize = 1 + 8 + 4
	snapshotOffsetFrameSize = 1 + 8

	// snapshotStreamFrameLimit is the frame payload limit; it equals the Raft frame limit.
	snapshotStreamFrameLimit = dynamicFrameLimit
	// snapshotStreamChunkSize is the chunk payload size a sender uses.
	snapshotStreamChunkSize = 512 << 10
	// snapshotStreamMaxBytes is maxSnapshotPayload (64 MiB) plus the MsgSnap envelope allowance. The
	// Raft adapter enforces the same bound (maxDynamicRaftSnapshotSize).
	snapshotStreamMaxBytes = 64<<20 + 1024
)

// Snapshot stream error codes carried by an ERROR frame.
const (
	snapshotErrorBusy            byte = 1
	snapshotErrorInvalidManifest byte = 2
	snapshotErrorOutOfOrder      byte = 3
	snapshotErrorChecksum        byte = 4
	snapshotErrorDigest          byte = 5
	snapshotErrorOverflow        byte = 6
	snapshotErrorRejected        byte = 7
	snapshotErrorUnsupported     byte = 8
)

// Snapshot stream failure reasons. They are the stable `reason` values of the failure log.
const (
	SnapshotReasonBusy            = "busy"
	SnapshotReasonInvalidManifest = "invalid_manifest"
	SnapshotReasonOutOfOrder      = "out_of_order"
	SnapshotReasonChecksum        = "checksum"
	SnapshotReasonDigest          = "digest"
	SnapshotReasonOverflow        = "overflow"
	SnapshotReasonRejected        = "rejected"
	SnapshotReasonUnsupported     = "unsupported"
	SnapshotReasonTimeout         = "timeout"
	SnapshotReasonCanceled        = "canceled"
	SnapshotReasonConnection      = "connection"
	SnapshotReasonBackpressure    = "backpressure"
)

var snapshotCRCTable = crc32.MakeTable(crc32.Castagnoli)

func snapshotErrorReason(code byte) string {
	switch code {
	case snapshotErrorBusy:
		return SnapshotReasonBusy
	case snapshotErrorInvalidManifest:
		return SnapshotReasonInvalidManifest
	case snapshotErrorOutOfOrder:
		return SnapshotReasonOutOfOrder
	case snapshotErrorChecksum:
		return SnapshotReasonChecksum
	case snapshotErrorDigest:
		return SnapshotReasonDigest
	case snapshotErrorOverflow:
		return SnapshotReasonOverflow
	case snapshotErrorRejected:
		return SnapshotReasonRejected
	case snapshotErrorUnsupported:
		return SnapshotReasonUnsupported
	default:
		return SnapshotReasonConnection
	}
}

type snapshotManifest struct {
	index  uint64
	term   uint64
	total  uint64
	digest [sha256.Size]byte
}

func (manifest snapshotManifest) valid() bool {
	return manifest.index != 0 && manifest.term != 0 && manifest.total != 0 && manifest.total <= snapshotStreamMaxBytes
}

func encodeSnapshotManifest(manifest snapshotManifest) []byte {
	payload := make([]byte, 0, snapshotManifestSize)
	payload = append(payload, snapshotFrameManifest)
	payload = appendUint64(payload, manifest.index)
	payload = appendUint64(payload, manifest.term)
	payload = appendUint64(payload, manifest.total)
	return append(payload, manifest.digest[:]...)
}

func decodeSnapshotManifest(payload []byte) (snapshotManifest, bool) {
	if len(payload) != snapshotManifestSize || payload[0] != snapshotFrameManifest {
		return snapshotManifest{}, false
	}
	manifest := snapshotManifest{
		index: binary.BigEndian.Uint64(payload[1:9]),
		term:  binary.BigEndian.Uint64(payload[9:17]),
		total: binary.BigEndian.Uint64(payload[17:25]),
	}
	copy(manifest.digest[:], payload[25:])
	return manifest, manifest.valid()
}

// encodeSnapshotChunk writes one CHUNK into buffer (reused by the sender) and returns the payload.
func encodeSnapshotChunk(buffer []byte, offset uint64, data []byte) []byte {
	buffer = append(buffer[:0], snapshotFrameChunk)
	buffer = appendUint64(buffer, offset)
	var checksum [4]byte
	binary.BigEndian.PutUint32(checksum[:], crc32.Checksum(data, snapshotCRCTable))
	buffer = append(buffer, checksum[:]...)
	return append(buffer, data...)
}

// decodeSnapshotChunk splits a CHUNK payload. The returned data aliases payload.
func decodeSnapshotChunk(payload []byte) (offset uint64, checksum uint32, data []byte, ok bool) {
	if len(payload) <= snapshotChunkHeaderSize || payload[0] != snapshotFrameChunk {
		return 0, 0, nil, false
	}
	return binary.BigEndian.Uint64(payload[1:9]), binary.BigEndian.Uint32(payload[9:13]), payload[snapshotChunkHeaderSize:], true
}

func encodeSnapshotOffsetFrame(kind byte, offset uint64) []byte {
	return appendUint64([]byte{kind}, offset)
}

func decodeSnapshotOffsetFrame(payload []byte, kind byte) (uint64, bool) {
	if len(payload) != snapshotOffsetFrameSize || payload[0] != kind {
		return 0, false
	}
	return binary.BigEndian.Uint64(payload[1:]), true
}
