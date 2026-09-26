package records

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"sort"

	"google.golang.org/protobuf/proto"
)

const (
	backupMagic            = "NMQBKUP3"
	maxBackupManifestBytes = 64 << 10
	maxBackupEntryBytes    = maxCommandBytes + 1024
	maxBackupEntries       = 1_000_000
)

// BackupFormatMajor is the supported cluster backup format major version.
const BackupFormatMajor = 3

// BackupFormatMinor is the supported cluster backup format minor version.
const BackupFormatMinor = 0

// MaxBackupArchiveBytes is the maximum encoded cluster backup size.
const MaxBackupArchiveBytes = 1 << 30

var (
	backupCRCTable = crc32.MakeTable(crc32.Castagnoli)
)

// BackupArchive contains a typed snapshot and every committed record after it.
type BackupArchive struct {
	Manifest *BackupManifest
	Snapshot []byte
	Entries  []*BackupEntry
}

// EncodeBackupArchive validates and encodes one checksummed cluster backup archive.
func EncodeBackupArchive(archive *BackupArchive) ([]byte, [sha256.Size]byte, error) {
	if archive == nil || archive.Manifest == nil {
		return nil, [sha256.Size]byte{}, backupArchiveError("manifest is required")
	}
	if err := rejectUnknown(archive.Manifest.ProtoReflect()); err != nil {
		return nil, [sha256.Size]byte{}, backupArchiveError("manifest: %v", err)
	}
	if err := validateBackupManifest(archive.Manifest); err != nil {
		return nil, [sha256.Size]byte{}, err
	}
	snapshot, err := UnmarshalSnapshot(archive.Snapshot)
	if err != nil {
		return nil, [sha256.Size]byte{}, backupArchiveError("snapshot: %v", err)
	}
	if snapshot.LastIncludedIndex != archive.Manifest.SnapshotIndex {
		return nil, [sha256.Size]byte{}, backupArchiveError("snapshot index does not match manifest")
	}
	if uint64(len(archive.Entries)) != archive.Manifest.EntryCount {
		return nil, [sha256.Size]byte{}, backupArchiveError("entry count mismatch")
	}
	if err := validateBackupEntries(archive.Manifest, archive.Entries); err != nil {
		return nil, [sha256.Size]byte{}, err
	}

	manifestBytes, err := deterministic.Marshal(archive.Manifest)
	if err != nil {
		return nil, [sha256.Size]byte{}, backupArchiveError("marshal manifest: %v", err)
	}
	if len(manifestBytes) > maxBackupManifestBytes {
		return nil, [sha256.Size]byte{}, backupArchiveError("manifest exceeds 64 KiB limit")
	}
	var encoded bytes.Buffer
	encoded.Grow(len(backupMagic) + 3*8 + len(manifestBytes) + len(archive.Snapshot) + 32)
	encoded.WriteString(backupMagic)
	if err := appendBackupFrame(&encoded, manifestBytes, maxBackupManifestBytes); err != nil {
		return nil, [sha256.Size]byte{}, err
	}
	if err := appendBackupFrame(&encoded, archive.Snapshot, maxSnapshotBytes); err != nil {
		return nil, [sha256.Size]byte{}, err
	}
	for _, entry := range archive.Entries {
		entryBytes, err := deterministic.Marshal(entry)
		if err != nil {
			return nil, [sha256.Size]byte{}, backupArchiveError("marshal entry: %v", err)
		}
		if err := appendBackupFrame(&encoded, entryBytes, maxBackupEntryBytes); err != nil {
			return nil, [sha256.Size]byte{}, err
		}
	}
	if encoded.Len()+sha256.Size > MaxBackupArchiveBytes {
		return nil, [sha256.Size]byte{}, backupArchiveError("archive exceeds 1 GiB limit")
	}
	checksum := sha256.Sum256(encoded.Bytes())
	encoded.Write(checksum[:])
	return encoded.Bytes(), checksum, nil
}

// DecodeBackupArchive verifies the framing, checksums, schemas and commit range of an archive.
func DecodeBackupArchive(data []byte) (*BackupArchive, error) {
	if len(data) >= len("NMQBKUP1") && string(data[:len("NMQBKUP1")]) == "NMQBKUP1" {
		return nil, backupArchiveError("unsupported archive format 1; capture a new backup with this release")
	}
	if len(data) >= len("NMQBKUP2") && string(data[:len("NMQBKUP2")]) == "NMQBKUP2" {
		return nil, backupArchiveError("unsupported archive format 2; capture a new backup with this release")
	}
	if len(data) < len(backupMagic)+sha256.Size || len(data) > MaxBackupArchiveBytes {
		return nil, backupArchiveError("archive size is out of range")
	}
	if string(data[:len(backupMagic)]) != backupMagic {
		return nil, backupArchiveError("magic mismatch")
	}
	payloadEnd := len(data) - sha256.Size
	checksum := sha256.Sum256(data[:payloadEnd])
	if !bytes.Equal(data[payloadEnd:], checksum[:]) {
		return nil, backupArchiveError("checksum mismatch")
	}

	offset := len(backupMagic)
	frameIndex := uint64(0)
	manifestBytes, err := readBackupFrame(data, &offset, payloadEnd, maxBackupManifestBytes, frameIndex)
	if err != nil {
		return nil, err
	}
	manifest := new(BackupManifest)
	if err := unmarshalCanonicalBackupMessage(manifestBytes, manifest, "manifest"); err != nil {
		return nil, err
	}
	if err := validateBackupManifest(manifest); err != nil {
		return nil, err
	}

	frameIndex++
	snapshotBytes, err := readBackupFrame(data, &offset, payloadEnd, maxSnapshotBytes, frameIndex)
	if err != nil {
		return nil, err
	}
	snapshot, err := UnmarshalSnapshot(snapshotBytes)
	if err != nil {
		return nil, backupArchiveError("snapshot: %v", err)
	}
	if snapshot.LastIncludedIndex != manifest.SnapshotIndex {
		return nil, backupArchiveError("snapshot index does not match manifest")
	}

	entries := make([]*BackupEntry, 0, int(manifest.EntryCount))
	for range manifest.EntryCount {
		frameIndex++
		entryBytes, err := readBackupFrame(data, &offset, payloadEnd, maxBackupEntryBytes, frameIndex)
		if err != nil {
			return nil, err
		}
		entry := new(BackupEntry)
		if err := unmarshalCanonicalBackupMessage(entryBytes, entry, fmt.Sprintf("entry frame %d", frameIndex)); err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
	if offset != payloadEnd {
		return nil, backupArchiveError("trailing bytes")
	}
	if err := validateBackupEntries(manifest, entries); err != nil {
		return nil, err
	}
	return &BackupArchive{Manifest: manifest, Snapshot: snapshotBytes, Entries: entries}, nil
}

// EqualBackupStateSummary reports whether two backup summaries have identical protobuf values.
func EqualBackupStateSummary(left, right *BackupStateSummary) bool {
	return proto.Equal(left, right)
}

// SummarizeSnapshot calculates stable whole-state and per-kind digests for a typed snapshot.
func SummarizeSnapshot(encoded []byte) (*BackupStateSummary, error) {
	envelope, err := UnmarshalSnapshot(encoded)
	if err != nil {
		return nil, backupArchiveError("snapshot: %v", err)
	}
	state := envelope.State
	members, err := summarizeBackupKind(&ReplicatedStateSnapshot{Members: state.Members}, uint64(len(state.Members)))
	if err != nil {
		return nil, err
	}
	sessions, err := summarizeBackupKind(&ReplicatedStateSnapshot{Ownerships: state.Ownerships}, uint64(len(state.Ownerships)))
	if err != nil {
		return nil, err
	}
	subscriptions, err := summarizeBackupKind(&ReplicatedStateSnapshot{Subscriptions: state.Subscriptions}, uint64(len(state.Subscriptions)))
	if err != nil {
		return nil, err
	}
	inflightCount := uint64(0)
	for _, checkpoint := range state.Checkpoints {
		inflightCount += uint64(len(checkpoint.Inbound) + len(checkpoint.Outbound))
	}
	inflight, err := summarizeBackupKind(&ReplicatedStateSnapshot{Checkpoints: state.Checkpoints}, inflightCount)
	if err != nil {
		return nil, err
	}
	membershipHistory, err := summarizeBackupKind(&ReplicatedStateSnapshot{MembershipHistory: state.MembershipHistory}, uint64(len(state.MembershipHistory)))
	if err != nil {
		return nil, err
	}
	retained, err := summarizeBackupKind(&ReplicatedStateSnapshot{Retained: state.Retained}, uint64(len(state.Retained)))
	if err != nil {
		return nil, err
	}
	offlineCount := uint64(0)
	for _, session := range state.OfflineSessions {
		offlineCount += uint64(1 + len(session.Messages))
	}
	offline, err := summarizeBackupKind(&ReplicatedStateSnapshot{OfflineSessions: state.OfflineSessions}, offlineCount)
	if err != nil {
		return nil, err
	}
	tenants, err := summarizeBackupTenants(state)
	if err != nil {
		return nil, err
	}
	return &BackupStateSummary{
		LastIncludedIndex: envelope.LastIncludedIndex,
		ObjectCount:       envelope.ObjectCount,
		Sha256:            bytes.Clone(envelope.Sha256),
		Members:           members,
		Tenants:           tenants,
		Sessions:          sessions,
		Subscriptions:     subscriptions,
		Inflight:          inflight,
		MembershipHistory: membershipHistory,
		Retained:          retained,
		Offline:           offline,
	}, nil
}

func validateBackupManifest(manifest *BackupManifest) error {
	if manifest == nil {
		return backupArchiveError("manifest is required")
	}
	if manifest.FormatMajor != BackupFormatMajor || manifest.FormatMinor != BackupFormatMinor {
		return backupArchiveError("unsupported archive format")
	}
	if !boundedUTF8(manifest.ClusterId, 1, 200) {
		return backupArchiveError("invalid cluster ID")
	}
	if manifest.SourceMemberId == 0 {
		return backupArchiveError("source member ID must be positive")
	}
	if !boundedUTF8(manifest.ProducerRelease, 1, 64) {
		return backupArchiveError("invalid producer release")
	}
	if manifest.ClusterProtocolMajor != 2 || manifest.ClusterProtocolMinor != 0 {
		return backupArchiveError("unsupported cluster protocol")
	}
	if manifest.StateSchemaMajor != 1 || manifest.StateSchemaMinor != 0 {
		return backupArchiveError("unsupported state schema")
	}
	if manifest.SnapshotIndex == 0 || manifest.SnapshotIndex > manifest.CommitIndex || manifest.SnapshotTerm == 0 || manifest.SnapshotTerm > manifest.CommitTerm {
		return backupArchiveError("invalid snapshot or commit boundary")
	}
	if len(manifest.VoterIds) == 0 || !strictlyIncreasingPositiveIDs(manifest.VoterIds) {
		return backupArchiveError("voter IDs must be positive and strictly increasing")
	}
	if !strictlyIncreasingPositiveIDs(manifest.LearnerIds) {
		return backupArchiveError("learner IDs must be positive and strictly increasing")
	}
	voters := make(map[uint64]struct{}, len(manifest.VoterIds))
	for _, id := range manifest.VoterIds {
		voters[id] = struct{}{}
	}
	for _, id := range manifest.LearnerIds {
		if _, exists := voters[id]; exists {
			return backupArchiveError("voter and learner IDs overlap")
		}
	}
	if manifest.EntryCount > maxBackupEntries {
		return backupArchiveError("entry count exceeds 1,000,000 limit")
	}
	state := manifest.BoundaryState
	if state == nil || state.LastIncludedIndex != manifest.CommitIndex {
		return backupArchiveError("boundary state index does not match commit index")
	}
	if len(state.Sha256) != sha256.Size {
		return backupArchiveError("boundary state SHA-256 must be 32 bytes")
	}
	for _, item := range []struct {
		name    string
		summary *BackupKindSummary
	}{
		{name: "members", summary: state.Members},
		{name: "tenants", summary: state.Tenants},
		{name: "sessions", summary: state.Sessions},
		{name: "subscriptions", summary: state.Subscriptions},
		{name: "inflight", summary: state.Inflight},
		{name: "membership_history", summary: state.MembershipHistory},
		{name: "retained", summary: state.Retained},
		{name: "offline", summary: state.Offline},
	} {
		if item.summary == nil || len(item.summary.Sha256) != sha256.Size {
			return backupArchiveError("%s summary and SHA-256 are required", item.name)
		}
	}
	return nil
}

func validateBackupEntries(manifest *BackupManifest, entries []*BackupEntry) error {
	if uint64(len(entries)) != manifest.EntryCount {
		return backupArchiveError("entry count mismatch")
	}
	var previous uint64
	for _, entry := range entries {
		if entry == nil || entry.Index <= previous || entry.Index <= manifest.SnapshotIndex || entry.Index > manifest.CommitIndex {
			return backupArchiveError("entry index out of range")
		}
		if err := rejectUnknown(entry.ProtoReflect()); err != nil {
			return backupArchiveError("entry: %v", err)
		}
		switch payload := entry.Payload.(type) {
		case *BackupEntry_Command:
			if _, err := UnmarshalCommand(payload.Command); err != nil {
				return backupArchiveError("entry command: %v", err)
			}
		case *BackupEntry_Configuration:
			change := payload.Configuration
			if change == nil || !validUUID(change.OperationId) || change.MemberId == 0 || change.Action < BackupConfigurationAction_BACKUP_CONFIGURATION_ACTION_ADD_LEARNER || change.Action > BackupConfigurationAction_BACKUP_CONFIGURATION_ACTION_REMOVE {
				return backupArchiveError("invalid configuration entry")
			}
		default:
			return backupArchiveError("entry payload is required")
		}
		previous = entry.Index
	}
	return nil
}

func strictlyIncreasingPositiveIDs(ids []uint64) bool {
	for index, id := range ids {
		if id == 0 || index > 0 && ids[index-1] >= id {
			return false
		}
	}
	return true
}

func appendBackupFrame(output *bytes.Buffer, payload []byte, maximum int) error {
	if len(payload) > maximum {
		return backupArchiveError("frame exceeds %d byte limit", maximum)
	}
	if output.Len()+8+len(payload)+sha256.Size > MaxBackupArchiveBytes {
		return backupArchiveError("archive exceeds 1 GiB limit")
	}
	var header [8]byte
	binary.BigEndian.PutUint32(header[:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(header[4:], crc32.Checksum(payload, backupCRCTable))
	output.Write(header[:])
	output.Write(payload)
	return nil
}

func readBackupFrame(data []byte, offset *int, payloadEnd, maximum int, frameIndex uint64) ([]byte, error) {
	if *offset < 0 || payloadEnd-*offset < 8 {
		return nil, backupArchiveError("frame %d is truncated", frameIndex)
	}
	header := data[*offset : *offset+8]
	length := binary.BigEndian.Uint32(header[:4])
	checksum := binary.BigEndian.Uint32(header[4:])
	*offset += 8
	if uint64(length) > uint64(maximum) || uint64(length) > uint64(payloadEnd-*offset) {
		return nil, backupArchiveError("frame %d length is out of range", frameIndex)
	}
	payload := data[*offset : *offset+int(length)]
	if crc32.Checksum(payload, backupCRCTable) != checksum {
		return nil, backupArchiveError("frame %d checksum mismatch", frameIndex)
	}
	*offset += int(length)
	return payload, nil
}

func unmarshalCanonicalBackupMessage(encoded []byte, message proto.Message, name string) error {
	if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(encoded, message); err != nil {
		return backupArchiveError("decode %s: %v", name, err)
	}
	if err := rejectUnknown(message.ProtoReflect()); err != nil {
		return backupArchiveError("decode %s: %v", name, err)
	}
	canonical, err := deterministic.Marshal(message)
	if err != nil {
		return backupArchiveError("marshal %s: %v", name, err)
	}
	if !bytes.Equal(encoded, canonical) {
		return backupArchiveError("%s wire encoding is not canonical", name)
	}
	return nil
}

func summarizeBackupKind(state *ReplicatedStateSnapshot, count uint64) (*BackupKindSummary, error) {
	encoded, err := deterministic.Marshal(state)
	if err != nil {
		return nil, backupArchiveError("marshal kind summary: %v", err)
	}
	checksum := sha256.Sum256(encoded)
	return &BackupKindSummary{Count: count, Sha256: bytes.Clone(checksum[:])}, nil
}

func summarizeBackupTenants(state *ReplicatedStateSnapshot) (*BackupKindSummary, error) {
	unique := make(map[string]struct{})
	for _, ownership := range state.Ownerships {
		unique[ownership.TenantId] = struct{}{}
	}
	for _, subscription := range state.Subscriptions {
		unique[subscription.TenantId] = struct{}{}
	}
	for _, checkpoint := range state.Checkpoints {
		unique[checkpoint.TenantId] = struct{}{}
	}
	for _, retained := range state.Retained {
		unique[retained.TenantId] = struct{}{}
	}
	for _, offline := range state.OfflineSessions {
		unique[offline.TenantId] = struct{}{}
	}
	tenants := make([]string, 0, len(unique))
	for tenant := range unique {
		tenants = append(tenants, tenant)
	}
	sort.Strings(tenants)
	var encoded bytes.Buffer
	for _, tenant := range tenants {
		var length [2]byte
		binary.BigEndian.PutUint16(length[:], uint16(len(tenant)))
		encoded.Write(length[:])
		encoded.WriteString(tenant)
	}
	checksum := sha256.Sum256(encoded.Bytes())
	return &BackupKindSummary{Count: uint64(len(tenants)), Sha256: bytes.Clone(checksum[:])}, nil
}

func backupArchiveError(format string, arguments ...any) error {
	return fmt.Errorf("backup archive: "+format, arguments...)
}
