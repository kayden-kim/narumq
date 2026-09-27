package records

import (
	"bytes"
	"crypto/sha256"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
)

func TestBackupArchiveRoundTripAndChecksums(t *testing.T) {
	archive := backupTestArchive(t)
	encoded, checksum, err := EncodeBackupArchive(archive)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(encoded[:len(backupMagic)], []byte(backupMagic)) || !bytes.Equal(encoded[len(encoded)-sha256.Size:], checksum[:]) {
		t.Fatal("archive magic or trailer checksum does not match encoded output")
	}
	decoded, err := DecodeBackupArchive(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(decoded.Manifest, archive.Manifest) || !bytes.Equal(decoded.Snapshot, archive.Snapshot) || len(decoded.Entries) != 1 {
		t.Fatalf("decoded archive differs from encoded archive: %+v", decoded)
	}
	if decoded.Entries[0].Index != 6 || !bytes.Equal(decoded.Entries[0].GetCommand(), archive.Entries[0].GetCommand()) {
		t.Fatalf("decoded tail entry = %+v", decoded.Entries[0])
	}
}

func TestBackupArchiveRejectsCorruptAndNoncanonicalFrames(t *testing.T) {
	archive := backupTestArchive(t)
	encoded, _, err := EncodeBackupArchive(archive)
	if err != nil {
		t.Fatal(err)
	}
	manifestBytes, err := deterministic.Marshal(archive.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	unknownManifest := proto.Clone(archive.Manifest).(*BackupManifest)
	unknownManifest.ProtoReflect().SetUnknown([]byte{0x98, 0x06, 0x01})
	unknownManifestBytes, err := deterministic.Marshal(unknownManifest)
	if err != nil {
		t.Fatal(err)
	}
	v1FormatArchive := backupTestAppendTrailer([]byte("NMQBKUP1"))
	v2FormatArchive := backupTestAppendTrailer([]byte("NMQBKUP2"))
	badMagic := bytes.Clone(encoded[:len(encoded)-sha256.Size])
	badMagic[0] ^= 0xff
	cases := []struct {
		name   string
		data   []byte
		marker string
	}{
		{name: "v1 archive", data: v1FormatArchive, marker: "unsupported archive format 1; capture a new backup with this release"},
		{name: "v2 archive", data: v2FormatArchive, marker: "unsupported archive format 2; capture a new backup with this release"},
		{name: "magic", data: backupTestAppendTrailer(badMagic), marker: "magic mismatch"},
		{name: "trailer", data: func() []byte {
			b := bytes.Clone(encoded)
			b[len(b)-1] ^= 0xff
			return b
		}(), marker: "checksum mismatch"},
		{name: "frame crc", data: backupTestCorruptAndResign(encoded, 12), marker: "frame 0 checksum mismatch"},
		{name: "truncated", data: bytes.Clone(encoded[:len(encoded)-1]), marker: "checksum mismatch"},
		{name: "trailing byte", data: backupTestAppendTrailer(append(bytes.Clone(encoded[:len(encoded)-sha256.Size]), 0xa5)), marker: "trailing bytes"},
		{name: "unknown manifest field", data: backupTestRawArchive(t, unknownManifestBytes, archive.Snapshot, archive.Entries), marker: "unknown fields"},
		{name: "noncanonical manifest", data: backupTestRawArchive(t, append(bytes.Clone(manifestBytes), 0x08, byte(BackupFormatMajor)), archive.Snapshot, archive.Entries), marker: "not canonical"},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := DecodeBackupArchive(testCase.data)
			if err == nil || !strings.Contains(err.Error(), testCase.marker) || !strings.HasPrefix(err.Error(), "backup archive:") {
				t.Fatalf("DecodeBackupArchive() error = %v, want backup archive error containing %q", err, testCase.marker)
			}
		})
	}
}

func TestBackupArchiveRejectsInvalidManifestAndEntryRanges(t *testing.T) {
	for _, testCase := range []struct {
		name   string
		mutate func(*BackupArchive)
		marker string
	}{
		{name: "entry count", mutate: func(archive *BackupArchive) { archive.Manifest.EntryCount++ }, marker: "entry count mismatch"},
		{name: "snapshot index", mutate: func(archive *BackupArchive) { archive.Manifest.SnapshotIndex-- }, marker: "snapshot index does not match manifest"},
		{name: "entry before snapshot", mutate: func(archive *BackupArchive) { archive.Entries[0].Index = archive.Manifest.SnapshotIndex }, marker: "entry index out of range"},
		{name: "missing offline summary", mutate: func(archive *BackupArchive) {
			archive.Manifest.BoundaryState.Offline = nil
		}, marker: "offline summary and SHA-256 are required"},
		{name: "missing retained summary", mutate: func(archive *BackupArchive) {
			archive.Manifest.BoundaryState.Retained = nil
		}, marker: "retained summary and SHA-256 are required"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			archive := backupTestArchive(t)
			testCase.mutate(archive)
			if _, _, err := EncodeBackupArchive(archive); err == nil || !strings.Contains(err.Error(), testCase.marker) {
				t.Fatalf("EncodeBackupArchive() error = %v, want %q", err, testCase.marker)
			}
		})
	}

	archive := backupTestArchive(t)
	second := proto.Clone(archive.Entries[0]).(*BackupEntry)
	archive.Manifest.CommitIndex = 7
	archive.Manifest.EntryCount = 2
	archive.Manifest.BoundaryState.LastIncludedIndex = 7
	archive.Entries[0].Index = 7
	second.Index = 6
	archive.Entries = []*BackupEntry{archive.Entries[0], second}
	if _, _, err := EncodeBackupArchive(archive); err == nil || !strings.Contains(err.Error(), "entry index out of range") {
		t.Fatalf("out-of-order entry error = %v", err)
	}
}

func TestSummarizeSnapshotCountsTenantsAndInflightDeterministically(t *testing.T) {
	first := &ReplicatedStateSnapshot{
		Ownerships: []*OwnershipState{
			{TenantId: "tenant-a", ClientId: "client-a", OwnerNodeId: 1, Epoch: 1},
			{TenantId: "tenant-a", ClientId: "client-b", OwnerNodeId: 2, Epoch: 1},
		},
		Subscriptions: []*SubscriptionState{{TenantId: "tenant-b", ClientId: "client-a", OwnerNodeId: 1, Epoch: 1, Filter: "orders/#", Qos: 1}},
		DeliverySessions: []*DeliverySessionState{{
			TenantId: "tenant-a", ClientId: "client-a", NextSequence: 2,
			Messages: []*DeliveryMessage{{
				Sequence: 1, Topic: "orders/a", Payload: []byte("outbound"), Qos: 1,
				PacketId: 8, Phase: DeliveryPhase_DELIVERY_PHASE_AWAIT_PUBACK,
			}},
			Inbound: []*InboundMessage{{
				PacketId: 7, Topic: "orders/a", Payload: []byte("inbound"),
			}},
		}},
		Retained: []*RetainedState{{
			TenantId: "tenant-c", Topic: "events", PublisherClientId: "publisher",
			Qos: 1, Payload: []byte("retained"),
		}},
		OfflineSessions: []*OfflineSessionState{{
			TenantId: "tenant-d", ClientId: "client-d", Epoch: 1, NextSequence: 3,
			Messages: []*OfflineMessage{
				{Sequence: 1, Topic: "offline/1", Payload: []byte("one"), Qos: 0},
				{Sequence: 2, Topic: "offline/2", Payload: []byte("two"), Qos: 0},
			},
		}},
	}
	second := proto.Clone(first).(*ReplicatedStateSnapshot)
	second.Ownerships[0], second.Ownerships[1] = second.Ownerships[1], second.Ownerships[0]
	firstBytes, err := MarshalSnapshot(first, 9)
	if err != nil {
		t.Fatal(err)
	}
	secondBytes, err := MarshalSnapshot(second, 9)
	if err != nil {
		t.Fatal(err)
	}
	firstSummary, err := SummarizeSnapshot(firstBytes)
	if err != nil {
		t.Fatal(err)
	}
	secondSummary, err := SummarizeSnapshot(secondBytes)
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(firstSummary, secondSummary) {
		t.Fatalf("summary changed with input ordering: first=%+v second=%+v", firstSummary, secondSummary)
	}
	if firstSummary.Tenants.Count != 4 || firstSummary.Sessions.Count != 2 || firstSummary.Subscriptions.Count != 1 || firstSummary.Inflight.Count != 2 || firstSummary.Retained.Count != 1 || firstSummary.Offline.Count != 3 {
		t.Fatalf("summary counts = tenants:%d sessions:%d subscriptions:%d inflight:%d retained:%d offline:%d", firstSummary.Tenants.Count, firstSummary.Sessions.Count, firstSummary.Subscriptions.Count, firstSummary.Inflight.Count, firstSummary.Retained.Count, firstSummary.Offline.Count)
	}
	// The test's tenant identifiers are distinct, so check the canonical encoding directly.
	canonicalTenants := []byte{0, 8, 't', 'e', 'n', 'a', 'n', 't', '-', 'a', 0, 8, 't', 'e', 'n', 'a', 'n', 't', '-', 'b', 0, 8, 't', 'e', 'n', 'a', 'n', 't', '-', 'c', 0, 8, 't', 'e', 'n', 'a', 'n', 't', '-', 'd'}
	wantTenantDigest := sha256.Sum256(canonicalTenants)
	if !bytes.Equal(firstSummary.Tenants.Sha256, wantTenantDigest[:]) {
		t.Fatalf("tenant digest = %x, want %x", firstSummary.Tenants.Sha256, wantTenantDigest)
	}
	wantRetained, err := summarizeBackupKind(&ReplicatedStateSnapshot{Retained: first.Retained}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(firstSummary.Retained, wantRetained) {
		t.Fatalf("retained summary = %+v, want %+v", firstSummary.Retained, wantRetained)
	}
	wantInflight, err := summarizeBackupKind(&ReplicatedStateSnapshot{DeliverySessions: first.DeliverySessions}, 2)
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(firstSummary.Inflight, wantInflight) {
		t.Fatalf("inflight summary = %+v, want %+v", firstSummary.Inflight, wantInflight)
	}
	wantOffline, err := summarizeBackupKind(&ReplicatedStateSnapshot{OfflineSessions: first.OfflineSessions}, 3)
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(firstSummary.Offline, wantOffline) {
		t.Fatalf("offline summary = %+v, want %+v", firstSummary.Offline, wantOffline)
	}
}

// Traceability: CLU-14; docs/specs/cluster-session-and-delivery.md §4.2, §11.11.
func TestBackupArchiveRejectsInconsistentMembershipAndEntries(t *testing.T) {
	for _, testCase := range []struct {
		name   string
		mutate func(*BackupArchive)
		marker string
	}{
		{name: "unsorted voters", mutate: func(archive *BackupArchive) { archive.Manifest.VoterIds = []uint64{3, 1} }, marker: "voter IDs must be positive and strictly increasing"},
		{name: "overlapping learner", mutate: func(archive *BackupArchive) { archive.Manifest.LearnerIds = []uint64{1} }, marker: "voter and learner IDs overlap"},
		{name: "unsupported state schema", mutate: func(archive *BackupArchive) { archive.Manifest.StateSchemaMajor++ }, marker: "unsupported state schema"},
		{name: "missing member summary", mutate: func(archive *BackupArchive) { archive.Manifest.BoundaryState.Members = nil }, marker: "members summary and SHA-256 are required"},
		{name: "nil entry", mutate: func(archive *BackupArchive) { archive.Entries[0] = nil }, marker: "entry index out of range"},
		{name: "missing entry payload", mutate: func(archive *BackupArchive) { archive.Entries[0].Payload = nil }, marker: "entry payload is required"},
		{name: "invalid command entry", mutate: func(archive *BackupArchive) {
			archive.Entries[0].Payload = &BackupEntry_Command{Command: []byte{0xff}}
		}, marker: "entry command"},
		{name: "invalid configuration entry", mutate: func(archive *BackupArchive) {
			archive.Entries[0].Payload = &BackupEntry_Configuration{Configuration: &BackupConfigurationChange{
				OperationId: "invalid", MemberId: 4,
				Action: BackupConfigurationAction_BACKUP_CONFIGURATION_ACTION_ADD_LEARNER,
			}}
		}, marker: "invalid configuration entry"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			archive := backupTestArchive(t)
			testCase.mutate(archive)
			if _, _, err := EncodeBackupArchive(archive); err == nil || !strings.Contains(err.Error(), testCase.marker) {
				t.Fatalf("EncodeBackupArchive() error=%v, want %q", err, testCase.marker)
			}
		})
	}
}

func backupTestArchive(t *testing.T) *BackupArchive {
	t.Helper()
	state := &ReplicatedStateSnapshot{}
	snapshot, err := MarshalSnapshot(state, 5)
	if err != nil {
		t.Fatal(err)
	}
	boundary, err := MarshalSnapshot(state, 6)
	if err != nil {
		t.Fatal(err)
	}
	boundarySummary, err := SummarizeSnapshot(boundary)
	if err != nil {
		t.Fatal(err)
	}
	command, err := MarshalCommand(&CommandEnvelope{
		SchemaMajor: 1,
		OperationId: op1,
		Payload: &CommandEnvelope_Ownership{Ownership: &OwnershipChange{
			TenantId: "tenant", ClientId: "client", OwnerNodeId: 1, Epoch: 1,
			Action: OwnershipAction_OWNERSHIP_ACTION_ACQUIRE,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return &BackupArchive{
		Manifest: &BackupManifest{
			FormatMajor: BackupFormatMajor, FormatMinor: BackupFormatMinor,
			ClusterId: "cluster", SourceMemberId: 1, ProducerRelease: "1.0.0",
			ClusterProtocolMajor: 2, StateSchemaMajor: 1, CreatedAtUnixMillis: 1,
			SnapshotIndex: 5, SnapshotTerm: 1, CommitIndex: 6, CommitTerm: 2,
			VoterIds: []uint64{1, 2, 3}, EntryCount: 1, BoundaryState: boundarySummary,
		},
		Snapshot: snapshot,
		Entries:  []*BackupEntry{{Index: 6, Payload: &BackupEntry_Command{Command: command}}},
	}
}

func backupTestRawArchive(t *testing.T, manifest []byte, snapshot []byte, entries []*BackupEntry) []byte {
	t.Helper()
	var data bytes.Buffer
	data.WriteString(backupMagic)
	if err := appendBackupFrame(&data, manifest, maxBackupManifestBytes); err != nil {
		t.Fatal(err)
	}
	if err := appendBackupFrame(&data, snapshot, maxSnapshotBytes); err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		encoded, err := deterministic.Marshal(entry)
		if err != nil {
			t.Fatal(err)
		}
		if err := appendBackupFrame(&data, encoded, maxBackupEntryBytes); err != nil {
			t.Fatal(err)
		}
	}
	return backupTestAppendTrailer(data.Bytes())
}

func backupTestCorruptAndResign(encoded []byte, index int) []byte {
	data := bytes.Clone(encoded[:len(encoded)-sha256.Size])
	data[index] ^= 0xff
	return backupTestAppendTrailer(data)
}

func backupTestAppendTrailer(payload []byte) []byte {
	data := bytes.Clone(payload)
	checksum := sha256.Sum256(data)
	return append(data, checksum[:]...)
}
func TestBackupArchiveRoundTripsConfigurationEntry(t *testing.T) {
	archive := backupTestArchive(t)
	boundary, err := MarshalSnapshot(&ReplicatedStateSnapshot{}, 7)
	if err != nil {
		t.Fatal(err)
	}
	archive.Manifest.CommitIndex = 7
	archive.Manifest.EntryCount = 2
	archive.Manifest.BoundaryState, err = SummarizeSnapshot(boundary)
	if err != nil {
		t.Fatal(err)
	}
	archive.Entries = append(archive.Entries, &BackupEntry{
		Index: 7,
		Payload: &BackupEntry_Configuration{Configuration: &BackupConfigurationChange{
			OperationId: op4, MemberId: 4,
			Action: BackupConfigurationAction_BACKUP_CONFIGURATION_ACTION_ADD_LEARNER,
		}},
	})
	encoded, _, err := EncodeBackupArchive(archive)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeBackupArchive(encoded)
	if err != nil {
		t.Fatal(err)
	}
	change := decoded.Entries[1].GetConfiguration()
	if change == nil || change.OperationId != op4 || change.MemberId != 4 || change.Action != BackupConfigurationAction_BACKUP_CONFIGURATION_ACTION_ADD_LEARNER {
		t.Fatalf("decoded configuration entry = %+v", decoded.Entries[1])
	}
}
