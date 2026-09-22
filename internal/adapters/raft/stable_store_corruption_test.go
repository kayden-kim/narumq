package raft

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"testing"

	netraft "go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func TestClusterMetadataAndSnapshotFilenameBoundaries(t *testing.T) {
	if _, err := prepareClusterDirectory("", 1, []NodeID{1, 2, 3}); !errors.Is(err, ErrConfigMismatch) {
		t.Fatalf("empty directory error = %v", err)
	}
	filePath := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(filePath, []byte("file"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := prepareClusterDirectory(filePath, 1, []NodeID{1, 2, 3}); !errors.Is(err, ErrConfigMismatch) {
		t.Fatalf("file directory error = %v", err)
	}
	incomplete := t.TempDir()
	if err := os.WriteFile(filepath.Join(incomplete, "leftover"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := prepareClusterDirectory(incomplete, 1, []NodeID{1, 2, 3}); !errors.Is(err, ErrBootstrapIncomplete) {
		t.Fatalf("incomplete bootstrap error = %v", err)
	}

	for _, testCase := range []struct {
		name   string
		mutate func([]byte)
	}{
		{name: "magic", mutate: func(data []byte) { data[0] = 'X' }},
		{name: "version", mutate: func(data []byte) { data[len(clusterMetaMagic)]++ }},
		{name: "checksum", mutate: func(data []byte) { data[len(data)-1] ^= 0xff }},
		{name: "identity", mutate: func(data []byte) { data[len(clusterMetaMagic)+1] ^= 0xff }},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			directory := t.TempDir()
			path := filepath.Join(directory, clusterMetaName)
			if err := writeClusterMeta(path, 1, []NodeID{1, 2, 3}); err != nil {
				t.Fatal(err)
			}
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			testCase.mutate(data)
			if err := os.WriteFile(path, data, 0o600); err != nil {
				t.Fatal(err)
			}
			if err := validateClusterMeta(path, 1, []NodeID{1, 2, 3}); !errors.Is(err, ErrConfigMismatch) {
				t.Fatalf("validateClusterMeta() error = %v, want ErrConfigMismatch", err)
			}
		})
	}

	if _, ok := parseSnapshotFilename(snapshotFilename(42)); !ok {
		t.Fatal("valid snapshot filename was rejected")
	}
	for _, name := range []string{"snapshot-1.bin", "snapshot-00000000000000000001.tmp", "snapshot-abcdefghijklmnopqr.bin", "other.bin"} {
		if _, ok := parseSnapshotFilename(name); ok {
			t.Fatalf("parseSnapshotFilename(%q) unexpectedly succeeded", name)
		}
	}
}

func TestApplicationStateAndWALCodecBoundaries(t *testing.T) {
	commits := []CommitDTO{
		{Index: 1, Data: []byte("alpha")},
		{Index: 2, Data: []byte("beta")},
	}
	state, err := encodeApplicationState(2, commits)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeApplicationState(2, state)
	if err != nil || len(decoded) != 2 || string(decoded[0].Data) != "alpha" || string(decoded[1].Data) != "beta" {
		t.Fatalf("decoded state = %#v, error = %v", decoded, err)
	}
	decoded[1].Data[0] = 'X'
	decodedAgain, err := decodeApplicationState(2, state)
	if err != nil || string(decodedAgain[1].Data) != "beta" {
		t.Fatalf("decoded state changed after caller mutation: %#v, error = %v", decodedAgain, err)
	}
	invalidStates := [][]byte{
		{},
		[]byte("bad"),
		[]byte("NMQSTATE1"),
		append([]byte(state), 0),
	}
	for _, invalid := range invalidStates {
		if _, err := decodeApplicationState(2, invalid); err == nil {
			t.Fatalf("decodeApplicationState(% X) unexpectedly succeeded", invalid)
		}
	}
	if _, err := encodeApplicationState(2, []CommitDTO{{Index: 2, Data: []byte("a")}, {Index: 1, Data: []byte("b")}}); err == nil {
		t.Fatal("encodeApplicationState accepted decreasing index")
	}
	if _, err := encodeApplicationState(2, []CommitDTO{{Index: 1, Data: []byte("a")}, {Index: 1, Data: []byte("b")}}); err == nil {
		t.Fatal("encodeApplicationState accepted duplicate index")
	}
	if _, err := encodeApplicationState(1, []CommitDTO{{Index: 2, Data: []byte("a")}}); err == nil {
		t.Fatal("encodeApplicationState accepted index exceeding snapshot index")
	}
	if _, err := encodeApplicationState(1, []CommitDTO{{Index: 1, Data: nil}}); err == nil {
		t.Fatal("encodeApplicationState accepted empty commit payload")
	}
	largeCommand := bytes.Repeat([]byte{'x'}, maxCommandSize+1)
	if _, err := encodeApplicationState(1, []CommitDTO{{Index: 1, Data: largeCommand}}); err == nil {
		t.Fatal("encodeApplicationState accepted oversized command")
	}

	snapshotData, err := encodeApplicationState(3, []CommitDTO{{Index: 3, Data: []byte("snap")}})
	if err != nil {
		t.Fatal(err)
	}
	term, vote, commit := uint64(2), uint64(1), uint64(3)
	index, entryTerm := uint64(3), uint64(2)
	record := walRecord{
		hardState: &pb.HardState{Term: &term, Vote: &vote, Commit: &commit},
		snapshot:  &pb.Snapshot{Metadata: &pb.SnapshotMetadata{Index: &index, Term: &entryTerm, ConfState: &pb.ConfState{Voters: []uint64{1, 2, 3}}}, Data: snapshotData},
		entries:   []*pb.Entry{{Index: &index, Term: &entryTerm, Type: pb.EntryNormal.Enum(), Data: []byte("command")}},
	}
	payload, err := encodeWALRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	decodedRecord, err := decodeWALRecord(payload)
	if err != nil || decodedRecord.hardState.GetCommit() != 3 || len(decodedRecord.entries) != 1 || decodedRecord.snapshot.GetMetadata().GetIndex() != 3 {
		t.Fatalf("decoded WAL record = %#v, error = %v", decodedRecord, err)
	}
	for _, invalid := range [][]byte{{2, 0, 0, 0}, {1, 0x80, 0, 0}, {1}, append(payload, 0)} {
		if _, err := decodeWALRecord(invalid); err == nil {
			t.Fatalf("decodeWALRecord(% X) unexpectedly succeeded", invalid)
		}
	}
	if err := validateEntrySequence([]*pb.Entry{nil}); err == nil {
		t.Fatal("validateEntrySequence accepted nil entry")
	}
	if err := validateEntrySequence([]*pb.Entry{{Index: uint64Ptr(0)}}); err == nil {
		t.Fatal("validateEntrySequence accepted zero index")
	}
	if err := validateEntrySequence([]*pb.Entry{{Index: uint64Ptr(1)}, {Index: uint64Ptr(3)}}); err == nil {
		t.Fatal("validateEntrySequence accepted a gap")
	}
}

func TestConfigurationChangesAndBoundedReader(t *testing.T) {
	current := &pb.ConfState{Voters: []uint64{1}, Learners: []uint64{2}}
	for _, change := range []*pb.ConfChangeV2{
		{Changes: []*pb.ConfChangeSingle{confChangeSingle(3, pb.ConfChangeAddNode)}},
		{Changes: []*pb.ConfChangeSingle{confChangeSingle(4, pb.ConfChangeAddLearnerNode)}},
		{Changes: []*pb.ConfChangeSingle{confChangeSingle(1, pb.ConfChangeRemoveNode)}},
		{Changes: []*pb.ConfChangeSingle{confChangeSingle(5, pb.ConfChangeUpdateNode)}},
	} {
		next, err := applyConfState(current, change)
		if err != nil {
			t.Fatal(err)
		}
		current = next
	}
	if _, err := applyConfState(current, &pb.ConfChangeV2{Changes: []*pb.ConfChangeSingle{confChangeSingle(0, pb.ConfChangeAddNode)}}); err == nil {
		t.Fatal("applyConfState accepted node zero")
	}
	unknownType := pb.ConfChangeType(99)
	if _, err := applyConfState(current, &pb.ConfChangeV2{Changes: []*pb.ConfChangeSingle{{NodeId: uint64Ptr(6), Type: &unknownType}}}); err == nil {
		t.Fatal("applyConfState accepted unknown type")
	}
	entry := &pb.Entry{Type: pb.EntryConfChange.Enum(), Data: mustMarshal(t, &pb.ConfChange{NodeId: uint64Ptr(7), Type: confChangeTypePtr(pb.ConfChangeAddNode)})}
	if change, err := decodeConfChange(entry); err != nil || len(change.Changes) != 1 {
		t.Fatalf("decodeConfChange v1 = %#v, error = %v", change, err)
	}
	entry.Type = pb.EntryConfChangeV2.Enum()
	entry.Data = mustMarshal(t, &pb.ConfChangeV2{Changes: []*pb.ConfChangeSingle{confChangeSingle(8, pb.ConfChangeAddNode)}})
	if _, err := decodeConfChange(entry); err != nil {
		t.Fatal(err)
	}
	if _, err := decodeConfChange(&pb.Entry{Type: pb.EntryNormal.Enum()}); err == nil {
		t.Fatal("decodeConfChange accepted normal entry")
	}

	reader := newBoundedReader([]byte{1, 2, 3, 4, 5, 6, 7, 8})
	if reader.remaining() != 8 {
		t.Fatal("bounded reader initial length mismatch")
	}
	if _, err := reader.byte(); err != nil {
		t.Fatal(err)
	}
	if _, err := reader.u32(); err != nil {
		t.Fatal(err)
	}
	if _, err := reader.u64(); err == nil {
		t.Fatal("bounded reader accepted truncated u64")
	}
	if _, err := reader.bytes(-1); err == nil {
		t.Fatal("bounded reader accepted negative length")
	}
}

func TestStableStoreSnapshotAndWALLifecycleErrors(t *testing.T) {
	directory := t.TempDir()
	if _, _, err := loadLatestSnapshot(filepath.Join(directory, "missing")); err == nil {
		t.Fatal("loadLatestSnapshot accepted missing directory")
	}
	if records, err := readWAL(filepath.Join(directory, walName)); err != nil || len(records) != 0 {
		t.Fatalf("empty WAL = %#v, error = %v", records, err)
	}
	store, err := openStableStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	if err := store.createSnapshot(0, &pb.ConfState{}, nil); err != nil {
		t.Fatal(err)
	}
	if err := store.createSnapshot(1, &pb.ConfState{}, nil); err == nil {
		t.Fatal("createSnapshot accepted index beyond WAL")
	}
	if err := persistSnapshotFile(directory, nil); err != nil {
		t.Fatal(err)
	}
	if err := persistSnapshotFile(directory, &pb.Snapshot{}); err != nil {
		t.Fatal(err)
	}
	if err := store.close(); err != nil {
		t.Fatal(err)
	}
	if err := store.close(); err != nil {
		t.Fatal(err)
	}
}

func mustMarshal(t *testing.T, message proto.Message) []byte {
	t.Helper()
	data, err := proto.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func uint64Ptr(value uint64) *uint64 {
	return &value
}

func confChangeTypePtr(value pb.ConfChangeType) *pb.ConfChangeType {
	return &value
}

func confChangeSingle(nodeID uint64, changeType pb.ConfChangeType) *pb.ConfChangeSingle {
	return &pb.ConfChangeSingle{NodeId: uint64Ptr(nodeID), Type: confChangeTypePtr(changeType)}
}
func TestStableStoreRecoveryAndSnapshotBranches(t *testing.T) {
	directory := t.TempDir()
	created, err := prepareClusterDirectory(directory, 1, []NodeID{1, 2, 3})
	if err != nil || !created {
		t.Fatalf("prepareClusterDirectory() = (%v, %v)", created, err)
	}
	store, err := openStableStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	term, commit := uint64(1), uint64(1)
	entryIndex := uint64(1)
	if err := store.appendReady(netraft.Ready{HardState: &pb.HardState{Term: &term, Commit: &commit}, Entries: []*pb.Entry{{Index: &entryIndex, Term: &term, Type: pb.EntryNormal.Enum(), Data: []byte("command")}}}); err != nil {
		t.Fatal(err)
	}
	if err := store.createSnapshot(1, &pb.ConfState{Voters: []uint64{1, 2, 3}}, []CommitDTO{{Index: 1, Data: []byte("command")}}); err != nil {
		t.Fatal(err)
	}
	if err := store.createSnapshot(1, &pb.ConfState{Voters: []uint64{1, 2, 3}}, nil); err != nil {
		t.Fatal(err)
	}
	if err := store.close(); err != nil {
		t.Fatal(err)
	}
	created, err = prepareClusterDirectory(directory, 1, []NodeID{1, 2, 3})
	if err != nil || created {
		t.Fatalf("finalized prepareClusterDirectory() = (%v, %v)", created, err)
	}
	recovered, err := openStableStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.close()
	if recovered.snapshotIndex != 1 || recovered.appliedIndex != 1 || len(recovered.commits) != 1 {
		t.Fatalf("recovered store = snapshot %d applied %d commits %d", recovered.snapshotIndex, recovered.appliedIndex, len(recovered.commits))
	}
	if err := persistSnapshotFile(directory, testSnapshot(1)); err != nil {
		t.Fatal(err)
	}
}

func TestStableStoreRebuildsAppliedStateAndRejectsUnknownEntries(t *testing.T) {
	memory := netraft.NewMemoryStorage()
	term := uint64(1)
	first, second := uint64(1), uint64(2)
	change := &pb.ConfChange{NodeId: uint64Ptr(2), Type: confChangeTypePtr(pb.ConfChangeAddNode)}
	changeData, err := proto.Marshal(change)
	if err != nil {
		t.Fatal(err)
	}
	if err := memory.Append([]*pb.Entry{
		{Index: &first, Term: &term, Type: pb.EntryNormal.Enum(), Data: []byte("command")},
		{Index: &second, Term: &term, Type: pb.EntryConfChange.Enum(), Data: changeData},
	}); err != nil {
		t.Fatal(err)
	}
	store := &stableStore{memory: memory, hardState: &pb.HardState{Commit: &second}, confState: &pb.ConfState{Voters: []uint64{1}}}
	if err := store.rebuildAppliedState(2); err != nil {
		t.Fatal(err)
	}
	if store.appliedIndex != 2 || len(store.commits) != 1 || !slicesContains(store.confState.Voters, 2) {
		t.Fatalf("rebuilt state = applied %d commits %d voters %#v", store.appliedIndex, len(store.commits), store.confState.Voters)
	}
	unknownIndex := uint64(1)
	unknownType := pb.EntryType(99)
	unknownStore := &stableStore{memory: netraft.NewMemoryStorage(), hardState: &pb.HardState{Commit: &unknownIndex}, confState: &pb.ConfState{}}
	if err := unknownStore.memory.Append([]*pb.Entry{{Index: &unknownIndex, Term: &term, Type: &unknownType}}); err != nil {
		t.Fatal(err)
	}
	if err := unknownStore.rebuildAppliedState(1); err == nil {
		t.Fatal("rebuildAppliedState accepted unknown entry type")
	}
}

func TestSnapshotLoaderRejectsCorruptFiles(t *testing.T) {
	cases := []struct {
		name   string
		mutate func([]byte) []byte
	}{
		{name: "magic", mutate: func(data []byte) []byte { data[0] = 'X'; return data }},
		{name: "length", mutate: func(data []byte) []byte { binary.BigEndian.PutUint32(data[8:12], uint32(len(data))); return data }},
		{name: "checksum", mutate: func(data []byte) []byte { data[len(data)-1] ^= 0xff; return data }},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			directory := t.TempDir()
			if err := persistSnapshotFile(directory, testSnapshot(7)); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(directory, snapshotFilename(7))
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, testCase.mutate(data), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, _, err := loadLatestSnapshot(directory); !errors.Is(err, ErrCorruptSnapshot) {
				t.Fatalf("loadLatestSnapshot() error = %v", err)
			}
		})
	}
	t.Run("filename", func(t *testing.T) {
		directory := t.TempDir()
		if err := persistSnapshotFile(directory, testSnapshot(7)); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(filepath.Join(directory, snapshotFilename(7)), filepath.Join(directory, snapshotFilename(8))); err != nil {
			t.Fatal(err)
		}
		if _, _, err := loadLatestSnapshot(directory); !errors.Is(err, ErrCorruptSnapshot) {
			t.Fatalf("filename mismatch error = %v", err)
		}
	})
}

func TestWALReaderHandlesTornAndCorruptFrames(t *testing.T) {
	validPayload, err := encodeWALRecord(walRecord{entries: []*pb.Entry{{Index: uint64Ptr(1), Type: pb.EntryNormal.Enum(), Data: []byte("x")}}})
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name    string
		data    []byte
		wantErr bool
	}{
		{name: "short magic", data: []byte("NMQ"), wantErr: true},
		{name: "bad magic", data: []byte("BADWAL01"), wantErr: true},
		{name: "torn header", data: append([]byte(walMagic), 1, 2, 3)},
		{name: "torn payload", data: truncatedWALFrame(validPayload)},
		{name: "tail checksum", data: walFrame(validPayload, false)},
		{name: "oversized frame", data: oversizedWALFrame(), wantErr: true},
		{name: "decode frame", data: walFrame([]byte{1, 4, 0, 0}, true), wantErr: true},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), walName)
			if err := os.WriteFile(path, testCase.data, 0o600); err != nil {
				t.Fatal(err)
			}
			_, err := readWAL(path)
			if testCase.wantErr && err == nil {
				t.Fatal("readWAL unexpectedly succeeded")
			}
			if !testCase.wantErr && err != nil {
				t.Fatalf("readWAL error = %v", err)
			}
		})
	}
	middle := append([]byte(walMagic), walFrame(validPayload, false)...)
	middle = append(middle, walFrame(validPayload, true)...)
	if _, err := readWAL(writeWALFixture(t, middle)); !errors.Is(err, ErrCorruptWAL) {
		t.Fatalf("middle checksum error = %v", err)
	}
}

func TestWALEncodingAndWriteAllFailureBranches(t *testing.T) {
	if _, err := decodeWALRecord([]byte{1, 4, 0, 0}); err == nil {
		t.Fatal("decodeWALRecord accepted unknown flags")
	}
	if _, err := decodeWALRecord([]byte{1, 0, 0, 0}); err == nil {
		t.Fatal("decodeWALRecord accepted missing entry count")
	}
	if _, err := decodeWALRecord([]byte{1, 0, 0, 0, 0, 0, 0, 0, 1}); err == nil {
		t.Fatal("decodeWALRecord accepted trailing bytes")
	}
	if _, err := readyRecord(netraft.Ready{Snapshot: &pb.Snapshot{Metadata: &pb.SnapshotMetadata{Index: uint64Ptr(1)}}}); err == nil {
		t.Fatal("readyRecord accepted snapshot without application state")
	}
	if _, err := encodeWALRecord(walRecord{entries: []*pb.Entry{{Index: uint64Ptr(1), Data: bytes.Repeat([]byte{'x'}, maxWALFramePayload)}}}); err == nil {
		t.Fatal("encodeWALRecord accepted oversized entry")
	}
	original := filePersistenceOps.write
	defer func() { filePersistenceOps.write = original }()
	for _, writer := range []func(io.Writer, []byte) (int, error){
		func(_ io.Writer, _ []byte) (int, error) { return -1, nil },
		func(_ io.Writer, data []byte) (int, error) { return len(data) + 1, nil },
		func(_ io.Writer, _ []byte) (int, error) { return 0, nil },
		func(_ io.Writer, _ []byte) (int, error) { return 0, io.ErrClosedPipe },
	} {
		filePersistenceOps.write = writer
		if err := writeAll(io.Discard, []byte("data")); err == nil {
			t.Fatal("writeAll unexpectedly succeeded")
		}
	}
}

func testSnapshot(index uint64) *pb.Snapshot {
	state, _ := encodeApplicationState(index, []CommitDTO{{Index: index, Data: []byte("command")}})
	return &pb.Snapshot{Metadata: &pb.SnapshotMetadata{Index: uint64Ptr(index), Term: uint64Ptr(1), ConfState: &pb.ConfState{Voters: []uint64{1, 2, 3}}}, Data: state}
}

func slicesContains(values []uint64, wanted uint64) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

func truncatedWALFrame(payload []byte) []byte {
	frame := make([]byte, len(walMagic)+8+len(payload)-1)
	copy(frame, walMagic)
	binary.BigEndian.PutUint32(frame[len(walMagic):len(walMagic)+4], uint32(len(payload)))
	binary.BigEndian.PutUint32(frame[len(walMagic)+4:len(walMagic)+8], crc32.Checksum(payload, crcTable))
	copy(frame[len(walMagic)+8:], payload[:len(payload)-1])
	return frame
}

func walFrame(payload []byte, validChecksum bool) []byte {
	frame := make([]byte, 8+len(payload))
	binary.BigEndian.PutUint32(frame[:4], uint32(len(payload)))
	checksum := crc32.Checksum(payload, crcTable)
	if !validChecksum {
		checksum++
	}
	binary.BigEndian.PutUint32(frame[4:8], checksum)
	copy(frame[8:], payload)
	return append([]byte(walMagic), frame...)
}

func oversizedWALFrame() []byte {
	frame := make([]byte, len(walMagic)+8)
	copy(frame, walMagic)
	binary.BigEndian.PutUint32(frame[len(walMagic):len(walMagic)+4], maxWALFramePayload+1)
	return frame
}

func writeWALFixture(t *testing.T, data []byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), walName)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}
