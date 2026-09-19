package raft

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	etcdraft "go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

func TestStableStoreReplayConflictingSuffix(t *testing.T) {
	directory := t.TempDir()
	writeTestWAL(t, directory,
		walRecord{hardState: &pb.HardState{Term: new(uint64(1)), Commit: new(uint64(1))}, entries: []*pb.Entry{{Index: new(uint64(1)), Term: new(uint64(1)), Type: pb.EntryNormal.Enum(), Data: []byte("alpha")}}},
		walRecord{hardState: &pb.HardState{Term: new(uint64(1)), Commit: new(uint64(2))}, entries: []*pb.Entry{{Index: new(uint64(2)), Term: new(uint64(1)), Type: pb.EntryNormal.Enum(), Data: []byte("old")}}},
		walRecord{hardState: &pb.HardState{Term: new(uint64(2)), Commit: new(uint64(3))}, entries: []*pb.Entry{
			{Index: new(uint64(2)), Term: new(uint64(2)), Type: pb.EntryNormal.Enum(), Data: []byte("beta")},
			{Index: new(uint64(3)), Term: new(uint64(2)), Type: pb.EntryNormal.Enum(), Data: []byte("gamma")},
		}},
	)
	store, err := openStableStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	if got := commandStrings(store.commands); !equalStringValues(got, []string{"alpha", "beta", "gamma"}) {
		t.Fatalf("replayed commands = %v", got)
	}
	if store.hardState.GetCommit() != 3 || store.appliedIndex != 3 {
		t.Fatalf("replayed progress = hardstate=%#v applied=%d", store.hardState, store.appliedIndex)
	}
}

func TestStableStoreTornTailAndMiddleCorruption(t *testing.T) {
	directory := t.TempDir()
	valid := encodeTestFrame(t, walRecord{hardState: &pb.HardState{Term: new(uint64(1)), Commit: new(uint64(1))}, entries: []*pb.Entry{{Index: new(uint64(1)), Term: new(uint64(1)), Type: pb.EntryNormal.Enum(), Data: []byte("alpha")}}})
	path := filepath.Join(directory, walName)
	if err := os.WriteFile(path, append(append([]byte(walMagic), valid...), []byte{0, 0, 0, 8}...), 0o600); err != nil {
		t.Fatal(err)
	}
	records, err := readWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 {
		t.Fatalf("torn-tail records = %d, want 1", len(records))
	}
	stat, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if stat.Size() != int64(len(walMagic)+len(valid)) {
		t.Fatalf("torn-tail size = %d, want %d", stat.Size(), len(walMagic)+len(valid))
	}

	middle := t.TempDir()
	frame2 := encodeTestFrame(t, walRecord{hardState: &pb.HardState{Term: new(uint64(1)), Commit: new(uint64(2))}, entries: []*pb.Entry{{Index: new(uint64(2)), Term: new(uint64(1)), Type: pb.EntryNormal.Enum(), Data: []byte("beta")}}})
	frame3 := encodeTestFrame(t, walRecord{hardState: &pb.HardState{Term: new(uint64(1)), Commit: new(uint64(3))}, entries: []*pb.Entry{{Index: new(uint64(3)), Term: new(uint64(1)), Type: pb.EntryNormal.Enum(), Data: []byte("gamma")}}})
	corruptFrame := bytes.Clone(frame2)
	corruptFrame[len(corruptFrame)-1] ^= 0xff
	if err := os.WriteFile(filepath.Join(middle, walName), append(append(append([]byte(walMagic), valid...), corruptFrame...), frame3...), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := readWAL(filepath.Join(middle, walName)); !errors.Is(err, ErrCorruptWAL) {
		t.Fatalf("middle corruption error = %v, want ErrCorruptWAL", err)
	}
}

func TestStableStoreRejectsLengthAllocationAndIdentityMismatch(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, walName)
	var header [8]byte
	binary.BigEndian.PutUint32(header[:4], maxWALFramePayload+1)
	if err := os.WriteFile(path, append([]byte(walMagic), header[:]...), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := readWAL(path); !errors.Is(err, ErrCorruptWAL) {
		t.Fatalf("oversized WAL frame error = %v, want ErrCorruptWAL", err)
	}

	nodeDirectory := filepath.Join(t.TempDir(), "node")
	node, err := Open(Config{NodeID: 1, PeerIDs: []NodeID{1, 2, 3}, Directory: nodeDirectory})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := node.ProcessReady(); err != nil {
		t.Fatal(err)
	}
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(Config{NodeID: 2, PeerIDs: []NodeID{1, 2, 3}, Directory: nodeDirectory}); !errors.Is(err, ErrConfigMismatch) {
		t.Fatalf("identity mismatch error = %v, want ErrConfigMismatch", err)
	}
}
func TestStableStoreRejectsCorruptLatestSnapshot(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "snapshot")
	node, err := Open(Config{NodeID: 1, PeerIDs: []NodeID{1, 2, 3}, Directory: directory})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := node.ProcessReady(); err != nil {
		t.Fatal(err)
	}
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	snapshots, err := filepath.Glob(filepath.Join(directory, "snapshot-*.bin"))
	if err != nil || len(snapshots) != 1 {
		t.Fatalf("snapshot files = %v, error = %v", snapshots, err)
	}
	data, err := os.ReadFile(snapshots[0])
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)-1] ^= 0xff
	if err := os.WriteFile(snapshots[0], data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(Config{NodeID: 1, PeerIDs: []NodeID{1, 2, 3}, Directory: directory}); !errors.Is(err, ErrCorruptSnapshot) {
		t.Fatalf("corrupt latest snapshot error = %v, want ErrCorruptSnapshot", err)
	}
}

func TestStableStoreBootstrapIncompleteAndSnapshotTmpIgnored(t *testing.T) {
	incompleteDirectory := filepath.Join(t.TempDir(), "incomplete")
	node, err := Open(Config{NodeID: 1, PeerIDs: []NodeID{1, 2, 3}, Directory: incompleteDirectory})
	if err != nil {
		t.Fatal(err)
	}
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(Config{NodeID: 1, PeerIDs: []NodeID{1, 2, 3}, Directory: incompleteDirectory}); !errors.Is(err, ErrBootstrapIncomplete) {
		t.Fatalf("incomplete bootstrap error = %v, want ErrBootstrapIncomplete", err)
	}

	directory := filepath.Join(t.TempDir(), "snapshot")
	node, err = Open(Config{NodeID: 1, PeerIDs: []NodeID{1, 2, 3}, Directory: directory})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := node.ProcessReady(); err != nil {
		t.Fatal(err)
	}
	snapshotIndex := node.Status().AppliedIndex
	oldOps := filePersistenceOps
	filePersistenceOps.rename = func(oldPath, newPath string) error {
		if strings.Contains(filepath.Base(newPath), "snapshot-") {
			return errors.New("injected snapshot rename failure")
		}
		return oldOps.rename(oldPath, newPath)
	}
	ready := etcdraft.Ready{
		Entries:   []*pb.Entry{{Index: new(uint64(4)), Term: new(uint64(1)), Type: pb.EntryNormal.Enum(), Data: []byte("alpha")}},
		HardState: &pb.HardState{Term: new(uint64(1)), Commit: new(uint64(4))},
	}
	if err := node.store.appendReady(ready); err != nil {
		t.Fatal(err)
	}
	node.appliedIndex = 4
	node.appliedCommands = [][]byte{[]byte("alpha")}
	node.store.appliedIndex = 4
	node.store.commands = cloneCommands(node.appliedCommands)
	err = node.Snapshot()
	filePersistenceOps = oldOps
	if !errors.Is(err, ErrFailed) {
		t.Fatalf("injected snapshot rename error = %v, want ErrFailed", err)
	}
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, fmt.Sprintf("snapshot-%020d.bin.tmp", snapshotIndex+1)), []byte("stale"), 0o600); err != nil {
		t.Fatal(err)
	}
	restarted, err := Open(Config{NodeID: 1, PeerIDs: []NodeID{1, 2, 3}, Directory: directory})
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	if got := commandStrings(restarted.AppliedCommands()); !equalStringValues(got, []string{"alpha"}) {
		t.Fatalf("restarted commands with stale tmp = %v", got)
	}
}

func TestNodeReadyOrderingAndPersistenceFailure(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "ordering")
	node, err := Open(Config{NodeID: 1, PeerIDs: []NodeID{1, 2, 3}, Directory: directory})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := node.ProcessReady(); err != nil {
		t.Fatal(err)
	}
	var events []string
	node.onReadyEvent = func(event string) { events = append(events, event) }
	if err := node.raw.Campaign(); err != nil {
		t.Fatal(err)
	}
	batch, err := node.ProcessReady()
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Messages) == 0 {
		t.Fatal("campaign did not produce outbound messages")
	}
	if !equalStringValues(events, []string{"persisted", "outbound", "applied", "advanced"}) {
		t.Fatalf("Ready events = %v", events)
	}
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}

	failureNode, err := Open(Config{NodeID: 1, PeerIDs: []NodeID{1, 2, 3}, Directory: filepath.Join(t.TempDir(), "failure")})
	if err != nil {
		t.Fatal(err)
	}
	oldOps := filePersistenceOps
	filePersistenceOps.sync = func(*os.File) error { return errors.New("injected sync failure") }
	failedBatch, failedErr := failureNode.ProcessReady()
	filePersistenceOps = oldOps
	if !errors.Is(failedErr, ErrFailed) || len(failedBatch.Messages) != 0 {
		t.Fatalf("persistence failure batch=%#v error=%v", failedBatch, failedErr)
	}
	if _, err := failureNode.ProcessReady(); !errors.Is(err, ErrFailed) {
		t.Fatalf("reprocessing failed node error = %v, want ErrFailed", err)
	}
	if err := failureNode.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestWriteAllHandlesShortWrites(t *testing.T) {
	oldOps := filePersistenceOps
	defer func() { filePersistenceOps = oldOps }()
	filePersistenceOps.write = func(writer io.Writer, data []byte) (int, error) {
		if len(data) == 0 {
			return 0, nil
		}
		return writer.Write(data[:1])
	}
	var buffer bytes.Buffer
	if err := writeAll(&buffer, []byte("short write safe")); err != nil {
		t.Fatal(err)
	}
	if buffer.String() != "short write safe" {
		t.Fatalf("short-write result = %q", buffer.String())
	}
}

func writeTestWAL(t *testing.T, directory string, records ...walRecord) {
	t.Helper()
	var data bytes.Buffer
	data.WriteString(walMagic)
	for _, record := range records {
		data.Write(encodeTestFrame(t, record))
	}
	if err := os.WriteFile(filepath.Join(directory, walName), data.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
}

func encodeTestFrame(t *testing.T, record walRecord) []byte {
	t.Helper()
	payload, err := encodeWALRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	frame := make([]byte, 8+len(payload))
	binary.BigEndian.PutUint32(frame[:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(frame[4:8], crc32.Checksum(payload, crcTable))
	copy(frame[8:], payload)
	return frame
}

func commandStrings(commands [][]byte) []string {
	values := make([]string, len(commands))
	for index, command := range commands {
		values[index] = string(command)
	}
	return values
}

func equalStringValues(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
