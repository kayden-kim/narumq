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
	"syscall"
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
	if _, err := processReadyForTest(t, node); err != nil {
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
	if _, err := processReadyForTest(t, node); err != nil {
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
	if _, err := processReadyForTest(t, node); err != nil {
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
	if err := appendReadyForTest(node.store, ready); err != nil {
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

func TestNodeBootstrapSyncFailureIsTerminal(t *testing.T) {
	failureNode, err := Open(Config{NodeID: 1, PeerIDs: []NodeID{1, 2, 3}, Directory: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	oldOps := filePersistenceOps
	filePersistenceOps.sync = func(*os.File) error { return errors.New("injected sync failure") }
	failedBatch, failedErr := processReadyForTest(t, failureNode)
	filePersistenceOps = oldOps
	if !errors.Is(failedErr, ErrFailed) || len(failedBatch.Messages) != 0 {
		t.Fatalf("persistence failure batch=%#v error=%v", failedBatch, failedErr)
	}
	if _, err := processReadyForTest(t, failureNode); !errors.Is(err, ErrFailed) {
		t.Fatalf("reprocessing failed node error = %v, want ErrFailed", err)
	}
	if err := failureNode.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestNodeWALWriteFailureStopsOutbound(t *testing.T) {
	root := t.TempDir()
	directories := map[NodeID]string{
		1: filepath.Join(root, "node-1"),
		2: filepath.Join(root, "node-2"),
		3: filepath.Join(root, "node-3"),
	}
	nodes := make(map[NodeID]*Node, 3)
	for _, nodeID := range []NodeID{1, 2, 3} {
		node, err := Open(Config{
			NodeID: nodeID, PeerIDs: []NodeID{1, 2, 3}, Directory: directories[nodeID],
		})
		if err != nil {
			t.Fatalf("open node %d: %v", nodeID, err)
		}
		nodes[nodeID] = node
	}
	defer func() {
		for nodeID, node := range nodes {
			if err := node.Close(); err != nil {
				t.Errorf("close node %d: %v", nodeID, err)
			}
		}
	}()

	pump := func() {
		t.Helper()
		var queue []MessageDTO
		for range 512 {
			progress := false
			for _, nodeID := range []NodeID{1, 2, 3} {
				batch, err := processReadyForTest(t, nodes[nodeID])
				if err != nil {
					t.Fatalf("node %d ProcessReady() = %v", nodeID, err)
				}
				if len(batch.Messages) != 0 || len(batch.Commands) != 0 {
					progress = true
				}
				queue = append(queue, batch.Messages...)
			}
			if len(queue) != 0 {
				pending := queue
				queue = nil
				for _, message := range pending {
					if _, err := nodes[message.To].Step(message); err != nil {
						t.Fatalf("deliver %d -> %d: %v", message.From, message.To, err)
					}
					progress = true
				}
			}
			if !progress && len(queue) == 0 {
				return
			}
		}
		t.Fatal("node pump exceeded 512 rounds")
	}

	pump()
	if err := nodes[1].Campaign(); err != nil {
		t.Fatal(err)
	}
	pump()
	if got := nodes[1].Status().Role; got != RoleLeader {
		t.Fatalf("node 1 role after campaign = %v, want leader", got)
	}
	if err := nodes[1].Propose([]byte("baseline")); err != nil {
		t.Fatal(err)
	}
	pump()

	commandsBefore := commandStrings(nodes[1].AppliedCommands())
	if err := nodes[1].Propose([]byte("write-failure")); err != nil {
		t.Fatal(err)
	}

	walPath := filepath.Join(directories[1], walName)
	oldWrite := filePersistenceOps.write
	defer func() { filePersistenceOps.write = oldWrite }()
	filePersistenceOps.write = func(writer io.Writer, data []byte) (int, error) {
		file, ok := writer.(*os.File)
		if ok && file.Name() == walPath {
			return 0, syscall.ENOSPC
		}
		return oldWrite(writer, data)
	}
	batch, processErr := processReadyForTest(t, nodes[1])
	filePersistenceOps.write = oldWrite

	if !errors.Is(processErr, ErrFailed) || !errors.Is(processErr, ErrStorage) || !IsStorageFull(processErr) || !errors.Is(processErr, syscall.ENOSPC) {
		t.Fatalf("ProcessReady() error = %v, want ErrFailed and ErrStorage wrapping ENOSPC", processErr)
	}
	if len(batch.Messages) != 0 || len(batch.Commands) != 0 {
		t.Fatalf("failed Ready batch = %#v, want empty", batch)
	}
	if got := commandStrings(nodes[1].AppliedCommands()); !equalStringValues(got, commandsBefore) {
		t.Fatalf("applied commands after WAL failure = %v, want %v", got, commandsBefore)
	}
	if err := nodes[1].Campaign(); !errors.Is(err, ErrFailed) {
		t.Fatalf("Campaign() after WAL failure = %v, want ErrFailed", err)
	}
	if err := nodes[1].Propose([]byte("after-wal-failure")); !errors.Is(err, ErrFailed) {
		t.Fatalf("Propose() after WAL failure = %v, want ErrFailed", err)
	}
	if err := nodes[1].ReadIndex([]byte("after-wal-failure")); !errors.Is(err, ErrFailed) {
		t.Fatalf("ReadIndex() after WAL failure = %v, want ErrFailed", err)
	}
	if got := commandStrings(nodes[1].AppliedCommands()); !equalStringValues(got, commandsBefore) {
		t.Fatalf("applied commands after rejected proposal/read = %v, want %v", got, commandsBefore)
	}
	if _, err := processReadyForTest(t, nodes[1]); !errors.Is(err, ErrFailed) {
		t.Fatalf("ProcessReady() after WAL failure = %v, want ErrFailed", err)
	}
	for nodeID, node := range nodes {
		if err := node.Close(); err != nil {
			t.Fatalf("close node %d after WAL failure: %v", nodeID, err)
		}
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
