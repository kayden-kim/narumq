package raft

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	pb "go.etcd.io/raft/v3/raftpb"
)

func storageBoundaryNode(t *testing.T) *Node {
	t.Helper()
	node, err := Open(Config{NodeID: 3, PeerIDs: []NodeID{1, 2, 3}, Directory: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := node.Close(); err != nil {
			t.Error(err)
		}
	})
	if _, err := processReadyForTest(t, node); err != nil {
		t.Fatal(err)
	}
	return node
}

func holdWALSync(t *testing.T, node *Node) (<-chan struct{}, func()) {
	t.Helper()
	original := filePersistenceOps
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	filePersistenceOps.sync = func(file *os.File) error {
		if filepath.Base(file.Name()) == walName {
			select {
			case <-entered:
			default:
				close(entered)
				<-release
			}
		}
		return original.sync(file)
	}
	t.Cleanup(func() { unblock(); _ = node.Close(); filePersistenceOps = original })
	return entered, unblock
}

func waitStorageBoundary(t *testing.T, entered <-chan struct{}) {
	t.Helper()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("WAL worker did not reach the held sync")
	}
}

func stepStorageAppend(t *testing.T, node *Node, leader, term, index, previousTerm, commit uint64, payload []byte) {
	t.Helper()
	message := &pb.Message{
		Type: pb.MsgApp.Enum(), From: new(leader), To: new(uint64(3)), Term: new(term),
		Index: new(index - 1), LogTerm: new(previousTerm), Commit: new(commit),
		Entries: []*pb.Entry{{Index: new(index), Term: new(term), Type: pb.EntryNormal.Enum(), Data: payload}},
	}
	if _, err := node.Step(MessageDTO{From: NodeID(leader), To: 3, Data: mustMarshal(t, message)}); err != nil {
		t.Fatal(err)
	}
}

func awaitStorageApply(t *testing.T, node *Node) BatchDTO {
	t.Helper()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	for {
		batch, err := node.ProcessReady()
		if err != nil {
			t.Fatal(err)
		}
		if batch.ApplySequence != 0 {
			return batch
		}
		if !batch.Progress {
			select {
			case <-node.StorageReady():
			case <-timer.C:
				t.Fatal("durable entry did not reach application apply")
			}
		}
	}
}

// Scenarios: cluster-session-and-delivery.md:CLU-SCN-114
func TestNodeHeldAppendWithholdsReplicationAndApply(t *testing.T) {
	node := storageBoundaryNode(t)
	entered, release := holdWALSync(t, node)
	stepStorageAppend(t, node, 1, 2, 4, 1, 4, []byte("durable"))
	batch, err := node.ProcessReady()
	if err != nil {
		t.Fatal(err)
	}
	waitStorageBoundary(t, entered)
	for _, message := range batch.Messages {
		if message.Type == "MsgAppResp" {
			t.Fatal("append response escaped before sync")
		}
	}
	if len(batch.Commands) != 0 || node.Status().AppliedIndex != 3 {
		t.Fatal("application advanced before sync")
	}
	// A heartbeat can be stepped and answered while the append is still held.
	heartbeat := &pb.Message{Type: pb.MsgHeartbeat.Enum(), From: new(uint64(1)), To: new(uint64(3)), Term: new(uint64(2)), Commit: new(uint64(4))}
	if _, err := node.Step(MessageDTO{From: 1, To: 3, Data: mustMarshal(t, heartbeat)}); err != nil {
		t.Fatal(err)
	}
	batch, err = node.ProcessReady()
	if err != nil {
		t.Fatal(err)
	}
	answered := false
	for _, message := range batch.Messages {
		if message.Type == "MsgAppResp" {
			t.Fatal("append response escaped while sync remained held")
		}
		answered = answered || message.Type == "MsgHeartbeatResp"
	}
	if !answered || len(batch.Commands) != 0 {
		t.Fatal("independent heartbeat did not progress without exposing apply")
	}
	release()
	batch = awaitStorageApply(t, node)
	if got := commandStrings(batch.Commands); !equalStringValues(got, []string{"durable"}) {
		t.Fatalf("applied commands = %v", got)
	}
	if node.raw.BasicStatus().Applied != 3 {
		t.Fatal("Raft apply completed before the consumer acknowledged it")
	}
	if err := node.CompleteApply(batch.ApplySequence + 1); !errors.Is(err, ErrApplySequence) {
		t.Fatalf("wrong apply token = %v", err)
	}
	if err := node.CompleteApply(batch.ApplySequence); err != nil {
		t.Fatal(err)
	}
	if node.raw.BasicStatus().Applied != 4 {
		t.Fatal("consumer completion did not advance Raft apply")
	}
	if err := node.CompleteApply(batch.ApplySequence); !errors.Is(err, ErrApplySequence) {
		t.Fatalf("duplicate apply token = %v", err)
	}
}

// Scenarios: cluster-session-and-delivery.md:CLU-SCN-116
func TestNodeCommitOnlyHardStateWaitsForSync(t *testing.T) {
	node := storageBoundaryNode(t)
	stepStorageAppend(t, node, 1, 2, 4, 1, 3, []byte("committed-later"))
	if _, err := processReadyForTest(t, node); err != nil {
		t.Fatal(err)
	}
	if node.Status().AppliedIndex != 3 {
		t.Fatal("uncommitted entry was applied")
	}
	entered, release := holdWALSync(t, node)
	heartbeat := &pb.Message{Type: pb.MsgHeartbeat.Enum(), From: new(uint64(1)), To: new(uint64(3)), Term: new(uint64(2)), Commit: new(uint64(4))}
	if _, err := node.Step(MessageDTO{From: 1, To: 3, Data: mustMarshal(t, heartbeat)}); err != nil {
		t.Fatal(err)
	}
	batch, err := node.ProcessReady()
	if err != nil {
		t.Fatal(err)
	}
	waitStorageBoundary(t, entered)
	if len(batch.Commands) != 0 || node.Status().AppliedIndex != 3 || node.store.hardState.GetCommit() != 3 {
		t.Fatal("commit-only update applied before sync")
	}
	release()
	batch = awaitStorageApply(t, node)
	if got := commandStrings(batch.Commands); !equalStringValues(got, []string{"committed-later"}) {
		t.Fatalf("apply = %v", got)
	}
	if err := node.CompleteApply(batch.ApplySequence); err != nil {
		t.Fatal(err)
	}
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(Config{NodeID: 3, PeerIDs: []NodeID{1, 2, 3}, Directory: node.store.directory})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if reopened.Status().CommitIndex != 4 || !equalStringValues(commandStrings(reopened.AppliedCommands()), []string{"committed-later"}) {
		t.Fatal("commit-only durability did not survive reopen")
	}
}

// Scenarios: cluster-session-and-delivery.md:CLU-SCN-116
func TestNodeOldTermCompletionCannotStabilizeReplacement(t *testing.T) {
	node := storageBoundaryNode(t)
	original := filePersistenceOps
	oldEntered, newEntered := make(chan struct{}), make(chan struct{})
	oldRelease, newRelease := make(chan struct{}), make(chan struct{})
	var oldOnce, newOnce sync.Once
	unblockOld := func() { oldOnce.Do(func() { close(oldRelease) }) }
	unblockNew := func() { newOnce.Do(func() { close(newRelease) }) }
	calls := 0 // The single WAL writer owns this counter.
	filePersistenceOps.sync = func(file *os.File) error {
		if filepath.Base(file.Name()) == walName {
			calls++
			switch calls {
			case 1:
				close(oldEntered)
				<-oldRelease
			case 2:
				close(newEntered)
				<-newRelease
			}
		}
		return original.sync(file)
	}
	t.Cleanup(func() { unblockOld(); unblockNew(); _ = node.Close(); filePersistenceOps = original })
	stepStorageAppend(t, node, 1, 2, 4, 1, 3, []byte("old-term"))
	if _, err := node.ProcessReady(); err != nil {
		t.Fatal(err)
	}
	waitStorageBoundary(t, oldEntered)
	stepStorageAppend(t, node, 2, 3, 4, 1, 4, []byte("replacement"))
	if _, err := node.ProcessReady(); err != nil {
		t.Fatal(err)
	}
	unblockOld()
	waitStorageBoundary(t, newEntered)
	batch, err := node.ProcessReady()
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Commands) != 0 || node.Status().AppliedIndex != 3 {
		t.Fatal("old completion stabilized a replacement that was not synced")
	}
	unblockNew()
	batch = awaitStorageApply(t, node)
	if got := commandStrings(batch.Commands); !equalStringValues(got, []string{"replacement"}) {
		t.Fatalf("overwrite apply = %v", got)
	}
	if err := node.CompleteApply(batch.ApplySequence); err != nil {
		t.Fatal(err)
	}
}

// Scenarios: cluster-session-and-delivery.md:CLU-SCN-116
func TestNodeStorageSaturationPreservesAcceptedFIFO(t *testing.T) {
	for _, scenario := range []struct {
		name string
		size int
	}{{"count", 32}, {"bytes", 64 << 10}} {
		t.Run(scenario.name, func(t *testing.T) {
			size := scenario.size
			node := storageBoundaryNode(t)
			entered, release := holdWALSync(t, node)
			stepStorageAppend(t, node, 1, 2, 4, 1, 3, make([]byte, size))
			if _, err := node.ProcessReady(); err != nil {
				t.Fatal(err)
			}
			waitStorageBoundary(t, entered)
			last := uint64(4)
			for !node.storageSaturated() {
				last++
				stepStorageAppend(t, node, 1, 2, last, 2, 3, make([]byte, size))
				if _, err := node.ProcessReady(); err != nil {
					t.Fatal(err)
				}
			}
			if node.appendPending > walAppendCapacity || node.appendBytes > walAppendBytes {
				t.Fatal("storage exceeded its in-flight count/byte budget")
			}
			rejected := &pb.Message{Type: pb.MsgApp.Enum(), From: new(uint64(1)), To: new(uint64(3)), Term: new(uint64(2)), Index: new(last), LogTerm: new(uint64(2)), Commit: new(uint64(3)), Entries: []*pb.Entry{{Index: new(last + 1), Term: new(uint64(2)), Type: pb.EntryNormal.Enum(), Data: []byte("retry")}}}
			if _, err := node.Step(MessageDTO{From: 1, To: 3, Data: mustMarshal(t, rejected)}); !errors.Is(err, ErrBackpressure) {
				t.Fatalf("saturated admission = %v", err)
			}
			if err := node.Tick(); err != nil {
				t.Fatal(err)
			}
			release()
			if _, err := processReadyForTest(t, node); err != nil {
				t.Fatal(err)
			}
			if _, err := node.Step(MessageDTO{From: 1, To: 3, Data: mustMarshal(t, rejected)}); err != nil {
				t.Fatal(err)
			}
			if _, err := processReadyForTest(t, node); err != nil {
				t.Fatal(err)
			}
			if err := node.Close(); err != nil {
				t.Fatal(err)
			}
			records, err := readWAL(filepath.Join(node.store.directory, walName))
			if err != nil {
				t.Fatal(err)
			}
			next := uint64(4)
			for _, record := range records {
				for _, entry := range record.entries {
					if entry.GetIndex() < 4 {
						continue
					}
					if entry.GetIndex() != next {
						t.Fatalf("durable FIFO index=%d want=%d", entry.GetIndex(), next)
					}
					next++
				}
			}
			if next != last+2 {
				t.Fatalf("accepted/retried tail missing: next=%d want=%d", next, last+2)
			}
		})
	}
}
