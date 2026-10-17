package raft

import (
	"bytes"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// Traceability: CLU-14; docs/specs/cluster-session-and-delivery.md §11.22.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-156
func TestWALWorkerAndSnapshotHandleReplacementKeepRecoverableState(t *testing.T) {
	root := t.TempDir()
	directory := filepath.Join(root, "node-1")
	nodes := make(map[NodeID]*Node, 3)
	for _, nodeID := range []NodeID{1, 2, 3} {
		nodeDirectory := filepath.Join(root, fmt.Sprintf("node-%d", nodeID))
		node, err := Open(dynamicTestConfig(nodeID, nodeDirectory))
		if err != nil {
			t.Fatalf("open node %d: %v", nodeID, err)
		}
		nodes[nodeID] = node
	}
	closed := false
	closeAll := func() {
		if closed {
			return
		}
		closed = true
		for nodeID, node := range nodes {
			if err := node.Close(); err != nil {
				t.Errorf("close node %d: %v", nodeID, err)
			}
		}
	}
	defer closeAll()

	applied := 0
	// pump delivers messages until the cluster is quiet and counts node 1's applied commands.
	pump := func() error {
		for range 512 {
			var messages []MessageDTO
			for _, nodeID := range []NodeID{1, 2, 3} {
				batch, err := processReadyForTest(t, nodes[nodeID])
				if err != nil {
					return fmt.Errorf("node %d ProcessReady(): %w", nodeID, err)
				}
				if nodeID == 1 {
					applied += len(batch.Commands)
				}
				messages = append(messages, batch.Messages...)
			}
			if len(messages) == 0 {
				return nil
			}
			for _, message := range messages {
				if _, err := nodes[message.To].Step(message); err != nil {
					return fmt.Errorf("deliver %d -> %d: %w", message.From, message.To, err)
				}
			}
		}
		return errors.New("Raft pump exceeded 512 rounds")
	}

	if err := pump(); err != nil {
		t.Fatal(err)
	}
	if err := nodes[1].Campaign(); err != nil {
		t.Fatal(err)
	}
	if err := pump(); err != nil {
		t.Fatal(err)
	}
	if nodes[1].Status().Role != RoleLeader {
		t.Fatalf("node 1 role = %v, want leader", nodes[1].Status().Role)
	}

	// The driver keeps the WAL worker busy with appends. The snapshotter replaces the WAL handle at
	// every successful snapshot. Under -race, any unsynchronized worker/handle access is reported.
	const snapshots = 20
	accepted, snapshotCount := 0, 0
	stop := make(chan struct{})
	var driverErr error
	var group sync.WaitGroup
	group.Add(1)
	go func() {
		defer group.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			err := nodes[1].Propose([]byte(fmt.Sprintf("command-%d", accepted)))
			if err == nil {
				accepted++
			} else if !errors.Is(err, ErrBackpressure) {
				driverErr = fmt.Errorf("propose: %w", err)
				return
			}
			if err := pump(); err != nil {
				driverErr = err
				return
			}
		}
	}()
	deadline := time.Now().Add(30 * time.Second)
	payload := []byte("snapshot-payload")
	snapshotIndex := func() uint64 {
		nodes[1].mu.Lock()
		defer nodes[1].mu.Unlock()
		return nodes[1].store.snapshotIndex
	}
	// A snapshot at an unchanged applied index is a no-op, so only an advanced snapshot index counts.
	replaced := snapshotIndex()
	for snapshotCount < snapshots && time.Now().Before(deadline) {
		err := nodes[1].SnapshotWithApplicationData(payload)
		if errors.Is(err, ErrBackpressure) {
			time.Sleep(time.Millisecond)
			continue
		}
		if err != nil {
			close(stop)
			group.Wait()
			t.Fatalf("snapshot %d: %v", snapshotCount, err)
		}
		if index := snapshotIndex(); index != replaced {
			replaced = index
			snapshotCount++
		}
		time.Sleep(time.Millisecond)
	}
	close(stop)
	group.Wait()
	if driverErr != nil {
		t.Fatal(driverErr)
	}
	if snapshotCount != snapshots {
		t.Fatalf("completed %d snapshots, want %d before the deadline", snapshotCount, snapshots)
	}
	if err := pump(); err != nil {
		t.Fatal(err)
	}
	if applied != accepted {
		t.Fatalf("applied %d commands, want every accepted proposal (%d)", applied, accepted)
	}
	if err := nodes[1].SnapshotWithApplicationData(payload); err != nil {
		t.Fatalf("final snapshot: %v", err)
	}
	final := payload
	finalApplied := nodes[1].Status().AppliedIndex
	closeAll()

	reopened, err := Open(dynamicTestConfig(1, directory))
	if err != nil {
		t.Fatalf("reopen after concurrent snapshots: %v", err)
	}
	defer reopened.Close()
	if got := reopened.ApplicationSnapshot(); !bytes.Equal(got, final) {
		t.Fatalf("recovered snapshot = %q, want %q", got, final)
	}
	if got := reopened.Status().AppliedIndex; got != finalApplied {
		t.Fatalf("recovered applied index = %d, want %d", got, finalApplied)
	}
}
