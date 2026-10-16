package raft

import (
	"bytes"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"go.etcd.io/raft/v3/tracker"
)

type lockedLogBuffer struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (b *lockedLogBuffer) Write(data []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.Write(data)
}

func (b *lockedLogBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.String()
}

// Traceability: CLU-14; docs/specs/cluster-session-and-delivery.md §7, §11.36.
func TestDynamicOversizedSnapshotIsNotSentAndNodeStaysHealthy(t *testing.T) {
	logs := &lockedLogBuffer{}
	logger := slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelWarn}))
	root := t.TempDir()
	nodes := make(map[NodeID]*Node, 4)
	for _, nodeID := range []NodeID{1, 2, 3} {
		config := dynamicTestConfig(nodeID, filepath.Join(root, fmt.Sprintf("node-%d", nodeID)))
		config.Logger = logger
		node, err := Open(config)
		if err != nil {
			t.Fatalf("open voter %d: %v", nodeID, err)
		}
		nodes[nodeID] = node
	}
	joiner, err := Open(Config{
		NodeID: 4, Directory: filepath.Join(root, "node-4"), DynamicMembership: true,
		ClusterID: dynamicTestClusterID, CertificateSHA256: [32]byte{0x44}, Logger: logger,
	})
	if err != nil {
		t.Fatalf("open unbootstrapped learner: %v", err)
	}
	nodes[4] = joiner
	t.Cleanup(func() {
		for nodeID, node := range nodes {
			if err := node.Close(); err != nil {
				t.Errorf("close node %d: %v", nodeID, err)
			}
		}
	})

	oversizedSnapshotMessages := 0
	// pump delivers messages until no node has work. It fails the test when any
	// node reports an error or emits a message above the peer frame limit.
	pump := func() {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			var messages []MessageDTO
			progress := false
			for _, nodeID := range []NodeID{1, 2, 3, 4} {
				batch, err := nodes[nodeID].ProcessReady()
				if err != nil {
					t.Fatalf("node %d ProcessReady(): %v", nodeID, err)
				}
				progress = progress || batch.Progress
				if err := nodes[nodeID].CompleteApply(batch.ApplySequence); err != nil {
					t.Fatalf("node %d CompleteApply(): %v", nodeID, err)
				}
				for _, message := range batch.Messages {
					if len(message.Data) > maxMessageSize {
						oversizedSnapshotMessages++
					}
				}
				messages = append(messages, batch.Messages...)
			}
			for _, message := range messages {
				if _, err := nodes[message.To].Step(message); err != nil {
					t.Fatalf("deliver %d -> %d: %v", message.From, message.To, err)
				}
			}
			if len(messages) != 0 || progress {
				continue
			}
			quiet := true
			for _, node := range nodes {
				quiet = quiet && !node.StoragePending()
			}
			if quiet {
				return
			}
			select {
			case <-nodes[1].StorageReady():
			case <-nodes[2].StorageReady():
			case <-nodes[3].StorageReady():
			case <-nodes[4].StorageReady():
			case <-time.After(50 * time.Millisecond):
			}
		}
		t.Fatal("Raft pump did not become quiet")
	}

	pump()
	if err := nodes[1].Campaign(); err != nil {
		t.Fatal(err)
	}
	pump()
	if status := nodes[1].Status(); status.Role != RoleLeader {
		t.Fatalf("node 1 role = %v, want leader", status.Role)
	}

	// The snapshot payload alone exceeds one frame, so any MsgSnap carrying it
	// is larger than maxMessageSize on the wire.
	if err := nodes[1].SnapshotWithApplicationData(bytes.Repeat([]byte{0x5A}, maxMessageSize+4096)); err != nil {
		t.Fatalf("leader snapshot: %v", err)
	}
	if err := nodes[1].ProposeMembershipChange(MembershipChangeDTO{
		OperationID: addLearnerOperation, MemberID: 4, Action: MembershipAddLearner,
	}); err != nil {
		t.Fatalf("propose learner: %v", err)
	}
	pump()

	// Each heartbeat response re-opens the learner for a snapshot attempt.
	for range 8 {
		if err := nodes[1].Tick(); err != nil {
			t.Fatalf("leader tick: %v", err)
		}
		pump()
	}

	if _, err := nodes[1].ProcessReady(); err != nil {
		t.Fatalf("leader failed after oversized snapshot: %v", err)
	}
	if oversizedSnapshotMessages != 0 {
		t.Fatalf("oversized messages emitted to the transport = %d, want 0", oversizedSnapshotMessages)
	}
	nodes[1].mu.Lock()
	learnerProgress := nodes[1].raw.Status().Progress[4]
	nodes[1].mu.Unlock()
	if learnerProgress.State == tracker.StateSnapshot || learnerProgress.PendingSnapshot != 0 {
		t.Fatalf("learner progress = %v pendingSnapshot=%d, want snapshot failure reported",
			learnerProgress.State, learnerProgress.PendingSnapshot)
	}
	if joinerStatus := joiner.Status(); joinerStatus.Bootstrapped {
		t.Fatalf("learner unexpectedly bootstrapped without a snapshot: %+v", joinerStatus)
	}

	command := []byte("after-oversized-snapshot")
	if err := nodes[1].Propose(command); err != nil {
		t.Fatalf("propose after oversized snapshot: %v", err)
	}
	pump()
	for _, nodeID := range []NodeID{1, 2, 3} {
		applied := nodes[nodeID].AppliedCommands()
		if len(applied) == 0 || !bytes.Equal(applied[len(applied)-1], command) {
			t.Fatalf("node %d applied commands = %q, want last %q", nodeID, applied, command)
		}
	}

	warnings := strings.Count(logs.String(), "raft snapshot exceeds peer frame limit; not sent")
	if warnings != 1 {
		t.Fatalf("oversized snapshot warnings = %d, want exactly 1 per peer interval\n%s", warnings, logs.String())
	}
	for _, field := range []string{"peer_id=4", "snapshot_index=", "encoded_bytes=", "limit_bytes=1048576"} {
		if !strings.Contains(logs.String(), field) {
			t.Fatalf("warning lacks field %q:\n%s", field, logs.String())
		}
	}
}

func TestPeerWarningLimiterAdmitsOncePerPeerInterval(t *testing.T) {
	var limiter peerWarningLimiter
	start := time.Unix(1000, 0)
	const interval = 30 * time.Second
	for _, step := range []struct {
		peer NodeID
		at   time.Duration
		want bool
	}{
		{4, 0, true},
		{4, interval - time.Nanosecond, false},
		{5, time.Second, true},
		{4, interval, true},
		{4, interval + time.Second, false},
	} {
		if got := limiter.allow(step.peer, start.Add(step.at), interval); got != step.want {
			t.Fatalf("allow(peer %d at +%v) = %t, want %t", step.peer, step.at, got, step.want)
		}
	}
}
