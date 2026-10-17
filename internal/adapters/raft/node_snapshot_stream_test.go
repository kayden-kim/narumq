package raft

import (
	"bytes"
	"errors"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	pb "go.etcd.io/raft/v3/raftpb"
	"go.etcd.io/raft/v3/tracker"
	"google.golang.org/protobuf/proto"
)

// snapshotStreamFixture is a 3-voter cluster plus an unbootstrapped learner 4. Messages travel in
// process; streamed snapshots go through handle so each test chooses how the stream ends.
type snapshotStreamFixture struct {
	t      *testing.T
	nodes  map[NodeID]*Node
	active atomic.Bool
	// handle receives each streamed snapshot. It returns the stream outcome.
	handle  func(stream SnapshotStreamDTO) (delivered bool)
	streams []SnapshotStreamDTO
	frames  int
}

func newSnapshotStreamFixture(t *testing.T, active bool) *snapshotStreamFixture {
	t.Helper()
	fixture := &snapshotStreamFixture{t: t, nodes: make(map[NodeID]*Node, 4)}
	fixture.active.Store(active)
	root := t.TempDir()
	for _, nodeID := range []NodeID{1, 2, 3} {
		config := dynamicTestConfig(nodeID, filepath.Join(root, fmt.Sprintf("node-%d", nodeID)))
		config.SnapshotStreamActive = fixture.active.Load
		node, err := Open(config)
		if err != nil {
			t.Fatalf("open voter %d: %v", nodeID, err)
		}
		fixture.nodes[nodeID] = node
	}
	joiner, err := Open(Config{
		NodeID: 4, Directory: filepath.Join(root, "node-4"), DynamicMembership: true,
		ClusterID: dynamicTestClusterID, CertificateSHA256: [32]byte{0x44},
	})
	if err != nil {
		t.Fatalf("open unbootstrapped learner: %v", err)
	}
	fixture.nodes[4] = joiner
	t.Cleanup(func() {
		for nodeID, node := range fixture.nodes {
			if err := node.Close(); err != nil {
				t.Errorf("close node %d: %v", nodeID, err)
			}
		}
	})
	return fixture
}

// pump delivers Raft messages and streamed snapshots until no node has work.
func (f *snapshotStreamFixture) pump() {
	f.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var messages []MessageDTO
		var streams []SnapshotStreamDTO
		progress := false
		for _, nodeID := range []NodeID{1, 2, 3, 4} {
			batch, err := f.nodes[nodeID].ProcessReady()
			if err != nil {
				f.t.Fatalf("node %d ProcessReady(): %v", nodeID, err)
			}
			progress = progress || batch.Progress
			if err := f.nodes[nodeID].CompleteApply(batch.ApplySequence); err != nil {
				f.t.Fatalf("node %d CompleteApply(): %v", nodeID, err)
			}
			for _, message := range batch.Messages {
				f.frames++
				if len(message.Data) > maxMessageSize {
					f.t.Fatalf("message %s of %d bytes exceeds the peer frame limit", message.Type, len(message.Data))
				}
			}
			messages = append(messages, batch.Messages...)
			streams = append(streams, batch.SnapshotStreams...)
		}
		for _, message := range messages {
			if _, err := f.nodes[message.To].Step(message); err != nil {
				f.t.Fatalf("deliver %d -> %d: %v", message.From, message.To, err)
			}
		}
		for _, stream := range streams {
			f.streams = append(f.streams, stream)
			delivered := f.handle != nil && f.handle(stream)
			if err := f.nodes[1].ReportSnapshotStream(stream.To, delivered); err != nil {
				f.t.Fatalf("ReportSnapshotStream(%d, %v): %v", stream.To, delivered, err)
			}
		}
		if len(messages) != 0 || len(streams) != 0 || progress {
			continue
		}
		quiet := true
		for _, node := range f.nodes {
			quiet = quiet && !node.StoragePending()
		}
		if quiet {
			return
		}
		select {
		case <-f.nodes[1].StorageReady():
		case <-f.nodes[2].StorageReady():
		case <-f.nodes[3].StorageReady():
		case <-f.nodes[4].StorageReady():
		case <-time.After(50 * time.Millisecond):
		}
	}
	f.t.Fatal("Raft pump did not become quiet")
}

// deliverStream steps the streamed snapshot into its target like the transport handler does.
func (f *snapshotStreamFixture) deliverStream(stream SnapshotStreamDTO) bool {
	err := f.nodes[stream.To].StepSnapshotStream(
		MessageDTO{From: 1, To: stream.To, Data: stream.Data}, stream.Index, stream.Term)
	if err != nil {
		f.t.Errorf("StepSnapshotStream() = %v", err)
	}
	return err == nil
}

// leaderWithLargeSnapshot elects node 1, compacts a snapshot above the frame limit and proposes
// learner 4 so node 1 has to ship that snapshot.
func (f *snapshotStreamFixture) leaderWithLargeSnapshot(payloadBytes int) []byte {
	f.t.Helper()
	f.pump()
	if err := f.nodes[1].Campaign(); err != nil {
		f.t.Fatal(err)
	}
	f.pump()
	if status := f.nodes[1].Status(); status.Role != RoleLeader {
		f.t.Fatalf("node 1 role = %v, want leader", status.Role)
	}
	payload := bytes.Repeat([]byte{0x5A}, payloadBytes)
	if err := f.nodes[1].SnapshotWithApplicationData(payload); err != nil {
		f.t.Fatalf("leader snapshot: %v", err)
	}
	if err := f.nodes[1].ProposeMembershipChange(MembershipChangeDTO{
		OperationID: addLearnerOperation, MemberID: 4, Action: MembershipAddLearner,
	}); err != nil {
		f.t.Fatalf("propose learner: %v", err)
	}
	// The first snapshot predates the learner, so its ConfState cannot restore node 4. Like the
	// cluster runtime, snapshot again once the configuration change committed. The stream stays
	// off until then, so only the restorable snapshot is shipped.
	wanted := f.active.Swap(false)
	f.pump()
	if err := f.nodes[1].SnapshotWithApplicationData(payload); err != nil {
		f.t.Fatalf("leader snapshot after the configuration change: %v", err)
	}
	f.active.Store(wanted)
	return payload
}

// tickUntil ticks the leader (heartbeats reopen a failed snapshot) until done reports true.
func (f *snapshotStreamFixture) tickUntil(what string, done func() bool) {
	f.t.Helper()
	for range 16 {
		if done() {
			return
		}
		if err := f.nodes[1].Tick(); err != nil {
			f.t.Fatalf("leader tick: %v", err)
		}
		f.pump()
	}
	if !done() {
		f.t.Fatalf("condition not reached after 16 ticks: %s", what)
	}
}

func (f *snapshotStreamFixture) learnerProgress() tracker.Progress {
	f.nodes[1].mu.Lock()
	defer f.nodes[1].mu.Unlock()
	return f.nodes[1].raw.Status().Progress[4]
}

// Traceability: CLU-14; docs/specs/cluster-session-and-delivery.md §11.37, ADR-0062.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-259
func TestSnapshotStreamDeliversOversizedSnapshotWhenActive(t *testing.T) {
	fixture := newSnapshotStreamFixture(t, true)
	fixture.handle = fixture.deliverStream
	payload := fixture.leaderWithLargeSnapshot(4 << 20)
	fixture.pump()
	for range 8 {
		if err := fixture.nodes[1].Tick(); err != nil {
			t.Fatalf("leader tick: %v", err)
		}
		fixture.pump()
	}

	if len(fixture.streams) != 1 {
		t.Fatalf("snapshot streams = %d, want exactly 1 (one delivery completes the follower)", len(fixture.streams))
	}
	stream := fixture.streams[0]
	if stream.To != 4 || stream.Index == 0 || stream.Term == 0 || len(stream.Data) <= 4<<20 {
		t.Fatalf("stream = to %d index %d term %d bytes %d", stream.To, stream.Index, stream.Term, len(stream.Data))
	}
	var message pb.Message
	if err := proto.Unmarshal(stream.Data, &message); err != nil {
		t.Fatal(err)
	}
	if message.GetType() != pb.MsgSnap || !bytes.Equal(message.GetSnapshot().GetData(), payload) {
		t.Fatalf("streamed message type %v does not carry the snapshot payload", message.GetType())
	}
	if got := fixture.nodes[4].ApplicationSnapshot(); !bytes.Equal(got, payload) {
		t.Fatalf("learner snapshot = %d bytes, want the %d byte leader payload", len(got), len(payload))
	}
	if status := fixture.nodes[4].Status(); !status.Bootstrapped || status.AppliedIndex < stream.Index {
		t.Fatalf("learner status = %+v, want bootstrapped at snapshot index %d", status, stream.Index)
	}
	if progress := fixture.learnerProgress(); progress.State == tracker.StateSnapshot || progress.PendingSnapshot != 0 {
		t.Fatalf("learner progress = %v pending %d, want snapshot finished", progress.State, progress.PendingSnapshot)
	}

	command := []byte("after-streamed-snapshot")
	if err := fixture.nodes[1].Propose(command); err != nil {
		t.Fatal(err)
	}
	fixture.pump()
	for _, nodeID := range []NodeID{1, 2, 3, 4} {
		applied := fixture.nodes[nodeID].AppliedCommands()
		if len(applied) == 0 || !bytes.Equal(applied[len(applied)-1], command) {
			t.Fatalf("node %d applied commands = %q, want last %q", nodeID, applied, command)
		}
	}
}

// Scenarios: cluster-session-and-delivery.md:CLU-SCN-259
func TestSnapshotStreamInactiveKeepsOversizedSnapshotRejected(t *testing.T) {
	fixture := newSnapshotStreamFixture(t, false)
	fixture.handle = fixture.deliverStream
	fixture.leaderWithLargeSnapshot(maxMessageSize + 4096)
	fixture.pump()
	for range 4 {
		if err := fixture.nodes[1].Tick(); err != nil {
			t.Fatalf("leader tick: %v", err)
		}
		fixture.pump()
	}
	if len(fixture.streams) != 0 {
		t.Fatalf("inactive feature emitted %d snapshot streams", len(fixture.streams))
	}
	if progress := fixture.learnerProgress(); progress.State == tracker.StateSnapshot || progress.PendingSnapshot != 0 {
		t.Fatalf("learner progress = %v pending %d, want snapshot failure reported", progress.State, progress.PendingSnapshot)
	}
	if fixture.nodes[4].Status().Bootstrapped {
		t.Fatal("learner bootstrapped without a snapshot")
	}
}

// Scenarios: cluster-session-and-delivery.md:CLU-SCN-259
func TestSnapshotStreamFailureLetsRaftRetry(t *testing.T) {
	fixture := newSnapshotStreamFixture(t, true)
	attempts := 0
	fixture.handle = func(stream SnapshotStreamDTO) bool {
		attempts++
		if attempts == 1 {
			return false // first stream fails
		}
		return fixture.deliverStream(stream)
	}
	fixture.leaderWithLargeSnapshot(2 << 20)
	fixture.tickUntil("first stream", func() bool { return attempts == 1 })
	if attempts != 1 || fixture.nodes[4].Status().Bootstrapped {
		t.Fatalf("after the failed stream: attempts=%d bootstrapped=%v", attempts, fixture.nodes[4].Status().Bootstrapped)
	}
	if progress := fixture.learnerProgress(); progress.State == tracker.StateSnapshot {
		t.Fatalf("learner stuck in snapshot state after failure report: %v", progress.State)
	}
	fixture.tickUntil("retry", func() bool { return fixture.nodes[4].Status().Bootstrapped })
	if attempts != 2 || !fixture.nodes[4].Status().Bootstrapped {
		t.Fatalf("after heartbeats: attempts=%d bootstrapped=%v, want a retry that delivers", attempts, fixture.nodes[4].Status().Bootstrapped)
	}
}

func TestStepSnapshotStreamRejectsInvalidInput(t *testing.T) {
	fixture := newSnapshotStreamFixture(t, true)
	var captured SnapshotStreamDTO
	fixture.handle = func(stream SnapshotStreamDTO) bool { captured = stream; return false }
	fixture.leaderWithLargeSnapshot(2 << 20)
	fixture.tickUntil("captured stream", func() bool { return captured.To == 4 })
	if captured.To != 4 {
		t.Fatalf("no snapshot stream captured: %+v", captured)
	}
	learner := fixture.nodes[4]
	valid := MessageDTO{From: 1, To: 4, Data: captured.Data}
	for _, test := range []struct {
		name    string
		message MessageDTO
		index   uint64
		term    uint64
	}{
		{"index differs from manifest", valid, captured.Index + 1, captured.Term},
		{"term differs from manifest", valid, captured.Index, captured.Term + 1},
		{"zero index", valid, 0, captured.Term},
		{"empty", MessageDTO{From: 1, To: 4}, captured.Index, captured.Term},
		{"wrong target", MessageDTO{From: 1, To: 3, Data: captured.Data}, captured.Index, captured.Term},
		{"sender differs from message", MessageDTO{From: 2, To: 4, Data: captured.Data}, captured.Index, captured.Term},
		{"not a snapshot message", MessageDTO{From: 1, To: 4, Data: mustMarshalMessage(t, &pb.Message{
			Type: pb.MsgHeartbeat.Enum(), From: new(uint64(1)), To: new(uint64(4)), Term: new(captured.Term),
		})}, captured.Index, captured.Term},
		{"above the snapshot limit", MessageDTO{From: 1, To: 4, Data: make([]byte, maxDynamicRaftSnapshotSize+1)}, captured.Index, captured.Term},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := learner.StepSnapshotStream(test.message, test.index, test.term); !errors.Is(err, ErrInvalidMessage) {
				t.Fatalf("StepSnapshotStream() = %v, want ErrInvalidMessage", err)
			}
		})
	}
	if learner.Status().Bootstrapped {
		t.Fatal("learner accepted an invalid streamed snapshot")
	}
	// The ordinary Step path keeps its frame limit for a snapshot message.
	if _, err := learner.Step(valid); !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("Step(oversized MsgSnap) = %v, want ErrInvalidMessage", err)
	}
}

func mustMarshalMessage(t *testing.T, message *pb.Message) []byte {
	t.Helper()
	data, err := proto.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestReportSnapshotStreamRejectsUnknownPeer(t *testing.T) {
	fixture := newSnapshotStreamFixture(t, true)
	fixture.pump()
	for _, peer := range []NodeID{0, 1, 9} {
		if err := fixture.nodes[1].ReportSnapshotStream(peer, true); !errors.Is(err, ErrInvalidMessage) {
			t.Fatalf("ReportSnapshotStream(%d) = %v, want ErrInvalidMessage", peer, err)
		}
	}
}
