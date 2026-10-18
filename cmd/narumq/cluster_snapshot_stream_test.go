package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	raftadapter "narumq/internal/adapters/raft"
	tcpadapter "narumq/internal/adapters/tcp"
	"narumq/internal/application/membership"
)

type snapshotStreamLogBuffer struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (b *snapshotStreamLogBuffer) Write(data []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.Write(data)
}

func (b *snapshotStreamLogBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.String()
}

type fakeSnapshotStreamSender struct {
	send  func(ctx context.Context, request tcpadapter.SnapshotStreamDTO) error
	calls chan tcpadapter.SnapshotStreamDTO
}

func (s *fakeSnapshotStreamSender) SendSnapshot(ctx context.Context, request tcpadapter.SnapshotStreamDTO) error {
	if s.calls != nil {
		s.calls <- request
	}
	return s.send(ctx, request)
}

type snapshotStreamReport struct {
	peer      raftadapter.NodeID
	delivered bool
}

type fakeSnapshotStreamReporter struct {
	mu      sync.Mutex
	reports []snapshotStreamReport
	err     error
}

func (r *fakeSnapshotStreamReporter) ReportSnapshotStream(peer raftadapter.NodeID, delivered bool) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reports = append(r.reports, snapshotStreamReport{peer, delivered})
	return r.err
}

func (r *fakeSnapshotStreamReporter) snapshot() []snapshotStreamReport {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]snapshotStreamReport(nil), r.reports...)
}

func newTestSnapshotStreams(sender clusterSnapshotStreamSender, isolated func(uint64) bool) (*clusterSnapshotStreams, *fakeSnapshotStreamReporter, *snapshotStreamLogBuffer) {
	logs := &snapshotStreamLogBuffer{}
	reporter := &fakeSnapshotStreamReporter{}
	logger := slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelInfo}))
	return newClusterSnapshotStreams(logger, sender, reporter, isolated), reporter, logs
}

// Traceability: CLU-14; docs/contracts/raft-snapshot-stream-v1.md §7, §9.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-261
func TestClusterSnapshotStreamsReportDeliveryAndLogOutcome(t *testing.T) {
	request := raftadapter.SnapshotStreamDTO{To: 4, Index: 42, Term: 3, Data: make([]byte, 3<<20)}
	sender := &fakeSnapshotStreamSender{send: func(context.Context, tcpadapter.SnapshotStreamDTO) error { return nil }}
	streams, reporter, logs := newTestSnapshotStreams(sender, nil)
	streams.start(context.Background(), []raftadapter.SnapshotStreamDTO{request})
	streams.wait()
	if got := reporter.snapshot(); len(got) != 1 || got[0] != (snapshotStreamReport{4, true}) {
		t.Fatalf("reports = %+v, want one delivered report for peer 4", got)
	}
	select {
	case <-streams.wake:
	default:
		t.Fatal("a reported result did not wake the driver")
	}
	output := logs.String()
	for _, want := range []string{
		`msg="raft snapshot stream started"`, "peer_id=4", "snapshot_index=42", "total_bytes=3145728",
		`msg="raft snapshot stream completed"`, "chunks=6",
	} {
		if !strings.Contains(output, want) {
			t.Errorf("log lacks %q:\n%s", want, output)
		}
	}

	failing := &fakeSnapshotStreamSender{send: func(context.Context, tcpadapter.SnapshotStreamDTO) error {
		return &tcpadapter.SnapshotStreamError{Reason: tcpadapter.SnapshotReasonDigest, Err: errors.New("private detail")}
	}}
	streams, reporter, logs = newTestSnapshotStreams(failing, nil)
	streams.start(context.Background(), []raftadapter.SnapshotStreamDTO{request})
	streams.wait()
	if got := reporter.snapshot(); len(got) != 1 || got[0] != (snapshotStreamReport{4, false}) {
		t.Fatalf("reports = %+v, want one failure report for peer 4", got)
	}
	output = logs.String()
	for _, want := range []string{"level=WARN", `msg="raft snapshot stream failed"`, "peer_id=4", "snapshot_index=42", "reason=digest"} {
		if !strings.Contains(output, want) {
			t.Errorf("failure log lacks %q:\n%s", want, output)
		}
	}
	if strings.Contains(output, "private detail") {
		t.Errorf("failure log leaked the raw error:\n%s", output)
	}
}

// Scenarios: cluster-session-and-delivery.md:CLU-SCN-261
func TestClusterSnapshotStreamsAllowOneTransferPerPeer(t *testing.T) {
	release := make(chan struct{})
	sender := &fakeSnapshotStreamSender{
		calls: make(chan tcpadapter.SnapshotStreamDTO, 4),
		send: func(ctx context.Context, _ tcpadapter.SnapshotStreamDTO) error {
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
	}
	streams, reporter, _ := newTestSnapshotStreams(sender, nil)
	first := raftadapter.SnapshotStreamDTO{To: 4, Index: 10, Term: 2, Data: []byte{1}}
	streams.start(context.Background(), []raftadapter.SnapshotStreamDTO{first})
	<-sender.calls
	// Raft cannot create another MsgSnap for peer 4 before the report, but the coordinator
	// still drops one rather than running two transfers to the same peer.
	streams.start(context.Background(), []raftadapter.SnapshotStreamDTO{{To: 4, Index: 11, Term: 2, Data: []byte{2}}})
	other := raftadapter.SnapshotStreamDTO{To: 5, Index: 11, Term: 2, Data: []byte{3}}
	streams.start(context.Background(), []raftadapter.SnapshotStreamDTO{other})
	if got := <-sender.calls; got.To != 5 {
		t.Fatalf("second transfer went to peer %d, want peer 5 (peer 4 is busy)", got.To)
	}
	select {
	case extra := <-sender.calls:
		t.Fatalf("a second transfer to peer %d started while one was in flight", extra.To)
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	streams.wait()
	if got := reporter.snapshot(); len(got) != 2 {
		t.Fatalf("reports = %+v, want exactly one per transfer", got)
	}
	// After the report the peer can receive a new transfer.
	again := &fakeSnapshotStreamSender{send: func(context.Context, tcpadapter.SnapshotStreamDTO) error { return nil }}
	streams.sender = again
	streams.start(context.Background(), []raftadapter.SnapshotStreamDTO{{To: 4, Index: 12, Term: 2, Data: []byte{4}}})
	streams.wait()
	if got := reporter.snapshot(); len(got) != 3 {
		t.Fatalf("reports after a new transfer = %+v, want 3", got)
	}
}

// Scenarios: cluster-session-and-delivery.md:CLU-SCN-261
func TestClusterSnapshotStreamsCancelWithContextAndReportFailure(t *testing.T) {
	sender := &fakeSnapshotStreamSender{send: func(ctx context.Context, _ tcpadapter.SnapshotStreamDTO) error {
		<-ctx.Done()
		return &tcpadapter.SnapshotStreamError{Reason: tcpadapter.SnapshotReasonCanceled, Err: ctx.Err()}
	}}
	streams, reporter, logs := newTestSnapshotStreams(sender, nil)
	ctx, cancel := context.WithCancel(context.Background())
	streams.start(ctx, []raftadapter.SnapshotStreamDTO{{To: 4, Index: 10, Term: 2, Data: []byte{1}}})
	cancel()
	streams.wait()
	if got := reporter.snapshot(); len(got) != 1 || got[0].delivered {
		t.Fatalf("reports = %+v, want one failure report", got)
	}
	if !strings.Contains(logs.String(), "reason=canceled") {
		t.Fatalf("cancellation log lacks reason=canceled:\n%s", logs.String())
	}
}

// Scenarios: cluster-session-and-delivery.md:CLU-SCN-261
func TestClusterSnapshotStreamsSkipIsolatedPeerWithFailureReport(t *testing.T) {
	sender := &fakeSnapshotStreamSender{send: func(context.Context, tcpadapter.SnapshotStreamDTO) error {
		t.Error("a fault-isolated peer received a snapshot stream")
		return nil
	}}
	streams, reporter, _ := newTestSnapshotStreams(sender, func(peer uint64) bool { return peer == 4 })
	streams.start(context.Background(), []raftadapter.SnapshotStreamDTO{{To: 4, Index: 10, Term: 2, Data: []byte{1}}})
	streams.wait()
	if got := reporter.snapshot(); len(got) != 1 || got[0] != (snapshotStreamReport{4, false}) {
		t.Fatalf("reports = %+v, want a failure report so Raft leaves snapshot state", got)
	}
}

func TestClusterSnapshotStreamsAreNilSafe(t *testing.T) {
	var streams *clusterSnapshotStreams
	streams.start(context.Background(), []raftadapter.SnapshotStreamDTO{{To: 4, Index: 1, Term: 1, Data: []byte{1}}})
	streams.signal()
	streams.wait()
	if streams.wakeChannel() != nil {
		t.Fatal("a missing coordinator must expose a nil wake channel")
	}
}

func TestClusterRuntimeSnapshotStreamActiveFollowsCommittedFeature(t *testing.T) {
	var missing *clusterRuntime
	if missing.snapshotStreamActive() {
		t.Fatal("a nil runtime reported the snapshot stream active")
	}
	if (&clusterRuntime{}).snapshotStreamActive() {
		t.Fatal("a runtime without a state machine reported the snapshot stream active")
	}
	machine := membership.NewStateMachine()
	members := make([]membership.MemberIdentityDTO, 3)
	for index := range members {
		id := uint64(index + 1)
		members[index] = membership.MemberIdentityDTO{
			MemberID: id, ClusterID: "cluster-a", Address: fmt.Sprintf("127.0.0.1:%d", 9000+id),
			DNSSAN: fmt.Sprintf("node%d.example", id), CertificateSHA256: bytes.Repeat([]byte{byte(id)}, 32),
			ClusterProtocolMajor: 2, StateSchemaMajor: 1,
			Phase: membership.MemberPhaseVoter, ReleaseVersion: "1.0.0", Features: []string{membership.SnapshotStreamFeature},
		}
	}
	if err := machine.InitializeBootstrap(1, members); err != nil {
		t.Fatal(err)
	}
	runtime := &clusterRuntime{machine: machine}
	if runtime.snapshotStreamActive() {
		t.Fatal("the stream was active before FeatureActivation committed")
	}
	result, err := machine.Apply(2, membership.CommandDTO{
		SchemaMajor: membership.SchemaMajor, SchemaMinor: membership.SchemaMinor,
		OperationID:       "00000000-0000-4000-8000-000000000002",
		FeatureActivation: &membership.FeatureActivationDTO{Feature: membership.SnapshotStreamFeature},
	})
	if err != nil || result.Outcome != membership.ApplyOutcomeApplied {
		t.Fatalf("activate = %+v, %v", result, err)
	}
	if !runtime.snapshotStreamActive() {
		t.Fatal("the stream was not active after FeatureActivation committed")
	}
}
