package main

import (
	"context"
	"errors"
	"testing"
	"time"

	adaptersmetrics "narumq/internal/adapters/metrics"
	raftadapter "narumq/internal/adapters/raft"
	appmgmt "narumq/internal/application/management"
	"narumq/internal/application/membership"
	managementports "narumq/internal/application/ports/management"
	observationports "narumq/internal/application/ports/observability"
)

type observabilityTestClock struct{ now time.Time }

func (clock observabilityTestClock) Now() time.Time { return clock.now }

type observabilityTestConnections int64

func (connections observabilityTestConnections) ActiveConnections() int64 {
	return int64(connections)
}

// observabilityFlipContext reports no error for the first remaining Err calls and
// cancellation afterwards, so a request that is cancelled while the snapshot is being
// assembled is distinguishable from one cancelled before it started.
type observabilityFlipContext struct {
	context.Context
	remaining int
}

func (ctx *observabilityFlipContext) Err() error {
	if ctx.remaining > 0 {
		ctx.remaining--
		return nil
	}
	return context.Canceled
}

var observabilityTestNow = time.Date(2026, 10, 17, 9, 30, 0, 0, time.FixedZone("KST", 9*60*60))

func newObservabilityTestState(nodeID string, connections managementports.ConnectionCounter) *appmgmt.Service {
	return appmgmt.NewService(appmgmt.ServiceConfig{
		ClusterID: "obs-cluster", NodeID: nodeID, ReleaseVersion: "1.2.3",
	}, nil, nil, nil, connections)
}

func newObservabilityTestSource(state *appmgmt.Service, nodeID string) *observabilitySource {
	return &observabilitySource{
		state: state, metrics: adaptersmetrics.NewRegistry(),
		clock: observabilityTestClock{now: observabilityTestNow}, clusterID: "obs-cluster",
		localNodeID: nodeID, releaseVersion: "1.2.3",
	}
}

// newObservabilityRaftNodes opens three dynamic voters and elects node 1, so the
// fixtures expose a real leader, a real follower and a node that has no leader.
func newObservabilityRaftNodes(t *testing.T) [3]*raftadapter.Node {
	t.Helper()
	var nodes [3]*raftadapter.Node
	for index := range nodes {
		id := raftadapter.NodeID(index + 1)
		node, err := raftadapter.Open(raftadapter.Config{
			NodeID: id, DynamicMembership: true, ClusterID: "obs-cluster",
			CertificateSHA256: [32]byte{byte(id)}, BootstrapVoterIDs: []raftadapter.NodeID{1, 2, 3},
			Directory: t.TempDir(),
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { node.Close() })
		nodes[index] = node
	}
	settle := func() {
		for range 64 {
			var messages []raftadapter.MessageDTO
			for _, node := range nodes {
				batch, err := processReadyForTest(t, node)
				if err != nil {
					t.Fatal(err)
				}
				messages = append(messages, batch.Messages...)
			}
			if len(messages) == 0 {
				return
			}
			for _, message := range messages {
				if _, err := nodes[message.To-1].Step(message); err != nil {
					t.Fatal(err)
				}
			}
		}
		t.Fatal("Raft election did not settle")
	}
	settle()
	if err := nodes[0].Campaign(); err != nil {
		t.Fatal(err)
	}
	settle()
	return nodes
}

func newObservabilityClusterSource(t *testing.T, node *raftadapter.Node, nodeID uint64) *observabilitySource {
	t.Helper()
	config := validBootstrapClusterConfig()
	config.NodeID = nodeID
	config.ClusterID = "obs-cluster"
	config.ReleaseVersion = "1.2.3"
	idText := map[uint64]string{1: "1", 2: "2", 3: "3", 4: "4"}[nodeID]
	source := newObservabilityTestSource(newObservabilityTestState(idText, nil), idText)
	source.runtime = &clusterRuntime{
		config: config, node: node, machine: retainedTestMachine(t, config, config.ReleaseVersion),
		storage: newClusterStorageMonitor(func() time.Time { return time.Unix(1, 0) }, nil),
	}
	return source
}

func TestObservabilitySourceRejectsNilAndCancelledContexts(t *testing.T) {
	source := newObservabilityTestSource(newObservabilityTestState("1", nil), "1")
	var nilContext context.Context
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	calls := map[string]func(context.Context) error{
		"ClusterSnapshot": func(ctx context.Context) error {
			snapshot, err := source.ClusterSnapshot(ctx)
			if err != nil && (snapshot.ClusterID != "" || snapshot.Members != nil) {
				t.Fatalf("ClusterSnapshot returned data alongside error: %+v", snapshot)
			}
			return err
		},
		"FreshQuorumAvailable": func(ctx context.Context) error {
			available, err := source.FreshQuorumAvailable(ctx)
			if available {
				t.Fatal("FreshQuorumAvailable reported quorum for an unusable context")
			}
			return err
		},
		"CaptureNodeSnapshot": func(ctx context.Context) error {
			snapshot, err := source.CaptureNodeSnapshot(ctx)
			if err != nil && snapshot.Ready != nil {
				t.Fatalf("CaptureNodeSnapshot returned data alongside error: %+v", snapshot)
			}
			return err
		},
		"CaptureMetricsSnapshot": func(ctx context.Context) error {
			snapshot, err := source.CaptureMetricsSnapshot(ctx)
			if err != nil && !snapshot.ObservedAt.IsZero() {
				t.Fatalf("CaptureMetricsSnapshot returned data alongside error: %+v", snapshot)
			}
			return err
		},
	}
	for name, call := range calls {
		t.Run(name, func(t *testing.T) {
			if err := call(nilContext); err == nil {
				t.Fatal("nil context was accepted")
			}
			if err := call(cancelled); !errors.Is(err, context.Canceled) {
				t.Fatalf("cancelled context error = %v, want context.Canceled", err)
			}
		})
	}
}

func TestObservabilityStandaloneClusterSnapshotReportsSingleStandaloneMember(t *testing.T) {
	state := newObservabilityTestState("1", nil)
	source := newObservabilityTestSource(state, "1")
	snapshot, err := source.ClusterSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.ClusterID != "obs-cluster" || snapshot.ReleaseVersion != "1.2.3" || snapshot.LocalNodeID != "1" {
		t.Fatalf("identity = %+v", snapshot)
	}
	if len(snapshot.Members) != 1 || snapshot.Members[0].NodeID != "1" ||
		snapshot.Members[0].Phase != observationports.MembershipPhaseStandalone {
		t.Fatalf("members = %+v, want only the local standalone member", snapshot.Members)
	}
	if snapshot.QuorumSize != 1 || snapshot.ReplicationFactor != 1 || snapshot.WriteQuorum != 1 || snapshot.UnderReplicated != 0 {
		t.Fatalf("standalone replication view = %+v", snapshot)
	}
	if snapshot.LeaderNodeID == nil || *snapshot.LeaderNodeID != "1" {
		t.Fatalf("leader = %v, want the local node", snapshot.LeaderNodeID)
	}
}

func TestObservabilityClusterSnapshotRequiresCommittedMembershipAndRaftNode(t *testing.T) {
	config := validBootstrapClusterConfig()
	for name, runtime := range map[string]*clusterRuntime{
		"without machine": {config: config, node: new(raftadapter.Node)},
		"without node":    {config: config, machine: membership.NewStateMachine()},
	} {
		t.Run(name, func(t *testing.T) {
			source := newObservabilityTestSource(newObservabilityTestState("1", nil), "1")
			source.runtime = runtime
			snapshot, err := source.ClusterSnapshot(context.Background())
			if err == nil {
				t.Fatalf("ClusterSnapshot() = %+v, want unavailable membership error", snapshot)
			}
			if len(snapshot.Members) != 0 || snapshot.LeaderNodeID != nil {
				t.Fatalf("ClusterSnapshot() leaked partial data with error: %+v", snapshot)
			}
		})
	}
}

func TestObservabilityClusterSnapshotReportsCommittedMembersAndLeader(t *testing.T) {
	nodes := newObservabilityRaftNodes(t)

	leaderView, err := newObservabilityClusterSource(t, nodes[0], 1).ClusterSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if leaderView.ClusterID != "obs-cluster" || leaderView.ReleaseVersion != "1.2.3" || leaderView.LocalNodeID != "1" {
		t.Fatalf("identity = %+v", leaderView)
	}
	if len(leaderView.Members) != 3 {
		t.Fatalf("members = %+v, want three committed voters", leaderView.Members)
	}
	for index, member := range leaderView.Members {
		wantID := string(rune('1' + index))
		if member.NodeID != wantID || member.Phase != observationports.MembershipPhaseVoter {
			t.Fatalf("member %d = %+v, want voter %s", index, member, wantID)
		}
	}
	if leaderView.QuorumSize != 2 || leaderView.ReplicationFactor != membership.StandardReplicationProfile.Factor ||
		leaderView.WriteQuorum != membership.StandardReplicationProfile.WriteQuorum || leaderView.UnderReplicated != 0 {
		t.Fatalf("replication view = %+v", leaderView)
	}
	if leaderView.LeaderNodeID == nil || *leaderView.LeaderNodeID != "1" {
		t.Fatalf("leader = %v, want node 1", leaderView.LeaderNodeID)
	}

	followerView, err := newObservabilityClusterSource(t, nodes[1], 2).ClusterSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if followerView.LeaderNodeID == nil || *followerView.LeaderNodeID != "1" || followerView.LocalNodeID != "2" {
		t.Fatalf("follower view = %+v, want leader 1 seen from node 2", followerView)
	}
}

func TestObservabilityClusterSnapshotWithoutLeaderOrVotersKeepsQuorumAtLeastOne(t *testing.T) {
	node, err := raftadapter.Open(raftadapter.Config{
		NodeID: 4, DynamicMembership: true, ClusterID: "obs-cluster",
		CertificateSHA256: [32]byte{4}, Directory: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { node.Close() })
	snapshot, err := newObservabilityClusterSource(t, node, 4).ClusterSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.LeaderNodeID != nil {
		t.Fatalf("leader = %q, want none for a node that has seen no leader", *snapshot.LeaderNodeID)
	}
	if snapshot.QuorumSize != 1 {
		t.Fatalf("quorum size = %d, want the lower bound 1 when no voters are known", snapshot.QuorumSize)
	}
}

func TestObservabilityMembershipPhaseMapsEveryCommittedPhase(t *testing.T) {
	tests := []struct {
		phase membership.MemberPhase
		want  observationports.MembershipPhase
	}{
		{membership.MemberPhaseStaged, observationports.MembershipPhaseStaged},
		{membership.MemberPhaseLearner, observationports.MembershipPhaseLearner},
		{membership.MemberPhaseCatchingUp, observationports.MembershipPhaseCatchingUp},
		{membership.MemberPhaseVoter, observationports.MembershipPhaseVoter},
		{membership.MemberPhaseLeaving, observationports.MembershipPhaseLeaving},
		{membership.MemberPhaseRetired, observationports.MembershipPhaseRetired},
	}
	for _, test := range tests {
		got, ok := observabilityMembershipPhase(test.phase)
		if !ok || got != test.want {
			t.Fatalf("observabilityMembershipPhase(%d) = %q, %t; want %q, true", test.phase, got, ok, test.want)
		}
	}
	for _, phase := range []membership.MemberPhase{membership.MemberPhaseUnspecified, membership.MemberPhaseRetired + 1} {
		if got, ok := observabilityMembershipPhase(phase); ok || got != "" {
			t.Fatalf("observabilityMembershipPhase(%d) = %q, %t; want rejection", phase, got, ok)
		}
	}
}

func TestObservabilityNonnegativeUint64ClampsNegativeCounts(t *testing.T) {
	for _, test := range []struct {
		value int
		want  uint64
	}{{-5, 0}, {0, 0}, {7, 7}} {
		if got := nonnegativeUint64(test.value); got != test.want {
			t.Fatalf("nonnegativeUint64(%d) = %d, want %d", test.value, got, test.want)
		}
	}
}

func TestObservabilityStandaloneNodeSnapshotReflectsManagementState(t *testing.T) {
	state := newObservabilityTestState("1", observabilityTestConnections(7))
	source := newObservabilityTestSource(state, "1")

	joining, err := source.CaptureNodeSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if joining.Ready == nil || *joining.Ready || joining.State == nil || *joining.State != observationports.NodeStateJoining {
		t.Fatalf("joining snapshot ready/state = %v/%v", joining.Ready, joining.State)
	}

	reason := "warming up"
	state.SetNodeState(managementports.NodeStateJoining, &reason)
	joining, err = source.CaptureNodeSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if joining.ReadinessReason == nil || *joining.ReadinessReason != reason {
		t.Fatalf("readiness reason = %v, want %q", joining.ReadinessReason, reason)
	}

	state.SetNodeState(managementports.NodeStateReady, nil)
	ready, err := source.CaptureNodeSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if ready.Ready == nil || !*ready.Ready || ready.Role == nil || *ready.Role != "LEADER" {
		t.Fatalf("ready snapshot = ready %v role %v", ready.Ready, ready.Role)
	}
	if ready.ConnectionCount == nil || *ready.ConnectionCount != 7 {
		t.Fatalf("connection count = %v, want 7", ready.ConnectionCount)
	}
	if ready.Version == nil || *ready.Version != "1.2.3" {
		t.Fatalf("version = %v", ready.Version)
	}
	if ready.StoragePressure == nil || *ready.StoragePressure || ready.ReplicationHealth != nil || ready.LastErrorCode != nil {
		t.Fatalf("standalone snapshot must not invent cluster replication data: %+v", ready)
	}
	if !ready.ObservedAt.Equal(observabilityTestNow) || ready.ObservedAt.Location() != time.UTC {
		t.Fatalf("observedAt = %v, want %v in UTC", ready.ObservedAt, observabilityTestNow)
	}
}

func TestObservabilityNodeSnapshotRejectsUnknownLocalNode(t *testing.T) {
	source := newObservabilityTestSource(newObservabilityTestState("1", nil), "other-node")
	snapshot, err := source.CaptureNodeSnapshot(context.Background())
	if !errors.Is(err, appmgmt.ErrNodeNotFound) {
		t.Fatalf("CaptureNodeSnapshot() error = %v, want %v", err, appmgmt.ErrNodeNotFound)
	}
	if snapshot.Ready != nil || snapshot.Version != nil {
		t.Fatalf("snapshot returned data with error: %+v", snapshot)
	}
}

func TestObservabilityNodeSnapshotDiscardsDataWhenCancelledDuringCapture(t *testing.T) {
	source := newObservabilityTestSource(newObservabilityTestState("1", nil), "1")
	snapshot, err := source.CaptureNodeSnapshot(&observabilityFlipContext{Context: context.Background(), remaining: 1})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("CaptureNodeSnapshot() error = %v, want context.Canceled", err)
	}
	if snapshot.Ready != nil || snapshot.Version != nil {
		t.Fatalf("snapshot returned data with error: %+v", snapshot)
	}
}

func TestObservabilityClusterNodeSnapshotDerivesRoleFromRaftStatus(t *testing.T) {
	nodes := newObservabilityRaftNodes(t)
	learner, err := raftadapter.Open(raftadapter.Config{
		NodeID: 4, DynamicMembership: true, ClusterID: "obs-cluster",
		CertificateSHA256: [32]byte{4}, Directory: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { learner.Close() })

	tests := []struct {
		name   string
		node   *raftadapter.Node
		nodeID uint64
		want   string
	}{
		{"elected voter is leader", nodes[0], 1, "LEADER"},
		{"voter following another node", nodes[1], 2, "FOLLOWER"},
		{"node outside the voter set", learner, 4, "LEARNER"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			source := newObservabilityClusterSource(t, test.node, test.nodeID)
			snapshot, err := source.CaptureNodeSnapshot(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if snapshot.Role == nil || *snapshot.Role != test.want {
				t.Fatalf("role = %v, want %s", snapshot.Role, test.want)
			}
			status := test.node.Status()
			if snapshot.LastAppliedIndex == nil || *snapshot.LastAppliedIndex != status.AppliedIndex {
				t.Fatalf("lastAppliedIndex = %v, want the Raft applied index %d", snapshot.LastAppliedIndex, status.AppliedIndex)
			}
			if snapshot.ReplicationHealth == nil || *snapshot.ReplicationHealth != clusterReplicationHealthy {
				t.Fatalf("replication health = %v, want %s", snapshot.ReplicationHealth, clusterReplicationHealthy)
			}
			if snapshot.LastErrorCode != nil {
				t.Fatalf("healthy storage reported error code %q", *snapshot.LastErrorCode)
			}
		})
	}
}

func TestObservabilityClusterNodeSnapshotHealthyStorageKeepsReadinessAndPressure(t *testing.T) {
	nodes := newObservabilityRaftNodes(t)
	source := newObservabilityClusterSource(t, nodes[0], 1)
	source.state.SetNodeState(managementports.NodeStateReady, nil)

	snapshot, err := source.CaptureNodeSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Ready == nil || !*snapshot.Ready || snapshot.ReadinessReason != nil {
		t.Fatalf("healthy ready node = ready %v reason %v", snapshot.Ready, snapshot.ReadinessReason)
	}
	if snapshot.StoragePressure == nil || *snapshot.StoragePressure {
		t.Fatalf("storage pressure = %v, want false", snapshot.StoragePressure)
	}

	pressure := &clusterPublishPressureController{minFreeBytes: 1}
	source.runtime.publishPressure = pressure
	snapshot, err = source.CaptureNodeSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.StoragePressure == nil || *snapshot.StoragePressure {
		t.Fatalf("inactive disk pressure reported storage pressure: %v", snapshot.StoragePressure)
	}

	pressure.diskPressure.Store(true)
	snapshot, err = source.CaptureNodeSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.StoragePressure == nil || !*snapshot.StoragePressure {
		t.Fatal("active publish disk pressure was not reported as storage pressure")
	}
	if snapshot.Ready == nil || !*snapshot.Ready {
		t.Fatal("disk pressure alone must not mark a healthy node not ready")
	}
}

func TestObservabilityClusterNodeSnapshotFailedStorageIsNotReady(t *testing.T) {
	nodes := newObservabilityRaftNodes(t)
	source := newObservabilityClusterSource(t, nodes[0], 1)
	source.state.SetNodeState(managementports.NodeStateReady, nil)
	source.runtime.storage.fail(raftadapter.ErrFailed)
	want := source.runtime.storage.view()

	snapshot, err := source.CaptureNodeSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Ready == nil || *snapshot.Ready {
		t.Fatalf("ready = %v, want false after a terminal storage failure", snapshot.Ready)
	}
	if snapshot.ReadinessReason == nil || *snapshot.ReadinessReason != want.reason || want.reason == "" {
		t.Fatalf("readiness reason = %v, want the storage failure reason %q", snapshot.ReadinessReason, want.reason)
	}
	if snapshot.ReplicationHealth == nil || *snapshot.ReplicationHealth != clusterReplicationFailed {
		t.Fatalf("replication health = %v, want %s", snapshot.ReplicationHealth, clusterReplicationFailed)
	}
	if snapshot.LastErrorCode == nil || *snapshot.LastErrorCode != clusterErrorRaftNodeFailed {
		t.Fatalf("last error code = %v, want %s", snapshot.LastErrorCode, clusterErrorRaftNodeFailed)
	}
}

func TestObservabilityClusterNodeSnapshotStorageIOFailureReportsPressure(t *testing.T) {
	nodes := newObservabilityRaftNodes(t)
	source := newObservabilityClusterSource(t, nodes[0], 1)
	source.runtime.storage.fail(raftadapter.ErrStorage)

	snapshot, err := source.CaptureNodeSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.StoragePressure == nil || !*snapshot.StoragePressure {
		t.Fatal("storage I/O failure did not report storage pressure")
	}
	if snapshot.LastErrorCode == nil || *snapshot.LastErrorCode != clusterErrorStorageIO {
		t.Fatalf("last error code = %v, want %s", snapshot.LastErrorCode, clusterErrorStorageIO)
	}
}

func TestObservabilityMetricsSnapshotRequiresRegistry(t *testing.T) {
	source := newObservabilityTestSource(newObservabilityTestState("1", nil), "1")
	source.metrics = nil
	snapshot, err := source.CaptureMetricsSnapshot(context.Background())
	if err == nil {
		t.Fatalf("CaptureMetricsSnapshot() without a registry = %+v, want error", snapshot)
	}
	if !snapshot.ObservedAt.IsZero() {
		t.Fatalf("snapshot returned data with error: %+v", snapshot)
	}
}

func TestObservabilityMetricsSnapshotCarriesResetBoundaryInUTC(t *testing.T) {
	source := newObservabilityTestSource(newObservabilityTestState("1", nil), "1")
	snapshot, err := source.CaptureMetricsSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !snapshot.ObservedAt.Equal(observabilityTestNow) || snapshot.ObservedAt.Location() != time.UTC {
		t.Fatalf("observedAt = %v, want %v in UTC", snapshot.ObservedAt, observabilityTestNow)
	}
	if !snapshot.StartedAt.Equal(source.metrics.StartedAt()) {
		t.Fatalf("startedAt = %v, want registry start %v", snapshot.StartedAt, source.metrics.StartedAt())
	}
}

func TestObservabilityMetricsSnapshotDiscardsDataWhenCancelledDuringCapture(t *testing.T) {
	source := newObservabilityTestSource(newObservabilityTestState("1", nil), "1")
	snapshot, err := source.CaptureMetricsSnapshot(&observabilityFlipContext{Context: context.Background(), remaining: 1})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("CaptureMetricsSnapshot() error = %v, want context.Canceled", err)
	}
	if !snapshot.ObservedAt.IsZero() {
		t.Fatalf("snapshot returned data with error: %+v", snapshot)
	}
}

func TestObservabilityStandaloneFreshQuorumFollowsNodeState(t *testing.T) {
	state := newObservabilityTestState("1", nil)
	source := newObservabilityTestSource(state, "1")
	if available, err := source.FreshQuorumAvailable(context.Background()); err != nil || available {
		t.Fatalf("joining standalone FreshQuorumAvailable() = %t, %v; want false, nil", available, err)
	}
	state.SetNodeState(managementports.NodeStateReady, nil)
	if available, err := source.FreshQuorumAvailable(context.Background()); err != nil || !available {
		t.Fatalf("ready standalone FreshQuorumAvailable() = %t, %v; want true, nil", available, err)
	}
}

func newObservabilityQuorumRuntime(t *testing.T) *clusterRuntime {
	t.Helper()
	config := validBootstrapClusterConfig()
	machine := retainedTestMachine(t, config, "1.2.3")
	if _, err := machine.Apply(2, membership.CommandDTO{
		SchemaMajor: membership.SchemaMajor, SchemaMinor: membership.SchemaMinor,
		OperationID: "00000000-0000-4000-8000-000000000001",
		TenantQuota: &membership.TenantQuotaChangeDTO{Profile: membership.DefaultTenantQuotaProfile()},
	}); err != nil {
		t.Fatalf("install default tenant quota profile: %v", err)
	}
	runtime := &clusterRuntime{
		config: config, machine: machine,
		storage:        newClusterStorageMonitor(func() time.Time { return time.Unix(1, 0) }, nil),
		releaseProfile: &clusterReleaseProfileController{},
		requests:       make(chan clusterDriverRequest, 1),
		driverStopped:  make(chan struct{}),
	}
	runtime.releaseProfile.synced.Store(true)
	runtime.initialized.Store(true)
	runtime.admissionBootstrapped.Store(true)
	runtime.admissionDynamicMembership.Store(true)
	runtime.admissionLocalVoter.Store(true)
	runtime.admissionLocalPhaseVoter.Store(true)
	runtime.admissionAppliedIndex.Store(5)
	runtime.projectionIndex.Store(5)
	return runtime
}

func TestObservabilityClusterFreshQuorumRequiresFreshProofCoveredByLocalState(t *testing.T) {
	tests := []struct {
		name     string
		response clusterReadResponse
		want     bool
	}{
		{"proof within applied projection", clusterReadResponse{index: 5}, true},
		{"proof beyond applied projection", clusterReadResponse{index: 6}, false},
		{"leadership lost during proof", clusterReadResponse{err: raftadapter.ErrNotLeader}, false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runtime := newObservabilityQuorumRuntime(t)
			source := newObservabilityTestSource(newObservabilityTestState("1", nil), "1")
			source.runtime = runtime
			go func() {
				request := <-runtime.requests
				request.readResult <- test.response
			}()
			available, err := source.FreshQuorumAvailable(context.Background())
			if err != nil {
				t.Fatalf("FreshQuorumAvailable() error = %v, want nil", err)
			}
			if available != test.want {
				t.Fatalf("FreshQuorumAvailable() = %t, want %t", available, test.want)
			}
		})
	}
}

func TestObservabilityClusterFreshQuorumFailedStorageSkipsReadBarrier(t *testing.T) {
	runtime := newObservabilityQuorumRuntime(t)
	runtime.storage.fail(raftadapter.ErrFailed)
	source := newObservabilityTestSource(newObservabilityTestState("1", nil), "1")
	source.runtime = runtime
	available, err := source.FreshQuorumAvailable(context.Background())
	if err != nil || available {
		t.Fatalf("FreshQuorumAvailable() with failed storage = %t, %v; want false, nil", available, err)
	}
	if len(runtime.requests) != 0 {
		t.Fatalf("failed storage queued %d read barriers, want none", len(runtime.requests))
	}
}

func TestObservabilityClusterFreshQuorumReturnsCancellationInsteadOfUnavailable(t *testing.T) {
	runtime := newObservabilityQuorumRuntime(t)
	source := newObservabilityTestSource(newObservabilityTestState("1", nil), "1")
	source.runtime = runtime
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		<-runtime.requests
		cancel()
	}()
	available, err := source.FreshQuorumAvailable(ctx)
	if !errors.Is(err, context.Canceled) || available {
		t.Fatalf("FreshQuorumAvailable() = %t, %v; want false, context.Canceled", available, err)
	}
}
