package observability

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	clockports "narumq/internal/application/ports/clock"
	metricsports "narumq/internal/application/ports/metrics"
	observationports "narumq/internal/application/ports/observability"
)

type observabilityTestClock struct{ now time.Time }

func (clock observabilityTestClock) Now() time.Time { return clock.now }

type observabilityTestSource struct {
	cluster observationports.ClusterSnapshotDTO
	node    observationports.NodeSnapshotDTO
	metrics observationports.MetricsSnapshotDTO
	quorum  bool

	clusterCalls atomic.Int32
	quorumCalls  atomic.Int32
	nodeCalls    atomic.Int32
	metricCalls  atomic.Int32
}

func (source *observabilityTestSource) ClusterSnapshot(ctx context.Context) (observationports.ClusterSnapshotDTO, error) {
	source.clusterCalls.Add(1)
	if err := ctx.Err(); err != nil {
		return observationports.ClusterSnapshotDTO{}, err
	}
	return source.cluster, nil
}

func (source *observabilityTestSource) FreshQuorumAvailable(ctx context.Context) (bool, error) {
	source.quorumCalls.Add(1)
	if err := ctx.Err(); err != nil {
		return false, err
	}
	return source.quorum, nil
}

func (source *observabilityTestSource) CaptureNodeSnapshot(ctx context.Context) (observationports.NodeSnapshotDTO, error) {
	source.nodeCalls.Add(1)
	if err := ctx.Err(); err != nil {
		return observationports.NodeSnapshotDTO{}, err
	}
	return source.node, nil
}

func (source *observabilityTestSource) CaptureMetricsSnapshot(ctx context.Context) (observationports.MetricsSnapshotDTO, error) {
	source.metricCalls.Add(1)
	if err := ctx.Err(); err != nil {
		return observationports.MetricsSnapshotDTO{}, err
	}
	return source.metrics, nil
}

type observabilityTestPeers struct {
	nodeErrors    map[string]error
	metricErrors  map[string]error
	metricResults map[string]observationports.MetricsSnapshotDTO
	nodeCalls     atomic.Int32
	metricCalls   atomic.Int32
}

func (peers *observabilityTestPeers) QueryNodeSnapshot(ctx context.Context, memberID string) (observationports.NodeSnapshotDTO, error) {
	peers.nodeCalls.Add(1)
	if err := ctx.Err(); err != nil {
		return observationports.NodeSnapshotDTO{}, err
	}
	if err := peers.nodeErrors[memberID]; err != nil {
		return observationports.NodeSnapshotDTO{}, err
	}
	return observationports.NodeSnapshotDTO{}, errors.New("unexpected node query")
}

func (peers *observabilityTestPeers) QueryMetricsSnapshot(ctx context.Context, memberID string) (observationports.MetricsSnapshotDTO, error) {
	peers.metricCalls.Add(1)
	if err := ctx.Err(); err != nil {
		return observationports.MetricsSnapshotDTO{}, err
	}
	if err := peers.metricErrors[memberID]; err != nil {
		return observationports.MetricsSnapshotDTO{}, err
	}
	if result, ok := peers.metricResults[memberID]; ok {
		return result, nil
	}
	return observationports.MetricsSnapshotDTO{}, errors.New("unexpected metrics query")
}

func newTestObservabilityService(t *testing.T, source *observabilityTestSource, peers observationports.PeerSnapshotReader) *Service {
	t.Helper()
	service, err := NewService(source, source, peers, observabilityTestClock{now: time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)})
	if err != nil {
		t.Fatal(err)
	}
	return service
}

func TestNodeMetricsModeUsesOnlyLocalSnapshot(t *testing.T) {
	descriptor := descriptorForTest("narumq_connections_active")
	source := &observabilityTestSource{
		cluster: observationports.ClusterSnapshotDTO{
			ClusterID: "cluster", ReleaseVersion: "1.0.0", LocalNodeID: "1",
			Members:    []observationports.MemberDTO{{NodeID: "1", Phase: observationports.MembershipPhaseVoter}, {NodeID: "2", Phase: observationports.MembershipPhaseVoter}},
			QuorumSize: 2, ReplicationFactor: 3, WriteQuorum: 2,
		},
		metrics: observationports.MetricsSnapshotDTO{
			ObservedAt: time.Date(2026, time.January, 2, 3, 4, 0, 0, time.UTC),
			StartedAt:  time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC),
			Snapshot: metricsports.MetricsSnapshotDTO{Families: []metricsports.MetricFamilyDTO{{
				Descriptor: descriptor,
				Samples: []metricsports.MetricSampleDTO{{
					Name:  descriptor.Name,
					Value: metricsports.MetricValueDTO{Kind: metricsports.MetricValueInteger, Uint64: 3},
				}},
			}}},
		},
	}
	peers := &observabilityTestPeers{}
	service := newTestObservabilityService(t, source, peers)

	result, err := service.CollectMetrics(context.Background(), observationports.MetricsModeNode)
	if err != nil {
		t.Fatalf("CollectMetrics(node): %v", err)
	}
	if result.Mode != observationports.MetricsModeNode || !result.Complete || len(result.Nodes) != 1 || result.Nodes[0].NodeID != "1" {
		t.Fatalf("node collection metadata = %+v", result)
	}
	if source.metricCalls.Load() != 1 || source.quorumCalls.Load() != 0 || peers.metricCalls.Load() != 0 {
		t.Fatalf("node mode calls: local metrics=%d quorum=%d peer metrics=%d; want 1, 0, 0",
			source.metricCalls.Load(), source.quorumCalls.Load(), peers.metricCalls.Load())
	}
	if len(result.Families) != 1 || len(result.Families[0].Samples) != 1 || result.Families[0].Samples[0].Value != "3" {
		t.Fatalf("node metric families = %+v", result.Families)
	}
}

func TestNodeListPreservesMeasuredNotReadyStatus(t *testing.T) {
	state := observationports.NodeStateDraining
	ready := false
	reason := "drain_in_progress"
	source := &observabilityTestSource{
		cluster: observationports.ClusterSnapshotDTO{
			ClusterID: "cluster", ReleaseVersion: "1.0.0", LocalNodeID: "1",
			Members: []observationports.MemberDTO{
				{NodeID: "1", Phase: observationports.MembershipPhaseVoter},
			},
			QuorumSize: 1, ReplicationFactor: 1, WriteQuorum: 1,
		},
		node: observationports.NodeSnapshotDTO{
			State: &state, Ready: &ready, ReadinessReason: &reason,
		},
		quorum: true,
	}
	service := newTestObservabilityService(t, source, &observabilityTestPeers{})

	nodes, _, err := service.ListNodes(context.Background(), observationports.NodeFilter{Limit: 10})
	if err != nil {
		t.Fatalf("ListNodes(): %v", err)
	}
	if len(nodes) != 1 || nodes[0].QueryStatus != observationports.QueryStatusOK ||
		nodes[0].State == nil || *nodes[0].State != observationports.NodeStateDraining ||
		nodes[0].Ready == nil || *nodes[0].Ready ||
		nodes[0].ReadinessReason == nil || *nodes[0].ReadinessReason != reason {
		t.Fatalf("measured not-ready node snapshot=%+v", nodes)
	}
}

func TestNodeListPreservesUnknownAndRetiredStates(t *testing.T) {
	readyState := observationports.NodeStateReady
	ready := true
	five, six := uint64(5), uint64(6)
	source := &observabilityTestSource{
		cluster: observationports.ClusterSnapshotDTO{
			ClusterID: "cluster", ReleaseVersion: "1.0.0", LocalNodeID: "1",
			Members: []observationports.MemberDTO{
				{NodeID: "1", Phase: observationports.MembershipPhaseVoter},
				{NodeID: "2", Phase: observationports.MembershipPhaseVoter},
				{NodeID: "3", Phase: observationports.MembershipPhaseRetired},
			},
			QuorumSize: 2, ReplicationFactor: 3, WriteQuorum: 2,
		},
		node: observationports.NodeSnapshotDTO{
			ObservedAt: time.Date(2026, time.January, 2, 3, 4, 0, 0, time.UTC),
			State:      &readyState, Ready: &ready, ConnectionCount: &five, SessionCount: &six,
		},
		quorum: true,
	}
	peers := &observabilityTestPeers{
		nodeErrors: map[string]error{"2": &observationports.PeerQueryError{Code: observationports.QueryErrorTimeout}},
	}
	service := newTestObservabilityService(t, source, peers)

	nodes, _, err := service.ListNodes(context.Background(), observationports.NodeFilter{Limit: 10})
	if err != nil {
		t.Fatalf("ListNodes(): %v", err)
	}
	if len(nodes) != 3 || nodes[0].NodeID != "1" || nodes[1].NodeID != "2" || nodes[2].NodeID != "3" {
		t.Fatalf("node order = %+v", nodes)
	}
	if nodes[1].QueryStatus != observationports.QueryStatusUnreachable || nodes[1].State != nil ||
		nodes[1].QueryErrorCode == nil || *nodes[1].QueryErrorCode != observationports.QueryErrorTimeout {
		t.Fatalf("unavailable remote node = %+v", nodes[1])
	}
	if nodes[2].QueryStatus != observationports.QueryStatusRetired || nodes[2].QueryErrorCode != nil || nodes[2].State != nil {
		t.Fatalf("retired node = %+v", nodes[2])
	}
	filtered, _, err := service.ListNodes(context.Background(), observationports.NodeFilter{State: observationports.NodeStateReady, Limit: 10})
	if err != nil {
		t.Fatalf("ListNodes(state=READY): %v", err)
	}
	if len(filtered) != 1 || filtered[0].NodeID != "1" {
		t.Fatalf("READY nodes = %+v, want only the successfully queried ready node", filtered)
	}
	cluster, err := service.GetCluster(context.Background())
	if err != nil {
		t.Fatalf("GetCluster(): %v", err)
	}
	if cluster.NodesByState.Ready != 1 || cluster.NodesByState.Unknown != 1 || cluster.NodeQueryComplete ||
		cluster.ConnectionCount != nil || cluster.SessionCount != nil {
		t.Fatalf("cluster unknown-state aggregation = %+v", cluster)
	}
}

func TestNodeMetricsModeRejectsInvalidSnapshotDescriptor(t *testing.T) {
	descriptor := descriptorForTest("narumq_connections_active")
	descriptor.Help = "not the catalog help"
	source := &observabilityTestSource{
		cluster: observationports.ClusterSnapshotDTO{
			ClusterID: "cluster", ReleaseVersion: "1.0.0", LocalNodeID: "1",
			Members:    []observationports.MemberDTO{{NodeID: "1", Phase: observationports.MembershipPhaseStandalone}},
			QuorumSize: 1, ReplicationFactor: 1, WriteQuorum: 1,
		},
		metrics: observationports.MetricsSnapshotDTO{
			StartedAt: time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC),
			Snapshot:  metricsports.MetricsSnapshotDTO{Families: []metricsports.MetricFamilyDTO{{Descriptor: descriptor}}},
		},
	}
	service := newTestObservabilityService(t, source, nil)
	result, err := service.GetNodeMetrics(context.Background(), "1")
	if err != nil {
		t.Fatalf("GetNodeMetrics(): %v", err)
	}
	if result.QueryStatus != observationports.QueryStatusError || result.QueryErrorCode == nil ||
		*result.QueryErrorCode != observationports.QueryErrorFailed || result.Families != nil {
		t.Fatalf("invalid local descriptor result = %+v", result)
	}
}

func descriptorForTest(name string) metricsports.MetricDescriptorDTO {
	for _, descriptor := range metricsports.MetricCatalog() {
		if descriptor.Name == name {
			descriptor.LabelNames = append([]string(nil), descriptor.LabelNames...)
			return descriptor
		}
	}
	panic("metric descriptor is absent from catalog: " + name)
}

var _ clockports.Clock = observabilityTestClock{}
var _ observationports.ClusterSnapshotSource = (*observabilityTestSource)(nil)
var _ observationports.LocalSnapshotSource = (*observabilityTestSource)(nil)
var _ observationports.PeerSnapshotReader = (*observabilityTestPeers)(nil)
