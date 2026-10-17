package observability

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	metricsports "narumq/internal/application/ports/metrics"
	observationports "narumq/internal/application/ports/observability"
)

var (
	queryTestNow     = time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)
	queryTestStarted = time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
)

// queryFaultSource wraps the shared source fake with injectable failures and hooks.
type queryFaultSource struct {
	*observabilityTestSource
	clusterErr error
	quorumErr  error
	nodeErr    error
	metricsErr error
	onQuorum   func()
	onMetrics  func()
}

func (source *queryFaultSource) ClusterSnapshot(ctx context.Context) (observationports.ClusterSnapshotDTO, error) {
	if source.clusterErr != nil {
		source.clusterCalls.Add(1)
		return observationports.ClusterSnapshotDTO{}, source.clusterErr
	}
	return source.observabilityTestSource.ClusterSnapshot(ctx)
}

func (source *queryFaultSource) FreshQuorumAvailable(ctx context.Context) (bool, error) {
	if source.onQuorum != nil {
		source.onQuorum()
	}
	if source.quorumErr != nil {
		return false, source.quorumErr
	}
	return source.observabilityTestSource.FreshQuorumAvailable(ctx)
}

func (source *queryFaultSource) CaptureNodeSnapshot(ctx context.Context) (observationports.NodeSnapshotDTO, error) {
	if source.nodeErr != nil {
		return observationports.NodeSnapshotDTO{}, source.nodeErr
	}
	return source.observabilityTestSource.CaptureNodeSnapshot(ctx)
}

func (source *queryFaultSource) CaptureMetricsSnapshot(ctx context.Context) (observationports.MetricsSnapshotDTO, error) {
	if source.onMetrics != nil {
		source.onMetrics()
	}
	if err := ctx.Err(); err != nil {
		return observationports.MetricsSnapshotDTO{}, err
	}
	if source.metricsErr != nil {
		return observationports.MetricsSnapshotDTO{}, source.metricsErr
	}
	return source.observabilityTestSource.CaptureMetricsSnapshot(ctx)
}

// scriptedPeers answers peer queries from per-node scripts and counts calls.
type scriptedPeers struct {
	nodes       map[string]observationports.NodeSnapshotDTO
	metrics     map[string]observationports.MetricsSnapshotDTO
	nodeErrs    map[string]error
	metricErrs  map[string]error
	defaultNode *observationports.NodeSnapshotDTO
	hook        func()
	nodeCalls   atomic.Int32
	metricCalls atomic.Int32
}

func (peers *scriptedPeers) QueryNodeSnapshot(ctx context.Context, memberID string) (observationports.NodeSnapshotDTO, error) {
	peers.nodeCalls.Add(1)
	if peers.hook != nil {
		peers.hook()
	}
	if err := ctx.Err(); err != nil {
		return observationports.NodeSnapshotDTO{}, err
	}
	if err := peers.nodeErrs[memberID]; err != nil {
		return observationports.NodeSnapshotDTO{}, err
	}
	if snapshot, ok := peers.nodes[memberID]; ok {
		return snapshot, nil
	}
	if peers.defaultNode != nil {
		return *peers.defaultNode, nil
	}
	return observationports.NodeSnapshotDTO{}, errors.New("no scripted node snapshot for " + memberID)
}

func (peers *scriptedPeers) QueryMetricsSnapshot(ctx context.Context, memberID string) (observationports.MetricsSnapshotDTO, error) {
	peers.metricCalls.Add(1)
	if peers.hook != nil {
		peers.hook()
	}
	if err := ctx.Err(); err != nil {
		return observationports.MetricsSnapshotDTO{}, err
	}
	if err := peers.metricErrs[memberID]; err != nil {
		return observationports.MetricsSnapshotDTO{}, err
	}
	if snapshot, ok := peers.metrics[memberID]; ok {
		return snapshot, nil
	}
	return observationports.MetricsSnapshotDTO{}, errors.New("no scripted metrics snapshot for " + memberID)
}

func newQueryService(t *testing.T, source *queryFaultSource, peers observationports.PeerSnapshotReader) *Service {
	t.Helper()
	service, err := NewService(source, source, peers, observabilityTestClock{now: queryTestNow})
	if err != nil {
		t.Fatal(err)
	}
	return service
}

func newQuerySource(cluster observationports.ClusterSnapshotDTO) *queryFaultSource {
	return &queryFaultSource{observabilityTestSource: &observabilityTestSource{cluster: cluster, quorum: true}}
}

func queryVoter(id string) observationports.MemberDTO {
	return observationports.MemberDTO{NodeID: id, Phase: observationports.MembershipPhaseVoter}
}

func queryCluster(local string, members ...observationports.MemberDTO) observationports.ClusterSnapshotDTO {
	return observationports.ClusterSnapshotDTO{
		ClusterID: "cluster", ReleaseVersion: "1.0.0", LocalNodeID: local, Members: members,
		QuorumSize: 2, ReplicationFactor: 3, WriteQuorum: 2,
	}
}

func queryNodeSnapshot(state observationports.NodeState, connections, sessions uint64) observationports.NodeSnapshotDTO {
	return observationports.NodeSnapshotDTO{
		ObservedAt: queryTestNow.Add(-time.Minute), State: new(state), Ready: new(state == observationports.NodeStateReady),
		ConnectionCount: new(connections), SessionCount: new(sessions),
	}
}

type queryCall struct {
	name string
	call func(*Service, context.Context) error
}

func queryCalls(nodeID string) []queryCall {
	return []queryCall{
		{"GetCluster", func(service *Service, ctx context.Context) error { _, err := service.GetCluster(ctx); return err }},
		{"GetNode", func(service *Service, ctx context.Context) error { _, err := service.GetNode(ctx, nodeID); return err }},
		{"ListNodes", func(service *Service, ctx context.Context) error {
			_, _, err := service.ListNodes(ctx, observationports.NodeFilter{})
			return err
		}},
		{"GetNodeMetrics", func(service *Service, ctx context.Context) error {
			_, err := service.GetNodeMetrics(ctx, nodeID)
			return err
		}},
		{"CollectMetrics/node", func(service *Service, ctx context.Context) error {
			_, err := service.CollectMetrics(ctx, observationports.MetricsModeNode)
			return err
		}},
		{"CollectMetrics/unaggregated", func(service *Service, ctx context.Context) error {
			_, err := service.CollectMetrics(ctx, observationports.MetricsModeAllNodesUnaggregated)
			return err
		}},
		{"CollectMetrics/aggregated", func(service *Service, ctx context.Context) error {
			_, err := service.CollectMetrics(ctx, observationports.MetricsModeAllNodesAggregated)
			return err
		}},
	}
}

func TestNewServiceRequiresClusterLocalAndClockButAllowsMissingPeers(t *testing.T) {
	source := &observabilityTestSource{}
	clock := observabilityTestClock{now: queryTestNow}
	if service, err := NewService(nil, source, nil, clock); err == nil || service != nil {
		t.Fatalf("missing cluster source accepted: service=%v err=%v", service, err)
	}
	if service, err := NewService(source, nil, nil, clock); err == nil || service != nil {
		t.Fatalf("missing local source accepted: service=%v err=%v", service, err)
	}
	if service, err := NewService(source, source, nil, nil); err == nil || service != nil {
		t.Fatalf("missing clock accepted: service=%v err=%v", service, err)
	}
	if service, err := NewService(source, source, nil, clock); err != nil || service == nil {
		t.Fatalf("service without peers rejected: service=%v err=%v", service, err)
	}
}

func TestQueriesRejectNilContextBeforeReadingAnySource(t *testing.T) {
	source := newQuerySource(queryCluster("1", queryVoter("1")))
	service := newQueryService(t, source, nil)
	var nilContext context.Context
	for _, test := range queryCalls("1") {
		t.Run(test.name, func(t *testing.T) {
			if err := test.call(service, nilContext); !errors.Is(err, errInvalidContext) {
				t.Fatalf("%s(nil context) error = %v, want %v", test.name, err, errInvalidContext)
			}
		})
	}
	if source.clusterCalls.Load() != 0 {
		t.Fatalf("a nil-context query read the cluster snapshot %d times", source.clusterCalls.Load())
	}
}

// Scenarios: management-api-and-observability.md:MGT-SCN-022
func TestInvalidMembershipSnapshotFailsEveryQueryWithoutQueryingNodes(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*observationports.ClusterSnapshotDTO)
	}{
		{"empty cluster ID", func(c *observationports.ClusterSnapshotDTO) { c.ClusterID = "" }},
		{"empty release version", func(c *observationports.ClusterSnapshotDTO) { c.ReleaseVersion = "" }},
		{"empty local node", func(c *observationports.ClusterSnapshotDTO) { c.LocalNodeID = "" }},
		{"zero quorum size", func(c *observationports.ClusterSnapshotDTO) { c.QuorumSize = 0 }},
		{"zero replication factor", func(c *observationports.ClusterSnapshotDTO) { c.ReplicationFactor = 0 }},
		{"zero write quorum", func(c *observationports.ClusterSnapshotDTO) { c.WriteQuorum = 0 }},
		{"negative under-replicated count", func(c *observationports.ClusterSnapshotDTO) { c.UnderReplicated = -1 }},
		{"member without ID", func(c *observationports.ClusterSnapshotDTO) { c.Members[1].NodeID = "" }},
		{"member with unknown phase", func(c *observationports.ClusterSnapshotDTO) { c.Members[1].Phase = "BOGUS" }},
		{"duplicate member", func(c *observationports.ClusterSnapshotDTO) { c.Members[1].NodeID = "1" }},
		{"local node is not a member", func(c *observationports.ClusterSnapshotDTO) { c.LocalNodeID = "9" }},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			cluster := queryCluster("1", queryVoter("1"), queryVoter("2"))
			test.mutate(&cluster)
			source := newQuerySource(cluster)
			peers := &scriptedPeers{}
			service := newQueryService(t, source, peers)
			for _, query := range queryCalls("1") {
				if err := query.call(service, context.Background()); !errors.Is(err, errInvalidMembership) {
					t.Fatalf("%s error = %v, want %v", query.name, err, errInvalidMembership)
				}
			}
			if source.nodeCalls.Load() != 0 || source.metricCalls.Load() != 0 || source.quorumCalls.Load() != 0 ||
				peers.nodeCalls.Load() != 0 || peers.metricCalls.Load() != 0 {
				t.Fatalf("invalid membership still queried nodes: local=%d/%d quorum=%d peers=%d/%d",
					source.nodeCalls.Load(), source.metricCalls.Load(), source.quorumCalls.Load(), peers.nodeCalls.Load(), peers.metricCalls.Load())
			}
		})
	}
}

func TestClusterReadFailuresAndCancellationPropagateToEveryQuery(t *testing.T) {
	t.Run("source error is returned unchanged", func(t *testing.T) {
		boom := errors.New("raft membership read failed")
		source := newQuerySource(queryCluster("1", queryVoter("1")))
		source.clusterErr = boom
		service := newQueryService(t, source, nil)
		for _, query := range queryCalls("1") {
			if err := query.call(service, context.Background()); !errors.Is(err, boom) {
				t.Fatalf("%s error = %v, want %v", query.name, err, boom)
			}
		}
	})
	t.Run("canceled caller gets its own error", func(t *testing.T) {
		source := newQuerySource(queryCluster("1", queryVoter("1")))
		service := newQueryService(t, source, nil)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		for _, query := range queryCalls("1") {
			if err := query.call(service, ctx); !errors.Is(err, context.Canceled) {
				t.Fatalf("%s error = %v, want context.Canceled", query.name, err)
			}
		}
		if source.nodeCalls.Load() != 0 || source.metricCalls.Load() != 0 {
			t.Fatalf("canceled queries captured local snapshots: %d/%d", source.nodeCalls.Load(), source.metricCalls.Load())
		}
	})
}

func TestCancellationDuringPeerQueryAbortsEveryQueryWithCallerError(t *testing.T) {
	for _, query := range queryCalls("2") {
		t.Run(query.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			source := newQuerySource(queryCluster("1", queryVoter("1"), queryVoter("2")))
			source.node = queryNodeSnapshot(observationports.NodeStateReady, 1, 1)
			source.metrics = observationports.MetricsSnapshotDTO{StartedAt: queryTestStarted}
			peers := &scriptedPeers{hook: cancel}
			service := newQueryService(t, source, peers)
			err := query.call(service, ctx)
			if query.name == "CollectMetrics/node" {
				// Node mode queries only the local node, so no peer is contacted and the query succeeds.
				if err != nil || peers.metricCalls.Load() != 0 {
					t.Fatalf("node mode contacted a peer: err=%v calls=%d", err, peers.metricCalls.Load())
				}
				return
			}
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("%s error = %v, want context.Canceled", query.name, err)
			}
		})
	}
}

func TestGetClusterDegradesQuorumToUnavailableWhenFreshProofFails(t *testing.T) {
	cluster := queryCluster("1", queryVoter("1"))
	cluster.LeaderNodeID = new("1")
	cluster.UnderReplicated, cluster.SubscriptionCount = 2, 17
	build := func(quorumErr error) observationports.ClusterDataDTO {
		source := newQuerySource(cluster)
		source.node = queryNodeSnapshot(observationports.NodeStateReady, 4, 9)
		source.quorumErr = quorumErr
		data, err := newQueryService(t, source, nil).GetCluster(context.Background())
		if err != nil {
			t.Fatalf("GetCluster(): %v", err)
		}
		return data
	}
	healthy := build(nil)
	degraded := build(errors.New("fresh quorum proof failed"))
	if !healthy.QuorumAvailable || degraded.QuorumAvailable {
		t.Fatalf("quorum available healthy=%v degraded=%v, want true/false", healthy.QuorumAvailable, degraded.QuorumAvailable)
	}
	if degraded.LeaderNodeID == nil || *degraded.LeaderNodeID != "1" || degraded.UnderReplicated != 2 || degraded.SubscriptionCount != 17 ||
		degraded.ClusterID != "cluster" || degraded.ReleaseVersion != "1.0.0" || degraded.QuorumSize != 2 || degraded.ReplicationFactor != 3 ||
		degraded.WriteQuorum != 2 || !degraded.ObservedAt.Equal(queryTestNow) {
		t.Fatalf("cluster metadata was not preserved while quorum was unknown: %+v", degraded)
	}
	if degraded.ConnectionCount == nil || *degraded.ConnectionCount != 4 || degraded.SessionCount == nil || *degraded.SessionCount != 9 {
		t.Fatalf("node measurements lost when quorum proof failed: %+v", degraded)
	}
}

func TestGetClusterReturnsCallerErrorWhenQuorumProofIsCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	source := newQuerySource(queryCluster("1", queryVoter("1")))
	source.onQuorum = cancel
	source.quorumErr = errors.New("quorum proof interrupted")
	if _, err := newQueryService(t, source, nil).GetCluster(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("GetCluster() error = %v, want context.Canceled", err)
	}
}

// Scenarios: management-api-and-observability.md:MGT-SCN-022
func TestGetNodeClassifiesEveryNodeQueryOutcome(t *testing.T) {
	validBase := queryNodeSnapshot(observationports.NodeStateReady, 1, 1)
	cases := []struct {
		name       string
		snapshot   observationports.NodeSnapshotDTO
		err        error
		wantStatus observationports.QueryStatus
		wantCode   observationports.QueryErrorCode
	}{
		{name: "unsupported peer", err: &observationports.PeerQueryError{Code: observationports.QueryErrorUnsupported}, wantStatus: observationports.QueryStatusUnsupported, wantCode: observationports.QueryErrorUnsupported},
		{name: "busy peer", err: &observationports.PeerQueryError{Code: observationports.QueryErrorBusy}, wantStatus: observationports.QueryStatusError, wantCode: observationports.QueryErrorBusy},
		{name: "unreachable peer", err: &observationports.PeerQueryError{Code: observationports.QueryErrorNodeUnreachable}, wantStatus: observationports.QueryStatusUnreachable, wantCode: observationports.QueryErrorNodeUnreachable},
		{name: "peer timeout code", err: &observationports.PeerQueryError{Code: observationports.QueryErrorTimeout}, wantStatus: observationports.QueryStatusUnreachable, wantCode: observationports.QueryErrorTimeout},
		{name: "peer reports oversized snapshot", err: &observationports.PeerQueryError{Code: observationports.QueryErrorSnapshotTooLarge}, wantStatus: observationports.QueryStatusError, wantCode: observationports.QueryErrorSnapshotTooLarge},
		{name: "peer error without code", err: &observationports.PeerQueryError{}, wantStatus: observationports.QueryStatusError, wantCode: observationports.QueryErrorFailed},
		{name: "deadline exceeded", err: fmt.Errorf("dial: %w", context.DeadlineExceeded), wantStatus: observationports.QueryStatusUnreachable, wantCode: observationports.QueryErrorTimeout},
		{name: "unclassified failure", err: errors.New("connection reset"), wantStatus: observationports.QueryStatusError, wantCode: observationports.QueryErrorFailed},
		{name: "negative uptime", snapshot: func() observationports.NodeSnapshotDTO {
			s := validBase
			s.UptimeMillis = new(int64(-1))
			return s
		}(), wantStatus: observationports.QueryStatusError, wantCode: observationports.QueryErrorFailed},
		{name: "unknown node state", snapshot: func() observationports.NodeSnapshotDTO {
			s := validBase
			s.State = new(observationports.NodeState("BOGUS"))
			return s
		}(), wantStatus: observationports.QueryStatusError, wantCode: observationports.QueryErrorFailed},
		{name: "unknown role", snapshot: func() observationports.NodeSnapshotDTO {
			s := validBase
			s.Role = new("WIZARD")
			return s
		}(), wantStatus: observationports.QueryStatusError, wantCode: observationports.QueryErrorFailed},
		{name: "unknown replication health", snapshot: func() observationports.NodeSnapshotDTO {
			s := validBase
			s.ReplicationHealth = new("WOBBLY")
			return s
		}(), wantStatus: observationports.QueryStatusError, wantCode: observationports.QueryErrorFailed},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			source := newQuerySource(queryCluster("1", queryVoter("1"), queryVoter("2")))
			peers := &scriptedPeers{nodes: map[string]observationports.NodeSnapshotDTO{"2": test.snapshot}}
			if test.err != nil {
				peers.nodeErrs = map[string]error{"2": test.err}
			}
			node, err := newQueryService(t, source, peers).GetNode(context.Background(), "2")
			if err != nil {
				t.Fatalf("GetNode(): %v", err)
			}
			if node.NodeID != "2" || node.MembershipPhase != observationports.MembershipPhaseVoter || node.QueryStatus != test.wantStatus ||
				node.QueryErrorCode == nil || *node.QueryErrorCode != test.wantCode {
				t.Fatalf("node = %+v, want status %s code %s", node, test.wantStatus, test.wantCode)
			}
			if node.State != nil || node.Ready != nil || node.ObservedAt != nil || node.ConnectionCount != nil || node.SessionCount != nil {
				t.Fatalf("failed query leaked measurements: %+v", node)
			}
		})
	}
}

// Scenarios: management-api-and-observability.md:MGT-SCN-022
func TestGetNodeReportsPhaseBasedStatusWithoutContactingStagedOrRetiredNodes(t *testing.T) {
	cluster := queryCluster("1", queryVoter("1"),
		observationports.MemberDTO{NodeID: "2", Phase: observationports.MembershipPhaseStaged},
		observationports.MemberDTO{NodeID: "3", Phase: observationports.MembershipPhaseRetired},
		queryVoter("4"))
	source := newQuerySource(cluster)
	peers := &scriptedPeers{}
	service := newQueryService(t, source, peers)

	staged, err := service.GetNode(context.Background(), "2")
	if err != nil || staged.QueryStatus != observationports.QueryStatusUnreachable || staged.QueryErrorCode == nil ||
		*staged.QueryErrorCode != observationports.QueryErrorNodeNotRunning || staged.MembershipPhase != observationports.MembershipPhaseStaged {
		t.Fatalf("staged node = %+v err=%v", staged, err)
	}
	retired, err := service.GetNode(context.Background(), "3")
	if err != nil || retired.QueryStatus != observationports.QueryStatusRetired || retired.QueryErrorCode != nil {
		t.Fatalf("retired node = %+v err=%v", retired, err)
	}
	if peers.nodeCalls.Load() != 0 {
		t.Fatalf("staged or retired node was contacted %d times", peers.nodeCalls.Load())
	}
	if _, err := service.GetNode(context.Background(), "99"); !errors.Is(err, observationports.ErrNodeNotFound) {
		t.Fatalf("unknown node error = %v, want %v", err, observationports.ErrNodeNotFound)
	}
	// Without a peer reader every remote node is unreachable, never a nil dereference.
	isolated, err := newQueryService(t, newQuerySource(cluster), nil).GetNode(context.Background(), "4")
	if err != nil || isolated.QueryStatus != observationports.QueryStatusUnreachable || isolated.QueryErrorCode == nil ||
		*isolated.QueryErrorCode != observationports.QueryErrorNodeUnreachable {
		t.Fatalf("node without peer reader = %+v err=%v", isolated, err)
	}
}

func TestGetNodeUsesLocalSnapshotForLocalNodeAndNormalizesMeasurements(t *testing.T) {
	zone := time.FixedZone("KST", 9*60*60)
	local := observationports.NodeSnapshotDTO{
		ObservedAt: time.Date(2026, time.January, 2, 12, 0, 0, 0, zone), Version: new("1.2.3"), UptimeMillis: new(int64(42)),
		State: new(observationports.NodeStateDraining), Ready: new(false), ReadinessReason: new("drain_in_progress"),
		Role: new("FOLLOWER"), LastAppliedIndex: new(uint64(77)), ConnectionCount: new(uint64(3)), SessionCount: new(uint64(5)),
		StoragePressure: new(true), ReplicationHealth: new("DEGRADED"), LastErrorCode: new("wal_slow"),
	}
	source := newQuerySource(queryCluster("1", queryVoter("1"), queryVoter("2")))
	source.node = local
	peers := &scriptedPeers{}
	node, err := newQueryService(t, source, peers).GetNode(context.Background(), "1")
	if err != nil {
		t.Fatal(err)
	}
	if peers.nodeCalls.Load() != 0 || source.nodeCalls.Load() != 1 {
		t.Fatalf("local node queried through peers: peers=%d local=%d", peers.nodeCalls.Load(), source.nodeCalls.Load())
	}
	if node.QueryStatus != observationports.QueryStatusOK || node.ObservedAt == nil || node.ObservedAt.Location() != time.UTC ||
		!node.ObservedAt.Equal(local.ObservedAt) || node.Version == nil || *node.Version != "1.2.3" || *node.UptimeMillis != 42 ||
		*node.State != observationports.NodeStateDraining || *node.Ready || *node.ReadinessReason != "drain_in_progress" || *node.Role != "FOLLOWER" ||
		*node.LastAppliedIndex != 77 || *node.ConnectionCount != 3 || *node.SessionCount != 5 || !*node.StoragePressure ||
		*node.ReplicationHealth != "DEGRADED" || *node.LastErrorCode != "wal_slow" {
		t.Fatalf("local node measurements = %+v", node)
	}

	local.ObservedAt = time.Time{}
	source.node = local
	node, err = newQueryService(t, source, peers).GetNode(context.Background(), "1")
	if err != nil || node.ObservedAt == nil || !node.ObservedAt.Equal(queryTestNow) {
		t.Fatalf("missing ObservedAt was not stamped with the service clock: %+v err=%v", node, err)
	}

	source.nodeErr = errors.New("local snapshot failed")
	node, err = newQueryService(t, source, peers).GetNode(context.Background(), "1")
	if err != nil || node.QueryStatus != observationports.QueryStatusError || node.QueryErrorCode == nil || *node.QueryErrorCode != observationports.QueryErrorFailed {
		t.Fatalf("local capture failure = %+v err=%v", node, err)
	}
}

func TestListNodesRejectsInvalidArgumentsBeforeReadingMembership(t *testing.T) {
	cursor := func(value string) string { return base64.RawURLEncoding.EncodeToString([]byte(value)) }
	cases := []struct {
		name    string
		filter  observationports.NodeFilter
		wantErr error
	}{
		{"unknown state", observationports.NodeFilter{State: "BOGUS"}, nil},
		{"negative limit", observationports.NodeFilter{Limit: -1}, observationports.ErrInvalidPageSize},
		{"limit above maximum", observationports.NodeFilter{Limit: 1001}, observationports.ErrInvalidPageSize},
		{"cursor is not base64", observationports.NodeFilter{Cursor: "!!!"}, observationports.ErrInvalidCursor},
		{"cursor lacks prefix", observationports.NodeFilter{Cursor: cursor("offset:1")}, observationports.ErrInvalidCursor},
		{"cursor offset is not a number", observationports.NodeFilter{Cursor: cursor("cur:abc")}, observationports.ErrInvalidCursor},
		{"cursor offset is negative", observationports.NodeFilter{Cursor: cursor("cur:-1")}, observationports.ErrInvalidCursor},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			source := newQuerySource(queryCluster("1", queryVoter("1")))
			nodes, page, err := newQueryService(t, source, nil).ListNodes(context.Background(), test.filter)
			if err == nil || (test.wantErr != nil && !errors.Is(err, test.wantErr)) || nodes != nil || page != nil {
				t.Fatalf("ListNodes(%+v) = %v, %v, %v; want error %v", test.filter, nodes, page, err, test.wantErr)
			}
			if source.clusterCalls.Load() != 0 {
				t.Fatalf("invalid filter still read membership")
			}
		})
	}
}

func TestListNodesPagesNumericallyOrderedMembersWithStableCursors(t *testing.T) {
	members := make([]observationports.MemberDTO, 0, 5)
	for _, id := range []string{"10", "2", "1", "11", "3"} {
		members = append(members, queryVoter(id))
	}
	source := newQuerySource(queryCluster("1", members...))
	source.node = queryNodeSnapshot(observationports.NodeStateReady, 1, 1)
	ready := queryNodeSnapshot(observationports.NodeStateReady, 1, 1)
	service := newQueryService(t, source, &scriptedPeers{defaultNode: &ready})

	var ids []string
	cursor := ""
	for pageNumber := range 3 {
		nodes, page, err := service.ListNodes(context.Background(), observationports.NodeFilter{Limit: 2, Cursor: cursor})
		if err != nil || page == nil {
			t.Fatalf("page %d: err=%v page=%v", pageNumber, err, page)
		}
		for _, node := range nodes {
			ids = append(ids, node.NodeID)
		}
		wantMore := pageNumber < 2
		if page.HasMore != wantMore || (page.NextCursor != nil) != wantMore {
			t.Fatalf("page %d HasMore=%v cursor=%v, want more=%v", pageNumber, page.HasMore, page.NextCursor, wantMore)
		}
		if wantMore {
			cursor = *page.NextCursor
		}
	}
	if strings.Join(ids, ",") != "1,2,3,10,11" {
		t.Fatalf("paged node order = %v, want ascending numeric IDs", ids)
	}
	beyond := base64.RawURLEncoding.EncodeToString([]byte("cur:99"))
	nodes, page, err := service.ListNodes(context.Background(), observationports.NodeFilter{Limit: 2, Cursor: beyond})
	if err != nil || len(nodes) != 0 || page == nil || page.HasMore || page.NextCursor != nil {
		t.Fatalf("cursor beyond the end = %v %+v err=%v, want an empty last page", nodes, page, err)
	}
}

func TestListNodesAppliesStateFilterBeforePaginationAndDefaultLimit(t *testing.T) {
	source := newQuerySource(queryCluster("1", queryVoter("1"), queryVoter("2"), queryVoter("3"), queryVoter("4"), queryVoter("5")))
	source.node = queryNodeSnapshot(observationports.NodeStateReady, 1, 1)
	peers := &scriptedPeers{
		nodes: map[string]observationports.NodeSnapshotDTO{
			"2": queryNodeSnapshot(observationports.NodeStateDraining, 0, 0),
			"3": queryNodeSnapshot(observationports.NodeStateReady, 1, 1),
			"4": queryNodeSnapshot(observationports.NodeStateReady, 1, 1),
		},
		nodeErrs: map[string]error{"5": &observationports.PeerQueryError{Code: observationports.QueryErrorNodeUnreachable}},
	}
	service := newQueryService(t, source, peers)
	first, page, err := service.ListNodes(context.Background(), observationports.NodeFilter{State: observationports.NodeStateReady, Limit: 2})
	if err != nil || len(first) != 2 || first[0].NodeID != "1" || first[1].NodeID != "3" || !page.HasMore || page.NextCursor == nil {
		t.Fatalf("first READY page = %+v %+v err=%v", first, page, err)
	}
	second, page, err := service.ListNodes(context.Background(), observationports.NodeFilter{State: observationports.NodeStateReady, Limit: 2, Cursor: *page.NextCursor})
	if err != nil || len(second) != 1 || second[0].NodeID != "4" || page.HasMore {
		t.Fatalf("second READY page = %+v %+v err=%v", second, page, err)
	}
	unfiltered, page, err := service.ListNodes(context.Background(), observationports.NodeFilter{})
	if err != nil || len(unfiltered) != 5 || page.HasMore || unfiltered[4].QueryStatus != observationports.QueryStatusUnreachable {
		t.Fatalf("unfiltered list = %d nodes page=%+v err=%v", len(unfiltered), page, err)
	}

	large := make([]observationports.MemberDTO, 60)
	for index := range large {
		large[index] = queryVoter(strconv.Itoa(index + 1))
	}
	largeSource := newQuerySource(queryCluster("1", large...))
	largeSource.node = queryNodeSnapshot(observationports.NodeStateReady, 1, 1)
	ready := queryNodeSnapshot(observationports.NodeStateReady, 1, 1)
	nodes, page, err := newQueryService(t, largeSource, &scriptedPeers{defaultNode: &ready}).ListNodes(context.Background(), observationports.NodeFilter{})
	if err != nil || len(nodes) != defaultPageLimit || !page.HasMore {
		t.Fatalf("default page = %d nodes page=%+v err=%v, want %d with more", len(nodes), page, err, defaultPageLimit)
	}
}

func TestMembersWithNonNumericIDsSortLexicographically(t *testing.T) {
	source := newQuerySource(queryCluster("2", queryVoter("a"), queryVoter("10"), queryVoter("2")))
	source.node = queryNodeSnapshot(observationports.NodeStateReady, 1, 1)
	ready := queryNodeSnapshot(observationports.NodeStateReady, 1, 1)
	nodes, _, err := newQueryService(t, source, &scriptedPeers{defaultNode: &ready}).ListNodes(context.Background(), observationports.NodeFilter{})
	if err != nil || len(nodes) != 3 || nodes[0].NodeID != "2" || nodes[1].NodeID != "10" || nodes[2].NodeID != "a" {
		t.Fatalf("mixed ID order = %+v err=%v", nodes, err)
	}
}

// Scenarios: management-api-and-observability.md:MGT-SCN-022
func TestGetClusterCountsEachStateAndWithholdsTotalsWhenAnyNodeIsUnknown(t *testing.T) {
	cluster := queryCluster("1", queryVoter("1"), queryVoter("2"), queryVoter("3"), queryVoter("4"), queryVoter("5"),
		observationports.MemberDTO{NodeID: "6", Phase: observationports.MembershipPhaseRetired},
		observationports.MemberDTO{NodeID: "7", Phase: observationports.MembershipPhaseStaged})
	source := newQuerySource(cluster)
	source.node = queryNodeSnapshot(observationports.NodeStateReady, 5, 6)
	noState := queryNodeSnapshot(observationports.NodeStateReady, 1, 1)
	noState.State = nil
	peers := &scriptedPeers{nodes: map[string]observationports.NodeSnapshotDTO{
		"2": queryNodeSnapshot(observationports.NodeStateDraining, 1, 2),
		"3": queryNodeSnapshot(observationports.NodeStateSafeToStop, 0, 0),
		"4": queryNodeSnapshot(observationports.NodeStateJoining, 1, 1),
		"5": noState,
	}}
	data, err := newQueryService(t, source, peers).GetCluster(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	want := observationports.NodesByStateDTO{Joining: 1, Ready: 1, Draining: 1, SafeToStop: 1, Unknown: 2}
	if data.MemberCount != 6 || data.NodesByState != want || data.NodeQueryComplete || data.ConnectionCount != nil || data.SessionCount != nil {
		t.Fatalf("cluster summary = %+v, want 6 non-retired members, %+v, incomplete, no totals", data, want)
	}
	if peers.nodeCalls.Load() != 4 {
		t.Fatalf("remote nodes contacted %d times, want 4 (staged and retired are skipped)", peers.nodeCalls.Load())
	}
}

func TestGetClusterTotalsRequireEveryCountAndRejectOverflow(t *testing.T) {
	cluster := queryCluster("1", queryVoter("1"), queryVoter("2"), observationports.MemberDTO{NodeID: "3", Phase: observationports.MembershipPhaseRetired})
	cases := []struct {
		name     string
		local    observationports.NodeSnapshotDTO
		remote   observationports.NodeSnapshotDTO
		wantConn *uint64
		wantSess *uint64
	}{
		{"all counts known", queryNodeSnapshot(observationports.NodeStateReady, 5, 6), queryNodeSnapshot(observationports.NodeStateReady, 1, 2), new(uint64(6)), new(uint64(8))},
		{"connection count missing", func() observationports.NodeSnapshotDTO {
			s := queryNodeSnapshot(observationports.NodeStateReady, 5, 6)
			s.ConnectionCount = nil
			return s
		}(), queryNodeSnapshot(observationports.NodeStateReady, 1, 2), nil, nil},
		{"session count missing", queryNodeSnapshot(observationports.NodeStateReady, 5, 6), func() observationports.NodeSnapshotDTO {
			s := queryNodeSnapshot(observationports.NodeStateReady, 1, 2)
			s.SessionCount = nil
			return s
		}(), nil, nil},
		{"connection total overflows", queryNodeSnapshot(observationports.NodeStateReady, math.MaxUint64, 1), queryNodeSnapshot(observationports.NodeStateReady, 1, 1), nil, nil},
		{"session total overflows", queryNodeSnapshot(observationports.NodeStateReady, 1, math.MaxUint64), queryNodeSnapshot(observationports.NodeStateReady, 1, 1), nil, nil},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			source := newQuerySource(cluster)
			source.node = test.local
			peers := &scriptedPeers{nodes: map[string]observationports.NodeSnapshotDTO{"2": test.remote}}
			data, err := newQueryService(t, source, peers).GetCluster(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			same := func(got, want *uint64) bool { return (got == nil) == (want == nil) && (got == nil || *got == *want) }
			if !same(data.ConnectionCount, test.wantConn) || !same(data.SessionCount, test.wantSess) {
				t.Fatalf("totals connections=%v sessions=%v, want %v/%v", data.ConnectionCount, data.SessionCount, test.wantConn, test.wantSess)
			}
			// Missing or overflowing counts must not make state counts or query completeness look wrong.
			if !data.NodeQueryComplete || data.NodesByState.Ready != 2 || data.MemberCount != 2 {
				t.Fatalf("known states were degraded by the totals: %+v", data)
			}
		})
	}
}

func queryMetricsSnapshot(connections uint64) observationports.MetricsSnapshotDTO {
	return observationports.MetricsSnapshotDTO{
		ObservedAt: queryTestNow.Add(-time.Second), StartedAt: queryTestStarted,
		Snapshot: metricsports.MetricsSnapshotDTO{Families: []metricsports.MetricFamilyDTO{{
			Descriptor: descriptorForTest("narumq_connections_active"),
			Samples:    []metricsports.MetricSampleDTO{queryIntSample("narumq_connections_active", connections)},
		}}},
	}
}

func queryIntSample(name string, value uint64, labels ...metricsports.MetricLabelDTO) metricsports.MetricSampleDTO {
	return metricsports.MetricSampleDTO{Name: name, Labels: labels, Value: metricsports.MetricValueDTO{Kind: metricsports.MetricValueInteger, Uint64: value}}
}

func queryFloatSample(name string, value float64, labels ...metricsports.MetricLabelDTO) metricsports.MetricSampleDTO {
	return metricsports.MetricSampleDTO{Name: name, Labels: labels, Value: metricsports.MetricValueDTO{Kind: metricsports.MetricValueFloat, Float64: value}}
}

// Scenarios: management-api-and-observability.md:MGT-SCN-023
func TestGetNodeMetricsClassifiesFailuresAndNormalizesSnapshots(t *testing.T) {
	cluster := queryCluster("1", queryVoter("1"), queryVoter("2"),
		observationports.MemberDTO{NodeID: "3", Phase: observationports.MembershipPhaseStaged},
		observationports.MemberDTO{NodeID: "4", Phase: observationports.MembershipPhaseRetired})
	t.Run("peer failure codes", func(t *testing.T) {
		for _, test := range []struct {
			name       string
			err        error
			wantStatus observationports.QueryStatus
			wantCode   observationports.QueryErrorCode
		}{
			{"unsupported", &observationports.PeerQueryError{Code: observationports.QueryErrorUnsupported}, observationports.QueryStatusUnsupported, observationports.QueryErrorUnsupported},
			{"busy", &observationports.PeerQueryError{Code: observationports.QueryErrorBusy}, observationports.QueryStatusError, observationports.QueryErrorBusy},
			{"deadline", fmt.Errorf("read: %w", context.DeadlineExceeded), observationports.QueryStatusUnreachable, observationports.QueryErrorTimeout},
			{"unclassified", errors.New("connection reset"), observationports.QueryStatusError, observationports.QueryErrorFailed},
		} {
			peers := &scriptedPeers{metricErrs: map[string]error{"2": test.err}}
			result, err := newQueryService(t, newQuerySource(cluster), peers).GetNodeMetrics(context.Background(), "2")
			if err != nil || result.QueryStatus != test.wantStatus || result.QueryErrorCode == nil || *result.QueryErrorCode != test.wantCode ||
				result.Families != nil || result.ObservedAt != nil || result.StartedAt != nil {
				t.Fatalf("%s: result=%+v err=%v", test.name, result, err)
			}
		}
	})
	t.Run("phase based results never contact peers", func(t *testing.T) {
		peers := &scriptedPeers{}
		service := newQueryService(t, newQuerySource(cluster), peers)
		staged, err := service.GetNodeMetrics(context.Background(), "3")
		if err != nil || staged.QueryStatus != observationports.QueryStatusUnreachable || staged.QueryErrorCode == nil || *staged.QueryErrorCode != observationports.QueryErrorNodeNotRunning {
			t.Fatalf("staged node metrics = %+v err=%v", staged, err)
		}
		retired, err := service.GetNodeMetrics(context.Background(), "4")
		if err != nil || retired.QueryStatus != observationports.QueryStatusRetired || retired.QueryErrorCode != nil {
			t.Fatalf("retired node metrics = %+v err=%v", retired, err)
		}
		if _, err := service.GetNodeMetrics(context.Background(), "99"); !errors.Is(err, observationports.ErrNodeNotFound) {
			t.Fatalf("unknown node error = %v", err)
		}
		if peers.metricCalls.Load() != 0 {
			t.Fatalf("peers contacted %d times", peers.metricCalls.Load())
		}
		isolated, err := newQueryService(t, newQuerySource(cluster), nil).GetNodeMetrics(context.Background(), "2")
		if err != nil || isolated.QueryStatus != observationports.QueryStatusUnreachable || *isolated.QueryErrorCode != observationports.QueryErrorNodeUnreachable {
			t.Fatalf("metrics without peer reader = %+v err=%v", isolated, err)
		}
	})
	t.Run("remote snapshot with unknown family is rejected whole", func(t *testing.T) {
		bad := queryMetricsSnapshot(1)
		bad.Snapshot.Families[0].Descriptor.Name = "narumq_made_up_total"
		peers := &scriptedPeers{metrics: map[string]observationports.MetricsSnapshotDTO{"2": bad}}
		result, err := newQueryService(t, newQuerySource(cluster), peers).GetNodeMetrics(context.Background(), "2")
		if err != nil || result.QueryStatus != observationports.QueryStatusError || *result.QueryErrorCode != observationports.QueryErrorFailed || result.Families != nil {
			t.Fatalf("unknown family result = %+v err=%v", result, err)
		}
	})
	t.Run("snapshot without reset boundary is rejected", func(t *testing.T) {
		source := newQuerySource(cluster)
		source.metrics = queryMetricsSnapshot(1)
		source.metrics.StartedAt = time.Time{}
		result, err := newQueryService(t, source, nil).GetNodeMetrics(context.Background(), "1")
		if err != nil || result.QueryStatus != observationports.QueryStatusError || *result.QueryErrorCode != observationports.QueryErrorFailed {
			t.Fatalf("zero StartedAt result = %+v err=%v", result, err)
		}
	})
	t.Run("missing ObservedAt uses clock and times are UTC", func(t *testing.T) {
		zone := time.FixedZone("KST", 9*60*60)
		source := newQuerySource(cluster)
		source.metrics = queryMetricsSnapshot(7)
		source.metrics.ObservedAt = time.Time{}
		source.metrics.StartedAt = time.Date(2026, time.January, 1, 9, 0, 0, 0, zone)
		result, err := newQueryService(t, source, nil).GetNodeMetrics(context.Background(), "1")
		if err != nil || result.QueryStatus != observationports.QueryStatusOK || result.ObservedAt == nil || !result.ObservedAt.Equal(queryTestNow) ||
			result.StartedAt == nil || result.StartedAt.Location() != time.UTC || !result.StartedAt.Equal(queryTestStarted) {
			t.Fatalf("normalized metrics = %+v err=%v", result, err)
		}
		if len(result.Families) != 1 || len(result.Families[0].Samples) != 1 || result.Families[0].Samples[0].Value != "7" {
			t.Fatalf("families = %+v", result.Families)
		}
	})
}

// Scenarios: management-api-and-observability.md:MGT-SCN-023
func TestCollectMetricsRejectsInvalidModeBeforeReadingMembership(t *testing.T) {
	source := newQuerySource(queryCluster("1", queryVoter("1")))
	result, err := newQueryService(t, source, nil).CollectMetrics(context.Background(), "everything")
	if err == nil || len(result.Nodes) != 0 || len(result.Families) != 0 || source.clusterCalls.Load() != 0 {
		t.Fatalf("invalid mode result=%+v err=%v clusterCalls=%d", result, err, source.clusterCalls.Load())
	}
}

// Scenarios: management-api-and-observability.md:MGT-SCN-023
func TestCollectMetricsHandlesPartialFailureAndRetiredMembersPerMode(t *testing.T) {
	t.Run("unaggregated returns incomplete partial result", func(t *testing.T) {
		source := newQuerySource(queryCluster("1", queryVoter("1"), queryVoter("2"), queryVoter("3")))
		source.metrics = queryMetricsSnapshot(2)
		peers := &scriptedPeers{
			metrics:    map[string]observationports.MetricsSnapshotDTO{"2": queryMetricsSnapshot(3)},
			metricErrs: map[string]error{"3": &observationports.PeerQueryError{Code: observationports.QueryErrorNodeUnreachable}},
		}
		result, err := newQueryService(t, source, peers).CollectMetrics(context.Background(), observationports.MetricsModeAllNodesUnaggregated)
		if err != nil {
			t.Fatal(err)
		}
		if result.Complete || len(result.Nodes) != 3 || result.Nodes[0].QueryStatus != observationports.QueryStatusOK ||
			result.Nodes[2].NodeID != "3" || result.Nodes[2].QueryStatus != observationports.QueryStatusUnreachable ||
			result.Nodes[2].QueryErrorCode == nil || *result.Nodes[2].QueryErrorCode != observationports.QueryErrorNodeUnreachable {
			t.Fatalf("collection status = %+v", result)
		}
		family := findFamilyForTest(t, result.Families, "narumq_connections_active")
		if len(family.Samples) != 2 || !hasString(family.LabelNames, "node_id") {
			t.Fatalf("partial family = %+v", family)
		}
		for index, want := range []struct{ node, value string }{{"1", "2"}, {"2", "3"}} {
			node, _ := labelValueForTest(family.Samples[index], "node_id")
			if node != want.node || family.Samples[index].Value != want.value {
				t.Fatalf("sample %d = %+v, want node %s value %s", index, family.Samples[index], want.node, want.value)
			}
		}
	})
	t.Run("aggregated refuses incomplete collection", func(t *testing.T) {
		source := newQuerySource(queryCluster("1", queryVoter("1"), queryVoter("2")))
		source.metrics = queryMetricsSnapshot(2)
		peers := &scriptedPeers{metricErrs: map[string]error{"2": errors.New("connection reset")}}
		result, err := newQueryService(t, source, peers).CollectMetrics(context.Background(), observationports.MetricsModeAllNodesAggregated)
		if !errors.Is(err, observationports.ErrMetricsCollectionIncomplete) || len(result.Families) != 0 || len(result.Nodes) != 0 {
			t.Fatalf("aggregated partial result=%+v err=%v", result, err)
		}
	})
	t.Run("retired members do not make aggregation incomplete", func(t *testing.T) {
		source := newQuerySource(queryCluster("1", queryVoter("1"), queryVoter("2"), observationports.MemberDTO{NodeID: "3", Phase: observationports.MembershipPhaseRetired}))
		source.metrics = queryMetricsSnapshot(2)
		peers := &scriptedPeers{metrics: map[string]observationports.MetricsSnapshotDTO{"2": queryMetricsSnapshot(3)}}
		result, err := newQueryService(t, source, peers).CollectMetrics(context.Background(), observationports.MetricsModeAllNodesAggregated)
		if err != nil || !result.Complete || len(result.Nodes) != 3 || result.Nodes[2].QueryStatus != observationports.QueryStatusRetired {
			t.Fatalf("collection with retired member = %+v err=%v", result, err)
		}
		family := findFamilyForTest(t, result.Families, "narumq_connections_active")
		if len(family.Samples) != 1 || family.Samples[0].Value != "5" || hasString(family.LabelNames, "node_id") {
			t.Fatalf("aggregated sum = %+v", family)
		}
		if peers.metricCalls.Load() != 1 {
			t.Fatalf("peer queries = %d, want one (retired node skipped)", peers.metricCalls.Load())
		}
	})
}

// Scenarios: management-api-and-observability.md:MGT-SCN-023
func TestCollectMetricsAggregationFailuresAreReportedWithoutPartialFamilies(t *testing.T) {
	floatSnapshot := queryMetricsSnapshot(0)
	floatSnapshot.Snapshot.Families[0].Samples = []metricsports.MetricSampleDTO{queryFloatSample("narumq_connections_active", 1.5)}
	cases := []struct {
		name   string
		local  observationports.MetricsSnapshotDTO
		remote observationports.MetricsSnapshotDTO
	}{
		{"integer gauge overflow", queryMetricsSnapshot(math.MaxUint64), queryMetricsSnapshot(1)},
		{"gauge value kinds differ across nodes", queryMetricsSnapshot(1), floatSnapshot},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			source := newQuerySource(queryCluster("1", queryVoter("1"), queryVoter("2")))
			source.metrics = test.local
			peers := &scriptedPeers{metrics: map[string]observationports.MetricsSnapshotDTO{"2": test.remote}}
			result, err := newQueryService(t, source, peers).CollectMetrics(context.Background(), observationports.MetricsModeAllNodesAggregated)
			if !errors.Is(err, observationports.ErrMetricsAggregationFailed) || len(result.Families) != 0 {
				t.Fatalf("result=%+v err=%v, want %v", result, err, observationports.ErrMetricsAggregationFailed)
			}
			// The same data stays readable without aggregation.
			raw, err := newQueryService(t, source, peers).CollectMetrics(context.Background(), observationports.MetricsModeAllNodesUnaggregated)
			if err != nil || !raw.Complete || len(findFamilyForTest(t, raw.Families, "narumq_connections_active").Samples) != 2 {
				t.Fatalf("unaggregated fallback = %+v err=%v", raw, err)
			}
		})
	}
}

func TestCollectMetricsNodeModeFailureAndCancellation(t *testing.T) {
	t.Run("local snapshot without reset boundary fails aggregation", func(t *testing.T) {
		source := newQuerySource(queryCluster("1", queryVoter("1")))
		source.metrics = queryMetricsSnapshot(1)
		source.metrics.StartedAt = time.Time{}
		result, err := newQueryService(t, source, nil).CollectMetrics(context.Background(), observationports.MetricsModeNode)
		if !errors.Is(err, observationports.ErrMetricsAggregationFailed) || len(result.Families) != 0 {
			t.Fatalf("result=%+v err=%v", result, err)
		}
	})
	t.Run("local capture canceled mid-query", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		source := newQuerySource(queryCluster("1", queryVoter("1")))
		source.metrics = queryMetricsSnapshot(1)
		source.onMetrics = cancel
		result, err := newQueryService(t, source, nil).CollectMetrics(ctx, observationports.MetricsModeNode)
		if !errors.Is(err, context.Canceled) || len(result.Families) != 0 || len(result.Nodes) != 0 {
			t.Fatalf("result=%+v err=%v", result, err)
		}
	})
	t.Run("local capture failure is a sanitized node error in cluster modes", func(t *testing.T) {
		source := newQuerySource(queryCluster("1", queryVoter("1")))
		source.metricsErr = errors.New("registry unavailable")
		result, err := newQueryService(t, source, nil).CollectMetrics(context.Background(), observationports.MetricsModeAllNodesUnaggregated)
		if err != nil || result.Complete || len(result.Nodes) != 1 || result.Nodes[0].QueryStatus != observationports.QueryStatusError ||
			*result.Nodes[0].QueryErrorCode != observationports.QueryErrorFailed || len(result.Families) != 0 {
			t.Fatalf("result=%+v err=%v", result, err)
		}
	})
}

// Traceability: PRD API-03; docs/specs/management-api-and-observability.md §7.2.
// Scenarios: management-api-and-observability.md:MGT-SCN-023
func TestMetricFamiliesFromSnapshotRejectsMalformedSamples(t *testing.T) {
	packet := func(label string, value uint64) metricsports.MetricSampleDTO {
		return queryIntSample("narumq_packets_received_total", value, metricsports.MetricLabelDTO{Name: "packet_type", Value: label})
	}
	counter := func(samples ...metricsports.MetricSampleDTO) metricsports.MetricFamilyDTO {
		return metricsports.MetricFamilyDTO{Descriptor: descriptorForTest("narumq_packets_received_total"), Samples: samples}
	}
	gauge := func(samples ...metricsports.MetricSampleDTO) metricsports.MetricFamilyDTO {
		return metricsports.MetricFamilyDTO{Descriptor: descriptorForTest("narumq_connections_active"), Samples: samples}
	}
	single := func(family metricsports.MetricFamilyDTO) metricsports.MetricsSnapshotDTO {
		return metricsports.MetricsSnapshotDTO{Families: []metricsports.MetricFamilyDTO{family}}
	}
	withValue := func(sample metricsports.MetricSampleDTO, value metricsports.MetricValueDTO) metricsports.MetricSampleDTO {
		sample.Value = value
		return sample
	}
	unknown := counter()
	unknown.Descriptor.Name = "narumq_made_up_total"
	relabelled := counter()
	relabelled.Descriptor.LabelNames = []string{"type"}
	cases := []struct {
		name     string
		snapshot metricsports.MetricsSnapshotDTO
		wantErr  string
	}{
		{"family absent from catalog", single(unknown), "metric descriptor does not match catalog"},
		{"descriptor labels differ from catalog", single(relabelled), "metric descriptor does not match catalog"},
		{"duplicate family", metricsports.MetricsSnapshotDTO{Families: []metricsports.MetricFamilyDTO{counter(), counter()}}, "duplicate metric family"},
		{"duplicate sample", single(counter(packet("CONNECT", 1), packet("CONNECT", 2))), "duplicate metric sample"},
		{"sample name differs from family", single(counter(queryIntSample("narumq_packets_sent_total", 1, metricsports.MetricLabelDTO{Name: "packet_type", Value: "CONNECT"}))), "metric sample name does not match family"},
		{"label missing", single(counter(queryIntSample("narumq_packets_received_total", 1))), "metric labels do not match descriptor"},
		{"label has the wrong name", single(counter(queryIntSample("narumq_packets_received_total", 1, metricsports.MetricLabelDTO{Name: "type", Value: "CONNECT"}))), "metric labels do not match descriptor"},
		{"unexpected extra label", single(counter(queryIntSample("narumq_packets_received_total", 1,
			metricsports.MetricLabelDTO{Name: "packet_type", Value: "CONNECT"}, metricsports.MetricLabelDTO{Name: "extra", Value: "x"}))), "metric labels do not match descriptor"},
		{"duplicate label name", single(counter(queryIntSample("narumq_packets_received_total", 1,
			metricsports.MetricLabelDTO{Name: "packet_type", Value: "CONNECT"}, metricsports.MetricLabelDTO{Name: "packet_type", Value: "PUBLISH"}))), "duplicate metric label name"},
		{"label value is not UTF-8", single(counter(packet("\xff", 1))), "invalid metric label value"},
		{"counter with float kind", single(counter(withValue(packet("CONNECT", 0), metricsports.MetricValueDTO{Kind: metricsports.MetricValueFloat, Float64: 1}))), "metric value kind does not match family"},
		{"counter with stray float payload", single(counter(withValue(packet("CONNECT", 1), metricsports.MetricValueDTO{Kind: metricsports.MetricValueInteger, Uint64: 1, Float64: 0.5}))), "invalid integer metric representation"},
		{"gauge without value kind", single(gauge(withValue(queryIntSample("narumq_connections_active", 1), metricsports.MetricValueDTO{Uint64: 1}))), "invalid gauge value kind"},
		{"integer gauge with stray float payload", single(gauge(withValue(queryIntSample("narumq_connections_active", 1), metricsports.MetricValueDTO{Kind: metricsports.MetricValueInteger, Uint64: 1, Float64: 1}))), "invalid integer metric representation"},
		{"float gauge with stray integer payload", single(gauge(withValue(queryFloatSample("narumq_connections_active", 1), metricsports.MetricValueDTO{Kind: metricsports.MetricValueFloat, Uint64: 1, Float64: 1}))), "invalid floating-point metric representation"},
		{"float gauge is NaN", single(gauge(queryFloatSample("narumq_connections_active", math.NaN()))), "invalid floating-point metric representation"},
		{"float gauge is infinite", single(gauge(queryFloatSample("narumq_connections_active", math.Inf(1)))), "invalid floating-point metric representation"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			families, err := metricFamiliesFromSnapshot(test.snapshot)
			if err == nil || err.Error() != test.wantErr || families != nil {
				t.Fatalf("metricFamiliesFromSnapshot() = %v, %v; want error %q", families, err, test.wantErr)
			}
		})
	}
}

func histBucket(family, boundary string, count uint64, extra ...metricsports.MetricLabelDTO) metricsports.MetricSampleDTO {
	labels := append(append([]metricsports.MetricLabelDTO(nil), extra...), metricsports.MetricLabelDTO{Name: "le", Value: boundary})
	return queryIntSample(family+"_bucket", count, labels...)
}

func histCount(family string, count uint64, labels ...metricsports.MetricLabelDTO) metricsports.MetricSampleDTO {
	return queryIntSample(family+"_count", count, labels...)
}

func histSum(family string, sum float64, labels ...metricsports.MetricLabelDTO) metricsports.MetricSampleDTO {
	return queryFloatSample(family+"_sum", sum, labels...)
}

// Traceability: PRD API-03; docs/specs/management-api-and-observability.md §7.2.
// Scenarios: management-api-and-observability.md:MGT-SCN-027
func TestMetricFamiliesFromSnapshotRejectsInconsistentHistograms(t *testing.T) {
	const latency = "narumq_publish_latency_seconds"
	histogram := func(samples ...metricsports.MetricSampleDTO) metricsports.MetricsSnapshotDTO {
		return metricsports.MetricsSnapshotDTO{Families: []metricsports.MetricFamilyDTO{{Descriptor: descriptorForTest(latency), Samples: samples}}}
	}
	stage := func(name string) metricsports.MetricLabelDTO {
		return metricsports.MetricLabelDTO{Name: "stage", Value: name}
	}
	staged := func(samples ...metricsports.MetricSampleDTO) metricsports.MetricsSnapshotDTO {
		return metricsports.MetricsSnapshotDTO{Families: []metricsports.MetricFamilyDTO{{Descriptor: descriptorForTest("narumq_raft_stage_duration_seconds"), Samples: samples}}}
	}
	cases := []struct {
		name     string
		snapshot metricsports.MetricsSnapshotDTO
		wantErr  string
	}{
		{"invalid sample name", histogram(queryIntSample(latency+"_total", 1)), "invalid histogram sample name"},
		{"count missing", histogram(histBucket(latency, "+Inf", 1), histSum(latency, 0.5)), "incomplete histogram series"},
		{"sum missing", histogram(histBucket(latency, "+Inf", 1), histCount(latency, 1)), "incomplete histogram series"},
		{"buckets missing", histogram(histSum(latency, 0.5), histCount(latency, 1)), "incomplete histogram series"},
		{"+Inf bucket differs from count", histogram(histBucket(latency, "+Inf", 2), histSum(latency, 0.5), histCount(latency, 1)), "histogram +Inf bucket does not match count"},
		{"+Inf bucket missing", histogram(histBucket(latency, "0.5", 1), histSum(latency, 0.5), histCount(latency, 1)), "histogram +Inf bucket does not match count"},
		{"equal boundaries", histogram(histBucket(latency, "0.5", 1), histBucket(latency, "0.50", 1), histBucket(latency, "+Inf", 1), histSum(latency, 0.5), histCount(latency, 1)),
			"histogram bucket boundaries are not strictly increasing"},
		{"cumulative count decreases", histogram(histBucket(latency, "0.1", 5), histBucket(latency, "0.5", 3), histBucket(latency, "+Inf", 3), histSum(latency, 0.5), histCount(latency, 3)),
			"histogram bucket counts decreased"},
		{"boundary is not a number", histogram(histBucket(latency, "abc", 1), histBucket(latency, "+Inf", 1), histSum(latency, 0.5), histCount(latency, 1)), "invalid histogram bucket boundary"},
		{"boundary is NaN", histogram(histBucket(latency, "NaN", 1), histBucket(latency, "+Inf", 1), histSum(latency, 0.5), histCount(latency, 1)), "invalid histogram bucket boundary"},
		{"finite boundary spelled Inf", histogram(histBucket(latency, "Inf", 1), histBucket(latency, "+Inf", 1), histSum(latency, 0.5), histCount(latency, 1)), "invalid histogram bucket boundary"},
		{"bucket schemas differ between series", staged(
			histBucket("narumq_raft_stage_duration_seconds", "0.1", 1, stage("append")), histBucket("narumq_raft_stage_duration_seconds", "+Inf", 1, stage("append")),
			histSum("narumq_raft_stage_duration_seconds", 0.1, stage("append")), histCount("narumq_raft_stage_duration_seconds", 1, stage("append")),
			histBucket("narumq_raft_stage_duration_seconds", "0.5", 1, stage("apply")), histBucket("narumq_raft_stage_duration_seconds", "+Inf", 1, stage("apply")),
			histSum("narumq_raft_stage_duration_seconds", 0.1, stage("apply")), histCount("narumq_raft_stage_duration_seconds", 1, stage("apply"))),
			"histogram bucket schemas differ"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			families, err := metricFamiliesFromSnapshot(test.snapshot)
			if err == nil || err.Error() != test.wantErr || families != nil {
				t.Fatalf("metricFamiliesFromSnapshot() = %v, %v; want error %q", families, err, test.wantErr)
			}
		})
	}

	valid := staged(
		histBucket("narumq_raft_stage_duration_seconds", "0.1", 1, stage("append")), histBucket("narumq_raft_stage_duration_seconds", "+Inf", 2, stage("append")),
		histSum("narumq_raft_stage_duration_seconds", 0.3, stage("append")), histCount("narumq_raft_stage_duration_seconds", 2, stage("append")),
		histBucket("narumq_raft_stage_duration_seconds", "0.1", 0, stage("apply")), histBucket("narumq_raft_stage_duration_seconds", "+Inf", 1, stage("apply")),
		histSum("narumq_raft_stage_duration_seconds", 0.7, stage("apply")), histCount("narumq_raft_stage_duration_seconds", 1, stage("apply")))
	if families, err := metricFamiliesFromSnapshot(valid); err != nil || len(families) != 1 || len(families[0].Samples) != 8 {
		t.Fatalf("consistent labelled histogram: families=%+v err=%v", families, err)
	}
}

func renderSample(sample observationports.MetricSampleDTO) string {
	labels := make([]string, 0, len(sample.Labels))
	for _, label := range sample.Labels {
		labels = append(labels, label.Name+"="+label.Value)
	}
	return sample.Name + "{" + strings.Join(labels, ",") + "}=" + sample.ValueKind + ":" + sample.Value
}

// Scenarios: management-api-and-observability.md:MGT-SCN-023
func TestMetricFamiliesFromSnapshotNormalizesOrderAndExactValues(t *testing.T) {
	const latency = "narumq_publish_latency_seconds"
	snapshot := metricsports.MetricsSnapshotDTO{Families: []metricsports.MetricFamilyDTO{
		{Descriptor: descriptorForTest(latency), Samples: []metricsports.MetricSampleDTO{
			histCount(latency, 3), histSum(latency, 1.25), histBucket(latency, "0.5", 2), histBucket(latency, "+Inf", 3), histBucket(latency, "0.1", 1),
		}},
		{Descriptor: descriptorForTest("narumq_packets_received_total"), Samples: []metricsports.MetricSampleDTO{
			queryIntSample("narumq_packets_received_total", math.MaxUint64, metricsports.MetricLabelDTO{Name: "packet_type", Value: "PUBLISH"}),
			queryIntSample("narumq_packets_received_total", 0, metricsports.MetricLabelDTO{Name: "packet_type", Value: "CONNECT"}),
		}},
		{Descriptor: descriptorForTest("narumq_connections_total"), Samples: []metricsports.MetricSampleDTO{
			queryIntSample("narumq_connections_total", 4,
				metricsports.MetricLabelDTO{Name: "transport", Value: "tcp"}, metricsports.MetricLabelDTO{Name: "protocol", Value: "5.0"}, metricsports.MetricLabelDTO{Name: "result", Value: "success"}),
		}},
		{Descriptor: descriptorForTest("narumq_connections_active"), Samples: []metricsports.MetricSampleDTO{queryFloatSample("narumq_connections_active", 0.5)}},
	}}
	families, err := metricFamiliesFromSnapshot(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, family := range families {
		names = append(names, family.Name)
	}
	wantNames := []string{"narumq_connections_active", "narumq_connections_total", "narumq_packets_received_total", latency}
	if strings.Join(names, ",") != strings.Join(wantNames, ",") {
		t.Fatalf("family order = %v, want %v", names, wantNames)
	}
	render := func(family observationports.MetricFamilyDTO) []string {
		var rendered []string
		for _, sample := range family.Samples {
			rendered = append(rendered, renderSample(sample))
		}
		return rendered
	}
	expect := map[string][]string{
		"narumq_connections_active":     {"narumq_connections_active{}=float:0.5"},
		"narumq_connections_total":      {"narumq_connections_total{protocol=5.0,result=success,transport=tcp}=integer:4"},
		"narumq_packets_received_total": {"narumq_packets_received_total{packet_type=CONNECT}=integer:0", "narumq_packets_received_total{packet_type=PUBLISH}=integer:18446744073709551615"},
		latency:                         {latency + "_bucket{le=+Inf}=integer:3", latency + "_bucket{le=0.1}=integer:1", latency + "_bucket{le=0.5}=integer:2", latency + "_count{}=integer:3", latency + "_sum{}=float:1.25"},
	}
	for _, family := range families {
		if got, want := render(family), expect[family.Name]; strings.Join(got, "\n") != strings.Join(want, "\n") {
			t.Fatalf("%s samples = %v, want %v", family.Name, got, want)
		}
	}
}

// Scenarios: management-api-and-observability.md:MGT-SCN-023
func TestCombineMetricFamiliesRejectsInvalidInputsAndInconsistentNodes(t *testing.T) {
	ok := func(node string, families ...observationports.MetricFamilyDTO) observationports.NodeMetricsDataDTO {
		return observationports.NodeMetricsDataDTO{NodeID: node, QueryStatus: observationports.QueryStatusOK, Families: families}
	}
	active := func(kind, value string) observationports.MetricFamilyDTO {
		return testMetricFamily("narumq_connections_active", metricSample("narumq_connections_active", nil, kind, value))
	}
	t.Run("node mode needs exactly one successful result", func(t *testing.T) {
		unreachable := observationports.NodeMetricsDataDTO{NodeID: "1", QueryStatus: observationports.QueryStatusUnreachable}
		for name, results := range map[string][]observationports.NodeMetricsDataDTO{
			"none": nil, "two": {ok("1"), ok("2")}, "failed": {unreachable},
		} {
			if _, err := combineMetricFamilies(results, observationports.MetricsModeNode); err == nil {
				t.Fatalf("node mode accepted %s results", name)
			}
		}
	})
	t.Run("node mode copies instead of aliasing the input", func(t *testing.T) {
		input := ok("1", testMetricFamily("narumq_packets_received_total",
			metricSample("narumq_packets_received_total", []observationports.MetricLabelDTO{{Name: "packet_type", Value: "CONNECT"}}, "integer", "1")))
		output, err := combineMetricFamilies([]observationports.NodeMetricsDataDTO{input}, observationports.MetricsModeNode)
		if err != nil || len(output) != 1 {
			t.Fatalf("combine: %v %v", output, err)
		}
		output[0].LabelNames[0] = "mutated"
		output[0].Samples[0].Labels[0].Value = "mutated"
		if input.Families[0].LabelNames[0] != "packet_type" || input.Families[0].Samples[0].Labels[0].Value != "CONNECT" {
			t.Fatalf("node mode result aliases the input: %+v", input.Families[0])
		}
	})
	t.Run("result without node ID", func(t *testing.T) {
		_, err := combineMetricFamilies([]observationports.NodeMetricsDataDTO{ok("", active("integer", "1"))}, observationports.MetricsModeAllNodesAggregated)
		if err == nil || err.Error() != "metric result has no node ID" {
			t.Fatalf("error = %v", err)
		}
	})
	t.Run("descriptor differs across nodes", func(t *testing.T) {
		second := active("integer", "1")
		second.Help = "a different help text"
		_, err := combineMetricFamilies([]observationports.NodeMetricsDataDTO{ok("1", active("integer", "1")), ok("2", second)}, observationports.MetricsModeAllNodesAggregated)
		if err == nil || err.Error() != "metric descriptors differ across nodes" {
			t.Fatalf("error = %v", err)
		}
	})
	t.Run("unsupported gauge aggregation", func(t *testing.T) {
		first, second := active("integer", "1"), active("integer", "2")
		first.Aggregation, second.Aggregation = "median", "median"
		_, err := combineMetricFamilies([]observationports.NodeMetricsDataDTO{ok("1", first), ok("2", second)}, observationports.MetricsModeAllNodesAggregated)
		if err == nil || err.Error() != "unsupported gauge aggregation" {
			t.Fatalf("error = %v", err)
		}
	})
	t.Run("unsuccessful nodes contribute nothing", func(t *testing.T) {
		failed := observationports.NodeMetricsDataDTO{NodeID: "2", QueryStatus: observationports.QueryStatusError, Families: []observationports.MetricFamilyDTO{active("integer", "100")}}
		output, err := combineMetricFamilies([]observationports.NodeMetricsDataDTO{ok("1", active("integer", "1")), failed}, observationports.MetricsModeAllNodesAggregated)
		if err != nil || len(findFamilyForTest(t, output, "narumq_connections_active").Samples) != 1 ||
			findFamilyForTest(t, output, "narumq_connections_active").Samples[0].Value != "1" {
			t.Fatalf("failed node contributed: %+v err=%v", output, err)
		}
	})
}

// Scenarios: management-api-and-observability.md:MGT-SCN-023
func TestCombineMetricFamiliesReducesGaugesPerLabelSet(t *testing.T) {
	ok := func(node string, families ...observationports.MetricFamilyDTO) observationports.NodeMetricsDataDTO {
		return observationports.NodeMetricsDataDTO{NodeID: node, QueryStatus: observationports.QueryStatusOK, Families: families}
	}
	value := func(family, kind, text string) observationports.NodeMetricsDataDTO {
		return ok("x", testMetricFamily(family, metricSample(family, nil, kind, text)))
	}
	reduce := func(family, firstKind, first, secondKind, second string) (string, error) {
		results := []observationports.NodeMetricsDataDTO{value(family, firstKind, first), value(family, secondKind, second)}
		results[0].NodeID, results[1].NodeID = "1", "2"
		output, err := combineMetricFamilies(results, observationports.MetricsModeAllNodesAggregated)
		if err != nil {
			return "", err
		}
		samples := findFamilyForTest(t, output, family).Samples
		if len(samples) != 1 {
			return "", fmt.Errorf("reduced to %d samples", len(samples))
		}
		return samples[0].Value, nil
	}
	for _, test := range []struct {
		name                             string
		family, kind, first, second, out string
	}{
		{"integer maximum, larger first", "narumq_retained_messages", "integer", "7", "3", "7"},
		{"integer maximum, larger second", "narumq_retained_messages", "integer", "3", "7", "7"},
		{"float maximum, larger first", "narumq_cluster_commit_backlog_oldest_seconds", "float", "1.5", "0.5", "1.5"},
		{"float maximum, larger second", "narumq_cluster_commit_backlog_oldest_seconds", "float", "0.5", "1.5", "1.5"},
		{"float sum", "narumq_connections_active", "float", "1.5", "2.25", "3.75"},
		{"integer sum", "narumq_connections_active", "integer", "40", "2", "42"},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := reduce(test.family, test.kind, test.first, test.kind, test.second)
			if err != nil || got != test.out {
				t.Fatalf("reduce = %q, %v; want %q", got, err, test.out)
			}
		})
	}
	for _, test := range []struct {
		name                        string
		family, kind, first, second string
		wantErr                     string
	}{
		{name: "sum integer left malformed", family: "narumq_connections_active", kind: "integer", first: "abc", second: "1", wantErr: "invalid integer gauge value"},
		{name: "sum integer right malformed", family: "narumq_connections_active", kind: "integer", first: "1", second: "abc", wantErr: "invalid integer gauge value"},
		{name: "sum float left malformed", family: "narumq_connections_active", kind: "float", first: "x", second: "1", wantErr: "invalid floating-point gauge value"},
		{name: "sum float right malformed", family: "narumq_connections_active", kind: "float", first: "1", second: "x", wantErr: "floating-point gauge sum is invalid"},
		{name: "sum float overflows to infinity", family: "narumq_connections_active", kind: "float", first: "1.7e308", second: "1.7e308", wantErr: "floating-point gauge sum is invalid"},
		{name: "sum integer overflows", family: "narumq_connections_active", kind: "integer", first: "18446744073709551615", second: "1", wantErr: "integer overflow"},
		{name: "max integer left malformed", family: "narumq_retained_messages", kind: "integer", first: "abc", second: "1", wantErr: "invalid integer gauge value"},
		{name: "max integer right malformed", family: "narumq_retained_messages", kind: "integer", first: "1", second: "abc", wantErr: "invalid integer gauge value"},
		{name: "max float left malformed", family: "narumq_cluster_commit_backlog_oldest_seconds", kind: "float", first: "x", second: "1", wantErr: "invalid floating-point gauge value"},
		{name: "max float right malformed", family: "narumq_cluster_commit_backlog_oldest_seconds", kind: "float", first: "1", second: "x", wantErr: "invalid floating-point gauge value"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := reduce(test.family, test.kind, test.first, test.kind, test.second); err == nil || err.Error() != test.wantErr {
				t.Fatalf("reduce error = %v, want %q", err, test.wantErr)
			}
		})
	}
	t.Run("kinds differ", func(t *testing.T) {
		if _, err := reduce("narumq_connections_active", "integer", "1", "float", "1.5"); err == nil || err.Error() != "gauge value kinds differ across nodes" {
			t.Fatalf("error = %v", err)
		}
	})
	t.Run("each label set is reduced independently", func(t *testing.T) {
		persistent := func(value string) []observationports.MetricLabelDTO {
			return []observationports.MetricLabelDTO{{Name: "persistent", Value: value}}
		}
		first := ok("1", testMetricFamily("narumq_sessions_active",
			metricSample("narumq_sessions_active", persistent("true"), "integer", "2"), metricSample("narumq_sessions_active", persistent("false"), "integer", "1")))
		second := ok("2", testMetricFamily("narumq_sessions_active",
			metricSample("narumq_sessions_active", persistent("true"), "integer", "3")))
		output, err := combineMetricFamilies([]observationports.NodeMetricsDataDTO{first, second}, observationports.MetricsModeAllNodesAggregated)
		if err != nil {
			t.Fatal(err)
		}
		samples := findFamilyForTest(t, output, "narumq_sessions_active").Samples
		var rendered []string
		for _, sample := range samples {
			rendered = append(rendered, renderSample(sample))
		}
		want := []string{"narumq_sessions_active{persistent=false}=integer:1", "narumq_sessions_active{persistent=true}=integer:5"}
		if strings.Join(rendered, "|") != strings.Join(want, "|") {
			t.Fatalf("per-label reduction = %v, want %v", rendered, want)
		}
	})
}
