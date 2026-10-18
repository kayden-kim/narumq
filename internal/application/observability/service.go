package observability

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"math"
	"math/bits"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	clockports "narumq/internal/application/ports/clock"
	managementports "narumq/internal/application/ports/management"
	observationports "narumq/internal/application/ports/observability"
)

const (
	queryTimeout     = 5 * time.Second
	queryWorkers     = 8
	defaultPageLimit = 50
	maxPageLimit     = 1000
)

var errInvalidContext = errors.New("observability query context is required")
var errInvalidMembership = errors.New("observability membership snapshot is invalid")
var errInvalidNodeSnapshot = errors.New("node snapshot is invalid")

// Service implements bounded node and cluster observation queries.
type Service struct {
	cluster observationports.ClusterSnapshotSource
	local   observationports.LocalSnapshotSource
	peers   observationports.PeerSnapshotReader
	clock   clockports.Clock
}

// NewService creates a query service with request-local snapshot sources.
func NewService(
	cluster observationports.ClusterSnapshotSource,
	local observationports.LocalSnapshotSource,
	peers observationports.PeerSnapshotReader,
	clock clockports.Clock,
) (*Service, error) {
	if cluster == nil {
		return nil, errors.New("cluster snapshot source is required")
	}
	if local == nil {
		return nil, errors.New("local snapshot source is required")
	}
	if clock == nil {
		return nil, errors.New("observability clock is required")
	}
	return &Service{cluster: cluster, local: local, peers: peers, clock: clock}, nil
}

var _ observationports.QueryService = (*Service)(nil)

// GetCluster returns cluster metadata and direct snapshots from current members.
func (service *Service) GetCluster(ctx context.Context) (observationports.ClusterDataDTO, error) {
	if ctx == nil {
		return observationports.ClusterDataDTO{}, errInvalidContext
	}
	queryContext, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()

	cluster, err := service.cluster.ClusterSnapshot(queryContext)
	if err != nil {
		if ctx.Err() != nil {
			return observationports.ClusterDataDTO{}, ctx.Err()
		}
		return observationports.ClusterDataDTO{}, err
	}
	cluster, err = orderClusterSnapshot(cluster)
	if err != nil {
		return observationports.ClusterDataDTO{}, err
	}
	quorumAvailable, quorumErr := service.cluster.FreshQuorumAvailable(queryContext)
	if quorumErr != nil {
		if ctx.Err() != nil {
			return observationports.ClusterDataDTO{}, ctx.Err()
		}
		quorumAvailable = false
	}
	nodes, err := service.queryNodes(queryContext, ctx, cluster)
	if err != nil {
		return observationports.ClusterDataDTO{}, err
	}
	return service.buildClusterData(cluster, nodes, quorumAvailable), nil
}

// GetNode returns one member's direct node snapshot or sanitized query status.
func (service *Service) GetNode(ctx context.Context, nodeID string) (observationports.NodeDataDTO, error) {
	if ctx == nil {
		return observationports.NodeDataDTO{}, errInvalidContext
	}
	queryContext, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()
	cluster, err := service.cluster.ClusterSnapshot(queryContext)
	if err != nil {
		if ctx.Err() != nil {
			return observationports.NodeDataDTO{}, ctx.Err()
		}
		return observationports.NodeDataDTO{}, err
	}
	cluster, err = orderClusterSnapshot(cluster)
	if err != nil {
		return observationports.NodeDataDTO{}, err
	}
	member, found := findMember(cluster.Members, nodeID)
	if !found {
		return observationports.NodeDataDTO{}, observationports.ErrNodeNotFound
	}
	return service.queryNode(queryContext, ctx, cluster.LocalNodeID, member)
}

// ListNodes queries the membership snapshot before applying state filtering and pagination.
func (service *Service) ListNodes(ctx context.Context, filter observationports.NodeFilter) ([]observationports.NodeDataDTO, *managementports.PageDTO, error) {
	if ctx == nil {
		return nil, nil, errInvalidContext
	}
	if filter.State != "" && filter.State != observationports.NodeStateJoining &&
		filter.State != observationports.NodeStateReady && filter.State != observationports.NodeStateDraining &&
		filter.State != observationports.NodeStateSafeToStop {
		return nil, nil, errors.New("invalid node state filter")
	}
	limit := filter.Limit
	if limit == 0 {
		limit = defaultPageLimit
	}
	if limit < 1 || limit > maxPageLimit {
		return nil, nil, observationports.ErrInvalidPageSize
	}
	offset, err := decodeCursor(filter.Cursor)
	if err != nil {
		return nil, nil, observationports.ErrInvalidCursor
	}
	queryContext, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()
	cluster, err := service.cluster.ClusterSnapshot(queryContext)
	if err != nil {
		if ctx.Err() != nil {
			return nil, nil, ctx.Err()
		}
		return nil, nil, err
	}
	cluster, err = orderClusterSnapshot(cluster)
	if err != nil {
		return nil, nil, err
	}
	nodes, err := service.queryNodes(queryContext, ctx, cluster)
	if err != nil {
		return nil, nil, err
	}
	filtered := make([]observationports.NodeDataDTO, 0, len(nodes))
	for _, node := range nodes {
		if filter.State == "" || (node.QueryStatus == observationports.QueryStatusOK && node.State != nil && *node.State == filter.State) {
			filtered = append(filtered, node)
		}
	}
	start := min(offset, len(filtered))
	end := min(start+limit, len(filtered))
	page := &managementports.PageDTO{HasMore: end < len(filtered)}
	if page.HasMore {
		cursor := encodeCursor(end)
		page.NextCursor = &cursor
	}
	return filtered[start:end], page, nil
}

// GetNodeMetrics returns one member's typed metric snapshot or sanitized status.
func (service *Service) GetNodeMetrics(ctx context.Context, nodeID string) (observationports.NodeMetricsDataDTO, error) {
	if ctx == nil {
		return observationports.NodeMetricsDataDTO{}, errInvalidContext
	}
	queryContext, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()
	cluster, err := service.cluster.ClusterSnapshot(queryContext)
	if err != nil {
		if ctx.Err() != nil {
			return observationports.NodeMetricsDataDTO{}, ctx.Err()
		}
		return observationports.NodeMetricsDataDTO{}, err
	}
	cluster, err = orderClusterSnapshot(cluster)
	if err != nil {
		return observationports.NodeMetricsDataDTO{}, err
	}
	member, found := findMember(cluster.Members, nodeID)
	if !found {
		return observationports.NodeMetricsDataDTO{}, observationports.ErrNodeNotFound
	}
	return service.queryNodeMetrics(queryContext, ctx, cluster.LocalNodeID, member)
}

// CollectMetrics returns a node-local or cluster metric collection.
func (service *Service) CollectMetrics(ctx context.Context, mode observationports.MetricsMode) (observationports.MetricsCollectionDataDTO, error) {
	if ctx == nil {
		return observationports.MetricsCollectionDataDTO{}, errInvalidContext
	}
	if mode != observationports.MetricsModeNode && mode != observationports.MetricsModeAllNodesUnaggregated &&
		mode != observationports.MetricsModeAllNodesAggregated {
		return observationports.MetricsCollectionDataDTO{}, errors.New("invalid metrics mode")
	}
	queryContext, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()
	cluster, err := service.cluster.ClusterSnapshot(queryContext)
	if err != nil {
		if ctx.Err() != nil {
			return observationports.MetricsCollectionDataDTO{}, ctx.Err()
		}
		return observationports.MetricsCollectionDataDTO{}, err
	}
	cluster, err = orderClusterSnapshot(cluster)
	if err != nil {
		return observationports.MetricsCollectionDataDTO{}, err
	}
	if mode == observationports.MetricsModeNode {
		member, found := findMember(cluster.Members, cluster.LocalNodeID)
		if !found {
			return observationports.MetricsCollectionDataDTO{}, errInvalidMembership
		}
		result, err := service.queryNodeMetrics(queryContext, ctx, cluster.LocalNodeID, member)
		if err != nil {
			return observationports.MetricsCollectionDataDTO{}, err
		}
		return service.buildMetricsCollection(mode, []observationports.MemberDTO{member}, []observationports.NodeMetricsDataDTO{result})
	}
	results, err := service.queryMetrics(queryContext, ctx, cluster)
	if err != nil {
		return observationports.MetricsCollectionDataDTO{}, err
	}
	return service.buildMetricsCollection(mode, cluster.Members, results)
}

func (service *Service) buildClusterData(
	cluster observationports.ClusterSnapshotDTO,
	nodes []observationports.NodeDataDTO,
	quorumAvailable bool,
) observationports.ClusterDataDTO {
	data := observationports.ClusterDataDTO{
		ClusterID: cluster.ClusterID, ReleaseVersion: cluster.ReleaseVersion,
		MemberCount: 0, QuorumSize: cluster.QuorumSize,
		ReplicationFactor: cluster.ReplicationFactor, WriteQuorum: cluster.WriteQuorum,
		QuorumAvailable: quorumAvailable, LeaderNodeID: cluster.LeaderNodeID,
		NodeQueryComplete: true, ObservedAt: service.clock.Now().UTC(),
		UnderReplicated: cluster.UnderReplicated, SubscriptionCount: cluster.SubscriptionCount,
	}
	connectionTotal, sessionTotal := uint64(0), uint64(0)
	countsKnown := true
	for index, member := range cluster.Members {
		if member.Phase == observationports.MembershipPhaseRetired {
			continue
		}
		data.MemberCount++
		node := nodes[index]
		if node.QueryStatus != observationports.QueryStatusOK || node.State == nil {
			data.NodesByState.Unknown++
			data.NodeQueryComplete = false
			countsKnown = false
			continue
		}
		switch *node.State {
		case observationports.NodeStateJoining:
			data.NodesByState.Joining++
		case observationports.NodeStateReady:
			data.NodesByState.Ready++
		case observationports.NodeStateDraining:
			data.NodesByState.Draining++
		case observationports.NodeStateSafeToStop:
			data.NodesByState.SafeToStop++
		default:
			data.NodesByState.Unknown++
			data.NodeQueryComplete = false
			countsKnown = false
		}
		if node.ConnectionCount == nil || node.SessionCount == nil {
			countsKnown = false
			continue
		}
		var carry uint64
		connectionTotal, carry = bits.Add64(connectionTotal, *node.ConnectionCount, 0)
		if carry != 0 {
			countsKnown = false
			continue
		}
		sessionTotal, carry = bits.Add64(sessionTotal, *node.SessionCount, 0)
		if carry != 0 {
			countsKnown = false
		}
	}
	if countsKnown {
		data.ConnectionCount = &connectionTotal
		data.SessionCount = &sessionTotal
	}
	return data
}

func (service *Service) queryNodes(
	queryContext context.Context,
	callerContext context.Context,
	cluster observationports.ClusterSnapshotDTO,
) ([]observationports.NodeDataDTO, error) {
	members := cluster.Members
	nodes := make([]observationports.NodeDataDTO, len(members))
	if len(members) == 0 {
		return nodes, nil
	}
	workerCount := min(queryWorkers, len(members))
	jobs := make(chan int)
	var workers sync.WaitGroup
	workers.Add(workerCount)
	for range workerCount {
		go func() {
			defer workers.Done()
			for index := range jobs {
				nodes[index], _ = service.queryNode(queryContext, callerContext, cluster.LocalNodeID, members[index])
			}
		}()
	}
	for index := range members {
		jobs <- index
	}
	close(jobs)
	workers.Wait()
	if err := callerContext.Err(); err != nil {
		return nil, err
	}
	for index := range nodes {
		if nodes[index].NodeID == "" {
			nodes[index] = unavailableNode(members[index], observationports.QueryErrorTimeout)
		}
	}
	return nodes, nil
}

func (service *Service) queryMetrics(
	queryContext context.Context,
	callerContext context.Context,
	cluster observationports.ClusterSnapshotDTO,
) ([]observationports.NodeMetricsDataDTO, error) {
	members := cluster.Members
	results := make([]observationports.NodeMetricsDataDTO, len(members))
	if len(members) == 0 {
		return results, nil
	}
	workerCount := min(queryWorkers, len(members))
	jobs := make(chan int)
	var workers sync.WaitGroup
	workers.Add(workerCount)
	for range workerCount {
		go func() {
			defer workers.Done()
			for index := range jobs {
				results[index], _ = service.queryNodeMetrics(queryContext, callerContext, cluster.LocalNodeID, members[index])
			}
		}()
	}
	for index := range members {
		jobs <- index
	}
	close(jobs)
	workers.Wait()
	if err := callerContext.Err(); err != nil {
		return nil, err
	}
	for index := range results {
		if results[index].NodeID == "" {
			results[index] = unavailableMetrics(members[index], observationports.QueryErrorTimeout)
		}
	}
	return results, nil
}

func (service *Service) queryNode(
	queryContext context.Context,
	callerContext context.Context,
	localNodeID string,
	member observationports.MemberDTO,
) (observationports.NodeDataDTO, error) {
	if err := callerContext.Err(); err != nil {
		return observationports.NodeDataDTO{}, err
	}
	if member.Phase == observationports.MembershipPhaseRetired {
		return unavailableNode(member, ""), nil
	}
	if member.Phase == observationports.MembershipPhaseStaged {
		return unavailableNode(member, observationports.QueryErrorNodeNotRunning), nil
	}
	var snapshot observationports.NodeSnapshotDTO
	var err error
	if member.NodeID == localNodeID {
		snapshot, err = service.local.CaptureNodeSnapshot(queryContext)
	} else if service.peers == nil {
		err = &observationports.PeerQueryError{Code: observationports.QueryErrorNodeUnreachable}
	} else {
		snapshot, err = service.peers.QueryNodeSnapshot(queryContext, member.NodeID)
	}
	if err != nil {
		if callerContext.Err() != nil {
			return observationports.NodeDataDTO{}, callerContext.Err()
		}
		return nodeQueryFailure(member, queryErrorCode(err)), nil
	}
	if err := validateNodeSnapshot(snapshot); err != nil {
		return nodeQueryFailure(member, observationports.QueryErrorFailed), nil
	}
	if snapshot.ObservedAt.IsZero() {
		snapshot.ObservedAt = service.clock.Now().UTC()
	}
	return nodeData(member, snapshot), nil
}

func (service *Service) queryNodeMetrics(
	queryContext context.Context,
	callerContext context.Context,
	localNodeID string,
	member observationports.MemberDTO,
) (observationports.NodeMetricsDataDTO, error) {
	if err := callerContext.Err(); err != nil {
		return observationports.NodeMetricsDataDTO{}, err
	}
	if member.Phase == observationports.MembershipPhaseRetired {
		return unavailableMetrics(member, ""), nil
	}
	if member.Phase == observationports.MembershipPhaseStaged {
		return unavailableMetrics(member, observationports.QueryErrorNodeNotRunning), nil
	}
	var snapshot observationports.MetricsSnapshotDTO
	var err error
	if member.NodeID == localNodeID {
		snapshot, err = service.local.CaptureMetricsSnapshot(queryContext)
	} else if service.peers == nil {
		err = &observationports.PeerQueryError{Code: observationports.QueryErrorNodeUnreachable}
	} else {
		snapshot, err = service.peers.QueryMetricsSnapshot(queryContext, member.NodeID)
	}
	if err != nil {
		if callerContext.Err() != nil {
			return observationports.NodeMetricsDataDTO{}, callerContext.Err()
		}
		return metricsQueryFailure(member, queryErrorCode(err)), nil
	}
	families, err := metricFamiliesFromSnapshot(snapshot.Snapshot)
	if err != nil {
		return metricsQueryFailure(member, observationports.QueryErrorFailed), nil
	}
	if snapshot.ObservedAt.IsZero() {
		snapshot.ObservedAt = service.clock.Now().UTC()
	}
	if snapshot.StartedAt.IsZero() {
		return metricsQueryFailure(member, observationports.QueryErrorFailed), nil
	}
	observedAt, startedAt := snapshot.ObservedAt.UTC(), snapshot.StartedAt.UTC()
	return observationports.NodeMetricsDataDTO{
		NodeID: member.NodeID, QueryStatus: observationports.QueryStatusOK,
		ObservedAt: &observedAt, StartedAt: &startedAt, Families: families,
	}, nil
}

func (service *Service) buildMetricsCollection(
	mode observationports.MetricsMode,
	members []observationports.MemberDTO,
	results []observationports.NodeMetricsDataDTO,
) (observationports.MetricsCollectionDataDTO, error) {
	collection := observationports.MetricsCollectionDataDTO{
		Mode: mode, ObservedAt: service.clock.Now().UTC(), Complete: true,
		Nodes: make([]observationports.MetricsNodeStatusDTO, 0, len(members)),
	}
	successfulResults := make([]observationports.NodeMetricsDataDTO, 0, len(results))
	for index, member := range members {
		result := results[index]
		collection.Nodes = append(collection.Nodes, observationports.MetricsNodeStatusDTO{
			NodeID: result.NodeID, QueryStatus: result.QueryStatus, QueryErrorCode: result.QueryErrorCode,
			ObservedAt: result.ObservedAt, StartedAt: result.StartedAt,
		})
		if member.Phase != observationports.MembershipPhaseRetired && result.QueryStatus != observationports.QueryStatusOK {
			collection.Complete = false
		}
		if result.QueryStatus == observationports.QueryStatusOK {
			successfulResults = append(successfulResults, result)
		}
	}
	if mode == observationports.MetricsModeAllNodesAggregated && !collection.Complete {
		return observationports.MetricsCollectionDataDTO{}, observationports.ErrMetricsCollectionIncomplete
	}
	families, err := combineMetricFamilies(successfulResults, mode)
	if err != nil {
		return observationports.MetricsCollectionDataDTO{}, observationports.ErrMetricsAggregationFailed
	}
	collection.Families = families
	return collection, nil
}

func validateClusterSnapshot(snapshot observationports.ClusterSnapshotDTO) error {
	if snapshot.ClusterID == "" || snapshot.ReleaseVersion == "" || snapshot.LocalNodeID == "" ||
		snapshot.QuorumSize < 1 || snapshot.ReplicationFactor < 1 || snapshot.WriteQuorum < 1 ||
		snapshot.UnderReplicated < 0 {
		return errInvalidMembership
	}
	seen := make(map[string]struct{}, len(snapshot.Members))
	for _, member := range snapshot.Members {
		if member.NodeID == "" || !validMembershipPhase(member.Phase) {
			return errInvalidMembership
		}
		if _, exists := seen[member.NodeID]; exists {
			return errInvalidMembership
		}
		seen[member.NodeID] = struct{}{}
	}
	if _, exists := seen[snapshot.LocalNodeID]; !exists {
		return errInvalidMembership
	}
	return nil
}

func orderClusterSnapshot(snapshot observationports.ClusterSnapshotDTO) (observationports.ClusterSnapshotDTO, error) {
	if err := validateClusterSnapshot(snapshot); err != nil {
		return observationports.ClusterSnapshotDTO{}, err
	}
	snapshot.Members = append([]observationports.MemberDTO(nil), snapshot.Members...)
	sortMembers(snapshot.Members)
	return snapshot, nil
}

func validMembershipPhase(phase observationports.MembershipPhase) bool {
	switch phase {
	case observationports.MembershipPhaseStandalone, observationports.MembershipPhaseStaged,
		observationports.MembershipPhaseLearner, observationports.MembershipPhaseCatchingUp,
		observationports.MembershipPhaseVoter, observationports.MembershipPhaseLeaving,
		observationports.MembershipPhaseRetired:
		return true
	default:
		return false
	}
}

func validateNodeSnapshot(snapshot observationports.NodeSnapshotDTO) error {
	if snapshot.UptimeMillis != nil && *snapshot.UptimeMillis < 0 {
		return errInvalidNodeSnapshot
	}
	if snapshot.State != nil && *snapshot.State != observationports.NodeStateJoining &&
		*snapshot.State != observationports.NodeStateReady && *snapshot.State != observationports.NodeStateDraining &&
		*snapshot.State != observationports.NodeStateSafeToStop {
		return errInvalidNodeSnapshot
	}
	if snapshot.Role != nil && *snapshot.Role != "LEADER" && *snapshot.Role != "FOLLOWER" && *snapshot.Role != "LEARNER" {
		return errInvalidNodeSnapshot
	}
	if snapshot.ReplicationHealth != nil && *snapshot.ReplicationHealth != "HEALTHY" &&
		*snapshot.ReplicationHealth != "DEGRADED" && *snapshot.ReplicationHealth != "FAILED" {
		return errInvalidNodeSnapshot
	}
	return nil
}

func nodeData(member observationports.MemberDTO, snapshot observationports.NodeSnapshotDTO) observationports.NodeDataDTO {
	observedAt := snapshot.ObservedAt.UTC()
	return observationports.NodeDataDTO{
		NodeID: member.NodeID, MembershipPhase: member.Phase,
		QueryStatus: observationports.QueryStatusOK, ObservedAt: &observedAt,
		Version: snapshot.Version, UptimeMillis: snapshot.UptimeMillis, State: snapshot.State,
		Ready: snapshot.Ready, ReadinessReason: snapshot.ReadinessReason, Role: snapshot.Role,
		LastAppliedIndex: snapshot.LastAppliedIndex, ConnectionCount: snapshot.ConnectionCount,
		SessionCount: snapshot.SessionCount, StoragePressure: snapshot.StoragePressure,
		ReplicationHealth: snapshot.ReplicationHealth, LastErrorCode: snapshot.LastErrorCode,
	}
}

func unavailableNode(member observationports.MemberDTO, code observationports.QueryErrorCode) observationports.NodeDataDTO {
	status, errorCode := queryStatusForError(code)
	if member.Phase == observationports.MembershipPhaseRetired {
		status, errorCode = observationports.QueryStatusRetired, nil
	}
	return observationports.NodeDataDTO{
		NodeID: member.NodeID, MembershipPhase: member.Phase,
		QueryStatus: status, QueryErrorCode: errorCode,
	}
}

func nodeQueryFailure(member observationports.MemberDTO, code observationports.QueryErrorCode) observationports.NodeDataDTO {
	return unavailableNode(member, code)
}

func unavailableMetrics(member observationports.MemberDTO, code observationports.QueryErrorCode) observationports.NodeMetricsDataDTO {
	status, errorCode := queryStatusForError(code)
	if member.Phase == observationports.MembershipPhaseRetired {
		status, errorCode = observationports.QueryStatusRetired, nil
	}
	return observationports.NodeMetricsDataDTO{
		NodeID: member.NodeID, QueryStatus: status, QueryErrorCode: errorCode,
	}
}

func metricsQueryFailure(member observationports.MemberDTO, code observationports.QueryErrorCode) observationports.NodeMetricsDataDTO {
	return unavailableMetrics(member, code)
}

func queryStatusForError(code observationports.QueryErrorCode) (observationports.QueryStatus, *observationports.QueryErrorCode) {
	if code == "" {
		return observationports.QueryStatusRetired, nil
	}
	codeCopy := code
	switch code {
	case observationports.QueryErrorNodeUnreachable, observationports.QueryErrorTimeout, observationports.QueryErrorNodeNotRunning:
		return observationports.QueryStatusUnreachable, &codeCopy
	case observationports.QueryErrorUnsupported:
		return observationports.QueryStatusUnsupported, &codeCopy
	default:
		return observationports.QueryStatusError, &codeCopy
	}
}

func queryErrorCode(err error) observationports.QueryErrorCode {
	var peerError *observationports.PeerQueryError
	if errors.As(err, &peerError) && peerError.Code != "" {
		return peerError.Code
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return observationports.QueryErrorTimeout
	}
	return observationports.QueryErrorFailed
}

func findMember(members []observationports.MemberDTO, memberID string) (observationports.MemberDTO, bool) {
	for _, member := range members {
		if member.NodeID == memberID {
			return member, true
		}
	}
	return observationports.MemberDTO{}, false
}

func decodeCursor(cursor string) (int, error) {
	if cursor == "" {
		return 0, nil
	}
	decoded, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return 0, err
	}
	value := string(decoded)
	if !strings.HasPrefix(value, "cur:") {
		return 0, errors.New("invalid cursor")
	}
	offset, err := strconv.Atoi(strings.TrimPrefix(value, "cur:"))
	if err != nil || offset < 0 {
		return 0, errors.New("invalid cursor")
	}
	return offset, nil
}

func encodeCursor(offset int) string {
	return base64.RawURLEncoding.EncodeToString([]byte("cur:" + strconv.Itoa(offset)))
}

func sortMembers(members []observationports.MemberDTO) {
	sort.Slice(members, func(i, j int) bool {
		left, leftErr := strconv.ParseUint(members[i].NodeID, 10, 64)
		right, rightErr := strconv.ParseUint(members[j].NodeID, 10, 64)
		if leftErr == nil && rightErr == nil {
			return left < right
		}
		return members[i].NodeID < members[j].NodeID
	})
}

func addUint64(total, value uint64) (uint64, error) {
	sum, carry := bits.Add64(total, value, 0)
	if carry != 0 {
		return 0, fmt.Errorf("integer overflow")
	}
	return sum, nil
}

func finiteFloat(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0)
}
