package observability

import (
	"context"
	"errors"
	"time"

	managementports "narumq/internal/application/ports/management"
	metricsports "narumq/internal/application/ports/metrics"
)

type MembershipPhase string

const (
	MembershipPhaseStandalone MembershipPhase = "STANDALONE"
	MembershipPhaseStaged     MembershipPhase = "STAGED"
	MembershipPhaseLearner    MembershipPhase = "LEARNER"
	MembershipPhaseCatchingUp MembershipPhase = "CATCHING_UP"
	MembershipPhaseVoter      MembershipPhase = "VOTER"
	MembershipPhaseLeaving    MembershipPhase = "LEAVING"
	MembershipPhaseRetired    MembershipPhase = "RETIRED"
)

type QueryStatus string

const (
	QueryStatusOK          QueryStatus = "OK"
	QueryStatusUnreachable QueryStatus = "UNREACHABLE"
	QueryStatusUnsupported QueryStatus = "UNSUPPORTED"
	QueryStatusRetired     QueryStatus = "RETIRED"
	QueryStatusError       QueryStatus = "ERROR"
)

type QueryErrorCode string

const (
	QueryErrorNodeUnreachable  QueryErrorCode = "node_unreachable"
	QueryErrorTimeout          QueryErrorCode = "node_query_timeout"
	QueryErrorUnsupported      QueryErrorCode = "node_query_unsupported"
	QueryErrorBusy             QueryErrorCode = "node_query_busy"
	QueryErrorInvalidResponse  QueryErrorCode = "node_query_invalid_response"
	QueryErrorSnapshotTooLarge QueryErrorCode = "node_snapshot_too_large"
	QueryErrorFailed           QueryErrorCode = "node_query_failed"
	QueryErrorNodeNotRunning   QueryErrorCode = "node_not_running"
)

type NodeState string

const (
	NodeStateJoining    NodeState = "JOINING"
	NodeStateReady      NodeState = "READY"
	NodeStateDraining   NodeState = "DRAINING"
	NodeStateSafeToStop NodeState = "SAFE_TO_STOP"
)

var (
	ErrNodeNotFound                = errors.New("node not found")
	ErrInvalidPageSize             = errors.New("invalid page size")
	ErrInvalidCursor               = errors.New("invalid cursor")
	ErrMetricsCollectionIncomplete = errors.New("metrics collection is incomplete")
	ErrMetricsSnapshotTooLarge     = errors.New("metrics snapshot is too large")
	ErrMetricsAggregationFailed    = errors.New("metrics aggregation failed")
)

// PeerQueryError carries a sanitized peer query failure code across the application port.
type PeerQueryError struct {
	Code QueryErrorCode
}

func (err *PeerQueryError) Error() string {
	if err == nil {
		return "peer query failed"
	}
	return string(err.Code)
}

// MemberDTO contains the committed membership data needed to query one node.
type MemberDTO struct {
	NodeID string
	Phase  MembershipPhase
}

// ClusterSnapshotDTO contains membership and local cluster metadata for one request.
type ClusterSnapshotDTO struct {
	ClusterID         string
	ReleaseVersion    string
	LocalNodeID       string
	Members           []MemberDTO
	QuorumSize        int
	ReplicationFactor int
	WriteQuorum       int
	LeaderNodeID      *string
	UnderReplicated   int
	SubscriptionCount uint64
}

// NodeSnapshotDTO contains measurements made directly by one node.
type NodeSnapshotDTO struct {
	ObservedAt        time.Time
	Version           *string
	UptimeMillis      *int64
	State             *NodeState
	Ready             *bool
	ReadinessReason   *string
	Role              *string
	LastAppliedIndex  *uint64
	ConnectionCount   *uint64
	SessionCount      *uint64
	StoragePressure   *bool
	ReplicationHealth *string
	LastErrorCode     *string
}

// MetricsSnapshotDTO contains one node's typed metrics and reset boundary.
type MetricsSnapshotDTO struct {
	ObservedAt time.Time
	StartedAt  time.Time
	Snapshot   metricsports.MetricsSnapshotDTO
}

// NodeDataDTO is the management API node response and peer snapshot body.
type NodeDataDTO struct {
	NodeID            string          `json:"nodeId"`
	MembershipPhase   MembershipPhase `json:"membershipPhase"`
	QueryStatus       QueryStatus     `json:"queryStatus"`
	QueryErrorCode    *QueryErrorCode `json:"queryErrorCode"`
	ObservedAt        *time.Time      `json:"observedAt"`
	Version           *string         `json:"version"`
	UptimeMillis      *int64          `json:"uptimeMillis"`
	State             *NodeState      `json:"state"`
	Ready             *bool           `json:"ready"`
	ReadinessReason   *string         `json:"readinessReason"`
	Role              *string         `json:"role"`
	LastAppliedIndex  *uint64         `json:"lastAppliedIndex"`
	ConnectionCount   *uint64         `json:"connectionCount"`
	SessionCount      *uint64         `json:"sessionCount"`
	StoragePressure   *bool           `json:"storagePressure"`
	ReplicationHealth *string         `json:"replicationHealth"`
	LastErrorCode     *string         `json:"lastErrorCode"`
}

// ClusterDataDTO is the management API cluster response.
type ClusterDataDTO struct {
	ClusterID         string          `json:"clusterId"`
	ReleaseVersion    string          `json:"releaseVersion"`
	MemberCount       int             `json:"memberCount"`
	QuorumSize        int             `json:"quorumSize"`
	ReplicationFactor int             `json:"replicationFactor"`
	WriteQuorum       int             `json:"writeQuorum"`
	QuorumAvailable   bool            `json:"quorumAvailable"`
	LeaderNodeID      *string         `json:"leaderNodeId"`
	NodesByState      NodesByStateDTO `json:"nodesByState"`
	NodeQueryComplete bool            `json:"nodeQueryComplete"`
	ObservedAt        time.Time       `json:"observedAt"`
	UnderReplicated   int             `json:"underReplicated"`
	ConnectionCount   *uint64         `json:"connectionCount"`
	SessionCount      *uint64         `json:"sessionCount"`
	SubscriptionCount uint64          `json:"subscriptionCount"`
}

// NodesByStateDTO counts actual node states and observations that are unavailable.
type NodesByStateDTO struct {
	Joining    int `json:"joining"`
	Ready      int `json:"ready"`
	Draining   int `json:"draining"`
	SafeToStop int `json:"safeToStop"`
	Unknown    int `json:"unknown"`
}

// NodeFilter selects a state and page from the sorted membership snapshot.
type NodeFilter struct {
	State  NodeState
	Limit  int
	Cursor string
}

type MetricsMode string

const (
	MetricsModeNode                 MetricsMode = "node"
	MetricsModeAllNodesUnaggregated MetricsMode = "all_nodes_unaggregated"
	MetricsModeAllNodesAggregated   MetricsMode = "all_nodes_aggregated"
)

// MetricLabelDTO is one metric sample label.
type MetricLabelDTO struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// MetricSampleDTO is one metric sample with an exact string value.
type MetricSampleDTO struct {
	Name      string           `json:"name"`
	Labels    []MetricLabelDTO `json:"labels"`
	ValueKind string           `json:"valueKind"`
	Value     string           `json:"value"`
}

// MetricFamilyDTO combines the canonical descriptor and captured samples.
type MetricFamilyDTO struct {
	Name        string            `json:"name"`
	Help        string            `json:"help"`
	Type        string            `json:"type"`
	Scope       string            `json:"scope"`
	Aggregation string            `json:"aggregation"`
	LabelNames  []string          `json:"labelNames"`
	Samples     []MetricSampleDTO `json:"samples"`
}

// NodeMetricsDataDTO is the management API node metrics response and peer body.
type NodeMetricsDataDTO struct {
	NodeID         string            `json:"nodeId"`
	QueryStatus    QueryStatus       `json:"queryStatus"`
	QueryErrorCode *QueryErrorCode   `json:"queryErrorCode"`
	ObservedAt     *time.Time        `json:"observedAt"`
	StartedAt      *time.Time        `json:"startedAt"`
	Families       []MetricFamilyDTO `json:"families"`
}

// MetricsNodeStatusDTO reports one node's contribution to a cluster collection.
type MetricsNodeStatusDTO struct {
	NodeID         string          `json:"nodeId"`
	QueryStatus    QueryStatus     `json:"queryStatus"`
	QueryErrorCode *QueryErrorCode `json:"queryErrorCode"`
	ObservedAt     *time.Time      `json:"observedAt"`
	StartedAt      *time.Time      `json:"startedAt"`
}

// MetricsCollectionDataDTO is a node or cluster metric collection response.
type MetricsCollectionDataDTO struct {
	Mode       MetricsMode            `json:"mode"`
	ObservedAt time.Time              `json:"observedAt"`
	Complete   bool                   `json:"complete"`
	Nodes      []MetricsNodeStatusDTO `json:"nodes"`
	Families   []MetricFamilyDTO      `json:"families"`
}

// ClusterSnapshotSource provides the membership and cluster metadata for one query.
type ClusterSnapshotSource interface {
	ClusterSnapshot(ctx context.Context) (ClusterSnapshotDTO, error)
	FreshQuorumAvailable(ctx context.Context) (bool, error)
}

// LocalSnapshotSource captures this process's node and typed metric snapshots.
type LocalSnapshotSource interface {
	CaptureNodeSnapshot(ctx context.Context) (NodeSnapshotDTO, error)
	CaptureMetricsSnapshot(ctx context.Context) (MetricsSnapshotDTO, error)
}

// PeerSnapshotReader reads one remote node without exposing transport types.
type PeerSnapshotReader interface {
	QueryNodeSnapshot(ctx context.Context, memberID string) (NodeSnapshotDTO, error)
	QueryMetricsSnapshot(ctx context.Context, memberID string) (MetricsSnapshotDTO, error)
}

// QueryService provides node and cluster observation queries to inbound adapters.
type QueryService interface {
	GetCluster(ctx context.Context) (ClusterDataDTO, error)
	GetNode(ctx context.Context, nodeID string) (NodeDataDTO, error)
	ListNodes(ctx context.Context, filter NodeFilter) ([]NodeDataDTO, *managementports.PageDTO, error)
	GetNodeMetrics(ctx context.Context, nodeID string) (NodeMetricsDataDTO, error)
	CollectMetrics(ctx context.Context, mode MetricsMode) (MetricsCollectionDataDTO, error)
}
