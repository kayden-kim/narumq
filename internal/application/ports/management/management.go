package management

import (
	"context"
	"errors"
	"time"

	portsretained "narumq/internal/application/ports/retained"
	"narumq/internal/domain/routing"
	"narumq/internal/domain/session"
	"narumq/internal/domain/tenancy"
)

// NodeState matches the OpenAPI NodeState schema.
type NodeState string

const (
	NodeStateJoining    NodeState = "JOINING"
	NodeStateReady      NodeState = "READY"
	NodeStateDraining   NodeState = "DRAINING"
	NodeStateSafeToStop NodeState = "SAFE_TO_STOP"
)

// HealthStatus matches the OpenAPI HealthData.status schema.
type HealthStatus string

const (
	HealthStatusUp         HealthStatus = "UP"
	HealthStatusStarting   HealthStatus = "STARTING"
	HealthStatusNotReady   HealthStatus = "NOT_READY"
	HealthStatusSafeToStop HealthStatus = "SAFE_TO_STOP"
	HealthStatusDown       HealthStatus = "DOWN"
)

// HealthDataDTO represents the payload of a health check response.
type HealthDataDTO struct {
	Status     HealthStatus `json:"status"`
	Reason     *string      `json:"reason"`
	ObservedAt time.Time    `json:"observedAt"`
}

// NodesByStateDTO contains counts of nodes in each lifecycle state.
type NodesByStateDTO struct {
	Joining    int `json:"joining"`
	Ready      int `json:"ready"`
	Draining   int `json:"draining"`
	SafeToStop int `json:"safeToStop"`
}

// ClusterSummaryDTO represents the cluster overview.
type ClusterSummaryDTO struct {
	ClusterID         string          `json:"clusterId"`
	ReleaseVersion    string          `json:"releaseVersion"`
	MemberCount       int             `json:"memberCount"`
	QuorumSize        int             `json:"quorumSize"`
	QuorumAvailable   bool            `json:"quorumAvailable"`
	LeaderNodeID      *string         `json:"leaderNodeId"`
	NodesByState      NodesByStateDTO `json:"nodesByState"`
	UnderReplicated   int             `json:"underReplicated"`
	ConnectionCount   int             `json:"connectionCount"`
	SessionCount      int             `json:"sessionCount"`
	SubscriptionCount int             `json:"subscriptionCount"`
}

// NodeSummaryDTO represents a node's operational snapshot.
type NodeSummaryDTO struct {
	NodeID            string               `json:"nodeId"`
	Version           string               `json:"version"`
	UptimeMs          int64                `json:"uptimeMs"`
	State             NodeState            `json:"state"`
	Ready             bool                 `json:"ready"`
	ReadinessReason   *string              `json:"readinessReason"`
	Role              string               `json:"role"` // LEADER, FOLLOWER, LEARNER
	LastAppliedIndex  int64                `json:"lastAppliedIndex"`
	ConnectionCount   int                  `json:"connectionCount"`
	StoragePressure   bool                 `json:"storagePressure"`
	ReplicationHealth *string              `json:"replicationHealth,omitempty"`
	LastErrorCode     *string              `json:"lastErrorCode,omitempty"`
	ReplicationQueue  *ReplicationQueueDTO `json:"replicationQueue,omitempty"`
}

// ReplicationQueueDTO reports the queried node's bounded outbound peer queue toward one member.
type ReplicationQueueDTO struct {
	OutboundMessages int    `json:"outboundMessages"`
	OutboundBytes    int    `json:"outboundBytes"`
	RejectedMessages uint64 `json:"rejectedMessages"`
}

// ClientSessionDTO represents a client's session state.
type ClientSessionDTO struct {
	TenantID          string     `json:"tenantId"`
	ClientID          string     `json:"clientId"`
	NodeID            *string    `json:"nodeId"`
	Protocol          string     `json:"protocol"` // MQTT_3_1_1, MQTT_5_0
	Connected         bool       `json:"connected"`
	SessionPresent    bool       `json:"sessionPresent"`
	ExpiryAt          *time.Time `json:"expiryAt"`
	InflightCount     int        `json:"inflightCount"`
	OfflineQueueCount int        `json:"offlineQueueCount"`
	MaskedIdentity    *string    `json:"maskedIdentity"`
}

// SubscriptionDTO represents an active topic subscription.
type SubscriptionDTO struct {
	TenantID    string  `json:"tenantId"`
	ClientID    string  `json:"clientId"`
	TopicFilter string  `json:"topicFilter"`
	QoS         int     `json:"qos"`
	SharedGroup *string `json:"sharedGroup"`
}

// RetainedMetadataDTO represents metadata of a retained message.
type RetainedMetadataDTO struct {
	TenantID     string     `json:"tenantId"`
	Topic        string     `json:"topic"`
	QoS          int        `json:"qos"`
	PayloadBytes int        `json:"payloadBytes"`
	UpdatedAt    time.Time  `json:"updatedAt"`
	ExpiresAt    *time.Time `json:"expiresAt"`
}

// TenantSummaryDTO represents a tenant overview.
type TenantSummaryDTO struct {
	TenantID    string    `json:"tenantId"`
	DisplayName *string   `json:"displayName"`
	Status      string    `json:"status"` // ACTIVE, SUSPENDED
	Version     int       `json:"version"`
	CreatedAt   time.Time `json:"createdAt"`
}

// PageDTO contains cursor-based pagination metadata.
type PageDTO struct {
	NextCursor *string `json:"nextCursor"`
	HasMore    bool    `json:"hasMore"`
}

// ClientFilter specifies query filters for client listing.
type ClientFilter struct {
	TenantID       string
	ClientIDPrefix string
	NodeID         string
	Protocol       string
	Limit          int
	Cursor         string
}

// SubscriptionFilter specifies query filters for subscription listing.
type SubscriptionFilter struct {
	TenantID string
	ClientID string
	Limit    int
	Cursor   string
}

// RetainedFilter specifies query filters for retained message metadata.
type RetainedFilter struct {
	TenantID    string
	TopicPrefix string
	Limit       int
	Cursor      string
}

// TenantFilter specifies query filters for tenant listing.
type TenantFilter struct {
	Limit  int
	Cursor string
}

// NodeFilter specifies query filters for node listing.
type NodeFilter struct {
	State  NodeState
	Limit  int
	Cursor string
}

// StateProvider exposes read-only queries for cluster, node, client, subscription, and retained states.
type StateProvider interface {
	GetClusterSummary(ctx context.Context) ClusterSummaryDTO
	GetNodeSummary(ctx context.Context, nodeID string) (*NodeSummaryDTO, error)
	ListNodeSummaries(ctx context.Context, filter NodeFilter) ([]NodeSummaryDTO, *PageDTO, error)
	ListClients(ctx context.Context, filter ClientFilter) ([]ClientSessionDTO, *PageDTO, error)
	GetClient(ctx context.Context, tenantID string, clientID string) (*ClientSessionDTO, error)
	ListSubscriptions(ctx context.Context, filter SubscriptionFilter) ([]SubscriptionDTO, *PageDTO, error)
	ListRetained(ctx context.Context, filter RetainedFilter) ([]RetainedMetadataDTO, *PageDTO, error)
	ListTenants(ctx context.Context, filter TenantFilter) ([]TenantSummaryDTO, *PageDTO, error)
}

// ErrClusterBackupUnavailable means a ready, quorum-fresh commit boundary could not be captured.
var ErrClusterBackupUnavailable = errors.New("cluster backup is unavailable")

// ClusterBackupDTO contains an encoded archive and its quorum-fresh commit boundary.
type ClusterBackupDTO struct {
	CommitIndex uint64
	// ArchiveSHA256 is the archive trailer digest over framed bytes before the trailer.
	ArchiveSHA256 [32]byte
	Archive       []byte
}

// ClusterBackupProvider captures an archive at a quorum-fresh commit boundary.
type ClusterBackupProvider interface {
	CaptureClusterBackup(ctx context.Context) (ClusterBackupDTO, error)
}

// HealthChecker evaluates liveness, readiness, startup, and drain health states.
type HealthChecker interface {
	CheckLiveness(ctx context.Context) (HealthStatus, *string)
	CheckReadiness(ctx context.Context) (HealthStatus, *string)
	CheckStartup(ctx context.Context) (HealthStatus, *string)
	CheckDrain(ctx context.Context) (HealthStatus, *string)
}

// ClusterSessionSource provides committed session metadata when available.
// The bool is false when the caller should use its local session store instead.
type ClusterSessionSource interface {
	ClusterSessions() ([]ClientSessionDTO, bool)
}

// SessionSnapshotter provides access to session lifecycles for management inspection.
type SessionSnapshotter interface {
	Snapshot() []session.Lifecycle
	Load(clientID string) (session.Lifecycle, bool, error)
	LoadKey(key tenancy.SessionKey) (session.Lifecycle, bool, error)
}

// SubscriptionSnapshotter provides access to routing subscriptions for management inspection.
type SubscriptionSnapshotter interface {
	Snapshot() []routing.SubscriptionRecord
}

// RetainedSnapshotter provides access to retained messages for management inspection.
type RetainedSnapshotter interface {
	List(now time.Time) ([]portsretained.MessageDTO, error)
	Stats(now time.Time) (int, int)
}

// ConnectionCounter provides current count of active connections.
type ConnectionCounter interface {
	ActiveConnections() int64
}
