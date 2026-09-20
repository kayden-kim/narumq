package management

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	ports "narumq/internal/application/ports/management"
	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/routing"
	"narumq/internal/domain/session"
	"narumq/internal/domain/tenancy"
)

var (
	// ErrNodeNotFound indicates that the requested node does not exist.
	ErrNodeNotFound = errors.New("node not found")
	// ErrClientNotFound indicates that the requested client session was not found.
	ErrClientNotFound = errors.New("client session not found")
	// ErrInvalidCursor indicates malformed pagination cursor token.
	ErrInvalidCursor = errors.New("invalid pagination cursor")
	// ErrInvalidPageSize indicates that limit exceeds bounds.
	ErrInvalidPageSize = errors.New("invalid page size")
)

const (
	defaultPageLimit = 50

	// MaxPageLimit is the maximum number of items returned by a paginated list.
	MaxPageLimit = 1000
)

// ServiceConfig holds configuration parameters for the management service.
type ServiceConfig struct {
	ClusterID      string
	NodeID         string
	ReleaseVersion string
	StartedAt      time.Time
}

// Service implements ports.StateProvider and ports.HealthChecker.
type Service struct {
	mu sync.RWMutex

	clusterID      string
	nodeID         string
	releaseVersion string
	startedAt      time.Time

	nodeState       ports.NodeState
	readinessReason *string

	sessionStore      ports.SessionSnapshotter
	routingTable      ports.SubscriptionSnapshotter
	retainedRepo      ports.RetainedSnapshotter
	connectionCounter ports.ConnectionCounter
	defaultTenantID   tenancy.TenantID
}

// NewService creates a new management Service.
func NewService(
	cfg ServiceConfig,
	sessionStore ports.SessionSnapshotter,
	routingTable ports.SubscriptionSnapshotter,
	retainedRepo ports.RetainedSnapshotter,
	connectionCounter ports.ConnectionCounter,
) *Service {
	started := cfg.StartedAt
	if started.IsZero() {
		started = time.Now().UTC()
	}
	clusterID := cfg.ClusterID
	if clusterID == "" {
		clusterID = "narumq-cluster-1"
	}
	nodeID := cfg.NodeID
	if nodeID == "" {
		nodeID = "narumq-node-1"
	}
	version := cfg.ReleaseVersion
	if version == "" {
		version = "1.0.0"
	}

	defTenant, _ := tenancy.NewTenantID("default")

	return &Service{
		clusterID:         clusterID,
		nodeID:            nodeID,
		releaseVersion:    version,
		startedAt:         started,
		nodeState:         ports.NodeStateJoining,
		sessionStore:      sessionStore,
		routingTable:      routingTable,
		retainedRepo:      retainedRepo,
		connectionCounter: connectionCounter,
		defaultTenantID:   defTenant,
	}
}

var _ ports.StateProvider = (*Service)(nil)
var _ ports.HealthChecker = (*Service)(nil)

// SetNodeState updates the node's lifecycle state.
func (s *Service) SetNodeState(state ports.NodeState, reason *string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nodeState = state
	s.readinessReason = reason
}

// NodeState returns the current node lifecycle state.
func (s *Service) NodeState() ports.NodeState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.nodeState
}

// CheckLiveness reports process liveness.
func (s *Service) CheckLiveness(ctx context.Context) (ports.HealthStatus, *string) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.nodeState == ports.NodeStateSafeToStop {
		reason := "node is safe to stop"
		return ports.HealthStatusSafeToStop, &reason
	}
	return ports.HealthStatusUp, nil
}

// CheckReadiness reports readiness to accept new MQTT / management traffic.
func (s *Service) CheckReadiness(ctx context.Context) (ports.HealthStatus, *string) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.nodeState == ports.NodeStateReady {
		return ports.HealthStatusUp, nil
	}
	reason := "node is not ready"
	if s.readinessReason != nil {
		reason = *s.readinessReason
	} else if s.nodeState == ports.NodeStateDraining {
		reason = "node is draining"
	} else if s.nodeState == ports.NodeStateSafeToStop {
		reason = "node is safe to stop"
	}
	return ports.HealthStatusNotReady, &reason
}

// CheckStartup reports whether initial recovery and setup is complete.
func (s *Service) CheckStartup(ctx context.Context) (ports.HealthStatus, *string) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.nodeState == ports.NodeStateJoining {
		reason := "node is joining"
		return ports.HealthStatusStarting, &reason
	}
	return ports.HealthStatusUp, nil
}

// CheckDrain reports whether drain has completed.
func (s *Service) CheckDrain(ctx context.Context) (ports.HealthStatus, *string) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.nodeState == ports.NodeStateSafeToStop {
		return ports.HealthStatusUp, nil
	}
	reason := "node drain is not complete"
	return ports.HealthStatusNotReady, &reason
}

// GetClusterSummary returns cluster overview.
func (s *Service) GetClusterSummary(ctx context.Context) ports.ClusterSummaryDTO {
	s.mu.RLock()
	state := s.nodeState
	nodeID := s.nodeID
	s.mu.RUnlock()

	nodesByState := ports.NodesByStateDTO{}
	switch state {
	case ports.NodeStateJoining:
		nodesByState.Joining = 1
	case ports.NodeStateReady:
		nodesByState.Ready = 1
	case ports.NodeStateDraining:
		nodesByState.Draining = 1
	case ports.NodeStateSafeToStop:
		nodesByState.SafeToStop = 1
	}

	activeConns := int(s.activeConnections())
	sessionCount := 0
	if s.sessionStore != nil {
		sessionCount = len(s.sessionStore.Snapshot())
	}
	subCount := 0
	if s.routingTable != nil {
		subCount = len(s.routingTable.Snapshot())
	}

	leaderID := nodeID

	return ports.ClusterSummaryDTO{
		ClusterID:         s.clusterID,
		ReleaseVersion:    s.releaseVersion,
		MemberCount:       1,
		QuorumSize:        1,
		QuorumAvailable:   state == ports.NodeStateReady,
		LeaderNodeID:      &leaderID,
		NodesByState:      nodesByState,
		UnderReplicated:   0,
		ConnectionCount:   activeConns,
		SessionCount:      sessionCount,
		SubscriptionCount: subCount,
	}
}

// GetNodeSummary returns status for a single node.
func (s *Service) GetNodeSummary(ctx context.Context, nodeID string) (*ports.NodeSummaryDTO, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if nodeID != s.nodeID {
		return nil, ErrNodeNotFound
	}

	uptimeMs := time.Since(s.startedAt).Milliseconds()
	if uptimeMs < 0 {
		uptimeMs = 0
	}

	return &ports.NodeSummaryDTO{
		NodeID:           s.nodeID,
		Version:          s.releaseVersion,
		UptimeMs:         uptimeMs,
		State:            s.nodeState,
		Ready:            s.nodeState == ports.NodeStateReady,
		ReadinessReason:  s.readinessReason,
		Role:             "LEADER",
		LastAppliedIndex: 0,
		ConnectionCount:  int(s.activeConnections()),
		StoragePressure:  false,
	}, nil
}

// ListNodeSummaries lists all node summaries matching filter.
func (s *Service) ListNodeSummaries(ctx context.Context, filter ports.NodeFilter) ([]ports.NodeSummaryDTO, *ports.PageDTO, error) {
	s.mu.RLock()
	state := s.nodeState
	reason := s.readinessReason
	s.mu.RUnlock()

	limit := filter.Limit
	if limit == 0 {
		limit = defaultPageLimit
	} else if limit < 0 || limit > MaxPageLimit {
		return nil, nil, ErrInvalidPageSize
	}

	offset, err := decodeCursor(filter.Cursor)
	if err != nil {
		return nil, nil, ErrInvalidCursor
	}

	list := make([]ports.NodeSummaryDTO, 0)
	if filter.State == "" || filter.State == state {
		uptimeMs := time.Since(s.startedAt).Milliseconds()
		if uptimeMs < 0 {
			uptimeMs = 0
		}
		list = append(list, ports.NodeSummaryDTO{
			NodeID:           s.nodeID,
			Version:          s.releaseVersion,
			UptimeMs:         uptimeMs,
			State:            state,
			Ready:            state == ports.NodeStateReady,
			ReadinessReason:  reason,
			Role:             "LEADER",
			LastAppliedIndex: 0,
			ConnectionCount:  int(s.activeConnections()),
			StoragePressure:  false,
		})
	}

	paged, page := paginateSlice(list, offset, limit)
	return paged, page, nil
}

// ListClients lists client sessions matching filter.
func (s *Service) ListClients(ctx context.Context, filter ports.ClientFilter) ([]ports.ClientSessionDTO, *ports.PageDTO, error) {
	limit := filter.Limit
	if limit == 0 {
		limit = defaultPageLimit
	} else if limit < 0 || limit > MaxPageLimit {
		return nil, nil, ErrInvalidPageSize
	}

	offset, err := decodeCursor(filter.Cursor)
	if err != nil {
		return nil, nil, ErrInvalidCursor
	}

	var rawSessions []session.Lifecycle
	if s.sessionStore != nil {
		rawSessions = s.sessionStore.Snapshot()
	}

	s.mu.RLock()
	currentNodeID := s.nodeID
	s.mu.RUnlock()

	clients := make([]ports.ClientSessionDTO, 0)
	for _, sess := range rawSessions {
		tenantStr := sess.TenantID().String()
		if tenantStr == "" {
			tenantStr = s.defaultTenantID.String()
		}
		if filter.TenantID != "" && tenantStr != filter.TenantID {
			continue
		}
		if filter.ClientIDPrefix != "" && !strings.HasPrefix(sess.ClientID(), filter.ClientIDPrefix) {
			continue
		}
		if filter.NodeID != "" && currentNodeID != filter.NodeID {
			continue
		}

		proto := "MQTT_5_0"
		if sess.Version() == mqtt.ProtocolV311 {
			proto = "MQTT_3_1_1"
		}
		if filter.Protocol != "" && proto != filter.Protocol {
			continue
		}

		var expiryAt *time.Time
		if exp, ok := sess.ExpiresAt(); ok {
			expiryAt = &exp
		}

		nodeIDCopy := currentNodeID
		clients = append(clients, ports.ClientSessionDTO{
			TenantID:          tenantStr,
			ClientID:          sess.ClientID(),
			NodeID:            &nodeIDCopy,
			Protocol:          proto,
			Connected:         sess.Connected(),
			SessionPresent:    true,
			ExpiryAt:          expiryAt,
			InflightCount:     0,
			OfflineQueueCount: 0,
			MaskedIdentity:    nil,
		})
	}

	// Deterministic sorting by ClientID
	sort.Slice(clients, func(i, j int) bool {
		return clients[i].ClientID < clients[j].ClientID
	})

	paged, page := paginateSlice(clients, offset, limit)
	return paged, page, nil
}

// GetClient returns a specific client session.
func (s *Service) GetClient(ctx context.Context, tenantID string, clientID string) (*ports.ClientSessionDTO, error) {
	if s.sessionStore == nil {
		return nil, ErrClientNotFound
	}

	var sess session.Lifecycle
	var found bool

	if tenantID != "" {
		tid, err := tenancy.NewTenantID(tenantID)
		if err == nil {
			skey, err := tenancy.NewSessionKey(tid, clientID)
			if err == nil {
				sess, found, _ = s.sessionStore.LoadKey(skey)
			}
		}
	}

	if !found {
		sess, found, _ = s.sessionStore.Load(clientID)
	}

	if !found {
		return nil, ErrClientNotFound
	}
	storedTenantID := sess.TenantID().String()
	if storedTenantID == "" {
		storedTenantID = s.defaultTenantID.String()
	}
	if tenantID != "" && storedTenantID != tenantID {
		return nil, ErrClientNotFound
	}
	s.mu.RLock()
	currentNodeID := s.nodeID
	s.mu.RUnlock()

	proto := "MQTT_5_0"
	if sess.Version() == mqtt.ProtocolV311 {
		proto = "MQTT_3_1_1"
	}
	var expiryAt *time.Time
	if exp, ok := sess.ExpiresAt(); ok {
		expiryAt = &exp
	}

	nodeIDCopy := currentNodeID
	tenantStr := storedTenantID
	return &ports.ClientSessionDTO{
		TenantID:          tenantStr,
		ClientID:          sess.ClientID(),
		NodeID:            &nodeIDCopy,
		Protocol:          proto,
		Connected:         sess.Connected(),
		SessionPresent:    true,
		ExpiryAt:          expiryAt,
		InflightCount:     0,
		OfflineQueueCount: 0,
		MaskedIdentity:    nil,
	}, nil
}

// ListSubscriptions lists active subscriptions matching filter.
func (s *Service) ListSubscriptions(ctx context.Context, filter ports.SubscriptionFilter) ([]ports.SubscriptionDTO, *ports.PageDTO, error) {
	limit := filter.Limit
	if limit == 0 {
		limit = defaultPageLimit
	} else if limit < 0 || limit > MaxPageLimit {
		return nil, nil, ErrInvalidPageSize
	}

	offset, err := decodeCursor(filter.Cursor)
	if err != nil {
		return nil, nil, ErrInvalidCursor
	}

	var rawSubs []routing.SubscriptionRecord
	if s.routingTable != nil {
		rawSubs = s.routingTable.Snapshot()
	}

	subs := make([]ports.SubscriptionDTO, 0)
	for _, r := range rawSubs {
		tenantStr := r.TenantID.String()
		if tenantStr == "" {
			tenantStr = s.defaultTenantID.String()
		}
		if filter.TenantID != "" && tenantStr != filter.TenantID {
			continue
		}
		if filter.ClientID != "" && string(r.SessionID) != filter.ClientID {
			continue
		}

		subs = append(subs, ports.SubscriptionDTO{
			TenantID:    tenantStr,
			ClientID:    string(r.SessionID),
			TopicFilter: r.TopicFilter,
			QoS:         int(r.QoS),
			SharedGroup: r.SharedGroup,
		})
	}

	// Deterministic sorting
	sort.Slice(subs, func(i, j int) bool {
		if subs[i].ClientID != subs[j].ClientID {
			return subs[i].ClientID < subs[j].ClientID
		}
		return subs[i].TopicFilter < subs[j].TopicFilter
	})

	paged, page := paginateSlice(subs, offset, limit)
	return paged, page, nil
}

// ListRetained lists retained message metadata matching filter.
func (s *Service) ListRetained(ctx context.Context, filter ports.RetainedFilter) ([]ports.RetainedMetadataDTO, *ports.PageDTO, error) {
	limit := filter.Limit
	if limit == 0 {
		limit = defaultPageLimit
	} else if limit < 0 || limit > MaxPageLimit {
		return nil, nil, ErrInvalidPageSize
	}

	offset, err := decodeCursor(filter.Cursor)
	if err != nil {
		return nil, nil, ErrInvalidCursor
	}

	now := time.Now().UTC()
	list := make([]ports.RetainedMetadataDTO, 0)
	if s.retainedRepo != nil {
		rawMessages, err := s.retainedRepo.List(now)
		if err == nil {
			tenantStr := s.defaultTenantID.String()
			for _, m := range rawMessages {
				if filter.TenantID != "" && tenantStr != filter.TenantID {
					continue
				}
				if filter.TopicPrefix != "" && !strings.HasPrefix(m.Topic, filter.TopicPrefix) {
					continue
				}

				var expiresAt *time.Time
				if !m.ExpiresAt.IsZero() {
					exp := m.ExpiresAt
					expiresAt = &exp
				}

				list = append(list, ports.RetainedMetadataDTO{
					TenantID:     tenantStr,
					Topic:        m.Topic,
					QoS:          int(m.QoS),
					PayloadBytes: len(m.Payload),
					UpdatedAt:    now,
					ExpiresAt:    expiresAt,
				})
			}
		}
	}

	// Deterministic sorting by topic
	sort.Slice(list, func(i, j int) bool {
		return list[i].Topic < list[j].Topic
	})

	paged, page := paginateSlice(list, offset, limit)
	return paged, page, nil
}

// ListTenants lists tenants matching filter.
func (s *Service) ListTenants(ctx context.Context, filter ports.TenantFilter) ([]ports.TenantSummaryDTO, *ports.PageDTO, error) {
	limit := filter.Limit
	if limit == 0 {
		limit = defaultPageLimit
	} else if limit < 0 || limit > MaxPageLimit {
		return nil, nil, ErrInvalidPageSize
	}

	offset, err := decodeCursor(filter.Cursor)
	if err != nil {
		return nil, nil, ErrInvalidCursor
	}

	tenantStr := s.defaultTenantID.String()
	dispName := "Default Tenant"
	tenants := []ports.TenantSummaryDTO{
		{
			TenantID:    tenantStr,
			DisplayName: &dispName,
			Status:      "ACTIVE",
			Version:     1,
			CreatedAt:   s.startedAt,
		},
	}

	paged, page := paginateSlice(tenants, offset, limit)
	return paged, page, nil
}

func (s *Service) activeConnections() int64 {
	if s.connectionCounter != nil {
		return s.connectionCounter.ActiveConnections()
	}
	return 0
}

func encodeCursor(offset int) string {
	raw := fmt.Sprintf("cur:%d", offset)
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

func decodeCursor(cursor string) (int, error) {
	if cursor == "" {
		return 0, nil
	}
	b, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return 0, ErrInvalidCursor
	}
	str := string(b)
	if !strings.HasPrefix(str, "cur:") {
		return 0, ErrInvalidCursor
	}
	numStr := strings.TrimPrefix(str, "cur:")
	offset, err := strconv.Atoi(numStr)
	if err != nil || offset < 0 {
		return 0, ErrInvalidCursor
	}
	return offset, nil
}

func paginateSlice[T any](items []T, offset, limit int) ([]T, *ports.PageDTO) {
	total := len(items)
	if offset >= total {
		return make([]T, 0), &ports.PageDTO{NextCursor: nil, HasMore: false}
	}
	end := offset + limit
	hasMore := end < total
	if end > total {
		end = total
	}
	paged := items[offset:end]
	if paged == nil {
		paged = make([]T, 0)
	}
	var nextCursor *string
	if hasMore {
		c := encodeCursor(end)
		nextCursor = &c
	}

	return paged, &ports.PageDTO{
		NextCursor: nextCursor,
		HasMore:    hasMore,
	}
}
