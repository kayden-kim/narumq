package management

import (
	"context"
	"testing"
	"time"

	"narumq/internal/adapters/memory/retained"
	"narumq/internal/adapters/memory/sessionstore"
	adaptersmetrics "narumq/internal/adapters/metrics"
	ports "narumq/internal/application/ports/management"
	applicationretained "narumq/internal/application/ports/retained"
	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/routing"
	"narumq/internal/domain/session"
)

func setupTestService(t *testing.T) (*Service, *sessionstore.MemorySessionRepository, *routing.Table, *retained.Repository, *adaptersmetrics.Registry) {
	t.Helper()

	sstore := sessionstore.New()
	rtable := routing.NewTable()
	rrepo, err := retained.New(100, 65536)
	if err != nil {
		t.Fatalf("failed to create retained repo: %v", err)
	}
	mreg := adaptersmetrics.NewRegistry()

	cfg := ServiceConfig{
		ClusterID:      "test-cluster",
		NodeID:         "test-node-1",
		ReleaseVersion: "1.0.0-test",
		StartedAt:      time.Now().Add(-1 * time.Hour),
	}
	svc := NewService(cfg, sstore, rtable, rrepo, mreg)
	return svc, sstore, rtable, rrepo, mreg
}

// Scenarios: management-api-and-observability.md:MGT-SCN-020
func TestService_HealthChecks(t *testing.T) {
	ctx := context.Background()
	svc, _, _, _, _ := setupTestService(t)

	// 1. Initial state: JOINING
	if svc.NodeState() != ports.NodeStateJoining {
		t.Fatalf("expected state JOINING, got %s", svc.NodeState())
	}

	status, _ := svc.CheckStartup(ctx)
	if status != ports.HealthStatusStarting {
		t.Errorf("expected CheckStartup STARTING, got %s", status)
	}
	status, _ = svc.CheckReadiness(ctx)
	if status != ports.HealthStatusNotReady {
		t.Errorf("expected CheckReadiness NOT_READY, got %s", status)
	}
	status, _ = svc.CheckLiveness(ctx)
	if status != ports.HealthStatusUp {
		t.Errorf("expected CheckLiveness UP, got %s", status)
	}
	status, _ = svc.CheckDrain(ctx)
	if status != ports.HealthStatusNotReady {
		t.Errorf("expected CheckDrain NOT_READY, got %s", status)
	}

	// 2. Transition to READY
	svc.SetNodeState(ports.NodeStateReady, nil)
	status, _ = svc.CheckStartup(ctx)
	if status != ports.HealthStatusUp {
		t.Errorf("expected CheckStartup UP, got %s", status)
	}
	status, _ = svc.CheckReadiness(ctx)
	if status != ports.HealthStatusUp {
		t.Errorf("expected CheckReadiness UP, got %s", status)
	}

	// 3. Transition to DRAINING
	drainingReason := "rolling update"
	svc.SetNodeState(ports.NodeStateDraining, &drainingReason)
	status, reason := svc.CheckReadiness(ctx)
	if status != ports.HealthStatusNotReady || reason == nil || *reason != drainingReason {
		t.Errorf("expected CheckReadiness NOT_READY with drainingReason, got %s, reason %v", status, reason)
	}

	// 4. Transition to SAFE_TO_STOP
	svc.SetNodeState(ports.NodeStateSafeToStop, nil)
	status, _ = svc.CheckDrain(ctx)
	if status != ports.HealthStatusUp {
		t.Errorf("expected CheckDrain UP, got %s", status)
	}
	status, _ = svc.CheckLiveness(ctx)
	if status != ports.HealthStatusSafeToStop {
		t.Errorf("expected CheckLiveness SAFE_TO_STOP, got %s", status)
	}
}

// Scenarios: management-api-and-observability.md:MGT-SCN-022
func TestService_ClusterAndNodeSummary(t *testing.T) {
	ctx := context.Background()
	svc, _, _, _, mreg := setupTestService(t)
	svc.SetNodeState(ports.NodeStateReady, nil)
	mreg.ConnectionOpened("MQTT_5_0", "tcp")

	cluster := svc.GetClusterSummary(ctx)
	if cluster.ClusterID != "test-cluster" {
		t.Errorf("unexpected cluster ID: %s", cluster.ClusterID)
	}
	if cluster.MemberCount != 1 || cluster.QuorumSize != 1 || !cluster.QuorumAvailable {
		t.Errorf("unexpected cluster quorum stats: %+v", cluster)
	}
	if cluster.ConnectionCount != 1 {
		t.Errorf("expected 1 connection count, got %d", cluster.ConnectionCount)
	}
	if cluster.NodesByState.Ready != 1 {
		t.Errorf("expected 1 ready node, got %d", cluster.NodesByState.Ready)
	}

	// Single node query
	node, err := svc.GetNodeSummary(ctx, "test-node-1")
	if err != nil {
		t.Fatalf("GetNodeSummary failed: %v", err)
	}
	if node.NodeID != "test-node-1" || !node.Ready || node.Role != "LEADER" {
		t.Errorf("unexpected node summary: %+v", node)
	}

	// Unknown node
	_, err = svc.GetNodeSummary(ctx, "unknown-node")
	if err != ErrNodeNotFound {
		t.Errorf("expected ErrNodeNotFound, got %v", err)
	}

	// List nodes
	nodes, page, err := svc.ListNodeSummaries(ctx, ports.NodeFilter{})
	if err != nil {
		t.Fatalf("ListNodeSummaries failed: %v", err)
	}
	if len(nodes) != 1 || page.HasMore {
		t.Errorf("unexpected node list result: nodes=%d, hasMore=%v", len(nodes), page.HasMore)
	}
}

func TestService_ClientsAndPagination(t *testing.T) {
	ctx := context.Background()
	svc, sstore, _, _, _ := setupTestService(t)

	// Save dummy sessions
	now := time.Now().UTC()
	for i := 1; i <= 5; i++ {
		connect := mqtt.Connect{
			ClientID:              "client-" + string(rune('a'+i-1)),
			Version:               mqtt.ProtocolV5,
			CleanStart:            false,
			SessionExpiryInterval: 300,
		}
		lifecycle, _, err := session.Open(nil, connect, now)
		if err != nil {
			t.Fatalf("failed to open session: %v", err)
		}
		if err := sstore.Save(lifecycle); err != nil {
			t.Fatalf("failed to save session: %v", err)
		}
	}

	// 1. List with page size 2
	clients, page, err := svc.ListClients(ctx, ports.ClientFilter{Limit: 2})
	if err != nil {
		t.Fatalf("ListClients failed: %v", err)
	}
	if len(clients) != 2 || !page.HasMore || page.NextCursor == nil {
		t.Fatalf("expected 2 clients with next cursor, got %d, hasMore=%v", len(clients), page.HasMore)
	}
	if clients[0].ClientID != "client-a" || clients[1].ClientID != "client-b" {
		t.Errorf("unexpected clients in page 1: %+v", clients)
	}

	// 2. Fetch page 2 using cursor
	clients2, page2, err := svc.ListClients(ctx, ports.ClientFilter{Limit: 2, Cursor: *page.NextCursor})
	if err != nil {
		t.Fatalf("ListClients page 2 failed: %v", err)
	}
	if len(clients2) != 2 || !page2.HasMore || page2.NextCursor == nil {
		t.Fatalf("expected 2 clients in page 2 with next cursor, got %d", len(clients2))
	}
	if clients2[0].ClientID != "client-c" || clients2[1].ClientID != "client-d" {
		t.Errorf("unexpected clients in page 2: %+v", clients2)
	}

	// 3. Fetch page 3 (last page)
	clients3, page3, err := svc.ListClients(ctx, ports.ClientFilter{Limit: 2, Cursor: *page2.NextCursor})
	if err != nil {
		t.Fatalf("ListClients page 3 failed: %v", err)
	}
	if len(clients3) != 1 || page3.HasMore || page3.NextCursor != nil {
		t.Fatalf("expected 1 client in page 3 with no next cursor, got %d, hasMore=%v", len(clients3), page3.HasMore)
	}
	if clients3[0].ClientID != "client-e" {
		t.Errorf("unexpected client in page 3: %+v", clients3)
	}

	// 4. Invalid cursor test
	_, _, err = svc.ListClients(ctx, ports.ClientFilter{Cursor: "invalid-not-base64"})
	if err != ErrInvalidCursor {
		t.Errorf("expected ErrInvalidCursor, got %v", err)
	}

	// 5. Invalid page size test
	_, _, err = svc.ListClients(ctx, ports.ClientFilter{Limit: 9999})
	if err != ErrInvalidPageSize {
		t.Errorf("expected ErrInvalidPageSize, got %v", err)
	}

	// 6. GetClient
	clientA, err := svc.GetClient(ctx, "default", "client-a")
	if err != nil {
		t.Fatalf("GetClient failed: %v", err)
	}
	if clientA.ClientID != "client-a" || !clientA.Connected {
		t.Errorf("unexpected client details: %+v", clientA)
	}
	if clientA.TenantID != "default" {
		t.Errorf("expected default tenant for unbound session, got %q", clientA.TenantID)
	}
	if _, err := svc.GetClient(ctx, "tenant-a", "client-a"); err != ErrClientNotFound {
		t.Fatalf("cross-tenant GetClient error = %v, want %v", err, ErrClientNotFound)
	}

	_, err = svc.GetClient(ctx, "default", "unknown-client")
	if err != ErrClientNotFound {
		t.Errorf("expected ErrClientNotFound, got %v", err)
	}
}

func TestService_SubscriptionsAndRetained(t *testing.T) {
	ctx := context.Background()
	svc, _, rtable, rrepo, _ := setupTestService(t)

	// Add subscriptions
	sessID := routing.SessionID("client-sub-1")
	rtable.Subscribe(sessID, mqtt.Subscription{
		Filter: "sensors/temperature",
		QoS:    mqtt.QoSAtLeastOnce,
	})
	rtable.Subscribe(sessID, mqtt.Subscription{
		Filter: "$share/group1/sensors/humidity",
		QoS:    mqtt.QoSExactlyOnce,
	})

	subs, page, err := svc.ListSubscriptions(ctx, ports.SubscriptionFilter{})
	if err != nil {
		t.Fatalf("ListSubscriptions failed: %v", err)
	}
	if len(subs) != 2 || page.HasMore {
		t.Fatalf("expected 2 subscriptions, got %d", len(subs))
	}
	filteredSubs, _, err := svc.ListSubscriptions(ctx, ports.SubscriptionFilter{TenantID: "default"})
	if err != nil {
		t.Fatalf("ListSubscriptions default tenant filter failed: %v", err)
	}
	if len(filteredSubs) != 2 {
		t.Fatalf("expected unbound subscriptions under default tenant, got %d", len(filteredSubs))
	}

	// Add retained message
	now := time.Now().UTC()
	err = rrepo.Replace(applicationretained.MessageDTO{
		Topic:              "sensors/temperature",
		Payload:            []byte("23.5"),
		PublisherSessionID: "pub-1",
		QoS:                mqtt.QoSAtLeastOnce,
	}, now)
	if err != nil {
		t.Fatalf("failed to add retained message: %v", err)
	}

	retainedList, pageR, err := svc.ListRetained(ctx, ports.RetainedFilter{})
	if err != nil {
		t.Fatalf("ListRetained failed: %v", err)
	}
	if len(retainedList) != 1 || pageR.HasMore {
		t.Fatalf("expected 1 retained item, got %d", len(retainedList))
	}
	if retainedList[0].Topic != "sensors/temperature" || retainedList[0].PayloadBytes != 4 {
		t.Errorf("unexpected retained item: %+v", retainedList[0])
	}
	filteredRetained, _, err := svc.ListRetained(ctx, ports.RetainedFilter{TenantID: "default"})
	if err != nil {
		t.Fatalf("ListRetained default tenant filter failed: %v", err)
	}
	if len(filteredRetained) != 1 {
		t.Fatalf("expected retained message under default tenant, got %d", len(filteredRetained))
	}

	tenants, pageT, err := svc.ListTenants(ctx, ports.TenantFilter{Limit: 1})
	if err != nil {
		t.Fatalf("ListTenants failed: %v", err)
	}
	if len(tenants) != 1 || pageT.HasMore || tenants[0].TenantID != "default" {
		t.Fatalf("unexpected tenant list: tenants=%+v, page=%+v", tenants, pageT)
	}
}

type testClusterSessionSource struct {
	sessions  []ports.ClientSessionDTO
	available bool
}

func (source testClusterSessionSource) ClusterSessions() ([]ports.ClientSessionDTO, bool) {
	return source.sessions, source.available
}

// Scenarios: cluster-session-and-delivery.md:CLU-SCN-170
func TestService_ClusterSessionSourceQueries(t *testing.T) {
	// Traceability: CLU-08; docs/specs/cluster-session-and-delivery.md §4.3, §11.24.
	ctx := context.Background()
	svc, _, _, _, _ := setupTestService(t)
	node1, node2 := "1", "2"
	svc.UseClusterSessionSource(testClusterSessionSource{
		available: true,
		sessions: []ports.ClientSessionDTO{
			{TenantID: "tenant-a", ClientID: "shared", NodeID: &node2, Protocol: "MQTT_5_0", Connected: true, SessionPresent: true},
			{TenantID: "default", ClientID: "shared", NodeID: &node1, Protocol: "MQTT_3_1_1", SessionPresent: true},
			{TenantID: "tenant-b", ClientID: "zulu", NodeID: nil, Protocol: "MQTT_5_0", SessionPresent: true},
			{TenantID: "tenant-a", ClientID: "alpha", NodeID: &node2, Protocol: "MQTT_3_1_1", SessionPresent: true},
			{TenantID: "default", ClientID: "bravo", NodeID: &node1, Protocol: "MQTT_5_0", SessionPresent: true},
		},
	})

	all, page, err := svc.ListClients(ctx, ports.ClientFilter{Limit: 2})
	if err != nil {
		t.Fatalf("ListClients cluster source: %v", err)
	}
	if len(all) != 2 || !page.HasMore || page.NextCursor == nil || all[0].ClientID != "alpha" || all[1].ClientID != "bravo" {
		t.Fatalf("first sorted cluster page = %+v, page %+v", all, page)
	}
	second, secondPage, err := svc.ListClients(ctx, ports.ClientFilter{Limit: 2, Cursor: *page.NextCursor})
	if err != nil {
		t.Fatalf("ListClients second cluster page: %v", err)
	}
	if len(second) != 2 || !secondPage.HasMore || second[0].ClientID != "shared" || second[0].TenantID != "default" || second[1].ClientID != "shared" || second[1].TenantID != "tenant-a" {
		t.Fatalf("equal ClientID rows must sort by tenant: clients=%+v page=%+v", second, secondPage)
	}

	filtered, _, err := svc.ListClients(ctx, ports.ClientFilter{TenantID: "tenant-a", NodeID: "2", Protocol: "MQTT_3_1_1"})
	if err != nil {
		t.Fatalf("ListClients combined cluster filters: %v", err)
	}
	if len(filtered) != 1 || filtered[0].ClientID != "alpha" {
		t.Fatalf("combined cluster filters returned %+v", filtered)
	}
	nilNode, _, err := svc.ListClients(ctx, ports.ClientFilter{ClientIDPrefix: "z", NodeID: "2"})
	if err != nil {
		t.Fatalf("ListClients nil-node filter: %v", err)
	}
	if len(nilNode) != 0 {
		t.Fatalf("nil NodeID matched a requested node: %+v", nilNode)
	}
	withoutNodeFilter, _, err := svc.ListClients(ctx, ports.ClientFilter{ClientIDPrefix: "z"})
	if err != nil || len(withoutNodeFilter) != 1 || withoutNodeFilter[0].TenantID != "tenant-b" {
		t.Fatalf("nil NodeID row without node filter = %+v, err %v", withoutNodeFilter, err)
	}

	if _, _, err := svc.ListClients(ctx, ports.ClientFilter{Cursor: "invalid-not-base64"}); err != ErrInvalidCursor {
		t.Fatalf("invalid cluster cursor error = %v, want %v", err, ErrInvalidCursor)
	}
	if _, _, err := svc.ListClients(ctx, ports.ClientFilter{Limit: MaxPageLimit + 1}); err != ErrInvalidPageSize {
		t.Fatalf("oversized cluster page error = %v, want %v", err, ErrInvalidPageSize)
	}
}

// Scenarios: cluster-session-and-delivery.md:CLU-SCN-170
func TestService_ClusterSessionSourceGetClient(t *testing.T) {
	// Traceability: CLU-08; docs/specs/cluster-session-and-delivery.md §4.3, §11.24.
	ctx := context.Background()
	svc, store, _, _, _ := setupTestService(t)
	// A local-only row must not leak through once the committed cluster view is available.
	connect := mqtt.Connect{ClientID: "local-only", Version: mqtt.ProtocolV5, CleanStart: false, SessionExpiryInterval: 300}
	local, _, err := session.Open(nil, connect, time.Now().UTC())
	if err != nil {
		t.Fatalf("open local session: %v", err)
	}
	if err := store.Save(local); err != nil {
		t.Fatalf("save local session: %v", err)
	}
	node := "3"
	svc.UseClusterSessionSource(testClusterSessionSource{
		available: true,
		sessions: []ports.ClientSessionDTO{
			{TenantID: "default", ClientID: "visible", NodeID: &node, Protocol: "MQTT_5_0", SessionPresent: true},
			{TenantID: "tenant-a", ClientID: "visible", NodeID: nil, Protocol: "MQTT_3_1_1", SessionPresent: true},
		},
	})

	defaultClient, err := svc.GetClient(ctx, "", "visible")
	if err != nil || defaultClient.TenantID != "default" || defaultClient.NodeID == nil || *defaultClient.NodeID != node {
		t.Fatalf("default-tenant cluster GetClient = %+v, err %v", defaultClient, err)
	}
	exact, err := svc.GetClient(ctx, "tenant-a", "visible")
	if err != nil || exact.TenantID != "tenant-a" || exact.NodeID != nil {
		t.Fatalf("exact cluster GetClient = %+v, err %v", exact, err)
	}
	if _, err := svc.GetClient(ctx, "", "local-only"); err != ErrClientNotFound {
		t.Fatalf("active cluster view fell back to local session: err %v", err)
	}
	if _, err := svc.GetClient(ctx, "tenant-missing", "visible"); err != ErrClientNotFound {
		t.Fatalf("missing cluster tenant GetClient error = %v", err)
	}

	svc.UseClusterSessionSource(testClusterSessionSource{
		available: false,
		sessions: []ports.ClientSessionDTO{
			{TenantID: "default", ClientID: "phantom", Protocol: "MQTT_5_0"},
		},
	})
	fallback, _, err := svc.ListClients(ctx, ports.ClientFilter{})
	if err != nil || len(fallback) != 1 || fallback[0].ClientID != "local-only" {
		t.Fatalf("unavailable cluster view ListClients did not use local sessions: clients=%+v err=%v", fallback, err)
	}
	fallbackClient, err := svc.GetClient(ctx, "default", "local-only")
	if err != nil || fallbackClient.ClientID != "local-only" {
		t.Fatalf("unavailable cluster view GetClient did not use local session store: client=%+v err=%v", fallbackClient, err)
	}
}
