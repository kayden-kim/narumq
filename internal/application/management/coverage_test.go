package management

import (
	"context"
	"encoding/base64"
	"errors"
	"reflect"
	"testing"
	"time"

	"narumq/internal/adapters/memory/sessionstore"
	ports "narumq/internal/application/ports/management"
	portsretained "narumq/internal/application/ports/retained"
	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/routing"
	"narumq/internal/domain/session"
	"narumq/internal/domain/tenancy"
)

type cmmSubscriptionSnapshot []routing.SubscriptionRecord

func (snapshot cmmSubscriptionSnapshot) Snapshot() []routing.SubscriptionRecord {
	return append([]routing.SubscriptionRecord(nil), snapshot...)
}

type cmmRetainedSnapshot []portsretained.MessageDTO

func (snapshot cmmRetainedSnapshot) List(time.Time) ([]portsretained.MessageDTO, error) {
	return append([]portsretained.MessageDTO(nil), snapshot...), nil
}

func (snapshot cmmRetainedSnapshot) Stats(time.Time) (int, int) {
	return len(snapshot), 0
}

func cmmTenantID(t *testing.T, value string) tenancy.TenantID {
	t.Helper()
	tenantID, err := tenancy.NewTenantID(value)
	if err != nil {
		t.Fatalf("tenant %q: %v", value, err)
	}
	return tenantID
}

func cmmSessionID(t *testing.T, tenantID tenancy.TenantID, clientID string) routing.SessionID {
	t.Helper()
	key, err := tenancy.NewSessionKey(tenantID, clientID)
	if err != nil {
		t.Fatalf("session key for %q: %v", clientID, err)
	}
	return routing.SessionID(key.RoutingID())
}

func cmmSaveSession(t *testing.T, store *sessionstore.MemorySessionRepository, connect mqtt.Connect, now time.Time, disconnect bool, tenant string) {
	t.Helper()
	var keys []tenancy.SessionKey
	if tenant != "" {
		key, err := tenancy.NewSessionKey(cmmTenantID(t, tenant), connect.ClientID)
		if err != nil {
			t.Fatalf("session key: %v", err)
		}
		keys = append(keys, key)
	}
	lifecycle, _, err := session.Open(nil, connect, now, keys...)
	if err != nil {
		t.Fatalf("open %q: %v", connect.ClientID, err)
	}
	if disconnect {
		result, err := lifecycle.Disconnect(now, session.DisconnectNormal)
		if err != nil {
			t.Fatalf("disconnect %q: %v", connect.ClientID, err)
		}
		lifecycle = result.Lifecycle
	}
	if err := store.Save(lifecycle); err != nil {
		t.Fatalf("save %q: %v", connect.ClientID, err)
	}
}

func cmmCursor(raw string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

func TestCmmNewServiceAppliesIdentityDefaultsWithoutAdapters(t *testing.T) {
	ctx := context.Background()
	before := time.Now().UTC()
	service := NewService(ServiceConfig{}, nil, nil, nil, nil)
	after := time.Now().UTC()

	summary := service.GetClusterSummary(ctx)
	if summary.ClusterID != "narumq-cluster-1" || summary.ReleaseVersion != "1.0.0" || summary.LeaderNodeID == nil || *summary.LeaderNodeID != "narumq-node-1" {
		t.Fatalf("default identity summary=%+v", summary)
	}
	if summary.ConnectionCount != 0 || summary.SessionCount != 0 || summary.SubscriptionCount != 0 || summary.QuorumAvailable || summary.NodesByState.Joining != 1 {
		t.Fatalf("adapter-free summary=%+v", summary)
	}
	node, err := service.GetNodeSummary(ctx, "narumq-node-1")
	if err != nil || node.Version != "1.0.0" || node.ConnectionCount != 0 || node.State != ports.NodeStateJoining || node.Ready {
		t.Fatalf("default node=%+v err=%v", node, err)
	}
	tenants, _, err := service.ListTenants(ctx, ports.TenantFilter{})
	if err != nil || len(tenants) != 1 {
		t.Fatalf("tenants=%+v err=%v", tenants, err)
	}
	if createdAt := tenants[0].CreatedAt; createdAt.Before(before) || createdAt.After(after) {
		t.Fatalf("defaulted start time %v outside [%v, %v]", createdAt, before, after)
	}
	if _, err := service.GetClient(ctx, "default", "client"); !errors.Is(err, ErrClientNotFound) {
		t.Fatalf("GetClient without session store err=%v", err)
	}
	clients, page, err := service.ListClients(ctx, ports.ClientFilter{})
	if err != nil || len(clients) != 0 || page.HasMore || page.NextCursor != nil {
		t.Fatalf("clients without store=%+v page=%+v err=%v", clients, page, err)
	}
	subscriptions, _, err := service.ListSubscriptions(ctx, ports.SubscriptionFilter{})
	if err != nil || len(subscriptions) != 0 {
		t.Fatalf("subscriptions without table=%+v err=%v", subscriptions, err)
	}
	retainedList, _, err := service.ListRetained(ctx, ports.RetainedFilter{})
	if err != nil || len(retainedList) != 0 {
		t.Fatalf("retained without repository=%+v err=%v", retainedList, err)
	}
}

func TestCmmNodeStateIsReflectedInSummaryAndReadiness(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		state      ports.NodeState
		wantCounts ports.NodesByStateDTO
		wantReason string
	}{
		{state: ports.NodeStateJoining, wantCounts: ports.NodesByStateDTO{Joining: 1}, wantReason: "node is not ready"},
		{state: ports.NodeStateDraining, wantCounts: ports.NodesByStateDTO{Draining: 1}, wantReason: "node is draining"},
		{state: ports.NodeStateSafeToStop, wantCounts: ports.NodesByStateDTO{SafeToStop: 1}, wantReason: "node is safe to stop"},
	}
	for _, testCase := range cases {
		t.Run(string(testCase.state), func(t *testing.T) {
			service := NewService(ServiceConfig{ClusterID: "c", NodeID: "n", ReleaseVersion: "v"}, nil, nil, nil, nil)
			service.SetNodeState(testCase.state, nil)
			summary := service.GetClusterSummary(ctx)
			if summary.NodesByState != testCase.wantCounts || summary.QuorumAvailable {
				t.Fatalf("nodesByState=%+v quorum=%v", summary.NodesByState, summary.QuorumAvailable)
			}
			status, reason := service.CheckReadiness(ctx)
			if status != ports.HealthStatusNotReady || reason == nil || *reason != testCase.wantReason {
				t.Fatalf("readiness status=%s reason=%v want %q", status, reason, testCase.wantReason)
			}
		})
	}
}

func TestCmmNodeUptimeIsClampedForFutureStartTime(t *testing.T) {
	ctx := context.Background()
	service := NewService(ServiceConfig{NodeID: "node-a", StartedAt: time.Now().Add(time.Hour)}, nil, nil, nil, nil)
	node, err := service.GetNodeSummary(ctx, "node-a")
	if err != nil || node.UptimeMs != 0 {
		t.Fatalf("future start node=%+v err=%v", node, err)
	}
	nodes, _, err := service.ListNodeSummaries(ctx, ports.NodeFilter{})
	if err != nil || len(nodes) != 1 || nodes[0].UptimeMs != 0 {
		t.Fatalf("future start list=%+v err=%v", nodes, err)
	}
}

func TestCmmListNodeSummariesFiltersByState(t *testing.T) {
	ctx := context.Background()
	service := NewService(ServiceConfig{NodeID: "node-a"}, nil, nil, nil, nil)
	reason := "warming caches"
	service.SetNodeState(ports.NodeStateDraining, &reason)

	nodes, page, err := service.ListNodeSummaries(ctx, ports.NodeFilter{State: ports.NodeStateReady})
	if err != nil || len(nodes) != 0 || page.HasMore {
		t.Fatalf("mismatched state nodes=%+v page=%+v err=%v", nodes, page, err)
	}
	nodes, _, err = service.ListNodeSummaries(ctx, ports.NodeFilter{State: ports.NodeStateDraining})
	if err != nil || len(nodes) != 1 || nodes[0].NodeID != "node-a" || nodes[0].ReadinessReason == nil || *nodes[0].ReadinessReason != reason {
		t.Fatalf("matching state nodes=%+v err=%v", nodes, err)
	}
}

func TestCmmListOperationsRejectInvalidPagingInput(t *testing.T) {
	ctx := context.Background()
	service, _, _, _, _ := setupTestService(t)
	lists := map[string]func(limit int, cursor string) error{
		"nodes": func(limit int, cursor string) error {
			_, _, err := service.ListNodeSummaries(ctx, ports.NodeFilter{Limit: limit, Cursor: cursor})
			return err
		},
		"clients": func(limit int, cursor string) error {
			_, _, err := service.ListClients(ctx, ports.ClientFilter{Limit: limit, Cursor: cursor})
			return err
		},
		"subscriptions": func(limit int, cursor string) error {
			_, _, err := service.ListSubscriptions(ctx, ports.SubscriptionFilter{Limit: limit, Cursor: cursor})
			return err
		},
		"retained": func(limit int, cursor string) error {
			_, _, err := service.ListRetained(ctx, ports.RetainedFilter{Limit: limit, Cursor: cursor})
			return err
		},
		"tenants": func(limit int, cursor string) error {
			_, _, err := service.ListTenants(ctx, ports.TenantFilter{Limit: limit, Cursor: cursor})
			return err
		},
	}
	cases := []struct {
		name   string
		limit  int
		cursor string
		want   error
	}{
		{name: "negative limit", limit: -1, want: ErrInvalidPageSize},
		{name: "limit above maximum", limit: MaxPageLimit + 1, want: ErrInvalidPageSize},
		{name: "maximum limit", limit: MaxPageLimit},
		{name: "cursor is not base64url", cursor: "not*base64", want: ErrInvalidCursor},
		{name: "cursor has wrong prefix", cursor: cmmCursor("off:1"), want: ErrInvalidCursor},
		{name: "cursor offset is not numeric", cursor: cmmCursor("cur:one"), want: ErrInvalidCursor},
		{name: "cursor offset is negative", cursor: cmmCursor("cur:-1"), want: ErrInvalidCursor},
		{name: "cursor beyond end", cursor: cmmCursor("cur:10")},
	}
	for name, list := range lists {
		for _, testCase := range cases {
			t.Run(name+"/"+testCase.name, func(t *testing.T) {
				if err := list(testCase.limit, testCase.cursor); !errors.Is(err, testCase.want) || (testCase.want == nil && err != nil) {
					t.Fatalf("err=%v want %v", err, testCase.want)
				}
			})
		}
	}

	nodes, page, err := service.ListNodeSummaries(ctx, ports.NodeFilter{Cursor: cmmCursor("cur:10")})
	if err != nil || len(nodes) != 0 || page.HasMore || page.NextCursor != nil {
		t.Fatalf("cursor beyond end nodes=%+v page=%+v err=%v", nodes, page, err)
	}
}

func TestCmmListClientsAppliesEveryFilterAndReportsExpiry(t *testing.T) {
	ctx := context.Background()
	service, store, _, _, _ := setupTestService(t)
	now := time.Date(2026, time.March, 1, 12, 0, 0, 0, time.UTC)
	cmmSaveSession(t, store, mqtt.Connect{ClientID: "alpha-1", Version: mqtt.ProtocolV5, SessionExpiryInterval: 300}, now, false, "tenant-a")
	cmmSaveSession(t, store, mqtt.Connect{ClientID: "beta-1", Version: mqtt.ProtocolV311}, now, false, "")
	cmmSaveSession(t, store, mqtt.Connect{ClientID: "alpha-2", Version: mqtt.ProtocolV5, SessionExpiryInterval: 60}, now, true, "")

	clientIDs := func(clients []ports.ClientSessionDTO) []string {
		ids := make([]string, 0, len(clients))
		for _, client := range clients {
			ids = append(ids, client.ClientID)
		}
		return ids
	}
	cases := []struct {
		name   string
		filter ports.ClientFilter
		want   []string
	}{
		{name: "all", filter: ports.ClientFilter{}, want: []string{"alpha-1", "alpha-2", "beta-1"}},
		{name: "tenant", filter: ports.ClientFilter{TenantID: "tenant-a"}, want: []string{"alpha-1"}},
		{name: "default tenant", filter: ports.ClientFilter{TenantID: "default"}, want: []string{"alpha-2", "beta-1"}},
		{name: "client prefix", filter: ports.ClientFilter{ClientIDPrefix: "alpha-"}, want: []string{"alpha-1", "alpha-2"}},
		{name: "this node", filter: ports.ClientFilter{NodeID: "test-node-1"}, want: []string{"alpha-1", "alpha-2", "beta-1"}},
		{name: "other node", filter: ports.ClientFilter{NodeID: "test-node-2"}, want: []string{}},
		{name: "mqtt 3.1.1", filter: ports.ClientFilter{Protocol: "MQTT_3_1_1"}, want: []string{"beta-1"}},
		{name: "mqtt 5", filter: ports.ClientFilter{Protocol: "MQTT_5_0"}, want: []string{"alpha-1", "alpha-2"}},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			clients, _, err := service.ListClients(ctx, testCase.filter)
			if err != nil {
				t.Fatal(err)
			}
			if got := clientIDs(clients); !reflect.DeepEqual(got, testCase.want) {
				t.Fatalf("clients=%v want %v", got, testCase.want)
			}
		})
	}

	clients, _, err := service.ListClients(ctx, ports.ClientFilter{})
	if err != nil {
		t.Fatal(err)
	}
	wantExpiry := now.Add(60 * time.Second)
	for _, client := range clients {
		switch client.ClientID {
		case "alpha-2":
			if client.Connected || client.ExpiryAt == nil || !client.ExpiryAt.Equal(wantExpiry) || client.TenantID != "default" {
				t.Fatalf("disconnected client=%+v expiry=%v want %v", client, client.ExpiryAt, wantExpiry)
			}
		case "beta-1":
			if client.Protocol != "MQTT_3_1_1" || client.ExpiryAt != nil {
				t.Fatalf("mqtt 3.1.1 client=%+v", client)
			}
		case "alpha-1":
			if client.TenantID != "tenant-a" || client.Protocol != "MQTT_5_0" || client.ExpiryAt != nil {
				t.Fatalf("tenant client=%+v", client)
			}
		}
	}
}

func TestCmmGetClientResolvesTenantProtocolAndExpiry(t *testing.T) {
	ctx := context.Background()
	service, store, _, _, _ := setupTestService(t)
	now := time.Date(2026, time.March, 1, 12, 0, 0, 0, time.UTC)
	cmmSaveSession(t, store, mqtt.Connect{ClientID: "tenant-client", Version: mqtt.ProtocolV5}, now, false, "tenant-a")
	cmmSaveSession(t, store, mqtt.Connect{ClientID: "legacy", Version: mqtt.ProtocolV311}, now, false, "")
	cmmSaveSession(t, store, mqtt.Connect{ClientID: "sleeping", Version: mqtt.ProtocolV5, SessionExpiryInterval: 30}, now, true, "")

	client, err := service.GetClient(ctx, "tenant-a", "tenant-client")
	if err != nil || client.TenantID != "tenant-a" || client.ClientID != "tenant-client" || client.NodeID == nil || *client.NodeID != "test-node-1" {
		t.Fatalf("tenant-scoped client=%+v err=%v", client, err)
	}
	client, err = service.GetClient(ctx, "", "tenant-client")
	if err != nil || client.TenantID != "tenant-a" {
		t.Fatalf("unscoped lookup client=%+v err=%v", client, err)
	}
	if _, err := service.GetClient(ctx, "default", "tenant-client"); !errors.Is(err, ErrClientNotFound) {
		t.Fatalf("cross-tenant lookup err=%v", err)
	}
	if _, err := service.GetClient(ctx, "Not A Tenant", "tenant-client"); !errors.Is(err, ErrClientNotFound) {
		t.Fatalf("invalid tenant lookup err=%v", err)
	}
	client, err = service.GetClient(ctx, "default", "legacy")
	if err != nil || client.Protocol != "MQTT_3_1_1" || client.ExpiryAt != nil || !client.Connected {
		t.Fatalf("legacy client=%+v err=%v", client, err)
	}
	client, err = service.GetClient(ctx, "default", "sleeping")
	if err != nil || client.Connected || client.ExpiryAt == nil || !client.ExpiryAt.Equal(now.Add(30*time.Second)) {
		t.Fatalf("sleeping client=%+v err=%v", client, err)
	}
}

func TestCmmListSubscriptionsFiltersAndOrdersByClientThenTopic(t *testing.T) {
	ctx := context.Background()
	tenantA := cmmTenantID(t, "tenant-a")
	group := "workers"
	snapshot := cmmSubscriptionSnapshot{
		{TenantID: tenantA, SessionID: cmmSessionID(t, tenantA, "c2"), TopicFilter: "b/#", QoS: mqtt.QoSAtLeastOnce},
		{SessionID: cmmSessionID(t, cmmTenantID(t, "default"), "c1"), TopicFilter: "z", QoS: mqtt.QoSAtMostOnce},
		{SessionID: cmmSessionID(t, cmmTenantID(t, "default"), "c1"), TopicFilter: "a", QoS: mqtt.QoSExactlyOnce, SharedGroup: &group},
		{TenantID: tenantA, SessionID: cmmSessionID(t, tenantA, "c1"), TopicFilter: "m", QoS: mqtt.QoSAtLeastOnce},
	}
	service := NewService(ServiceConfig{}, nil, snapshot, nil, nil)
	type key struct {
		tenant, client, topic string
	}
	collect := func(subscriptions []ports.SubscriptionDTO) []key {
		keys := make([]key, 0, len(subscriptions))
		for _, subscription := range subscriptions {
			keys = append(keys, key{subscription.TenantID, subscription.ClientID, subscription.TopicFilter})
		}
		return keys
	}
	cases := []struct {
		name   string
		filter ports.SubscriptionFilter
		want   []key
	}{
		{name: "all ordered", filter: ports.SubscriptionFilter{}, want: []key{{"default", "c1", "a"}, {"tenant-a", "c1", "m"}, {"default", "c1", "z"}, {"tenant-a", "c2", "b/#"}}},
		{name: "tenant", filter: ports.SubscriptionFilter{TenantID: "tenant-a"}, want: []key{{"tenant-a", "c1", "m"}, {"tenant-a", "c2", "b/#"}}},
		{name: "client", filter: ports.SubscriptionFilter{ClientID: "c2"}, want: []key{{"tenant-a", "c2", "b/#"}}},
		{name: "tenant and client", filter: ports.SubscriptionFilter{TenantID: "default", ClientID: "c2"}, want: []key{}},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			subscriptions, _, err := service.ListSubscriptions(ctx, testCase.filter)
			if err != nil {
				t.Fatal(err)
			}
			if got := collect(subscriptions); !reflect.DeepEqual(got, testCase.want) {
				t.Fatalf("subscriptions=%v want %v", got, testCase.want)
			}
		})
	}
	subscriptions, _, err := service.ListSubscriptions(ctx, ports.SubscriptionFilter{ClientID: "c1", Limit: 1})
	if err != nil || len(subscriptions) != 1 || subscriptions[0].QoS != int(mqtt.QoSExactlyOnce) || subscriptions[0].SharedGroup == nil || *subscriptions[0].SharedGroup != group {
		t.Fatalf("first c1 subscription=%+v err=%v", subscriptions, err)
	}
}

func TestCmmListSubscriptionsResolvesLegacyClientIDs(t *testing.T) {
	ctx := context.Background()
	tenantA := cmmTenantID(t, "tenant-a")
	snapshot := cmmSubscriptionSnapshot{
		{TenantID: tenantA, SessionID: "tenant-a/client/with/slash", TopicFilter: "tenant/#"},
		{SessionID: "raw-client", TopicFilter: "default/#"},
	}
	service := NewService(ServiceConfig{}, nil, snapshot, nil, nil)

	cases := []struct {
		name, tenant, client string
	}{
		{name: "tenant-prefixed ID", tenant: "tenant-a", client: "client/with/slash"},
		{name: "raw client ID", tenant: "default", client: "raw-client"},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			subscriptions, _, err := service.ListSubscriptions(ctx, ports.SubscriptionFilter{
				TenantID: testCase.tenant, ClientID: testCase.client,
			})
			if err != nil {
				t.Fatal(err)
			}
			if len(subscriptions) != 1 ||
				subscriptions[0].TenantID != testCase.tenant ||
				subscriptions[0].ClientID != testCase.client {
				t.Fatalf("subscriptions = %+v, want one entry for tenant %q client %q",
					subscriptions, testCase.tenant, testCase.client)
			}
		})
	}
}

func TestCmmListRetainedFiltersOrdersAndReportsExpiry(t *testing.T) {
	ctx := context.Background()
	expiresAt := time.Date(2026, time.April, 1, 0, 0, 0, 0, time.UTC)
	snapshot := cmmRetainedSnapshot{
		{Topic: "sensors/b", Payload: []byte("22"), QoS: mqtt.QoSAtLeastOnce, ExpiresAt: expiresAt},
		{Topic: "alerts/x", Payload: []byte("fire!")},
		{Topic: "sensors/a", Payload: []byte("1")},
	}
	service := NewService(ServiceConfig{}, nil, nil, snapshot, nil)

	all, page, err := service.ListRetained(ctx, ports.RetainedFilter{Limit: 2})
	if err != nil || len(all) != 2 || !page.HasMore || page.NextCursor == nil {
		t.Fatalf("first retained page=%+v page=%+v err=%v", all, page, err)
	}
	if all[0].Topic != "alerts/x" || all[0].PayloadBytes != 5 || all[1].Topic != "sensors/a" || all[1].ExpiresAt != nil {
		t.Fatalf("retained order=%+v", all)
	}
	rest, page, err := service.ListRetained(ctx, ports.RetainedFilter{Limit: 2, Cursor: *page.NextCursor})
	if err != nil || len(rest) != 1 || page.HasMore {
		t.Fatalf("second retained page=%+v page=%+v err=%v", rest, page, err)
	}
	if rest[0].Topic != "sensors/b" || rest[0].QoS != int(mqtt.QoSAtLeastOnce) || rest[0].ExpiresAt == nil || !rest[0].ExpiresAt.Equal(expiresAt) {
		t.Fatalf("expiring retained=%+v", rest[0])
	}

	prefixed, _, err := service.ListRetained(ctx, ports.RetainedFilter{TopicPrefix: "sensors/"})
	if err != nil || len(prefixed) != 2 || prefixed[0].Topic != "sensors/a" || prefixed[1].Topic != "sensors/b" {
		t.Fatalf("prefixed retained=%+v err=%v", prefixed, err)
	}
	otherTenant, _, err := service.ListRetained(ctx, ports.RetainedFilter{TenantID: "tenant-a"})
	if err != nil || len(otherTenant) != 0 {
		t.Fatalf("other tenant retained=%+v err=%v", otherTenant, err)
	}
}

// Traceability: PRD TEN-04/TEN-05; docs/specs/mqtt-protocol-and-session.md §5.
func TestCmmListRetainedUsesTenantLabelsAndFilter(t *testing.T) {
	ctx := context.Background()
	tenantA := cmmTenantID(t, "tenant-a")
	tenantB := cmmTenantID(t, "tenant-b")
	service := NewService(ServiceConfig{}, nil, nil, cmmRetainedSnapshot{
		{TenantID: tenantB, Topic: "shared/topic", Payload: []byte("b")},
		{TenantID: tenantA, Topic: "shared/topic", Payload: []byte("a")},
		{Topic: "shared/topic", Payload: []byte("default")},
	}, nil)

	all, _, err := service.ListRetained(ctx, ports.RetainedFilter{})
	if err != nil || len(all) != 3 ||
		all[0].TenantID != "default" || all[1].TenantID != "tenant-a" || all[2].TenantID != "tenant-b" {
		t.Fatalf("tenant-sorted retained list = %+v, %v", all, err)
	}
	filtered, _, err := service.ListRetained(ctx, ports.RetainedFilter{TenantID: "tenant-b"})
	if err != nil || len(filtered) != 1 || filtered[0].TenantID != "tenant-b" || filtered[0].PayloadBytes != 1 {
		t.Fatalf("tenant-b retained filter = %+v, %v", filtered, err)
	}
	defaultTenant, _, err := service.ListRetained(ctx, ports.RetainedFilter{TenantID: "default"})
	if err != nil || len(defaultTenant) != 1 || defaultTenant[0].TenantID != "default" {
		t.Fatalf("default tenant retained filter = %+v, %v", defaultTenant, err)
	}
}
