package management

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	adaptersmetrics "narumq/internal/adapters/metrics"
	appmgmt "narumq/internal/application/management"
	ports "narumq/internal/application/ports/management"
	portsmetrics "narumq/internal/application/ports/metrics"
	observationports "narumq/internal/application/ports/observability"
)

type mockStateProvider struct {
	cluster     ports.ClusterSummaryDTO
	nodes       []ports.NodeSummaryDTO
	clients     []ports.ClientSessionDTO
	subs        []ports.SubscriptionDTO
	retained    []ports.RetainedMetadataDTO
	tenants     []ports.TenantSummaryDTO
	nodesErr    error
	clientsErr  error
	subsErr     error
	retainedErr error
	tenantsErr  error
}

func (m *mockStateProvider) GetClusterSummary(ctx context.Context) ports.ClusterSummaryDTO {
	return m.cluster
}
func (m *mockStateProvider) GetNodeSummary(ctx context.Context, nodeID string) (*ports.NodeSummaryDTO, error) {
	for _, n := range m.nodes {
		if n.NodeID == nodeID {
			return &n, nil
		}
	}
	return nil, appmgmt.ErrNodeNotFound
}
func (m *mockStateProvider) ListNodeSummaries(ctx context.Context, filter ports.NodeFilter) ([]ports.NodeSummaryDTO, *ports.PageDTO, error) {
	if m.nodesErr != nil {
		return nil, nil, m.nodesErr
	}
	return m.nodes, &ports.PageDTO{NextCursor: nil, HasMore: false}, nil
}
func (m *mockStateProvider) ListClients(ctx context.Context, filter ports.ClientFilter) ([]ports.ClientSessionDTO, *ports.PageDTO, error) {
	if m.clientsErr != nil {
		return nil, nil, m.clientsErr
	}
	if filter.Cursor == "bad" {
		return nil, nil, appmgmt.ErrInvalidCursor
	}
	return m.clients, &ports.PageDTO{NextCursor: nil, HasMore: false}, nil
}
func (m *mockStateProvider) GetClient(ctx context.Context, tenantID, clientID string) (*ports.ClientSessionDTO, error) {
	for _, c := range m.clients {
		if c.TenantID == tenantID && c.ClientID == clientID {
			return &c, nil
		}
	}
	return nil, appmgmt.ErrClientNotFound
}
func (m *mockStateProvider) ListSubscriptions(ctx context.Context, filter ports.SubscriptionFilter) ([]ports.SubscriptionDTO, *ports.PageDTO, error) {
	if m.subsErr != nil {
		return nil, nil, m.subsErr
	}
	return m.subs, &ports.PageDTO{NextCursor: nil, HasMore: false}, nil
}
func (m *mockStateProvider) ListRetained(ctx context.Context, filter ports.RetainedFilter) ([]ports.RetainedMetadataDTO, *ports.PageDTO, error) {
	if m.retainedErr != nil {
		return nil, nil, m.retainedErr
	}
	return m.retained, &ports.PageDTO{NextCursor: nil, HasMore: false}, nil
}
func (m *mockStateProvider) ListTenants(ctx context.Context, filter ports.TenantFilter) ([]ports.TenantSummaryDTO, *ports.PageDTO, error) {
	if m.tenantsErr != nil {
		return nil, nil, m.tenantsErr
	}
	return m.tenants, &ports.PageDTO{NextCursor: nil, HasMore: false}, nil
}

type mockHealthChecker struct {
	liveStatus  ports.HealthStatus
	liveReason  *string
	readyStatus ports.HealthStatus
	readyReason *string
	startStatus ports.HealthStatus
	startReason *string
	drainStatus ports.HealthStatus
	drainReason *string
}

func (m *mockHealthChecker) CheckLiveness(ctx context.Context) (ports.HealthStatus, *string) {
	return m.liveStatus, m.liveReason
}
func (m *mockHealthChecker) CheckReadiness(ctx context.Context) (ports.HealthStatus, *string) {
	return m.readyStatus, m.readyReason
}
func (m *mockHealthChecker) CheckStartup(ctx context.Context) (ports.HealthStatus, *string) {
	return m.startStatus, m.startReason
}
func (m *mockHealthChecker) CheckDrain(ctx context.Context) (ports.HealthStatus, *string) {
	return m.drainStatus, m.drainReason
}

// Scenarios: management-api-and-observability.md:MGT-SCN-020
func TestHealthEndpoints(t *testing.T) {
	reason := "initializing consensus"
	checker := &mockHealthChecker{
		liveStatus:  ports.HealthStatusUp,
		readyStatus: ports.HealthStatusNotReady,
		readyReason: &reason,
		startStatus: ports.HealthStatusStarting,
		drainStatus: ports.HealthStatusNotReady,
	}

	handler := NewRouter(ServerConfig{AllowAnonymousProbes: true}, &mockStateProvider{}, checker, nil)

	// 1. /health/live -> 200
	req := httptest.NewRequest("GET", "/health/live", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 for live, got %d", rec.Code)
	}
	if rec.Header().Get(HeaderRequestID) == "" {
		t.Errorf("missing X-Request-Id header")
	}

	var liveResp struct {
		Data struct {
			Status string `json:"status"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &liveResp); err != nil {
		t.Fatalf("invalid json: %v", err)
	}
	if liveResp.Data.Status != "UP" {
		t.Errorf("expected status UP, got %s", liveResp.Data.Status)
	}

	// 2. /health/ready -> 503
	req = httptest.NewRequest("GET", "/health/ready", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 for ready, got %d", rec.Code)
	}
	var errResp ErrorResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &errResp); err != nil {
		t.Fatalf("invalid error json: %v", err)
	}
	if errResp.Error.Code != "unavailable" || !errResp.Error.Retryable {
		t.Errorf("unexpected error payload: %+v", errResp)
	}
	// 3. /health/startup and /health/drain expose unavailable probe details.
	for _, path := range []string{"/health/startup", "/health/drain"} {
		req = httptest.NewRequest("GET", path, nil)
		rec = httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("expected 503 for %s, got %d", path, rec.Code)
		}
	}

	// 4. SAFE_TO_STOP remains a successful health response.
	checker.startStatus = ports.HealthStatusUp
	checker.drainStatus = ports.HealthStatusSafeToStop
	for _, path := range []string{"/health/startup", "/health/drain"} {
		req = httptest.NewRequest("GET", path, nil)
		rec = httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200 for safe %s, got %d", path, rec.Code)
		}
	}
}

// Traceability: API-01; docs/specs/management-api-and-observability.md §9.
// Scenarios: management-api-and-observability.md:MGT-SCN-025
func TestMetricsEndpoint(t *testing.T) {
	reg := adaptersmetrics.NewRegistry()
	reg.ConnectionOpened(portsmetrics.ProtocolMQTT50, "tcp")

	checker := &mockHealthChecker{liveStatus: ports.HealthStatusUp}
	handler := NewRouter(ServerConfig{AllowAnonymousMetrics: true}, &mockStateProvider{}, checker, reg)

	// 1. Plain text scrape
	req := httptest.NewRequest("GET", "/metrics", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if !strings.Contains(rec.Header().Get("Content-Type"), "text/plain") {
		t.Errorf("expected text/plain content type, got %s", rec.Header().Get("Content-Type"))
	}
	if strings.Contains(rec.Body.String(), "# EOF") {
		t.Errorf("prometheus format should not have # EOF")
	}

	// 2. OpenMetrics scrape
	req = httptest.NewRequest("GET", "/metrics", nil)
	req.Header.Set("Accept", "application/openmetrics-text; version=1.0.0")
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if !strings.Contains(rec.Header().Get("Content-Type"), "application/openmetrics-text") {
		t.Errorf("expected openmetrics content type, got %s", rec.Header().Get("Content-Type"))
	}
	if !strings.HasSuffix(rec.Body.String(), "# EOF\n") {
		t.Errorf("openmetrics format must end with # EOF\n")
	}
}

func TestManagementV1Endpoints(t *testing.T) {
	leader := "node-1"
	provider := &mockStateProvider{
		cluster: ports.ClusterSummaryDTO{
			ClusterID:       "c-1",
			ReleaseVersion:  "1.0.0",
			MemberCount:     1,
			QuorumSize:      1,
			QuorumAvailable: true,
			LeaderNodeID:    &leader,
		},
		nodes: []ports.NodeSummaryDTO{
			{NodeID: "node-1", Version: "1.0.0", State: ports.NodeStateReady, Ready: true},
		},
		clients:  []ports.ClientSessionDTO{},
		subs:     []ports.SubscriptionDTO{},
		retained: []ports.RetainedMetadataDTO{},
		tenants: []ports.TenantSummaryDTO{
			{TenantID: "default", Status: "ACTIVE", Version: 1, CreatedAt: time.Now().UTC()},
		},
	}
	checker := &mockHealthChecker{liveStatus: ports.HealthStatusUp}
	observability := successHTTPObservabilityStub()
	handler := NewRouter(ServerConfig{Observability: observability}, provider, checker, nil)

	// 1. GET /api/v1/cluster
	req := httptest.NewRequest("GET", "/api/v1/cluster", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 for cluster, got %d: %s", rec.Code, rec.Body.String())
	}
	var clusterResp struct {
		Data observationports.ClusterDataDTO `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &clusterResp); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}
	if clusterResp.Data.ClusterID != "c-1" {
		t.Errorf("unexpected cluster ID: %s", clusterResp.Data.ClusterID)
	}

	// 2. GET /api/v1/nodes/node-1 (found) and node-2 (not found)
	req = httptest.NewRequest("GET", "/api/v1/nodes/node-1", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 for node-1, got %d", rec.Code)
	}

	req = httptest.NewRequest("GET", "/api/v1/nodes/node-2", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for node-2, got %d", rec.Code)
	}

	// 3. GET /api/v1/clients (empty array serialization check)
	req = httptest.NewRequest("GET", "/api/v1/clients", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 for clients, got %d", rec.Code)
	}
	bodyStr := rec.Body.String()
	if !strings.Contains(bodyStr, `"data":[]`) {
		t.Errorf("empty list must serialize to '\"data\":[]', got %s", bodyStr)
	}
	if !strings.Contains(bodyStr, `"nextCursor":null`) {
		t.Errorf("absent nextCursor must serialize to '\"nextCursor\":null', got %s", bodyStr)
	}

	// 4. Invalid cursor -> 400
	req = httptest.NewRequest("GET", "/api/v1/clients?cursor=bad", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for bad cursor, got %d", rec.Code)
	}

	// 5. GET /api/v1/tenants
	req = httptest.NewRequest("GET", "/api/v1/tenants", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 for tenants, got %d", rec.Code)
	}
	req = httptest.NewRequest(http.MethodGet, "/not-found", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("unknown management path status = %d, want 404", rec.Code)
	}
}

// Scenarios: management-api-and-observability.md:MGT-SCN-003
func TestAuthAndRBAC(t *testing.T) {
	provider := &mockStateProvider{}
	checker := &mockHealthChecker{liveStatus: ports.HealthStatusUp}
	cfg := ServerConfig{
		AuthToken:            "secret-token-123",
		APIKeys:              []APIKeyCredential{{ID: "test", Key: "api-key-456", Roles: []Role{RoleReadOnly}}},
		AllowAnonymousProbes: true,
		Observability:        successHTTPObservabilityStub(),
	}
	handler := NewRouter(cfg, provider, checker, nil)

	// 1. Health probe should pass anonymously
	req := httptest.NewRequest("GET", "/health/live", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 for anonymous probe, got %d", rec.Code)
	}

	// 2. /api/v1/cluster without auth -> 401
	req = httptest.NewRequest("GET", "/api/v1/cluster", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 without auth, got %d", rec.Code)
	}

	// 3. /api/v1/cluster with Bearer token -> 200
	req = httptest.NewRequest("GET", "/api/v1/cluster", nil)
	req.Header.Set("Authorization", "Bearer secret-token-123")
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 with Bearer token, got %d", rec.Code)
	}

	// 4. /api/v1/cluster with API Key -> 200
	req = httptest.NewRequest("GET", "/api/v1/cluster", nil)
	req.Header.Set("X-API-Key", "api-key-456")
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 with API Key, got %d", rec.Code)
	}

	// 5. Wrong token -> 401
	req = httptest.NewRequest("GET", "/api/v1/cluster", nil)
	req.Header.Set("Authorization", "Bearer wrong-token")
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 with wrong token, got %d", rec.Code)
	}
}

// Scenarios: management-api-and-observability.md:MGT-SCN-003
func TestRBACReturnsForbiddenForInsufficientRole(t *testing.T) {
	handler := NewRouter(ServerConfig{
		APIKeys: []APIKeyCredential{{ID: "test", Key: "read-only-token", Roles: []Role{RoleReadOnly}}},
	}, &mockStateProvider{}, &mockHealthChecker{liveStatus: ports.HealthStatusUp}, nil)

	req := httptest.NewRequest("GET", "/api/v1/tenants", nil)
	req.Header.Set("X-API-Key", "read-only-token")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403 for read-only tenant listing, got %d", rec.Code)
	}
}

// Scenarios: management-api-and-observability.md:MGT-SCN-002
func TestManagementCollectionEndpoints(t *testing.T) {
	leader := "node-1"
	provider := &mockStateProvider{
		cluster: ports.ClusterSummaryDTO{
			ClusterID:       "c-1",
			ReleaseVersion:  "1.0.0",
			MemberCount:     1,
			QuorumSize:      1,
			QuorumAvailable: true,
			LeaderNodeID:    &leader,
		},
		nodes: []ports.NodeSummaryDTO{
			{NodeID: "node-1", State: ports.NodeStateReady, Ready: true},
		},
		clients: []ports.ClientSessionDTO{
			{TenantID: "tenant-a", ClientID: "client-a", Protocol: "MQTT_5_0"},
		},
		subs: []ports.SubscriptionDTO{
			{TenantID: "tenant-a", ClientID: "client-a", TopicFilter: "sensors/#"},
		},
		retained: []ports.RetainedMetadataDTO{
			{TenantID: "tenant-a", Topic: "sensors/temp", PayloadBytes: 4},
		},
		tenants: []ports.TenantSummaryDTO{
			{TenantID: "tenant-a", Status: "ACTIVE"},
		},
	}
	handler := NewRouter(
		ServerConfig{AllowAnonymousProbes: true, AllowAnonymousMetrics: true, Observability: successHTTPObservabilityStub()},
		provider,
		&mockHealthChecker{liveStatus: ports.HealthStatusUp},
		nil,
	)

	paths := []string{
		"/api/v1/nodes?state=READY&limit=1",
		"/api/v1/clients?tenantId=tenant-a&clientIdPrefix=client&nodeId=node-1&protocol=MQTT_5_0&limit=1",
		"/api/v1/clients/tenant-a/client-a",
		"/api/v1/subscriptions?tenantId=tenant-a&clientId=client-a&limit=1",
		"/api/v1/retained?tenantId=tenant-a&topicPrefix=sensors/&limit=1",
		"/api/v1/tenants?limit=1",
	}
	for _, path := range paths {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("GET %s returned %d: %s", path, rec.Code, rec.Body.String())
		}
	}

	for _, path := range []string{
		"/api/v1/nodes?state=invalid",
		"/api/v1/nodes?limit=not-a-number",
		"/api/v1/nodes?unknown=value",
		"/api/v1/clients?limit=0",
		"/api/v1/clients?limit=1&limit=2",
		"/api/v1/clients?protocol=invalid",
		"/api/v1/subscriptions",
		"/api/v1/subscriptions?limit=0&tenantId=tenant-a",
		"/api/v1/retained",
		"/api/v1/retained?limit=-1&tenantId=tenant-a",
		"/api/v1/tenants?limit=0",
		"/api/v1/cluster?unknown=value",
		"/api/v1/nodes/node-1?unknown=value",
		"/api/v1/clients/tenant-a/client-a?unknown=value",
		"/api/v1/subscriptions?tenantId=",
		"/api/v1/retained?tenantId=",
		"/api/v1/nodes?limit=",
		"/api/v1/nodes?cursor=",
		"/api/v1/nodes?state=",
		"/api/v1/clients?cursor=",
		"/api/v1/clients?tenantId=",
		"/api/v1/clients?clientIdPrefix=",
		"/api/v1/clients?nodeId=",
		"/api/v1/clients?protocol=",
		"/api/v1/subscriptions?unknown=value",
		"/api/v1/subscriptions?tenantId=tenant-a&clientId=",
		"/api/v1/retained?unknown=value",
		"/api/v1/retained?tenantId=tenant-a&topicPrefix=",
		"/api/v1/tenants?unknown=value",
	} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("GET %s returned %d, want 400", path, rec.Code)
		}
		var response ErrorResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
			t.Fatalf("GET %s returned invalid error response: %v", path, err)
		}
		if response.Error.Code != "invalid_request" {
			t.Fatalf("GET %s error code = %q, want invalid_request", path, response.Error.Code)
		}
	}

	overLimitRequest := httptest.NewRequest(http.MethodGet, "/api/v1/clients?limit=1001", nil)
	overLimitRecorder := httptest.NewRecorder()
	handler.ServeHTTP(overLimitRecorder, overLimitRequest)
	if overLimitRecorder.Code != http.StatusBadRequest {
		t.Fatalf("over-limit status = %d, want 400", overLimitRecorder.Code)
	}
	var pageSizeResponse ErrorResponse
	if err := json.Unmarshal(overLimitRecorder.Body.Bytes(), &pageSizeResponse); err != nil {
		t.Fatalf("over-limit returned invalid error response: %v", err)
	}
	if pageSizeResponse.Error.Code != "invalid_request" {
		t.Fatalf("over-limit error code = %q, want invalid_request", pageSizeResponse.Error.Code)
	}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/clients/tenant-a/missing", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("missing client returned %d, want 404", rec.Code)
	}

}

func TestManagementMethodAndEnvelopeContracts(t *testing.T) {
	provider := &mockStateProvider{}
	v1 := NewV1Handler(provider)
	request := httptest.NewRequest(http.MethodPost, "/", nil)
	handlerCases := []struct {
		name string
		fn   http.HandlerFunc
	}{
		{name: "cluster", fn: v1.GetCluster},
		{name: "nodes", fn: v1.ListNodes},
		{name: "node", fn: v1.GetNode},
		{name: "clients", fn: v1.ListClients},
		{name: "client", fn: v1.GetClient},
		{name: "subscriptions", fn: v1.ListSubscriptions},
		{name: "retained", fn: v1.ListRetained},
		{name: "tenants", fn: v1.ListTenants},
	}
	for _, testCase := range handlerCases {
		t.Run(testCase.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			testCase.fn(rec, request.Clone(request.Context()))
			if rec.Code != http.StatusMethodNotAllowed {
				t.Fatalf("status = %d, want 405", rec.Code)
			}
		})
	}

	checker := &mockHealthChecker{
		liveStatus:  ports.HealthStatusUp,
		readyStatus: ports.HealthStatusUp,
		startStatus: ports.HealthStatusUp,
		drainStatus: ports.HealthStatusUp,
	}
	health := NewHealthHandler(checker)
	healthHandlers := []struct {
		name string
		fn   http.HandlerFunc
	}{
		{name: "live", fn: health.Live},
		{name: "ready", fn: health.Ready},
		{name: "startup", fn: health.Startup},
		{name: "drain", fn: health.Drain},
	}
	for _, testCase := range healthHandlers {
		t.Run("health-"+testCase.name+"-method", func(t *testing.T) {
			rec := httptest.NewRecorder()
			testCase.fn(rec, request.Clone(request.Context()))
			if rec.Code != http.StatusMethodNotAllowed {
				t.Fatalf("status = %d, want 405", rec.Code)
			}
		})
		t.Run("health-"+testCase.name+"-query", func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/?unknown=value", nil)
			rec := httptest.NewRecorder()
			testCase.fn(rec, req)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400", rec.Code)
			}
		})
	}

	metrics := NewMetricsHandler(nil)
	rec := httptest.NewRecorder()
	metrics.ServeHTTP(rec, request.Clone(request.Context()))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("metrics status = %d, want 405", rec.Code)
	}

	req := httptest.NewRequest(http.MethodGet, "/?unknown=value", nil)
	rec = httptest.NewRecorder()
	metrics.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("metrics unknown query status = %d, want 400", rec.Code)
	}

	getRequest := httptest.NewRequest(http.MethodGet, "/", nil)
	rec = httptest.NewRecorder()
	metrics.ServeHTTP(rec, getRequest)
	if rec.Code != http.StatusOK || rec.Header().Get("Content-Type") != ContentTypeMetrics {
		t.Fatalf("nil metrics GET = %d, %q", rec.Code, rec.Header().Get("Content-Type"))
	}

	registry := adaptersmetrics.NewRegistry()
	registry.PacketReceived("CONNECT")
	openMetrics := NewMetricsHandler(registry)
	openMetricsRequest := httptest.NewRequest(http.MethodGet, "/", nil)
	openMetricsRequest.Header.Set("Accept", "application/openmetrics-text; version=1.0.0")
	rec = httptest.NewRecorder()
	openMetrics.ServeHTTP(rec, openMetricsRequest)
	if rec.Code != http.StatusOK || !strings.HasSuffix(rec.Body.String(), "# EOF\n") {
		t.Fatalf("OpenMetrics response = %d, %q", rec.Code, rec.Body.String())
	}

	for _, testCase := range []struct {
		name string
		fn   http.HandlerFunc
	}{
		{name: "node-missing-path", fn: v1.GetNode},
		{name: "client-missing-path", fn: v1.GetClient},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			testCase.fn(rec, getRequest.Clone(getRequest.Context()))
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400", rec.Code)
			}
		})
	}

	rec = httptest.NewRecorder()
	WriteListEnvelope(rec, getRequest, http.StatusOK, nil, nil)
	if !strings.Contains(rec.Body.String(), `"data":[]`) {
		t.Fatalf("nil list data was not encoded as an array: %s", rec.Body.String())
	}

	var typedNil []ports.ClientSessionDTO
	rec = httptest.NewRecorder()
	listRequest := httptest.NewRequest(http.MethodGet, "/", nil)
	listRequest.Header.Set(HeaderRequestID, "stable-request-id")
	WriteListEnvelope(rec, listRequest, http.StatusOK, typedNil, nil)
	if rec.Header().Get(HeaderRequestID) != "stable-request-id" {
		t.Fatalf("request ID was not preserved")
	}
	if !strings.Contains(rec.Body.String(), `"data":[]`) || !strings.Contains(rec.Body.String(), `"nextCursor":null`) {
		t.Fatalf("unexpected typed-nil list envelope: %s", rec.Body.String())
	}

	invalidIDRequest := httptest.NewRequest(http.MethodGet, "/", nil)
	invalidIDRequest.Header.Set(HeaderRequestID, strings.Repeat("x", 129))
	if generated := GetOrGenerateRequestID(invalidIDRequest); generated == strings.Repeat("x", 129) || generated == "" {
		t.Fatalf("invalid request ID was not replaced")
	}
	controlIDRequest := httptest.NewRequest(http.MethodGet, "/", nil)
	controlIDRequest.Header.Set(HeaderRequestID, "bad\nid")
	if generated := GetOrGenerateRequestID(controlIDRequest); generated == "bad\nid" || generated == "" {
		t.Fatalf("control-character request ID was not replaced")
	}

	fallbackID := generateRequestID(func([]byte) (int, error) {
		return 0, errors.New("entropy unavailable")
	})
	if !strings.HasPrefix(fallbackID, "req_") {
		t.Fatalf("fallback request ID = %q", fallbackID)
	}

	for _, err := range []error{
		appmgmt.ErrNodeNotFound,
		appmgmt.ErrClientNotFound,
		appmgmt.ErrInvalidCursor,
		appmgmt.ErrInvalidPageSize,
		errors.New("unexpected"),
	} {
		rec = httptest.NewRecorder()
		v1.handleError(rec, httptest.NewRequest(http.MethodGet, "/", nil), err)

		if rec.Code == http.StatusOK {
			t.Fatalf("error %v produced 200", err)
		}
	}
}
func TestManagementAuthMiddlewareModes(t *testing.T) {
	nextCalled := false
	next := func(w http.ResponseWriter, r *http.Request) {
		nextCalled = true
		w.WriteHeader(http.StatusNoContent)
	}

	auth := NewAuthenticator(AuthConfig{
		APIKeys: []APIKeyCredential{{ID: "readonly", Key: "read-only", Roles: []Role{RoleReadOnly}}},
	})
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	auth.MaybeAllowProbe(next)(rec, req)
	if rec.Code != http.StatusUnauthorized || nextCalled {
		t.Fatalf("unauthenticated probe = %d, nextCalled=%v", rec.Code, nextCalled)
	}

	req = httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-API-Key", "read-only")
	rec = httptest.NewRecorder()
	auth.MaybeAllowMetrics(next)(rec, req)
	if rec.Code != http.StatusNoContent || !nextCalled {
		t.Fatalf("authenticated metrics = %d, nextCalled=%v", rec.Code, nextCalled)
	}

	anonymous := NewAuthenticator(AuthConfig{})
	anonymousCalled := false
	anonymous.RequirePermission(PermissionClusterAdmin, func(http.ResponseWriter, *http.Request) {
		anonymousCalled = true
	})(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
	if !anonymousCalled {
		t.Fatal("unauthenticated configuration did not admit ClusterAdmin permission")
	}
}

func TestManagementProviderErrorsBecomeInternalErrors(t *testing.T) {
	checker := &mockHealthChecker{liveStatus: ports.HealthStatusUp}
	for _, testCase := range []struct {
		name          string
		path          string
		provider      *mockStateProvider
		observability *httpObservabilityStub
	}{
		{name: "nodes", path: "/api/v1/nodes", observability: &httpObservabilityStub{listNodesErr: errors.New("nodes")}},
		{name: "clients", path: "/api/v1/clients", provider: &mockStateProvider{clientsErr: errors.New("clients")}},
		{name: "subscriptions", path: "/api/v1/subscriptions?tenantId=tenant-a", provider: &mockStateProvider{subsErr: errors.New("subscriptions")}},
		{name: "retained", path: "/api/v1/retained?tenantId=tenant-a", provider: &mockStateProvider{retainedErr: errors.New("retained")}},
		{name: "tenants", path: "/api/v1/tenants", provider: &mockStateProvider{tenantsErr: errors.New("tenants")}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			handler := NewRouter(ServerConfig{Observability: testCase.observability}, testCase.provider, checker, nil)
			req := httptest.NewRequest(http.MethodGet, testCase.path, nil)
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			if rec.Code != http.StatusInternalServerError {
				t.Fatalf("GET %s returned %d, want 500", testCase.path, rec.Code)
			}
		})
	}
	direct := NewV1Handler(&mockStateProvider{clientsErr: errors.New("clients")})
	rec := httptest.NewRecorder()
	direct.ListClients(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("direct client error status = %d, want 500", rec.Code)
	}
}

func TestNewServerAppliesDefaults(t *testing.T) {
	server := NewServer(
		ServerConfig{},
		&mockStateProvider{},
		&mockHealthChecker{liveStatus: ports.HealthStatusUp},
		nil,
	)
	if server.Addr != ":8080" {
		t.Fatalf("default management address = %q, want :8080", server.Addr)
	}
	if server.ReadTimeout != 10*time.Second || server.WriteTimeout != 10*time.Second {
		t.Fatalf("default management timeouts = (%s, %s), want 10s/10s", server.ReadTimeout, server.WriteTimeout)
	}
}
func TestNewServerPreservesConfiguredAddressAndTimeouts(t *testing.T) {
	server := NewServer(
		ServerConfig{
			ListenAddr: ":9090", ReadTimeout: 3 * time.Second, WriteTimeout: 4 * time.Second,
		},
		&mockStateProvider{},
		&mockHealthChecker{liveStatus: ports.HealthStatusUp},
		nil,
	)
	if server.Addr != ":9090" || server.ReadTimeout != 3*time.Second || server.WriteTimeout != 4*time.Second {
		t.Fatalf("configured server = addr %q, timeouts (%s, %s)", server.Addr, server.ReadTimeout, server.WriteTimeout)
	}
}
