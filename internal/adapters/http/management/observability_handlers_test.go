package management

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	managementports "narumq/internal/application/ports/management"
	observationports "narumq/internal/application/ports/observability"
)

type httpObservabilityStub struct {
	cluster           observationports.ClusterDataDTO
	nodes             []observationports.NodeDataDTO
	nodeByID          map[string]observationports.NodeDataDTO
	nodeMetricsByID   map[string]observationports.NodeMetricsDataDTO
	metrics           observationports.MetricsCollectionDataDTO
	clusterErr        error
	listNodesErr      error
	getNodeErr        error
	getNodeMetricsErr error
	collectMetricsErr error
}

func (stub *httpObservabilityStub) GetCluster(context.Context) (observationports.ClusterDataDTO, error) {
	return stub.cluster, stub.clusterErr
}

func (stub *httpObservabilityStub) GetNode(_ context.Context, nodeID string) (observationports.NodeDataDTO, error) {
	if stub.getNodeErr != nil {
		return observationports.NodeDataDTO{}, stub.getNodeErr
	}
	if node, exists := stub.nodeByID[nodeID]; exists {
		return node, nil
	}
	return observationports.NodeDataDTO{}, observationports.ErrNodeNotFound
}

func (stub *httpObservabilityStub) ListNodes(context.Context, observationports.NodeFilter) ([]observationports.NodeDataDTO, *managementports.PageDTO, error) {
	if stub.listNodesErr != nil {
		return nil, nil, stub.listNodesErr
	}
	return stub.nodes, &managementports.PageDTO{HasMore: false}, nil
}

func (stub *httpObservabilityStub) GetNodeMetrics(_ context.Context, nodeID string) (observationports.NodeMetricsDataDTO, error) {
	if stub.getNodeMetricsErr != nil {
		return observationports.NodeMetricsDataDTO{}, stub.getNodeMetricsErr
	}
	if data, exists := stub.nodeMetricsByID[nodeID]; exists {
		return data, nil
	}
	return observationports.NodeMetricsDataDTO{}, observationports.ErrNodeNotFound
}

func (stub *httpObservabilityStub) CollectMetrics(context.Context, observationports.MetricsMode) (observationports.MetricsCollectionDataDTO, error) {
	if stub.collectMetricsErr != nil {
		return observationports.MetricsCollectionDataDTO{}, stub.collectMetricsErr
	}
	return stub.metrics, nil
}

func successHTTPObservabilityStub() *httpObservabilityStub {
	return &httpObservabilityStub{
		cluster: observationports.ClusterDataDTO{ClusterID: "c-1", ReleaseVersion: "1.0.0"},
		nodeByID: map[string]observationports.NodeDataDTO{
			"node-1": {NodeID: "node-1", QueryStatus: observationports.QueryStatusOK},
		},
		metrics: observationports.MetricsCollectionDataDTO{
			Mode:     observationports.MetricsModeAllNodesUnaggregated,
			Complete: true,
			Nodes:    []observationports.MetricsNodeStatusDTO{},
			Families: []observationports.MetricFamilyDTO{},
		},
		nodeMetricsByID: map[string]observationports.NodeMetricsDataDTO{},
	}
}

func TestObservabilityMetricsAPIEndpoints(t *testing.T) {
	queryError := observationports.QueryErrorNodeUnreachable
	stub := successHTTPObservabilityStub()
	stub.nodeMetricsByID["node-1"] = observationports.NodeMetricsDataDTO{
		NodeID:         "node-1",
		QueryStatus:    observationports.QueryStatusUnreachable,
		QueryErrorCode: &queryError,
		Families:       nil,
	}
	stub.metrics = observationports.MetricsCollectionDataDTO{
		Mode:     observationports.MetricsModeAllNodesUnaggregated,
		Complete: false,
		Nodes: []observationports.MetricsNodeStatusDTO{{
			NodeID:         "node-2",
			QueryStatus:    observationports.QueryStatusUnreachable,
			QueryErrorCode: &queryError,
		}},
		Families: []observationports.MetricFamilyDTO{},
	}
	handler := NewRouter(ServerConfig{
		APIKeys:       []APIKeyCredential{{ID: "read", Key: "read-key", Roles: []Role{RoleReadOnly}}},
		Observability: stub,
	}, &mockStateProvider{}, &mockHealthChecker{liveStatus: managementports.HealthStatusUp}, nil)

	request := httptest.NewRequest(http.MethodGet, "/api/v1/metrics", nil)
	request.Header.Set("X-API-Key", "read-key")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("default metrics collection status=%d body=%s", response.Code, response.Body.String())
	}
	var collection struct {
		Data observationports.MetricsCollectionDataDTO `json:"data"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &collection); err != nil {
		t.Fatalf("decode metrics collection: %v", err)
	}
	if collection.Data.Mode != observationports.MetricsModeAllNodesUnaggregated || collection.Data.Complete || len(collection.Data.Nodes) != 1 {
		t.Fatalf("default metrics collection=%+v", collection.Data)
	}

	request = httptest.NewRequest(http.MethodGet, "/api/v1/nodes/node-1/metrics", nil)
	request.Header.Set("X-API-Key", "read-key")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("node metrics status=%d body=%s", response.Code, response.Body.String())
	}
	var nodeMetrics struct {
		Data observationports.NodeMetricsDataDTO `json:"data"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &nodeMetrics); err != nil {
		t.Fatalf("decode node metrics: %v", err)
	}
	if nodeMetrics.Data.QueryStatus != observationports.QueryStatusUnreachable || nodeMetrics.Data.Families != nil {
		t.Fatalf("node metrics failure must retain status and null families: %+v", nodeMetrics.Data)
	}

	request = httptest.NewRequest(http.MethodGet, "/api/v1/metrics?mode=invalid", nil)
	request.Header.Set("X-API-Key", "read-key")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("invalid metrics mode status=%d body=%s", response.Code, response.Body.String())
	}

	stub.collectMetricsErr = observationports.ErrMetricsCollectionIncomplete
	request = httptest.NewRequest(http.MethodGet, "/api/v1/metrics?mode=all_nodes_aggregated", nil)
	request.Header.Set("X-API-Key", "read-key")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable || !strings.Contains(response.Body.String(), `"code":"metrics_collection_incomplete"`) || !strings.Contains(response.Body.String(), `"retryable":true`) {
		t.Fatalf("incomplete aggregate response=%d %s", response.Code, response.Body.String())
	}
}

func TestPrometheusMetricsModesAndAuthorization(t *testing.T) {
	stub := successHTTPObservabilityStub()
	stub.metrics = observationports.MetricsCollectionDataDTO{
		Mode:     observationports.MetricsModeAllNodesUnaggregated,
		Complete: true,
		Nodes:    []observationports.MetricsNodeStatusDTO{},
		Families: []observationports.MetricFamilyDTO{{
			Name: "narumq_connections",
			Help: "active connections\nper node",
			Type: "gauge",
			Samples: []observationports.MetricSampleDTO{{
				Name:      "narumq_connections",
				Labels:    []observationports.MetricLabelDTO{{Name: "zone", Value: `edge"a`}},
				ValueKind: "float64",
				Value:     "3",
			}},
		}},
	}
	handler := NewRouter(ServerConfig{
		AllowAnonymousMetrics: true,
		APIKeys:               []APIKeyCredential{{ID: "read", Key: "read-key", Roles: []Role{RoleReadOnly}}},
		Observability:         stub,
	}, &mockStateProvider{}, &mockHealthChecker{liveStatus: managementports.HealthStatusUp}, nil)

	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || response.Header().Get("Content-Type") != ContentTypeMetrics {
		t.Fatalf("default local scrape status=%d content-type=%q body=%s", response.Code, response.Header().Get("Content-Type"), response.Body.String())
	}

	request = httptest.NewRequest(http.MethodGet, "/metrics?mode=all_nodes_unaggregated", nil)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("anonymous cluster scrape status=%d, want 401", response.Code)
	}

	request = httptest.NewRequest(http.MethodGet, "/metrics?mode=all_nodes_unaggregated", nil)
	request.Header.Set("X-API-Key", "read-key")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `# HELP narumq_connections active connections\nper node`) || !strings.Contains(response.Body.String(), `zone="edge\"a"`) || !strings.Contains(response.Body.String(), "narumq_connections{zone=\"edge\\\"a\"} 3") {
		t.Fatalf("cluster scrape status=%d body=%s", response.Code, response.Body.String())
	}

	request = httptest.NewRequest(http.MethodGet, "/metrics?mode=all_nodes_aggregated", nil)
	request.Header.Set("X-API-Key", "read-key")
	request.Header.Set("Accept", "application/openmetrics-text; version=1.0.0")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || response.Header().Get("Content-Type") != ContentTypeOpenMetrics || !strings.HasSuffix(response.Body.String(), "# EOF\n") {
		t.Fatalf("OpenMetrics scrape status=%d content-type=%q body=%s", response.Code, response.Header().Get("Content-Type"), response.Body.String())
	}

	stub.metrics.Complete = false
	request = httptest.NewRequest(http.MethodGet, "/metrics?mode=all_nodes_unaggregated", nil)
	request.Header.Set("X-API-Key", "read-key")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable ||
		!strings.Contains(response.Body.String(), `"code":"metrics_collection_incomplete"`) ||
		!strings.Contains(response.Body.String(), `"retryable":true`) {
		t.Fatalf("incomplete cluster scrape response=%d %s", response.Code, response.Body.String())
	}
}

var _ observationports.QueryService = (*httpObservabilityStub)(nil)
