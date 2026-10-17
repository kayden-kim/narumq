package integration

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"
)

// Traceability: UI-01; docs/specs/management-api-and-observability.md §7.2.
// Scenarios: management-api-and-observability.md:MGT-SCN-022, management-api-and-observability.md:MGT-SCN-023, management-api-and-observability.md:MGT-SCN-024
func TestDynamicClusterObservabilityProcessAcceptance(t *testing.T) {
	t.Parallel()

	fixture := newClusterMembershipProcessFixture(t, 3, "")
	configPaths := make([]string, 3)
	for index := range configPaths {
		configPaths[index] = fixture.bootstrapConfig(uint64(index + 1))
		clusterTenantQuotaProcessSetProfile(t, configPaths[index], clusterTenantQuotaProcessProfile(1))
	}
	processes := make([]*clusterMembershipProcess, len(configPaths))
	for index, path := range configPaths {
		process, err := fixture.harness.start(
			fmt.Sprintf("observability bootstrap node %d", index+1), path, fixture.managementAddresses[index],
		)
		if err != nil {
			t.Fatalf("start observability bootstrap node %d: %v", index+1, err)
		}
		processes[index] = process
	}
	ctx := fixture.ctx
	clusterMembershipProcessWaitForBootstrap(t, ctx, processes)
	for _, process := range processes {
		clusterMembershipProcessWaitForStatus(t, ctx, process, "/health/ready", http.StatusOK)
	}

	apiURL := processes[0].managementURL() + "/api/v1"
	var healthyNodes nodeListResponse
	clusterMembershipProcessWait(t, ctx, "all healthy nodes have direct observability snapshots", func() (bool, string, error) {
		status, body, err := clusterMembershipProcessRequest(ctx, http.MethodGet, apiURL+"/nodes", nil, clusterMembershipProcessAdmin, "")
		if err != nil {
			return false, err.Error(), nil
		}
		if status != http.StatusOK {
			return false, fmt.Sprintf("status=%d body=%s", status, body), nil
		}
		var response nodeListResponse
		if err := json.Unmarshal(body, &response); err != nil {
			return false, "decode /nodes: " + err.Error(), nil
		}
		if len(response.Data) != 3 {
			return false, fmt.Sprintf("node count=%d want=3", len(response.Data)), nil
		}
		for _, node := range response.Data {
			if node.QueryStatus != "OK" || node.Ready == nil || !*node.Ready || node.State == nil || *node.State != "READY" {
				return false, fmt.Sprintf("node %s snapshot=%+v", node.NodeID, node), nil
			}
		}
		healthyNodes = response
		return true, "three directly queried READY nodes", nil
	})
	if len(healthyNodes.Data) != 3 {
		t.Fatalf("healthy nodes not observed: %+v", healthyNodes)
	}

	var healthyCluster clusterSummaryResponse
	clusterMembershipProcessWait(t, ctx, "healthy cluster state counts are complete", func() (bool, string, error) {
		status, body, err := clusterMembershipProcessRequest(ctx, http.MethodGet, apiURL+"/cluster", nil, clusterMembershipProcessAdmin, "")
		if err != nil {
			return false, err.Error(), nil
		}
		if status != http.StatusOK {
			return false, fmt.Sprintf("status=%d body=%s", status, body), nil
		}
		var response clusterSummaryResponse
		if err := json.Unmarshal(body, &response); err != nil {
			return false, "decode /cluster: " + err.Error(), nil
		}
		if !response.Data.NodeQueryComplete || response.Data.NodesByState.Ready != 3 || response.Data.NodesByState.Unknown != 0 {
			return false, fmt.Sprintf("cluster summary=%+v", response.Data), nil
		}
		healthyCluster = response
		return true, "three READY, zero UNKNOWN", nil
	})
	if !healthyCluster.Data.NodeQueryComplete {
		t.Fatalf("healthy cluster summary not observed: %+v", healthyCluster)
	}

	status, body, err := clusterMembershipProcessRequest(ctx, http.MethodGet, apiURL+"/nodes/2/metrics", nil, clusterMembershipProcessAdmin, "")
	if err != nil || status != http.StatusOK {
		t.Fatalf("GET /nodes/2/metrics status=%d err=%v body=%s", status, err, body)
	}
	var healthyNodeMetrics nodeMetricsResponse
	if err := json.Unmarshal(body, &healthyNodeMetrics); err != nil {
		t.Fatalf("decode /nodes/2/metrics: %v", err)
	}
	if healthyNodeMetrics.Data.QueryStatus != "OK" || healthyNodeMetrics.Data.QueryErrorCode != nil ||
		healthyNodeMetrics.Data.ObservedAt == nil || healthyNodeMetrics.Data.StartedAt == nil ||
		healthyNodeMetrics.Data.Families == nil {
		t.Fatalf("healthy node metrics snapshot=%+v", healthyNodeMetrics.Data)
	}

	status, body, err = clusterMembershipProcessRequest(ctx, http.MethodGet, apiURL+"/metrics", nil, clusterMembershipProcessAdmin, "")
	if err != nil || status != http.StatusOK {
		t.Fatalf("GET /metrics status=%d err=%v body=%s", status, err, body)
	}
	var healthyMetrics metricsCollectionResponse
	if err := json.Unmarshal(body, &healthyMetrics); err != nil {
		t.Fatalf("decode /metrics: %v", err)
	}
	if !healthyMetrics.Data.Complete || len(healthyMetrics.Data.Nodes) != 3 {
		t.Fatalf("healthy metrics collection=%+v", healthyMetrics.Data)
	}
	for _, node := range healthyMetrics.Data.Nodes {
		if node.QueryStatus != "OK" || node.QueryErrorCode != nil || node.ObservedAt == nil || node.StartedAt == nil {
			t.Fatalf("healthy metrics node status=%+v", node)
		}
	}

	if err := processes[2].crash(10 * time.Second); err != nil {
		t.Fatalf("crash node 3: %v", err)
	}

	var partialNodes nodeListResponse
	clusterMembershipProcessWait(t, ctx, "unreachable member has null direct measurements", func() (bool, string, error) {
		status, body, err := clusterMembershipProcessRequest(ctx, http.MethodGet, apiURL+"/nodes", nil, clusterMembershipProcessAdmin, "")
		if err != nil {
			return false, err.Error(), nil
		}
		if status != http.StatusOK {
			return false, fmt.Sprintf("status=%d body=%s", status, body), nil
		}
		var response nodeListResponse
		if err := json.Unmarshal(body, &response); err != nil {
			return false, "decode /nodes: " + err.Error(), nil
		}
		if len(response.Data) != 3 {
			return false, fmt.Sprintf("node count=%d want=3", len(response.Data)), nil
		}
		for _, node := range response.Data {
			if node.NodeID != "3" {
				continue
			}
			if node.MembershipPhase != "VOTER" || node.QueryStatus != "UNREACHABLE" ||
				node.QueryErrorCode == nil || *node.QueryErrorCode != "node_unreachable" ||
				node.ObservedAt != nil || node.Version != nil || node.UptimeMillis != nil || node.State != nil ||
				node.Ready != nil || node.ReadinessReason != nil || node.Role != nil || node.LastAppliedIndex != nil ||
				node.ConnectionCount != nil || node.SessionCount != nil || node.StoragePressure != nil ||
				node.ReplicationHealth != nil || node.LastErrorCode != nil {
				return false, fmt.Sprintf("unreachable node snapshot=%+v", node), nil
			}
			partialNodes = response
			return true, "node 3 unreachable; measurements omitted", nil
		}
		return false, "node 3 missing", nil
	})
	if len(partialNodes.Data) != 3 {
		t.Fatalf("partial node list not observed: %+v", partialNodes)
	}

	var partialCluster clusterSummaryResponse
	status, body, err = clusterMembershipProcessRequest(ctx, http.MethodGet, apiURL+"/cluster", nil, clusterMembershipProcessAdmin, "")
	if err != nil || status != http.StatusOK {
		t.Fatalf("GET /cluster with unreachable member status=%d err=%v body=%s", status, err, body)
	}
	if err := json.Unmarshal(body, &partialCluster); err != nil {
		t.Fatalf("decode partial /cluster: %v", err)
	}
	if partialCluster.Data.NodeQueryComplete || partialCluster.Data.NodesByState.Ready != 2 ||
		partialCluster.Data.NodesByState.Unknown != 1 || partialCluster.Data.ConnectionCount != nil || partialCluster.Data.SessionCount != nil {
		t.Fatalf("partial cluster summary=%+v", partialCluster.Data)
	}

	status, body, err = clusterMembershipProcessRequest(ctx, http.MethodGet, apiURL+"/nodes/3/metrics", nil, clusterMembershipProcessAdmin, "")
	if err != nil || status != http.StatusOK {
		t.Fatalf("GET /nodes/3/metrics status=%d err=%v body=%s", status, err, body)
	}
	var unavailableNodeMetrics nodeMetricsResponse
	if err := json.Unmarshal(body, &unavailableNodeMetrics); err != nil {
		t.Fatalf("decode unavailable /nodes/3/metrics: %v", err)
	}
	if unavailableNodeMetrics.Data.QueryStatus != "UNREACHABLE" ||
		unavailableNodeMetrics.Data.QueryErrorCode == nil || *unavailableNodeMetrics.Data.QueryErrorCode != "node_unreachable" ||
		unavailableNodeMetrics.Data.ObservedAt != nil || unavailableNodeMetrics.Data.StartedAt != nil ||
		unavailableNodeMetrics.Data.Families != nil {
		t.Fatalf("unavailable node metrics=%+v", unavailableNodeMetrics.Data)
	}

	status, body, err = clusterMembershipProcessRequest(ctx, http.MethodGet, apiURL+"/metrics", nil, clusterMembershipProcessAdmin, "")
	if err != nil || status != http.StatusOK {
		t.Fatalf("GET partial /metrics status=%d err=%v body=%s", status, err, body)
	}
	var partialMetrics metricsCollectionResponse
	if err := json.Unmarshal(body, &partialMetrics); err != nil {
		t.Fatalf("decode partial /metrics: %v", err)
	}
	if partialMetrics.Data.Complete || len(partialMetrics.Data.Nodes) != 3 {
		t.Fatalf("partial metrics collection=%+v", partialMetrics.Data)
	}
	if node := metricsNodeByID(partialMetrics.Data.Nodes, "3"); node == nil ||
		node.QueryStatus != "UNREACHABLE" || node.QueryErrorCode == nil ||
		*node.QueryErrorCode != "node_unreachable" || node.ObservedAt != nil || node.StartedAt != nil {
		t.Fatalf("partial metrics node status=%+v", node)
	}

	status, body, err = clusterMembershipProcessRequest(ctx, http.MethodGet, apiURL+"/metrics?mode=all_nodes_aggregated", nil, clusterMembershipProcessAdmin, "")
	if err != nil || status != http.StatusServiceUnavailable {
		t.Fatalf("GET aggregated partial /metrics status=%d err=%v body=%s", status, err, body)
	}
	if !jsonResponseHasErrorCode(body, "metrics_collection_incomplete") {
		t.Fatalf("aggregated partial metrics error body=%s", body)
	}
	status, body, err = clusterMembershipProcessRequest(ctx, http.MethodGet, processes[0].managementURL()+"/metrics?mode=all_nodes_unaggregated", nil, clusterMembershipProcessAdmin, "")
	if err != nil || status != http.StatusServiceUnavailable {
		t.Fatalf("GET cluster text metrics status=%d err=%v body=%s", status, err, body)
	}
	if !jsonResponseHasErrorCode(body, "metrics_collection_incomplete") {
		t.Fatalf("cluster text metrics error body=%s", body)
	}
}

type nodeListResponse struct {
	Data []struct {
		NodeID            string     `json:"nodeId"`
		MembershipPhase   string     `json:"membershipPhase"`
		QueryStatus       string     `json:"queryStatus"`
		QueryErrorCode    *string    `json:"queryErrorCode"`
		ObservedAt        *time.Time `json:"observedAt"`
		Version           *string    `json:"version"`
		UptimeMillis      *int64     `json:"uptimeMillis"`
		State             *string    `json:"state"`
		Ready             *bool      `json:"ready"`
		ReadinessReason   *string    `json:"readinessReason"`
		Role              *string    `json:"role"`
		LastAppliedIndex  *uint64    `json:"lastAppliedIndex"`
		ConnectionCount   *uint64    `json:"connectionCount"`
		SessionCount      *uint64    `json:"sessionCount"`
		StoragePressure   *bool      `json:"storagePressure"`
		ReplicationHealth *string    `json:"replicationHealth"`
		LastErrorCode     *string    `json:"lastErrorCode"`
	} `json:"data"`
}

type clusterSummaryResponse struct {
	Data struct {
		NodeQueryComplete bool `json:"nodeQueryComplete"`
		NodesByState      struct {
			Ready   uint64 `json:"ready"`
			Unknown uint64 `json:"unknown"`
		} `json:"nodesByState"`
		ConnectionCount *uint64 `json:"connectionCount"`
		SessionCount    *uint64 `json:"sessionCount"`
	} `json:"data"`
}

type nodeMetricsResponse struct {
	Data struct {
		QueryStatus    string            `json:"queryStatus"`
		QueryErrorCode *string           `json:"queryErrorCode"`
		ObservedAt     *time.Time        `json:"observedAt"`
		StartedAt      *time.Time        `json:"startedAt"`
		Families       []json.RawMessage `json:"families"`
	} `json:"data"`
}

type metricNodeStatus struct {
	NodeID         string     `json:"nodeId"`
	QueryStatus    string     `json:"queryStatus"`
	QueryErrorCode *string    `json:"queryErrorCode"`
	ObservedAt     *time.Time `json:"observedAt"`
	StartedAt      *time.Time `json:"startedAt"`
}

type metricsCollectionResponse struct {
	Data struct {
		Complete bool               `json:"complete"`
		Nodes    []metricNodeStatus `json:"nodes"`
	} `json:"data"`
}

func metricsNodeByID(nodes []metricNodeStatus, nodeID string) *metricNodeStatus {
	for index := range nodes {
		if nodes[index].NodeID == nodeID {
			return &nodes[index]
		}
	}
	return nil
}

func jsonResponseHasErrorCode(body []byte, want string) bool {
	var response struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	return json.Unmarshal(body, &response) == nil && response.Error.Code == want
}
