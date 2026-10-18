package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"path/filepath"
	"testing"
	"time"

	"narumq/internal/adapters/auditlog"
)

const clusterManagementRBACOperatorAPIKey = "process-acceptance-operator-api-key"

// Traceability: SEC-10; docs/specs/tenancy-and-security.md §6; ADR-0048 §7.
// Scenarios: management-api-and-observability.md:MGT-SCN-017, management-api-and-observability.md:MGT-SCN-019, tenancy-and-security.md:SEC-SCN-031, tenancy-and-security.md:SEC-SCN-032, tenancy-and-security.md:SEC-SCN-033
func TestDynamicClusterManagementRBACProcessAcceptance(t *testing.T) {
	t.Parallel()

	fixture := newClusterMembershipProcessFixture(t, 3, "")
	operatorKeyPath := filepath.Join(fixture.tempDir, "cluster-operator-api-key")
	clusterMembershipProcessWriteFile(t, operatorKeyPath, []byte(clusterManagementRBACOperatorAPIKey+"\n"), 0o600)
	issuer := newClusterProcessJWTIssuer(t, "management-process-key")
	securityAdminToken := issuer.sign(t, "security-admin", time.Now().Add(time.Hour), map[string]any{
		"roles": []string{"ClusterAdmin"},
	})
	fixture.managementAPIKeys = []clusterMembershipProcessAPIKey{
		{ID: "dashboard", KeyFile: fixture.readOnlyAPIKeyPath, Roles: []string{"ReadOnly"}},
		{ID: "operator", KeyFile: operatorKeyPath, Roles: []string{"Operator"}},
	}
	fixture.managementOIDC = &clusterMembershipProcessOIDC{
		Issuer: "https://issuer.example", Audience: "narumq", Algorithms: []string{"RS256"},
		JWKSURL: issuer.jwksURL, CacheTTLMillis: 300000, RefreshCooldownMillis: 30000, ClockSkewMillis: 30000, HTTPTimeoutMillis: 5000,
		Identities: []clusterMembershipProcessOIDCIdentity{{Subject: "security-admin", Roles: []string{"SecurityAdmin"}}},
	}

	processes := make([]*clusterMembershipProcess, 3)
	for index := range processes {
		process, err := fixture.harness.start(
			fmt.Sprintf("management RBAC bootstrap node %d", index+1),
			fixture.bootstrapConfig(uint64(index+1)),
			fixture.managementAddresses[index],
		)
		if err != nil {
			t.Fatalf("start bootstrap node %d: %v", index+1, err)
		}
		processes[index] = process
	}
	clusterMembershipProcessWaitForBootstrap(t, fixture.ctx, processes)

	request := func(method, path string, body []byte, headerName, headerValue, idempotencyKey string) (int, []byte, error) {
		header := make(http.Header)
		if headerName != "" {
			header.Set(headerName, headerValue)
		}
		if idempotencyKey != "" {
			header.Set("Idempotency-Key", idempotencyKey)
		}
		return clusterMembershipProcessRequestWithHeader(
			fixture.ctx, method, processes[0].managementURL()+path, body, header,
		)
	}
	requestBearer := func(method, path string, body []byte, token, idempotencyKey string) (int, []byte, error) {
		return request(method, path, body, "Authorization", "Bearer "+token, idempotencyKey)
	}
	requestAPIKey := func(method, path string, body []byte, key, idempotencyKey string) (int, []byte, error) {
		return request(method, path, body, "X-API-Key", key, idempotencyKey)
	}
	assertStatus := func(name string, status int, body []byte, err error, want int) {
		t.Helper()
		if err != nil || status != want {
			t.Fatalf("%s status=%d body=%s err=%v, want %d", name, status, body, err, want)
		}
	}

	status, body, err := request(http.MethodGet, "/api/v1/cluster", nil, "", "", "")
	assertStatus("unauthenticated cluster read", status, body, err, http.StatusUnauthorized)
	status, body, err = requestBearer(http.MethodGet, "/api/v1/cluster", nil, "not-a-jwt", "")
	assertStatus("invalid OIDC bearer", status, body, err, http.StatusUnauthorized)

	for _, credential := range []struct {
		name, header, value string
	}{
		{name: "ReadOnly API key", header: "X-API-Key", value: clusterMembershipProcessReadOnlyAPIKey},
		{name: "Operator API key", header: "X-API-Key", value: clusterManagementRBACOperatorAPIKey},
		{name: "SecurityAdmin OIDC", header: "Authorization", value: "Bearer " + securityAdminToken},
		{name: "ClusterAdmin break-glass", header: "Authorization", value: "Bearer " + clusterMembershipProcessAdmin},
	} {
		status, body, err = request(http.MethodGet, "/api/v1/cluster", nil, credential.header, credential.value, "")
		assertStatus(credential.name+" cluster read", status, body, err, http.StatusOK)
	}

	featurePath := "/api/v1/cluster/features/connection-rebalance"
	for _, credential := range []struct {
		name, header, value string
	}{
		{name: "ReadOnly", header: "X-API-Key", value: clusterMembershipProcessReadOnlyAPIKey},
		{name: "Operator", header: "X-API-Key", value: clusterManagementRBACOperatorAPIKey},
		{name: "SecurityAdmin with ClusterAdmin claim", header: "Authorization", value: "Bearer " + securityAdminToken},
	} {
		status, body, err = request(http.MethodPost, featurePath, nil, credential.header, credential.value, "")
		assertStatus(credential.name+" cluster-admin permission", status, body, err, http.StatusForbidden)
	}
	clusterProcessActivateFeature(t, fixture.ctx, processes, "connection-rebalance")

	rebalanceBody := []byte(`{"serverReference":"lb.sec10.example.test:1883","maxDeviationConnections":1,"connectionRatePerSecond":5,"loadBalancerPropagationDelayMillis":60000,"timeoutMillis":120000}`)
	for _, credential := range []struct {
		name, header, value string
	}{
		{name: "ReadOnly", header: "X-API-Key", value: clusterMembershipProcessReadOnlyAPIKey},
		{name: "SecurityAdmin", header: "Authorization", value: "Bearer " + securityAdminToken},
	} {
		status, body, err = request(http.MethodPost, "/api/v1/cluster/rebalance", rebalanceBody, credential.header, credential.value, "")
		assertStatus(credential.name+" operate permission", status, body, err, http.StatusForbidden)
	}
	status, body, err = requestAPIKey(
		http.MethodPost, "/api/v1/cluster/rebalance", rebalanceBody,
		clusterManagementRBACOperatorAPIKey, "sec10-operator-rebalance",
	)
	assertStatus("Operator starts connection rebalance", status, body, err, http.StatusAccepted)
	started := clusterRebalanceProcessDecode(t, body)
	if started.State != "RUNNING" {
		t.Fatalf("Operator rebalance state=%q, want RUNNING", started.State)
	}
	status, body, err = requestAPIKey(
		http.MethodDelete, "/api/v1/cluster/rebalance", nil,
		clusterManagementRBACOperatorAPIKey, "sec10-operator-cancel",
	)
	assertStatus("Operator cancels connection rebalance", status, body, err, http.StatusOK)
	cancelled := clusterRebalanceProcessDecode(t, body)
	if cancelled.State != "CANCELLED" {
		t.Fatalf("Operator cancellation state=%q, want CANCELLED", cancelled.State)
	}
	readyCtx, cancelReady := context.WithTimeout(fixture.ctx, 10*time.Second)
	clusterMembershipProcessWaitForStatus(t, readyCtx, processes[0], "/health/ready", http.StatusOK)
	cancelReady()

	status, body, err = requestAPIKey(http.MethodGet, "/api/v1/nodes/1/audit/events?limit=1", nil, clusterMembershipProcessReadOnlyAPIKey, "")
	assertStatus("ReadOnly audit query permission", status, body, err, http.StatusForbidden)
	status, body, err = requestAPIKey(http.MethodGet, "/api/v1/nodes/1/audit/events?limit=1", nil, clusterManagementRBACOperatorAPIKey, "")
	assertStatus("Operator audit query permission", status, body, err, http.StatusForbidden)
	status, body, err = request(http.MethodPost, "/api/v1/cluster/backups", nil, "Authorization", "Bearer "+clusterMembershipProcessAdmin, "")
	assertStatus("ClusterAdmin backup permission", status, body, err, http.StatusOK)

	status, body, err = requestBearer(
		http.MethodGet, "/api/v1/nodes/1/audit/events?result=UNKNOWN", nil, securityAdminToken, "",
	)
	assertStatus("invalid audit result filter", status, body, err, http.StatusBadRequest)

	query := url.Values{
		"actorSubject": {"apikey:operator"},
		"result":       {"ALLOWED"},
		"limit":        {"1"},
	}
	status, body, err = requestBearer(http.MethodGet, "/api/v1/nodes/1/audit/events?"+query.Encode(), nil, securityAdminToken, "")
	assertStatus("first Operator audit page", status, body, err, http.StatusOK)
	firstPage := decodeClusterManagementRBACAuditPage(t, body)
	if len(firstPage.Data) != 1 || !firstPage.Page.HasMore || firstPage.Page.NextCursor == nil {
		t.Fatalf("first Operator audit page=%+v, want one record and a next cursor", firstPage)
	}
	assertClusterManagementRBACAuditEvent(t, firstPage.Data[0], "apikey:operator", "Operator", "cluster.rebalance.cancel", "ALLOWED")

	query.Set("cursor", *firstPage.Page.NextCursor)
	status, body, err = requestBearer(http.MethodGet, "/api/v1/nodes/1/audit/events?"+query.Encode(), nil, securityAdminToken, "")
	assertStatus("second Operator audit page", status, body, err, http.StatusOK)
	secondPage := decodeClusterManagementRBACAuditPage(t, body)
	if len(secondPage.Data) != 1 || secondPage.Page.HasMore || secondPage.Page.NextCursor != nil {
		t.Fatalf("second Operator audit page=%+v, want one final record", secondPage)
	}
	assertClusterManagementRBACAuditEvent(t, secondPage.Data[0], "apikey:operator", "Operator", "cluster.rebalance.start", "ALLOWED")
	if firstPage.Data[0].Seq <= secondPage.Data[0].Seq {
		t.Fatalf("cursor pages are not newest first: first=%d second=%d", firstPage.Data[0].Seq, secondPage.Data[0].Seq)
	}

	query = url.Values{
		"actorSubject": {"oidc:security-admin"},
		"action":       {"management.request"},
		"result":       {"DENIED"},
		"limit":        {"10"},
	}
	status, body, err = requestBearer(http.MethodGet, "/api/v1/nodes/1/audit/events?"+query.Encode(), nil, securityAdminToken, "")
	assertStatus("OIDC actor audit filter", status, body, err, http.StatusOK)
	securityAdminEvents := decodeClusterManagementRBACAuditPage(t, body)
	if len(securityAdminEvents.Data) == 0 {
		t.Fatal("OIDC actor filter returned no denied management event")
	}
	for _, record := range securityAdminEvents.Data {
		assertClusterManagementRBACAuditEvent(t, record, "oidc:security-admin", "SecurityAdmin", "management.request", "DENIED")
	}

	verification, err := auditlog.VerifyDirectory(fixture.auditDirectory(1))
	if err != nil {
		t.Fatalf("verify node-local audit chain: %v", err)
	}
	if verification.Records == 0 || verification.FirstSeq != 1 || verification.LastSeq != verification.Records {
		t.Fatalf("node-local audit chain summary=%+v, want an intact sequence from 1", verification)
	}
}

type clusterManagementRBACAuditRecord struct {
	Seq      uint64 `json:"seq"`
	PrevHash string `json:"prevHash"`
	Hash     string `json:"hash"`
	Event    struct {
		Actor struct {
			Subject string   `json:"subject"`
			Roles   []string `json:"roles"`
		} `json:"actor"`
		Action string `json:"action"`
		Result string `json:"result"`
	} `json:"event"`
}

type clusterManagementRBACAuditPage struct {
	Data []clusterManagementRBACAuditRecord `json:"data"`
	Page struct {
		NextCursor *string `json:"nextCursor"`
		HasMore    bool    `json:"hasMore"`
	} `json:"page"`
}

func decodeClusterManagementRBACAuditPage(t *testing.T, body []byte) clusterManagementRBACAuditPage {
	t.Helper()
	var page clusterManagementRBACAuditPage
	if err := json.Unmarshal(body, &page); err != nil {
		t.Fatalf("decode audit query page: %v; body=%s", err, body)
	}
	return page
}

func assertClusterManagementRBACAuditEvent(
	t *testing.T,
	record clusterManagementRBACAuditRecord,
	subject, role, action, result string,
) {
	t.Helper()
	if record.Seq == 0 || len(record.PrevHash) != 64 || len(record.Hash) != 64 ||
		record.Event.Actor.Subject != subject || len(record.Event.Actor.Roles) != 1 ||
		record.Event.Actor.Roles[0] != role || record.Event.Action != action || record.Event.Result != result {
		t.Fatalf("audit record=%+v, want seq/hash metadata and %s %s %s %s", record, subject, role, action, result)
	}
}
