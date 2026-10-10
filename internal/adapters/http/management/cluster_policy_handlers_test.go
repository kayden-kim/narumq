package management

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"narumq/internal/application/membership"
	portsaudit "narumq/internal/application/ports/audit"
)

type clusterPolicyHTTPStub struct {
	state       membership.ClusterPolicyStateDTO
	found       bool
	getErr      error
	updateErr   error
	getCalls    int
	updateCalls []membership.UpdateClusterPolicyRequestDTO
}

func (stub *clusterPolicyHTTPStub) GetClusterPolicy(context.Context) (membership.ClusterPolicyStateDTO, bool, error) {
	stub.getCalls++
	return stub.state, stub.found, stub.getErr
}

func (stub *clusterPolicyHTTPStub) UpdateClusterPolicy(_ context.Context, request membership.UpdateClusterPolicyRequestDTO) (membership.ClusterPolicyStateDTO, error) {
	stub.updateCalls = append(stub.updateCalls, request)
	if stub.updateErr != nil {
		return membership.ClusterPolicyStateDTO{}, stub.updateErr
	}
	stub.state = membership.ClusterPolicyStateDTO{
		Version: request.ExpectedVersion + 1, Policy: request.Policy,
		IdempotencyKey: request.IdempotencyKey, PreviousVersion: request.ExpectedVersion,
	}
	stub.found = true
	return stub.state, nil
}

func newClusterPolicyHTTPTestHandler(service ClusterPolicyUseCase, role Role, auditService AuditUseCase) http.Handler {
	return NewRouterWithMembership(
		ServerConfig{APIKeys: []APIKeyCredential{{ID: "policy-test", Key: "policy-secret", Roles: []Role{role}}}, ClusterID: "cluster-policy-test"},
		&mockStateProvider{}, nil, nil, nil, nil, nil, nil, auditService, nil, nil, nil, service,
	)
}

func clusterPolicyHTTPRequest(method, path string, body string) *http.Request {
	request := httptest.NewRequest(method, path, strings.NewReader(body))
	request.Header.Set("X-API-Key", "policy-secret")
	if body != "" {
		request.Header.Set("Content-Type", "application/json")
	}
	return request
}

const validClusterPolicyJSON = `{"mqttLimits":{"maxPacketSize":1048576,"receiveMaximum":64,"connectTimeoutMillis":10000,"packetReadTimeoutMillis":30000,"writeTimeoutMillis":5000}}`

// Traceability: API-09; docs/contracts/management-api-v1.md §1, §4; ADR-0052 §1.1.1.
func TestClusterPolicyHTTPGetAndUpdate(t *testing.T) {
	service := &clusterPolicyHTTPStub{}
	auditService := &spyAuditUseCase{}
	handler := newClusterPolicyHTTPTestHandler(service, RoleClusterAdmin, auditService)

	getRequest := clusterPolicyHTTPRequest(http.MethodGet, "/api/v1/cluster/policy", "")
	getResponse := httptest.NewRecorder()
	handler.ServeHTTP(getResponse, getRequest)
	var empty struct {
		Data clusterPolicyResponse `json:"data"`
	}
	if err := json.Unmarshal(getResponse.Body.Bytes(), &empty); err != nil {
		t.Fatalf("decode empty policy: %v", err)
	}
	if getResponse.Code != http.StatusOK || getResponse.Header().Get("ETag") != `"0"` || empty.Data.Version != 0 || empty.Data.Policy != nil || service.getCalls != 1 {
		t.Fatalf("empty policy status=%d etag=%q body=%+v calls=%d", getResponse.Code, getResponse.Header().Get("ETag"), empty.Data, service.getCalls)
	}

	updateRequest := clusterPolicyHTTPRequest(http.MethodPut, "/api/v1/cluster/policy", validClusterPolicyJSON)
	updateRequest.Header.Set("If-Match", `"0"`)
	updateRequest.Header.Set("Idempotency-Key", "policy-update-1")
	updateResponse := httptest.NewRecorder()
	handler.ServeHTTP(updateResponse, updateRequest)
	var updated struct {
		Data clusterPolicyResponse `json:"data"`
	}
	if err := json.Unmarshal(updateResponse.Body.Bytes(), &updated); err != nil {
		t.Fatalf("decode updated policy: %v", err)
	}
	wantPolicy := membership.ClusterPolicyDTO{MQTTLimits: membership.MQTTLimitsDTO{
		MaxPacketSize: 1048576, ReceiveMaximum: 64, ConnectTimeoutMillis: 10000,
		PacketReadTimeoutMillis: 30000, WriteTimeoutMillis: 5000,
	}}
	if updateResponse.Code != http.StatusOK || updateResponse.Header().Get("ETag") != `"1"` || updated.Data.Version != 1 || updated.Data.Policy == nil || updated.Data.Policy.MQTTLimits.MaxPacketSize != wantPolicy.MQTTLimits.MaxPacketSize {
		t.Fatalf("update status=%d etag=%q body=%+v", updateResponse.Code, updateResponse.Header().Get("ETag"), updated.Data)
	}
	if len(service.updateCalls) != 1 || service.updateCalls[0] != (membership.UpdateClusterPolicyRequestDTO{
		ExpectedVersion: 0, IdempotencyKey: "policy-update-1", Policy: wantPolicy,
	}) {
		t.Fatalf("update request = %+v", service.updateCalls)
	}
	if len(auditService.commands) != 1 {
		t.Fatalf("audit records = %d, want 1", len(auditService.commands))
	}
	event := auditService.commands[0]
	if event.Action != "cluster.policy.update" || event.Target.Type != "cluster_policy" || event.Target.ID != "cluster-policy-test" || event.Result != portsaudit.AuditResultAllowed || event.Details["previousVersion"] != uint64(0) || event.Details["newVersion"] != uint64(1) || event.Details["idempotencyKey"] != "policy-update-1" {
		t.Fatalf("audit event = %+v", event)
	}

	getRequest = clusterPolicyHTTPRequest(http.MethodGet, "/api/v1/cluster/policy", "")
	getResponse = httptest.NewRecorder()
	handler.ServeHTTP(getResponse, getRequest)
	if getResponse.Code != http.StatusOK || getResponse.Header().Get("ETag") != `"1"` || service.getCalls != 2 {
		t.Fatalf("committed policy status=%d etag=%q calls=%d body=%s", getResponse.Code, getResponse.Header().Get("ETag"), service.getCalls, getResponse.Body.String())
	}
}

// Traceability: API-09; docs/contracts/management-api-v1.md §1, §4; ADR-0052 §1.1.1.
func TestClusterPolicyHTTPRejectsInvalidRequestAndMapsErrors(t *testing.T) {
	invalidIfMatch := []struct {
		name    string
		headers []string
	}{
		{name: "missing"},
		{name: "unquoted", headers: []string{"0"}},
		{name: "leading zero", headers: []string{`"00"`}},
		{name: "wildcard", headers: []string{"*"}},
		{name: "weak", headers: []string{`W/"0"`}},
		{name: "overflow sentinel", headers: []string{`"18446744073709551615"`}},
		{name: "duplicate", headers: []string{`"0"`, `"0"`}},
	}
	for _, testCase := range invalidIfMatch {
		t.Run(testCase.name, func(t *testing.T) {
			service := &clusterPolicyHTTPStub{}
			handler := newClusterPolicyHTTPTestHandler(service, RoleClusterAdmin, &spyAuditUseCase{})
			request := clusterPolicyHTTPRequest(http.MethodPut, "/api/v1/cluster/policy", validClusterPolicyJSON)
			for _, value := range testCase.headers {
				request.Header.Add("If-Match", value)
			}
			request.Header.Set("Idempotency-Key", "policy-invalid")
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			var body ErrorResponse
			if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
				t.Fatalf("decode error: %v body=%s", err, response.Body.String())
			}
			if response.Code != http.StatusBadRequest || body.Error.Code != "invalid_request" || len(service.updateCalls) != 0 {
				t.Fatalf("status=%d code=%q calls=%d body=%s", response.Code, body.Error.Code, len(service.updateCalls), response.Body.String())
			}
		})
	}

	invalidBodies := []struct {
		name string
		body string
	}{
		{name: "missing limit", body: `{"mqttLimits":{"maxPacketSize":1048576,"receiveMaximum":64,"connectTimeoutMillis":10000,"packetReadTimeoutMillis":30000}}`},
		{name: "unknown field", body: `{"mqttLimits":{"maxPacketSize":1048576,"receiveMaximum":64,"connectTimeoutMillis":10000,"packetReadTimeoutMillis":30000,"writeTimeoutMillis":5000,"extra":1}}`},
		{name: "duplicate field", body: `{"mqttLimits":{"maxPacketSize":1048576,"maxPacketSize":2048,"receiveMaximum":64,"connectTimeoutMillis":10000,"packetReadTimeoutMillis":30000,"writeTimeoutMillis":5000}}`},
		{name: "out of range", body: `{"mqttLimits":{"maxPacketSize":0,"receiveMaximum":64,"connectTimeoutMillis":10000,"packetReadTimeoutMillis":30000,"writeTimeoutMillis":5000}}`},
	}
	for _, testCase := range invalidBodies {
		t.Run(testCase.name, func(t *testing.T) {
			service := &clusterPolicyHTTPStub{}
			handler := newClusterPolicyHTTPTestHandler(service, RoleClusterAdmin, &spyAuditUseCase{})
			request := clusterPolicyHTTPRequest(http.MethodPut, "/api/v1/cluster/policy", testCase.body)
			request.Header.Set("If-Match", `"0"`)
			request.Header.Set("Idempotency-Key", "policy-invalid")
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			var body ErrorResponse
			if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
				t.Fatalf("decode error: %v body=%s", err, response.Body.String())
			}
			if response.Code != http.StatusBadRequest || body.Error.Code != "invalid_request" || len(service.updateCalls) != 0 {
				t.Fatalf("status=%d code=%q calls=%d body=%s", response.Code, body.Error.Code, len(service.updateCalls), response.Body.String())
			}
		})
	}

	service := &clusterPolicyHTTPStub{updateErr: &membership.ServiceError{Code: membership.ServiceErrorClusterPolicyVersionConflict, Message: "stale version"}}
	handler := newClusterPolicyHTTPTestHandler(service, RoleClusterAdmin, &spyAuditUseCase{})
	request := clusterPolicyHTTPRequest(http.MethodPut, "/api/v1/cluster/policy", validClusterPolicyJSON)
	request.Header.Set("If-Match", `"0"`)
	request.Header.Set("Idempotency-Key", "policy-stale")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	var conflict ErrorResponse
	if err := json.Unmarshal(response.Body.Bytes(), &conflict); err != nil {
		t.Fatalf("decode conflict: %v", err)
	}
	if response.Code != http.StatusConflict || conflict.Error.Code != string(membership.ServiceErrorClusterPolicyVersionConflict) || conflict.Error.Retryable {
		t.Fatalf("conflict status=%d body=%+v", response.Code, conflict.Error)
	}

	service = &clusterPolicyHTTPStub{getErr: &membership.ServiceError{Code: membership.ServiceErrorQuorumUnavailable, Message: "unavailable", Retryable: true}}
	handler = newClusterPolicyHTTPTestHandler(service, RoleReadOnly, nil)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, clusterPolicyHTTPRequest(http.MethodGet, "/api/v1/cluster/policy", ""))
	var unavailable ErrorResponse
	if err := json.Unmarshal(response.Body.Bytes(), &unavailable); err != nil {
		t.Fatalf("decode unavailable: %v", err)
	}
	if response.Code != http.StatusServiceUnavailable || unavailable.Error.Code != string(membership.ServiceErrorQuorumUnavailable) || !unavailable.Error.Retryable {
		t.Fatalf("unavailable status=%d body=%+v", response.Code, unavailable.Error)
	}
}

// Traceability: API-09; docs/contracts/management-api-v1.md §1, §4; ADR-0052 §1.1.1.
func TestClusterPolicyHTTPAuthorizationAndQueryBodyRules(t *testing.T) {
	service := &clusterPolicyHTTPStub{}
	auditService := &spyAuditUseCase{}
	handler := newClusterPolicyHTTPTestHandler(service, RoleOperator, auditService)
	request := clusterPolicyHTTPRequest(http.MethodPut, "/api/v1/cluster/policy", validClusterPolicyJSON)
	request.Header.Set("If-Match", `"0"`)
	request.Header.Set("Idempotency-Key", "policy-forbidden")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusForbidden || len(service.updateCalls) != 0 || len(auditService.commands) != 1 || auditService.commands[0].Result != portsaudit.AuditResultDenied {
		t.Fatalf("forbidden status=%d updates=%d audit=%+v", response.Code, len(service.updateCalls), auditService.commands)
	}

	readHandler := newClusterPolicyHTTPTestHandler(service, RoleReadOnly, nil)
	for _, request := range []*http.Request{
		clusterPolicyHTTPRequest(http.MethodGet, "/api/v1/cluster/policy?unexpected=1", ""),
		clusterPolicyHTTPRequest(http.MethodGet, "/api/v1/cluster/policy", "body"),
	} {
		response = httptest.NewRecorder()
		readHandler.ServeHTTP(response, request)
		var body ErrorResponse
		if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
			t.Fatalf("decode invalid GET: %v", err)
		}
		if response.Code != http.StatusBadRequest || body.Error.Code != "invalid_request" {
			t.Fatalf("invalid GET status=%d body=%+v", response.Code, body.Error)
		}
	}

	withoutPolicyService := NewRouterWithMembership(ServerConfig{}, &mockStateProvider{}, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	response = httptest.NewRecorder()
	withoutPolicyService.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/v1/cluster/policy", nil))
	if response.Code != http.StatusNotFound {
		t.Fatalf("route without policy service status=%d body=%s", response.Code, response.Body.String())
	}

}
