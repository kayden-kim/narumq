package management

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"narumq/internal/application/membership"
	"narumq/internal/domain/connrebalance"
)

const rebalanceHTTPKey = "00000000-0000-4000-8000-000000000017"

type rebalanceHTTPStub struct {
	operation membership.ConnectionRebalanceOperationDTO
	err       error
	calls     int
}

func (stub *rebalanceHTTPStub) StartConnectionRebalance(_ context.Context, _ membership.StartConnectionRebalanceRequestDTO) (membership.ConnectionRebalanceOperationDTO, error) {
	stub.calls++
	return stub.operation, stub.err
}
func (stub *rebalanceHTTPStub) GetConnectionRebalance(context.Context) (membership.ConnectionRebalanceOperationDTO, error) {
	return stub.operation, stub.err
}
func (stub *rebalanceHTTPStub) CancelConnectionRebalance(context.Context) (membership.ConnectionRebalanceOperationDTO, error) {
	return stub.operation, stub.err
}

func newRebalanceHTTPTestHandler(service ConnectionRebalanceUseCase, role Role) http.Handler {
	return NewRouterWithMembership(ServerConfig{APIKeys: []APIKeyCredential{{ID: "test", Key: "rebalance-secret", Roles: []Role{role}}}, ClusterID: "test-cluster"},
		&mockStateProvider{}, nil, nil, nil, nil, service, nil, &spyAuditUseCase{}, nil, nil, nil, nil, nil)
}

func rebalanceHTTPRequest(method, body, key string) *http.Request {
	r := httptest.NewRequest(method, "/api/v1/cluster/rebalance", strings.NewReader(body))
	r.Header.Set("X-API-Key", "rebalance-secret")
	if body != "" {
		r.Header.Set("Content-Type", "application/json")
	}
	if key != "" {
		r.Header.Set("Idempotency-Key", key)
	}
	return r
}

const validRebalanceRequestBody = `{"serverReference":"lb.example:1883","maxDeviationConnections":1,"connectionRatePerSecond":5,"loadBalancerPropagationDelayMillis":500,"timeoutMillis":120000}`

func rebalanceHTTPOperation() membership.ConnectionRebalanceOperationDTO {
	return membership.ConnectionRebalanceOperationDTO{State: membership.ConnectionRebalanceStateDTO{
		RebalanceOperationID: "00000000-0000-4000-8000-000000000018", Phase: connrebalance.PhaseRunning,
		ServerReference: "lb.example:1883", MaxDeviationConnections: 1, ConnectionRatePerSecond: 5,
		LoadBalancerPropagationDelayMillis: 500, TimeoutMillis: 120000,
		StartedAtUnixMillis: 1_800_000_000_000, DeadlineAtUnixMillis: 1_800_000_120_000,
		Participants: []membership.ConnectionRebalanceReportDTO{
			{NodeID: 1, Connections: 12, Shedding: true, ReportedAtUnixMillis: 1_800_000_000_000},
			{NodeID: 2, Connections: 6, ReportedAtUnixMillis: 1_800_000_000_000},
			{NodeID: 3, Connections: 0},
		},
	}}
}

// Scenarios: cluster-session-and-delivery.md:CLU-SCN-225
func TestConnectionRebalanceHTTPRejectsInvalidJSONAndRequiredFields(t *testing.T) {
	stub := &rebalanceHTTPStub{}
	handler := newRebalanceHTTPTestHandler(stub, RoleOperator)
	cases := []struct {
		name string
		body string
	}{
		{name: "malformed JSON", body: `{"serverReference":`},
		{name: "missing required field", body: `{"serverReference":"lb","maxDeviationConnections":1,"connectionRatePerSecond":5,"loadBalancerPropagationDelayMillis":500}`},
		{name: "unknown field", body: `{"serverReference":"lb","maxDeviationConnections":1,"connectionRatePerSecond":5,"loadBalancerPropagationDelayMillis":500,"timeoutMillis":120000,"extra":true}`},
		{name: "legacy duration names", body: `{"serverReference":"lb","maxDeviationConnections":1,"connectionRatePerSecond":5,"loadBalancerPropagationDelayMs":500,"timeoutMs":120000}`},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, rebalanceHTTPRequest(http.MethodPost, test.body, rebalanceHTTPKey))
			var envelope ErrorResponse
			if err := json.Unmarshal(recorder.Body.Bytes(), &envelope); err != nil {
				t.Fatalf("decode invalid-request envelope: %v; body=%s", err, recorder.Body.String())
			}
			if recorder.Code != http.StatusBadRequest || envelope.Error.Code != "invalid_request" || envelope.Error.Retryable {
				t.Fatalf("invalid body %s: status=%d error=%+v", test.body, recorder.Code, envelope.Error)
			}
		})
	}
	if stub.calls != 0 {
		t.Fatalf("invalid requests reached service %d times", stub.calls)
	}
}

// Scenarios: cluster-session-and-delivery.md:CLU-SCN-225
func TestConnectionRebalanceHTTPConflictMapping(t *testing.T) {
	for _, code := range []membership.ServiceErrorCode{
		membership.ServiceErrorConnectionRebalanceInProgress,
		membership.ServiceErrorRebalanceNotCancellable,
		membership.ServiceErrorFeatureInactive,
	} {
		t.Run(string(code), func(t *testing.T) {
			stub := &rebalanceHTTPStub{err: &membership.ServiceError{Code: code, Message: "conflict"}}
			recorder := httptest.NewRecorder()
			newRebalanceHTTPTestHandler(stub, RoleOperator).ServeHTTP(recorder, rebalanceHTTPRequest(http.MethodPost, validRebalanceRequestBody, rebalanceHTTPKey))
			var envelope ErrorResponse
			if err := json.Unmarshal(recorder.Body.Bytes(), &envelope); err != nil {
				t.Fatalf("decode conflict envelope: %v; body=%s", err, recorder.Body.String())
			}
			if recorder.Code != http.StatusConflict || envelope.Error.Code != string(code) || envelope.Error.Retryable || stub.calls != 1 {
				t.Fatalf("status=%d error=%+v service calls=%d", recorder.Code, envelope.Error, stub.calls)
			}
		})
	}
}

// Scenarios: cluster-session-and-delivery.md:CLU-SCN-225
func TestConnectionRebalanceHTTPUnavailableMappingIsRetryable(t *testing.T) {
	stub := &rebalanceHTTPStub{err: &membership.ServiceError{
		Code: membership.ServiceErrorQuorumUnavailable, Message: "quorum is unavailable", Retryable: true,
	}}
	recorder := httptest.NewRecorder()
	newRebalanceHTTPTestHandler(stub, RoleOperator).ServeHTTP(recorder, rebalanceHTTPRequest(http.MethodPost, validRebalanceRequestBody, rebalanceHTTPKey))
	var envelope ErrorResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("decode unavailable envelope: %v; body=%s", err, recorder.Body.String())
	}
	if recorder.Code != http.StatusServiceUnavailable || envelope.Error.Code != string(membership.ServiceErrorQuorumUnavailable) || !envelope.Error.Retryable || stub.calls != 1 {
		t.Fatalf("status=%d error=%+v service calls=%d", recorder.Code, envelope.Error, stub.calls)
	}
}

// Scenarios: cluster-session-and-delivery.md:CLU-SCN-225
func TestConnectionRebalanceHTTPRoleAuthorizationAndDeleteKey(t *testing.T) {
	stub := &rebalanceHTTPStub{operation: rebalanceHTTPOperation()}
	readOnly := newRebalanceHTTPTestHandler(stub, RoleReadOnly)
	recorder := httptest.NewRecorder()
	readOnly.ServeHTTP(recorder, rebalanceHTTPRequest(http.MethodPost, validRebalanceRequestBody, rebalanceHTTPKey))
	var forbidden ErrorResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &forbidden); err != nil {
		t.Fatalf("decode forbidden envelope: %v; body=%s", err, recorder.Body.String())
	}
	if recorder.Code != http.StatusForbidden || forbidden.Error.Code != "forbidden" || forbidden.Error.Retryable || stub.calls != 0 {
		t.Fatalf("read-only POST status=%d error=%+v service calls=%d", recorder.Code, forbidden.Error, stub.calls)
	}

	unauthenticated := rebalanceHTTPRequest(http.MethodPost, validRebalanceRequestBody, rebalanceHTTPKey)
	unauthenticated.Header.Del("X-API-Key")
	recorder = httptest.NewRecorder()
	newRebalanceHTTPTestHandler(stub, RoleOperator).ServeHTTP(recorder, unauthenticated)
	var unauthorized ErrorResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &unauthorized); err != nil {
		t.Fatalf("decode unauthorized envelope: %v; body=%s", err, recorder.Body.String())
	}
	if recorder.Code != http.StatusUnauthorized || unauthorized.Error.Code != "unauthorized" || unauthorized.Error.Retryable || stub.calls != 0 {
		t.Fatalf("unauthenticated POST status=%d error=%+v service calls=%d", recorder.Code, unauthorized.Error, stub.calls)
	}

	operator := newRebalanceHTTPTestHandler(stub, RoleOperator)
	recorder = httptest.NewRecorder()
	operator.ServeHTTP(recorder, rebalanceHTTPRequest(http.MethodDelete, "", ""))
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("DELETE without idempotency key status=%d want=%d body=%s", recorder.Code, http.StatusBadRequest, recorder.Body.String())
	}
}

// Scenarios: cluster-session-and-delivery.md:CLU-SCN-225
func TestConnectionRebalanceHTTPStartReplayAndResponse(t *testing.T) {
	stub := &rebalanceHTTPStub{operation: rebalanceHTTPOperation()}
	handler := newRebalanceHTTPTestHandler(stub, RoleOperator)
	for _, test := range []struct {
		replayed bool
		status   int
	}{{false, http.StatusAccepted}, {true, http.StatusOK}} {
		stub.operation.Replayed = test.replayed
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, rebalanceHTTPRequest(http.MethodPost, validRebalanceRequestBody, rebalanceHTTPKey))
		if recorder.Code != test.status {
			t.Fatalf("replayed=%v status=%d want=%d body=%s", test.replayed, recorder.Code, test.status, recorder.Body.String())
		}
		var envelope struct {
			Data map[string]any `json:"data"`
		}
		if err := json.Unmarshal(recorder.Body.Bytes(), &envelope); err != nil {
			t.Fatal(err)
		}
		if envelope.Data["operationId"] != stub.operation.State.RebalanceOperationID || envelope.Data["state"] != "RUNNING" || envelope.Data["totalConnections"] != float64(18) || envelope.Data["fairShare"] != float64(6) || envelope.Data["ceiling"] != float64(7) {
			t.Fatalf("unexpected response data: %#v", envelope.Data)
		}
		if envelope.Data["finishedAt"] != nil || envelope.Data["failure"] != nil || envelope.Data["serverReference"] != "lb.example:1883" {
			t.Fatalf("unexpected nullable fields: %#v", envelope.Data)
		}
	}
	if stub.calls != 2 {
		t.Fatalf("start calls=%d want=2", stub.calls)
	}
}
