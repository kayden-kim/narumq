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
	return NewRouterWithMembership(
		ServerConfig{AuthToken: "rebalance-secret", AuthTokenRole: role},
		&mockStateProvider{}, nil, nil, nil, nil, service, nil,
	)
}

func rebalanceHTTPRequest(method, body, key string) *http.Request {
	r := httptest.NewRequest(method, "/api/v1/cluster/rebalance", strings.NewReader(body))
	r.Header.Set("Authorization", "Bearer rebalance-secret")
	if body != "" {
		r.Header.Set("Content-Type", "application/json")
	}
	if key != "" {
		r.Header.Set("Idempotency-Key", key)
	}
	return r
}

const validRebalanceRequestBody = `{"serverReference":"lb.example:1883","maxDeviationConnections":1,"connectionRatePerSecond":5,"loadBalancerPropagationDelayMs":500,"timeoutMs":120000}`

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

func TestConnectionRebalanceHTTPRequiredFieldsAndUnknownFields(t *testing.T) {
	for _, body := range []string{
		`{"maxDeviationConnections":1,"connectionRatePerSecond":5,"loadBalancerPropagationDelayMs":500}`,
		`{"serverReference":"lb","maxDeviationConnections":1,"connectionRatePerSecond":5,"loadBalancerPropagationDelayMs":500,"timeoutMs":120000,"extra":true}`,
	} {
		recorder := httptest.NewRecorder()
		newRebalanceHTTPTestHandler(&rebalanceHTTPStub{}, RoleOperator).ServeHTTP(recorder, rebalanceHTTPRequest(http.MethodPost, body, rebalanceHTTPKey))
		if recorder.Code != http.StatusBadRequest {
			t.Fatalf("body %s: status=%d want=%d body=%s", body, recorder.Code, http.StatusBadRequest, recorder.Body.String())
		}
	}
}

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
			if recorder.Code != http.StatusConflict || !strings.Contains(recorder.Body.String(), `"code":"`+string(code)+`"`) {
				t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
			}
		})
	}
}

func TestConnectionRebalanceHTTPRoleAuthorizationAndDeleteKey(t *testing.T) {
	handler := newRebalanceHTTPTestHandler(&rebalanceHTTPStub{operation: rebalanceHTTPOperation()}, RoleReadOnly)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, rebalanceHTTPRequest(http.MethodPost, validRebalanceRequestBody, rebalanceHTTPKey))
	if recorder.Code != http.StatusForbidden {
		t.Fatalf("POST status=%d want=%d", recorder.Code, http.StatusForbidden)
	}

	operator := newRebalanceHTTPTestHandler(&rebalanceHTTPStub{operation: rebalanceHTTPOperation()}, RoleOperator)
	recorder = httptest.NewRecorder()
	operator.ServeHTTP(recorder, rebalanceHTTPRequest(http.MethodDelete, "", ""))
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("DELETE without idempotency key status=%d want=%d body=%s", recorder.Code, http.StatusBadRequest, recorder.Body.String())
	}
}
