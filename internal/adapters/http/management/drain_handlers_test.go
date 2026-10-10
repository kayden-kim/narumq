package management

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"narumq/internal/application/membership"
	"narumq/internal/domain/nodedrain"
)

const drainHTTPKey = "00000000-0000-4000-8000-000000000016"

type drainHTTPStub struct {
	startResult  membership.NodeDrainOperationDTO
	getResult    membership.NodeDrainOperationDTO
	cancelResult membership.NodeDrainOperationDTO
	startError   error
	getError     error
	cancelError  error
	startCalls   int
	getCalls     int
	cancelCalls  int
	startRequest membership.StartNodeDrainRequestDTO
	memberID     uint64
}

func (stub *drainHTTPStub) StartNodeDrain(_ context.Context, request membership.StartNodeDrainRequestDTO) (membership.NodeDrainOperationDTO, error) {
	stub.startCalls++
	stub.startRequest = request
	return stub.startResult, stub.startError
}

func (stub *drainHTTPStub) GetNodeDrain(_ context.Context, memberID uint64) (membership.NodeDrainOperationDTO, error) {
	stub.getCalls++
	stub.memberID = memberID
	return stub.getResult, stub.getError
}

func (stub *drainHTTPStub) CancelNodeDrain(_ context.Context, memberID uint64) (membership.NodeDrainOperationDTO, error) {
	stub.cancelCalls++
	stub.memberID = memberID
	return stub.cancelResult, stub.cancelError
}

func newDrainHTTPTestHandler(service NodeDrainUseCase, role Role) http.Handler {
	return NewRouterWithMembership(ServerConfig{APIKeys: []APIKeyCredential{{ID: "test", Key: "drain-secret", Roles: []Role{role}}}, ClusterID: "test-cluster"},
		&mockStateProvider{}, nil, nil, nil, service, nil, nil, &spyAuditUseCase{}, nil, nil, nil, nil)
}

func drainHTTPRequest(method, path, body string, key string) *http.Request {
	request := httptest.NewRequest(method, path, strings.NewReader(body))
	request.Header.Set("X-API-Key", "drain-secret")
	if body != "" {
		request.Header.Set("Content-Type", "application/json")
	}
	if key != "" {
		request.Header.Set("Idempotency-Key", key)
	}
	return request
}

func validDrainRequestBody() string {
	return `{"serverReference":"mqtt.example:8883","connectionRatePerSecond":2,"loadBalancerPropagationDelayMillis":1500,"timeoutMillis":60000}`
}

func drainHTTPState(phase nodedrain.Phase) membership.NodeDrainStateDTO {
	return membership.NodeDrainStateDTO{
		DrainOperationID: drainHTTPKey, MemberID: 2, Phase: phase,
		StartedAtUnixMillis: 1000, DeadlineAtUnixMillis: 61000,
		Progress: membership.NodeDrainProgressDTO{
			ConnectionsInitial: 4, ConnectionsRemaining: 3,
			SessionsReadyForTakeover: 4, SessionsTakenOver: 1,
			PendingWAL: 2, UnderReplicated: 1,
		},
	}
}

// Traceability: DEP-01/DEP-02; docs/specs/drain-and-rolling-update.md §2.1, §2.2, §10.
func TestNodeDrainHTTPStartReplayAndReadAuthorization(t *testing.T) {
	stub := &drainHTTPStub{startResult: membership.NodeDrainOperationDTO{State: drainHTTPState(nodedrain.PhaseExcludingTraffic)}}
	handler := newDrainHTTPTestHandler(stub, RoleOperator)
	request := drainHTTPRequest(http.MethodPost, "/api/v1/nodes/2/drain", validDrainRequestBody(), "drain-key-2")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusAccepted {
		t.Fatalf("initial drain status=%d body=%s", response.Code, response.Body.String())
	}
	var body struct {
		Data struct {
			OperationID string `json:"operationId"`
			NodeID      string `json:"nodeId"`
			State       string `json:"state"`
			Phase       string `json:"phase"`
			StartedAt   string `json:"startedAt"`
			DeadlineAt  string `json:"deadlineAt"`
			SafeToStop  bool   `json:"safeToStop"`
		} `json:"data"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode drain response: %v", err)
	}
	if body.Data.OperationID != drainHTTPKey || body.Data.NodeID != "2" || body.Data.State != "DRAINING" ||
		body.Data.Phase != "EXCLUDING_TRAFFIC" || body.Data.StartedAt != "1970-01-01T00:00:01Z" ||
		body.Data.DeadlineAt != "1970-01-01T00:01:01Z" || body.Data.SafeToStop {
		t.Fatalf("drain response = %+v", body.Data)
	}
	if stub.startCalls != 1 || stub.startRequest.MemberID != 2 || stub.startRequest.IdempotencyKey != "drain-key-2" ||
		stub.startRequest.ConnectionRatePerSecond != 2 || stub.startRequest.LoadBalancerPropagationDelayMillis != 1500 ||
		stub.startRequest.TimeoutMillis != 60000 || stub.startRequest.ServerReference != "mqtt.example:8883" {
		t.Fatalf("start request = %+v, calls=%d", stub.startRequest, stub.startCalls)
	}

	stub.startResult.Replayed = true
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, drainHTTPRequest(http.MethodPost, "/api/v1/nodes/2/drain", validDrainRequestBody(), "drain-key-2"))
	if response.Code != http.StatusOK || stub.startCalls != 2 {
		t.Fatalf("replayed drain status=%d calls=%d body=%s", response.Code, stub.startCalls, response.Body.String())
	}

	readOnlyHandler := newDrainHTTPTestHandler(stub, RoleReadOnly)
	response = httptest.NewRecorder()
	readOnlyHandler.ServeHTTP(response, drainHTTPRequest(http.MethodPost, "/api/v1/nodes/2/drain", validDrainRequestBody(), "drain-key-2"))
	if response.Code != http.StatusForbidden || stub.startCalls != 2 {
		t.Fatalf("read-only POST status=%d calls=%d body=%s", response.Code, stub.startCalls, response.Body.String())
	}
}

// Traceability: DEP-01/DEP-02; docs/specs/drain-and-rolling-update.md §2.1, §2.2, §2.3, §10.
func TestNodeDrainHTTPQueriesCancelAndRejectInvalidRequests(t *testing.T) {
	stub := &drainHTTPStub{
		getResult:    membership.NodeDrainOperationDTO{State: drainHTTPState(nodedrain.PhaseTransferringSessions)},
		cancelResult: membership.NodeDrainOperationDTO{State: drainHTTPState(nodedrain.PhaseCancelled)},
	}
	handler := newDrainHTTPTestHandler(stub, RoleOperator)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, drainHTTPRequest(http.MethodGet, "/api/v1/nodes/2/drain", "", ""))
	if response.Code != http.StatusOK || stub.getCalls != 1 || stub.memberID != 2 {
		t.Fatalf("GET drain status=%d calls=%d member=%d body=%s", response.Code, stub.getCalls, stub.memberID, response.Body.String())
	}
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, drainHTTPRequest(http.MethodDelete, "/api/v1/nodes/2/drain", "", "cancel-key-2"))
	if response.Code != http.StatusOK || stub.cancelCalls != 1 || stub.memberID != 2 {
		t.Fatalf("DELETE drain status=%d calls=%d member=%d body=%s", response.Code, stub.cancelCalls, stub.memberID, response.Body.String())
	}

	for _, test := range []struct {
		name    string
		request *http.Request
	}{
		{"required numeric fields", drainHTTPRequest(http.MethodPost, "/api/v1/nodes/2/drain", `{"connectionRatePerSecond":2,"timeoutMillis":60000}`, "drain-key")},
		{"legacy duration names", drainHTTPRequest(http.MethodPost, "/api/v1/nodes/2/drain", `{"connectionRatePerSecond":2,"loadBalancerPropagationDelayMs":1500,"timeoutMs":60000}`, "drain-key")},
		{"unknown query", drainHTTPRequest(http.MethodGet, "/api/v1/nodes/2/drain?unexpected=1", "", "")},
		{"noncanonical node ID", drainHTTPRequest(http.MethodGet, "/api/v1/nodes/02/drain", "", "")},
		{"delete body", drainHTTPRequest(http.MethodDelete, "/api/v1/nodes/2/drain", ` `, "cancel-key")},
		{"delete missing key", drainHTTPRequest(http.MethodDelete, "/api/v1/nodes/2/drain", "", "")},
	} {
		t.Run(test.name, func(t *testing.T) {
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, test.request)
			wantStatus := http.StatusBadRequest
			if test.name == "noncanonical node ID" {
				wantStatus = http.StatusNotFound
			}
			if response.Code != wantStatus {
				t.Fatalf("status=%d want=%d body=%s", response.Code, wantStatus, response.Body.String())
			}
		})
	}
	if stub.startCalls != 0 {
		t.Fatalf("invalid request reached start service %d times", stub.startCalls)
	}
}

// Traceability: DEP-01; docs/specs/drain-and-rolling-update.md §1, §2.3, §10.
func TestNodeDrainHTTPConflictIncludesCurrentNodeState(t *testing.T) {
	stub := &drainHTTPStub{startError: &membership.ServiceError{
		Code:      membership.ServiceErrorInvalidNodeTransition,
		Message:   "node lifecycle transition is not allowed",
		NodeState: "JOINING",
	}}
	handler := newDrainHTTPTestHandler(stub, RoleOperator)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, drainHTTPRequest(http.MethodPost, "/api/v1/nodes/2/drain", validDrainRequestBody(), "drain-key"))
	if response.Code != http.StatusConflict {
		t.Fatalf("invalid transition status=%d body=%s", response.Code, response.Body.String())
	}
	var envelope struct {
		Error struct {
			Code    string `json:"code"`
			Details struct {
				State string `json:"state"`
			} `json:"details"`
		} `json:"error"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("decode error envelope: %v", err)
	}
	if envelope.Error.Code != "invalid_node_transition" || envelope.Error.Details.State != "JOINING" {
		t.Fatalf("error response = %+v", envelope.Error)
	}
}
func TestNodeDrainHTTPMapsReadAndCancelServiceErrors(t *testing.T) {
	stub := &drainHTTPStub{
		getError: &membership.ServiceError{
			Code: membership.ServiceErrorResourceNotFound, Message: "node drain was not found",
		},
		cancelError: &membership.ServiceError{
			Code: membership.ServiceErrorDrainNotCancellable, Message: "node drain is already terminal",
		},
	}
	handler := newDrainHTTPTestHandler(stub, RoleOperator)
	for _, testCase := range []struct {
		name       string
		method     string
		key        string
		wantStatus int
		wantCode   string
	}{
		{name: "get missing drain", method: http.MethodGet, wantStatus: http.StatusNotFound, wantCode: "resource_not_found"},
		{name: "cancel completed drain", method: http.MethodDelete, key: "cancel-key", wantStatus: http.StatusConflict, wantCode: "drain_not_cancellable"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			request := drainHTTPRequest(testCase.method, "/api/v1/nodes/2/drain", "", testCase.key)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)

			var envelope ErrorResponse
			if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
				t.Fatalf("decode error response: %v", err)
			}
			if response.Code != testCase.wantStatus || envelope.Error.Code != testCase.wantCode {
				t.Fatalf("status=%d code=%q, want status=%d code=%q body=%s", response.Code, envelope.Error.Code, testCase.wantStatus, testCase.wantCode, response.Body.String())
			}
		})
	}
	if stub.getCalls != 1 || stub.cancelCalls != 1 {
		t.Fatalf("service calls get=%d cancel=%d, want one each", stub.getCalls, stub.cancelCalls)
	}
}
