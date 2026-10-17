package management

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	auditapp "narumq/internal/application/audit"
	portsaudit "narumq/internal/application/ports/audit"
)

type auditQueryStub struct {
	result auditapp.QueryResultDTO
	err    error
	calls  int
}

func (stub *auditQueryStub) Query(context.Context, auditapp.QueryCommandDTO) (auditapp.QueryResultDTO, error) {
	stub.calls++
	return stub.result, stub.err
}

func auditQueryTestRouter(query AuditQueryUseCase, roles ...Role) (http.Handler, *spyAuditUseCase) {
	spy := &spyAuditUseCase{}
	return NewRouterWithMembership(ServerConfig{LocalNodeID: 1, APIKeys: []APIKeyCredential{{ID: "query", Key: "query-secret", Roles: roles}}}, &mockStateProvider{}, nil, nil, nil, nil, nil, nil, spy, query, nil, nil, nil, nil), spy
}

// Scenarios: management-api-and-observability.md:MGT-SCN-019, tenancy-and-security.md:SEC-SCN-033
func TestAuditQueryHandlerResponsesAndStoredBytes(t *testing.T) {
	const event = `{"schemaVersion":"1.0","occurredAt":"2026-10-10T12:00:00Z","details":{"raw":"<value>"}}`
	query := &auditQueryStub{result: auditapp.QueryResultDTO{Records: []portsaudit.AuditRecordDTO{{Seq: 7, PrevHash: "prev", Hash: "hash", EventJSON: []byte(event)}}}}
	handler, spy := auditQueryTestRouter(query, RoleSecurityAdmin)
	request := httptest.NewRequest(http.MethodGet, "/api/v1/nodes/1/audit/events?limit=1&actorSubject=alice&action=act&targetType=node&targetId=4&requestId=req&result=ALLOWED&from=2026-10-10T00%3A00%3A00Z&to=2026-10-11T00%3A00%3A00Z", nil)
	request.Header.Set("X-API-Key", "query-secret")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"event":`+event) || strings.Contains(response.Body.String(), `\u003c`) {
		t.Fatalf("response status=%d body=%s", response.Code, response.Body.String())
	}
	if len(spy.commands) != 0 {
		t.Fatalf("successful query created audit events: %+v", spy.commands)
	}
	for _, target := range []string{"?unknown=x", "?limit=1&limit=2", "?from=not-a-time"} {
		request = httptest.NewRequest(http.MethodGet, "/api/v1/nodes/1/audit/events"+target, nil)
		request.Header.Set("X-API-Key", "query-secret")
		response = httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusBadRequest || !strings.Contains(response.Body.String(), `"code":"invalid_request"`) {
			t.Errorf("%s status=%d body=%s", target, response.Code, response.Body.String())
		}
	}
	if query.calls != 1 {
		t.Fatalf("invalid requests reached use case %d times", query.calls)
	}
}

// Scenarios: management-api-and-observability.md:MGT-SCN-019, tenancy-and-security.md:SEC-SCN-033
func TestAuditQueryHandlerUnavailableAndPermissionGating(t *testing.T) {
	query := &auditQueryStub{err: errors.New("private file path")}
	handler, _ := auditQueryTestRouter(query, RoleSecurityAdmin)
	request := httptest.NewRequest(http.MethodGet, "/api/v1/nodes/1/audit/events", nil)
	request.Header.Set("X-API-Key", "query-secret")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable || !strings.Contains(response.Body.String(), `"code":"audit_unavailable"`) || !strings.Contains(response.Body.String(), `"retryable":true`) || strings.Contains(response.Body.String(), "private file path") {
		t.Fatalf("unavailable response=%d %s", response.Code, response.Body.String())
	}
	badQuery := &auditQueryStub{err: auditapp.ErrInvalidQuery}
	invalidHandler, _ := auditQueryTestRouter(badQuery, RoleSecurityAdmin)
	request = httptest.NewRequest(http.MethodGet, "/api/v1/nodes/1/audit/events", nil)
	request.Header.Set("X-API-Key", "query-secret")
	response = httptest.NewRecorder()
	invalidHandler.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("invalid use case error status=%d", response.Code)
	}
	readHandler, _ := auditQueryTestRouter(&auditQueryStub{}, RoleReadOnly)
	request = httptest.NewRequest(http.MethodGet, "/api/v1/nodes/1/audit/events", nil)
	request.Header.Set("X-API-Key", "query-secret")
	response = httptest.NewRecorder()
	readHandler.ServeHTTP(response, request)
	if response.Code != http.StatusForbidden {
		t.Fatalf("read-only audit status=%d", response.Code)
	}
	request = httptest.NewRequest(http.MethodPost, "/api/v1/nodes/1/audit/events", nil)
	request.Header.Set("X-API-Key", "query-secret")
	response = httptest.NewRecorder()
	readHandler.ServeHTTP(response, request)
	if response.Code != http.StatusMethodNotAllowed || response.Header().Get("Allow") != http.MethodGet {
		t.Fatalf("POST status=%d Allow=%q", response.Code, response.Header().Get("Allow"))
	}
	noRoute, _ := auditQueryTestRouter(nil, RoleClusterAdmin)
	request = httptest.NewRequest(http.MethodGet, "/api/v1/nodes/1/audit/events", nil)
	request.Header.Set("X-API-Key", "query-secret")
	response = httptest.NewRecorder()
	noRoute.ServeHTTP(response, request)
	if response.Code != http.StatusNotFound {
		t.Fatalf("uninjected route status=%d", response.Code)
	}
}

// Traceability: NFR-17; docs/specs/tenancy-and-security.md §8.
// Scenarios: tenancy-and-security.md:SEC-SCN-051
func TestAuditQueryOnlySecurityAdminAndClusterAdminCanRead(t *testing.T) {
	for _, test := range []struct {
		role Role
		want int
	}{
		{role: RoleReadOnly, want: http.StatusForbidden},
		{role: RoleOperator, want: http.StatusForbidden},
		{role: RoleSecurityAdmin, want: http.StatusOK},
		{role: RoleClusterAdmin, want: http.StatusOK},
	} {
		t.Run(test.role.String(), func(t *testing.T) {
			query := &auditQueryStub{result: auditapp.QueryResultDTO{Records: []portsaudit.AuditRecordDTO{}}}
			handler, _ := auditQueryTestRouter(query, test.role)
			request := httptest.NewRequest(http.MethodGet, "/api/v1/nodes/1/audit/events", nil)
			request.Header.Set("X-API-Key", "query-secret")
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != test.want {
				t.Fatalf("audit query status=%d, want %d: %s", response.Code, test.want, response.Body.String())
			}
			wantCalls := 0
			if test.want == http.StatusOK {
				wantCalls = 1
			}
			if query.calls != wantCalls {
				t.Fatalf("audit storage reads=%d, want %d", query.calls, wantCalls)
			}
		})
	}
}

// Scenarios: configuration-and-runtime-options.md:CFG-SCN-32
func TestNodeLocalRouteScope(t *testing.T) {
	query := &auditQueryStub{}
	handler, _ := auditQueryTestRouter(query, RoleSecurityAdmin)

	for _, testCase := range []struct {
		name       string
		path       string
		wantStatus int
		wantBody   string
	}{
		{
			name: "non-local node", path: "/api/v1/nodes/2/audit/events",
			wantStatus: http.StatusConflict, wantBody: `"code":"node_not_local","message":"nodeId does not identify the node serving this request","retryable":false,"details":{"servingNodeId":"1"}`,
		},
		{
			name: "noncanonical node ID", path: "/api/v1/nodes/01/audit/events",
			wantStatus: http.StatusNotFound, wantBody: `"code":"resource_not_found"`,
		},
		{
			name: "local node", path: "/api/v1/nodes/1/audit/events",
			wantStatus: http.StatusOK,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodGet, testCase.path, nil)
			request.Header.Set("X-API-Key", "query-secret")
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != testCase.wantStatus {
				t.Fatalf("status=%d body=%s, want %d", response.Code, response.Body.String(), testCase.wantStatus)
			}
			if testCase.wantBody != "" && !strings.Contains(response.Body.String(), testCase.wantBody) {
				t.Fatalf("body=%s, want %q", response.Body.String(), testCase.wantBody)
			}
		})
	}
	if query.calls != 1 {
		t.Fatalf("query use case calls=%d, want only the local-node request", query.calls)
	}
}
