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
	return NewRouterWithMembership(ServerConfig{APIKeys: []APIKeyCredential{{ID: "query", Key: "query-secret", Roles: roles}}}, &mockStateProvider{}, nil, nil, nil, nil, nil, nil, spy, query, nil, nil, nil), spy
}

func TestAuditQueryHandlerResponsesAndStoredBytes(t *testing.T) {
	const event = `{"schemaVersion":"1.0","occurredAt":"2026-10-10T12:00:00Z","details":{"raw":"<value>"}}`
	query := &auditQueryStub{result: auditapp.QueryResultDTO{Records: []portsaudit.AuditRecordDTO{{Seq: 7, PrevHash: "prev", Hash: "hash", EventJSON: []byte(event)}}}}
	handler, spy := auditQueryTestRouter(query, RoleSecurityAdmin)
	request := httptest.NewRequest(http.MethodGet, "/api/v1/audit/events?limit=1&actorSubject=alice&action=act&targetType=node&targetId=4&requestId=req&result=ALLOWED&from=2026-10-10T00%3A00%3A00Z&to=2026-10-11T00%3A00%3A00Z", nil)
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
		request = httptest.NewRequest(http.MethodGet, "/api/v1/audit/events"+target, nil)
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

func TestAuditQueryHandlerUnavailableAndPermissionGating(t *testing.T) {
	query := &auditQueryStub{err: errors.New("private file path")}
	handler, _ := auditQueryTestRouter(query, RoleSecurityAdmin)
	request := httptest.NewRequest(http.MethodGet, "/api/v1/audit/events", nil)
	request.Header.Set("X-API-Key", "query-secret")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable || !strings.Contains(response.Body.String(), `"code":"audit_unavailable"`) || !strings.Contains(response.Body.String(), `"retryable":true`) || strings.Contains(response.Body.String(), "private file path") {
		t.Fatalf("unavailable response=%d %s", response.Code, response.Body.String())
	}
	badQuery := &auditQueryStub{err: auditapp.ErrInvalidQuery}
	invalidHandler, _ := auditQueryTestRouter(badQuery, RoleSecurityAdmin)
	request = httptest.NewRequest(http.MethodGet, "/api/v1/audit/events", nil)
	request.Header.Set("X-API-Key", "query-secret")
	response = httptest.NewRecorder()
	invalidHandler.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("invalid use case error status=%d", response.Code)
	}
	readHandler, _ := auditQueryTestRouter(&auditQueryStub{}, RoleReadOnly)
	request = httptest.NewRequest(http.MethodGet, "/api/v1/audit/events", nil)
	request.Header.Set("X-API-Key", "query-secret")
	response = httptest.NewRecorder()
	readHandler.ServeHTTP(response, request)
	if response.Code != http.StatusForbidden {
		t.Fatalf("read-only audit status=%d", response.Code)
	}
	request = httptest.NewRequest(http.MethodPost, "/api/v1/audit/events", nil)
	request.Header.Set("X-API-Key", "query-secret")
	response = httptest.NewRecorder()
	readHandler.ServeHTTP(response, request)
	if response.Code != http.StatusMethodNotAllowed || response.Header().Get("Allow") != http.MethodGet {
		t.Fatalf("POST status=%d Allow=%q", response.Code, response.Header().Get("Allow"))
	}
	noRoute, _ := auditQueryTestRouter(nil, RoleClusterAdmin)
	request = httptest.NewRequest(http.MethodGet, "/api/v1/audit/events", nil)
	request.Header.Set("X-API-Key", "query-secret")
	response = httptest.NewRecorder()
	noRoute.ServeHTTP(response, request)
	if response.Code != http.StatusNotFound {
		t.Fatalf("uninjected route status=%d", response.Code)
	}
}
