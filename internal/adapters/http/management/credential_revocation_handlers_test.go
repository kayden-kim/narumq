package management

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"narumq/internal/application/membership"
	portsaudit "narumq/internal/application/ports/audit"
)

type credentialRevocationHTTPStub struct {
	states        []membership.CredentialRevocationStateDTO
	listErr       error
	revokeStatus  membership.CredentialRevocationStatusDTO
	revokeErr     error
	restoreStatus membership.CredentialRevocationStatusDTO
	restoreErr    error
	listCalls     int
	revokeCalls   int
	restoreCalls  int
	revokeID      string
	restoreID     string
}

func (stub *credentialRevocationHTTPStub) ListCredentialRevocations(context.Context) ([]membership.CredentialRevocationStateDTO, error) {
	stub.listCalls++
	return stub.states, stub.listErr
}

func (stub *credentialRevocationHTTPStub) RevokeCredential(_ context.Context, credentialID string) (membership.CredentialRevocationStatusDTO, error) {
	stub.revokeCalls++
	stub.revokeID = credentialID
	return stub.revokeStatus, stub.revokeErr
}

func (stub *credentialRevocationHTTPStub) RestoreCredential(_ context.Context, credentialID string) (membership.CredentialRevocationStatusDTO, error) {
	stub.restoreCalls++
	stub.restoreID = credentialID
	return stub.restoreStatus, stub.restoreErr
}

func newCredentialRevocationHTTPTestHandler(service CredentialRevocationUseCase, role Role, auditService AuditUseCase) http.Handler {
	return NewRouterWithMembership(ServerConfig{APIKeys: []APIKeyCredential{{ID: "test", Key: "credential-secret", Roles: []Role{role}}}, ClusterID: "test-cluster"},
		&mockStateProvider{}, nil, nil, nil, nil, nil, nil, auditService, nil, service, nil, nil, nil)
}

func credentialRevocationHTTPRequest(method, path string, body io.Reader) *http.Request {
	request := httptest.NewRequest(method, path, body)
	request.Header.Set("X-API-Key", "credential-secret")
	return request
}

// Traceability: SEC-07; docs/specs/tenancy-and-security.md §7; docs/contracts/management-api-v1.md.
func TestCredentialRevocationHTTPListReturnsTimestampedState(t *testing.T) {
	service := &credentialRevocationHTTPStub{states: []membership.CredentialRevocationStateDTO{
		{CredentialID: "credential-a", RevokedAtUnixMillis: 1_700_000_000_000},
		{CredentialID: "credential-z", RevokedAtUnixMillis: 1_700_000_000_123},
	}}
	handler := newCredentialRevocationHTTPTestHandler(service, RoleSecurityAdmin, nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, credentialRevocationHTTPRequest(http.MethodGet, "/api/v1/security/credential-revocations", nil))
	if response.Code != http.StatusOK || service.listCalls != 1 {
		t.Fatalf("list response=%d calls=%d body=%s", response.Code, service.listCalls, response.Body.String())
	}
	var body struct {
		Data struct {
			Revocations []struct {
				CredentialID string    `json:"credentialId"`
				RevokedAt    time.Time `json:"revokedAt"`
			} `json:"revocations"`
		} `json:"data"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if len(body.Data.Revocations) != 2 || body.Data.Revocations[0].CredentialID != "credential-a" || body.Data.Revocations[1].CredentialID != "credential-z" ||
		body.Data.Revocations[1].RevokedAt.UnixMilli() != 1_700_000_000_123 {
		t.Fatalf("revocation response = %+v", body.Data.Revocations)
	}
}

// Traceability: SEC-07; docs/specs/tenancy-and-security.md §7; docs/contracts/management-api-v1.md; docs/contracts/audit-event-v1.md.
func TestCredentialRevocationHTTPMutationsReturnStatusAndAuditCommittedIdentity(t *testing.T) {
	const revokedAt = uint64(1_700_000_000_123)
	service := &credentialRevocationHTTPStub{
		revokeStatus:  membership.CredentialRevocationStatusDTO{CredentialID: "credential-é", Revoked: true, RevokedAtUnixMillis: revokedAt},
		restoreStatus: membership.CredentialRevocationStatusDTO{CredentialID: "credential-é"},
	}
	auditService := &spyAuditUseCase{}
	handler := newCredentialRevocationHTTPTestHandler(service, RoleSecurityAdmin, auditService)

	request := credentialRevocationHTTPRequest(http.MethodPost, "/api/v1/security/credential-revocations/credential-%C3%A9", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	var revokeBody struct {
		Data struct {
			CredentialID string    `json:"credentialId"`
			Revoked      bool      `json:"revoked"`
			RevokedAt    time.Time `json:"revokedAt"`
		} `json:"data"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &revokeBody); err != nil {
		t.Fatal(err)
	}
	if response.Code != http.StatusOK || service.revokeCalls != 1 || service.revokeID != "credential-é" || !revokeBody.Data.Revoked || revokeBody.Data.CredentialID != service.revokeID || revokeBody.Data.RevokedAt.UnixMilli() != int64(revokedAt) {
		t.Fatalf("revoke response=%d body=%+v service=%+v", response.Code, revokeBody.Data, service)
	}
	if len(auditService.commands) != 1 {
		t.Fatalf("revoke audit events=%+v", auditService.commands)
	}
	revokeEvent := auditService.commands[0]
	if revokeEvent.Action != "security.credential.revoke" || revokeEvent.Target.Type != "mqtt_credential" || revokeEvent.Target.ID != service.revokeID || revokeEvent.Result != portsaudit.AuditResultAllowed || revokeEvent.Reason != "succeeded" || revokeEvent.Actor.Subject != "apikey:test" {
		t.Fatalf("revoke audit event=%+v", revokeEvent)
	}

	request = credentialRevocationHTTPRequest(http.MethodDelete, "/api/v1/security/credential-revocations/credential-%C3%A9", nil)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	var restoreBody map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &restoreBody); err != nil {
		t.Fatal(err)
	}
	data, ok := restoreBody["data"].(map[string]any)
	_, hasRevokedAt := data["revokedAt"]
	if response.Code != http.StatusOK || service.restoreCalls != 1 || service.restoreID != service.revokeID || !ok || data["revoked"] != false || hasRevokedAt {
		t.Fatalf("restore response=%d body=%v service=%+v", response.Code, restoreBody, service)
	}
	if len(auditService.commands) != 2 || auditService.commands[1].Action != "security.credential.restore" || auditService.commands[1].Result != portsaudit.AuditResultAllowed {
		t.Fatalf("restore audit events=%+v", auditService.commands)
	}
}

// Traceability: SEC-07; docs/specs/tenancy-and-security.md §7; docs/contracts/management-api-v1.md.
func TestCredentialRevocationHTTPEnforcesPermissionAndAuditAdmission(t *testing.T) {
	service := &credentialRevocationHTTPStub{
		revokeStatus: membership.CredentialRevocationStatusDTO{CredentialID: "credential-a", Revoked: true, RevokedAtUnixMillis: 1},
	}
	for _, role := range []Role{RoleReadOnly, RoleOperator} {
		auditService := &spyAuditUseCase{}
		handler := newCredentialRevocationHTTPTestHandler(service, role, auditService)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, credentialRevocationHTTPRequest(http.MethodPost, "/api/v1/security/credential-revocations/credential-a", nil))
		if response.Code != http.StatusForbidden || service.revokeCalls != 0 || len(auditService.commands) != 1 || auditService.commands[0].Reason != "forbidden" {
			t.Fatalf("role %s response=%d calls=%d audit=%+v", role, response.Code, service.revokeCalls, auditService.commands)
		}
	}

	blockedAudit := &spyAuditUseCase{admitErr: errors.New("audit unavailable")}
	blockedHandler := newCredentialRevocationHTTPTestHandler(service, RoleClusterAdmin, blockedAudit)
	response := httptest.NewRecorder()
	blockedHandler.ServeHTTP(response, credentialRevocationHTTPRequest(http.MethodPost, "/api/v1/security/credential-revocations/credential-a", nil))
	if response.Code != http.StatusServiceUnavailable || service.revokeCalls != 0 || blockedAudit.admitCalls != 1 || len(blockedAudit.commands) != 0 {
		t.Fatalf("blocked mutation response=%d calls=%d audit=%+v body=%s", response.Code, service.revokeCalls, blockedAudit, response.Body.String())
	}
}

// Traceability: SEC-07; docs/specs/tenancy-and-security.md §7; docs/contracts/management-api-v1.md; docs/contracts/audit-event-v1.md.
func TestCredentialRevocationHTTPMapsFailuresAndRejectsInvalidRequests(t *testing.T) {
	service := &credentialRevocationHTTPStub{
		revokeErr: &membership.ServiceError{Code: membership.ServiceErrorCredentialRevocationCapacity, Message: "credential revocation capacity is exhausted"},
	}
	auditService := &spyAuditUseCase{}
	handler := newCredentialRevocationHTTPTestHandler(service, RoleSecurityAdmin, auditService)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, credentialRevocationHTTPRequest(http.MethodPost, "/api/v1/security/credential-revocations/credential-overflow", nil))
	var envelope ErrorResponse
	if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
		t.Fatal(err)
	}
	if response.Code != http.StatusConflict || envelope.Error.Code != string(membership.ServiceErrorCredentialRevocationCapacity) || len(auditService.commands) != 1 || auditService.commands[0].Result != portsaudit.AuditResultFailed || auditService.commands[0].Reason != envelope.Error.Code {
		t.Fatalf("capacity response=%d error=%+v audit=%+v", response.Code, envelope.Error, auditService.commands)
	}

	for _, request := range []*http.Request{
		credentialRevocationHTTPRequest(http.MethodPost, "/api/v1/security/credential-revocations/credential-a?unexpected=1", nil),
		credentialRevocationHTTPRequest(http.MethodDelete, "/api/v1/security/credential-revocations/credential-a", strings.NewReader("x")),
		credentialRevocationHTTPRequest(http.MethodPost, "/api/v1/security/credential-revocations/"+strings.Repeat("x", membership.MaxCredentialIDBytes+1), nil),
	} {
		response = httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusBadRequest {
			t.Errorf("invalid request %s %s status=%d body=%s", request.Method, request.URL, response.Code, response.Body.String())
		}
	}
	if service.revokeCalls != 1 || service.restoreCalls != 0 {
		t.Fatalf("invalid requests reached service: revoke=%d restore=%d", service.revokeCalls, service.restoreCalls)
	}

	methodRequest := credentialRevocationHTTPRequest(http.MethodPut, "/api/v1/security/credential-revocations/credential-a", nil)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, methodRequest)
	if response.Code != http.StatusMethodNotAllowed || response.Header().Get("Allow") != "POST, DELETE" {
		t.Fatalf("unsupported method response=%d Allow=%q", response.Code, response.Header().Get("Allow"))
	}
}

// Traceability: SEC-07; docs/specs/tenancy-and-security.md §7.
func TestCredentialRevocationRoutesAreAbsentWithoutClusterService(t *testing.T) {
	handler := NewRouterWithMembership(ServerConfig{}, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/api/v1/security/credential-revocations", nil)
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusNotFound {
		t.Fatalf("route without cluster service status=%d", response.Code)
	}
}
