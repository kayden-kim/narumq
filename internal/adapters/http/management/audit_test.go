package management

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"narumq/internal/application/audit"
	"narumq/internal/application/membership"
	portsaudit "narumq/internal/application/ports/audit"
	portsmanagement "narumq/internal/application/ports/management"
	"narumq/internal/domain/nodedrain"
)

type spyAuditUseCase struct {
	admitErr   error
	admitCalls int
	commands   []audit.RecordCommandDTO
	recordErr  error
}

func (spy *spyAuditUseCase) AdmitMutation() error {
	spy.admitCalls++
	return spy.admitErr
}

func (spy *spyAuditUseCase) Record(command audit.RecordCommandDTO) error {
	spy.commands = append(spy.commands, command)
	return spy.recordErr
}

func TestManagementMutationAuditRecordsSuccessfulDrain(t *testing.T) {
	service := &drainHTTPStub{startResult: membership.NodeDrainOperationDTO{State: drainHTTPState(nodedrain.PhaseExcludingTraffic)}}
	spy := &spyAuditUseCase{}
	handler := NewRouterWithMembership(ServerConfig{APIKeys: []APIKeyCredential{{ID: "test", Key: "audit-token", Roles: []Role{RoleOperator}}}, ClusterID: "test-cluster"}, &mockStateProvider{}, nil, nil, nil, service, nil, nil, spy, nil, nil)
	request := drainHTTPRequest(http.MethodPost, "/api/v1/nodes/3/drain", validDrainRequestBody(), "audit-idempotency-key")
	request.Header.Set("X-API-Key", "audit-token")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusAccepted || len(spy.commands) != 1 {
		t.Fatalf("status=%d events=%d body=%s", response.Code, len(spy.commands), response.Body.String())
	}
	event := spy.commands[0]
	if event.Result != portsaudit.AuditResultAllowed || event.Action != "node.drain.start" || event.Target.Type != "node" || event.Target.ID != "3" || event.Actor.Subject != "apikey:test" {
		t.Fatalf("event=%+v", event)
	}
	if event.RequestID == "" || event.RequestID != response.Header().Get(HeaderRequestID) || event.Details["httpStatus"] != http.StatusAccepted || event.Details["idempotencyKey"] != "audit-idempotency-key" {
		t.Fatalf("requestId/details=%q/%v response=%q", event.RequestID, event.Details, response.Header().Get(HeaderRequestID))
	}
}

func TestManagementAuditDenialsAndAdmission(t *testing.T) {
	service := &drainHTTPStub{startResult: membership.NodeDrainOperationDTO{State: drainHTTPState(nodedrain.PhaseExcludingTraffic)}}
	spy := &spyAuditUseCase{}
	handler := NewRouterWithMembership(ServerConfig{APIKeys: []APIKeyCredential{{ID: "test", Key: "audit-token", Roles: []Role{RoleReadOnly}}}}, &mockStateProvider{}, nil, nil, nil, service, nil, nil, spy, nil, nil)
	response := httptest.NewRecorder()
	request := auditDrainRequest(http.MethodPost, "/api/v1/nodes/3/drain")
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusForbidden || len(spy.commands) != 1 || spy.admitCalls != 0 || spy.commands[0].Result != portsaudit.AuditResultDenied || spy.commands[0].Reason != "forbidden" {
		t.Fatalf("forbidden status=%d events=%+v admission=%d", response.Code, spy.commands, spy.admitCalls)
	}

	bad := httptest.NewRequest(http.MethodGet, "/api/v1/cluster", nil)
	bad.Header.Set("Authorization", "Bearer invalid-token")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, bad)
	denied := spy.commands[1]
	if response.Code != http.StatusUnauthorized || denied.Result != portsaudit.AuditResultDenied || denied.Action != "management.request" || denied.Target.Type != "management_endpoint" || denied.Target.ID != "/api/v1/cluster" || denied.Actor.Subject != "unauthenticated" || len(denied.Actor.Roles) != 0 {
		t.Fatalf("unauthorized status=%d event=%+v", response.Code, denied)
	}
	encoded, err := json.Marshal(spy.commands)
	if err != nil || strings.Contains(string(encoded), "invalid-token") || strings.Contains(string(encoded), "audit-token") {
		t.Fatalf("credential leaked in audit events: %s err=%v", encoded, err)
	}

	blocked := &spyAuditUseCase{admitErr: errors.New("sink unavailable")}
	blockedHandler := NewRouterWithMembership(ServerConfig{APIKeys: []APIKeyCredential{{ID: "test", Key: "audit-token", Roles: []Role{RoleOperator}}}}, &mockStateProvider{}, nil, nil, nil, service, nil, nil, blocked, nil, nil)
	request = auditDrainRequest(http.MethodPost, "/api/v1/nodes/3/drain")
	response = httptest.NewRecorder()
	blockedHandler.ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable || !strings.Contains(response.Body.String(), `"code":"audit_unavailable"`) || !strings.Contains(response.Body.String(), `"retryable":true`) || service.startCalls != 0 || blocked.admitCalls != 1 || len(blocked.commands) != 0 {
		t.Fatalf("admission response=%s calls=%d audit=%+v", response.Body.String(), service.startCalls, blocked)
	}
	noAudit := NewRouterWithMembership(ServerConfig{APIKeys: []APIKeyCredential{{ID: "test", Key: "audit-token", Roles: []Role{RoleOperator}}}}, &mockStateProvider{}, nil, nil, nil, service, nil, nil, nil, nil, nil)
	request = auditDrainRequest(http.MethodPost, "/api/v1/nodes/3/drain")
	response = httptest.NewRecorder()
	noAudit.ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable || service.startCalls != 0 {
		t.Fatalf("nil audit response=%d service calls=%d", response.Code, service.startCalls)
	}
}

func TestManagementAuditMutationFailuresAndBackup(t *testing.T) {
	spy := &spyAuditUseCase{}
	conflict := &drainHTTPStub{startError: errors.New("conflict")}
	handler := NewRouterWithMembership(ServerConfig{APIKeys: []APIKeyCredential{{ID: "test", Key: "audit-token", Roles: []Role{RoleOperator}}}}, &mockStateProvider{}, nil, nil, nil, conflict, nil, nil, spy, nil, nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, auditDrainRequest(http.MethodPost, "/api/v1/nodes/3/drain"))
	var envelope ErrorResponse
	if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
		t.Fatal(err)
	}
	if len(spy.commands) != 1 || spy.commands[0].Result != portsaudit.AuditResultFailed || spy.commands[0].Reason != envelope.Error.Code {
		t.Fatalf("failure response=%+v event=%+v", envelope, spy.commands)
	}

	backupSpy := &spyAuditUseCase{}
	backup := &clusterBackupProviderStub{backup: portsmanagement.ClusterBackupDTO{Archive: []byte("backup")}}
	request := httptest.NewRequest(http.MethodPost, "/api/v1/cluster/backups", nil)
	request.Header.Set("Authorization", "Bearer backup-token")
	backupRouter := newRouter(ServerConfig{AuthToken: "backup-token"}, &mockStateProvider{}, nil, nil, nil, nil, nil, backup, backupSpy, nil, nil)
	response = httptest.NewRecorder()
	backupRouter.ServeHTTP(response, request)
	if response.Code != http.StatusOK || len(backupSpy.commands) != 1 || backupSpy.commands[0].Result != portsaudit.AuditResultAllowed || backupSpy.commands[0].Action != "cluster.backup.create" || backupSpy.commands[0].Details["httpStatus"] != http.StatusOK {
		t.Fatalf("backup status=%d event=%+v", response.Code, backupSpy.commands)
	}
}

func TestStageMemberAuditTargetAndReadDoesNotAdmit(t *testing.T) {
	spy := &spyAuditUseCase{}
	service := &membershipServiceStub{}
	handler := NewRouterWithMembership(ServerConfig{AuthToken: "audit-token", ClusterID: "test-cluster"}, &mockStateProvider{}, nil, nil, service, nil, nil, nil, spy, nil, nil)
	request := httptest.NewRequest(http.MethodPost, "/api/v1/cluster/members", strings.NewReader(validMemberAdmissionJSON()))
	request.Header.Set("Authorization", "Bearer audit-token")
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", "stage-key")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if len(spy.commands) != 1 || spy.commands[0].Target.Type != "cluster_member" || spy.commands[0].Target.ID != "4" {
		t.Fatalf("stage event=%+v", spy.commands)
	}
	badSpy := &spyAuditUseCase{}
	badHandler := NewRouterWithMembership(ServerConfig{AuthToken: "audit-token", ClusterID: "test-cluster"}, &mockStateProvider{}, nil, nil, service, nil, nil, nil, badSpy, nil, nil)
	bad := httptest.NewRequest(http.MethodPost, "/api/v1/cluster/members", strings.NewReader("{"))
	bad.Header.Set("Authorization", "Bearer audit-token")
	bad.Header.Set("Content-Type", "application/json")
	bad.Header.Set("Idempotency-Key", "stage-key")
	badResponse := httptest.NewRecorder()
	badHandler.ServeHTTP(badResponse, bad)
	if len(badSpy.commands) != 1 || badSpy.commands[0].Target.Type != "cluster" || badSpy.commands[0].Target.ID != "test-cluster" || badSpy.commands[0].Reason != "invalid_request" {
		t.Fatalf("invalid stage event=%+v", badSpy.commands)
	}
	readSpy := &spyAuditUseCase{}
	readHandler := NewRouterWithMembership(ServerConfig{AuthToken: "audit-token"}, &mockStateProvider{}, nil, nil, service, nil, nil, nil, readSpy, nil, nil)
	get := httptest.NewRequest(http.MethodGet, "/api/v1/cluster/members", nil)
	get.Header.Set("Authorization", "Bearer audit-token")
	getResponse := httptest.NewRecorder()
	readHandler.ServeHTTP(getResponse, get)
	if readSpy.admitCalls != 0 || len(readSpy.commands) != 0 || getResponse.Code != http.StatusMethodNotAllowed {
		t.Fatalf("GET members status=%d admissions=%d events=%d", getResponse.Code, readSpy.admitCalls, len(readSpy.commands))
	}
}

func auditDrainRequest(method, path string) *http.Request {
	request := drainHTTPRequest(method, path, validDrainRequestBody(), "key")
	request.Header.Set("X-API-Key", "audit-token")
	return request
}

func TestManagementAuditPanicRecordsInternalErrorWithServerErrorStatus(t *testing.T) {
	spy := &spyAuditUseCase{}
	authenticator := NewAuthenticator(AuthConfig{
		APIKeys:   []APIKeyCredential{{ID: "test", Key: "audit-token", Roles: []Role{RoleOperator}}},
		ClusterID: "test-cluster",
		Audit:     spy,
	})
	handler := authenticator.RequireMutationPermission(PermissionOperate, auditMutation{
		action: "node.drain.start", targetType: "node",
	}, func(http.ResponseWriter, *http.Request) {
		panic("handler panic")
	})
	request := httptest.NewRequest(http.MethodPost, "/api/v1/nodes/3/drain", nil)
	request.Header.Set("X-API-Key", "audit-token")
	response := httptest.NewRecorder()

	var recovered any
	func() {
		defer func() {
			recovered = recover()
		}()
		handler.ServeHTTP(response, request)
	}()
	if recovered != "handler panic" {
		t.Fatalf("recovered panic = %v, want original handler panic", recovered)
	}
	if len(spy.commands) != 1 {
		t.Fatalf("recorded audit commands = %d, want 1", len(spy.commands))
	}
	event := spy.commands[0]
	if event.Result != portsaudit.AuditResultFailed || event.Reason != "internal_error" ||
		event.Details["httpStatus"] != http.StatusInternalServerError {
		t.Fatalf("panic audit event=%+v, want FAILED internal_error with status 500", event)
	}
}
func TestManagementMutationAuditUsesImplicitSuccessStatus(t *testing.T) {
	spy := &spyAuditUseCase{}
	authenticator := NewAuthenticator(AuthConfig{
		APIKeys: []APIKeyCredential{{ID: "test", Key: "audit-token", Roles: []Role{RoleOperator}}}, Audit: spy,
	})
	handler := authenticator.RequireMutationPermission(PermissionOperate, auditMutation{
		action: "node.drain.cancel", targetType: "node",
	}, func(http.ResponseWriter, *http.Request) {})
	request := httptest.NewRequest(http.MethodDelete, "/api/v1/nodes/3/drain", nil)
	request.Header.Set("X-API-Key", "audit-token")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	if response.Code != http.StatusOK || len(spy.commands) != 1 {
		t.Fatalf("implicit response status=%d audit events=%d", response.Code, len(spy.commands))
	}
	event := spy.commands[0]
	if event.Result != portsaudit.AuditResultAllowed || event.Reason != "succeeded" ||
		event.Details["httpStatus"] != http.StatusOK || event.Target.Type != "node" {
		t.Fatalf("implicit success audit event=%+v", event)
	}
}
