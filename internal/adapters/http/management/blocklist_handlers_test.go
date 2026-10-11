package management

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"narumq/internal/application/membership"
	portsaudit "narumq/internal/application/ports/audit"
	"narumq/internal/domain/security"
)

type blocklistHTTPStub struct {
	state         membership.BlocklistDTO
	listErr       error
	blockErr      error
	unblockErr    error
	blockStatus   membership.BlocklistStatusDTO
	unblockStatus membership.BlocklistStatusDTO
	listCalls     int
	blockCalls    int
	unblockCalls  int
	blockEntry    security.BlockEntry
	unblockEntry  security.BlockEntry
}

func (stub *blocklistHTTPStub) ListBlocklist(context.Context) (membership.BlocklistDTO, error) {
	stub.listCalls++
	return stub.state, stub.listErr
}

func (stub *blocklistHTTPStub) Block(_ context.Context, entry security.BlockEntry) (membership.BlocklistStatusDTO, error) {
	stub.blockCalls++
	stub.blockEntry = entry
	status := stub.blockStatus
	status.Entry = entry
	return status, stub.blockErr
}

func (stub *blocklistHTTPStub) Unblock(_ context.Context, entry security.BlockEntry) (membership.BlocklistStatusDTO, error) {
	stub.unblockCalls++
	stub.unblockEntry = entry
	status := stub.unblockStatus
	status.Entry = entry
	return status, stub.unblockErr
}

func newBlocklistHTTPTestHandler(service BlocklistUseCase, role Role, auditService AuditUseCase) http.Handler {
	if auditService == nil {
		auditService = &spyAuditUseCase{}
	}
	return NewRouterWithMembership(ServerConfig{APIKeys: []APIKeyCredential{{ID: "test", Key: "blocklist-secret", Roles: []Role{role}}}, ClusterID: "test-cluster"},
		&mockStateProvider{}, nil, nil, nil, nil, nil, nil, auditService, nil, nil, service, nil, nil)
}

func blocklistHTTPRequest(method, path, body string) *http.Request {
	request := httptest.NewRequest(method, path, strings.NewReader(body))
	if body == "" {
		request.Body = http.NoBody
	}
	request.Header.Set("X-API-Key", "blocklist-secret")
	return request
}

// Traceability: SEC-12; docs/specs/tenancy-and-security.md §7.1, §9; ADR-0050.
func TestBlocklistHTTPListReturnsCanonicalTimestampedState(t *testing.T) {
	entry := security.BlockEntry{Kind: security.BlockKindIPPrefix, Value: "203.0.113.0/24"}
	service := &blocklistHTTPStub{state: membership.BlocklistDTO{
		PolicyVersion: 19,
		Entries:       []membership.BlocklistStateDTO{{Entry: entry, BlockedAtUnixMillis: 1_700_000_000_123}},
	}}
	handler := newBlocklistHTTPTestHandler(service, RoleSecurityAdmin, nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, blocklistHTTPRequest(http.MethodGet, "/api/v1/security/blocklist", ""))
	var body struct {
		Data struct {
			PolicyVersion uint64 `json:"policyVersion"`
			Entries       []struct {
				Kind      string    `json:"kind"`
				TenantID  string    `json:"tenantId"`
				Value     string    `json:"value"`
				BlockedAt time.Time `json:"blockedAt"`
			} `json:"entries"`
		} `json:"data"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if response.Code != http.StatusOK || service.listCalls != 1 || body.Data.PolicyVersion != 19 || len(body.Data.Entries) != 1 ||
		body.Data.Entries[0].Kind != "ipPrefix" || body.Data.Entries[0].TenantID != "" || body.Data.Entries[0].Value != entry.Value ||
		body.Data.Entries[0].BlockedAt.Location() != time.UTC || body.Data.Entries[0].BlockedAt.UnixMilli() != 1_700_000_000_123 {
		t.Fatalf("list response=%d body=%+v calls=%d", response.Code, body.Data, service.listCalls)
	}
}

// Traceability: SEC-12; docs/specs/tenancy-and-security.md §7.1, §9; ADR-0050; docs/contracts/audit-event-v1.md.
func TestBlocklistRoutesParseEntriesAndAudit(t *testing.T) {
	const blockedAt = uint64(1_700_000_000_321)
	service := &blocklistHTTPStub{
		blockStatus:   membership.BlocklistStatusDTO{Blocked: true, BlockedAtUnixMillis: blockedAt, PolicyVersion: 31},
		unblockStatus: membership.BlocklistStatusDTO{PolicyVersion: 32},
	}
	auditService := &spyAuditUseCase{}
	handler := newBlocklistHTTPTestHandler(service, RoleSecurityAdmin, auditService)

	request := blocklistHTTPRequest(http.MethodPost, "/api/v1/security/blocklist/client-ids/tenant-a/client%2Fa", "")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	var blockBody struct {
		Data struct {
			Kind          string    `json:"kind"`
			TenantID      string    `json:"tenantId"`
			Value         string    `json:"value"`
			Blocked       bool      `json:"blocked"`
			BlockedAt     time.Time `json:"blockedAt"`
			PolicyVersion uint64    `json:"policyVersion"`
		} `json:"data"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &blockBody); err != nil {
		t.Fatal(err)
	}
	wantEntry := security.BlockEntry{Kind: security.BlockKindClientID, TenantID: "tenant-a", Value: "client/a"}
	if response.Code != http.StatusOK || service.blockCalls != 1 || service.blockEntry != wantEntry || blockBody.Data.Kind != "clientId" ||
		blockBody.Data.TenantID != "tenant-a" || blockBody.Data.Value != "client/a" || !blockBody.Data.Blocked ||
		blockBody.Data.BlockedAt.UnixMilli() != int64(blockedAt) || blockBody.Data.BlockedAt.Location() != time.UTC || blockBody.Data.PolicyVersion != 31 {
		t.Fatalf("block response=%d body=%+v service=%+v", response.Code, blockBody.Data, service)
	}
	if len(auditService.commands) != 1 {
		t.Fatalf("block audit events=%+v", auditService.commands)
	}
	blockEvent := auditService.commands[0]
	if blockEvent.Action != "security.blocklist.add" || blockEvent.Target.Type != "mqtt_blocklist_entry" || blockEvent.Target.ID != wantEntry.Key() ||
		blockEvent.Result != portsaudit.AuditResultAllowed || blockEvent.Actor.Subject != "apikey:test" {
		t.Fatalf("block audit event=%+v", blockEvent)
	}

	request = blocklistHTTPRequest(http.MethodDelete, "/api/v1/security/blocklist/client-ids/tenant-a/client%2Fa", "")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	var unblockBody map[string]json.RawMessage
	if err := json.Unmarshal(response.Body.Bytes(), &unblockBody); err != nil {
		t.Fatal(err)
	}
	var data map[string]json.RawMessage
	if err := json.Unmarshal(unblockBody["data"], &data); err != nil {
		t.Fatal(err)
	}
	var status struct {
		Blocked       bool   `json:"blocked"`
		PolicyVersion uint64 `json:"policyVersion"`
	}
	if err := json.Unmarshal(unblockBody["data"], &status); err != nil {
		t.Fatal(err)
	}
	if response.Code != http.StatusOK || service.unblockCalls != 1 || service.unblockEntry != wantEntry || status.Blocked || status.PolicyVersion != 32 {
		t.Fatalf("unblock response=%d status=%+v service=%+v", response.Code, status, service)
	}
	if _, present := data["blockedAt"]; present {
		t.Fatalf("unblock response unexpectedly has blockedAt: %s", response.Body.String())
	}
	if len(auditService.commands) != 2 || auditService.commands[1].Action != "security.blocklist.remove" ||
		auditService.commands[1].Target.Type != "mqtt_blocklist_entry" || auditService.commands[1].Target.ID != wantEntry.Key() ||
		auditService.commands[1].Result != portsaudit.AuditResultAllowed {
		t.Fatalf("unblock audit events=%+v", auditService.commands)
	}
}

// Traceability: SEC-12; docs/specs/tenancy-and-security.md §7.1, §9; ADR-0050.
func TestBlocklistHTTPParsesEveryEntryKind(t *testing.T) {
	for _, test := range []struct {
		name  string
		path  string
		entry security.BlockEntry
	}{
		{name: "client id", path: "/api/v1/security/blocklist/client-ids/tenant-a/device", entry: security.BlockEntry{Kind: security.BlockKindClientID, TenantID: "tenant-a", Value: "device"}},
		{name: "username", path: "/api/v1/security/blocklist/usernames/guest", entry: security.BlockEntry{Kind: security.BlockKindUsername, Value: "guest"}},
		{name: "certificate", path: "/api/v1/security/blocklist/certificates/0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", entry: security.BlockEntry{Kind: security.BlockKindCertificateSHA256, Value: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"}},
		{name: "IP prefix", path: "/api/v1/security/blocklist/ip-prefixes/2001%3Adb8%3A%3A/32", entry: security.BlockEntry{Kind: security.BlockKindIPPrefix, Value: "2001:db8::/32"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			service := &blocklistHTTPStub{blockStatus: membership.BlocklistStatusDTO{Blocked: true, BlockedAtUnixMillis: 1, PolicyVersion: 2}}
			handler := newBlocklistHTTPTestHandler(service, RoleSecurityAdmin, nil)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, blocklistHTTPRequest(http.MethodPost, test.path, ""))
			if response.Code != http.StatusOK || service.blockCalls != 1 || service.blockEntry != test.entry {
				t.Fatalf("block response=%d entry=%+v, want %+v", response.Code, service.blockEntry, test.entry)
			}
		})
	}
}

// Traceability: SEC-12; docs/specs/tenancy-and-security.md §7.1, §9; ADR-0050.
func TestBlocklistRoutesParseEveryEntryKindForBlockAndUnblock(t *testing.T) {
	for _, test := range []struct {
		name  string
		path  string
		entry security.BlockEntry
	}{
		{name: "client ID", path: "/api/v1/security/blocklist/client-ids/tenant-a/client%2Fa", entry: security.BlockEntry{Kind: security.BlockKindClientID, TenantID: "tenant-a", Value: "client/a"}},
		{name: "username", path: "/api/v1/security/blocklist/usernames/guest", entry: security.BlockEntry{Kind: security.BlockKindUsername, Value: "guest"}},
		{name: "certificate", path: "/api/v1/security/blocklist/certificates/0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", entry: security.BlockEntry{Kind: security.BlockKindCertificateSHA256, Value: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"}},
		{name: "IP prefix", path: "/api/v1/security/blocklist/ip-prefixes/2001%3Adb8%3A%3A/32", entry: security.BlockEntry{Kind: security.BlockKindIPPrefix, Value: "2001:db8::/32"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			service := &blocklistHTTPStub{
				blockStatus:   membership.BlocklistStatusDTO{Blocked: true, BlockedAtUnixMillis: 1, PolicyVersion: 2},
				unblockStatus: membership.BlocklistStatusDTO{PolicyVersion: 3},
			}
			auditService := &spyAuditUseCase{}
			handler := newBlocklistHTTPTestHandler(service, RoleSecurityAdmin, auditService)
			for _, method := range []string{http.MethodPost, http.MethodDelete} {
				response := httptest.NewRecorder()
				handler.ServeHTTP(response, blocklistHTTPRequest(method, test.path, ""))
				if response.Code != http.StatusOK {
					t.Fatalf("%s response=%d body=%s", method, response.Code, response.Body.String())
				}
			}
			if service.blockCalls != 1 || service.unblockCalls != 1 ||
				service.blockEntry != test.entry || service.unblockEntry != test.entry {
				t.Fatalf("block/unblock entries = %+v/%+v calls=%d/%d, want %+v",
					service.blockEntry, service.unblockEntry, service.blockCalls, service.unblockCalls, test.entry)
			}
			if len(auditService.commands) != 2 ||
				auditService.commands[0].Action != "security.blocklist.add" ||
				auditService.commands[0].Target.Type != "mqtt_blocklist_entry" ||
				auditService.commands[0].Target.ID != test.entry.Key() ||
				auditService.commands[1].Action != "security.blocklist.remove" ||
				auditService.commands[1].Target.Type != "mqtt_blocklist_entry" ||
				auditService.commands[1].Target.ID != test.entry.Key() {
				t.Fatalf("block/unblock audit commands = %+v", auditService.commands)
			}
		})
	}
}

// Traceability: SEC-12; docs/specs/tenancy-and-security.md §7.1, §9; ADR-0050.
func TestBlocklistRoutesRejectInvalidInputAndRoles(t *testing.T) {
	service := &blocklistHTTPStub{}
	handler := newBlocklistHTTPTestHandler(service, RoleSecurityAdmin, nil)
	for _, request := range []*http.Request{
		blocklistHTTPRequest(http.MethodGet, "/api/v1/security/blocklist?debug=1", ""),
		blocklistHTTPRequest(http.MethodGet, "/api/v1/security/blocklist", "unexpected"),
		blocklistHTTPRequest(http.MethodPost, "/api/v1/security/blocklist/usernames/guest?debug=1", ""),
		blocklistHTTPRequest(http.MethodPost, "/api/v1/security/blocklist/usernames/guest", "unexpected"),
		blocklistHTTPRequest(http.MethodPost, "/api/v1/security/blocklist/client-ids/tenant-a/"+strings.Repeat("c", 257), ""),
		blocklistHTTPRequest(http.MethodPost, "/api/v1/security/blocklist/usernames/%00", ""),
		blocklistHTTPRequest(http.MethodPost, "/api/v1/security/blocklist/certificates/AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", ""),
		blocklistHTTPRequest(http.MethodPost, "/api/v1/security/blocklist/ip-prefixes/203.0.113.1/24", ""),
	} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusBadRequest {
			t.Errorf("invalid request %s %s status=%d body=%s", request.Method, request.URL, response.Code, response.Body.String())
		}
	}
	if service.listCalls != 0 || service.blockCalls != 0 || service.unblockCalls != 0 {
		t.Fatalf("invalid requests reached service: list=%d block=%d unblock=%d", service.listCalls, service.blockCalls, service.unblockCalls)
	}

	readOnly := newBlocklistHTTPTestHandler(service, RoleReadOnly, nil)
	response := httptest.NewRecorder()
	readOnly.ServeHTTP(response, blocklistHTTPRequest(http.MethodGet, "/api/v1/security/blocklist", ""))
	if response.Code != http.StatusForbidden || service.listCalls != 0 {
		t.Fatalf("unauthorized list response=%d calls=%d", response.Code, service.listCalls)
	}

	response = httptest.NewRecorder()
	readOnly.ServeHTTP(response, blocklistHTTPRequest(http.MethodPost, "/api/v1/security/blocklist/usernames/guest", ""))
	if response.Code != http.StatusForbidden || service.blockCalls != 0 {
		t.Fatalf("unauthorized block response=%d calls=%d", response.Code, service.blockCalls)
	}

	operator := newBlocklistHTTPTestHandler(service, RoleOperator, nil)
	response = httptest.NewRecorder()
	operator.ServeHTTP(response, blocklistHTTPRequest(http.MethodPost, "/api/v1/security/blocklist/usernames/guest", ""))
	if response.Code != http.StatusForbidden || service.blockCalls != 0 {
		t.Fatalf("operator block response=%d calls=%d", response.Code, service.blockCalls)
	}

	methodRequest := blocklistHTTPRequest(http.MethodPut, "/api/v1/security/blocklist/usernames/guest", "")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, methodRequest)
	if response.Code != http.StatusMethodNotAllowed || response.Header().Get("Allow") != "POST, DELETE" {
		t.Fatalf("unsupported method response=%d Allow=%q", response.Code, response.Header().Get("Allow"))
	}

	response = httptest.NewRecorder()
	handler.ServeHTTP(response, blocklistHTTPRequest(http.MethodPut, "/api/v1/security/blocklist", ""))
	if response.Code != http.StatusMethodNotAllowed || response.Header().Get("Allow") != http.MethodGet {
		t.Fatalf("list unsupported method response=%d Allow=%q", response.Code, response.Header().Get("Allow"))
	}

	noService := NewRouterWithMembership(ServerConfig{}, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	response = httptest.NewRecorder()
	noService.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/v1/security/blocklist", nil))
	if response.Code != http.StatusNotFound {
		t.Fatalf("route without blocklist service response=%d", response.Code)
	}
}

// Traceability: SEC-12; docs/specs/tenancy-and-security.md §7.1, §9; ADR-0050.
func TestBlocklistHTTPMapsCapacityAndAuditAdmissionFailures(t *testing.T) {
	capacity := &membership.ServiceError{Code: membership.ServiceErrorBlocklistCapacity, Message: "blocklist capacity is exhausted"}
	service := &blocklistHTTPStub{blockErr: capacity}
	handler := newBlocklistHTTPTestHandler(service, RoleSecurityAdmin, nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, blocklistHTTPRequest(http.MethodPost, "/api/v1/security/blocklist/usernames/guest", ""))
	if response.Code != http.StatusConflict || service.blockCalls != 1 {
		t.Fatalf("capacity response=%d calls=%d body=%s", response.Code, service.blockCalls, response.Body.String())
	}

	blockedAudit := &spyAuditUseCase{admitErr: errors.New("audit sink unavailable")}
	service = &blocklistHTTPStub{}
	handler = newBlocklistHTTPTestHandler(service, RoleSecurityAdmin, blockedAudit)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, blocklistHTTPRequest(http.MethodPost, "/api/v1/security/blocklist/usernames/guest", ""))
	if response.Code != http.StatusServiceUnavailable || service.blockCalls != 0 {
		t.Fatalf("audit admission response=%d block calls=%d body=%s", response.Code, service.blockCalls, response.Body.String())
	}
}
