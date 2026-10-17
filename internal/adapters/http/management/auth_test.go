package management

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	applicationauthentication "narumq/internal/application/ports/authentication"
)

type managementTokenVerifier struct {
	tokens map[string]string
	err    error
}

func (v managementTokenVerifier) VerifyToken(_ context.Context, token []byte) (applicationauthentication.VerifiedToken, error) {
	if v.err != nil {
		return applicationauthentication.VerifiedToken{}, v.err
	}
	subject, ok := v.tokens[string(token)]
	if !ok {
		return applicationauthentication.VerifiedToken{}, errors.New("invalid token")
	}
	return applicationauthentication.VerifiedToken{Issuer: "issuer", Subject: subject, ExpiresAt: time.Unix(2_000_000_000, 0)}, nil
}

// Traceability: SEC-07, SEC-10; docs/specs/tenancy-and-security.md §6-§7.
// Scenarios: management-api-and-observability.md:MGT-SCN-017, tenancy-and-security.md:SEC-SCN-031, tenancy-and-security.md:SEC-SCN-037
func TestPermissionMatrixSeparatesManagementRoles(t *testing.T) {
	cases := []struct {
		role Role
		want map[Permission]bool
	}{
		{RoleReadOnly, map[Permission]bool{PermissionRead: true, PermissionOperate: false, PermissionAuditRead: false, PermissionClusterAdmin: false, PermissionCredentialAdmin: false}},
		{RoleOperator, map[Permission]bool{PermissionRead: true, PermissionOperate: true, PermissionAuditRead: false, PermissionClusterAdmin: false, PermissionCredentialAdmin: false}},
		{RoleSecurityAdmin, map[Permission]bool{PermissionRead: true, PermissionOperate: false, PermissionAuditRead: true, PermissionClusterAdmin: false, PermissionCredentialAdmin: true}},
		{RoleClusterAdmin, map[Permission]bool{PermissionRead: true, PermissionOperate: true, PermissionAuditRead: true, PermissionClusterAdmin: true, PermissionCredentialAdmin: true}},
	}
	permissions := []Permission{PermissionRead, PermissionOperate, PermissionAuditRead, PermissionClusterAdmin, PermissionCredentialAdmin}
	for _, testCase := range cases {
		roles := newRoleSet([]Role{testCase.role})
		for _, permission := range permissions {
			if got := roles.allows(permission); got != testCase.want[permission] {
				t.Errorf("role %s allows permission %d = %t, want %t", testCase.role, permission, got, testCase.want[permission])
			}
		}
	}
	union := newRoleSet([]Role{RoleOperator, RoleSecurityAdmin})
	if !union.allows(PermissionOperate) || !union.allows(PermissionAuditRead) || !union.allows(PermissionCredentialAdmin) || union.allows(PermissionClusterAdmin) {
		t.Fatalf("Operator+SecurityAdmin union permissions incorrect: %v", union.names())
	}
	for _, name := range []string{"ReadOnly", "Operator", "SecurityAdmin", "ClusterAdmin"} {
		if role, ok := ParseRole(name); !ok || role.String() != name {
			t.Errorf("ParseRole(%q) = (%v, %t)", name, role, ok)
		}
	}
	if _, ok := ParseRole("operator"); ok {
		t.Fatal("ParseRole accepted a non-canonical role name")
	}
	if got, want := union.names(), []string{"Operator", "SecurityAdmin"}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("role names = %v, want %v", got, want)
	}
}

// Traceability: SEC-10; docs/specs/tenancy-and-security.md §6.
// Scenarios: management-api-and-observability.md:MGT-SCN-017, tenancy-and-security.md:SEC-SCN-032
func TestManagementPrincipalCredentialBindings(t *testing.T) {
	verifier := managementTokenVerifier{tokens: map[string]string{"alice-token": "alice", "mallory-token": "mallory"}}
	audit := &spyAuditUseCase{}
	auth := NewAuthenticator(AuthConfig{
		AuthToken: "break-glass",
		APIKeys:   []APIKeyCredential{{ID: "dashboard", Key: "read-key", Roles: []Role{RoleReadOnly}}},
		OIDC: &OIDCConfig{
			Verifier:   verifier,
			Identities: []OIDCIdentity{{Subject: "alice", Roles: []Role{RoleSecurityAdmin}}},
		},
		Audit: audit,
	})
	allows := func(header, value string, permission Permission) int {
		request := httptest.NewRequest(http.MethodGet, "/", nil)
		request.Header.Set(header, value)
		recorder := httptest.NewRecorder()
		auth.RequirePermission(permission, func(http.ResponseWriter, *http.Request) {})(recorder, request)
		return recorder.Code
	}
	if got := allows("Authorization", "Bearer break-glass", PermissionClusterAdmin); got != http.StatusOK {
		t.Fatalf("break-glass permission status=%d, want 200", got)
	}
	if got := allows("Authorization", "Bearer alice-token", PermissionAuditRead); got != http.StatusOK {
		t.Fatalf("OIDC bound permission status=%d, want 200", got)
	}
	if got := allows("Authorization", "Bearer alice-token", PermissionOperate); got != http.StatusForbidden {
		t.Fatalf("OIDC unbound permission status=%d, want 403", got)
	}
	if got := allows("Authorization", "Bearer mallory-token", PermissionRead); got != http.StatusUnauthorized {
		t.Fatalf("unmapped subject status=%d, want 401", got)
	}
	if got := allows("X-API-Key", "read-key", PermissionRead); got != http.StatusOK {
		t.Fatalf("API key permission status=%d, want 200", got)
	}
	if got := allows("X-API-Key", "read-key", PermissionOperate); got != http.StatusForbidden {
		t.Fatalf("API key forbidden status=%d, want 403", got)
	}
	if got := audit.commands[len(audit.commands)-1].Actor.Subject; got != "apikey:dashboard" {
		t.Fatalf("API key audit subject = %q, want apikey:dashboard", got)
	}
	if len(audit.commands[len(audit.commands)-1].Actor.Roles) != 1 || audit.commands[len(audit.commands)-1].Actor.Roles[0] != "ReadOnly" {
		t.Fatalf("API key audit roles = %v, want [ReadOnly]", audit.commands[len(audit.commands)-1].Actor.Roles)
	}
}

// Traceability: SEC-10; docs/specs/tenancy-and-security.md §6.
// Scenarios: management-api-and-observability.md:MGT-SCN-018, tenancy-and-security.md:SEC-SCN-032
func TestManagementOIDCFailuresAreUnauthorized(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	request.Header.Set("Authorization", "Bearer token")
	request.Header.Set("X-API-Key", "read-key")
	for _, cfg := range []AuthConfig{
		{APIKeys: []APIKeyCredential{{ID: "dashboard", Key: "read-key", Roles: []Role{RoleReadOnly}}}, OIDC: &OIDCConfig{Verifier: managementTokenVerifier{err: errors.New("verifier failed")}}},
		{APIKeys: []APIKeyCredential{{ID: "dashboard", Key: "read-key", Roles: []Role{RoleReadOnly}}}, OIDC: &OIDCConfig{Verifier: managementTokenVerifier{tokens: map[string]string{"token": "alice"}}}},
	} {
		recorder := httptest.NewRecorder()
		NewAuthenticator(cfg).RequirePermission(PermissionRead, func(http.ResponseWriter, *http.Request) {})(recorder, request)
		if recorder.Code != http.StatusUnauthorized {
			t.Errorf("OIDC configuration %+v status=%d, want 401", cfg.OIDC, recorder.Code)
		}
	}
}
