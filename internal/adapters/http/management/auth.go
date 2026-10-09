package management

import (
	"crypto/subtle"
	"log/slog"
	"net/http"
	"strings"

	applicationauthentication "narumq/internal/application/ports/authentication"
	applicationclock "narumq/internal/application/ports/clock"
)

// Role names a management principal's assigned role.
type Role int

const (
	RoleReadOnly Role = iota + 1
	RoleOperator
	RoleSecurityAdmin
	RoleClusterAdmin
)

func (role Role) String() string {
	switch role {
	case RoleReadOnly:
		return "ReadOnly"
	case RoleOperator:
		return "Operator"
	case RoleSecurityAdmin:
		return "SecurityAdmin"
	case RoleClusterAdmin:
		return "ClusterAdmin"
	default:
		return "None"
	}
}

// ParseRole parses one of the four supported management role names.
func ParseRole(name string) (Role, bool) {
	switch name {
	case "ReadOnly":
		return RoleReadOnly, true
	case "Operator":
		return RoleOperator, true
	case "SecurityAdmin":
		return RoleSecurityAdmin, true
	case "ClusterAdmin":
		return RoleClusterAdmin, true
	default:
		return 0, false
	}
}

type Permission uint8

const (
	PermissionRead Permission = iota + 1
	PermissionOperate
	PermissionAuditRead
	PermissionClusterAdmin
)

type roleSet uint8

func newRoleSet(roles []Role) roleSet {
	var set roleSet
	for _, role := range roles {
		if role >= RoleReadOnly && role <= RoleClusterAdmin {
			set |= 1 << (role - RoleReadOnly)
		}
	}
	return set
}

func (roles roleSet) has(role Role) bool {
	return role >= RoleReadOnly && role <= RoleClusterAdmin && roles&(1<<(role-RoleReadOnly)) != 0
}

func (roles roleSet) allows(permission Permission) bool {
	switch permission {
	case PermissionRead:
		return roles != 0
	case PermissionOperate:
		return roles.has(RoleOperator) || roles.has(RoleClusterAdmin)
	case PermissionAuditRead:
		return roles.has(RoleSecurityAdmin) || roles.has(RoleClusterAdmin)
	case PermissionClusterAdmin:
		return roles.has(RoleClusterAdmin)
	default:
		return false
	}
}

func (roles roleSet) names() []string {
	result := make([]string, 0, 4)
	for _, role := range [...]Role{RoleReadOnly, RoleOperator, RoleSecurityAdmin, RoleClusterAdmin} {
		if roles.has(role) {
			result = append(result, role.String())
		}
	}
	return result
}

// APIKeyCredential is a named management API key with explicit roles.
type APIKeyCredential struct {
	ID    string
	Key   string
	Roles []Role
}

// OIDCIdentity statically binds a verified token subject to management roles.
type OIDCIdentity struct {
	Subject string
	Roles   []Role
}

// OIDCConfig configures token verification and static subject bindings.
type OIDCConfig struct {
	Verifier   applicationauthentication.TokenVerifier
	Identities []OIDCIdentity
}

// AuthConfig configures authentication and RBAC for management endpoints.
type AuthConfig struct {
	AuthToken             string
	APIKeys               []APIKeyCredential
	OIDC                  *OIDCConfig
	AllowAnonymousProbes  bool
	AllowAnonymousMetrics bool
	Audit                 AuditUseCase
	Logger                *slog.Logger
	ClusterID             string
	RequestRate           int                    // Per-second rate and burst per authenticated principal; <= 0 uses the default.
	AuthFailureRate       int                    // Per-second rate and burst per remote IP; <= 0 uses the default.
	Clock                 applicationclock.Clock // Nil uses the system clock.
}

type apiKeyEntry struct {
	key     string
	subject string
	roles   roleSet
}

// Authenticator verifies incoming credentials and enforces permissions.
type Authenticator struct {
	cfg                AuthConfig
	apiKeys            []apiKeyEntry
	oidcRoles          map[string]roleSet
	principalLimiter   *requestRateLimiter
	authFailureLimiter *requestRateLimiter
}

func NewAuthenticator(cfg AuthConfig) *Authenticator {
	if cfg.RequestRate <= 0 {
		cfg.RequestRate = DefaultRequestRate
	}
	if cfg.AuthFailureRate <= 0 {
		cfg.AuthFailureRate = DefaultAuthFailureRate
	}
	if cfg.Clock == nil {
		cfg.Clock = systemClock{}
	}
	a := &Authenticator{
		cfg:                cfg,
		apiKeys:            make([]apiKeyEntry, 0, len(cfg.APIKeys)),
		oidcRoles:          make(map[string]roleSet),
		principalLimiter:   newRequestRateLimiter(cfg.RequestRate, cfg.Clock),
		authFailureLimiter: newRequestRateLimiter(cfg.AuthFailureRate, cfg.Clock),
	}
	for _, key := range cfg.APIKeys {
		a.apiKeys = append(a.apiKeys, apiKeyEntry{key: key.Key, subject: "apikey:" + key.ID, roles: newRoleSet(key.Roles)})
	}
	if cfg.OIDC != nil {
		for _, identity := range cfg.OIDC.Identities {
			a.oidcRoles[identity.Subject] = newRoleSet(identity.Roles)
		}
	}
	return a
}

// Enabled reports whether authentication is configured.
func (a *Authenticator) Enabled() bool {
	return a.cfg.AuthToken != "" || len(a.apiKeys) != 0 || a.cfg.OIDC != nil
}

type managementPrincipal struct {
	subject string
	roles   roleSet
}

func (a *Authenticator) authenticate(r *http.Request) (managementPrincipal, bool) {
	if !a.Enabled() {
		return managementPrincipal{roles: newRoleSet([]Role{RoleClusterAdmin}), subject: "local-unauthenticated"}, true
	}
	authHeader := r.Header.Get("Authorization")
	if token, ok := strings.CutPrefix(authHeader, "Bearer "); ok && token != "" {
		if a.cfg.AuthToken != "" && constantTimeEqual(token, a.cfg.AuthToken) {
			return managementPrincipal{roles: newRoleSet([]Role{RoleClusterAdmin}), subject: "management-bearer-token"}, true
		}
		if a.cfg.OIDC != nil && a.cfg.OIDC.Verifier != nil {
			verified, err := a.cfg.OIDC.Verifier.VerifyToken(r.Context(), []byte(token))
			if err != nil {
				return managementPrincipal{}, false
			}
			if roles, found := a.oidcRoles[verified.Subject]; found {
				return managementPrincipal{roles: roles, subject: "oidc:" + verified.Subject}, true
			}
			return managementPrincipal{}, false
		}
	}
	apiKey := r.Header.Get("X-API-Key")
	if apiKey != "" {
		matched := -1
		for index := range a.apiKeys {
			if constantTimeEqual(apiKey, a.apiKeys[index].key) {
				matched = index
			}
		}
		if matched >= 0 {
			entry := a.apiKeys[matched]
			return managementPrincipal{roles: entry.roles, subject: entry.subject}, true
		}
	}
	return managementPrincipal{}, false
}

func constantTimeEqual(left, right string) bool {
	if len(left) != len(right) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(left), []byte(right)) == 1
}

// RequirePermission returns middleware requiring the specified explicit permission.
func (a *Authenticator) RequirePermission(permission Permission, next http.HandlerFunc) http.HandlerFunc {
	return a.require(permission, nil, true, next)
}

// RequireMutationPermission requires a permission before admitting a mutation to audit.
func (a *Authenticator) RequireMutationPermission(permission Permission, mutation auditMutation, next http.HandlerFunc) http.HandlerFunc {
	return a.require(permission, &mutation, true, next)
}

// MaybeAllowProbe returns a middleware for health probes.
func (a *Authenticator) MaybeAllowProbe(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if a.cfg.AllowAnonymousProbes || !a.Enabled() {
			next(w, r)
			return
		}
		a.require(PermissionRead, nil, false, next)(w, r)
	}
}

// MaybeAllowMetrics returns a middleware for the /metrics endpoint.
func (a *Authenticator) MaybeAllowMetrics(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if a.cfg.AllowAnonymousMetrics || !a.Enabled() {
			next(w, r)
			return
		}
		a.require(PermissionRead, nil, false, next)(w, r)
	}
}
