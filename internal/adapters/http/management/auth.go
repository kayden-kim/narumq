package management

import (
	"context"
	"crypto/subtle"
	"log/slog"
	"net/http"
	"strings"
)

// Role defines the authorization privilege level.
type Role int

const (
	RoleNone Role = iota
	RoleReadOnly
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

type roleContextKey struct{}

// AuthConfig configures authentication and RBAC for management endpoints.
type AuthConfig struct {
	AuthToken             string
	AuthTokenRole         Role
	APIKey                string
	APIKeyRole            Role
	AllowAnonymousProbes  bool
	AllowAnonymousMetrics bool
	Audit                 AuditUseCase
	Logger                *slog.Logger
	ClusterID             string
}

// Authenticator verifies incoming credentials and enforces RBAC.
type Authenticator struct {
	cfg AuthConfig
}

func NewAuthenticator(cfg AuthConfig) *Authenticator {
	if cfg.AuthTokenRole == RoleNone {
		cfg.AuthTokenRole = RoleClusterAdmin
	}
	if cfg.APIKeyRole == RoleNone {
		cfg.APIKeyRole = RoleClusterAdmin
	}
	return &Authenticator{cfg: cfg}
}

// Enabled reports whether authentication is configured.
func (a *Authenticator) Enabled() bool {
	return a.cfg.AuthToken != "" || a.cfg.APIKey != ""
}

// RoleFromContext extracts the authenticated Role from context.
func RoleFromContext(ctx context.Context) Role {
	if r, ok := ctx.Value(roleContextKey{}).(Role); ok {
		return r
	}
	return RoleNone
}

func contextWithRole(ctx context.Context, role Role) context.Context {
	return context.WithValue(ctx, roleContextKey{}, role)
}

type managementPrincipal struct {
	role    Role
	subject string
}

func (a *Authenticator) authenticate(r *http.Request) (managementPrincipal, bool) {
	if !a.Enabled() {
		return managementPrincipal{role: RoleClusterAdmin, subject: "local-unauthenticated"}, true
	}
	authHeader := r.Header.Get("Authorization")
	if token, ok := strings.CutPrefix(authHeader, "Bearer "); ok && token != "" &&
		a.cfg.AuthToken != "" && constantTimeEqual(token, a.cfg.AuthToken) {
		return managementPrincipal{role: a.cfg.AuthTokenRole, subject: "management-bearer-token"}, true
	}
	apiKey := r.Header.Get("X-API-Key")
	if apiKey != "" && a.cfg.APIKey != "" && constantTimeEqual(apiKey, a.cfg.APIKey) {
		return managementPrincipal{role: a.cfg.APIKeyRole, subject: "management-api-key"}, true
	}
	return managementPrincipal{}, false
}

func (a *Authenticator) AuthenticateRequest(r *http.Request) (Role, bool) {
	principal, ok := a.authenticate(r)
	return principal.role, ok
}

func constantTimeEqual(left, right string) bool {
	if len(left) != len(right) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(left), []byte(right)) == 1
}

// RequireRole returns a middleware requiring at least minRole.
func (a *Authenticator) RequireRole(minRole Role, next http.HandlerFunc) http.HandlerFunc {
	return a.require(minRole, nil, next)
}

func (a *Authenticator) RequireMutationRole(minRole Role, mutation auditMutation, next http.HandlerFunc) http.HandlerFunc {
	return a.require(minRole, &mutation, next)
}

// MaybeAllowProbe returns a middleware for health probes.
func (a *Authenticator) MaybeAllowProbe(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if a.cfg.AllowAnonymousProbes || !a.Enabled() {
			next(w, r)
			return
		}
		a.RequireRole(RoleReadOnly, next)(w, r)
	}
}

// MaybeAllowMetrics returns a middleware for the /metrics endpoint.
func (a *Authenticator) MaybeAllowMetrics(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if a.cfg.AllowAnonymousMetrics || !a.Enabled() {
			next(w, r)
			return
		}
		a.RequireRole(RoleReadOnly, next)(w, r)
	}
}
