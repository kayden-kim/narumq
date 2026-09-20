package management

import (
	"context"
	"crypto/subtle"
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

type roleContextKey struct{}

// AuthConfig configures authentication and RBAC for management endpoints.
type AuthConfig struct {
	// AuthToken is the optional shared Bearer token.
	AuthToken string
	// AuthTokenRole is the role granted to a valid Bearer token.
	AuthTokenRole Role
	// APIKey is the optional API key accepted in X-API-Key header.
	APIKey string
	// APIKeyRole is the role granted to a valid API key.
	APIKeyRole Role
	// AllowAnonymousProbes allows GET /health/* without auth.
	AllowAnonymousProbes bool
	// AllowAnonymousMetrics allows GET /metrics without auth.
	AllowAnonymousMetrics bool
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

// AuthenticateRequest validates credentials from headers and returns the assigned role.
func (a *Authenticator) AuthenticateRequest(r *http.Request) (Role, bool) {
	// If no auth is configured, grant ClusterAdmin for local development/single-node profile.
	if !a.Enabled() {
		return RoleClusterAdmin, true
	}

	// 1. Check Bearer token
	authHeader := r.Header.Get("Authorization")
	if token, ok := strings.CutPrefix(authHeader, "Bearer "); ok && token != "" &&
		a.cfg.AuthToken != "" && constantTimeEqual(token, a.cfg.AuthToken) {
		return a.cfg.AuthTokenRole, true
	}

	// 2. Check X-API-Key header
	apiKey := r.Header.Get("X-API-Key")
	if apiKey != "" && a.cfg.APIKey != "" && constantTimeEqual(apiKey, a.cfg.APIKey) {
		return a.cfg.APIKeyRole, true
	}

	return RoleNone, false
}

func constantTimeEqual(left, right string) bool {
	if len(left) != len(right) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(left), []byte(right)) == 1
}

// RequireRole returns a middleware requiring at least minRole.
func (a *Authenticator) RequireRole(minRole Role, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		role, ok := a.AuthenticateRequest(r)
		if !ok {
			WriteError(w, r, http.StatusUnauthorized, "unauthorized", "authentication required or credentials invalid", false, nil)
			return
		}
		if role < minRole {
			WriteError(w, r, http.StatusForbidden, "forbidden", "authenticated principal lacks required permissions", false, nil)
			return
		}

		ctx := contextWithRole(r.Context(), role)
		next(w, r.WithContext(ctx))
	}
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
