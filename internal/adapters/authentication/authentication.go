// Package authentication adapts JWT/JWKS verification to the application authentication port.
package authentication

import (
	"context"
	"errors"
	"net/http"
	"time"

	applicationauthentication "narumq/internal/application/ports/authentication"
	"narumq/internal/domain/security"
	"narumq/internal/domain/tenancy"
)

var (
	// ErrInvalidCredential means that a token is malformed or fails verification.
	ErrInvalidCredential = errors.New("invalid authentication credential")
	// ErrUnsupportedAlgorithm means that a token algorithm is outside the configured allowlist.
	ErrUnsupportedAlgorithm = errors.New("unsupported JWT algorithm")
	// ErrUnknownKey means that the token kid is not present in an allowed JWKS set.
	ErrUnknownKey = errors.New("unknown JWT key")
	// ErrJWKSUnavailable means that a mandatory JWKS refresh failed.
	ErrJWKSUnavailable = errors.New("JWKS unavailable")
	// ErrUnmappedSubject means that a verified issuer and subject have no static binding.
	ErrUnmappedSubject = errors.New("unmapped JWT subject")
)

const maxJWKSBodyBytes = 4 << 20

// Clock supplies deterministic wall-clock time to the cache and claim validator.
type Clock interface {
	Now() time.Time
}

// IdentityBinding binds one verified issuer and subject to a tenant and client scope.
type IdentityBinding struct {
	Issuer           string
	Subject          string
	TenantID         tenancy.TenantID
	CredentialID     string
	AllowedClientIDs []string
	Roles            []security.Role
}

// Config configures a JWKSAuthenticationProvider.
type Config struct {
	Issuer          string
	Audience        string
	Algorithms      []string
	JWKSURL         string
	CacheTTL        time.Duration
	RefreshCooldown time.Duration
	ClockSkew       time.Duration
	HTTPTimeout     time.Duration
	HTTPClient      *http.Client
	Clock           Clock
	Bindings        []IdentityBinding
}

type identityKey struct {
	issuer  string
	subject string
}

// JWKSAuthenticationProvider verifies JWTs and resolves their static identity binding.
type JWKSAuthenticationProvider struct {
	verifier *JWTVerifier
	bindings map[identityKey]IdentityBinding
}

func NewJWKSAuthenticationProvider(config Config) (*JWKSAuthenticationProvider, error) {
	verifier, err := NewJWTVerifier(VerifierConfig{
		Issuer: config.Issuer, Audience: config.Audience, Algorithms: config.Algorithms,
		JWKSURL: config.JWKSURL, CacheTTL: config.CacheTTL,
		RefreshCooldown: config.RefreshCooldown, ClockSkew: config.ClockSkew,
		HTTPTimeout: config.HTTPTimeout, HTTPClient: config.HTTPClient, Clock: config.Clock,
	})
	if err != nil {
		return nil, err
	}
	bindings := make(map[identityKey]IdentityBinding, len(config.Bindings))
	for _, binding := range config.Bindings {
		if binding.Issuer == "" || binding.Subject == "" || binding.CredentialID == "" || binding.TenantID == (tenancy.TenantID{}) || len(binding.AllowedClientIDs) == 0 || len(binding.Roles) == 0 {
			return nil, errors.New("JWKS authentication identity binding is incomplete")
		}
		key := identityKey{issuer: binding.Issuer, subject: binding.Subject}
		if _, exists := bindings[key]; exists {
			return nil, errors.New("JWKS authentication identity binding is duplicated")
		}
		allowedClientIDs := make([]string, len(binding.AllowedClientIDs))
		seenClientIDs := make(map[string]struct{}, len(binding.AllowedClientIDs))
		for index, clientID := range binding.AllowedClientIDs {
			if clientID == "" {
				return nil, errors.New("JWKS authentication allowed Client ID is empty")
			}
			if _, exists := seenClientIDs[clientID]; exists {
				return nil, errors.New("JWKS authentication allowed Client ID is duplicated")
			}
			seenClientIDs[clientID] = struct{}{}
			allowedClientIDs[index] = clientID
		}
		roles := make([]security.Role, len(binding.Roles))
		seenRoles := make(map[security.Role]struct{}, len(binding.Roles))
		for index, role := range binding.Roles {
			if role == "" {
				return nil, errors.New("JWKS authentication role is empty")
			}
			if _, exists := seenRoles[role]; exists {
				return nil, errors.New("JWKS authentication role is duplicated")
			}
			seenRoles[role] = struct{}{}
			roles[index] = role
		}
		binding.AllowedClientIDs = allowedClientIDs
		binding.Roles = roles
		bindings[key] = binding
	}
	return &JWKSAuthenticationProvider{verifier: verifier, bindings: bindings}, nil
}

// Authenticate verifies a compact JWT and returns its static identity binding.
func (provider *JWKSAuthenticationProvider) Authenticate(ctx context.Context, credential applicationauthentication.Credential) (applicationauthentication.Result, error) {
	if provider == nil || ctx == nil || len(credential.Password) == 0 {
		return applicationauthentication.Result{}, ErrInvalidCredential
	}
	verified, err := provider.verifier.VerifyToken(ctx, credential.Password)
	if err != nil {
		return applicationauthentication.Result{}, err
	}
	binding, ok := provider.bindings[identityKey{issuer: verified.Issuer, subject: verified.Subject}]
	if !ok {
		return applicationauthentication.Result{}, ErrUnmappedSubject
	}
	return applicationauthentication.Result{
		TenantID:         binding.TenantID,
		Subject:          binding.Subject,
		CredentialID:     binding.CredentialID,
		AllowedClientIDs: append([]string(nil), binding.AllowedClientIDs...),
		Roles:            append([]security.Role(nil), binding.Roles...),
		ExpiresAt:        verified.ExpiresAt,
	}, nil
}
