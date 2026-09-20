// Package authentication adapts JWT/JWKS verification to the application authentication port.
package authentication

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/lestrrat-go/jwx/v3/jwa"
	"github.com/lestrrat-go/jwx/v3/jwk"
	"github.com/lestrrat-go/jwx/v3/jws"
	"github.com/lestrrat-go/jwx/v3/jwt"

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
type refreshCall struct {
	done chan struct{}
	err  error
}

type JWKSAuthenticationProvider struct {
	issuer           string
	audience         string
	algorithms       map[string]struct{}
	jwksURL          string
	cacheTTL         time.Duration
	refreshCooldown  time.Duration
	clockSkew        time.Duration
	httpClient       *http.Client
	clock            Clock
	bindings         map[identityKey]IdentityBinding
	mu               sync.Mutex
	keys             jwk.Set
	fetchedAt        time.Time
	expiresAt        time.Time
	refreshAttemptAt time.Time
	refreshing       *refreshCall
}

func NewJWKSAuthenticationProvider(config Config) (*JWKSAuthenticationProvider, error) {
	if config.Issuer == "" || config.Audience == "" || config.JWKSURL == "" {
		return nil, errors.New("JWKS authentication issuer, audience, and URL are required")
	}
	if len(config.Algorithms) == 0 {
		return nil, errors.New("JWKS authentication algorithms are required")
	}
	algorithms := make(map[string]struct{}, len(config.Algorithms))
	for _, algorithm := range config.Algorithms {
		if algorithm == "" {
			return nil, errors.New("JWKS authentication algorithm is empty")
		}
		if _, exists := algorithms[algorithm]; exists {
			return nil, errors.New("JWKS authentication algorithm is duplicated")
		}
		algorithms[algorithm] = struct{}{}
	}
	if config.CacheTTL <= 0 || config.RefreshCooldown <= 0 || config.ClockSkew <= 0 || config.HTTPTimeout <= 0 {
		return nil, errors.New("JWKS authentication durations must be greater than zero")
	}
	if config.HTTPClient == nil {
		return nil, errors.New("JWKS authentication HTTP client is nil")
	}
	if config.Clock == nil {
		return nil, errors.New("JWKS authentication clock is nil")
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
	client := *config.HTTPClient
	if client.Timeout != config.HTTPTimeout {
		client.Timeout = config.HTTPTimeout
	}
	return &JWKSAuthenticationProvider{
		issuer:          config.Issuer,
		audience:        config.Audience,
		algorithms:      algorithms,
		jwksURL:         config.JWKSURL,
		cacheTTL:        config.CacheTTL,
		refreshCooldown: config.RefreshCooldown,
		clockSkew:       config.ClockSkew,
		httpClient:      &client,
		clock:           config.Clock,
		bindings:        bindings,
	}, nil
}

// Authenticate verifies a compact JWT and returns its static identity binding.
func (provider *JWKSAuthenticationProvider) Authenticate(ctx context.Context, credential applicationauthentication.Credential) (applicationauthentication.Result, error) {
	if provider == nil || ctx == nil || len(credential.Token) == 0 {
		return applicationauthentication.Result{}, ErrInvalidCredential
	}
	algorithm, kid, err := parseHeader(credential.Token)
	if err != nil {
		return applicationauthentication.Result{}, err
	}
	if _, allowed := provider.algorithms[algorithm]; !allowed {
		return applicationauthentication.Result{}, ErrUnsupportedAlgorithm
	}
	keys, err := provider.keySetFor(ctx, kid)
	if err != nil {
		return applicationauthentication.Result{}, err
	}
	key, found := keys.LookupKeyID(kid)
	if !found {
		return applicationauthentication.Result{}, ErrUnknownKey
	}
	signatureAlgorithm, ok := jwa.LookupSignatureAlgorithm(algorithm)
	if !ok {
		return applicationauthentication.Result{}, ErrUnsupportedAlgorithm
	}
	token, err := jwt.Parse(credential.Token,
		jwt.WithKey(signatureAlgorithm, key),
		jwt.WithValidate(false),
	)
	if err != nil {
		return applicationauthentication.Result{}, ErrInvalidCredential
	}
	if err := provider.validateClaims(token); err != nil {
		return applicationauthentication.Result{}, err
	}
	issuer, _ := token.Issuer()
	subject, _ := token.Subject()
	binding, ok := provider.bindings[identityKey{issuer: issuer, subject: subject}]
	if !ok {
		return applicationauthentication.Result{}, ErrUnmappedSubject
	}
	return applicationauthentication.Result{
		TenantID:         binding.TenantID,
		Subject:          binding.Subject,
		CredentialID:     binding.CredentialID,
		AllowedClientIDs: append([]string(nil), binding.AllowedClientIDs...),
		Roles:            append([]security.Role(nil), binding.Roles...),
		ExpiresAt:        tokenExpiration(token),
	}, nil
}

func parseHeader(raw []byte) (string, string, error) {
	message, err := jws.Parse(raw, jws.WithCompact())
	if err != nil || len(message.Signatures()) != 1 {
		return "", "", ErrInvalidCredential
	}
	headers := message.Signatures()[0].ProtectedHeaders()
	algorithm, ok := headers.Algorithm()
	if !ok || algorithm.String() == "" {
		return "", "", ErrInvalidCredential
	}
	kid, ok := headers.KeyID()
	if !ok || kid == "" {
		return "", "", ErrInvalidCredential
	}
	return algorithm.String(), kid, nil
}

func (provider *JWKSAuthenticationProvider) validateClaims(token jwt.Token) error {
	issuer, ok := token.Issuer()
	if !ok || issuer != provider.issuer {
		return ErrInvalidCredential
	}
	audience, ok := token.Audience()
	if !ok || !contains(audience, provider.audience) {
		return ErrInvalidCredential
	}
	subject, ok := token.Subject()
	if !ok || subject == "" {
		return ErrInvalidCredential
	}
	if !token.Has("exp") {
		return ErrInvalidCredential
	}
	if _, ok := token.Expiration(); !ok {
		return ErrInvalidCredential
	}
	if err := jwt.Validate(token,
		jwt.WithClock(jwt.ClockFunc(provider.clock.Now)),
		jwt.WithAcceptableSkew(provider.clockSkew),
		jwt.WithRequiredClaim("exp"),
	); err != nil {
		return ErrInvalidCredential
	}
	return nil
}

func tokenExpiration(token jwt.Token) time.Time {
	expiresAt, _ := token.Expiration()
	return expiresAt
}

func contains(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

func (provider *JWKSAuthenticationProvider) keySetFor(ctx context.Context, kid string) (jwk.Set, error) {
	for {
		now := provider.clock.Now()
		provider.mu.Lock()
		cacheValid := provider.keys != nil && now.Before(provider.expiresAt)
		if cacheValid {
			if _, found := provider.keys.LookupKeyID(kid); found {
				keys := provider.keys
				provider.mu.Unlock()
				return keys, nil
			}
			if provider.refreshing != nil {
				call := provider.refreshing
				provider.mu.Unlock()
				if err := waitForRefresh(ctx, call); err != nil {
					if ctx.Err() != nil {
						return nil, ctx.Err()
					}
					return nil, ErrUnknownKey
				}
				continue
			}
			if !provider.refreshAttemptAt.IsZero() && now.Sub(provider.refreshAttemptAt) < provider.refreshCooldown {
				provider.mu.Unlock()
				return nil, ErrUnknownKey
			}
			provider.mu.Unlock()
			if err := provider.refresh(ctx); err != nil {
				return nil, ErrUnknownKey
			}
			continue
		}
		if provider.refreshing != nil {
			call := provider.refreshing
			provider.mu.Unlock()
			if err := waitForRefresh(ctx, call); err != nil {
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				return nil, fmt.Errorf("%w: refresh failed", ErrJWKSUnavailable)
			}
			continue
		}
		provider.mu.Unlock()
		if err := provider.refresh(ctx); err != nil {
			return nil, fmt.Errorf("%w: refresh failed", ErrJWKSUnavailable)
		}
	}
}

func waitForRefresh(ctx context.Context, call *refreshCall) error {
	select {
	case <-call.done:
		return call.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (provider *JWKSAuthenticationProvider) refresh(ctx context.Context) error {
	provider.mu.Lock()
	if provider.refreshing != nil {
		call := provider.refreshing
		provider.mu.Unlock()
		return waitForRefresh(ctx, call)
	}
	call := &refreshCall{done: make(chan struct{})}
	provider.refreshing = call
	attemptAt := provider.clock.Now()
	provider.mu.Unlock()

	keys, err := provider.fetch(ctx)
	provider.mu.Lock()
	provider.refreshAttemptAt = attemptAt
	if err == nil {
		fetchedAt := provider.clock.Now()
		provider.keys = keys
		provider.fetchedAt = fetchedAt
		provider.expiresAt = fetchedAt.Add(provider.cacheTTL)
	}
	call.err = err
	provider.refreshing = nil
	close(call.done)
	provider.mu.Unlock()
	return err
}

func (provider *JWKSAuthenticationProvider) fetch(ctx context.Context) (jwk.Set, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, provider.jwksURL, nil)
	if err != nil {
		return nil, ErrJWKSUnavailable
	}
	response, err := provider.httpClient.Do(request)
	if err != nil {
		return nil, ErrJWKSUnavailable
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, ErrJWKSUnavailable
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, maxJWKSBodyBytes+1))
	if err != nil || len(body) > maxJWKSBodyBytes {
		return nil, ErrJWKSUnavailable
	}
	keys, err := jwk.Parse(body, jwk.WithRejectDuplicateKID(true), jwk.WithStrictKeySetParsing(true))
	if err != nil || keys.Len() == 0 {
		return nil, ErrJWKSUnavailable
	}
	return keys, nil
}
