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
)

type refreshCall struct {
	done chan struct{}
	err  error
}

// VerifierConfig configures cryptographic and claim verification without identity mapping.
type VerifierConfig struct {
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
}

// JWTVerifier validates compact JWTs against configured issuer, audience, algorithms, and JWKS.
type JWTVerifier struct {
	issuer           string
	audience         string
	algorithms       map[string]struct{}
	jwksURL          string
	cacheTTL         time.Duration
	refreshCooldown  time.Duration
	clockSkew        time.Duration
	httpClient       *http.Client
	clock            Clock
	mu               sync.Mutex
	keys             jwk.Set
	fetchedAt        time.Time
	expiresAt        time.Time
	refreshAttemptAt time.Time
	refreshing       *refreshCall
}

// NewJWTVerifier constructs a verifier while preserving the MQTT provider's validation errors.
func NewJWTVerifier(config VerifierConfig) (*JWTVerifier, error) {
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
	client := *config.HTTPClient
	if client.Timeout != config.HTTPTimeout {
		client.Timeout = config.HTTPTimeout
	}
	return &JWTVerifier{
		issuer: config.Issuer, audience: config.Audience, algorithms: algorithms,
		jwksURL: config.JWKSURL, cacheTTL: config.CacheTTL,
		refreshCooldown: config.RefreshCooldown, clockSkew: config.ClockSkew,
		httpClient: &client, clock: config.Clock,
	}, nil
}

// VerifyToken verifies a compact JWT and returns its trusted issuer, subject, and expiration.
func (verifier *JWTVerifier) VerifyToken(ctx context.Context, raw []byte) (applicationauthentication.VerifiedToken, error) {
	if verifier == nil || ctx == nil || len(raw) == 0 {
		return applicationauthentication.VerifiedToken{}, ErrInvalidCredential
	}
	algorithm, kid, err := parseHeader(raw)
	if err != nil {
		return applicationauthentication.VerifiedToken{}, err
	}
	if _, allowed := verifier.algorithms[algorithm]; !allowed {
		return applicationauthentication.VerifiedToken{}, ErrUnsupportedAlgorithm
	}
	keys, err := verifier.keySetFor(ctx, kid)
	if err != nil {
		return applicationauthentication.VerifiedToken{}, err
	}
	key, found := keys.LookupKeyID(kid)
	if !found {
		return applicationauthentication.VerifiedToken{}, ErrUnknownKey
	}
	signatureAlgorithm, ok := jwa.LookupSignatureAlgorithm(algorithm)
	if !ok {
		return applicationauthentication.VerifiedToken{}, ErrUnsupportedAlgorithm
	}
	token, err := jwt.Parse(raw, jwt.WithKey(signatureAlgorithm, key), jwt.WithValidate(false))
	if err != nil {
		return applicationauthentication.VerifiedToken{}, ErrInvalidCredential
	}
	if err := verifier.validateClaims(token); err != nil {
		return applicationauthentication.VerifiedToken{}, err
	}
	issuer, _ := token.Issuer()
	subject, _ := token.Subject()
	return applicationauthentication.VerifiedToken{Issuer: issuer, Subject: subject, ExpiresAt: tokenExpiration(token)}, nil
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

func (verifier *JWTVerifier) validateClaims(token jwt.Token) error {
	issuer, ok := token.Issuer()
	if !ok || issuer != verifier.issuer {
		return ErrInvalidCredential
	}
	audience, ok := token.Audience()
	if !ok || !contains(audience, verifier.audience) {
		return ErrInvalidCredential
	}
	subject, ok := token.Subject()
	if !ok || subject == "" || !token.Has("exp") {
		return ErrInvalidCredential
	}
	if _, ok := token.Expiration(); !ok {
		return ErrInvalidCredential
	}
	if err := jwt.Validate(token, jwt.WithClock(jwt.ClockFunc(verifier.clock.Now)), jwt.WithAcceptableSkew(verifier.clockSkew), jwt.WithRequiredClaim("exp")); err != nil {
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

func (verifier *JWTVerifier) keySetFor(ctx context.Context, kid string) (jwk.Set, error) {
	for {
		now := verifier.clock.Now()
		verifier.mu.Lock()
		cacheValid := verifier.keys != nil && now.Before(verifier.expiresAt)
		if cacheValid {
			if _, found := verifier.keys.LookupKeyID(kid); found {
				keys := verifier.keys
				verifier.mu.Unlock()
				return keys, nil
			}
			if verifier.refreshing != nil {
				call := verifier.refreshing
				verifier.mu.Unlock()
				if err := waitForRefresh(ctx, call); err != nil {
					if ctx.Err() != nil {
						return nil, ctx.Err()
					}
					return nil, ErrUnknownKey
				}
				continue
			}
			if !verifier.refreshAttemptAt.IsZero() && now.Sub(verifier.refreshAttemptAt) < verifier.refreshCooldown {
				verifier.mu.Unlock()
				return nil, ErrUnknownKey
			}
			verifier.mu.Unlock()
			if err := verifier.refresh(ctx); err != nil {
				return nil, ErrUnknownKey
			}
			continue
		}
		if verifier.refreshing != nil {
			call := verifier.refreshing
			verifier.mu.Unlock()
			if err := waitForRefresh(ctx, call); err != nil {
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				return nil, fmt.Errorf("%w: refresh failed", ErrJWKSUnavailable)
			}
			continue
		}
		verifier.mu.Unlock()
		if err := verifier.refresh(ctx); err != nil {
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

func (verifier *JWTVerifier) refresh(ctx context.Context) error {
	verifier.mu.Lock()
	if verifier.refreshing != nil {
		call := verifier.refreshing
		verifier.mu.Unlock()
		return waitForRefresh(ctx, call)
	}
	call := &refreshCall{done: make(chan struct{})}
	verifier.refreshing = call
	attemptAt := verifier.clock.Now()
	verifier.mu.Unlock()

	keys, err := verifier.fetch(ctx)
	verifier.mu.Lock()
	verifier.refreshAttemptAt = attemptAt
	if err == nil {
		fetchedAt := verifier.clock.Now()
		verifier.keys = keys
		verifier.fetchedAt = fetchedAt
		verifier.expiresAt = fetchedAt.Add(verifier.cacheTTL)
	}
	call.err = err
	verifier.refreshing = nil
	close(call.done)
	verifier.mu.Unlock()
	return err
}

func (verifier *JWTVerifier) fetch(ctx context.Context) (jwk.Set, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, verifier.jwksURL, nil)
	if err != nil {
		return nil, ErrJWKSUnavailable
	}
	response, err := verifier.httpClient.Do(request)
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
