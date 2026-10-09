package authentication

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	applicationauthentication "narumq/internal/application/ports/authentication"
)

const (
	cmmIssuer   = "https://issuer.example"
	cmmAudience = "narumq"
	cmmSubject  = "device-1"
)

// cmmGatedJWKS serves a JWKS document and, while armed, parks every request
// until release is called so tests can observe in-flight refreshes.
type cmmGatedJWKS struct {
	url         string
	armed       atomic.Bool
	status      atomic.Int32
	requests    atomic.Int64
	started     chan struct{}
	release     chan struct{}
	releaseOnce sync.Once
}

func cmmNewGatedJWKS(t *testing.T, body []byte) *cmmGatedJWKS {
	t.Helper()
	gate := &cmmGatedJWKS{started: make(chan struct{}, 16), release: make(chan struct{})}
	gate.status.Store(http.StatusOK)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		gate.requests.Add(1)
		if gate.armed.Load() {
			gate.started <- struct{}{}
			<-gate.release
		}
		if status := int(gate.status.Load()); status != http.StatusOK {
			writer.WriteHeader(status)
			return
		}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write(body)
	}))
	t.Cleanup(server.Close)
	// Registered after server.Close so it runs first and unblocks parked handlers.
	t.Cleanup(gate.Release)
	gate.url = server.URL
	return gate
}

func (gate *cmmGatedJWKS) Release() {
	gate.releaseOnce.Do(func() { close(gate.release) })
}

// cmmDoneSignalContext reports the first Done call, which happens only once a
// caller has joined an in-flight refresh and is about to block on it.
type cmmDoneSignalContext struct {
	context.Context
	once   sync.Once
	joined chan struct{}
}

func cmmNewDoneSignalContext() *cmmDoneSignalContext {
	return &cmmDoneSignalContext{Context: context.Background(), joined: make(chan struct{})}
}

func (ctx *cmmDoneSignalContext) Done() <-chan struct{} {
	ctx.once.Do(func() { close(ctx.joined) })
	return ctx.Context.Done()
}

func cmmCanceledContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

func cmmAuthenticate(ctx context.Context, provider *JWKSAuthenticationProvider, token []byte) error {
	_, err := provider.Authenticate(ctx, applicationauthentication.Credential{Password: token})
	return err
}

func TestCmmAuthenticateRejectsMalformedHeaderBeforeFetchingKeys(t *testing.T) {
	now := time.Unix(5000, 0)
	key := newTestKey(t, "key-1")
	provider, requests := newProviderFixture(t, &testClock{now: now}, encodeJWKS(t, key.public), nil)
	valid := signedToken(t, key.private, now, cmmIssuer, cmmAudience, cmmSubject, timePtr(now.Add(time.Minute)), nil)
	cases := []struct {
		name  string
		token []byte
	}{
		{name: "not compact", token: []byte("not-a-jwt")},
		{name: "two segments", token: []byte(strings.Join(strings.Split(string(valid), ".")[:2], "."))},
		{name: "missing alg", token: mutateHeader(t, valid, map[string]any{"alg": nil})},
		{name: "unregistered alg", token: mutateHeader(t, valid, map[string]any{"alg": "XX999"})},
		{name: "missing kid", token: mutateHeader(t, valid, map[string]any{"kid": nil})},
		{name: "empty kid", token: mutateHeader(t, valid, map[string]any{"kid": ""})},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			if err := cmmAuthenticate(context.Background(), provider, testCase.token); !errors.Is(err, ErrInvalidCredential) {
				t.Fatalf("Authenticate() error = %v, want %v", err, ErrInvalidCredential)
			}
		})
	}
	if got := requests(); got != 0 {
		t.Fatalf("JWKS requests for malformed headers = %d, want 0", got)
	}
}

func TestCmmAuthenticateRequiresExpirationAndStaticBinding(t *testing.T) {
	now := time.Unix(6000, 0)
	key := newTestKey(t, "key-1")
	provider, _ := newProviderFixture(t, &testClock{now: now}, encodeJWKS(t, key.public), nil)
	expires := now.Add(time.Minute)

	noExpiry := signedToken(t, key.private, now, cmmIssuer, cmmAudience, cmmSubject, nil, nil)
	if err := cmmAuthenticate(context.Background(), provider, noExpiry); !errors.Is(err, ErrInvalidCredential) {
		t.Fatalf("missing exp error = %v, want %v", err, ErrInvalidCredential)
	}

	unmapped := signedToken(t, key.private, now, cmmIssuer, cmmAudience, "device-2", &expires, nil)
	if err := cmmAuthenticate(context.Background(), provider, unmapped); !errors.Is(err, ErrUnmappedSubject) {
		t.Fatalf("unmapped subject error = %v, want %v", err, ErrUnmappedSubject)
	}

	valid := signedToken(t, key.private, now, cmmIssuer, cmmAudience, cmmSubject, &expires, nil)
	result, err := provider.Authenticate(context.Background(), applicationauthentication.Credential{Password: valid})
	if err != nil {
		t.Fatalf("valid token error = %v", err)
	}
	if result.Subject != cmmSubject || result.CredentialID != "device-key" || !result.ExpiresAt.Equal(expires) {
		t.Fatalf("Authenticate() = %+v, want subject %q credential %q expiry %v", result, cmmSubject, "device-key", expires)
	}
}

func TestCmmJWKSFetchFailuresAreUnavailable(t *testing.T) {
	now := time.Unix(7000, 0)
	key := newTestKey(t, "key-1")
	jwks := encodeJWKS(t, key.public)
	token := signedToken(t, key.private, now, cmmIssuer, cmmAudience, cmmSubject, timePtr(now.Add(time.Minute)), nil)
	cases := []struct {
		name string
		body []byte
		url  string
	}{
		{name: "unparseable URL", body: jwks, url: "://missing-scheme"},
		{name: "unsupported URL scheme", body: jwks, url: "ftp://issuer.example/jwks"},
		{name: "body over size limit", body: append(append([]byte(nil), jwks...), strings.Repeat(" ", maxJWKSBodyBytes-len(jwks)+1)...)},
		{name: "invalid JSON", body: []byte("{not json")},
		{name: "empty key set", body: []byte(`{"keys":[]}`)},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			provider, _ := newProviderFixture(t, &testClock{now: now}, testCase.body, nil)
			if testCase.url != "" {
				provider.verifier.jwksURL = testCase.url
			}
			if err := cmmAuthenticate(context.Background(), provider, token); !errors.Is(err, ErrJWKSUnavailable) {
				t.Fatalf("Authenticate() error = %v, want %v", err, ErrJWKSUnavailable)
			}
		})
	}
}

func TestCmmJWKSBodyAtSizeLimitIsAccepted(t *testing.T) {
	now := time.Unix(7500, 0)
	key := newTestKey(t, "key-1")
	jwks := encodeJWKS(t, key.public)
	body := append(append([]byte(nil), jwks...), strings.Repeat(" ", maxJWKSBodyBytes-len(jwks))...)
	provider, _ := newProviderFixture(t, &testClock{now: now}, body, nil)
	token := signedToken(t, key.private, now, cmmIssuer, cmmAudience, cmmSubject, timePtr(now.Add(time.Minute)), nil)
	if err := cmmAuthenticate(context.Background(), provider, token); err != nil {
		t.Fatalf("Authenticate() with %d-byte JWKS error = %v, want nil", len(body), err)
	}
}

// Traceability: SEC-08
func TestCmmBootstrapRefreshWaitersHonorCancellationAndShareFailure(t *testing.T) {
	now := time.Unix(8000, 0)
	key := newTestKey(t, "key-1")
	provider, _ := newProviderFixture(t, &testClock{now: now}, nil, nil)
	gate := cmmNewGatedJWKS(t, encodeJWKS(t, key.public))
	provider.verifier.jwksURL = gate.url
	gate.status.Store(http.StatusInternalServerError)
	gate.armed.Store(true)
	token := signedToken(t, key.private, now, cmmIssuer, cmmAudience, cmmSubject, timePtr(now.Add(time.Minute)), nil)

	leader := make(chan error, 1)
	go func() { leader <- cmmAuthenticate(context.Background(), provider, token) }()
	<-gate.started

	if err := cmmAuthenticate(cmmCanceledContext(), provider, token); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled bootstrap waiter error = %v, want %v", err, context.Canceled)
	}

	joinCtx := cmmNewDoneSignalContext()
	joined := make(chan error, 1)
	go func() { joined <- provider.verifier.refresh(joinCtx) }()
	<-joinCtx.joined
	gate.Release()

	if err := <-leader; !errors.Is(err, ErrJWKSUnavailable) {
		t.Fatalf("bootstrap leader error = %v, want %v", err, ErrJWKSUnavailable)
	}
	if err := <-joined; !errors.Is(err, ErrJWKSUnavailable) {
		t.Fatalf("joined refresh error = %v, want %v", err, ErrJWKSUnavailable)
	}
	if got := gate.requests.Load(); got != 1 {
		t.Fatalf("JWKS requests = %d, want 1", got)
	}
}

// Traceability: SEC-02, SEC-08
func TestCmmUnknownKeyRefreshWaitersHonorCancellationAndShareFailure(t *testing.T) {
	now := time.Unix(9000, 0)
	clock := &testClock{now: now}
	knownKey := newTestKey(t, "known")
	unknownKey := newTestKey(t, "unknown")
	provider, _ := newProviderFixture(t, clock, nil, nil)
	gate := cmmNewGatedJWKS(t, encodeJWKS(t, knownKey.public))
	provider.verifier.jwksURL = gate.url
	expires := now.Add(time.Minute)
	knownToken := signedToken(t, knownKey.private, now, cmmIssuer, cmmAudience, cmmSubject, &expires, nil)
	unknownToken := signedToken(t, unknownKey.private, now, cmmIssuer, cmmAudience, cmmSubject, &expires, nil)
	if err := cmmAuthenticate(context.Background(), provider, knownToken); err != nil {
		t.Fatalf("bootstrap Authenticate() error = %v", err)
	}
	clock.Advance(11 * time.Second)
	gate.status.Store(http.StatusInternalServerError)
	gate.armed.Store(true)

	leader := make(chan error, 1)
	go func() { leader <- cmmAuthenticate(context.Background(), provider, unknownToken) }()
	<-gate.started

	if err := cmmAuthenticate(cmmCanceledContext(), provider, unknownToken); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled unknown-key waiter error = %v, want %v", err, context.Canceled)
	}

	waiterCtx := cmmNewDoneSignalContext()
	waiter := make(chan error, 1)
	go func() { waiter <- cmmAuthenticate(waiterCtx, provider, unknownToken) }()
	<-waiterCtx.joined
	gate.Release()

	if err := <-leader; !errors.Is(err, ErrUnknownKey) {
		t.Fatalf("unknown-key leader error = %v, want %v", err, ErrUnknownKey)
	}
	if err := <-waiter; !errors.Is(err, ErrUnknownKey) {
		t.Fatalf("unknown-key waiter error = %v, want %v", err, ErrUnknownKey)
	}
	if err := cmmAuthenticate(context.Background(), provider, knownToken); err != nil {
		t.Fatalf("known key after failed refresh error = %v, want nil", err)
	}
	if got := gate.requests.Load(); got != 2 {
		t.Fatalf("JWKS requests = %d, want 2", got)
	}
}
