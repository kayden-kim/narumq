package authentication

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lestrrat-go/jwx/v3/jwa"
	"github.com/lestrrat-go/jwx/v3/jwk"
	"github.com/lestrrat-go/jwx/v3/jwt"

	applicationauthentication "narumq/internal/application/ports/authentication"
	"narumq/internal/domain/security"
	"narumq/internal/domain/tenancy"
)

type testClock struct {
	mu  sync.Mutex
	now time.Time
}

func (clock *testClock) Now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.now
}

func (clock *testClock) Advance(delta time.Duration) {
	clock.mu.Lock()
	clock.now = clock.now.Add(delta)
	clock.mu.Unlock()
}

type testKey struct {
	private jwk.Key
	public  jwk.Key
}

func newTestKey(t *testing.T, kid string) testKey {
	t.Helper()
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	private, err := jwk.Import(privateKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := private.Set("kid", kid); err != nil {
		t.Fatal(err)
	}
	if err := private.Set("alg", "RS256"); err != nil {
		t.Fatal(err)
	}
	public, err := jwk.PublicKeyOf(private)
	if err != nil {
		t.Fatal(err)
	}
	if err := public.Set("kid", kid); err != nil {
		t.Fatal(err)
	}
	if err := public.Set("alg", "RS256"); err != nil {
		t.Fatal(err)
	}
	return testKey{private: private, public: public}
}

func encodeJWKS(t *testing.T, keys ...jwk.Key) []byte {
	t.Helper()
	set := jwk.NewSet()
	for _, key := range keys {
		if err := set.AddKey(key); err != nil {
			t.Fatal(err)
		}
	}
	body, err := json.Marshal(set)
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func signedToken(t *testing.T, key jwk.Key, now time.Time, issuer, audience, subject string, expiration, notBefore *time.Time) []byte {
	t.Helper()
	token := jwt.New()
	if err := token.Set("iss", issuer); err != nil {
		t.Fatal(err)
	}
	if err := token.Set("aud", []string{audience}); err != nil {
		t.Fatal(err)
	}
	if subject != "" {
		if err := token.Set("sub", subject); err != nil {
			t.Fatal(err)
		}
	}
	if expiration != nil {
		if err := token.Set("exp", *expiration); err != nil {
			t.Fatal(err)
		}
	}
	if notBefore != nil {
		if err := token.Set("nbf", *notBefore); err != nil {
			t.Fatal(err)
		}
	}
	encoded, err := jwt.Sign(token, jwt.WithKey(jwa.RS256(), key))
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func mutateHeader(t *testing.T, token []byte, changes map[string]any) []byte {
	t.Helper()
	parts := strings.Split(string(token), ".")
	if len(parts) != 3 {
		t.Fatal("token is not compact")
	}
	headerBytes, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		t.Fatal(err)
	}
	var header map[string]any
	if err := json.Unmarshal(headerBytes, &header); err != nil {
		t.Fatal(err)
	}
	for key, value := range changes {
		if value == nil {
			delete(header, key)
		} else {
			header[key] = value
		}
	}
	headerBytes, err = json.Marshal(header)
	if err != nil {
		t.Fatal(err)
	}
	parts[0] = base64.RawURLEncoding.EncodeToString(headerBytes)
	return []byte(strings.Join(parts, "."))
}

func newProviderFixture(t *testing.T, clock *testClock, body []byte, handlerStatus *atomic.Int32) (*JWKSAuthenticationProvider, func() int64) {
	t.Helper()
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requests.Add(1)
		if handlerStatus != nil && handlerStatus.Load() != http.StatusOK {
			writer.WriteHeader(int(handlerStatus.Load()))
			return
		}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write(body)
	}))
	t.Cleanup(server.Close)
	tenant, err := tenancy.NewTenantID("tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	provider, err := NewJWKSAuthenticationProvider(Config{
		Issuer:          "https://issuer.example",
		Audience:        "narumq",
		Algorithms:      []string{"RS256"},
		JWKSURL:         server.URL,
		CacheTTL:        time.Minute,
		RefreshCooldown: 10 * time.Second,
		ClockSkew:       time.Second,
		HTTPTimeout:     time.Second,
		HTTPClient:      &http.Client{},
		Clock:           clock,
		Bindings: []IdentityBinding{{
			Issuer:           "https://issuer.example",
			Subject:          "device-1",
			TenantID:         tenant,
			CredentialID:     "device-key",
			AllowedClientIDs: []string{"device-1"},
			Roles:            []security.Role{"publisher"},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return provider, func() int64 { return requests.Load() }
}

// Requirements: SEC-02
func TestJWKSAuthenticationRejectsAlgorithmSignatureAndClaims(t *testing.T) {
	now := time.Unix(1000, 0)
	clock := &testClock{now: now}
	key := newTestKey(t, "key-1")
	status := &atomic.Int32{}
	status.Store(http.StatusOK)
	provider, _ := newProviderFixture(t, clock, encodeJWKS(t, key.public), status)
	expires := now.Add(time.Minute)
	valid := signedToken(t, key.private, now, "https://issuer.example", "narumq", "device-1", &expires, nil)
	if _, err := provider.Authenticate(context.Background(), applicationauthentication.Credential{Token: valid}); err != nil {
		t.Fatalf("valid token rejected: %v", err)
	}
	cases := []struct {
		name  string
		token []byte
		want  error
	}{
		{name: "algorithm confusion", token: mutateHeader(t, valid, map[string]any{"alg": "HS256"}), want: ErrUnsupportedAlgorithm},
		{name: "bad signature", token: signedToken(t, newTestKey(t, "key-1").private, now, "https://issuer.example", "narumq", "device-1", &expires, nil), want: ErrInvalidCredential},
		{name: "wrong issuer", token: signedToken(t, key.private, now, "https://other.example", "narumq", "device-1", &expires, nil), want: ErrInvalidCredential},
		{name: "wrong audience", token: signedToken(t, key.private, now, "https://issuer.example", "other", "device-1", &expires, nil), want: ErrInvalidCredential},
		{name: "missing subject", token: signedToken(t, key.private, now, "https://issuer.example", "narumq", "", &expires, nil), want: ErrInvalidCredential},
		{name: "expired", token: signedToken(t, key.private, now, "https://issuer.example", "narumq", "device-1", timePtr(now.Add(-time.Minute)), nil), want: ErrInvalidCredential},
		{name: "not yet valid", token: signedToken(t, key.private, now, "https://issuer.example", "narumq", "device-1", &expires, timePtr(now.Add(time.Minute))), want: ErrInvalidCredential},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			if _, err := provider.Authenticate(context.Background(), applicationauthentication.Credential{Token: testCase.token}); !errors.Is(err, testCase.want) {
				t.Fatalf("Authenticate() error = %v, want %v", err, testCase.want)
			}
		})
	}
}

// Requirements: SEC-02
func TestJWKSBootstrapFetchCollapses(t *testing.T) {
	now := time.Unix(2000, 0)
	clock := &testClock{now: now}
	key := newTestKey(t, "key-1")
	if err := key.public.Remove("alg"); err != nil {
		t.Fatal(err)
	}
	status := &atomic.Int32{}
	status.Store(http.StatusOK)
	provider, requests := newProviderFixture(t, clock, encodeJWKS(t, key.public), status)
	expires := now.Add(time.Minute)
	token := signedToken(t, key.private, now, "https://issuer.example", "narumq", "device-1", &expires, nil)
	start := make(chan struct{})
	errorsCh := make(chan error, 16)
	for index := 0; index < 16; index++ {
		go func() {
			<-start
			_, err := provider.Authenticate(context.Background(), applicationauthentication.Credential{Token: token})
			errorsCh <- err
		}()
	}
	close(start)
	for index := 0; index < 16; index++ {
		if err := <-errorsCh; err != nil {
			t.Fatalf("concurrent Authenticate() error = %v", err)
		}
	}
	if got := requests(); got != 1 {
		t.Fatalf("JWKS requests = %d, want 1", got)
	}
}

// Requirements: SEC-08
func TestJWKSBootstrapFailureCollapses(t *testing.T) {
	now := time.Unix(2500, 0)
	clock := &testClock{now: now}
	key := newTestKey(t, "key-1")
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requests.Add(1)
		started <- struct{}{}
		<-release
		writer.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(server.Close)
	tenant, err := tenancy.NewTenantID("tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	provider, err := NewJWKSAuthenticationProvider(Config{
		Issuer: "https://issuer.example", Audience: "narumq", Algorithms: []string{"RS256"}, JWKSURL: server.URL,
		CacheTTL: time.Minute, RefreshCooldown: 10 * time.Second, ClockSkew: time.Second, HTTPTimeout: time.Second,
		HTTPClient: &http.Client{}, Clock: clock,
		Bindings: []IdentityBinding{{Issuer: "https://issuer.example", Subject: "device-1", TenantID: tenant, CredentialID: "device-key", AllowedClientIDs: []string{"device-1"}, Roles: []security.Role{"publisher"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	expires := now.Add(time.Minute)
	token := signedToken(t, key.private, now, "https://issuer.example", "narumq", "device-1", &expires, nil)
	start := make(chan struct{})
	errorsCh := make(chan error, 8)
	for index := 0; index < 8; index++ {
		go func() {
			<-start
			_, err := provider.Authenticate(context.Background(), applicationauthentication.Credential{Token: token})
			errorsCh <- err
		}()
	}
	close(start)
	<-started
	time.Sleep(25 * time.Millisecond)
	close(release)
	for index := 0; index < 8; index++ {
		if err := <-errorsCh; !errors.Is(err, ErrJWKSUnavailable) {
			t.Fatalf("bootstrap failure error = %v, want %v", err, ErrJWKSUnavailable)
		}
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("JWKS requests after collapsed failure = %d, want 1", got)
	}
}

// Requirements: SEC-02
func TestJWKSUnknownKeyRefreshCollapsesAndThrottles(t *testing.T) {
	now := time.Unix(3000, 0)
	clock := &testClock{now: now}
	oldKey := newTestKey(t, "old")
	newKey := newTestKey(t, "new")
	var bodyMu sync.RWMutex
	body := encodeJWKS(t, oldKey.public)
	status := &atomic.Int32{}
	status.Store(http.StatusOK)
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requests.Add(1)
		bodyMu.RLock()
		defer bodyMu.RUnlock()
		_, _ = writer.Write(body)
	}))
	t.Cleanup(server.Close)
	tenant, err := tenancy.NewTenantID("tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	provider, err := NewJWKSAuthenticationProvider(Config{
		Issuer: "https://issuer.example", Audience: "narumq", Algorithms: []string{"RS256"}, JWKSURL: server.URL,
		CacheTTL: time.Minute, RefreshCooldown: 10 * time.Second, ClockSkew: time.Second, HTTPTimeout: time.Second,
		HTTPClient: &http.Client{}, Clock: clock,
		Bindings: []IdentityBinding{{Issuer: "https://issuer.example", Subject: "device-1", TenantID: tenant, CredentialID: "device-key", AllowedClientIDs: []string{"device-1"}, Roles: []security.Role{"publisher"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	expires := now.Add(time.Minute)
	knownToken := signedToken(t, oldKey.private, now, "https://issuer.example", "narumq", "device-1", &expires, nil)
	if _, err := provider.Authenticate(context.Background(), applicationauthentication.Credential{Token: knownToken}); err != nil {
		t.Fatal(err)
	}
	clock.Advance(11 * time.Second)
	bodyMu.Lock()
	body = encodeJWKS(t, oldKey.public, newKey.public)
	bodyMu.Unlock()
	unknownToken := signedToken(t, newKey.private, now, "https://issuer.example", "narumq", "device-1", &expires, nil)
	start := make(chan struct{})
	errorsCh := make(chan error, 16)
	for index := 0; index < 16; index++ {
		go func() {
			<-start
			_, err := provider.Authenticate(context.Background(), applicationauthentication.Credential{Token: unknownToken})
			errorsCh <- err
		}()
	}
	close(start)
	for index := 0; index < 16; index++ {
		if err := <-errorsCh; err != nil {
			t.Fatalf("unknown key refresh error = %v", err)
		}
	}
	if got := requests.Load(); got != 2 {
		t.Fatalf("JWKS requests after collapsed refresh = %d, want 2", got)
	}
	if _, err := provider.Authenticate(context.Background(), applicationauthentication.Credential{Token: signedToken(t, newTestKey(t, "other").private, now, "https://issuer.example", "narumq", "device-1", &expires, nil)}); !errors.Is(err, ErrUnknownKey) {
		t.Fatalf("throttled unknown key error = %v, want %v", err, ErrUnknownKey)
	}
	if got := requests.Load(); got != 2 {
		t.Fatalf("JWKS requests after throttled key = %d, want 2", got)
	}
}

// Requirements: SEC-02, SEC-08
func TestJWKSFailedEarlyRefreshPreservesKnownKeyAndTTLDoesNotExtend(t *testing.T) {
	now := time.Unix(4000, 0)
	clock := &testClock{now: now}
	oldKey := newTestKey(t, "old")
	unknownKey := newTestKey(t, "unknown")
	status := &atomic.Int32{}
	status.Store(http.StatusOK)
	provider, requests := newProviderFixture(t, clock, encodeJWKS(t, oldKey.public), status)
	expires := now.Add(time.Minute)
	knownToken := signedToken(t, oldKey.private, now, "https://issuer.example", "narumq", "device-1", &expires, nil)
	if _, err := provider.Authenticate(context.Background(), applicationauthentication.Credential{Token: knownToken}); err != nil {
		t.Fatal(err)
	}
	clock.Advance(11 * time.Second)
	status.Store(http.StatusInternalServerError)
	unknownToken := signedToken(t, unknownKey.private, now, "https://issuer.example", "narumq", "device-1", &expires, nil)
	if _, err := provider.Authenticate(context.Background(), applicationauthentication.Credential{Token: unknownToken}); !errors.Is(err, ErrUnknownKey) {
		t.Fatalf("failed early refresh error = %v, want %v", err, ErrUnknownKey)
	}
	if _, err := provider.Authenticate(context.Background(), applicationauthentication.Credential{Token: knownToken}); err != nil {
		t.Fatalf("known key fallback error = %v", err)
	}
	if got := requests(); got != 2 {
		t.Fatalf("JWKS requests after early failure = %d, want 2", got)
	}
	clock.Advance(61 * time.Second)
	if _, err := provider.Authenticate(context.Background(), applicationauthentication.Credential{Token: knownToken}); !errors.Is(err, ErrJWKSUnavailable) {
		t.Fatalf("expired cache error = %v, want %v", err, ErrJWKSUnavailable)
	}
	if got := requests(); got != 3 {
		t.Fatalf("JWKS requests after TTL expiry = %d, want 3", got)
	}
}

func timePtr(value time.Time) *time.Time {
	return &value
}

func TestNewJWKSAuthenticationProvider_ValidationErrors(t *testing.T) {
	tenant, _ := tenancy.NewTenantID("tenant-a")
	validBinding := IdentityBinding{
		Issuer:           "https://issuer.example",
		Subject:          "device-1",
		TenantID:         tenant,
		CredentialID:     "device-key",
		AllowedClientIDs: []string{"device-1"},
		Roles:            []security.Role{"publisher"},
	}
	baseConfig := func() Config {
		return Config{
			Issuer:          "https://issuer.example",
			Audience:        "narumq",
			Algorithms:      []string{"RS256"},
			JWKSURL:         "https://issuer.example/jwks",
			CacheTTL:        time.Minute,
			RefreshCooldown: 10 * time.Second,
			ClockSkew:       time.Second,
			HTTPTimeout:     time.Second,
			HTTPClient:      &http.Client{},
			Clock:           &testClock{now: time.Now()},
			Bindings:        []IdentityBinding{validBinding},
		}
	}

	cases := []struct {
		name   string
		modify func(*Config)
	}{
		{"missing issuer", func(c *Config) { c.Issuer = "" }},
		{"missing audience", func(c *Config) { c.Audience = "" }},
		{"missing jwksURL", func(c *Config) { c.JWKSURL = "" }},
		{"empty algorithms", func(c *Config) { c.Algorithms = nil }},
		{"empty algorithm string", func(c *Config) { c.Algorithms = []string{""} }},
		{"duplicate algorithm", func(c *Config) { c.Algorithms = []string{"RS256", "RS256"} }},
		{"invalid cacheTTL", func(c *Config) { c.CacheTTL = 0 }},
		{"invalid refreshCooldown", func(c *Config) { c.RefreshCooldown = -1 }},
		{"invalid clockSkew", func(c *Config) { c.ClockSkew = 0 }},
		{"invalid httpTimeout", func(c *Config) { c.HTTPTimeout = 0 }},
		{"nil httpClient", func(c *Config) { c.HTTPClient = nil }},
		{"nil clock", func(c *Config) { c.Clock = nil }},
		{"incomplete binding issuer", func(c *Config) { c.Bindings[0].Issuer = "" }},
		{"incomplete binding subject", func(c *Config) { c.Bindings[0].Subject = "" }},
		{"incomplete binding cred", func(c *Config) { c.Bindings[0].CredentialID = "" }},
		{"incomplete binding tenant", func(c *Config) { c.Bindings[0].TenantID = tenancy.TenantID{} }},
		{"incomplete binding clients", func(c *Config) { c.Bindings[0].AllowedClientIDs = nil }},
		{"incomplete binding roles", func(c *Config) { c.Bindings[0].Roles = nil }},
		{"duplicate binding", func(c *Config) { c.Bindings = []IdentityBinding{validBinding, validBinding} }},
		{"empty client id", func(c *Config) { c.Bindings[0].AllowedClientIDs = []string{""} }},
		{"duplicate client id", func(c *Config) { c.Bindings[0].AllowedClientIDs = []string{"c", "c"} }},
		{"empty role", func(c *Config) { c.Bindings[0].Roles = []security.Role{""} }},
		{"duplicate role", func(c *Config) { c.Bindings[0].Roles = []security.Role{"r", "r"} }},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := baseConfig()
			tc.modify(&cfg)
			if _, err := NewJWKSAuthenticationProvider(cfg); err == nil {
				t.Fatalf("expected error for %s, got nil", tc.name)
			}
		})
	}
}

func TestAuthenticate_NilInputs(t *testing.T) {
	var provider *JWKSAuthenticationProvider
	if _, err := provider.Authenticate(context.Background(), applicationauthentication.Credential{Token: []byte("a.b.c")}); !errors.Is(err, ErrInvalidCredential) {
		t.Fatalf("nil provider Authenticate error = %v", err)
	}
	clock := &testClock{now: time.Now()}
	p, _ := newProviderFixture(t, clock, []byte("{}"), nil)
	if _, err := p.Authenticate(nil, applicationauthentication.Credential{Token: []byte("a.b.c")}); !errors.Is(err, ErrInvalidCredential) {
		t.Fatalf("nil context Authenticate error = %v", err)
	}
	if _, err := p.Authenticate(context.Background(), applicationauthentication.Credential{Token: nil}); !errors.Is(err, ErrInvalidCredential) {
		t.Fatalf("nil token Authenticate error = %v", err)
	}
}
