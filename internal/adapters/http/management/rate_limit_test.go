package management

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	adaptersmetrics "narumq/internal/adapters/metrics"
	portsaudit "narumq/internal/application/ports/audit"
	ports "narumq/internal/application/ports/management"
)

type rateLimitTestClock struct {
	now time.Time
}

func (clock *rateLimitTestClock) Now() time.Time {
	return clock.now
}

func (clock *rateLimitTestClock) Advance(duration time.Duration) {
	clock.now = clock.now.Add(duration)
}

func serveRateLimitRequest(handler http.Handler, path, bearer, apiKey, remoteAddr string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodGet, path, nil)
	if bearer != "" {
		request.Header.Set("Authorization", "Bearer "+bearer)
		request.Header.Set("X-API-Key", bearer)
	}
	if apiKey != "" {
		request.Header.Set("X-API-Key", apiKey)
	}
	if remoteAddr != "" {
		request.RemoteAddr = remoteAddr
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

// TestRateLimitPrincipalBucketRejectsThenRefills verifies principal burst, refill and clock reversal.
// Traceability: PRD API-05; docs/specs/management-api-and-observability.md §1.5.
func TestRateLimitPrincipalBucketRejectsThenRefills(t *testing.T) {
	clock := &rateLimitTestClock{now: time.Unix(1_800_000_000, 0)}
	handler := NewRouter(ServerConfig{
		AuthToken:     "bearer-secret",
		RequestRate:   2,
		Clock:         clock,
		Observability: successHTTPObservabilityStub(),
	}, &mockStateProvider{}, &mockHealthChecker{liveStatus: ports.HealthStatusUp}, nil)

	for index, wantStatus := range []int{http.StatusOK, http.StatusOK, http.StatusTooManyRequests} {
		response := serveRateLimitRequest(handler, "/api/v1/cluster", "bearer-secret", "", "192.0.2.10:1234")
		if response.Code != wantStatus {
			t.Fatalf("request %d status = %d, want %d: %s", index+1, response.Code, wantStatus, response.Body.String())
		}
		if index == 2 {
			assertRateLimitedResponse(t, response)
		}
	}

	clock.Advance(500 * time.Millisecond)
	if response := serveRateLimitRequest(handler, "/api/v1/cluster", "bearer-secret", "", "192.0.2.10:1234"); response.Code != http.StatusOK {
		t.Fatalf("request after 500ms refill status = %d, want 200", response.Code)
	}
	if response := serveRateLimitRequest(handler, "/api/v1/cluster", "bearer-secret", "", "192.0.2.10:1234"); response.Code != http.StatusTooManyRequests {
		t.Fatalf("request after consuming refill status = %d, want 429", response.Code)
	}

	clock.Advance(-time.Second)
	if response := serveRateLimitRequest(handler, "/api/v1/cluster", "bearer-secret", "", "192.0.2.10:1234"); response.Code != http.StatusTooManyRequests {
		t.Fatalf("request after clock reversal status = %d, want 429", response.Code)
	}
}

// TestRateLimitSeparatesPrincipalsAndAuthFailures verifies per-subject and per-IP buckets.
// Traceability: PRD API-05; docs/specs/management-api-and-observability.md §1.5.
func TestRateLimitSeparatesPrincipalsAndAuthFailures(t *testing.T) {
	clock := &rateLimitTestClock{now: time.Unix(1_800_000_000, 0)}
	handler := NewRouter(ServerConfig{
		AuthToken:       "bearer-secret",
		APIKeys:         []APIKeyCredential{{ID: "test", Key: "api-key-secret", Roles: []Role{RoleReadOnly}}},
		RequestRate:     1,
		AuthFailureRate: 1,
		Clock:           clock,
		Observability:   successHTTPObservabilityStub(),
	}, &mockStateProvider{}, &mockHealthChecker{liveStatus: ports.HealthStatusUp}, nil)

	if response := serveRateLimitRequest(handler, "/api/v1/cluster", "bearer-secret", "", "192.0.2.10:1234"); response.Code != http.StatusOK {
		t.Fatalf("bearer request status = %d, want 200", response.Code)
	}
	if response := serveRateLimitRequest(handler, "/api/v1/cluster", "", "api-key-secret", "192.0.2.10:1234"); response.Code != http.StatusOK {
		t.Fatalf("API key request status = %d, want 200", response.Code)
	}
	if response := serveRateLimitRequest(handler, "/api/v1/cluster", "invalid", "", "192.0.2.10:1234"); response.Code != http.StatusUnauthorized {
		t.Fatalf("first invalid credential status = %d, want 401", response.Code)
	}
	if response := serveRateLimitRequest(handler, "/api/v1/cluster", "invalid", "", "192.0.2.10:1234"); response.Code != http.StatusTooManyRequests {
		t.Fatalf("second invalid credential status = %d, want 429", response.Code)
	}

	clock.Advance(time.Second)
	if response := serveRateLimitRequest(handler, "/api/v1/cluster", "bearer-secret", "", "192.0.2.10:1234"); response.Code != http.StatusOK {
		t.Fatalf("valid request after principal refill status = %d, want 200", response.Code)
	}
	if response := serveRateLimitRequest(handler, "/api/v1/cluster", "invalid", "", "192.0.2.11:1234"); response.Code != http.StatusUnauthorized {
		t.Fatalf("invalid credential from another IP status = %d, want 401", response.Code)
	}

	request := httptest.NewRequest(http.MethodGet, "/api/v1/cluster", nil)
	request.RemoteAddr = "192.0.2.10:4321"
	request.Header.Set("Authorization", "Bearer invalid")
	request.Header.Set("X-Forwarded-For", "192.0.2.11")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("X-Forwarded-For changed the RemoteAddr bucket: status = %d, want 401", response.Code)
	}
}

// TestRateLimitExcludesProbesAndMetrics verifies probes, scrapes and unknown paths do not consume API tokens.
// Traceability: PRD API-05; docs/specs/management-api-and-observability.md §1.5.
func TestRateLimitExcludesProbesAndMetrics(t *testing.T) {
	clock := &rateLimitTestClock{now: time.Unix(1_800_000_000, 0)}
	registry := adaptersmetrics.NewRegistry()
	handler := NewRouter(ServerConfig{
		AuthToken:       "bearer-secret",
		RequestRate:     1,
		AuthFailureRate: 1,
		Clock:           clock,
		Observability:   successHTTPObservabilityStub(),
	}, &mockStateProvider{}, &mockHealthChecker{liveStatus: ports.HealthStatusUp}, registry)

	for _, path := range []string{"/health/live", "/metrics", "/not-registered"} {
		for index := range 5 {
			response := serveRateLimitRequest(handler, path, "bearer-secret", "", "192.0.2.10:1234")
			if response.Code == http.StatusTooManyRequests {
				t.Fatalf("%s request %d was rate limited", path, index+1)
			}
		}
	}
	if response := serveRateLimitRequest(handler, "/api/v1/cluster", "bearer-secret", "", "192.0.2.10:1234"); response.Code != http.StatusOK {
		t.Fatalf("first API request after probe and scrape requests status = %d, want 200", response.Code)
	}
}

// TestRateLimitForbiddenConsumesPrincipalToken verifies role-denied requests consume a principal token.
// Traceability: PRD API-05; docs/specs/management-api-and-observability.md §1.5.
func TestRateLimitForbiddenConsumesPrincipalToken(t *testing.T) {
	clock := &rateLimitTestClock{now: time.Unix(1_800_000_000, 0)}
	handler := NewRouter(ServerConfig{
		APIKeys:     []APIKeyCredential{{ID: "test", Key: "read-only-secret", Roles: []Role{RoleReadOnly}}},
		RequestRate: 1,
		Clock:       clock,
	}, &mockStateProvider{}, &mockHealthChecker{liveStatus: ports.HealthStatusUp}, nil)

	if response := serveRateLimitRequest(handler, "/api/v1/tenants", "read-only-secret", "", "192.0.2.10:1234"); response.Code != http.StatusForbidden {
		t.Fatalf("forbidden request status = %d, want 403", response.Code)
	}
	if response := serveRateLimitRequest(handler, "/api/v1/cluster", "read-only-secret", "", "192.0.2.10:1234"); response.Code != http.StatusTooManyRequests {
		t.Fatalf("request after forbidden request status = %d, want 429", response.Code)
	}
}

// TestRateLimitedMutationIsAuditedAndNotExecuted verifies rate rejection precedes mutation admission.
// Traceability: PRD API-05; docs/specs/management-api-and-observability.md §1.5.
func TestRateLimitedMutationIsAuditedAndNotExecuted(t *testing.T) {
	clock := &rateLimitTestClock{now: time.Unix(1_800_000_000, 0)}
	spy := &spyAuditUseCase{}
	auth := NewAuthenticator(AuthConfig{
		APIKeys:         []APIKeyCredential{{ID: "test", Key: "bearer-secret", Roles: []Role{RoleOperator}}},
		Audit:           spy,
		RequestRate:     1,
		AuthFailureRate: 1,
		Clock:           clock,
	})
	calls := 0
	handler := auth.RequireMutationPermission(PermissionOperate, auditMutation{action: "node.drain.start", targetType: "node"}, func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.WriteHeader(http.StatusAccepted)
	})

	for index, wantStatus := range []int{http.StatusAccepted, http.StatusTooManyRequests} {
		request := httptest.NewRequest(http.MethodPost, "/api/v1/nodes/1/drain", nil)
		request.Header.Set("X-API-Key", "bearer-secret")
		request.RemoteAddr = "192.0.2.10:1234"
		response := httptest.NewRecorder()
		handler(response, request)
		if response.Code != wantStatus {
			t.Fatalf("mutation request %d status = %d, want %d", index+1, response.Code, wantStatus)
		}
	}
	if calls != 1 || spy.admitCalls != 1 {
		t.Fatalf("handler calls = %d and admission calls = %d, want 1 each", calls, spy.admitCalls)
	}
	if len(spy.commands) != 2 {
		t.Fatalf("audit events = %d, want 2: %+v", len(spy.commands), spy.commands)
	}
	event := spy.commands[len(spy.commands)-1]
	if event.Action != "management.request" || event.Result != portsaudit.AuditResultDenied || event.Reason != "rate_limited" || event.Details["httpStatus"] != http.StatusTooManyRequests || event.Actor.Subject != "apikey:test" {
		t.Fatalf("rate-limited mutation audit event = %+v", event)
	}

	failureAuth := NewAuthenticator(AuthConfig{
		AuthToken:       "bearer-secret",
		Audit:           spy,
		RequestRate:     1,
		AuthFailureRate: 1,
		Clock:           clock,
	})
	failureHandler := failureAuth.RequirePermission(PermissionRead, func(http.ResponseWriter, *http.Request) {})
	for index, wantStatus := range []int{http.StatusUnauthorized, http.StatusTooManyRequests} {
		request := httptest.NewRequest(http.MethodGet, "/api/v1/cluster", nil)
		request.Header.Set("Authorization", "Bearer invalid")
		request.RemoteAddr = "192.0.2.20:1234"
		response := httptest.NewRecorder()
		failureHandler(response, request)
		if response.Code != wantStatus {
			t.Fatalf("authentication failure request %d status = %d, want %d", index+1, response.Code, wantStatus)
		}
	}
	failedActor := spy.commands[len(spy.commands)-1].Actor
	if failedActor.Subject != "unauthenticated" || len(failedActor.Roles) != 0 {
		t.Fatalf("authentication failure rate-limit actor = %+v, want unauthenticated without roles", failedActor)
	}
}

// TestRateLimiterBoundsKeysAndEvictsFullBuckets verifies the key cap and full-bucket eviction rule.
// Traceability: PRD API-05; docs/specs/management-api-and-observability.md §1.5.
func TestRateLimiterBoundsKeysAndEvictsFullBuckets(t *testing.T) {
	clock := &rateLimitTestClock{now: time.Unix(1_800_000_000, 0)}
	limiter := newRequestRateLimiter(1, clock)
	for index := range maxRateLimitBuckets {
		if !limiter.allow("key-" + strconv.Itoa(index)) {
			t.Fatalf("initial request for key %d was rejected", index)
		}
	}
	if limiter.allow("new-key") {
		t.Fatal("new key was admitted at the bucket cap before refill")
	}
	if limiter.allow("key-0") {
		t.Fatal("existing depleted key was admitted before refill")
	}

	clock.Advance(time.Second)
	if !limiter.allow("new-key") {
		t.Fatal("new key was rejected after full buckets were refilled and evicted")
	}
}

// TestRemoteAddressKey verifies host extraction and malformed address handling.
// Traceability: PRD API-05; docs/specs/management-api-and-observability.md §1.5.
func TestRemoteAddressKey(t *testing.T) {
	for _, testCase := range []struct {
		remoteAddr string
		want       string
	}{
		{remoteAddr: "192.0.2.1:1234", want: "192.0.2.1"},
		{remoteAddr: "[2001:db8::1]:80", want: "2001:db8::1"},
		{remoteAddr: "bad", want: "bad"},
		{remoteAddr: "", want: "unknown"},
	} {
		if got := remoteAddressKey(testCase.remoteAddr); got != testCase.want {
			t.Errorf("remoteAddressKey(%q) = %q, want %q", testCase.remoteAddr, got, testCase.want)
		}
	}
}

func assertRateLimitedResponse(t *testing.T, response *httptest.ResponseRecorder) {
	t.Helper()
	if response.Header().Get("Retry-After") != "1" {
		t.Errorf("Retry-After = %q, want 1", response.Header().Get("Retry-After"))
	}
	var body struct {
		Error struct {
			Code      string `json:"code"`
			Message   string `json:"message"`
			Retryable bool   `json:"retryable"`
		} `json:"error"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode 429 response: %v", err)
	}
	if body.Error.Code != "rate_limited" || body.Error.Message != "management API rate limit exceeded" || !body.Error.Retryable {
		t.Errorf("429 response error = %+v, want rate_limited, message, and retryable", body.Error)
	}
	if response.Code != http.StatusTooManyRequests {
		t.Errorf("status = %d, want 429", response.Code)
	}
}
