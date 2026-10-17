package authentication

import (
	"context"
	"errors"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	applicationauthentication "narumq/internal/application/ports/authentication"
)

// Traceability: SEC-10; docs/specs/tenancy-and-security.md §6.
// Scenarios: management-api-and-observability.md:MGT-SCN-018, tenancy-and-security.md:SEC-SCN-032
func TestJWTVerifierReturnsVerifiedClaimsWithoutBinding(t *testing.T) {
	now := time.Unix(1_000, 0)
	key := newTestKey(t, "key-1")
	var status atomic.Int32
	status.Store(http.StatusOK)
	provider, _ := newProviderFixture(t, &testClock{now: now}, encodeJWKS(t, key.public), &status)
	expires := now.Add(time.Minute)
	token := signedToken(t, key.private, now, "https://issuer.example", "narumq", "unbound-subject", &expires, nil)

	verified, err := provider.verifier.VerifyToken(context.Background(), token)
	if err != nil {
		t.Fatalf("VerifyToken() error = %v", err)
	}
	if verified.Issuer != "https://issuer.example" || verified.Subject != "unbound-subject" || !verified.ExpiresAt.Equal(expires) {
		t.Fatalf("VerifyToken() = %+v, want verified claims with expiration %v", verified, expires)
	}
	_, err = provider.Authenticate(context.Background(), applicationauthentication.Credential{Password: token})
	if !errors.Is(err, ErrUnmappedSubject) {
		t.Fatalf("Authenticate() error = %v, want ErrUnmappedSubject", err)
	}
}
