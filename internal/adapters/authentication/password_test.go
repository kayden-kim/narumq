package authentication

import (
	"context"
	"errors"
	"math"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	applicationauthentication "narumq/internal/application/ports/authentication"
	"narumq/internal/domain/security"
	"narumq/internal/domain/tenancy"
)

func newPasswordTestTenant(t *testing.T, raw string) tenancy.TenantID {
	t.Helper()
	tenantID, err := tenancy.NewTenantID(raw)
	if err != nil {
		t.Fatal(err)
	}
	return tenantID
}

func newPasswordTestBinding(t *testing.T, username string, password []byte) PasswordIdentityBinding {
	t.Helper()
	encoded, err := HashPassword(password)
	if err != nil {
		t.Fatal(err)
	}
	passwordHash, err := ParsePasswordHash(encoded)
	if err != nil {
		t.Fatal(err)
	}
	return PasswordIdentityBinding{
		Username:         username,
		PasswordHash:     passwordHash,
		TenantID:         newPasswordTestTenant(t, "tenant-a"),
		Subject:          "device-subject",
		CredentialID:     username + "-credential",
		AllowedClientIDs: []string{"device-client"},
		Roles:            []security.Role{"publisher"},
	}
}

// Scenarios: tenancy-and-security.md:SEC-SCN-014
func TestPasswordAuthenticationProviderVerifiesExactBytesAndCopiesBindings(t *testing.T) {
	password := []byte{0x00, ' ', 'p', 'a', 's', 's', 0xff, '\n'}
	first := newPasswordTestBinding(t, "device-a", password)
	second := newPasswordTestBinding(t, "device-b", password)
	if first.PasswordHash == second.PasswordHash {
		t.Fatal("HashPassword returned the same salt and digest twice")
	}
	allowedClientIDs := first.AllowedClientIDs
	roles := first.Roles
	provider, err := NewPasswordAuthenticationProvider([]PasswordIdentityBinding{first, second})
	if err != nil {
		t.Fatal(err)
	}
	allowedClientIDs[0] = "mutated-client"
	roles[0] = "mutated-role"

	for _, username := range []string{"device-a", "device-b"} {
		result, err := provider.Authenticate(context.Background(), applicationauthentication.Credential{Username: username, Password: password})
		if err != nil {
			t.Fatalf("Authenticate(%q) rejected exact password bytes: %v", username, err)
		}
		if result.Subject != "device-subject" || result.CredentialID != username+"-credential" || result.TenantID.String() != "tenant-a" {
			t.Fatalf("Authenticate(%q) result = %#v", username, result)
		}
		if !result.ExpiresAt.Equal(time.UnixMilli(math.MaxInt64)) {
			t.Fatalf("expiry = %s, want no-expiry representation", result.ExpiresAt)
		}
		result.AllowedClientIDs[0] = "mutated-result"
		result.Roles[0] = "mutated-result"
	}

	for _, credential := range []applicationauthentication.Credential{
		{Username: "device-a", Password: []byte{0x00, ' ', 'p', 'a', 's', 's', 0xff, '\r'}},
		{Username: "unknown", Password: password},
		{Username: "", Password: password},
	} {
		result, err := provider.Authenticate(context.Background(), credential)
		if !errors.Is(err, ErrInvalidCredential) || !reflect.DeepEqual(result, applicationauthentication.Result{}) {
			t.Fatalf("Authenticate(%q) = %#v, %v; want empty result and ErrInvalidCredential", credential.Username, result, err)
		}
	}
	result, err := provider.Authenticate(context.Background(), applicationauthentication.Credential{Username: "device-a", Password: password})
	if err != nil || result.AllowedClientIDs[0] != "device-client" || result.Roles[0] != "publisher" {
		t.Fatalf("provider bindings changed through slices: result=%#v err=%v", result, err)
	}
}

// Scenarios: tenancy-and-security.md:SEC-SCN-015
func TestParsePasswordHashAcceptsOnlyCanonicalProfile(t *testing.T) {
	valid := "$argon2id$v=19$m=19456,t=2,p=1$AAAAAAAAAAAAAAAAAAAAAA$AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	if _, err := ParsePasswordHash(valid); err != nil {
		t.Fatalf("valid fixed-profile PHC hash rejected: %v", err)
	}

	invalid := []struct {
		name  string
		value string
	}{
		{name: "empty"},
		{name: "algorithm", value: strings.Replace(valid, "argon2id", "argon2i", 1)},
		{name: "version", value: strings.Replace(valid, "v=19", "v=16", 1)},
		{name: "parameters", value: strings.Replace(valid, "m=19456,t=2,p=1", "m=19456,t=3,p=1", 1)},
		{name: "padded salt", value: strings.Replace(valid, "$AAAAAAAAAAAAAAAAAAAAAA$", "$AAAAAAAAAAAAAAAAAAAAAA==$", 1)},
		{name: "invalid salt base64", value: strings.Replace(valid, "$AAAAAAAAAAAAAAAAAAAAAA$", "$AAAAAAAAAAAAAAAAAAAAA!$", 1)},
		{name: "short digest", value: strings.TrimSuffix(valid, "A")},
		{name: "padded digest", value: valid + "="},
		{name: "noncanonical digest", value: strings.TrimSuffix(valid, "A") + "B"},
		{name: "carriage return", value: valid + "\r"},
		{name: "line feed", value: valid + "\n"},
		{name: "trailing field", value: valid + "$extra"},
	}
	for _, testCase := range invalid {
		t.Run(testCase.name, func(t *testing.T) {
			if _, err := ParsePasswordHash(testCase.value); !errors.Is(err, ErrInvalidPasswordHash) {
				t.Fatalf("ParsePasswordHash() error = %v, want %v", err, ErrInvalidPasswordHash)
			}
		})
	}
}

// Scenarios: tenancy-and-security.md:SEC-SCN-015
func TestHashPasswordAndAuthenticateRespectByteLengthBoundaries(t *testing.T) {
	if _, err := HashPassword(nil); !errors.Is(err, ErrInvalidPasswordLength) {
		t.Fatalf("HashPassword(empty) error = %v, want %v", err, ErrInvalidPasswordLength)
	}
	if _, err := HashPassword(make([]byte, maxPasswordLength+1)); !errors.Is(err, ErrInvalidPasswordLength) {
		t.Fatalf("HashPassword(oversized) error = %v, want %v", err, ErrInvalidPasswordLength)
	}

	oneBytePassword := []byte{0xff}
	oneByteBinding := newPasswordTestBinding(t, "one-byte-password", oneBytePassword)
	password := make([]byte, maxPasswordLength)
	for index := range password {
		password[index] = byte(index)
	}
	binding := newPasswordTestBinding(t, "large-password", password)
	provider, err := NewPasswordAuthenticationProvider([]PasswordIdentityBinding{oneByteBinding, binding})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := provider.Authenticate(context.Background(), applicationauthentication.Credential{Username: "one-byte-password", Password: oneBytePassword}); err != nil {
		t.Fatalf("Authenticate(one-byte password) error = %v", err)
	}
	if _, err := provider.Authenticate(context.Background(), applicationauthentication.Credential{Username: "large-password"}); !errors.Is(err, ErrInvalidCredential) {
		t.Fatalf("Authenticate(empty password) error = %v, want %v", err, ErrInvalidCredential)
	}
	if _, err := provider.Authenticate(context.Background(), applicationauthentication.Credential{Username: "large-password", Password: make([]byte, maxPasswordLength+1)}); !errors.Is(err, ErrInvalidCredential) {
		t.Fatalf("Authenticate(oversized password) error = %v, want %v", err, ErrInvalidCredential)
	}
	if _, err := provider.Authenticate(context.Background(), applicationauthentication.Credential{Username: "large-password", Password: password}); err != nil {
		t.Fatalf("Authenticate(maximum password length) error = %v", err)
	}
}

type cancelAfterPasswordContext struct {
	context.Context
	remainingChecks int
}

func (ctx *cancelAfterPasswordContext) Err() error {
	if ctx.remainingChecks == 0 {
		return context.Canceled
	}
	ctx.remainingChecks--
	return nil
}

// Scenarios: tenancy-and-security.md:SEC-SCN-015
func TestPasswordAuthenticationRejectsCanceledContextAndSaturatedSlots(t *testing.T) {
	password := []byte("slot-boundary-password")
	binding := newPasswordTestBinding(t, "device", password)
	provider, err := NewPasswordAuthenticationProvider([]PasswordIdentityBinding{binding})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := provider.Authenticate(ctx, applicationauthentication.Credential{Username: "device", Password: password}); !errors.Is(err, ErrInvalidCredential) {
		t.Fatalf("Authenticate(canceled context) error = %v, want %v", err, ErrInvalidCredential)
	}
	postHashContext := &cancelAfterPasswordContext{Context: context.Background(), remainingChecks: 2}
	if result, err := provider.Authenticate(postHashContext, applicationauthentication.Credential{Username: "device", Password: password}); !errors.Is(err, ErrInvalidCredential) || !reflect.DeepEqual(result, applicationauthentication.Result{}) {
		t.Fatalf("Authenticate(context canceled during hash) = %#v, %v; want empty result and ErrInvalidCredential", result, err)
	}

	for range maxConcurrentPasswordHashes {
		provider.slots <- struct{}{}
	}
	if _, err := provider.Authenticate(context.Background(), applicationauthentication.Credential{Username: "device", Password: password}); !errors.Is(err, ErrInvalidCredential) {
		t.Fatalf("Authenticate(saturated slots) error = %v, want %v", err, ErrInvalidCredential)
	}
	<-provider.slots
	if _, err := provider.Authenticate(context.Background(), applicationauthentication.Credential{Username: "device", Password: password}); err != nil {
		t.Fatalf("Authenticate after slot release error = %v", err)
	}
}

// Scenarios: tenancy-and-security.md:SEC-SCN-014
func TestPasswordAuthenticationProviderRejectsIncompleteBindings(t *testing.T) {
	password := []byte("constructor-boundary")
	valid := newPasswordTestBinding(t, "device", password)
	cases := []struct {
		name     string
		bindings []PasswordIdentityBinding
	}{
		{name: "empty bindings"},
		{name: "zero hash", bindings: []PasswordIdentityBinding{{Username: "device", TenantID: valid.TenantID, Subject: "subject", CredentialID: "credential", AllowedClientIDs: []string{"client"}, Roles: []security.Role{"publisher"}}}},
		{name: "duplicate username", bindings: []PasswordIdentityBinding{valid, valid}},
		{name: "duplicate credential ID", bindings: []PasswordIdentityBinding{valid, func() PasswordIdentityBinding {
			duplicate := valid
			duplicate.Username = "other"
			return duplicate
		}()}},
		{name: "empty Client ID", bindings: []PasswordIdentityBinding{func() PasswordIdentityBinding {
			binding := valid
			binding.AllowedClientIDs = []string{""}
			return binding
		}()}},
		{name: "duplicate role", bindings: []PasswordIdentityBinding{func() PasswordIdentityBinding {
			binding := valid
			binding.Roles = []security.Role{"publisher", "publisher"}
			return binding
		}()}},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			if _, err := NewPasswordAuthenticationProvider(testCase.bindings); err == nil {
				t.Fatal("NewPasswordAuthenticationProvider() accepted invalid bindings")
			}
		})
	}
}

// Scenarios: tenancy-and-security.md:SEC-SCN-014
func TestPasswordAuthenticationProviderConcurrentSuccessAndFailure(t *testing.T) {
	password := []byte("concurrent-authentication-password")
	provider, err := NewPasswordAuthenticationProvider([]PasswordIdentityBinding{newPasswordTestBinding(t, "device", password)})
	if err != nil {
		t.Fatal(err)
	}

	start := make(chan struct{})
	var workers sync.WaitGroup
	errorsByWorker := make(chan error, maxConcurrentPasswordHashes)
	for worker := 0; worker < maxConcurrentPasswordHashes; worker++ {
		workers.Add(1)
		go func(worker int) {
			defer workers.Done()
			<-start
			for attempt := 0; attempt < 4; attempt++ {
				credential := applicationauthentication.Credential{Username: "device", Password: password}
				if (worker+attempt)%2 == 1 {
					credential.Password = []byte("wrong-password")
				}
				result, authErr := provider.Authenticate(context.Background(), credential)
				if (worker+attempt)%2 == 0 {
					if authErr != nil || result.Subject != "device-subject" {
						errorsByWorker <- errors.New("valid concurrent credential did not return its identity")
						return
					}
				} else if !errors.Is(authErr, ErrInvalidCredential) || !reflect.DeepEqual(result, applicationauthentication.Result{}) {
					errorsByWorker <- errors.New("invalid concurrent credential did not fail closed")
					return
				}
			}
		}(worker)
	}
	close(start)
	workers.Wait()
	close(errorsByWorker)
	for err := range errorsByWorker {
		t.Fatal(err)
	}
}
