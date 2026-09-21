package authentication

import (
	"context"
	"errors"
	"testing"
	"time"

	applicationauthentication "narumq/internal/application/ports/authentication"
	"narumq/internal/domain/security"
	"narumq/internal/domain/tenancy"
)

func newMTLSTestTenant(t *testing.T, raw string) tenancy.TenantID {
	t.Helper()
	tenantID, err := tenancy.NewTenantID(raw)
	if err != nil {
		t.Fatal(err)
	}
	return tenantID
}

func newMTLSTestBinding(t *testing.T, kind ClientCertificateIdentityKind, value, credentialID string) ClientCertificateIdentityBinding {
	t.Helper()
	return ClientCertificateIdentityBinding{
		Kind:             kind,
		Value:            value,
		TenantID:         newMTLSTestTenant(t, "tenant-a"),
		Subject:          "device-subject",
		CredentialID:     credentialID,
		AllowedClientIDs: []string{"device-client"},
		Roles:            []security.Role{"publisher"},
	}
}

func TestMTLSAuthenticationProviderMatchesConfiguredSelectorKinds(t *testing.T) {
	now := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	clock := &testClock{now: now}
	bindings := []ClientCertificateIdentityBinding{
		newMTLSTestBinding(t, ClientCertificateIdentityKindCommonName, "cn-device", "cn-credential"),
		newMTLSTestBinding(t, ClientCertificateIdentityKindDNSSAN, "dns-device.example", "dns-credential"),
		newMTLSTestBinding(t, ClientCertificateIdentityKindEmailSAN, "email-device@example", "email-credential"),
	}
	provider, err := NewMTLSAuthenticationProvider(clock, bindings)
	if err != nil {
		t.Fatal(err)
	}
	expiresAt := now.Add(time.Hour)
	cases := []struct {
		name        string
		certificate applicationauthentication.ClientCertificateDTO
		credential  string
	}{
		{
			name:        "common name",
			certificate: applicationauthentication.ClientCertificateDTO{CommonName: "cn-device", NotAfter: expiresAt},
			credential:  "cn-credential",
		},
		{
			name:        "dns san",
			certificate: applicationauthentication.ClientCertificateDTO{DNSNames: []string{"dns-device.example"}, NotAfter: expiresAt},
			credential:  "dns-credential",
		},
		{
			name:        "email san",
			certificate: applicationauthentication.ClientCertificateDTO{EmailAddresses: []string{"email-device@example"}, NotAfter: expiresAt},
			credential:  "email-credential",
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			result, err := provider.AuthenticateClientCertificate(context.Background(), testCase.certificate)
			if err != nil {
				t.Fatal(err)
			}
			if result.CredentialID != testCase.credential || result.ExpiresAt != expiresAt {
				t.Fatalf("result = %#v, want credential %q and expiry %s", result, testCase.credential, expiresAt)
			}
			if result.Subject != "device-subject" || result.TenantID.String() != "tenant-a" {
				t.Fatalf("result binding = %#v", result)
			}
			if len(result.AllowedClientIDs) != 1 || result.AllowedClientIDs[0] != "device-client" || len(result.Roles) != 1 || result.Roles[0] != "publisher" {
				t.Fatalf("result scopes = clients=%v roles=%v", result.AllowedClientIDs, result.Roles)
			}
		})
	}
}

func TestMTLSAuthenticationProviderCopiesBindingsAndResults(t *testing.T) {
	now := time.Unix(1000, 0)
	clock := &testClock{now: now}
	allowedClientIDs := []string{"device-client"}
	roles := []security.Role{"publisher"}
	bindings := []ClientCertificateIdentityBinding{{
		Kind:             ClientCertificateIdentityKindCommonName,
		Value:            "device",
		TenantID:         newMTLSTestTenant(t, "tenant-a"),
		Subject:          "subject",
		CredentialID:     "credential",
		AllowedClientIDs: allowedClientIDs,
		Roles:            roles,
	}}
	provider, err := NewMTLSAuthenticationProvider(clock, bindings)
	if err != nil {
		t.Fatal(err)
	}
	allowedClientIDs[0] = "mutated-input"
	roles[0] = "mutated-input"
	result, err := provider.AuthenticateClientCertificate(context.Background(), applicationauthentication.ClientCertificateDTO{
		CommonName: "device",
		NotAfter:   now.Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	result.AllowedClientIDs[0] = "mutated-result"
	result.Roles[0] = "mutated-result"
	resultAgain, err := provider.AuthenticateClientCertificate(context.Background(), applicationauthentication.ClientCertificateDTO{
		CommonName: "device",
		NotAfter:   now.Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	if resultAgain.AllowedClientIDs[0] != "device-client" || resultAgain.Roles[0] != "publisher" {
		t.Fatalf("provider state changed through slices: %#v", resultAgain)
	}
}

func TestMTLSAuthenticationProviderFailsClosedForAmbiguityAndCaseMismatch(t *testing.T) {
	now := time.Unix(2000, 0)
	clock := &testClock{now: now}
	bindings := []ClientCertificateIdentityBinding{
		newMTLSTestBinding(t, ClientCertificateIdentityKindCommonName, "device", "cn-credential"),
		newMTLSTestBinding(t, ClientCertificateIdentityKindDNSSAN, "device", "dns-credential"),
	}
	provider, err := NewMTLSAuthenticationProvider(clock, bindings)
	if err != nil {
		t.Fatal(err)
	}
	expiresAt := now.Add(time.Hour)
	_, err = provider.AuthenticateClientCertificate(context.Background(), applicationauthentication.ClientCertificateDTO{
		CommonName: "device",
		DNSNames:   []string{"device"},
		NotAfter:   expiresAt,
	})
	if !errors.Is(err, ErrAmbiguousClientCertificate) {
		t.Fatalf("ambiguous certificate error = %v, want %v", err, ErrAmbiguousClientCertificate)
	}
	_, err = provider.AuthenticateClientCertificate(context.Background(), applicationauthentication.ClientCertificateDTO{
		CommonName: "Device",
		NotAfter:   expiresAt,
	})
	if !errors.Is(err, ErrUnmappedClientCertificate) {
		t.Fatalf("case mismatch error = %v, want %v", err, ErrUnmappedClientCertificate)
	}
}

func TestMTLSAuthenticationProviderRejectsInvalidAndExpiredCertificates(t *testing.T) {
	now := time.Unix(3000, 0)
	clock := &testClock{now: now}
	provider, err := NewMTLSAuthenticationProvider(clock, []ClientCertificateIdentityBinding{
		newMTLSTestBinding(t, ClientCertificateIdentityKindCommonName, "device", "credential"),
	})
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name string
		ctx  context.Context
		cert applicationauthentication.ClientCertificateDTO
		want error
	}{
		{name: "nil context", cert: applicationauthentication.ClientCertificateDTO{NotAfter: now.Add(time.Hour)}, want: ErrInvalidClientCertificate},
		{name: "zero expiry", ctx: context.Background(), want: ErrInvalidClientCertificate},
		{name: "expired at boundary", ctx: context.Background(), cert: applicationauthentication.ClientCertificateDTO{CommonName: "device", NotAfter: now}, want: ErrExpiredClientCertificate},
		{name: "expired before boundary", ctx: context.Background(), cert: applicationauthentication.ClientCertificateDTO{CommonName: "device", NotAfter: now.Add(-time.Nanosecond)}, want: ErrExpiredClientCertificate},
		{name: "unmapped", ctx: context.Background(), cert: applicationauthentication.ClientCertificateDTO{CommonName: "other", NotAfter: now.Add(time.Hour)}, want: ErrUnmappedClientCertificate},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := provider.AuthenticateClientCertificate(testCase.ctx, testCase.cert)
			if !errors.Is(err, testCase.want) {
				t.Fatalf("error = %v, want %v", err, testCase.want)
			}
		})
	}
}

func TestNewMTLSAuthenticationProviderRejectsInvalidBindings(t *testing.T) {
	clock := &testClock{now: time.Unix(4000, 0)}
	valid := newMTLSTestBinding(t, ClientCertificateIdentityKindCommonName, "device", "credential")
	cases := []struct {
		name     string
		clock    Clock
		bindings []ClientCertificateIdentityBinding
	}{
		{name: "nil clock", bindings: []ClientCertificateIdentityBinding{valid}},
		{name: "empty bindings", clock: clock},
		{name: "unknown kind", clock: clock, bindings: []ClientCertificateIdentityBinding{{Kind: "uriSan", Value: "device", TenantID: valid.TenantID, Subject: "subject", CredentialID: "credential", AllowedClientIDs: []string{"client"}, Roles: []security.Role{"publisher"}}}},
		{name: "empty value", clock: clock, bindings: []ClientCertificateIdentityBinding{{Kind: valid.Kind, TenantID: valid.TenantID, Subject: "subject", CredentialID: "credential", AllowedClientIDs: []string{"client"}, Roles: []security.Role{"publisher"}}}},
		{name: "empty tenant", clock: clock, bindings: []ClientCertificateIdentityBinding{{Kind: valid.Kind, Value: "device", Subject: "subject", CredentialID: "credential", AllowedClientIDs: []string{"client"}, Roles: []security.Role{"publisher"}}}},
		{name: "empty subject", clock: clock, bindings: []ClientCertificateIdentityBinding{{Kind: valid.Kind, Value: "device", TenantID: valid.TenantID, CredentialID: "credential", AllowedClientIDs: []string{"client"}, Roles: []security.Role{"publisher"}}}},
		{name: "empty credential", clock: clock, bindings: []ClientCertificateIdentityBinding{{Kind: valid.Kind, Value: "device", TenantID: valid.TenantID, Subject: "subject", AllowedClientIDs: []string{"client"}, Roles: []security.Role{"publisher"}}}},
		{name: "empty clients", clock: clock, bindings: []ClientCertificateIdentityBinding{{Kind: valid.Kind, Value: "device", TenantID: valid.TenantID, Subject: "subject", CredentialID: "credential", Roles: []security.Role{"publisher"}}}},
		{name: "empty client element", clock: clock, bindings: []ClientCertificateIdentityBinding{{Kind: valid.Kind, Value: "device", TenantID: valid.TenantID, Subject: "subject", CredentialID: "credential", AllowedClientIDs: []string{""}, Roles: []security.Role{"publisher"}}}},
		{name: "duplicate client", clock: clock, bindings: []ClientCertificateIdentityBinding{{Kind: valid.Kind, Value: "device", TenantID: valid.TenantID, Subject: "subject", CredentialID: "credential", AllowedClientIDs: []string{"client", "client"}, Roles: []security.Role{"publisher"}}}},
		{name: "empty roles", clock: clock, bindings: []ClientCertificateIdentityBinding{{Kind: valid.Kind, Value: "device", TenantID: valid.TenantID, Subject: "subject", CredentialID: "credential", AllowedClientIDs: []string{"client"}}}},
		{name: "empty role element", clock: clock, bindings: []ClientCertificateIdentityBinding{{Kind: valid.Kind, Value: "device", TenantID: valid.TenantID, Subject: "subject", CredentialID: "credential", AllowedClientIDs: []string{"client"}, Roles: []security.Role{""}}}},
		{name: "duplicate role", clock: clock, bindings: []ClientCertificateIdentityBinding{{Kind: valid.Kind, Value: "device", TenantID: valid.TenantID, Subject: "subject", CredentialID: "credential", AllowedClientIDs: []string{"client"}, Roles: []security.Role{"publisher", "publisher"}}}},
		{name: "duplicate selector", clock: clock, bindings: []ClientCertificateIdentityBinding{valid, valid}},
		{name: "duplicate credential", clock: clock, bindings: []ClientCertificateIdentityBinding{valid, newMTLSTestBinding(t, ClientCertificateIdentityKindDNSSAN, "device.example", valid.CredentialID)}},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			if testCase.clock == nil {
				if _, err := NewMTLSAuthenticationProvider(nil, testCase.bindings); err == nil {
					t.Fatal("constructor accepted nil clock")
				}
				return
			}
			if _, err := NewMTLSAuthenticationProvider(testCase.clock, testCase.bindings); err == nil {
				t.Fatal("constructor accepted invalid binding")
			}
		})
	}
}
