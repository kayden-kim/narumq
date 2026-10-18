package connect

import (
	"context"
	"testing"
	"time"

	applicationauthentication "narumq/internal/application/ports/authentication"
	portsmetrics "narumq/internal/application/ports/metrics"
	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/security"
	"narumq/internal/domain/tenancy"
)

type connectBlocklistMatcher struct {
	match    func(security.BlockSubject) bool
	kind     security.BlockKind
	subjects []security.BlockSubject
}

func (matcher *connectBlocklistMatcher) MatchBlocklist(subject security.BlockSubject) (security.BlockKind, bool) {
	matcher.subjects = append(matcher.subjects, subject)
	if matcher.match == nil || !matcher.match(subject) {
		return security.BlockKindUnspecified, false
	}
	return matcher.kind, true
}

// Traceability: SEC-12; docs/specs/tenancy-and-security.md §7.1, §9; ADR-0050.
// Scenarios: tenancy-and-security.md:SEC-SCN-041
func TestConnectBlocklistMatcherIsCheckedPreAuthentication(t *testing.T) {
	now := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	for _, version := range []mqtt.ProtocolVersion{mqtt.ProtocolV311, mqtt.ProtocolV5} {
		versionName := "MQTT 3.1.1"
		if version == mqtt.ProtocolV5 {
			versionName = "MQTT 5"
		}
		t.Run(versionName, func(t *testing.T) {
			tenant, err := tenancy.NewTenantID("tenant-a")
			if err != nil {
				t.Fatal(err)
			}
			authenticator := &fakeAuthenticator{result: applicationauthentication.Result{TenantID: tenant}}
			certificateAuthenticator := &fakeCertificateAuthenticator{}
			metrics := &fakeMetrics{}
			useCase, repository, _ := newSecurityTestUseCase(
				t, now, authenticator, certificateAuthenticator, security.Policy{}, portsmetrics.AuthMethodJWT, metrics,
			)
			matcher := &connectBlocklistMatcher{kind: security.BlockKindIPPrefix, match: func(security.BlockSubject) bool { return true }}
			WithBlocklistMatcher(matcher)(useCase)
			coordinator := &fakeOwnershipCoordinator{epoch: 7}
			WithOwnershipCoordinator(coordinator, 7)(useCase)
			audit := &connectDataPlaneAuditRecorder{}
			WithDataPlaneAudit(audit)(useCase)

			request := ConnectRequestDTO{
				Packet: mqtt.Connect{
					Version: version, ClientID: "client-a", HasUsername: true, Username: "blocked-user",
					HasPassword: true, Password: []byte("secret"),
				},
				ClientCertificate: &applicationauthentication.ClientCertificateDTO{SHA256: "0123456789abcdef"},
				SourceIP:          "192.0.2.9",
			}
			result, err := useCase.ConnectContext(context.Background(), request)
			if err != nil || result.Accepted || result.ConnAck.ReasonCode != bannedReason(version) {
				t.Fatalf("blocked CONNECT = %+v, %v; want banned reason %#x", result, err, bannedReason(version))
			}
			wantSubject := security.BlockSubject{
				HasUsername: true, Username: "blocked-user", CertificateSHA256: "0123456789abcdef", SourceIP: "192.0.2.9",
			}
			if len(matcher.subjects) != 1 || matcher.subjects[0] != wantSubject {
				t.Fatalf("pre-auth matcher subjects = %+v; want [%+v]", matcher.subjects, wantSubject)
			}
			if authenticator.calls != 0 || certificateAuthenticator.calls != 0 || coordinator.calls != 0 || repository.loadCalls != 0 {
				t.Fatalf("pre-auth rejection reached downstream work: auth=%d certificate=%d ownership=%d loads=%d",
					authenticator.calls, certificateAuthenticator.calls, coordinator.calls, repository.loadCalls)
			}
			if len(metrics.attempts) != 1 || metrics.attempts[0] != (authAttempt{method: portsmetrics.AuthMethodTLS, reason: "blocked"}) {
				t.Fatalf("blocked auth metric = %+v", metrics.attempts)
			}
			if len(audit.denials) != 1 || audit.denials[0].Reason != "blocked" || audit.denials[0].AuthMethod != portsmetrics.AuthMethodTLS ||
				audit.denials[0].ClientID != "client-a" || audit.denials[0].SourceIP != "192.0.2.9" || audit.denials[0].TenantID != "" {
				t.Fatalf("pre-auth block audit = %+v", audit.denials)
			}
		})
	}
}

// Traceability: SEC-12; docs/specs/tenancy-and-security.md §7.1, §9; ADR-0050.
// Scenarios: tenancy-and-security.md:SEC-SCN-041
func TestConnectBlocklistRejectsBeforeAuthenticationAndOwnership(t *testing.T) {
	now := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	const fingerprint = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	for _, blocked := range []struct {
		name  string
		kind  security.BlockKind
		value string
	}{
		{name: "source IP", kind: security.BlockKindIPPrefix, value: "192.0.2.0/24"},
		{name: "username", kind: security.BlockKindUsername, value: "blocked-user"},
		{name: "certificate", kind: security.BlockKindCertificateSHA256, value: fingerprint},
	} {
		for _, version := range []mqtt.ProtocolVersion{mqtt.ProtocolV311, mqtt.ProtocolV5} {
			versionName := "MQTT 3.1.1"
			if version == mqtt.ProtocolV5 {
				versionName = "MQTT 5"
			}
			t.Run(blocked.name+"/"+versionName, func(t *testing.T) {
				entry, err := security.NewBlockEntry(blocked.kind, "", blocked.value)
				if err != nil {
					t.Fatal(err)
				}
				index := security.NewBlocklist()
				index.Set(entry, true)
				tenant, err := tenancy.NewTenantID("tenant-a")
				if err != nil {
					t.Fatal(err)
				}
				authenticator := &fakeAuthenticator{result: applicationauthentication.Result{TenantID: tenant}}
				certificateAuthenticator := &fakeCertificateAuthenticator{}
				metrics := &fakeMetrics{}
				useCase, repository, _ := newSecurityTestUseCase(
					t, now, authenticator, certificateAuthenticator, security.Policy{}, portsmetrics.AuthMethodJWT, metrics,
				)
				matcher := &connectBlocklistMatcher{
					kind: blocked.kind,
					match: func(subject security.BlockSubject) bool {
						kind, found := index.Match(subject)
						return found && kind == blocked.kind
					},
				}
				WithBlocklistMatcher(matcher)(useCase)
				coordinator := &fakeOwnershipCoordinator{epoch: 7}
				WithOwnershipCoordinator(coordinator, 7)(useCase)
				audit := &connectDataPlaneAuditRecorder{}
				WithDataPlaneAudit(audit)(useCase)

				request := ConnectRequestDTO{
					Packet: mqtt.Connect{
						Version: version, ClientID: "client-a", HasUsername: true, Username: "blocked-user",
						HasPassword: true, Password: []byte("secret"),
					},
					SourceIP: "192.0.2.9",
				}
				wantFingerprint := ""
				if blocked.kind == security.BlockKindCertificateSHA256 {
					request.ClientCertificate = &applicationauthentication.ClientCertificateDTO{SHA256: fingerprint}
					wantFingerprint = fingerprint
				}
				result, err := useCase.ConnectContext(context.Background(), request)
				if err != nil || result.Accepted || result.ConnAck.ReasonCode != bannedReason(version) {
					t.Fatalf("blocked CONNECT = %+v, %v; want banned reason %#x", result, err, bannedReason(version))
				}
				if authenticator.calls != 0 || certificateAuthenticator.calls != 0 ||
					coordinator.calls != 0 || repository.loadCalls != 0 {
					t.Fatalf("pre-auth block reached downstream work: auth=%d certificate=%d ownership=%d loads=%d",
						authenticator.calls, certificateAuthenticator.calls, coordinator.calls, repository.loadCalls)
				}
				if len(matcher.subjects) != 1 || matcher.subjects[0].SourceIP != "192.0.2.9" ||
					!matcher.subjects[0].HasUsername || matcher.subjects[0].Username != "blocked-user" ||
					matcher.subjects[0].CertificateSHA256 != wantFingerprint {
					t.Fatalf("pre-auth matcher subjects = %+v", matcher.subjects)
				}
				if len(metrics.attempts) != 1 || metrics.attempts[0].reason != "blocked" {
					t.Fatalf("blocked auth metric = %+v", metrics.attempts)
				}
				if len(audit.denials) != 1 || audit.denials[0].Reason != "blocked" {
					t.Fatalf("blocked audit = %+v", audit.denials)
				}
			})
		}
	}
}

// Traceability: SEC-12; docs/specs/tenancy-and-security.md §7.1, §9; ADR-0050.
// Scenarios: tenancy-and-security.md:SEC-SCN-041
func TestConnectBlocklistClientIDIsTenantScoped(t *testing.T) {
	now := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	tenant, err := tenancy.NewTenantID("tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	authenticator := &fakeAuthenticator{result: applicationauthentication.Result{
		TenantID: tenant, Subject: "device-subject", CredentialID: "credential-a",
		AllowedClientIDs: []string{"client-a"}, Roles: []security.Role{"publisher"}, ExpiresAt: now.Add(time.Hour),
	}}
	useCase, repository, _ := newSecurityTestUseCase(
		t, now, authenticator, nil, security.Policy{}, portsmetrics.AuthMethodJWT, nil,
	)
	matcher := &connectBlocklistMatcher{
		kind: security.BlockKindClientID,
		match: func(subject security.BlockSubject) bool {
			return subject.TenantID == "tenant-a" && subject.ClientID == "client-a"
		},
	}
	WithBlocklistMatcher(matcher)(useCase)
	coordinator := &fakeOwnershipCoordinator{epoch: 7}
	WithOwnershipCoordinator(coordinator, 7)(useCase)
	audit := &connectDataPlaneAuditRecorder{}
	WithDataPlaneAudit(audit)(useCase)

	result, err := useCase.ConnectContext(context.Background(), ConnectRequestDTO{Packet: mqtt.Connect{
		Version: mqtt.ProtocolV5, ClientID: "client-a", HasUsername: true, Username: "login",
		HasPassword: true, Password: []byte("secret"),
	}})
	if err != nil || result.Accepted || result.ConnAck.ReasonCode != 0x8A {
		t.Fatalf("tenant-blocked CONNECT = %+v, %v; want MQTT 5 banned reason", result, err)
	}
	if authenticator.calls != 1 || coordinator.calls != 0 || repository.loadCalls != 0 {
		t.Fatalf("authenticated block reached wrong stages: auth=%d ownership=%d loads=%d",
			authenticator.calls, coordinator.calls, repository.loadCalls)
	}
	wantSubject := security.BlockSubject{TenantID: "tenant-a", ClientID: "client-a"}
	if len(matcher.subjects) != 2 || matcher.subjects[1] != wantSubject {
		t.Fatalf("tenant matcher subjects = %+v; want authenticated subject %+v", matcher.subjects, wantSubject)
	}
	if len(audit.denials) != 1 {
		t.Fatalf("authenticated block audit count = %d", len(audit.denials))
	}
	denial := audit.denials[0]
	if denial.Reason != "blocked" || denial.AuthMethod != portsmetrics.AuthMethodJWT || denial.TenantID != "tenant-a" ||
		denial.Subject != "device-subject" || denial.CredentialID != "credential-a" || len(denial.Roles) != 1 || denial.Roles[0] != "publisher" {
		t.Fatalf("authenticated block audit = %+v", denial)
	}
}

// Traceability: SEC-12; docs/specs/tenancy-and-security.md §7.1, §9; ADR-0050.
// Scenarios: tenancy-and-security.md:SEC-SCN-041
func TestConnectBlocklistAllowsSameClientIDInAnotherTenant(t *testing.T) {
	now := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	tenant, err := tenancy.NewTenantID("tenant-b")
	if err != nil {
		t.Fatal(err)
	}
	authenticator := &fakeAuthenticator{result: applicationauthentication.Result{
		TenantID: tenant, Subject: "device-b", CredentialID: "credential-b",
		AllowedClientIDs: []string{"client-a"}, Roles: []security.Role{"publisher"}, ExpiresAt: now.Add(time.Hour),
	}}
	useCase, _, _ := newSecurityTestUseCase(
		t, now, authenticator, nil, security.Policy{}, portsmetrics.AuthMethodJWT, nil,
	)
	matcher := &connectBlocklistMatcher{
		kind: security.BlockKindClientID,
		match: func(subject security.BlockSubject) bool {
			return subject.TenantID == "tenant-a" && subject.ClientID == "client-a"
		},
	}
	WithBlocklistMatcher(matcher)(useCase)
	coordinator := &fakeOwnershipCoordinator{epoch: 7}
	WithOwnershipCoordinator(coordinator, 7)(useCase)
	result, err := useCase.ConnectContext(context.Background(), ConnectRequestDTO{Packet: mqtt.Connect{
		Version: mqtt.ProtocolV5, ClientID: "client-a", HasUsername: true, Username: "login",
		HasPassword: true, Password: []byte("secret"),
	}})
	if err != nil || !result.Accepted {
		t.Fatalf("same client ID in tenant-b = %+v, %v; want accepted", result, err)
	}
	if coordinator.calls != 1 || authenticator.calls != 1 || len(matcher.subjects) != 2 ||
		matcher.subjects[1] != (security.BlockSubject{TenantID: "tenant-b", ClientID: "client-a"}) {
		t.Fatalf("cross-tenant client-ID admission: ownership=%d auth=%d subjects=%+v",
			coordinator.calls, authenticator.calls, matcher.subjects)
	}
}

// Traceability: SEC-12; docs/specs/tenancy-and-security.md §7.1, §9; ADR-0050.
// Scenarios: tenancy-and-security.md:SEC-SCN-041
func TestBlocklistConnectUsesDefaultTenantOnlyWithClusterOwnership(t *testing.T) {
	now := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	matchDefault := func(subject security.BlockSubject) bool {
		return subject.TenantID == "default" && subject.ClientID == "anonymous-client"
	}

	t.Run("cluster", func(t *testing.T) {
		useCase, _, repository, _, _, _, _ := newUseCase(t, now)
		matcher := &connectBlocklistMatcher{kind: security.BlockKindClientID, match: matchDefault}
		WithBlocklistMatcher(matcher)(useCase)
		coordinator := &fakeOwnershipCoordinator{epoch: 7}
		WithOwnershipCoordinator(coordinator, 7)(useCase)
		result, err := useCase.ConnectContext(context.Background(), ConnectRequestDTO{Packet: mqtt.Connect{
			Version: mqtt.ProtocolV311, ClientID: "anonymous-client",
		}})
		if err != nil || result.Accepted || result.ConnAck.ReasonCode != 0x05 {
			t.Fatalf("anonymous block = %+v, %v; want MQTT 3.1.1 not-authorized reason", result, err)
		}
		if len(matcher.subjects) != 2 || matcher.subjects[1] != (security.BlockSubject{TenantID: "default", ClientID: "anonymous-client"}) ||
			coordinator.calls != 0 || repository.loadCalls != 0 {
			t.Fatalf("cluster anonymous block subjects=%+v ownership=%d loads=%d", matcher.subjects, coordinator.calls, repository.loadCalls)
		}
	})

	t.Run("standalone", func(t *testing.T) {
		useCase, _, _, _, _, _, _ := newUseCase(t, now)
		matcher := &connectBlocklistMatcher{kind: security.BlockKindClientID, match: matchDefault}
		WithBlocklistMatcher(matcher)(useCase)
		result, err := useCase.ConnectContext(context.Background(), ConnectRequestDTO{Packet: mqtt.Connect{
			Version: mqtt.ProtocolV5, ClientID: "anonymous-client",
		}})
		if err != nil || !result.Accepted || len(matcher.subjects) != 1 || matcher.subjects[0].TenantID != "" || matcher.subjects[0].ClientID != "" {
			t.Fatalf("standalone anonymous CONNECT = %+v, %v; matcher subjects=%+v", result, err, matcher.subjects)
		}
	})
}
