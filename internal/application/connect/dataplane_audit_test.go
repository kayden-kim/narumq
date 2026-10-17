package connect

import (
	"context"
	"errors"
	applicationauthentication "narumq/internal/application/ports/authentication"
	"reflect"
	"testing"
	"time"

	"narumq/internal/application/membership"
	portsaudit "narumq/internal/application/ports/audit"
	portsmetrics "narumq/internal/application/ports/metrics"
	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/security"
	"narumq/internal/domain/session"
	"narumq/internal/domain/tenancy"
)

type connectDataPlaneAuditRecorder struct {
	denials []portsaudit.DataPlaneDenialDTO
}

func (recorder *connectDataPlaneAuditRecorder) RecordDataPlaneDenial(denial portsaudit.DataPlaneDenialDTO) {
	recorder.denials = append(recorder.denials, denial)
}

// Traceability: SEC-05/SEC-11/NFR-17; docs/specs/tenancy-and-security.md §2.3, §2.4, §8, §9.
// Scenarios: configuration-and-runtime-options.md:CFG-SCN-29, tenancy-and-security.md:SEC-SCN-014, tenancy-and-security.md:SEC-SCN-047
func TestConnectRecordsAuthenticationDenialsWithoutChangingConnAck(t *testing.T) {
	now := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		name             string
		packet           mqtt.Connect
		certificate      bool
		authErr          error
		credentialMethod string
		wantReason       string
		wantMethod       string
	}{
		{
			name:    "invalid credentials",
			packet:  mqtt.Connect{Version: mqtt.ProtocolV5, ClientID: "client-a", HasUsername: true, HasPassword: true, Password: []byte("bad")},
			authErr: errors.New("invalid token"), wantReason: "invalid_credentials", wantMethod: portsmetrics.AuthMethodJWT,
		},
		{
			name:       "missing credentials",
			packet:     mqtt.Connect{Version: mqtt.ProtocolV5, ClientID: "client-a"},
			wantReason: "missing_credentials", wantMethod: portsmetrics.AuthMethodJWT,
		},
		{
			name: "password invalid credentials",
			packet: mqtt.Connect{Version: mqtt.ProtocolV5, ClientID: "client-a", HasUsername: true, Username: "unknown",
				HasPassword: true, Password: []byte("password-not-for-logs")},
			authErr: errors.New("invalid credentials"), wantReason: "invalid_credentials",
			wantMethod: portsmetrics.AuthMethodPassword, credentialMethod: portsmetrics.AuthMethodPassword,
		},
		{
			name:        "dual credentials",
			packet:      mqtt.Connect{Version: mqtt.ProtocolV5, ClientID: "client-a", HasUsername: true, Username: "password-user", HasPassword: true, Password: []byte("password")},
			certificate: true, wantReason: "dual_credentials", wantMethod: portsmetrics.AuthMethodTLS,
			credentialMethod: portsmetrics.AuthMethodPassword,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			method := test.credentialMethod
			if method == "" {
				method = portsmetrics.AuthMethodJWT
			}
			makeUseCase := func(recorder portsaudit.DataPlaneDenialRecorder) *UseCase {
				credentialProvider := &fakeAuthenticator{err: test.authErr}
				mtls := &fakeCertificateAuthenticator{}
				useCase, _, _, _, _, _, _ := newUseCase(t, now)
				WithSecurity(credentialProvider, mtls, security.Policy{}, method)(useCase)
				if recorder != nil {
					WithDataPlaneAudit(recorder)(useCase)
				}
				return useCase
			}
			certificate := (*applicationauthentication.ClientCertificateDTO)(nil)
			if test.certificate {
				certificate = &applicationauthentication.ClientCertificateDTO{CommonName: "device", NotAfter: now.Add(time.Hour)}
			}
			request := ConnectRequestDTO{Packet: test.packet, ClientCertificate: certificate, SourceIP: "203.0.113.9"}
			plain, err := makeUseCase(nil).ConnectContext(context.Background(), request)
			if err != nil || plain.Accepted || plain.ConnAck.ReasonCode != 0x87 {
				t.Fatalf("CONNECT without recorder = %+v, %v; want unchanged MQTT 5 not-authorized CONNACK", plain, err)
			}

			recorder := &connectDataPlaneAuditRecorder{}
			result, err := makeUseCase(recorder).ConnectContext(context.Background(), request)
			if err != nil || result.Accepted || !reflect.DeepEqual(result.ConnAck, plain.ConnAck) {
				t.Fatalf("CONNECT with recorder = %+v, %v; plain CONNACK = %+v", result, err, plain.ConnAck)
			}
			if len(recorder.denials) != 1 {
				t.Fatalf("recorded denials = %d, want 1: %+v", len(recorder.denials), recorder.denials)
			}
			denial := recorder.denials[0]
			if denial.Action != portsaudit.DataPlaneActionConnect || denial.Reason != test.wantReason || denial.Subject != "" ||
				len(denial.Roles) != 0 || denial.ClientID != "client-a" || denial.SourceIP != "203.0.113.9" || denial.AuthMethod != test.wantMethod {
				t.Fatalf("authentication denial = %+v", denial)
			}
		})
	}
}

// Traceability: SEC-05/SEC-11/NFR-17; docs/specs/tenancy-and-security.md §2.3, §2.4, §8, §9.
// Scenarios: tenancy-and-security.md:SEC-SCN-047
func TestConnectRecordsAuthenticatedClientAndWillDenials(t *testing.T) {
	now := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	tenantID, err := tenancy.NewTenantID("tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name         string
		clientID     string
		wantClientID string
		willTopic    string
		wantReason   string
		wantTopic    string
		username     string
		authMethod   string
	}{
		{name: "client id not allowed", clientID: "other-client", wantClientID: "other-client", wantReason: "client_id_not_allowed", authMethod: portsmetrics.AuthMethodJWT},
		{name: "will denied", clientID: "client-a", wantClientID: "client-a", username: "password-user", authMethod: portsmetrics.AuthMethodPassword, willTopic: "private/will", wantReason: "will_unauthorized", wantTopic: "private/will"},
	} {
		t.Run(test.name, func(t *testing.T) {
			identity := applicationauthentication.Result{
				TenantID: tenantID, Subject: "device-a", CredentialID: "credential-a",
				AllowedClientIDs: []string{"client-a"}, Roles: []security.Role{"publisher"}, ExpiresAt: now.Add(time.Hour),
			}
			useCase, _, _, _, _, _, _ := newUseCase(t, now)
			WithSecurity(&fakeAuthenticator{result: identity}, nil, security.Policy{}, test.authMethod)(useCase)
			recorder := &connectDataPlaneAuditRecorder{}
			WithDataPlaneAudit(recorder)(useCase)
			packet := mqtt.Connect{Version: mqtt.ProtocolV5, ClientID: test.clientID, HasUsername: true, Username: test.username,
				HasPassword: true, Password: []byte("credential-not-for-audit")}
			if test.willTopic != "" {
				packet.ClientID = "client-a"
				packet.Will = metadataWill(test.willTopic, 0)
			}
			result, err := useCase.ConnectContext(context.Background(), ConnectRequestDTO{Packet: packet, SourceIP: "198.51.100.8"})
			if err != nil || result.Accepted || result.ConnAck.ReasonCode != 0x87 {
				t.Fatalf("CONNECT = %+v, %v; want MQTT 5 not-authorized CONNACK", result, err)
			}
			if len(recorder.denials) != 1 {
				t.Fatalf("recorded denials = %d, want 1: %+v", len(recorder.denials), recorder.denials)
			}
			denial := recorder.denials[0]
			if denial.Action != portsaudit.DataPlaneActionConnect || denial.Reason != test.wantReason || denial.TenantID != "tenant-a" ||
				denial.Subject != "device-a" || denial.CredentialID != "credential-a" || !reflect.DeepEqual(denial.Roles, []string{"publisher"}) ||
				denial.ClientID != test.wantClientID || denial.SourceIP != "198.51.100.8" || denial.AuthMethod != test.authMethod || denial.Topic != test.wantTopic {
				t.Fatalf("authenticated denial = %+v", denial)
			}
		})
	}
}

// Traceability: SEC-11/NFR-17; docs/specs/tenancy-and-security.md §8.
// Scenarios: tenancy-and-security.md:SEC-SCN-047
func TestConnectSuccessDoesNotRecordDenial(t *testing.T) {
	now := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	tenantID, err := tenancy.NewTenantID("tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	useCase, _, _, _, _, _, _ := newUseCase(t, now)
	WithSecurity(&fakeAuthenticator{result: applicationauthentication.Result{
		TenantID: tenantID, Subject: "device-a", CredentialID: "credential-a",
		AllowedClientIDs: []string{"client-a"}, Roles: []security.Role{"publisher"}, ExpiresAt: now.Add(time.Hour),
	}}, nil, security.Policy{}, "jwt")(useCase)
	recorder := &connectDataPlaneAuditRecorder{}
	WithDataPlaneAudit(recorder)(useCase)
	result, err := useCase.ConnectContext(context.Background(), ConnectRequestDTO{
		Packet:   mqtt.Connect{Version: mqtt.ProtocolV5, ClientID: "client-a", HasUsername: true, HasPassword: true, Password: []byte("valid-token")},
		SourceIP: "198.51.100.8",
	})
	if err != nil || !result.Accepted || result.ConnAck.ReasonCode != 0 {
		t.Fatalf("successful CONNECT = %+v, %v", result, err)
	}
	if len(recorder.denials) != 0 {
		t.Fatalf("successful CONNECT recorded denials: %+v", recorder.denials)
	}
}

// Traceability: SEC-11/NFR-17; docs/specs/tenancy-and-security.md §8.
// Scenarios: tenancy-and-security.md:SEC-SCN-047
func TestDelayedWillAuthorizationDenialRecordsWithoutSourceIP(t *testing.T) {
	now := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	tenantID, err := tenancy.NewTenantID("tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	filter, err := tenancy.NewTopicFilter(tenantID, "sensors/#")
	if err != nil {
		t.Fatal(err)
	}
	rule, err := security.NewRule(tenantID, "publisher", security.EffectAllow, security.ActionWill, filter)
	if err != nil {
		t.Fatal(err)
	}
	policy, err := security.NewPolicy([]security.Rule{rule})
	if err != nil {
		t.Fatal(err)
	}
	useCase, _, _, _, _, scheduler, _ := newUseCase(t, now)
	WithSecurity(&fakeAuthenticator{result: applicationauthentication.Result{
		TenantID: tenantID, Subject: "device-a", CredentialID: "credential-a",
		AllowedClientIDs: []string{"client-a"}, Roles: []security.Role{"publisher"}, ExpiresAt: now.Add(time.Hour),
	}}, nil, policy, "jwt")(useCase)
	recorder := &connectDataPlaneAuditRecorder{}
	WithDataPlaneAudit(recorder)(useCase)
	result, err := useCase.ConnectContext(context.Background(), ConnectRequestDTO{
		Packet: mqtt.Connect{
			Version: mqtt.ProtocolV5, ClientID: "client-a", HasUsername: true, HasPassword: true,
			Password: []byte("valid-token"), SessionExpiryInterval: 60, Will: metadataWill("sensors/late", 5),
		},
		SourceIP: "198.51.100.8",
	})
	if err != nil || !result.Accepted {
		t.Fatalf("initial CONNECT = %+v, %v; want accepted before policy change", result, err)
	}
	useCase.policy = security.Policy{}
	if err := useCase.Disconnect(result.SessionID, session.DisconnectAbnormal); err != nil {
		t.Fatalf("Disconnect() = %v", err)
	}
	if len(scheduler.timers) == 0 {
		t.Fatal("delayed Will timer was not scheduled")
	}
	scheduler.timers[0].callback()
	if len(recorder.denials) != 1 {
		t.Fatalf("recorded denials = %d, want 1: %+v", len(recorder.denials), recorder.denials)
	}
	denial := recorder.denials[0]
	if denial.Action != portsaudit.DataPlaneActionWill || denial.Reason != portsaudit.DataPlaneReasonNotAuthorized ||
		denial.TenantID != "tenant-a" || denial.Subject != "device-a" || denial.CredentialID != "credential-a" ||
		!reflect.DeepEqual(denial.Roles, []string{"publisher"}) || denial.ClientID != "client-a" || denial.Topic != "sensors/late" || denial.SourceIP != "" {
		t.Fatalf("delayed Will denial = %+v", denial)
	}
}

type connectRevocationCheckerStub struct {
	revoked bool
	err     error
	checked []string
}

func (checker *connectRevocationCheckerStub) IsCredentialRevoked(_ context.Context, credentialID string) (bool, error) {
	checker.checked = append(checker.checked, credentialID)
	return checker.revoked, checker.err
}

// Traceability: SEC-07; docs/specs/tenancy-and-security.md §7.
// Scenarios: tenancy-and-security.md:SEC-SCN-035, tenancy-and-security.md:SEC-SCN-037
func TestCredentialRevocationSuppressesImmediateAndDelayedWills(t *testing.T) {
	now := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	tenant, err := tenancy.NewTenantID("tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name       string
		delay      uint32
		revoked    bool
		checkErr   error
		wantAudits int
	}{
		{name: "immediate revoked", revoked: true, wantAudits: 1},
		{name: "delayed revoked", delay: 5, revoked: true, wantAudits: 1},
		{name: "checker unavailable", revoked: false, checkErr: errors.New("quorum unavailable")},
	} {
		t.Run(test.name, func(t *testing.T) {
			useCase, _, _, _, _, scheduler, publisher := newUseCase(t, now)
			WithSecurity(&fakeAuthenticator{result: cmmAuthResult(tenant, now, "client")}, nil, cmmWillPolicy(t, tenant), "jwt")(useCase)
			checker := &connectRevocationCheckerStub{}
			WithCredentialRevocationChecker(checker)(useCase)
			auditRecorder := &connectDataPlaneAuditRecorder{}
			WithDataPlaneAudit(auditRecorder)(useCase)

			result, err := useCase.ConnectContext(context.Background(), ConnectRequestDTO{
				Packet: cmmJWTConnect("client", 60, metadataWill("sensors/late", test.delay)),
			})
			if err != nil || !result.Accepted {
				t.Fatalf("CONNECT = %+v, %v; want accepted", result, err)
			}
			checker.revoked = test.revoked
			checker.err = test.checkErr
			if err := useCase.Disconnect(result.SessionID, session.DisconnectAbnormal); err != nil {
				t.Fatalf("Disconnect() = %v", err)
			}
			if test.delay != 0 {
				if len(scheduler.timers) == 0 {
					t.Fatal("delayed Will timer was not scheduled")
				}
				scheduler.timers[0].callback()
			}
			if len(checker.checked) != 2 || checker.checked[0] != "cred-1" || checker.checked[1] != "cred-1" {
				t.Fatalf("credential checks = %v; want CONNECT and Will checks for [cred-1]", checker.checked)
			}
			if len(publisher.published) != 0 {
				t.Fatalf("revoked or uncertain credential published Will: %+v", publisher.published)
			}
			if len(auditRecorder.denials) != test.wantAudits {
				t.Fatalf("Will denial audits = %+v; want %d", auditRecorder.denials, test.wantAudits)
			}
			if test.wantAudits == 1 {
				denial := auditRecorder.denials[0]
				if denial.Action != portsaudit.DataPlaneActionWill || denial.Reason != "credential_revoked" ||
					denial.CredentialID != "cred-1" || denial.Topic != "sensors/late" {
					t.Fatalf("credential revocation denial = %+v", denial)
				}
			}
		})
	}
}

type credentialRevocationOwnershipCoordinatorStub struct {
	calls      int
	superseded *membership.SupersededWillDTO
}

func (coordinator *credentialRevocationOwnershipCoordinatorStub) AcquireOwnership(
	context.Context, string, string, uint64,
) (uint64, error) {
	coordinator.calls++
	return 7, nil
}

func (coordinator *credentialRevocationOwnershipCoordinatorStub) AcquireSessionOwnership(
	context.Context, SessionOwnershipRequestDTO,
) (SessionOwnershipResultDTO, error) {
	coordinator.calls++
	return SessionOwnershipResultDTO{Epoch: 7, SupersededWill: coordinator.superseded}, nil
}

// Traceability: SEC-07; docs/specs/tenancy-and-security.md §7, §9.
// Scenarios: tenancy-and-security.md:SEC-SCN-034
func TestCredentialRevocationRejectsBeforeOwnershipSideEffects(t *testing.T) {
	now := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	tenant, err := tenancy.NewTenantID("tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name       string
		version    mqtt.ProtocolVersion
		clientID   string
		revoked    bool
		checkErr   error
		wantReason byte
		wantAudit  string
	}{
		{name: "revoked MQTT 3.1.1", version: mqtt.ProtocolV311, revoked: true, wantReason: 0x05, wantAudit: "credential_revoked"},
		{name: "revoked MQTT 5", version: mqtt.ProtocolV5, revoked: true, wantReason: 0x87, wantAudit: "credential_revoked"},
		{name: "checker unavailable MQTT 3.1.1", version: mqtt.ProtocolV311, checkErr: errors.New("quorum unavailable"), wantReason: 0x03},
		{name: "checker unavailable MQTT 5", version: mqtt.ProtocolV5, checkErr: errors.New("quorum unavailable"), wantReason: 0x88},
		{name: "revoked before allowed Client ID check", version: mqtt.ProtocolV5, clientID: "not-allowed", revoked: true, wantReason: 0x87, wantAudit: "credential_revoked"},
	} {
		t.Run(test.name, func(t *testing.T) {
			useCase, _, repository, queue, _, scheduler, publisher := newUseCase(t, now)
			authResult := cmmAuthResult(tenant, now, "client-a")
			WithSecurity(&fakeAuthenticator{result: authResult}, nil, cmmWillPolicy(t, tenant), "jwt")(useCase)
			superseded := &membership.SupersededWillDTO{
				Will: *metadataWillDTO(metadataWill("sensors/old", 0)),
				Principal: &membership.SessionPrincipalDTO{
					Subject: "previous-device", CredentialID: "previous-credential",
					Roles: []string{"publisher"}, ExpiresAtUnixMillis: uint64(now.Add(time.Hour).UnixMilli()),
				},
				ProtocolVersion: 5, Retention: membership.SessionRetentionInfinite, PlannedHandoff: true,
			}
			coordinator := &credentialRevocationOwnershipCoordinatorStub{superseded: superseded}
			WithOwnershipCoordinator(coordinator, 7)(useCase)
			checker := &connectRevocationCheckerStub{revoked: test.revoked, err: test.checkErr}
			WithCredentialRevocationChecker(checker)(useCase)
			auditRecorder := &connectDataPlaneAuditRecorder{}
			WithDataPlaneAudit(auditRecorder)(useCase)

			clientID := test.clientID
			if clientID == "" {
				clientID = "client-a"
			}
			packet := mqtt.Connect{
				Version: test.version, ClientID: clientID, SessionExpiryInterval: 60,
				HasUsername: true, Username: "device", HasPassword: true, Password: []byte("credential-not-for-logs"),
				Will: metadataWill("sensors/new", 0),
			}
			result, err := useCase.ConnectContext(context.Background(), ConnectRequestDTO{Packet: packet})
			if err != nil || result.Accepted || result.ConnAck.ReasonCode != test.wantReason || result.OwnershipEpoch != 0 {
				t.Fatalf("CONNECT = %+v, %v; want rejection reason 0x%02x before ownership", result, err, test.wantReason)
			}
			if len(checker.checked) != 1 || checker.checked[0] != "cred-1" {
				t.Fatalf("credential checks = %v, want [cred-1]", checker.checked)
			}
			if coordinator.calls != 0 {
				t.Fatalf("ownership coordinator calls = %d, want 0", coordinator.calls)
			}
			if repository.loadCalls != 0 || repository.saveCalls != 0 || repository.deleteCall != 0 ||
				len(repository.states) != 0 || len(repository.keyedStates) != 0 || len(queue.registers) != 0 ||
				len(queue.removes) != 0 || len(scheduler.timers) != 0 || len(publisher.published) != 0 {
				t.Fatalf("rejected CONNECT changed state: loads=%d saves=%d deletes=%d states=%v keyed=%v registers=%v removes=%v timers=%v wills=%v",
					repository.loadCalls, repository.saveCalls, repository.deleteCall, repository.states,
					repository.keyedStates, queue.registers, queue.removes, scheduler.timers, publisher.published)
			}
			if test.wantAudit == "" {
				if len(auditRecorder.denials) != 0 {
					t.Fatalf("unavailable checker was recorded as a denial: %+v", auditRecorder.denials)
				}
				return
			}
			if len(auditRecorder.denials) != 1 || auditRecorder.denials[0].Reason != test.wantAudit ||
				auditRecorder.denials[0].CredentialID != "cred-1" {
				t.Fatalf("CONNECT denial audit = %+v; want reason %q", auditRecorder.denials, test.wantAudit)
			}
		})
	}
}
