package connect

import (
	"context"
	"errors"
	"testing"
	"time"

	applicationauthentication "narumq/internal/application/ports/authentication"
	portsmetrics "narumq/internal/application/ports/metrics"
	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/routing"
	"narumq/internal/domain/security"
	"narumq/internal/domain/session"
	"narumq/internal/domain/tenancy"
)

// cmmPlainSessionRepository implements only the Client ID keyed repository
// contract, so tenant-scoped sessions must fall back to Client ID storage.
type cmmPlainSessionRepository struct {
	states map[string]session.Lifecycle
}

func (repository *cmmPlainSessionRepository) Load(clientID string) (session.Lifecycle, bool, error) {
	state, found := repository.states[clientID]
	return state, found, nil
}

func (repository *cmmPlainSessionRepository) Save(state session.Lifecycle) error {
	repository.states[state.ClientID()] = state
	return nil
}

func (repository *cmmPlainSessionRepository) Delete(clientID string) error {
	delete(repository.states, clientID)
	return nil
}

func (repository *cmmPlainSessionRepository) Snapshot() []session.Lifecycle {
	states := make([]session.Lifecycle, 0, len(repository.states))
	for _, state := range repository.states {
		states = append(states, state)
	}
	return states
}

type cmmSessionGaugeMetrics struct {
	fakeMetrics
	persistent    int64
	nonPersistent int64
}

func (metrics *cmmSessionGaugeMetrics) SetSessionsActive(persistent bool, count int64) {
	if persistent {
		metrics.persistent = count
		return
	}
	metrics.nonPersistent = count
}

const cmmWillTopic = "sensors/status"

func cmmWillPolicy(t *testing.T, tenant tenancy.TenantID) security.Policy {
	t.Helper()
	filter, err := tenancy.NewTopicFilter(tenant, "sensors/#")
	if err != nil {
		t.Fatal(err)
	}
	rule, err := security.NewRule(tenant, "publisher", security.EffectAllow, security.ActionWill, filter)
	if err != nil {
		t.Fatal(err)
	}
	policy, err := security.NewPolicy([]security.Rule{rule})
	if err != nil {
		t.Fatal(err)
	}
	return policy
}

func cmmAuthResult(tenant tenancy.TenantID, now time.Time, clientIDs ...string) applicationauthentication.Result {
	return applicationauthentication.Result{
		TenantID:         tenant,
		Subject:          "device-1",
		CredentialID:     "cred-1",
		AllowedClientIDs: clientIDs,
		Roles:            []security.Role{"publisher"},
		ExpiresAt:        now.Add(time.Hour),
	}
}

func cmmJWTConnect(clientID string, expiry uint32, will *mqtt.Will) mqtt.Connect {
	return mqtt.Connect{
		Version: mqtt.ProtocolV5, ClientID: clientID, SessionExpiryInterval: expiry,
		HasUsername: true, Username: "device", HasPassword: true, Password: []byte("jwt"), Will: will,
	}
}

func TestCmmClusterOwnershipConfiguredRequiresMatchingNode(t *testing.T) {
	var nilUseCase *UseCase
	if nilUseCase.ClusterOwnershipConfigured(7) {
		t.Fatal("nil use case reported configured ownership")
	}
	useCase, _, _, _, _, _, _ := newUseCase(t, time.Unix(0, 0))
	if useCase.ClusterOwnershipConfigured(7) {
		t.Fatal("use case without coordinator reported configured ownership")
	}
	WithOwnershipCoordinator(&fakeOwnershipCoordinator{epoch: 1}, 7)(useCase)
	cases := []struct {
		node uint64
		want bool
	}{
		{node: 0, want: false},
		{node: 7, want: true},
		{node: 8, want: false},
	}
	for _, testCase := range cases {
		if got := useCase.ClusterOwnershipConfigured(testCase.node); got != testCase.want {
			t.Fatalf("ClusterOwnershipConfigured(%d)=%v want %v", testCase.node, got, testCase.want)
		}
	}
}

func TestCmmConnectRejectsCredentialModeMismatchesWithoutMutation(t *testing.T) {
	now := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	tenant := newMTLSTestTenantForConnect(t, "tenant-a")
	certificate := &applicationauthentication.ClientCertificateDTO{CommonName: "device", NotAfter: now.Add(time.Hour)}
	cases := []struct {
		name        string
		jwt         bool
		mtls        bool
		certificate *applicationauthentication.ClientCertificateDTO
		version     mqtt.ProtocolVersion
		wantReason  byte
		wantAttempt authAttempt
	}{
		{name: "certificate without mTLS provider v5", jwt: true, certificate: certificate, version: mqtt.ProtocolV5, wantReason: 0x87,
			wantAttempt: authAttempt{method: portsmetrics.AuthMethodTLS, reason: "unsupported_credentials"}},
		{name: "certificate on anonymous listener v3.1.1", certificate: certificate, version: mqtt.ProtocolV311, wantReason: 0x05,
			wantAttempt: authAttempt{method: portsmetrics.AuthMethodTLS, reason: "unsupported_credentials"}},
		{name: "mTLS-only listener without certificate v5", mtls: true, version: mqtt.ProtocolV5, wantReason: 0x87,
			wantAttempt: authAttempt{method: portsmetrics.AuthMethodTLS, reason: "missing_certificate"}},
		{name: "mTLS-only listener without certificate v3.1.1", mtls: true, version: mqtt.ProtocolV311, wantReason: 0x05,
			wantAttempt: authAttempt{method: portsmetrics.AuthMethodTLS, reason: "missing_certificate"}},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			jwtProvider := &fakeAuthenticator{result: cmmAuthResult(tenant, now, "client-1")}
			mtlsProvider := &fakeCertificateAuthenticator{result: cmmAuthResult(tenant, now, "client-1")}
			var jwt applicationauthentication.AuthenticationProvider
			if testCase.jwt {
				jwt = jwtProvider
			}
			var mtls applicationauthentication.ClientCertificateAuthenticationProvider
			if testCase.mtls {
				mtls = mtlsProvider
			}
			metrics := &fakeMetrics{}
			useCase, repository, queue := newSecurityTestUseCase(t, now, jwt, mtls, security.Policy{}, portsmetrics.AuthMethodJWT, metrics)
			result, err := useCase.ConnectContext(context.Background(), ConnectRequestDTO{
				Packet:            mqtt.Connect{Version: testCase.version, ClientID: "client-1"},
				ClientCertificate: testCase.certificate,
			})
			if err != nil || result.Accepted || result.ConnAck.ReasonCode != testCase.wantReason {
				t.Fatalf("ConnectContext=%#v err=%v want reason %#x", result, err, testCase.wantReason)
			}
			if jwtProvider.calls != 0 || mtlsProvider.calls != 0 {
				t.Fatalf("providers invoked jwt=%d mtls=%d", jwtProvider.calls, mtlsProvider.calls)
			}
			if len(metrics.attempts) != 1 || metrics.attempts[0] != testCase.wantAttempt {
				t.Fatalf("auth attempts=%#v want %#v", metrics.attempts, testCase.wantAttempt)
			}
			if repository.loadCalls != 0 || repository.saveCalls != 0 || len(queue.registers) != 0 {
				t.Fatalf("rejected CONNECT mutated state loads=%d saves=%d registers=%v", repository.loadCalls, repository.saveCalls, queue.registers)
			}
		})
	}
}

func TestCmmConnectRejectsUnusableAuthenticatedIdentity(t *testing.T) {
	now := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	tenant := newMTLSTestTenantForConnect(t, "tenant-a")
	valid := cmmAuthResult(tenant, now, "client-1")
	cases := []struct {
		name       string
		mutate     func(*applicationauthentication.Result)
		will       *mqtt.Will
		wantReason string
	}{
		{name: "empty subject", mutate: func(result *applicationauthentication.Result) { result.Subject = "" }, wantReason: "not_authorized"},
		{name: "empty credential", mutate: func(result *applicationauthentication.Result) { result.CredentialID = "" }, wantReason: "not_authorized"},
		{name: "no expiry", mutate: func(result *applicationauthentication.Result) { result.ExpiresAt = time.Time{} }, wantReason: "not_authorized"},
		{name: "duplicate role", mutate: func(result *applicationauthentication.Result) {
			result.Roles = []security.Role{"publisher", "publisher"}
		}, wantReason: "not_authorized"},
		{name: "wildcard will topic", will: &mqtt.Will{Topic: "sensors/+", Payload: []byte("bye")}, wantReason: "invalid_will_topic"},
		{name: "empty will topic", will: &mqtt.Will{Topic: "", Payload: []byte("bye")}, wantReason: "invalid_will_topic"},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			authResult := valid
			authResult.AllowedClientIDs = append([]string(nil), valid.AllowedClientIDs...)
			authResult.Roles = append([]security.Role(nil), valid.Roles...)
			if testCase.mutate != nil {
				testCase.mutate(&authResult)
			}
			metrics := &fakeMetrics{}
			useCase, repository, queue := newSecurityTestUseCase(t, now, &fakeAuthenticator{result: authResult}, nil, cmmWillPolicy(t, tenant), portsmetrics.AuthMethodJWT, metrics)
			result, err := useCase.Connect(cmmJWTConnect("client-1", 0, testCase.will))
			if err != nil || result.Accepted || result.ConnAck.ReasonCode != 0x87 {
				t.Fatalf("Connect=%#v err=%v", result, err)
			}
			want := authAttempt{method: portsmetrics.AuthMethodJWT, reason: testCase.wantReason}
			if len(metrics.attempts) != 1 || metrics.attempts[0] != want {
				t.Fatalf("auth attempts=%#v want %#v", metrics.attempts, want)
			}
			if repository.loadCalls != 0 || repository.saveCalls != 0 || len(queue.registers) != 0 {
				t.Fatalf("rejected CONNECT mutated state loads=%d saves=%d registers=%v", repository.loadCalls, repository.saveCalls, queue.registers)
			}
		})
	}
}

func TestCmmClusterConnectRejectsClientIDOutsideSessionKeyDomain(t *testing.T) {
	now := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	useCase, _, repository, queue, _, _, _ := newUseCase(t, now)
	coordinator := &fakeOwnershipCoordinator{epoch: 1}
	WithOwnershipCoordinator(coordinator, 3)(useCase)
	for _, clientID := range []string{"nul\x00client", "non\uFDD0character", "bad-\xff-utf8"} {
		result, err := useCase.Connect(mqtt.Connect{Version: mqtt.ProtocolV5, ClientID: clientID})
		if !errors.Is(err, tenancy.ErrInvalidClientID) || result.Accepted {
			t.Fatalf("Connect(%q)=%#v err=%v want ErrInvalidClientID", clientID, result, err)
		}
	}
	if coordinator.calls != 0 || repository.loadCalls != 0 || repository.saveCalls != 0 || len(queue.registers) != 0 {
		t.Fatalf("invalid Client ID reached ownership=%d loads=%d saves=%d registers=%v", coordinator.calls, repository.loadCalls, repository.saveCalls, queue.registers)
	}
}

func TestCmmClusterOwnershipUnavailableReasonByVersion(t *testing.T) {
	now := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	cases := []struct {
		name       string
		version    mqtt.ProtocolVersion
		epoch      uint64
		err        error
		wantReason byte
	}{
		{name: "v3.1.1 commit failure", version: mqtt.ProtocolV311, err: errors.New("no quorum"), wantReason: 0x03},
		{name: "v3.1.1 zero epoch", version: mqtt.ProtocolV311, wantReason: 0x03},
		{name: "v5 zero epoch", version: mqtt.ProtocolV5, wantReason: 0x88},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			useCase, _, repository, queue, _, _, _ := newUseCase(t, now)
			WithOwnershipCoordinator(&fakeOwnershipCoordinator{epoch: testCase.epoch, err: testCase.err}, 3)(useCase)
			result, err := useCase.Connect(mqtt.Connect{Version: testCase.version, ClientID: "cluster-client"})
			if err != nil || result.Accepted || result.ConnAck.ReasonCode != testCase.wantReason || result.OwnershipEpoch != 0 {
				t.Fatalf("Connect=%#v err=%v want reason %#x", result, err, testCase.wantReason)
			}
			if repository.saveCalls != 0 || len(queue.registers) != 0 {
				t.Fatalf("unowned session activated saves=%d registers=%v", repository.saveCalls, queue.registers)
			}
		})
	}
}

func TestCmmConnectJoinsRegistrationAndRollbackFailures(t *testing.T) {
	now := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	useCase, _, repository, queue, _, _, _ := newUseCase(t, now)
	registerErr := errors.New("queue registration failed")
	deleteErr := errors.New("rollback delete failed")
	queue.registerErr = registerErr
	repository.deleteErr = deleteErr

	result, err := useCase.Connect(mqtt.Connect{Version: mqtt.ProtocolV5, ClientID: "client", SessionExpiryInterval: 30})
	if !errors.Is(err, registerErr) || !errors.Is(err, deleteErr) || result.Accepted {
		t.Fatalf("Connect=%#v err=%v want joined registration and rollback errors", result, err)
	}
	if repository.deleteCall != 1 {
		t.Fatalf("rollback delete calls=%d", repository.deleteCall)
	}
}

func TestCmmTenantSessionsFallBackToClientIDRepositoryAndReportGauge(t *testing.T) {
	now := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	tenant := newMTLSTestTenantForConnect(t, "tenant-a")
	repository := &cmmPlainSessionRepository{states: make(map[string]session.Lifecycle)}
	queue := newFakeDeliveryQueue()
	metrics := &cmmSessionGaugeMetrics{}
	useCase, err := New(&fakeClock{now: now}, repository, queue, routing.NewTable(), &fakeScheduler{}, &fakeWillPublisher{},
		WithSecurity(&fakeAuthenticator{result: cmmAuthResult(tenant, now, "transient", "persistent")}, nil, security.Policy{}, "jwt"),
		WithMetrics(metrics))
	if err != nil {
		t.Fatal(err)
	}

	transient, err := useCase.Connect(cmmJWTConnect("transient", 0, nil))
	if err != nil || !transient.Accepted || transient.SessionID != "tenant-a/transient" {
		t.Fatalf("transient Connect=%#v err=%v", transient, err)
	}
	persistent, err := useCase.Connect(cmmJWTConnect("persistent", 120, nil))
	if err != nil || !persistent.Accepted {
		t.Fatalf("persistent Connect=%#v err=%v", persistent, err)
	}
	if _, found := repository.states["transient"]; !found {
		t.Fatalf("tenant session not stored by Client ID: %v", repository.states)
	}
	if metrics.persistent != 1 || metrics.nonPersistent != 1 {
		t.Fatalf("session gauge persistent=%d nonPersistent=%d", metrics.persistent, metrics.nonPersistent)
	}

	if err := useCase.Disconnect(transient.SessionID, session.DisconnectNormal); err != nil {
		t.Fatal(err)
	}
	if _, found := repository.states["transient"]; found {
		t.Fatal("immediate tenant session was not deleted through Client ID fallback")
	}
	if _, found := repository.states["persistent"]; !found {
		t.Fatal("unrelated persistent session was deleted")
	}
	if metrics.persistent != 1 || metrics.nonPersistent != 0 {
		t.Fatalf("session gauge after disconnect persistent=%d nonPersistent=%d", metrics.persistent, metrics.nonPersistent)
	}
	if len(queue.removes) == 0 || queue.removes[len(queue.removes)-1] != transient.SessionID {
		t.Fatalf("delivery queue removes=%v", queue.removes)
	}
}

func TestCmmDuplicateDisconnectOfExpiringSessionIsRejected(t *testing.T) {
	now := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	useCase, _, repository, _, _, scheduler, willPublisher := newUseCase(t, now)
	result, err := useCase.Connect(mqtt.Connect{
		Version: mqtt.ProtocolV5, ClientID: "client", SessionExpiryInterval: 60,
		Will: &mqtt.Will{Topic: cmmWillTopic, Payload: []byte("lost"), DelayInterval: 10},
	})
	if err != nil || !result.Accepted {
		t.Fatalf("Connect=%#v err=%v", result, err)
	}
	if err := useCase.Disconnect(result.SessionID, session.DisconnectAbnormal); err != nil {
		t.Fatal(err)
	}
	if len(scheduler.timers) != 2 {
		t.Fatalf("scheduled timers=%d want will and expiry", len(scheduler.timers))
	}
	before := repository.states["client"]

	if err := useCase.Disconnect(result.SessionID, session.DisconnectAbnormal); !errors.Is(err, session.ErrNotConnected) {
		t.Fatalf("duplicate Disconnect err=%v want ErrNotConnected", err)
	}
	after, found := repository.states["client"]
	if !found || after.Connected() || after != before {
		t.Fatalf("duplicate Disconnect changed stored session found=%v before=%#v after=%#v", found, before, after)
	}
	if len(willPublisher.published) != 0 {
		t.Fatalf("duplicate Disconnect published will %#v", willPublisher.published)
	}
}

func TestCmmAuthenticatedDelayedWillTimerPublishesAuthorizedWill(t *testing.T) {
	now := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	tenant := newMTLSTestTenantForConnect(t, "tenant-a")
	clock := &fakeClock{now: now}
	repository := newFakeSessionRepository()
	scheduler := &fakeScheduler{}
	publisher := &fakeWillPublisher{}
	useCase, err := New(clock, repository, newFakeDeliveryQueue(), routing.NewTable(), scheduler, publisher,
		WithSecurity(&fakeAuthenticator{result: cmmAuthResult(tenant, now, "client-1")}, nil, cmmWillPolicy(t, tenant), "jwt"))
	if err != nil {
		t.Fatal(err)
	}
	result, err := useCase.Connect(cmmJWTConnect("client-1", 60, &mqtt.Will{Topic: cmmWillTopic, Payload: []byte("lost"), DelayInterval: 5}))
	if err != nil || !result.Accepted {
		t.Fatalf("Connect=%#v err=%v", result, err)
	}
	if err := useCase.Disconnect(result.SessionID, session.DisconnectAbnormal); err != nil {
		t.Fatal(err)
	}
	if len(scheduler.timers) != 2 || scheduler.timers[0].delay != 5*time.Second || scheduler.timers[1].delay != 60*time.Second {
		t.Fatalf("scheduled timers=%#v", scheduler.timers)
	}
	if len(publisher.published) != 0 {
		t.Fatalf("delayed will published at disconnect: %#v", publisher.published)
	}

	clock.now = now.Add(5 * time.Second)
	scheduler.timers[0].callback()
	if len(publisher.published) != 1 || publisher.published[0].sessionID != result.SessionID || publisher.published[0].will.Topic != cmmWillTopic {
		t.Fatalf("published wills=%#v", publisher.published)
	}
	key, err := tenancy.NewSessionKey(tenant, "client-1")
	if err != nil {
		t.Fatal(err)
	}
	stored, found := repository.keyedStates[key]
	if !found || !stored.WillPublished() {
		t.Fatalf("stored session found=%v willPublished=%v", found, stored.WillPublished())
	}
	scheduler.timers[0].callback()
	if len(publisher.published) != 1 {
		t.Fatalf("will published twice: %#v", publisher.published)
	}
}

func TestCmmAuthenticatedSessionExpiryPublishesPendingWill(t *testing.T) {
	now := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	tenant := newMTLSTestTenantForConnect(t, "tenant-a")
	clock := &fakeClock{now: now}
	repository := newFakeSessionRepository()
	queue := newFakeDeliveryQueue()
	scheduler := &fakeScheduler{}
	publisher := &fakeWillPublisher{}
	useCase, err := New(clock, repository, queue, routing.NewTable(), scheduler, publisher,
		WithSecurity(&fakeAuthenticator{result: cmmAuthResult(tenant, now, "client-1")}, nil, cmmWillPolicy(t, tenant), "jwt"))
	if err != nil {
		t.Fatal(err)
	}
	result, err := useCase.Connect(cmmJWTConnect("client-1", 5, &mqtt.Will{Topic: cmmWillTopic, Payload: []byte("lost"), DelayInterval: 30}))
	if err != nil || !result.Accepted {
		t.Fatalf("Connect=%#v err=%v", result, err)
	}
	if err := useCase.Disconnect(result.SessionID, session.DisconnectAbnormal); err != nil {
		t.Fatal(err)
	}
	if len(scheduler.timers) != 2 {
		t.Fatalf("scheduled timers=%d", len(scheduler.timers))
	}
	willTimer, expiryTimer := scheduler.timers[0], scheduler.timers[1]
	if willTimer.delay != 5*time.Second || expiryTimer.delay != 5*time.Second {
		t.Fatalf("will delay=%v expiry delay=%v want both clamped to 5s", willTimer.delay, expiryTimer.delay)
	}

	clock.now = now.Add(5 * time.Second)
	expiryTimer.callback()
	if len(publisher.published) != 1 || publisher.published[0].sessionID != result.SessionID || string(publisher.published[0].will.Payload) != "lost" {
		t.Fatalf("published wills=%#v", publisher.published)
	}
	if !willTimer.stopped {
		t.Fatal("expiry did not stop the pending will timer")
	}
	if len(repository.keyedStates) != 0 || len(repository.states) != 0 {
		t.Fatalf("expired session still stored keyed=%v plain=%v", repository.keyedStates, repository.states)
	}
	if len(queue.removes) == 0 || queue.removes[len(queue.removes)-1] != result.SessionID {
		t.Fatalf("delivery queue removes=%v", queue.removes)
	}
}
