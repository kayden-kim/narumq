package connect

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"narumq/internal/adapters/memory/sessionstore"
	applicationauthentication "narumq/internal/application/ports/authentication"
	"narumq/internal/application/tenantlimit"
	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/routing"
	"narumq/internal/domain/security"
	"narumq/internal/domain/session"
	"narumq/internal/domain/tenancy"
)

type tenantLimitReporter struct{ dimensions []tenantlimit.Dimension }

func (reporter *tenantLimitReporter) TenantLimitRejected(_ tenancy.TenantID, dimension tenantlimit.Dimension) {
	reporter.dimensions = append(reporter.dimensions, dimension)
}

func tenantLimitConnectFixture(t *testing.T, limits tenantlimit.Limits) (*UseCase, *sessionstore.MemorySessionRepository, *fakeDeliveryQueue, *fakeAuthenticator, *tenantLimitReporter, tenancy.TenantID) {
	t.Helper()
	now := time.Unix(100, 0)
	clock := &fakeClock{now: now}
	tenant, _ := tenancy.NewTenantID("a")
	reporter := &tenantLimitReporter{}
	service, err := tenantlimit.New(clock, tenantlimit.Profile{Default: limits}, reporter)
	if err != nil {
		t.Fatal(err)
	}
	repository := sessionstore.New()
	queue := newFakeDeliveryQueue()
	auth := &fakeAuthenticator{result: applicationauthentication.Result{TenantID: tenant, Subject: "device", CredentialID: "cred", AllowedClientIDs: []string{"one", "two", "three"}, Roles: []security.Role{"client"}, ExpiresAt: now.Add(time.Hour)}}
	useCase, err := New(clock, repository, queue, routing.NewTable(), &fakeScheduler{}, &fakeWillPublisher{}, WithSecurity(auth, nil, security.Policy{}, "jwt"), WithTenantLimits(service, repository))
	if err != nil {
		t.Fatal(err)
	}
	return useCase, repository, queue, auth, reporter, tenant
}
func tenantLimitConnect(t *testing.T, useCase *UseCase, version mqtt.ProtocolVersion, clientID string, clean bool) ResultDTO {
	t.Helper()
	result, err := useCase.ConnectContext(context.Background(), ConnectRequestDTO{Packet: mqtt.Connect{Version: version, ClientID: clientID, CleanStart: clean, SessionExpiryInterval: 60, HasUsername: true, HasPassword: true, Password: []byte("valid")}})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

// Traceability: TEN-06, TEN-07; docs/specs/tenancy-and-security.md §5.2.
func TestConnectTenantConnectionQuota(t *testing.T) {
	for _, version := range []mqtt.ProtocolVersion{mqtt.ProtocolV5, mqtt.ProtocolV311} {
		name := "v5"
		if version == mqtt.ProtocolV311 {
			name = "v311"
		}
		t.Run(name, func(t *testing.T) {
			useCase, repository, queue, auth, reporter, tenant := tenantLimitConnectFixture(t, tenantlimit.Limits{MaxConnections: 1, MaxSessions: 1})
			first := tenantLimitConnect(t, useCase, version, "one", true)
			if !first.Accepted {
				t.Fatal(first)
			}
			if _, err := useCase.routes.Subscribe(first.SessionID, mqtt.Subscription{Filter: "kept/#"}); err != nil {
				t.Fatal(err)
			}
			states := repository.Snapshot()
			routes := useCase.routes.Snapshot()
			registers, removes := len(queue.registers), len(queue.removes)
			refused := tenantLimitConnect(t, useCase, version, "two", true)
			want := byte(0x97)
			if version == mqtt.ProtocolV311 {
				want = 0x03
			}
			if refused.Accepted || refused.ConnAck.ReasonCode != want {
				t.Fatalf("rejected=%+v want=%x", refused, want)
			}
			if !reflect.DeepEqual(states, repository.Snapshot()) || !reflect.DeepEqual(routes, useCase.routes.Snapshot()) || len(queue.registers) != registers || len(queue.removes) != removes {
				t.Fatal("quota rejection mutated session, route, or queue")
			}
			if !reflect.DeepEqual(reporter.dimensions, []tenantlimit.Dimension{tenantlimit.DimensionConnections}) {
				t.Fatal(reporter.dimensions)
			}
			// The existing duplicate-connect error must precede quota admission.
			duplicate := tenantLimitConnect(t, useCase, version, "one", false)
			if duplicate.Accepted || duplicate.ConnAck.ReasonCode != duplicateConnectReason(version) || len(reporter.dimensions) != 1 {
				t.Fatal(duplicate, reporter.dimensions)
			}
			other, _ := tenancy.NewTenantID("b")
			auth.result.TenantID = other
			if got := tenantLimitConnect(t, useCase, version, "two", true); !got.Accepted {
				t.Fatal(got)
			}
			if sessions, connected := repository.TenantSessionCounts(tenant); sessions != 1 || connected != 1 {
				t.Fatal(sessions, connected)
			}
		})
	}
}

// Traceability: TEN-06, TEN-07; docs/specs/tenancy-and-security.md §5.2.
func TestConnectTenantSessionQuotaAndResume(t *testing.T) {
	useCase, repository, _, _, reporter, tenant := tenantLimitConnectFixture(t, tenantlimit.Limits{MaxSessions: 1})
	first := tenantLimitConnect(t, useCase, mqtt.ProtocolV5, "one", true)
	if !first.Accepted {
		t.Fatal(first)
	}
	if err := useCase.Disconnect(first.SessionID, session.DisconnectNormal); err != nil {
		t.Fatal(err)
	}
	refused := tenantLimitConnect(t, useCase, mqtt.ProtocolV5, "two", true)
	if refused.Accepted || refused.ConnAck.ReasonCode != 0x97 {
		t.Fatal(refused)
	}
	if !reflect.DeepEqual(reporter.dimensions, []tenantlimit.Dimension{tenantlimit.DimensionSessions}) {
		t.Fatal(reporter.dimensions)
	}
	resumed := tenantLimitConnect(t, useCase, mqtt.ProtocolV5, "one", false)
	if !resumed.Accepted || !resumed.ConnAck.SessionPresent {
		t.Fatal(resumed)
	}
	if err := useCase.Disconnect(resumed.SessionID, session.DisconnectNormal); err != nil {
		t.Fatal(err)
	}
	replaced := tenantLimitConnect(t, useCase, mqtt.ProtocolV5, "one", true)
	if !replaced.Accepted || replaced.ConnAck.SessionPresent {
		t.Fatalf("clean start replacement at the session limit = %+v", replaced)
	}
	if sessions, connected := repository.TenantSessionCounts(tenant); sessions != 1 || connected != 1 {
		t.Fatal(sessions, connected)
	}
}

// Traceability: TEN-06, TEN-07; docs/specs/tenancy-and-security.md §5.2.
func TestConnectTenantQuotaPreservesDetachedTimers(t *testing.T) {
	useCase, repository, _, _, _, tenant := tenantLimitConnectFixture(t, tenantlimit.Limits{MaxConnections: 1})
	first := tenantLimitConnect(t, useCase, mqtt.ProtocolV5, "one", true)
	if err := useCase.Disconnect(first.SessionID, session.DisconnectNormal); err != nil {
		t.Fatal(err)
	}
	timers := useCase.activeTimers[string(first.SessionID)]
	expiry := timers.expiryTimer.(*fakeTimer)
	if got := tenantLimitConnect(t, useCase, mqtt.ProtocolV5, "two", true); !got.Accepted {
		t.Fatal(got)
	}
	states := repository.Snapshot()
	refused := tenantLimitConnect(t, useCase, mqtt.ProtocolV5, "one", false)
	if refused.Accepted || refused.ConnAck.ReasonCode != 0x97 {
		t.Fatal(refused)
	}
	if expiry.stopped || useCase.activeTimers[string(first.SessionID)] != timers {
		t.Fatal("refused resume cancelled the detached expiry timer")
	}
	for _, before := range states {
		after, found, err := repository.LoadKey(before.SessionKey())
		if err != nil || !found || !reflect.DeepEqual(before, after) {
			t.Fatalf("refused resume changed lifecycle %s: found=%t err=%v", before.ClientID(), found, err)
		}
	}
	if sessions, connected := repository.TenantSessionCounts(tenant); sessions != 2 || connected != 1 {
		t.Fatal(sessions, connected)
	}
}

// Traceability: TEN-06, TEN-07; docs/specs/tenancy-and-security.md §5.2.
func TestConnectTenantLimitsRequireCounterAndSkipAnonymous(t *testing.T) {
	clock := &fakeClock{now: time.Unix(100, 0)}
	service, err := tenantlimit.New(clock, tenantlimit.Profile{Default: tenantlimit.Limits{MaxConnections: 1, MaxSessions: 1}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	repository := sessionstore.New()
	args := func(options ...Option) (*UseCase, error) {
		return New(clock, repository, newFakeDeliveryQueue(), routing.NewTable(), &fakeScheduler{}, &fakeWillPublisher{}, options...)
	}
	if _, err := args(WithTenantLimits(service, nil)); !errors.Is(err, ErrNilTenantSessionCounter) {
		t.Fatal(err)
	}
	useCase, err := args(WithTenantLimits(service, repository))
	if err != nil {
		t.Fatal(err)
	}
	for _, client := range []string{"one", "two"} {
		result, err := useCase.Connect(mqtt.Connect{Version: mqtt.ProtocolV5, ClientID: client, CleanStart: true})
		if err != nil || !result.Accepted {
			t.Fatal(result, err)
		}
	}
	if _, err := args(WithTenantLimits(nil, nil)); err != nil {
		t.Fatal(err)
	}
}
