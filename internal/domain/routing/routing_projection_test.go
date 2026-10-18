package routing

import (
	"testing"

	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/tenancy"
)

func TestReplaceProjectionIsAtomicAndReplacesCommittedRoutes(t *testing.T) {
	table := NewTable()
	oldTenant, err := tenancy.NewTenantID("tenant-old")
	if err != nil {
		t.Fatal(err)
	}
	newTenant, err := tenancy.NewTenantID("tenant-new")
	if err != nil {
		t.Fatal(err)
	}
	oldSession := SessionID("old-client")
	if err := table.BindSessionTenant(oldSession, oldTenant); err != nil {
		t.Fatal(err)
	}
	if _, err := table.Subscribe(oldSession, mqtt.Subscription{Filter: "old/#", QoS: mqtt.QoSAtLeastOnce}); err != nil {
		t.Fatal(err)
	}

	invalid := []ProjectionSubscription{{
		TenantID: newTenant, SessionID: "new-client",
		Subscription: mqtt.Subscription{Filter: "invalid/#/tail"},
	}}
	if err := table.ReplaceProjection(invalid); err == nil {
		t.Fatal("ReplaceProjection accepted an invalid topic filter")
	}
	if _, found := table.Lookup(oldSession, "old/#"); !found {
		t.Fatal("failed projection replacement removed the prior committed route")
	}
	if got := table.SessionTenant(oldSession); got != oldTenant {
		t.Fatalf("failed projection replacement changed prior tenant binding to %q", got.String())
	}

	newSession := SessionID("new-client")
	publisher := SessionID("publisher")
	projection := []ProjectionSubscription{{
		TenantID: newTenant, SessionID: newSession,
		Subscription: mqtt.Subscription{
			Filter: "new/#", QoS: mqtt.QoSAtLeastOnce, NoLocal: true,
			RetainAsPublished: true, RetainHandling: mqtt.SendRetainedAtSubscribe,
		},
	}}
	if err := table.ReplaceProjection(projection); err != nil {
		t.Fatalf("ReplaceProjection(): %v", err)
	}
	if _, found := table.Lookup(oldSession, "old/#"); found {
		t.Fatal("projection replacement retained a subscription absent from committed state")
	}
	gotSubscription, found := table.Lookup(newSession, "new/#")
	if !found || gotSubscription.QoS != mqtt.QoSAtLeastOnce || !gotSubscription.NoLocal || !gotSubscription.RetainAsPublished || gotSubscription.RetainHandling != mqtt.SendRetainedAtSubscribe {
		t.Fatalf("projected subscription = %+v, found=%v", gotSubscription, found)
	}
	if got := table.SessionTenant(oldSession); got.String() != "" {
		t.Fatalf("projection replacement retained removed tenant binding %q", got.String())
	}
	if err := table.BindSessionTenant(publisher, newTenant); err != nil {
		t.Fatal(err)
	}
	matches, err := table.Match(publisher, "new/topic", mqtt.QoSExactlyOnce, false)
	if err != nil {
		t.Fatalf("Match() after projection replacement: %v", err)
	}
	if len(matches) != 1 || matches[0].Primary.SessionID != newSession || matches[0].Primary.QoS != mqtt.QoSAtLeastOnce {
		t.Fatalf("committed route matches = %+v", matches)
	}
}

func TestReplaceProjectionRejectsDuplicateEntriesWithoutMutation(t *testing.T) {
	table := NewTable()
	session := SessionID("client-1")
	original := mqtt.Subscription{Filter: "stable/#", QoS: mqtt.QoSAtMostOnce}
	if _, err := table.Subscribe(session, original); err != nil {
		t.Fatal(err)
	}
	projection := []ProjectionSubscription{
		{SessionID: session, Subscription: mqtt.Subscription{Filter: "new/#"}},
		{SessionID: session, Subscription: mqtt.Subscription{Filter: "new/#"}},
	}
	if err := table.ReplaceProjection(projection); err == nil {
		t.Fatal("ReplaceProjection accepted duplicate session/filter entries")
	}
	if got, found := table.Lookup(session, original.Filter); !found || got != original {
		t.Fatalf("duplicate projection failure changed existing route: %+v, found=%v", got, found)
	}
}
func TestReplaceProjectionRejectsSessionTenantAliasingWithoutMutation(t *testing.T) {
	table := NewTable()
	session := SessionID("internal-session")
	originalTenant, err := tenancy.NewTenantID("original")
	if err != nil {
		t.Fatal(err)
	}
	firstTenant, err := tenancy.NewTenantID("a")
	if err != nil {
		t.Fatal(err)
	}
	secondTenant, err := tenancy.NewTenantID("a/b")
	if err != nil {
		t.Fatal(err)
	}
	if err := table.BindSessionTenant(session, originalTenant); err != nil {
		t.Fatal(err)
	}
	original := mqtt.Subscription{Filter: "stable/#"}
	if _, err := table.Subscribe(session, original); err != nil {
		t.Fatal(err)
	}

	projection := []ProjectionSubscription{
		{TenantID: firstTenant, SessionID: session, Subscription: mqtt.Subscription{Filter: "one/#"}},
		{TenantID: secondTenant, SessionID: session, Subscription: mqtt.Subscription{Filter: "two/#"}},
	}
	if err := table.ReplaceProjection(projection); err == nil {
		t.Fatal("ReplaceProjection accepted one session ID for multiple tenants")
	}
	if got, found := table.Lookup(session, original.Filter); !found || got != original {
		t.Fatalf("tenant alias rejection changed existing route: %+v, found=%v", got, found)
	}
	if got := table.SessionTenant(session); got != originalTenant {
		t.Fatalf("tenant alias rejection changed prior tenant binding to %q", got.String())
	}
}

func TestReplaceProjectionWithSessionsBindsUnsubscribedOwners(t *testing.T) {
	table := NewTable()
	tenantA, err := tenancy.NewTenantID("tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	tenantB, err := tenancy.NewTenantID("tenant-b")
	if err != nil {
		t.Fatal(err)
	}
	publisher := SessionID("publisher-a")
	subscriberA := SessionID("subscriber-a")
	subscriberB := SessionID("subscriber-b")
	sessions := []ProjectionSession{{TenantID: tenantA, SessionID: publisher}}
	projection := []ProjectionSubscription{
		{TenantID: tenantA, SessionID: subscriberA, Subscription: mqtt.Subscription{Filter: "events/#"}},
		{TenantID: tenantB, SessionID: subscriberB, Subscription: mqtt.Subscription{Filter: "events/#"}},
	}
	if err := table.ReplaceProjectionWithSessions(sessions, projection); err != nil {
		t.Fatalf("ReplaceProjectionWithSessions(): %v", err)
	}
	if got := table.SessionTenant(publisher); got != tenantA {
		t.Fatalf("unsubscribed publisher tenant = %q, want %q", got.String(), tenantA.String())
	}
	matches, err := table.Match(publisher, "events/item", mqtt.QoSAtMostOnce, false)
	if err != nil {
		t.Fatalf("Match() from committed unsubscribed owner: %v", err)
	}
	if len(matches) != 1 || matches[0].Primary.SessionID != subscriberA {
		t.Fatalf("committed owner matched cross-tenant routes: %+v", matches)
	}
}

// Traceability: TEN-06, TEN-07; docs/specs/tenancy-and-security.md §5.2.
// Scenarios: tenancy-and-security.md:SEC-SCN-025
func TestTenantSubscriptionCountIncludesSharedAndProjectedSubscriptions(t *testing.T) {
	var nilTable *Table
	if got := nilTable.TenantSubscriptionCount(tenancy.TenantID{}); got != 0 {
		t.Fatalf("nil TenantSubscriptionCount() = %d, want 0", got)
	}

	table := NewTable()
	tenantA, err := tenancy.NewTenantID("tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	tenantB, err := tenancy.NewTenantID("tenant-b")
	if err != nil {
		t.Fatal(err)
	}
	if got := table.TenantSubscriptionCount(tenantA); got != 0 {
		t.Fatalf("empty TenantSubscriptionCount() = %d, want 0", got)
	}

	projection := []ProjectionSubscription{
		{TenantID: tenantA, SessionID: "non-shared-a", Subscription: mqtt.Subscription{Filter: "events/#"}},
		{TenantID: tenantA, SessionID: "shared-a", Subscription: mqtt.Subscription{Filter: "$share/workers/events/+"}},
		{TenantID: tenantA, SessionID: "shared-b", Subscription: mqtt.Subscription{Filter: "$share/workers/events/+"}},
		{TenantID: tenantB, SessionID: "non-shared-b", Subscription: mqtt.Subscription{Filter: "events/#"}},
	}
	if err := table.ReplaceProjectionWithSessions(nil, projection); err != nil {
		t.Fatalf("ReplaceProjectionWithSessions(): %v", err)
	}
	if got := table.TenantSubscriptionCount(tenantA); got != 3 {
		t.Fatalf("tenant-a TenantSubscriptionCount() = %d, want 3", got)
	}
	if got := table.TenantSubscriptionCount(tenantB); got != 1 {
		t.Fatalf("tenant-b TenantSubscriptionCount() = %d, want 1", got)
	}
	if got := table.TenantSubscriptionCount(tenancy.TenantID{}); got != 0 {
		t.Fatalf("unbound TenantSubscriptionCount() = %d, want 0", got)
	}

	if err := table.ReplaceProjectionWithSessions(nil, nil); err != nil {
		t.Fatalf("ReplaceProjectionWithSessions(empty): %v", err)
	}
	if got := table.TenantSubscriptionCount(tenantA); got != 0 {
		t.Fatalf("TenantSubscriptionCount() after empty projection = %d, want 0", got)
	}
	if got := table.TenantSubscriptionCount(tenantB); got != 0 {
		t.Fatalf("tenant-b TenantSubscriptionCount() after empty projection = %d, want 0", got)
	}
}
