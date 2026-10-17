package subscribe

import (
	"context"
	"reflect"
	"testing"
	"time"

	"narumq/internal/application/tenantlimit"
	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/routing"
	"narumq/internal/domain/security"
	"narumq/internal/domain/tenancy"
)

type dataPlaneSubscribeClock struct{}

func (dataPlaneSubscribeClock) Now() time.Time { return time.Unix(100, 0).UTC() }

func dataPlaneSubscribeFixture(t *testing.T) (*UseCase, *routing.Table, security.Principal, time.Time) {
	t.Helper()
	now := dataPlaneSubscribeClock{}.Now()
	tenant, err := tenancy.NewTenantID("tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	allowedFilter, err := tenancy.NewTopicFilter(tenant, "allowed/#")
	if err != nil {
		t.Fatal(err)
	}
	subscribeRule, err := security.NewRule(tenant, "client", security.EffectAllow, security.ActionSubscribe, allowedFilter)
	if err != nil {
		t.Fatal(err)
	}
	unsubscribeRule, err := security.NewRule(tenant, "client", security.EffectAllow, security.ActionUnsubscribe, allowedFilter)
	if err != nil {
		t.Fatal(err)
	}
	policy, err := security.NewPolicy([]security.Rule{subscribeRule, unsubscribeRule})
	if err != nil {
		t.Fatal(err)
	}
	principal, err := security.NewPrincipal(tenant, "client-1", "device-1", "credential-1", []security.Role{"client"}, now.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	routes := routing.NewTable()
	if err := routes.BindSessionTenant("client-1", tenant); err != nil {
		t.Fatal(err)
	}
	if _, err := routes.Subscribe("client-1", mqtt.Subscription{Filter: "allowed/existing", QoS: mqtt.QoSAtLeastOnce}); err != nil {
		t.Fatal(err)
	}
	limits, err := tenantlimit.New(dataPlaneSubscribeClock{}, tenantlimit.Profile{Default: tenantlimit.Limits{MaxSubscriptions: 1}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	useCase, err := New(routes, &recordingQueue{}, &recordingRetained{}, WithPolicy(policy), WithTenantLimits(limits))
	if err != nil {
		t.Fatal(err)
	}
	return useCase, routes, principal, now
}

// Traceability: SEC-11/NFR-17; docs/specs/tenancy-and-security.md §8.
// Scenarios: tenancy-and-security.md:SEC-SCN-047
func TestSubscribeResultReportsOnlyACLDeniedFilters(t *testing.T) {
	for _, version := range []mqtt.ProtocolVersion{mqtt.ProtocolV311, mqtt.ProtocolV5} {
		name := "mqtt311"
		wantReasons := []byte{0x80, 0x80, 0x80}
		if version == mqtt.ProtocolV5 {
			name = "mqtt5"
			wantReasons = []byte{0x87, 0x8F, 0x97}
		}
		t.Run(name, func(t *testing.T) {
			useCase, _, principal, now := dataPlaneSubscribeFixture(t)
			result, err := useCase.SubscribeCommitted(context.Background(), "client-1", mqtt.Subscribe{
				Version: version, PacketID: 11,
				Subscriptions: []mqtt.Subscription{
					{Filter: "forbidden/#", QoS: mqtt.QoSAtMostOnce},
					{Filter: "broken/#/filter", QoS: mqtt.QoSAtMostOnce},
					{Filter: "allowed/new", QoS: mqtt.QoSAtMostOnce},
				},
			}, now, SessionOwnership{}, principal)
			if err != nil {
				t.Fatalf("SubscribeCommitted() error = %v", err)
			}
			if !reflect.DeepEqual(result.Ack.ReasonCodes, wantReasons) {
				t.Fatalf("SUBACK reason codes = % X, want % X", result.Ack.ReasonCodes, wantReasons)
			}
			if !reflect.DeepEqual(result.ACLDeniedFilters, []string{"forbidden/#"}) {
				t.Fatalf("ACL-denied filters = %v, want only forbidden/#", result.ACLDeniedFilters)
			}
		})
	}
}

// Traceability: SEC-11/NFR-17; docs/specs/tenancy-and-security.md §8.
// Scenarios: tenancy-and-security.md:SEC-SCN-047
func TestUnsubscribeResultReportsOnlyACLDeniedFilters(t *testing.T) {
	for _, version := range []mqtt.ProtocolVersion{mqtt.ProtocolV311, mqtt.ProtocolV5} {
		name := "mqtt311"
		wantReasons := []byte(nil)
		if version == mqtt.ProtocolV5 {
			name = "mqtt5"
			wantReasons = []byte{0x87, 0x8F, 0x00}
		}
		t.Run(name, func(t *testing.T) {
			useCase, routes, principal, _ := dataPlaneSubscribeFixture(t)
			result, err := useCase.UnsubscribeCommitted(context.Background(), "client-1", mqtt.Unsubscribe{
				Version: version, PacketID: 12, Filters: []string{"forbidden/old", "broken/#/filter", "allowed/existing"},
			}, SessionOwnership{}, principal)
			if err != nil {
				t.Fatalf("UnsubscribeCommitted() error = %v", err)
			}
			if !reflect.DeepEqual(result.Ack.ReasonCodes, wantReasons) {
				t.Fatalf("UNSUBACK reason codes = % X, want % X", result.Ack.ReasonCodes, wantReasons)
			}
			if !reflect.DeepEqual(result.ACLDeniedFilters, []string{"forbidden/old"}) {
				t.Fatalf("ACL-denied filters = %v, want only forbidden/old", result.ACLDeniedFilters)
			}
			if _, found := routes.Lookup("client-1", "allowed/existing"); found {
				t.Fatal("authorized filter remained after unsubscribe")
			}
		})
	}
}
