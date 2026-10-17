package records

import (
	"strings"
	"testing"
)

const sharedFilterTestValue = "$share/g/jobs/#"

func sharedFilterSnapshot(filter string, features ...string) *ReplicatedStateSnapshot {
	session := crrDeliverySession("tenant", "client")
	session.Messages[1].SharedFilter = filter
	return &ReplicatedStateSnapshot{
		Ownerships:       []*OwnershipState{{TenantId: "tenant", ClientId: "client", OwnerNodeId: 1, Epoch: 1}},
		DeliverySessions: []*DeliverySessionState{session},
		ActiveFeatures:   features,
	}
}

// Traceability: CLU-11, CLU-14; docs/contracts/replicated-state-v1.md; docs/specs/cluster-session-and-delivery.md §11.35.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-255
func TestDeliverySharedFilterCommandRoundTripAndValidation(t *testing.T) {
	publish := crrDelivery(DeliveryAction_DELIVERY_ACTION_PUBLISH)
	publish.Targets = []*DeliveryTarget{{ClientId: "client", SharedFilter: sharedFilterTestValue}}
	wire, err := MarshalCommand(crrDeliveryCommand(publish))
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := UnmarshalCommand(wire)
	if err != nil {
		t.Fatal(err)
	}
	if got := decoded.GetDelivery().GetTargets()[0].GetSharedFilter(); got != sharedFilterTestValue {
		t.Fatalf("decoded shared_filter = %q", got)
	}
	for name, filter := range map[string]string{
		"not shared":         "jobs/#",
		"no group":           "$share//jobs",
		"no topic filter":    "$share/g/",
		"invalid utf8":       "$share/g/\xff",
		"oversized":          "$share/g/" + strings.Repeat("a", 65536),
		"invalid wildcard":   "$share/g/jobs/#/x",
		"group has wildcard": "$share/g+/jobs",
	} {
		invalid := crrDelivery(DeliveryAction_DELIVERY_ACTION_PUBLISH)
		invalid.Targets = []*DeliveryTarget{{ClientId: "client", SharedFilter: filter}}
		if _, err := MarshalCommand(crrDeliveryCommand(invalid)); err == nil || !strings.Contains(err.Error(), "invalid delivery target shared_filter") {
			t.Fatalf("%s: shared_filter %q error = %v", name, filter, err)
		}
	}
}

// Traceability: CLU-11, CLU-14; docs/contracts/replicated-state-v1.md; docs/specs/cluster-session-and-delivery.md §11.35.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-255
func TestDeliverySharedFilterSnapshotRequiresActiveFeature(t *testing.T) {
	wire, err := MarshalSnapshot(sharedFilterSnapshot(sharedFilterTestValue, "shared-subscription-reroute"), 5)
	if err != nil {
		t.Fatal(err)
	}
	restored, err := UnmarshalSnapshot(wire)
	if err != nil {
		t.Fatal(err)
	}
	if got := restored.State.DeliverySessions[0].Messages[1].SharedFilter; got != sharedFilterTestValue {
		t.Fatalf("restored shared_filter = %q", got)
	}
	if _, err := MarshalSnapshot(sharedFilterSnapshot(sharedFilterTestValue), 5); err == nil || !strings.Contains(err.Error(), "requires active shared-subscription-reroute feature") {
		t.Fatalf("snapshot without feature error = %v", err)
	}
	if _, err := MarshalSnapshot(sharedFilterSnapshot("jobs/#", "shared-subscription-reroute"), 5); err == nil || !strings.Contains(err.Error(), "invalid snapshot delivery message shared_filter") {
		t.Fatalf("snapshot with non-shared filter error = %v", err)
	}
	if _, err := UnmarshalSnapshot(crrRawSnapshot(t, sharedFilterSnapshot(sharedFilterTestValue), 5)); err == nil {
		t.Fatal("foreign snapshot with a shared origin and no active feature was accepted")
	}
}
