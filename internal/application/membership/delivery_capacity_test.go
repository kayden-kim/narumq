package membership

import (
	"fmt"
	"reflect"
	"testing"
)

// Traceability: CLU-12; docs/specs/cluster-session-and-delivery.md §4.5, §11.28.
func TestDeliveryPublishCapacity(t *testing.T) {
	for _, tc := range []struct {
		name, reason                         string
		entries, payloadBytes                int
		quota                                bool
		full, ineligible, expired, exhausted bool
	}{
		{name: "fast-path"},
		{name: "global-entries", entries: MaxDeliveryEntries - 1, reason: DeliveryCapacityEntries},
		{name: "global-bytes", entries: 17, payloadBytes: 980000, reason: DeliveryCapacityBytes},
		{name: "tenant-quota", quota: true, reason: DeliveryCapacityTenantQuota},
		{name: "session-full-exact", quota: true, full: true},
		{name: "ineligible-exact", quota: true, ineligible: true},
		{name: "expired", entries: MaxDeliveryEntries, expired: true},
		{name: "sequence-exhausted-exact", quota: true, exhausted: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			machine := NewStateMachine()
			index := uint64(1)
			setupTarget := func(client string, qos uint32) {
				deliveryTestAcquire(t, machine, index, client)
				index++
				deliveryTestSubscribe(t, machine, index, client, "capacity/#", qos)
				index++
			}
			setupTarget("a", 1)
			setupTarget("b", 1)
			change := DeliveryChangeDTO{TenantID: "t", ClientID: "publisher", Action: DeliveryActionPublish, ProposedAtUnixMillis: 1, Topic: "capacity/new", Payload: []byte("value"), PublishQoS: 1, Targets: []DeliveryTargetDTO{{ClientID: "a"}, {ClientID: "b"}}}
			if tc.payloadBytes > 0 {
				change.Payload = make([]byte, tc.payloadBytes)
			}
			for i := 0; i < tc.entries; i++ {
				client := fmt.Sprintf("fill-%d", i/MaxDeliveryMessagesPerSession)
				if i%MaxDeliveryMessagesPerSession == 0 {
					setupTarget(client, 1)
				}
				fill := change
				fill.Targets = []DeliveryTargetDTO{{ClientID: client}}
				fill.Payload = make([]byte, tc.payloadBytes)
				deliveryTestApply(t, machine, index, fill)
				index++
			}
			if tc.full {
				fill := change
				fill.Targets = []DeliveryTargetDTO{{ClientID: "a"}}
				for i := 0; i < MaxDeliveryMessagesPerSession; i++ {
					deliveryTestApply(t, machine, index, fill)
					index++
				}
			}
			if tc.exhausted {
				snapshot, err := machine.Snapshot(index - 1)
				if err != nil {
					t.Fatal(err)
				}
				for i := range snapshot.DeliverySessions {
					if snapshot.DeliverySessions[i].ClientID == "a" {
						snapshot.DeliverySessions[i].NextSequence = ^uint64(0)
					}
				}
				machine = NewStateMachine()
				if err := machine.Restore(snapshot); err != nil {
					t.Fatal(err)
				}
			}
			if tc.ineligible {
				setupTarget("zero", 0)
				change.Targets = []DeliveryTargetDTO{{ClientID: "a"}, {ClientID: "missing"}, {ClientID: "zero"}}
			}
			if tc.quota {
				limit := uint64(1)
				if tc.full {
					limit = MaxDeliveryMessagesPerSession + 1
				}
				profile := tenantQuotaProfileForTest(1, tenantQuotaForTest("t", MaxTenantQuotaRetainedBytes, limit, MaxTenantQuotaOfflineQueueBytes, MaxTenantQuotaStorageBytes))
				result := applyTenantQuotaForTest(t, machine, index, profile)
				index++
				if result.Outcome != ApplyOutcomeApplied {
					t.Fatalf("quota apply: %+v", result)
				}
			}
			if tc.expired {
				change.ExpiresAtUnixMillis = 1
			}
			before := machine.DeliverySessions()
			result := machine.DeliveryPublishCapacity(change)
			if result.Fits != (tc.reason == "") || result.Reason != tc.reason {
				t.Fatalf("capacity = %+v; reason %q", result, tc.reason)
			}
			if tc.reason == DeliveryCapacityTenantQuota && (result.TenantQuota == nil || result.TenantQuota.Dimension != "offline_queue_messages") {
				t.Fatalf("quota dimension: %+v", result.TenantQuota)
			}
			if !reflect.DeepEqual(before, machine.DeliverySessions()) {
				t.Fatal("read-only estimate mutated delivery state")
			}
			if result.Fits {
				applied := deliveryTestApply(t, machine, index, change)
				var wantSaturated []string
				if tc.full || tc.exhausted {
					wantSaturated = []string{"a"}
				}
				if !reflect.DeepEqual(applied.Delivery.Saturated, wantSaturated) {
					t.Fatalf("apply saturation: %+v", applied.Delivery)
				}
			}
		})
	}
}
