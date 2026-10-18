package membership

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func tenantQuotaForTest(tenantID string, retainedBytes, queueMessages, queueBytes, storageBytes uint64) TenantStorageQuotaDTO {
	return TenantStorageQuotaDTO{
		TenantID: tenantID, RetainedBytes: retainedBytes, OfflineQueueMessages: queueMessages,
		OfflineQueueBytes: queueBytes, StorageBytes: storageBytes,
	}
}

func tenantQuotaProfileForTest(version uint64, tenants ...TenantStorageQuotaDTO) TenantQuotaProfileDTO {
	profile := DefaultTenantQuotaProfile()
	profile.Version = version
	profile.Tenants = tenants
	return profile
}

func applyTenantQuotaForTest(t *testing.T, machine *StateMachine, index uint64, profile TenantQuotaProfileDTO) ApplyResultDTO {
	t.Helper()
	result, err := machine.Apply(index, CommandDTO{
		SchemaMajor: SchemaMajor, SchemaMinor: SchemaMinor,
		OperationID: fmt.Sprintf("00000000-0000-4000-8000-%012d", index),
		TenantQuota: &TenantQuotaChangeDTO{Profile: profile},
	})
	if err != nil {
		t.Fatalf("Apply(tenant quota %d): %v", index, err)
	}
	return result
}

func applyQuotaOwnershipForTest(t *testing.T, machine *StateMachine, index uint64, tenantID, clientID string) {
	t.Helper()
	applyMustSucceed(t, machine, index, CommandDTO{
		SchemaMajor: SchemaMajor, SchemaMinor: SchemaMinor,
		OperationID: fmt.Sprintf("00000000-0000-4000-8000-%012d", index),
		Ownership:   &OwnershipChangeDTO{TenantID: tenantID, ClientID: clientID, OwnerNodeID: 1, Epoch: 1, Action: OwnershipActionAcquire},
	})
}

// Traceability: TEN-08; docs/specs/cluster-session-and-delivery.md §11.16.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-098
func TestTenantQuotaProfileValidationVersioningAndOwnership(t *testing.T) {
	profile := DefaultTenantQuotaProfile()
	if err := ValidateTenantQuotaProfile(profile); err != nil {
		t.Fatalf("ValidateTenantQuotaProfile(default): %v", err)
	}
	if profile.Version != 0 || profile.Default.RetainedBytes != MaxTenantQuotaRetainedBytes || profile.Default.OfflineQueueMessages != MaxTenantQuotaOfflineQueueMessages || profile.Default.OfflineQueueBytes != MaxTenantQuotaOfflineQueueBytes || profile.Default.StorageBytes != MaxTenantQuotaStorageBytes {
		t.Fatalf("default quota profile = %+v", profile)
	}

	valid := tenantQuotaProfileForTest(5, tenantQuotaForTest("tenant-a", 1, 1, 1, 1))
	machine := NewStateMachine()
	if result := applyTenantQuotaForTest(t, machine, 1, valid); result.Outcome != ApplyOutcomeApplied {
		t.Fatalf("initial profile result = %+v", result)
	}
	if result := applyTenantQuotaForTest(t, machine, 2, valid); result.Outcome != ApplyOutcomeApplied {
		t.Fatalf("identical same-version profile result = %+v", result)
	}
	changed := tenantQuotaProfileForTest(5, tenantQuotaForTest("tenant-a", 2, 1, 1, 1))
	if result := applyTenantQuotaForTest(t, machine, 3, changed); result.Outcome != ApplyOutcomeConflict || result.Conflict != ConflictTenantQuotaStale {
		t.Fatalf("changed same-version profile result = %+v", result)
	}
	older := valid
	older.Version = 4
	if result := applyTenantQuotaForTest(t, machine, 4, older); result.Outcome != ApplyOutcomeConflict || result.Conflict != ConflictTenantQuotaStale {
		t.Fatalf("lower-version profile result = %+v", result)
	}
	got, exists := machine.TenantQuotaProfile()
	if !exists || !EqualTenantQuotaProfile(got, valid) {
		t.Fatalf("committed profile = (%+v, %t), want %+v", got, exists, valid)
	}
	got.Tenants[0].TenantID = "caller-mutation"
	if version, exists := machine.TenantQuotaProfileVersion(); !exists || version != 5 {
		t.Fatalf("profile version = (%d, %t), want (5, true)", version, exists)
	}
	stored, _ := machine.TenantQuotaProfile()
	if stored.Tenants[0].TenantID != "tenant-a" {
		t.Fatalf("mutating getter result changed committed profile: %+v", stored)
	}

	invalid := []struct {
		name    string
		profile TenantQuotaProfileDTO
	}{
		{name: "default tenant id", profile: func() TenantQuotaProfileDTO {
			p := DefaultTenantQuotaProfile()
			p.Default.TenantID = "tenant"
			return p
		}()},
		{name: "zero quota", profile: tenantQuotaProfileForTest(1, tenantQuotaForTest("tenant-a", 0, 1, 1, 1))},
		{name: "retained quota above capacity", profile: tenantQuotaProfileForTest(1, tenantQuotaForTest("tenant-a", uint64(MaxTenantQuotaRetainedBytes)+1, 1, 1, 1))},
		{name: "invalid empty tenant id", profile: tenantQuotaProfileForTest(1, tenantQuotaForTest("", 1, 1, 1, 1))},
		{name: "tenant id exceeds bound", profile: tenantQuotaProfileForTest(1, tenantQuotaForTest(strings.Repeat("a", maxTenantIDBytes+1), 1, 1, 1, 1))},
		{name: "unsorted tenants", profile: tenantQuotaProfileForTest(1, tenantQuotaForTest("tenant-b", 1, 1, 1, 1), tenantQuotaForTest("tenant-a", 1, 1, 1, 1))},
		{name: "duplicate tenant", profile: tenantQuotaProfileForTest(1, tenantQuotaForTest("tenant-a", 1, 1, 1, 1), tenantQuotaForTest("tenant-a", 1, 1, 1, 1))},
	}
	for _, test := range invalid {
		t.Run(test.name, func(t *testing.T) {
			if err := ValidateTenantQuotaProfile(test.profile); !errors.Is(err, ErrInvalidCommand) {
				t.Fatalf("ValidateTenantQuotaProfile() = %v, want ErrInvalidCommand", err)
			}
		})
	}

	maxTenants := make([]TenantStorageQuotaDTO, MaxTenantQuotaTenants)
	for index := range maxTenants {
		maxTenants[index] = tenantQuotaForTest(fmt.Sprintf("tenant-%04d", index), 1, 1, 1, 1)
	}
	if err := ValidateTenantQuotaProfile(tenantQuotaProfileForTest(1, maxTenants...)); err != nil {
		t.Fatalf("profile at tenant-count maximum: %v", err)
	}
	tooMany := append(append([]TenantStorageQuotaDTO(nil), maxTenants...), tenantQuotaForTest("tenant-1024", 1, 1, 1, 1))
	if err := ValidateTenantQuotaProfile(tenantQuotaProfileForTest(1, tooMany...)); !errors.Is(err, ErrInvalidCommand) {
		t.Fatalf("profile above tenant-count maximum = %v, want ErrInvalidCommand", err)
	}
}

// Traceability: TEN-08; docs/specs/cluster-session-and-delivery.md §4 and §11.16.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-098
func TestTenantRetainedQuotaReductionPreservesExistingValues(t *testing.T) {
	machine := NewStateMachine()
	oldChange := retainedReplaceForTest("t", "status", "publisher", []byte("large"), 1, 0, 1000)
	smallChange := retainedReplaceForTest("t", "status", "publisher", []byte("x"), 1, 0, 1000)
	oldSize := uint64(retainedStateBytes(RetainedStateDTO{TenantID: oldChange.TenantID, Topic: oldChange.Topic, PublisherClientID: oldChange.PublisherClientID, Payload: oldChange.Payload}))
	smallSize := uint64(retainedStateBytes(RetainedStateDTO{TenantID: smallChange.TenantID, Topic: smallChange.Topic, PublisherClientID: smallChange.PublisherClientID, Payload: smallChange.Payload}))

	limits := DefaultTenantQuotaProfile().Default
	limits.TenantID = "t"
	limits.RetainedBytes = oldSize
	if result := applyTenantQuotaForTest(t, machine, 1, tenantQuotaProfileForTest(1, limits)); result.Outcome != ApplyOutcomeApplied {
		t.Fatalf("install initial profile = %+v", result)
	}
	if result := applyRetainedForTest(t, machine, 2, oldChange); result.Outcome != ApplyOutcomeApplied {
		t.Fatalf("store retained value = %+v", result)
	}
	limits.RetainedBytes = smallSize
	if result := applyTenantQuotaForTest(t, machine, 3, tenantQuotaProfileForTest(2, limits)); result.Outcome != ApplyOutcomeApplied {
		t.Fatalf("lower retained quota = %+v", result)
	}

	rejected := applyRetainedForTest(t, machine, 4, retainedReplaceForTest("t", "other", "publisher", []byte("x"), 1, 0, 1000))
	if rejected.Outcome != ApplyOutcomeConflict || rejected.Conflict != ConflictRetainedCapacity || rejected.TenantQuota == nil || rejected.TenantQuota.Dimension != TenantQuotaRetainedBytes || rejected.TenantQuota.ProfileVersion != 2 {
		t.Fatalf("retained over-quota result = %+v", rejected)
	}
	if got := machine.RetainedMessages(); len(got) != 1 || string(got[0].Payload) != "large" {
		t.Fatalf("lowering the quota evicted or changed the existing value: %+v", got)
	}
	if result := applyRetainedForTest(t, machine, 5, smallChange); result.Outcome != ApplyOutcomeApplied {
		t.Fatalf("shrinking an over-quota value = %+v", result)
	}
	if rejected := applyRetainedForTest(t, machine, 6, retainedReplaceForTest("t", "other", "publisher", []byte("x"), 1, 0, 1000)); rejected.TenantQuota == nil || rejected.TenantQuota.Dimension != TenantQuotaRetainedBytes {
		t.Fatalf("retained addition after shrinking = %+v", rejected)
	}
	if result := applyRetainedForTest(t, machine, 7, retainedReplaceForTest("other", "status", "publisher", []byte("x"), 1, 0, 1000)); result.Outcome != ApplyOutcomeApplied {
		t.Fatalf("one tenant's quota blocked another tenant: %+v", result)
	}
}

// Traceability: TEN-08; docs/specs/cluster-session-and-delivery.md §4 and §11.16.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-098
func TestTenantOfflineQueueQuotaDimensionsAndClaimRelease(t *testing.T) {
	machine := NewStateMachine()
	message := OfflineMessageDTO{Topic: "a/b", Payload: []byte("xy")}
	messageBytes := uint64(offlineMessageBytes("t", "c", message))
	limits := DefaultTenantQuotaProfile().Default
	limits.TenantID = "t"
	limits.OfflineQueueMessages = 1
	limits.OfflineQueueBytes = messageBytes
	limits.StorageBytes = messageBytes
	if result := applyTenantQuotaForTest(t, machine, 1, tenantQuotaProfileForTest(1, limits)); result.Outcome != ApplyOutcomeApplied {
		t.Fatalf("install initial profile = %+v", result)
	}
	applyQuotaOwnershipForTest(t, machine, 2, "t", "c")
	detach := OfflineChangeDTO{TenantID: "t", ClientID: "c", OwnerNodeID: 1, Epoch: 1, Action: OfflineActionDetach, ProposedAtUnixMillis: 1000, Messages: []OfflineMessageDTO{message}}
	if result := applyOfflineForTest(t, machine, 3, detach); result.Outcome != ApplyOutcomeApplied {
		t.Fatalf("initial offline detach = %+v", result)
	}
	enqueue := OfflineChangeDTO{TenantID: "t", ClientID: "c", Epoch: 1, Action: OfflineActionEnqueue, ProposedAtUnixMillis: 1000, Messages: []OfflineMessageDTO{message}}
	rejected := applyOfflineForTest(t, machine, 4, enqueue)
	if rejected.Conflict != ConflictOfflineCapacity || rejected.TenantQuota == nil || rejected.TenantQuota.Dimension != TenantQuotaOfflineQueueMessages {
		t.Fatalf("offline message-count rejection = %+v", rejected)
	}

	limits.OfflineQueueMessages = 2
	limits.OfflineQueueBytes = messageBytes
	limits.StorageBytes = 100
	if result := applyTenantQuotaForTest(t, machine, 5, tenantQuotaProfileForTest(2, limits)); result.Outcome != ApplyOutcomeApplied {
		t.Fatalf("raise message quota = %+v", result)
	}
	rejected = applyOfflineForTest(t, machine, 6, enqueue)
	if rejected.Conflict != ConflictOfflineCapacity || rejected.TenantQuota == nil || rejected.TenantQuota.Dimension != TenantQuotaOfflineQueueBytes {
		t.Fatalf("offline byte rejection = %+v", rejected)
	}

	limits.OfflineQueueBytes = 100
	limits.StorageBytes = messageBytes
	if result := applyTenantQuotaForTest(t, machine, 7, tenantQuotaProfileForTest(3, limits)); result.Outcome != ApplyOutcomeApplied {
		t.Fatalf("lower aggregate storage quota = %+v", result)
	}
	rejected = applyOfflineForTest(t, machine, 8, enqueue)
	if rejected.Conflict != ConflictOfflineCapacity || rejected.TenantQuota == nil || rejected.TenantQuota.Dimension != TenantQuotaStorageBytes {
		t.Fatalf("offline aggregate-storage rejection = %+v", rejected)
	}

	claim := OfflineChangeDTO{TenantID: "t", ClientID: "c", OwnerNodeID: 1, Epoch: 1, Action: OfflineActionClaim, ProposedAtUnixMillis: 1000}
	claimed := applyOfflineForTest(t, machine, 9, claim)
	if claimed.Outcome != ApplyOutcomeApplied || claimed.OfflineClaim == nil || !claimed.OfflineClaim.Found || len(claimed.OfflineClaim.Messages) != 1 {
		t.Fatalf("offline CLAIM = %+v", claimed)
	}
	if result := applyOfflineForTest(t, machine, 10, detach); result.Outcome != ApplyOutcomeApplied {
		t.Fatalf("new detach after CLAIM did not regain quota: %+v", result)
	}
}

// Traceability: TEN-08; docs/specs/cluster-session-and-delivery.md §4 and §11.16.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-098
func TestTenantDeliveryQuotaSaturatesTargetAndCompletionReleasesUsage(t *testing.T) {
	machine := NewStateMachine()
	limits := DefaultTenantQuotaProfile().Default
	limits.TenantID = "t"
	limits.OfflineQueueMessages = 1
	if result := applyTenantQuotaForTest(t, machine, 1, tenantQuotaProfileForTest(1, limits)); result.Outcome != ApplyOutcomeApplied {
		t.Fatalf("install profile = %+v", result)
	}
	deliveryTestAcquire(t, machine, 2, "subscriber")
	deliveryTestSubscribe(t, machine, 3, "subscriber", "orders/#", 1)
	publish := DeliveryChangeDTO{TenantID: "t", ClientID: "publisher", Action: DeliveryActionPublish, Topic: "orders/new", Payload: []byte("payload"), PublishQoS: 1, ProposedAtUnixMillis: 1000, Targets: []DeliveryTargetDTO{{ClientID: "subscriber"}}}
	first := deliveryTestApply(t, machine, 4, publish)
	if len(first.Delivery.Enqueued) != 1 || first.TenantQuota != nil {
		t.Fatalf("first delivery enqueue = %+v", first)
	}
	second := deliveryTestApply(t, machine, 5, publish)
	if second.Outcome != ApplyOutcomeApplied || !reflect.DeepEqual(second.Delivery.Saturated, []string{"subscriber"}) || second.TenantQuota == nil || second.TenantQuota.Dimension != TenantQuotaOfflineQueueMessages || second.TenantQuota.ProfileVersion != 1 {
		t.Fatalf("tenant-saturated delivery target = %+v", second)
	}

	deliveryTestApply(t, machine, 6, DeliveryChangeDTO{TenantID: "t", ClientID: "subscriber", OwnerNodeID: 1, Epoch: 1, Action: DeliveryActionSend, Sequence: 1, PacketID: 10, ProposedAtUnixMillis: 1001})
	deliveryTestApply(t, machine, 7, DeliveryChangeDTO{TenantID: "t", ClientID: "subscriber", OwnerNodeID: 1, Epoch: 1, Action: DeliveryActionComplete, PacketID: 10, ProposedAtUnixMillis: 1002})
	third := deliveryTestApply(t, machine, 8, publish)
	if len(third.Delivery.Enqueued) != 1 || third.TenantQuota != nil {
		t.Fatalf("delivery enqueue after COMPLETE = %+v", third)
	}
}

// Traceability: TEN-08; docs/specs/cluster-session-and-delivery.md §10 and §11.16.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-098
func TestTenantStorageUsageAggregatesQueuesAndInboundAcrossSnapshot(t *testing.T) {
	machine := NewStateMachine()
	key := sessionKey{tenantID: "t", clientID: "subscriber"}
	retainedChange := retainedReplaceForTest("t", "status", "publisher", []byte("retained"), 1, 0, 1000)
	retainedBytes := retainedStateBytes(RetainedStateDTO{TenantID: retainedChange.TenantID, Topic: retainedChange.Topic, PublisherClientID: retainedChange.PublisherClientID, Payload: retainedChange.Payload})
	offlineMessage := OfflineMessageDTO{Topic: "offline", Payload: []byte("offline-payload")}
	offlineBytes := offlineMessageBytes("t", "offline-client", offlineMessage)
	outbound := DeliveryMessageDTO{Sequence: 1, Topic: "orders/new", Payload: []byte("outbound-payload"), QoS: 1, Phase: DeliveryPhaseQueued}
	outboundBytes := deliveryMessageBytes(key, outbound)
	inbound := DeliveryInboundDTO{PacketID: 42, Topic: "incoming", Payload: []byte("inbound-payload")}
	inboundBytes := deliveryInboundBytes(key, inbound)
	totalBytes := retainedBytes + offlineBytes + outboundBytes + inboundBytes

	limits := DefaultTenantQuotaProfile().Default
	limits.TenantID = "t"
	limits.OfflineQueueMessages = 10
	limits.OfflineQueueBytes = uint64(MaxTenantQuotaOfflineQueueBytes)
	limits.StorageBytes = uint64(totalBytes - 1)
	if result := applyTenantQuotaForTest(t, machine, 1, tenantQuotaProfileForTest(1, limits)); result.Outcome != ApplyOutcomeApplied {
		t.Fatalf("install initial profile = %+v", result)
	}
	applyQuotaOwnershipForTest(t, machine, 2, "t", "offline-client")
	deliveryTestAcquire(t, machine, 3, "subscriber")
	deliveryTestSubscribe(t, machine, 4, "subscriber", "orders/#", 1)
	if result := applyRetainedForTest(t, machine, 5, retainedChange); result.Outcome != ApplyOutcomeApplied {
		t.Fatalf("store retained value = %+v", result)
	}
	if result := applyOfflineForTest(t, machine, 6, OfflineChangeDTO{TenantID: "t", ClientID: "offline-client", OwnerNodeID: 1, Epoch: 1, Action: OfflineActionDetach, ProposedAtUnixMillis: 1000, Messages: []OfflineMessageDTO{offlineMessage}}); result.Outcome != ApplyOutcomeApplied {
		t.Fatalf("store offline message = %+v", result)
	}
	publish := deliveryTestApply(t, machine, 7, DeliveryChangeDTO{TenantID: "t", ClientID: "publisher", Action: DeliveryActionPublish, Topic: outbound.Topic, Payload: outbound.Payload, PublishQoS: 1, ProposedAtUnixMillis: 1000, Targets: []DeliveryTargetDTO{{ClientID: "subscriber"}}})
	if len(publish.Delivery.Enqueued) != 1 {
		t.Fatalf("store outbound message = %+v", publish.Delivery)
	}

	usage := machine.tenantUsage["t"]
	if usage.retainedBytes != retainedBytes || usage.queueMessages != 2 || usage.queueBytes != offlineBytes+outboundBytes || usage.storageBytes != retainedBytes+offlineBytes+outboundBytes {
		t.Fatalf("per-tenant usage before inbound RECEIVE = %+v; expected retained=%d queue messages=2 queue bytes=%d storage=%d", usage, retainedBytes, offlineBytes+outboundBytes, retainedBytes+offlineBytes+outboundBytes)
	}
	receive := DeliveryChangeDTO{TenantID: "t", ClientID: "subscriber", OwnerNodeID: 1, Epoch: 1, Action: DeliveryActionReceive, PacketID: inbound.PacketID, Topic: inbound.Topic, Payload: inbound.Payload, ProposedAtUnixMillis: 1001}
	rejected := deliveryTestApplyResult(t, machine, 8, receive)
	if rejected.Conflict != ConflictDeliveryCapacity || rejected.TenantQuota == nil || rejected.TenantQuota.Dimension != TenantQuotaStorageBytes || rejected.TenantQuota.ProfileVersion != 1 {
		t.Fatalf("aggregate inbound-storage rejection = %+v", rejected)
	}

	limits.StorageBytes = uint64(totalBytes)
	if result := applyTenantQuotaForTest(t, machine, 9, tenantQuotaProfileForTest(2, limits)); result.Outcome != ApplyOutcomeApplied {
		t.Fatalf("raise aggregate storage quota = %+v", result)
	}
	if result := deliveryTestApply(t, machine, 10, receive); result.Outcome != ApplyOutcomeApplied {
		t.Fatalf("inbound RECEIVE at aggregate limit = %+v", result)
	}
	snapshot, err := machine.Snapshot(10)
	if err != nil {
		t.Fatalf("Snapshot(): %v", err)
	}
	if snapshot.TenantQuotaProfile == nil || !EqualTenantQuotaProfile(*snapshot.TenantQuotaProfile, tenantQuotaProfileForTest(2, limits)) {
		t.Fatalf("snapshot quota profile = %+v", snapshot.TenantQuotaProfile)
	}
	if err := validSnapshotCheckForTest(snapshot); err != nil {
		t.Fatalf("validSnapshot(snapshot): %v", err)
	}

	restored := NewStateMachine()
	if err := restored.Restore(snapshot); err != nil {
		t.Fatalf("Restore(snapshot): %v", err)
	}
	profile, exists := restored.TenantQuotaProfile()
	if !exists || !EqualTenantQuotaProfile(profile, *snapshot.TenantQuotaProfile) {
		t.Fatalf("restored profile = (%+v, %t)", profile, exists)
	}
	if got := restored.tenantUsage["t"]; got != machine.tenantUsage["t"] {
		t.Fatalf("restored usage = %+v, want %+v", got, machine.tenantUsage["t"])
	}
	additional := receive
	additional.PacketID = 43
	additional.Payload = []byte("x")
	if result := deliveryTestApplyResult(t, restored, 11, additional); result.Conflict != ConflictDeliveryCapacity || result.TenantQuota == nil || result.TenantQuota.Dimension != TenantQuotaStorageBytes || result.TenantQuota.ProfileVersion != 2 {
		t.Fatalf("post-restore quota rejection = %+v", result)
	}

	changed := cloneSnapshot(snapshot)
	changed.TenantQuotaProfile.Tenants[0].StorageBytes--
	if err := restored.Restore(changed); !errors.Is(err, ErrSnapshotIndexMismatch) {
		t.Fatalf("same-index snapshot with a different quota profile = %v, want ErrSnapshotIndexMismatch", err)
	}
	invalid := cloneSnapshot(snapshot)
	invalid.ObjectCount++
	if err := validSnapshotCheckForTest(invalid); !errors.Is(err, ErrInvalidSnapshot) {
		t.Fatalf("snapshot with incorrect quota-aware object count = %v, want ErrInvalidSnapshot", err)
	}
}
