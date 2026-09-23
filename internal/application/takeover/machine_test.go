package takeover_test

import (
	"errors"
	"reflect"
	"sync"
	"testing"

	"narumq/internal/application/takeover"
	"narumq/internal/domain/delivery"
	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/routing"
	"narumq/internal/domain/session"
	"narumq/internal/domain/tenancy"
)

func mustTenantID(t *testing.T, raw string) tenancy.TenantID {
	t.Helper()
	tenantID, err := tenancy.NewTenantID(raw)
	if err != nil {
		t.Fatalf("NewTenantID(%q) error: %v", raw, err)
	}
	return tenantID
}

func mustSessionKey(t *testing.T, tenantRaw, clientID string) tenancy.SessionKey {
	t.Helper()
	key, err := tenancy.NewSessionKey(mustTenantID(t, tenantRaw), clientID)
	if err != nil {
		t.Fatalf("NewSessionKey(%q, %q) error: %v", tenantRaw, clientID, err)
	}
	return key
}

func TestStateMachine_OrderedApply(t *testing.T) {
	t.Parallel()

	sm := takeover.NewStateMachine()
	key := mustSessionKey(t, "tenant-1", "device-1")

	// 1. Initial acquire
	res1 := sm.Apply(10, takeover.AcquireOwnershipCommandDTO{
		SessionKey:    key,
		ExpectedEpoch: 0,
		OwnerNodeID:   "node-1",
	})
	if !res1.Applied || res1.Err != nil {
		t.Fatalf("initial acquire failed: %+v", res1)
	}
	if res1.Ownership.Epoch() != 1 {
		t.Fatalf("res1 Epoch = %d, want 1", res1.Ownership.Epoch())
	}
	if sm.LastAppliedIndex() != 10 {
		t.Fatalf("LastAppliedIndex = %d, want 10", sm.LastAppliedIndex())
	}

	// 2. Takeover by node-2
	res2 := sm.Apply(11, takeover.AcquireOwnershipCommandDTO{
		SessionKey:    key,
		ExpectedEpoch: 1,
		OwnerNodeID:   "node-2",
	})
	if !res2.Applied || res2.Err != nil {
		t.Fatalf("takeover failed: %+v", res2)
	}
	if res2.Ownership.Epoch() != 2 {
		t.Fatalf("res2 Epoch = %d, want 2", res2.Ownership.Epoch())
	}
	if res2.Ownership.OwnerNodeID() != "node-2" {
		t.Fatalf("res2 OwnerNodeID = %q, want node-2", res2.Ownership.OwnerNodeID())
	}
	if sm.LastAppliedIndex() != 11 {
		t.Fatalf("LastAppliedIndex = %d, want 11", sm.LastAppliedIndex())
	}

	// Verify current ownership query
	owner, ok := sm.Ownership(key)
	if !ok {
		t.Fatalf("Ownership(%v) not found", key)
	}
	if owner.Epoch() != 2 || owner.OwnerNodeID() != "node-2" {
		t.Fatalf("got owner %+v, want epoch 2 node-2", owner)
	}
}

func TestStateMachine_RejectedCommandAdvancesSequenceIndexWithoutMutatingMap(t *testing.T) {
	t.Parallel()

	sm := takeover.NewStateMachine()
	key := mustSessionKey(t, "tenant-1", "device-1")

	// 1. Initial acquire
	res1 := sm.Apply(10, takeover.AcquireOwnershipCommandDTO{
		SessionKey:    key,
		ExpectedEpoch: 0,
		OwnerNodeID:   "node-1",
	})
	if !res1.Applied || res1.Err != nil {
		t.Fatalf("initial acquire failed: %+v", res1)
	}

	// 2. Competing acquire with stale expected epoch (0 instead of 1) at sequence position 11.
	res2 := sm.Apply(11, takeover.AcquireOwnershipCommandDTO{
		SessionKey:    key,
		ExpectedEpoch: 0,
		OwnerNodeID:   "node-3",
	})
	if res2.Applied {
		t.Fatalf("expected res2 to be rejected, got applied: %+v", res2)
	}
	if !errors.Is(res2.Err, session.ErrStaleEpoch) {
		t.Fatalf("res2 Err = %v, want ErrStaleEpoch", res2.Err)
	}

	// lastAppliedIndex must have advanced to 11 because the committed log entry was processed
	if sm.LastAppliedIndex() != 11 {
		t.Fatalf("LastAppliedIndex = %d, want 11", sm.LastAppliedIndex())
	}

	// State of ownership must still be epoch 1 owned by node-1
	owner, ok := sm.Ownership(key)
	if !ok {
		t.Fatalf("Ownership(%v) missing", key)
	}
	if owner.Epoch() != 1 || owner.OwnerNodeID() != "node-1" {
		t.Fatalf("ownership was mutated after rejected apply: %+v", owner)
	}
}

func TestStateMachine_OutOfOrderCommit(t *testing.T) {
	t.Parallel()

	sm := takeover.NewStateMachine()
	key := mustSessionKey(t, "tenant-1", "device-1")

	// Apply at index 10
	res1 := sm.Apply(10, takeover.AcquireOwnershipCommandDTO{
		SessionKey:    key,
		ExpectedEpoch: 0,
		OwnerNodeID:   "node-1",
	})
	if !res1.Applied {
		t.Fatalf("apply at 10 failed: %+v", res1)
	}

	// Duplicate application sequence position 10.
	resDup := sm.Apply(10, takeover.AcquireOwnershipCommandDTO{
		SessionKey:    key,
		ExpectedEpoch: 1,
		OwnerNodeID:   "node-2",
	})
	if resDup.Applied || !errors.Is(resDup.Err, takeover.ErrOutOfOrderCommit) {
		t.Fatalf("duplicate commit error = %v, want ErrOutOfOrderCommit", resDup.Err)
	}

	// Decreasing application sequence position 9.
	resDec := sm.Apply(9, takeover.AcquireOwnershipCommandDTO{
		SessionKey:    key,
		ExpectedEpoch: 1,
		OwnerNodeID:   "node-2",
	})
	if resDec.Applied || !errors.Is(resDec.Err, takeover.ErrOutOfOrderCommit) {
		t.Fatalf("decreasing commit error = %v, want ErrOutOfOrderCommit", resDec.Err)
	}

	// Zero application sequence position.
	resZero := sm.Apply(0, takeover.AcquireOwnershipCommandDTO{
		SessionKey:    key,
		ExpectedEpoch: 1,
		OwnerNodeID:   "node-2",
	})
	if resZero.Applied || !errors.Is(resZero.Err, takeover.ErrOutOfOrderCommit) {
		t.Fatalf("zero commit error = %v, want ErrOutOfOrderCommit", resZero.Err)
	}

	// State machine lastAppliedIndex must still be 10
	if sm.LastAppliedIndex() != 10 {
		t.Fatalf("LastAppliedIndex = %d, want 10", sm.LastAppliedIndex())
	}
}

func TestStateMachine_CrossTenantIsolation(t *testing.T) {
	t.Parallel()

	sm := takeover.NewStateMachine()
	keyA := mustSessionKey(t, "tenant-A", "client-common")
	keyB := mustSessionKey(t, "tenant-B", "client-common")

	resA := sm.Apply(1, takeover.AcquireOwnershipCommandDTO{
		SessionKey:    keyA,
		ExpectedEpoch: 0,
		OwnerNodeID:   "node-1",
	})
	if !resA.Applied {
		t.Fatalf("tenant A apply failed: %+v", resA)
	}

	resB := sm.Apply(2, takeover.AcquireOwnershipCommandDTO{
		SessionKey:    keyB,
		ExpectedEpoch: 0,
		OwnerNodeID:   "node-2",
	})
	if !resB.Applied {
		t.Fatalf("tenant B apply failed: %+v", resB)
	}

	ownerA, okA := sm.Ownership(keyA)
	ownerB, okB := sm.Ownership(keyB)
	if !okA || !okB {
		t.Fatalf("missing ownership records: okA=%v, okB=%v", okA, okB)
	}
	if ownerA.OwnerNodeID() != "node-1" || ownerB.OwnerNodeID() != "node-2" {
		t.Fatalf("cross tenant interference: ownerA=%+v, ownerB=%+v", ownerA, ownerB)
	}
}

func TestStateMachine_Authorize(t *testing.T) {
	t.Parallel()

	sm := takeover.NewStateMachine()
	key := mustSessionKey(t, "tenant-1", "device-1")

	// Authorize before any ownership
	if err := sm.Authorize(key, "node-1", 1); !errors.Is(err, session.ErrInvalidOwnership) {
		t.Fatalf("authorize unowned session error = %v, want ErrInvalidOwnership", err)
	}

	// Apply ownership (epoch 1, node-1)
	sm.Apply(1, takeover.AcquireOwnershipCommandDTO{
		SessionKey:    key,
		ExpectedEpoch: 0,
		OwnerNodeID:   "node-1",
	})

	if err := sm.Authorize(key, "node-1", 1); err != nil {
		t.Fatalf("valid authorize failed: %v", err)
	}
	if err := sm.Authorize(key, "node-1", 0); !errors.Is(err, session.ErrStaleEpoch) {
		t.Fatalf("stale epoch error = %v, want ErrStaleEpoch", err)
	}
	if err := sm.Authorize(key, "node-2", 1); !errors.Is(err, session.ErrInvalidOwnership) {
		t.Fatalf("wrong owner error = %v, want ErrInvalidOwnership", err)
	}
}

func TestStateMachine_ConcurrentAccessRace(t *testing.T) {
	t.Parallel()

	sm := takeover.NewStateMachine()
	key := mustSessionKey(t, "tenant-1", "device-1")

	var wg sync.WaitGroup
	// 5 reader goroutines
	for range 5 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				_, _ = sm.Ownership(key)
				_ = sm.LastAppliedIndex()
				_ = sm.Authorize(key, "node-1", 1)
			}
		}()
	}

	// 1 sequential writer applying increasing commits
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := uint64(1); i <= 100; i++ {
			_ = sm.Apply(i, takeover.AcquireOwnershipCommandDTO{
				SessionKey:    key,
				ExpectedEpoch: session.Epoch(i - 1),
				OwnerNodeID:   "node-1",
			})
		}
	}()

	wg.Wait()
}

func TestStateMachine_SessionMutationsFenceWithoutChangingAggregate(t *testing.T) {
	t.Parallel()
	sm := takeover.NewStateMachine()
	key := mustSessionKey(t, "tenant-1", "device-1")
	missing := mustSessionKey(t, "tenant-1", "missing")
	if got := sm.PersistentSubscriptions(missing); got != nil {
		t.Fatalf("unowned subscriptions = %v, want nil", got)
	}
	if got := sm.InboundQoSInflight(missing); got != nil {
		t.Fatalf("unowned inbound = %v, want nil", got)
	}
	if got := sm.OutboundQoSInflight(missing); got != nil {
		t.Fatalf("unowned outbound = %v, want nil", got)
	}
	sub := takeover.PersistentSubscriptionCommandDTO{SessionKey: key, OwnerNodeID: "node-1", Epoch: 1,
		Subscription: mqtt.Subscription{Filter: "orders/#", QoS: mqtt.QoSAtLeastOnce}}
	in := takeover.QoSInflightCommandDTO{SessionKey: key, OwnerNodeID: "node-1", Epoch: 1,
		Publish: mqtt.Publish{Version: mqtt.ProtocolV5, QoS: mqtt.QoSAtLeastOnce, PacketID: 10, Topic: "orders/1", Payload: []byte("payload")}}
	out := in
	out.Publish.PacketID = 20
	if result := sm.ApplyPersistentSubscription(1, sub); result.Applied || !errors.Is(result.Err, session.ErrInvalidOwnership) {
		t.Fatalf("unowned subscription result = %+v", result)
	}
	if result := sm.Apply(2, takeover.AcquireOwnershipCommandDTO{SessionKey: key, OwnerNodeID: "node-1"}); !result.Applied {
		t.Fatalf("acquire result = %+v", result)
	}
	for _, apply := range []func(uint64) takeover.ApplyResultDTO{
		func(index uint64) takeover.ApplyResultDTO { return sm.ApplyPersistentSubscription(index, sub) },
		func(index uint64) takeover.ApplyResultDTO { return sm.ApplyInboundQoSInflight(index, in) },
		func(index uint64) takeover.ApplyResultDTO { return sm.ApplyOutboundQoSInflight(index, out) },
	} {
		if result := apply(sm.LastAppliedIndex() + 1); !result.Applied || result.Err != nil {
			t.Fatalf("authorized mutation result = %+v", result)
		}
	}
	if got := sm.InboundQoSInflight(key); !reflect.DeepEqual(got, []mqtt.PacketIdentifier{10}) {
		t.Fatalf("inbound IDs = %v", got)
	}
	if got := sm.OutboundQoSInflight(key); !reflect.DeepEqual(got, []mqtt.PacketIdentifier{20}) {
		t.Fatalf("outbound IDs = %v", got)
	}
	if result := sm.Apply(6, takeover.AcquireOwnershipCommandDTO{SessionKey: key, OwnerNodeID: "node-2", ExpectedEpoch: 1}); !result.Applied {
		t.Fatalf("takeover result = %+v", result)
	}
	owner, _ := sm.Ownership(key)
	subs := sm.PersistentSubscriptions(key)
	inIDs := sm.InboundQoSInflight(key)
	outIDs := sm.OutboundQoSInflight(key)
	sub.Subscription.QoS = mqtt.QoSExactlyOnce
	in.Publish.PacketID = 11
	out.Publish.PacketID = 21
	for _, apply := range []func(uint64) takeover.ApplyResultDTO{
		func(index uint64) takeover.ApplyResultDTO { return sm.ApplyPersistentSubscription(index, sub) },
		func(index uint64) takeover.ApplyResultDTO { return sm.ApplyInboundQoSInflight(index, in) },
		func(index uint64) takeover.ApplyResultDTO { return sm.ApplyOutboundQoSInflight(index, out) },
	} {
		if result := apply(sm.LastAppliedIndex() + 1); result.Applied || !errors.Is(result.Err, session.ErrStaleEpoch) {
			t.Fatalf("stale mutation result = %+v", result)
		}
	}
	if sm.LastAppliedIndex() != 9 {
		t.Fatalf("consumed sequence = %d, want 9", sm.LastAppliedIndex())
	}
	current, _ := sm.Ownership(key)
	if current != owner || owner.CommitIndex() != 6 {
		t.Fatalf("ownership changed: before %v after %v", owner, current)
	}
	if !reflect.DeepEqual(sm.PersistentSubscriptions(key), subs) || !reflect.DeepEqual(sm.InboundQoSInflight(key), inIDs) || !reflect.DeepEqual(sm.OutboundQoSInflight(key), outIDs) {
		t.Fatal("stale command changed aggregate")
	}
	subs[0].QoS = mqtt.QoSExactlyOnce
	inIDs[0], outIDs[0] = 42, 42
	if sm.PersistentSubscriptions(key)[0].QoS != mqtt.QoSAtLeastOnce || sm.InboundQoSInflight(key)[0] != 10 || sm.OutboundQoSInflight(key)[0] != 20 {
		t.Fatal("query result aliases authoritative state")
	}
}

func TestStateMachine_SessionMutationValidationAndSequence(t *testing.T) {
	t.Parallel()
	sm := takeover.NewStateMachine()
	key := mustSessionKey(t, "tenant-1", "device-1")
	if result := sm.Apply(1, takeover.AcquireOwnershipCommandDTO{SessionKey: key, OwnerNodeID: "node-1"}); !result.Applied {
		t.Fatalf("acquire result = %+v", result)
	}
	sub := takeover.PersistentSubscriptionCommandDTO{SessionKey: key, OwnerNodeID: "node-1", Epoch: 1,
		Subscription: mqtt.Subscription{Filter: "z/#", QoS: mqtt.QoSAtLeastOnce}}
	in := takeover.QoSInflightCommandDTO{SessionKey: key, OwnerNodeID: "node-1", Epoch: 1,
		Publish: mqtt.Publish{Version: mqtt.ProtocolV5, QoS: mqtt.QoSAtLeastOnce, PacketID: 10}}
	out := in
	out.Publish.PacketID = 20
	for _, apply := range []func(uint64) takeover.ApplyResultDTO{
		func(i uint64) takeover.ApplyResultDTO { return sm.ApplyPersistentSubscription(i, sub) },
		func(i uint64) takeover.ApplyResultDTO { return sm.ApplyInboundQoSInflight(i, in) },
		func(i uint64) takeover.ApplyResultDTO { return sm.ApplyOutboundQoSInflight(i, out) },
	} {
		if result := apply(sm.LastAppliedIndex() + 1); !result.Applied {
			t.Fatalf("baseline result = %+v", result)
		}
	}
	owner, _ := sm.Ownership(key)
	type attempt struct {
		name  string
		apply func(uint64) takeover.ApplyResultDTO
		want  error
	}
	wrongSub := sub
	wrongSub.OwnerNodeID = "node-2"
	futureIn := in
	futureIn.Epoch = 2
	priorOut := out
	priorOut.Epoch = 0
	invalidSub := sub
	invalidSub.Subscription.Filter = "bad/#/filter"
	invalidQoS := sub
	invalidQoS.Subscription.QoS = 3
	invalidRetain := sub
	invalidRetain.Subscription.RetainHandling = 3
	invalidInQoS := in
	invalidInQoS.Publish.QoS = mqtt.QoSAtMostOnce
	invalidOutID := out
	invalidOutID.Publish.PacketID = 0
	missing := sub
	missing.SessionKey = mustSessionKey(t, "tenant-1", "missing")
	for _, tc := range []attempt{
		{"wrong owner", func(i uint64) takeover.ApplyResultDTO { return sm.ApplyPersistentSubscription(i, wrongSub) }, session.ErrInvalidOwnership},
		{"future epoch", func(i uint64) takeover.ApplyResultDTO { return sm.ApplyInboundQoSInflight(i, futureIn) }, session.ErrStaleEpoch},
		{"prior epoch", func(i uint64) takeover.ApplyResultDTO { return sm.ApplyOutboundQoSInflight(i, priorOut) }, session.ErrStaleEpoch},
		{"missing owner", func(i uint64) takeover.ApplyResultDTO { return sm.ApplyPersistentSubscription(i, missing) }, session.ErrInvalidOwnership},
		{"invalid filter", func(i uint64) takeover.ApplyResultDTO { return sm.ApplyPersistentSubscription(i, invalidSub) }, routing.ErrInvalidTopicFilter},
		{"invalid subscription qos", func(i uint64) takeover.ApplyResultDTO { return sm.ApplyPersistentSubscription(i, invalidQoS) }, routing.ErrInvalidQoS},
		{"invalid retain handling", func(i uint64) takeover.ApplyResultDTO { return sm.ApplyPersistentSubscription(i, invalidRetain) }, routing.ErrInvalidRetainHandling},
		{"invalid inbound qos", func(i uint64) takeover.ApplyResultDTO { return sm.ApplyInboundQoSInflight(i, invalidInQoS) }, delivery.ErrInvalidQoS},
		{"invalid outbound packet id", func(i uint64) takeover.ApplyResultDTO { return sm.ApplyOutboundQoSInflight(i, invalidOutID) }, delivery.ErrPacketIdentifierMismatch},
	} {
		index := sm.LastAppliedIndex() + 1
		if result := tc.apply(index); result.Applied || !errors.Is(result.Err, tc.want) {
			t.Fatalf("%s result = %+v, want %v", tc.name, result, tc.want)
		}
		if sm.LastAppliedIndex() != index {
			t.Fatalf("%s did not consume committed position %d", tc.name, index)
		}
	}
	for _, apply := range []func(uint64) takeover.ApplyResultDTO{
		func(i uint64) takeover.ApplyResultDTO { return sm.ApplyPersistentSubscription(i, sub) },
		func(i uint64) takeover.ApplyResultDTO { return sm.ApplyInboundQoSInflight(i, in) },
		func(i uint64) takeover.ApplyResultDTO { return sm.ApplyOutboundQoSInflight(i, out) },
	} {
		for _, index := range []uint64{0, 1, sm.LastAppliedIndex()} {
			if result := apply(index); result.Applied || !errors.Is(result.Err, takeover.ErrOutOfOrderCommit) {
				t.Fatalf("out-of-order %d result = %+v", index, result)
			}
		}
	}
	current, _ := sm.Ownership(key)
	if current != owner || !reflect.DeepEqual(sm.PersistentSubscriptions(key), []mqtt.Subscription{sub.Subscription}) ||
		!reflect.DeepEqual(sm.InboundQoSInflight(key), []mqtt.PacketIdentifier{10}) ||
		!reflect.DeepEqual(sm.OutboundQoSInflight(key), []mqtt.PacketIdentifier{20}) {
		t.Fatal("invalid or duplicate commands changed aggregate")
	}
	sub.Subscription.Filter = "a/#"
	in.Publish.PacketID = 5
	out.Publish.PacketID = 3
	for _, apply := range []func(uint64) takeover.ApplyResultDTO{
		func(i uint64) takeover.ApplyResultDTO { return sm.ApplyPersistentSubscription(i, sub) },
		func(i uint64) takeover.ApplyResultDTO { return sm.ApplyInboundQoSInflight(i, in) },
		func(i uint64) takeover.ApplyResultDTO { return sm.ApplyOutboundQoSInflight(i, out) },
	} {
		if result := apply(sm.LastAppliedIndex() + 1); !result.Applied {
			t.Fatalf("sorted-state result = %+v", result)
		}
	}
	if got := sm.PersistentSubscriptions(key); !reflect.DeepEqual(got, []mqtt.Subscription{sub.Subscription, {Filter: "z/#", QoS: mqtt.QoSAtLeastOnce}}) {
		t.Fatalf("subscription order = %v", got)
	}
	if got := sm.InboundQoSInflight(key); !reflect.DeepEqual(got, []mqtt.PacketIdentifier{5, 10}) {
		t.Fatalf("inbound order = %v", got)
	}
	if got := sm.OutboundQoSInflight(key); !reflect.DeepEqual(got, []mqtt.PacketIdentifier{3, 20}) {
		t.Fatalf("outbound order = %v", got)
	}
}
