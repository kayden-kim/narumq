package takeover_test

import (
	"errors"
	"sync"
	"testing"

	"narumq/internal/application/takeover"
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
