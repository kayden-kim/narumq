package session_test

import (
	"errors"
	"math"
	"testing"

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

func TestOwnership_InitialAcquire(t *testing.T) {
	t.Parallel()

	key := mustSessionKey(t, "tenant-a", "client-1")

	// Successful initial acquire with expected=0 at application sequence position 100.
	rec, err := session.AcquireOwnership(nil, key, 0, "node-1", 100)
	if err != nil {
		t.Fatalf("initial acquire failed: %v", err)
	}
	if rec.SessionKey() != key {
		t.Fatalf("SessionKey = %v, want %v", rec.SessionKey(), key)
	}
	if rec.OwnerNodeID() != "node-1" {
		t.Fatalf("OwnerNodeID = %q, want %q", rec.OwnerNodeID(), "node-1")
	}
	if rec.Epoch() != 1 {
		t.Fatalf("Epoch = %d, want 1", rec.Epoch())
	}
	if rec.CommitIndex() != 100 {
		t.Fatalf("CommitIndex = %d, want 100", rec.CommitIndex())
	}

	// Initial acquire with non-zero expected epoch must fail with ErrStaleEpoch
	_, err = session.AcquireOwnership(nil, key, 1, "node-1", 100)
	if !errors.Is(err, session.ErrStaleEpoch) {
		t.Fatalf("initial acquire with expected=1 error = %v, want ErrStaleEpoch", err)
	}
}

func TestOwnership_SubsequentAcquire(t *testing.T) {
	t.Parallel()

	key := mustSessionKey(t, "tenant-a", "client-1")
	first, err := session.AcquireOwnership(nil, key, 0, "node-1", 100)
	if err != nil {
		t.Fatalf("initial acquire failed: %v", err)
	}

	// Takeover by node-2 with expected=1 and greater application sequence position.
	second, err := session.AcquireOwnership(&first, key, 1, "node-2", 105)
	if err != nil {
		t.Fatalf("takeover acquire failed: %v", err)
	}
	if second.OwnerNodeID() != "node-2" {
		t.Fatalf("OwnerNodeID = %q, want node-2", second.OwnerNodeID())
	}
	if second.Epoch() != 2 {
		t.Fatalf("Epoch = %d, want 2", second.Epoch())
	}
	if second.CommitIndex() != 105 {
		t.Fatalf("CommitIndex = %d, want 105", second.CommitIndex())
	}

	// Reconnect by same owner (node-2) also advances epoch
	third, err := session.AcquireOwnership(&second, key, 2, "node-2", 110)
	if err != nil {
		t.Fatalf("same owner reacquire failed: %v", err)
	}
	if third.Epoch() != 3 {
		t.Fatalf("Epoch = %d, want 3", third.Epoch())
	}
	if third.CommitIndex() != 110 {
		t.Fatalf("CommitIndex = %d, want 110", third.CommitIndex())
	}

	// Stale expected epoch rejection
	_, err = session.AcquireOwnership(&third, key, 2, "node-3", 115)
	if !errors.Is(err, session.ErrStaleEpoch) {
		t.Fatalf("stale expected epoch error = %v, want ErrStaleEpoch", err)
	}

	// Stale/equal application sequence position rejection.
	_, err = session.AcquireOwnership(&third, key, 3, "node-3", 110)
	if !errors.Is(err, session.ErrStaleCommitIndex) {
		t.Fatalf("equal commit index error = %v, want ErrStaleCommitIndex", err)
	}
	_, err = session.AcquireOwnership(&third, key, 3, "node-3", 109)
	if !errors.Is(err, session.ErrStaleCommitIndex) {
		t.Fatalf("decreased commit index error = %v, want ErrStaleCommitIndex", err)
	}
}

func TestOwnership_Overflow(t *testing.T) {
	key := mustSessionKey(t, "tenant-a", "client-1")
	maxOwnership := session.NewOwnershipForTest(key, "node-1", math.MaxUint64, 100)

	_, err := session.AcquireOwnership(&maxOwnership, key, math.MaxUint64, "node-2", 101)
	if !errors.Is(err, session.ErrEpochOverflow) {
		t.Fatalf("acquire at max epoch error = %v, want ErrEpochOverflow", err)
	}
}

func TestOwnership_InvalidInputs(t *testing.T) {
	t.Parallel()

	validKey := mustSessionKey(t, "tenant-a", "client-1")
	zeroKey := tenancy.SessionKey{}

	// Empty key
	if _, err := session.AcquireOwnership(nil, zeroKey, 0, "node-1", 100); !errors.Is(err, session.ErrInvalidOwnership) {
		t.Fatalf("empty key error = %v, want ErrInvalidOwnership", err)
	}

	// Empty node ID
	if _, err := session.AcquireOwnership(nil, validKey, 0, "", 100); !errors.Is(err, session.ErrInvalidOwnership) {
		t.Fatalf("empty node ID error = %v, want ErrInvalidOwnership", err)
	}

	// Zero application sequence position.
	if _, err := session.AcquireOwnership(nil, validKey, 0, "node-1", 0); !errors.Is(err, session.ErrInvalidOwnership) {
		t.Fatalf("zero commit index error = %v, want ErrInvalidOwnership", err)
	}

	// Key mismatch on update
	current, err := session.AcquireOwnership(nil, validKey, 0, "node-1", 100)
	if err != nil {
		t.Fatalf("initial acquire failed: %v", err)
	}
	otherKey := mustSessionKey(t, "tenant-b", "client-1")
	if _, err := session.AcquireOwnership(&current, otherKey, 1, "node-1", 101); !errors.Is(err, session.ErrInvalidOwnership) {
		t.Fatalf("key mismatch error = %v, want ErrInvalidOwnership", err)
	}

	// Uninitialized current pointer
	emptyCurrent := session.Ownership{}
	if _, err := session.AcquireOwnership(&emptyCurrent, validKey, 0, "node-1", 101); !errors.Is(err, session.ErrInvalidOwnership) {
		t.Fatalf("uninitialized current error = %v, want ErrInvalidOwnership", err)
	}
}

func TestOwnership_Authorize(t *testing.T) {
	t.Parallel()

	key := mustSessionKey(t, "tenant-a", "client-1")
	owner, err := session.AcquireOwnership(nil, key, 0, "node-1", 100)
	if err != nil {
		t.Fatalf("acquire failed: %v", err)
	}

	// Valid authorization
	if err := owner.Authorize(key, "node-1", 1); err != nil {
		t.Fatalf("Authorize(key, node-1, 1) failed: %v", err)
	}

	// Stale epoch (e.g. caller has epoch 0 or 2, current is 1)
	if err := owner.Authorize(key, "node-1", 0); !errors.Is(err, session.ErrStaleEpoch) {
		t.Fatalf("Authorize with stale epoch error = %v, want ErrStaleEpoch", err)
	}
	if err := owner.Authorize(key, "node-1", 2); !errors.Is(err, session.ErrStaleEpoch) {
		t.Fatalf("Authorize with future epoch error = %v, want ErrStaleEpoch", err)
	}

	// Wrong owner
	if err := owner.Authorize(key, "node-2", 1); !errors.Is(err, session.ErrInvalidOwnership) {
		t.Fatalf("Authorize with wrong owner error = %v, want ErrInvalidOwnership", err)
	}

	// Wrong key
	otherKey := mustSessionKey(t, "tenant-b", "client-1")
	if err := owner.Authorize(otherKey, "node-1", 1); !errors.Is(err, session.ErrInvalidOwnership) {
		t.Fatalf("Authorize with wrong key error = %v, want ErrInvalidOwnership", err)
	}

	// Empty node ID
	if err := owner.Authorize(key, "", 1); !errors.Is(err, session.ErrInvalidOwnership) {
		t.Fatalf("Authorize with empty node ID error = %v, want ErrInvalidOwnership", err)
	}

	// Uninitialized ownership struct
	var zeroOwner session.Ownership
	if err := zeroOwner.Authorize(key, "node-1", 1); !errors.Is(err, session.ErrInvalidOwnership) {
		t.Fatalf("Authorize on zero struct error = %v, want ErrInvalidOwnership", err)
	}
}

func TestOwnership_ImmutabilityOnFailure(t *testing.T) {
	t.Parallel()

	key := mustSessionKey(t, "tenant-a", "client-1")
	original, err := session.AcquireOwnership(nil, key, 0, "node-1", 100)
	if err != nil {
		t.Fatalf("initial acquire failed: %v", err)
	}

	snapshot := original

	// Failed acquire attempts
	_, _ = session.AcquireOwnership(&original, key, 999, "node-2", 105)
	_, _ = session.AcquireOwnership(&original, key, 1, "node-2", 50)
	_, _ = session.AcquireOwnership(&original, key, 1, "", 105)

	if original.SessionKey() != snapshot.SessionKey() ||
		original.OwnerNodeID() != snapshot.OwnerNodeID() ||
		original.Epoch() != snapshot.Epoch() ||
		original.CommitIndex() != snapshot.CommitIndex() {
		t.Fatalf("original modified after failures: got %+v, want %+v", original, snapshot)
	}
}

// Scenarios: tenancy-and-security.md:SEC-SCN-019
func TestOwnership_CrossTenantIsolation(t *testing.T) {
	t.Parallel()

	keyA := mustSessionKey(t, "tenant-a", "shared-client")
	keyB := mustSessionKey(t, "tenant-b", "shared-client")

	ownerA, err := session.AcquireOwnership(nil, keyA, 0, "node-1", 100)
	if err != nil {
		t.Fatalf("acquire tenant A failed: %v", err)
	}

	ownerB, err := session.AcquireOwnership(nil, keyB, 0, "node-2", 101)
	if err != nil {
		t.Fatalf("acquire tenant B failed: %v", err)
	}

	if ownerA.SessionKey() == ownerB.SessionKey() {
		t.Fatalf("tenant A and B keys collided: %v", ownerA.SessionKey())
	}
	if err := ownerA.Authorize(keyB, "node-1", 1); !errors.Is(err, session.ErrInvalidOwnership) {
		t.Fatalf("tenant A authorized tenant B key: %v", err)
	}
}
