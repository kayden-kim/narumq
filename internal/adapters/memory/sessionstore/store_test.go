package sessionstore

import (
	"errors"
	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/session"
	"narumq/internal/domain/tenancy"
	"testing"
	"time"
)

func TestMemorySessionRepositoryValidatesKeysAndCopiesValues(t *testing.T) {
	repository := New()
	if _, _, err := repository.Load(""); !errors.Is(err, ErrInvalidClientID) {
		t.Fatalf("Load(\"\") error = %v, want %v", err, ErrInvalidClientID)
	}
	if err := repository.Delete(""); !errors.Is(err, ErrInvalidClientID) {
		t.Fatalf("Delete(\"\") error = %v, want %v", err, ErrInvalidClientID)
	}
	state, _, err := session.Open(nil, mqtt.Connect{Version: mqtt.ProtocolV5, ClientID: "client", SessionExpiryInterval: 10}, time.Unix(1, 0))
	if err != nil {
		t.Fatal(err)
	}
	if err := repository.Save(state); err != nil {
		t.Fatal(err)
	}
	loaded, found, err := repository.Load("client")
	if err != nil || !found || loaded.ClientID() != "client" || !loaded.Connected() {
		t.Fatalf("Load() = (%#v, %v, %v)", loaded, found, err)
	}
	if err := repository.Delete("client"); err != nil {
		t.Fatal(err)
	}
	if _, found, err := repository.Load("client"); err != nil || found {
		t.Fatalf("Load() after Delete = found %v, err %v", found, err)
	}
	if err := repository.Delete("client"); err != nil {
		t.Fatalf("idempotent Delete() error = %v", err)
	}
}
func TestNilAndInvalidSessionRepositoryOperations(t *testing.T) {
	var repository *MemorySessionRepository
	if _, _, err := repository.Load("client"); !errors.Is(err, ErrInvalidClientID) {
		t.Fatalf("nil Load() error = %v", err)
	}
	if err := repository.Save(session.Lifecycle{}); !errors.Is(err, ErrInvalidClientID) {
		t.Fatalf("nil Save() error = %v", err)
	}
	if err := repository.Delete("client"); !errors.Is(err, ErrInvalidClientID) {
		t.Fatalf("nil Delete() error = %v", err)
	}
	store := New()
	if err := store.Save(session.Lifecycle{}); !errors.Is(err, ErrInvalidClientID) {
		t.Fatalf("zero Save() error = %v", err)
	}
	if err := store.Delete(""); !errors.Is(err, ErrInvalidClientID) {
		t.Fatalf("empty Delete() error = %v", err)
	}
}

// Requirements: TEN-02
func TestMemorySessionRepository_SessionKeyMultiTenant(t *testing.T) {
	repository := New()
	tenantA, err := tenancy.NewTenantID("tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	tenantB, err := tenancy.NewTenantID("tenant-b")
	if err != nil {
		t.Fatal(err)
	}
	keyA, err := tenancy.NewSessionKey(tenantA, "device-1")
	if err != nil {
		t.Fatal(err)
	}
	keyB, err := tenancy.NewSessionKey(tenantB, "device-1")
	if err != nil {
		t.Fatal(err)
	}

	now := time.Unix(100, 0)
	stateA, _, err := session.Open(nil, mqtt.Connect{Version: mqtt.ProtocolV5, ClientID: "device-1", SessionExpiryInterval: 10}, now, keyA)
	if err != nil {
		t.Fatal(err)
	}
	stateB, _, err := session.Open(nil, mqtt.Connect{Version: mqtt.ProtocolV5, ClientID: "device-1", SessionExpiryInterval: 20}, now, keyB)
	if err != nil {
		t.Fatal(err)
	}

	if err := repository.SaveKey(keyA, stateA); err != nil {
		t.Fatal(err)
	}
	if err := repository.SaveKey(keyB, stateB); err != nil {
		t.Fatal(err)
	}

	loadedA, foundA, err := repository.LoadKey(keyA)
	if err != nil || !foundA || loadedA.ExpiryInterval() != 10 {
		t.Fatalf("LoadKey(keyA) = (%#v, %v, %v)", loadedA, foundA, err)
	}
	loadedB, foundB, err := repository.LoadKey(keyB)
	if err != nil || !foundB || loadedB.ExpiryInterval() != 20 {
		t.Fatalf("LoadKey(keyB) = (%#v, %v, %v)", loadedB, foundB, err)
	}

	if err := repository.DeleteKey(keyA); err != nil {
		t.Fatal(err)
	}
	_, foundA, err = repository.LoadKey(keyA)
	if err != nil || foundA {
		t.Fatalf("LoadKey(keyA) after delete = found %v, err %v", foundA, err)
	}
	// Tenant B's session must still be intact!
	loadedB, foundB, err = repository.LoadKey(keyB)
	if err != nil || !foundB || loadedB.ExpiryInterval() != 20 {
		t.Fatalf("LoadKey(keyB) unexpectedly affected: (%#v, %v, %v)", loadedB, foundB, err)
	}
}

// Traceability: TEN-06, TEN-07; docs/specs/tenancy-and-security.md §5.2.
func TestTenantSessionCountsTrackLifecycleChanges(t *testing.T) {
	repository := New()
	tenantA, err := tenancy.NewTenantID("tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	tenantB, err := tenancy.NewTenantID("tenant-b")
	if err != nil {
		t.Fatal(err)
	}
	keyA, err := tenancy.NewSessionKey(tenantA, "client-a")
	if err != nil {
		t.Fatal(err)
	}
	keyB, err := tenancy.NewSessionKey(tenantA, "client-b")
	if err != nil {
		t.Fatal(err)
	}
	keyOtherTenant, err := tenancy.NewSessionKey(tenantB, "client-a")
	if err != nil {
		t.Fatal(err)
	}
	assertCounts := func(tenantID tenancy.TenantID, wantSessions, wantConnected int) {
		t.Helper()
		sessions, connected := repository.TenantSessionCounts(tenantID)
		if sessions != wantSessions || connected != wantConnected {
			t.Fatalf("TenantSessionCounts(%q) = (%d, %d), want (%d, %d)", tenantID.String(), sessions, connected, wantSessions, wantConnected)
		}
	}
	assertCounts(tenantA, 0, 0)
	assertCounts(tenantB, 0, 0)

	now := time.Unix(100, 0)
	stateA, _, err := session.Open(nil, mqtt.Connect{Version: mqtt.ProtocolV5, ClientID: keyA.ClientID(), SessionExpiryInterval: 10}, now, keyA)
	if err != nil {
		t.Fatal(err)
	}
	if err := repository.Save(stateA); err != nil {
		t.Fatal(err)
	}
	assertCounts(tenantA, 1, 1)

	disconnected, err := stateA.Disconnect(now, session.DisconnectNormal)
	if err != nil {
		t.Fatal(err)
	}
	if err := repository.Save(disconnected.Lifecycle); err != nil {
		t.Fatal(err)
	}
	assertCounts(tenantA, 1, 0)

	reconnected, _, err := session.Open(&disconnected.Lifecycle, mqtt.Connect{Version: mqtt.ProtocolV5, ClientID: keyA.ClientID(), SessionExpiryInterval: 10}, now.Add(time.Second), keyA)
	if err != nil {
		t.Fatal(err)
	}
	if err := repository.SaveKey(keyA, reconnected); err != nil {
		t.Fatal(err)
	}
	assertCounts(tenantA, 1, 1)

	stateB, _, err := session.Open(nil, mqtt.Connect{Version: mqtt.ProtocolV5, ClientID: keyB.ClientID(), SessionExpiryInterval: 10}, now, keyB)
	if err != nil {
		t.Fatal(err)
	}
	detachedB, err := stateB.Disconnect(now, session.DisconnectNormal)
	if err != nil {
		t.Fatal(err)
	}
	if err := repository.SaveKey(keyB, detachedB.Lifecycle); err != nil {
		t.Fatal(err)
	}

	otherTenantState, _, err := session.Open(nil, mqtt.Connect{Version: mqtt.ProtocolV5, ClientID: keyOtherTenant.ClientID(), SessionExpiryInterval: 10}, now, keyOtherTenant)
	if err != nil {
		t.Fatal(err)
	}
	if err := repository.SaveKey(keyOtherTenant, otherTenantState); err != nil {
		t.Fatal(err)
	}
	assertCounts(tenantA, 2, 1)
	assertCounts(tenantB, 1, 1)

	if err := repository.Delete(keyA.ClientID()); err != nil {
		t.Fatal(err)
	}
	assertCounts(tenantA, 2, 1)
	if err := repository.DeleteKey(keyA); err != nil {
		t.Fatal(err)
	}
	assertCounts(tenantA, 1, 0)
	if err := repository.DeleteKey(keyA); err != nil {
		t.Fatal(err)
	}
	assertCounts(tenantA, 1, 0)
	if err := repository.DeleteKey(keyB); err != nil {
		t.Fatal(err)
	}
	assertCounts(tenantA, 0, 0)
	assertCounts(tenantB, 1, 1)
	if err := repository.DeleteKey(keyOtherTenant); err != nil {
		t.Fatal(err)
	}
	assertCounts(tenantB, 0, 0)
}

// Traceability: TEN-06, TEN-07; docs/specs/tenancy-and-security.md §5.2.
func TestTenantSessionCountsNilAndUnkeyedRepository(t *testing.T) {
	var nilRepository *MemorySessionRepository
	if sessions, connected := nilRepository.TenantSessionCounts(tenancy.TenantID{}); sessions != 0 || connected != 0 {
		t.Fatalf("nil TenantSessionCounts() = (%d, %d), want (0, 0)", sessions, connected)
	}

	repository := New()
	state, _, err := session.Open(nil, mqtt.Connect{Version: mqtt.ProtocolV5, ClientID: "unkeyed"}, time.Unix(100, 0))
	if err != nil {
		t.Fatal(err)
	}
	if err := repository.Save(state); err != nil {
		t.Fatal(err)
	}
	if sessions, connected := repository.TenantSessionCounts(tenancy.TenantID{}); sessions != 0 || connected != 0 {
		t.Fatalf("unkeyed TenantSessionCounts() = (%d, %d), want (0, 0)", sessions, connected)
	}
}
