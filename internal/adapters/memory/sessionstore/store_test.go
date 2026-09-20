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
