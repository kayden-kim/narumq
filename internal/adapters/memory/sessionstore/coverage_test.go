package sessionstore

import (
	"errors"
	"testing"
	"time"

	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/session"
	"narumq/internal/domain/tenancy"
)

func cmmSessionKey(t *testing.T, tenant string, clientID string) tenancy.SessionKey {
	t.Helper()
	tenantID, err := tenancy.NewTenantID(tenant)
	if err != nil {
		t.Fatal(err)
	}
	key, err := tenancy.NewSessionKey(tenantID, clientID)
	if err != nil {
		t.Fatal(err)
	}
	return key
}

func cmmOpenLifecycle(t *testing.T, clientID string, expiry uint32, keys ...tenancy.SessionKey) session.Lifecycle {
	t.Helper()
	state, _, err := session.Open(nil, mqtt.Connect{Version: mqtt.ProtocolV5, ClientID: clientID, SessionExpiryInterval: expiry}, time.Unix(100, 0), keys...)
	if err != nil {
		t.Fatal(err)
	}
	return state
}

func TestCmmSessionKeyOperationsRejectNilRepositoryAndZeroKey(t *testing.T) {
	key := cmmSessionKey(t, "tenant-a", "device-1")
	state := cmmOpenLifecycle(t, "device-1", 10, key)
	var nilRepository *MemorySessionRepository
	store := New()

	tests := []struct {
		name string
		call func() error
	}{
		{name: "nil LoadKey", call: func() error { _, _, err := nilRepository.LoadKey(key); return err }},
		{name: "nil SaveKey", call: func() error { return nilRepository.SaveKey(key, state) }},
		{name: "nil DeleteKey", call: func() error { return nilRepository.DeleteKey(key) }},
		{name: "zero LoadKey", call: func() error { _, _, err := store.LoadKey(tenancy.SessionKey{}); return err }},
		{name: "zero SaveKey", call: func() error { return store.SaveKey(tenancy.SessionKey{}, state) }},
		{name: "zero DeleteKey", call: func() error { return store.DeleteKey(tenancy.SessionKey{}) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.call(); !errors.Is(err, ErrInvalidSessionKey) {
				t.Fatalf("error = %v, want %v", err, ErrInvalidSessionKey)
			}
		})
	}
	if snapshot := store.Snapshot(); len(snapshot) != 0 {
		t.Fatalf("rejected operations stored sessions: %d", len(snapshot))
	}
	if snapshot := nilRepository.Snapshot(); snapshot != nil {
		t.Fatalf("nil Snapshot() = %v, want nil", snapshot)
	}
}

func TestCmmSaveIndexesLifecycleSessionKeyInZeroValueRepository(t *testing.T) {
	key := cmmSessionKey(t, "tenant-a", "device-1")
	state := cmmOpenLifecycle(t, "device-1", 30, key)
	var repository MemorySessionRepository

	if err := repository.Save(state); err != nil {
		t.Fatal(err)
	}
	loaded, found, err := repository.LoadKey(key)
	if err != nil || !found || loaded.ExpiryInterval() != 30 {
		t.Fatalf("LoadKey() = (%#v, %v, %v), want saved lifecycle", loaded, found, err)
	}
	byClient, found, err := repository.Load("device-1")
	if err != nil || !found || byClient.ExpiryInterval() != 30 {
		t.Fatalf("Load() = (%#v, %v, %v), want saved lifecycle", byClient, found, err)
	}

	otherKey := cmmSessionKey(t, "tenant-b", "device-1")
	if _, found, err := repository.LoadKey(otherKey); err != nil || found {
		t.Fatalf("LoadKey(other tenant) = found %v, err %v, want missing", found, err)
	}
}

func TestCmmSaveWithoutSessionKeyDoesNotIndexByKey(t *testing.T) {
	store := New()
	state := cmmOpenLifecycle(t, "device-1", 10)
	if err := store.Save(state); err != nil {
		t.Fatal(err)
	}
	key := cmmSessionKey(t, "tenant-a", "device-1")
	if _, found, err := store.LoadKey(key); err != nil || found {
		t.Fatalf("LoadKey() for unkeyed lifecycle = found %v, err %v, want missing", found, err)
	}
}

func TestCmmSaveKeyInZeroValueRepositoryAndDeleteKeyRemovesBothIndexes(t *testing.T) {
	key := cmmSessionKey(t, "tenant-a", "device-1")
	state := cmmOpenLifecycle(t, "device-1", 15, key)
	var repository MemorySessionRepository

	if err := repository.SaveKey(key, state); err != nil {
		t.Fatal(err)
	}
	snapshot := repository.Snapshot()
	if len(snapshot) != 1 || snapshot[0].ExpiryInterval() != 15 {
		t.Fatalf("Snapshot() = %#v, want one saved lifecycle", snapshot)
	}
	if _, found, err := repository.Load(key.ClientID()); err != nil || !found {
		t.Fatalf("Load(client ID) after SaveKey = found %v, err %v", found, err)
	}

	if err := repository.DeleteKey(key); err != nil {
		t.Fatal(err)
	}
	if _, found, err := repository.LoadKey(key); err != nil || found {
		t.Fatalf("LoadKey() after DeleteKey = found %v, err %v", found, err)
	}
	if _, found, err := repository.Load(key.ClientID()); err != nil || found {
		t.Fatalf("Load() after DeleteKey = found %v, err %v", found, err)
	}
	if err := repository.DeleteKey(key); err != nil {
		t.Fatalf("idempotent DeleteKey() error = %v", err)
	}
}
