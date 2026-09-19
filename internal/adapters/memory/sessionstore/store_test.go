package sessionstore

import (
	"errors"
	"testing"
	"time"

	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/session"
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
