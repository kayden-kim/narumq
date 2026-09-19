package session

import (
	"errors"
	"testing"
	"time"

	"narumq/internal/domain/mqtt"
)

// Traceability: PRD MQTT-03/TEST-03/04; MQTT-3.1.2-2/3.
func TestOpenAndDisconnectMQTT311CleanStartLifecycle(t *testing.T) {
	now := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	clean, present, err := Open(nil, mqtt.Connect{Version: mqtt.ProtocolV311, CleanStart: true, ClientID: "client"}, now)
	if err != nil || present || !clean.Connected() || clean.ClientID() != "client" || clean.ExpiryInterval() != 0 {
		t.Fatalf("clean Open() = (%#v, %v, %v)", clean, present, err)
	}
	closed, removed, err := clean.Disconnect(now)
	if err != nil || !removed || !closed.Expired(now) || closed.Connected() {
		t.Fatalf("clean Disconnect() = (%#v, %v, %v)", closed, removed, err)
	}
	if _, _, err := closed.Disconnect(now); !errors.Is(err, ErrNotConnected) {
		t.Fatalf("second Disconnect() error = %v, want ErrNotConnected", err)
	}

	persistent, present, err := Open(nil, mqtt.Connect{Version: mqtt.ProtocolV311, ClientID: "persistent"}, now)
	if err != nil || present {
		t.Fatalf("persistent initial Open() = (%#v, %v, %v)", persistent, present, err)
	}
	persistent, removed, err = persistent.Disconnect(now)
	if err != nil || removed || persistent.Expired(now) {
		t.Fatalf("persistent Disconnect() = (%#v, %v, %v)", persistent, removed, err)
	}
	resumed, present, err := Open(&persistent, mqtt.Connect{Version: mqtt.ProtocolV311, ClientID: "persistent"}, now.Add(24*time.Hour))
	if err != nil || !present || !resumed.Connected() || resumed.ClientID() != "persistent" {
		t.Fatalf("persistent resume = (%#v, %v, %v)", resumed, present, err)
	}
	if _, finite := persistent.ExpiresAt(); finite {
		t.Fatal("MQTT 3.1.1 persistent session unexpectedly has finite deadline")
	}
}

// Traceability: PRD MQTT-03/TEST-03/04/07; MQTT-3.1.2-2/3.
func TestMQTT5SessionExpiryBoundariesAndReconnect(t *testing.T) {
	start := time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)
	finiteConnect := mqtt.Connect{Version: mqtt.ProtocolV5, ClientID: "finite", SessionExpiryInterval: 7}
	state, present, err := Open(nil, finiteConnect, start)
	if err != nil || present || state.ExpiryInterval() != 7 || !state.Connected() {
		t.Fatalf("finite Open() = (%#v, %v, %v)", state, present, err)
	}
	closed, removed, err := state.Disconnect(start)
	if err != nil || removed {
		t.Fatalf("finite Disconnect() = (%#v, %v, %v)", closed, removed, err)
	}
	deadline, ok := closed.ExpiresAt()
	if !ok || !deadline.Equal(start.Add(7*time.Second)) {
		t.Fatalf("deadline = (%v, %v), want %v", deadline, ok, start.Add(7*time.Second))
	}
	for _, testCase := range []struct {
		name    string
		at      time.Time
		expired bool
	}{
		{name: "before-deadline", at: deadline.Add(-time.Nanosecond), expired: false},
		{name: "at-deadline", at: deadline, expired: true},
		{name: "after-deadline", at: deadline.Add(time.Nanosecond), expired: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			if got := closed.Expired(testCase.at); got != testCase.expired {
				t.Fatalf("Expired(%v) = %v, want %v", testCase.at, got, testCase.expired)
			}
		})
	}
	resumed, present, err := Open(&closed, mqtt.Connect{Version: mqtt.ProtocolV5, ClientID: "finite", SessionExpiryInterval: 9}, deadline.Add(-time.Nanosecond))
	if err != nil || !present || resumed.ExpiryInterval() != 9 {
		t.Fatalf("finite resume before deadline = (%#v, %v, %v)", resumed, present, err)
	}
	expired, present, err := Open(&closed, mqtt.Connect{Version: mqtt.ProtocolV5, ClientID: "finite", SessionExpiryInterval: 9}, deadline)
	if err != nil || present || !expired.Connected() {
		t.Fatalf("finite reopen at deadline = (%#v, %v, %v)", expired, present, err)
	}

	immediate, present, err := Open(nil, mqtt.Connect{Version: mqtt.ProtocolV5, ClientID: "immediate"}, start)
	if err != nil || present {
		t.Fatalf("immediate Open() = (%#v, %v, %v)", immediate, present, err)
	}
	immediate, removed, err = immediate.Disconnect(start)
	if err != nil || !removed || !immediate.Expired(start) {
		t.Fatalf("immediate Disconnect() = (%#v, %v, %v)", immediate, removed, err)
	}
	if _, finite := immediate.ExpiresAt(); finite {
		t.Fatal("immediate expiry unexpectedly exposed a finite deadline")
	}

	never, present, err := Open(nil, mqtt.Connect{Version: mqtt.ProtocolV5, ClientID: "never", SessionExpiryInterval: ^uint32(0)}, start)
	if err != nil || present {
		t.Fatalf("never-expire Open() = (%#v, %v, %v)", never, present, err)
	}
	never, removed, err = never.Disconnect(start)
	if err != nil || removed || never.Expired(start.Add(1000*time.Hour)) {
		t.Fatalf("never-expire Disconnect() = (%#v, %v, %v)", never, removed, err)
	}
	if _, finite := never.ExpiresAt(); finite {
		t.Fatal("never-expire session unexpectedly has finite deadline")
	}
}

// Traceability: PRD MQTT-03/TEST-03/04; MQTT-3.1.2-2 and MQTT-3.1.2-3.
func TestMQTT5CleanStartFiniteExpiryPersistsAfterDisconnect(t *testing.T) {
	now := time.Date(2026, time.January, 4, 0, 0, 0, 0, time.UTC)
	connect := mqtt.Connect{
		Version:               mqtt.ProtocolV5,
		CleanStart:            true,
		ClientID:              "clean-finite",
		SessionExpiryInterval: 7,
	}
	state, present, err := Open(nil, connect, now)
	if err != nil || present || !state.Connected() {
		t.Fatalf("Clean Start finite Open() = (%#v, %v, %v)", state, present, err)
	}
	closed, removed, err := state.Disconnect(now)
	if err != nil || removed {
		t.Fatalf("Clean Start finite Disconnect() = (%#v, %v, %v)", closed, removed, err)
	}
	deadline, ok := closed.ExpiresAt()
	if !ok || !deadline.Equal(now.Add(7*time.Second)) {
		t.Fatalf("Clean Start finite deadline = (%v, %v), want %v", deadline, ok, now.Add(7*time.Second))
	}
	if closed.Expired(deadline.Add(-time.Nanosecond)) || !closed.Expired(deadline) {
		t.Fatalf("Clean Start finite expiry boundary is incorrect")
	}
	resumed, present, err := Open(&closed, mqtt.Connect{Version: mqtt.ProtocolV5, ClientID: "clean-finite"}, deadline.Add(-time.Nanosecond))
	if err != nil || !present || !resumed.Connected() {
		t.Fatalf("Clean Start finite resume = (%#v, %v, %v)", resumed, present, err)
	}
}

// Traceability: PRD MQTT-03/TEST-03/04; ownership transition boundary.
func TestOpenCleanStartAndIllegalLifecycleTransitions(t *testing.T) {
	now := time.Date(2026, time.January, 3, 0, 0, 0, 0, time.UTC)
	old, _, err := Open(nil, mqtt.Connect{Version: mqtt.ProtocolV5, ClientID: "client", SessionExpiryInterval: 100}, now)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := Open(&old, mqtt.Connect{Version: mqtt.ProtocolV5, ClientID: "other"}, now); !errors.Is(err, ErrIdentityMismatch) {
		t.Fatalf("identity mismatch error = %v, want ErrIdentityMismatch", err)
	}
	if _, _, err := Open(&old, mqtt.Connect{Version: mqtt.ProtocolV5, ClientID: "client", CleanStart: true}, now); !errors.Is(err, ErrAlreadyConnected) {
		t.Fatalf("connected Open() error = %v, want ErrAlreadyConnected", err)
	}
	old, _, err = old.Disconnect(now)
	if err != nil {
		t.Fatal(err)
	}
	fresh, present, err := Open(&old, mqtt.Connect{Version: mqtt.ProtocolV5, ClientID: "client", CleanStart: true}, now.Add(time.Second))
	if err != nil || present || !fresh.Connected() {
		t.Fatalf("Clean Start replacement = (%#v, %v, %v)", fresh, present, err)
	}
	fresh, removed, err := fresh.Disconnect(now.Add(time.Second))
	if err != nil || !removed || !fresh.Expired(now.Add(time.Second)) {
		t.Fatalf("Clean Start disconnect = (%#v, %v, %v)", fresh, removed, err)
	}
	if _, _, err := fresh.Disconnect(now); !errors.Is(err, ErrNotConnected) {
		t.Fatalf("disconnected Disconnect() error = %v, want ErrNotConnected", err)
	}
}
