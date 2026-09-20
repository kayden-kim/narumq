package session

import (
	"errors"
	"testing"
	"time"

	"narumq/internal/domain/mqtt"
)

// Traceability: PRD MQTT-03/TEST-13.
func FuzzLifecycleTransitions(f *testing.F) {
	f.Add(byte(0), true, byte(0), uint32(1))
	f.Add(byte(0), false, byte(1), uint32(1))
	f.Add(byte(1), false, byte(0), uint32(1))
	f.Add(byte(1), false, byte(2), uint32(7))
	f.Add(byte(1), false, byte(1), uint32(86400))
	f.Add(byte(1), true, byte(2), uint32(1))
	f.Add(byte(1), false, byte(2), uint32(7))

	f.Fuzz(func(t *testing.T, protocolRaw byte, cleanStart bool, expirySelector byte, finiteRaw uint32) {
		now := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
		version := fuzzProtocolVersion(protocolRaw)
		interval := fuzzExpiryInterval(expirySelector, finiteRaw)
		connect := mqtt.Connect{
			Version:               version,
			CleanStart:            cleanStart,
			SessionExpiryInterval: interval,
			ClientID:              "fuzz-client",
		}
		if version == mqtt.ProtocolV311 {
			connect.SessionExpiryInterval = 0
		}

		connected, present, err := Open(nil, connect, now)
		if err != nil || present || !connected.Connected() || connected.ClientID() != connect.ClientID {
			t.Fatalf("initial Open() = (%#v, %v, %v), want connected new session", connected, present, err)
		}

		activeSame, present, err := Open(&connected, connect, now)
		if !errors.Is(err, ErrAlreadyConnected) || present || activeSame != connected {
			t.Fatalf("active duplicate Open() = (%#v, %v, %v), want unchanged ErrAlreadyConnected", activeSame, present, err)
		}
		otherConnect := connect
		otherConnect.ClientID = "other-client"
		identityMismatch, present, err := Open(&connected, otherConnect, now)
		if !errors.Is(err, ErrIdentityMismatch) || present || identityMismatch != connected {
			t.Fatalf("identity mismatch Open() = (%#v, %v, %v), want unchanged ErrIdentityMismatch", identityMismatch, present, err)
		}

		disconnected, removed, err := connected.Disconnect(now)
		if err != nil {
			t.Fatalf("Disconnect() error = %v", err)
		}
		deadline, finite := disconnected.ExpiresAt()
		switch version {
		case mqtt.ProtocolV311:
			if cleanStart {
				if !removed || !disconnected.Expired(now) || finite {
					t.Fatalf("MQTT 3.1.1 clean Disconnect() = (%#v, %v, %v), want immediate removal", disconnected, removed, finite)
				}
			} else if removed || disconnected.Expired(now) || finite {
				t.Fatalf("MQTT 3.1.1 persistent Disconnect() = (%#v, %v, %v), want infinite retention", disconnected, removed, finite)
			}
		case mqtt.ProtocolV5:
			switch interval {
			case 0:
				if !removed || !disconnected.Expired(now) || finite {
					t.Fatalf("MQTT 5 immediate Disconnect() = (%#v, %v, %v), want immediate removal", disconnected, removed, finite)
				}
			case ^uint32(0):
				if removed || disconnected.Expired(now.Add(24*time.Hour)) || finite {
					t.Fatalf("MQTT 5 infinite Disconnect() = (%#v, %v, %v), want infinite retention", disconnected, removed, finite)
				}
			default:
				wantDeadline := now.Add(time.Duration(interval) * time.Second)
				if removed || !finite || !deadline.Equal(wantDeadline) {
					t.Fatalf("MQTT 5 finite deadline = (%v, %v), want (%v, false)", deadline, finite, wantDeadline)
				}
				if disconnected.Expired(deadline.Add(-time.Nanosecond)) || !disconnected.Expired(deadline) {
					t.Fatalf("MQTT 5 finite expiry boundary is incorrect at %v", deadline)
				}
			}
		}

		secondDisconnect, secondRemoved, err := disconnected.Disconnect(now)
		if !errors.Is(err, ErrNotConnected) || secondRemoved || secondDisconnect != disconnected {
			t.Fatalf("second Disconnect() = (%#v, %v, %v), want unchanged ErrNotConnected", secondDisconnect, secondRemoved, err)
		}

		cleanReconnect := connect
		cleanReconnect.CleanStart = true
		fresh, present, err := Open(&disconnected, cleanReconnect, now)
		if err != nil || present || !fresh.Connected() || fresh.ClientID() != connect.ClientID {
			t.Fatalf("Clean Start reconnect = (%#v, %v, %v), want new session", fresh, present, err)
		}

		resumeConnect := connect
		resumeConnect.CleanStart = false
		resumeAt := now
		wantPresent := false
		if version == mqtt.ProtocolV311 {
			wantPresent = !cleanStart
		} else {
			switch interval {
			case 0:
				wantPresent = false
			case ^uint32(0):
				wantPresent = true
			default:
				resumeAt = deadline.Add(-time.Nanosecond)
				wantPresent = true
			}
		}
		resumed, present, err := Open(&disconnected, resumeConnect, resumeAt)
		if err != nil || present != wantPresent || !resumed.Connected() {
			t.Fatalf("resume before expiry = (%#v, %v, %v), want present=%v", resumed, present, err, wantPresent)
		}
		if version == mqtt.ProtocolV5 && interval != 0 && interval != ^uint32(0) {
			expired, present, err := Open(&disconnected, resumeConnect, deadline)
			if err != nil || present || !expired.Connected() {
				t.Fatalf("resume at expiry = (%#v, %v, %v), want present=false", expired, present, err)
			}
		}
	})
}

func fuzzProtocolVersion(value byte) mqtt.ProtocolVersion {
	if value%2 == 0 {
		return mqtt.ProtocolV311
	}
	return mqtt.ProtocolV5
}

func fuzzExpiryInterval(selector byte, finiteRaw uint32) uint32 {
	switch selector % 3 {
	case 0:
		return 0
	case 1:
		return ^uint32(0)
	default:
		return finiteRaw%86400 + 1
	}
}
