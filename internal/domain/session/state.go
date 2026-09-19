package session

import (
	"errors"
	"time"

	"narumq/internal/domain/mqtt"
)

var (
	// ErrIdentityMismatch means that a lifecycle belongs to another Client ID.
	ErrIdentityMismatch = errors.New("session identity mismatch")
	// ErrAlreadyConnected means that the existing lifecycle still has an owner.
	ErrAlreadyConnected = errors.New("session is already connected")
	// ErrNotConnected means that Disconnect was applied to a disconnected state.
	ErrNotConnected = errors.New("session is not connected")
)

type retention byte

const (
	retentionImmediate retention = iota
	retentionInfinite
	retentionFinite
)

// Lifecycle is a value-owned MQTT session connection and expiry state.
type Lifecycle struct {
	clientID       string
	version        mqtt.ProtocolVersion
	connected      bool
	expiryInterval uint32
	retention      retention
	expiresAt      time.Time
	hasExpiry      bool
	discarded      bool
}

// Open opens a connection and reports whether an existing session was resumed.
// The existing lifecycle is never mutated; a connected existing lifecycle is
// rejected because takeover and fencing belong to a higher application layer.
func Open(existing *Lifecycle, connect mqtt.Connect, now time.Time) (Lifecycle, bool, error) {
	unchanged := Lifecycle{}
	if existing != nil {
		unchanged = *existing
		if existing.clientID != connect.ClientID {
			return unchanged, false, ErrIdentityMismatch
		}
		if existing.connected {
			return unchanged, false, ErrAlreadyConnected
		}
	}

	present := existing != nil && !connect.CleanStart && !existing.discarded && !existing.Expired(now)
	return newLifecycle(connect), present, nil
}

// Disconnect closes the connection and reports whether the session is removed
// immediately. Finite expiry begins at the caller-provided disconnect time.
func (state Lifecycle) Disconnect(now time.Time) (Lifecycle, bool, error) {
	if !state.connected {
		return state, false, ErrNotConnected
	}
	state.connected = false
	state.hasExpiry = false
	state.expiresAt = time.Time{}
	switch state.retention {
	case retentionImmediate:
		state.discarded = true
		return state, true, nil
	case retentionFinite:
		state.discarded = false
		state.hasExpiry = true
		state.expiresAt = now.Add(time.Duration(state.expiryInterval) * time.Second)
		return state, false, nil
	case retentionInfinite:
		state.discarded = false
		return state, false, nil
	default:
		return state, false, ErrNotConnected
	}
}

// Expired reports whether this disconnected lifecycle reached its expiry edge.
func (state Lifecycle) Expired(now time.Time) bool {
	if state.discarded {
		return true
	}
	if state.connected || !state.hasExpiry {
		return false
	}
	return !now.Before(state.expiresAt)
}

// ClientID returns the identity bound to the lifecycle.
func (state Lifecycle) ClientID() string {
	return state.clientID
}

// Connected reports whether the lifecycle currently has an active connection.
func (state Lifecycle) Connected() bool {
	return state.connected
}

// ExpiryInterval returns the MQTT 5 interval requested by the active session.
// MQTT 3.1.1 persistent sessions return zero because that version has no
// numeric Session Expiry property.
func (state Lifecycle) ExpiryInterval() uint32 {
	return state.expiryInterval
}

// ExpiresAt returns the finite deadline after Disconnect, if one exists.
func (state Lifecycle) ExpiresAt() (time.Time, bool) {
	if state.connected || state.discarded || !state.hasExpiry {
		return time.Time{}, false
	}
	return state.expiresAt, true
}

func newLifecycle(connect mqtt.Connect) Lifecycle {
	state := Lifecycle{
		clientID:  connect.ClientID,
		version:   connect.Version,
		connected: true,
	}
	if connect.Version == mqtt.ProtocolV5 {
		state.expiryInterval = connect.SessionExpiryInterval
		switch connect.SessionExpiryInterval {
		case 0:
			state.retention = retentionImmediate
		case ^uint32(0):
			state.retention = retentionInfinite
		default:
			state.retention = retentionFinite
		}
		return state
	}
	if connect.CleanStart {
		state.retention = retentionImmediate
	} else {
		state.retention = retentionInfinite
	}
	return state
}
