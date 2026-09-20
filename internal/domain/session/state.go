package session

import (
	"errors"
	"time"

	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/tenancy"
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
	sessionKey     tenancy.SessionKey
	clientID       string
	version        mqtt.ProtocolVersion
	connected      bool
	expiryInterval uint32
	retention      retention
	expiresAt      time.Time
	hasExpiry      bool
	discarded      bool
	will           *mqtt.Will
	willPublished  bool
	willDiscarded  bool
}

// DisconnectReason represents the trigger that closed an MQTT network connection.
type DisconnectReason byte

const (
	// DisconnectNormal indicates graceful DISCONNECT with Reason Code 0x00.
	// Will message is discarded without being published (MQTT-3.14.4-3).
	DisconnectNormal DisconnectReason = iota
	// DisconnectWithWill indicates DISCONNECT with Reason Code 0x04.
	// Will message must be published (MQTT-3.1.2-8).
	DisconnectWithWill
	// DisconnectAbnormal indicates unexpected connection loss, I/O error, or error code.
	// Will message must be published (MQTT-3.1.2-8).
	DisconnectAbnormal
)

// DisconnectResult describes the state changes and scheduled actions resulting from Disconnect.
type DisconnectResult struct {
	// Lifecycle is the updated disconnected session state.
	Lifecycle Lifecycle
	// SessionRemoved reports whether the session was removed immediately.
	SessionRemoved bool
	// PublishWillNow reports whether the will message must be published immediately.
	PublishWillNow bool
	// Will is the will message to publish immediately if PublishWillNow is true.
	Will *mqtt.Will
	// ScheduleWill reports whether the will message publication should be scheduled.
	ScheduleWill bool
	// WillDelay is the delay duration after which the will message should be published.
	WillDelay time.Duration
	// ScheduleExpiry reports whether session expiry should be scheduled.
	ScheduleExpiry bool
	// ExpiryDuration is the duration until session expiry.
	ExpiryDuration time.Duration
}

// Open opens a connection and reports whether an existing session was resumed.
// The existing lifecycle is never mutated; a connected existing lifecycle is
// rejected because takeover and fencing belong to a higher application layer.
func Open(existing *Lifecycle, connect mqtt.Connect, now time.Time, key ...tenancy.SessionKey) (Lifecycle, bool, error) {
	unchanged := Lifecycle{}
	var activeKey tenancy.SessionKey
	if len(key) > 0 {
		activeKey = key[0]
	}
	if existing != nil {
		unchanged = *existing
		unchanged.will = existing.will.Clone()
		if existing.clientID != connect.ClientID {
			return unchanged, false, ErrIdentityMismatch
		}
		if activeKey != (tenancy.SessionKey{}) && existing.sessionKey != (tenancy.SessionKey{}) {
			if existing.sessionKey != activeKey {
				return unchanged, false, ErrIdentityMismatch
			}
		}
		if existing.connected {
			return unchanged, false, ErrAlreadyConnected
		}
	}

	present := existing != nil && !connect.CleanStart && !existing.discarded && !existing.Expired(now)
	opened := newLifecycle(connect)
	if activeKey != (tenancy.SessionKey{}) {
		opened.sessionKey = activeKey
	} else if existing != nil {
		opened.sessionKey = existing.sessionKey
	}
	return opened, present, nil
}
func (state Lifecycle) Disconnect(now time.Time, reason DisconnectReason) (DisconnectResult, error) {
	if !state.connected {
		return DisconnectResult{Lifecycle: state}, ErrNotConnected
	}
	state.connected = false
	state.hasExpiry = false
	state.expiresAt = time.Time{}

	var result DisconnectResult
	switch state.retention {
	case retentionImmediate:
		state.discarded = true
		result.SessionRemoved = true
	case retentionFinite:
		state.discarded = false
		result.ScheduleExpiry = true
		result.ExpiryDuration = time.Duration(state.expiryInterval) * time.Second
		state.hasExpiry = true
		state.expiresAt = now.Add(result.ExpiryDuration)
	case retentionInfinite:
		state.discarded = false
	default:
		return DisconnectResult{}, ErrNotConnected
	}

	hasWill := state.will != nil && !state.willPublished && !state.willDiscarded
	if hasWill {
		if reason == DisconnectNormal {
			state.willDiscarded = true
		} else {
			if state.version == mqtt.ProtocolV311 || state.will.DelayInterval == 0 || state.retention == retentionImmediate {
				state.willPublished = true
				result.PublishWillNow = true
				result.Will = state.will.Clone()
			} else {
				willDelay := time.Duration(state.will.DelayInterval) * time.Second
				if state.retention == retentionFinite && result.ExpiryDuration < willDelay {
					willDelay = result.ExpiryDuration
				}
				result.ScheduleWill = true
				result.WillDelay = willDelay
			}
		}
	}

	result.Lifecycle = state
	return result, nil
}

// ClaimWill extracts the will message if it has not yet been published or discarded,
// marking it published to prevent duplicate delivery.
func (state Lifecycle) ClaimWill() (Lifecycle, *mqtt.Will, bool) {
	if state.will == nil || state.willPublished || state.willDiscarded {
		return state, nil, false
	}
	state.willPublished = true
	return state, state.will.Clone(), true
}

// Expire marks the session as expired if its deadline has passed, claiming any pending
// will message before the session is purged.
func (state Lifecycle) Expire(now time.Time) (Lifecycle, *mqtt.Will, bool) {
	if state.discarded || !state.Expired(now) {
		return state, nil, false
	}
	state.discarded = true
	var will *mqtt.Will
	if state.will != nil && !state.willPublished && !state.willDiscarded {
		state.willPublished = true
		will = state.will.Clone()
	}
	return state, will, true
}

// Will returns a copy of the will message stored in the session, or nil if none.
func (state Lifecycle) Will() *mqtt.Will {
	return state.will.Clone()
}

// WillPublished reports whether the will message was published.
func (state Lifecycle) WillPublished() bool {
	return state.willPublished
}

// WillDiscarded reports whether the will message was discarded without publishing.
func (state Lifecycle) WillDiscarded() bool {
	return state.willDiscarded
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

// Version returns the MQTT protocol version bound to the lifecycle.
func (state Lifecycle) Version() mqtt.ProtocolVersion {
	return state.version
}

func (state Lifecycle) Connected() bool {
	return state.connected
}

// SessionKey returns the tenant-scoped session key.
func (state Lifecycle) SessionKey() tenancy.SessionKey {
	return state.sessionKey
}

// TenantID returns the tenant ID of the session key.
func (state Lifecycle) TenantID() tenancy.TenantID {
	return state.sessionKey.TenantID()
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
		will:      connect.Will.Clone(),
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
