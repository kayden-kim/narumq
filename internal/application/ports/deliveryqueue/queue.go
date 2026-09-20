// Package deliveryqueue defines the application-owned delivery queue port.
package deliveryqueue

import (
	"errors"
	"time"

	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/routing"
)

var (
	// ErrFull means that a registered session reached its item capacity.
	ErrFull = errors.New("delivery queue full")
	// ErrUnavailable means that a session has no registered delivery queue.
	ErrUnavailable = errors.New("delivery queue unavailable")
)

// DeliveryDTO is the application boundary value handed to one session queue.
type DeliveryDTO struct {
	SessionID routing.SessionID
	Topic     string
	Payload   []byte
	QoS       mqtt.QoS
	Retain    bool
	ExpiresAt time.Time
}

// Expired reports whether this delivery has reached its absolute expiry
// deadline. A zero deadline means that the publication had no expiry property.
func (delivery DeliveryDTO) Expired(now time.Time) bool {
	return !delivery.ExpiresAt.IsZero() && !now.Before(delivery.ExpiresAt)
}

// RemainingExpiryInterval returns the positive whole-second interval that
// remains until ExpiresAt, rounded upward. The caller must check Expired first.
func (delivery DeliveryDTO) RemainingExpiryInterval(now time.Time) (uint32, bool) {
	if delivery.ExpiresAt.IsZero() {
		return 0, false
	}
	remaining := delivery.ExpiresAt.Sub(now)
	if remaining <= 0 {
		return 0, true
	}
	seconds := uint64(remaining / time.Second)
	if remaining%time.Second != 0 {
		seconds++
	}
	if seconds > uint64(^uint32(0)) {
		return ^uint32(0), true
	}
	return uint32(seconds), true
}

// Queue is the bounded delivery capability shared by publish and connection
// lifecycle use cases. TryEnqueue and TryEnqueueBatch read DeliveryDTO.Payload
// synchronously; implementations must copy it before retaining delivery.
type Queue interface {
	Register(sessionID routing.SessionID) error
	Remove(sessionID routing.SessionID)
	TryEnqueue(delivery DeliveryDTO, now time.Time) error
	TryEnqueueBatch(deliveries []DeliveryDTO, now time.Time) error
	Drain(sessionID routing.SessionID, now time.Time) ([]DeliveryDTO, error)
}
