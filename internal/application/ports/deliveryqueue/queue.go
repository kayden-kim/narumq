// Package deliveryqueue defines the application-owned delivery queue port.
package deliveryqueue

import (
	"errors"

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
}

// Queue is the bounded delivery capability shared by publish and connection
// lifecycle use cases. TryEnqueue and TryEnqueueBatch read DeliveryDTO.Payload
// synchronously; implementations must copy it before retaining delivery.
type Queue interface {
	Register(sessionID routing.SessionID) error
	Remove(sessionID routing.SessionID)
	TryEnqueue(delivery DeliveryDTO) error
	TryEnqueueBatch(deliveries []DeliveryDTO) error
	Drain(sessionID routing.SessionID) ([]DeliveryDTO, error)
}
