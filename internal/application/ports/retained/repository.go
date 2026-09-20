// Package retained defines the application-owned retained-message repository port.
package retained

import (
	"errors"

	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/routing"
)

var (
	// ErrFull means that the retained repository cannot accept another value
	// within its configured entry or byte capacity.
	ErrFull = errors.New("retained repository capacity exceeded")
)

// MessageDTO is the application boundary value for one retained publication.
// Callers lend Payload for the duration of a method call. Implementations that
// retain or return it must copy it.
type MessageDTO struct {
	PublisherSessionID routing.SessionID
	Topic              string
	Payload            []byte
	QoS                mqtt.QoS
}

// Repository stores one retained value per exact topic. List returns values
// sorted by Topic; every returned payload is independent from repository
// storage and from every other returned value.
type Repository interface {
	Replace(message MessageDTO) error
	Delete(topic string) error
	List() ([]MessageDTO, error)
}
