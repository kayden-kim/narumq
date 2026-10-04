package publish

import (
	"errors"

	"narumq/internal/application/membership"
)

// ErrPublishBackpressure means commit lag or disk pressure rejects new durable publish state.
var ErrPublishBackpressure = errors.New("publish backpressure")

// Admission is consulted before new durable publish state is proposed.
type Admission interface {
	// CheckPressure returns an error wrapping ErrPublishBackpressure while pressure is active.
	CheckPressure() error
	// CheckDeliveryCapacity returns an error wrapping ErrDurableDeliveryCapacity when committed
	// global delivery capacity or the publisher tenant quota cannot hold every eligible target.
	CheckDeliveryCapacity(change membership.DeliveryChangeDTO) error
}

// WithAdmission configures ingress admission for new durable publish state.
func WithAdmission(admission Admission) Option {
	return func(useCase *UseCase) { useCase.admission = admission }
}
