// Package deliverystate defines the application-owned port for committing
// durable delivery transitions to replicated state.
package deliverystate

import (
	"context"

	"narumq/internal/application/membership"
	"narumq/internal/domain/routing"
)

// Committer returns the deterministic application result for a committed
// delivery-state transition.
type Committer interface {
	CommitDelivery(ctx context.Context, change membership.DeliveryChangeDTO) (membership.ApplyResultDTO, error)
}

// Activator marks a session's durable queue as projected after local reset.
// Runtime implementations use loadedThrough to deliver commits that raced with
// attachment without re-enqueuing records already included in the reset.
type Activator interface {
	ActivateDelivery(sessionID routing.SessionID, epoch, loadedThrough uint64) error
}

// Deactivator removes a failed or disconnected local durable projection.
type Deactivator interface {
	DeactivateDelivery(sessionID routing.SessionID, epoch uint64)
}
