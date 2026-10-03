// Package deliverystate defines the application-owned port for committing
// durable delivery transitions to replicated state.
package deliverystate

import (
	"context"

	"narumq/internal/application/membership"
	"narumq/internal/domain/routing"
)

// Pending owns a queued transition. Wait must be called exactly once, including
// on connection shutdown, to finish waiting and release its deadline resources.
// Queueing alone is not a quorum, durability, or success proof.
type Pending interface {
	Wait() (membership.ApplyResultDTO, error)
}

// Committer returns the deterministic application result for a committed
// delivery-state transition.
type Committer interface {
	CommitDelivery(ctx context.Context, change membership.DeliveryChangeDTO) (membership.ApplyResultDTO, error)
	// BeginDelivery queues a change; Raft submission requires its fresh quorum proof.
	BeginDelivery(ctx context.Context, change membership.DeliveryChangeDTO) (Pending, error)
}

// QuorumBarrier owns one queued proposal-free quorum read. Wait must be called
// exactly once; nil means a fresh quorum proof was applied and projected locally.
type QuorumBarrier interface {
	Wait() error
}

// PublishCommitter is the publish use case's durable port. Publishes with no
// durable delivery target use BeginQuorumBarrier instead of an empty PUBLISH.
type PublishCommitter interface {
	Committer
	// BeginQuorumBarrier queues a fresh quorum ReadIndex proof without proposing state.
	BeginQuorumBarrier(ctx context.Context) (QuorumBarrier, error)
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
