package publish

import (
	"context"
	"errors"
	"fmt"
	"time"

	"narumq/internal/application/membership"
	"narumq/internal/application/ports/deliveryqueue"
	"narumq/internal/application/ports/deliverystate"
	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/routing"
	"narumq/internal/domain/security"
)

// PendingReceive must be waited exactly once before acknowledging the RECEIVE.
type PendingReceive struct {
	pending deliverystate.Pending
}

func (pending PendingReceive) Wait() error {
	committed, err := pending.pending.Wait()
	_, err = durableDeliveryResult(committed, err)
	return err
}

// PendingPublish retains the prepared routing plan until its durable transition
// or proposal-free quorum barrier completes. Wait must be called exactly once,
// in input order by a TCP lane.
type PendingPublish struct {
	useCase  *UseCase
	prepared preparedPublication
	pending  deliverystate.Pending
	barrier  deliverystate.QuorumBarrier
	targets  []durableTarget
	started  time.Time
	release  bool
}

// BeginPublish submits a non-retained durable QoS 1 PUBLISH in caller order.
// Submission does not permit a success PUBACK; Wait is the completion boundary.
func (useCase *UseCase) BeginPublish(
	publisherSessionID routing.SessionID,
	packet mqtt.Publish,
	receivedAt time.Time,
	principal ...security.Principal,
) (PendingPublish, error) {
	if useCase == nil {
		return PendingPublish{}, ErrNilClock
	}
	if !useCase.DurableEnabled() || packet.QoS != mqtt.QoSAtLeastOnce || packet.Retain {
		return PendingPublish{}, ErrDurableDeliveryRejected
	}
	return useCase.beginPublish(publisherSessionID, packet, receivedAt, nil, principal...)
}

// BeginQoS2Release queues a non-retained RELEASE in input order. PUBCOMP
// remains forbidden until the returned pending transition completes.
func (useCase *UseCase) BeginQoS2Release(
	publisherSessionID routing.SessionID,
	ownerNodeID, epoch uint64,
	packet mqtt.Publish,
	receivedAt time.Time,
	principal ...security.Principal,
) (PendingPublish, error) {
	if useCase == nil {
		return PendingPublish{}, ErrNilClock
	}
	if !useCase.DurableEnabled() || packet.QoS != mqtt.QoSExactlyOnce || packet.Retain ||
		ownerNodeID == 0 || epoch == 0 || packet.PacketID == 0 {
		return PendingPublish{}, ErrDurableDeliveryRejected
	}
	return useCase.beginPublish(publisherSessionID, packet, receivedAt,
		&durableRelease{ownerNodeID: ownerNodeID, epoch: epoch}, principal...)
}

func (useCase *UseCase) beginPublish(
	publisherSessionID routing.SessionID,
	packet mqtt.Publish,
	receivedAt time.Time,
	release *durableRelease,
	principal ...security.Principal,
) (PendingPublish, error) {
	var started time.Time
	if useCase.metrics != nil {
		started = time.Now()
	}
	submitted := false
	defer func() {
		if !submitted && useCase.metrics != nil {
			useCase.metrics.ObservePublishDuration(time.Since(started))
		}
	}()
	if release == nil && useCase.admission != nil {
		if err := useCase.admission.CheckPressure(); err != nil {
			return PendingPublish{}, err
		}
	}
	prepared, err := useCase.preparePublication(publisherSessionID, packet, receivedAt, nil, principal...)
	if err != nil {
		return PendingPublish{}, err
	}
	change, targets, err := useCase.prepareDurableDelivery(publisherSessionID, nil, packet, prepared.expiresAt, prepared.now, prepared.matches, release)
	if err != nil {
		return PendingPublish{}, err
	}
	if release == nil && useCase.admission != nil && len(targets) > 0 {
		if err := useCase.admission.CheckDeliveryCapacity(change); err != nil {
			return PendingPublish{}, err
		}
	}
	if !needsDeliveryCommand(release, targets) {
		barrier, err := useCase.durableDeliveryCommitter.BeginQuorumBarrier(context.Background())
		if err != nil {
			return PendingPublish{}, err
		}
		submitted = true
		return PendingPublish{useCase: useCase, prepared: prepared, barrier: barrier, started: started}, nil
	}
	pending, err := useCase.durableDeliveryCommitter.BeginDelivery(context.Background(), change)
	if err != nil {
		_, err = durableDeliveryResult(membership.ApplyResultDTO{}, err)
		return PendingPublish{}, err
	}
	submitted = true
	return PendingPublish{useCase: useCase, prepared: prepared, pending: pending, targets: targets, started: started, release: release != nil}, nil
}

func (pending PendingPublish) Wait() (ResultDTO, error) {
	if pending.useCase.metrics != nil {
		defer func() {
			pending.useCase.metrics.ObservePublishDuration(time.Since(pending.started))
		}()
	}
	if pending.barrier != nil {
		if err := pending.barrier.Wait(); err != nil {
			return ResultDTO{}, err
		}
		return pending.useCase.completePublication(pending.prepared, &membership.DeliveryResultDTO{}, nil, false)
	}
	committed, err := pending.pending.Wait()
	deliveryResult, err := durableDeliveryResult(committed, err)
	if err != nil {
		return ResultDTO{}, err
	}
	return pending.useCase.completePublication(pending.prepared, &deliveryResult, pending.targets, pending.release)
}

func durableDeliveryResult(committed membership.ApplyResultDTO, err error) (membership.DeliveryResultDTO, error) {
	if err != nil {
		if errors.Is(err, deliveryqueue.ErrFull) {
			return membership.DeliveryResultDTO{}, NewRejectionError(ErrDurableDeliveryCapacity, RejectionClassServerBackpressure, RejectionCauseReceiveCapacity)
		}
		return membership.DeliveryResultDTO{}, err
	}
	if committed.Outcome != membership.ApplyOutcomeApplied || committed.Delivery == nil {
		if committed.Conflict == membership.ConflictDeliveryCapacity {
			if committed.TenantQuota != nil {
				return membership.DeliveryResultDTO{}, NewRejectionError(ErrDurableDeliveryCapacity, RejectionClassTenantLimit, RejectionCauseTenantQuota)
			}
			return membership.DeliveryResultDTO{}, NewRejectionError(ErrDurableDeliveryCapacity, RejectionClassServerBackpressure, RejectionCauseDeliveryCapacity)
		}
		return membership.DeliveryResultDTO{}, fmt.Errorf("%w: outcome=%d conflict=%s", ErrDurableDeliveryRejected, committed.Outcome, committed.Conflict)
	}
	return *committed.Delivery, nil
}
