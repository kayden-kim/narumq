package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"narumq/internal/adapters/raft/records"
	"narumq/internal/application/membership"
	applicationclock "narumq/internal/application/ports/clock"
	applicationdeliveryqueue "narumq/internal/application/ports/deliveryqueue"
	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/routing"
	"narumq/internal/domain/tenancy"
)

const clusterDurableCommitTimeout = 5 * time.Second

var (
	errClusterDurableStale    = errors.New("durable delivery owner epoch is stale")
	errClusterDurableConflict = errors.New("durable delivery transition conflicted")
)

type clusterDurableAttachment struct {
	epoch         uint64
	loadedThrough uint64
}

type clusterDurableSink interface {
	DeliverDurable(applicationdeliveryqueue.DeliveryDTO) error
}

// clusterDurableQueue commits sender-side state transitions before mutating the
// process-local projection used by the TCP writer.
type clusterDurableQueue struct {
	applicationdeliveryqueue.Queue
	runtime *clusterRuntime
	clock   applicationclock.Clock
}

func (runtime *clusterRuntime) CommitDelivery(
	ctx context.Context,
	change membership.DeliveryChangeDTO,
) (membership.ApplyResultDTO, error) {
	if runtime == nil {
		return membership.ApplyResultDTO{}, membership.ErrQuorumUnavailable
	}
	if change.Action == membership.DeliveryActionPublish || change.Action == membership.DeliveryActionRelease {
		if _, err := clusterSessionID(change.TenantID, change.ClientID); err != nil {
			return membership.ApplyResultDTO{}, err
		}
		for _, target := range change.Targets {
			if _, err := clusterSessionID(change.TenantID, target.ClientID); err != nil {
				return membership.ApplyResultDTO{}, err
			}
		}
	}
	if change.Action == membership.DeliveryActionDetach || change.Action == membership.DeliveryActionDiscard {
		if sessionID, err := clusterSessionID(change.TenantID, change.ClientID); err == nil {
			runtime.durableMu.Lock()
			runtime.ensureDurableAttachmentsLocked()
			if attachment, exists := runtime.durableAttachments[sessionID]; exists && attachment.epoch == change.Epoch {
				delete(runtime.durableAttachments, sessionID)
			}
			runtime.durableMu.Unlock()
		}
	}
	if ctx == nil {
		ctx = context.Background()
	}
	commitCtx, cancel := context.WithTimeout(ctx, clusterDurableCommitTimeout)
	defer cancel()
	operationID, err := newClusterOperationID()
	if err != nil {
		return membership.ApplyResultDTO{}, err
	}
	result, err := runtime.Commit(commitCtx, membership.CommandDTO{
		SchemaMajor: membership.SchemaMajor, SchemaMinor: membership.SchemaMinor,
		OperationID: operationID, Delivery: &change,
	})
	if errors.Is(err, records.ErrCommandTooLarge) {
		return membership.ApplyResultDTO{}, applicationdeliveryqueue.ErrFull
	}
	return result, err
}

func (runtime *clusterRuntime) ActivateDelivery(sessionID routing.SessionID, epoch, loadedThrough uint64) error {
	if runtime == nil || runtime.machine == nil || epoch == 0 {
		return errClusterDurableConflict
	}
	key, err := tenancy.ParseSessionKeyRoutingID(string(sessionID))
	if err != nil {
		return err
	}
	runtime.durableMu.Lock()
	defer runtime.durableMu.Unlock()
	runtime.ensureDurableAttachmentsLocked()
	owner, exists := runtime.machine.GetOwnership(key.TenantID().String(), key.ClientID())
	if !exists || owner.OwnerNodeID != runtime.config.NodeID || owner.Epoch != epoch {
		return errClusterDurableStale
	}
	state, exists := runtime.machine.DeliverySession(key.TenantID().String(), key.ClientID())
	if !exists {
		return errClusterDurableConflict
	}
	runtime.durableAttachments[sessionID] = clusterDurableAttachment{epoch: epoch, loadedThrough: loadedThrough}
	enqueued := make([]membership.DeliveryEnqueuedDTO, 0)
	for _, message := range state.Messages {
		if message.Sequence > loadedThrough && message.Phase == membership.DeliveryPhaseQueued {
			enqueued = append(enqueued, membership.DeliveryEnqueuedDTO{ClientID: key.ClientID(), Sequence: message.Sequence})
		}
	}
	runtime.notifyDurableLocked(key.TenantID().String(), enqueued)
	return nil
}

func (runtime *clusterRuntime) DeactivateDelivery(sessionID routing.SessionID, epoch uint64) {
	if runtime == nil || epoch == 0 {
		return
	}
	runtime.durableMu.Lock()
	defer runtime.durableMu.Unlock()
	runtime.ensureDurableAttachmentsLocked()
	if attachment, exists := runtime.durableAttachments[sessionID]; exists && attachment.epoch == epoch {
		delete(runtime.durableAttachments, sessionID)
	}
}

func (runtime *clusterRuntime) notifyDurable(tenantID string, enqueued []membership.DeliveryEnqueuedDTO) {
	runtime.durableMu.Lock()
	defer runtime.durableMu.Unlock()
	runtime.ensureDurableAttachmentsLocked()
	runtime.notifyDurableLocked(tenantID, enqueued)
}

func (runtime *clusterRuntime) notifyDurableLocked(tenantID string, enqueued []membership.DeliveryEnqueuedDTO) {
	for _, item := range enqueued {
		sessionID, err := clusterSessionID(tenantID, item.ClientID)
		if err != nil {
			continue
		}
		attachment, exists := runtime.durableAttachments[sessionID]
		if !exists || item.Sequence <= attachment.loadedThrough {
			continue
		}
		owner, exists := runtime.machine.GetOwnership(tenantID, item.ClientID)
		if !exists || owner.OwnerNodeID != runtime.config.NodeID || owner.Epoch != attachment.epoch {
			continue
		}
		message, exists := runtime.machine.DeliveryMessage(tenantID, item.ClientID, item.Sequence)
		if !exists || message.Phase != membership.DeliveryPhaseQueued {
			continue
		}
		expiresAt := time.Time{}
		if message.ExpiresAtUnixMillis > 0 {
			if message.ExpiresAtUnixMillis > uint64(1<<63-1) {
				if runtime.logger != nil {
					runtime.logger.Debug("skip durable delivery with invalid expiry", "session_id", sessionID, "sequence", item.Sequence)
				}
				continue
			}
			expiresAt = time.UnixMilli(int64(message.ExpiresAtUnixMillis))
		}
		attachment.loadedThrough = item.Sequence
		runtime.durableAttachments[sessionID] = attachment
		if runtime.durableSink == nil {
			continue
		}
		delivery := applicationdeliveryqueue.DeliveryDTO{
			SessionID: sessionID, Topic: message.Topic, Payload: message.Payload,
			QoS: mqtt.QoS(message.QoS), Retain: message.Retain, ExpiresAt: expiresAt,
			DurableSequence: message.Sequence, DurableEpoch: attachment.epoch,
		}
		if err := runtime.durableSink.DeliverDurable(delivery); err != nil {
			if runtime.logger != nil {
				runtime.logger.Warn("durable delivery projection was not queued", "session_id", sessionID, "sequence", item.Sequence, "error", err)
			}
		}
	}
}

func (runtime *clusterRuntime) ensureDurableAttachmentsLocked() {
	if runtime.durableAttachments == nil {
		runtime.durableAttachments = make(map[routing.SessionID]clusterDurableAttachment)
	}
}

func (runtime *clusterRuntime) removeStaleDurableAttachment(sessionID routing.SessionID, ownership membership.OwnershipStateDTO) {
	runtime.durableMu.Lock()
	defer runtime.durableMu.Unlock()
	runtime.ensureDurableAttachmentsLocked()
	if attachment, exists := runtime.durableAttachments[sessionID]; exists &&
		(ownership.OwnerNodeID != runtime.config.NodeID || attachment.epoch != ownership.Epoch) {
		delete(runtime.durableAttachments, sessionID)
	}
}

func (runtime *clusterRuntime) commitDurableTransition(
	sessionID routing.SessionID,
	epoch uint64,
	action membership.DeliveryAction,
	sequence uint64,
	packetID mqtt.PacketIdentifier,
	proposedAt time.Time,
) (membership.ApplyResultDTO, error) {
	key, err := tenancy.ParseSessionKeyRoutingID(string(sessionID))
	if err != nil {
		return membership.ApplyResultDTO{}, err
	}
	if proposedAt.UnixMilli() <= 0 {
		return membership.ApplyResultDTO{}, membership.ErrInvalidCommand
	}
	return runtime.CommitDelivery(context.Background(), membership.DeliveryChangeDTO{
		TenantID: key.TenantID().String(), ClientID: key.ClientID(), OwnerNodeID: runtime.config.NodeID,
		Epoch: epoch, Action: action, ProposedAtUnixMillis: uint64(proposedAt.UnixMilli()),
		Sequence: sequence, PacketID: uint32(packetID),
	})
}

func durableApplyError(result membership.ApplyResultDTO) error {
	if result.Conflict == membership.ConflictStaleEpoch {
		return errClusterDurableStale
	}
	return fmt.Errorf("%w: %s", errClusterDurableConflict, result.Conflict)
}

func (queue *clusterDurableQueue) PromoteHead(outbound applicationdeliveryqueue.OutboundDTO, now time.Time) error {
	if queue == nil || queue.Queue == nil {
		return applicationdeliveryqueue.ErrUnavailable
	}
	if outbound.DurableSequence == 0 || (!outbound.ExpiresAt.IsZero() && !now.Before(outbound.ExpiresAt)) {
		return queue.Queue.PromoteHead(outbound, now)
	}
	result, err := queue.runtime.commitDurableTransition(
		outbound.SessionID, outbound.DurableEpoch, membership.DeliveryActionSend,
		outbound.DurableSequence, outbound.PacketID, now,
	)
	if errors.Is(err, membership.ErrQuorumUnavailable) {
		return applicationdeliveryqueue.ErrRetryableUnavailable
	}
	if err != nil {
		return err
	}
	if result.Outcome != membership.ApplyOutcomeApplied {
		if result.Conflict == membership.ConflictDeliveryMissing {
			if queue.runtime != nil && queue.runtime.logger != nil {
				queue.runtime.logger.Warn("durable delivery missing conflict on promote, dropping queued head", "session_id", outbound.SessionID, "sequence", outbound.DurableSequence)
			}
			if err := queue.Queue.CompleteHead(outbound.SessionID); err != nil && !errors.Is(err, applicationdeliveryqueue.ErrUnavailable) {
				return err
			}
			return applicationdeliveryqueue.ErrExpired
		}
		return durableApplyError(result)
	}
	return queue.Queue.PromoteHead(outbound, now)
}

func (queue *clusterDurableQueue) ReplaceOutbound(outbound applicationdeliveryqueue.OutboundDTO) error {
	if queue == nil || queue.Queue == nil {
		return applicationdeliveryqueue.ErrUnavailable
	}
	if outbound.DurableSequence > 0 {
		result, err := queue.runtime.commitDurableTransition(
			outbound.SessionID, outbound.DurableEpoch, membership.DeliveryActionAdvance,
			0, outbound.PacketID, queue.clock.Now(),
		)
		if err != nil {
			return err
		}
		if result.Outcome != membership.ApplyOutcomeApplied {
			return durableApplyError(result)
		}
	}
	return queue.Queue.ReplaceOutbound(outbound)
}

func (queue *clusterDurableQueue) CompleteOutbound(sessionID routing.SessionID, packetID mqtt.PacketIdentifier) error {
	if queue == nil || queue.Queue == nil {
		return applicationdeliveryqueue.ErrUnavailable
	}
	outbound, err := queue.Queue.ListOutbound(sessionID)
	if err != nil {
		return err
	}
	for _, item := range outbound {
		if item.PacketID != packetID || item.DurableSequence == 0 {
			continue
		}
		result, err := queue.runtime.commitDurableTransition(
			sessionID, item.DurableEpoch, membership.DeliveryActionComplete, 0, packetID, queue.clock.Now(),
		)
		if err != nil {
			return err
		}
		if result.Outcome != membership.ApplyOutcomeApplied {
			return durableApplyError(result)
		}
		break
	}
	return queue.Queue.CompleteOutbound(sessionID, packetID)
}
