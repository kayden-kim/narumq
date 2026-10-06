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
	"narumq/internal/application/ports/deliverystate"
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
	pending, err := runtime.BeginDelivery(ctx, change)
	if err != nil {
		return membership.ApplyResultDTO{}, err
	}
	return pending.Wait()
}

func (runtime *clusterRuntime) BeginDelivery(
	ctx context.Context,
	change membership.DeliveryChangeDTO,
) (deliverystate.Pending, error) {
	if runtime == nil {
		return nil, membership.ErrQuorumUnavailable
	}
	if change.Action == membership.DeliveryActionPublish || change.Action == membership.DeliveryActionRelease {
		if _, err := clusterSessionID(change.TenantID, change.ClientID); err != nil {
			return nil, err
		}
		for _, target := range change.Targets {
			if _, err := clusterSessionID(change.TenantID, target.ClientID); err != nil {
				return nil, err
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
	operationID, err := newClusterOperationID()
	if err != nil {
		cancel()
		return nil, err
	}
	entry := runtime.addCommitBacklog(time.Now())
	pending, err := runtime.beginCommit(commitCtx, membership.CommandDTO{
		SchemaMajor: membership.SchemaMajor, SchemaMinor: membership.SchemaMinor,
		OperationID: operationID, Delivery: &change,
	}, entry)
	if err != nil {
		runtime.removeCommitBacklog(entry)
		cancel()
		if errors.Is(err, records.ErrCommandTooLarge) {
			return nil, applicationdeliveryqueue.ErrFull
		}
		return nil, err
	}
	pending.deliveryCancel = cancel
	return pending, nil
}

type clusterQuorumBarrier struct {
	request *clusterReadRequest
	cancel  context.CancelFunc
}

func (barrier clusterQuorumBarrier) Wait() error {
	defer barrier.cancel()
	if _, err := barrier.request.wait(); err != nil {
		return classifyClusterPortError(err)
	}
	return nil
}

// BeginQuorumBarrier replaces an empty-target PUBLISH command with a fresh
// quorum ReadIndex that is locally applied and projected; it proposes nothing.
func (runtime *clusterRuntime) BeginQuorumBarrier(ctx context.Context) (deliverystate.QuorumBarrier, error) {
	if runtime == nil || runtime.storage.view().health == clusterReplicationFailed {
		return nil, membership.ErrQuorumUnavailable
	}
	if ctx == nil {
		ctx = context.Background()
	}
	barrierCtx, cancel := context.WithTimeout(ctx, clusterDurableCommitTimeout)
	request, err := runtime.beginReadBarrier(barrierCtx)
	if err != nil {
		cancel()
		return nil, classifyClusterPortError(err)
	}
	return clusterQuorumBarrier{request: request, cancel: cancel}, nil
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

func (queue *clusterDurableQueue) PeekBatch(sessionID routing.SessionID, now time.Time, destination []applicationdeliveryqueue.DeliveryDTO) (int, error) {
	if queue == nil || queue.Queue == nil {
		return 0, applicationdeliveryqueue.ErrUnavailable
	}
	return queue.Queue.PeekBatch(sessionID, now, destination)
}

func (queue *clusterDurableQueue) PromoteBatch(outbounds []applicationdeliveryqueue.OutboundDTO, now time.Time) (int, error) {
	if queue == nil || queue.Queue == nil {
		return 0, applicationdeliveryqueue.ErrUnavailable
	}
	if len(outbounds) == 0 {
		return 0, nil
	}
	type pendingItem struct {
		pending  deliverystate.Pending
		beginErr error
	}
	pendingItems := make([]pendingItem, len(outbounds))
	for i, outbound := range outbounds {
		if outbound.DurableSequence == 0 || (!outbound.ExpiresAt.IsZero() && !now.Before(outbound.ExpiresAt)) {
			continue
		}
		key, err := tenancy.ParseSessionKeyRoutingID(string(outbound.SessionID))
		if err != nil {
			pendingItems[i].beginErr = err
			break
		}
		if now.UnixMilli() <= 0 {
			pendingItems[i].beginErr = membership.ErrInvalidCommand
			break
		}
		p, err := queue.runtime.BeginDelivery(context.Background(), membership.DeliveryChangeDTO{
			TenantID: key.TenantID().String(), ClientID: key.ClientID(), OwnerNodeID: queue.runtime.config.NodeID,
			Epoch: outbound.DurableEpoch, Action: membership.DeliveryActionSend,
			ProposedAtUnixMillis: uint64(now.UnixMilli()), Sequence: outbound.DurableSequence,
			PacketID: uint32(outbound.PacketID),
		})
		pendingItems[i] = pendingItem{pending: p, beginErr: err}
		if err != nil {
			break
		}
	}
	results := make([]struct {
		res membership.ApplyResultDTO
		err error
	}, len(outbounds))
	for i, item := range pendingItems {
		if item.beginErr != nil {
			results[i].err = item.beginErr
		} else if item.pending != nil {
			results[i].res, results[i].err = item.pending.Wait()
		}
	}
	promoted := 0
	for i, outbound := range outbounds {
		if outbound.DurableSequence > 0 && (outbound.ExpiresAt.IsZero() || now.Before(outbound.ExpiresAt)) {
			r := results[i]
			if errors.Is(r.err, membership.ErrQuorumUnavailable) {
				return promoted, applicationdeliveryqueue.ErrRetryableUnavailable
			}
			if r.err != nil {
				return promoted, r.err
			}
			if r.res.Outcome != membership.ApplyOutcomeApplied {
				if r.res.Conflict == membership.ConflictDeliveryMissing {
					if queue.runtime != nil && queue.runtime.logger != nil {
						queue.runtime.logger.Warn("durable delivery missing conflict on promote, dropping queued head", "session_id", outbound.SessionID, "sequence", outbound.DurableSequence)
					}
					_ = queue.Queue.CompleteHead(outbound.SessionID)
					return promoted, applicationdeliveryqueue.ErrExpired
				}
				return promoted, durableApplyError(r.res)
			}
		}
		if err := queue.Queue.PromoteHead(outbound, now); err != nil {
			return promoted, err
		}
		promoted++
	}
	return promoted, nil
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

type clusterPendingOutbound struct {
	queue    *clusterDurableQueue
	outbound applicationdeliveryqueue.OutboundDTO
	pending  deliverystate.Pending
	complete bool
}

func (pending clusterPendingOutbound) Wait() error {
	result, err := pending.pending.Wait()
	if err != nil {
		return err
	}
	if result.Outcome != membership.ApplyOutcomeApplied {
		return durableApplyError(result)
	}
	if pending.complete {
		return pending.queue.Queue.CompleteOutbound(pending.outbound.SessionID, pending.outbound.PacketID)
	}
	return pending.queue.Queue.ReplaceOutbound(pending.outbound)
}

func (queue *clusterDurableQueue) beginOutbound(outbound applicationdeliveryqueue.OutboundDTO, complete bool) (applicationdeliveryqueue.PendingTransition, error) {
	if queue == nil || queue.Queue == nil {
		return nil, applicationdeliveryqueue.ErrUnavailable
	}
	if outbound.DurableSequence == 0 {
		if complete {
			return queue.Queue.BeginCompleteOutbound(outbound)
		}
		return queue.Queue.BeginReplaceOutbound(outbound)
	}
	key, err := tenancy.ParseSessionKeyRoutingID(string(outbound.SessionID))
	if err != nil {
		return nil, err
	}
	now := queue.clock.Now()
	if now.UnixMilli() <= 0 {
		return nil, membership.ErrInvalidCommand
	}
	action := membership.DeliveryActionAdvance
	if complete {
		action = membership.DeliveryActionComplete
	}
	pending, err := queue.runtime.BeginDelivery(context.Background(), membership.DeliveryChangeDTO{
		TenantID: key.TenantID().String(), ClientID: key.ClientID(), OwnerNodeID: queue.runtime.config.NodeID,
		Epoch: outbound.DurableEpoch, Action: action, ProposedAtUnixMillis: uint64(now.UnixMilli()),
		PacketID: uint32(outbound.PacketID),
	})
	if err != nil {
		return nil, err
	}
	return clusterPendingOutbound{queue: queue, outbound: outbound, pending: pending, complete: complete}, nil
}

func (queue *clusterDurableQueue) BeginReplaceOutbound(outbound applicationdeliveryqueue.OutboundDTO) (applicationdeliveryqueue.PendingTransition, error) {
	return queue.beginOutbound(outbound, false)
}

func (queue *clusterDurableQueue) BeginCompleteOutbound(outbound applicationdeliveryqueue.OutboundDTO) (applicationdeliveryqueue.PendingTransition, error) {
	return queue.beginOutbound(outbound, true)
}
