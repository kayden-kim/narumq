package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"narumq/internal/adapters/raft/records"
	"narumq/internal/application/connect"
	"narumq/internal/application/membership"
	"narumq/internal/application/ports/deliveryqueue"
	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/tenancy"
)

const clusterOfflineCommitTimeout = 5 * time.Second

var errClusterOfflineConflict = errors.New("cluster offline command conflict")

func (runtime *clusterRuntime) DetachSession(ctx context.Context, request connect.DetachRequestDTO) error {
	if ctx == nil {
		ctx = context.Background()
	}

	for _, d := range request.Pending {
		if d.QoS != mqtt.QoSAtMostOnce {
			return errors.New("cluster offline detach requires QoS 0")
		}
	}

	var sessionExpiresAtUnixMillis uint64
	if !request.SessionExpiresAt.IsZero() {
		sessionExpiresAtUnixMillis = uint64(request.SessionExpiresAt.UnixMilli())
	}
	proposedAtUnixMillis := uint64(request.DetachedAt.UnixMilli())

	// Split pending messages into primary detach batch and subsequent enqueue messages.
	detachMessages := make([]membership.OfflineMessageDTO, 0, len(request.Pending))
	var overflowMessages []deliveryqueue.DeliveryDTO

	totalDetachBytes := 0
	for i, d := range request.Pending {
		msgBytes := len(d.Topic) + len(d.Payload)
		if len(detachMessages) < membership.MaxOfflineMessagesPerSession && totalDetachBytes+msgBytes <= membership.MaxOfflineDetachMessageBytes {
			totalDetachBytes += msgBytes
			var exp uint64
			if !d.ExpiresAt.IsZero() {
				exp = uint64(d.ExpiresAt.UnixMilli())
			}
			detachMessages = append(detachMessages, membership.OfflineMessageDTO{
				Sequence:            0,
				Topic:               d.Topic,
				Payload:             d.Payload,
				QoS:                 uint32(d.QoS),
				Retain:              d.Retain,
				ExpiresAtUnixMillis: exp,
			})
		} else {
			overflowMessages = request.Pending[i:]
			break
		}
	}

	opID, err := newClusterOperationID()
	if err != nil {
		return err
	}

	detachCtx, cancel := context.WithTimeout(ctx, clusterOfflineCommitTimeout)
	res, err := runtime.Commit(detachCtx, membership.CommandDTO{
		SchemaMajor: membership.SchemaMajor, SchemaMinor: membership.SchemaMinor,
		OperationID: opID,
		Offline: &membership.OfflineChangeDTO{
			TenantID:                   request.TenantID,
			ClientID:                   request.ClientID,
			OwnerNodeID:                request.OwnerNodeID,
			Epoch:                      request.Epoch,
			Action:                     membership.OfflineActionDetach,
			SessionExpiresAtUnixMillis: sessionExpiresAtUnixMillis,
			ProposedAtUnixMillis:       proposedAtUnixMillis,
			Messages:                   detachMessages,
		},
	})
	cancel()
	if err != nil {
		return err
	}
	if res.Outcome != membership.ApplyOutcomeApplied {
		return fmt.Errorf("%w: %s", errClusterOfflineConflict, res.Conflict)
	}

	// Commit any remaining messages one by one via ENQUEUE.
	for i, d := range overflowMessages {
		var exp uint64
		if !d.ExpiresAt.IsZero() {
			exp = uint64(d.ExpiresAt.UnixMilli())
		}
		enqOpID, enqErr := newClusterOperationID()
		if enqErr != nil {
			runtime.logger.Warn("offline detach tail dropped", "tenant", request.TenantID, "client", request.ClientID, "dropped", len(overflowMessages)-i)
			return nil
		}
		enqCtx, enqCancel := context.WithTimeout(ctx, clusterOfflineCommitTimeout)
		enqRes, enqErr := runtime.Commit(enqCtx, membership.CommandDTO{
			SchemaMajor: membership.SchemaMajor, SchemaMinor: membership.SchemaMinor,
			OperationID: enqOpID,
			Offline: &membership.OfflineChangeDTO{
				TenantID:             request.TenantID,
				ClientID:             request.ClientID,
				OwnerNodeID:          0,
				Epoch:                request.Epoch,
				Action:               membership.OfflineActionEnqueue,
				ProposedAtUnixMillis: proposedAtUnixMillis,
				Messages: []membership.OfflineMessageDTO{{
					Sequence:            0,
					Topic:               d.Topic,
					Payload:             d.Payload,
					QoS:                 uint32(d.QoS),
					Retain:              d.Retain,
					ExpiresAtUnixMillis: exp,
				}},
			},
		})
		enqCancel()
		if enqErr != nil || enqRes.Outcome != membership.ApplyOutcomeApplied {
			runtime.logger.Warn("offline detach tail dropped", "tenant", request.TenantID, "client", request.ClientID, "dropped", len(overflowMessages)-i)
			return nil
		}
	}

	return nil
}

func (runtime *clusterRuntime) ClaimOffline(ctx context.Context, tenantID, clientID string, ownerNodeID, epoch uint64, claimedAt time.Time) (connect.OfflineClaimDTO, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	claimCtx, cancel := context.WithTimeout(ctx, clusterOfflineCommitTimeout)
	defer cancel()

	opID, err := newClusterOperationID()
	if err != nil {
		return connect.OfflineClaimDTO{}, err
	}

	res, err := runtime.Commit(claimCtx, membership.CommandDTO{
		SchemaMajor: membership.SchemaMajor, SchemaMinor: membership.SchemaMinor,
		OperationID: opID,
		Offline: &membership.OfflineChangeDTO{
			TenantID:             tenantID,
			ClientID:             clientID,
			OwnerNodeID:          ownerNodeID,
			Epoch:                epoch,
			Action:               membership.OfflineActionClaim,
			ProposedAtUnixMillis: uint64(claimedAt.UnixMilli()),
		},
	})
	if err != nil {
		return connect.OfflineClaimDTO{}, err
	}
	if res.Outcome != membership.ApplyOutcomeApplied {
		return connect.OfflineClaimDTO{}, fmt.Errorf("%w: %s", errClusterOfflineConflict, res.Conflict)
	}
	if res.OfflineClaim == nil || !res.OfflineClaim.Found {
		return connect.OfflineClaimDTO{Found: false}, nil
	}

	sessionID, err := clusterSessionID(tenantID, clientID)
	if err != nil {
		return connect.OfflineClaimDTO{}, err
	}

	deliveries := make([]deliveryqueue.DeliveryDTO, 0, len(res.OfflineClaim.Messages))
	for _, msg := range res.OfflineClaim.Messages {
		var expiresAt time.Time
		if msg.ExpiresAtUnixMillis > 0 {
			expiresAt = time.UnixMilli(int64(msg.ExpiresAtUnixMillis))
		}
		deliveries = append(deliveries, deliveryqueue.DeliveryDTO{
			SessionID: sessionID,
			Topic:     msg.Topic,
			Payload:   msg.Payload,
			QoS:       mqtt.QoS(msg.QoS),
			Retain:    msg.Retain,
			ExpiresAt: expiresAt,
		})
	}

	return connect.OfflineClaimDTO{
		Found:      true,
		Deliveries: deliveries,
	}, nil
}

func (runtime *clusterRuntime) CommitOffline(ctx context.Context, delivery deliveryqueue.DeliveryDTO, proposedAt time.Time) (bool, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	sessionKey, err := tenancy.ParseSessionKeyRoutingID(string(delivery.SessionID))
	if err != nil {
		return false, nil
	}

	epoch, detached := runtime.machine.DetachedEpoch(sessionKey.TenantID().String(), sessionKey.ClientID())
	if !detached {
		return false, nil
	}

	if delivery.QoS != mqtt.QoSAtMostOnce {
		return true, deliveryqueue.ErrUnavailable
	}
	if len(delivery.Payload) > membership.MaxOfflinePayloadBytes {
		return true, deliveryqueue.ErrFull
	}

	var expiresAtUnixMillis uint64
	if !delivery.ExpiresAt.IsZero() {
		expiresAtUnixMillis = uint64(delivery.ExpiresAt.UnixMilli())
	}

	opID, err := newClusterOperationID()
	if err != nil {
		return true, deliveryqueue.ErrUnavailable
	}

	commitCtx, cancel := context.WithTimeout(ctx, clusterOfflineCommitTimeout)
	defer cancel()

	res, err := runtime.Commit(commitCtx, membership.CommandDTO{
		SchemaMajor: membership.SchemaMajor, SchemaMinor: membership.SchemaMinor,
		OperationID: opID,
		Offline: &membership.OfflineChangeDTO{
			TenantID:             sessionKey.TenantID().String(),
			ClientID:             sessionKey.ClientID(),
			OwnerNodeID:          0,
			Epoch:                epoch,
			Action:               membership.OfflineActionEnqueue,
			ProposedAtUnixMillis: uint64(proposedAt.UnixMilli()),
			Messages: []membership.OfflineMessageDTO{{
				Sequence:            0,
				Topic:               delivery.Topic,
				Payload:             delivery.Payload,
				QoS:                 uint32(delivery.QoS),
				Retain:              delivery.Retain,
				ExpiresAtUnixMillis: expiresAtUnixMillis,
			}},
		},
	})
	if err != nil {
		if errors.Is(err, records.ErrCommandTooLarge) {
			return true, deliveryqueue.ErrFull
		}
		return true, deliveryqueue.ErrUnavailable
	}
	if res.Outcome == membership.ApplyOutcomeApplied {
		return true, nil
	}
	if res.Outcome == membership.ApplyOutcomeConflict && res.Conflict == membership.ConflictOfflineCapacity {
		return true, deliveryqueue.ErrFull
	}
	return true, deliveryqueue.ErrUnavailable
}
