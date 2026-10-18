package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"narumq/internal/application/membership"
	applicationretained "narumq/internal/application/ports/retained"
	"narumq/internal/application/publish"
	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/tenancy"
)

const clusterRetainedCommitTimeout = 5 * time.Second

var errClusterRetainedPublisher = errors.New("retained publisher is not a cluster session")

func (runtime *clusterRuntime) CommitRetained(ctx context.Context, change publish.RetainedChangeDTO) error {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, clusterRetainedCommitTimeout)
	defer cancel()

	publisher, err := tenancy.ParseSessionKeyRoutingID(string(change.Message.PublisherSessionID))
	if err != nil || publisher.TenantID() != change.Message.TenantID {
		return errClusterRetainedPublisher
	}
	if len(change.Message.Payload) > membership.MaxRetainedPayloadBytes {
		return applicationretained.ErrFull
	}

	action := membership.RetainedActionReplace
	if change.Delete {
		action = membership.RetainedActionDelete
	}
	var expiresAtUnixMillis uint64
	if !change.Message.ExpiresAt.IsZero() {
		expiresAtUnixMillis = uint64(change.Message.ExpiresAt.UnixMilli())
	}
	operationID, err := newClusterOperationID()
	if err != nil {
		return err
	}
	result, err := runtime.Commit(ctx, membership.CommandDTO{
		SchemaMajor: membership.SchemaMajor, SchemaMinor: membership.SchemaMinor,
		OperationID: operationID,
		Retained: &membership.RetainedChangeDTO{
			TenantID: change.Message.TenantID.String(), Topic: change.Message.Topic,
			PublisherClientID: publisher.ClientID(), QoS: uint32(change.Message.QoS),
			Payload: change.Message.Payload, ExpiresAtUnixMillis: expiresAtUnixMillis,
			ProposedAtUnixMillis: uint64(change.ProposedAt.UnixMilli()), Action: action,
		},
	})
	return clusterRetainedCommitError(result, err)
}

func clusterRetainedCommitError(result membership.ApplyResultDTO, err error) error {
	if err != nil {
		return err
	}
	if result.Outcome == membership.ApplyOutcomeApplied {
		return nil
	}
	if result.Outcome == membership.ApplyOutcomeConflict && result.Conflict == membership.ConflictRetainedCapacity {
		if result.TenantQuota != nil {
			return publish.NewRejectionError(applicationretained.ErrFull, publish.RejectionClassTenantLimit, publish.RejectionCauseTenantQuota)
		}
		return applicationretained.ErrFull
	}
	return membership.ErrQuorumUnavailable
}

func clusterRetainedMessage(state membership.RetainedStateDTO) (applicationretained.MessageDTO, error) {
	tenantID, err := tenancy.NewTenantID(state.TenantID)
	if err != nil {
		return applicationretained.MessageDTO{}, fmt.Errorf("committed retained tenant is invalid: %w", err)
	}
	publisherSessionID, err := clusterSessionID(state.TenantID, state.PublisherClientID)
	if err != nil {
		return applicationretained.MessageDTO{}, fmt.Errorf("committed retained publisher is invalid: %w", err)
	}
	var expiresAt time.Time
	if state.ExpiresAtUnixMillis != 0 {
		expiresAt = time.UnixMilli(int64(state.ExpiresAtUnixMillis))
	}
	return applicationretained.MessageDTO{
		TenantID: tenantID, PublisherSessionID: publisherSessionID, Topic: state.Topic,
		Payload: state.Payload, QoS: mqtt.QoS(state.QoS), ExpiresAt: expiresAt,
	}, nil
}

// rebuildRetainedProjectionLocked replaces the local cache from committed state.
// The caller must hold projectionMu.
func (runtime *clusterRuntime) rebuildRetainedProjectionLocked() error {
	messages := runtime.machine.RetainedMessages()
	projection := make([]applicationretained.MessageDTO, 0, len(messages))
	for _, state := range messages {
		message, err := clusterRetainedMessage(state)
		if err != nil {
			return err
		}
		projection = append(projection, message)
	}
	if err := runtime.retained.ReplaceAll(projection); err != nil {
		return fmt.Errorf("replace committed retained projection: %w", err)
	}
	runtime.retainedResyncRequired.Store(false)
	return nil
}

func (runtime *clusterRuntime) syncRetainedProjection(change membership.RetainedChangeDTO) {
	runtime.projectionMu.Lock()
	defer runtime.projectionMu.Unlock()

	if runtime.retainedResyncRequired.Load() {
		if err := runtime.rebuildRetainedProjectionLocked(); err != nil {
			runtime.failRetainedProjection(err)
		}
		return
	}

	var err error
	switch change.Action {
	case membership.RetainedActionReplace:
		message, conversionErr := clusterRetainedMessage(membership.RetainedStateDTO{
			TenantID: change.TenantID, Topic: change.Topic, PublisherClientID: change.PublisherClientID,
			QoS: change.QoS, Payload: change.Payload, ExpiresAtUnixMillis: change.ExpiresAtUnixMillis,
		})
		if conversionErr != nil {
			err = conversionErr
		} else {
			err = runtime.retained.Replace(message, time.Now())
		}
	case membership.RetainedActionDelete:
		var tenantID tenancy.TenantID
		tenantID, err = tenancy.NewTenantID(change.TenantID)
		if err == nil {
			err = runtime.retained.Delete(tenantID, change.Topic)
		}
	default:
		err = fmt.Errorf("committed retained action %d is unsupported", change.Action)
	}
	if err == nil {
		return
	}
	if rebuildErr := runtime.rebuildRetainedProjectionLocked(); rebuildErr != nil {
		runtime.failRetainedProjection(rebuildErr)
	}
}

func (runtime *clusterRuntime) failRetainedProjection(err error) {
	runtime.retainedResyncRequired.Store(true)
	if !runtime.projectionFailed.Swap(true) {
		runtime.logger.Warn("cluster retained projection rebuild failed", "error", err.Error())
	}
}

var _ publish.RetainedCommitter = (*clusterRuntime)(nil)
