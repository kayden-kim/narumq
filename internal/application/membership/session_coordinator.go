package membership

import "context"

// AcquireOwnership commits a higher session epoch before a cluster-mode CONNACK can succeed.
func (service *Service) AcquireOwnership(ctx context.Context, tenantID, clientID string, ownerNodeID uint64) (uint64, error) {
	if ctx == nil || ctx.Err() != nil {
		return 0, newServiceError(ServiceErrorQuorumUnavailable, true)
	}
	if !validSessionIdentity(tenantID, clientID) || ownerNodeID == 0 {
		return 0, newServiceError(ServiceErrorInvalidRequest, false)
	}
	current, exists, err := service.port.GetOwnership(ctx, tenantID, clientID)
	if err != nil {
		return 0, portFailure(err)
	}
	expectedEpoch := uint64(0)
	nextEpoch := uint64(1)
	if exists {
		expectedEpoch = current.Epoch
		if expectedEpoch == ^uint64(0) {
			return 0, newServiceError(ServiceErrorOperationConflict, false)
		}
		nextEpoch = expectedEpoch + 1
	}
	operationID, err := newOperationID()
	if err != nil {
		return 0, newServiceError(ServiceErrorQuorumUnavailable, true)
	}
	result, err := service.port.Commit(ctx, CommandDTO{
		SchemaMajor: SchemaMajor, SchemaMinor: SchemaMinor, OperationID: operationID,
		Ownership: &OwnershipChangeDTO{
			TenantID: tenantID, ClientID: clientID, ExpectedEpoch: expectedEpoch,
			OwnerNodeID: ownerNodeID, Epoch: nextEpoch, Action: OwnershipActionAcquire,
		},
	})
	if err != nil {
		return 0, portFailure(err)
	}
	if result.Outcome == ApplyOutcomeConflict {
		return 0, conflictServiceError(result.Conflict)
	}
	if result.Outcome != ApplyOutcomeApplied || result.Index == 0 || result.Ownership == nil || result.Ownership.CommittedIndex != result.Index {
		return 0, newServiceError(ServiceErrorQuorumUnavailable, true)
	}
	return result.Ownership.Epoch, nil
}

// CommitSubscription orders a persistent subscription mutation before its MQTT acknowledgement.
func (service *Service) CommitSubscription(
	ctx context.Context,
	tenantID, clientID string,
	ownerNodeID, epoch uint64,
	filter string,
	qos uint32,
	noLocal, retainAsPublished bool,
	retainHandling uint32,
	action uint32,
) error {
	if ctx == nil || ctx.Err() != nil {
		return newServiceError(ServiceErrorQuorumUnavailable, true)
	}
	if !validSessionIdentity(tenantID, clientID) || ownerNodeID == 0 || epoch == 0 || action < uint32(SubscriptionActionAdd) || action > uint32(SubscriptionActionRemove) {
		return newServiceError(ServiceErrorInvalidRequest, false)
	}
	operationID, err := newOperationID()
	if err != nil {
		return newServiceError(ServiceErrorQuorumUnavailable, true)
	}
	result, err := service.port.Commit(ctx, CommandDTO{
		SchemaMajor: SchemaMajor, SchemaMinor: SchemaMinor, OperationID: operationID,
		Subscription: &SubscriptionChangeDTO{
			TenantID: tenantID, ClientID: clientID, OwnerNodeID: ownerNodeID, Epoch: epoch,
			Filter: filter, QoS: qos, NoLocal: noLocal, RetainAsPublished: retainAsPublished,
			RetainHandling: retainHandling, Action: SubscriptionAction(action),
		},
	})
	if err != nil {
		return portFailure(err)
	}
	if result.Outcome == ApplyOutcomeConflict {
		return conflictServiceError(result.Conflict)
	}
	if result.Outcome != ApplyOutcomeApplied || result.Index == 0 {
		return newServiceError(ServiceErrorQuorumUnavailable, true)
	}
	return nil
}
