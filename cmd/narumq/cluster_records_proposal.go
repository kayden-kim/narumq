package main

import (
	"crypto/sha256"
	"errors"
	"slices"

	"narumq/internal/adapters/raft/records"
	"narumq/internal/application/membership"
)

func proposalIdentityToRecord(value *membership.ProposalIdentityDTO) *records.ProposalIdentity {
	if value == nil {
		return nil
	}
	return &records.ProposalIdentity{OriginNodeId: value.OriginNodeID, ProofIndex: value.ProofIndex,
		RetiredThrough: value.RetiredThrough, RequestId: value.RequestID, CommandDigest: slices.Clone(value.CommandDigest)}
}

func computeProposalDigest(wire *records.CommandEnvelope) ([]byte, error) {
	clone := &records.CommandEnvelope{
		SchemaMajor: wire.SchemaMajor,
		SchemaMinor: wire.SchemaMinor,
		OperationId: wire.OperationId,
		Payload:     wire.Payload,
	}
	encoded, err := records.MarshalCommand(clone)
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(encoded)
	return digest[:], nil
}

func proposalIdentityFromRecord(value *records.ProposalIdentity) *membership.ProposalIdentityDTO {
	if value == nil {
		return nil
	}
	return &membership.ProposalIdentityDTO{OriginNodeID: value.OriginNodeId, ProofIndex: value.ProofIndex,
		RetiredThrough: value.RetiredThrough, RequestID: value.RequestId, CommandDigest: value.CommandDigest}
}

func proposalOriginsToRecords(values []membership.ProposalOriginStateDTO) ([]*records.ProposalOriginState, error) {
	var origins []*records.ProposalOriginState
	for _, value := range values {
		origin := &records.ProposalOriginState{OriginNodeId: value.OriginNodeID, RetiredThrough: value.RetiredThrough, RejectedThrough: value.RejectedThrough, RejectedIndex: value.RejectedIndex}
		for _, receipt := range value.Receipts {
			result, err := proposalResultToRecord(receipt.Result)
			if err != nil {
				return nil, err
			}
			origin.Receipts = append(origin.Receipts, &records.ProposalReceipt{RequestId: receipt.RequestID,
				ProofIndex: receipt.ProofIndex, CommandDigest: receipt.CommandDigest, Result: result})
		}
		origins = append(origins, origin)
	}
	return origins, nil
}

func proposalOriginsFromRecords(values []*records.ProposalOriginState) ([]membership.ProposalOriginStateDTO, error) {
	var origins []membership.ProposalOriginStateDTO
	for _, value := range values {
		origin := membership.ProposalOriginStateDTO{OriginNodeID: value.OriginNodeId, RetiredThrough: value.RetiredThrough,
			RejectedThrough: value.RejectedThrough, RejectedIndex: value.RejectedIndex,
			Receipts: make([]membership.ProposalReceiptDTO, 0, len(value.Receipts))}
		for _, receipt := range value.Receipts {
			result, err := proposalResultFromRecord(receipt.Result)
			if err != nil {
				return nil, err
			}
			origin.Receipts = append(origin.Receipts, membership.ProposalReceiptDTO{RequestID: receipt.RequestId,
				ProofIndex: receipt.ProofIndex, CommandDigest: receipt.CommandDigest, Result: result})
		}
		origins = append(origins, origin)
	}
	return origins, nil
}

func proposalSnapshotObjectCount(values []membership.ProposalOriginStateDTO) int {
	count := len(values)
	for _, origin := range values {
		count += len(origin.Receipts)
	}
	return count
}

func proposalResultToRecord(result membership.ApplyResultDTO) (*records.ProposalResult, error) {
	if !membership.ValidProposalResult(result) {
		return nil, errors.New("invalid proposal receipt result")
	}
	wire := &records.ProposalResult{Index: result.Index, Outcome: uint32(result.Outcome), Conflict: string(result.Conflict)}
	if value := result.Operation; value != nil {
		member, err := memberIdentityToRecord(value.Record.Member)
		if err != nil {
			return nil, err
		}
		action, err := membershipActionToRecord(value.Record.Action)
		if err != nil {
			return nil, err
		}
		wire.Operation = &records.MembershipRecord{OperationId: value.Record.OperationID, Action: action,
			IdempotencyKey: value.Record.IdempotencyKey, Member: member, TargetMemberId: value.Record.TargetMemberID,
			ExpectedConfStateIndex: value.Record.ExpectedConfStateIndex, CommittedIndex: value.Record.CommittedIndex}
		wire.OperationReplayed = value.Replayed
		wire.OperationMemberId, wire.OperationPhase = value.MemberID, string(value.Phase)
	}
	if value := result.Ownership; value != nil {
		wire.Ownership = &records.OwnershipState{TenantId: value.TenantID, ClientId: value.ClientID, OwnerNodeId: value.OwnerNodeID, Epoch: value.Epoch, CommittedIndex: value.CommittedIndex}
	}
	if value := result.Subscription; value != nil {
		wire.Subscription = &records.SubscriptionState{TenantId: value.TenantID, ClientId: value.ClientID, OwnerNodeId: value.OwnerNodeID, Epoch: value.Epoch, Filter: value.Filter, Qos: value.QoS, NoLocal: value.NoLocal, RetainAsPublished: value.RetainAsPublished, RetainHandling: value.RetainHandling}
	}
	if value := result.Delivery; value != nil {
		delivery := &records.ProposalDeliveryResult{Found: value.Found, Released: value.Released,
			Saturated: slices.Clone(value.Saturated), Skipped: slices.Clone(value.Skipped)}
		for _, entry := range value.Enqueued {
			delivery.Enqueued = append(delivery.Enqueued, &records.ProposalDeliveryEnqueued{ClientId: entry.ClientID, Sequence: entry.Sequence})
		}
		if value.Session != nil {
			state, err := deliverySessionToRecord(*value.Session)
			if err != nil {
				return nil, err
			}
			delivery.Session = state
		}
		wire.Delivery = delivery
	}
	if value := result.OfflineClaim; value != nil {
		claim := &records.ProposalOfflineClaim{Found: value.Found}
		for _, msg := range value.Messages {
			claim.Messages = append(claim.Messages, &records.OfflineMessage{Sequence: msg.Sequence, Topic: msg.Topic, Payload: msg.Payload, Qos: msg.QoS, Retain: msg.Retain, ExpiresAtUnixMillis: msg.ExpiresAtUnixMillis})
		}
		wire.OfflineClaim = claim
	}
	if value := result.NodeDrain; value != nil {
		phase, err := drainPhaseToRecord(value.Phase)
		if err != nil {
			return nil, err
		}
		wire.NodeDrain = &records.NodeDrainState{DrainOperationId: value.DrainOperationID, MemberId: value.MemberID, IdempotencyKey: value.IdempotencyKey,
			Phase: phase, ServerReference: value.ServerReference, ConnectionRatePerSecond: value.ConnectionRatePerSecond,
			LoadBalancerPropagationDelayMillis: value.LoadBalancerPropagationDelayMillis, TimeoutMillis: value.TimeoutMillis,
			StartedAtUnixMillis: value.StartedAtUnixMillis, DeadlineAtUnixMillis: value.DeadlineAtUnixMillis, PhaseStartedAtUnixMillis: value.PhaseStartedAtUnixMillis,
			Progress: nodeDrainProgressToRecord(value.Progress), FailureCode: value.FailureCode, BlockingReason: value.BlockingReason, CommittedIndex: value.CommittedIndex}
	}
	if value := result.TenantQuota; value != nil {
		wire.TenantQuota = &records.ProposalTenantQuotaRejection{TenantId: value.TenantID, Dimension: string(value.Dimension), ProfileVersion: value.ProfileVersion}
	}
	if value := result.SessionMetadata; value != nil {
		wire.SessionMetadata = &records.ProposalSessionMetadataResult{Found: value.Found, Will: sessionWillToRecord(value.Will)}
	}
	if value := result.ConnectionRebalance; value != nil {
		state, err := rebalanceStateToRecord(*value)
		if err != nil {
			return nil, err
		}
		wire.ConnectionRebalance = state
	}
	if value := result.SupersededWill; value != nil {
		retention, err := sessionRetentionToRecord(value.Retention)
		if err != nil {
			return nil, err
		}
		wire.SupersededWill = &records.ProposalSupersededWill{Will: sessionWillToRecord(&value.Will), Principal: sessionPrincipalToRecord(value.Principal), ProtocolVersion: value.ProtocolVersion, Retention: retention, PlannedHandoff: value.PlannedHandoff}
	}
	return wire, nil
}

func proposalResultFromRecord(wire *records.ProposalResult) (membership.ApplyResultDTO, error) {
	if wire == nil {
		return membership.ApplyResultDTO{}, errors.New("missing proposal receipt result")
	}
	result := membership.ApplyResultDTO{Index: wire.Index, Outcome: membership.ApplyOutcome(wire.Outcome), Conflict: membership.ApplyConflictCode(wire.Conflict)}
	if value := wire.Operation; value != nil {
		member, err := memberIdentityFromRecord(value.Member)
		if err != nil {
			return membership.ApplyResultDTO{}, err
		}
		action, err := membershipActionFromRecord(value.Action)
		if err != nil {
			return membership.ApplyResultDTO{}, err
		}
		record := membership.MembershipRecordDTO{OperationID: value.OperationId, Action: action, IdempotencyKey: value.IdempotencyKey,
			Member: member, TargetMemberID: value.TargetMemberId, ExpectedConfStateIndex: value.ExpectedConfStateIndex, CommittedIndex: value.CommittedIndex}
		operation := membership.MembershipOperationDTO{OperationID: record.OperationID, Action: action, CommittedIndex: record.CommittedIndex, Record: record, Replayed: wire.OperationReplayed,
			MemberID: wire.OperationMemberId, Phase: membership.MembershipOperationPhase(wire.OperationPhase)}
		result.Operation = &operation
	}
	if value := wire.Ownership; value != nil {
		result.Ownership = &membership.OwnershipStateDTO{TenantID: value.TenantId, ClientID: value.ClientId, OwnerNodeID: value.OwnerNodeId, Epoch: value.Epoch, CommittedIndex: value.CommittedIndex}
	}
	if value := wire.Subscription; value != nil {
		result.Subscription = &membership.SubscriptionStateDTO{TenantID: value.TenantId, ClientID: value.ClientId, OwnerNodeID: value.OwnerNodeId, Epoch: value.Epoch, Filter: value.Filter, QoS: value.Qos, NoLocal: value.NoLocal, RetainAsPublished: value.RetainAsPublished, RetainHandling: value.RetainHandling}
	}
	if value := wire.Delivery; value != nil {
		delivery := &membership.DeliveryResultDTO{Found: value.Found, Released: value.Released, Saturated: slices.Clone(value.Saturated), Skipped: slices.Clone(value.Skipped)}
		for _, entry := range value.Enqueued {
			delivery.Enqueued = append(delivery.Enqueued, membership.DeliveryEnqueuedDTO{ClientID: entry.ClientId, Sequence: entry.Sequence})
		}
		if value.Session != nil {
			state, err := deliverySessionFromRecord(value.Session)
			if err != nil {
				return membership.ApplyResultDTO{}, err
			}
			delivery.Session = &state
		}
		result.Delivery = delivery
	}
	if value := wire.OfflineClaim; value != nil {
		claim := &membership.OfflineClaimDTO{Found: value.Found}
		for _, msg := range value.Messages {
			claim.Messages = append(claim.Messages, membership.OfflineMessageDTO{Sequence: msg.Sequence, Topic: msg.Topic, Payload: msg.Payload, QoS: msg.Qos, Retain: msg.Retain, ExpiresAtUnixMillis: msg.ExpiresAtUnixMillis})
		}
		result.OfflineClaim = claim
	}
	if value := wire.NodeDrain; value != nil {
		phase, err := drainPhaseFromRecord(value.Phase, false)
		if err != nil {
			return membership.ApplyResultDTO{}, err
		}
		result.NodeDrain = &membership.NodeDrainStateDTO{DrainOperationID: value.DrainOperationId, MemberID: value.MemberId, IdempotencyKey: value.IdempotencyKey,
			Phase: phase, ServerReference: value.ServerReference, ConnectionRatePerSecond: value.ConnectionRatePerSecond,
			LoadBalancerPropagationDelayMillis: value.LoadBalancerPropagationDelayMillis, TimeoutMillis: value.TimeoutMillis,
			StartedAtUnixMillis: value.StartedAtUnixMillis, DeadlineAtUnixMillis: value.DeadlineAtUnixMillis, PhaseStartedAtUnixMillis: value.PhaseStartedAtUnixMillis,
			Progress: nodeDrainProgressFromRecord(value.Progress), FailureCode: value.FailureCode, BlockingReason: value.BlockingReason, CommittedIndex: value.CommittedIndex}
	}
	if value := wire.TenantQuota; value != nil {
		result.TenantQuota = &membership.TenantQuotaRejectionDTO{TenantID: value.TenantId, Dimension: membership.TenantQuotaDimension(value.Dimension), ProfileVersion: value.ProfileVersion}
	}
	if value := wire.SessionMetadata; value != nil {
		result.SessionMetadata = &membership.SessionMetadataResultDTO{Found: value.Found, Will: sessionWillFromRecord(value.Will)}
	}
	if value := wire.ConnectionRebalance; value != nil {
		state, err := rebalanceStateFromRecord(value)
		if err != nil {
			return membership.ApplyResultDTO{}, err
		}
		result.ConnectionRebalance = &state
	}
	if value := wire.SupersededWill; value != nil {
		retention, err := sessionRetentionFromRecord(value.Retention)
		if err != nil || value.Will == nil {
			return membership.ApplyResultDTO{}, errors.New("invalid proposal superseded Will")
		}
		result.SupersededWill = &membership.SupersededWillDTO{Will: *sessionWillFromRecord(value.Will), Principal: sessionPrincipalFromRecord(value.Principal), ProtocolVersion: value.ProtocolVersion, Retention: retention, PlannedHandoff: value.PlannedHandoff}
	}
	if !membership.ValidProposalResult(result) {
		return membership.ApplyResultDTO{}, errors.New("invalid proposal receipt result")
	}
	return result, nil
}
