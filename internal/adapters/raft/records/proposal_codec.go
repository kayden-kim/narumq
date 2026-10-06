package records

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"slices"
	"sort"

	"google.golang.org/protobuf/proto"
)

func validateProposalIdentity(command *CommandEnvelope) error {
	identity := command.Proposal
	if identity == nil {
		return nil
	}
	if identity.OriginNodeId == 0 || identity.ProofIndex == 0 || identity.RetiredThrough >= identity.ProofIndex || !validUUID(identity.RequestId) || len(identity.CommandDigest) != sha256.Size {
		return errors.New("invalid proposal identity")
	}
	// Shallow-copy the envelope; only omit its metadata. Payload is immutable here.
	original := &CommandEnvelope{SchemaMajor: command.SchemaMajor, SchemaMinor: command.SchemaMinor, OperationId: command.OperationId, Payload: command.Payload}
	encoded, err := deterministic.Marshal(original)
	if err != nil {
		return err
	}
	digest := sha256.Sum256(encoded)
	if !bytes.Equal(digest[:], identity.CommandDigest) {
		return errors.New("proposal command digest mismatch")
	}
	return nil
}

func proposalOriginsObjectCount(origins []*ProposalOriginState) int {
	count := len(origins)
	for _, origin := range origins {
		if origin != nil {
			count += len(origin.Receipts)
		}
	}
	return count
}

func sortProposalOrigins(origins []*ProposalOriginState) {
	sort.Slice(origins, func(i, j int) bool { return origins[i].GetOriginNodeId() < origins[j].GetOriginNodeId() })
	for _, origin := range origins {
		if origin != nil {
			sort.Slice(origin.Receipts, func(i, j int) bool { return origin.Receipts[i].GetRequestId() < origin.Receipts[j].GetRequestId() })
		}
	}
}

func validateProposalOrigins(state *ReplicatedStateSnapshot, lastIncludedIndex uint64) error {
	if len(state.ProposalOrigins) != 0 && !slices.Contains(state.ActiveFeatures, "proposal-recovery") {
		return errors.New("proposal origins require active proposal-recovery")
	}
	count, size := 0, 0
	for i, origin := range state.ProposalOrigins {
		if origin == nil || origin.OriginNodeId == 0 || origin.RetiredThrough > lastIncludedIndex || len(origin.Receipts) > 1024 || (i > 0 && state.ProposalOrigins[i-1].OriginNodeId >= origin.OriginNodeId) {
			return errors.New("invalid proposal origin")
		}
		if (origin.RejectedThrough == 0) != (origin.RejectedIndex == 0) || (origin.RejectedIndex != 0 && origin.RejectedThrough >= origin.RejectedIndex) || origin.RejectedIndex > lastIncludedIndex {
			return errors.New("invalid proposal capacity fence")
		}
		memberFound := false
		for _, member := range state.Members {
			if member.MemberId == origin.OriginNodeId {
				memberFound = true
				break
			}
		}
		if !memberFound {
			return errors.New("proposal origin is not a committed member")
		}
		for j, receipt := range origin.Receipts {
			if receipt == nil || !validUUID(receipt.RequestId) || len(receipt.CommandDigest) != sha256.Size || receipt.ProofIndex <= origin.RetiredThrough || receipt.Result == nil || receipt.ProofIndex >= receipt.Result.Index || receipt.Result.Index > lastIncludedIndex || (j > 0 && origin.Receipts[j-1].RequestId >= receipt.RequestId) {
				return errors.New("invalid proposal receipt")
			}
			if err := validateProposalResult(receipt.Result); err != nil {
				return err
			}
			count++
			size += proto.Size(receipt.Result)
			if count > 4096 || size > 32<<20 {
				return errors.New("proposal receipts exceed snapshot capacity")
			}
		}
	}
	return nil
}

func validateProposalResult(result *ProposalResult) error {
	if result == nil || result.Index == 0 || (result.Outcome != 1 && result.Outcome != 2) || (result.Outcome == 1 && result.Conflict != "") || (result.Outcome == 2 && !validProposalConflict(result.Conflict)) {
		return errors.New("invalid proposal result outcome")
	}
	if result.Operation != nil {
		if err := validateMembership(result.Operation); err != nil {
			return err
		}
		if result.Operation.CommittedIndex == 0 || result.Operation.CommittedIndex > result.Index {
			return errors.New("invalid proposal membership result index")
		}
		if result.OperationMemberId == 0 || !slices.Contains([]string{"STAGED", "LEARNER", "CATCHING_UP", "PROMOTING", "TRANSFERRING", "REMOVING", "SUCCEEDED", "ABORTED"}, result.OperationPhase) {
			return errors.New("invalid proposal operation view")
		}
	} else if result.OperationReplayed || result.OperationMemberId != 0 || result.OperationPhase != "" {
		return errors.New("operation replay flag requires operation")
	}
	if value := result.Ownership; value != nil && (!boundedUTF8(value.TenantId, 1, 128) || !boundedUTF8(value.ClientId, 1, 256) || value.Epoch == 0 || value.CommittedIndex == 0 || value.CommittedIndex > result.Index) {
		return errors.New("invalid proposal ownership result")
	}
	if value := result.Subscription; value != nil && (!boundedUTF8(value.TenantId, 1, 128) || !boundedUTF8(value.ClientId, 1, 256) || value.OwnerNodeId == 0 || value.Epoch == 0 || !validFilter(value.Filter) || value.Qos > 2 || value.RetainHandling > 2) {
		return errors.New("invalid proposal subscription result")
	}
	if value := result.Delivery; value != nil {
		if len(value.Enqueued)+len(value.Saturated)+len(value.Skipped) > maxDeliveryTargets {
			return errors.New("proposal delivery result exceeds targets")
		}
		for _, entry := range value.Enqueued {
			if entry == nil || !boundedUTF8(entry.ClientId, 1, 256) || entry.Sequence == 0 {
				return errors.New("invalid proposal enqueued result")
			}
		}
		for _, client := range value.Saturated {
			if !boundedUTF8(client, 1, 256) {
				return errors.New("invalid proposal saturated client")
			}
		}
		for _, client := range value.Skipped {
			if !boundedUTF8(client, 1, 256) {
				return errors.New("invalid proposal skipped client")
			}
		}
		if value.Session != nil {
			if err := validateDeliverySessionState(value.Session); err != nil {
				return err
			}
			for _, inbound := range value.Session.Inbound {
				if inbound.ReleasedIndex > result.Index {
					return errors.New("proposal inbound index exceeds result")
				}
			}
		}
	}
	if value := result.OfflineClaim; value != nil {
		if len(value.Messages) > maxOfflineMessagesPerSession || (!value.Found && len(value.Messages) != 0) {
			return errors.New("invalid proposal offline claim")
		}
		var previous uint64
		for _, msg := range value.Messages {
			if msg == nil || msg.Sequence <= previous || !validTopic(msg.Topic) || len(msg.Payload) > maxPublishPayload || msg.Qos != 0 {
				return errors.New("invalid proposal offline message")
			}
			previous = msg.Sequence
		}
	}
	if value := result.NodeDrain; value != nil {
		if err := validateNodeDrainState(value); err != nil {
			return err
		}
		if value.CommittedIndex > result.Index {
			return errors.New("invalid proposal drain index")
		}
	}
	if value := result.TenantQuota; value != nil && (!boundedUTF8(value.TenantId, 1, 128) || value.ProfileVersion == 0 || (value.Dimension != "retained_bytes" && value.Dimension != "offline_queue_messages" && value.Dimension != "offline_queue_bytes" && value.Dimension != "storage_bytes")) {
		return errors.New("invalid proposal quota result")
	}
	if value := result.SessionMetadata; value != nil && value.Will != nil {
		if err := validateSessionWill(value.Will); err != nil {
			return err
		}
	}
	if value := result.ConnectionRebalance; value != nil {
		if err := validateConnectionRebalanceState(value); err != nil {
			return err
		}
		if value.CommittedIndex > result.Index {
			return errors.New("invalid proposal rebalance index")
		}
	}
	if value := result.SupersededWill; value != nil {
		if value.Will == nil {
			return errors.New("proposal superseded Will is missing")
		}
		if err := validateSessionWill(value.Will); err != nil {
			return err
		}
		if err := validateSessionPrincipal(value.Principal, true); err != nil {
			return err
		}
		if value.ProtocolVersion != 4 && value.ProtocolVersion != 5 {
			return errors.New("invalid proposal Will protocol")
		}
		if value.Retention != SessionRetention_SESSION_RETENTION_IMMEDIATE && value.Retention != SessionRetention_SESSION_RETENTION_FINITE && value.Retention != SessionRetention_SESSION_RETENTION_INFINITE {
			return errors.New("invalid proposal Will retention")
		}
	}
	return nil
}

func validProposalConflict(code string) bool {
	switch code {
	case "member_id_reserved", "member_identity_conflict", "idempotency_conflict", "operation_id_conflict", "membership_change_in_progress", "operation_not_found", "phase_conflict", "configuration_mismatch", "stale_epoch", "ownership_missing", "member_not_found", "compatibility_unsupported", "active_feature_missing", "feature_not_supported_by_all_members", "retained_capacity_exceeded", "offline_capacity_exceeded", "offline_session_missing", "delivery_capacity_exceeded", "delivery_missing", "delivery_packet_id_conflict", "node_drain_active", "node_drain_in_progress", "drain_not_cancellable", "delivery_session_missing", "tenant_quota_stale", "feature_inactive", "session_metadata_capacity_exceeded", "session_metadata_invalid", "connection_rebalance_in_progress", "rebalance_not_cancellable", "proposal_retired", "proposal_identity_conflict", "proposal_receipt_capacity_exceeded":
		return true
	default:
		return false
	}
}
