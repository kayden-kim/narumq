package membership

import (
	"bytes"
	"slices"
	"sort"
)

type proposalOriginState struct {
	retiredThrough  uint64
	rejectedThrough uint64
	rejectedIndex   uint64
	receipts        map[string]ProposalReceiptDTO
}

// ProposalReceipt returns an owned first result, including results installed by a snapshot.
func (machine *StateMachine) ProposalReceipt(origin uint64, requestID string) (ProposalReceiptDTO, bool) {
	machine.mu.RLock()
	defer machine.mu.RUnlock()
	state := machine.proposalOrigins[origin]
	if state == nil {
		return ProposalReceiptDTO{}, false
	}
	receipt, found := state.receipts[requestID]
	if !found {
		return ProposalReceiptDTO{}, false
	}
	return cloneProposalReceipt(receipt), true
}

// ProposalAdmission reports counts after retiring this origin's floor.
func (machine *StateMachine) ProposalAdmission(origin, floor uint64) (count, total int, retired uint64) {
	machine.mu.RLock()
	defer machine.mu.RUnlock()
	total = machine.proposalReceiptCount
	if state := machine.proposalOrigins[origin]; state != nil {
		retired = state.retiredThrough
		for _, receipt := range state.receipts {
			if receipt.ProofIndex > floor {
				count++
			} else {
				total--
			}
		}
	}
	return count, total, retired
}

func (machine *StateMachine) applyProposal(index uint64, command CommandDTO) (ApplyResultDTO, error) {
	identity := command.Proposal
	if !slices.Contains(machine.activeFeatures, ProposalRecoveryFeature) {
		return conflictResult(index, ConflictFeatureInactive), nil
	}
	member, found := machine.members[identity.OriginNodeID]
	if !found || (member.Phase != MemberPhaseVoter && member.Phase != MemberPhaseLeaving) {
		return conflictResult(index, ConflictProposalRetired), nil
	}
	state := machine.proposalOrigins[identity.OriginNodeID]
	if state == nil {
		if machine.proposalOrigins == nil {
			machine.proposalOrigins = make(map[uint64]*proposalOriginState)
		}
		state = &proposalOriginState{receipts: make(map[string]ProposalReceiptDTO)}
		machine.proposalOrigins[identity.OriginNodeID] = state
	}
	if receipt, exists := state.receipts[identity.RequestID]; exists {
		if receipt.ProofIndex != identity.ProofIndex || !bytes.Equal(receipt.CommandDigest, identity.CommandDigest) {
			return conflictResult(index, ConflictProposalIdentity), nil
		}
		result := cloneProposalResult(receipt.Result)
		result.Replayed = true
		return result, nil
	}
	machine.retireProposalReceiptsLocked(state, identity.RetiredThrough)
	if identity.ProofIndex <= state.retiredThrough {
		return conflictResult(index, ConflictProposalRetired), nil
	}
	if identity.ProofIndex <= state.rejectedThrough {
		return conflictResult(state.rejectedIndex, ConflictProposalCapacity), nil
	}
	if len(state.receipts) >= MaxProposalReceiptsPerOrigin || machine.proposalReceiptCount >= MaxProposalReceipts {
		// Count exhaustion records a bounded proof fence instead of an unbounded
		// rejection receipt. Already remembered results remain replayable.
		state.rejectedThrough, state.rejectedIndex = identity.ProofIndex, index
		return conflictResult(index, ConflictProposalCapacity), nil
	}
	var result ApplyResultDTO
	var err error
	reservation := machine.proposalResultReservationLocked(command)
	// Leave a fixed 512-byte result slot for every unused receipt. A capacity
	// rejection can always be remembered before the variable payload budget fills.
	available := MaxProposalResultBytes - machine.proposalResultBytes - (MaxProposalReceipts-machine.proposalReceiptCount-1)*512
	if reservation > available {
		result = conflictResult(index, ConflictProposalCapacity)
	} else {
		result, err = machine.applyCommandLocked(index, command)
		if err != nil {
			return ApplyResultDTO{}, err
		}
	}
	actualSize := ProposalResultStorageBytes(result)
	if actualSize > available || actualSize > reservation {
		return ApplyResultDTO{}, ErrInvalidCommand
	}
	receipt := ProposalReceiptDTO{RequestID: identity.RequestID, ProofIndex: identity.ProofIndex,
		CommandDigest: bytes.Clone(identity.CommandDigest), Result: cloneProposalResult(result)}
	state.receipts[identity.RequestID] = receipt
	machine.proposalReceiptCount++
	machine.proposalResultBytes += actualSize
	return result, nil
}

func (machine *StateMachine) retireProposalReceiptsLocked(state *proposalOriginState, through uint64) {
	if through <= state.retiredThrough {
		return
	}
	state.retiredThrough = through
	for key, receipt := range state.receipts {
		if receipt.ProofIndex <= through {
			machine.proposalResultBytes -= ProposalResultStorageBytes(receipt.Result)
			machine.proposalReceiptCount--
			delete(state.receipts, key)
		}
	}
}

func (machine *StateMachine) proposalSnapshotLocked() []ProposalOriginStateDTO {
	if len(machine.proposalOrigins) == 0 {
		return nil
	}
	origins := make([]ProposalOriginStateDTO, 0, len(machine.proposalOrigins))
	for nodeID, state := range machine.proposalOrigins {
		origin := ProposalOriginStateDTO{OriginNodeID: nodeID, RetiredThrough: state.retiredThrough,
			RejectedThrough: state.rejectedThrough, RejectedIndex: state.rejectedIndex,
			Receipts: make([]ProposalReceiptDTO, 0, len(state.receipts))}
		for _, receipt := range state.receipts {
			origin.Receipts = append(origin.Receipts, cloneProposalReceipt(receipt))
		}
		sort.Slice(origin.Receipts, func(i, j int) bool { return origin.Receipts[i].RequestID < origin.Receipts[j].RequestID })
		origins = append(origins, origin)
	}
	sort.Slice(origins, func(i, j int) bool { return origins[i].OriginNodeID < origins[j].OriginNodeID })
	return origins
}

func proposalObjectCount(origins []ProposalOriginStateDTO) int {
	count := len(origins)
	for _, origin := range origins {
		count += len(origin.Receipts)
	}
	return count
}

func restoreProposalOrigins(origins []ProposalOriginStateDTO) (map[uint64]*proposalOriginState, int, int) {
	states := make(map[uint64]*proposalOriginState, len(origins))
	count, size := 0, 0
	for _, origin := range origins {
		state := &proposalOriginState{retiredThrough: origin.RetiredThrough, rejectedThrough: origin.RejectedThrough, rejectedIndex: origin.RejectedIndex, receipts: make(map[string]ProposalReceiptDTO, len(origin.Receipts))}
		for _, receipt := range origin.Receipts {
			state.receipts[receipt.RequestID] = receipt
			count++
			size += ProposalResultStorageBytes(receipt.Result)
		}
		states[origin.OriginNodeID] = state
	}
	return states, count, size
}

func cloneProposalReceipt(receipt ProposalReceiptDTO) ProposalReceiptDTO {
	receipt.CommandDigest = bytes.Clone(receipt.CommandDigest)
	receipt.Result = cloneProposalResult(receipt.Result)
	return receipt
}

func cloneProposalOrigins(origins []ProposalOriginStateDTO) []ProposalOriginStateDTO {
	if origins == nil {
		return nil
	}
	cloned := make([]ProposalOriginStateDTO, len(origins))
	for i, origin := range origins {
		cloned[i] = origin
		cloned[i].Receipts = make([]ProposalReceiptDTO, len(origin.Receipts))
		for j, receipt := range origin.Receipts {
			cloned[i].Receipts[j] = cloneProposalReceipt(receipt)
		}
	}
	return cloned
}

func cloneProposalResult(result ApplyResultDTO) ApplyResultDTO {
	if result.Operation != nil {
		result.Operation = new(cloneOperation(*result.Operation))
	}
	if result.Ownership != nil {
		result.Ownership = new(*result.Ownership)
	}
	if result.Subscription != nil {
		result.Subscription = new(*result.Subscription)
	}
	if result.Delivery != nil {
		delivery := *result.Delivery
		delivery.Enqueued = slices.Clone(delivery.Enqueued)
		delivery.Saturated = slices.Clone(delivery.Saturated)
		delivery.Skipped = slices.Clone(delivery.Skipped)
		if delivery.Session != nil {
			delivery.Session = new(cloneDeliverySession(*delivery.Session))
		}
		result.Delivery = &delivery
	}
	if result.OfflineClaim != nil {
		claim := *result.OfflineClaim
		claim.Messages = slices.Clone(claim.Messages)
		for i := range claim.Messages {
			claim.Messages[i].Payload = bytes.Clone(claim.Messages[i].Payload)
		}
		result.OfflineClaim = &claim
	}
	if result.NodeDrain != nil {
		result.NodeDrain = new(*result.NodeDrain)
	}
	if result.TenantQuota != nil {
		result.TenantQuota = new(*result.TenantQuota)
	}
	if result.SessionMetadata != nil {
		metadata := *result.SessionMetadata
		metadata.Will = cloneSessionWillDTO(metadata.Will)
		result.SessionMetadata = &metadata
	}
	result.ConnectionRebalance = cloneConnectionRebalanceStatePointer(result.ConnectionRebalance)
	if result.SupersededWill != nil {
		will := *result.SupersededWill
		will.Will.Payload = bytes.Clone(will.Will.Payload)
		will.Principal = cloneSessionPrincipalDTO(will.Principal)
		result.SupersededWill = &will
	}
	return result
}

// ProposalResultStorageBytes defines deterministic logical storage accounting.
// Fixed overhead is charged independently of the host's pointer size or protobuf encoding.
func ProposalResultStorageBytes(result ApplyResultDTO) int {
	size := 256 + len(result.Conflict)
	if result.Operation != nil {
		r := result.Operation.Record
		size += 256 + len(r.OperationID) + len(r.IdempotencyKey)
		if r.Member != nil {
			size += proposalMemberBytes(*r.Member)
		}
	}
	if result.Ownership != nil {
		size += 64 + len(result.Ownership.TenantID) + len(result.Ownership.ClientID)
	}
	if result.Subscription != nil {
		size += 64 + len(result.Subscription.TenantID) + len(result.Subscription.ClientID) + len(result.Subscription.Filter)
	}
	if result.Delivery != nil {
		size += 64
		for _, entry := range result.Delivery.Enqueued {
			size += 32 + len(entry.ClientID)
		}
		for _, client := range result.Delivery.Saturated {
			size += 16 + len(client)
		}
		for _, client := range result.Delivery.Skipped {
			size += 16 + len(client)
		}
		if result.Delivery.Session != nil {
			size += proposalDeliverySessionBytes(*result.Delivery.Session)
		}
	}
	if result.OfflineClaim != nil {
		size += 64
		for _, message := range result.OfflineClaim.Messages {
			size += 64 + len(message.Topic) + len(message.Payload)
		}
	}
	if result.NodeDrain != nil {
		size += 256 + len(result.NodeDrain.DrainOperationID) + len(result.NodeDrain.IdempotencyKey) + len(result.NodeDrain.ServerReference) + len(result.NodeDrain.FailureCode) + len(result.NodeDrain.BlockingReason)
	}
	if result.TenantQuota != nil {
		size += 64 + len(result.TenantQuota.TenantID) + len(result.TenantQuota.Dimension)
	}
	if result.SessionMetadata != nil {
		size += 64 + proposalWillBytes(result.SessionMetadata.Will, nil)
	}
	if result.ConnectionRebalance != nil {
		size += 256 + len(result.ConnectionRebalance.RebalanceOperationID) + len(result.ConnectionRebalance.IdempotencyKey) + len(result.ConnectionRebalance.ServerReference) + len(result.ConnectionRebalance.FailureCode) + 64*len(result.ConnectionRebalance.Participants)
	}
	if result.SupersededWill != nil {
		size += 64 + proposalWillBytes(&result.SupersededWill.Will, result.SupersededWill.Principal)
	}
	return size
}

func proposalMemberBytes(member MemberIdentityDTO) int {
	size := 128 + len(member.ClusterID) + len(member.Address) + len(member.DNSSAN) + len(member.CertificateSHA256) + len(member.ReleaseVersion) + len(member.OperationID)
	for _, feature := range member.Features {
		size += 16 + len(feature)
	}
	return size
}

func proposalDeliverySessionBytes(state DeliverySessionStateDTO) int {
	size := 64 + len(state.TenantID) + len(state.ClientID)
	for _, message := range state.Messages {
		size += 64 + len(message.Topic) + len(message.Payload)
	}
	for _, inbound := range state.Inbound {
		size += 64 + len(inbound.Topic) + len(inbound.Payload)
	}
	return size
}

func proposalWillBytes(will *SessionWillDTO, principal *SessionPrincipalDTO) int {
	size := 0
	if will != nil {
		size += 64 + len(will.Topic) + len(will.Payload)
	}
	if principal != nil {
		size += 64 + len(principal.Subject) + len(principal.CredentialID)
		for _, role := range principal.Roles {
			size += 16 + len(role)
		}
	}
	return size
}

// Reserve before mutation. Each branch bounds the fields its apply method may return.
func (machine *StateMachine) proposalResultReservationLocked(command CommandDTO) int {
	size := 1024
	switch {
	case command.Membership != nil:
		size += len(command.Membership.IdempotencyKey)
		if command.Membership.Member != nil {
			size += proposalMemberBytes(*command.Membership.Member)
		}
		if operation, found := machine.operations[command.OperationID]; found && operation.view.Record.Member != nil {
			size += proposalMemberBytes(*operation.view.Record.Member) + len(operation.view.Record.IdempotencyKey)
		}
	case command.Ownership != nil:
		state := machine.sessionMetadata[sessionKey{tenantID: command.Ownership.TenantID, clientID: command.Ownership.ClientID}]
		size += proposalWillBytes(state.Will, state.Principal) + len(command.Ownership.TenantID) + len(command.Ownership.ClientID)
	case command.Subscription != nil:
		size += len(command.Subscription.TenantID) + len(command.Subscription.ClientID) + len(command.Subscription.Filter)
	case command.Delivery != nil:
		change := command.Delivery
		for _, target := range change.Targets {
			size += 64 + len(target.ClientID)
		}
		if change.Action == DeliveryActionAttach && !change.Reset {
			state := machine.delivery[sessionKey{tenantID: change.TenantID, clientID: change.ClientID}]
			size += proposalDeliverySessionBytes(state)
		}
	case command.Offline != nil:
		if command.Offline.Action == OfflineActionClaim {
			state := machine.offline[sessionKey{tenantID: command.Offline.TenantID, clientID: command.Offline.ClientID}]
			for _, message := range state.Messages {
				size += 64 + len(message.Topic) + len(message.Payload)
			}
		}
	case command.NodeDrain != nil:
		change := command.NodeDrain
		state := machine.nodeDrains[change.MemberID]
		size += len(change.IdempotencyKey) + len(change.ServerReference) + len(change.FailureCode) + len(change.BlockingReason) + len(state.IdempotencyKey) + len(state.ServerReference) + len(state.FailureCode) + len(state.BlockingReason)
	case command.ConnectionRebalance != nil:
		change := command.ConnectionRebalance
		size += len(change.IdempotencyKey) + len(change.ServerReference) + len(change.FailureCode) + 64*len(machine.members)
		if state := machine.connectionRebalance; state != nil {
			size += len(state.IdempotencyKey) + len(state.ServerReference) + len(state.FailureCode) + 64*len(state.Participants)
		}
	case command.SessionMetadata != nil:
		state := machine.sessionMetadata[sessionKey{tenantID: command.SessionMetadata.TenantID, clientID: command.SessionMetadata.ClientID}]
		size += proposalWillBytes(state.Will, nil)
	}
	return size
}

func validProposalIdentity(identity *ProposalIdentityDTO) bool {
	return identity != nil && identity.OriginNodeID > 0 && identity.ProofIndex > 0 && identity.RetiredThrough < identity.ProofIndex && validUUID(identity.RequestID) && len(identity.CommandDigest) == 32
}

func validateProposalSnapshot(snapshot SnapshotDTO) error {
	if len(snapshot.ProposalOrigins) != 0 && !slices.Contains(snapshot.ActiveFeatures, ProposalRecoveryFeature) {
		return ErrInvalidSnapshot
	}
	count, size := 0, 0
	for i, origin := range snapshot.ProposalOrigins {
		if origin.OriginNodeID == 0 || origin.RetiredThrough > snapshot.LastIncludedIndex || len(origin.Receipts) > MaxProposalReceiptsPerOrigin || (i > 0 && snapshot.ProposalOrigins[i-1].OriginNodeID >= origin.OriginNodeID) {
			return ErrInvalidSnapshot
		}
		if (origin.RejectedThrough == 0) != (origin.RejectedIndex == 0) || origin.RejectedThrough >= origin.RejectedIndex && origin.RejectedIndex != 0 || origin.RejectedIndex > snapshot.LastIncludedIndex {
			return ErrInvalidSnapshot
		}
		foundMember := false
		for _, member := range snapshot.Members {
			if member.MemberID == origin.OriginNodeID {
				foundMember = true
				break
			}
		}
		if !foundMember {
			return ErrInvalidSnapshot
		}
		for j, receipt := range origin.Receipts {
			if !validUUID(receipt.RequestID) || receipt.ProofIndex <= origin.RetiredThrough || receipt.ProofIndex >= receipt.Result.Index || receipt.Result.Index > snapshot.LastIncludedIndex || len(receipt.CommandDigest) != 32 || receipt.Result.Replayed || (j > 0 && origin.Receipts[j-1].RequestID >= receipt.RequestID) {
				return ErrInvalidSnapshot
			}
			if !ValidProposalResult(receipt.Result) {
				return ErrInvalidSnapshot
			}
			count++
			size += ProposalResultStorageBytes(receipt.Result)
			if count > MaxProposalReceipts || size > MaxProposalResultBytes {
				return ErrInvalidSnapshot
			}
		}
	}
	if size+(MaxProposalReceipts-count)*512 > MaxProposalResultBytes {
		return ErrInvalidSnapshot
	}
	return nil
}

// ValidProposalResult validates a cached historical result without requiring it to match current state.
func ValidProposalResult(result ApplyResultDTO) bool {
	if result.Index == 0 || result.Replayed || (result.Outcome != ApplyOutcomeApplied && result.Outcome != ApplyOutcomeConflict) || (result.Outcome == ApplyOutcomeApplied && result.Conflict != ConflictNone) || (result.Outcome == ApplyOutcomeConflict && !validProposalConflict(result.Conflict)) {
		return false
	}
	if r := result.Operation; r != nil && (validateMembershipRecord(r.Record) != nil || r.MemberID == 0 || !validProposalOperationPhase(r.Phase) || r.Action != r.Record.Action || r.Record.CommittedIndex == 0 || r.Record.CommittedIndex > result.Index || r.OperationID != r.Record.OperationID || r.CommittedIndex != r.Record.CommittedIndex) {
		return false
	}
	if r := result.Ownership; r != nil && (!validSessionIdentity(r.TenantID, r.ClientID) || r.Epoch == 0 || r.CommittedIndex == 0 || r.CommittedIndex > result.Index) {
		return false
	}
	if r := result.Subscription; r != nil && (!validSessionIdentity(r.TenantID, r.ClientID) || r.OwnerNodeID == 0 || r.Epoch == 0 || !validTopicFilter(r.Filter) || r.QoS > 2 || r.RetainHandling > 2) {
		return false
	}
	if r := result.Delivery; r != nil {
		if len(r.Enqueued)+len(r.Saturated)+len(r.Skipped) > MaxDeliveryTargets {
			return false
		}
		for _, item := range r.Enqueued {
			if !validSessionIdentity("default", item.ClientID) || item.Sequence == 0 {
				return false
			}
		}
		for _, client := range r.Saturated {
			if !validSessionIdentity("default", client) {
				return false
			}
		}
		for _, client := range r.Skipped {
			if !validSessionIdentity("default", client) {
				return false
			}
		}
		if r.Session != nil {
			state := r.Session
			if !validSessionIdentity(state.TenantID, state.ClientID) || state.NextSequence == 0 || len(state.Messages) > MaxDeliveryMessagesPerSession || len(state.Inbound) > MaxDeliveryInboundPerSession {
				return false
			}
			for i, message := range state.Messages {
				if message.Sequence == 0 || message.Sequence >= state.NextSequence || (i > 0 && state.Messages[i-1].Sequence >= message.Sequence) || !validTopic(message.Topic) || len(message.Payload) > MaxDeliveryPayloadBytes || message.QoS < 1 || message.QoS > 2 || message.PacketID > 65535 {
					return false
				}
				if message.PacketID == 0 {
					if message.Phase != DeliveryPhaseQueued {
						return false
					}
				} else if (message.QoS == 1 && message.Phase != DeliveryPhaseAwaitPubAck) || (message.QoS == 2 && message.Phase != DeliveryPhaseAwaitPubRec && message.Phase != DeliveryPhaseAwaitPubComp) {
					return false
				}
			}
			for i, inbound := range state.Inbound {
				if inbound.PacketID == 0 || inbound.PacketID > 65535 || (i > 0 && state.Inbound[i-1].PacketID >= inbound.PacketID) || !validTopic(inbound.Topic) || len(inbound.Payload) > MaxDeliveryPayloadBytes || (!inbound.Released && inbound.ReleasedIndex != 0) || (inbound.Released && (inbound.ReleasedIndex == 0 || inbound.ReleasedIndex > result.Index || len(inbound.Payload) != 0)) {
					return false
				}
			}
		}
	}
	if r := result.OfflineClaim; r != nil {
		if len(r.Messages) > MaxOfflineMessagesPerSession || (!r.Found && len(r.Messages) != 0) {
			return false
		}
		for i, message := range r.Messages {
			if message.Sequence == 0 || (i > 0 && r.Messages[i-1].Sequence >= message.Sequence) || !validTopic(message.Topic) || message.QoS != 0 || len(message.Payload) > MaxOfflinePayloadBytes {
				return false
			}
		}
	}
	if r := result.NodeDrain; r != nil && (validateNodeDrainState(*r) != nil || r.CommittedIndex > result.Index) {
		return false
	}
	if r := result.TenantQuota; r != nil && (!validSessionIdentity(r.TenantID, "receipt") || r.ProfileVersion == 0 || (r.Dimension != TenantQuotaRetainedBytes && r.Dimension != TenantQuotaOfflineQueueMessages && r.Dimension != TenantQuotaOfflineQueueBytes && r.Dimension != TenantQuotaStorageBytes)) {
		return false
	}
	if r := result.SessionMetadata; r != nil && r.Will != nil && (!validTopic(r.Will.Topic) || r.Will.QoS > 2 || len(r.Will.Payload) > MaxSessionWillPayloadBytes) {
		return false
	}
	if r := result.ConnectionRebalance; r != nil && (validateConnectionRebalanceState(*r) != nil || r.CommittedIndex > result.Index) {
		return false
	}
	if r := result.SupersededWill; r != nil && (!validTopic(r.Will.Topic) || r.Will.QoS > 2 || len(r.Will.Payload) > MaxSessionWillPayloadBytes || (r.ProtocolVersion != 4 && r.ProtocolVersion != 5)) {
		return false
	}
	return true
}

func validProposalConflict(code ApplyConflictCode) bool {
	switch code {
	case ConflictMemberIDReserved, ConflictMemberIdentity, ConflictIdempotency, ConflictOperationID,
		ConflictMembershipInProgress, ConflictOperationNotFound, ConflictPhase, ConflictConfigurationMismatch,
		ConflictStaleEpoch, ConflictOwnershipMissing, ConflictMemberNotFound, ConflictCompatibilityUnsupported,
		ConflictActiveFeatureMissing, ConflictFeatureUnsupported, ConflictRetainedCapacity, ConflictOfflineCapacity,
		ConflictOfflineSessionMissing, ConflictDeliveryCapacity, ConflictDeliveryMissing, ConflictDeliveryPacketID,
		ConflictNodeDrainActive, ConflictNodeDrainInProgress, ConflictDrainNotCancellable, ConflictDeliverySessionMissing,
		ConflictTenantQuotaStale, ConflictFeatureInactive, ConflictClusterPolicyVersion, ConflictSessionMetadataCapacity, ConflictSessionMetadataInvalid,
		ConflictConnectionRebalanceInProgress, ConflictRebalanceNotCancellable, ConflictProposalRetired,
		ConflictProposalIdentity, ConflictProposalCapacity:
		return true
	default:
		return false
	}
}

func validProposalOperationPhase(phase MembershipOperationPhase) bool {
	switch phase {
	case OperationPhaseStaged, OperationPhaseLearner, OperationPhaseCatchingUp, OperationPhasePromoting, OperationPhaseTransferring, OperationPhaseRemoving, OperationPhaseSucceeded, OperationPhaseAborted:
		return true
	default:
		return false
	}
}
