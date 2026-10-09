package membership

import (
	"bytes"
	"reflect"
	"slices"
	"sort"
)

// Snapshot returns a canonical deep-owned state image at the machine's exact applied index.
func (machine *StateMachine) Snapshot(lastIncludedIndex uint64) (SnapshotDTO, error) {
	machine.mu.RLock()
	defer machine.mu.RUnlock()
	if lastIncludedIndex != machine.lastAppliedIndex {
		return SnapshotDTO{}, ErrSnapshotIndexMismatch
	}
	return machine.snapshotLocked(lastIncludedIndex), nil
}

// Restore validates and atomically replaces machine state with a canonical snapshot.
func (machine *StateMachine) Restore(snapshot SnapshotDTO) error {
	copyOfSnapshot := cloneSnapshot(snapshot)
	if validSnapshot(copyOfSnapshot) != nil {
		return ErrInvalidSnapshot
	}
	machine.mu.Lock()
	defer machine.mu.Unlock()
	if copyOfSnapshot.LastIncludedIndex < machine.lastAppliedIndex {
		return ErrSnapshotIndexMismatch
	}
	if copyOfSnapshot.LastIncludedIndex == machine.lastAppliedIndex {
		current := machine.snapshotLocked(machine.lastAppliedIndex)
		if !sameSnapshotState(current, copyOfSnapshot) {
			return ErrSnapshotIndexMismatch
		}
	}

	members := make(map[uint64]MemberIdentityDTO, len(copyOfSnapshot.Members))
	for _, member := range copyOfSnapshot.Members {
		members[member.MemberID] = cloneMember(member)
	}
	operations := make(map[string]operationState, len(copyOfSnapshot.MembershipHistory))
	idempotency := make(map[string]string, len(copyOfSnapshot.MembershipHistory))
	for _, record := range copyOfSnapshot.MembershipHistory {
		operation := operationFromRecord(record)
		operations[record.OperationID] = operationState{view: operation}
		idempotency[record.IdempotencyKey] = record.OperationID
	}
	ownerships := make(map[sessionKey]OwnershipStateDTO, len(copyOfSnapshot.Ownerships))
	for _, ownership := range copyOfSnapshot.Ownerships {
		ownerships[sessionKey{tenantID: ownership.TenantID, clientID: ownership.ClientID}] = ownership
	}
	subscriptions := make(map[subscriptionKey]SubscriptionStateDTO, len(copyOfSnapshot.Subscriptions))
	for _, subscription := range copyOfSnapshot.Subscriptions {
		subscriptions[subscriptionKey{sessionKey: sessionKey{tenantID: subscription.TenantID, clientID: subscription.ClientID}, filter: subscription.Filter}] = subscription
	}
	delivery := make(map[sessionKey]DeliverySessionStateDTO, len(copyOfSnapshot.DeliverySessions))
	deliveryEntries, deliveryBytes := 0, 0
	tenantUsage := make(map[string]tenantStorageUsage, len(copyOfSnapshot.DeliverySessions)+len(copyOfSnapshot.Retained)+len(copyOfSnapshot.OfflineSessions))
	for _, record := range copyOfSnapshot.DeliverySessions {
		key := sessionKey{tenantID: record.TenantID, clientID: record.ClientID}
		cloned := cloneDeliverySession(record)
		delivery[key] = cloned
		deliveryEntries += len(cloned.Messages) + len(cloned.Inbound)
		usage := tenantUsage[key.tenantID]
		for _, message := range cloned.Messages {
			byteCount := deliveryMessageBytes(key, message)
			deliveryBytes += byteCount
			usage.queueMessages++
			usage.queueBytes += byteCount
			usage.storageBytes += byteCount
		}
		for _, inbound := range cloned.Inbound {
			byteCount := deliveryInboundBytes(key, inbound)
			deliveryBytes += byteCount
			usage.storageBytes += byteCount
		}
		if usage != (tenantStorageUsage{}) {
			tenantUsage[key.tenantID] = usage
		}
	}
	retained := make(map[retainedKey]RetainedStateDTO, len(copyOfSnapshot.Retained))
	retainedBytes := 0
	for _, state := range copyOfSnapshot.Retained {
		retained[retainedKey{tenantID: state.TenantID, topic: state.Topic}] = state
		byteCount := retainedStateBytes(state)
		retainedBytes += byteCount
		usage := tenantUsage[state.TenantID]
		usage.retainedBytes += byteCount
		usage.storageBytes += byteCount
		tenantUsage[state.TenantID] = usage
	}
	offline := make(map[sessionKey]OfflineSessionStateDTO, len(copyOfSnapshot.OfflineSessions))
	offlineMessages := 0
	offlineBytes := 0
	for _, session := range copyOfSnapshot.OfflineSessions {
		cloned := cloneOfflineSession(session)
		key := sessionKey{tenantID: session.TenantID, clientID: session.ClientID}
		offline[key] = cloned
		offlineMessages += len(session.Messages)
		usage := tenantUsage[key.tenantID]
		for _, msg := range session.Messages {
			byteCount := offlineMessageBytes(session.TenantID, session.ClientID, msg)
			offlineBytes += byteCount
			usage.queueMessages++
			usage.queueBytes += byteCount
			usage.storageBytes += byteCount
		}
		if usage != (tenantStorageUsage{}) {
			tenantUsage[key.tenantID] = usage
		}
	}
	nodeDrains := make(map[uint64]NodeDrainStateDTO, len(copyOfSnapshot.NodeDrains))
	for _, state := range copyOfSnapshot.NodeDrains {
		nodeDrains[state.MemberID] = state
	}
	sessionMetadata := make(map[sessionKey]SessionMetadataStateDTO, len(copyOfSnapshot.SessionMetadata))
	totalSessionWillBytes := 0
	for _, state := range copyOfSnapshot.SessionMetadata {
		cloned := cloneSessionMetadataStateDTO(state)
		sessionMetadata[sessionKey{tenantID: state.TenantID, clientID: state.ClientID}] = cloned
		totalSessionWillBytes += sessionWillBytes(cloned)
	}
	credentialRevocations := make(map[string]CredentialRevocationStateDTO, len(copyOfSnapshot.CredentialRevocations))
	for _, state := range copyOfSnapshot.CredentialRevocations {
		credentialRevocations[state.CredentialID] = state
	}
	var connectionRebalance *ConnectionRebalanceStateDTO
	if copyOfSnapshot.ConnectionRebalance != nil {
		state := cloneConnectionRebalanceState(*copyOfSnapshot.ConnectionRebalance)
		connectionRebalance = &state
	}
	tenantQuota, tenantQuotaByTenant := installTenantQuotaProfile(copyOfSnapshot.TenantQuotaProfile)
	proposalOrigins, proposalCount, proposalBytes := restoreProposalOrigins(copyOfSnapshot.ProposalOrigins)
	machine.lastAppliedIndex = copyOfSnapshot.LastIncludedIndex
	machine.members = members
	machine.operations = operations
	machine.idempotency = idempotency
	machine.ownerships = ownerships
	machine.subscriptions = subscriptions
	machine.retained = retained
	machine.retainedBytes = retainedBytes
	machine.offline = offline
	machine.offlineBytes = offlineBytes
	machine.nodeDrains = nodeDrains
	machine.sessionMetadata = sessionMetadata
	machine.sessionWillBytes = totalSessionWillBytes
	machine.delivery = delivery
	machine.deliveryEntries = deliveryEntries
	machine.deliveryBytes = deliveryBytes
	machine.tenantUsage = tenantUsage
	machine.tenantQuota = tenantQuota
	machine.tenantQuotaByTenant = tenantQuotaByTenant
	machine.proposalOrigins = proposalOrigins
	machine.proposalReceiptCount = proposalCount
	machine.proposalResultBytes = proposalBytes
	machine.connectionRebalance = connectionRebalance
	machine.credentialRevocations = credentialRevocations
	machine.activeFeatures = slices.Clone(copyOfSnapshot.ActiveFeatures)
	return nil
}

func (machine *StateMachine) snapshotLocked(lastIncludedIndex uint64) SnapshotDTO {
	snapshot := SnapshotDTO{
		SchemaMajor: SchemaMajor, SchemaMinor: SchemaMinor, LastIncludedIndex: lastIncludedIndex,
		Members:               make([]MemberIdentityDTO, 0, len(machine.members)),
		Ownerships:            make([]OwnershipStateDTO, 0, len(machine.ownerships)),
		Subscriptions:         make([]SubscriptionStateDTO, 0, len(machine.subscriptions)),
		DeliverySessions:      make([]DeliverySessionStateDTO, 0, len(machine.delivery)),
		MembershipHistory:     make([]MembershipRecordDTO, 0, len(machine.operations)),
		OfflineSessions:       make([]OfflineSessionStateDTO, 0, len(machine.offline)),
		Retained:              make([]RetainedStateDTO, 0, len(machine.retained)),
		NodeDrains:            make([]NodeDrainStateDTO, 0, len(machine.nodeDrains)),
		SessionMetadata:       make([]SessionMetadataStateDTO, 0, len(machine.sessionMetadata)),
		ConnectionRebalance:   cloneConnectionRebalanceStatePointer(machine.connectionRebalance),
		CredentialRevocations: make([]CredentialRevocationStateDTO, 0, len(machine.credentialRevocations)),
		ActiveFeatures:        slices.Clone(machine.activeFeatures),
		TenantQuotaProfile:    cloneTenantQuotaProfile(machine.tenantQuota),
		ProposalOrigins:       machine.proposalSnapshotLocked(),
	}
	for _, member := range machine.members {
		snapshot.Members = append(snapshot.Members, cloneMember(member))
	}
	for _, ownership := range machine.ownerships {
		snapshot.Ownerships = append(snapshot.Ownerships, ownership)
	}
	for _, subscription := range machine.subscriptions {
		snapshot.Subscriptions = append(snapshot.Subscriptions, subscription)
	}
	for _, record := range machine.delivery {
		snapshot.DeliverySessions = append(snapshot.DeliverySessions, cloneDeliverySession(record))
	}
	for _, operation := range machine.operations {
		snapshot.MembershipHistory = append(snapshot.MembershipHistory, cloneMembershipRecord(operation.view.Record))
	}
	for _, retained := range machine.retained {
		snapshot.Retained = append(snapshot.Retained, cloneRetainedState(retained))
	}
	offlineMessagesCount := 0
	for _, session := range machine.offline {
		snapshot.OfflineSessions = append(snapshot.OfflineSessions, cloneOfflineSession(session))
		offlineMessagesCount += len(session.Messages)
	}
	for _, state := range machine.nodeDrains {
		snapshot.NodeDrains = append(snapshot.NodeDrains, state)
	}
	for _, state := range machine.sessionMetadata {
		snapshot.SessionMetadata = append(snapshot.SessionMetadata, cloneSessionMetadataStateDTO(state))
	}
	for _, state := range machine.credentialRevocations {
		snapshot.CredentialRevocations = append(snapshot.CredentialRevocations, state)
	}
	sort.Slice(snapshot.Members, func(left, right int) bool { return snapshot.Members[left].MemberID < snapshot.Members[right].MemberID })
	sort.Slice(snapshot.Ownerships, func(left, right int) bool {
		return compareSession(snapshot.Ownerships[left].TenantID, snapshot.Ownerships[left].ClientID, snapshot.Ownerships[right].TenantID, snapshot.Ownerships[right].ClientID) < 0
	})
	sort.Slice(snapshot.Subscriptions, func(left, right int) bool {
		return compareSubscription(snapshot.Subscriptions[left], snapshot.Subscriptions[right]) < 0
	})
	sort.Slice(snapshot.DeliverySessions, func(left, right int) bool {
		return compareSession(snapshot.DeliverySessions[left].TenantID, snapshot.DeliverySessions[left].ClientID, snapshot.DeliverySessions[right].TenantID, snapshot.DeliverySessions[right].ClientID) < 0
	})
	sort.Slice(snapshot.Retained, func(left, right int) bool {
		return compareRetained(snapshot.Retained[left], snapshot.Retained[right]) < 0
	})
	sort.Slice(snapshot.MembershipHistory, func(left, right int) bool {
		return snapshot.MembershipHistory[left].OperationID < snapshot.MembershipHistory[right].OperationID
	})
	sort.Slice(snapshot.OfflineSessions, func(left, right int) bool {
		return compareOffline(snapshot.OfflineSessions[left], snapshot.OfflineSessions[right]) < 0
	})
	sort.Slice(snapshot.NodeDrains, func(left, right int) bool {
		return snapshot.NodeDrains[left].MemberID < snapshot.NodeDrains[right].MemberID
	})
	sort.Slice(snapshot.SessionMetadata, func(left, right int) bool {
		return compareSession(snapshot.SessionMetadata[left].TenantID, snapshot.SessionMetadata[left].ClientID, snapshot.SessionMetadata[right].TenantID, snapshot.SessionMetadata[right].ClientID) < 0
	})
	sort.Slice(snapshot.CredentialRevocations, func(left, right int) bool {
		return snapshot.CredentialRevocations[left].CredentialID < snapshot.CredentialRevocations[right].CredentialID
	})
	for index := range snapshot.DeliverySessions {
		sort.Slice(snapshot.DeliverySessions[index].Messages, func(left, right int) bool {
			return snapshot.DeliverySessions[index].Messages[left].Sequence < snapshot.DeliverySessions[index].Messages[right].Sequence
		})
		sort.Slice(snapshot.DeliverySessions[index].Inbound, func(left, right int) bool {
			return snapshot.DeliverySessions[index].Inbound[left].PacketID < snapshot.DeliverySessions[index].Inbound[right].PacketID
		})
	}
	deliveryEntries := 0
	for _, record := range snapshot.DeliverySessions {
		deliveryEntries += len(record.Messages) + len(record.Inbound)
	}
	snapshot.ObjectCount = uint64(len(snapshot.Members) + len(snapshot.Ownerships) + len(snapshot.Subscriptions) + len(snapshot.DeliverySessions) + deliveryEntries + len(snapshot.MembershipHistory) + len(snapshot.Retained) + len(snapshot.OfflineSessions) + offlineMessagesCount + len(snapshot.NodeDrains) + len(snapshot.ActiveFeatures) + len(snapshot.SessionMetadata) + len(snapshot.CredentialRevocations) + tenantQuotaObjectCount(snapshot.TenantQuotaProfile) + proposalObjectCount(snapshot.ProposalOrigins))
	if snapshot.ConnectionRebalance != nil {
		snapshot.ObjectCount++
	}
	return snapshot
}

func validateMembershipState(members []MemberIdentityDTO, history []MembershipRecordDTO) error {
	byID := make(map[uint64]MemberIdentityDTO, len(members))
	for _, member := range members {
		byID[member.MemberID] = member
	}
	idempotencyKeys := make(map[string]string, len(history))
	inProgress := 0
	activeMembers := make(map[uint64]string, 1)
	for _, record := range history {
		if record.Member == nil {
			return ErrInvalidSnapshot
		}
		member, exists := byID[record.Member.MemberID]
		if !exists || !sameMemberIdentity(member, *record.Member) {
			return ErrInvalidSnapshot
		}
		if previous, duplicate := idempotencyKeys[record.IdempotencyKey]; duplicate && previous != record.OperationID {
			return ErrInvalidSnapshot
		}
		idempotencyKeys[record.IdempotencyKey] = record.OperationID
		switch record.Action {
		case MembershipActionStageJoin:
			if record.Member.Phase != MemberPhaseStaged || record.Member.OperationID != record.OperationID {
				return ErrInvalidSnapshot
			}
			inProgress++
			activeMembers[record.Member.MemberID] = record.OperationID
		case MembershipActionAddLearner:
			if (record.Member.Phase != MemberPhaseLearner && record.Member.Phase != MemberPhaseCatchingUp) || record.Member.OperationID != record.OperationID {
				return ErrInvalidSnapshot
			}
			inProgress++
			activeMembers[record.Member.MemberID] = record.OperationID
		case MembershipActionPromoteVoter:
			if record.Member.Phase != MemberPhaseVoter || record.Member.OperationID != "" {
				return ErrInvalidSnapshot
			}
		case MembershipActionBeginRemove:
			if record.Member.Phase != MemberPhaseLeaving || record.Member.OperationID != record.OperationID || record.TargetMemberID != record.Member.MemberID {
				return ErrInvalidSnapshot
			}
			inProgress++
			activeMembers[record.Member.MemberID] = record.OperationID
		case MembershipActionRemoveMember:
			if record.Member.Phase != MemberPhaseRetired || record.Member.OperationID != "" || record.TargetMemberID != record.Member.MemberID {
				return ErrInvalidSnapshot
			}
		case MembershipActionAbortStage:
			if record.Member.Phase != MemberPhaseRetired || record.Member.OperationID != "" {
				return ErrInvalidSnapshot
			}
		default:
			return ErrInvalidSnapshot
		}
	}
	if inProgress > 1 {
		return ErrInvalidSnapshot
	}
	for _, member := range members {
		if member.Phase != MemberPhaseStaged && member.Phase != MemberPhaseLearner && member.Phase != MemberPhaseCatchingUp && member.Phase != MemberPhaseLeaving {
			continue
		}
		if activeMembers[member.MemberID] != member.OperationID {
			return ErrInvalidSnapshot
		}
	}
	return nil
}

func operationFromRecord(record MembershipRecordDTO) MembershipOperationDTO {
	view := MembershipOperationDTO{
		OperationID: record.OperationID, Action: record.Action,
		CommittedIndex: record.CommittedIndex, Record: cloneMembershipRecord(record),
	}
	if record.Member != nil {
		view.MemberID = record.Member.MemberID
		switch record.Action {
		case MembershipActionStageJoin:
			view.Phase = OperationPhaseStaged
		case MembershipActionAddLearner:
			if record.Member.Phase == MemberPhaseCatchingUp {
				view.Phase = OperationPhaseCatchingUp
			} else {
				view.Phase = OperationPhaseLearner
			}
		case MembershipActionPromoteVoter, MembershipActionRemoveMember:
			view.Phase = OperationPhaseSucceeded
		case MembershipActionBeginRemove:
			view.Phase = OperationPhaseRemoving
		case MembershipActionAbortStage:
			view.Phase = OperationPhaseAborted
		}
	}
	return view
}

func cloneSnapshot(snapshot SnapshotDTO) SnapshotDTO {
	copyOfSnapshot := snapshot
	copyOfSnapshot.Digest = bytes.Clone(snapshot.Digest)
	copyOfSnapshot.Members = make([]MemberIdentityDTO, len(snapshot.Members))
	for index, member := range snapshot.Members {
		copyOfSnapshot.Members[index] = cloneMember(member)
	}
	copyOfSnapshot.Ownerships = append([]OwnershipStateDTO(nil), snapshot.Ownerships...)
	copyOfSnapshot.Subscriptions = append([]SubscriptionStateDTO(nil), snapshot.Subscriptions...)
	copyOfSnapshot.DeliverySessions = make([]DeliverySessionStateDTO, len(snapshot.DeliverySessions))
	for index, record := range snapshot.DeliverySessions {
		copyOfSnapshot.DeliverySessions[index] = cloneDeliverySession(record)
	}
	copyOfSnapshot.MembershipHistory = make([]MembershipRecordDTO, len(snapshot.MembershipHistory))
	for index, record := range snapshot.MembershipHistory {
		copyOfSnapshot.MembershipHistory[index] = cloneMembershipRecord(record)
	}
	copyOfSnapshot.Retained = make([]RetainedStateDTO, len(snapshot.Retained))
	for index, state := range snapshot.Retained {
		copyOfSnapshot.Retained[index] = cloneRetainedState(state)
	}
	if snapshot.OfflineSessions != nil {
		copyOfSnapshot.OfflineSessions = make([]OfflineSessionStateDTO, len(snapshot.OfflineSessions))
		for index, session := range snapshot.OfflineSessions {
			copyOfSnapshot.OfflineSessions[index] = cloneOfflineSession(session)
		}
	}
	copyOfSnapshot.NodeDrains = append([]NodeDrainStateDTO(nil), snapshot.NodeDrains...)
	copyOfSnapshot.ConnectionRebalance = cloneConnectionRebalanceStatePointer(snapshot.ConnectionRebalance)
	copyOfSnapshot.CredentialRevocations = make([]CredentialRevocationStateDTO, len(snapshot.CredentialRevocations))
	copy(copyOfSnapshot.CredentialRevocations, snapshot.CredentialRevocations)
	copyOfSnapshot.ActiveFeatures = slices.Clone(snapshot.ActiveFeatures)
	copyOfSnapshot.SessionMetadata = make([]SessionMetadataStateDTO, len(snapshot.SessionMetadata))
	for index, state := range snapshot.SessionMetadata {
		copyOfSnapshot.SessionMetadata[index] = cloneSessionMetadataStateDTO(state)
	}
	copyOfSnapshot.TenantQuotaProfile = cloneTenantQuotaProfile(snapshot.TenantQuotaProfile)
	copyOfSnapshot.ProposalOrigins = cloneProposalOrigins(snapshot.ProposalOrigins)
	return copyOfSnapshot
}

func sameSnapshotState(left, right SnapshotDTO) bool {
	return left.SchemaMajor == right.SchemaMajor && left.SchemaMinor == right.SchemaMinor && left.LastIncludedIndex == right.LastIncludedIndex && left.ObjectCount == right.ObjectCount &&
		reflect.DeepEqual(left.Members, right.Members) && reflect.DeepEqual(left.Ownerships, right.Ownerships) && reflect.DeepEqual(left.Subscriptions, right.Subscriptions) &&
		reflect.DeepEqual(left.DeliverySessions, right.DeliverySessions) && reflect.DeepEqual(left.MembershipHistory, right.MembershipHistory) &&
		reflect.DeepEqual(left.Retained, right.Retained) && reflect.DeepEqual(left.OfflineSessions, right.OfflineSessions) && reflect.DeepEqual(left.NodeDrains, right.NodeDrains) &&
		reflect.DeepEqual(left.SessionMetadata, right.SessionMetadata) && reflect.DeepEqual(left.ConnectionRebalance, right.ConnectionRebalance) &&
		reflect.DeepEqual(left.CredentialRevocations, right.CredentialRevocations) && reflect.DeepEqual(left.ProposalOrigins, right.ProposalOrigins) &&
		sameTenantQuotaProfile(left.TenantQuotaProfile, right.TenantQuotaProfile) && slices.Equal(left.ActiveFeatures, right.ActiveFeatures)
}
