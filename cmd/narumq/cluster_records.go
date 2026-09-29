package main

import (
	"bytes"
	"errors"
	"fmt"
	"narumq/internal/domain/nodedrain"

	"narumq/internal/adapters/raft/records"
	"narumq/internal/application/membership"
)

func encodeMembershipCommand(command membership.CommandDTO) ([]byte, error) {
	wire := &records.CommandEnvelope{
		SchemaMajor: command.SchemaMajor,
		SchemaMinor: command.SchemaMinor,
		OperationId: command.OperationID,
	}
	payloadCount := 0
	if command.Membership != nil {
		payloadCount++
		member, err := memberIdentityToRecord(command.Membership.Member)
		if err != nil {
			return nil, err
		}
		action, err := membershipActionToRecord(command.Membership.Action)
		if err != nil {
			return nil, err
		}
		wire.Payload = &records.CommandEnvelope_Membership{Membership: &records.MembershipRecord{
			OperationId: command.Membership.OperationID, Action: action,
			IdempotencyKey: command.Membership.IdempotencyKey, Member: member,
			TargetMemberId:         command.Membership.TargetMemberID,
			ExpectedConfStateIndex: command.Membership.ExpectedConfStateIndex,
			CommittedIndex:         command.Membership.CommittedIndex,
		}}
	}
	if command.Ownership != nil {
		payloadCount++
		action, err := ownershipActionToRecord(command.Ownership.Action)
		if err != nil {
			return nil, err
		}
		wire.Payload = &records.CommandEnvelope_Ownership{Ownership: &records.OwnershipChange{
			TenantId: command.Ownership.TenantID, ClientId: command.Ownership.ClientID,
			ExpectedEpoch: command.Ownership.ExpectedEpoch, OwnerNodeId: command.Ownership.OwnerNodeID,
			Epoch: command.Ownership.Epoch, Action: action,
		}}
	}
	if command.Subscription != nil {
		payloadCount++
		action, err := subscriptionActionToRecord(command.Subscription.Action)
		if err != nil {
			return nil, err
		}
		wire.Payload = &records.CommandEnvelope_Subscription{Subscription: &records.SubscriptionChange{
			TenantId: command.Subscription.TenantID, ClientId: command.Subscription.ClientID,
			OwnerNodeId: command.Subscription.OwnerNodeID, Epoch: command.Subscription.Epoch,
			Filter: command.Subscription.Filter, Qos: command.Subscription.QoS,
			NoLocal: command.Subscription.NoLocal, RetainAsPublished: command.Subscription.RetainAsPublished,
			RetainHandling: command.Subscription.RetainHandling, Action: action,
		}}
	}
	if command.Delivery != nil {
		payloadCount++
		value, err := deliveryChangeToRecord(*command.Delivery)
		if err != nil {
			return nil, err
		}
		wire.Payload = &records.CommandEnvelope_Delivery{Delivery: value}
	}
	if command.Retained != nil {
		payloadCount++
		action, err := retainedActionToRecord(command.Retained.Action)
		if err != nil {
			return nil, err
		}
		wire.Payload = &records.CommandEnvelope_Retained{Retained: &records.RetainedChange{
			TenantId: command.Retained.TenantID, Topic: command.Retained.Topic,
			PublisherClientId: command.Retained.PublisherClientID, Qos: command.Retained.QoS,
			Payload: command.Retained.Payload, ExpiresAtUnixMillis: command.Retained.ExpiresAtUnixMillis,
			ProposedAtUnixMillis: command.Retained.ProposedAtUnixMillis, Action: action,
		}}
	}
	if command.Offline != nil {
		payloadCount++
		action, err := offlineActionToRecord(command.Offline.Action)
		if err != nil {
			return nil, err
		}
		messages := make([]*records.OfflineMessage, 0, len(command.Offline.Messages))
		for _, msg := range command.Offline.Messages {
			messages = append(messages, &records.OfflineMessage{
				Sequence:            msg.Sequence,
				Topic:               msg.Topic,
				Payload:             msg.Payload,
				Qos:                 msg.QoS,
				Retain:              msg.Retain,
				ExpiresAtUnixMillis: msg.ExpiresAtUnixMillis,
			})
		}
		wire.Payload = &records.CommandEnvelope_Offline{Offline: &records.OfflineChange{
			TenantId:                   command.Offline.TenantID,
			ClientId:                   command.Offline.ClientID,
			OwnerNodeId:                command.Offline.OwnerNodeID,
			Epoch:                      command.Offline.Epoch,
			Action:                     action,
			SessionExpiresAtUnixMillis: command.Offline.SessionExpiresAtUnixMillis,
			ProposedAtUnixMillis:       command.Offline.ProposedAtUnixMillis,
			Messages:                   messages,
		}}
	}
	if command.NodeDrain != nil {
		payloadCount++
		action, err := nodeDrainActionToRecord(command.NodeDrain.Action)
		if err != nil {
			return nil, err
		}
		phase, err := drainPhaseToRecord(command.NodeDrain.Phase)
		if err != nil {
			return nil, err
		}
		wire.Payload = &records.CommandEnvelope_NodeDrain{NodeDrain: &records.NodeDrainChange{
			Action: action, MemberId: command.NodeDrain.MemberID,
			DrainOperationId: command.NodeDrain.DrainOperationID, IdempotencyKey: command.NodeDrain.IdempotencyKey,
			ServerReference: command.NodeDrain.ServerReference, ConnectionRatePerSecond: command.NodeDrain.ConnectionRatePerSecond,
			LoadBalancerPropagationDelayMillis: command.NodeDrain.LoadBalancerPropagationDelayMillis,
			TimeoutMillis:                      command.NodeDrain.TimeoutMillis, Phase: phase,
			Progress:    nodeDrainProgressToRecord(command.NodeDrain.Progress),
			FailureCode: command.NodeDrain.FailureCode, BlockingReason: command.NodeDrain.BlockingReason,
			ProposedAtUnixMillis: command.NodeDrain.ProposedAtUnixMillis,
		}}
	}
	if command.TenantQuota != nil {
		payloadCount++
		wire.Payload = &records.CommandEnvelope_TenantQuota{TenantQuota: &records.TenantQuotaChange{
			Profile: tenantQuotaProfileToRecord(&command.TenantQuota.Profile),
		}}
	}
	if payloadCount != 1 {
		return nil, errors.New("application command must contain exactly one payload")
	}
	return records.MarshalCommand(wire)
}

func decodeMembershipCommand(data []byte) (membership.CommandDTO, error) {
	wire, err := records.UnmarshalCommand(data)
	if err != nil {
		return membership.CommandDTO{}, err
	}
	command := membership.CommandDTO{
		SchemaMajor: wire.GetSchemaMajor(), SchemaMinor: wire.GetSchemaMinor(), OperationID: wire.GetOperationId(),
	}
	switch payload := wire.GetPayload().(type) {
	case *records.CommandEnvelope_Membership:
		record := payload.Membership
		member, err := memberIdentityFromRecord(record.GetMember())
		if err != nil {
			return membership.CommandDTO{}, err
		}
		action, err := membershipActionFromRecord(record.GetAction())
		if err != nil {
			return membership.CommandDTO{}, err
		}
		command.Membership = &membership.MembershipRecordDTO{
			OperationID: record.GetOperationId(), Action: action, IdempotencyKey: record.GetIdempotencyKey(),
			Member: member, TargetMemberID: record.GetTargetMemberId(),
			ExpectedConfStateIndex: record.GetExpectedConfStateIndex(),
			CommittedIndex:         record.GetCommittedIndex(),
		}
	case *records.CommandEnvelope_Ownership:
		value := payload.Ownership
		action, err := ownershipActionFromRecord(value.GetAction())
		if err != nil {
			return membership.CommandDTO{}, err
		}
		command.Ownership = &membership.OwnershipChangeDTO{
			TenantID: value.GetTenantId(), ClientID: value.GetClientId(),
			ExpectedEpoch: value.GetExpectedEpoch(), OwnerNodeID: value.GetOwnerNodeId(),
			Epoch: value.GetEpoch(), Action: action,
		}
	case *records.CommandEnvelope_Subscription:
		value := payload.Subscription
		action, err := subscriptionActionFromRecord(value.GetAction())
		if err != nil {
			return membership.CommandDTO{}, err
		}
		command.Subscription = &membership.SubscriptionChangeDTO{
			TenantID: value.GetTenantId(), ClientID: value.GetClientId(), OwnerNodeID: value.GetOwnerNodeId(),
			Epoch: value.GetEpoch(), Filter: value.GetFilter(), QoS: value.GetQos(),
			NoLocal: value.GetNoLocal(), RetainAsPublished: value.GetRetainAsPublished(),
			RetainHandling: value.GetRetainHandling(), Action: action,
		}
	case *records.CommandEnvelope_Delivery:
		value, err := deliveryChangeFromRecord(payload.Delivery)
		if err != nil {
			return membership.CommandDTO{}, err
		}
		command.Delivery = value
	case *records.CommandEnvelope_Retained:
		value := payload.Retained
		action, err := retainedActionFromRecord(value.GetAction())
		if err != nil {
			return membership.CommandDTO{}, err
		}
		command.Retained = &membership.RetainedChangeDTO{
			TenantID: value.GetTenantId(), Topic: value.GetTopic(), PublisherClientID: value.GetPublisherClientId(),
			QoS: value.GetQos(), Payload: value.GetPayload(), ExpiresAtUnixMillis: value.GetExpiresAtUnixMillis(),
			ProposedAtUnixMillis: value.GetProposedAtUnixMillis(), Action: action,
		}
	case *records.CommandEnvelope_Offline:
		value := payload.Offline
		action, err := offlineActionFromRecord(value.GetAction())
		if err != nil {
			return membership.CommandDTO{}, err
		}
		messages := make([]membership.OfflineMessageDTO, 0, len(value.GetMessages()))
		for _, msg := range value.GetMessages() {
			messages = append(messages, membership.OfflineMessageDTO{
				Sequence:            msg.GetSequence(),
				Topic:               msg.GetTopic(),
				Payload:             msg.GetPayload(),
				QoS:                 msg.GetQos(),
				Retain:              msg.GetRetain(),
				ExpiresAtUnixMillis: msg.GetExpiresAtUnixMillis(),
			})
		}
		command.Offline = &membership.OfflineChangeDTO{
			TenantID:                   value.GetTenantId(),
			ClientID:                   value.GetClientId(),
			OwnerNodeID:                value.GetOwnerNodeId(),
			Epoch:                      value.GetEpoch(),
			Action:                     action,
			SessionExpiresAtUnixMillis: value.GetSessionExpiresAtUnixMillis(),
			ProposedAtUnixMillis:       value.GetProposedAtUnixMillis(),
			Messages:                   messages,
		}
	case *records.CommandEnvelope_NodeDrain:
		value := payload.NodeDrain
		action, err := nodeDrainActionFromRecord(value.GetAction())
		if err != nil {
			return membership.CommandDTO{}, err
		}
		phase, err := drainPhaseFromRecord(value.GetPhase(), true)
		if err != nil {
			return membership.CommandDTO{}, err
		}
		command.NodeDrain = &membership.NodeDrainChangeDTO{
			Action: action, MemberID: value.GetMemberId(), DrainOperationID: value.GetDrainOperationId(),
			IdempotencyKey: value.GetIdempotencyKey(), ServerReference: value.GetServerReference(),
			ConnectionRatePerSecond:            value.GetConnectionRatePerSecond(),
			LoadBalancerPropagationDelayMillis: value.GetLoadBalancerPropagationDelayMillis(),
			TimeoutMillis:                      value.GetTimeoutMillis(), Phase: phase,
			Progress:    nodeDrainProgressFromRecord(value.GetProgress()),
			FailureCode: value.GetFailureCode(), BlockingReason: value.GetBlockingReason(),
			ProposedAtUnixMillis: value.GetProposedAtUnixMillis(),
		}
	case *records.CommandEnvelope_TenantQuota:
		profile := tenantQuotaProfileFromRecord(payload.TenantQuota.GetProfile())
		if profile == nil {
			return membership.CommandDTO{}, errors.New("tenant quota profile is required")
		}
		command.TenantQuota = &membership.TenantQuotaChangeDTO{Profile: *profile}
	default:
		return membership.CommandDTO{}, errors.New("decoded command has no supported payload")
	}
	return command, nil
}

func encodeMembershipSnapshot(snapshot membership.SnapshotDTO) ([]byte, error) {
	if snapshot.SchemaMajor != membership.SchemaMajor || snapshot.SchemaMinor != membership.SchemaMinor {
		return nil, errors.New("application snapshot schema is unsupported")
	}
	state := &records.ReplicatedStateSnapshot{}
	var err error
	for _, member := range snapshot.Members {
		var item *records.MemberIdentity
		item, err = memberIdentityToRecord(&member)
		if err != nil {
			return nil, err
		}
		state.Members = append(state.Members, item)
	}
	for _, ownership := range snapshot.Ownerships {
		ownershipRecord := &records.OwnershipState{
			TenantId: ownership.TenantID, ClientId: ownership.ClientID,
			OwnerNodeId: ownership.OwnerNodeID, Epoch: ownership.Epoch,
			CommittedIndex: ownership.CommittedIndex,
		}
		state.Ownerships = append(state.Ownerships, ownershipRecord)
	}
	for _, subscription := range snapshot.Subscriptions {
		state.Subscriptions = append(state.Subscriptions, &records.SubscriptionState{
			TenantId: subscription.TenantID, ClientId: subscription.ClientID,
			OwnerNodeId: subscription.OwnerNodeID, Epoch: subscription.Epoch,
			Filter: subscription.Filter, Qos: subscription.QoS, NoLocal: subscription.NoLocal,
			RetainAsPublished: subscription.RetainAsPublished, RetainHandling: subscription.RetainHandling,
		})
	}
	deliveryEntriesCount := 0
	for _, session := range snapshot.DeliverySessions {
		value, conversionErr := deliverySessionToRecord(session)
		if conversionErr != nil {
			return nil, conversionErr
		}
		state.DeliverySessions = append(state.DeliverySessions, value)
		deliveryEntriesCount += len(session.Messages) + len(session.Inbound)
	}
	for _, record := range snapshot.MembershipHistory {
		member, conversionErr := memberIdentityToRecord(record.Member)
		if conversionErr != nil {
			return nil, conversionErr
		}
		action, conversionErr := membershipActionToRecord(record.Action)
		if conversionErr != nil {
			return nil, conversionErr
		}
		state.MembershipHistory = append(state.MembershipHistory, &records.MembershipRecord{
			OperationId: record.OperationID, Action: action, IdempotencyKey: record.IdempotencyKey,
			Member: member, TargetMemberId: record.TargetMemberID,
			ExpectedConfStateIndex: record.ExpectedConfStateIndex, CommittedIndex: record.CommittedIndex,
		})
	}
	for _, retained := range snapshot.Retained {
		state.Retained = append(state.Retained, &records.RetainedState{
			TenantId: retained.TenantID, Topic: retained.Topic, PublisherClientId: retained.PublisherClientID,
			Qos: retained.QoS, Payload: retained.Payload, ExpiresAtUnixMillis: retained.ExpiresAtUnixMillis,
		})
	}
	offlineMessagesCount := 0
	for _, session := range snapshot.OfflineSessions {
		messages := make([]*records.OfflineMessage, 0, len(session.Messages))
		for _, msg := range session.Messages {
			messages = append(messages, &records.OfflineMessage{
				Sequence:            msg.Sequence,
				Topic:               msg.Topic,
				Payload:             msg.Payload,
				Qos:                 msg.QoS,
				Retain:              msg.Retain,
				ExpiresAtUnixMillis: msg.ExpiresAtUnixMillis,
			})
		}
		offlineMessagesCount += len(session.Messages)
		state.OfflineSessions = append(state.OfflineSessions, &records.OfflineSessionState{
			TenantId:                   session.TenantID,
			ClientId:                   session.ClientID,
			Epoch:                      session.Epoch,
			SessionExpiresAtUnixMillis: session.SessionExpiresAtUnixMillis,
			NextSequence:               session.NextSequence,
			Messages:                   messages,
		})
	}
	for _, drain := range snapshot.NodeDrains {
		phase, conversionErr := drainPhaseToRecord(drain.Phase)
		if conversionErr != nil {
			return nil, conversionErr
		}
		state.NodeDrains = append(state.NodeDrains, &records.NodeDrainState{
			DrainOperationId: drain.DrainOperationID, MemberId: drain.MemberID, IdempotencyKey: drain.IdempotencyKey,
			Phase: phase, ServerReference: drain.ServerReference, ConnectionRatePerSecond: drain.ConnectionRatePerSecond,
			LoadBalancerPropagationDelayMillis: drain.LoadBalancerPropagationDelayMillis,
			TimeoutMillis:                      drain.TimeoutMillis, StartedAtUnixMillis: drain.StartedAtUnixMillis,
			DeadlineAtUnixMillis: drain.DeadlineAtUnixMillis, PhaseStartedAtUnixMillis: drain.PhaseStartedAtUnixMillis,
			Progress: nodeDrainProgressToRecord(drain.Progress), FailureCode: drain.FailureCode,
			BlockingReason: drain.BlockingReason, CommittedIndex: drain.CommittedIndex,
		})
	}
	state.TenantQuotaProfile = tenantQuotaProfileToRecord(snapshot.TenantQuotaProfile)
	quotaProfileObjectCount := membershipQuotaProfileObjectCount(snapshot.TenantQuotaProfile)
	objectCount := uint64(len(snapshot.Members) + len(snapshot.Ownerships) + len(snapshot.Subscriptions) + len(snapshot.DeliverySessions) + deliveryEntriesCount + len(snapshot.MembershipHistory) + len(snapshot.Retained) + len(snapshot.OfflineSessions) + offlineMessagesCount + len(snapshot.NodeDrains) + quotaProfileObjectCount)
	if snapshot.ObjectCount != objectCount {
		return nil, errors.New("application snapshot object count does not match its contents")
	}
	wire, err := records.MarshalSnapshot(state, snapshot.LastIncludedIndex)
	if err != nil {
		return nil, err
	}
	if len(snapshot.Digest) != 0 {
		decoded, err := records.UnmarshalSnapshot(wire)
		if err != nil {
			return nil, err
		}
		if !bytes.Equal(snapshot.Digest, decoded.GetSha256()) {
			return nil, errors.New("application snapshot digest does not match its contents")
		}
	}
	return wire, nil
}

func decodeMembershipSnapshot(data []byte) (membership.SnapshotDTO, error) {
	wire, err := records.UnmarshalSnapshot(data)
	if err != nil {
		return membership.SnapshotDTO{}, err
	}
	state := wire.GetState()
	snapshot := membership.SnapshotDTO{
		SchemaMajor: wire.GetSchemaMajor(), SchemaMinor: wire.GetSchemaMinor(),
		LastIncludedIndex: wire.GetLastIncludedIndex(), ObjectCount: wire.GetObjectCount(),
		Digest: append([]byte(nil), wire.GetSha256()...),
	}
	for _, member := range state.GetMembers() {
		value, err := memberIdentityFromRecord(member)
		if err != nil {
			return membership.SnapshotDTO{}, err
		}
		snapshot.Members = append(snapshot.Members, *value)
	}
	for _, ownership := range state.GetOwnerships() {
		snapshot.Ownerships = append(snapshot.Ownerships, membership.OwnershipStateDTO{
			TenantID: ownership.GetTenantId(), ClientID: ownership.GetClientId(),
			OwnerNodeID: ownership.GetOwnerNodeId(), Epoch: ownership.GetEpoch(),
			CommittedIndex: ownership.GetCommittedIndex(),
		})
	}
	for _, subscription := range state.GetSubscriptions() {
		snapshot.Subscriptions = append(snapshot.Subscriptions, membership.SubscriptionStateDTO{
			TenantID: subscription.GetTenantId(), ClientID: subscription.GetClientId(),
			OwnerNodeID: subscription.GetOwnerNodeId(), Epoch: subscription.GetEpoch(),
			Filter: subscription.GetFilter(), QoS: subscription.GetQos(),
			NoLocal: subscription.GetNoLocal(), RetainAsPublished: subscription.GetRetainAsPublished(),
			RetainHandling: subscription.GetRetainHandling(),
		})
	}
	for _, session := range state.GetDeliverySessions() {
		value, err := deliverySessionFromRecord(session)
		if err != nil {
			return membership.SnapshotDTO{}, err
		}
		snapshot.DeliverySessions = append(snapshot.DeliverySessions, value)
	}
	for _, record := range state.GetMembershipHistory() {
		member, err := memberIdentityFromRecord(record.GetMember())
		if err != nil {
			return membership.SnapshotDTO{}, err
		}
		action, err := membershipActionFromRecord(record.GetAction())
		if err != nil {
			return membership.SnapshotDTO{}, err
		}
		snapshot.MembershipHistory = append(snapshot.MembershipHistory, membership.MembershipRecordDTO{
			OperationID: record.GetOperationId(), Action: action, IdempotencyKey: record.GetIdempotencyKey(),
			Member: member, TargetMemberID: record.GetTargetMemberId(),
			ExpectedConfStateIndex: record.GetExpectedConfStateIndex(), CommittedIndex: record.GetCommittedIndex(),
		})
	}
	for _, retained := range state.GetRetained() {
		snapshot.Retained = append(snapshot.Retained, membership.RetainedStateDTO{
			TenantID: retained.GetTenantId(), Topic: retained.GetTopic(),
			PublisherClientID: retained.GetPublisherClientId(), QoS: retained.GetQos(),
			Payload: retained.GetPayload(), ExpiresAtUnixMillis: retained.GetExpiresAtUnixMillis(),
		})
	}
	for _, session := range state.GetOfflineSessions() {
		messages := make([]membership.OfflineMessageDTO, 0, len(session.GetMessages()))
		for _, msg := range session.GetMessages() {
			messages = append(messages, membership.OfflineMessageDTO{
				Sequence:            msg.GetSequence(),
				Topic:               msg.GetTopic(),
				Payload:             msg.GetPayload(),
				QoS:                 msg.GetQos(),
				Retain:              msg.GetRetain(),
				ExpiresAtUnixMillis: msg.GetExpiresAtUnixMillis(),
			})
		}
		snapshot.OfflineSessions = append(snapshot.OfflineSessions, membership.OfflineSessionStateDTO{
			TenantID:                   session.GetTenantId(),
			ClientID:                   session.GetClientId(),
			Epoch:                      session.GetEpoch(),
			SessionExpiresAtUnixMillis: session.GetSessionExpiresAtUnixMillis(),
			NextSequence:               session.GetNextSequence(),
			Messages:                   messages,
		})
	}
	for _, drain := range state.GetNodeDrains() {
		phase, conversionErr := drainPhaseFromRecord(drain.GetPhase(), false)
		if conversionErr != nil {
			return membership.SnapshotDTO{}, conversionErr
		}
		snapshot.NodeDrains = append(snapshot.NodeDrains, membership.NodeDrainStateDTO{
			DrainOperationID: drain.GetDrainOperationId(), MemberID: drain.GetMemberId(), IdempotencyKey: drain.GetIdempotencyKey(),
			Phase: phase, ServerReference: drain.GetServerReference(),
			ConnectionRatePerSecond:            drain.GetConnectionRatePerSecond(),
			LoadBalancerPropagationDelayMillis: drain.GetLoadBalancerPropagationDelayMillis(),
			TimeoutMillis:                      drain.GetTimeoutMillis(), StartedAtUnixMillis: drain.GetStartedAtUnixMillis(),
			DeadlineAtUnixMillis: drain.GetDeadlineAtUnixMillis(), PhaseStartedAtUnixMillis: drain.GetPhaseStartedAtUnixMillis(),
			Progress: nodeDrainProgressFromRecord(drain.GetProgress()), FailureCode: drain.GetFailureCode(),
			BlockingReason: drain.GetBlockingReason(), CommittedIndex: drain.GetCommittedIndex(),
		})
	}
	snapshot.TenantQuotaProfile = tenantQuotaProfileFromRecord(state.GetTenantQuotaProfile())
	return snapshot, nil
}
func tenantQuotaProfileToRecord(profile *membership.TenantQuotaProfileDTO) *records.TenantQuotaProfile {
	if profile == nil {
		return nil
	}
	var tenants []*records.TenantStorageQuota
	if profile.Tenants != nil {
		tenants = make([]*records.TenantStorageQuota, 0, len(profile.Tenants))
	}
	for _, quota := range profile.Tenants {
		tenants = append(tenants, tenantStorageQuotaToRecord(quota))
	}
	return &records.TenantQuotaProfile{
		Version: profile.Version, DefaultQuota: tenantStorageQuotaToRecord(profile.Default), Tenants: tenants,
	}
}

func tenantQuotaProfileFromRecord(profile *records.TenantQuotaProfile) *membership.TenantQuotaProfileDTO {
	if profile == nil {
		return nil
	}
	var tenants []membership.TenantStorageQuotaDTO
	if profile.Tenants != nil {
		tenants = make([]membership.TenantStorageQuotaDTO, 0, len(profile.Tenants))
	}
	for _, quota := range profile.Tenants {
		if quota == nil {
			return nil
		}
		tenants = append(tenants, tenantStorageQuotaFromRecord(quota))
	}
	if profile.DefaultQuota == nil {
		return nil
	}
	return &membership.TenantQuotaProfileDTO{
		Version: profile.Version, Default: tenantStorageQuotaFromRecord(profile.DefaultQuota), Tenants: tenants,
	}
}

func tenantStorageQuotaToRecord(quota membership.TenantStorageQuotaDTO) *records.TenantStorageQuota {
	return &records.TenantStorageQuota{
		TenantId: quota.TenantID, RetainedBytes: quota.RetainedBytes,
		OfflineQueueMessages: quota.OfflineQueueMessages, OfflineQueueBytes: quota.OfflineQueueBytes,
		StorageBytes: quota.StorageBytes,
	}
}

func tenantStorageQuotaFromRecord(quota *records.TenantStorageQuota) membership.TenantStorageQuotaDTO {
	return membership.TenantStorageQuotaDTO{
		TenantID: quota.GetTenantId(), RetainedBytes: quota.GetRetainedBytes(),
		OfflineQueueMessages: quota.GetOfflineQueueMessages(), OfflineQueueBytes: quota.GetOfflineQueueBytes(),
		StorageBytes: quota.GetStorageBytes(),
	}
}

func membershipQuotaProfileObjectCount(profile *membership.TenantQuotaProfileDTO) int {
	if profile == nil {
		return 0
	}
	return 1 + len(profile.Tenants)
}

func offlineActionToRecord(action membership.OfflineAction) (records.OfflineAction, error) {
	switch action {
	case membership.OfflineActionDetach:
		return records.OfflineAction_OFFLINE_ACTION_DETACH, nil
	case membership.OfflineActionEnqueue:
		return records.OfflineAction_OFFLINE_ACTION_ENQUEUE, nil
	case membership.OfflineActionClaim:
		return records.OfflineAction_OFFLINE_ACTION_CLAIM, nil
	default:
		return 0, fmt.Errorf("unsupported offline action %d", action)
	}
}

func offlineActionFromRecord(action records.OfflineAction) (membership.OfflineAction, error) {
	switch action {
	case records.OfflineAction_OFFLINE_ACTION_DETACH:
		return membership.OfflineActionDetach, nil
	case records.OfflineAction_OFFLINE_ACTION_ENQUEUE:
		return membership.OfflineActionEnqueue, nil
	case records.OfflineAction_OFFLINE_ACTION_CLAIM:
		return membership.OfflineActionClaim, nil
	default:
		return 0, fmt.Errorf("unsupported offline action %d", action)
	}
}

func memberIdentityToRecord(member *membership.MemberIdentityDTO) (*records.MemberIdentity, error) {
	if member == nil {
		return nil, nil
	}
	phase, err := memberPhaseToRecord(member.Phase)
	if err != nil {
		return nil, err
	}
	return &records.MemberIdentity{
		MemberId: member.MemberID, ClusterId: member.ClusterID, Address: member.Address, DnsSan: member.DNSSAN,
		CertificateSha256: append([]byte(nil), member.CertificateSHA256...), ReleaseVersion: member.ReleaseVersion,
		ClusterProtocolMajor: member.ClusterProtocolMajor, ClusterProtocolMinor: member.ClusterProtocolMinor,
		StateSchemaMajor: member.StateSchemaMajor, StateSchemaMinor: member.StateSchemaMinor,
		Phase: phase, OperationId: member.OperationID,
	}, nil
}

func memberIdentityFromRecord(member *records.MemberIdentity) (*membership.MemberIdentityDTO, error) {
	if member == nil {
		return nil, nil
	}
	phase, err := memberPhaseFromRecord(member.GetPhase())
	if err != nil {
		return nil, err
	}
	return &membership.MemberIdentityDTO{
		MemberID: member.GetMemberId(), ClusterID: member.GetClusterId(), Address: member.GetAddress(), DNSSAN: member.GetDnsSan(),
		CertificateSHA256: append([]byte(nil), member.GetCertificateSha256()...), ReleaseVersion: member.GetReleaseVersion(),
		ClusterProtocolMajor: member.GetClusterProtocolMajor(), ClusterProtocolMinor: member.GetClusterProtocolMinor(),
		StateSchemaMajor: member.GetStateSchemaMajor(), StateSchemaMinor: member.GetStateSchemaMinor(),
		Phase: phase, OperationID: member.GetOperationId(),
	}, nil
}

func membershipActionToRecord(action membership.MembershipAction) (records.MembershipAction, error) {
	if action < membership.MembershipActionStageJoin || action > membership.MembershipActionAbortStage {
		return 0, fmt.Errorf("unsupported membership action %d", action)
	}
	return records.MembershipAction(action), nil
}

func membershipActionFromRecord(action records.MembershipAction) (membership.MembershipAction, error) {
	if action < records.MembershipAction_MEMBERSHIP_ACTION_STAGE_JOIN || action > records.MembershipAction_MEMBERSHIP_ACTION_ABORT_STAGE {
		return 0, fmt.Errorf("unsupported membership action %d", action)
	}
	return membership.MembershipAction(action), nil
}

func memberPhaseToRecord(phase membership.MemberPhase) (records.MemberPhase, error) {
	if phase < membership.MemberPhaseStaged || phase > membership.MemberPhaseRetired {
		return 0, fmt.Errorf("unsupported member phase %d", phase)
	}
	return records.MemberPhase(phase), nil
}

func memberPhaseFromRecord(phase records.MemberPhase) (membership.MemberPhase, error) {
	if phase < records.MemberPhase_MEMBER_PHASE_STAGED || phase > records.MemberPhase_MEMBER_PHASE_RETIRED {
		return 0, fmt.Errorf("unsupported member phase %d", phase)
	}
	return membership.MemberPhase(phase), nil
}

func ownershipActionToRecord(action membership.OwnershipAction) (records.OwnershipAction, error) {
	if action < membership.OwnershipActionAcquire || action > membership.OwnershipActionRelease {
		return 0, fmt.Errorf("unsupported ownership action %d", action)
	}
	return records.OwnershipAction(action), nil
}

func ownershipActionFromRecord(action records.OwnershipAction) (membership.OwnershipAction, error) {
	if action < records.OwnershipAction_OWNERSHIP_ACTION_ACQUIRE || action > records.OwnershipAction_OWNERSHIP_ACTION_RELEASE {
		return 0, fmt.Errorf("unsupported ownership action %d", action)
	}
	return membership.OwnershipAction(action), nil
}

func subscriptionActionToRecord(action membership.SubscriptionAction) (records.SubscriptionAction, error) {
	if action < membership.SubscriptionActionAdd || action > membership.SubscriptionActionRemove {
		return 0, fmt.Errorf("unsupported subscription action %d", action)
	}
	return records.SubscriptionAction(action), nil
}

func subscriptionActionFromRecord(action records.SubscriptionAction) (membership.SubscriptionAction, error) {
	if action < records.SubscriptionAction_SUBSCRIPTION_ACTION_ADD || action > records.SubscriptionAction_SUBSCRIPTION_ACTION_REMOVE {
		return 0, fmt.Errorf("unsupported subscription action %d", action)
	}
	return membership.SubscriptionAction(action), nil
}

func retainedActionToRecord(action membership.RetainedAction) (records.RetainedAction, error) {
	if action < membership.RetainedActionReplace || action > membership.RetainedActionDelete {
		return 0, fmt.Errorf("unsupported retained action %d", action)
	}
	return records.RetainedAction(action), nil
}

func retainedActionFromRecord(action records.RetainedAction) (membership.RetainedAction, error) {
	if action < records.RetainedAction_RETAINED_ACTION_REPLACE || action > records.RetainedAction_RETAINED_ACTION_DELETE {
		return 0, fmt.Errorf("unsupported retained action %d", action)
	}
	return membership.RetainedAction(action), nil
}

func deliveryActionToRecord(action membership.DeliveryAction) (records.DeliveryAction, error) {
	if action < membership.DeliveryActionPublish || action > membership.DeliveryActionDiscard {
		return 0, fmt.Errorf("unsupported delivery action %d", action)
	}
	return records.DeliveryAction(action), nil
}

func deliveryActionFromRecord(action records.DeliveryAction) (membership.DeliveryAction, error) {
	if action < records.DeliveryAction_DELIVERY_ACTION_PUBLISH || action > records.DeliveryAction_DELIVERY_ACTION_DISCARD {
		return 0, fmt.Errorf("unsupported delivery action %d", action)
	}
	return membership.DeliveryAction(action), nil
}

func deliveryPhaseToRecord(phase membership.DeliveryPhase) (records.DeliveryPhase, error) {
	if phase < membership.DeliveryPhaseQueued || phase > membership.DeliveryPhaseAwaitPubComp {
		return 0, fmt.Errorf("unsupported delivery phase %d", phase)
	}
	return records.DeliveryPhase(phase), nil
}

func deliveryPhaseFromRecord(phase records.DeliveryPhase) (membership.DeliveryPhase, error) {
	if phase < records.DeliveryPhase_DELIVERY_PHASE_QUEUED || phase > records.DeliveryPhase_DELIVERY_PHASE_AWAIT_PUBCOMP {
		return 0, fmt.Errorf("unsupported delivery phase %d", phase)
	}
	return membership.DeliveryPhase(phase), nil
}

func deliveryChangeToRecord(change membership.DeliveryChangeDTO) (*records.DeliveryChange, error) {
	action, err := deliveryActionToRecord(change.Action)
	if err != nil {
		return nil, err
	}
	value := &records.DeliveryChange{
		TenantId: change.TenantID, ClientId: change.ClientID, OwnerNodeId: change.OwnerNodeID,
		Epoch: change.Epoch, Action: action, ProposedAtUnixMillis: change.ProposedAtUnixMillis,
		Topic: change.Topic, Payload: append([]byte(nil), change.Payload...), Retain: change.Retain,
		ExpiresAtUnixMillis: change.ExpiresAtUnixMillis, PacketId: change.PacketID,
		Sequence: change.Sequence, Reset_: change.Reset,
		SessionExpiresAtUnixMillis: change.SessionExpiresAtUnixMillis, PublishQos: change.PublishQoS,
	}
	for _, target := range change.Targets {
		value.Targets = append(value.Targets, &records.DeliveryTarget{ClientId: target.ClientID, Retain: target.Retain})
	}
	return value, nil
}

func deliveryChangeFromRecord(change *records.DeliveryChange) (*membership.DeliveryChangeDTO, error) {
	if change == nil {
		return nil, errors.New("delivery change record is nil")
	}
	action, err := deliveryActionFromRecord(change.GetAction())
	if err != nil {
		return nil, err
	}
	value := &membership.DeliveryChangeDTO{
		TenantID: change.GetTenantId(), ClientID: change.GetClientId(), OwnerNodeID: change.GetOwnerNodeId(),
		Epoch: change.GetEpoch(), Action: action, ProposedAtUnixMillis: change.GetProposedAtUnixMillis(),
		Topic: change.GetTopic(), Payload: append([]byte(nil), change.GetPayload()...), Retain: change.GetRetain(),
		ExpiresAtUnixMillis: change.GetExpiresAtUnixMillis(), PacketID: change.GetPacketId(),
		Sequence: change.GetSequence(), Reset: change.GetReset_(),
		SessionExpiresAtUnixMillis: change.GetSessionExpiresAtUnixMillis(), PublishQoS: change.GetPublishQos(),
	}
	for _, target := range change.GetTargets() {
		value.Targets = append(value.Targets, membership.DeliveryTargetDTO{ClientID: target.GetClientId(), Retain: target.GetRetain()})
	}
	return value, nil
}

func deliverySessionToRecord(session membership.DeliverySessionStateDTO) (*records.DeliverySessionState, error) {
	value := &records.DeliverySessionState{
		TenantId: session.TenantID, ClientId: session.ClientID,
		SessionExpiresAtUnixMillis: session.SessionExpiresAtUnixMillis, NextSequence: session.NextSequence,
	}
	for _, message := range session.Messages {
		phase, err := deliveryPhaseToRecord(message.Phase)
		if err != nil {
			return nil, err
		}
		value.Messages = append(value.Messages, &records.DeliveryMessage{
			Sequence: message.Sequence, Topic: message.Topic, Payload: append([]byte(nil), message.Payload...),
			Qos: message.QoS, Retain: message.Retain, ExpiresAtUnixMillis: message.ExpiresAtUnixMillis,
			PacketId: message.PacketID, Phase: phase,
		})
	}
	for _, inbound := range session.Inbound {
		value.Inbound = append(value.Inbound, &records.InboundMessage{
			PacketId: inbound.PacketID, Topic: inbound.Topic, Payload: append([]byte(nil), inbound.Payload...),
			Retain: inbound.Retain, ExpiresAtUnixMillis: inbound.ExpiresAtUnixMillis,
			Released: inbound.Released, ReleasedIndex: inbound.ReleasedIndex,
		})
	}
	return value, nil
}

func deliverySessionFromRecord(session *records.DeliverySessionState) (membership.DeliverySessionStateDTO, error) {
	if session == nil {
		return membership.DeliverySessionStateDTO{}, errors.New("delivery session record is nil")
	}
	value := membership.DeliverySessionStateDTO{
		TenantID: session.GetTenantId(), ClientID: session.GetClientId(),
		SessionExpiresAtUnixMillis: session.GetSessionExpiresAtUnixMillis(), NextSequence: session.GetNextSequence(),
	}
	for _, message := range session.GetMessages() {
		phase, err := deliveryPhaseFromRecord(message.GetPhase())
		if err != nil {
			return membership.DeliverySessionStateDTO{}, err
		}
		value.Messages = append(value.Messages, membership.DeliveryMessageDTO{
			Sequence: message.GetSequence(), Topic: message.GetTopic(), Payload: append([]byte(nil), message.GetPayload()...),
			QoS: message.GetQos(), Retain: message.GetRetain(), ExpiresAtUnixMillis: message.GetExpiresAtUnixMillis(),
			PacketID: message.GetPacketId(), Phase: phase,
		})
	}
	for _, inbound := range session.GetInbound() {
		value.Inbound = append(value.Inbound, membership.DeliveryInboundDTO{
			PacketID: inbound.GetPacketId(), Topic: inbound.GetTopic(), Payload: append([]byte(nil), inbound.GetPayload()...),
			Retain: inbound.GetRetain(), ExpiresAtUnixMillis: inbound.GetExpiresAtUnixMillis(),
			Released: inbound.GetReleased(), ReleasedIndex: inbound.GetReleasedIndex(),
		})
	}
	return value, nil
}
func nodeDrainActionToRecord(action membership.NodeDrainAction) (records.NodeDrainAction, error) {
	if action < membership.NodeDrainActionStart || action > membership.NodeDrainActionClear {
		return 0, fmt.Errorf("unsupported node drain action %d", action)
	}
	return records.NodeDrainAction(action), nil
}

func nodeDrainActionFromRecord(action records.NodeDrainAction) (membership.NodeDrainAction, error) {
	if action < records.NodeDrainAction_NODE_DRAIN_ACTION_START || action > records.NodeDrainAction_NODE_DRAIN_ACTION_CLEAR {
		return 0, fmt.Errorf("unsupported node drain action %d", action)
	}
	return membership.NodeDrainAction(action), nil
}

func drainPhaseToRecord(phase nodedrain.Phase) (records.NodeDrainPhase, error) {
	if phase != nodedrain.PhaseUnspecified && !phase.Valid() {
		return 0, fmt.Errorf("unsupported node drain phase %d", phase)
	}
	return records.NodeDrainPhase(phase), nil
}

func drainPhaseFromRecord(phase records.NodeDrainPhase, allowUnspecified bool) (nodedrain.Phase, error) {
	value := nodedrain.Phase(phase)
	if (allowUnspecified && phase == records.NodeDrainPhase_NODE_DRAIN_PHASE_UNSPECIFIED) || value.Valid() {
		return value, nil
	}
	return 0, fmt.Errorf("unsupported node drain phase %d", phase)
}

func nodeDrainProgressToRecord(progress membership.NodeDrainProgressDTO) *records.NodeDrainProgress {
	return &records.NodeDrainProgress{
		ConnectionsInitial: progress.ConnectionsInitial, ConnectionsRemaining: progress.ConnectionsRemaining,
		SessionsReadyForTakeover: progress.SessionsReadyForTakeover, SessionsTakenOver: progress.SessionsTakenOver,
		SessionsFailed: progress.SessionsFailed, PendingWal: progress.PendingWAL, UnderReplicated: progress.UnderReplicated,
	}
}

func nodeDrainProgressFromRecord(progress *records.NodeDrainProgress) membership.NodeDrainProgressDTO {
	if progress == nil {
		return membership.NodeDrainProgressDTO{}
	}
	return membership.NodeDrainProgressDTO{
		ConnectionsInitial: progress.GetConnectionsInitial(), ConnectionsRemaining: progress.GetConnectionsRemaining(),
		SessionsReadyForTakeover: progress.GetSessionsReadyForTakeover(), SessionsTakenOver: progress.GetSessionsTakenOver(),
		SessionsFailed: progress.GetSessionsFailed(), PendingWAL: progress.GetPendingWal(),
		UnderReplicated: progress.GetUnderReplicated(),
	}
}
