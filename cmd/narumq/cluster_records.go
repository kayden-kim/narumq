package main

import (
	"bytes"
	"errors"
	"fmt"

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
	if command.Checkpoint != nil {
		payloadCount++
		checkpoint, err := checkpointToRecord(*command.Checkpoint)
		if err != nil {
			return nil, err
		}
		wire.Payload = &records.CommandEnvelope_Checkpoint{Checkpoint: checkpoint}
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
	case *records.CommandEnvelope_Checkpoint:
		checkpoint, err := checkpointFromRecord(payload.Checkpoint)
		if err != nil {
			return membership.CommandDTO{}, err
		}
		command.Checkpoint = checkpoint
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
	for _, checkpoint := range snapshot.Checkpoints {
		value, conversionErr := checkpointToRecord(checkpoint)
		if conversionErr != nil {
			return nil, conversionErr
		}
		state.Checkpoints = append(state.Checkpoints, value)
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
	objectCount := uint64(len(snapshot.Members) + len(snapshot.Ownerships) + len(snapshot.Subscriptions) + len(snapshot.Checkpoints) + len(snapshot.MembershipHistory))
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
	for _, checkpoint := range state.GetCheckpoints() {
		value, err := checkpointFromRecord(checkpoint)
		if err != nil {
			return membership.SnapshotDTO{}, err
		}
		snapshot.Checkpoints = append(snapshot.Checkpoints, *value)
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
	return snapshot, nil
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

func inboundPhaseToRecord(phase membership.InboundDeliveryPhase) (records.InboundDeliveryPhase, error) {
	if phase < membership.InboundQoS1AwaitAcceptance || phase > membership.InboundQoS2Complete {
		return 0, fmt.Errorf("unsupported inbound delivery phase %d", phase)
	}
	return records.InboundDeliveryPhase(phase), nil
}

func inboundPhaseFromRecord(phase records.InboundDeliveryPhase) (membership.InboundDeliveryPhase, error) {
	if phase < records.InboundDeliveryPhase_INBOUND_QOS1_AWAIT_ACCEPTANCE || phase > records.InboundDeliveryPhase_INBOUND_QOS2_COMPLETE {
		return 0, fmt.Errorf("unsupported inbound delivery phase %d", phase)
	}
	return membership.InboundDeliveryPhase(phase), nil
}

func outboundPhaseToRecord(phase membership.OutboundDeliveryPhase) (records.OutboundDeliveryPhase, error) {
	if phase < membership.OutboundAwaitPubAck || phase > membership.OutboundComplete {
		return 0, fmt.Errorf("unsupported outbound delivery phase %d", phase)
	}
	return records.OutboundDeliveryPhase(phase), nil
}

func outboundPhaseFromRecord(phase records.OutboundDeliveryPhase) (membership.OutboundDeliveryPhase, error) {
	if phase < records.OutboundDeliveryPhase_OUTBOUND_AWAIT_PUBACK || phase > records.OutboundDeliveryPhase_OUTBOUND_COMPLETE {
		return 0, fmt.Errorf("unsupported outbound delivery phase %d", phase)
	}
	return membership.OutboundDeliveryPhase(phase), nil
}

func checkpointToRecord(checkpoint membership.SessionCheckpointDTO) (*records.SessionCheckpoint, error) {
	wire := &records.SessionCheckpoint{
		TenantId: checkpoint.TenantID, ClientId: checkpoint.ClientID,
		OwnerNodeId: checkpoint.OwnerNodeID, Epoch: checkpoint.Epoch,
	}
	for _, item := range checkpoint.Inbound {
		phase, err := inboundPhaseToRecord(item.Phase)
		if err != nil {
			return nil, err
		}
		wire.Inbound = append(wire.Inbound, &records.InboundCheckpoint{Publish: publishToRecord(item.Publish), Phase: phase})
	}
	for _, item := range checkpoint.Outbound {
		phase, err := outboundPhaseToRecord(item.Phase)
		if err != nil {
			return nil, err
		}
		wire.Outbound = append(wire.Outbound, &records.OutboundCheckpoint{Publish: publishToRecord(item.Publish), Phase: phase})
	}
	return wire, nil
}

func checkpointFromRecord(checkpoint *records.SessionCheckpoint) (*membership.SessionCheckpointDTO, error) {
	if checkpoint == nil {
		return nil, errors.New("checkpoint record is nil")
	}
	result := &membership.SessionCheckpointDTO{
		TenantID: checkpoint.GetTenantId(), ClientID: checkpoint.GetClientId(),
		OwnerNodeID: checkpoint.GetOwnerNodeId(), Epoch: checkpoint.GetEpoch(),
	}
	for _, item := range checkpoint.GetInbound() {
		phase, err := inboundPhaseFromRecord(item.GetPhase())
		if err != nil {
			return nil, err
		}
		result.Inbound = append(result.Inbound, membership.InboundCheckpointDTO{Publish: publishFromRecord(item.GetPublish()), Phase: phase})
	}
	for _, item := range checkpoint.GetOutbound() {
		phase, err := outboundPhaseFromRecord(item.GetPhase())
		if err != nil {
			return nil, err
		}
		result.Outbound = append(result.Outbound, membership.OutboundCheckpointDTO{Publish: publishFromRecord(item.GetPublish()), Phase: phase})
	}
	return result, nil
}

func publishToRecord(packet membership.PublishPacketDTO) *records.PublishPacket {
	return &records.PublishPacket{
		ProtocolVersion: packet.ProtocolVersion, Dup: packet.DUP, Qos: packet.QoS,
		Retain: packet.Retain, PacketId: packet.PacketID, Topic: packet.Topic,
		Payload:                  append([]byte(nil), packet.Payload...),
		HasMessageExpiryInterval: packet.HasMessageExpiryInterval,
		MessageExpiryInterval:    packet.MessageExpiryInterval,
	}
}

func publishFromRecord(packet *records.PublishPacket) membership.PublishPacketDTO {
	if packet == nil {
		return membership.PublishPacketDTO{}
	}
	return membership.PublishPacketDTO{
		ProtocolVersion: packet.GetProtocolVersion(), DUP: packet.GetDup(), QoS: packet.GetQos(),
		Retain: packet.GetRetain(), PacketID: packet.GetPacketId(), Topic: packet.GetTopic(),
		Payload:                  append([]byte(nil), packet.GetPayload()...),
		HasMessageExpiryInterval: packet.GetHasMessageExpiryInterval(),
		MessageExpiryInterval:    packet.GetMessageExpiryInterval(),
	}
}
