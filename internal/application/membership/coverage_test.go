package membership

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

const (
	cmaTenantID = "tenant"
	cmaClientID = "client"
)

func cmaOperationID(sequence int) string {
	return fmt.Sprintf("00000000-0000-4000-8000-%012d", sequence)
}

func cmaMemberPointer(memberID uint64, phase MemberPhase, operationID string) *MemberIdentityDTO {
	member := testMember(memberID, phase, operationID)
	return &member
}

func cmaMembershipCommand(action MembershipAction, targetMemberID uint64, member *MemberIdentityDTO, mutate func(*MembershipRecordDTO)) CommandDTO {
	record := MembershipRecordDTO{OperationID: testOperation4, Action: action, IdempotencyKey: "key-4", Member: member, TargetMemberID: targetMemberID}
	if mutate != nil {
		mutate(&record)
	}
	return testCommand(testOperation4, &record)
}

func cmaStageJoinCommand(mutate func(*MembershipRecordDTO)) CommandDTO {
	record := stageRecord(testOperation4, "join-4", 4)
	if mutate != nil {
		mutate(&record)
	}
	return testCommand(testOperation4, &record)
}

func cmaWithMember(mutate func(*MemberIdentityDTO)) CommandDTO {
	return cmaStageJoinCommand(func(record *MembershipRecordDTO) { mutate(record.Member) })
}

func cmaOwnershipCommand(operationID string, change OwnershipChangeDTO) CommandDTO {
	return CommandDTO{SchemaMajor: SchemaMajor, SchemaMinor: SchemaMinor, OperationID: operationID, Ownership: &change}
}

func cmaSubscriptionCommand(operationID string, change SubscriptionChangeDTO) CommandDTO {
	return CommandDTO{SchemaMajor: SchemaMajor, SchemaMinor: SchemaMinor, OperationID: operationID, Subscription: &change}
}

func cmaDeliveryCommand(change DeliveryChangeDTO) CommandDTO {
	return CommandDTO{SchemaMajor: SchemaMajor, SchemaMinor: SchemaMinor, OperationID: testOperation4, Delivery: &change}
}

func cmaWithDelivery(mutate func(*DeliveryChangeDTO)) CommandDTO {
	change := DeliveryChangeDTO{
		TenantID: cmaTenantID, ClientID: cmaClientID, OwnerNodeID: 1, Epoch: 1,
		Action: DeliveryActionReceive, ProposedAtUnixMillis: 1, PacketID: 7,
		Topic: "orders/new", Payload: []byte("payload"),
	}
	mutate(&change)
	return cmaDeliveryCommand(change)
}

func cmaWithOwnership(mutate func(*OwnershipChangeDTO)) CommandDTO {
	change := OwnershipChangeDTO{TenantID: cmaTenantID, ClientID: "other", OwnerNodeID: 1, Epoch: 1, Action: OwnershipActionAcquire}
	mutate(&change)
	return cmaOwnershipCommand(testOperation4, change)
}

func cmaWithSubscription(mutate func(*SubscriptionChangeDTO)) CommandDTO {
	change := SubscriptionChangeDTO{TenantID: cmaTenantID, ClientID: cmaClientID, OwnerNodeID: 1, Epoch: 1, Filter: "orders/#", QoS: 1, Action: SubscriptionActionAdd}
	mutate(&change)
	return cmaSubscriptionCommand(testOperation4, change)
}

// cmaLongDNSName returns a four-label DNS name of 192+lastLabel bytes whose labels are individually valid.
func cmaLongDNSName(lastLabel int) string {
	return strings.Repeat("a", 63) + "." + strings.Repeat("b", 63) + "." + strings.Repeat("c", 63) + "." + strings.Repeat("d", lastLabel)
}

// cmaSessionMachine returns a bootstrapped machine at index 2 whose tenant/client session is owned by node 1 at epoch 1.
func cmaSessionMachine(t *testing.T) *StateMachine {
	t.Helper()
	machine := NewStateMachine()
	if err := machine.InitializeBootstrap(1, testVoters()); err != nil {
		t.Fatalf("initialize bootstrap: %v", err)
	}
	applyMustSucceed(t, machine, 2, cmaOwnershipCommand(cmaOperationID(900), OwnershipChangeDTO{
		TenantID: cmaTenantID, ClientID: cmaClientID, OwnerNodeID: 1, Epoch: 1, Action: OwnershipActionAcquire,
	}))
	return machine
}

func cmaApplyConfiguration(t *testing.T, machine *StateMachine, index uint64, operationID string, action ConfigurationAction, memberID uint64) ApplyResultDTO {
	t.Helper()
	result, err := machine.ApplyConfiguration(index, operationID, action, memberID)
	if err != nil || result.Outcome != ApplyOutcomeApplied {
		t.Fatalf("ApplyConfiguration(%d, %s, %d, %d) = (%+v, %v), want applied", index, operationID, action, memberID, result, err)
	}
	return result
}

func cmaConfigurationConflict(t *testing.T, machine *StateMachine, index uint64, operationID string, action ConfigurationAction, memberID uint64, want ApplyConflictCode) {
	t.Helper()
	result, err := machine.ApplyConfiguration(index, operationID, action, memberID)
	if err != nil || result.Outcome != ApplyOutcomeConflict || result.Conflict != want || result.Index != index {
		t.Fatalf("ApplyConfiguration(%d, %s, %d, %d) = (%+v, %v), want conflict %q", index, operationID, action, memberID, result, err, want)
	}
	if machine.LastAppliedIndex() != index {
		t.Fatalf("configuration conflict did not consume committed index %d; last applied %d", index, machine.LastAppliedIndex())
	}
}

func cmaMemberByID(t *testing.T, machine *StateMachine, memberID uint64) MemberIdentityDTO {
	t.Helper()
	for _, member := range machine.Members() {
		if member.MemberID == memberID {
			return member
		}
	}
	t.Fatalf("member %d is not committed", memberID)
	return MemberIdentityDTO{}
}

func TestCMAApplyAcceptsStructurallyValidBaseCommands(t *testing.T) {
	// The rejection table below mutates exactly one field of these commands, so each
	// base must be structurally valid for the rejection to be attributable to that field.
	tests := []struct {
		name     string
		command  CommandDTO
		outcome  ApplyOutcome
		conflict ApplyConflictCode
	}{
		{name: "stage join", command: cmaStageJoinCommand(nil), outcome: ApplyOutcomeApplied},
		{name: "begin remove", command: cmaMembershipCommand(MembershipActionBeginRemove, 3, cmaMemberPointer(3, MemberPhaseLeaving, testOperation4), nil), outcome: ApplyOutcomeApplied},
		{name: "remove member", command: cmaMembershipCommand(MembershipActionRemoveMember, 3, cmaMemberPointer(3, MemberPhaseRetired, ""), nil), outcome: ApplyOutcomeConflict, conflict: ConflictConfigurationMismatch},
		{name: "add learner", command: cmaMembershipCommand(MembershipActionAddLearner, 0, cmaMemberPointer(4, MemberPhaseCatchingUp, testOperation4), nil), outcome: ApplyOutcomeConflict, conflict: ConflictConfigurationMismatch},
		{name: "promote voter", command: cmaMembershipCommand(MembershipActionPromoteVoter, 0, cmaMemberPointer(4, MemberPhaseVoter, ""), nil), outcome: ApplyOutcomeConflict, conflict: ConflictConfigurationMismatch},
		{name: "abort stage", command: cmaMembershipCommand(MembershipActionAbortStage, 0, cmaMemberPointer(4, MemberPhaseRetired, ""), nil), outcome: ApplyOutcomeConflict, conflict: ConflictConfigurationMismatch},
		{name: "ownership", command: cmaWithOwnership(func(*OwnershipChangeDTO) {}), outcome: ApplyOutcomeApplied},
		{name: "subscription", command: cmaWithSubscription(func(*SubscriptionChangeDTO) {}), outcome: ApplyOutcomeApplied},
		{name: "delivery receive", command: cmaWithDelivery(func(*DeliveryChangeDTO) {}), outcome: ApplyOutcomeConflict, conflict: ConflictDeliverySessionMissing},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			machine := cmaSessionMachine(t)
			result, err := machine.Apply(3, test.command)
			if err != nil || result.Outcome != test.outcome || result.Conflict != test.conflict {
				t.Fatalf("Apply = (%+v, %v), want outcome %d conflict %q", result, err, test.outcome, test.conflict)
			}
		})
	}
}

func TestCMAApplyRejectsMalformedCommandsWithoutAdvancing(t *testing.T) {
	uppercaseOperation := cmaWithOwnership(func(*OwnershipChangeDTO) {})
	uppercaseOperation.OperationID = "00000000-0000-4000-8000-00000000000A"
	misplacedSeparator := cmaWithOwnership(func(*OwnershipChangeDTO) {})
	misplacedSeparator.OperationID = "000000000-000-4000-8000-000000000004"
	nonHexOperation := cmaWithOwnership(func(*OwnershipChangeDTO) {})
	nonHexOperation.OperationID = "00000000-0000-4000-8000-00000000000g"
	schemaMajor := cmaStageJoinCommand(nil)
	schemaMajor.SchemaMajor = 2
	schemaMinor := cmaStageJoinCommand(nil)
	schemaMinor.SchemaMinor = 1
	twoPayloads := cmaWithOwnership(func(*OwnershipChangeDTO) {})
	twoPayloads.Subscription = cmaWithSubscription(func(*SubscriptionChangeDTO) {}).Subscription

	beginRemove := func(mutate func(*MembershipRecordDTO)) CommandDTO {
		return cmaMembershipCommand(MembershipActionBeginRemove, 3, cmaMemberPointer(3, MemberPhaseLeaving, testOperation4), mutate)
	}
	removeMember := func(mutate func(*MembershipRecordDTO)) CommandDTO {
		return cmaMembershipCommand(MembershipActionRemoveMember, 3, cmaMemberPointer(3, MemberPhaseRetired, ""), mutate)
	}
	addLearner := func(mutate func(*MembershipRecordDTO)) CommandDTO {
		return cmaMembershipCommand(MembershipActionAddLearner, 0, cmaMemberPointer(4, MemberPhaseCatchingUp, testOperation4), mutate)
	}
	promoteVoter := func(mutate func(*MembershipRecordDTO)) CommandDTO {
		return cmaMembershipCommand(MembershipActionPromoteVoter, 0, cmaMemberPointer(4, MemberPhaseVoter, ""), mutate)
	}
	abortStage := func(mutate func(*MembershipRecordDTO)) CommandDTO {
		return cmaMembershipCommand(MembershipActionAbortStage, 0, cmaMemberPointer(4, MemberPhaseRetired, ""), mutate)
	}

	tests := []struct {
		name    string
		command CommandDTO
	}{
		{name: "unsupported schema major", command: schemaMajor},
		{name: "unsupported schema minor", command: schemaMinor},
		{name: "empty discriminated union", command: CommandDTO{SchemaMajor: SchemaMajor, OperationID: testOperation4}},
		{name: "two payloads", command: twoPayloads},
		{name: "uppercase operation ID", command: uppercaseOperation},
		{name: "operation ID separator misplaced", command: misplacedSeparator},
		{name: "operation ID non-hex", command: nonHexOperation},
		{name: "record operation ID differs from command", command: cmaStageJoinCommand(func(record *MembershipRecordDTO) {
			record.OperationID = testOperation5
			record.Member.OperationID = testOperation5
		})},

		{name: "unspecified membership action", command: cmaStageJoinCommand(func(record *MembershipRecordDTO) { record.Action = MembershipActionUnspecified })},
		{name: "membership action beyond abort", command: cmaStageJoinCommand(func(record *MembershipRecordDTO) { record.Action = MembershipActionAbortStage + 1 })},
		{name: "empty idempotency key", command: cmaStageJoinCommand(func(record *MembershipRecordDTO) { record.IdempotencyKey = "" })},
		{name: "idempotency key above 128 bytes", command: cmaStageJoinCommand(func(record *MembershipRecordDTO) { record.IdempotencyKey = strings.Repeat("k", 129) })},
		{name: "idempotency key with space", command: cmaStageJoinCommand(func(record *MembershipRecordDTO) { record.IdempotencyKey = "join 4" })},
		{name: "idempotency key with DEL", command: cmaStageJoinCommand(func(record *MembershipRecordDTO) { record.IdempotencyKey = "join\x7f" })},
		{name: "idempotency key with non-ASCII", command: cmaStageJoinCommand(func(record *MembershipRecordDTO) { record.IdempotencyKey = "join-\u00fc" })},

		{name: "stage join with target member", command: cmaStageJoinCommand(func(record *MembershipRecordDTO) { record.TargetMemberID = 4 })},
		{name: "stage join without member", command: cmaStageJoinCommand(func(record *MembershipRecordDTO) { record.Member = nil })},
		{name: "stage join learner phase", command: cmaStageJoinCommand(func(record *MembershipRecordDTO) { record.Member.Phase = MemberPhaseLearner })},
		{name: "stage join member operation differs", command: cmaStageJoinCommand(func(record *MembershipRecordDTO) { record.Member.OperationID = testOperation5 })},
		{name: "begin remove without target", command: beginRemove(func(record *MembershipRecordDTO) { record.TargetMemberID = 0 })},
		{name: "begin remove without member", command: beginRemove(func(record *MembershipRecordDTO) { record.Member = nil })},
		{name: "begin remove member differs from target", command: beginRemove(func(record *MembershipRecordDTO) { record.TargetMemberID = 2 })},
		{name: "begin remove voter phase", command: beginRemove(func(record *MembershipRecordDTO) { record.Member = cmaMemberPointer(3, MemberPhaseVoter, "") })},
		{name: "begin remove member operation differs", command: beginRemove(func(record *MembershipRecordDTO) { record.Member.OperationID = testOperation5 })},
		{name: "remove member without target", command: removeMember(func(record *MembershipRecordDTO) { record.TargetMemberID = 0 })},
		{name: "remove member without member", command: removeMember(func(record *MembershipRecordDTO) { record.Member = nil })},
		{name: "remove member differs from target", command: removeMember(func(record *MembershipRecordDTO) { record.TargetMemberID = 2 })},
		{name: "remove member leaving phase", command: removeMember(func(record *MembershipRecordDTO) {
			record.Member = cmaMemberPointer(3, MemberPhaseLeaving, testOperation4)
		})},
		{name: "add learner with target", command: addLearner(func(record *MembershipRecordDTO) { record.TargetMemberID = 4 })},
		{name: "add learner without member", command: addLearner(func(record *MembershipRecordDTO) { record.Member = nil })},
		{name: "add learner staged phase", command: addLearner(func(record *MembershipRecordDTO) { record.Member.Phase = MemberPhaseStaged })},
		{name: "add learner member operation differs", command: addLearner(func(record *MembershipRecordDTO) { record.Member.OperationID = testOperation5 })},
		{name: "promote voter with target", command: promoteVoter(func(record *MembershipRecordDTO) { record.TargetMemberID = 4 })},
		{name: "promote voter without member", command: promoteVoter(func(record *MembershipRecordDTO) { record.Member = nil })},
		{name: "promote voter learner phase", command: promoteVoter(func(record *MembershipRecordDTO) {
			record.Member = cmaMemberPointer(4, MemberPhaseLearner, testOperation4)
		})},
		{name: "voter member with operation ID", command: promoteVoter(func(record *MembershipRecordDTO) { record.Member.OperationID = testOperation4 })},
		{name: "abort stage without member", command: abortStage(func(record *MembershipRecordDTO) { record.Member = nil })},
		{name: "abort stage staged phase", command: abortStage(func(record *MembershipRecordDTO) {
			record.Member = cmaMemberPointer(4, MemberPhaseStaged, testOperation4)
		})},

		{name: "zero member ID", command: cmaWithMember(func(member *MemberIdentityDTO) { member.MemberID = 0 })},
		{name: "empty cluster ID", command: cmaWithMember(func(member *MemberIdentityDTO) { member.ClusterID = "" })},
		{name: "cluster ID above 200 bytes", command: cmaWithMember(func(member *MemberIdentityDTO) { member.ClusterID = strings.Repeat("c", maxClusterIDBytes+1) })},
		{name: "cluster ID invalid UTF-8", command: cmaWithMember(func(member *MemberIdentityDTO) { member.ClusterID = "\xff" })},
		{name: "empty address", command: cmaWithMember(func(member *MemberIdentityDTO) { member.Address = "" })},
		{name: "address above 255 bytes", command: cmaWithMember(func(member *MemberIdentityDTO) { member.Address = cmaLongDNSName(61) + ":19" })},
		{name: "address without port", command: cmaWithMember(func(member *MemberIdentityDTO) { member.Address = "node4.example" })},
		{name: "address empty port", command: cmaWithMember(func(member *MemberIdentityDTO) { member.Address = "node4.example:" })},
		{name: "address empty host", command: cmaWithMember(func(member *MemberIdentityDTO) { member.Address = ":19004" })},
		{name: "address wildcard host", command: cmaWithMember(func(member *MemberIdentityDTO) { member.Address = "*:19004" })},
		{name: "address zero port", command: cmaWithMember(func(member *MemberIdentityDTO) { member.Address = "node4.example:0" })},
		{name: "address port above 65535", command: cmaWithMember(func(member *MemberIdentityDTO) { member.Address = "node4.example:65536" })},
		{name: "address named port", command: cmaWithMember(func(member *MemberIdentityDTO) { member.Address = "node4.example:http" })},
		{name: "address unspecified IPv4", command: cmaWithMember(func(member *MemberIdentityDTO) { member.Address = "0.0.0.0:19004" })},
		{name: "address unspecified IPv6", command: cmaWithMember(func(member *MemberIdentityDTO) { member.Address = "[::]:19004" })},
		{name: "address host with underscore", command: cmaWithMember(func(member *MemberIdentityDTO) { member.Address = "node_4.example:19004" })},
		{name: "empty DNS SAN", command: cmaWithMember(func(member *MemberIdentityDTO) { member.DNSSAN = "" })},
		{name: "IP DNS SAN", command: cmaWithMember(func(member *MemberIdentityDTO) { member.DNSSAN = "10.0.0.4" })},
		{name: "DNS SAN above 253 bytes", command: cmaWithMember(func(member *MemberIdentityDTO) { member.DNSSAN = cmaLongDNSName(62) })},
		{name: "DNS SAN empty label", command: cmaWithMember(func(member *MemberIdentityDTO) { member.DNSSAN = "node4..example" })},
		{name: "DNS SAN leading hyphen", command: cmaWithMember(func(member *MemberIdentityDTO) { member.DNSSAN = "-node4.example" })},
		{name: "DNS SAN trailing hyphen", command: cmaWithMember(func(member *MemberIdentityDTO) { member.DNSSAN = "node4-.example" })},
		{name: "DNS SAN label above 63 bytes", command: cmaWithMember(func(member *MemberIdentityDTO) { member.DNSSAN = strings.Repeat("a", 64) + ".example" })},
		{name: "DNS SAN with underscore", command: cmaWithMember(func(member *MemberIdentityDTO) { member.DNSSAN = "node_4.example" })},
		{name: "certificate digest 31 bytes", command: cmaWithMember(func(member *MemberIdentityDTO) { member.CertificateSHA256 = member.CertificateSHA256[:31] })},
		{name: "empty release version", command: cmaWithMember(func(member *MemberIdentityDTO) { member.ReleaseVersion = "" })},
		{name: "release version above 64 bytes", command: cmaWithMember(func(member *MemberIdentityDTO) {
			member.ReleaseVersion = strings.Repeat("r", maxReleaseVersionBytes+1)
		})},
		{name: "cluster protocol major 3", command: cmaWithMember(func(member *MemberIdentityDTO) { member.ClusterProtocolMajor = 3 })},
		{name: "cluster protocol minor 1", command: cmaWithMember(func(member *MemberIdentityDTO) { member.ClusterProtocolMinor = 1 })},
		{name: "state schema major 2", command: cmaWithMember(func(member *MemberIdentityDTO) { member.StateSchemaMajor = 2 })},
		{name: "state schema minor 1", command: cmaWithMember(func(member *MemberIdentityDTO) { member.StateSchemaMinor = 1 })},
		{name: "unspecified member phase", command: cmaWithMember(func(member *MemberIdentityDTO) { member.Phase = MemberPhaseUnspecified })},
		{name: "member phase beyond retired", command: cmaWithMember(func(member *MemberIdentityDTO) { member.Phase = MemberPhaseRetired + 1 })},
		{name: "staged member without operation ID", command: cmaWithMember(func(member *MemberIdentityDTO) { member.OperationID = "" })},

		{name: "ownership empty tenant", command: cmaWithOwnership(func(change *OwnershipChangeDTO) { change.TenantID = "" })},
		{name: "ownership tenant above 128 bytes", command: cmaWithOwnership(func(change *OwnershipChangeDTO) { change.TenantID = strings.Repeat("t", maxTenantIDBytes+1) })},
		{name: "ownership tenant with NUL", command: cmaWithOwnership(func(change *OwnershipChangeDTO) { change.TenantID = "ten\x00ant" })},
		{name: "ownership empty client", command: cmaWithOwnership(func(change *OwnershipChangeDTO) { change.ClientID = "" })},
		{name: "ownership client above 256 bytes", command: cmaWithOwnership(func(change *OwnershipChangeDTO) { change.ClientID = strings.Repeat("c", maxClientIDBytes+1) })},
		{name: "ownership client with NUL", command: cmaWithOwnership(func(change *OwnershipChangeDTO) { change.ClientID = "cli\x00ent" })},
		{name: "ownership client invalid UTF-8", command: cmaWithOwnership(func(change *OwnershipChangeDTO) { change.ClientID = "\xff" })},
		{name: "ownership zero owner", command: cmaWithOwnership(func(change *OwnershipChangeDTO) { change.OwnerNodeID = 0 })},
		{name: "ownership zero epoch", command: cmaWithOwnership(func(change *OwnershipChangeDTO) { change.Epoch = 0 })},
		{name: "ownership unspecified action", command: cmaWithOwnership(func(change *OwnershipChangeDTO) { change.Action = OwnershipActionUnspecified })},
		{name: "ownership action beyond release", command: cmaWithOwnership(func(change *OwnershipChangeDTO) { change.Action = OwnershipActionRelease + 1 })},

		{name: "subscription invalid session", command: cmaWithSubscription(func(change *SubscriptionChangeDTO) { change.TenantID = "" })},
		{name: "subscription zero owner", command: cmaWithSubscription(func(change *SubscriptionChangeDTO) { change.OwnerNodeID = 0 })},
		{name: "subscription zero epoch", command: cmaWithSubscription(func(change *SubscriptionChangeDTO) { change.Epoch = 0 })},
		{name: "subscription empty filter", command: cmaWithSubscription(func(change *SubscriptionChangeDTO) { change.Filter = "" })},
		{name: "subscription misplaced multi-level wildcard", command: cmaWithSubscription(func(change *SubscriptionChangeDTO) { change.Filter = "orders/#/new" })},
		{name: "subscription QoS 3", command: cmaWithSubscription(func(change *SubscriptionChangeDTO) { change.QoS = 3 })},
		{name: "subscription retain handling 3", command: cmaWithSubscription(func(change *SubscriptionChangeDTO) { change.RetainHandling = 3 })},
		{name: "subscription unspecified action", command: cmaWithSubscription(func(change *SubscriptionChangeDTO) { change.Action = SubscriptionActionUnspecified })},
		{name: "subscription action beyond remove", command: cmaWithSubscription(func(change *SubscriptionChangeDTO) { change.Action = SubscriptionActionRemove + 1 })},

		{name: "delivery empty tenant", command: cmaWithDelivery(func(change *DeliveryChangeDTO) { change.TenantID = "" })},
		{name: "delivery empty client", command: cmaWithDelivery(func(change *DeliveryChangeDTO) { change.ClientID = "" })},
		{name: "delivery zero owner", command: cmaWithDelivery(func(change *DeliveryChangeDTO) { change.OwnerNodeID = 0 })},
		{name: "delivery zero epoch", command: cmaWithDelivery(func(change *DeliveryChangeDTO) { change.Epoch = 0 })},
		{name: "delivery zero proposed time", command: cmaWithDelivery(func(change *DeliveryChangeDTO) { change.ProposedAtUnixMillis = 0 })},
		{name: "delivery packet ID above MQTT range", command: cmaWithDelivery(func(change *DeliveryChangeDTO) { change.PacketID = 65536 })},
		{name: "delivery invalid topic", command: cmaWithDelivery(func(change *DeliveryChangeDTO) { change.Topic = "orders/+" })},
		{name: "delivery payload above limit", command: cmaWithDelivery(func(change *DeliveryChangeDTO) { change.Payload = make([]byte, MaxDeliveryPayloadBytes+1) })},
		{name: "delivery action beyond discard", command: cmaWithDelivery(func(change *DeliveryChangeDTO) { change.Action = DeliveryActionDiscard + 1 })},
		{name: "delivery target duplicate", command: cmaDeliveryCommand(DeliveryChangeDTO{
			TenantID: cmaTenantID, ClientID: cmaClientID, Action: DeliveryActionPublish, ProposedAtUnixMillis: 1, Topic: "orders/new", PublishQoS: 1,
			Targets: []DeliveryTargetDTO{{ClientID: "a"}, {ClientID: "a"}},
		})},
		{name: "delivery publish with owner", command: cmaDeliveryCommand(DeliveryChangeDTO{
			TenantID: cmaTenantID, ClientID: cmaClientID, OwnerNodeID: 1, Epoch: 1, Action: DeliveryActionPublish,
			ProposedAtUnixMillis: 1, Topic: "orders/new", PublishQoS: 1,
		})},
		{name: "delivery receive with zero packet ID", command: cmaWithDelivery(func(change *DeliveryChangeDTO) { change.PacketID = 0 })},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			machine := cmaSessionMachine(t)
			before, err := machine.Snapshot(2)
			if err != nil {
				t.Fatalf("snapshot before: %v", err)
			}
			if _, err := machine.Apply(3, test.command); !errors.Is(err, ErrInvalidCommand) {
				t.Fatalf("Apply error = %v, want ErrInvalidCommand", err)
			}
			after, err := machine.Snapshot(2)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("rejected command changed state or index: err=%v last applied=%d", err, machine.LastAppliedIndex())
			}
		})
	}
}

func TestCMAApplyAcceptsInclusiveFieldBoundaries(t *testing.T) {
	memberTests := []struct {
		name   string
		mutate func(*MembershipRecordDTO)
	}{
		{name: "cluster ID of 200 bytes", mutate: func(record *MembershipRecordDTO) { record.Member.ClusterID = strings.Repeat("c", maxClusterIDBytes) }},
		{name: "release version of 64 bytes", mutate: func(record *MembershipRecordDTO) {
			record.Member.ReleaseVersion = strings.Repeat("r", maxReleaseVersionBytes)
		}},
		{name: "IPv6 address with maximum port", mutate: func(record *MembershipRecordDTO) { record.Member.Address = "[2001:db8::4]:65535" }},
		{name: "address of 255 bytes", mutate: func(record *MembershipRecordDTO) { record.Member.Address = cmaLongDNSName(61) + ":1" }},
		{name: "DNS SAN of 253 bytes", mutate: func(record *MembershipRecordDTO) { record.Member.DNSSAN = cmaLongDNSName(61) }},
		{name: "idempotency key of 128 printable bytes", mutate: func(record *MembershipRecordDTO) {
			record.IdempotencyKey = "!" + strings.Repeat("~", 127)
		}},
	}
	for _, test := range memberTests {
		t.Run(test.name, func(t *testing.T) {
			machine := NewStateMachine()
			command := cmaStageJoinCommand(test.mutate)
			result := applyMustSucceed(t, machine, 1, command)
			stored, exists := machine.GetOperationByIdempotencyKey(command.Membership.IdempotencyKey)
			if !exists || !reflect.DeepEqual(stored, *result.Operation) || !sameMemberIdentity(*stored.Record.Member, *command.Membership.Member) {
				t.Fatalf("boundary stage record was not committed verbatim: stored=%+v exists=%t", stored, exists)
			}
		})
	}

	t.Run("session identity at maximum lengths", func(t *testing.T) {
		machine := NewStateMachine()
		tenantID := strings.Repeat("t", maxTenantIDBytes)
		clientID := strings.Repeat("c", maxClientIDBytes)
		applyMustSucceed(t, machine, 1, cmaOwnershipCommand(testOperation4, OwnershipChangeDTO{TenantID: tenantID, ClientID: clientID, OwnerNodeID: 1, Epoch: 1, Action: OwnershipActionAcquire}))
		if ownership, exists := machine.GetOwnership(tenantID, clientID); !exists || ownership.Epoch != 1 {
			t.Fatalf("maximum-length session ownership = (%+v, %t)", ownership, exists)
		}
	})

	t.Run("delivery receive at maximum payload and packet ID", func(t *testing.T) {
		machine := cmaSessionMachine(t)
		applyMustSucceed(t, machine, 3, cmaDeliveryCommand(DeliveryChangeDTO{
			TenantID: cmaTenantID, ClientID: cmaClientID, OwnerNodeID: 1, Epoch: 1,
			Action: DeliveryActionAttach, ProposedAtUnixMillis: 1,
		}))
		command := cmaDeliveryCommand(DeliveryChangeDTO{
			TenantID: cmaTenantID, ClientID: cmaClientID, OwnerNodeID: 1, Epoch: 1,
			Action: DeliveryActionReceive, ProposedAtUnixMillis: 2, PacketID: 65535,
			Topic: "orders/new", Payload: make([]byte, MaxDeliveryPayloadBytes),
		})
		applyMustSucceed(t, machine, 4, command)
		stored, exists := machine.DeliverySession(cmaTenantID, cmaClientID)
		if !exists || len(stored.Inbound) != 1 || stored.Inbound[0].PacketID != 65535 || len(stored.Inbound[0].Payload) != MaxDeliveryPayloadBytes {
			t.Fatalf("boundary delivery receive was not stored: %+v", stored)
		}
	})
}

func TestCMAApplyConfigurationRejectsInvalidInputsAndMisalignedPhases(t *testing.T) {
	machine := NewStateMachine()
	stage := stageRecord(testOperation4, "join-4", 4)
	applyMustSucceed(t, machine, 1, testCommand(testOperation4, &stage))

	invalid := []struct {
		name        string
		index       uint64
		operationID string
		action      ConfigurationAction
		memberID    uint64
		want        error
	}{
		{name: "zero index", index: 0, operationID: testOperation4, action: ConfigurationActionAddLearner, memberID: 4, want: ErrInvalidIndex},
		{name: "already applied index", index: 1, operationID: testOperation4, action: ConfigurationActionAddLearner, memberID: 4, want: ErrInvalidIndex},
		{name: "noncanonical operation ID", index: 2, operationID: "join-4", action: ConfigurationActionAddLearner, memberID: 4, want: ErrInvalidCommand},
		{name: "zero member ID", index: 2, operationID: testOperation4, action: ConfigurationActionAddLearner, memberID: 0, want: ErrInvalidCommand},
		{name: "unspecified action", index: 2, operationID: testOperation4, action: ConfigurationActionUnspecified, memberID: 4, want: ErrInvalidCommand},
		{name: "action beyond remove", index: 2, operationID: testOperation4, action: ConfigurationActionRemoveMember + 1, memberID: 4, want: ErrInvalidCommand},
	}
	for _, test := range invalid {
		if _, err := machine.ApplyConfiguration(test.index, test.operationID, test.action, test.memberID); !errors.Is(err, test.want) {
			t.Fatalf("%s: error=%v, want %v", test.name, err, test.want)
		}
	}
	if machine.LastAppliedIndex() != 1 || cmaMemberByID(t, machine, 4).Phase != MemberPhaseStaged {
		t.Fatalf("rejected configuration changed state: index=%d members=%+v", machine.LastAppliedIndex(), machine.Members())
	}

	cmaConfigurationConflict(t, machine, 2, testOperation5, ConfigurationActionAddLearner, 4, ConflictOperationNotFound)
	cmaApplyConfiguration(t, machine, 3, testOperation4, ConfigurationActionAddLearner, 4)
	cmaConfigurationConflict(t, machine, 4, testOperation4, ConfigurationActionAddLearner, 4, ConflictConfigurationMismatch)
	cmaConfigurationConflict(t, machine, 5, testOperation4, ConfigurationActionRemoveMember, 4, ConflictConfigurationMismatch)
	if member := cmaMemberByID(t, machine, 4); member.Phase != MemberPhaseLearner || member.OperationID != testOperation4 {
		t.Fatalf("misaligned configuration changed learner: %+v", member)
	}

	catchingUp := MembershipRecordDTO{OperationID: testOperation4, Action: MembershipActionAddLearner, IdempotencyKey: "join-4", Member: cmaMemberPointer(4, MemberPhaseCatchingUp, testOperation4)}
	applyMustSucceed(t, machine, 6, testCommand(testOperation4, &catchingUp))
	promoted := cmaApplyConfiguration(t, machine, 7, testOperation4, ConfigurationActionPromoteVoter, 4)
	cmaConfigurationConflict(t, machine, 8, testOperation4, ConfigurationActionPromoteVoter, 4, ConflictConfigurationMismatch)
	operation, exists := machine.GetOperation(testOperation4)
	if !exists || !reflect.DeepEqual(operation, *promoted.Operation) || operation.CommittedIndex != 7 {
		t.Fatalf("duplicate promotion rewrote the succeeded operation: %+v exists=%t", operation, exists)
	}
}

func TestCMAApplyRejectsConfigurationOnlyMembershipRecords(t *testing.T) {
	machine := NewStateMachine()
	if err := machine.InitializeBootstrap(1, testVoters()); err != nil {
		t.Fatalf("initialize bootstrap: %v", err)
	}
	promote := cmaMembershipCommand(MembershipActionPromoteVoter, 0, cmaMemberPointer(4, MemberPhaseVoter, ""), nil)
	if result := mustApplyResult(t, machine, 2, promote); result.Conflict != ConflictConfigurationMismatch {
		t.Fatalf("promotion record without ConfChange conflict=%q", result.Conflict)
	}
	remove := cmaMembershipCommand(MembershipActionRemoveMember, 3, cmaMemberPointer(3, MemberPhaseRetired, ""), nil)
	if result := mustApplyResult(t, machine, 3, remove); result.Conflict != ConflictConfigurationMismatch {
		t.Fatalf("removal record without ConfChange conflict=%q", result.Conflict)
	}
	if !reflect.DeepEqual(machine.Members(), testVoters()) || len(machine.Operations()) != 0 || machine.LastAppliedIndex() != 3 {
		t.Fatalf("configuration-only records changed membership: members=%+v operations=%+v", machine.Members(), machine.Operations())
	}
}

func TestCMAStageJoinOperationIDReplayAndReuseConflicts(t *testing.T) {
	machine := NewStateMachine()
	original := stageRecord(testOperation4, "join-4", 4)
	first := applyMustSucceed(t, machine, 1, testCommand(testOperation4, &original))

	sameOperationNewKey := stageRecord(testOperation4, "join-4-retry", 4)
	replayed := applyMustSucceed(t, machine, 2, testCommand(testOperation4, &sameOperationNewKey))
	if !reflect.DeepEqual(*replayed.Operation, *first.Operation) || replayed.Index != 2 {
		t.Fatalf("same operation ID retry = %+v, want original operation %+v at index 2", replayed, first.Operation)
	}

	changedIdentity := stageRecord(testOperation4, "join-4-changed", 4)
	changedIdentity.Member.Address = "other.example:19004"
	if result := mustApplyResult(t, machine, 3, testCommand(testOperation4, &changedIdentity)); result.Conflict != ConflictOperationID {
		t.Fatalf("operation ID reuse with changed identity conflict=%q", result.Conflict)
	}
	otherMember := stageRecord(testOperation4, "join-5", 5)
	otherMember.Member.OperationID = testOperation4
	if result := mustApplyResult(t, machine, 4, testCommand(testOperation4, &otherMember)); result.Conflict != ConflictOperationID {
		t.Fatalf("operation ID reuse for another member conflict=%q", result.Conflict)
	}
	if members := machine.Members(); len(members) != 1 || members[0].Address != original.Member.Address {
		t.Fatalf("operation ID conflicts changed members: %+v", members)
	}
}

func TestCMAJoinCannotReuseRemovalKeyOrOperationID(t *testing.T) {
	machine := NewStateMachine()
	if err := machine.InitializeBootstrap(1, testVoters()); err != nil {
		t.Fatalf("initialize bootstrap: %v", err)
	}
	remove := MembershipRecordDTO{OperationID: testOperation6, Action: MembershipActionBeginRemove, IdempotencyKey: "remove-3", Member: cmaMemberPointer(3, MemberPhaseLeaving, testOperation6), TargetMemberID: 3}
	applyMustSucceed(t, machine, 2, testCommand(testOperation6, &remove))

	joinWithRemovalKey := stageRecord(testOperation4, "remove-3", 4)
	if result := mustApplyResult(t, machine, 3, testCommand(testOperation4, &joinWithRemovalKey)); result.Conflict != ConflictIdempotency {
		t.Fatalf("join with removal idempotency key conflict=%q", result.Conflict)
	}
	joinWithRemovalOperation := stageRecord(testOperation6, "join-4", 4)
	if result := mustApplyResult(t, machine, 4, testCommand(testOperation6, &joinWithRemovalOperation)); result.Conflict != ConflictOperationID {
		t.Fatalf("join with removal operation ID conflict=%q", result.Conflict)
	}
	if len(machine.Members()) != 3 || len(machine.Operations()) != 1 {
		t.Fatalf("rejected joins changed state: members=%+v operations=%+v", machine.Members(), machine.Operations())
	}
}

func TestCMABeginRemovePreconditionsAndIdempotentReplay(t *testing.T) {
	machine := NewStateMachine()
	if err := machine.InitializeBootstrap(1, testVoters()); err != nil {
		t.Fatalf("initialize bootstrap: %v", err)
	}
	removal := func(operationID, key string, memberID uint64) CommandDTO {
		record := MembershipRecordDTO{OperationID: operationID, Action: MembershipActionBeginRemove, IdempotencyKey: key, Member: cmaMemberPointer(memberID, MemberPhaseLeaving, operationID), TargetMemberID: memberID}
		return testCommand(operationID, &record)
	}

	if result := mustApplyResult(t, machine, 2, removal(testOperation6, "remove-9", 9)); result.Conflict != ConflictMemberNotFound {
		t.Fatalf("unknown removal target conflict=%q", result.Conflict)
	}
	wrongIdentity := removal(testOperation6, "remove-3", 3)
	wrongIdentity.Membership.Member.Address = "other.example:19003"
	if result := mustApplyResult(t, machine, 3, wrongIdentity); result.Conflict != ConflictMemberIdentity {
		t.Fatalf("mismatched removal identity conflict=%q", result.Conflict)
	}

	stage := stageRecord(testOperation4, "join-4", 4)
	applyMustSucceed(t, machine, 4, testCommand(testOperation4, &stage))
	if result := mustApplyResult(t, machine, 5, removal(testOperation6, "remove-3", 3)); result.Conflict != ConflictMembershipInProgress {
		t.Fatalf("removal during join conflict=%q", result.Conflict)
	}
	if result := mustApplyResult(t, machine, 6, removal(testOperation7, "remove-4", 4)); result.Conflict != ConflictPhase {
		t.Fatalf("removal of staged member conflict=%q", result.Conflict)
	}
	if cmaMemberByID(t, machine, 3).Phase != MemberPhaseVoter || cmaMemberByID(t, machine, 4).Phase != MemberPhaseStaged {
		t.Fatalf("rejected removals changed members: %+v", machine.Members())
	}
	abort := MembershipRecordDTO{OperationID: testOperation4, Action: MembershipActionAbortStage, IdempotencyKey: "join-4", Member: cmaMemberPointer(4, MemberPhaseRetired, "")}
	applyMustSucceed(t, machine, 7, testCommand(testOperation4, &abort))

	started := applyMustSucceed(t, machine, 8, removal(testOperation6, "remove-3", 3))
	sameKey := applyMustSucceed(t, machine, 9, removal(testOperation7, "remove-3", 3))
	sameOperation := applyMustSucceed(t, machine, 10, removal(testOperation6, "remove-3-retry", 3))
	for name, replay := range map[string]ApplyResultDTO{"same key": sameKey, "same operation ID": sameOperation} {
		if !reflect.DeepEqual(*replay.Operation, *started.Operation) || replay.Operation.CommittedIndex != 8 {
			t.Fatalf("%s removal retry = %+v, want original %+v", name, replay.Operation, started.Operation)
		}
	}
	if result := mustApplyResult(t, machine, 11, removal(testOperation7, "remove-3", 2)); result.Conflict != ConflictIdempotency {
		t.Fatalf("removal key reuse for another member conflict=%q", result.Conflict)
	}
	if result := mustApplyResult(t, machine, 12, removal(testOperation6, "remove-2", 2)); result.Conflict != ConflictOperationID {
		t.Fatalf("removal operation ID reuse for another member conflict=%q", result.Conflict)
	}
	if cmaMemberByID(t, machine, 2).Phase != MemberPhaseVoter || len(machine.Operations()) != 2 {
		t.Fatalf("removal conflicts changed state: members=%+v operations=%+v", machine.Members(), machine.Operations())
	}

	cmaApplyConfiguration(t, machine, 13, testOperation6, ConfigurationActionRemoveMember, 3)
	if result := mustApplyResult(t, machine, 14, removal(testOperation8, "remove-3-again", 3)); result.Conflict != ConflictPhase {
		t.Fatalf("removal of retired member conflict=%q", result.Conflict)
	}
	if member := cmaMemberByID(t, machine, 3); member.Phase != MemberPhaseRetired || member.OperationID != "" {
		t.Fatalf("retired member changed after rejected removal: %+v", member)
	}
}

func TestCMAMarkCatchingUpRequiresCommittedLearner(t *testing.T) {
	machine := NewStateMachine()
	stage := stageRecord(testOperation4, "join-4", 4)
	applyMustSucceed(t, machine, 1, testCommand(testOperation4, &stage))
	catchingUp := func(operationID, key string, member *MemberIdentityDTO) CommandDTO {
		record := MembershipRecordDTO{OperationID: operationID, Action: MembershipActionAddLearner, IdempotencyKey: key, Member: member}
		return testCommand(operationID, &record)
	}

	if result := mustApplyResult(t, machine, 2, catchingUp(testOperation4, "join-4", cmaMemberPointer(4, MemberPhaseCatchingUp, testOperation4))); result.Conflict != ConflictConfigurationMismatch {
		t.Fatalf("catch-up before learner ConfChange conflict=%q", result.Conflict)
	}
	if cmaMemberByID(t, machine, 4).Phase != MemberPhaseStaged {
		t.Fatal("catch-up before learner ConfChange changed staged member")
	}
	cmaApplyConfiguration(t, machine, 3, testOperation4, ConfigurationActionAddLearner, 4)

	changedAddress := cmaMemberPointer(4, MemberPhaseCatchingUp, testOperation4)
	changedAddress.Address = "other.example:19004"
	conflicts := []struct {
		name    string
		command CommandDTO
		want    ApplyConflictCode
	}{
		{name: "unknown operation", command: catchingUp(testOperation5, "join-4", cmaMemberPointer(4, MemberPhaseCatchingUp, testOperation5)), want: ConflictConfigurationMismatch},
		{name: "different idempotency key", command: catchingUp(testOperation4, "join-4-other", cmaMemberPointer(4, MemberPhaseCatchingUp, testOperation4)), want: ConflictConfigurationMismatch},
		{name: "changed identity", command: catchingUp(testOperation4, "join-4", changedAddress), want: ConflictMemberIdentity},
		{name: "learner phase instead of catching up", command: catchingUp(testOperation4, "join-4", cmaMemberPointer(4, MemberPhaseLearner, testOperation4)), want: ConflictPhase},
	}
	index := uint64(3)
	for _, test := range conflicts {
		index++
		if result := mustApplyResult(t, machine, index, test.command); result.Conflict != test.want {
			t.Fatalf("%s conflict=%q, want %q", test.name, result.Conflict, test.want)
		}
	}
	if member := cmaMemberByID(t, machine, 4); member.Phase != MemberPhaseLearner || member.Address != testMember(4, 0, "").Address {
		t.Fatalf("rejected catch-up changed learner: %+v", member)
	}

	index++
	caughtUp := applyMustSucceed(t, machine, index, catchingUp(testOperation4, "join-4", cmaMemberPointer(4, MemberPhaseCatchingUp, testOperation4)))
	index++
	replayed := applyMustSucceed(t, machine, index, catchingUp(testOperation4, "join-4", cmaMemberPointer(4, MemberPhaseCatchingUp, testOperation4)))
	if !reflect.DeepEqual(*replayed.Operation, *caughtUp.Operation) || replayed.Operation.Phase != OperationPhaseCatchingUp || replayed.Operation.CommittedIndex != index-1 {
		t.Fatalf("catch-up retry = %+v, want original %+v", replayed.Operation, caughtUp.Operation)
	}
}

func TestCMAAbortStageOnlyCancelsMatchingPreLearnerStage(t *testing.T) {
	machine := NewStateMachine()
	stage := stageRecord(testOperation4, "join-4", 4)
	applyMustSucceed(t, machine, 1, testCommand(testOperation4, &stage))
	abort := func(operationID, key string, member *MemberIdentityDTO) CommandDTO {
		record := MembershipRecordDTO{OperationID: operationID, Action: MembershipActionAbortStage, IdempotencyKey: key, Member: member}
		return testCommand(operationID, &record)
	}
	changedAddress := cmaMemberPointer(4, MemberPhaseRetired, "")
	changedAddress.Address = "other.example:19004"
	conflicts := []struct {
		name    string
		command CommandDTO
		want    ApplyConflictCode
	}{
		{name: "unknown operation", command: abort(testOperation5, "join-4", cmaMemberPointer(4, MemberPhaseRetired, "")), want: ConflictConfigurationMismatch},
		{name: "different idempotency key", command: abort(testOperation4, "join-4-other", cmaMemberPointer(4, MemberPhaseRetired, "")), want: ConflictConfigurationMismatch},
		{name: "other member", command: abort(testOperation4, "join-4", cmaMemberPointer(5, MemberPhaseRetired, "")), want: ConflictConfigurationMismatch},
		{name: "changed identity", command: abort(testOperation4, "join-4", changedAddress), want: ConflictMemberIdentity},
	}
	index := uint64(1)
	for _, test := range conflicts {
		index++
		if result := mustApplyResult(t, machine, index, test.command); result.Conflict != test.want {
			t.Fatalf("%s conflict=%q, want %q", test.name, result.Conflict, test.want)
		}
	}
	if operation, _ := machine.GetOperation(testOperation4); operation.Phase != OperationPhaseStaged || cmaMemberByID(t, machine, 4).Phase != MemberPhaseStaged {
		t.Fatalf("rejected abort changed stage: operation=%+v members=%+v", operation, machine.Members())
	}

	index++
	cmaApplyConfiguration(t, machine, index, testOperation4, ConfigurationActionAddLearner, 4)
	index++
	if result := mustApplyResult(t, machine, index, abort(testOperation4, "join-4", cmaMemberPointer(4, MemberPhaseRetired, ""))); result.Conflict != ConflictConfigurationMismatch {
		t.Fatalf("abort after learner ConfChange conflict=%q", result.Conflict)
	}
	if member := cmaMemberByID(t, machine, 4); member.Phase != MemberPhaseLearner || member.OperationID != testOperation4 {
		t.Fatalf("abort after learner ConfChange changed member: %+v", member)
	}
}

func TestCMAOwnershipAcquireTransferReleaseFencing(t *testing.T) {
	machine := NewStateMachine()
	index := uint64(0)
	change := func(clientID string, action OwnershipAction, ownerNodeID, expectedEpoch, epoch uint64) OwnershipChangeDTO {
		return OwnershipChangeDTO{TenantID: cmaTenantID, ClientID: clientID, ExpectedEpoch: expectedEpoch, OwnerNodeID: ownerNodeID, Epoch: epoch, Action: action}
	}
	apply := func(command CommandDTO) ApplyResultDTO {
		t.Helper()
		index++
		result, err := machine.Apply(index, command)
		if err != nil {
			t.Fatalf("Apply(%d) error: %v", index, err)
		}
		return result
	}
	expectConflict := func(name string, command CommandDTO, want ApplyConflictCode) {
		t.Helper()
		if result := apply(command); result.Outcome != ApplyOutcomeConflict || result.Conflict != want {
			t.Fatalf("%s = %+v, want conflict %q", name, result, want)
		}
	}
	ownership := func(ownershipChange OwnershipChangeDTO) CommandDTO {
		return cmaOwnershipCommand(cmaOperationID(int(index)+1), ownershipChange)
	}
	subscribe := func(clientID, filter string, ownerNodeID, epoch uint64) CommandDTO {
		return cmaSubscriptionCommand(cmaOperationID(int(index)+1), SubscriptionChangeDTO{TenantID: cmaTenantID, ClientID: clientID, OwnerNodeID: ownerNodeID, Epoch: epoch, Filter: filter, QoS: 1, Action: SubscriptionActionAdd})
	}

	expectConflict("first acquire with nonzero expected epoch", ownership(change(cmaClientID, OwnershipActionAcquire, 1, 1, 2)), ConflictStaleEpoch)
	expectConflict("first acquire skipping epoch one", ownership(change(cmaClientID, OwnershipActionAcquire, 1, 0, 2)), ConflictStaleEpoch)
	expectConflict("transfer of unowned session", ownership(change(cmaClientID, OwnershipActionTransfer, 2, 0, 1)), ConflictOwnershipMissing)
	expectConflict("release of unowned session", ownership(change(cmaClientID, OwnershipActionRelease, 1, 0, 1)), ConflictOwnershipMissing)
	if _, exists := machine.GetOwnership(cmaTenantID, cmaClientID); exists {
		t.Fatal("rejected first acquire created ownership")
	}

	apply(ownership(change(cmaClientID, OwnershipActionAcquire, 1, 0, 1)))
	apply(ownership(change("bystander", OwnershipActionAcquire, 1, 0, 1)))
	apply(subscribe(cmaClientID, "orders/#", 1, 1))
	apply(subscribe("bystander", "alerts/#", 1, 1))

	expectConflict("reacquire with stale expected epoch", ownership(change(cmaClientID, OwnershipActionAcquire, 2, 0, 1)), ConflictStaleEpoch)
	expectConflict("reacquire skipping an epoch", ownership(change(cmaClientID, OwnershipActionAcquire, 2, 1, 3)), ConflictStaleEpoch)
	expectConflict("transfer to current owner", ownership(change(cmaClientID, OwnershipActionTransfer, 1, 1, 2)), ConflictStaleEpoch)
	if current, _ := machine.GetOwnership(cmaTenantID, cmaClientID); current.OwnerNodeID != 1 || current.Epoch != 1 {
		t.Fatalf("rejected ownership changes moved owner: %+v", current)
	}

	reacquired := apply(ownership(change(cmaClientID, OwnershipActionAcquire, 2, 1, 2)))
	if reacquired.Outcome != ApplyOutcomeApplied || reacquired.Ownership == nil || *reacquired.Ownership != (OwnershipStateDTO{TenantID: cmaTenantID, ClientID: cmaClientID, OwnerNodeID: 2, Epoch: 2, CommittedIndex: index}) {
		t.Fatalf("takeover acquire = %+v", reacquired)
	}
	wantSubscriptions := []SubscriptionStateDTO{
		{TenantID: cmaTenantID, ClientID: "bystander", OwnerNodeID: 1, Epoch: 1, Filter: "alerts/#", QoS: 1},
		{TenantID: cmaTenantID, ClientID: cmaClientID, OwnerNodeID: 2, Epoch: 2, Filter: "orders/#", QoS: 1},
	}
	if got := machine.Subscriptions(); !reflect.DeepEqual(got, wantSubscriptions) {
		t.Fatalf("takeover subscriptions = %+v, want only the taken-over session rebound: %+v", got, wantSubscriptions)
	}

	expectConflict("release by fenced owner", ownership(change(cmaClientID, OwnershipActionRelease, 1, 2, 2)), ConflictStaleEpoch)
	expectConflict("release with mismatched epoch", ownership(change(cmaClientID, OwnershipActionRelease, 2, 2, 3)), ConflictStaleEpoch)
	expectConflict("release with stale expected epoch", ownership(change(cmaClientID, OwnershipActionRelease, 2, 1, 2)), ConflictStaleEpoch)
	if current, exists := machine.GetOwnership(cmaTenantID, cmaClientID); !exists || current.OwnerNodeID != 2 || current.Epoch != 2 {
		t.Fatalf("rejected release changed ownership: (%+v, %t)", current, exists)
	}

	released := apply(ownership(change(cmaClientID, OwnershipActionRelease, 2, 2, 2)))
	if released.Outcome != ApplyOutcomeApplied || released.Ownership != nil {
		t.Fatalf("release result = %+v", released)
	}
	if _, exists := machine.GetOwnership(cmaTenantID, cmaClientID); exists {
		t.Fatal("released ownership is still committed")
	}
	if bystander, exists := machine.GetOwnership(cmaTenantID, "bystander"); !exists || bystander.OwnerNodeID != 1 {
		t.Fatalf("release affected another session: (%+v, %t)", bystander, exists)
	}
	expectConflict("subscription by released owner", subscribe(cmaClientID, "orders/new", 2, 2), ConflictStaleEpoch)
}

func TestCMAOwnershipEpochDoesNotWrapAtMaximum(t *testing.T) {
	const maxEpoch = ^uint64(0)
	machine := NewStateMachine()
	exhausted := SnapshotDTO{
		SchemaMajor: SchemaMajor, SchemaMinor: SchemaMinor, LastIncludedIndex: 5, ObjectCount: 1,
		Ownerships: []OwnershipStateDTO{{TenantID: cmaTenantID, ClientID: cmaClientID, OwnerNodeID: 1, Epoch: maxEpoch, CommittedIndex: 5}},
	}
	if err := machine.Restore(exhausted); err != nil {
		t.Fatalf("restore exhausted ownership: %v", err)
	}
	for index, change := range []OwnershipChangeDTO{
		{TenantID: cmaTenantID, ClientID: cmaClientID, ExpectedEpoch: maxEpoch, OwnerNodeID: 2, Epoch: 1, Action: OwnershipActionAcquire},
		{TenantID: cmaTenantID, ClientID: cmaClientID, ExpectedEpoch: maxEpoch, OwnerNodeID: 2, Epoch: maxEpoch, Action: OwnershipActionTransfer},
	} {
		if result := mustApplyResult(t, machine, uint64(6+index), cmaOwnershipCommand(cmaOperationID(6+index), change)); result.Conflict != ConflictStaleEpoch {
			t.Fatalf("mutation %d at maximum epoch conflict=%q", index, result.Conflict)
		}
	}
	if current, _ := machine.GetOwnership(cmaTenantID, cmaClientID); current != exhausted.Ownerships[0] {
		t.Fatalf("maximum-epoch ownership changed: %+v", current)
	}

	service, port := cmaNewPortService(t, machine)
	_, err := service.AcquireOwnership(context.Background(), cmaTenantID, cmaClientID, 2)
	cmaRequireServiceError(t, err, ServiceErrorOperationConflict, false)
	if port.commitCalls != 0 {
		t.Fatalf("exhausted epoch acquisition proposed %d commands", port.commitCalls)
	}
}

// cmaRemovalInProgressMachine commits a completed join (4), an aborted stage (5), an
// in-progress removal (3) and multi-entry session state for one tenant.
func cmaRemovalInProgressMachine(t *testing.T) (*StateMachine, uint64) {
	t.Helper()
	machine := NewStateMachine()
	if err := machine.InitializeBootstrap(1, testVoters()); err != nil {
		t.Fatalf("initialize bootstrap: %v", err)
	}
	index := uint64(1)
	next := func() uint64 {
		index++
		return index
	}
	stage4 := stageRecord(testOperation4, "join-4", 4)
	applyMustSucceed(t, machine, next(), testCommand(testOperation4, &stage4))
	cmaApplyConfiguration(t, machine, next(), testOperation4, ConfigurationActionAddLearner, 4)
	catchingUp := MembershipRecordDTO{OperationID: testOperation4, Action: MembershipActionAddLearner, IdempotencyKey: "join-4", Member: cmaMemberPointer(4, MemberPhaseCatchingUp, testOperation4)}
	applyMustSucceed(t, machine, next(), testCommand(testOperation4, &catchingUp))
	cmaApplyConfiguration(t, machine, next(), testOperation4, ConfigurationActionPromoteVoter, 4)
	stage5 := stageRecord(testOperation5, "join-5", 5)
	applyMustSucceed(t, machine, next(), testCommand(testOperation5, &stage5))
	abort5 := MembershipRecordDTO{OperationID: testOperation5, Action: MembershipActionAbortStage, IdempotencyKey: "join-5", Member: cmaMemberPointer(5, MemberPhaseRetired, "")}
	applyMustSucceed(t, machine, next(), testCommand(testOperation5, &abort5))
	remove3 := MembershipRecordDTO{OperationID: testOperation6, Action: MembershipActionBeginRemove, IdempotencyKey: "remove-3", Member: cmaMemberPointer(3, MemberPhaseLeaving, testOperation6), TargetMemberID: 3}
	applyMustSucceed(t, machine, next(), testCommand(testOperation6, &remove3))
	for sequence, clientID := range []string{"c", "a", "b"} {
		applyMustSucceed(t, machine, next(), cmaOwnershipCommand(cmaOperationID(100+sequence), OwnershipChangeDTO{TenantID: cmaTenantID, ClientID: clientID, OwnerNodeID: 1, Epoch: 1, Action: OwnershipActionAcquire}))
	}
	for sequence, filter := range []string{"orders/#", "orders/new"} {
		applyMustSucceed(t, machine, next(), cmaSubscriptionCommand(cmaOperationID(110+sequence), SubscriptionChangeDTO{TenantID: cmaTenantID, ClientID: "a", OwnerNodeID: 1, Epoch: 1, Filter: filter, QoS: 1, Action: SubscriptionActionAdd}))
	}
	attach := DeliveryChangeDTO{TenantID: cmaTenantID, ClientID: "a", OwnerNodeID: 1, Epoch: 1, Action: DeliveryActionAttach, ProposedAtUnixMillis: 1}
	applyMustSucceed(t, machine, next(), cmaDeliveryCommand(attach))

	for _, packetID := range []uint32{3, 9} {
		receive := DeliveryChangeDTO{TenantID: cmaTenantID, ClientID: "a", OwnerNodeID: 1, Epoch: 1, Action: DeliveryActionReceive, ProposedAtUnixMillis: 1, PacketID: packetID, Topic: "orders/new", Payload: []byte("payload")}
		applyMustSucceed(t, machine, next(), cmaDeliveryCommand(receive))
	}
	for _, qos := range []uint32{1, 2} {
		publish := DeliveryChangeDTO{TenantID: cmaTenantID, ClientID: "publisher", Action: DeliveryActionPublish, ProposedAtUnixMillis: 1, Topic: "orders/new", Payload: []byte("payload"), PublishQoS: qos, Targets: []DeliveryTargetDTO{{ClientID: "a"}}}
		applyMustSucceed(t, machine, next(), cmaDeliveryCommand(publish))
	}
	for index, packetID := range []uint32{4, 8} {
		send := DeliveryChangeDTO{TenantID: cmaTenantID, ClientID: "a", OwnerNodeID: 1, Epoch: 1, Action: DeliveryActionSend, ProposedAtUnixMillis: 1, Sequence: uint64(index + 1), PacketID: packetID}
		applyMustSucceed(t, machine, next(), cmaDeliveryCommand(send))
	}
	return machine, index
}

// cmaCatchingUpMachine commits a completed removal (3) and a join (4) that is catching up.
func cmaCatchingUpMachine(t *testing.T) (*StateMachine, uint64) {
	t.Helper()
	machine := NewStateMachine()
	if err := machine.InitializeBootstrap(1, testVoters()); err != nil {
		t.Fatalf("initialize bootstrap: %v", err)
	}
	remove3 := MembershipRecordDTO{OperationID: testOperation6, Action: MembershipActionBeginRemove, IdempotencyKey: "remove-3", Member: cmaMemberPointer(3, MemberPhaseLeaving, testOperation6), TargetMemberID: 3}
	applyMustSucceed(t, machine, 2, testCommand(testOperation6, &remove3))
	cmaApplyConfiguration(t, machine, 3, testOperation6, ConfigurationActionRemoveMember, 3)
	stage4 := stageRecord(testOperation4, "join-4", 4)
	applyMustSucceed(t, machine, 4, testCommand(testOperation4, &stage4))
	cmaApplyConfiguration(t, machine, 5, testOperation4, ConfigurationActionAddLearner, 4)
	catchingUp := MembershipRecordDTO{OperationID: testOperation4, Action: MembershipActionAddLearner, IdempotencyKey: "join-4", Member: cmaMemberPointer(4, MemberPhaseCatchingUp, testOperation4)}
	applyMustSucceed(t, machine, 6, testCommand(testOperation4, &catchingUp))
	return machine, 6
}

func TestCMASnapshotRestoreRoundTripsEveryOperationPhase(t *testing.T) {
	tests := []struct {
		name         string
		build        func(*testing.T) (*StateMachine, uint64)
		afterRestore func(*testing.T, *StateMachine, uint64)
	}{
		{
			name:  "removal in progress",
			build: cmaRemovalInProgressMachine,
			afterRestore: func(t *testing.T, machine *StateMachine, index uint64) {
				stage := stageRecord(testOperation7, "join-6", 6)
				if result := mustApplyResult(t, machine, index+1, testCommand(testOperation7, &stage)); result.Conflict != ConflictMembershipInProgress {
					t.Fatalf("join during restored removal conflict=%q", result.Conflict)
				}
				cmaApplyConfiguration(t, machine, index+2, testOperation6, ConfigurationActionRemoveMember, 3)
				if member := cmaMemberByID(t, machine, 3); member.Phase != MemberPhaseRetired {
					t.Fatalf("restored removal did not retire member: %+v", member)
				}
			},
		},
		{
			name:  "join catching up",
			build: cmaCatchingUpMachine,
			afterRestore: func(t *testing.T, machine *StateMachine, index uint64) {
				cmaApplyConfiguration(t, machine, index+1, testOperation4, ConfigurationActionPromoteVoter, 4)
				result, err := machine.ApplyConfiguration(index+2, testOperation4, ConfigurationActionAddLearner, 4)
				if err != nil || result.Conflict != ConflictConfigurationMismatch {
					t.Fatalf("duplicate learner addition after promotion = (%+v, %v), want configuration mismatch", result, err)
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			source, index := test.build(t)
			snapshot, err := source.Snapshot(index)
			if err != nil {
				t.Fatalf("snapshot: %v", err)
			}
			restored := NewStateMachine()
			if err := restored.Restore(snapshot); err != nil {
				t.Fatalf("restore: %v", err)
			}
			again, err := restored.Snapshot(index)
			if err != nil || !reflect.DeepEqual(again, snapshot) {
				t.Fatalf("restored snapshot differs: err=%v\nrestored=%+v\nsource=%+v", err, again, snapshot)
			}
			if !reflect.DeepEqual(restored.Operations(), source.Operations()) {
				t.Fatalf("restored operation views differ:\nrestored=%+v\nsource=%+v", restored.Operations(), source.Operations())
			}
			for _, operation := range source.Operations() {
				replayed, exists := restored.GetOperationByIdempotencyKey(operation.Record.IdempotencyKey)
				if !exists || replayed.OperationID != operation.OperationID {
					t.Fatalf("restored idempotency key %q = (%+v, %t)", operation.Record.IdempotencyKey, replayed, exists)
				}
			}

			// Restored and original machines must stay deterministic replicas under the same committed tail.
			test.afterRestore(t, source, index)
			test.afterRestore(t, restored, index)
			sourceAfter, sourceErr := source.Snapshot(index + 2)
			restoredAfter, restoredErr := restored.Snapshot(index + 2)
			if sourceErr != nil || restoredErr != nil || !reflect.DeepEqual(sourceAfter, restoredAfter) {
				t.Fatalf("replicas diverged after restore: sourceErr=%v restoredErr=%v", sourceErr, restoredErr)
			}
		})
	}
}

func TestCMARestoreRejectsInconsistentSnapshotsAtomically(t *testing.T) {
	source, index := cmaRemovalInProgressMachine(t)
	valid, err := source.Snapshot(index)
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if len(valid.Members) != 5 || len(valid.Ownerships) != 3 || len(valid.Subscriptions) != 2 || len(valid.DeliverySessions) != 1 || len(valid.DeliverySessions[0].Messages) != 2 || len(valid.DeliverySessions[0].Inbound) != 2 || len(valid.MembershipHistory) != 3 {
		t.Fatalf("unexpected fixture shape: %+v", valid)
	}
	tests := []struct {
		name   string
		mutate func(*SnapshotDTO)
	}{
		{name: "unsupported schema major", mutate: func(snapshot *SnapshotDTO) { snapshot.SchemaMajor = 2 }},
		{name: "unsupported schema minor", mutate: func(snapshot *SnapshotDTO) { snapshot.SchemaMinor = 1 }},
		{name: "digest is not SHA-256 sized", mutate: func(snapshot *SnapshotDTO) { snapshot.Digest = make([]byte, 16) }},
		{name: "objects without included index", mutate: func(snapshot *SnapshotDTO) { snapshot.LastIncludedIndex = 0 }},
		{name: "ownership invalid tenant", mutate: func(snapshot *SnapshotDTO) { snapshot.Ownerships[0].TenantID = "ten\x00ant" }},
		{name: "ownership zero owner", mutate: func(snapshot *SnapshotDTO) { snapshot.Ownerships[0].OwnerNodeID = 0 }},
		{name: "ownership zero epoch", mutate: func(snapshot *SnapshotDTO) { snapshot.Ownerships[0].Epoch = 0 }},
		{name: "ownership zero committed index", mutate: func(snapshot *SnapshotDTO) { snapshot.Ownerships[0].CommittedIndex = 0 }},
		{name: "ownership committed after snapshot", mutate: func(snapshot *SnapshotDTO) {
			snapshot.Ownerships[0].CommittedIndex = snapshot.LastIncludedIndex + 1
		}},
		{name: "ownership duplicate session", mutate: func(snapshot *SnapshotDTO) { snapshot.Ownerships[1] = snapshot.Ownerships[0] }},
		{name: "ownership clients out of order", mutate: func(snapshot *SnapshotDTO) {
			snapshot.Ownerships[0], snapshot.Ownerships[1] = snapshot.Ownerships[1], snapshot.Ownerships[0]
		}},
		{name: "subscription invalid session", mutate: func(snapshot *SnapshotDTO) { snapshot.Subscriptions[0].ClientID = "" }},
		{name: "subscription zero owner", mutate: func(snapshot *SnapshotDTO) { snapshot.Subscriptions[0].OwnerNodeID = 0 }},
		{name: "subscription zero epoch", mutate: func(snapshot *SnapshotDTO) { snapshot.Subscriptions[0].Epoch = 0 }},
		{name: "subscription invalid filter", mutate: func(snapshot *SnapshotDTO) { snapshot.Subscriptions[0].Filter = "m/#/x" }},
		{name: "subscription QoS above 2", mutate: func(snapshot *SnapshotDTO) { snapshot.Subscriptions[0].QoS = 3 }},
		{name: "subscription retain handling above 2", mutate: func(snapshot *SnapshotDTO) { snapshot.Subscriptions[0].RetainHandling = 3 }},
		{name: "subscription duplicate filter", mutate: func(snapshot *SnapshotDTO) { snapshot.Subscriptions[1] = snapshot.Subscriptions[0] }},
		{name: "subscription filters out of order", mutate: func(snapshot *SnapshotDTO) {
			snapshot.Subscriptions[0], snapshot.Subscriptions[1] = snapshot.Subscriptions[1], snapshot.Subscriptions[0]
		}},
		{name: "delivery next sequence zero", mutate: func(snapshot *SnapshotDTO) { snapshot.DeliverySessions[0].NextSequence = 0 }},
		{name: "delivery message packet phase mismatch", mutate: func(snapshot *SnapshotDTO) { snapshot.DeliverySessions[0].Messages[0].PacketID = 0 }},
		{name: "delivery duplicate session", mutate: func(snapshot *SnapshotDTO) {
			copy := cloneDeliverySession(snapshot.DeliverySessions[0])
			snapshot.DeliverySessions = append(snapshot.DeliverySessions, copy)
			snapshot.ObjectCount += 5
		}},
		{name: "history member not committed", mutate: func(snapshot *SnapshotDTO) { snapshot.MembershipHistory[1].Member.MemberID = 99 }},
		{name: "history identity differs from member", mutate: func(snapshot *SnapshotDTO) {
			snapshot.MembershipHistory[1].Member.Address = "other.example:19005"
		}},
		{name: "history idempotency key reused", mutate: func(snapshot *SnapshotDTO) {
			snapshot.MembershipHistory[1].IdempotencyKey = snapshot.MembershipHistory[0].IdempotencyKey
		}},
		{name: "two in-progress operations", mutate: func(snapshot *SnapshotDTO) {
			record := &snapshot.MembershipHistory[1]
			record.Action = MembershipActionStageJoin
			record.Member.Phase = MemberPhaseStaged
			record.Member.OperationID = record.OperationID
		}},
		{name: "active member without in-progress operation", mutate: func(snapshot *SnapshotDTO) {
			snapshot.Members[4].Phase = MemberPhaseStaged
			snapshot.Members[4].OperationID = testOperation5
		}},
		{name: "history out of order", mutate: func(snapshot *SnapshotDTO) {
			snapshot.MembershipHistory[0], snapshot.MembershipHistory[1] = snapshot.MembershipHistory[1], snapshot.MembershipHistory[0]
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			target := NewStateMachine()
			if err := target.InitializeBootstrap(1, testVoters()); err != nil {
				t.Fatalf("initialize target: %v", err)
			}
			before, err := target.Snapshot(1)
			if err != nil {
				t.Fatalf("target snapshot: %v", err)
			}
			corrupt := cloneSnapshot(valid)
			test.mutate(&corrupt)
			if err := target.Restore(corrupt); !errors.Is(err, ErrInvalidSnapshot) {
				t.Fatalf("Restore error=%v, want ErrInvalidSnapshot", err)
			}
			after, err := target.Snapshot(1)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("rejected snapshot changed target state: err=%v last applied=%d", err, target.LastAppliedIndex())
			}
		})
	}
	target := NewStateMachine()
	if err := target.Restore(valid); err != nil {
		t.Fatalf("unmodified fixture snapshot was rejected: %v", err)
	}
}

type cmaPort struct {
	*stateMachinePort
	membersErr      error
	lookupErr       error
	getOperationErr error
	ownershipErr    error
	beforeCommit    func()
}

func (port *cmaPort) Commit(ctx context.Context, command CommandDTO) (ApplyResultDTO, error) {
	if port.beforeCommit != nil {
		port.beforeCommit()
	}
	return port.stateMachinePort.Commit(ctx, command)
}

func (port *cmaPort) Members(ctx context.Context) ([]MemberIdentityDTO, error) {
	if port.membersErr != nil {
		return nil, port.membersErr
	}
	return port.stateMachinePort.Members(ctx)
}

func (port *cmaPort) GetOperation(ctx context.Context, operationID string) (MembershipOperationDTO, bool, error) {
	if port.getOperationErr != nil {
		return MembershipOperationDTO{}, false, port.getOperationErr
	}
	return port.stateMachinePort.GetOperation(ctx, operationID)
}

func (port *cmaPort) GetOperationByIdempotencyKey(ctx context.Context, idempotencyKey string) (MembershipOperationDTO, bool, error) {
	if port.lookupErr != nil {
		return MembershipOperationDTO{}, false, port.lookupErr
	}
	return port.stateMachinePort.GetOperationByIdempotencyKey(ctx, idempotencyKey)
}

func (port *cmaPort) GetOwnership(ctx context.Context, tenantID, clientID string) (OwnershipStateDTO, bool, error) {
	if port.ownershipErr != nil {
		return OwnershipStateDTO{}, false, port.ownershipErr
	}
	return port.stateMachinePort.GetOwnership(ctx, tenantID, clientID)
}

func cmaNewPortService(t *testing.T, machine *StateMachine) (*Service, *cmaPort) {
	t.Helper()
	port := &cmaPort{stateMachinePort: &stateMachinePort{machine: machine, nextIndex: machine.LastAppliedIndex()}}
	service, err := NewService(port)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	return service, port
}

func cmaBootstrappedService(t *testing.T) (*Service, *cmaPort) {
	t.Helper()
	machine := NewStateMachine()
	if err := machine.InitializeBootstrap(1, testVoters()); err != nil {
		t.Fatalf("initialize bootstrap: %v", err)
	}
	return cmaNewPortService(t, machine)
}

const cmaSecretCause = "secret adapter cause"

func cmaRequireServiceError(t *testing.T, err error, code ServiceErrorCode, retryable bool) {
	t.Helper()
	var serviceError *ServiceError
	if !errors.As(err, &serviceError) || serviceError.Code != code || serviceError.Retryable != retryable {
		t.Fatalf("error=%v (%+v), want code %q retryable=%t", err, serviceError, code, retryable)
	}
	if strings.Contains(err.Error(), cmaSecretCause) {
		t.Fatalf("service error leaked adapter cause: %q", err.Error())
	}
}

func cmaCanceledContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

func TestCMANewServiceRequiresPort(t *testing.T) {
	service, err := NewService(nil)
	if err == nil || service != nil {
		t.Fatalf("NewService(nil) = (%v, %v), want rejection", service, err)
	}
}

func TestCMAServiceStageJoinRejectsBeforeCommitting(t *testing.T) {
	var nilContext context.Context
	secret := errors.New(cmaSecretCause)
	tests := []struct {
		name         string
		ctx          context.Context
		emptyCluster bool
		configure    func(*cmaPort, *StageJoinRequestDTO)
		code         ServiceErrorCode
		retryable    bool
	}{
		{name: "nil context", ctx: nilContext, code: ServiceErrorInvalidRequest},
		{name: "idempotency key with space", configure: func(_ *cmaPort, request *StageJoinRequestDTO) { request.IdempotencyKey = "join 4" }, code: ServiceErrorInvalidRequest},
		{name: "idempotency key above 128 bytes", configure: func(_ *cmaPort, request *StageJoinRequestDTO) { request.IdempotencyKey = strings.Repeat("k", 129) }, code: ServiceErrorInvalidRequest},
		{name: "canceled context", ctx: cmaCanceledContext(), code: ServiceErrorQuorumUnavailable, retryable: true},
		{name: "registry read failure", configure: func(port *cmaPort, _ *StageJoinRequestDTO) { port.membersErr = secret }, code: ServiceErrorQuorumUnavailable, retryable: true},
		{name: "registry read on follower", configure: func(port *cmaPort, _ *StageJoinRequestDTO) {
			port.membersErr = fmt.Errorf("members: %w", ErrNotLeader)
		}, code: ServiceErrorNotLeader, retryable: true},
		{name: "empty committed registry", emptyCluster: true, code: ServiceErrorInvalidRequest},
		{name: "zero member ID", configure: func(_ *cmaPort, request *StageJoinRequestDTO) { request.Member.MemberID = 0 }, code: ServiceErrorInvalidRequest},
		{name: "address without port", configure: func(_ *cmaPort, request *StageJoinRequestDTO) { request.Member.Address = "node4.example" }, code: ServiceErrorInvalidRequest},
		{name: "IP DNS SAN", configure: func(_ *cmaPort, request *StageJoinRequestDTO) { request.Member.DNSSAN = "10.0.0.4" }, code: ServiceErrorInvalidRequest},
		{name: "truncated certificate digest", configure: func(_ *cmaPort, request *StageJoinRequestDTO) {
			request.Member.CertificateSHA256 = request.Member.CertificateSHA256[:31]
		}, code: ServiceErrorInvalidRequest},
		{name: "idempotency lookup failure", configure: func(port *cmaPort, _ *StageJoinRequestDTO) { port.lookupErr = secret }, code: ServiceErrorQuorumUnavailable, retryable: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			machine := NewStateMachine()
			if !test.emptyCluster {
				if err := machine.InitializeBootstrap(1, testVoters()); err != nil {
					t.Fatalf("initialize bootstrap: %v", err)
				}
			}
			service, port := cmaNewPortService(t, machine)
			request := StageJoinRequestDTO{IdempotencyKey: "join-4", Member: testMember(4, MemberPhaseUnspecified, "")}
			if test.configure != nil {
				test.configure(port, &request)
			}
			ctx := test.ctx
			if ctx == nil && test.name != "nil context" {
				ctx = context.Background()
			}
			_, err := service.StageJoin(ctx, request)
			cmaRequireServiceError(t, err, test.code, test.retryable)
			if port.commitCalls != 0 || len(machine.Operations()) != 0 {
				t.Fatalf("rejected stage reached commit: calls=%d operations=%+v", port.commitCalls, machine.Operations())
			}
		})
	}
}

func TestCMAServiceMapsCommittedMembershipConflicts(t *testing.T) {
	t.Run("reserved member ID", func(t *testing.T) {
		service, port := cmaBootstrappedService(t)
		_, err := service.StageJoin(context.Background(), StageJoinRequestDTO{IdempotencyKey: "join-1", Member: testMember(1, MemberPhaseUnspecified, "")})
		cmaRequireServiceError(t, err, ServiceErrorMemberIDReserved, false)
		if port.commitCalls != 1 || len(port.machine.Operations()) != 0 {
			t.Fatalf("reserved ID stage calls=%d operations=%+v", port.commitCalls, port.machine.Operations())
		}
	})
	t.Run("join while another join is in progress", func(t *testing.T) {
		service, port := cmaBootstrappedService(t)
		if _, err := service.StageJoin(context.Background(), StageJoinRequestDTO{IdempotencyKey: "join-4", Member: testMember(4, MemberPhaseUnspecified, "")}); err != nil {
			t.Fatalf("first stage: %v", err)
		}
		_, err := service.StageJoin(context.Background(), StageJoinRequestDTO{IdempotencyKey: "join-5", Member: testMember(5, MemberPhaseUnspecified, "")})
		cmaRequireServiceError(t, err, ServiceErrorMembershipInProgress, true)
		if len(port.machine.Members()) != 4 {
			t.Fatalf("in-progress rejection staged another member: %+v", port.machine.Members())
		}
	})
	t.Run("removal while a join is in progress", func(t *testing.T) {
		service, port := cmaBootstrappedService(t)
		if _, err := service.StageJoin(context.Background(), StageJoinRequestDTO{IdempotencyKey: "join-4", Member: testMember(4, MemberPhaseUnspecified, "")}); err != nil {
			t.Fatalf("stage: %v", err)
		}
		_, err := service.StartPlannedRemoval(context.Background(), 3, "remove-3", 1)
		cmaRequireServiceError(t, err, ServiceErrorMembershipInProgress, true)
		if cmaMemberByID(t, port.machine, 3).Phase != MemberPhaseVoter {
			t.Fatal("in-progress rejection started removal")
		}
	})
}

func TestCMAServiceStartPlannedRemovalRejections(t *testing.T) {
	var nilContext context.Context
	secret := errors.New(cmaSecretCause)
	tests := []struct {
		name      string
		ctx       context.Context
		memberID  uint64
		key       string
		configure func(*cmaPort)
		code      ServiceErrorCode
		retryable bool
	}{
		{name: "nil context", ctx: nilContext, memberID: 3, key: "remove-3", code: ServiceErrorInvalidRequest},
		{name: "zero member ID", ctx: context.Background(), memberID: 0, key: "remove-0", code: ServiceErrorInvalidRequest},
		{name: "empty idempotency key", ctx: context.Background(), memberID: 3, key: "", code: ServiceErrorInvalidRequest},
		{name: "canceled context", ctx: cmaCanceledContext(), memberID: 3, key: "remove-3", code: ServiceErrorQuorumUnavailable, retryable: true},
		{name: "idempotency lookup failure", ctx: context.Background(), memberID: 3, key: "remove-3", configure: func(port *cmaPort) { port.lookupErr = secret }, code: ServiceErrorQuorumUnavailable, retryable: true},
		{name: "registry read failure", ctx: context.Background(), memberID: 3, key: "remove-3", configure: func(port *cmaPort) { port.membersErr = secret }, code: ServiceErrorQuorumUnavailable, retryable: true},
		{name: "unknown member", ctx: context.Background(), memberID: 9, key: "remove-9", code: ServiceErrorResourceNotFound},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			service, port := cmaBootstrappedService(t)
			if test.configure != nil {
				test.configure(port)
			}
			_, err := service.StartPlannedRemoval(test.ctx, test.memberID, test.key, 1)
			cmaRequireServiceError(t, err, test.code, test.retryable)
			if port.commitCalls != 0 || len(port.machine.Operations()) != 0 {
				t.Fatalf("rejected removal reached commit: calls=%d", port.commitCalls)
			}
		})
	}

	t.Run("key bound to a join", func(t *testing.T) {
		service, port := cmaBootstrappedService(t)
		if _, err := service.StageJoin(context.Background(), StageJoinRequestDTO{IdempotencyKey: "shared-key", Member: testMember(4, MemberPhaseUnspecified, "")}); err != nil {
			t.Fatalf("stage: %v", err)
		}
		_, err := service.StartPlannedRemoval(context.Background(), 3, "shared-key", 1)
		cmaRequireServiceError(t, err, ServiceErrorIdempotencyConflict, false)
		if port.commitCalls != 1 || cmaMemberByID(t, port.machine, 3).Phase != MemberPhaseVoter {
			t.Fatalf("idempotency conflict proposed removal: calls=%d", port.commitCalls)
		}
	})
	t.Run("key bound to another member removal", func(t *testing.T) {
		service, port := cmaBootstrappedService(t)
		if _, err := service.StartPlannedRemoval(context.Background(), 3, "remove-key", 1); err != nil {
			t.Fatalf("start removal: %v", err)
		}
		_, err := service.StartPlannedRemoval(context.Background(), 2, "remove-key", 1)
		cmaRequireServiceError(t, err, ServiceErrorIdempotencyConflict, false)
		if port.commitCalls != 1 || cmaMemberByID(t, port.machine, 2).Phase != MemberPhaseVoter {
			t.Fatalf("idempotency conflict proposed removal: calls=%d", port.commitCalls)
		}
	})
	t.Run("member is not a voter", func(t *testing.T) {
		service, port := cmaBootstrappedService(t)
		if _, err := service.StageJoin(context.Background(), StageJoinRequestDTO{IdempotencyKey: "join-4", Member: testMember(4, MemberPhaseUnspecified, "")}); err != nil {
			t.Fatalf("stage: %v", err)
		}
		_, err := service.StartPlannedRemoval(context.Background(), 4, "remove-4", 1)
		cmaRequireServiceError(t, err, ServiceErrorOperationConflict, false)
		if port.commitCalls != 1 || cmaMemberByID(t, port.machine, 4).Phase != MemberPhaseStaged {
			t.Fatalf("staged member removal proposed a command: calls=%d", port.commitCalls)
		}
	})
}

func TestCMAServiceGetOperationPortFailures(t *testing.T) {
	var nilContext context.Context
	service, port := cmaBootstrappedService(t)
	_, err := service.GetOperation(nilContext, testOperation4)
	cmaRequireServiceError(t, err, ServiceErrorInvalidRequest, false)

	port.getOperationErr = errors.New(cmaSecretCause)
	_, err = service.GetOperation(context.Background(), testOperation4)
	cmaRequireServiceError(t, err, ServiceErrorQuorumUnavailable, true)

	port.getOperationErr = fmt.Errorf("read: %w", ErrNotLeader)
	_, err = service.GetOperation(context.Background(), testOperation4)
	cmaRequireServiceError(t, err, ServiceErrorNotLeader, true)
	if !errors.Is(err, ErrNotLeader) || errors.Is(err, ErrQuorumUnavailable) {
		t.Fatalf("not-leader classification lost: %v", err)
	}
}

func TestCMAConflictServiceErrorMatchesHTTPContract(t *testing.T) {
	tests := []struct {
		conflict  ApplyConflictCode
		code      ServiceErrorCode
		retryable bool
	}{
		{conflict: ConflictMemberIDReserved, code: ServiceErrorMemberIDReserved},
		{conflict: ConflictMemberIdentity, code: ServiceErrorMemberIdentityConflict},
		{conflict: ConflictIdempotency, code: ServiceErrorIdempotencyConflict},
		{conflict: ConflictMembershipInProgress, code: ServiceErrorMembershipInProgress, retryable: true},
		{conflict: ConflictOperationNotFound, code: ServiceErrorResourceNotFound},
		{conflict: ConflictMemberNotFound, code: ServiceErrorResourceNotFound},
		{conflict: ConflictCompatibilityUnsupported, code: ServiceErrorCompatibilityUnsupported},
		{conflict: ConflictOperationID, code: ServiceErrorOperationConflict},
		{conflict: ConflictPhase, code: ServiceErrorOperationConflict},
		{conflict: ConflictConfigurationMismatch, code: ServiceErrorOperationConflict},
		{conflict: ConflictStaleEpoch, code: ServiceErrorOperationConflict},
		{conflict: ConflictOwnershipMissing, code: ServiceErrorOperationConflict},
	}
	for _, test := range tests {
		t.Run(string(test.conflict), func(t *testing.T) {
			err := conflictServiceError(test.conflict)
			if err.Code != test.code || err.Retryable != test.retryable {
				t.Fatalf("conflict %q = %+v, want code %q retryable=%t", test.conflict, err, test.code, test.retryable)
			}
			if errors.Is(err, ErrNotLeader) || errors.Is(err, ErrQuorumUnavailable) {
				t.Fatalf("domain conflict %q was classified as a port failure", test.conflict)
			}
		})
	}

	codes := []ServiceErrorCode{
		ServiceErrorInvalidRequest, ServiceErrorResourceNotFound, ServiceErrorMemberIDReserved,
		ServiceErrorMemberIdentityConflict, ServiceErrorIdempotencyConflict, ServiceErrorMembershipInProgress,
		ServiceErrorOperationConflict, ServiceErrorNotLeader, ServiceErrorQuorumUnavailable, ServiceErrorCompatibilityUnsupported,
	}
	fallback := newServiceError(ServiceErrorCode("unmapped"), false).Message
	seen := make(map[string]ServiceErrorCode, len(codes))
	for _, code := range codes {
		err := newServiceError(code, false)
		if err.Error() != err.Message || err.Message == fallback {
			t.Fatalf("code %q message %q is not a dedicated stable message", code, err.Message)
		}
		if previous, duplicate := seen[err.Message]; duplicate {
			t.Fatalf("codes %q and %q share message %q", previous, code, err.Message)
		}
		seen[err.Message] = code
	}
}

func TestCMAAcquireOwnershipRejectsBeforeCommitting(t *testing.T) {
	var nilContext context.Context
	secret := errors.New(cmaSecretCause)
	tests := []struct {
		name      string
		ctx       context.Context
		tenantID  string
		clientID  string
		owner     uint64
		configure func(*cmaPort)
		code      ServiceErrorCode
		retryable bool
	}{
		{name: "nil context", ctx: nilContext, tenantID: cmaTenantID, clientID: cmaClientID, owner: 1, code: ServiceErrorQuorumUnavailable, retryable: true},
		{name: "canceled context", ctx: cmaCanceledContext(), tenantID: cmaTenantID, clientID: cmaClientID, owner: 1, code: ServiceErrorQuorumUnavailable, retryable: true},
		{name: "tenant with NUL", ctx: context.Background(), tenantID: "ten\x00ant", clientID: cmaClientID, owner: 1, code: ServiceErrorInvalidRequest},
		{name: "tenant above 128 bytes", ctx: context.Background(), tenantID: strings.Repeat("t", maxTenantIDBytes+1), clientID: cmaClientID, owner: 1, code: ServiceErrorInvalidRequest},
		{name: "empty client", ctx: context.Background(), tenantID: cmaTenantID, clientID: "", owner: 1, code: ServiceErrorInvalidRequest},
		{name: "client with NUL", ctx: context.Background(), tenantID: cmaTenantID, clientID: "cli\x00ent", owner: 1, code: ServiceErrorInvalidRequest},
		{name: "zero owner", ctx: context.Background(), tenantID: cmaTenantID, clientID: cmaClientID, owner: 0, code: ServiceErrorInvalidRequest},
		{name: "ownership read failure", ctx: context.Background(), tenantID: cmaTenantID, clientID: cmaClientID, owner: 1, configure: func(port *cmaPort) { port.ownershipErr = secret }, code: ServiceErrorQuorumUnavailable, retryable: true},
		{name: "ownership read on follower", ctx: context.Background(), tenantID: cmaTenantID, clientID: cmaClientID, owner: 1, configure: func(port *cmaPort) {
			port.ownershipErr = fmt.Errorf("ownership: %w", ErrNotLeader)
		}, code: ServiceErrorNotLeader, retryable: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			service, port := cmaBootstrappedService(t)
			if test.configure != nil {
				test.configure(port)
			}
			epoch, err := service.AcquireOwnership(test.ctx, test.tenantID, test.clientID, test.owner)
			cmaRequireServiceError(t, err, test.code, test.retryable)
			if epoch != 0 || port.commitCalls != 0 || len(port.machine.Ownerships()) != 0 {
				t.Fatalf("rejected acquire returned epoch %d with %d commits", epoch, port.commitCalls)
			}
		})
	}
}

func TestCMAAcquireOwnershipCommitFailures(t *testing.T) {
	t.Run("commit failure is sanitized", func(t *testing.T) {
		service, port := cmaBootstrappedService(t)
		port.commitError = errors.New(cmaSecretCause)
		epoch, err := service.AcquireOwnership(context.Background(), cmaTenantID, cmaClientID, 1)
		cmaRequireServiceError(t, err, ServiceErrorQuorumUnavailable, true)
		if epoch != 0 {
			t.Fatalf("failed acquire returned epoch %d", epoch)
		}
	})
	t.Run("commit on follower", func(t *testing.T) {
		service, port := cmaBootstrappedService(t)
		port.commitError = ErrNotLeader
		_, err := service.AcquireOwnership(context.Background(), cmaTenantID, cmaClientID, 1)
		cmaRequireServiceError(t, err, ServiceErrorNotLeader, true)
	})
	t.Run("uncommitted outcome", func(t *testing.T) {
		service, port := cmaBootstrappedService(t)
		port.returnUncommitted = true
		epoch, err := service.AcquireOwnership(context.Background(), cmaTenantID, cmaClientID, 1)
		cmaRequireServiceError(t, err, ServiceErrorQuorumUnavailable, true)
		if epoch != 0 || len(port.machine.Ownerships()) != 0 {
			t.Fatalf("uncommitted acquire returned epoch %d ownerships=%+v", epoch, port.machine.Ownerships())
		}
	})
	t.Run("lost race to a concurrent takeover", func(t *testing.T) {
		service, port := cmaBootstrappedService(t)
		competitor := OwnershipStateDTO{TenantID: cmaTenantID, ClientID: cmaClientID, OwnerNodeID: 2, Epoch: 1}
		port.beforeCommit = func() {
			port.nextIndex++
			competitor.CommittedIndex = port.nextIndex
			applyMustSucceed(t, port.machine, port.nextIndex, cmaOwnershipCommand(testOperation8, OwnershipChangeDTO{
				TenantID: cmaTenantID, ClientID: cmaClientID, OwnerNodeID: 2, Epoch: 1, Action: OwnershipActionAcquire,
			}))
		}
		epoch, err := service.AcquireOwnership(context.Background(), cmaTenantID, cmaClientID, 1)
		cmaRequireServiceError(t, err, ServiceErrorOperationConflict, false)
		if current, exists := port.machine.GetOwnership(cmaTenantID, cmaClientID); epoch != 0 || !exists || current != competitor {
			t.Fatalf("stale acquire epoch=%d ownership=(%+v, %t), want competitor %+v", epoch, current, exists, competitor)
		}
	})
}

func TestCMACommitSubscriptionRejectionsAndPortFailures(t *testing.T) {
	var nilContext context.Context
	tests := []struct {
		name      string
		ctx       context.Context
		tenantID  string
		owner     uint64
		epoch     uint64
		action    uint32
		code      ServiceErrorCode
		retryable bool
	}{
		{name: "nil context", ctx: nilContext, tenantID: cmaTenantID, owner: 1, epoch: 1, action: uint32(SubscriptionActionAdd), code: ServiceErrorQuorumUnavailable, retryable: true},
		{name: "canceled context", ctx: cmaCanceledContext(), tenantID: cmaTenantID, owner: 1, epoch: 1, action: uint32(SubscriptionActionAdd), code: ServiceErrorQuorumUnavailable, retryable: true},
		{name: "invalid tenant", ctx: context.Background(), tenantID: "", owner: 1, epoch: 1, action: uint32(SubscriptionActionAdd), code: ServiceErrorInvalidRequest},
		{name: "zero owner", ctx: context.Background(), tenantID: cmaTenantID, owner: 0, epoch: 1, action: uint32(SubscriptionActionAdd), code: ServiceErrorInvalidRequest},
		{name: "zero epoch", ctx: context.Background(), tenantID: cmaTenantID, owner: 1, epoch: 0, action: uint32(SubscriptionActionAdd), code: ServiceErrorInvalidRequest},
		{name: "unspecified action", ctx: context.Background(), tenantID: cmaTenantID, owner: 1, epoch: 1, action: uint32(SubscriptionActionUnspecified), code: ServiceErrorInvalidRequest},
		{name: "action beyond remove", ctx: context.Background(), tenantID: cmaTenantID, owner: 1, epoch: 1, action: uint32(SubscriptionActionRemove) + 1, code: ServiceErrorInvalidRequest},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			service, port := cmaBootstrappedService(t)
			err := service.CommitSubscription(test.ctx, test.tenantID, cmaClientID, test.owner, test.epoch, "orders/#", 1, false, false, 0, test.action)
			cmaRequireServiceError(t, err, test.code, test.retryable)
			if port.commitCalls != 0 {
				t.Fatalf("rejected subscription reached commit %d times", port.commitCalls)
			}
		})
	}

	failures := []struct {
		name      string
		configure func(*cmaPort)
		code      ServiceErrorCode
	}{
		{name: "commit failure", configure: func(port *cmaPort) { port.commitError = errors.New(cmaSecretCause) }, code: ServiceErrorQuorumUnavailable},
		{name: "commit on follower", configure: func(port *cmaPort) { port.commitError = fmt.Errorf("commit: %w", ErrNotLeader) }, code: ServiceErrorNotLeader},
		{name: "uncommitted outcome", configure: func(port *cmaPort) { port.returnUncommitted = true }, code: ServiceErrorQuorumUnavailable},
	}
	for _, test := range failures {
		t.Run(test.name, func(t *testing.T) {
			service, port := cmaBootstrappedService(t)
			epoch, err := service.AcquireOwnership(context.Background(), cmaTenantID, cmaClientID, 1)
			if err != nil {
				t.Fatalf("acquire: %v", err)
			}
			test.configure(port)
			err = service.CommitSubscription(context.Background(), cmaTenantID, cmaClientID, 1, epoch, "orders/#", 1, false, false, 0, uint32(SubscriptionActionAdd))
			cmaRequireServiceError(t, err, test.code, true)
			if subscriptions := port.machine.Subscriptions(); len(subscriptions) != 0 {
				t.Fatalf("failed subscription commit left state: %+v", subscriptions)
			}
		})
	}
}
