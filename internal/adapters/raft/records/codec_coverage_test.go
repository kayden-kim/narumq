package records

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"slices"
	"strings"
	"testing"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

const (
	crrOp2 = "00000000-0000-4000-8000-000000000002"
	crrOp3 = "00000000-0000-4000-8000-000000000003"
)

var crrUnknownField = protoreflect.RawFields{0x78, 0x01}

func crrWantErr(t *testing.T, err error, want string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected error containing %q, got nil", want)
	}
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("error = %q, want it to contain %q", err, want)
	}
}

func crrVoter(id uint64) *MemberIdentity {
	return &MemberIdentity{
		MemberId: id, ClusterId: "cluster-a", Address: "node.example:7000", DnsSan: "node.example",
		CertificateSha256: bytes.Repeat([]byte{0xab}, 32), ReleaseVersion: "1.2.3",
		ClusterProtocolMajor: 2, StateSchemaMajor: 1, Phase: MemberPhase_MEMBER_PHASE_VOTER,
	}
}

func crrStaged(id uint64, operationID string) *MemberIdentity {
	member := crrVoter(id)
	member.Phase = MemberPhase_MEMBER_PHASE_STAGED
	member.OperationId = operationID
	return member
}

func crrStageJoin() *MembershipRecord {
	return &MembershipRecord{
		OperationId: op1, Action: MembershipAction_MEMBERSHIP_ACTION_STAGE_JOIN,
		IdempotencyKey: "join-1", Member: crrStaged(4, op1), ExpectedConfStateIndex: 3,
	}
}

func crrMembershipCommand(record *MembershipRecord) *CommandEnvelope {
	return &CommandEnvelope{SchemaMajor: 1, OperationId: record.OperationId, Payload: &CommandEnvelope_Membership{Membership: record}}
}

func crrOwnership() *OwnershipChange {
	return &OwnershipChange{TenantId: "tenant", ClientId: "client", OwnerNodeId: 1, Epoch: 1, Action: OwnershipAction_OWNERSHIP_ACTION_ACQUIRE}
}

func crrSubscription() *SubscriptionChange {
	return &SubscriptionChange{TenantId: "tenant", ClientId: "client", OwnerNodeId: 1, Epoch: 1, Filter: "a/+/c", Qos: 1, Action: SubscriptionAction_SUBSCRIPTION_ACTION_ADD}
}

func crrDelivery(action DeliveryAction) *DeliveryChange {
	change := &DeliveryChange{
		TenantId: "tenant", ClientId: "client", Action: action, ProposedAtUnixMillis: 10,
	}
	switch action {
	case DeliveryAction_DELIVERY_ACTION_PUBLISH:
		change.Topic, change.Payload, change.PublishQos = "a/b", []byte("payload"), 1
	case DeliveryAction_DELIVERY_ACTION_RECEIVE:
		change.OwnerNodeId, change.Epoch, change.PacketId = 1, 1, 7
		change.Topic, change.Payload = "a/b", []byte("payload")
	case DeliveryAction_DELIVERY_ACTION_RELEASE:
		change.OwnerNodeId, change.Epoch, change.PacketId, change.PublishQos = 1, 1, 7, 2
	}
	return change
}

func crrDeliverySession(tenant, client string) *DeliverySessionState {
	return &DeliverySessionState{
		TenantId: tenant, ClientId: client, NextSequence: 3,
		Messages: []*DeliveryMessage{
			{Sequence: 1, Topic: "a/b", Payload: []byte("payload"), Qos: 1, PacketId: 1, Phase: DeliveryPhase_DELIVERY_PHASE_AWAIT_PUBACK},
			{Sequence: 2, Topic: "a/b", Payload: []byte("payload"), Qos: 2, Phase: DeliveryPhase_DELIVERY_PHASE_QUEUED},
		},
		Inbound: []*InboundMessage{{PacketId: 7, Topic: "a/b", Payload: []byte("payload")}},
	}
}

func crrDeliveryCommand(change *DeliveryChange) *CommandEnvelope {
	return crrCommand(&CommandEnvelope_Delivery{Delivery: change})
}

func crrCommand(payload isCommandEnvelope_Payload) *CommandEnvelope {
	return &CommandEnvelope{SchemaMajor: 1, OperationId: op1, Payload: payload}
}

func crrHistory(operationID string, committed uint64) *MembershipRecord {
	return &MembershipRecord{
		OperationId: operationID, Action: MembershipAction_MEMBERSHIP_ACTION_STAGE_JOIN,
		IdempotencyKey: "key-" + operationID[len(operationID)-1:], Member: crrStaged(9, operationID), CommittedIndex: committed,
	}
}

func crrObjectCount(state *ReplicatedStateSnapshot) uint64 {
	return uint64(len(state.Members) + len(state.Ownerships) + len(state.Subscriptions) + len(state.DeliverySessions) + deliveryObjectCount(state.DeliverySessions) + len(state.MembershipHistory) + len(state.Retained) + len(state.ActiveFeatures) + len(state.CredentialRevocations) + len(state.Blocklist) + clusterPolicyStateObjectCount(state.ClusterPolicy))
}

// crrRawSnapshot encodes a snapshot envelope with a correct digest and count but
// without the producer's sorting and validation, emulating a foreign encoder.
func crrRawSnapshot(t *testing.T, state *ReplicatedStateSnapshot, lastIncluded uint64) []byte {
	t.Helper()
	stateBytes, err := deterministic.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(stateBytes)
	wire, err := deterministic.Marshal(&SnapshotEnvelope{SchemaMajor: 1, LastIncludedIndex: lastIncluded, ObjectCount: crrObjectCount(state), Sha256: digest[:], State: state})
	if err != nil {
		t.Fatal(err)
	}
	return wire
}

func TestCrrCommandRoundTripPreservesEveryRecordKind(t *testing.T) {
	publish := crrDelivery(DeliveryAction_DELIVERY_ACTION_PUBLISH)
	publish.Targets = []*DeliveryTarget{{ClientId: "client"}}
	receive := crrDelivery(DeliveryAction_DELIVERY_ACTION_RECEIVE)
	receive.Retain = true
	receive.ExpiresAtUnixMillis = 100

	learner := crrVoter(5)
	learner.Phase, learner.OperationId = MemberPhase_MEMBER_PHASE_LEARNER, crrOp2
	transfer := crrOwnership()
	transfer.Action, transfer.ExpectedEpoch, transfer.Epoch = OwnershipAction_OWNERSHIP_ACTION_TRANSFER, 1, 2
	release := crrOwnership()
	release.Action = OwnershipAction_OWNERSHIP_ACTION_RELEASE
	shared := crrSubscription()
	shared.Filter, shared.Qos, shared.RetainHandling, shared.NoLocal, shared.RetainAsPublished = "$share/group/a/#", 2, 2, true, true
	remove := crrSubscription()
	remove.Action = SubscriptionAction_SUBSCRIPTION_ACTION_REMOVE
	membership := func(action MembershipAction, member *MemberIdentity, target uint64) *CommandEnvelope {
		return crrMembershipCommand(&MembershipRecord{OperationId: crrOp2, Action: action, IdempotencyKey: strings.Repeat("~", 128), Member: member, TargetMemberId: target})
	}

	cases := map[string]*CommandEnvelope{
		"stage_join":       crrMembershipCommand(crrStageJoin()),
		"add_learner":      membership(MembershipAction_MEMBERSHIP_ACTION_ADD_LEARNER, learner, 0),
		"promote_voter":    membership(MembershipAction_MEMBERSHIP_ACTION_PROMOTE_VOTER, nil, 0),
		"begin_remove":     membership(MembershipAction_MEMBERSHIP_ACTION_BEGIN_REMOVE, nil, 5),
		"remove_member":    membership(MembershipAction_MEMBERSHIP_ACTION_REMOVE_MEMBER, nil, 5),
		"abort_stage":      membership(MembershipAction_MEMBERSHIP_ACTION_ABORT_STAGE, nil, 0),
		"ownership":        crrCommand(&CommandEnvelope_Ownership{Ownership: crrOwnership()}),
		"transfer":         crrCommand(&CommandEnvelope_Ownership{Ownership: transfer}),
		"release":          crrCommand(&CommandEnvelope_Ownership{Ownership: release}),
		"shared_filter":    crrCommand(&CommandEnvelope_Subscription{Subscription: shared}),
		"unsubscribe":      crrCommand(&CommandEnvelope_Subscription{Subscription: remove}),
		"delivery_publish": crrDeliveryCommand(publish),
		"delivery_receive": crrDeliveryCommand(receive),
		"connection_rebalance_start": crrCommand(&CommandEnvelope_ConnectionRebalance{ConnectionRebalance: &ConnectionRebalanceChange{
			Action: ConnectionRebalanceAction_CONNECTION_REBALANCE_ACTION_START, RebalanceOperationId: op1, IdempotencyKey: "rb-1",
			ServerReference: "lb.example:1883", MaxDeviationConnections: 1, ConnectionRatePerSecond: 2,
			LoadBalancerPropagationDelayMillis: 250, TimeoutMillis: 2000, ProposedAtUnixMillis: 100,
		}}),
	}
	for name, command := range cases {
		t.Run(name, func(t *testing.T) {
			before := proto.Clone(command)
			wire, err := MarshalCommand(command)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			if !proto.Equal(before, command) {
				t.Fatal("MarshalCommand modified its input")
			}
			decoded, err := UnmarshalCommand(wire)
			if err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			if !proto.Equal(decoded, command) {
				t.Fatalf("round trip mismatch:\n got %v\nwant %v", decoded, command)
			}
			again, err := MarshalCommand(decoded)
			if err != nil || !bytes.Equal(again, wire) {
				t.Fatalf("re-encoding is not stable: err=%v equal=%v", err, bytes.Equal(again, wire))
			}
		})
	}
}

func TestCrrMarshalCommandRejectsInvalidEnvelope(t *testing.T) {
	if _, err := MarshalCommand(nil); err == nil || !strings.Contains(err.Error(), "command is nil") {
		t.Fatalf("nil command error = %v", err)
	}
	nestedUnknown := crrOwnership()
	nestedUnknown.ProtoReflect().SetUnknown(crrUnknownField)
	deepUnknown := crrDeliveryCommand(crrDelivery(DeliveryAction_DELIVERY_ACTION_PUBLISH))
	deepUnknown.GetDelivery().Targets = []*DeliveryTarget{{ClientId: "target"}}
	deepUnknown.GetDelivery().Targets[0].ProtoReflect().SetUnknown(crrUnknownField)
	topUnknown := crrCommand(&CommandEnvelope_Ownership{Ownership: crrOwnership()})
	topUnknown.ProtoReflect().SetUnknown(crrUnknownField)

	cases := []struct {
		name       string
		command    *CommandEnvelope
		want       string
		classified bool
	}{
		{"top_level_unknown", topUnknown, "unknown fields", true},
		{"nested_unknown", crrCommand(&CommandEnvelope_Ownership{Ownership: nestedUnknown}), "unknown fields", true},
		{"delivery_target_unknown", deepUnknown, "unknown fields", true},
		{"schema_major_zero", &CommandEnvelope{OperationId: op1, Payload: &CommandEnvelope_Ownership{Ownership: crrOwnership()}}, "unsupported command schema", true},
		{"schema_major_two", &CommandEnvelope{SchemaMajor: 2, OperationId: op1, Payload: &CommandEnvelope_Ownership{Ownership: crrOwnership()}}, "unsupported command schema", true},
		{"empty_operation_id", &CommandEnvelope{SchemaMajor: 1, Payload: &CommandEnvelope_Ownership{Ownership: crrOwnership()}}, "operation_id must be a canonical", false},
		{"misplaced_dash", &CommandEnvelope{SchemaMajor: 1, OperationId: "000000000-000-4000-8000-000000000001", Payload: &CommandEnvelope_Ownership{Ownership: crrOwnership()}}, "operation_id must be a canonical", false},
		{"non_hex", &CommandEnvelope{SchemaMajor: 1, OperationId: "00000000-0000-4000-8000-00000000000g", Payload: &CommandEnvelope_Ownership{Ownership: crrOwnership()}}, "operation_id must be a canonical", false},
		{"no_payload", &CommandEnvelope{SchemaMajor: 1, OperationId: op1}, "exactly one known payload", true},
		{"nil_membership", crrCommand(&CommandEnvelope_Membership{}), "membership payload is required", false},
		{"nil_ownership", crrCommand(&CommandEnvelope_Ownership{}), "ownership payload is required", false},
		{"nil_subscription", crrCommand(&CommandEnvelope_Subscription{}), "subscription payload is required", false},
		{"nil_delivery", crrCommand(&CommandEnvelope_Delivery{}), "delivery payload is required", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := MarshalCommand(tc.command)
			crrWantErr(t, err, tc.want)
			if errors.Is(err, ErrIncompatibleStateSchema) != tc.classified {
				t.Fatalf("schema classification = %v, want %v: %v", errors.Is(err, ErrIncompatibleStateSchema), tc.classified, err)
			}
		})
	}
}

func TestCrrMembershipCommandValidation(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*MembershipRecord, *CommandEnvelope)
		want   string
	}{
		{"committed_index_in_command", func(r *MembershipRecord, _ *CommandEnvelope) { r.CommittedIndex = 1 }, "derived from its Raft entry"},
		{"operation_mismatch", func(r *MembershipRecord, _ *CommandEnvelope) { r.OperationId = crrOp2 }, "does not match command"},
		{"action_unspecified", func(r *MembershipRecord, _ *CommandEnvelope) {
			r.Action = MembershipAction_MEMBERSHIP_ACTION_UNSPECIFIED
		}, "invalid membership action"},
		{"action_out_of_range", func(r *MembershipRecord, _ *CommandEnvelope) {
			r.Action = MembershipAction_MEMBERSHIP_ACTION_ABORT_STAGE + 1
		}, "invalid membership action"},
		{"empty_idempotency_key", func(r *MembershipRecord, _ *CommandEnvelope) { r.IdempotencyKey = "" }, "idempotency_key"},
		{"long_idempotency_key", func(r *MembershipRecord, _ *CommandEnvelope) { r.IdempotencyKey = strings.Repeat("k", 129) }, "idempotency_key"},
		{"space_in_idempotency_key", func(r *MembershipRecord, _ *CommandEnvelope) { r.IdempotencyKey = "a b" }, "idempotency_key"},
		{"delete_in_idempotency_key", func(r *MembershipRecord, _ *CommandEnvelope) { r.IdempotencyKey = "a\x7f" }, "idempotency_key"},
		{"invalid_member_identity", func(r *MembershipRecord, _ *CommandEnvelope) { r.Member.MemberId = 0 }, "member_id must be positive"},
		{"stage_join_without_member", func(r *MembershipRecord, _ *CommandEnvelope) { r.Member = nil }, "stage join requires"},
		{"stage_join_learner_member", func(r *MembershipRecord, _ *CommandEnvelope) { r.Member.Phase = MemberPhase_MEMBER_PHASE_LEARNER }, "stage join requires"},
		{"stage_join_foreign_operation", func(r *MembershipRecord, _ *CommandEnvelope) { r.Member.OperationId = crrOp2 }, "stage join requires"},
		{"target_on_add_learner", func(r *MembershipRecord, _ *CommandEnvelope) {
			r.Action, r.Member, r.TargetMemberId = MembershipAction_MEMBERSHIP_ACTION_ADD_LEARNER, nil, 4
		}, "target_member_id is only valid"},
		{"target_on_stage_join", func(r *MembershipRecord, _ *CommandEnvelope) { r.TargetMemberId = 4 }, "target_member_id is only valid"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			record := crrStageJoin()
			command := crrMembershipCommand(record)
			tc.mutate(record, command)
			_, err := MarshalCommand(command)
			crrWantErr(t, err, tc.want)
		})
	}
}

func TestCrrProfileCommandsRoundTripAndValidation(t *testing.T) {
	commands := []*CommandEnvelope{
		{
			SchemaMajor: 1, OperationId: crrOp2,
			Payload: &CommandEnvelope_MemberProfile{MemberProfile: &MemberProfileChange{
				MemberId: 3, ReleaseVersion: "1.2.0", Features: []string{"alpha", "compatibility-probe"},
			}},
		},
		{
			SchemaMajor: 1, OperationId: crrOp3,
			Payload: &CommandEnvelope_FeatureActivation{FeatureActivation: &FeatureActivation{Feature: "compatibility-probe"}},
		},
	}
	for _, command := range commands {
		encoded, err := MarshalCommand(command)
		if err != nil {
			t.Fatalf("marshal %T: %v", command.Payload, err)
		}
		decoded, err := UnmarshalCommand(encoded)
		if err != nil {
			t.Fatalf("unmarshal %T: %v", command.Payload, err)
		}
		if !proto.Equal(command, decoded) {
			t.Fatalf("round trip %T = %v, want %v", command.Payload, decoded, command)
		}
	}

	invalid := []*CommandEnvelope{
		{
			SchemaMajor: 1, OperationId: crrOp2,
			Payload: &CommandEnvelope_MemberProfile{MemberProfile: &MemberProfileChange{ReleaseVersion: "1.2.0"}},
		},
		{
			SchemaMajor: 1, OperationId: crrOp2,
			Payload: &CommandEnvelope_MemberProfile{MemberProfile: &MemberProfileChange{
				MemberId: 3, ReleaseVersion: "v1.2.0",
			}},
		},
		{
			SchemaMajor: 1, OperationId: crrOp2,
			Payload: &CommandEnvelope_MemberProfile{MemberProfile: &MemberProfileChange{
				MemberId: 3, ReleaseVersion: "1.2.0", Features: []string{"zeta", "alpha"},
			}},
		},
		{
			SchemaMajor: 1, OperationId: crrOp3,
			Payload: &CommandEnvelope_FeatureActivation{FeatureActivation: &FeatureActivation{Feature: "Bad"}},
		},
	}
	for index, command := range invalid {
		if _, err := MarshalCommand(command); err == nil {
			t.Errorf("invalid profile command %d was accepted", index)
		}
	}
}

func TestCrrActiveFeaturesSnapshotRoundTripAndObjectCount(t *testing.T) {
	state := &ReplicatedStateSnapshot{ActiveFeatures: []string{"alpha", "compatibility-probe"}}
	encoded, err := MarshalSnapshot(state, 1)
	if err != nil {
		t.Fatalf("marshal active feature snapshot: %v", err)
	}
	envelope, err := UnmarshalSnapshot(encoded)
	if err != nil {
		t.Fatalf("unmarshal active feature snapshot: %v", err)
	}
	if envelope.ObjectCount != 2 || !slices.Equal(envelope.State.ActiveFeatures, state.ActiveFeatures) {
		t.Fatalf("active feature snapshot count=%d features=%q", envelope.ObjectCount, envelope.State.ActiveFeatures)
	}
}

func TestCrrMemberIdentityValidation(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*MemberIdentity)
		want   string
	}{
		{"zero_member_id", func(m *MemberIdentity) { m.MemberId = 0 }, "member_id must be positive"},
		{"empty_cluster", func(m *MemberIdentity) { m.ClusterId = "" }, "cluster_id"},
		{"long_cluster", func(m *MemberIdentity) { m.ClusterId = strings.Repeat("c", 201) }, "cluster_id"},
		{"invalid_utf8_cluster", func(m *MemberIdentity) { m.ClusterId = "\xff" }, "cluster_id"},
		{"empty_address", func(m *MemberIdentity) { m.Address = "" }, "address must be 1..255"},
		{"long_address", func(m *MemberIdentity) { m.Address = strings.Repeat("a", 250) + ".x:7000" }, "address must be 1..255"},
		{"missing_port", func(m *MemberIdentity) { m.Address = "node.example" }, "valid host:port"},
		{"empty_host", func(m *MemberIdentity) { m.Address = ":7000" }, "valid host:port"},
		{"empty_port", func(m *MemberIdentity) { m.Address = "node.example:" }, "valid host:port"},
		{"zero_port", func(m *MemberIdentity) { m.Address = "node.example:0" }, "port must be in 1..65535"},
		{"port_overflow", func(m *MemberIdentity) { m.Address = "node.example:65536" }, "port must be in 1..65535"},
		{"named_port", func(m *MemberIdentity) { m.Address = "node.example:http" }, "port must be in 1..65535"},
		{"star_host", func(m *MemberIdentity) { m.Address = "*:7000" }, "wildcard"},
		{"ipv4_unspecified", func(m *MemberIdentity) { m.Address = "0.0.0.0:7000" }, "wildcard"},
		{"ipv6_unspecified", func(m *MemberIdentity) { m.Address = "[::]:7000" }, "wildcard"},
		{"leading_hyphen_host", func(m *MemberIdentity) { m.Address = "-node.example:7000" }, "DNS name or IP literal"},
		{"underscore_host", func(m *MemberIdentity) { m.Address = "node_1.example:7000" }, "DNS name or IP literal"},
		{"empty_dns_san", func(m *MemberIdentity) { m.DnsSan = "" }, "dns_san"},
		{"ip_dns_san", func(m *MemberIdentity) { m.DnsSan = "10.0.0.1" }, "dns_san"},
		{"long_dns_san", func(m *MemberIdentity) { m.DnsSan = strings.Repeat("a.", 127) }, "dns_san"},
		{"long_label_dns_san", func(m *MemberIdentity) { m.DnsSan = strings.Repeat("a", 64) + ".example" }, "dns_san"},
		{"empty_label_dns_san", func(m *MemberIdentity) { m.DnsSan = "node..example" }, "dns_san"},
		{"trailing_hyphen_dns_san", func(m *MemberIdentity) { m.DnsSan = "node-.example" }, "dns_san"},
		{"wildcard_dns_san", func(m *MemberIdentity) { m.DnsSan = "*.example" }, "dns_san"},
		{"short_certificate", func(m *MemberIdentity) { m.CertificateSha256 = m.CertificateSha256[:31] }, "certificate_sha256"},
		{"missing_certificate", func(m *MemberIdentity) { m.CertificateSha256 = nil }, "certificate_sha256"},
		{"empty_release", func(m *MemberIdentity) { m.ReleaseVersion = "" }, "release_version"},
		{"long_release", func(m *MemberIdentity) { m.ReleaseVersion = strings.Repeat("v", 65) }, "release_version"},
		{"noncanonical_release", func(m *MemberIdentity) { m.ReleaseVersion = "v1.2.3" }, "release_version"},
		{"invalid_feature", func(m *MemberIdentity) { m.Features = []string{"Bad"} }, "member features"},
		{"unsorted_features", func(m *MemberIdentity) { m.Features = []string{"zeta", "alpha"} }, "member features"},
		{"protocol_major", func(m *MemberIdentity) { m.ClusterProtocolMajor = 3 }, "unsupported cluster protocol"},
		{"protocol_minor", func(m *MemberIdentity) { m.ClusterProtocolMinor = 1 }, "unsupported cluster protocol"},
		{"schema_major", func(m *MemberIdentity) { m.StateSchemaMajor = 2 }, "unsupported cluster protocol"},
		{"schema_minor", func(m *MemberIdentity) { m.StateSchemaMinor = 1 }, "unsupported cluster protocol"},
		{"phase_unspecified", func(m *MemberIdentity) { m.Phase = MemberPhase_MEMBER_PHASE_UNSPECIFIED }, "invalid member phase"},
		{"phase_out_of_range", func(m *MemberIdentity) { m.Phase = MemberPhase_MEMBER_PHASE_RETIRED + 1 }, "invalid member phase"},
		{"learner_without_operation", func(m *MemberIdentity) { m.Phase = MemberPhase_MEMBER_PHASE_LEARNER }, "in-progress member phase"},
		{"catching_up_bad_operation", func(m *MemberIdentity) { m.Phase, m.OperationId = MemberPhase_MEMBER_PHASE_CATCHING_UP, "op" }, "in-progress member phase"},
		{"leaving_without_operation", func(m *MemberIdentity) { m.Phase = MemberPhase_MEMBER_PHASE_LEAVING }, "in-progress member phase"},
		{"voter_with_operation", func(m *MemberIdentity) { m.OperationId = op1 }, "stable member phase"},
		{"retired_with_operation", func(m *MemberIdentity) { m.Phase, m.OperationId = MemberPhase_MEMBER_PHASE_RETIRED, op1 }, "stable member phase"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			member := crrVoter(1)
			tc.mutate(member)
			_, err := MarshalSnapshot(&ReplicatedStateSnapshot{Members: []*MemberIdentity{member}}, 1)
			crrWantErr(t, err, tc.want)
		})
	}

	accepted := map[string]func(*MemberIdentity){
		"ipv4_literal": func(m *MemberIdentity) { m.Address = "10.0.0.1:1" },
		"ipv6_literal": func(m *MemberIdentity) { m.Address = "[::1]:65535" },
		"max_bounds": func(m *MemberIdentity) {
			m.ClusterId, m.ReleaseVersion = strings.Repeat("c", 200), "4294967295.4294967295.4294967295"
		},
		"max_label":          func(m *MemberIdentity) { m.DnsSan = strings.Repeat("a", 63) + ".example" },
		"max_dns_san":        func(m *MemberIdentity) { m.DnsSan = strings.Repeat("a.", 126) + "a" },
		"catching_up":        func(m *MemberIdentity) { m.Phase, m.OperationId = MemberPhase_MEMBER_PHASE_CATCHING_UP, op1 },
		"leaving":            func(m *MemberIdentity) { m.Phase, m.OperationId = MemberPhase_MEMBER_PHASE_LEAVING, op1 },
		"retired":            func(m *MemberIdentity) { m.Phase = MemberPhase_MEMBER_PHASE_RETIRED },
		"hyphenated_address": func(m *MemberIdentity) { m.Address, m.DnsSan = "node-1.example:7000", "node-1.example" },
	}
	for name, mutate := range accepted {
		t.Run("accepts_"+name, func(t *testing.T) {
			member := crrVoter(1)
			mutate(member)
			wire, err := MarshalSnapshot(&ReplicatedStateSnapshot{Members: []*MemberIdentity{member}}, 1)
			if err != nil {
				t.Fatalf("valid member rejected: %v", err)
			}
			decoded, err := UnmarshalSnapshot(wire)
			if err != nil {
				t.Fatal(err)
			}
			if !proto.Equal(decoded.State.Members[0], member) {
				t.Fatalf("member round trip mismatch: %v", decoded.State.Members[0])
			}
		})
	}
}

func TestCrrOwnershipAndSubscriptionValidation(t *testing.T) {
	ownership := []struct {
		name   string
		mutate func(*OwnershipChange)
	}{
		{"empty_tenant", func(o *OwnershipChange) { o.TenantId = "" }},
		{"long_tenant", func(o *OwnershipChange) { o.TenantId = strings.Repeat("t", 129) }},
		{"invalid_utf8_tenant", func(o *OwnershipChange) { o.TenantId = "\xc3" }},
		{"empty_client", func(o *OwnershipChange) { o.ClientId = "" }},
		{"long_client", func(o *OwnershipChange) { o.ClientId = strings.Repeat("c", 257) }},
		{"zero_owner", func(o *OwnershipChange) { o.OwnerNodeId = 0 }},
		{"zero_epoch", func(o *OwnershipChange) { o.Epoch = 0 }},
		{"action_unspecified", func(o *OwnershipChange) { o.Action = OwnershipAction_OWNERSHIP_ACTION_UNSPECIFIED }},
		{"action_out_of_range", func(o *OwnershipChange) { o.Action = OwnershipAction_OWNERSHIP_ACTION_RELEASE + 1 }},
	}
	ownershipWant := map[string]string{"zero_owner": "must be positive", "zero_epoch": "must be positive", "action_unspecified": "invalid ownership action", "action_out_of_range": "invalid ownership action"}
	for _, tc := range ownership {
		t.Run("ownership_"+tc.name, func(t *testing.T) {
			change := crrOwnership()
			tc.mutate(change)
			want := ownershipWant[tc.name]
			if want == "" {
				want = "ownership tenant_id or client_id is out of bounds"
			}
			_, err := MarshalCommand(crrCommand(&CommandEnvelope_Ownership{Ownership: change}))
			crrWantErr(t, err, want)
		})
	}

	subscription := []struct {
		name   string
		mutate func(*SubscriptionChange)
		want   string
	}{
		{"empty_tenant", func(s *SubscriptionChange) { s.TenantId = "" }, "tenant_id or client_id"},
		{"long_client", func(s *SubscriptionChange) { s.ClientId = strings.Repeat("c", 257) }, "tenant_id or client_id"},
		{"zero_owner", func(s *SubscriptionChange) { s.OwnerNodeId = 0 }, "owner_node_id and epoch"},
		{"zero_epoch", func(s *SubscriptionChange) { s.Epoch = 0 }, "owner_node_id and epoch"},
		{"qos_three", func(s *SubscriptionChange) { s.Qos = 3 }, "QoS or retain handling"},
		{"retain_handling_three", func(s *SubscriptionChange) { s.RetainHandling = 3 }, "QoS or retain handling"},
		{"action_unspecified", func(s *SubscriptionChange) { s.Action = SubscriptionAction_SUBSCRIPTION_ACTION_UNSPECIFIED }, "invalid subscription action"},
		{"action_out_of_range", func(s *SubscriptionChange) { s.Action = SubscriptionAction_SUBSCRIPTION_ACTION_REMOVE + 1 }, "invalid subscription action"},
	}
	for _, tc := range subscription {
		t.Run("subscription_"+tc.name, func(t *testing.T) {
			change := crrSubscription()
			tc.mutate(change)
			_, err := MarshalCommand(crrCommand(&CommandEnvelope_Subscription{Subscription: change}))
			crrWantErr(t, err, tc.want)
		})
	}
}

func TestCrrSubscriptionFilterGrammar(t *testing.T) {
	rejected := []string{
		"", "a/#/b", "a+", "a/b#", "+a/b", "#/", "a\x00b", "\ufdd0", "a/\ufdef", "x\uffff", "x\ufffe", "\U0001fffe", "\xff",
		strings.Repeat("a", 65536), "$share/", "$share/group", "$share//a", "$share/group/", "$share/gr+up/a", "$share/gr#up/a",
	}
	for _, filter := range rejected {
		t.Run("rejects_"+filter[:min(len(filter), 16)], func(t *testing.T) {
			change := crrSubscription()
			change.Filter = filter
			_, err := MarshalCommand(crrCommand(&CommandEnvelope_Subscription{Subscription: change}))
			crrWantErr(t, err, "invalid MQTT subscription filter")
		})
	}
	accepted := []string{"#", "+", "/", "a//b", "a/+/c/#", "$SYS/+", "$share/g/#", "$share/g/a/+/c", "\u00e9/\U0001f600", strings.Repeat("a", 65535)}
	for _, filter := range accepted {
		t.Run("accepts_"+filter[:min(len(filter), 16)], func(t *testing.T) {
			change := crrSubscription()
			change.Filter = filter
			wire, err := MarshalCommand(crrCommand(&CommandEnvelope_Subscription{Subscription: change}))
			if err != nil {
				t.Fatalf("valid filter rejected: %v", err)
			}
			decoded, err := UnmarshalCommand(wire)
			if err != nil {
				t.Fatal(err)
			}
			if got := decoded.GetSubscription().GetFilter(); got != filter {
				t.Fatalf("filter round trip = %q", got)
			}
		})
	}
}

func TestCrrDeliveryChangeValidation(t *testing.T) {
	cases := []struct {
		name   string
		action DeliveryAction
		mutate func(*DeliveryChange)
		want   string
	}{
		{"empty_tenant", DeliveryAction_DELIVERY_ACTION_PUBLISH, func(c *DeliveryChange) { c.TenantId = "" }, "delivery tenant_id or client_id"},
		{"long_client", DeliveryAction_DELIVERY_ACTION_PUBLISH, func(c *DeliveryChange) { c.ClientId = strings.Repeat("c", 257) }, "delivery tenant_id or client_id"},
		{"unspecified_action", DeliveryAction_DELIVERY_ACTION_PUBLISH, func(c *DeliveryChange) { c.Action = DeliveryAction_DELIVERY_ACTION_UNSPECIFIED }, "invalid delivery action"},
		{"missing_proposed_at", DeliveryAction_DELIVERY_ACTION_PUBLISH, func(c *DeliveryChange) { c.ProposedAtUnixMillis = 0 }, "proposed_at_unix_millis must be positive"},
		{"publish_qos_zero", DeliveryAction_DELIVERY_ACTION_PUBLISH, func(c *DeliveryChange) { c.PublishQos = 0 }, "invalid delivery publish message"},
		{"publish_qos_three", DeliveryAction_DELIVERY_ACTION_PUBLISH, func(c *DeliveryChange) { c.PublishQos = 3 }, "publish_qos is out of range"},
		{"publish_wildcard_topic", DeliveryAction_DELIVERY_ACTION_PUBLISH, func(c *DeliveryChange) { c.Topic = "a/+" }, "invalid delivery publish message"},
		{"publish_oversize_payload", DeliveryAction_DELIVERY_ACTION_PUBLISH, func(c *DeliveryChange) { c.Payload = make([]byte, maxPublishPayload+1) }, "invalid delivery publish message"},
		{"publish_target_duplicate", DeliveryAction_DELIVERY_ACTION_PUBLISH, func(c *DeliveryChange) { c.Targets = []*DeliveryTarget{{ClientId: "target"}, {ClientId: "target"}} }, "duplicate delivery target"},
		{"receive_zero_owner", DeliveryAction_DELIVERY_ACTION_RECEIVE, func(c *DeliveryChange) { c.OwnerNodeId = 0 }, "invalid delivery receive command fields"},
		{"receive_packet_id_overflow", DeliveryAction_DELIVERY_ACTION_RECEIVE, func(c *DeliveryChange) { c.PacketId = 65536 }, "invalid delivery receive command fields"},
		{"receive_invalid_topic", DeliveryAction_DELIVERY_ACTION_RECEIVE, func(c *DeliveryChange) { c.Topic = "#" }, "invalid delivery receive message"},
		{"receive_publish_qos_forbidden", DeliveryAction_DELIVERY_ACTION_RECEIVE, func(c *DeliveryChange) { c.PublishQos = 1 }, "invalid delivery receive command fields"},
		{"release_requires_qos2", DeliveryAction_DELIVERY_ACTION_RELEASE, func(c *DeliveryChange) { c.PublishQos = 1 }, "invalid delivery release command fields"},
		{"send_requires_sequence", DeliveryAction_DELIVERY_ACTION_SEND, func(c *DeliveryChange) { c.Sequence = 0 }, "invalid delivery send command fields"},
		{"advance_rejects_topic", DeliveryAction_DELIVERY_ACTION_ADVANCE, func(c *DeliveryChange) { c.Topic = "a/b" }, "invalid delivery acknowledgement command fields"},
		{"attach_rejects_packet", DeliveryAction_DELIVERY_ACTION_ATTACH, func(c *DeliveryChange) { c.PacketId = 1 }, "invalid delivery attach command fields"},
		{"discard_rejects_reset", DeliveryAction_DELIVERY_ACTION_DISCARD, func(c *DeliveryChange) { c.Reset_ = true }, "invalid delivery discard command fields"},
		{"nil_target", DeliveryAction_DELIVERY_ACTION_PUBLISH, func(c *DeliveryChange) { c.Targets = []*DeliveryTarget{nil} }, "invalid delivery target client_id"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			change := crrDelivery(tc.action)
			switch tc.action {
			case DeliveryAction_DELIVERY_ACTION_PUBLISH:
				change.Targets = []*DeliveryTarget{{ClientId: "target"}}
			case DeliveryAction_DELIVERY_ACTION_SEND:
				change.OwnerNodeId, change.Epoch, change.Sequence, change.PacketId = 1, 1, 1, 2
			case DeliveryAction_DELIVERY_ACTION_ADVANCE, DeliveryAction_DELIVERY_ACTION_COMPLETE:
				change.OwnerNodeId, change.Epoch, change.PacketId = 1, 1, 2
			case DeliveryAction_DELIVERY_ACTION_ATTACH, DeliveryAction_DELIVERY_ACTION_DETACH, DeliveryAction_DELIVERY_ACTION_DISCARD:
				change.OwnerNodeId, change.Epoch = 1, 1
			}
			tc.mutate(change)
			_, err := MarshalCommand(crrDeliveryCommand(change))
			crrWantErr(t, err, tc.want)
		})
	}
}
func TestCrrCommandSizeLimits(t *testing.T) {
	big := crrDelivery(DeliveryAction_DELIVERY_ACTION_PUBLISH)
	big.Payload = make([]byte, maxPublishPayload)
	big.Targets = make([]*DeliveryTarget, 256)
	for id := range big.Targets {
		suffix := string([]byte{byte('a' + id/26), byte('a' + id%26)})
		big.Targets[id] = &DeliveryTarget{ClientId: strings.Repeat("c", 254) + suffix}
	}
	_, err := MarshalCommand(crrDeliveryCommand(big))
	crrWantErr(t, err, "command exceeds 1 MiB limit")

	_, err = UnmarshalCommand(make([]byte, maxCommandBytes+1))
	crrWantErr(t, err, "command exceeds 1 MiB limit")
}

func TestCrrUnmarshalCommandRejectsDecodedButInvalidInput(t *testing.T) {
	encode := func(t *testing.T, command *CommandEnvelope) []byte {
		t.Helper()
		wire, err := deterministic.Marshal(command)
		if err != nil {
			t.Fatal(err)
		}
		return wire
	}
	nestedUnknown := crrOwnership()
	nestedUnknown.ProtoReflect().SetUnknown(crrUnknownField)
	emptyMembership := crrCommand(&CommandEnvelope_Membership{Membership: &MembershipRecord{}})
	cases := []struct {
		name string
		wire []byte
		want string
	}{
		{"schema_two", encode(t, &CommandEnvelope{SchemaMajor: 2, OperationId: op1, Payload: &CommandEnvelope_Ownership{Ownership: crrOwnership()}}), "unsupported command schema"},
		{"no_payload", encode(t, &CommandEnvelope{SchemaMajor: 1, OperationId: op1}), "exactly one known payload"},
		{"empty_membership", encode(t, emptyMembership), "does not match command"},
		{"nested_unknown", encode(t, crrCommand(&CommandEnvelope_Ownership{Ownership: nestedUnknown})), "unknown fields"},
		{"invalid_filter", encode(t, crrCommand(&CommandEnvelope_Subscription{Subscription: &SubscriptionChange{TenantId: "t", ClientId: "c", OwnerNodeId: 1, Epoch: 1, Filter: "a#", Action: SubscriptionAction_SUBSCRIPTION_ACTION_ADD}})), "invalid MQTT subscription filter"},
		{"truncated", encode(t, crrCommand(&CommandEnvelope_Ownership{Ownership: crrOwnership()}))[:10], "unmarshal command"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			decoded, err := UnmarshalCommand(tc.wire)
			crrWantErr(t, err, tc.want)
			if decoded != nil {
				t.Fatal("rejected command returned a value")
			}
		})
	}
}

func TestCrrSnapshotRoundTripSortsEveryCollection(t *testing.T) {
	catchingUp := crrVoter(3)
	catchingUp.Phase, catchingUp.OperationId, catchingUp.Address = MemberPhase_MEMBER_PHASE_CATCHING_UP, crrOp3, "10.0.0.3:7000"
	subscription := func(tenant, client, filter string) *SubscriptionState {
		return &SubscriptionState{TenantId: tenant, ClientId: client, OwnerNodeId: 1, Epoch: 1, Filter: filter, Qos: 2, RetainHandling: 1}
	}
	session := func(tenant, client string) *DeliverySessionState {
		value := crrDeliverySession(tenant, client)
		value.Messages[0], value.Messages[1] = value.Messages[1], value.Messages[0]
		value.Inbound = append(value.Inbound, &InboundMessage{PacketId: 3, Topic: "a/b", Payload: []byte("third")})
		value.Inbound[0], value.Inbound[1] = value.Inbound[1], value.Inbound[0]
		return value
	}
	state := &ReplicatedStateSnapshot{
		Members: []*MemberIdentity{crrVoter(7), catchingUp, crrVoter(1)},
		Ownerships: []*OwnershipState{
			{TenantId: "t2", ClientId: "a", OwnerNodeId: 2, Epoch: 1, CommittedIndex: 4},
			{TenantId: "t1", ClientId: "b", OwnerNodeId: 1, Epoch: 3},
			{TenantId: "t1", ClientId: "a", OwnerNodeId: 1, Epoch: 2},
		},
		Subscriptions: []*SubscriptionState{
			subscription("t2", "a", "x"), subscription("t1", "b", "x"), subscription("t1", "a", "y"), subscription("t1", "a", "x"),
		},
		DeliverySessions:  []*DeliverySessionState{session("t2", "a"), session("t1", "b"), session("t1", "a")},
		MembershipHistory: []*MembershipRecord{crrHistory(crrOp3, 9), crrHistory(op1, 2), crrHistory(crrOp2, 5)},
	}
	before := proto.Clone(state)
	wire, err := MarshalSnapshot(state, 9)
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(before, state) {
		t.Fatal("MarshalSnapshot modified its input")
	}
	envelope, err := UnmarshalSnapshot(wire)
	if err != nil {
		t.Fatal(err)
	}
	if envelope.SchemaMajor != 1 || envelope.LastIncludedIndex != 9 || envelope.ObjectCount != 28 {
		t.Fatalf("envelope header = schema %d index %d count %d", envelope.SchemaMajor, envelope.LastIncludedIndex, envelope.ObjectCount)
	}
	got := envelope.State
	if ids := [3]uint64{got.Members[0].MemberId, got.Members[1].MemberId, got.Members[2].MemberId}; ids != [3]uint64{1, 3, 7} {
		t.Fatalf("member order = %v", ids)
	}
	var owners, subs, sessions, history []string
	for _, o := range got.Ownerships {
		owners = append(owners, o.TenantId+"/"+o.ClientId)
	}
	for _, s := range got.Subscriptions {
		subs = append(subs, s.TenantId+"/"+s.ClientId+"/"+s.Filter)
	}
	for _, s := range got.DeliverySessions {
		sessions = append(sessions, s.TenantId+"/"+s.ClientId)
	}
	for _, h := range got.MembershipHistory {
		history = append(history, h.OperationId)
	}
	for name, pair := range map[string][2]string{
		"ownerships":        {strings.Join(owners, ","), "t1/a,t1/b,t2/a"},
		"subscriptions":     {strings.Join(subs, ","), "t1/a/x,t1/a/y,t1/b/x,t2/a/x"},
		"delivery_sessions": {strings.Join(sessions, ","), "t1/a,t1/b,t2/a"},
		"history":           {strings.Join(history, ","), strings.Join([]string{op1, crrOp2, crrOp3}, ",")},
	} {
		if pair[0] != pair[1] {
			t.Fatalf("%s order = %s, want %s", name, pair[0], pair[1])
		}
	}
	if first := got.DeliverySessions[0]; first.Messages[0].Sequence != 1 || first.Inbound[0].PacketId != 3 {
		t.Fatalf("delivery records not sorted: %v", first)
	}
	again, err := MarshalSnapshot(got, 9)
	if err != nil || !bytes.Equal(again, wire) {
		t.Fatalf("restored snapshot does not re-encode identically: err=%v", err)
	}
}

func TestCrrMarshalSnapshotRejectsInvalidState(t *testing.T) {
	if _, err := MarshalSnapshot(nil, 1); err == nil || !strings.Contains(err.Error(), "snapshot state is nil") {
		t.Fatalf("nil state error = %v", err)
	}
	withUnknownMember := crrVoter(1)
	withUnknownMember.ProtoReflect().SetUnknown(crrUnknownField)
	withUnknownHistory := crrHistory(op1, 1)
	withUnknownHistory.Member.ProtoReflect().SetUnknown(crrUnknownField)
	topUnknown := &ReplicatedStateSnapshot{}
	topUnknown.ProtoReflect().SetUnknown(crrUnknownField)
	badHistoryOperation := crrHistory(op1, 1)
	badHistoryOperation.OperationId = strings.ToUpper("0000000a-0000-4000-8000-000000000001")
	badHistoryOperation.Member = nil
	badHistoryOperation.Action = MembershipAction_MEMBERSHIP_ACTION_ABORT_STAGE
	invalidHistory := crrHistory(op1, 1)
	invalidHistory.Action = MembershipAction_MEMBERSHIP_ACTION_UNSPECIFIED
	ownership := func(mutate func(*OwnershipState)) *ReplicatedStateSnapshot {
		o := &OwnershipState{TenantId: "t", ClientId: "c", OwnerNodeId: 1, Epoch: 1}
		mutate(o)
		return &ReplicatedStateSnapshot{Ownerships: []*OwnershipState{o}}
	}
	subscription := func(mutate func(*SubscriptionState)) *ReplicatedStateSnapshot {
		s := &SubscriptionState{TenantId: "t", ClientId: "c", OwnerNodeId: 1, Epoch: 1, Filter: "a"}
		mutate(s)
		return &ReplicatedStateSnapshot{Subscriptions: []*SubscriptionState{s}}
	}
	withUnknownDelivery := crrDeliverySession("tenant", "client")
	withUnknownDelivery.Messages[0].ProtoReflect().SetUnknown(crrUnknownField)
	invalidDeliveryIdentity := crrDeliverySession("tenant", "client")
	invalidDeliveryIdentity.TenantId = ""
	invalidDeliveryPhase := crrDeliverySession("tenant", "client")
	invalidDeliveryPhase.Messages[0].Phase = DeliveryPhase_DELIVERY_PHASE_QUEUED
	invalidInbound := crrDeliverySession("tenant", "client")
	invalidInbound.Inbound[0].Released, invalidInbound.Inbound[0].ReleasedIndex = true, 3

	cases := []struct {
		name       string
		state      *ReplicatedStateSnapshot
		want       string
		classified bool
	}{
		{"state_unknown", topUnknown, "unknown fields", true},
		{"member_unknown", &ReplicatedStateSnapshot{Members: []*MemberIdentity{withUnknownMember}}, "unknown fields", true},
		{"history_member_unknown", &ReplicatedStateSnapshot{MembershipHistory: []*MembershipRecord{withUnknownHistory}}, "unknown fields", true},
		{"invalid_member", &ReplicatedStateSnapshot{Members: []*MemberIdentity{crrVoter(0)}}, "member_id must be positive", false},
		{"active_features_unsorted", &ReplicatedStateSnapshot{ActiveFeatures: []string{"zeta", "alpha"}}, "active_features", false},
		{"ownership_empty_tenant", ownership(func(o *OwnershipState) { o.TenantId = "" }), "invalid snapshot ownership", false},
		{"ownership_long_client", ownership(func(o *OwnershipState) { o.ClientId = strings.Repeat("c", 257) }), "invalid snapshot ownership", false},
		{"ownership_zero_owner", ownership(func(o *OwnershipState) { o.OwnerNodeId = 0 }), "invalid snapshot ownership", false},
		{"ownership_zero_epoch", ownership(func(o *OwnershipState) { o.Epoch = 0 }), "invalid snapshot ownership", false},
		{"subscription_long_tenant", subscription(func(s *SubscriptionState) { s.TenantId = strings.Repeat("t", 129) }), "invalid snapshot subscription", false},
		{"subscription_empty_client", subscription(func(s *SubscriptionState) { s.ClientId = "" }), "invalid snapshot subscription", false},
		{"subscription_zero_owner", subscription(func(s *SubscriptionState) { s.OwnerNodeId = 0 }), "invalid snapshot subscription", false},
		{"subscription_zero_epoch", subscription(func(s *SubscriptionState) { s.Epoch = 0 }), "invalid snapshot subscription", false},
		{"subscription_bad_filter", subscription(func(s *SubscriptionState) { s.Filter = "#/a" }), "invalid snapshot subscription", false},
		{"subscription_qos", subscription(func(s *SubscriptionState) { s.Qos = 3 }), "invalid snapshot subscription", false},
		{"subscription_retain_handling", subscription(func(s *SubscriptionState) { s.RetainHandling = 3 }), "invalid snapshot subscription", false},
		{"duplicate_subscription", &ReplicatedStateSnapshot{Subscriptions: []*SubscriptionState{
			{TenantId: "t", ClientId: "c", OwnerNodeId: 1, Epoch: 1, Filter: "a"},
			{TenantId: "t", ClientId: "c", OwnerNodeId: 2, Epoch: 2, Filter: "a"},
		}}, "snapshot subscriptions are not uniquely sorted", false},
		{"delivery_message_unknown", &ReplicatedStateSnapshot{DeliverySessions: []*DeliverySessionState{withUnknownDelivery}}, "unknown fields", true},
		{"invalid_delivery_identity", &ReplicatedStateSnapshot{DeliverySessions: []*DeliverySessionState{invalidDeliveryIdentity}}, "invalid snapshot delivery session identity or sequence", false},
		{"invalid_delivery_phase", &ReplicatedStateSnapshot{DeliverySessions: []*DeliverySessionState{invalidDeliveryPhase}}, "delivery phase conflicts with QoS", false},
		{"invalid_inbound_release_state", &ReplicatedStateSnapshot{DeliverySessions: []*DeliverySessionState{invalidInbound}}, "invalid snapshot inbound release state", false},
		{"duplicate_delivery_session", &ReplicatedStateSnapshot{DeliverySessions: []*DeliverySessionState{crrDeliverySession("t", "c"), crrDeliverySession("t", "c")}}, "snapshot delivery sessions are not uniquely sorted", false},
		{"history_bad_operation", &ReplicatedStateSnapshot{MembershipHistory: []*MembershipRecord{badHistoryOperation}}, "membership operation_id must be a canonical", false},
		{"history_invalid_action", &ReplicatedStateSnapshot{MembershipHistory: []*MembershipRecord{invalidHistory}}, "invalid membership action", false},
		{"history_uncommitted", &ReplicatedStateSnapshot{MembershipHistory: []*MembershipRecord{crrHistory(op1, 0)}}, "committed_index must be positive", false},
		{"duplicate_history", &ReplicatedStateSnapshot{MembershipHistory: []*MembershipRecord{crrHistory(op1, 1), crrHistory(op1, 1)}}, "snapshot membership history is not uniquely sorted", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			wire, err := MarshalSnapshot(tc.state, 10)
			crrWantErr(t, err, tc.want)
			if errors.Is(err, ErrIncompatibleStateSchema) != tc.classified {
				t.Fatalf("schema classification = %v, want %v: %v", errors.Is(err, ErrIncompatibleStateSchema), tc.classified, err)
			}
			if wire != nil {
				t.Fatal("rejected snapshot returned bytes")
			}
		})
	}
}

func TestCrrUnmarshalSnapshotRejectsInvalidEnvelope(t *testing.T) {
	valid, err := MarshalSnapshot(&ReplicatedStateSnapshot{Ownerships: []*OwnershipState{{TenantId: "t", ClientId: "c", OwnerNodeId: 1, Epoch: 1}}}, 3)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := UnmarshalSnapshot(valid)
	if err != nil {
		t.Fatal(err)
	}
	reencode := func(t *testing.T, mutate func(*SnapshotEnvelope)) []byte {
		t.Helper()
		envelope := proto.Clone(decoded).(*SnapshotEnvelope)
		mutate(envelope)
		wire, err := deterministic.Marshal(envelope)
		if err != nil {
			t.Fatal(err)
		}
		return wire
	}
	cases := []struct {
		name       string
		wire       []byte
		want       string
		classified bool
	}{
		{"malformed", []byte{0xff}, "unmarshal snapshot", false},
		{"truncated", valid[:len(valid)-1], "unmarshal snapshot", false},
		{"schema_minor", reencode(t, func(e *SnapshotEnvelope) { e.SchemaMinor = 1 }), "unsupported snapshot schema", true},
		{"schema_major_zero", reencode(t, func(e *SnapshotEnvelope) { e.SchemaMajor = 0 }), "unsupported snapshot schema", true},
		{"missing_state", reencode(t, func(e *SnapshotEnvelope) { e.State = nil }), "snapshot state is required", false},
		{"short_digest", reencode(t, func(e *SnapshotEnvelope) { e.Sha256 = e.Sha256[:31] }), "SHA-256 must be 32 bytes", false},
		{"invalid_state", reencode(t, func(e *SnapshotEnvelope) { e.State.Ownerships[0].Epoch = 0 }), "invalid snapshot ownership", false},
		{"explicit_default_field", append(append([]byte(nil), valid...), 0x10, 0x00), "not canonical", false},
		{"state_before_header", append(append([]byte(nil), valid[2:]...), valid[:2]...), "not canonical", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			envelope, err := UnmarshalSnapshot(tc.wire)
			crrWantErr(t, err, tc.want)
			if errors.Is(err, ErrIncompatibleStateSchema) != tc.classified {
				t.Fatalf("schema classification = %v, want %v: %v", errors.Is(err, ErrIncompatibleStateSchema), tc.classified, err)
			}
			if envelope != nil {
				t.Fatal("rejected snapshot returned a value")
			}
		})
	}
}

func TestCrrUnmarshalSnapshotRejectsUnsortedForeignState(t *testing.T) {
	cases := []struct {
		name  string
		state *ReplicatedStateSnapshot
		want  string
	}{
		{"members", &ReplicatedStateSnapshot{Members: []*MemberIdentity{crrVoter(2), crrVoter(1)}}, "snapshot members are not uniquely sorted"},
		{"ownerships_by_tenant", &ReplicatedStateSnapshot{Ownerships: []*OwnershipState{
			{TenantId: "b", ClientId: "a", OwnerNodeId: 1, Epoch: 1}, {TenantId: "a", ClientId: "z", OwnerNodeId: 1, Epoch: 1},
		}}, "snapshot ownerships are not uniquely sorted"},
		{"subscriptions_by_filter", &ReplicatedStateSnapshot{Subscriptions: []*SubscriptionState{
			{TenantId: "t", ClientId: "c", OwnerNodeId: 1, Epoch: 1, Filter: "b"}, {TenantId: "t", ClientId: "c", OwnerNodeId: 1, Epoch: 1, Filter: "a"},
		}}, "snapshot subscriptions are not uniquely sorted"},
		{"subscriptions_by_client", &ReplicatedStateSnapshot{Subscriptions: []*SubscriptionState{
			{TenantId: "t", ClientId: "d", OwnerNodeId: 1, Epoch: 1, Filter: "a"}, {TenantId: "t", ClientId: "c", OwnerNodeId: 1, Epoch: 1, Filter: "b"},
		}}, "snapshot subscriptions are not uniquely sorted"},
		{"delivery_sessions", &ReplicatedStateSnapshot{DeliverySessions: func() []*DeliverySessionState {
			first, second := crrDeliverySession("t", "z"), crrDeliverySession("t", "a")
			return []*DeliverySessionState{first, second}
		}()}, "snapshot delivery sessions are not uniquely sorted"},
		{"delivery_messages", &ReplicatedStateSnapshot{DeliverySessions: func() []*DeliverySessionState {
			session := crrDeliverySession("t", "c")
			session.Messages[0], session.Messages[1] = session.Messages[1], session.Messages[0]
			return []*DeliverySessionState{session}
		}()}, "snapshot delivery messages are not strictly ordered by sequence"},
		{"delivery_inbound", &ReplicatedStateSnapshot{DeliverySessions: func() []*DeliverySessionState {
			session := crrDeliverySession("t", "c")
			session.Inbound = append(session.Inbound, &InboundMessage{PacketId: 3, Topic: "a/b"})
			return []*DeliverySessionState{session}
		}()}, "snapshot inbound messages are not strictly ordered by packet ID"},
		{"history", &ReplicatedStateSnapshot{MembershipHistory: []*MembershipRecord{crrHistory(crrOp2, 1), crrHistory(op1, 1)}}, "snapshot membership history is not uniquely sorted"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := UnmarshalSnapshot(crrRawSnapshot(t, tc.state, 5))
			crrWantErr(t, err, tc.want)
		})
	}
}

func TestCrrSnapshotSizeLimits(t *testing.T) {
	_, err := UnmarshalSnapshot(make([]byte, maxSnapshotBytes+1))
	crrWantErr(t, err, "snapshot exceeds 64 MiB limit")

}

func TestCrrUnmarshalSnapshotRejectsTooManyEntries(t *testing.T) {
	var state []byte
	for i := 0; i <= maxEntries; i++ {
		state = protowire.AppendTag(state, 2, protowire.BytesType)
		state = protowire.AppendVarint(state, 0)
	}
	var wire []byte
	wire = protowire.AppendTag(wire, 1, protowire.VarintType)
	wire = protowire.AppendVarint(wire, 1)
	wire = protowire.AppendTag(wire, 3, protowire.VarintType)
	wire = protowire.AppendVarint(wire, 1)
	wire = protowire.AppendTag(wire, 4, protowire.VarintType)
	wire = protowire.AppendVarint(wire, maxEntries+1)
	wire = protowire.AppendTag(wire, 5, protowire.BytesType)
	wire = protowire.AppendBytes(wire, make([]byte, sha256.Size))
	wire = protowire.AppendTag(wire, 6, protowire.BytesType)
	wire = protowire.AppendBytes(wire, state)
	_, err := UnmarshalSnapshot(wire)
	crrWantErr(t, err, "exceeds 1,000,000 entries")
}
