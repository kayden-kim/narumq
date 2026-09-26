package records

import (
	"bytes"
	"crypto/sha256"
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
		CertificateSha256: bytes.Repeat([]byte{0xab}, 32), ReleaseVersion: "v1.2.3",
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

func crrPublish(qos, packetID uint32) *PublishPacket {
	return &PublishPacket{ProtocolVersion: 5, Qos: qos, PacketId: packetID, Topic: "a/b", Payload: []byte("payload")}
}

func crrCheckpoint() *SessionCheckpoint {
	return &SessionCheckpoint{
		TenantId: "tenant", ClientId: "client", OwnerNodeId: 1, Epoch: 1,
		Inbound: []*InboundCheckpoint{
			{Publish: crrPublish(1, 1), Phase: InboundDeliveryPhase_INBOUND_QOS1_AWAIT_ACCEPTANCE},
			{Publish: crrPublish(2, 2), Phase: InboundDeliveryPhase_INBOUND_QOS2_AWAIT_RELEASE},
		},
		Outbound: []*OutboundCheckpoint{
			{Publish: crrPublish(1, 3), Phase: OutboundDeliveryPhase_OUTBOUND_AWAIT_PUBACK},
			{Publish: crrPublish(2, 4), Phase: OutboundDeliveryPhase_OUTBOUND_AWAIT_PUBCOMP},
		},
	}
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
	return uint64(len(state.Members) + len(state.Ownerships) + len(state.Subscriptions) + len(state.Checkpoints) + len(state.MembershipHistory) + len(state.Retained))
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
	v4 := crrPublish(1, 9)
	v4.ProtocolVersion = 4
	v4.Retain, v4.Dup = true, true
	expiring := crrPublish(2, 10)
	expiring.HasMessageExpiryInterval, expiring.MessageExpiryInterval = true, 60
	maxPayload := crrPublish(1, 11)
	maxPayload.Payload = bytes.Repeat([]byte{7}, maxPublishPayload)

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
	completed := &SessionCheckpoint{
		TenantId: "tenant", ClientId: "client", OwnerNodeId: 2, Epoch: 3,
		Inbound: []*InboundCheckpoint{
			{Publish: v4, Phase: InboundDeliveryPhase_INBOUND_QOS1_ACCEPTED},
			{Publish: expiring, Phase: InboundDeliveryPhase_INBOUND_QOS2_COMPLETE},
			{Publish: maxPayload, Phase: InboundDeliveryPhase_INBOUND_QOS1_AWAIT_ACCEPTANCE},
		},
		Outbound: []*OutboundCheckpoint{
			{Publish: crrPublish(1, 1), Phase: OutboundDeliveryPhase_OUTBOUND_COMPLETE},
			{Publish: crrPublish(2, 2), Phase: OutboundDeliveryPhase_OUTBOUND_AWAIT_PUBREC},
			{Publish: crrPublish(2, 3), Phase: OutboundDeliveryPhase_OUTBOUND_COMPLETE},
		},
	}
	membership := func(action MembershipAction, member *MemberIdentity, target uint64) *CommandEnvelope {
		return crrMembershipCommand(&MembershipRecord{OperationId: crrOp2, Action: action, IdempotencyKey: strings.Repeat("~", 128), Member: member, TargetMemberId: target})
	}

	cases := map[string]*CommandEnvelope{
		"stage_join":      crrMembershipCommand(crrStageJoin()),
		"add_learner":     membership(MembershipAction_MEMBERSHIP_ACTION_ADD_LEARNER, learner, 0),
		"promote_voter":   membership(MembershipAction_MEMBERSHIP_ACTION_PROMOTE_VOTER, nil, 0),
		"begin_remove":    membership(MembershipAction_MEMBERSHIP_ACTION_BEGIN_REMOVE, nil, 5),
		"remove_member":   membership(MembershipAction_MEMBERSHIP_ACTION_REMOVE_MEMBER, nil, 5),
		"abort_stage":     membership(MembershipAction_MEMBERSHIP_ACTION_ABORT_STAGE, nil, 0),
		"ownership":       crrCommand(&CommandEnvelope_Ownership{Ownership: crrOwnership()}),
		"transfer":        crrCommand(&CommandEnvelope_Ownership{Ownership: transfer}),
		"release":         crrCommand(&CommandEnvelope_Ownership{Ownership: release}),
		"shared_filter":   crrCommand(&CommandEnvelope_Subscription{Subscription: shared}),
		"unsubscribe":     crrCommand(&CommandEnvelope_Subscription{Subscription: remove}),
		"checkpoint":      crrCommand(&CommandEnvelope_Checkpoint{Checkpoint: crrCheckpoint()}),
		"checkpoint_done": crrCommand(&CommandEnvelope_Checkpoint{Checkpoint: completed}),
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
	listUnknown := crrCheckpoint()
	listUnknown.Outbound[1].ProtoReflect().SetUnknown(crrUnknownField)
	deepUnknown := crrCheckpoint()
	deepUnknown.Inbound[0].Publish.ProtoReflect().SetUnknown(crrUnknownField)
	topUnknown := crrCommand(&CommandEnvelope_Ownership{Ownership: crrOwnership()})
	topUnknown.ProtoReflect().SetUnknown(crrUnknownField)

	cases := []struct {
		name    string
		command *CommandEnvelope
		want    string
	}{
		{"top_level_unknown", topUnknown, "unknown fields"},
		{"nested_unknown", crrCommand(&CommandEnvelope_Ownership{Ownership: nestedUnknown}), "unknown fields"},
		{"list_element_unknown", crrCommand(&CommandEnvelope_Checkpoint{Checkpoint: listUnknown}), "unknown fields"},
		{"deep_unknown", crrCommand(&CommandEnvelope_Checkpoint{Checkpoint: deepUnknown}), "unknown fields"},
		{"schema_major_zero", &CommandEnvelope{OperationId: op1, Payload: &CommandEnvelope_Ownership{Ownership: crrOwnership()}}, "unsupported command schema"},
		{"schema_major_two", &CommandEnvelope{SchemaMajor: 2, OperationId: op1, Payload: &CommandEnvelope_Ownership{Ownership: crrOwnership()}}, "unsupported command schema"},
		{"empty_operation_id", &CommandEnvelope{SchemaMajor: 1, Payload: &CommandEnvelope_Ownership{Ownership: crrOwnership()}}, "operation_id must be a canonical"},
		{"misplaced_dash", &CommandEnvelope{SchemaMajor: 1, OperationId: "000000000-000-4000-8000-000000000001", Payload: &CommandEnvelope_Ownership{Ownership: crrOwnership()}}, "operation_id must be a canonical"},
		{"non_hex", &CommandEnvelope{SchemaMajor: 1, OperationId: "00000000-0000-4000-8000-00000000000g", Payload: &CommandEnvelope_Ownership{Ownership: crrOwnership()}}, "operation_id must be a canonical"},
		{"no_payload", &CommandEnvelope{SchemaMajor: 1, OperationId: op1}, "exactly one known payload"},
		{"nil_membership", crrCommand(&CommandEnvelope_Membership{}), "membership payload is required"},
		{"nil_ownership", crrCommand(&CommandEnvelope_Ownership{}), "ownership payload is required"},
		{"nil_subscription", crrCommand(&CommandEnvelope_Subscription{}), "subscription payload is required"},
		{"nil_checkpoint", crrCommand(&CommandEnvelope_Checkpoint{}), "checkpoint payload is required"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := MarshalCommand(tc.command)
			crrWantErr(t, err, tc.want)
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
			m.ClusterId, m.ReleaseVersion = strings.Repeat("c", 200), strings.Repeat("v", 64)
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

func TestCrrCheckpointValidation(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*SessionCheckpoint)
		want   string
	}{
		{"empty_tenant", func(c *SessionCheckpoint) { c.TenantId = "" }, "checkpoint tenant_id or client_id"},
		{"long_client", func(c *SessionCheckpoint) { c.ClientId = strings.Repeat("c", 257) }, "checkpoint tenant_id or client_id"},
		{"zero_owner", func(c *SessionCheckpoint) { c.OwnerNodeId = 0 }, "checkpoint owner_node_id and epoch"},
		{"zero_epoch", func(c *SessionCheckpoint) { c.Epoch = 0 }, "checkpoint owner_node_id and epoch"},
		{"nil_inbound", func(c *SessionCheckpoint) { c.Inbound[0] = nil }, "inbound checkpoint requires publish"},
		{"inbound_without_publish", func(c *SessionCheckpoint) { c.Inbound[1].Publish = nil }, "inbound checkpoint requires publish"},
		{"protocol_version_three", func(c *SessionCheckpoint) { c.Inbound[0].Publish.ProtocolVersion = 3 }, "protocol_version must be 4 or 5"},
		{"qos_three", func(c *SessionCheckpoint) { c.Inbound[0].Publish.Qos = 3 }, "QoS and packet_id are inconsistent"},
		{"qos1_without_packet_id", func(c *SessionCheckpoint) { c.Inbound[0].Publish.PacketId = 0 }, "QoS and packet_id are inconsistent"},
		{"qos0_with_packet_id", func(c *SessionCheckpoint) { c.Inbound[0].Publish.Qos = 0 }, "QoS and packet_id are inconsistent"},
		{"qos0_in_checkpoint", func(c *SessionCheckpoint) { c.Outbound[0].Publish.Qos, c.Outbound[0].Publish.PacketId = 0, 0 }, "checkpoint publish QoS must be 1 or 2"},
		{"v4_message_expiry", func(c *SessionCheckpoint) {
			c.Inbound[0].Publish.ProtocolVersion, c.Inbound[0].Publish.HasMessageExpiryInterval = 4, true
		}, "cannot carry message expiry"},
		{"wildcard_topic_plus", func(c *SessionCheckpoint) { c.Inbound[0].Publish.Topic = "a/+" }, "invalid MQTT topic"},
		{"wildcard_topic_hash", func(c *SessionCheckpoint) { c.Outbound[0].Publish.Topic = "#" }, "invalid MQTT topic"},
		{"empty_topic", func(c *SessionCheckpoint) { c.Inbound[0].Publish.Topic = "" }, "invalid MQTT topic"},
		{"noncharacter_topic", func(c *SessionCheckpoint) { c.Inbound[0].Publish.Topic = "a\ufdd5" }, "invalid MQTT topic"},
		{"oversize_payload", func(c *SessionCheckpoint) { c.Inbound[0].Publish.Payload = make([]byte, maxPublishPayload+1) }, "payload exceeds 1,000,000 bytes"},
		{"expiry_value_without_flag", func(c *SessionCheckpoint) { c.Inbound[0].Publish.MessageExpiryInterval = 5 }, "must be zero when absent"},
		{"inbound_phase_unspecified", func(c *SessionCheckpoint) { c.Inbound[0].Phase = InboundDeliveryPhase_INBOUND_PHASE_UNSPECIFIED }, "invalid inbound delivery phase"},
		{"inbound_phase_out_of_range", func(c *SessionCheckpoint) { c.Inbound[1].Phase = InboundDeliveryPhase_INBOUND_QOS2_COMPLETE + 1 }, "invalid inbound delivery phase"},
		{"qos1_with_qos2_phase", func(c *SessionCheckpoint) { c.Inbound[0].Phase = InboundDeliveryPhase_INBOUND_QOS2_AWAIT_ACCEPTANCE }, "inbound delivery phase conflicts with QoS"},
		{"qos2_with_qos1_phase", func(c *SessionCheckpoint) { c.Inbound[1].Phase = InboundDeliveryPhase_INBOUND_QOS1_ACCEPTED }, "inbound delivery phase conflicts with QoS"},
		{"duplicate_inbound", func(c *SessionCheckpoint) { c.Inbound[1].Publish.PacketId = 1 }, "duplicate inbound packet ID"},
		{"unsorted_inbound", func(c *SessionCheckpoint) { c.Inbound[0], c.Inbound[1] = c.Inbound[1], c.Inbound[0] }, "inbound checkpoints are not uniquely sorted"},
		{"nil_outbound", func(c *SessionCheckpoint) { c.Outbound[1] = nil }, "outbound checkpoint requires publish"},
		{"outbound_without_publish", func(c *SessionCheckpoint) { c.Outbound[0].Publish = nil }, "outbound checkpoint requires publish"},
		{"outbound_invalid_publish", func(c *SessionCheckpoint) { c.Outbound[1].Publish.ProtocolVersion = 6 }, "protocol_version must be 4 or 5"},
		{"outbound_phase_unspecified", func(c *SessionCheckpoint) { c.Outbound[0].Phase = OutboundDeliveryPhase_OUTBOUND_PHASE_UNSPECIFIED }, "invalid outbound delivery phase"},
		{"outbound_phase_out_of_range", func(c *SessionCheckpoint) { c.Outbound[0].Phase = OutboundDeliveryPhase_OUTBOUND_COMPLETE + 1 }, "invalid outbound delivery phase"},
		{"qos1_awaiting_pubrec", func(c *SessionCheckpoint) { c.Outbound[0].Phase = OutboundDeliveryPhase_OUTBOUND_AWAIT_PUBREC }, "outbound delivery phase conflicts with QoS"},
		{"qos1_awaiting_pubcomp", func(c *SessionCheckpoint) { c.Outbound[0].Phase = OutboundDeliveryPhase_OUTBOUND_AWAIT_PUBCOMP }, "outbound delivery phase conflicts with QoS"},
		{"qos2_awaiting_puback", func(c *SessionCheckpoint) { c.Outbound[1].Phase = OutboundDeliveryPhase_OUTBOUND_AWAIT_PUBACK }, "outbound delivery phase conflicts with QoS"},
		{"duplicate_outbound", func(c *SessionCheckpoint) { c.Outbound[1].Publish.PacketId = 3 }, "duplicate outbound packet ID"},
		{"unsorted_outbound", func(c *SessionCheckpoint) { c.Outbound[0], c.Outbound[1] = c.Outbound[1], c.Outbound[0] }, "outbound checkpoints are not uniquely sorted"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			checkpoint := crrCheckpoint()
			tc.mutate(checkpoint)
			_, err := MarshalCommand(crrCommand(&CommandEnvelope_Checkpoint{Checkpoint: checkpoint}))
			crrWantErr(t, err, tc.want)
		})
	}
}

func TestCrrCommandSizeLimits(t *testing.T) {
	big := crrCheckpoint()
	big.Inbound[0].Publish.Payload = make([]byte, 600_000)
	big.Inbound[1].Publish.Payload = make([]byte, 600_000)
	_, err := MarshalCommand(crrCommand(&CommandEnvelope_Checkpoint{Checkpoint: big}))
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
	checkpoint := func(tenant, client string) *SessionCheckpoint {
		c := crrCheckpoint()
		c.TenantId, c.ClientId = tenant, client
		return c
	}
	unsortedPackets := checkpoint("t1", "a")
	unsortedPackets.Inbound[0], unsortedPackets.Inbound[1] = unsortedPackets.Inbound[1], unsortedPackets.Inbound[0]
	unsortedPackets.Outbound[0], unsortedPackets.Outbound[1] = unsortedPackets.Outbound[1], unsortedPackets.Outbound[0]

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
		Checkpoints:       []*SessionCheckpoint{checkpoint("t2", "a"), checkpoint("t1", "b"), unsortedPackets},
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
	if envelope.SchemaMajor != 1 || envelope.LastIncludedIndex != 9 || envelope.ObjectCount != 16 {
		t.Fatalf("envelope header = schema %d index %d count %d", envelope.SchemaMajor, envelope.LastIncludedIndex, envelope.ObjectCount)
	}
	got := envelope.State
	if ids := [3]uint64{got.Members[0].MemberId, got.Members[1].MemberId, got.Members[2].MemberId}; ids != [3]uint64{1, 3, 7} {
		t.Fatalf("member order = %v", ids)
	}
	var owners, subs, checkpoints, history []string
	for _, o := range got.Ownerships {
		owners = append(owners, o.TenantId+"/"+o.ClientId)
	}
	for _, s := range got.Subscriptions {
		subs = append(subs, s.TenantId+"/"+s.ClientId+"/"+s.Filter)
	}
	for _, c := range got.Checkpoints {
		checkpoints = append(checkpoints, c.TenantId+"/"+c.ClientId)
	}
	for _, h := range got.MembershipHistory {
		history = append(history, h.OperationId)
	}
	for name, pair := range map[string][2]string{
		"ownerships":    {strings.Join(owners, ","), "t1/a,t1/b,t2/a"},
		"subscriptions": {strings.Join(subs, ","), "t1/a/x,t1/a/y,t1/b/x,t2/a/x"},
		"checkpoints":   {strings.Join(checkpoints, ","), "t1/a,t1/b,t2/a"},
		"history":       {strings.Join(history, ","), strings.Join([]string{op1, crrOp2, crrOp3}, ",")},
	} {
		if pair[0] != pair[1] {
			t.Fatalf("%s order = %s, want %s", name, pair[0], pair[1])
		}
	}
	if first := got.Checkpoints[0]; first.Inbound[0].Publish.PacketId != 1 || first.Outbound[0].Publish.PacketId != 3 {
		t.Fatalf("checkpoint packets not sorted: %v", first)
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
	missingPublish := crrCheckpoint()
	missingPublish.Inbound = []*InboundCheckpoint{
		{Phase: InboundDeliveryPhase_INBOUND_QOS1_ACCEPTED}, missingPublish.Inbound[1], missingPublish.Inbound[0], {Phase: InboundDeliveryPhase_INBOUND_QOS1_ACCEPTED},
	}
	missingOutbound := crrCheckpoint()
	missingOutbound.Outbound = append([]*OutboundCheckpoint{nil}, append(missingOutbound.Outbound, &OutboundCheckpoint{})...)
	invalidCheckpoint := crrCheckpoint()
	invalidCheckpoint.Epoch = 0

	cases := []struct {
		name  string
		state *ReplicatedStateSnapshot
		want  string
	}{
		{"state_unknown", topUnknown, "unknown fields"},
		{"member_unknown", &ReplicatedStateSnapshot{Members: []*MemberIdentity{withUnknownMember}}, "unknown fields"},
		{"history_member_unknown", &ReplicatedStateSnapshot{MembershipHistory: []*MembershipRecord{withUnknownHistory}}, "unknown fields"},
		{"invalid_member", &ReplicatedStateSnapshot{Members: []*MemberIdentity{crrVoter(0)}}, "member_id must be positive"},
		{"duplicate_member", &ReplicatedStateSnapshot{Members: []*MemberIdentity{crrVoter(2), crrVoter(2)}}, "snapshot members are not uniquely sorted"},
		{"ownership_empty_tenant", ownership(func(o *OwnershipState) { o.TenantId = "" }), "invalid snapshot ownership"},
		{"ownership_long_client", ownership(func(o *OwnershipState) { o.ClientId = strings.Repeat("c", 257) }), "invalid snapshot ownership"},
		{"ownership_zero_owner", ownership(func(o *OwnershipState) { o.OwnerNodeId = 0 }), "invalid snapshot ownership"},
		{"ownership_zero_epoch", ownership(func(o *OwnershipState) { o.Epoch = 0 }), "invalid snapshot ownership"},
		{"subscription_long_tenant", subscription(func(s *SubscriptionState) { s.TenantId = strings.Repeat("t", 129) }), "invalid snapshot subscription"},
		{"subscription_empty_client", subscription(func(s *SubscriptionState) { s.ClientId = "" }), "invalid snapshot subscription"},
		{"subscription_zero_owner", subscription(func(s *SubscriptionState) { s.OwnerNodeId = 0 }), "invalid snapshot subscription"},
		{"subscription_zero_epoch", subscription(func(s *SubscriptionState) { s.Epoch = 0 }), "invalid snapshot subscription"},
		{"subscription_bad_filter", subscription(func(s *SubscriptionState) { s.Filter = "#/a" }), "invalid snapshot subscription"},
		{"subscription_qos", subscription(func(s *SubscriptionState) { s.Qos = 3 }), "invalid snapshot subscription"},
		{"subscription_retain_handling", subscription(func(s *SubscriptionState) { s.RetainHandling = 3 }), "invalid snapshot subscription"},
		{"duplicate_subscription", &ReplicatedStateSnapshot{Subscriptions: []*SubscriptionState{
			{TenantId: "t", ClientId: "c", OwnerNodeId: 1, Epoch: 1, Filter: "a"},
			{TenantId: "t", ClientId: "c", OwnerNodeId: 2, Epoch: 2, Filter: "a"},
		}}, "snapshot subscriptions are not uniquely sorted"},
		{"invalid_checkpoint", &ReplicatedStateSnapshot{Checkpoints: []*SessionCheckpoint{invalidCheckpoint}}, "checkpoint owner_node_id and epoch"},
		{"checkpoint_missing_inbound_publish", &ReplicatedStateSnapshot{Checkpoints: []*SessionCheckpoint{missingPublish}}, "inbound checkpoint requires publish"},
		{"checkpoint_missing_outbound_publish", &ReplicatedStateSnapshot{Checkpoints: []*SessionCheckpoint{missingOutbound}}, "outbound checkpoint requires publish"},
		{"duplicate_checkpoint", &ReplicatedStateSnapshot{Checkpoints: []*SessionCheckpoint{crrCheckpoint(), crrCheckpoint()}}, "snapshot checkpoints are not uniquely sorted"},
		{"history_bad_operation", &ReplicatedStateSnapshot{MembershipHistory: []*MembershipRecord{badHistoryOperation}}, "membership operation_id must be a canonical"},
		{"history_invalid_action", &ReplicatedStateSnapshot{MembershipHistory: []*MembershipRecord{invalidHistory}}, "invalid membership action"},
		{"history_uncommitted", &ReplicatedStateSnapshot{MembershipHistory: []*MembershipRecord{crrHistory(op1, 0)}}, "committed_index must be positive"},
		{"duplicate_history", &ReplicatedStateSnapshot{MembershipHistory: []*MembershipRecord{crrHistory(op1, 1), crrHistory(op1, 1)}}, "snapshot membership history is not uniquely sorted"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			wire, err := MarshalSnapshot(tc.state, 10)
			crrWantErr(t, err, tc.want)
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
		name string
		wire []byte
		want string
	}{
		{"malformed", []byte{0xff}, "unmarshal snapshot"},
		{"truncated", valid[:len(valid)-1], "unmarshal snapshot"},
		{"schema_minor", reencode(t, func(e *SnapshotEnvelope) { e.SchemaMinor = 1 }), "unsupported snapshot schema"},
		{"schema_major_zero", reencode(t, func(e *SnapshotEnvelope) { e.SchemaMajor = 0 }), "unsupported snapshot schema"},
		{"missing_state", reencode(t, func(e *SnapshotEnvelope) { e.State = nil }), "snapshot state is required"},
		{"short_digest", reencode(t, func(e *SnapshotEnvelope) { e.Sha256 = e.Sha256[:31] }), "SHA-256 must be 32 bytes"},
		{"invalid_state", reencode(t, func(e *SnapshotEnvelope) { e.State.Ownerships[0].Epoch = 0 }), "invalid snapshot ownership"},
		{"explicit_default_field", append(append([]byte(nil), valid...), 0x10, 0x00), "not canonical"},
		{"state_before_header", append(append([]byte(nil), valid[2:]...), valid[:2]...), "not canonical"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			envelope, err := UnmarshalSnapshot(tc.wire)
			crrWantErr(t, err, tc.want)
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
		{"checkpoints", &ReplicatedStateSnapshot{Checkpoints: func() []*SessionCheckpoint {
			first, second := crrCheckpoint(), crrCheckpoint()
			first.ClientId = "z"
			return []*SessionCheckpoint{first, second}
		}()}, "snapshot checkpoints are not uniquely sorted"},
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

	// 65 one-megabyte inbound publishes keep the command-level bounds valid but
	// push the encoded snapshot beyond 64 MiB.
	payload := make([]byte, maxPublishPayload)
	checkpoint := &SessionCheckpoint{TenantId: "t", ClientId: "c", OwnerNodeId: 1, Epoch: 1}
	for id := uint32(1); id <= 70; id++ {
		publish := crrPublish(1, id)
		publish.Payload = payload
		checkpoint.Inbound = append(checkpoint.Inbound, &InboundCheckpoint{Publish: publish, Phase: InboundDeliveryPhase_INBOUND_QOS1_ACCEPTED})
	}
	_, err = MarshalSnapshot(&ReplicatedStateSnapshot{Checkpoints: []*SessionCheckpoint{checkpoint}}, 1)
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
	crrWantErr(t, err, "exceeds 1,000,000 top-level entries")
}
