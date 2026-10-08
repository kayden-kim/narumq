package records

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
)

const op1 = "00000000-0000-4000-8000-000000000001"
const op4 = "00000000-0000-4000-8000-000000000004"

func TestCommandReferenceVectors(t *testing.T) {
	publish := &DeliveryChange{
		TenantId: "t", ClientId: "c", Action: DeliveryAction_DELIVERY_ACTION_PUBLISH,
		ProposedAtUnixMillis: 1, Topic: "a", Payload: []byte("x"), PublishQos: 1,
	}
	cases := []struct {
		name    string
		command *CommandEnvelope
		wire    string
	}{
		{"stage_join", &CommandEnvelope{SchemaMajor: 1, OperationId: op4, Payload: &CommandEnvelope_Membership{Membership: &MembershipRecord{OperationId: op4, Action: MembershipAction_MEMBERSHIP_ACTION_STAGE_JOIN, IdempotencyKey: "k1", Member: &MemberIdentity{MemberId: 4, ClusterId: "c", Address: "n:1", DnsSan: "n", CertificateSha256: bytes.Repeat([]byte{0x11}, 32), ReleaseVersion: "1.0.0", ClusterProtocolMajor: 2, StateSchemaMajor: 1, Phase: MemberPhase_MEMBER_PHASE_STAGED, OperationId: op4}}}}, "08011a2430303030303030302d303030302d343030302d383030302d3030303030303030303030345290010a2430303030303030302d303030302d343030302d383030302d30303030303030303030303410011a026b31226208041201631a036e3a3122016e2a20" + strings.Repeat("11", 32) + "3205312e302e30380248015801622430303030303030302d303030302d343030302d383030302d303030303030303030303034"},
		{"ownership_acquire", &CommandEnvelope{SchemaMajor: 1, OperationId: op1, Payload: &CommandEnvelope_Ownership{Ownership: &OwnershipChange{TenantId: "t", ClientId: "c", OwnerNodeId: 1, Epoch: 1, Action: OwnershipAction_OWNERSHIP_ACTION_ACQUIRE}}}, "08011a2430303030303030302d303030302d343030302d383030302d3030303030303030303030315a0c0a0174120163200128013001"},
		{"subscription_add", &CommandEnvelope{SchemaMajor: 1, OperationId: op1, Payload: &CommandEnvelope_Subscription{Subscription: &SubscriptionChange{TenantId: "t", ClientId: "c", OwnerNodeId: 1, Epoch: 1, Filter: "#", Qos: 1, Action: SubscriptionAction_SUBSCRIPTION_ACTION_ADD}}}, "08011a2430303030303030302d303030302d343030302d383030302d30303030303030303030303162110a0174120163180120012a012330015001"},
		{"subscription_remove", &CommandEnvelope{SchemaMajor: 1, OperationId: op1, Payload: &CommandEnvelope_Subscription{Subscription: &SubscriptionChange{TenantId: "t", ClientId: "c", OwnerNodeId: 1, Epoch: 1, Filter: "#", Qos: 1, Action: SubscriptionAction_SUBSCRIPTION_ACTION_REMOVE}}}, "08011a2430303030303030302d303030302d343030302d383030302d30303030303030303030303162110a0174120163180120012a012330015002"},
		{"delivery_publish", &CommandEnvelope{SchemaMajor: 1, OperationId: op1, Payload: &CommandEnvelope_Delivery{Delivery: publish}}, "08011a2430303030303030302d303030302d343030302d383030302d3030303030303030303030318201130a0174120163280130013a0161420178800101"},
		// Traceability: CLU-13; docs/specs/cluster-session-and-delivery.md §9, §11.31.
		{"connection_rebalance_report", &CommandEnvelope{SchemaMajor: 1, OperationId: op1, Payload: &CommandEnvelope_ConnectionRebalance{ConnectionRebalance: &ConnectionRebalanceChange{Action: ConnectionRebalanceAction_CONNECTION_REBALANCE_ACTION_REPORT, RebalanceOperationId: op4, NodeId: 1, Connections: 13, ProposedAtUnixMillis: 2}}}, "08011a2430303030303030302d303030302d343030302d383030302d303030303030303030303031b2012e0802122430303030303030302d303030302d343030302d383030302d3030303030303030303030344801500d7002"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := MarshalCommand(tc.command)
			if err != nil {
				t.Fatal(err)
			}
			want, err := hex.DecodeString(tc.wire)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("wire mismatch: got %x want %x", got, want)
			}
			decoded, err := UnmarshalCommand(want)
			if err != nil {
				t.Fatalf("decode fixture: %v", err)
			}
			if !proto.Equal(decoded, tc.command) {
				t.Fatalf("decoded fixture = %v, want %v", decoded, tc.command)
			}
		})
	}
}

func TestEmptySnapshotReferenceVector(t *testing.T) {
	want, _ := hex.DecodeString("080118012a20e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b8553200")
	got, err := MarshalSnapshot(&ReplicatedStateSnapshot{}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("wire mismatch: got %x want %x", got, want)
	}
	if _, err := UnmarshalSnapshot(want); err != nil {
		t.Fatal(err)
	}
}

func TestCommandRejectsNoncanonicalAndInvalid(t *testing.T) {
	valid := &CommandEnvelope{SchemaMajor: 1, OperationId: op1, Payload: &CommandEnvelope_Ownership{Ownership: &OwnershipChange{TenantId: "t", ClientId: "c", OwnerNodeId: 1, Epoch: 1, Action: OwnershipAction_OWNERSHIP_ACTION_ACQUIRE}}}
	wire, err := MarshalCommand(valid)
	if err != nil {
		t.Fatal(err)
	}
	unknown := append(append([]byte(nil), wire...), 0x78, 0x01)
	dup := append(append([]byte(nil), wire...), 0x08, 0x01)
	defaultField := append(append([]byte(nil), wire...), 0x10, 0x00)
	reordered := append(append([]byte(nil), wire[2:]...), wire[:2]...)
	for name, input := range map[string][]byte{"unknown": unknown, "duplicate": dup, "default": defaultField, "trailing": append(append([]byte(nil), wire...), 0xff), "malformed": wire[:len(wire)-1], "field_order": reordered} {
		t.Run(name, func(t *testing.T) {
			if _, err := UnmarshalCommand(input); err == nil {
				t.Fatal("expected rejection")
			}
		})
	}
	invalid := proto.Clone(valid).(*CommandEnvelope)
	invalid.OperationId = "00000000-0000-4000-8000-00000000000A"
	if _, err := MarshalCommand(invalid); err == nil {
		t.Fatal("expected UUID validation error")
	}
	invalid = proto.Clone(valid).(*CommandEnvelope)
	invalid.SchemaMinor = 1
	if _, err := MarshalCommand(invalid); err == nil {
		t.Fatal("expected schema validation error")
	}
	invalid = proto.Clone(valid).(*CommandEnvelope)
	invalid.Payload = nil
	if _, err := MarshalCommand(invalid); err == nil {
		t.Fatal("expected payload validation error")
	}
}

func TestSnapshotRejectsChecksumCountSchemaAndUnknown(t *testing.T) {
	wire, err := MarshalSnapshot(&ReplicatedStateSnapshot{}, 1)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := UnmarshalSnapshot(wire)
	if err != nil {
		t.Fatal(err)
	}
	bad := proto.Clone(decoded).(*SnapshotEnvelope)
	bad.Sha256[0] ^= 0xff
	encoded, _ := deterministic.Marshal(bad)
	if _, err := UnmarshalSnapshot(encoded); err == nil {
		t.Fatal("expected checksum rejection")
	}
	bad = proto.Clone(decoded).(*SnapshotEnvelope)
	bad.ObjectCount = 1
	encoded, _ = deterministic.Marshal(bad)
	if _, err := UnmarshalSnapshot(encoded); err == nil {
		t.Fatal("expected count rejection")
	}
	bad = proto.Clone(decoded).(*SnapshotEnvelope)
	bad.SchemaMajor = 2
	encoded, _ = deterministic.Marshal(bad)
	if _, err := UnmarshalSnapshot(encoded); err == nil {
		t.Fatal("expected schema rejection")
	}
	if _, err := UnmarshalSnapshot(append(append([]byte(nil), wire...), 0x78, 0x01)); err == nil {
		t.Fatal("expected unknown-field rejection")
	}
}

func TestCodecRepresentativeBounds(t *testing.T) {
	command := &CommandEnvelope{SchemaMajor: 1, OperationId: op1, Payload: &CommandEnvelope_Ownership{Ownership: &OwnershipChange{TenantId: strings.Repeat("t", 129), ClientId: "c", OwnerNodeId: 1, Epoch: 1, Action: OwnershipAction_OWNERSHIP_ACTION_ACQUIRE}}}
	if _, err := MarshalCommand(command); err == nil {
		t.Fatal("expected tenant bound rejection")
	}
	command = &CommandEnvelope{SchemaMajor: 1, OperationId: op1, Payload: &CommandEnvelope_Membership{Membership: &MembershipRecord{OperationId: op1, Action: MembershipAction_MEMBERSHIP_ACTION_STAGE_JOIN, IdempotencyKey: "k", Member: &MemberIdentity{MemberId: 1, ClusterId: "c", Address: "n:1", DnsSan: "n", CertificateSha256: []byte{1}, ReleaseVersion: "1.0.0", ClusterProtocolMajor: 2, StateSchemaMajor: 1, Phase: MemberPhase_MEMBER_PHASE_STAGED, OperationId: op1}}}}
	if _, err := MarshalCommand(command); err == nil {
		t.Fatal("expected fingerprint length rejection")
	}
}

func TestMembershipCommittedIndexIsSnapshotOnlyAndSurvivesRestore(t *testing.T) {
	member := &MemberIdentity{
		MemberId: 4, ClusterId: "c", Address: "n:1", DnsSan: "n",
		CertificateSha256: bytes.Repeat([]byte{0x11}, 32), ReleaseVersion: "1.0.0",
		ClusterProtocolMajor: 2, StateSchemaMajor: 1,
		Phase: MemberPhase_MEMBER_PHASE_STAGED, OperationId: op4,
	}
	record := &MembershipRecord{
		OperationId: op4, Action: MembershipAction_MEMBERSHIP_ACTION_STAGE_JOIN,
		IdempotencyKey: "key", Member: member, CommittedIndex: 7,
	}
	command := &CommandEnvelope{
		SchemaMajor: 1, OperationId: op4,
		Payload: &CommandEnvelope_Membership{Membership: proto.Clone(record).(*MembershipRecord)},
	}
	if _, err := MarshalCommand(command); err == nil {
		t.Fatal("command-supplied committed index was accepted")
	}

	state := &ReplicatedStateSnapshot{MembershipHistory: []*MembershipRecord{record}}
	wire, err := MarshalSnapshot(state, 10)
	if err != nil {
		t.Fatalf("marshal history snapshot: %v", err)
	}
	envelope, err := UnmarshalSnapshot(wire)
	if err != nil {
		t.Fatalf("restore history snapshot: %v", err)
	}
	if got := envelope.State.MembershipHistory[0].GetCommittedIndex(); got != 7 {
		t.Fatalf("restored operation index = %d, want 7", got)
	}

	if _, err := MarshalSnapshot(state, 6); err == nil {
		t.Fatal("snapshot accepted a membership index beyond its included index")
	}
	envelope.LastIncludedIndex = 6
	noncanonical, err := deterministic.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := UnmarshalSnapshot(noncanonical); err == nil {
		t.Fatal("decoder accepted a membership index beyond its included index")
	}
}

func TestSnapshotProducerSortsCopyAndRejectsDuplicateKeys(t *testing.T) {
	first := &OwnershipState{TenantId: "a", ClientId: "z", OwnerNodeId: 1, Epoch: 1}
	second := &OwnershipState{TenantId: "a", ClientId: "b", OwnerNodeId: 1, Epoch: 1}
	state := &ReplicatedStateSnapshot{Ownerships: []*OwnershipState{first, second}}
	wire, err := MarshalSnapshot(state, 2)
	if err != nil {
		t.Fatal(err)
	}
	if state.Ownerships[0] != first || state.Ownerships[1] != second {
		t.Fatal("snapshot producer mutated caller-owned ordering")
	}
	decoded, err := UnmarshalSnapshot(wire)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.State.Ownerships[0].ClientId != "b" {
		t.Fatal("snapshot was not sorted canonically")
	}
	duplicate := &ReplicatedStateSnapshot{Ownerships: []*OwnershipState{second, proto.Clone(second).(*OwnershipState)}}
	if _, err := MarshalSnapshot(duplicate, 2); err == nil {
		t.Fatal("expected duplicate-key rejection")
	}
}
func TestSnapshotRejectsStateIndexesBeyondLastIncluded(t *testing.T) {
	record := &MembershipRecord{
		OperationId: op4, Action: MembershipAction_MEMBERSHIP_ACTION_STAGE_JOIN,
		IdempotencyKey: "key",
		Member: &MemberIdentity{
			MemberId: 4, ClusterId: "c", Address: "n:1", DnsSan: "n",
			CertificateSha256: bytes.Repeat([]byte{0x11}, 32), ReleaseVersion: "1.0.0",
			ClusterProtocolMajor: 2, StateSchemaMajor: 1,
			Phase: MemberPhase_MEMBER_PHASE_STAGED, OperationId: op4,
		},
	}
	cases := []struct {
		name    string
		state   *ReplicatedStateSnapshot
		message string
	}{
		{
			name: "membership history",
			state: &ReplicatedStateSnapshot{MembershipHistory: []*MembershipRecord{{
				OperationId: record.OperationId, Action: record.Action, IdempotencyKey: record.IdempotencyKey,
				Member: proto.Clone(record.Member).(*MemberIdentity), CommittedIndex: 11,
			}}},
			message: "membership history committed_index exceeds last_included_index",
		},
		{
			name: "node drain",
			state: func() *ReplicatedStateSnapshot {
				drain := validDrainState(1, NodeDrainPhase_NODE_DRAIN_PHASE_EXCLUDING_TRAFFIC)
				drain.CommittedIndex = 11
				return &ReplicatedStateSnapshot{Members: []*MemberIdentity{crrVoter(1)}, NodeDrains: []*NodeDrainState{drain}}
			}(),
			message: "node drain committed_index exceeds last_included_index",
		},
		{
			name: "connection rebalance",
			state: func() *ReplicatedStateSnapshot {
				rebalance := validConnectionRebalanceState()
				rebalance.CommittedIndex = 11
				return &ReplicatedStateSnapshot{ActiveFeatures: []string{"connection-rebalance"}, ConnectionRebalance: rebalance}
			}(),
			message: "connection rebalance committed_index exceeds last_included_index",
		},
		{
			name: "delivery released index",
			state: &ReplicatedStateSnapshot{DeliverySessions: []*DeliverySessionState{{
				TenantId: "tenant", ClientId: "client", NextSequence: 1,
				Inbound: []*InboundMessage{{PacketId: 1, Topic: "topic", Released: true, ReleasedIndex: 11}},
			}}},
			message: "delivery released_index exceeds last_included_index",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			before := proto.Clone(tc.state)
			if _, err := MarshalSnapshot(tc.state, 10); err == nil || !strings.Contains(err.Error(), tc.message) {
				t.Fatalf("MarshalSnapshot() error = %v, want message %q", err, tc.message)
			}
			if !proto.Equal(tc.state, before) {
				t.Fatal("MarshalSnapshot mutated caller-owned state after rejection")
			}
			wire, err := MarshalSnapshot(tc.state, 11)
			if err != nil {
				t.Fatalf("MarshalSnapshot(valid boundary snapshot): %v", err)
			}
			envelope := new(SnapshotEnvelope)
			if err := proto.Unmarshal(wire, envelope); err != nil {
				t.Fatal(err)
			}
			envelope.LastIncludedIndex = 10
			wire, err = deterministic.Marshal(envelope)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := UnmarshalSnapshot(wire); err == nil || !strings.Contains(err.Error(), tc.message) {
				t.Fatalf("UnmarshalSnapshot() error = %v, want message %q", err, tc.message)
			}
		})
	}
}

func TestCommandRejectsInvalidProposalIdentityAndDigest(t *testing.T) {
	base := &CommandEnvelope{
		SchemaMajor: 1, OperationId: op1,
		Payload: &CommandEnvelope_Ownership{Ownership: &OwnershipChange{
			TenantId: "t", ClientId: "c", OwnerNodeId: 1, Epoch: 1,
			Action: OwnershipAction_OWNERSHIP_ACTION_ACQUIRE,
		}},
	}
	baseWire, err := deterministic.Marshal(base)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(baseWire)
	cases := []struct {
		name     string
		identity *ProposalIdentity
		message  string
	}{
		{name: "zero origin", identity: &ProposalIdentity{ProofIndex: 2, RequestId: op4, CommandDigest: digest[:]}, message: "invalid proposal identity"},
		{name: "invalid request id", identity: &ProposalIdentity{OriginNodeId: 1, ProofIndex: 2, RequestId: "bad", CommandDigest: digest[:]}, message: "invalid proposal identity"},
		{name: "wrong digest", identity: &ProposalIdentity{OriginNodeId: 1, ProofIndex: 2, RequestId: op4, CommandDigest: make([]byte, sha256.Size)}, message: "proposal command digest mismatch"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			command := proto.Clone(base).(*CommandEnvelope)
			command.Proposal = proto.Clone(tc.identity).(*ProposalIdentity)
			before := proto.Clone(command).(*CommandEnvelope)
			if _, err := MarshalCommand(command); err == nil || !strings.Contains(err.Error(), tc.message) {
				t.Fatalf("MarshalCommand() error = %v, want message %q", err, tc.message)
			}
			raw, marshalErr := deterministic.Marshal(command)
			if marshalErr != nil {
				t.Fatal(marshalErr)
			}
			if _, err := UnmarshalCommand(raw); err == nil || !strings.Contains(err.Error(), tc.message) {
				t.Fatalf("UnmarshalCommand() error = %v, want message %q", err, tc.message)
			}
			if !proto.Equal(command, before) {
				t.Fatal("MarshalCommand mutated command while rejecting proposal identity")
			}
		})
	}
	valid := proto.Clone(base).(*CommandEnvelope)
	valid.Proposal = &ProposalIdentity{OriginNodeId: 1, ProofIndex: 2, RequestId: op4, CommandDigest: digest[:]}
	encoded, err := MarshalCommand(valid)
	if err != nil {
		t.Fatalf("MarshalCommand(valid proposal identity): %v", err)
	}
	if _, err := UnmarshalCommand(encoded); err != nil {
		t.Fatalf("UnmarshalCommand(valid proposal identity): %v", err)
	}
}

func TestSnapshotRejectsInvalidProposalOriginAndReceipt(t *testing.T) {
	cases := []struct {
		name    string
		state   *ReplicatedStateSnapshot
		message string
	}{
		{
			name:    "origin requires feature",
			state:   &ReplicatedStateSnapshot{ProposalOrigins: []*ProposalOriginState{{OriginNodeId: 1}}},
			message: "proposal origins require active proposal-recovery",
		},
		{
			name: "zero origin id",
			state: &ReplicatedStateSnapshot{
				ActiveFeatures:  []string{"proposal-recovery"},
				ProposalOrigins: []*ProposalOriginState{{}},
			},
			message: "invalid proposal origin",
		},
		{
			name: "invalid receipt result family",
			state: &ReplicatedStateSnapshot{
				ActiveFeatures: []string{"proposal-recovery"},
				Members:        []*MemberIdentity{crrVoter(1)},
				ProposalOrigins: []*ProposalOriginState{{
					OriginNodeId: 1,
					Receipts: []*ProposalReceipt{{
						RequestId: op4, ProofIndex: 1, CommandDigest: bytes.Repeat([]byte{0x22}, sha256.Size),
						Result: &ProposalResult{Index: 2, Outcome: 9},
					}},
				}},
			},
			message: "invalid proposal result outcome",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			before := proto.Clone(tc.state)
			if _, err := MarshalSnapshot(tc.state, 10); err == nil || !strings.Contains(err.Error(), tc.message) {
				t.Fatalf("MarshalSnapshot() error = %v, want message %q", err, tc.message)
			}
			if !proto.Equal(tc.state, before) {
				t.Fatal("MarshalSnapshot mutated caller-owned proposal state after rejection")
			}
			stateBytes, err := deterministic.Marshal(tc.state)
			if err != nil {
				t.Fatal(err)
			}
			digest := sha256.Sum256(stateBytes)
			count := len(tc.state.Members) + len(tc.state.ActiveFeatures) + proposalOriginsObjectCount(tc.state.ProposalOrigins)
			wire, err := deterministic.Marshal(&SnapshotEnvelope{
				SchemaMajor: 1, LastIncludedIndex: 10, ObjectCount: uint64(count), Sha256: digest[:], State: tc.state,
			})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := UnmarshalSnapshot(wire); err == nil || !strings.Contains(err.Error(), tc.message) {
				t.Fatalf("UnmarshalSnapshot() error = %v, want message %q", err, tc.message)
			}
		})
	}
}
