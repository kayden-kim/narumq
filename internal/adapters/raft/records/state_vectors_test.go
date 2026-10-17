package records

import (
	"bytes"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
)

// Traceability: CLU-11; docs/specs/cluster-session-and-delivery.md §8, §11.29.
func producerCommandVectors() map[string]*CommandEnvelope {
	const operation = "00000000-0000-4000-8000-000000000001"
	const joinOperation = "00000000-0000-4000-8000-000000000004"
	command := func(payload isCommandEnvelope_Payload) *CommandEnvelope {
		return &CommandEnvelope{SchemaMajor: 1, OperationId: operation, Payload: payload}
	}
	subscription := func(action SubscriptionAction) *CommandEnvelope {
		return command(&CommandEnvelope_Subscription{Subscription: &SubscriptionChange{TenantId: "t", ClientId: "c", OwnerNodeId: 1, Epoch: 1, Filter: "#", Qos: 1, Action: action}})
	}
	return map[string]*CommandEnvelope{
		"stage_join": {SchemaMajor: 1, OperationId: joinOperation, Payload: &CommandEnvelope_Membership{Membership: &MembershipRecord{
			OperationId: joinOperation, Action: MembershipAction_MEMBERSHIP_ACTION_STAGE_JOIN, IdempotencyKey: "k1",
			Member: &MemberIdentity{MemberId: 4, ClusterId: "c", Address: "n:1", DnsSan: "n", CertificateSha256: bytes.Repeat([]byte{0x11}, 32), ReleaseVersion: "1.0.0", ClusterProtocolMajor: 2, StateSchemaMajor: 1, Phase: MemberPhase_MEMBER_PHASE_STAGED, OperationId: joinOperation},
		}}},
		"ownership_acquire":   command(&CommandEnvelope_Ownership{Ownership: &OwnershipChange{TenantId: "t", ClientId: "c", OwnerNodeId: 1, Epoch: 1, Action: OwnershipAction_OWNERSHIP_ACTION_ACQUIRE}}),
		"subscription_add":    subscription(SubscriptionAction_SUBSCRIPTION_ACTION_ADD),
		"subscription_remove": subscription(SubscriptionAction_SUBSCRIPTION_ACTION_REMOVE),
		"delivery_publish":    command(&CommandEnvelope_Delivery{Delivery: &DeliveryChange{TenantId: "t", ClientId: "c", Action: DeliveryAction_DELIVERY_ACTION_PUBLISH, ProposedAtUnixMillis: 1, Topic: "a", Payload: []byte("x"), PublishQos: 1, Targets: []*DeliveryTarget{{ClientId: "c"}}}}),
	}
}

func readStateVector(t *testing.T, name string) []byte {
	t.Helper()
	text, err := os.ReadFile(filepath.Join("testdata", "replicated-state-v1", name+".hex"))
	if err != nil {
		t.Fatal(err)
	}
	wire, err := hex.DecodeString(strings.Join(strings.Fields(string(text)), ""))
	if err != nil {
		t.Fatal(err)
	}
	return wire
}

// Traceability: CLU-11; docs/specs/cluster-session-and-delivery.md §8, §11.29.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-206, cluster-session-and-delivery.md:CLU-SCN-236
func TestReplicatedStateContractVectors(t *testing.T) {
	for name, command := range producerCommandVectors() {
		t.Run(name, func(t *testing.T) {
			want := readStateVector(t, name)
			wire, err := MarshalCommand(command)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(wire, want) {
				t.Fatalf("producer bytes = %x, want %x", wire, want)
			}
			decoded, err := UnmarshalCommand(want)
			if err != nil {
				t.Fatal(err)
			}
			if !proto.Equal(decoded, command) {
				t.Fatalf("decoded = %v, want %v", decoded, command)
			}
		})
	}
	t.Run("empty_snapshot", func(t *testing.T) {
		state := &ReplicatedStateSnapshot{}
		wire, err := MarshalSnapshot(state, 1)
		if err != nil {
			t.Fatal(err)
		}
		want := readStateVector(t, "empty_snapshot")
		if !bytes.Equal(wire, want) {
			t.Fatalf("producer bytes = %x, want %x", wire, want)
		}
		decoded, err := UnmarshalSnapshot(want)
		if err != nil {
			t.Fatal(err)
		}
		digest := []byte{0xe3, 0xb0, 0xc4, 0x42, 0x98, 0xfc, 0x1c, 0x14, 0x9a, 0xfb, 0xf4, 0xc8, 0x99, 0x6f, 0xb9, 0x24, 0x27, 0xae, 0x41, 0xe4, 0x64, 0x9b, 0x93, 0x4c, 0xa4, 0x95, 0x99, 0x1b, 0x78, 0x52, 0xb8, 0x55}
		if !proto.Equal(decoded, &SnapshotEnvelope{SchemaMajor: 1, LastIncludedIndex: 1, Sha256: digest, State: state}) {
			t.Fatalf("decoded snapshot = %v", decoded)
		}
	})
	for _, name := range []string{"command_ownership_unknown_field", "command_unknown_payload", "command_schema_minor_1", "snapshot_record_unknown_field", "snapshot_unknown_collection", "snapshot_schema_minor_1"} {
		t.Run(name, func(t *testing.T) {
			wire := readStateVector(t, name)
			var err error
			if strings.HasPrefix(name, "command_") {
				_, err = UnmarshalCommand(wire)
			} else {
				_, err = UnmarshalSnapshot(wire)
			}
			if !errors.Is(err, ErrIncompatibleStateSchema) {
				t.Fatalf("decode error = %v, want incompatible schema", err)
			}
			if name == "command_ownership_unknown_field" && !strings.Contains(err.Error(), "incompatible_state_schema: protobuf contains unknown fields") {
				t.Fatalf("unknown field error = %v", err)
			}
		})
	}
}
