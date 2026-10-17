package records

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"testing"

	"google.golang.org/protobuf/proto"
)

// Traceability: CLU-14; docs/specs/cluster-session-and-delivery.md §4, §10, §11.8.
func TestRetainedCommandCodecMatchesWireVector(t *testing.T) {
	command := &CommandEnvelope{
		SchemaMajor: 1, OperationId: op1,
		Payload: &CommandEnvelope_Retained{Retained: &RetainedChange{
			TenantId: "t", Topic: "status", PublisherClientId: "publisher", Qos: 1,
			Payload: []byte("x"), ProposedAtUnixMillis: 1,
			Action: RetainedAction_RETAINED_ACTION_REPLACE,
		}},
	}
	got, err := MarshalCommand(command)
	if err != nil {
		t.Fatalf("MarshalCommand(): %v", err)
	}
	want, err := hex.DecodeString("08011a2430303030303030302d303030302d343030302d383030302d303030303030303030303031721f0a017412067374617475731a097075626c697368657220012a017838014001")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("retained command wire = %x, want %x", got, want)
	}
	decoded, err := UnmarshalCommand(want)
	if err != nil || !proto.Equal(decoded, command) {
		t.Fatalf("UnmarshalCommand() = %v, error %v", decoded, err)
	}
}

// Traceability: CLU-14; docs/specs/cluster-session-and-delivery.md §4, §10, §11.8.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-037
func TestRetainedCommandCodecRejectsInvalidMutations(t *testing.T) {
	valid := &RetainedChange{
		TenantId: "tenant", Topic: "status", PublisherClientId: "publisher",
		Qos: 1, Payload: []byte("value"), ProposedAtUnixMillis: 1000,
		Action: RetainedAction_RETAINED_ACTION_REPLACE,
	}
	cases := []struct {
		name   string
		change *RetainedChange
	}{
		{name: "nil payload", change: nil},
		{name: "empty tenant", change: mutateRetainedRecordForTest(valid, func(change *RetainedChange) { change.TenantId = "" })},
		{name: "long publisher", change: mutateRetainedRecordForTest(valid, func(change *RetainedChange) { change.PublisherClientId = string(bytes.Repeat([]byte{'c'}, 257)) })},
		{name: "invalid topic", change: mutateRetainedRecordForTest(valid, func(change *RetainedChange) { change.Topic = "#" })},
		{name: "zero proposal time", change: mutateRetainedRecordForTest(valid, func(change *RetainedChange) { change.ProposedAtUnixMillis = 0 })},
		{name: "empty replacement", change: mutateRetainedRecordForTest(valid, func(change *RetainedChange) { change.Payload = nil })},
		{name: "oversized replacement", change: mutateRetainedRecordForTest(valid, func(change *RetainedChange) { change.Payload = make([]byte, maxPublishPayload+1) })},
		{name: "replacement qos", change: mutateRetainedRecordForTest(valid, func(change *RetainedChange) { change.Qos = 3 })},
		{name: "delete payload", change: mutateRetainedRecordForTest(valid, func(change *RetainedChange) { change.Action = RetainedAction_RETAINED_ACTION_DELETE })},
		{name: "delete qos", change: mutateRetainedRecordForTest(valid, func(change *RetainedChange) {
			change.Action = RetainedAction_RETAINED_ACTION_DELETE
			change.Payload = nil
			change.Qos = 1
		})},
		{name: "delete expiry", change: mutateRetainedRecordForTest(valid, func(change *RetainedChange) {
			change.Action = RetainedAction_RETAINED_ACTION_DELETE
			change.Payload = nil
			change.Qos = 0
			change.ExpiresAtUnixMillis = 1
		})},
		{name: "unknown action", change: mutateRetainedRecordForTest(valid, func(change *RetainedChange) { change.Action = RetainedAction(99) })},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			command := &CommandEnvelope{SchemaMajor: 1, OperationId: op1, Payload: &CommandEnvelope_Retained{Retained: test.change}}
			if _, err := MarshalCommand(command); err == nil {
				t.Fatal("MarshalCommand() accepted invalid retained mutation")
			}
		})
	}
}

// Traceability: CLU-14; docs/specs/cluster-session-and-delivery.md §4, §10, §11.8.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-037
func TestRetainedSnapshotCodecSortsAndCounts(t *testing.T) {
	state := &ReplicatedStateSnapshot{Retained: []*RetainedState{
		retainedRecordForTest("tenant-b", "alpha"),
		retainedRecordForTest("tenant-a", "zeta"),
		retainedRecordForTest("tenant-a", "alpha"),
	}}
	before := proto.Clone(state)
	wire, err := MarshalSnapshot(state, 7)
	if err != nil {
		t.Fatalf("MarshalSnapshot(): %v", err)
	}
	if !proto.Equal(state, before) {
		t.Fatal("MarshalSnapshot() mutated the source snapshot")
	}
	envelope, err := UnmarshalSnapshot(wire)
	if err != nil {
		t.Fatalf("UnmarshalSnapshot(): %v", err)
	}
	if envelope.ObjectCount != 3 || len(envelope.State.Retained) != 3 {
		t.Fatalf("retained snapshot count = %d/%d, want 3", envelope.ObjectCount, len(envelope.State.Retained))
	}
	got := envelope.State.Retained
	if got[0].TenantId != "tenant-a" || got[0].Topic != "alpha" || got[1].TenantId != "tenant-a" || got[1].Topic != "zeta" || got[2].TenantId != "tenant-b" || got[2].Topic != "alpha" {
		t.Fatalf("retained snapshot order = %+v", got)
	}
	reencoded, err := MarshalSnapshot(envelope.State, 7)
	if err != nil || !bytes.Equal(reencoded, wire) {
		t.Fatalf("canonical retained snapshot re-encode error = %v", err)
	}
}

// Traceability: CLU-14; docs/specs/cluster-session-and-delivery.md §4, §10, §11.8.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-037
func TestRetainedSnapshotCodecRejectsInvalidState(t *testing.T) {
	valid := retainedRecordForTest("tenant", "status")
	cases := []struct {
		name     string
		retained []*RetainedState
	}{
		{name: "nil retained entry", retained: []*RetainedState{nil}},
		{name: "empty tenant", retained: []*RetainedState{mutateRetainedStateRecordForTest(valid, func(state *RetainedState) { state.TenantId = "" })}},
		{name: "empty publisher", retained: []*RetainedState{mutateRetainedStateRecordForTest(valid, func(state *RetainedState) { state.PublisherClientId = "" })}},
		{name: "invalid topic", retained: []*RetainedState{mutateRetainedStateRecordForTest(valid, func(state *RetainedState) { state.Topic = "#" })}},
		{name: "empty payload", retained: []*RetainedState{mutateRetainedStateRecordForTest(valid, func(state *RetainedState) { state.Payload = nil })}},
		{name: "oversized payload", retained: []*RetainedState{mutateRetainedStateRecordForTest(valid, func(state *RetainedState) { state.Payload = make([]byte, maxPublishPayload+1) })}},
		{name: "qos", retained: []*RetainedState{mutateRetainedStateRecordForTest(valid, func(state *RetainedState) { state.Qos = 3 })}},
		{name: "duplicate key", retained: []*RetainedState{retainedRecordForTest("tenant", "status"), retainedRecordForTest("tenant", "status")}},
		{name: "retained count", retained: retainedRecordsForTest(maxRetainedMessages + 1)},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			if _, err := MarshalSnapshot(&ReplicatedStateSnapshot{Retained: test.retained}, 1); err == nil {
				t.Fatal("MarshalSnapshot() accepted invalid retained state")
			}
		})
	}
}

func retainedRecordForTest(tenant, topic string) *RetainedState {
	return &RetainedState{TenantId: tenant, Topic: topic, PublisherClientId: "publisher", Qos: 1, Payload: []byte("value")}
}

func retainedRecordsForTest(count int) []*RetainedState {
	states := make([]*RetainedState, count)
	for index := range states {
		states[index] = retainedRecordForTest("tenant", fmt.Sprintf("topic-%04d", index))
	}
	return states
}

func mutateRetainedRecordForTest(change *RetainedChange, mutate func(*RetainedChange)) *RetainedChange {
	copyOfChange := proto.Clone(change).(*RetainedChange)
	mutate(copyOfChange)
	return copyOfChange
}

func mutateRetainedStateRecordForTest(state *RetainedState, mutate func(*RetainedState)) *RetainedState {
	copyOfState := proto.Clone(state).(*RetainedState)
	mutate(copyOfState)
	return copyOfState
}
