package membership

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"testing"
)

// Traceability: CLU-14; docs/specs/cluster-session-and-delivery.md §4, §10, §11.8.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-037
func TestRetainedMutationIsTenantScopedAndOwnsPayload(t *testing.T) {
	machine := NewStateMachine()
	firstPayload := []byte("first")
	applyRetainedForTest(t, machine, 1, retainedReplaceForTest("tenant-a", "status", "publisher-a", firstPayload, 1, 0, 1000))
	firstPayload[0] = 'X'
	applyRetainedForTest(t, machine, 2, retainedReplaceForTest("tenant-b", "status", "publisher-b", []byte("other"), 2, 0, 1100))

	messages := machine.RetainedMessages()
	if len(messages) != 2 || messages[0].TenantID != "tenant-a" || string(messages[0].Payload) != "first" || messages[1].TenantID != "tenant-b" {
		t.Fatalf("same-topic tenant values = %+v", messages)
	}
	messages[0].Payload[0] = 'Y'
	if got := string(machine.RetainedMessages()[0].Payload); got != "first" {
		t.Fatalf("returned payload mutation changed state: %q", got)
	}

	deleted := retainedReplaceForTest("tenant-a", "status", "publisher-a", nil, 0, 0, 1200)
	deleted.Action = RetainedActionDelete
	result := applyRetainedForTest(t, machine, 3, deleted)
	if result.Outcome != ApplyOutcomeApplied {
		t.Fatalf("tenant-scoped delete outcome = %v", result.Outcome)
	}
	remaining := machine.RetainedMessages()
	if len(remaining) != 1 || remaining[0].TenantID != "tenant-b" || string(remaining[0].Payload) != "other" {
		t.Fatalf("delete crossed tenant boundary: %+v", remaining)
	}
}

// Traceability: CLU-14; docs/specs/cluster-session-and-delivery.md §4, §10, §11.8.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-037
func TestRetainedExpiryUsesCommittedProposerTime(t *testing.T) {
	machine := NewStateMachine()
	applyRetainedForTest(t, machine, 1, retainedReplaceForTest("tenant", "expired", "publisher", []byte("value"), 0, 2000, 1000))

	deleteMissing := retainedReplaceForTest("other", "missing", "publisher", nil, 0, 0, 2000)
	deleteMissing.Action = RetainedActionDelete
	applyRetainedForTest(t, machine, 2, deleteMissing)
	if messages := machine.RetainedMessages(); len(messages) != 0 {
		t.Fatalf("entry expiring at proposer time was retained: %+v", messages)
	}
}

// Traceability: CLU-14; docs/specs/cluster-session-and-delivery.md §4, §10, §11.8.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-037
func TestRetainedSnapshotIsCanonicalAndDeepOwned(t *testing.T) {
	machine := NewStateMachine()
	for index, change := range []RetainedChangeDTO{
		retainedReplaceForTest("tenant-b", "alpha", "publisher", []byte("b"), 0, 0, 1000),
		retainedReplaceForTest("tenant-a", "zeta", "publisher", []byte("z"), 0, 0, 1000),
		retainedReplaceForTest("tenant-a", "alpha", "publisher", []byte("a"), 0, 0, 1000),
	} {
		applyRetainedForTest(t, machine, uint64(index+1), change)
	}

	snapshot, err := machine.Snapshot(3)
	if err != nil {
		t.Fatalf("Snapshot(): %v", err)
	}
	want := []RetainedStateDTO{
		{TenantID: "tenant-a", Topic: "alpha", PublisherClientID: "publisher", Payload: []byte("a")},
		{TenantID: "tenant-a", Topic: "zeta", PublisherClientID: "publisher", Payload: []byte("z")},
		{TenantID: "tenant-b", Topic: "alpha", PublisherClientID: "publisher", Payload: []byte("b")},
	}
	if !reflect.DeepEqual(snapshot.Retained, want) || snapshot.ObjectCount != 3 {
		t.Fatalf("canonical retained snapshot = %+v, object count %d", snapshot.Retained, snapshot.ObjectCount)
	}

	restored := NewStateMachine()
	if err := restored.Restore(snapshot); err != nil {
		t.Fatalf("Restore(): %v", err)
	}
	restoredSnapshot, err := restored.Snapshot(3)
	if err != nil || !reflect.DeepEqual(restoredSnapshot.Retained, want) {
		t.Fatalf("restored retained snapshot = %+v, error %v", restoredSnapshot.Retained, err)
	}
	snapshot.Retained[0].Payload[0] = 'X'
	if got := string(restored.RetainedMessages()[0].Payload); got != "a" {
		t.Fatalf("restore retained caller-owned payload: %q", got)
	}

	invalid := cloneSnapshot(restoredSnapshot)
	invalid.Retained[1].TenantID = invalid.Retained[0].TenantID
	invalid.Retained[1].Topic = invalid.Retained[0].Topic
	if err := NewStateMachine().Restore(invalid); !errors.Is(err, ErrInvalidSnapshot) {
		t.Fatalf("duplicate retained key restore error = %v, want ErrInvalidSnapshot", err)
	}
}

// Traceability: CLU-14; docs/specs/cluster-session-and-delivery.md §4, §10, §11.8.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-037
func TestRetainedCapacityConflictsPreserveExistingState(t *testing.T) {
	for _, test := range []struct {
		name    string
		count   int
		payload []byte
	}{
		{name: "message count", count: MaxRetainedMessages, payload: []byte("x")},
		{name: "aggregate bytes", count: 16, payload: bytes.Repeat([]byte{'x'}, MaxRetainedPayloadBytes)},
	} {
		t.Run(test.name, func(t *testing.T) {
			states := make([]RetainedStateDTO, test.count)
			for index := range states {
				states[index] = RetainedStateDTO{
					TenantID: "tenant", Topic: fmt.Sprintf("topic-%04d", index),
					PublisherClientID: "publisher", Payload: test.payload,
				}
			}
			initial := SnapshotDTO{
				SchemaMajor: SchemaMajor, SchemaMinor: SchemaMinor, LastIncludedIndex: 1,
				ObjectCount: uint64(len(states)), Retained: states,
			}
			machine := NewStateMachine()
			if err := machine.Restore(initial); err != nil {
				t.Fatalf("Restore(capacity boundary): %v", err)
			}
			beforeBytes := machine.retainedBytes
			result := applyRetainedForTest(t, machine, 2, retainedReplaceForTest("tenant", "overflow", "publisher", test.payload, 0, 0, 1000))
			if result.Outcome != ApplyOutcomeConflict || result.Conflict != ConflictRetainedCapacity {
				t.Fatalf("over-capacity result = %+v, want retained capacity conflict", result)
			}
			if len(machine.RetainedMessages()) != test.count || machine.retainedBytes != beforeBytes || machine.LastAppliedIndex() != 2 {
				t.Fatalf("capacity conflict changed state: count=%d bytes=%d index=%d", len(machine.retained), machine.retainedBytes, machine.LastAppliedIndex())
			}
		})
	}
}

// Traceability: CLU-14; docs/specs/cluster-session-and-delivery.md §4, §10, §11.8.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-037
func TestRetainedCommandAndSnapshotValidation(t *testing.T) {
	validReplace := retainedReplaceForTest("tenant", "status", "publisher", []byte("value"), 1, 0, 1000)
	cases := []struct {
		name   string
		change RetainedChangeDTO
	}{
		{name: "empty tenant", change: mutateRetainedForTest(validReplace, func(change *RetainedChangeDTO) { change.TenantID = "" })},
		{name: "invalid topic", change: mutateRetainedForTest(validReplace, func(change *RetainedChangeDTO) { change.Topic = "#" })},
		{name: "empty publisher", change: mutateRetainedForTest(validReplace, func(change *RetainedChangeDTO) { change.PublisherClientID = "" })},
		{name: "zero proposer time", change: mutateRetainedForTest(validReplace, func(change *RetainedChangeDTO) { change.ProposedAtUnixMillis = 0 })},
		{name: "empty replacement payload", change: mutateRetainedForTest(validReplace, func(change *RetainedChangeDTO) { change.Payload = nil })},
		{name: "replacement payload too large", change: mutateRetainedForTest(validReplace, func(change *RetainedChangeDTO) { change.Payload = make([]byte, MaxRetainedPayloadBytes+1) })},
		{name: "replacement qos", change: mutateRetainedForTest(validReplace, func(change *RetainedChangeDTO) { change.QoS = 3 })},
		{name: "delete payload", change: mutateRetainedForTest(validReplace, func(change *RetainedChangeDTO) { change.Action = RetainedActionDelete })},
		{name: "delete qos", change: mutateRetainedForTest(validReplace, func(change *RetainedChangeDTO) {
			change.Action = RetainedActionDelete
			change.Payload = nil
			change.QoS = 1
		})},
		{name: "delete expiry", change: mutateRetainedForTest(validReplace, func(change *RetainedChangeDTO) {
			change.Action = RetainedActionDelete
			change.Payload = nil
			change.QoS = 0
			change.ExpiresAtUnixMillis = 1
		})},
		{name: "unknown action", change: mutateRetainedForTest(validReplace, func(change *RetainedChangeDTO) { change.Action = RetainedAction(99) })},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			machine := NewStateMachine()
			_, err := machine.Apply(1, retainedCommandForTest(test.change))
			if !errors.Is(err, ErrInvalidCommand) || machine.LastAppliedIndex() != 0 {
				t.Fatalf("invalid command error/index = %v/%d, want ErrInvalidCommand/0", err, machine.LastAppliedIndex())
			}
		})
	}

	validSnapshot := SnapshotDTO{
		SchemaMajor: SchemaMajor, SchemaMinor: SchemaMinor, LastIncludedIndex: 1,
		ObjectCount: 1, Retained: []RetainedStateDTO{{
			TenantID: "tenant", Topic: "status", PublisherClientID: "publisher", QoS: 1, Payload: []byte("value"),
		}},
	}
	invalidSnapshots := []struct {
		name   string
		mutate func(*SnapshotDTO)
	}{
		{name: "empty retained tenant", mutate: func(snapshot *SnapshotDTO) { snapshot.Retained[0].TenantID = "" }},
		{name: "invalid retained topic", mutate: func(snapshot *SnapshotDTO) { snapshot.Retained[0].Topic = "#" }},
		{name: "empty retained publisher", mutate: func(snapshot *SnapshotDTO) { snapshot.Retained[0].PublisherClientID = "" }},
		{name: "empty retained payload", mutate: func(snapshot *SnapshotDTO) { snapshot.Retained[0].Payload = nil }},
		{name: "retained qos", mutate: func(snapshot *SnapshotDTO) { snapshot.Retained[0].QoS = 3 }},
		{name: "retained count", mutate: func(snapshot *SnapshotDTO) {
			snapshot.Retained = make([]RetainedStateDTO, MaxRetainedMessages+1)
			for index := range snapshot.Retained {
				snapshot.Retained[index] = RetainedStateDTO{TenantID: "tenant", Topic: fmt.Sprintf("topic-%04d", index), PublisherClientID: "publisher", Payload: []byte("x")}
			}
			snapshot.ObjectCount = uint64(len(snapshot.Retained))
		}},
		{name: "retained aggregate bytes", mutate: func(snapshot *SnapshotDTO) {
			snapshot.Retained = make([]RetainedStateDTO, 17)
			payload := bytes.Repeat([]byte{'x'}, MaxRetainedPayloadBytes)
			for index := range snapshot.Retained {
				snapshot.Retained[index] = RetainedStateDTO{TenantID: "tenant", Topic: fmt.Sprintf("topic-%04d", index), PublisherClientID: "publisher", Payload: payload}
			}
			snapshot.ObjectCount = uint64(len(snapshot.Retained))
		}},
	}
	for _, test := range invalidSnapshots {
		t.Run("snapshot/"+test.name, func(t *testing.T) {
			snapshot := cloneSnapshot(validSnapshot)
			test.mutate(&snapshot)
			if err := validSnapshotCheckForTest(snapshot); !errors.Is(err, ErrInvalidSnapshot) {
				t.Fatalf("invalid snapshot error = %v, want ErrInvalidSnapshot", err)
			}
		})
	}
}

func retainedReplaceForTest(tenantID, topic, clientID string, payload []byte, qos uint32, expiresAt, proposedAt uint64) RetainedChangeDTO {
	return RetainedChangeDTO{
		TenantID: tenantID, Topic: topic, PublisherClientID: clientID, Payload: payload, QoS: qos,
		ExpiresAtUnixMillis: expiresAt, ProposedAtUnixMillis: proposedAt, Action: RetainedActionReplace,
	}
}

func retainedCommandForTest(change RetainedChangeDTO) CommandDTO {
	return CommandDTO{SchemaMajor: SchemaMajor, SchemaMinor: SchemaMinor, OperationID: testOperation4, Retained: &change}
}

func applyRetainedForTest(t *testing.T, machine *StateMachine, index uint64, change RetainedChangeDTO) ApplyResultDTO {
	t.Helper()
	result, err := machine.Apply(index, retainedCommandForTest(change))
	if err != nil {
		t.Fatalf("Apply(retained %d): %v", index, err)
	}
	return result
}

func mutateRetainedForTest(change RetainedChangeDTO, mutate func(*RetainedChangeDTO)) RetainedChangeDTO {
	mutated := change
	mutated.Payload = bytes.Clone(change.Payload)
	mutate(&mutated)
	return mutated
}

func validSnapshotCheckForTest(snapshot SnapshotDTO) error {
	return validSnapshot(snapshot)
}
