package records

import (
	"bytes"
	"testing"

	"google.golang.org/protobuf/proto"
)

// Traceability: CLU-14; docs/specs/cluster-session-and-delivery.md §4.1, §10, §11.9.
func TestOfflineCommandCodecRoundTrip(t *testing.T) {
	cases := []struct {
		name    string
		command *CommandEnvelope
	}{
		{
			name: "detach command",
			command: &CommandEnvelope{
				SchemaMajor: 1, OperationId: op1,
				Payload: &CommandEnvelope_Offline{Offline: &OfflineChange{
					TenantId: "t1", ClientId: "c1", OwnerNodeId: 1, Epoch: 1,
					Action:               OfflineAction_OFFLINE_ACTION_DETACH,
					ProposedAtUnixMillis: 1000, SessionExpiresAtUnixMillis: 2000,
					Messages: []*OfflineMessage{
						{Sequence: 0, Topic: "topic/1", Payload: []byte("p1"), Qos: 0},
						{Sequence: 0, Topic: "topic/2", Payload: []byte("p2"), Qos: 0, ExpiresAtUnixMillis: 1500},
					},
				}},
			},
		},
		{
			name: "enqueue command",
			command: &CommandEnvelope{
				SchemaMajor: 1, OperationId: op1,
				Payload: &CommandEnvelope_Offline{Offline: &OfflineChange{
					TenantId: "t1", ClientId: "c1", OwnerNodeId: 0, Epoch: 1,
					Action:               OfflineAction_OFFLINE_ACTION_ENQUEUE,
					ProposedAtUnixMillis: 1100,
					Messages: []*OfflineMessage{
						{Sequence: 0, Topic: "topic/3", Payload: []byte("p3"), Qos: 0},
					},
				}},
			},
		},
		{
			name: "claim command",
			command: &CommandEnvelope{
				SchemaMajor: 1, OperationId: op1,
				Payload: &CommandEnvelope_Offline{Offline: &OfflineChange{
					TenantId: "t1", ClientId: "c1", OwnerNodeId: 2, Epoch: 2,
					Action:               OfflineAction_OFFLINE_ACTION_CLAIM,
					ProposedAtUnixMillis: 1200,
				}},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			wire, err := MarshalCommand(tc.command)
			if err != nil {
				t.Fatalf("MarshalCommand() error = %v", err)
			}
			decoded, err := UnmarshalCommand(wire)
			if err != nil {
				t.Fatalf("UnmarshalCommand() error = %v", err)
			}
			if !proto.Equal(decoded, tc.command) {
				t.Fatalf("round-trip mismatch: got %+v, want %+v", decoded, tc.command)
			}
		})
	}
}

// Traceability: CLU-14; docs/specs/cluster-session-and-delivery.md §4.1, §10, §11.9.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-041
func TestOfflineCommandCodecRejectsInvalidMutations(t *testing.T) {
	validDetach := &OfflineChange{
		TenantId: "tenant", ClientId: "client", OwnerNodeId: 1, Epoch: 1,
		Action: OfflineAction_OFFLINE_ACTION_DETACH, ProposedAtUnixMillis: 1000,
		Messages: []*OfflineMessage{
			{Topic: "t/1", Payload: []byte("v"), Qos: 0},
		},
	}

	cases := []struct {
		name   string
		change *OfflineChange
	}{
		{name: "nil payload", change: nil},
		{name: "empty tenant", change: mutateOfflineChange(validDetach, func(c *OfflineChange) { c.TenantId = "" })},
		{name: "empty client", change: mutateOfflineChange(validDetach, func(c *OfflineChange) { c.ClientId = "" })},
		{name: "zero epoch", change: mutateOfflineChange(validDetach, func(c *OfflineChange) { c.Epoch = 0 })},
		{name: "zero proposed at", change: mutateOfflineChange(validDetach, func(c *OfflineChange) { c.ProposedAtUnixMillis = 0 })},
		{name: "nil message in detach", change: mutateOfflineChange(validDetach, func(c *OfflineChange) { c.Messages = []*OfflineMessage{nil} })},
		{name: "message sequence non-zero in command", change: mutateOfflineChange(validDetach, func(c *OfflineChange) { c.Messages[0].Sequence = 1 })},
		{name: "invalid message topic", change: mutateOfflineChange(validDetach, func(c *OfflineChange) { c.Messages[0].Topic = "#" })},
		{name: "message QoS non-zero", change: mutateOfflineChange(validDetach, func(c *OfflineChange) { c.Messages[0].Qos = 1 })},
		{name: "oversized payload", change: mutateOfflineChange(validDetach, func(c *OfflineChange) { c.Messages[0].Payload = make([]byte, maxPublishPayload+1) })},
		{name: "detach zero owner node ID", change: mutateOfflineChange(validDetach, func(c *OfflineChange) { c.OwnerNodeId = 0 })},
		{name: "detach message count exceeds 64", change: mutateOfflineChange(validDetach, func(c *OfflineChange) {
			c.Messages = make([]*OfflineMessage, maxOfflineMessagesPerSession+1)
			for i := range c.Messages {
				c.Messages[i] = &OfflineMessage{Topic: "t", Payload: []byte("p"), Qos: 0}
			}
		})},
		{name: "detach message bytes exceeds 768 KiB", change: mutateOfflineChange(validDetach, func(c *OfflineChange) {
			c.Messages = []*OfflineMessage{
				{Topic: "t", Payload: make([]byte, maxOfflineDetachMessageBytes+1), Qos: 0},
			}
		})},
		{name: "enqueue owner node ID non-zero", change: &OfflineChange{
			TenantId: "t", ClientId: "c", OwnerNodeId: 1, Epoch: 1,
			Action: OfflineAction_OFFLINE_ACTION_ENQUEUE, ProposedAtUnixMillis: 1000,
			Messages: []*OfflineMessage{{Topic: "t", Payload: []byte("p"), Qos: 0}},
		}},
		{name: "enqueue message count != 1", change: &OfflineChange{
			TenantId: "t", ClientId: "c", OwnerNodeId: 0, Epoch: 1,
			Action: OfflineAction_OFFLINE_ACTION_ENQUEUE, ProposedAtUnixMillis: 1000,
			Messages: []*OfflineMessage{
				{Topic: "t1", Payload: []byte("p1"), Qos: 0},
				{Topic: "t2", Payload: []byte("p2"), Qos: 0},
			},
		}},
		{name: "enqueue session expires at non-zero", change: &OfflineChange{
			TenantId: "t", ClientId: "c", OwnerNodeId: 0, Epoch: 1,
			Action: OfflineAction_OFFLINE_ACTION_ENQUEUE, ProposedAtUnixMillis: 1000,
			SessionExpiresAtUnixMillis: 100,
			Messages:                   []*OfflineMessage{{Topic: "t", Payload: []byte("p"), Qos: 0}},
		}},
		{name: "claim owner node ID zero", change: &OfflineChange{
			TenantId: "t", ClientId: "c", OwnerNodeId: 0, Epoch: 1,
			Action: OfflineAction_OFFLINE_ACTION_CLAIM, ProposedAtUnixMillis: 1000,
		}},
		{name: "claim with messages", change: &OfflineChange{
			TenantId: "t", ClientId: "c", OwnerNodeId: 1, Epoch: 1,
			Action: OfflineAction_OFFLINE_ACTION_CLAIM, ProposedAtUnixMillis: 1000,
			Messages: []*OfflineMessage{{Topic: "t", Payload: []byte("p"), Qos: 0}},
		}},
		{name: "claim session expires at non-zero", change: &OfflineChange{
			TenantId: "t", ClientId: "c", OwnerNodeId: 1, Epoch: 1,
			Action: OfflineAction_OFFLINE_ACTION_CLAIM, ProposedAtUnixMillis: 1000,
			SessionExpiresAtUnixMillis: 100,
		}},
		{name: "unknown action", change: mutateOfflineChange(validDetach, func(c *OfflineChange) { c.Action = OfflineAction(99) })},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cmd := &CommandEnvelope{
				SchemaMajor: 1, OperationId: op1,
				Payload: &CommandEnvelope_Offline{Offline: tc.change},
			}
			if _, err := MarshalCommand(cmd); err == nil {
				t.Fatalf("MarshalCommand() accepted invalid offline mutation: %s", tc.name)
			}
		})
	}
}

// Traceability: CLU-14; docs/specs/cluster-session-and-delivery.md §4.1, §10, §11.9.
func TestOfflineSnapshotCodecSortsAndCounts(t *testing.T) {
	state := &ReplicatedStateSnapshot{
		OfflineSessions: []*OfflineSessionState{
			{
				TenantId: "tenant-b", ClientId: "client-b", Epoch: 1, NextSequence: 2,
				Messages: []*OfflineMessage{
					{Sequence: 1, Topic: "t/b", Payload: []byte("pb"), Qos: 0},
				},
			},
			{
				TenantId: "tenant-a", ClientId: "client-a", Epoch: 2, NextSequence: 3,
				Messages: []*OfflineMessage{
					{Sequence: 1, Topic: "t/a1", Payload: []byte("pa1"), Qos: 0},
					{Sequence: 2, Topic: "t/a2", Payload: []byte("pa2"), Qos: 0},
				},
			},
		},
	}

	before := proto.Clone(state)
	wire, err := MarshalSnapshot(state, 5)
	if err != nil {
		t.Fatalf("MarshalSnapshot(): %v", err)
	}
	if !proto.Equal(state, before) {
		t.Fatal("MarshalSnapshot() mutated source snapshot")
	}

	envelope, err := UnmarshalSnapshot(wire)
	if err != nil {
		t.Fatalf("UnmarshalSnapshot(): %v", err)
	}

	// 2 sessions + 3 messages = 5 objects
	if envelope.ObjectCount != 5 {
		t.Fatalf("envelope ObjectCount = %d, want 5", envelope.ObjectCount)
	}
	if len(envelope.State.OfflineSessions) != 2 {
		t.Fatalf("envelope OfflineSessions count = %d, want 2", len(envelope.State.OfflineSessions))
	}
	// Sorted: tenant-a before tenant-b
	if envelope.State.OfflineSessions[0].TenantId != "tenant-a" || envelope.State.OfflineSessions[1].TenantId != "tenant-b" {
		t.Fatalf("snapshot OfflineSessions not sorted: %+v", envelope.State.OfflineSessions)
	}

	reencoded, err := MarshalSnapshot(envelope.State, 5)
	if err != nil || !bytes.Equal(reencoded, wire) {
		t.Fatalf("canonical offline snapshot re-encode error = %v", err)
	}
}

// Traceability: CLU-14; docs/specs/cluster-session-and-delivery.md §4.1, §10, §11.9.
func TestOfflineSnapshotCodecRejectsInvalidState(t *testing.T) {
	validSession := &OfflineSessionState{
		TenantId: "tenant", ClientId: "client", Epoch: 1, NextSequence: 2,
		Messages: []*OfflineMessage{
			{Sequence: 1, Topic: "t/1", Payload: []byte("val"), Qos: 0},
		},
	}

	cases := []struct {
		name     string
		sessions []*OfflineSessionState
	}{
		{name: "nil session", sessions: []*OfflineSessionState{nil}},
		{name: "empty tenant", sessions: []*OfflineSessionState{
			mutateOfflineSession(validSession, func(s *OfflineSessionState) { s.TenantId = "" }),
		}},
		{name: "empty client", sessions: []*OfflineSessionState{
			mutateOfflineSession(validSession, func(s *OfflineSessionState) { s.ClientId = "" }),
		}},
		{name: "zero epoch", sessions: []*OfflineSessionState{
			mutateOfflineSession(validSession, func(s *OfflineSessionState) { s.Epoch = 0 }),
		}},
		{name: "zero next sequence", sessions: []*OfflineSessionState{
			mutateOfflineSession(validSession, func(s *OfflineSessionState) { s.NextSequence = 0 }),
		}},
		{name: "session messages exceed 64", sessions: []*OfflineSessionState{
			mutateOfflineSession(validSession, func(s *OfflineSessionState) {
				s.NextSequence = 100
				s.Messages = make([]*OfflineMessage, maxOfflineMessagesPerSession+1)
				for i := range s.Messages {
					s.Messages[i] = &OfflineMessage{Sequence: uint64(i + 1), Topic: "t", Payload: []byte("p"), Qos: 0}
				}
			}),
		}},
		{name: "nil message in session", sessions: []*OfflineSessionState{
			mutateOfflineSession(validSession, func(s *OfflineSessionState) {
				s.Messages = []*OfflineMessage{nil}
			}),
		}},
		{name: "message sequence zero", sessions: []*OfflineSessionState{
			mutateOfflineSession(validSession, func(s *OfflineSessionState) {
				s.Messages[0].Sequence = 0
			}),
		}},
		{name: "message sequence >= next sequence", sessions: []*OfflineSessionState{
			mutateOfflineSession(validSession, func(s *OfflineSessionState) {
				s.Messages[0].Sequence = s.NextSequence
			}),
		}},
		{name: "message sequences not strictly increasing", sessions: []*OfflineSessionState{
			mutateOfflineSession(validSession, func(s *OfflineSessionState) {
				s.NextSequence = 10
				s.Messages = []*OfflineMessage{
					{Sequence: 1, Topic: "t1", Payload: []byte("p1"), Qos: 0},
					{Sequence: 1, Topic: "t2", Payload: []byte("p2"), Qos: 0},
				}
			}),
		}},
		{name: "invalid message topic", sessions: []*OfflineSessionState{
			mutateOfflineSession(validSession, func(s *OfflineSessionState) {
				s.Messages[0].Topic = "#"
			}),
		}},
		{name: "message QoS non-zero", sessions: []*OfflineSessionState{
			mutateOfflineSession(validSession, func(s *OfflineSessionState) {
				s.Messages[0].Qos = 1
			}),
		}},
		{name: "oversized payload", sessions: []*OfflineSessionState{
			mutateOfflineSession(validSession, func(s *OfflineSessionState) {
				s.Messages[0].Payload = make([]byte, maxPublishPayload+1)
			}),
		}},
		{name: "duplicate session", sessions: []*OfflineSessionState{
			validSession,
			validSession,
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			snap := &ReplicatedStateSnapshot{OfflineSessions: tc.sessions}
			if _, err := MarshalSnapshot(snap, 1); err == nil {
				t.Fatalf("MarshalSnapshot() accepted invalid offline state: %s", tc.name)
			}
		})
	}
}

// Traceability: CLU-14; docs/specs/cluster-session-and-delivery.md §4.1, §10, §11.9.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-041
func TestOfflineSnapshotCodecRejectsAggregateByteLimit(t *testing.T) {
	messages := make([]*OfflineMessage, 17)
	for i := range messages {
		messages[i] = &OfflineMessage{
			Sequence: uint64(i + 1), Topic: "t",
			Payload: make([]byte, maxPublishPayload), Qos: 0,
		}
	}
	snapshot := &ReplicatedStateSnapshot{
		OfflineSessions: []*OfflineSessionState{{
			TenantId: "tenant", ClientId: "client", Epoch: 1,
			NextSequence: uint64(len(messages) + 1), Messages: messages,
		}},
	}
	if _, err := MarshalSnapshot(snapshot, 1); err == nil {
		t.Fatal("MarshalSnapshot() accepted offline bytes above the aggregate limit")
	}
}

func mutateOfflineChange(c *OfflineChange, mutate func(*OfflineChange)) *OfflineChange {
	copied := proto.Clone(c).(*OfflineChange)
	mutate(copied)
	return copied
}

func mutateOfflineSession(s *OfflineSessionState, mutate func(*OfflineSessionState)) *OfflineSessionState {
	copied := proto.Clone(s).(*OfflineSessionState)
	mutate(copied)
	return copied
}
