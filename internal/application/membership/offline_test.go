package membership

import (
	"errors"
	"fmt"
	"testing"
)

// Traceability: CLU-14; docs/specs/cluster-session-and-delivery.md §4.1, §10, §11.9.
func TestOfflineDetachEnqueueClaimPreservesFIFOAndFencing(t *testing.T) {
	machine := NewStateMachine()

	// 1. Acquire ownership for tenant-a / client-a on node 1 at epoch 1.
	applyMustSucceed(t, machine, 1, CommandDTO{
		SchemaMajor: SchemaMajor, SchemaMinor: SchemaMinor, OperationID: testOperation4,
		Ownership: &OwnershipChangeDTO{
			TenantID: "tenant-a", ClientID: "client-a", OwnerNodeID: 1, Epoch: 1, ExpectedEpoch: 0,
			Action: OwnershipActionAcquire,
		},
	})

	// 2. Detach with pending 2 messages
	detachChange := OfflineChangeDTO{
		TenantID: "tenant-a", ClientID: "client-a", OwnerNodeID: 1, Epoch: 1,
		Action: OfflineActionDetach, ProposedAtUnixMillis: 1000,
		Messages: []OfflineMessageDTO{
			{Topic: "topic/1", Payload: []byte("payload-1"), QoS: 0},
			{Topic: "topic/2", Payload: []byte("payload-2"), QoS: 0},
		},
	}
	res := applyOfflineForTest(t, machine, 2, detachChange)
	if res.Outcome != ApplyOutcomeApplied {
		t.Fatalf("detach outcome = %v, want Applied", res.Outcome)
	}

	// Idempotent detach retry: should succeed as no-op
	resRetry := applyOfflineForTest(t, machine, 3, detachChange)
	if resRetry.Outcome != ApplyOutcomeApplied {
		t.Fatalf("detach retry outcome = %v, want Applied", resRetry.Outcome)
	}

	// Stale epoch detach: wrong owner or wrong epoch
	staleDetach := detachChange
	staleDetach.OwnerNodeID = 2
	resStale := applyOfflineForTest(t, machine, 4, staleDetach)
	if resStale.Outcome != ApplyOutcomeConflict || resStale.Conflict != ConflictStaleEpoch {
		t.Fatalf("stale detach = %+v, want ConflictStaleEpoch", resStale)
	}

	// 3. Enqueue two more messages
	enqueue1 := OfflineChangeDTO{
		TenantID: "tenant-a", ClientID: "client-a", OwnerNodeID: 0, Epoch: 1,
		Action: OfflineActionEnqueue, ProposedAtUnixMillis: 1100,
		Messages: []OfflineMessageDTO{
			{Topic: "topic/3", Payload: []byte("payload-3"), QoS: 0},
		},
	}
	resEnq1 := applyOfflineForTest(t, machine, 5, enqueue1)
	if resEnq1.Outcome != ApplyOutcomeApplied {
		t.Fatalf("enqueue 1 outcome = %v", resEnq1.Outcome)
	}

	enqueue2 := OfflineChangeDTO{
		TenantID: "tenant-a", ClientID: "client-a", OwnerNodeID: 0, Epoch: 1,
		Action: OfflineActionEnqueue, ProposedAtUnixMillis: 1200,
		Messages: []OfflineMessageDTO{
			{Topic: "topic/4", Payload: []byte("payload-4"), QoS: 0},
		},
	}
	resEnq2 := applyOfflineForTest(t, machine, 6, enqueue2)
	if resEnq2.Outcome != ApplyOutcomeApplied {
		t.Fatalf("enqueue 2 outcome = %v", resEnq2.Outcome)
	}

	// Enqueue with wrong epoch or missing session
	wrongEpochEnq := enqueue1
	wrongEpochEnq.Epoch = 2
	resWrongEpoch := applyOfflineForTest(t, machine, 7, wrongEpochEnq)
	if resWrongEpoch.Outcome != ApplyOutcomeConflict || resWrongEpoch.Conflict != ConflictOfflineSessionMissing {
		t.Fatalf("wrong epoch enqueue = %+v, want ConflictOfflineSessionMissing", resWrongEpoch)
	}

	missingSessionEnq := enqueue1
	missingSessionEnq.ClientID = "client-missing"
	resMissing := applyOfflineForTest(t, machine, 8, missingSessionEnq)
	if resMissing.Outcome != ApplyOutcomeConflict || resMissing.Conflict != ConflictOfflineSessionMissing {
		t.Fatalf("missing session enqueue = %+v, want ConflictOfflineSessionMissing", resMissing)
	}

	// 4. Non-owner claim should be rejected with stale epoch
	claimNonOwner := OfflineChangeDTO{
		TenantID: "tenant-a", ClientID: "client-a", OwnerNodeID: 2, Epoch: 1,
		Action: OfflineActionClaim, ProposedAtUnixMillis: 1300,
	}
	resClaimNonOwner := applyOfflineForTest(t, machine, 9, claimNonOwner)
	if resClaimNonOwner.Outcome != ApplyOutcomeConflict || resClaimNonOwner.Conflict != ConflictStaleEpoch {
		t.Fatalf("non-owner claim = %+v, want ConflictStaleEpoch", resClaimNonOwner)
	}

	// 5. Transfer ownership to node 2 at epoch 2
	applyMustSucceed(t, machine, 10, CommandDTO{
		SchemaMajor: SchemaMajor, SchemaMinor: SchemaMinor, OperationID: testOperation5,
		Ownership: &OwnershipChangeDTO{
			TenantID: "tenant-a", ClientID: "client-a", OwnerNodeID: 2, Epoch: 2, ExpectedEpoch: 1,
			Action: OwnershipActionTransfer,
		},
	})

	// 6. Authorized Claim by node 2 at epoch 2
	claimAuthorized := OfflineChangeDTO{
		TenantID: "tenant-a", ClientID: "client-a", OwnerNodeID: 2, Epoch: 2,
		Action: OfflineActionClaim, ProposedAtUnixMillis: 1400,
	}
	resClaim := applyOfflineForTest(t, machine, 11, claimAuthorized)
	if resClaim.Outcome != ApplyOutcomeApplied {
		t.Fatalf("claim outcome = %v", resClaim.Outcome)
	}
	if resClaim.OfflineClaim == nil || !resClaim.OfflineClaim.Found {
		t.Fatalf("claim result = %+v, want Found=true", resClaim.OfflineClaim)
	}
	if len(resClaim.OfflineClaim.Messages) != 4 {
		t.Fatalf("claimed message count = %d, want 4", len(resClaim.OfflineClaim.Messages))
	}

	// Check sequences and FIFO order
	for i, want := range []struct {
		seq     uint64
		topic   string
		payload string
	}{
		{seq: 1, topic: "topic/1", payload: "payload-1"},
		{seq: 2, topic: "topic/2", payload: "payload-2"},
		{seq: 3, topic: "topic/3", payload: "payload-3"},
		{seq: 4, topic: "topic/4", payload: "payload-4"},
	} {
		msg := resClaim.OfflineClaim.Messages[i]
		if msg.Sequence != want.seq || msg.Topic != want.topic || string(msg.Payload) != want.payload {
			t.Fatalf("msg[%d] = %+v, want sequence %d topic %s payload %s", i, msg, want.seq, want.topic, want.payload)
		}
	}

	// Offline record should now be deleted
	if len(machine.OfflineSessions()) != 0 {
		t.Fatalf("OfflineSessions() not empty after claim: %+v", machine.OfflineSessions())
	}
	if _, ok := machine.DetachedEpoch("tenant-a", "client-a"); ok {
		t.Fatalf("DetachedEpoch reported true after claim")
	}

	// 7. Second claim should return Found=false
	resSecondClaim := applyOfflineForTest(t, machine, 12, claimAuthorized)
	if resSecondClaim.Outcome != ApplyOutcomeApplied {
		t.Fatalf("second claim outcome = %v", resSecondClaim.Outcome)
	}
	if resSecondClaim.OfflineClaim == nil || resSecondClaim.OfflineClaim.Found || len(resSecondClaim.OfflineClaim.Messages) != 0 {
		t.Fatalf("second claim = %+v, want Found=false and 0 messages", resSecondClaim.OfflineClaim)
	}
}

// Traceability: CLU-14; docs/specs/cluster-session-and-delivery.md §4.1, §10, §11.9.
func TestOfflineCapacityConflictsPreserveState(t *testing.T) {
	machine := NewStateMachine()

	applyMustSucceed(t, machine, 1, CommandDTO{
		SchemaMajor: SchemaMajor, SchemaMinor: SchemaMinor, OperationID: testOperation4,
		Ownership: &OwnershipChangeDTO{
			TenantID: "tenant", ClientID: "client", OwnerNodeID: 1, Epoch: 1, ExpectedEpoch: 0,
			Action: OwnershipActionAcquire,
		},
	})

	// Fill session to 64 messages via detach
	messages := make([]OfflineMessageDTO, MaxOfflineMessagesPerSession)
	for i := range messages {
		messages[i] = OfflineMessageDTO{
			Topic:   fmt.Sprintf("t/%02d", i),
			Payload: []byte(fmt.Sprintf("p-%02d", i)),
			QoS:     0,
		}
	}
	detach := OfflineChangeDTO{
		TenantID: "tenant", ClientID: "client", OwnerNodeID: 1, Epoch: 1,
		Action: OfflineActionDetach, ProposedAtUnixMillis: 1000,
		Messages: messages,
	}
	res := applyOfflineForTest(t, machine, 2, detach)
	if res.Outcome != ApplyOutcomeApplied {
		t.Fatalf("detach 64 messages outcome = %v", res.Outcome)
	}

	// 65th message via enqueue should conflict with ConflictOfflineCapacity
	overflow := OfflineChangeDTO{
		TenantID: "tenant", ClientID: "client", OwnerNodeID: 0, Epoch: 1,
		Action: OfflineActionEnqueue, ProposedAtUnixMillis: 1100,
		Messages: []OfflineMessageDTO{
			{Topic: "t/overflow", Payload: []byte("overflow"), QoS: 0},
		},
	}
	resOverflow := applyOfflineForTest(t, machine, 3, overflow)
	if resOverflow.Outcome != ApplyOutcomeConflict || resOverflow.Conflict != ConflictOfflineCapacity {
		t.Fatalf("overflow enqueue = %+v, want ConflictOfflineCapacity", resOverflow)
	}

	// Ensure state is preserved (still exactly 64 messages)
	sessions := machine.OfflineSessions()
	if len(sessions) != 1 || len(sessions[0].Messages) != MaxOfflineMessagesPerSession {
		t.Fatalf("session count = %d, messages = %d, want 1 session and 64 messages", len(sessions), len(sessions[0].Messages))
	}
}

// Traceability: CLU-14; docs/specs/cluster-session-and-delivery.md §4.1, §10, §11.9.
func TestOfflineExpiryUsesProposerTime(t *testing.T) {
	machine := NewStateMachine()

	applyMustSucceed(t, machine, 1, CommandDTO{
		SchemaMajor: SchemaMajor, SchemaMinor: SchemaMinor, OperationID: testOperation4,
		Ownership: &OwnershipChangeDTO{
			TenantID: "tenant", ClientID: "client", OwnerNodeID: 1, Epoch: 1, ExpectedEpoch: 0,
			Action: OwnershipActionAcquire,
		},
	})

	// Detach session expiring at 2000 ms, with message 1 expiring at 1500 ms and message 2 with no expiry (0)
	detach := OfflineChangeDTO{
		TenantID: "tenant", ClientID: "client", OwnerNodeID: 1, Epoch: 1,
		Action: OfflineActionDetach, ProposedAtUnixMillis: 1000, SessionExpiresAtUnixMillis: 2000,
		Messages: []OfflineMessageDTO{
			{Topic: "t/1", Payload: []byte("p1"), QoS: 0, ExpiresAtUnixMillis: 1500},
			{Topic: "t/2", Payload: []byte("p2"), QoS: 0, ExpiresAtUnixMillis: 0},
		},
	}
	applyOfflineForTest(t, machine, 2, detach)

	// Command proposed at 1600 ms should purge message 1
	enq := OfflineChangeDTO{
		TenantID: "tenant", ClientID: "client", OwnerNodeID: 0, Epoch: 1,
		Action: OfflineActionEnqueue, ProposedAtUnixMillis: 1600,
		Messages: []OfflineMessageDTO{
			{Topic: "t/3", Payload: []byte("p3"), QoS: 0},
		},
	}
	applyOfflineForTest(t, machine, 3, enq)

	sessions := machine.OfflineSessions()
	if len(sessions) != 1 || len(sessions[0].Messages) != 2 {
		t.Fatalf("after msg 1 expiry, messages count = %d, want 2", len(sessions[0].Messages))
	}
	if sessions[0].Messages[0].Topic != "t/2" || sessions[0].Messages[1].Topic != "t/3" {
		t.Fatalf("remaining messages = %+v, want t/2 and t/3", sessions[0].Messages)
	}

	// Command proposed at 2500 ms should purge the entire session
	claim := OfflineChangeDTO{
		TenantID: "tenant", ClientID: "client", OwnerNodeID: 1, Epoch: 1,
		Action: OfflineActionClaim, ProposedAtUnixMillis: 2500,
	}
	resClaim := applyOfflineForTest(t, machine, 4, claim)
	if resClaim.OfflineClaim == nil || resClaim.OfflineClaim.Found {
		t.Fatalf("claim after session expiry = %+v, want Found=false", resClaim.OfflineClaim)
	}
	if len(machine.OfflineSessions()) != 0 {
		t.Fatalf("sessions remaining after session expiry = %d", len(machine.OfflineSessions()))
	}
}

// Traceability: CLU-14; docs/specs/cluster-session-and-delivery.md §4.1, §10, §11.9.
func TestOfflineRecordFollowsOwnershipRelease(t *testing.T) {
	machine := NewStateMachine()

	applyMustSucceed(t, machine, 1, CommandDTO{
		SchemaMajor: SchemaMajor, SchemaMinor: SchemaMinor, OperationID: testOperation4,
		Ownership: &OwnershipChangeDTO{
			TenantID: "tenant", ClientID: "client", OwnerNodeID: 1, Epoch: 1, ExpectedEpoch: 0,
			Action: OwnershipActionAcquire,
		},
	})

	detach := OfflineChangeDTO{
		TenantID: "tenant", ClientID: "client", OwnerNodeID: 1, Epoch: 1,
		Action: OfflineActionDetach, ProposedAtUnixMillis: 1000,
		Messages: []OfflineMessageDTO{
			{Topic: "t/1", Payload: []byte("p1"), QoS: 0},
		},
	}
	applyOfflineForTest(t, machine, 2, detach)

	if len(machine.OfflineSessions()) != 1 {
		t.Fatalf("offline sessions count = %d, want 1", len(machine.OfflineSessions()))
	}

	// Release ownership
	applyMustSucceed(t, machine, 3, CommandDTO{
		SchemaMajor: SchemaMajor, SchemaMinor: SchemaMinor, OperationID: testOperation5,
		Ownership: &OwnershipChangeDTO{
			TenantID: "tenant", ClientID: "client", OwnerNodeID: 1, Epoch: 1, ExpectedEpoch: 1,
			Action: OwnershipActionRelease,
		},
	})

	if len(machine.OfflineSessions()) != 0 {
		t.Fatalf("offline sessions count after release = %d, want 0", len(machine.OfflineSessions()))
	}
	if _, ok := machine.DetachedEpoch("tenant", "client"); ok {
		t.Fatalf("DetachedEpoch returned true after release")
	}
}

// Traceability: CLU-14; docs/specs/cluster-session-and-delivery.md §4.1, §10, §11.9.
func TestOfflineSnapshotRoundTripAndValidation(t *testing.T) {
	machine := NewStateMachine()

	applyMustSucceed(t, machine, 1, CommandDTO{
		SchemaMajor: SchemaMajor, SchemaMinor: SchemaMinor, OperationID: testOperation4,
		Ownership: &OwnershipChangeDTO{
			TenantID: "tenant-a", ClientID: "client-a", OwnerNodeID: 1, Epoch: 1, ExpectedEpoch: 0,
			Action: OwnershipActionAcquire,
		},
	})
	applyMustSucceed(t, machine, 2, CommandDTO{
		SchemaMajor: SchemaMajor, SchemaMinor: SchemaMinor, OperationID: testOperation5,
		Ownership: &OwnershipChangeDTO{
			TenantID: "tenant-a", ClientID: "client-a", OwnerNodeID: 2, Epoch: 2, ExpectedEpoch: 1,
			Action: OwnershipActionTransfer,
		},
	})
	applyMustSucceed(t, machine, 3, CommandDTO{
		SchemaMajor: SchemaMajor, SchemaMinor: SchemaMinor, OperationID: testOperation6,
		Ownership: &OwnershipChangeDTO{
			TenantID: "tenant-b", ClientID: "client-b", OwnerNodeID: 1, Epoch: 1, ExpectedEpoch: 0,
			Action: OwnershipActionAcquire,
		},
	})

	applyOfflineForTest(t, machine, 4, OfflineChangeDTO{
		TenantID: "tenant-b", ClientID: "client-b", OwnerNodeID: 1, Epoch: 1,
		Action: OfflineActionDetach, ProposedAtUnixMillis: 1000,
		Messages: []OfflineMessageDTO{
			{Topic: "t/b", Payload: []byte("pb"), QoS: 0},
		},
	})
	applyOfflineForTest(t, machine, 5, OfflineChangeDTO{
		TenantID: "tenant-a", ClientID: "client-a", OwnerNodeID: 2, Epoch: 2,
		Action: OfflineActionDetach, ProposedAtUnixMillis: 1000,
		Messages: []OfflineMessageDTO{
			{Topic: "t/a1", Payload: []byte("pa1"), QoS: 0},
			{Topic: "t/a2", Payload: []byte("pa2"), QoS: 0},
		},
	})

	snapshot, err := machine.Snapshot(5)
	if err != nil {
		t.Fatalf("Snapshot(): %v", err)
	}

	// 2 ownerships + 2 offline sessions + 3 messages = 7 objects
	if snapshot.ObjectCount != 7 {
		t.Fatalf("snapshot ObjectCount = %d, want 7", snapshot.ObjectCount)
	}
	if len(snapshot.OfflineSessions) != 2 {
		t.Fatalf("snapshot OfflineSessions count = %d, want 2", len(snapshot.OfflineSessions))
	}
	// Sorted: tenant-a before tenant-b
	if snapshot.OfflineSessions[0].TenantID != "tenant-a" || snapshot.OfflineSessions[1].TenantID != "tenant-b" {
		t.Fatalf("snapshot OfflineSessions not sorted: %+v", snapshot.OfflineSessions)
	}

	// Restore onto a clean machine
	machine2 := NewStateMachine()
	if err := machine2.Restore(snapshot); err != nil {
		t.Fatalf("Restore(): %v", err)
	}

	snapshot2, err := machine2.Snapshot(5)
	if err != nil {
		t.Fatalf("Snapshot() after restore: %v", err)
	}
	if !sameSnapshotState(snapshot, snapshot2) {
		t.Fatalf("snapshot mismatch after restore")
	}

	// Negative validation tests
	invalidSnapshots := []struct {
		name   string
		mutate func(*SnapshotDTO)
	}{
		{name: "unsorted offline sessions", mutate: func(s *SnapshotDTO) {
			s.OfflineSessions[0], s.OfflineSessions[1] = s.OfflineSessions[1], s.OfflineSessions[0]
		}},
		{name: "duplicate offline session", mutate: func(s *SnapshotDTO) {
			s.OfflineSessions[1] = s.OfflineSessions[0]
		}},
		{name: "sequence zero", mutate: func(s *SnapshotDTO) {
			s.OfflineSessions[0].Messages[0].Sequence = 0
		}},
		{name: "sequence greater or equal next sequence", mutate: func(s *SnapshotDTO) {
			s.OfflineSessions[0].Messages[0].Sequence = s.OfflineSessions[0].NextSequence
		}},
		{name: "sequence not strictly increasing", mutate: func(s *SnapshotDTO) {
			s.OfflineSessions[0].Messages[1].Sequence = s.OfflineSessions[0].Messages[0].Sequence
		}},
		{name: "message QoS non-zero", mutate: func(s *SnapshotDTO) {
			s.OfflineSessions[0].Messages[0].QoS = 1
		}},
		{name: "missing ownership row", mutate: func(s *SnapshotDTO) {
			s.Ownerships = s.Ownerships[:1] // drop tenant-b ownership
			s.ObjectCount--
		}},
		{name: "ownership epoch smaller than session epoch", mutate: func(s *SnapshotDTO) {
			s.Ownerships[0].Epoch = s.OfflineSessions[0].Epoch - 1
		}},
		{name: "object count mismatch", mutate: func(s *SnapshotDTO) {
			s.ObjectCount++
		}},
	}

	for _, tc := range invalidSnapshots {
		t.Run("validation/"+tc.name, func(t *testing.T) {
			mutated := cloneSnapshot(snapshot)
			tc.mutate(&mutated)
			if err := validSnapshotCheckForTest(mutated); !errors.Is(err, ErrInvalidSnapshot) {
				t.Fatalf("validSnapshot = %v, want ErrInvalidSnapshot", err)
			}
		})
	}
}

func applyOfflineForTest(t *testing.T, machine *StateMachine, index uint64, change OfflineChangeDTO) ApplyResultDTO {
	t.Helper()
	res, err := machine.Apply(index, CommandDTO{
		SchemaMajor: SchemaMajor, SchemaMinor: SchemaMinor, OperationID: testOperation4,
		Offline: &change,
	})
	if err != nil {
		t.Fatalf("Apply(offline %d): %v", index, err)
	}
	return res
}
func TestOfflineCommandValidationRejectsMalformedChanges(t *testing.T) {
	base := func() OfflineChangeDTO {
		return OfflineChangeDTO{
			TenantID: "tenant-a", ClientID: "client-a", OwnerNodeID: 1, Epoch: 1,
			Action: OfflineActionDetach, ProposedAtUnixMillis: 1,
			Messages: []OfflineMessageDTO{{Topic: "events/a"}},
		}
	}
	tests := []struct {
		name   string
		mutate func(*OfflineChangeDTO)
	}{
		{name: "missing tenant", mutate: func(change *OfflineChangeDTO) { change.TenantID = "" }},
		{name: "zero epoch", mutate: func(change *OfflineChangeDTO) { change.Epoch = 0 }},
		{name: "zero proposal time", mutate: func(change *OfflineChangeDTO) { change.ProposedAtUnixMillis = 0 }},
		{name: "nonzero sequence", mutate: func(change *OfflineChangeDTO) { change.Messages[0].Sequence = 1 }},
		{name: "invalid topic", mutate: func(change *OfflineChangeDTO) { change.Messages[0].Topic = "" }},
		{name: "unsupported qos", mutate: func(change *OfflineChangeDTO) { change.Messages[0].QoS = 1 }},
		{name: "oversized payload", mutate: func(change *OfflineChangeDTO) {
			change.Messages[0].Payload = make([]byte, MaxOfflinePayloadBytes+1)
		}},
		{name: "unknown action", mutate: func(change *OfflineChangeDTO) { change.Action = OfflineAction(255) }},
		{name: "detach without owner", mutate: func(change *OfflineChangeDTO) { change.OwnerNodeID = 0 }},
		{name: "detach over message limit", mutate: func(change *OfflineChangeDTO) {
			change.Messages = make([]OfflineMessageDTO, MaxOfflineMessagesPerSession+1)
			for i := range change.Messages {
				change.Messages[i].Topic = "events/a"
			}
		}},
		{name: "detach over byte limit", mutate: func(change *OfflineChangeDTO) {
			change.Messages[0].Payload = make([]byte, MaxOfflineDetachMessageBytes+1)
		}},
		{name: "enqueue with owner", mutate: func(change *OfflineChangeDTO) {
			change.Action = OfflineActionEnqueue
		}},
		{name: "enqueue without exactly one message", mutate: func(change *OfflineChangeDTO) {
			change.Action = OfflineActionEnqueue
			change.Messages = nil
		}},
		{name: "enqueue with session expiry", mutate: func(change *OfflineChangeDTO) {
			change.Action = OfflineActionEnqueue
			change.OwnerNodeID = 0
			change.SessionExpiresAtUnixMillis = 1
		}},
		{name: "claim without owner", mutate: func(change *OfflineChangeDTO) {
			change.Action = OfflineActionClaim
			change.Messages = nil
			change.OwnerNodeID = 0
		}},
		{name: "claim with messages", mutate: func(change *OfflineChangeDTO) {
			change.Action = OfflineActionClaim
		}},
		{name: "claim with session expiry", mutate: func(change *OfflineChangeDTO) {
			change.Action = OfflineActionClaim
			change.Messages = nil
			change.SessionExpiresAtUnixMillis = 1
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			change := base()
			tc.mutate(&change)
			err := validateCommand(CommandDTO{
				SchemaMajor: SchemaMajor, SchemaMinor: SchemaMinor, OperationID: testOperation4,
				Offline: &change,
			})
			if !errors.Is(err, ErrInvalidCommand) {
				t.Fatalf("validateCommand() = %v, want ErrInvalidCommand", err)
			}
		})
	}
}
