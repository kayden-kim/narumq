package takeover_test

import (
	"bytes"
	"errors"
	"testing"

	"narumq/internal/application/takeover"
	"narumq/internal/domain/session"
)

func TestOwnershipCodec_EncodeDecodeRoundTrip(t *testing.T) {
	t.Parallel()

	key := mustSessionKey(t, "tenant-a", "client-1")
	command := takeover.AcquireOwnershipCommandDTO{
		SessionKey:    key,
		ExpectedEpoch: 42,
		OwnerNodeID:   "node-2",
	}

	data, err := takeover.EncodeAcquireOwnershipCommand(command)
	if err != nil {
		t.Fatalf("EncodeAcquireOwnershipCommand failed: %v", err)
	}

	// Verify byte immutability: mutating encoded data does not affect re-decoding.
	decoded, err := takeover.DecodeAcquireOwnershipCommand(data)
	if err != nil {
		t.Fatalf("DecodeAcquireOwnershipCommand failed: %v", err)
	}

	if decoded.SessionKey != command.SessionKey {
		t.Fatalf("SessionKey = %v, want %v", decoded.SessionKey, command.SessionKey)
	}
	if decoded.ExpectedEpoch != command.ExpectedEpoch {
		t.Fatalf("ExpectedEpoch = %d, want %d", decoded.ExpectedEpoch, command.ExpectedEpoch)
	}
	if decoded.OwnerNodeID != command.OwnerNodeID {
		t.Fatalf("OwnerNodeID = %q, want %q", decoded.OwnerNodeID, command.OwnerNodeID)
	}
}

func TestOwnershipCodec_DecodeRejections(t *testing.T) {
	t.Parallel()

	validKey := mustSessionKey(t, "tenant-a", "client-1")
	validCmd := takeover.AcquireOwnershipCommandDTO{
		SessionKey:    validKey,
		ExpectedEpoch: 0,
		OwnerNodeID:   "node-1",
	}
	validBytes, err := takeover.EncodeAcquireOwnershipCommand(validCmd)
	if err != nil {
		t.Fatal(err)
	}

	// 1. Empty payload.
	if _, err := takeover.DecodeAcquireOwnershipCommand(nil); !errors.Is(err, takeover.ErrInvalidOwnershipCommand) {
		t.Fatalf("empty payload error = %v, want ErrInvalidOwnershipCommand", err)
	}

	// 2. Malformed JSON.
	if _, err := takeover.DecodeAcquireOwnershipCommand([]byte("{invalid json")); !errors.Is(err, takeover.ErrInvalidOwnershipCommand) {
		t.Fatalf("malformed json error = %v, want ErrInvalidOwnershipCommand", err)
	}

	// 3. Trailing JSON value.
	trailingBytes := append(bytes.Clone(validBytes), []byte(" 123")...)
	if _, err := takeover.DecodeAcquireOwnershipCommand(trailingBytes); !errors.Is(err, takeover.ErrInvalidOwnershipCommand) {
		t.Fatalf("trailing data error = %v, want ErrInvalidOwnershipCommand", err)
	}

	// 4. Unknown fields (disallowed).
	unknownFieldBytes := []byte(`{"schemaVersion":1,"commandType":"acquire_session_owner","tenantId":"tenant-a","clientId":"client-1","expectedEpoch":0,"ownerNodeId":"node-1","unknown":true}`)
	if _, err := takeover.DecodeAcquireOwnershipCommand(unknownFieldBytes); !errors.Is(err, takeover.ErrInvalidOwnershipCommand) {
		t.Fatalf("unknown field error = %v, want ErrInvalidOwnershipCommand", err)
	}

	// 5. Unsupported schema version.
	badVersionBytes := []byte(`{"schemaVersion":2,"commandType":"acquire_session_owner","tenantId":"tenant-a","clientId":"client-1","expectedEpoch":0,"ownerNodeId":"node-1"}`)
	if _, err := takeover.DecodeAcquireOwnershipCommand(badVersionBytes); !errors.Is(err, takeover.ErrInvalidOwnershipCommand) {
		t.Fatalf("bad version error = %v, want ErrInvalidOwnershipCommand", err)
	}

	// 6. Unknown command type.
	badTypeBytes := []byte(`{"schemaVersion":1,"commandType":"delete_session_owner","tenantId":"tenant-a","clientId":"client-1","expectedEpoch":0,"ownerNodeId":"node-1"}`)
	if _, err := takeover.DecodeAcquireOwnershipCommand(badTypeBytes); !errors.Is(err, takeover.ErrInvalidOwnershipCommand) {
		t.Fatalf("bad command type error = %v, want ErrInvalidOwnershipCommand", err)
	}

	// 7. Empty owner node ID.
	emptyOwnerBytes := []byte(`{"schemaVersion":1,"commandType":"acquire_session_owner","tenantId":"tenant-a","clientId":"client-1","expectedEpoch":0,"ownerNodeId":""}`)
	if _, err := takeover.DecodeAcquireOwnershipCommand(emptyOwnerBytes); !errors.Is(err, takeover.ErrInvalidOwnershipCommand) {
		t.Fatalf("empty ownerNodeId error = %v, want ErrInvalidOwnershipCommand", err)
	}

	// 8. Invalid tenant ID.
	badTenantBytes := []byte(`{"schemaVersion":1,"commandType":"acquire_session_owner","tenantId":"","clientId":"client-1","expectedEpoch":0,"ownerNodeId":"node-1"}`)
	if _, err := takeover.DecodeAcquireOwnershipCommand(badTenantBytes); !errors.Is(err, takeover.ErrInvalidOwnershipCommand) {
		t.Fatalf("bad tenant ID error = %v, want ErrInvalidOwnershipCommand", err)
	}
}

func TestOwnershipCodec_ApplyOwnershipCommands(t *testing.T) {
	t.Parallel()

	machine := takeover.NewStateMachine()
	key := mustSessionKey(t, "tenant-a", "client-1")

	cmd1, err := takeover.EncodeAcquireOwnershipCommand(takeover.AcquireOwnershipCommandDTO{
		SessionKey:    key,
		ExpectedEpoch: 0,
		OwnerNodeID:   "node-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	// cmd2 has stale expected epoch (0 instead of 1).
	cmd2Stale, err := takeover.EncodeAcquireOwnershipCommand(takeover.AcquireOwnershipCommandDTO{
		SessionKey:    key,
		ExpectedEpoch: 0,
		OwnerNodeID:   "node-3",
	})
	if err != nil {
		t.Fatal(err)
	}
	// cmd3 has correct expected epoch 1.
	cmd3Takeover, err := takeover.EncodeAcquireOwnershipCommand(takeover.AcquireOwnershipCommandDTO{
		SessionKey:    key,
		ExpectedEpoch: 1,
		OwnerNodeID:   "node-2",
	})
	if err != nil {
		t.Fatal(err)
	}

	commands := [][]byte{cmd1, cmd2Stale, cmd3Takeover}
	results, err := takeover.ApplyOwnershipCommands(machine, commands)
	if err != nil {
		t.Fatalf("ApplyOwnershipCommands failed: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("len(results) = %d, want 3", len(results))
	}

	// Command 1: applied at application sequence position 1.
	if !results[0].Applied || results[0].Ownership.Epoch() != 1 || results[0].Ownership.CommitIndex() != 1 {
		t.Fatalf("results[0] = %+v, want epoch 1 at sequence 1", results[0])
	}
	// Command 2: rejected with ErrStaleEpoch, but execution continued at position 2.
	if results[1].Applied || !errors.Is(results[1].Err, session.ErrStaleEpoch) {
		t.Fatalf("results[1] = %+v, want ErrStaleEpoch", results[1])
	}
	// Command 3: applied at position 3.
	if !results[2].Applied || results[2].Ownership.Epoch() != 2 || results[2].Ownership.OwnerNodeID() != "node-2" || results[2].Ownership.CommitIndex() != 3 {
		t.Fatalf("results[2] = %+v, want epoch 2 owned by node-2 at sequence 3", results[2])
	}
	if machine.LastAppliedIndex() != 3 {
		t.Fatalf("LastAppliedIndex = %d, want 3", machine.LastAppliedIndex())
	}

	replayed := takeover.NewStateMachine()
	if _, err := takeover.ApplyOwnershipCommands(replayed, commands); err != nil {
		t.Fatalf("replaying valid command stream failed: %v", err)
	}
	replayedOwner, ok := replayed.Ownership(key)
	if !ok {
		t.Fatal("replayed ownership missing")
	}
	owner, ok := machine.Ownership(key)
	if !ok {
		t.Fatal("ownership missing")
	}
	if replayedOwner.OwnerNodeID() != owner.OwnerNodeID() || replayedOwner.Epoch() != owner.Epoch() || replayedOwner.CommitIndex() != owner.CommitIndex() {
		t.Fatalf("replayed ownership = %+v, want %+v", replayedOwner, owner)
	}
	if replayed.LastAppliedIndex() != machine.LastAppliedIndex() {
		t.Fatalf("replayed LastAppliedIndex = %d, want %d", replayed.LastAppliedIndex(), machine.LastAppliedIndex())
	}

	malformedMachine := takeover.NewStateMachine()
	malformedCommands := [][]byte{cmd1, []byte("malformed json"), cmd3Takeover}
	malformedResults, err := takeover.ApplyOwnershipCommands(malformedMachine, malformedCommands)
	if !errors.Is(err, takeover.ErrInvalidOwnershipCommand) {
		t.Fatalf("malformed middle command error = %v, want ErrInvalidOwnershipCommand", err)
	}
	if len(malformedResults) != 1 || malformedMachine.LastAppliedIndex() != 1 {
		t.Fatalf("malformed stream results=%d index=%d, want one result and index 1", len(malformedResults), malformedMachine.LastAppliedIndex())
	}
	malformedOwner, ok := malformedMachine.Ownership(key)
	if !ok || malformedOwner.OwnerNodeID() != "node-1" || malformedOwner.Epoch() != 1 || malformedOwner.CommitIndex() != 1 {
		t.Fatalf("malformed stream ownership = %+v, want node-1 epoch 1 sequence 1", malformedOwner)
	}
}
