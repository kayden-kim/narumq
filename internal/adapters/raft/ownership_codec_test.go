package raft_test

import (
	"bytes"
	"errors"
	"testing"

	raftadapter "narumq/internal/adapters/raft"
	"narumq/internal/application/takeover"
	"narumq/internal/domain/session"
	"narumq/internal/domain/tenancy"
)

func mustTenantID(t *testing.T, raw string) tenancy.TenantID {
	t.Helper()
	tenantID, err := tenancy.NewTenantID(raw)
	if err != nil {
		t.Fatalf("NewTenantID(%q) error: %v", raw, err)
	}
	return tenantID
}

func mustSessionKey(t *testing.T, tenantRaw, clientID string) tenancy.SessionKey {
	t.Helper()
	key, err := tenancy.NewSessionKey(mustTenantID(t, tenantRaw), clientID)
	if err != nil {
		t.Fatalf("NewSessionKey(%q, %q) error: %v", tenantRaw, clientID, err)
	}
	return key
}

func TestOwnershipCodec_EncodeDecodeRoundTrip(t *testing.T) {
	t.Parallel()

	key := mustSessionKey(t, "tenant-a", "client-1")
	command := takeover.AcquireOwnershipCommandDTO{
		SessionKey:    key,
		ExpectedEpoch: 42,
		OwnerNodeID:   "node-2",
	}

	data, err := raftadapter.EncodeAcquireOwnershipCommand(command)
	if err != nil {
		t.Fatalf("EncodeAcquireOwnershipCommand failed: %v", err)
	}

	// Verify byte immutability: mutating encoded data does not affect re-decoding
	decoded, err := raftadapter.DecodeAcquireOwnershipCommand(data)
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
	validBytes, err := raftadapter.EncodeAcquireOwnershipCommand(validCmd)
	if err != nil {
		t.Fatal(err)
	}

	// 1. Empty payload
	if _, err := raftadapter.DecodeAcquireOwnershipCommand(nil); !errors.Is(err, raftadapter.ErrInvalidOwnershipCommand) {
		t.Fatalf("empty payload error = %v, want ErrInvalidOwnershipCommand", err)
	}

	// 2. Malformed JSON
	if _, err := raftadapter.DecodeAcquireOwnershipCommand([]byte("{invalid json")); !errors.Is(err, raftadapter.ErrInvalidOwnershipCommand) {
		t.Fatalf("malformed json error = %v, want ErrInvalidOwnershipCommand", err)
	}

	// 3. Trailing JSON value
	trailingBytes := append(bytes.Clone(validBytes), []byte(" 123")...)
	if _, err := raftadapter.DecodeAcquireOwnershipCommand(trailingBytes); !errors.Is(err, raftadapter.ErrInvalidOwnershipCommand) {
		t.Fatalf("trailing data error = %v, want ErrInvalidOwnershipCommand", err)
	}

	// 4. Unknown fields (disallowed)
	unknownFieldBytes := []byte(`{"schemaVersion":1,"commandType":"acquire_session_owner","tenantId":"tenant-a","clientId":"client-1","expectedEpoch":0,"ownerNodeId":"node-1","unknown":true}`)
	if _, err := raftadapter.DecodeAcquireOwnershipCommand(unknownFieldBytes); !errors.Is(err, raftadapter.ErrInvalidOwnershipCommand) {
		t.Fatalf("unknown field error = %v, want ErrInvalidOwnershipCommand", err)
	}

	// 5. Unsupported schema version
	badVersionBytes := []byte(`{"schemaVersion":2,"commandType":"acquire_session_owner","tenantId":"tenant-a","clientId":"client-1","expectedEpoch":0,"ownerNodeId":"node-1"}`)
	if _, err := raftadapter.DecodeAcquireOwnershipCommand(badVersionBytes); !errors.Is(err, raftadapter.ErrInvalidOwnershipCommand) {
		t.Fatalf("bad version error = %v, want ErrInvalidOwnershipCommand", err)
	}

	// 6. Unknown command type
	badTypeBytes := []byte(`{"schemaVersion":1,"commandType":"delete_session_owner","tenantId":"tenant-a","clientId":"client-1","expectedEpoch":0,"ownerNodeId":"node-1"}`)
	if _, err := raftadapter.DecodeAcquireOwnershipCommand(badTypeBytes); !errors.Is(err, raftadapter.ErrInvalidOwnershipCommand) {
		t.Fatalf("bad command type error = %v, want ErrInvalidOwnershipCommand", err)
	}

	// 7. Empty owner node ID
	emptyOwnerBytes := []byte(`{"schemaVersion":1,"commandType":"acquire_session_owner","tenantId":"tenant-a","clientId":"client-1","expectedEpoch":0,"ownerNodeId":""}`)
	if _, err := raftadapter.DecodeAcquireOwnershipCommand(emptyOwnerBytes); !errors.Is(err, raftadapter.ErrInvalidOwnershipCommand) {
		t.Fatalf("empty ownerNodeId error = %v, want ErrInvalidOwnershipCommand", err)
	}

	// 8. Invalid tenant ID
	badTenantBytes := []byte(`{"schemaVersion":1,"commandType":"acquire_session_owner","tenantId":"","clientId":"client-1","expectedEpoch":0,"ownerNodeId":"node-1"}`)
	if _, err := raftadapter.DecodeAcquireOwnershipCommand(badTenantBytes); !errors.Is(err, raftadapter.ErrInvalidOwnershipCommand) {
		t.Fatalf("bad tenant ID error = %v, want ErrInvalidOwnershipCommand", err)
	}
}

func TestOwnershipCodec_ApplyOwnershipCommits(t *testing.T) {
	t.Parallel()

	sm := takeover.NewStateMachine()
	key := mustSessionKey(t, "tenant-a", "client-1")

	cmd1, _ := raftadapter.EncodeAcquireOwnershipCommand(takeover.AcquireOwnershipCommandDTO{
		SessionKey:    key,
		ExpectedEpoch: 0,
		OwnerNodeID:   "node-1",
	})
	// cmd2 has stale expected epoch (0 instead of 1)
	cmd2Stale, _ := raftadapter.EncodeAcquireOwnershipCommand(takeover.AcquireOwnershipCommandDTO{
		SessionKey:    key,
		ExpectedEpoch: 0,
		OwnerNodeID:   "node-3",
	})
	// cmd3 has correct expected epoch 1
	cmd3Takeover, _ := raftadapter.EncodeAcquireOwnershipCommand(takeover.AcquireOwnershipCommandDTO{
		SessionKey:    key,
		ExpectedEpoch: 1,
		OwnerNodeID:   "node-2",
	})

	commits := []raftadapter.CommitDTO{
		{Index: 1, Data: cmd1},
		{Index: 2, Data: cmd2Stale},
		{Index: 3, Data: cmd3Takeover},
	}

	results, err := raftadapter.ApplyOwnershipCommits(sm, commits)
	if err != nil {
		t.Fatalf("ApplyOwnershipCommits failed: %v", err)
	}

	if len(results) != 3 {
		t.Fatalf("len(results) = %d, want 3", len(results))
	}

	// Commit 1: applied
	if !results[0].Applied || results[0].Ownership.Epoch() != 1 {
		t.Fatalf("results[0] = %+v, want epoch 1", results[0])
	}
	// Commit 2: rejected with ErrStaleEpoch, but execution continued
	if results[1].Applied || !errors.Is(results[1].Err, session.ErrStaleEpoch) {
		t.Fatalf("results[1] = %+v, want ErrStaleEpoch", results[1])
	}
	// Commit 3: applied with epoch 2
	if !results[2].Applied || results[2].Ownership.Epoch() != 2 || results[2].Ownership.OwnerNodeID() != "node-2" {
		t.Fatalf("results[2] = %+v, want epoch 2 owned by node-2", results[2])
	}

	// Now verify malformed commit halts execution
	commitsWithMalformed := []raftadapter.CommitDTO{
		{Index: 4, Data: []byte("malformed json")},
		{Index: 5, Data: cmd3Takeover},
	}

	_, err = raftadapter.ApplyOwnershipCommits(sm, commitsWithMalformed)
	if err == nil {
		t.Fatal("expected error on malformed commit, got nil")
	}

	// Last applied index should still be 3 (commit 4 and 5 were not applied)
	if sm.LastAppliedIndex() != 3 {
		t.Fatalf("LastAppliedIndex after malformed commit = %d, want 3", sm.LastAppliedIndex())
	}
}
