package raft

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"narumq/internal/application/takeover"
	"narumq/internal/domain/session"
	"narumq/internal/domain/tenancy"
)

const (
	// OwnershipCommandSchemaVersion defines the experimental internal command schema version.
	OwnershipCommandSchemaVersion = 1

	// OwnershipCommandTypeAcquire identifies the acquire session ownership command.
	OwnershipCommandTypeAcquire = "acquire_session_owner"
)

var (
	// ErrInvalidOwnershipCommand indicates that a committed command payload is malformed or unsupported.
	ErrInvalidOwnershipCommand = errors.New("invalid ownership command")
)

type ownershipCommandEnvelope struct {
	SchemaVersion int           `json:"schemaVersion"`
	CommandType   string        `json:"commandType"`
	TenantID      string        `json:"tenantId"`
	ClientID      string        `json:"clientId"`
	ExpectedEpoch session.Epoch `json:"expectedEpoch"`
	OwnerNodeID   string        `json:"ownerNodeId"`
}

// EncodeAcquireOwnershipCommand encodes an application takeover command into canonical internal JSON bytes.
func EncodeAcquireOwnershipCommand(command takeover.AcquireOwnershipCommandDTO) ([]byte, error) {
	if command.SessionKey.TenantID().String() == "" || command.SessionKey.ClientID() == "" || command.OwnerNodeID == "" {
		return nil, fmt.Errorf("%w: incomplete command fields", ErrInvalidOwnershipCommand)
	}

	envelope := ownershipCommandEnvelope{
		SchemaVersion: OwnershipCommandSchemaVersion,
		CommandType:   OwnershipCommandTypeAcquire,
		TenantID:      command.SessionKey.TenantID().String(),
		ClientID:      command.SessionKey.ClientID(),
		ExpectedEpoch: command.ExpectedEpoch,
		OwnerNodeID:   command.OwnerNodeID,
	}

	data, err := json.Marshal(envelope)
	if err != nil {
		return nil, fmt.Errorf("%w: marshal ownership command: %v", ErrInvalidOwnershipCommand, err)
	}
	return data, nil
}

// DecodeAcquireOwnershipCommand parses and validates internal JSON command bytes into an application command DTO.
//
// Invariants enforced:
// - Disallows unknown fields.
// - Strictly single JSON value (no trailing data).
// - Exact schemaVersion == 1.
// - Exact commandType == "acquire_session_owner".
// - Valid tenancy.NewTenantID and tenancy.NewSessionKey.
// - Non-empty OwnerNodeID.
func DecodeAcquireOwnershipCommand(data []byte) (takeover.AcquireOwnershipCommandDTO, error) {
	if len(data) == 0 {
		return takeover.AcquireOwnershipCommandDTO{}, fmt.Errorf("%w: empty payload", ErrInvalidOwnershipCommand)
	}

	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()

	var envelope ownershipCommandEnvelope
	if err := decoder.Decode(&envelope); err != nil {
		return takeover.AcquireOwnershipCommandDTO{}, fmt.Errorf("%w: %v", ErrInvalidOwnershipCommand, err)
	}

	var extra json.RawMessage
	if err := decoder.Decode(&extra); err != io.EOF {
		return takeover.AcquireOwnershipCommandDTO{}, fmt.Errorf("%w: trailing data after JSON value", ErrInvalidOwnershipCommand)
	}

	if envelope.SchemaVersion != OwnershipCommandSchemaVersion {
		return takeover.AcquireOwnershipCommandDTO{}, fmt.Errorf("%w: unsupported schema version %d", ErrInvalidOwnershipCommand, envelope.SchemaVersion)
	}
	if envelope.CommandType != OwnershipCommandTypeAcquire {
		return takeover.AcquireOwnershipCommandDTO{}, fmt.Errorf("%w: unknown command type %q", ErrInvalidOwnershipCommand, envelope.CommandType)
	}
	if envelope.OwnerNodeID == "" {
		return takeover.AcquireOwnershipCommandDTO{}, fmt.Errorf("%w: empty ownerNodeId", ErrInvalidOwnershipCommand)
	}

	tenantID, err := tenancy.NewTenantID(envelope.TenantID)
	if err != nil {
		return takeover.AcquireOwnershipCommandDTO{}, fmt.Errorf("%w: invalid tenant ID: %v", ErrInvalidOwnershipCommand, err)
	}

	sessionKey, err := tenancy.NewSessionKey(tenantID, envelope.ClientID)
	if err != nil {
		return takeover.AcquireOwnershipCommandDTO{}, fmt.Errorf("%w: invalid session key: %v", ErrInvalidOwnershipCommand, err)
	}

	return takeover.AcquireOwnershipCommandDTO{
		SessionKey:    sessionKey,
		ExpectedEpoch: envelope.ExpectedEpoch,
		OwnerNodeID:   envelope.OwnerNodeID,
	}, nil
}

// ApplyOwnershipCommits decodes and applies a sequence of committed entries to the takeover state machine.
//
// If any commit payload is malformed (DecodeAcquireOwnershipCommand fails), processing halts immediately
// and the error is returned. That malformed commit and subsequent commits are NOT applied to the state machine.
//
// If a commit is syntactically valid but fails domain rules (e.g. stale expected epoch, overflow),
// the deterministic domain rejection is recorded in the returned ApplyResultDTO slice, and subsequent
// commits continue to be applied.
func ApplyOwnershipCommits(machine *takeover.StateMachine, commits []CommitDTO) ([]takeover.ApplyResultDTO, error) {
	results := make([]takeover.ApplyResultDTO, 0, len(commits))
	for _, commit := range commits {
		command, err := DecodeAcquireOwnershipCommand(commit.Data)
		if err != nil {
			return results, fmt.Errorf("decode commit at index %d: %w", commit.Index, err)
		}
		result := machine.Apply(commit.Index, command)
		results = append(results, result)
	}
	return results, nil
}
