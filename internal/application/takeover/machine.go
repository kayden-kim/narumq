// Package takeover implements a deterministic session ownership state machine.
package takeover

import (
	"errors"
	"sync"

	"narumq/internal/domain/session"
	"narumq/internal/domain/tenancy"
)

var (
	// ErrOutOfOrderCommit indicates that an application sequence index is zero or not strictly greater than the last applied sequence position.
	ErrOutOfOrderCommit = errors.New("out of order commit index")
)

// AcquireOwnershipCommandDTO carries input parameters for acquiring session ownership.
type AcquireOwnershipCommandDTO struct {
	SessionKey    tenancy.SessionKey
	ExpectedEpoch session.Epoch
	OwnerNodeID   string
}

// ApplyResultDTO represents the deterministic outcome of applying a committed ownership command.
type ApplyResultDTO struct {
	Ownership session.Ownership
	Applied   bool
	Err       error
}

// StateMachine maintains replicated session ownership records indexed by session key.
type StateMachine struct {
	mu               sync.RWMutex
	lastAppliedIndex uint64
	ownerships       map[tenancy.SessionKey]session.Ownership
}

// NewStateMachine constructs an empty session ownership state machine.
func NewStateMachine() *StateMachine {
	return &StateMachine{
		ownerships: make(map[tenancy.SessionKey]session.Ownership),
	}
}

// LastAppliedIndex returns the highest application committed-command sequence position applied so far.
func (sm *StateMachine) LastAppliedIndex() uint64 {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.lastAppliedIndex
}

// Ownership returns the current ownership record for the given key, if any.
func (sm *StateMachine) Ownership(key tenancy.SessionKey) (session.Ownership, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	record, exists := sm.ownerships[key]
	return record, exists
}

// Authorize verifies that the given key, ownerNodeID, and epoch match the active ownership.
func (sm *StateMachine) Authorize(key tenancy.SessionKey, ownerNodeID string, epoch session.Epoch) error {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	record, exists := sm.ownerships[key]
	if !exists {
		return session.ErrInvalidOwnership
	}
	return record.Authorize(key, ownerNodeID, epoch)
}

// Apply processes a committed ownership command at the specified application sequence index.
//
// The commitIndex parameter is an application-owned monotonic sequence number
// assigned while consuming the ownership aggregate's committed-command stream.
// It is not a Raft log index or the consensus adapter's StatusDTO.CommitIndex.
//
// Invariants:
//   - commitIndex must be non-zero and strictly greater than lastAppliedIndex.
//   - If commitIndex is invalid or out-of-order, ErrOutOfOrderCommit is returned without modifying any state.
//   - If commitIndex is valid, lastAppliedIndex advances to commitIndex.
//   - If the command fails domain validation (e.g. ErrStaleEpoch, ErrInvalidOwnership),
//     the domain error is returned in ApplyResultDTO with Applied = false, leaving the ownership map untouched.
//   - If the command succeeds, the new ownership is recorded and returned with Applied = true.
func (sm *StateMachine) Apply(commitIndex uint64, command AcquireOwnershipCommandDTO) ApplyResultDTO {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if commitIndex == 0 || commitIndex <= sm.lastAppliedIndex {
		return ApplyResultDTO{
			Applied: false,
			Err:     ErrOutOfOrderCommit,
		}
	}

	sm.lastAppliedIndex = commitIndex

	var current *session.Ownership
	if existing, exists := sm.ownerships[command.SessionKey]; exists {
		current = &existing
	}

	record, err := session.AcquireOwnership(current, command.SessionKey, command.ExpectedEpoch, command.OwnerNodeID, commitIndex)
	if err != nil {
		return ApplyResultDTO{
			Applied: false,
			Err:     err,
		}
	}

	sm.ownerships[command.SessionKey] = record
	return ApplyResultDTO{
		Ownership: record,
		Applied:   true,
		Err:       nil,
	}
}
