package session

import (
	"errors"
	"math"

	"narumq/internal/domain/tenancy"
)

var (
	// ErrInvalidOwnership indicates an invalid session key, node ID, or application sequence index.
	ErrInvalidOwnership = errors.New("invalid ownership")
	// ErrStaleEpoch indicates that the expected epoch does not match the current ownership epoch.
	ErrStaleEpoch = errors.New("stale ownership epoch")
	// ErrEpochOverflow indicates that the ownership epoch cannot exceed math.MaxUint64.
	ErrEpochOverflow = errors.New("ownership epoch overflow")
	// ErrStaleCommitIndex indicates that the application sequence index is not strictly greater than the current sequence index.
	ErrStaleCommitIndex = errors.New("stale ownership commit index")
)

// Epoch represents a monotonically increasing ownership generation number.
type Epoch uint64

// Ownership is a value-owned session ownership record.
type Ownership struct {
	sessionKey  tenancy.SessionKey
	ownerNodeID string
	epoch       Epoch
	commitIndex uint64
}

// SessionKey returns the tenant-scoped session key.
func (o Ownership) SessionKey() tenancy.SessionKey {
	return o.sessionKey
}

// OwnerNodeID returns the identifier of the node currently owning the session.
func (o Ownership) OwnerNodeID() string {
	return o.ownerNodeID
}

// Epoch returns the generation number of the ownership.
func (o Ownership) Epoch() Epoch {
	return o.epoch
}

// CommitIndex returns the application committed-command sequence position for this ownership record.
func (o Ownership) CommitIndex() uint64 {
	return o.commitIndex
}

// Authorize checks whether the given session key, owner node ID, and epoch match this ownership record.
func (o Ownership) Authorize(key tenancy.SessionKey, ownerNodeID string, epoch Epoch) error {
	if o.sessionKey.TenantID().String() == "" || o.sessionKey.ClientID() == "" || o.epoch == 0 {
		return ErrInvalidOwnership
	}
	if key != o.sessionKey {
		return ErrInvalidOwnership
	}
	if ownerNodeID == "" {
		return ErrInvalidOwnership
	}
	if epoch != o.epoch {
		return ErrStaleEpoch
	}
	if ownerNodeID != o.ownerNodeID {
		return ErrInvalidOwnership
	}
	return nil
}

// AcquireOwnership validates and creates or updates a session ownership record.
//
// The commitIndex parameter is an application-owned monotonic sequence position
// assigned while consuming the ownership aggregate's committed-command stream.
// It is not a Raft log index or the consensus adapter's StatusDTO.CommitIndex.
//
// Invariants:
// - key must be valid (non-empty TenantID and ClientID).
// - ownerNodeID must not be empty.
// - commitIndex must be > 0.
// - If current == nil (first acquire for this key): expected must be 0; returns Ownership with Epoch = 1.
// - If current != nil:
//   - current must be valid (non-empty key, epoch > 0, commitIndex > 0).
//   - key must equal current.SessionKey().
//   - expected must equal current.Epoch().
//   - commitIndex must be strictly greater than current.CommitIndex().
//   - current.Epoch() must not be math.MaxUint64 (if it is, ErrEpochOverflow).
//   - Returns new Ownership with Epoch = current.Epoch() + 1.
//
// Failure leaves input current unchanged (value-owned immutability).
func AcquireOwnership(current *Ownership, key tenancy.SessionKey, expected Epoch, ownerNodeID string, commitIndex uint64) (Ownership, error) {
	if key.TenantID().String() == "" || key.ClientID() == "" {
		return Ownership{}, ErrInvalidOwnership
	}
	if ownerNodeID == "" || commitIndex == 0 {
		return Ownership{}, ErrInvalidOwnership
	}

	if current == nil {
		if expected != 0 {
			return Ownership{}, ErrStaleEpoch
		}
		return Ownership{
			sessionKey:  key,
			ownerNodeID: ownerNodeID,
			epoch:       1,
			commitIndex: commitIndex,
		}, nil
	}

	if current.sessionKey.TenantID().String() == "" || current.sessionKey.ClientID() == "" || current.epoch == 0 || current.commitIndex == 0 {
		return Ownership{}, ErrInvalidOwnership
	}
	if current.sessionKey != key {
		return Ownership{}, ErrInvalidOwnership
	}
	if expected != current.epoch {
		return Ownership{}, ErrStaleEpoch
	}
	if commitIndex <= current.commitIndex {
		return Ownership{}, ErrStaleCommitIndex
	}
	if current.epoch == math.MaxUint64 {
		return Ownership{}, ErrEpochOverflow
	}

	return Ownership{
		sessionKey:  key,
		ownerNodeID: ownerNodeID,
		epoch:       current.epoch + 1,
		commitIndex: commitIndex,
	}, nil
}
