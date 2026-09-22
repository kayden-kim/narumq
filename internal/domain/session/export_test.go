package session

import "narumq/internal/domain/tenancy"

// NewOwnershipForTest constructs an Ownership record directly for testing boundaries.
func NewOwnershipForTest(key tenancy.SessionKey, ownerNodeID string, epoch Epoch, commitIndex uint64) Ownership {
	return Ownership{
		sessionKey:  key,
		ownerNodeID: ownerNodeID,
		epoch:       epoch,
		commitIndex: commitIndex,
	}
}
