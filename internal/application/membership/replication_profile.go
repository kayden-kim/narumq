package membership

import "errors"

// ReplicationProfileDTO is the configured voter count and durable write quorum (ADR-0029).
type ReplicationProfileDTO struct {
	Factor      int
	WriteQuorum int
}

// StandardReplicationProfile is the only supported profile.
var StandardReplicationProfile = ReplicationProfileDTO{Factor: 3, WriteQuorum: 2}

var ErrUnsupportedReplicationProfile = errors.New("replication profile must be factor 3 and writeQuorum 2")

// ValidateReplicationProfile rejects profiles outside the supported 3/2 profile.
func ValidateReplicationProfile(profile ReplicationProfileDTO) error {
	if profile != StandardReplicationProfile {
		return ErrUnsupportedReplicationProfile
	}
	return nil
}

// RemovalKeepsReplicationFactor reports whether voters other than leavingMemberID still reach profile.Factor.
// Voter IDs must be unique.
func RemovalKeepsReplicationFactor(leavingMemberID uint64, voterIDs []uint64, profile ReplicationProfileDTO) bool {
	remaining := 0
	for _, id := range voterIDs {
		if id != leavingMemberID {
			remaining++
		}
	}
	return remaining >= profile.Factor
}
