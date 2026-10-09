package membership

import (
	"slices"
	"sort"
)

func validateCredentialRevocationChange(change CredentialRevocationChangeDTO) error {
	if !boundedUTF8(change.CredentialID, 1, MaxCredentialIDBytes) || change.ProposedAtUnixMillis == 0 {
		return ErrInvalidCommand
	}
	switch change.Action {
	case CredentialRevocationActionRevoke, CredentialRevocationActionRestore:
		return nil
	default:
		return ErrInvalidCommand
	}
}

func validCredentialRevocationState(state CredentialRevocationStateDTO) bool {
	return boundedUTF8(state.CredentialID, 1, MaxCredentialIDBytes) && state.RevokedAtUnixMillis > 0
}

func (machine *StateMachine) applyCredentialRevocation(index uint64, change CredentialRevocationChangeDTO) ApplyResultDTO {
	if !slices.Contains(machine.activeFeatures, CredentialRevocationFeature) {
		return conflictResult(index, ConflictFeatureInactive)
	}
	result := ApplyResultDTO{Index: index, Outcome: ApplyOutcomeApplied}
	status := CredentialRevocationStatusDTO{CredentialID: change.CredentialID}
	switch change.Action {
	case CredentialRevocationActionRevoke:
		state, exists := machine.credentialRevocations[change.CredentialID]
		if !exists {
			if len(machine.credentialRevocations) >= MaxCredentialRevocations {
				return conflictResult(index, ConflictCredentialRevocationCapacity)
			}
			state = CredentialRevocationStateDTO{
				CredentialID: change.CredentialID, RevokedAtUnixMillis: change.ProposedAtUnixMillis,
			}
			machine.credentialRevocations[change.CredentialID] = state
		}
		status.Revoked = true
		status.RevokedAtUnixMillis = state.RevokedAtUnixMillis
	case CredentialRevocationActionRestore:
		delete(machine.credentialRevocations, change.CredentialID)
	}
	result.CredentialRevocation = &status
	return result
}

// CredentialRevocations returns the committed revocation list in credential-ID order.
func (machine *StateMachine) CredentialRevocations() []CredentialRevocationStateDTO {
	machine.mu.RLock()
	defer machine.mu.RUnlock()
	states := make([]CredentialRevocationStateDTO, 0, len(machine.credentialRevocations))
	for _, state := range machine.credentialRevocations {
		states = append(states, state)
	}
	sort.Slice(states, func(i, j int) bool { return states[i].CredentialID < states[j].CredentialID })
	return states
}

// GetCredentialRevocation returns one committed revocation entry, if present.
func (machine *StateMachine) GetCredentialRevocation(credentialID string) (CredentialRevocationStateDTO, bool) {
	machine.mu.RLock()
	defer machine.mu.RUnlock()
	state, ok := machine.credentialRevocations[credentialID]
	return state, ok
}

// IsCredentialRevoked reports whether credentialID has a committed revocation entry.
func (machine *StateMachine) IsCredentialRevoked(credentialID string) bool {
	machine.mu.RLock()
	defer machine.mu.RUnlock()
	_, revoked := machine.credentialRevocations[credentialID]
	return revoked
}
