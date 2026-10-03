package membership

import (
	"bytes"
	"slices"
)

func validateSessionConnect(connect SessionConnectDTO) error {
	if connect.ProtocolVersion != 4 && connect.ProtocolVersion != 5 {
		return ErrInvalidCommand
	}
	switch connect.ProtocolVersion {
	case 4:
		if (connect.Retention != SessionRetentionImmediate && connect.Retention != SessionRetentionInfinite) || connect.ExpiryIntervalSeconds != 0 {
			return ErrInvalidCommand
		}
	case 5:
		switch connect.Retention {
		case SessionRetentionImmediate:
			if connect.ExpiryIntervalSeconds != 0 {
				return ErrInvalidCommand
			}
		case SessionRetentionFinite:
			if connect.ExpiryIntervalSeconds == 0 || connect.ExpiryIntervalSeconds == ^uint32(0) {
				return ErrInvalidCommand
			}
		case SessionRetentionInfinite:
			if connect.ExpiryIntervalSeconds != ^uint32(0) {
				return ErrInvalidCommand
			}
		default:
			return ErrInvalidCommand
		}
	}
	if connect.Will != nil {
		if !validTopic(connect.Will.Topic) || len(connect.Will.Payload) > MaxSessionWillPayloadBytes || connect.Will.QoS > 2 || (connect.ProtocolVersion == 4 && connect.Will.DelayIntervalSeconds != 0) {
			return ErrInvalidCommand
		}
	}
	if connect.Principal != nil {
		if connect.Will == nil || !boundedUTF8(connect.Principal.Subject, 1, MaxSessionPrincipalFieldBytes) || !boundedUTF8(connect.Principal.CredentialID, 1, MaxSessionPrincipalFieldBytes) || connect.Principal.ExpiresAtUnixMillis == 0 || len(connect.Principal.Roles) > MaxSessionPrincipalRoles {
			return ErrInvalidCommand
		}
		seen := make(map[string]struct{}, len(connect.Principal.Roles))
		for _, role := range connect.Principal.Roles {
			if !boundedUTF8(role, 1, MaxSessionPrincipalRoleBytes) {
				return ErrInvalidCommand
			}
			if _, exists := seen[role]; exists {
				return ErrInvalidCommand
			}
			seen[role] = struct{}{}
		}
	}
	return nil
}

func validateSessionMetadataChange(change SessionMetadataChangeDTO) error {
	if !validSessionIdentity(change.TenantID, change.ClientID) || change.OwnerNodeID == 0 || change.Epoch == 0 || change.ProposedAtUnixMillis == 0 {
		return ErrInvalidCommand
	}
	if change.SessionExpiresAtUnixMillis != 0 && change.SessionExpiresAtUnixMillis <= change.ProposedAtUnixMillis {
		return ErrInvalidCommand
	}
	switch change.Action {
	case SessionMetadataActionDetach:
		switch change.WillDisposition {
		case SessionWillDispositionSchedule:
			if change.WillDueAtUnixMillis == 0 || change.WillDueAtUnixMillis < change.ProposedAtUnixMillis {
				return ErrInvalidCommand
			}
		case SessionWillDispositionDiscard, SessionWillDispositionClaim:
			if change.WillDueAtUnixMillis != 0 {
				return ErrInvalidCommand
			}
		default:
			return ErrInvalidCommand
		}
	case SessionMetadataActionClaimWill:
		if change.WillDisposition != SessionWillDispositionUnspecified || change.SessionExpiresAtUnixMillis != 0 || change.WillDueAtUnixMillis != 0 {
			return ErrInvalidCommand
		}
	case SessionMetadataActionDiscard:
		if (change.WillDisposition != SessionWillDispositionDiscard && change.WillDisposition != SessionWillDispositionClaim) || change.SessionExpiresAtUnixMillis != 0 || change.WillDueAtUnixMillis != 0 {
			return ErrInvalidCommand
		}
	default:
		return ErrInvalidCommand
	}
	return nil
}

func validateSessionMetadataState(state SessionMetadataStateDTO) error {
	if !validSessionIdentity(state.TenantID, state.ClientID) || state.Epoch == 0 || validateSessionConnect(SessionConnectDTO{
		ProtocolVersion: state.ProtocolVersion, Retention: state.Retention, ExpiryIntervalSeconds: state.ExpiryIntervalSeconds,
		Will: state.Will, Principal: state.Principal,
	}) != nil {
		return ErrInvalidSnapshot
	}
	if state.Connected {
		if state.DetachedAtUnixMillis != 0 || state.SessionExpiresAtUnixMillis != 0 || state.WillDueAtUnixMillis != 0 {
			return ErrInvalidSnapshot
		}
		return nil
	}
	if state.DetachedAtUnixMillis == 0 || state.Retention == SessionRetentionImmediate {
		return ErrInvalidSnapshot
	}
	switch state.Retention {
	case SessionRetentionFinite:
		if state.SessionExpiresAtUnixMillis <= state.DetachedAtUnixMillis {
			return ErrInvalidSnapshot
		}
	case SessionRetentionInfinite:
		if state.SessionExpiresAtUnixMillis != 0 {
			return ErrInvalidSnapshot
		}
	default:
		return ErrInvalidSnapshot
	}
	if (state.Will != nil) != (state.WillDueAtUnixMillis > 0) {
		return ErrInvalidSnapshot
	}
	return nil
}

func sessionMetadataFromConnect(key sessionKey, epoch uint64, connect SessionConnectDTO) SessionMetadataStateDTO {
	return SessionMetadataStateDTO{
		TenantID: key.tenantID, ClientID: key.clientID, Epoch: epoch,
		ProtocolVersion: connect.ProtocolVersion, Retention: connect.Retention,
		ExpiryIntervalSeconds: connect.ExpiryIntervalSeconds, Connected: true,
		Will: cloneSessionWillDTO(connect.Will), Principal: cloneSessionPrincipalDTO(connect.Principal),
	}
}

func (machine *StateMachine) applySessionMetadata(index uint64, change SessionMetadataChangeDTO) ApplyResultDTO {
	if !slices.Contains(machine.activeFeatures, SessionMetadataFeature) {
		return conflictResult(index, ConflictFeatureInactive)
	}
	key := sessionKey{tenantID: change.TenantID, clientID: change.ClientID}
	if !machine.authorized(key, change.OwnerNodeID, change.Epoch) {
		return conflictResult(index, ConflictStaleEpoch)
	}
	result := ApplyResultDTO{Index: index, Outcome: ApplyOutcomeApplied, SessionMetadata: &SessionMetadataResultDTO{}}
	state, exists := machine.sessionMetadata[key]
	if !exists {
		return result
	}
	result.SessionMetadata.Found = true
	oldBytes := sessionWillBytes(state)
	switch change.Action {
	case SessionMetadataActionDetach:
		if state.Retention == SessionRetentionImmediate || (state.Retention == SessionRetentionFinite && change.SessionExpiresAtUnixMillis == 0) || (state.Retention == SessionRetentionInfinite && change.SessionExpiresAtUnixMillis != 0) {
			return conflictResult(index, ConflictSessionMetadataInvalid)
		}
		state.Epoch = change.Epoch
		state.Connected = false
		state.DetachedAtUnixMillis = change.ProposedAtUnixMillis
		state.SessionExpiresAtUnixMillis = change.SessionExpiresAtUnixMillis
		switch change.WillDisposition {
		case SessionWillDispositionDiscard:
			state.Will = nil
			state.Principal = nil
			state.WillDueAtUnixMillis = 0
		case SessionWillDispositionClaim:
			result.SessionMetadata.Will = cloneSessionWillDTO(state.Will)
			state.Will = nil
			state.Principal = nil
			state.WillDueAtUnixMillis = 0
		case SessionWillDispositionSchedule:
			if state.Will != nil {
				state.WillDueAtUnixMillis = change.WillDueAtUnixMillis
			} else {
				state.WillDueAtUnixMillis = 0
				state.Principal = nil
			}
		}
	case SessionMetadataActionClaimWill:
		result.SessionMetadata.Will = cloneSessionWillDTO(state.Will)
		state.Will = nil
		state.Principal = nil
		state.WillDueAtUnixMillis = 0
	case SessionMetadataActionDiscard:
		if change.WillDisposition == SessionWillDispositionClaim {
			result.SessionMetadata.Will = cloneSessionWillDTO(state.Will)
		}
		delete(machine.sessionMetadata, key)
		machine.sessionWillBytes -= oldBytes
		return result
	}
	machine.sessionWillBytes += sessionWillBytes(state) - oldBytes
	machine.sessionMetadata[key] = cloneSessionMetadataStateDTO(state)
	return result
}

func (machine *StateMachine) removeSessionMetadataLocked(key sessionKey) {
	if state, exists := machine.sessionMetadata[key]; exists {
		machine.sessionWillBytes -= sessionWillBytes(state)
		delete(machine.sessionMetadata, key)
	}
}

func (machine *StateMachine) SessionMetadata() []SessionMetadataStateDTO {
	machine.mu.RLock()
	defer machine.mu.RUnlock()
	states := make([]SessionMetadataStateDTO, 0, len(machine.sessionMetadata))
	for _, state := range machine.sessionMetadata {
		states = append(states, cloneSessionMetadataStateDTO(state))
	}
	slices.SortFunc(states, func(left, right SessionMetadataStateDTO) int {
		return compareSession(left.TenantID, left.ClientID, right.TenantID, right.ClientID)
	})
	return states
}

func (machine *StateMachine) GetSessionMetadata(tenantID, clientID string) (SessionMetadataStateDTO, bool) {
	machine.mu.RLock()
	defer machine.mu.RUnlock()
	state, ok := machine.sessionMetadata[sessionKey{tenantID: tenantID, clientID: clientID}]
	return cloneSessionMetadataStateDTO(state), ok
}

func sessionWillBytes(state SessionMetadataStateDTO) int {
	if state.Will == nil {
		return 0
	}
	total := len(state.Will.Topic) + len(state.Will.Payload)
	if state.Principal != nil {
		total += len(state.Principal.Subject) + len(state.Principal.CredentialID)
		for _, role := range state.Principal.Roles {
			total += len(role)
		}
	}
	return total
}

func cloneSessionWillDTO(will *SessionWillDTO) *SessionWillDTO {
	if will == nil {
		return nil
	}
	cloned := *will
	cloned.Payload = bytes.Clone(will.Payload)
	return &cloned
}

func cloneSessionPrincipalDTO(principal *SessionPrincipalDTO) *SessionPrincipalDTO {
	if principal == nil {
		return nil
	}
	cloned := *principal
	cloned.Roles = slices.Clone(principal.Roles)
	return &cloned
}

func cloneSessionMetadataStateDTO(state SessionMetadataStateDTO) SessionMetadataStateDTO {
	state.Will = cloneSessionWillDTO(state.Will)
	state.Principal = cloneSessionPrincipalDTO(state.Principal)
	return state
}
