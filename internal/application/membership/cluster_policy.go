package membership

import "slices"

const maxClusterPolicyPacketSize uint32 = 268435455

// ValidateClusterPolicy validates the MQTT limits implemented by this policy version.
func ValidateClusterPolicy(policy ClusterPolicyDTO) error {
	limits := policy.MQTTLimits
	if limits.MaxPacketSize == 0 || limits.MaxPacketSize > maxClusterPolicyPacketSize ||
		limits.ReceiveMaximum == 0 || limits.ReceiveMaximum > 65535 ||
		limits.ConnectTimeoutMillis == 0 || limits.PacketReadTimeoutMillis == 0 || limits.WriteTimeoutMillis == 0 {
		return ErrInvalidCommand
	}
	return nil
}

func validateClusterPolicyChange(change ClusterPolicyChangeDTO) error {
	if change.Version == 0 || change.Version <= change.ExpectedVersion ||
		ValidateClusterPolicy(change.Policy) != nil ||
		(change.IdempotencyKey != "" && !validIdempotencyKey(change.IdempotencyKey)) {
		return ErrInvalidCommand
	}
	return nil
}

func validateClusterPolicyState(state ClusterPolicyStateDTO) error {
	if state.Version == 0 || state.PreviousVersion >= state.Version ||
		ValidateClusterPolicy(state.Policy) != nil ||
		(state.IdempotencyKey != "" && !validIdempotencyKey(state.IdempotencyKey)) {
		return ErrInvalidSnapshot
	}
	return nil
}

func cloneClusterPolicyState(state *ClusterPolicyStateDTO) *ClusterPolicyStateDTO {
	if state == nil {
		return nil
	}
	cloned := *state
	return &cloned
}

func (machine *StateMachine) applyClusterPolicy(index uint64, change ClusterPolicyChangeDTO) ApplyResultDTO {
	if !slices.Contains(machine.activeFeatures, ClusterPolicyFeature) {
		return conflictResult(index, ConflictFeatureInactive)
	}

	currentVersion := uint64(0)
	if current := machine.clusterPolicy; current != nil {
		if change.IdempotencyKey != "" && change.IdempotencyKey == current.IdempotencyKey {
			if change.ExpectedVersion == current.PreviousVersion && change.Version == current.Version && change.Policy == current.Policy {
				return ApplyResultDTO{Index: index, Outcome: ApplyOutcomeApplied}
			}
			return conflictResult(index, ConflictIdempotency)
		}
		currentVersion = current.Version
	}
	if change.ExpectedVersion != currentVersion {
		return conflictResult(index, ConflictClusterPolicyVersion)
	}
	machine.clusterPolicy = &ClusterPolicyStateDTO{
		Version: change.Version, Policy: change.Policy,
		IdempotencyKey: change.IdempotencyKey, PreviousVersion: change.ExpectedVersion,
	}
	return ApplyResultDTO{Index: index, Outcome: ApplyOutcomeApplied}
}

// ClusterPolicy returns the latest committed policy as a value copy.
func (machine *StateMachine) ClusterPolicy() (ClusterPolicyStateDTO, bool) {
	machine.mu.RLock()
	defer machine.mu.RUnlock()
	if machine.clusterPolicy == nil {
		return ClusterPolicyStateDTO{}, false
	}
	return *machine.clusterPolicy, true
}
