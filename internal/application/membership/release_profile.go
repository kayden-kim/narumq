package membership

import (
	"slices"
	"sort"

	"narumq/internal/domain/release"
)

func (machine *StateMachine) applyMemberProfile(index uint64, change MemberProfileChangeDTO) ApplyResultDTO {
	result := ApplyResultDTO{Index: index, Outcome: ApplyOutcomeApplied}
	member, exists := machine.members[change.MemberID]
	if !exists || member.Phase == MemberPhaseRetired {
		return conflictResult(index, ConflictMemberNotFound)
	}
	for _, feature := range machine.activeFeatures {
		if !release.Contains(change.Features, feature) {
			return conflictResult(index, ConflictActiveFeatureMissing)
		}
	}
	for memberID, committed := range machine.members {
		if memberID != change.MemberID && committed.Phase != MemberPhaseRetired && !release.CompatibleStrings(change.ReleaseVersion, committed.ReleaseVersion) {
			return conflictResult(index, ConflictCompatibilityUnsupported)
		}
	}
	if member.ReleaseVersion == change.ReleaseVersion && slices.Equal(member.Features, change.Features) {
		return result
	}
	member.ReleaseVersion = change.ReleaseVersion
	member.Features = slices.Clone(change.Features)
	machine.members[change.MemberID] = member
	return result
}

func (machine *StateMachine) applyFeatureActivation(index uint64, activation FeatureActivationDTO) ApplyResultDTO {
	result := ApplyResultDTO{Index: index, Outcome: ApplyOutcomeApplied}
	position := sort.SearchStrings(machine.activeFeatures, activation.Feature)
	if position < len(machine.activeFeatures) && machine.activeFeatures[position] == activation.Feature {
		return result
	}
	for _, member := range machine.members {
		if member.Phase != MemberPhaseRetired && !release.Contains(member.Features, activation.Feature) {
			return conflictResult(index, ConflictFeatureUnsupported)
		}
	}
	machine.activeFeatures = append(machine.activeFeatures, "")
	copy(machine.activeFeatures[position+1:], machine.activeFeatures[position:])
	machine.activeFeatures[position] = activation.Feature
	return result
}

// ActiveFeatures returns an owned copy of the sorted committed active feature set.
func (machine *StateMachine) ActiveFeatures() []string {
	machine.mu.RLock()
	defer machine.mu.RUnlock()
	return slices.Clone(machine.activeFeatures)
}

func activeReleaseWindowValid(members []MemberIdentityDTO) bool {
	var major, minimumMinor, maximumMinor uint32
	found := false
	for _, member := range members {
		if member.Phase == MemberPhaseRetired {
			continue
		}
		version, err := release.Parse(member.ReleaseVersion)
		if err != nil {
			return false
		}
		if !found {
			major = version.Major
			minimumMinor = version.Minor
			maximumMinor = version.Minor
			found = true
			continue
		}
		if version.Major != major {
			return false
		}
		if version.Minor < minimumMinor {
			minimumMinor = version.Minor
		}
		if version.Minor > maximumMinor {
			maximumMinor = version.Minor
		}
	}
	return !found || maximumMinor-minimumMinor <= 1
}
