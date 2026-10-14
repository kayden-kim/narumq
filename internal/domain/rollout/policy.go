// Package rollout defines pure policy for ordered StatefulSet rollouts.
package rollout

import (
	"errors"
	"fmt"

	"narumq/internal/domain/release"
)

const NodeCount = 3

var (
	ErrIncompleteMembership = errors.New("rollout requires exactly three committed members")
	ErrInvalidProfile       = errors.New("rollout release profile is invalid")
	ErrIncompatibleRelease  = errors.New("rollout release is outside the supported compatibility window")
	ErrUnsupportedFeature   = errors.New("rollout profile does not support an active feature")
)

type ReleaseProfile struct {
	NodeID   uint32
	Version  string
	Features []string
}

// ValidateCompatibility rejects incomplete or unknown cluster evidence and unsupported targets.
func ValidateCompatibility(target ReleaseProfile, members []ReleaseProfile, activeFeatures []string) error {
	targetVersion, err := release.Parse(target.Version)
	if err != nil || !release.ValidFeatureSet(target.Features) {
		return fmt.Errorf("%w: target", ErrInvalidProfile)
	}
	if len(members) != NodeCount || !release.ValidFeatureSet(activeFeatures) {
		return ErrIncompleteMembership
	}
	seen := [NodeCount]bool{}
	for _, member := range members {
		if member.NodeID < 1 || member.NodeID > NodeCount || seen[member.NodeID-1] {
			return ErrIncompleteMembership
		}
		seen[member.NodeID-1] = true
		version, err := release.Parse(member.Version)
		if err != nil || !release.ValidFeatureSet(member.Features) {
			return fmt.Errorf("%w: node %d", ErrInvalidProfile, member.NodeID)
		}
		if !release.Compatible(targetVersion, version) {
			return fmt.Errorf("%w: node %d", ErrIncompatibleRelease, member.NodeID)
		}
		for _, feature := range activeFeatures {
			if !release.Contains(member.Features, feature) || !release.Contains(target.Features, feature) {
				return fmt.Errorf("%w: node %d requires %s", ErrUnsupportedFeature, member.NodeID, feature)
			}
		}
	}
	for _, exists := range seen {
		if !exists {
			return ErrIncompleteMembership
		}
	}
	return nil
}

// SafeToStop reports success only for a complete, explicitly safe, failure-free observation.
func SafeToStop(phase, state string, safeToStop *bool, failurePresent bool) bool {
	return phase == "COMPLETED" && state == "SAFE_TO_STOP" && safeToStop != nil && *safeToStop && !failurePresent
}
