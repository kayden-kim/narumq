package main

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"narumq/internal/application/membership"
	"narumq/internal/domain/release"
)

var (
	buildReleaseVersion = "0.0.0"
	buildFeatures       = ""
)

func resolveBuildIdentity() (string, []string, error) {
	if _, err := release.Parse(buildReleaseVersion); err != nil {
		return "", nil, fmt.Errorf("invalid build identity: %w", err)
	}

	features := []string{
		membership.ClientBlocklistFeature, membership.ConnectionRebalanceFeature,
		membership.CredentialRevocationFeature, membership.ProposalRecoveryFeature, membership.SessionMetadataFeature,
	}
	if buildFeatures != "" {
		features = append(features, strings.Split(buildFeatures, ",")...)
	}
	slices.Sort(features)
	features = slices.Compact(features)
	if !release.ValidFeatureSet(features) {
		return "", nil, fmt.Errorf("invalid build identity: %w", errors.New("build features are invalid"))
	}
	return buildReleaseVersion, features, nil
}
