package main

import (
	"fmt"

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
	features, err := membership.BuildFeatures(buildFeatures)
	if err != nil {
		return "", nil, fmt.Errorf("invalid build identity: %w", err)
	}
	return buildReleaseVersion, features, nil
}
