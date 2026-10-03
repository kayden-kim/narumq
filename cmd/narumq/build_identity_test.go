package main

import (
	"slices"
	"strings"
	"testing"
)

func TestResolveBuildIdentity(t *testing.T) {
	originalRelease, originalFeatures := buildReleaseVersion, buildFeatures
	t.Cleanup(func() {
		buildReleaseVersion, buildFeatures = originalRelease, originalFeatures
	})

	tests := []struct {
		name         string
		release      string
		features     string
		wantRelease  string
		wantFeatures []string
		wantError    bool
	}{
		{name: "defaults", release: "0.0.0", wantRelease: "0.0.0", wantFeatures: []string{"session-metadata"}},
		{
			name: "sorts features", release: "1.2.3", features: "zeta,compatibility-probe,alpha",
			wantRelease: "1.2.3", wantFeatures: []string{"alpha", "compatibility-probe", "session-metadata", "zeta"},
		},
		{name: "invalid release", release: "01.2.3", wantError: true},
		{name: "empty feature token", release: "1.2.3", features: "alpha,,zeta", wantError: true},
		{name: "duplicate feature", release: "1.2.3", features: "alpha,alpha,session-metadata", wantRelease: "1.2.3", wantFeatures: []string{"alpha", "session-metadata"}},
		{name: "invalid feature", release: "1.2.3", features: "Bad", wantError: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			buildReleaseVersion, buildFeatures = test.release, test.features
			gotRelease, gotFeatures, err := resolveBuildIdentity()
			if test.wantError {
				if err == nil || !strings.HasPrefix(err.Error(), "invalid build identity: ") {
					t.Fatalf("resolveBuildIdentity() error = %v, want invalid build identity error", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveBuildIdentity() error = %v", err)
			}
			if gotRelease != test.wantRelease || !slices.Equal(gotFeatures, test.wantFeatures) {
				t.Fatalf("resolveBuildIdentity() = (%q, %q), want (%q, %q)", gotRelease, gotFeatures, test.wantRelease, test.wantFeatures)
			}
		})
	}
}
