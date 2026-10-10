package main

import (
	"slices"
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
		{
			name: "sorts features", release: "1.2.3", features: "zeta,compatibility-probe,alpha",
			wantRelease: "1.2.3", wantFeatures: []string{"alpha", "compatibility-probe", "zeta"},
		},
		{
			name: "advertises credential revocation and client blocklist capabilities by default", release: "1.2.3", wantRelease: "1.2.3",
			wantFeatures: []string{"client-blocklist", "credential-revocation"},
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
				if err == nil {
					t.Fatal("invalid build identity was accepted")
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveBuildIdentity() error = %v", err)
			}
			if gotRelease != test.wantRelease || !slices.IsSorted(gotFeatures) {
				t.Fatalf("non-canonical build identity: (%q, %q)", gotRelease, gotFeatures)
			}
			for i, feature := range gotFeatures {
				if i != 0 && feature == gotFeatures[i-1] {
					t.Fatalf("duplicate feature %q", feature)
				}
			}
			for _, feature := range test.wantFeatures {
				if !slices.Contains(gotFeatures, feature) {
					t.Fatalf("advertised feature %q was lost", feature)
				}
			}
		})
	}
}
