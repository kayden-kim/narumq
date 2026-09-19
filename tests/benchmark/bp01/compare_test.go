package main

import "testing"

func TestBuildComparisonRequiresEquivalentSuccessfulTargets(t *testing.T) {
	profile := validProfile()
	limits := resourceLimits{
		CPUs:              profile.BrokerCPUs,
		MemoryBytes:       profile.BrokerMemoryBytes,
		ReadOnlyRootFS:    true,
		NetworkMode:       "none",
		GeneratorCPUs:     profile.GeneratorCPUs,
		GeneratorMemory:   profile.GeneratorMemoryBytes,
		RunnerReadOnlyFS:  true,
		RunnerNetworkMode: "container:<broker>",
	}
	validSummary := &Summary{Valid: true}
	value := manifest{
		RunID:   "run-1",
		Profile: artifactProfile{RawSHA256: "sha", Resolved: profile},
		Targets: map[string]targetManifest{
			"narumq": {Success: true, ProfileSHA256: "sha", ResolvedProfile: profile, RunnerImageID: "runner", NetworkMode: "none", ResourceLimits: limits, Summary: validSummary},
			"emqx":   {Success: true, ProfileSHA256: "sha", ResolvedProfile: profile, RunnerImageID: "runner", NetworkMode: "none", ResourceLimits: limits, Summary: validSummary},
		},
	}
	comparison := buildComparison(value)
	if !comparison.Equivalent || comparison.ReleaseGating {
		t.Fatalf("buildComparison() = %#v, want equivalent non-gating result", comparison)
	}

	mismatchedProfile := profile
	mismatchedProfile.Topic = "bp01/other"
	value.Targets["emqx"] = targetManifest{Success: true, ProfileSHA256: "sha", ResolvedProfile: mismatchedProfile, RunnerImageID: "runner", NetworkMode: "none", ResourceLimits: limits, Summary: validSummary}
	comparison = buildComparison(value)
	if comparison.Equivalent || comparison.Reason != "resolved profile digest mismatch" {
		t.Fatalf("mismatched profile comparison = %#v, want profile mismatch", comparison)
	}

	value.Targets["emqx"] = targetManifest{Success: false, ProfileSHA256: "sha", RunnerImageID: "runner", NetworkMode: "none", ResourceLimits: limits}
	comparison = buildComparison(value)
	if comparison.Equivalent || comparison.Reason != "both target runs must succeed" {
		t.Fatalf("failed target comparison = %#v", comparison)
	}
}

func TestRunCLIRejectsUnknownSubcommand(t *testing.T) {
	if err := runCLI([]string{"unknown"}, discardWriter{}, discardWriter{}); err == nil {
		t.Fatal("runCLI() accepted unknown subcommand")
	}
}

type discardWriter struct{}

func (discardWriter) Write(value []byte) (int, error) {
	return len(value), nil
}
