package main

import (
	"testing"

	"narumq/tests/benchmark/internal/runtimeartifact"
)

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
	validNaruMQRuntime := &runtimeartifact.Summary{Valid: true, Applicability: "required"}
	validEMQXRuntime := runtimeartifact.NotApplicable("emqx", notApplicableRuntimeReason)
	validSummary := &Summary{Valid: true}

	value := manifest{
		RunID:   "run-1",
		Profile: artifactProfile{RawSHA256: "sha", Resolved: profile},
		Targets: map[string]targetManifest{
			"narumq": {Success: true, ProfileSHA256: "sha", ResolvedProfile: profile, RunnerImageID: "runner", NetworkMode: "none", ResourceLimits: limits, Summary: validSummary, RuntimeSummary: validNaruMQRuntime},
			"emqx":   {Success: true, ProfileSHA256: "sha", ResolvedProfile: profile, RunnerImageID: "runner", NetworkMode: "none", ResourceLimits: limits, Summary: validSummary, RuntimeSummary: &validEMQXRuntime},
		},
	}
	comparison := buildComparison(value)
	if !comparison.Equivalent || comparison.ReleaseGating {
		t.Fatalf("buildComparison() = %#v, want equivalent non-gating result", comparison)
	}
	narumqTarget := value.Targets["narumq"]
	narumqTarget.RuntimeSummary = nil
	value.Targets["narumq"] = narumqTarget
	comparison = buildComparison(value)
	if comparison.Equivalent || comparison.Reason != "both workload summaries and target runtime applicability must be valid" {
		t.Fatalf("missing NaruMQ runtime comparison = %#v", comparison)
	}
	narumqTarget.RuntimeSummary = &runtimeartifact.Summary{Valid: true, Applicability: "not_applicable"}
	value.Targets["narumq"] = narumqTarget
	comparison = buildComparison(value)
	if comparison.Equivalent {
		t.Fatalf("NaruMQ not-applicable runtime comparison = %#v", comparison)
	}
	narumqTarget.RuntimeSummary = validNaruMQRuntime
	value.Targets["narumq"] = narumqTarget
	emqxTarget := value.Targets["emqx"]
	emqxTarget.RuntimeSummary = &runtimeartifact.Summary{Valid: true, Applicability: "required"}
	value.Targets["emqx"] = emqxTarget
	comparison = buildComparison(value)
	if comparison.Equivalent {
		t.Fatalf("EMQX required runtime comparison = %#v", comparison)
	}
	emqxTarget.RuntimeSummary = &validEMQXRuntime
	value.Targets["emqx"] = emqxTarget

	mismatchedProfile := profile
	mismatchedProfile.Topic = "bp01/other"
	value.Targets["emqx"] = targetManifest{Success: true, ProfileSHA256: "sha", ResolvedProfile: mismatchedProfile, RunnerImageID: "runner", NetworkMode: "none", ResourceLimits: limits, Summary: validSummary, RuntimeSummary: &validEMQXRuntime}
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
