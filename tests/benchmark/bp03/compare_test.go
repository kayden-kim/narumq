package main

import (
	"path/filepath"
	"testing"
	"time"
)

func loadSmokeProfileForCompare(t *testing.T) Profile {
	t.Helper()
	profile, _, _, err := LoadProfile(filepath.Join("testdata", "smoke.json"))
	if err != nil {
		t.Fatal(err)
	}
	return profile
}

func TestBP03TargetTimeoutIncludesOfflineMilliseconds(t *testing.T) {
	profile := loadSmokeProfileForCompare(t)
	timeout, err := bp03TargetTimeout(profile)
	if err != nil {
		t.Fatal(err)
	}
	const want = 344400 * time.Millisecond
	if timeout != want {
		t.Fatalf("target timeout = %s, want %s", timeout, want)
	}
}

// TestBuildBP03ComparisonEquivalenceAndNonEquivalentDimensions verifies equivalence evidence and nonEquivalent dimensions.
// Scenarios: performance-and-verification.md:PRF-SCN-007
func TestBuildBP03ComparisonEquivalenceAndNonEquivalentDimensions(t *testing.T) {
	profile := loadSmokeProfileForCompare(t)
	limits := bp03ResourceLimits{
		BrokerCPUs:       profile.BrokerCPUs,
		BrokerMemory:     profile.BrokerMemoryBytes,
		GeneratorCPUs:    profile.GeneratorCPUs,
		GeneratorMemory:  profile.GeneratorMemoryBytes,
		BrokerReadOnlyFS: true,
		RunnerReadOnlyFS: true,
		NetworkMode:      "bridge",
	}
	manifest := bp03Manifest{
		RunID:         "bp03-test",
		Profile:       bp03ArtifactProfile{RawSHA256: "profile-hash", Resolved: profile},
		RunnerImageID: "sha256:runner",
		Topology:      bp03Topology{Nodes: 3},
		Targets: map[string]bp03TargetManifest{
			"narumq": {ProfileSHA256: "profile-hash", ResourceLimits: limits, RunnerImageID: "sha256:runner", Success: true},
			"emqx":   {ProfileSHA256: "profile-hash", ResourceLimits: limits, RunnerImageID: "sha256:runner", Success: true},
		},
	}
	summary := bp03Summary{Targets: map[string]bp03TargetSummary{
		"narumq": {Valid: true},
		"emqx":   {Valid: true},
	}}
	comparison := buildBP03Comparison(manifest, summary)
	if !comparison.Equivalent || comparison.ReleaseGating || comparison.AckLatencyDirectlyComparable {
		t.Fatalf("comparison equivalence/gating = equivalent:%t releaseGating:%t ackLatencyDirectlyComparable:%t", comparison.Equivalent, comparison.ReleaseGating, comparison.AckLatencyDirectlyComparable)
	}
	dimensions := make(map[string]bool)
	for _, difference := range comparison.NonEquivalent {
		dimensions[difference.Dimension] = true
	}
	if !dimensions["durability"] || !dimensions["authentication"] || dimensions["replication"] {
		t.Fatalf("comparison concealed remaining contracts or claimed memory-only replication: %+v", comparison.NonEquivalent)
	}

	cases := []struct {
		name   string
		mutate func(*bp03Manifest)
	}{
		{name: "profile hash", mutate: func(value *bp03Manifest) {
			value.Targets["emqx"] = cloneTargetManifest(value.Targets["emqx"], func(target *bp03TargetManifest) { target.ProfileSHA256 = "different" })
		}},
		{name: "resource limits", mutate: func(value *bp03Manifest) {
			value.Targets["emqx"] = cloneTargetManifest(value.Targets["emqx"], func(target *bp03TargetManifest) { target.ResourceLimits.GeneratorCPUs++ })
		}},
		{name: "runner image", mutate: func(value *bp03Manifest) {
			value.Targets["emqx"] = cloneTargetManifest(value.Targets["emqx"], func(target *bp03TargetManifest) { target.RunnerImageID = "sha256:different" })
		}},
		{name: "topology", mutate: func(value *bp03Manifest) { value.Topology.Nodes = 2 }},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			changed := cloneBP03Manifest(manifest)
			test.mutate(&changed)
			if buildBP03Comparison(changed, summary).Equivalent {
				t.Fatal("comparison remained equivalent after a required equivalence input changed")
			}
		})
	}
}

// TestSummarizeBP03RunDoesNotRequireEMQXWALApplicability verifies the run summary does not require EMQX WAL applicability.
// Scenarios: performance-and-verification.md:PRF-SCN-007
func TestSummarizeBP03RunDoesNotRequireEMQXWALApplicability(t *testing.T) {
	validWorkload := &workloadSummary{Valid: true}
	validResources := &resourceSummary{Valid: true}
	manifest := bp03Manifest{
		RunID: "bp03-summary-test",
		Targets: map[string]bp03TargetManifest{
			"narumq": {
				Valid:           true,
				WorkloadSummary: validWorkload,
				ResourceSummary: validResources,
				WALSummary:      &WALSummary{Valid: true},
			},
			"emqx": {
				Valid:           true,
				WorkloadSummary: validWorkload,
				ResourceSummary: validResources,
				WALSummary:      &WALSummary{Applicability: "not_applicable", Valid: false},
			},
		},
	}
	summary := summarizeBP03Run(manifest)
	if !summary.Valid || summary.ReleaseGating {
		t.Fatalf("summary valid/releaseGating = %t/%t, want true/false", summary.Valid, summary.ReleaseGating)
	}
}

func cloneTargetManifest(value bp03TargetManifest, mutate func(*bp03TargetManifest)) bp03TargetManifest {
	mutate(&value)
	return value
}

func cloneBP03Manifest(value bp03Manifest) bp03Manifest {
	cloned := value
	cloned.Targets = make(map[string]bp03TargetManifest, len(value.Targets))
	for key, target := range value.Targets {
		cloned.Targets[key] = target
	}
	return cloned
}
