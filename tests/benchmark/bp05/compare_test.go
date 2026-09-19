package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"narumq/tests/benchmark/internal/environment"
)

func TestBuildComparisonRequiresEquivalentValidTargets(t *testing.T) {
	profile := validBP05Profile()
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
	workload := &WorkloadSummary{Valid: true}
	resource := &ResourceSummary{Valid: true}
	value := manifest{
		RunID:   "bp05-test-run",
		Profile: artifactProfile{RawSHA256: "sha", Resolved: profile},
		Targets: map[string]targetManifest{
			"narumq": {Success: true, ProfileSHA256: "sha", ResolvedProfile: profile, RunnerImageID: "runner", NetworkMode: "none", ResourceLimits: limits, WorkloadSummary: workload, ResourceSummary: resource},
			"emqx":   {Success: true, ProfileSHA256: "sha", ResolvedProfile: profile, RunnerImageID: "runner", NetworkMode: "none", ResourceLimits: limits, WorkloadSummary: workload, ResourceSummary: resource},
		},
	}
	comparison := buildComparison(value)
	if !comparison.Equivalent || comparison.ReleaseGating || comparison.ProfileSHA256 != "sha" {
		t.Fatalf("buildComparison() = %#v", comparison)
	}

	value.Targets["emqx"] = targetManifest{Success: true, ProfileSHA256: "sha", ResolvedProfile: profile, RunnerImageID: "runner", NetworkMode: "none", ResourceLimits: limits, WorkloadSummary: workload, ResourceSummary: &ResourceSummary{Valid: false}}
	comparison = buildComparison(value)
	if comparison.Equivalent || comparison.Reason != "both workload and resource summaries must be valid" {
		t.Fatalf("invalid resource comparison = %#v", comparison)
	}
	value.Targets["emqx"] = targetManifest{Success: false, ProfileSHA256: "sha", RunnerImageID: "runner", NetworkMode: "none", ResourceLimits: limits}
	comparison = buildComparison(value)
	if comparison.Equivalent || comparison.Reason != "both target runs must succeed" {
		t.Fatalf("failed target comparison = %#v", comparison)
	}
}
func TestFinishTargetWritesResourceSummaryArtifact(t *testing.T) {
	dir := t.TempDir()
	statsPath := filepath.Join(dir, "broker-stats.jsonl")
	phasesPath := filepath.Join(dir, "phases.jsonl")
	workloadPath := filepath.Join(dir, "workload-summary.json")
	stats := strings.Join([]string{
		`{"collectedAt":"2026-09-19T00:00:00Z","container":"broker","docker":{"CPUPerc":"1.00%","MemUsage":"100B / 1GiB"}}`,
		`{"collectedAt":"2026-09-19T00:00:01Z","container":"broker","docker":{"CPUPerc":"1.00%","MemUsage":"200B / 1GiB"}}`,
		`{"collectedAt":"2026-09-19T00:00:03Z","container":"broker","docker":{"CPUPerc":"1.00%","MemUsage":"300B / 1GiB"}}`,
		`{"collectedAt":"2026-09-19T00:00:04Z","container":"broker","docker":{"CPUPerc":"1.00%","MemUsage":"500B / 1GiB"}}`,
	}, "\n") + "\n"
	if err := os.WriteFile(statsPath, []byte(stats), 0o644); err != nil {
		t.Fatal(err)
	}
	phases := strings.Join([]string{
		`{"phase":"ramp","event":"start","timestamp":"2026-09-19T00:00:02Z","activeConnections":0}`,
		`{"phase":"idle","event":"start","timestamp":"2026-09-19T00:00:03Z","activeConnections":8}`,
		`{"phase":"idle","event":"end","timestamp":"2026-09-19T00:00:05Z","activeConnections":8}`,
	}, "\n") + "\n"
	if err := os.WriteFile(phasesPath, []byte(phases), 0o644); err != nil {
		t.Fatal(err)
	}
	workloadData, err := json.Marshal(WorkloadSummary{Valid: true, IdleSurvivors: 8})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(workloadPath, workloadData, 0o644); err != nil {
		t.Fatal(err)
	}

	profile := validBP05Profile()
	target := finishTarget(environment.TargetResult{
		Target:          "narumq",
		Status:          "success",
		Success:         true,
		BrokerStatsPath: statsPath,
	}, profile, "sha", dir, "image", nil)
	if !target.Success || target.ResourceSummary == nil || !target.ResourceSummary.Valid {
		t.Fatalf("finishTarget() = %#v", target)
	}

	resourceData, err := os.ReadFile(filepath.Join(dir, "resource-summary.json"))
	if err != nil {
		t.Fatal(err)
	}
	var resource ResourceSummary
	if err := json.Unmarshal(resourceData, &resource); err != nil {
		t.Fatal(err)
	}
	if !resource.Valid || resource.IncrementalMemoryBytes != 250 {
		t.Fatalf("resource-summary.json = %#v", resource)
	}
}

func TestBP05RunCLIRejectsUnknownSubcommand(t *testing.T) {
	if err := runCLI([]string{"unknown"}, discardBP05Writer{}, discardBP05Writer{}); err == nil {
		t.Fatal("runCLI() accepted unknown subcommand")
	}
}

type discardBP05Writer struct{}

func (discardBP05Writer) Write(value []byte) (int, error) { return len(value), nil }
