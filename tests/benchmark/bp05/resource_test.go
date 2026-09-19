package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestComputeResourceSummaryUsesPhaseWindowsAndSignedPerConnectionDelta(t *testing.T) {
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
	workload := WorkloadSummary{Valid: true, IdleSurvivors: 8}
	workloadData, err := json.Marshal(workload)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(workloadPath, workloadData, 0o644); err != nil {
		t.Fatal(err)
	}

	summary, err := ComputeResourceSummary(statsPath, phasesPath, workloadPath)
	if err != nil {
		t.Fatal(err)
	}
	if !summary.Valid || summary.BaselineMemoryBytes != 150 || summary.PlateauMemoryBytes != 400 || summary.IncrementalMemoryBytes != 250 || summary.MemoryBytesPerIdleConnection != 31.25 {
		t.Fatalf("ComputeResourceSummary() = %#v", summary)
	}
}

func TestComputeResourceSummaryRejectsInsufficientSamples(t *testing.T) {
	dir := t.TempDir()
	statsPath := filepath.Join(dir, "broker-stats.jsonl")
	phasesPath := filepath.Join(dir, "phases.jsonl")
	workloadPath := filepath.Join(dir, "workload-summary.json")
	if err := os.WriteFile(statsPath, []byte(`{"collectedAt":"2026-09-19T00:00:00Z","container":"broker","docker":{"CPUPerc":"1.00%","MemUsage":"100B / 1GiB"}}`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(phasesPath, []byte(strings.Join([]string{
		`{"phase":"ramp","event":"start","timestamp":"2026-09-19T00:00:02Z","activeConnections":0}`,
		`{"phase":"idle","event":"start","timestamp":"2026-09-19T00:00:03Z","activeConnections":1}`,
		`{"phase":"idle","event":"end","timestamp":"2026-09-19T00:00:04Z","activeConnections":1}`,
	}, "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(workloadPath, []byte(`{"valid":true,"idleSurvivors":1}`), 0o644); err != nil {
		t.Fatal(err)
	}
	summary, err := ComputeResourceSummary(statsPath, phasesPath, workloadPath)
	if err == nil || summary.Valid || !strings.Contains(err.Error(), "at least two baseline") {
		t.Fatalf("ComputeResourceSummary() summary=%#v error=%v, want baseline sample failure", summary, err)
	}
}
