package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"narumq/tests/benchmark/internal/environment"
)

// TestBuildResourceSummaryUsesSteadyAndFailureWindows verifies steady and round windows with counter-reset-aware values.
// Scenarios: performance-and-verification.md:PRF-SCN-033
func TestBuildResourceSummaryUsesSteadyAndFailureWindows(t *testing.T) {
	root, nodes := resourceFixture(t, true)
	summary, err := buildResourceSummary("narumq", nodes, filepath.Join(root, "failure.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if !summary.Valid {
		t.Fatalf("invalid summary: %v", summary.FailureReasons)
	}
	if len(summary.Steady.Nodes) != 3 || len(summary.Rounds) != 1 {
		t.Fatalf("summary windows=%+v", summary)
	}
	round := summary.Rounds[0]
	if round.CPUPeakPercent != 80 || round.CPUPeakDeltaPercent != 65 || round.MemoryPeakBytes != 700 || round.MemoryPeakDeltaBytes != 500 {
		t.Fatalf("peak values/baselines=%+v", round)
	}
	if round.Nodes[2].BlockWriteBytes != 15 {
		t.Fatalf("reset-aware block writes=%d", round.Nodes[2].BlockWriteBytes)
	}
	if !summary.Network.NetworkStatsAvailable || summary.Network.RxBytes == 0 {
		t.Fatalf("network summary=%+v", summary.Network)
	}
}

// TestBuildResourceSummaryRejectsMissingRestartedNodeSample verifies a missing restarted-node sample invalidates resources.
// Scenarios: performance-and-verification.md:PRF-SCN-033
func TestBuildResourceSummaryRejectsMissingRestartedNodeSample(t *testing.T) {
	root, nodes := resourceFixture(t, false)
	summary, err := buildResourceSummary("emqx", nodes, filepath.Join(root, "failure.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if summary.Valid {
		t.Fatal("missing target post-restart sample accepted")
	}
	if len(summary.FailureReasons) == 0 {
		t.Fatal("invalid resource summary lacks reason")
	}
}

// TestBuildResourceSummaryRequiresTwoSamplesPerSurvivor verifies the sample count threshold per surviving node.
// Scenarios: performance-and-verification.md:PRF-SCN-033
func TestBuildResourceSummaryRequiresTwoSamplesPerSurvivor(t *testing.T) {
	root, nodes := resourceFixture(t, true)
	for i := range nodes {
		if nodes[i].Name == "node1" {
			continue
		}
		sampleAt := "2026-10-06T00:00:12Z"
		if nodes[i].Name == "node3" {
			sampleAt = "2026-10-06T00:00:16Z"
		}
		writeResourceStats(t, nodes[i].BrokerStatsPath, []resourceFixtureSample{
			{at: "2026-10-06T00:00:01Z", cpu: 10, memory: 100},
			{at: "2026-10-06T00:00:04Z", cpu: 20, memory: 200},
			{at: sampleAt, cpu: 10, memory: 100},
		})
	}
	summary, err := buildResourceSummary("narumq", nodes, filepath.Join(root, "failure.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if summary.Valid {
		t.Fatal("one running sample per survivor accepted")
	}
}

// TestReadFailureWindowsRejectsEmptyAndIncompleteRound verifies required windows are present and complete.
// Scenarios: performance-and-verification.md:PRF-SCN-033
func TestReadFailureWindowsRejectsEmptyAndIncompleteRound(t *testing.T) {
	path := filepath.Join(t.TempDir(), "failure.jsonl")
	if err := os.WriteFile(path, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	windows, err := readFailureWindows(path)
	if err != nil || windows.steady != nil || len(windows.rounds) != 0 {
		t.Fatalf("empty events=(%+v,%v)", windows, err)
	}
	writeEvents(t, path, []failureEventWindow{
		{Round: 0, Step: "steady", StartedAt: "2026-10-06T00:00:00Z", FinishedAt: "2026-10-06T00:00:05Z"},
		{Round: 1, Node: "node1", Step: "kill", StartedAt: "2026-10-06T00:00:10Z", FinishedAt: "2026-10-06T00:00:11Z"},
	})
	if _, err := readFailureWindows(path); err == nil {
		t.Fatal("incomplete round window accepted")
	}
}

type resourceFixtureSample struct {
	at                 string
	cpu                float64
	memory, blockWrite int64
}

func resourceFixture(t *testing.T, includeTarget bool) (string, []environment.ClusterNodeResult) {
	t.Helper()
	root := t.TempDir()
	writeEvents(t, filepath.Join(root, "failure.jsonl"), []failureEventWindow{
		{Round: 0, Step: "steady", StartedAt: "2026-10-06T00:00:00Z", FinishedAt: "2026-10-06T00:00:05Z"},
		{Round: 1, Node: "node3", Step: "kill", StartedAt: "2026-10-06T00:00:10Z", FinishedAt: "2026-10-06T00:00:11Z"},
		{Round: 1, Node: "node3", Step: "restart", StartedAt: "2026-10-06T00:00:14Z", FinishedAt: "2026-10-06T00:00:15Z"},
		{Round: 1, Node: "node3", Step: "settle", StartedAt: "2026-10-06T00:00:20Z", FinishedAt: "2026-10-06T00:00:21Z"},
	})
	nodes := make([]environment.ClusterNodeResult, 3)
	for i := range nodes {
		name := fmt.Sprintf("node%d", i+1)
		nodes[i].Name = name
		nodes[i].BrokerStatsPath = filepath.Join(root, name+"-broker.jsonl")
		nodes[i].NetworkStatsPath = filepath.Join(root, name+"-network.jsonl")
		samples := []resourceFixtureSample{{at: "2026-10-06T00:00:01Z", cpu: 10, memory: 100, blockWrite: 100}, {at: "2026-10-06T00:00:04Z", cpu: 20, memory: 200, blockWrite: 130}}
		if i == 0 {
			samples = append(samples, resourceFixtureSample{at: "2026-10-06T00:00:12Z", cpu: 25, memory: 600, blockWrite: 140}, resourceFixtureSample{at: "2026-10-06T00:00:19Z", cpu: 30, memory: 700, blockWrite: 160})
		} else if i == 1 {
			samples = append(samples, resourceFixtureSample{at: "2026-10-06T00:00:12Z", cpu: 25, memory: 250, blockWrite: 140}, resourceFixtureSample{at: "2026-10-06T00:00:19Z", cpu: 30, memory: 260, blockWrite: 160})
		} else if includeTarget {
			samples = append(samples, resourceFixtureSample{at: "2026-10-06T00:00:12Z", cpu: 40, memory: 250, blockWrite: 140}, resourceFixtureSample{at: "2026-10-06T00:00:16Z", cpu: 80, memory: 300, blockWrite: 5}, resourceFixtureSample{at: "2026-10-06T00:00:19Z", cpu: 60, memory: 280, blockWrite: 15})
		}
		writeResourceStats(t, nodes[i].BrokerStatsPath, samples)
		writeResourceStats(t, nodes[i].NetworkStatsPath, []resourceFixtureSample{
			{at: "2026-10-06T00:00:10Z", memory: 1}, {at: "2026-10-06T00:00:20Z", memory: 1},
		})
	}
	return root, nodes
}

func writeEvents(t *testing.T, path string, events []failureEventWindow) {
	t.Helper()
	var data []byte
	for _, event := range events {
		line, err := json.Marshal(event)
		if err != nil {
			t.Fatal(err)
		}
		data = append(data, line...)
		data = append(data, '\n')
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
}

func writeResourceStats(t *testing.T, path string, samples []resourceFixtureSample) {
	t.Helper()
	var data []byte
	for i, sample := range samples {
		entry := map[string]any{
			"collectedAt": sample.at,
			"container":   fmt.Sprintf("container-%d", i),
			"docker": map[string]string{
				"CPUPerc":  fmt.Sprintf("%g%%", sample.cpu),
				"MemUsage": fmt.Sprintf("%dB / 1GiB", sample.memory),
				"NetIO":    fmt.Sprintf("%dB / %dB", i*10, i*20),
				"BlockIO":  fmt.Sprintf("0B / %dB", sample.blockWrite),
			},
		}
		line, err := json.Marshal(entry)
		if err != nil {
			t.Fatal(err)
		}
		data = append(data, line...)
		data = append(data, '\n')
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
}
