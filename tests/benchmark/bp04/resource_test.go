package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"narumq/tests/benchmark/internal/environment"
)

func TestMedianMemoryUsesLowerPlusHalfDifference(t *testing.T) {
	samples := []environment.StatsSample{{MemoryUsageBytes: 1}, {MemoryUsageBytes: 2}, {MemoryUsageBytes: 4}, {MemoryUsageBytes: 9}}
	if got := medianMemoryBytes(samples); got != 3 {
		t.Fatalf("median memory = %d, want 3", got)
	}
}

func TestBuildResourceSummaryAcceptsZeroNetworkDeltas(t *testing.T) {
	summary := buildTestResourceSummary(t, 2)
	if !summary.Valid {
		t.Fatalf("resource summary invalid: %v", summary.FailureReasons)
	}
	if !summary.NetworkStatsAvailable {
		t.Fatalf("all-zero network counters were marked unavailable: %q", summary.NetworkStatsReason)
	}
	if len(summary.Windows) != 3 {
		t.Fatalf("resource windows = %d, want idle, subscribed-idle, and one measure window", len(summary.Windows))
	}
	if got := summary.Windows[0].Cluster.MedianCPUPercent; got != 45 {
		t.Fatalf("idle cluster median CPU = %v, want 45", got)
	}
	if got := summary.Windows[0].Cluster.MedianMemoryBytes; got != 480 {
		t.Fatalf("idle cluster median memory = %d, want 480", got)
	}
	if got := summary.Memory.IncrementalMemoryBytes; got != 300 {
		t.Fatalf("incremental memory = %d, want 300", got)
	}
	if got := summary.Memory.MemoryBytesPerSetupClient; got != 300 {
		t.Fatalf("memory bytes per setup client = %v, want 300", got)
	}
	if got := summary.Windows[2].Deliveries; got != 6 {
		t.Fatalf("step deliveries = %d, want 6", got)
	}
}

func TestBuildResourceSummaryRejectsInsufficientSamples(t *testing.T) {
	summary := buildTestResourceSummary(t, 1)
	if summary.Valid {
		t.Fatal("resource summary accepted one sample per node and window")
	}
	if !strings.Contains(strings.Join(summary.FailureReasons, " "), "fewer than two") {
		t.Fatalf("failure reasons do not identify insufficient samples: %v", summary.FailureReasons)
	}
}

func buildTestResourceSummary(t *testing.T, samplesPerWindow int) resourceSummary {
	t.Helper()
	base := time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)
	windows := []struct {
		name  string
		start time.Time
	}{
		{name: "idle-baseline", start: base},
		{name: "subscribed-idle", start: base.Add(10 * time.Second)},
		{name: "step-01-measure", start: base.Add(20 * time.Second)},
	}
	phasePath := filepath.Join(t.TempDir(), "phases.jsonl")
	var phaseData []byte
	for _, window := range windows {
		for _, event := range []phaseEvent{
			{Phase: window.name, Event: "start", Timestamp: window.start.Format(time.RFC3339Nano)},
			{Phase: window.name, Event: "end", Timestamp: window.start.Add(4 * time.Second).Format(time.RFC3339Nano)},
		} {
			encoded, err := json.Marshal(event)
			if err != nil {
				t.Fatal(err)
			}
			phaseData = append(phaseData, encoded...)
			phaseData = append(phaseData, '\n')
		}
	}
	if err := os.WriteFile(phasePath, phaseData, 0o600); err != nil {
		t.Fatal(err)
	}

	profile := workloadSummary{
		Setup: setupSummary{Clients: 1},
		Steps: []stepSummary{{Step: 1, Classes: map[string]classSummary{
			classFanout: {Delivered: 5}, classShared: {Delivered: 1},
		}}},
	}
	nodes := make([]environment.ClusterNodeResult, bp04NodeCount)
	for nodeIndex := range nodes {
		nodeName := fmt.Sprintf("node%d", nodeIndex+1)
		brokerPath := filepath.Join(t.TempDir(), "broker-stats.jsonl")
		networkPath := filepath.Join(t.TempDir(), "network-stats.jsonl")
		var brokerData, networkData []byte
		for phaseIndex, window := range windows {
			for sampleIndex := 0; sampleIndex < samplesPerWindow; sampleIndex++ {
				offset := time.Duration(sampleIndex+1) * time.Second
				at := window.start.Add(offset)
				memory := int64(100 + nodeIndex*10 + phaseIndex*100)
				if sampleIndex%2 == 1 {
					memory += 100
				}
				brokerData = append(brokerData, resourceStatsLine(t, nodeName, at, 10+float64(sampleIndex)*10, memory)...)
				brokerData = append(brokerData, '\n')
				networkData = append(networkData, resourceStatsLine(t, nodeName+"-net", at, 1, 10)...)
				networkData = append(networkData, '\n')
			}
		}
		if err := os.WriteFile(brokerPath, brokerData, 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(networkPath, networkData, 0o600); err != nil {
			t.Fatal(err)
		}
		nodes[nodeIndex] = environment.ClusterNodeResult{Name: nodeName, BrokerStatsPath: brokerPath, NetworkStatsPath: networkPath}
	}
	summary, err := buildResourceSummary("narumq", nodes, phasePath, profile)
	if err != nil {
		t.Fatal(err)
	}
	return summary
}

func resourceStatsLine(t *testing.T, container string, at time.Time, cpu float64, memory int64) []byte {
	t.Helper()
	line := struct {
		CollectedAt string         `json:"collectedAt"`
		Container   string         `json:"container"`
		Docker      map[string]any `json:"docker"`
	}{
		CollectedAt: at.Format(time.RFC3339Nano),
		Container:   container,
		Docker: map[string]any{
			"CPUPerc":  fmt.Sprintf("%.2f%%", cpu),
			"MemUsage": fmt.Sprintf("%dB / 1073741824B", memory),
			"NetIO":    "0B / 0B",
			"BlockIO":  "0B / 0B",
		},
	}
	encoded, err := json.Marshal(line)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}
