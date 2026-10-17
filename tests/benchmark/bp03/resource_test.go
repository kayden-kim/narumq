package main

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"

	"narumq/tests/benchmark/internal/environment"
)

func TestBuildResourceSummaryDeltasAndRatios(t *testing.T) {
	phasePath, nodes := resourceFixture(t, true, true)
	workload := workloadSummary{
		Steps:   []stepSummary{{Step: 1, QoS: map[string]qosStats{"1": {Acked: 3}, "2": {Acked: 2}}}},
		Offline: offlineSummary{QoS: map[string]offlineQoSStats{"1": {Acked: 2}, "2": {Acked: 1}}},
	}
	summary, err := buildResourceSummary("narumq", nodes, phasePath, workload)
	if err != nil {
		t.Fatal(err)
	}
	if !summary.Valid || !summary.NetworkStatsAvailable {
		t.Fatalf("summary validity: %+v", summary)
	}
	if summary.ClockSource != "stats=host clock, phases=runner container clock" {
		t.Fatalf("clock source = %q", summary.ClockSource)
	}
	if len(summary.Windows) != 2 {
		t.Fatalf("windows = %d", len(summary.Windows))
	}
	measure := summary.Windows[0]
	if measure.Window != "step-01-measure" || measure.AckedMessages != 5 {
		t.Fatalf("measure window = %+v", measure)
	}
	want := resourceMeasurements{NetworkRxBytes: 40, NetworkTxBytes: 80, BlockReadBytes: 20, BlockWriteBytes: 40, MedianCPUPercent: 40, MaxMemoryBytes: 300}
	if measure.Cluster != want {
		t.Fatalf("measure cluster = %+v, want %+v", measure.Cluster, want)
	}
	if measure.Nodes[0].NetworkRxBytes != 20 || measure.Nodes[0].NetworkTxBytes != 40 || measure.Nodes[0].BlockReadBytes != 10 || measure.Nodes[0].BlockWriteBytes != 20 {
		t.Fatalf("node deltas = %+v", measure.Nodes[0])
	}
	if measure.Nodes[0].MedianCPUPercent != 20 || measure.Nodes[0].MaxMemoryBytes != 300 {
		t.Fatalf("broker stats = %+v", measure.Nodes[0])
	}
	if got := *measure.NetworkTxBytesPerAckedMessage; got != 16 {
		t.Fatalf("network tx/acked = %v", got)
	}
	if got := *measure.BlockWriteBytesPerAckedMessage; got != 8 {
		t.Fatalf("block write/acked = %v", got)
	}
	if summary.Windows[1].AckedMessages != 3 || *summary.Windows[1].NetworkTxBytesPerAckedMessage != 80.0/3 {
		t.Fatalf("offline window = %+v", summary.Windows[1])
	}
}

// TestBuildResourceSummaryInsufficientSamplesInvalidates verifies insufficient samples invalidate the resource summary.
// Scenarios: performance-and-verification.md:PRF-SCN-006
func TestBuildResourceSummaryInsufficientSamplesInvalidates(t *testing.T) {
	phasePath, nodes := resourceFixture(t, true, true)
	writeResourceStats(t, nodes[1].NetworkStatsPath, []resourceStat{{at: "2026-01-01T00:00:01Z", networkRx: 10}})
	summary, err := buildResourceSummary("emqx", nodes, phasePath, workloadSummary{})
	if err != nil {
		t.Fatal(err)
	}
	if summary.Valid || len(summary.FailureReasons) == 0 {
		t.Fatalf("insufficient samples were not reported: %+v", summary)
	}
}

// TestBuildResourceSummaryZeroBlockWritesValid verifies zero block writes keep the summary valid.
// Scenarios: performance-and-verification.md:PRF-SCN-006
func TestBuildResourceSummaryZeroBlockWritesValid(t *testing.T) {
	phasePath, nodes := resourceFixture(t, true, true)
	for _, node := range nodes {
		writeResourceStats(t, node.BrokerStatsPath, []resourceStat{
			{at: "2026-01-01T00:00:01Z"}, {at: "2026-01-01T00:00:02Z"},
			{at: "2026-01-01T00:00:04Z"}, {at: "2026-01-01T00:00:05Z"},
		})
	}
	summary, err := buildResourceSummary("narumq", nodes, phasePath, workloadSummary{})
	if err != nil {
		t.Fatal(err)
	}
	if !summary.Valid {
		t.Fatalf("zero block writes invalidated summary: %+v", summary)
	}
	for _, window := range summary.Windows {
		if window.Cluster.BlockWriteBytes != 0 {
			t.Fatalf("block writes = %d", window.Cluster.BlockWriteBytes)
		}
	}
}

// TestBuildResourceSummaryNetworkUnavailableDoesNotInvalidate verifies missing network samples do not invalidate the workload.
// Scenarios: performance-and-verification.md:PRF-SCN-006
func TestBuildResourceSummaryNetworkUnavailableDoesNotInvalidate(t *testing.T) {
	phasePath, nodes := resourceFixture(t, false, true)
	workload := workloadSummary{Steps: []stepSummary{{Step: 1, QoS: map[string]qosStats{"1": {Acked: 5}}}}}
	summary, err := buildResourceSummary("narumq", nodes, phasePath, workload)
	if err != nil {
		t.Fatal(err)
	}
	if !summary.Valid || summary.NetworkStatsAvailable {
		t.Fatalf("network availability result = %+v", summary)
	}
	if summary.NetworkStatsReason == "" || summary.Windows[0].NetworkTxBytesPerAckedMessage != nil {
		t.Fatalf("network metrics were not disabled: %+v", summary.Windows[0])
	}
	if summary.Windows[0].BlockWriteBytesPerAckedMessage == nil || math.IsNaN(*summary.Windows[0].BlockWriteBytesPerAckedMessage) {
		t.Fatalf("block metric unavailable: %+v", summary.Windows[0])
	}
}

type resourceStat struct {
	at                                          string
	networkRx, networkTx, blockRead, blockWrite int64
	cpu                                         float64
	memory                                      int64
}

func resourceFixture(t *testing.T, network, twoNodes bool) (string, []environment.ClusterNodeResult) {
	t.Helper()
	dir := t.TempDir()
	phasePath := filepath.Join(dir, "phases.jsonl")
	phaseData := "{\"phase\":\"step-01-measure\",\"event\":\"start\",\"timestamp\":\"2026-01-01T00:00:00Z\"}\n" +
		"{\"phase\":\"step-01-measure\",\"event\":\"end\",\"timestamp\":\"2026-01-01T00:00:03Z\"}\n" +
		"{\"phase\":\"offline\",\"event\":\"start\",\"timestamp\":\"2026-01-01T00:00:03Z\"}\n" +
		"{\"phase\":\"offline\",\"event\":\"end\",\"timestamp\":\"2026-01-01T00:00:06Z\"}\n"
	if err := os.WriteFile(phasePath, []byte(phaseData), 0o600); err != nil {
		t.Fatal(err)
	}
	count := 2
	if !twoNodes {
		count = 1
	}
	nodes := make([]environment.ClusterNodeResult, count)
	for n := range nodes {
		nodeDir := filepath.Join(dir, fmt.Sprintf("node%d", n+1))
		if err := os.MkdirAll(nodeDir, 0o755); err != nil {
			t.Fatal(err)
		}
		nodes[n] = environment.ClusterNodeResult{Name: fmt.Sprintf("node%d", n+1), BrokerStatsPath: filepath.Join(nodeDir, "broker.jsonl"), NetworkStatsPath: filepath.Join(nodeDir, "network.jsonl")}
		broker := []resourceStat{
			{at: "2026-01-01T00:00:01Z", blockRead: int64(2 * n), blockWrite: int64(5 * n), cpu: 10, memory: 200},
			{at: "2026-01-01T00:00:02Z", blockRead: int64(2*n + 10), blockWrite: int64(5*n + 20), cpu: 30, memory: 300},
			{at: "2026-01-01T00:00:04Z", blockRead: int64(2*n + 10), blockWrite: int64(5*n + 20), cpu: 15, memory: 250},
			{at: "2026-01-01T00:00:05Z", blockRead: int64(2*n + 20), blockWrite: int64(5*n + 40), cpu: 25, memory: 350},
		}
		writeResourceStats(t, nodes[n].BrokerStatsPath, broker)
		networkSamples := []resourceStat{{at: broker[0].at}, {at: broker[1].at}, {at: broker[2].at}, {at: broker[3].at}}
		if network {
			networkSamples[0].networkRx, networkSamples[0].networkTx = int64(10*n), int64(20*n)
			networkSamples[1].networkRx, networkSamples[1].networkTx = int64(10*n+20), int64(20*n+40)
			networkSamples[2].networkRx, networkSamples[2].networkTx = int64(10*n+20), int64(20*n+40)
			networkSamples[3].networkRx, networkSamples[3].networkTx = int64(10*n+40), int64(20*n+80)
		}
		writeResourceStats(t, nodes[n].NetworkStatsPath, networkSamples)
	}
	return phasePath, nodes
}

func writeResourceStats(t *testing.T, path string, samples []resourceStat) {
	t.Helper()
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	encoder := json.NewEncoder(file)
	for _, sample := range samples {
		entry := map[string]any{"collectedAt": sample.at, "container": "test", "docker": map[string]string{
			"CPUPerc": fmt.Sprintf("%g%%", sample.cpu), "MemUsage": fmt.Sprintf("%dB / 1GB", sample.memory),
			"NetIO":   fmt.Sprintf("%dB / %dB", sample.networkRx, sample.networkTx),
			"BlockIO": fmt.Sprintf("%dB / %dB", sample.blockRead, sample.blockWrite),
		}}
		if err := encoder.Encode(entry); err != nil {
			t.Fatal(err)
		}
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
}
