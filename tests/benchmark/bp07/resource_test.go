package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"narumq/tests/benchmark/internal/environment"
)

func TestBuildResourceSummaryUsesRunningSamplesAndResetAwareWrites(t *testing.T) {
	phasePath, nodes := bp07ResourceFixture(t, []resourceStat{
		{at: "2026-10-05T00:00:00Z", cpu: 10, memory: 100, blockRead: 10, blockWrite: 100},
		{at: "2026-10-05T00:00:02Z", cpu: 99, memory: 0, blockRead: 900, blockWrite: 500},
		{at: "2026-10-05T00:00:04Z", cpu: 30, memory: 300, blockRead: 12, blockWrite: 150},
		{at: "2026-10-05T00:00:06Z", cpu: 50, memory: 200, blockRead: 1, blockWrite: 20},
	}, []resourceStat{
		{at: "2026-10-05T00:00:00Z", memory: 1, networkRx: 100, networkTx: 200},
		{at: "2026-10-05T00:00:10Z", memory: 1, networkRx: 140, networkTx: 280},
	})

	summary, err := buildResourceSummary("narumq", nodes, phasePath)
	if err != nil {
		t.Fatal(err)
	}
	if !summary.Valid || !summary.NetworkStatsAvailable {
		t.Fatalf("summary validity = (%t, %t), failures = %v", summary.Valid, summary.NetworkStatsAvailable, summary.FailureReasons)
	}
	if len(summary.Windows) != 1 {
		t.Fatalf("resource summary metadata = %+v", summary)
	}
	window := summary.Windows[0]
	if window.Window != "rolling" || len(window.Nodes) != 1 {
		t.Fatalf("rolling window = %+v", window)
	}
	want := resourceMeasurements{
		NetworkRxBytes:   40,
		NetworkTxBytes:   80,
		BlockReadBytes:   3,
		BlockWriteBytes:  70,
		MedianCPUPercent: 30,
		MaxMemoryBytes:   300,
	}
	if window.Cluster != want || window.Nodes[0].resourceMeasurements != want {
		t.Fatalf("rolling measurements = node %+v, cluster %+v; want %+v", window.Nodes[0].resourceMeasurements, window.Cluster, want)
	}

}

func TestBuildResourceSummaryMarksZeroOwnerNetworkUnavailable(t *testing.T) {
	phasePath, nodes := bp07ResourceFixture(t, []resourceStat{
		{at: "2026-10-05T00:00:00Z", cpu: 10, memory: 100},
		{at: "2026-10-05T00:00:10Z", cpu: 20, memory: 200},
	}, []resourceStat{
		{at: "2026-10-05T00:00:00Z", memory: 1},
		{at: "2026-10-05T00:00:10Z", memory: 1},
	})

	summary, err := buildResourceSummary("emqx", nodes, phasePath)
	if err != nil {
		t.Fatal(err)
	}
	if !summary.Valid {
		t.Fatalf("zero network deltas invalidated resource summary: %v", summary.FailureReasons)
	}
	if summary.NetworkStatsAvailable {
		t.Fatalf("zero network result = (%t, %q)", summary.NetworkStatsAvailable, summary.NetworkStatsReason)
	}
}

func TestBuildResourceSummaryRequiresTwoRunningBrokerSamples(t *testing.T) {
	phasePath, nodes := bp07ResourceFixture(t, []resourceStat{
		{at: "2026-10-05T00:00:00Z", cpu: 10, memory: 100},
		{at: "2026-10-05T00:00:10Z", cpu: 99, memory: 0},
	}, []resourceStat{
		{at: "2026-10-05T00:00:00Z", memory: 1},
		{at: "2026-10-05T00:00:10Z", memory: 1},
	})

	summary, err := buildResourceSummary("narumq", nodes, phasePath)
	if err != nil {
		t.Fatal(err)
	}
	if summary.Valid {
		t.Fatal("summary is valid with fewer than two running broker samples")
	}
	if !strings.Contains(strings.Join(summary.FailureReasons, "; "), "fewer than two running broker stats samples") {
		t.Fatalf("failure reasons = %v", summary.FailureReasons)
	}
}

type resourceStat struct {
	at                    string
	cpu                   float64
	memory                int64
	networkRx, networkTx  int64
	blockRead, blockWrite int64
}

func bp07ResourceFixture(t *testing.T, brokerSamples, networkSamples []resourceStat) (string, []environment.ClusterNodeResult) {
	t.Helper()
	root := t.TempDir()
	phasePath := filepath.Join(root, "phases.jsonl")
	phaseEvents := []map[string]string{
		{"phase": "rolling", "event": "start", "timestamp": "2026-10-05T00:00:00Z"},
		{"phase": "rolling", "event": "end", "timestamp": "2026-10-05T00:00:10Z"},
	}
	var phases bytes.Buffer
	encoder := json.NewEncoder(&phases)
	for _, event := range phaseEvents {
		if err := encoder.Encode(event); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(phasePath, phases.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}

	brokerPath := filepath.Join(root, "node1-broker-stats.jsonl")
	networkPath := filepath.Join(root, "node1-network-stats.jsonl")
	writeBP07ResourceStats(t, brokerPath, "broker", brokerSamples)
	writeBP07ResourceStats(t, networkPath, "network-owner", networkSamples)
	return phasePath, []environment.ClusterNodeResult{{
		Name: "node1", BrokerStatsPath: brokerPath, NetworkStatsPath: networkPath,
	}}
}

func writeBP07ResourceStats(t *testing.T, path, container string, samples []resourceStat) {
	t.Helper()
	var data bytes.Buffer
	encoder := json.NewEncoder(&data)
	for _, sample := range samples {
		entry := struct {
			CollectedAt string            `json:"collectedAt"`
			Container   string            `json:"container"`
			Docker      map[string]string `json:"docker"`
		}{
			CollectedAt: sample.at,
			Container:   container,
			Docker: map[string]string{
				"CPUPerc":  fmt.Sprintf("%g%%", sample.cpu),
				"MemUsage": fmt.Sprintf("%dB / 1GiB", sample.memory),
				"NetIO":    fmt.Sprintf("%dB / %dB", sample.networkRx, sample.networkTx),
				"BlockIO":  fmt.Sprintf("%dB / %dB", sample.blockRead, sample.blockWrite),
			},
		}
		if err := encoder.Encode(entry); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(path, data.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
}
