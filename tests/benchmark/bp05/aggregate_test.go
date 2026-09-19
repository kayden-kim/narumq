package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestNearestRankPercentiles(t *testing.T) {
	values := make([]int64, 100)
	for index := range values {
		values[index] = int64(index + 1)
	}
	percentiles := nearestRankPercentiles(values)
	if percentiles.P50Nanoseconds != 50 || percentiles.P95Nanoseconds != 95 || percentiles.P99Nanoseconds != 99 || percentiles.P999Nanoseconds != 100 {
		t.Fatalf("nearestRankPercentiles() = %#v", percentiles)
	}
}

func TestSelectReconnectRecordsUsesLowestClientIndexes(t *testing.T) {
	alive := map[string]*connectionRecord{
		"bp05-000003": {clientID: "bp05-000003", index: 3},
		"bp05-000001": {clientID: "bp05-000001", index: 1},
		"bp05-000002": {clientID: "bp05-000002", index: 2},
	}
	selected := selectReconnectRecords(alive, 2)
	if len(selected) != 2 || selected[0].index != 1 || selected[1].index != 2 {
		t.Fatalf("selectReconnectRecords() = %#v, want indexes 1 and 2", selected)
	}
}

func TestBuildWorkloadSummaryRequiresEverySmokeInvariant(t *testing.T) {
	profile := validBP05Profile()
	counters := workloadCounters{
		initialAttempted:   profile.ConnectionCount,
		initialSucceeded:   profile.ConnectionCount,
		initialDuration:    time.Second,
		idleSurvivors:      profile.ConnectionCount,
		pingSent:           16,
		pingReceived:       16,
		reconnectAttempted: profile.ReconnectCount,
		reconnectSucceeded: profile.ReconnectCount,
		reconnectDuration:  time.Second,
		finalPingRequested: profile.ConnectionCount,
		finalPingSucceeded: profile.ConnectionCount,
		initialLatencies:   []int64{1, 2, 3},
		reconnectLatencies: []int64{4, 5, 6},
	}
	summary := buildWorkloadSummary(profile, "narumq", counters)
	if !summary.Valid || summary.ReleaseGating || summary.InitialConnectLatency.P50Nanoseconds != 2 || summary.ReconnectLatency.P99Nanoseconds != 6 {
		t.Fatalf("buildWorkloadSummary() = %#v", summary)
	}
	counters.generatorOverload = 1
	summary = buildWorkloadSummary(profile, "narumq", counters)
	if summary.Valid {
		t.Fatal("generator overload was accepted as valid")
	}
}

func TestWriteWorkloadSummaryUsesStableArtifactName(t *testing.T) {
	outputDir := t.TempDir()
	want := WorkloadSummary{SchemaVersion: 1, Target: "narumq", Purpose: purposeSmoke, ReleaseGating: false}
	if err := WriteWorkloadSummary(outputDir, want); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(outputDir, "workload-summary.json"))
	if err != nil {
		t.Fatal(err)
	}
	var got WorkloadSummary
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	if got.SchemaVersion != 1 || got.Target != "narumq" || got.ReleaseGating {
		t.Fatalf("workload-summary.json = %#v", got)
	}
}
