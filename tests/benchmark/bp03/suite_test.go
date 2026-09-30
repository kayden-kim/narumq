package main

import (
	"context"
	"fmt"
	"io"
	"regexp"
	"strings"
	"testing"

	"narumq/tests/benchmark/internal/suiteartifact"
)

func TestBP03MetricsEmitRequiredKeysAndUnits(t *testing.T) {
	profile := loadSmokeProfileForCompare(t)
	for _, target := range []string{"narumq", "emqx"} {
		manifest := suiteTestTarget(profile, target, true)
		metrics, err := bp03Metrics(target, manifest)
		if err != nil {
			t.Fatal(err)
		}
		units, err := bp03SuiteMetricUnits(profile, target, true)
		if err != nil {
			t.Fatal(err)
		}
		if len(metrics) != len(units) {
			t.Fatalf("%s metric count = %d, units = %d", target, len(metrics), len(units))
		}
		for key := range units {
			if _, exists := metrics[key]; !exists {
				t.Errorf("%s metrics missing %q", target, key)
			}
		}
		if target == "narumq" {
			for _, key := range []string{"step01_wal_syncs_per_second", "step01_wal_mean_entries_per_sync", "step01_wal_bytes_per_acked_message"} {
				if _, exists := metrics[key]; !exists {
					t.Errorf("NaruMQ metrics missing %q", key)
				}
			}
		} else if _, exists := metrics["step01_wal_syncs_per_second"]; exists {
			t.Fatal("EMQX metrics unexpectedly include WAL measurements")
		}
	}

	unavailable := suiteTestTarget(profile, "emqx", false)
	metrics, err := bp03Metrics("emqx", unavailable)
	if err != nil {
		t.Fatal(err)
	}
	if _, exists := metrics["step01_network_tx_bytes_per_acked_message"]; exists {
		t.Fatal("network metric was emitted while owner NetIO was unavailable")
	}
	if _, exists := metrics["step01_block_write_bytes_per_acked_message"]; !exists {
		t.Fatal("zero block-write metric was omitted")
	}
}

func TestBuildBP03SuiteSummaryOmitsNetworkMetricsWhenAnyRunLacksNetworkStats(t *testing.T) {
	profile := loadSmokeProfileForCompare(t)
	manifest, runs := suiteTestRuns(profile, true)
	for index := range runs {
		if index != len(runs)-1 {
			continue
		}
		for target, entry := range runs[index].Manifest.Targets {
			resources := *entry.ResourceSummary
			resources.NetworkStatsAvailable = false
			entry.ResourceSummary = &resources
			runs[index].Manifest.Targets[target] = entry
		}
	}
	summary := buildBP03SuiteSummary(bp03SuiteOptions{Profile: profile, Repetitions: suiteartifact.MinimumRepetitions}, manifest, runs)
	if !summary.Valid {
		t.Fatalf("network-unavailable suite became invalid: %s", summary.FailureReason)
	}
	for target, targetSummary := range summary.Targets {
		if targetSummary.NetworkStatsAvailable {
			t.Errorf("%s marked network stats available", target)
		}
		for key := range targetSummary.Metrics {
			if strings.Contains(key, "_network_tx_bytes_per_acked_message") {
				t.Errorf("%s retained unavailable network metric %q", target, key)
			}
		}
		wantUnits, err := bp03SuiteMetricUnits(profile, target, false)
		if err != nil {
			t.Fatal(err)
		}
		if len(targetSummary.Metrics) != len(wantUnits) {
			t.Errorf("%s metric count = %d, want %d without network", target, len(targetSummary.Metrics), len(wantUnits))
		}
	}
}

func TestBuildBP03SuiteSummaryInvalidatesAllMetricsOnOneFailedRun(t *testing.T) {
	profile := loadSmokeProfileForCompare(t)
	manifest, runs := suiteTestRuns(profile, true)
	runs[2].Success = false
	runs[2].Manifest.FailureReason = "target failed"
	summary := buildBP03SuiteSummary(bp03SuiteOptions{Profile: profile, Repetitions: suiteartifact.MinimumRepetitions}, manifest, runs)
	if summary.Valid || summary.SuccessfulRepetitions != suiteartifact.MinimumRepetitions-1 {
		t.Fatalf("invalid suite status = valid:%t successful:%d", summary.Valid, summary.SuccessfulRepetitions)
	}
	for target, targetSummary := range summary.Targets {
		if len(targetSummary.Metrics) != 0 {
			t.Errorf("%s retained %d parent metrics after a failed repetition", target, len(targetSummary.Metrics))
		}
	}
}

func TestSuiteCommandRejectsFewerThanMinimumRepetitions(t *testing.T) {
	err := suiteCommand(context.Background(), []string{"--profile", "missing.json", "--repetitions", "4"}, io.Discard, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "at least 5 repetitions") {
		t.Fatalf("suite with four repetitions error = %v", err)
	}
}

func TestBP03SuiteIDFormat(t *testing.T) {
	pattern := regexp.MustCompile(`^bp03-suite-[0-9]{8}T[0-9]{6}\.[0-9]{9}Z-[0-9]+$`)
	if suiteID := newBP03SuiteID(); !pattern.MatchString(suiteID) {
		t.Fatalf("suite ID %q does not match the required UTC timestamp and PID format", suiteID)
	}
}

func suiteTestRuns(profile Profile, networkAvailable bool) (bp03SuiteManifest, []bp03SuiteRunData) {
	manifest := bp03SuiteManifest{
		SuiteID:              "bp03-suite-test",
		RequestedRepetitions: suiteartifact.MinimumRepetitions,
		CompletedRepetitions: suiteartifact.MinimumRepetitions,
	}
	runs := make([]bp03SuiteRunData, 0, suiteartifact.MinimumRepetitions)
	for repetition := 1; repetition <= suiteartifact.MinimumRepetitions; repetition++ {
		runID := fmt.Sprintf("run-%02d", repetition)
		narumq := suiteTestTarget(profile, "narumq", networkAvailable)
		emqx := suiteTestTarget(profile, "emqx", networkAvailable)
		child := bp03Manifest{Targets: map[string]bp03TargetManifest{"narumq": narumq, "emqx": emqx}}
		runs = append(runs, bp03SuiteRunData{Repetition: repetition, RunID: runID, Manifest: child, Success: true})
		manifest.Runs = append(manifest.Runs, bp03SuiteRun{Repetition: repetition, RunID: runID, Success: true})
	}
	return manifest, runs
}

func suiteTestTarget(profile Profile, target string, networkAvailable bool) bp03TargetManifest {
	workload := &workloadSummary{
		Target: target,
		Valid:  true,
		Steps:  make([]stepSummary, len(profile.RatesPerStreamPerSecond)),
	}
	resources := &resourceSummary{Target: target, Valid: true, NetworkStatsAvailable: networkAvailable}
	wal := &WALSummary{Target: target, Valid: target == "narumq"}
	for index := range profile.RatesPerStreamPerSecond {
		stepNumber := index + 1
		qos := map[string]qosStats{
			"1": {
				AckRatePerSecond:           float64(10 + stepNumber),
				AckLatencyNanoseconds:      quantiles{P99: int64(1000 + stepNumber), P999: int64(2000 + stepNumber)},
				DeliveryLatencyNanoseconds: quantiles{P99: int64(3000 + stepNumber)},
			},
			"2": {
				AckRatePerSecond:           float64(20 + stepNumber),
				AckLatencyNanoseconds:      quantiles{P99: int64(4000 + stepNumber), P999: int64(5000 + stepNumber)},
				DeliveryLatencyNanoseconds: quantiles{P99: int64(6000 + stepNumber)},
			},
		}
		workload.Steps[index] = stepSummary{Step: stepNumber, QoS: qos}
		zero := 0.0
		network := float64(11 + stepNumber)
		resources.Windows = append(resources.Windows, resourceWindow{
			Window:                         fmt.Sprintf("step-%02d-measure", stepNumber),
			NetworkTxBytesPerAckedMessage:  &network,
			BlockWriteBytesPerAckedMessage: &zero,
		})
		if target == "narumq" {
			bytesPerAck := float64(21 + stepNumber)
			wal.Windows = append(wal.Windows, walWindowSummary{
				Name: fmt.Sprintf("step-%02d", stepNumber),
				Cluster: walMetricSummary{
					SyncsPerSecond:          float64(31 + stepNumber),
					MeanEntriesPerSync:      float64(41 + stepNumber),
					WALBytesPerAckedMessage: &bytesPerAck,
				},
			})
		}
	}
	workload.Offline.ReconnectDrainMillis.Max = 12.5
	return bp03TargetManifest{Success: true, Valid: true, WorkloadSummary: workload, ResourceSummary: resources, WALSummary: wal}
}
