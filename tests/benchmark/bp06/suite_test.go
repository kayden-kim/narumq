package main

import (
	"fmt"
	"testing"

	"narumq/tests/benchmark/internal/suiteartifact"
)

// TestBP06MetricsEmitExactKeysAndUnits verifies suite metric keys and units.
// Scenarios: performance-and-verification.md:PRF-SCN-034
func TestBP06MetricsEmitExactKeysAndUnits(t *testing.T) {
	profile := loadBP06SuiteTestProfile(t)
	wantUnits := make(map[string]string, profile.rounds()*6+3)
	for round := 1; round <= profile.rounds(); round++ {
		prefix := fmt.Sprintf("round%02d_", round)
		wantUnits[prefix+"rto_millis"] = "ms"
		wantUnits[prefix+"rejoin_ready_millis"] = "ms"
		wantUnits[prefix+"resync_millis"] = "ms"
		wantUnits[prefix+"reconnect_latency_max_millis"] = "ms"
		wantUnits[prefix+"recovery_cpu_peak_percent"] = "percent"
		wantUnits[prefix+"recovery_memory_peak_bytes"] = "bytes"
	}
	wantUnits["reconnects_total"] = "connections"
	wantUnits["qos1_duplicates_total"] = "messages"
	wantUnits["qos0_lost_total"] = "messages"
	gotUnits := bp06SuiteMetricUnits(profile)
	if len(gotUnits) != len(wantUnits) {
		t.Fatalf("unit key count = %d, want %d", len(gotUnits), len(wantUnits))
	}
	for key, unit := range wantUnits {
		if gotUnits[key] != unit {
			t.Errorf("metric %q unit = %q, want %q", key, gotUnits[key], unit)
		}
	}
	for _, target := range []string{"narumq", "emqx"} {
		metrics, err := bp06Metrics(target, suiteTestTarget(profile, target, true, 0))
		if err != nil {
			t.Fatal(err)
		}
		if len(metrics) != len(wantUnits) {
			t.Fatalf("%s metric count = %d, want %d", target, len(metrics), len(wantUnits))
		}
		for key := range wantUnits {
			if _, exists := metrics[key]; !exists {
				t.Errorf("%s metrics missing %q", target, key)
			}
		}
		for key := range metrics {
			if _, exists := wantUnits[key]; !exists {
				t.Errorf("%s metrics unexpectedly include %q", target, key)
			}
		}
	}
	for key := range gotUnits {
		if _, exists := wantUnits[key]; !exists {
			t.Errorf("unexpected unit key %q", key)
		}
	}
}

// TestBP06MetricsRejectDuplicateOrMissingRounds verifies suite metrics reject duplicate or missing rounds.
// Scenarios: performance-and-verification.md:PRF-SCN-034
func TestBP06MetricsRejectDuplicateOrMissingRounds(t *testing.T) {
	profile := loadBP06SuiteTestProfile(t)
	t.Run("duplicate failure round", func(t *testing.T) {
		target := suiteTestTarget(profile, "narumq", true, 0)
		failure := *target.FailureSummary
		failure.Rounds = append([]failureRoundSummary(nil), failure.Rounds...)
		failure.Rounds[1].Round = failure.Rounds[0].Round
		target.FailureSummary = &failure
		if _, err := bp06Metrics("narumq", target); err == nil {
			t.Fatal("duplicate failure round was accepted")
		}
	})
	t.Run("missing workload round", func(t *testing.T) {
		target := suiteTestTarget(profile, "emqx", true, 0)
		workload := *target.WorkloadSummary
		workload.Rounds = append([]roundSummary(nil), workload.Rounds[:len(workload.Rounds)-1]...)
		target.WorkloadSummary = &workload
		if _, err := bp06Metrics("emqx", target); err == nil {
			t.Fatal("missing workload round was accepted")
		}
	})
	t.Run("missing resource round", func(t *testing.T) {
		target := suiteTestTarget(profile, "narumq", true, 0)
		target.ResourceSummary.Rounds = target.ResourceSummary.Rounds[:profile.rounds()-1]
		if _, err := bp06Metrics("narumq", target); err == nil {
			t.Fatal("missing resource round was accepted")
		}
	})
}

// TestBuildBP06SuiteSummaryKeepsMetricsWithoutNetworkStats verifies suite metrics when network stats are unavailable.
// Scenarios: performance-and-verification.md:PRF-SCN-034
func TestBuildBP06SuiteSummaryKeepsMetricsWithoutNetworkStats(t *testing.T) {
	profile := loadBP06SuiteTestProfile(t)
	manifest, runs := suiteTestRuns(profile)
	last := len(runs) - 1
	for target, entry := range runs[last].Manifest.Targets {
		resources := *entry.ResourceSummary
		resources.Network.NetworkStatsAvailable = false
		entry.ResourceSummary = &resources
		runs[last].Manifest.Targets[target] = entry
	}
	summary := buildBP06SuiteSummary(bp06SuiteOptions{Profile: profile, Repetitions: suiteartifact.MinimumRepetitions}, manifest, runs)
	if !summary.Valid {
		t.Fatalf("network-unavailable suite became invalid: %s", summary.FailureReason)
	}
	for target, targetSummary := range summary.Targets {
		if targetSummary.NetworkStatsAvailable {
			t.Errorf("%s marked network stats available", target)
		}
		if len(targetSummary.Metrics) != len(bp06SuiteMetricUnits(profile)) {
			t.Errorf("%s metric count = %d, want %d", target, len(targetSummary.Metrics), len(bp06SuiteMetricUnits(profile)))
		}
	}
}

// TestBuildBP06SuiteSummaryInvalidatesAllMetricsOnOneFailedRun verifies one invalid child run empties the parent metrics.
// Scenarios: performance-and-verification.md:PRF-SCN-034
func TestBuildBP06SuiteSummaryInvalidatesAllMetricsOnOneFailedRun(t *testing.T) {
	profile := loadBP06SuiteTestProfile(t)
	manifest, runs := suiteTestRuns(profile)
	runs[2].Success = false
	runs[2].Manifest.FailureReason = "target failed"
	summary := buildBP06SuiteSummary(bp06SuiteOptions{Profile: profile, Repetitions: suiteartifact.MinimumRepetitions}, manifest, runs)
	if summary.Valid || summary.SuccessfulRepetitions != suiteartifact.MinimumRepetitions-1 {
		t.Fatalf("invalid suite status = valid:%t successful:%d", summary.Valid, summary.SuccessfulRepetitions)
	}
	for target, targetSummary := range summary.Targets {
		if len(targetSummary.Metrics) != 0 {
			t.Errorf("%s retained %d parent metrics after a failed repetition", target, len(targetSummary.Metrics))
		}
	}
}

// TestBuildBP06SuiteSummaryUsesEveryRepetitionForMedianAndMAD verifies the median and MAD over repetitions.
// Scenarios: performance-and-verification.md:PRF-SCN-034
func TestBuildBP06SuiteSummaryUsesEveryRepetitionForMedianAndMAD(t *testing.T) {
	profile := loadBP06SuiteTestProfile(t)
	manifest, runs := suiteTestRuns(profile)
	for index := range runs {
		bias := index
		for target, entry := range runs[index].Manifest.Targets {
			entry = suiteTestTarget(profile, target, true, bias)
			runs[index].Manifest.Targets[target] = entry
		}
	}
	summary := buildBP06SuiteSummary(bp06SuiteOptions{Profile: profile, Repetitions: suiteartifact.MinimumRepetitions}, manifest, runs)
	if !summary.Valid {
		t.Fatalf("suite invalid: %s", summary.FailureReason)
	}
	for target, targetSummary := range summary.Targets {
		metric := targetSummary.Metrics["round01_rto_millis"]
		if metric.SampleCount != suiteartifact.MinimumRepetitions || metric.Median != 12 || metric.MedianAbsoluteDeviation != 1 {
			t.Errorf("%s RTO median/MAD/sample count = %v/%v/%d, want 12/1/5", target, metric.Median, metric.MedianAbsoluteDeviation, metric.SampleCount)
		}
		for index, sample := range metric.Samples {
			if sample.Repetition != index+1 || sample.Value != float64(10+index) {
				t.Errorf("%s sample %d = %#v, want repetition %d value %d", target, index, sample, index+1, 10+index)
			}
		}
	}
}
func loadBP06SuiteTestProfile(t *testing.T) Profile {
	t.Helper()
	profile, _, _, err := LoadProfile("testdata/smoke.json")
	if err != nil {
		t.Fatal(err)
	}
	return profile
}

func suiteTestRuns(profile Profile) (bp06SuiteManifest, []bp06SuiteRunData) {
	manifest := bp06SuiteManifest{
		SuiteID:              "bp06-suite-test",
		RequestedRepetitions: suiteartifact.MinimumRepetitions,
		CompletedRepetitions: suiteartifact.MinimumRepetitions,
	}
	runs := make([]bp06SuiteRunData, 0, suiteartifact.MinimumRepetitions)
	for repetition := 1; repetition <= suiteartifact.MinimumRepetitions; repetition++ {
		runID := fmt.Sprintf("run-%02d", repetition)
		narumq := suiteTestTarget(profile, "narumq", true, 0)
		emqx := suiteTestTarget(profile, "emqx", true, 0)
		child := bp06Manifest{Targets: map[string]bp06TargetManifest{"narumq": narumq, "emqx": emqx}}
		runs = append(runs, bp06SuiteRunData{Repetition: repetition, RunID: runID, Manifest: child, Success: true})
		manifest.Runs = append(manifest.Runs, bp06SuiteRun{Repetition: repetition, RunID: runID, Success: true})
	}
	return manifest, runs
}

func suiteTestTarget(profile Profile, target string, networkAvailable bool, bias int) bp06TargetManifest {
	workload := &workloadSummary{
		Target: target,
		Valid:  true,
		Rounds: make([]roundSummary, profile.rounds()),
		Streams: []streamSummary{
			{Stream: "v5-q0", QoS: 0, QoS0Lost: uint64(8 + bias)},
			{Stream: "v5-q1", QoS: 1},
		},
		Totals: totalsSummary{QoS1Duplicates: uint64(7 + bias)},
	}
	resources := &resourceSummary{Valid: true, Rounds: make([]resourceRoundSummary, profile.rounds())}
	resources.Network.NetworkStatsAvailable = networkAvailable
	failure := &failureSummary{Target: target, Valid: true, Rounds: make([]failureRoundSummary, profile.rounds())}
	for round := 1; round <= profile.rounds(); round++ {
		workload.Rounds[round-1] = roundSummary{
			Round:      round,
			Reconnects: uint64(round + bias),
			Recovery:   recoverySummary{Recovered: true, RTOMillis: float64(10*round + bias), ReconnectMaxMillis: float64(30*round + bias)},
		}
		failure.Rounds[round-1] = failureRoundSummary{
			Round:             round,
			RejoinReadyMillis: float64(20*round + bias),
			ResyncMillis:      float64(25*round + bias),
		}
		resources.Rounds[round-1] = resourceRoundSummary{Round: round, CPUPeakPercent: float64(40*round + bias), MemoryPeakBytes: int64(1024*round + bias)}
	}
	return bp06TargetManifest{
		Target:          target,
		Success:         true,
		Valid:           true,
		ResolvedProfile: profile,
		WorkloadSummary: workload,
		ResourceSummary: resources,
		FailureSummary:  failure,
	}
}
