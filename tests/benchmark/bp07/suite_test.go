package main

import (
	"fmt"
	"testing"

	"narumq/tests/benchmark/internal/suiteartifact"
)

func TestBP07MetricsEmitExactKeysAndUnits(t *testing.T) {
	profile := loadBP07SuiteTestProfile(t)
	wantUnits := make(map[string]string, profile.rounds()*3+3)
	for round := 1; round <= profile.rounds(); round++ {
		prefix := fmt.Sprintf("round%02d_", round)
		wantUnits[prefix+"drain_millis"] = "ms"
		wantUnits[prefix+"restart_ready_millis"] = "ms"
		wantUnits[prefix+"reconnect_latency_max_millis"] = "ms"
	}
	wantUnits["reconnects_total"] = "connections"
	wantUnits["qos1_duplicates_total"] = "messages"
	wantUnits["qos0_lost_total"] = "messages"
	gotUnits := bp07SuiteMetricUnits(profile)
	if len(gotUnits) != len(wantUnits) {
		t.Fatalf("unit key count = %d, want %d", len(gotUnits), len(wantUnits))
	}
	for key, unit := range wantUnits {
		if gotUnits[key] != unit {
			t.Errorf("metric %q unit = %q, want %q", key, gotUnits[key], unit)
		}
	}
	for _, target := range []string{"narumq", "emqx"} {
		metrics, err := bp07Metrics(target, suiteTestTarget(profile, target, true, 0))
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

func TestBP07MetricsRejectDuplicateOrMissingRounds(t *testing.T) {
	profile := loadBP07SuiteTestProfile(t)
	t.Run("duplicate rolling round", func(t *testing.T) {
		target := suiteTestTarget(profile, "narumq", true, 0)
		rolling := *target.RollingSummary
		rolling.Rounds = append([]rollingRoundSummary(nil), rolling.Rounds...)
		rolling.Rounds[1].Round = rolling.Rounds[0].Round
		target.RollingSummary = &rolling
		if _, err := bp07Metrics("narumq", target); err == nil {
			t.Fatal("duplicate rolling round was accepted")
		}
	})
	t.Run("missing workload round", func(t *testing.T) {
		target := suiteTestTarget(profile, "emqx", true, 0)
		workload := *target.WorkloadSummary
		workload.Rounds = append([]roundSummary(nil), workload.Rounds[:len(workload.Rounds)-1]...)
		target.WorkloadSummary = &workload
		if _, err := bp07Metrics("emqx", target); err == nil {
			t.Fatal("missing workload round was accepted")
		}
	})
}

func TestBuildBP07SuiteSummaryKeepsMetricsWithoutNetworkStats(t *testing.T) {
	profile := loadBP07SuiteTestProfile(t)
	manifest, runs := suiteTestRuns(profile)
	last := len(runs) - 1
	for target, entry := range runs[last].Manifest.Targets {
		resources := *entry.ResourceSummary
		resources.NetworkStatsAvailable = false
		entry.ResourceSummary = &resources
		runs[last].Manifest.Targets[target] = entry
	}
	summary := buildBP07SuiteSummary(bp07SuiteOptions{Profile: profile, Repetitions: suiteartifact.MinimumRepetitions}, manifest, runs)
	if !summary.Valid {
		t.Fatalf("network-unavailable suite became invalid: %s", summary.FailureReason)
	}
	for target, targetSummary := range summary.Targets {
		if targetSummary.NetworkStatsAvailable {
			t.Errorf("%s marked network stats available", target)
		}
		if len(targetSummary.Metrics) != len(bp07SuiteMetricUnits(profile)) {
			t.Errorf("%s metric count = %d, want %d", target, len(targetSummary.Metrics), len(bp07SuiteMetricUnits(profile)))
		}
	}
}

func TestBuildBP07SuiteSummaryInvalidatesAllMetricsOnOneFailedRun(t *testing.T) {
	profile := loadBP07SuiteTestProfile(t)
	manifest, runs := suiteTestRuns(profile)
	runs[2].Success = false
	runs[2].Manifest.FailureReason = "target failed"
	summary := buildBP07SuiteSummary(bp07SuiteOptions{Profile: profile, Repetitions: suiteartifact.MinimumRepetitions}, manifest, runs)
	if summary.Valid || summary.SuccessfulRepetitions != suiteartifact.MinimumRepetitions-1 {
		t.Fatalf("invalid suite status = valid:%t successful:%d", summary.Valid, summary.SuccessfulRepetitions)
	}
	for target, targetSummary := range summary.Targets {
		if len(targetSummary.Metrics) != 0 {
			t.Errorf("%s retained %d parent metrics after a failed repetition", target, len(targetSummary.Metrics))
		}
	}
}

func TestBuildBP07SuiteSummaryUsesEveryRepetitionForMedianAndMAD(t *testing.T) {
	profile := loadBP07SuiteTestProfile(t)
	manifest, runs := suiteTestRuns(profile)
	for index := range runs {
		bias := index
		for target, entry := range runs[index].Manifest.Targets {
			entry = suiteTestTarget(profile, target, true, bias)
			runs[index].Manifest.Targets[target] = entry
		}
	}
	summary := buildBP07SuiteSummary(bp07SuiteOptions{Profile: profile, Repetitions: suiteartifact.MinimumRepetitions}, manifest, runs)
	if !summary.Valid {
		t.Fatalf("suite invalid: %s", summary.FailureReason)
	}
	for target, targetSummary := range summary.Targets {
		metric := targetSummary.Metrics["round01_drain_millis"]
		if metric.SampleCount != suiteartifact.MinimumRepetitions || metric.Median != 12 || metric.MedianAbsoluteDeviation != 1 {
			t.Errorf("%s drain median/MAD/sample count = %v/%v/%d, want 12/1/5", target, metric.Median, metric.MedianAbsoluteDeviation, metric.SampleCount)
		}
		for index, sample := range metric.Samples {
			if sample.Repetition != index+1 || sample.Value != float64(10+index) {
				t.Errorf("%s sample %d = %#v, want repetition %d value %d", target, index, sample, index+1, 10+index)
			}
		}
	}
}
func loadBP07SuiteTestProfile(t *testing.T) Profile {
	t.Helper()
	profile, _, _, err := LoadProfile("testdata/smoke.json")
	if err != nil {
		t.Fatal(err)
	}
	return profile
}

func suiteTestRuns(profile Profile) (bp07SuiteManifest, []bp07SuiteRunData) {
	manifest := bp07SuiteManifest{
		SuiteID:              "bp07-suite-test",
		RequestedRepetitions: suiteartifact.MinimumRepetitions,
		CompletedRepetitions: suiteartifact.MinimumRepetitions,
	}
	runs := make([]bp07SuiteRunData, 0, suiteartifact.MinimumRepetitions)
	for repetition := 1; repetition <= suiteartifact.MinimumRepetitions; repetition++ {
		runID := fmt.Sprintf("run-%02d", repetition)
		narumq := suiteTestTarget(profile, "narumq", true, 0)
		emqx := suiteTestTarget(profile, "emqx", true, 0)
		child := bp07Manifest{Targets: map[string]bp07TargetManifest{"narumq": narumq, "emqx": emqx}}
		runs = append(runs, bp07SuiteRunData{Repetition: repetition, RunID: runID, Manifest: child, Success: true})
		manifest.Runs = append(manifest.Runs, bp07SuiteRun{Repetition: repetition, RunID: runID, Success: true})
	}
	return manifest, runs
}

func suiteTestTarget(profile Profile, target string, networkAvailable bool, bias int) bp07TargetManifest {
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
	resources := &resourceSummary{Valid: true, NetworkStatsAvailable: networkAvailable}
	rolling := &rollingSummary{Target: target, Valid: true, Rounds: make([]rollingRoundSummary, profile.rounds())}
	for round := 1; round <= profile.rounds(); round++ {
		workload.Rounds[round-1] = roundSummary{
			Round:                  round,
			Reconnects:             uint64(round + bias),
			ReconnectLatencyMillis: latencySummary{Max: float64(30*round + bias)},
		}
		rolling.Rounds[round-1] = rollingRoundSummary{
			Round:              round,
			DrainMillis:        float64(10*round + bias),
			RestartReadyMillis: float64(20*round + bias),
		}
	}
	return bp07TargetManifest{
		Target:          target,
		Success:         true,
		Valid:           true,
		ResolvedProfile: profile,
		WorkloadSummary: workload,
		ResourceSummary: resources,
		RollingSummary:  rolling,
	}
}
