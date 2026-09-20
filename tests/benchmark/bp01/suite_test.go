package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"narumq/tests/benchmark/internal/runtimeartifact"
)

func TestSuiteCLIRejectsInvalidInvocationBeforeArtifacts(t *testing.T) {
	cases := []struct {
		name string
		args func(outputRoot string) []string
	}{
		{
			name: "empty profile",
			args: func(outputRoot string) []string {
				return []string{"suite", "--profile", "", "--output", outputRoot}
			},
		},
		{
			name: "too few repetitions",
			args: func(outputRoot string) []string {
				return []string{"suite", "--profile", filepath.Join(t.TempDir(), "missing.json"), "--output", outputRoot, "--repetitions", "4"}
			},
		},
		{
			name: "unexpected positional argument",
			args: func(outputRoot string) []string {
				return []string{"suite", "--profile", filepath.Join(t.TempDir(), "missing.json"), "--output", outputRoot, "extra"}
			},
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			outputRoot := filepath.Join(t.TempDir(), "artifacts")
			if err := runCLI(testCase.args(outputRoot), discardWriter{}, discardWriter{}); err == nil {
				t.Fatal("runCLI() accepted invalid suite invocation")
			}
			if _, err := os.Stat(outputRoot); !os.IsNotExist(err) {
				t.Fatalf("suite created output root: stat error = %v", err)
			}
		})
	}
}

func TestExecuteSuiteRunsFiveAlternatingValidRepetitions(t *testing.T) {
	options := testBP01SuiteOptions(t)
	var calls []comparisonRunOptions
	_, summary, err := executeSuite(context.Background(), options, func(_ context.Context, runOptions comparisonRunOptions) (manifest, comparison, error) {
		calls = append(calls, runOptions)
		return validBP01SuiteChild(runOptions.Profile, runOptions.RunID, len(calls), true), validBP01SuiteComparison(runOptions.Profile, runOptions.RunID, len(calls), true), nil
	})
	if err != nil {
		t.Fatalf("executeSuite() error = %v", err)
	}
	if len(calls) != 5 {
		t.Fatalf("comparison calls = %d, want 5", len(calls))
	}
	for index, call := range calls {
		wantOrder := []string{"narumq", "emqx"}
		if index%2 == 1 {
			wantOrder = []string{"emqx", "narumq"}
		}
		if !reflect.DeepEqual(call.TargetOrder, wantOrder) {
			t.Fatalf("repetition %d target order = %#v, want %#v", index+1, call.TargetOrder, wantOrder)
		}
		if _, err := os.Stat(filepath.Join(options.SuiteDir, "runs", formatRepetition(index+1))); err != nil {
			t.Fatalf("repetition %d artifact directory: %v", index+1, err)
		}
	}
	for index, run := range calls {
		wantRunID := options.SuiteID + "-r00" + string(rune('1'+index))
		if run.RunID != wantRunID {
			t.Fatalf("run %d ID = %q, want %q", index+1, run.RunID, wantRunID)
		}
	}
	if !summary.Valid || summary.SuccessfulRepetitions != 5 || summary.Targets["emqx"].Metrics["publishRatePerSecond"].Median != 3 {
		t.Fatalf("suite summary = %#v, want valid five-run median 3", summary)
	}
	publish := summary.Targets["narumq"].Metrics["publishRatePerSecond"]
	if publish.MedianAbsoluteDeviation != 1 || publish.SampleCount != 5 {
		t.Fatalf("publish metric = %#v, want median 3 MAD 1 and five samples", publish)
	}
	p999 := summary.Targets["narumq"].Metrics["latencyP999Nanoseconds"]
	if p999.Median != 300 || p999.MedianAbsoluteDeviation != 100 {
		t.Fatalf("p999 metric = %#v, want median 300 MAD 100", p999)
	}
}

func TestExecuteSuiteContinuesAfterFailedRepetitionAndInvalidatesSummary(t *testing.T) {
	options := testBP01SuiteOptions(t)
	calls := 0
	value, summary, err := executeSuite(context.Background(), options, func(_ context.Context, runOptions comparisonRunOptions) (manifest, comparison, error) {
		calls++
		child := validBP01SuiteChild(runOptions.Profile, runOptions.RunID, calls, true)
		childComparison := validBP01SuiteComparison(runOptions.Profile, runOptions.RunID, calls, true)
		if calls == 2 {
			child.Targets["emqx"] = targetManifest{Success: false, FailureReason: "emqx failed"}
			childComparison = buildComparison(child)
			return child, childComparison, errors.New("target execution failed")
		}
		return child, childComparison, nil
	})
	if err == nil {
		t.Fatal("executeSuite() accepted failed repetition")
	}
	if calls != 5 || value.CompletedRepetitions != 5 || len(value.Runs) != 5 {
		t.Fatalf("suite execution calls=%d completed=%d runs=%d, want five each", calls, value.CompletedRepetitions, len(value.Runs))
	}
	if summary.Valid || summary.SuccessfulRepetitions != 4 || len(summary.Targets["narumq"].Metrics) != 0 || len(summary.Targets["emqx"].Metrics) != 0 {
		t.Fatalf("invalid suite summary = %#v", summary)
	}
	if value.Runs[1].Success || value.Runs[1].Equivalent || value.Runs[1].FailureReason == "" {
		t.Fatalf("failed repetition record = %#v", value.Runs[1])
	}
}

func TestExecuteSuiteStopsBeforeNextRepetitionWhenContextCanceled(t *testing.T) {
	options := testBP01SuiteOptions(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	value, summary, err := executeSuite(ctx, options, func(_ context.Context, runOptions comparisonRunOptions) (manifest, comparison, error) {
		calls++
		cancel()
		return validBP01SuiteChild(runOptions.Profile, runOptions.RunID, calls, true), validBP01SuiteComparison(runOptions.Profile, runOptions.RunID, calls, true), nil
	})
	if err == nil {
		t.Fatal("executeSuite() accepted canceled context")
	}
	if calls != 1 || value.CompletedRepetitions != 1 || len(value.Runs) != 1 || summary.Valid {
		t.Fatalf("canceled suite calls=%d completed=%d runs=%d summary=%#v", calls, value.CompletedRepetitions, len(value.Runs), summary)
	}
}

func TestExecuteSuitePreservesValidSuiteWhenRuntimeCPUIsUnavailable(t *testing.T) {
	options := testBP01SuiteOptions(t)
	_, summary, err := executeSuite(context.Background(), options, func(_ context.Context, runOptions comparisonRunOptions) (manifest, comparison, error) {
		repetition := int(runOptions.RunID[len(runOptions.RunID)-1] - '0')
		return validBP01SuiteChild(runOptions.Profile, runOptions.RunID, repetition, false), validBP01SuiteComparison(runOptions.Profile, runOptions.RunID, repetition, false), nil
	})
	if err != nil || !summary.Valid {
		t.Fatalf("CPU-unavailable suite error=%v summary=%#v", err, summary)
	}
	target := summary.Targets["narumq"]
	if target.CPUMetricsAvailable == nil || *target.CPUMetricsAvailable || len(target.CPUMetricsReasons) != 5 {
		t.Fatalf("CPU availability = %#v reasons=%#v, want false and five reasons", target.CPUMetricsAvailable, target.CPUMetricsReasons)
	}
	for _, key := range []string{"gcCPUSeconds", "runtimeCPUSeconds", "gcCPUFraction"} {
		if _, exists := target.Metrics[key]; exists {
			t.Fatalf("CPU metric %q was aggregated despite unavailable CPU metrics", key)
		}
	}
}

func TestExecuteSuiteRejectsMissingMetricSource(t *testing.T) {
	options := testBP01SuiteOptions(t)
	_, summary, err := executeSuite(context.Background(), options, func(_ context.Context, runOptions comparisonRunOptions) (manifest, comparison, error) {
		child := validBP01SuiteChild(runOptions.Profile, runOptions.RunID, 1, true)
		if runOptions.RunID[len(runOptions.RunID)-1] == '3' {
			target := child.Targets["narumq"]
			target.RuntimeSummary = nil
			child.Targets["narumq"] = target
		}
		return child, comparison{Equivalent: true, RunID: runOptions.RunID}, nil
	})
	if err == nil || summary.Valid || len(summary.Targets["narumq"].Metrics) != 0 {
		t.Fatalf("missing metric source error=%v summary=%#v", err, summary)
	}
}

func TestExecuteSuiteRecordsSummaryWriteFailureInManifest(t *testing.T) {
	options := testBP01SuiteOptions(t)
	if err := os.Mkdir(filepath.Join(options.SuiteDir, "suite-summary.json"), 0o755); err != nil {
		t.Fatal(err)
	}
	value, summary, err := executeSuite(context.Background(), options, func(_ context.Context, runOptions comparisonRunOptions) (manifest, comparison, error) {
		return validBP01SuiteChild(runOptions.Profile, runOptions.RunID, 1, true), validBP01SuiteComparison(runOptions.Profile, runOptions.RunID, 1, true), nil
	})
	if err == nil || value.Valid || summary.Valid || !strings.Contains(value.FailureReason, "write suite summary") {
		t.Fatalf("summary write failure value=%#v summary=%#v error=%v", value, summary, err)
	}
	data, readErr := os.ReadFile(filepath.Join(options.SuiteDir, "suite-manifest.json"))
	if readErr != nil {
		t.Fatal(readErr)
	}
	var persisted suiteManifest
	if err := json.Unmarshal(data, &persisted); err != nil {
		t.Fatal(err)
	}
	if persisted.Valid || !strings.Contains(persisted.FailureReason, "write suite summary") {
		t.Fatalf("persisted suite manifest = %#v", persisted)
	}
}

func testBP01SuiteOptions(t *testing.T) suiteExecutionOptions {
	t.Helper()
	return suiteExecutionOptions{
		Profile:       validProfile(),
		ProfileBytes:  []byte(`{"profile":"test"}`),
		ProfileSHA256: "sha",
		RepoRoot:      t.TempDir(),
		SuiteID:       "bp01-suite-test",
		SuiteDir:      t.TempDir(),
		Repetitions:   5,
		Command:       []string{"suite", "--profile", "profile.json", "--repetitions", "5"},
	}
}

func validBP01SuiteChild(profile Profile, runID string, repetition int, cpuAvailable bool) manifest {
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
	latency := int64(repetition * 100)
	runtimeSummary := &runtimeartifact.Summary{
		Valid:                    true,
		Applicability:            "required",
		CPUMetricsAvailable:      cpuAvailable,
		CPUAvailabilityReason:    "GC did not run in the measurement phase",
		AllocationBytesPerSecond: float64(repetition),
		AllocationsPerSecond:     float64(repetition),
		GCPauseP50Seconds:        float64(repetition) / 1000,
		GCPauseP95Seconds:        float64(repetition) / 1000,
		GCPauseP99Seconds:        float64(repetition) / 1000,
		GCPauseP999Seconds:       float64(repetition) / 1000,
		GCCPUSeconds:             float64(repetition) / 10,
		RuntimeCPUSeconds:        float64(repetition) / 5,
		GCCPUFraction:            0.1,
	}
	validSummary := &Summary{
		Valid:                     true,
		PublishRatePerSecond:      float64(repetition),
		DeliveryRatePerSecond:     float64(repetition),
		LatencyNanoseconds:        Percentiles{P50Nanoseconds: latency, P95Nanoseconds: latency, P99Nanoseconds: latency, P999Nanoseconds: latency},
		MaxScheduleLagNanoseconds: latency,
	}
	return manifest{
		RunID:   runID,
		Profile: artifactProfile{RawSHA256: "sha", Resolved: profile},
		Targets: map[string]targetManifest{
			"narumq": {Success: true, ProfileSHA256: "sha", ResolvedProfile: profile, RunnerImageID: "runner", NetworkMode: "none", ResourceLimits: limits, Summary: validSummary, RuntimeSummary: runtimeSummary},
			"emqx": {Success: true, ProfileSHA256: "sha", ResolvedProfile: profile, RunnerImageID: "runner", NetworkMode: "none", ResourceLimits: limits, Summary: validSummary, RuntimeSummary: func() *runtimeartifact.Summary {
				value := runtimeartifact.NotApplicable("emqx", notApplicableRuntimeReason)
				return &value
			}()},
		},
	}
}

func validBP01SuiteComparison(profile Profile, runID string, repetition int, cpuAvailable bool) comparison {
	return buildComparison(validBP01SuiteChild(profile, runID, repetition, cpuAvailable))
}

func formatRepetition(repetition int) string {
	return "00" + string(rune('0'+repetition))
}
