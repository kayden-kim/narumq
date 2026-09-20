package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
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
			if err := runCLI(testCase.args(outputRoot), discardBP05Writer{}, discardBP05Writer{}); err == nil {
				t.Fatal("runCLI() accepted invalid suite invocation")
			}
			if _, err := os.Stat(outputRoot); !os.IsNotExist(err) {
				t.Fatalf("suite created output root: stat error = %v", err)
			}
		})
	}
}

func TestExecuteSuiteRunsFiveAlternatingValidRepetitions(t *testing.T) {
	options := testBP05SuiteOptions(t)
	var calls []comparisonRunOptions
	_, summary, err := executeSuite(context.Background(), options, func(_ context.Context, runOptions comparisonRunOptions) (manifest, comparison, error) {
		calls = append(calls, runOptions)
		repetition := len(calls)
		return validBP05SuiteChild(runOptions.Profile, runOptions.RunID, repetition, true), validBP05SuiteComparison(runOptions.Profile, runOptions.RunID, repetition, true), nil
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
		wantRunID := options.SuiteID + "-r00" + string(rune('1'+index))
		if call.RunID != wantRunID {
			t.Fatalf("run %d ID = %q, want %q", index+1, call.RunID, wantRunID)
		}
		if _, err := os.Stat(filepath.Join(options.SuiteDir, "runs", formatBP05Repetition(index+1))); err != nil {
			t.Fatalf("repetition %d artifact directory: %v", index+1, err)
		}
	}
	if !summary.Valid || summary.SuccessfulRepetitions != 5 {
		t.Fatalf("suite summary = %#v, want valid five-run summary", summary)
	}
	reconnect := summary.Targets["narumq"].Metrics["reconnectRatePerSecond"]
	if reconnect.Median != 3 || reconnect.MedianAbsoluteDeviation != 1 || reconnect.SampleCount != 5 {
		t.Fatalf("reconnect metric = %#v, want median 3 MAD 1 and five samples", reconnect)
	}
	memory := summary.Targets["emqx"].Metrics["memoryBytesPerIdleConnection"]
	if memory.Median != 300 || memory.MedianAbsoluteDeviation != 100 {
		t.Fatalf("memory metric = %#v, want median 300 MAD 100", memory)
	}
	if summary.Targets["emqx"].RuntimeApplicability != "not_applicable" || summary.Targets["emqx"].CPUMetricsAvailable != nil {
		t.Fatalf("EMQX runtime summary = %#v, want not-applicable without CPU field", summary.Targets["emqx"])
	}
}

func TestExecuteSuiteContinuesAfterFailedRepetitionAndInvalidatesSummary(t *testing.T) {
	options := testBP05SuiteOptions(t)
	calls := 0
	value, summary, err := executeSuite(context.Background(), options, func(_ context.Context, runOptions comparisonRunOptions) (manifest, comparison, error) {
		calls++
		child := validBP05SuiteChild(runOptions.Profile, runOptions.RunID, calls, true)
		childComparison := validBP05SuiteComparison(runOptions.Profile, runOptions.RunID, calls, true)
		if calls == 3 {
			child.Targets["narumq"] = targetManifest{Success: false, FailureReason: "narumq failed"}
			childComparison = buildComparison(child)
			return child, childComparison, errors.New("target execution failed")
		}
		return child, childComparison, nil
	})
	if err == nil {
		t.Fatal("executeSuite() accepted failed repetition")
	}
	if calls != 5 || value.CompletedRepetitions != 5 || len(value.Runs) != 5 || summary.Valid || summary.SuccessfulRepetitions != 4 {
		t.Fatalf("suite calls=%d completed=%d runs=%d summary=%#v", calls, value.CompletedRepetitions, len(value.Runs), summary)
	}
	if len(summary.Targets["narumq"].Metrics) != 0 || len(summary.Targets["emqx"].Metrics) != 0 {
		t.Fatalf("failed suite retained aggregate metrics: %#v", summary.Targets)
	}
}

func TestExecuteSuiteStopsBeforeNextRepetitionWhenContextCanceled(t *testing.T) {
	options := testBP05SuiteOptions(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	value, summary, err := executeSuite(ctx, options, func(_ context.Context, runOptions comparisonRunOptions) (manifest, comparison, error) {
		calls++
		cancel()
		return validBP05SuiteChild(runOptions.Profile, runOptions.RunID, calls, true), validBP05SuiteComparison(runOptions.Profile, runOptions.RunID, calls, true), nil
	})
	if err == nil {
		t.Fatal("executeSuite() accepted canceled context")
	}
	if calls != 1 || value.CompletedRepetitions != 1 || len(value.Runs) != 1 || summary.Valid {
		t.Fatalf("canceled suite calls=%d completed=%d runs=%d summary=%#v", calls, value.CompletedRepetitions, len(value.Runs), summary)
	}
}

func TestExecuteSuitePreservesValidSuiteWhenRuntimeCPUIsUnavailable(t *testing.T) {
	options := testBP05SuiteOptions(t)
	_, summary, err := executeSuite(context.Background(), options, func(_ context.Context, runOptions comparisonRunOptions) (manifest, comparison, error) {
		repetition := int(runOptions.RunID[len(runOptions.RunID)-1] - '0')
		return validBP05SuiteChild(runOptions.Profile, runOptions.RunID, repetition, false), validBP05SuiteComparison(runOptions.Profile, runOptions.RunID, repetition, false), nil
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

func TestExecuteSuiteRejectsRuntimeApplicabilityMismatch(t *testing.T) {
	options := testBP05SuiteOptions(t)
	_, summary, err := executeSuite(context.Background(), options, func(_ context.Context, runOptions comparisonRunOptions) (manifest, comparison, error) {
		child := validBP05SuiteChild(runOptions.Profile, runOptions.RunID, 1, true)
		if runOptions.RunID[len(runOptions.RunID)-1] == '4' {
			value := runtimeartifact.Summary{Valid: true, Applicability: "required"}
			target := child.Targets["emqx"]
			target.RuntimeSummary = &value
			child.Targets["emqx"] = target
		}
		return child, comparison{Equivalent: true, RunID: runOptions.RunID}, nil
	})
	if err == nil || summary.Valid || len(summary.Targets["emqx"].Metrics) != 0 {
		t.Fatalf("runtime applicability mismatch error=%v summary=%#v", err, summary)
	}
}

func testBP05SuiteOptions(t *testing.T) suiteExecutionOptions {
	t.Helper()
	return suiteExecutionOptions{
		Profile:       validBP05Profile(),
		ProfileBytes:  []byte(`{"profile":"test"}`),
		ProfileSHA256: "sha",
		RepoRoot:      t.TempDir(),
		SuiteID:       "bp05-suite-test",
		SuiteDir:      t.TempDir(),
		Repetitions:   5,
		Command:       []string{"suite", "--profile", "profile.json", "--repetitions", "5"},
	}
}

func validBP05SuiteChild(profile Profile, runID string, repetition int, cpuAvailable bool) manifest {
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
	workload := &WorkloadSummary{
		Valid:                          true,
		InitialConnectionRatePerSecond: float64(repetition),
		InitialConnectLatency:          Percentiles{P50Nanoseconds: latency, P95Nanoseconds: latency, P99Nanoseconds: latency, P999Nanoseconds: latency},
		ReconnectRatePerSecond:         float64(repetition),
		ReconnectLatency:               Percentiles{P50Nanoseconds: latency, P95Nanoseconds: latency, P99Nanoseconds: latency, P999Nanoseconds: latency},
		MaxScheduleLagNanoseconds:      latency,
	}
	resource := &ResourceSummary{Valid: true, MemoryBytesPerIdleConnection: float64(repetition * 100)}
	return manifest{
		RunID:   runID,
		Profile: artifactProfile{RawSHA256: "sha", Resolved: profile},
		Targets: map[string]targetManifest{
			"narumq": {Success: true, ProfileSHA256: "sha", ResolvedProfile: profile, RunnerImageID: "runner", NetworkMode: "none", ResourceLimits: limits, WorkloadSummary: workload, ResourceSummary: resource, RuntimeSummary: runtimeSummary},
			"emqx": {Success: true, ProfileSHA256: "sha", ResolvedProfile: profile, RunnerImageID: "runner", NetworkMode: "none", ResourceLimits: limits, WorkloadSummary: workload, ResourceSummary: resource, RuntimeSummary: func() *runtimeartifact.Summary {
				value := runtimeartifact.NotApplicable("emqx", notApplicableRuntimeReason)
				return &value
			}()},
		},
	}
}

func validBP05SuiteComparison(profile Profile, runID string, repetition int, cpuAvailable bool) comparison {
	return buildComparison(validBP05SuiteChild(profile, runID, repetition, cpuAvailable))
}

func formatBP05Repetition(repetition int) string {
	return "00" + string(rune('0'+repetition))
}
