package main

import (
	"fmt"
	"testing"

	"narumq/tests/benchmark/internal/suiteartifact"
)

// TestBuildBP04ComparisonRequiresMatchingEvidence verifies comparison requires matching evidence.
// Scenarios: performance-and-verification.md:PRF-SCN-039
func TestBuildBP04ComparisonRequiresMatchingEvidence(t *testing.T) {
	profile := smokeProfile(t)
	limits := bp04ResourceLimits{BrokerCPUs: profile.BrokerCPUs, BrokerMemory: profile.BrokerMemoryBytes, GeneratorCPUs: profile.GeneratorCPUs, GeneratorMemory: profile.GeneratorMemoryBytes, NetworkMode: "bridge"}
	manifest := bp04Manifest{
		RunID:         "bp04-test",
		Profile:       bp04ArtifactProfile{RawSHA256: "profile-sha", Resolved: profile},
		RunnerImageID: "runner-image-id",
		Topology:      bp04Topology{Nodes: bp04NodeCount},
		Targets: map[string]bp04TargetManifest{
			"narumq": {Target: "narumq", ProfileSHA256: "profile-sha", ResourceLimits: limits, RunnerImageID: "runner-image-id", Nodes: testBP04Nodes()},
			"emqx":   {Target: "emqx", ProfileSHA256: "profile-sha", ResourceLimits: limits, RunnerImageID: "runner-image-id", Nodes: testBP04Nodes()},
		},
	}
	summary := bp04Summary{Targets: map[string]bp04TargetSummary{"narumq": {Valid: true}, "emqx": {Valid: true}}}
	if comparison := buildBP04Comparison(manifest, summary); !comparison.Equivalent {
		t.Fatalf("comparison equivalent = false, reason %q", comparison.Reason)
	}

	mutations := []struct {
		name   string
		change func(*bp04Manifest)
	}{
		{name: "profile hash", change: func(value *bp04Manifest) {
			target := value.Targets["emqx"]
			target.ProfileSHA256 = "different"
			value.Targets["emqx"] = target
		}},
		{name: "resource limit", change: func(value *bp04Manifest) {
			target := value.Targets["emqx"]
			target.ResourceLimits.BrokerMemory++
			value.Targets["emqx"] = target
		}},
		{name: "runner image ID", change: func(value *bp04Manifest) {
			target := value.Targets["emqx"]
			target.RunnerImageID = "different"
			value.Targets["emqx"] = target
		}},
		{name: "topology node count", change: func(value *bp04Manifest) {
			target := value.Targets["emqx"]
			target.Nodes = target.Nodes[:2]
			value.Targets["emqx"] = target
		}},
	}
	for _, test := range mutations {
		t.Run(test.name, func(t *testing.T) {
			changed := manifest
			changed.Targets = make(map[string]bp04TargetManifest, len(manifest.Targets))
			for name, target := range manifest.Targets {
				target.Nodes = append([]bp04NodeArtifacts(nil), target.Nodes...)
				changed.Targets[name] = target
			}
			test.change(&changed)
			if comparison := buildBP04Comparison(changed, summary); comparison.Equivalent {
				t.Fatal("comparison remained equivalent after evidence changed")
			}
		})
	}
}

// TestBP04MetricsSelectProfileAndResourceObservations verifies metric selection.
// Scenarios: performance-and-verification.md:PRF-SCN-039
func TestBP04MetricsSelectProfileAndResourceObservations(t *testing.T) {
	profile := smokeProfile(t)
	steps := make([]stepSummary, 0, len(profile.RatesPerStreamPerSecond))
	windows := make([]resourceWindow, 0, len(profile.RatesPerStreamPerSecond))
	for index := range profile.RatesPerStreamPerSecond {
		stepNumber := index + 1
		classes := make(map[string]classSummary, 4)
		for _, class := range []string{classFanout, classWildcard, classOverlap, classShared} {
			classes[class] = classSummary{DeliveryLatency: latencySummary{P99Nanoseconds: int64(stepNumber * 100), P999Nanoseconds: int64(stepNumber * 200)}}
		}
		steps = append(steps, stepSummary{Step: stepNumber, DeliveredPerSecond: float64(stepNumber * 10), Classes: classes})
		windows = append(windows, resourceWindow{Window: fmt.Sprintf("step-%02d-measure", stepNumber), Deliveries: 20, Cluster: resourceMeasurements{MedianCPUPercent: float64(stepNumber * 5), NetworkTxBytes: int64(stepNumber * 400)}})
	}
	entry := bp04TargetManifest{
		Success: true,
		Valid:   true,
		WorkloadSummary: &workloadSummary{
			Valid:            true,
			Steps:            steps,
			Setup:            setupSummary{SubscribeAckLatency: latencySummary{P99Nanoseconds: 13}},
			RoutePropagation: routePropagationSummary{RouteVisibleLatency: latencySummary{P50Nanoseconds: 11, P99Nanoseconds: 22, MaxNanoseconds: 33}},
		},
		ResourceSummary: &resourceSummary{
			Valid:                 true,
			NetworkStatsAvailable: true,
			Windows:               windows,
			Memory:                memorySummary{MemoryBytesPerSetupClient: -20},
		},
	}
	metrics, err := bp04Metrics("narumq", entry)
	if err != nil {
		t.Fatal(err)
	}
	checks := map[string]float64{
		"step01_delivered_per_second":            10,
		"step02_cluster_median_cpu_percent":      10,
		"step02_shared_delivery_latency_p999_ns": 400,
		"step02_network_tx_bytes_per_delivery":   40,
		"route_visible_latency_p50_ns":           11,
		"route_visible_latency_p99_ns":           22,
		"route_visible_latency_max_ns":           33,
		"subscribe_ack_latency_p99_ns":           13,
		"memory_bytes_per_setup_client":          -20,
	}
	for key, want := range checks {
		if got := metrics[key]; got != want {
			t.Errorf("metric %s = %v, want %v", key, got, want)
		}
	}
	if _, exists := bp04SuiteMetricUnits(profile, false)["step01_network_tx_bytes_per_delivery"]; exists {
		t.Fatal("suite metric units include network bytes when network stats are unavailable")
	}
}

// TestBP04SuiteFailureLeavesParentMetricsEmpty verifies a failed suite leaves parent metrics empty.
// Scenarios: performance-and-verification.md:PRF-SCN-039
func TestBP04SuiteFailureLeavesParentMetricsEmpty(t *testing.T) {
	profile := smokeProfile(t)
	options := bp04SuiteOptions{Profile: profile, Repetitions: suiteartifact.MinimumRepetitions}
	manifest := bp04SuiteManifest{SuiteID: "suite-test", RequestedRepetitions: suiteartifact.MinimumRepetitions, CompletedRepetitions: suiteartifact.MinimumRepetitions, FailureReason: "one repetition failed"}
	runs := make([]bp04SuiteRunData, suiteartifact.MinimumRepetitions)
	for index := range runs {
		runs[index] = bp04SuiteRunData{Repetition: index + 1, RunID: fmt.Sprintf("run-%d", index+1), Success: index != 2}
	}
	summary := buildBP04SuiteSummary(options, manifest, runs)
	if summary.Valid || summary.SuccessfulRepetitions != suiteartifact.MinimumRepetitions-1 {
		t.Fatalf("suite validity/success count = %v/%d", summary.Valid, summary.SuccessfulRepetitions)
	}
	for _, target := range []string{"narumq", "emqx"} {
		if summary.Targets[target].NetworkStatsAvailable {
			t.Errorf("failed suite reports %s network stats as available", target)
		}
		if metrics := summary.Targets[target].Metrics; len(metrics) != 0 {
			t.Errorf("failed suite retained %s parent metrics: %v", target, metrics)
		}
	}
}

func testBP04Nodes() []bp04NodeArtifacts {
	return []bp04NodeArtifacts{{Name: "node1"}, {Name: "node2"}, {Name: "node3"}}
}
