package main

import (
	"fmt"
	"reflect"
	"testing"

	"narumq/tests/benchmark/internal/suiteartifact"
)

// TestBuildBP02ComparisonRequiresMatchingEvidence verifies comparison requires matching evidence.
// Scenarios: performance-and-verification.md:PRF-SCN-048
func TestBuildBP02ComparisonRequiresMatchingEvidence(t *testing.T) {
	profile := bp02SmokeProfile(t)
	limits := bp02ResourceLimits{BrokerCPUs: profile.BrokerCPUs, BrokerMemory: profile.BrokerMemoryBytes, GeneratorCPUs: profile.GeneratorCPUs, GeneratorMemory: profile.GeneratorMemoryBytes, NetworkMode: "bridge"}
	nodes := []bp02NodeArtifacts{{Name: "node1"}, {Name: "node2"}, {Name: "node3"}}
	manifest := bp02Manifest{RunID: "bp02-test", Profile: bp02ArtifactProfile{RawSHA256: "profile-sha", Resolved: profile}, RunnerImageID: "runner-image", Topology: bp02Topology{Nodes: 3}, Targets: map[string]bp02TargetManifest{
		"narumq": {Target: "narumq", ProfileSHA256: "profile-sha", ResourceLimits: limits, RunnerImageID: "runner-image", Nodes: nodes},
		"emqx":   {Target: "emqx", ProfileSHA256: "profile-sha", ResourceLimits: limits, RunnerImageID: "runner-image", Nodes: nodes},
	}}
	summary := bp02Summary{Targets: map[string]bp02TargetSummary{"narumq": {Valid: true}, "emqx": {Valid: true}}}
	comparison := buildBP02Comparison(manifest, summary)
	if !comparison.Equivalent {
		t.Fatalf("matching evidence not equivalent: %s", comparison.Reason)
	}
	dimensions := make([]string, 0, 6)
	for _, difference := range comparison.NonEquivalent {
		dimensions = append(dimensions, difference.Dimension)
	}
	if want := []string{"sessionDurability", "tenantIsolation", "authentication", "tlsListener", "sharedSelection", "retainedReplication"}; !reflect.DeepEqual(dimensions, want) {
		t.Fatalf("nonEquivalent=%v, want%v", dimensions, want)
	}
	for _, boundary := range []string{"runner", "profile", "limits", "nodes"} {
		t.Run(boundary, func(t *testing.T) {
			changed := manifest
			changed.Targets = make(map[string]bp02TargetManifest, 2)
			for key, value := range manifest.Targets {
				changed.Targets[key] = value
			}
			target := changed.Targets["emqx"]
			switch boundary {
			case "runner":
				target.RunnerImageID = "other-image"
			case "profile":
				target.ProfileSHA256 = "other-sha"
			case "limits":
				target.ResourceLimits.BrokerMemory++
			case "nodes":
				target.Nodes = target.Nodes[:2]
			}
			changed.Targets["emqx"] = target
			if buildBP02Comparison(changed, summary).Equivalent {
				t.Fatal("different evidence treated as equivalent")
			}
		})
	}
}

func bp02MetricEntry(profile Profile) bp02TargetManifest {
	steps := make([]stepSummary, 0, len(profile.RatesPerStreamPerSecond))
	windows := make([]resourceWindow, 0, len(profile.RatesPerStreamPerSecond))
	for index := range profile.RatesPerStreamPerSecond {
		step := index + 1
		streams := make(map[string]streamSummary, 6)
		for _, stream := range bp02Streams[:6] {
			streams[stream.name] = streamSummary{DeliveryLatency: latencySummary{P99Nanoseconds: int64(step * 100), P999Nanoseconds: int64(step * 200)}, PublishAckLatency: latencySummary{P99Nanoseconds: int64(step * 300)}}
		}
		steps = append(steps, stepSummary{Step: step, DeliveredPerSecond: float64(step * 10), Streams: streams})
		windows = append(windows, resourceWindow{Window: fmt.Sprintf("step-%02d-measure", step), Deliveries: 20, Cluster: resourceMeasurements{MedianCPUPercent: float64(step * 5), NetworkTxBytes: int64(step * 400)}})
	}
	setup := setupSummary{}
	for _, spec := range bp02SetupClients(profile) {
		if spec.tls {
			setup.TLSClients++
		} else {
			setup.TCPClients++
		}
	}
	setup.TLSConnectLatency = latencySummary{P99Nanoseconds: 11}
	setup.TCPConnectLatency = latencySummary{P99Nanoseconds: 12}
	return bp02TargetManifest{Success: true, Valid: true, ResolvedProfile: profile, WorkloadSummary: &workloadSummary{Valid: true, Steps: steps, Setup: setup, Offline: offlineSummary{ReconnectConnAckLatency: latencySummary{P99Nanoseconds: 13}, DrainDurationMaxNanoseconds: 14}, Retained: retainedSummary{ReplayDurationMaxNanoseconds: 15}}, ResourceSummary: &resourceSummary{Valid: true, NetworkStatsAvailable: true, Windows: windows, Memory: memorySummary{MemoryBytesPerSetupClient: -20}}}
}

// TestBP02MetricsSelectStreamsAndPhases verifies metric selection by stream and phase.
// Scenarios: performance-and-verification.md:PRF-SCN-048
func TestBP02MetricsSelectStreamsAndPhases(t *testing.T) {
	profile := bp02SmokeProfile(t)
	profile.TLSEveryNthClient = 1
	metrics, err := bp02Metrics("narumq", bp02MetricEntry(profile))
	if err != nil {
		t.Fatal(err)
	}
	checks := map[string]float64{"step01_delivered_per_second": 10, "step02_cluster_median_cpu_percent": 10, "step02_a_telemetry_delivery_latency_p999_ns": 400, "step02_a_command_publish_ack_latency_p99_ns": 600, "step02_network_tx_bytes_per_delivery": 40, "setup_tls_connect_latency_p99_ns": 11, "offline_reconnect_connack_latency_p99_ns": 13, "offline_drain_duration_max_ns": 14, "retained_replay_duration_max_ns": 15, "memory_bytes_per_setup_client": -20}
	for key, want := range checks {
		if got, exists := metrics[key]; !exists || got != want {
			t.Errorf("metric%s=%v exists%v, want%v", key, got, exists, want)
		}
	}
	for _, key := range []string{"step01_a_telemetry_publish_ack_latency_p99_ns", "step01_b_telemetry_publish_ack_latency_p99_ns", "setup_tcp_connect_latency_p99_ns"} {
		if _, exists := metrics[key]; exists {
			t.Errorf("unexpected metric %s", key)
		}
	}
	units := bp02SuiteMetricUnits(profile, false)
	if _, exists := units["step01_network_tx_bytes_per_delivery"]; exists {
		t.Fatal("network metric without stats")
	}
	if _, exists := units["setup_tcp_connect_latency_p99_ns"]; exists {
		t.Fatal("TCP metric with TLS-only plan")
	}
}

// TestBP02SuiteFailureLeavesParentMetricsEmpty verifies a failed suite leaves parent metrics empty.
// Scenarios: performance-and-verification.md:PRF-SCN-048
func TestBP02SuiteFailureLeavesParentMetricsEmpty(t *testing.T) {
	profile := bp02SmokeProfile(t)
	options := bp02SuiteOptions{Profile: profile, Repetitions: suiteartifact.MinimumRepetitions}
	manifest := bp02SuiteManifest{SuiteID: "suite-test", RequestedRepetitions: 5, CompletedRepetitions: 5, FailureReason: "one repetition failed"}
	runs := make([]bp02SuiteRunData, 5)
	for index := range runs {
		entry := bp02MetricEntry(profile)
		runs[index] = bp02SuiteRunData{Repetition: index + 1, RunID: fmt.Sprintf("run-%d", index+1), Success: index != 2, Manifest: bp02Manifest{Targets: map[string]bp02TargetManifest{"narumq": entry, "emqx": entry}}}
	}
	summary := buildBP02SuiteSummary(options, manifest, runs)
	if summary.Valid || summary.SuccessfulRepetitions != 4 {
		t.Fatalf("valid/successful=%v/%d", summary.Valid, summary.SuccessfulRepetitions)
	}
	for _, target := range []string{"narumq", "emqx"} {
		if len(summary.Targets[target].Metrics) != 0 {
			t.Fatalf("failed suite retained %s metrics", target)
		}
	}
}
