package observability

import (
	"fmt"
	"strings"
	"testing"

	metricsports "narumq/internal/application/ports/metrics"
	observationports "narumq/internal/application/ports/observability"
)

// Scenarios: management-api-and-observability.md:MGT-SCN-023
func TestCombineMetricFamiliesAppliesClusterRules(t *testing.T) {
	first := observationports.NodeMetricsDataDTO{
		NodeID: "1", QueryStatus: observationports.QueryStatusOK,
		Families: []observationports.MetricFamilyDTO{
			testMetricFamily("narumq_connections_active",
				metricSample("narumq_connections_active", nil, "integer", "2")),
			testMetricFamily("narumq_cluster_commit_backlog_oldest_seconds",
				metricSample("narumq_cluster_commit_backlog_oldest_seconds", nil, "float", "1.5")),
			testMetricFamily("narumq_audit_publisher_ready",
				metricSample("narumq_audit_publisher_ready", nil, "integer", "1")),
			testMetricFamily("narumq_connections_total",
				metricSample("narumq_connections_total", []observationports.MetricLabelDTO{
					{Name: "protocol", Value: "5.0"}, {Name: "result", Value: "success"}, {Name: "transport", Value: "tcp"},
				}, "integer", "11")),
			testMetricFamily("narumq_publish_latency_seconds",
				metricSample("narumq_publish_latency_seconds_bucket", []observationports.MetricLabelDTO{{Name: "le", Value: "0.5"}}, "integer", "1"),
				metricSample("narumq_publish_latency_seconds_bucket", []observationports.MetricLabelDTO{{Name: "le", Value: "+Inf"}}, "integer", "1"),
				metricSample("narumq_publish_latency_seconds_sum", nil, "float", "0.25"),
				metricSample("narumq_publish_latency_seconds_count", nil, "integer", "1")),
			testMetricFamily("narumq_raft_peer_replication_lag_entries",
				metricSample("narumq_raft_peer_replication_lag_entries", []observationports.MetricLabelDTO{{Name: "peer", Value: "2"}}, "integer", "4")),
		},
	}
	second := observationports.NodeMetricsDataDTO{
		NodeID: "2", QueryStatus: observationports.QueryStatusOK,
		Families: []observationports.MetricFamilyDTO{
			testMetricFamily("narumq_connections_active",
				metricSample("narumq_connections_active", nil, "integer", "3")),
			testMetricFamily("narumq_cluster_commit_backlog_oldest_seconds",
				metricSample("narumq_cluster_commit_backlog_oldest_seconds", nil, "float", "2.5")),
			testMetricFamily("narumq_audit_publisher_ready",
				metricSample("narumq_audit_publisher_ready", nil, "integer", "0")),
			testMetricFamily("narumq_connections_total",
				metricSample("narumq_connections_total", []observationports.MetricLabelDTO{
					{Name: "protocol", Value: "5.0"}, {Name: "result", Value: "success"}, {Name: "transport", Value: "tcp"},
				}, "integer", "13")),
			testMetricFamily("narumq_publish_latency_seconds",
				metricSample("narumq_publish_latency_seconds_bucket", []observationports.MetricLabelDTO{{Name: "le", Value: "0.5"}}, "integer", "2"),
				metricSample("narumq_publish_latency_seconds_bucket", []observationports.MetricLabelDTO{{Name: "le", Value: "+Inf"}}, "integer", "2"),
				metricSample("narumq_publish_latency_seconds_sum", nil, "float", "0.75"),
				metricSample("narumq_publish_latency_seconds_count", nil, "integer", "2")),
			testMetricFamily("narumq_raft_peer_replication_lag_entries",
				metricSample("narumq_raft_peer_replication_lag_entries", []observationports.MetricLabelDTO{{Name: "peer", Value: "2"}}, "integer", "9")),
		},
	}

	aggregated, err := combineMetricFamilies([]observationports.NodeMetricsDataDTO{first, second}, observationports.MetricsModeAllNodesAggregated)
	if err != nil {
		t.Fatalf("combine aggregated families: %v", err)
	}
	if got := findFamilyForTest(t, aggregated, "narumq_connections_active").Samples; len(got) != 1 || got[0].Value != "5" {
		t.Fatalf("summed connections = %+v, want one value 5", got)
	}
	if got := findFamilyForTest(t, aggregated, "narumq_cluster_commit_backlog_oldest_seconds").Samples; len(got) != 1 || got[0].Value != "2.5" {
		t.Fatalf("maximum backlog = %+v, want one value 2.5", got)
	}
	perNode := findFamilyForTest(t, aggregated, "narumq_audit_publisher_ready")
	if !hasString(perNode.LabelNames, "node_id") || len(perNode.Samples) != 2 ||
		perNode.Samples[0].Value != "1" || perNode.Samples[0].Labels[0].Value != "1" ||
		perNode.Samples[1].Value != "0" || perNode.Samples[1].Labels[0].Value != "2" {
		t.Fatalf("per-node gauge = %+v", perNode)
	}
	counter := findFamilyForTest(t, aggregated, "narumq_connections_total")
	firstNodeID, firstHasNodeID := labelValueForTest(counter.Samples[0], "node_id")
	secondNodeID, secondHasNodeID := labelValueForTest(counter.Samples[1], "node_id")
	if !hasString(counter.LabelNames, "node_id") || len(counter.Samples) != 2 ||
		counter.Samples[0].Value != "11" || !firstHasNodeID || firstNodeID != "1" ||
		counter.Samples[1].Value != "13" || !secondHasNodeID || secondNodeID != "2" {
		t.Fatalf("counter raw series = %+v", counter)
	}
	histogram := findFamilyForTest(t, aggregated, "narumq_publish_latency_seconds")
	if !hasString(histogram.LabelNames, "node_id") || len(histogram.Samples) != 8 {
		t.Fatalf("histogram raw series = %+v", histogram)
	}
	lag := findFamilyForTest(t, aggregated, "narumq_raft_peer_replication_lag_entries")
	if hasString(lag.LabelNames, "node_id") || len(lag.Samples) != 1 || lag.Samples[0].Value != "9" {
		t.Fatalf("leader lag maximum by peer = %+v", lag)
	}

	unaggregated, err := combineMetricFamilies([]observationports.NodeMetricsDataDTO{first, second}, observationports.MetricsModeAllNodesUnaggregated)
	if err != nil {
		t.Fatalf("combine unaggregated families: %v", err)
	}
	connections := findFamilyForTest(t, unaggregated, "narumq_connections_active")
	if len(connections.Samples) != 2 || connections.Samples[0].Value != "2" || connections.Samples[1].Value != "3" ||
		!hasString(connections.LabelNames, "node_id") {
		t.Fatalf("unaggregated connections = %+v", connections)
	}
}

// Scenarios: management-api-and-observability.md:MGT-SCN-023
func TestCombineMetricFamiliesRejectsIntegerOverflow(t *testing.T) {
	descriptor := testMetricFamily("narumq_connections_active",
		metricSample("narumq_connections_active", nil, "integer", "18446744073709551615"))
	overflow := testMetricFamily("narumq_connections_active",
		metricSample("narumq_connections_active", nil, "integer", "1"))
	_, err := combineMetricFamilies([]observationports.NodeMetricsDataDTO{
		{NodeID: "1", QueryStatus: observationports.QueryStatusOK, Families: []observationports.MetricFamilyDTO{descriptor}},
		{NodeID: "2", QueryStatus: observationports.QueryStatusOK, Families: []observationports.MetricFamilyDTO{overflow}},
	}, observationports.MetricsModeAllNodesAggregated)
	if err == nil {
		t.Fatal("integer gauge overflow was accepted")
	}
}

// Traceability: PRD API-03; docs/specs/management-api-and-observability.md §7.2.
// Scenarios: management-api-and-observability.md:MGT-SCN-044
func TestMetricFamiliesFromSnapshotRejectsOverLimitInsteadOfTruncating(t *testing.T) {
	descriptor := descriptorForTest("narumq_packets_received_total")
	counter := func(labels ...metricsports.MetricLabelDTO) metricsports.MetricSampleDTO {
		return metricsports.MetricSampleDTO{
			Name: descriptor.Name, Labels: labels,
			Value: metricsports.MetricValueDTO{Kind: metricsports.MetricValueInteger, Uint64: 1},
		}
	}
	single := func(samples ...metricsports.MetricSampleDTO) metricsports.MetricsSnapshotDTO {
		return metricsports.MetricsSnapshotDTO{Families: []metricsports.MetricFamilyDTO{{Descriptor: descriptor, Samples: samples}}}
	}
	packetSamples := func(count int) []metricsports.MetricSampleDTO {
		samples := make([]metricsports.MetricSampleDTO, count)
		for index := range samples {
			samples[index] = counter(metricsports.MetricLabelDTO{Name: "packet_type", Value: fmt.Sprintf("type-%d", index)})
		}
		return samples
	}

	if families, err := metricFamiliesFromSnapshot(single(packetSamples(maxMetricSamples)...)); err != nil || len(families) != 1 || len(families[0].Samples) != maxMetricSamples {
		t.Fatalf("snapshot with exactly %d samples: families=%d err=%v", maxMetricSamples, len(families), err)
	}
	if families, err := metricFamiliesFromSnapshot(single(counter(metricsports.MetricLabelDTO{
		Name: "packet_type", Value: strings.Repeat("v", maxLabelValueSize),
	}))); err != nil || len(families) != 1 || len(families[0].Samples) != 1 {
		t.Fatalf("label value of exactly %d bytes: families=%d err=%v", maxLabelValueSize, len(families), err)
	}

	nineLabels := make([]metricsports.MetricLabelDTO, 0, maxMetricLabels+1)
	for index := range maxMetricLabels + 1 {
		nineLabels = append(nineLabels, metricsports.MetricLabelDTO{Name: fmt.Sprintf("label_%d", index), Value: "v"})
	}
	for _, test := range []struct {
		name     string
		snapshot metricsports.MetricsSnapshotDTO
		wantErr  string
	}{
		{
			name:     "65 families",
			snapshot: metricsports.MetricsSnapshotDTO{Families: make([]metricsports.MetricFamilyDTO, maxMetricFamilies+1)},
			wantErr:  "metric family limit exceeded",
		},
		{
			name:     "16385 samples",
			snapshot: single(packetSamples(maxMetricSamples + 1)...),
			wantErr:  "metric sample limit exceeded",
		},
		{
			name:     "9 labels on one sample",
			snapshot: single(counter(nineLabels...)),
			wantErr:  "metric label limit exceeded",
		},
		{
			name: "1025 byte label value",
			snapshot: single(counter(metricsports.MetricLabelDTO{
				Name: "packet_type", Value: strings.Repeat("v", maxLabelValueSize+1),
			})),
			wantErr: "invalid metric label value",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			families, err := metricFamiliesFromSnapshot(test.snapshot)
			if err == nil || err.Error() != test.wantErr {
				t.Fatalf("metricFamiliesFromSnapshot() error = %v, want %q", err, test.wantErr)
			}
			if families != nil {
				t.Fatalf("over-limit snapshot returned %d truncated families", len(families))
			}
		})
	}
}

func testMetricFamily(name string, samples ...observationports.MetricSampleDTO) observationports.MetricFamilyDTO {
	for _, descriptor := range metricsports.MetricCatalog() {
		if descriptor.Name == name {
			labels := append([]string(nil), descriptor.LabelNames...)
			return observationports.MetricFamilyDTO{
				Name: descriptor.Name, Help: descriptor.Help, Type: string(descriptor.Type),
				Scope: string(descriptor.Scope), Aggregation: string(descriptor.Aggregation),
				LabelNames: labels, Samples: samples,
			}
		}
	}
	panic("metric descriptor is absent from catalog: " + name)
}

func metricSample(name string, labels []observationports.MetricLabelDTO, kind, value string) observationports.MetricSampleDTO {
	return observationports.MetricSampleDTO{
		Name: name, Labels: labels, ValueKind: kind, Value: value,
	}
}

func findFamilyForTest(t *testing.T, families []observationports.MetricFamilyDTO, name string) observationports.MetricFamilyDTO {
	t.Helper()
	for _, family := range families {
		if family.Name == name {
			return family
		}
	}
	t.Fatalf("metric family %q is missing", name)
	return observationports.MetricFamilyDTO{}
}

func labelValueForTest(sample observationports.MetricSampleDTO, name string) (string, bool) {
	for _, label := range sample.Labels {
		if label.Name == name {
			return label.Value, true
		}
	}
	return "", false
}

func hasString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
