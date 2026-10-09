package metrics

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	ports "narumq/internal/application/ports/metrics"
	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/routing"
)

type subscriptionCount int

func (count subscriptionCount) SubscriptionCount() int {
	return int(count)
}

func TestHistogram(t *testing.T) {
	h := NewHistogram([]float64{0.001, 0.01, 0.1, 1.0})

	h.Observe(0.0005) // <= 0.001
	h.Observe(0.005)  // <= 0.01
	h.Observe(0.05)   // <= 0.1
	h.Observe(0.5)    // <= 1.0
	h.Observe(2.0)    // > 1.0

	buckets, counts, count, sum := h.Snapshot()

	if count != 5 {
		t.Fatalf("expected count 5, got %d", count)
	}
	if sum < 2.555 || sum > 2.556 {
		t.Fatalf("unexpected sum: %f", sum)
	}
	if len(buckets) != 4 {
		t.Fatalf("expected 4 buckets, got %d", len(buckets))
	}

	// Cumulative counts:
	// <= 0.001: 1
	// <= 0.01: 2
	// <= 0.1: 3
	// <= 1.0: 4
	expected := []uint64{1, 2, 3, 4}
	for i, exp := range expected {
		if counts[i] != exp {
			t.Errorf("bucket %g: expected count %d, got %d", buckets[i], exp, counts[i])
		}
	}
}

func TestRegistry_OperationsAndRendering(t *testing.T) {
	reg := NewRegistry()

	// Connections
	reg.ConnectionOpened(ports.ProtocolMQTT50, ports.TransportTCP)
	reg.ConnectionOpened(ports.ProtocolMQTT311, ports.TransportTLS)
	if reg.ActiveConnections() != 2 {
		t.Fatalf("expected 2 active connections, got %d", reg.ActiveConnections())
	}
	reg.ConnectionClosed(ports.ProtocolMQTT311, ports.TransportTLS)
	if reg.ActiveConnections() != 1 {
		t.Fatalf("expected 1 active connection, got %d", reg.ActiveConnections())
	}

	// Packets
	reg.PacketReceived("CONNECT")
	reg.PacketReceived("PUBLISH")
	reg.PacketReceived("PUBLISH")
	reg.PacketSent("CONNACK")
	reg.PacketSent("PUBACK")

	// Auth
	reg.AuthAttempt(ports.AuthMethodPassword, true, "none")
	reg.AuthAttempt(ports.AuthMethodAnonymous, false, "not_authorized")

	// Sessions & Subscriptions
	reg.SetSessionsActive(true, 10)
	reg.SetSessionsActive(false, 25)
	reg.ObserveSubscriptions(subscriptionCount(100))

	// Retained
	reg.SetRetainedStats(42, 8192)

	// Latency
	reg.ObservePublishDuration(2 * time.Millisecond)

	// Test OpenMetrics rendering
	omOutput := string(reg.Render(true))
	if !strings.HasSuffix(omOutput, "# EOF\n") {
		t.Errorf("OpenMetrics output must end with '# EOF\\n'")
	}

	// Verify required metrics presence
	requiredMetrics := []string{
		"narumq_connections_active 1",
		`narumq_connections_total{protocol="MQTT_5_0",transport="tcp",result="success"} 1`,
		`narumq_connections_total{protocol="MQTT_3_1_1",transport="tls",result="success"} 1`,
		`narumq_sessions_active{persistent="true"} 10`,
		`narumq_sessions_active{persistent="false"} 25`,
		"narumq_subscriptions_active 100",
		"narumq_retained_messages 42",
		"narumq_retained_bytes 8192",
		`narumq_packets_received_total{packet_type="CONNECT"} 1`,
		`narumq_packets_received_total{packet_type="PUBLISH"} 2`,
		`narumq_packets_sent_total{packet_type="CONNACK"} 1`,
		`narumq_packets_sent_total{packet_type="PUBACK"} 1`,
		`narumq_auth_attempts_total{method="password",result="success",reason="none"} 1`,
		`narumq_auth_attempts_total{method="anonymous",result="failure",reason="not_authorized"} 1`,
		"narumq_publish_latency_seconds_count 1",
	}

	for _, req := range requiredMetrics {
		if !strings.Contains(omOutput, req) {
			t.Errorf("missing expected metric line: %s\nFull output:\n%s", req, omOutput)
		}
	}

	// Verify Prometheus rendering (no # EOF)
	promOutput := string(reg.Render(false))
	if strings.Contains(promOutput, "# EOF") {
		t.Errorf("Prometheus v0.0.4 output must not contain '# EOF'")
	}

	// Verify cardinality: forbid client identifiers, IPs, arbitrary topics, bearer tokens in labels
	forbiddenTokens := []string{"clientId", "client_id", "127.0.0.1", "topic/", "bearer", "token"}
	for _, f := range forbiddenTokens {
		if strings.Contains(omOutput, f) {
			t.Errorf("output contains forbidden token: %s", f)
		}
	}
}

// Traceability: API-03; docs/specs/management-api-and-observability.md §7.1.
func TestRegistryReadsSubscriptionCountAtScrape(t *testing.T) {
	registry := NewRegistry()
	routes := routing.NewTable()
	registry.ObserveSubscriptions(routes)
	requireCount := func(want int) {
		t.Helper()
		line := fmt.Sprintf("narumq_subscriptions_active %d\n", want)
		if output := string(registry.Render(false)); !strings.Contains(output, line) {
			t.Fatalf("Render() missing %q:\n%s", line, output)
		}
	}

	requireCount(0)
	if _, err := routes.Subscribe("client", mqtt.Subscription{Filter: "sensors/#", QoS: mqtt.QoSAtMostOnce}); err != nil {
		t.Fatalf("Subscribe(client, sensors/#): %v", err)
	}
	requireCount(1)
	if _, err := routes.Subscribe("worker", mqtt.Subscription{Filter: "$share/group/jobs", QoS: mqtt.QoSAtLeastOnce}); err != nil {
		t.Fatalf("Subscribe(worker, $share/group/jobs): %v", err)
	}
	requireCount(2)
	if removed, err := routes.Unsubscribe("client", "sensors/#"); err != nil || !removed {
		t.Fatalf("Unsubscribe(client, sensors/#) = (%t, %v), want (true, nil)", removed, err)
	}
	requireCount(1)
}

func TestRegistryRendersWALSyncMetrics(t *testing.T) {
	registry := NewRegistry()
	registry.ObserveWALSync(300*time.Microsecond, 0, 100)
	registry.ObserveWALSync(20*time.Millisecond, 3, 4104)
	registry.ObserveWALSync(2*time.Second, 300, 8)

	output := string(registry.Render(true))
	wantLines := []string{
		`narumq_raft_wal_sync_duration_seconds_bucket{le="0.0005"} 1`,
		`narumq_raft_wal_sync_duration_seconds_bucket{le="0.025"} 2`,
		`narumq_raft_wal_sync_duration_seconds_bucket{le="1"} 2`,
		`narumq_raft_wal_sync_duration_seconds_bucket{le="+Inf"} 3`,
		`narumq_raft_wal_sync_entries_bucket{le="0"} 1`,
		`narumq_raft_wal_sync_entries_bucket{le="4"} 2`,
		`narumq_raft_wal_sync_entries_bucket{le="256"} 2`,
		"narumq_raft_wal_sync_entries_count 3",
		"narumq_raft_wal_written_bytes_total 4212",
	}
	for _, line := range wantLines {
		if !strings.Contains(output, line+"\n") {
			t.Errorf("missing WAL metric line %q", line)
		}
	}
	if !strings.HasSuffix(output, "# EOF\n") {
		t.Fatalf("OpenMetrics output must end with '# EOF\\n'")
	}
}

func TestRaftStageHistogramsKeepStagesIsolatedAndBoundOverflow(t *testing.T) {
	registry := NewRegistry()
	registry.ObserveRaftStage(RaftInboundQueue, time.Millisecond)
	registry.ObserveRaftStage(RaftReadIndex, 2*time.Second)
	registry.ObserveRaftStage(RaftReadIndex, 12*time.Second)
	registry.ObserveRaftStage(255, time.Hour)
	output := string(registry.Render(false))
	for _, line := range []string{
		`narumq_raft_stage_duration_seconds_count{stage="inbound_queue"} 1`,
		`narumq_raft_stage_duration_seconds_bucket{stage="read_index",le="2"} 1`,
		`narumq_raft_stage_duration_seconds_bucket{stage="read_index",le="10"} 1`,
		`narumq_raft_stage_duration_seconds_bucket{stage="read_index",le="+Inf"} 2`,
		`narumq_raft_stage_duration_seconds_sum{stage="read_index"} 14.000000`,
		`narumq_raft_stage_duration_seconds_count{stage="proposal"} 0`,
	} {
		if !strings.Contains(output, line+"\n") {
			t.Errorf("missing stage distribution %q", line)
		}
	}
}

// Traceability: UI-03; docs/specs/management-api-and-observability.md §7.
func TestRegistryRendersConnectRejectionsByProtocolAndReason(t *testing.T) {
	registry := NewRegistry()
	registry.ConnectRejected(ports.ProtocolMQTT311, ports.ConnectRejectReasonOther)
	registry.ConnectRejected(ports.ProtocolMQTT311, ports.ConnectRejectReasonOther)
	registry.ConnectRejected(ports.ProtocolMQTT50, ports.ConnectRejectReasonNotAuthorized)
	registry.ConnectRejected(ports.ProtocolMQTT310, ports.ConnectRejectReasonUnsupportedProtocolVersion)
	registry.ConnectRejected("unknown", ports.ConnectRejectReasonOther)
	registry.ConnectRejected(ports.ProtocolMQTT50, "unknown")

	protocols := []string{"3.1.0", "MQTT_3_1_1", "MQTT_5_0"}
	reasons := []string{
		"unsupported_protocol_version",
		"identifier_rejected",
		"bad_credentials",
		"not_authorized",
		"server_unavailable",
		"quota_exceeded",
		"other",
	}
	output := string(registry.Render(false))
	lastIndex := -1
	seriesCount := 0
	for _, protocol := range protocols {
		protocolReasons := reasons
		if protocol == "3.1.0" {
			protocolReasons = reasons[:1]
		}
		for _, reason := range protocolReasons {
			want := uint64(0)
			switch {
			case protocol == "3.1.0" && reason == "unsupported_protocol_version":
				want = 1
			case protocol == "MQTT_3_1_1" && reason == "other":
				want = 2
			case protocol == "MQTT_5_0" && reason == "not_authorized":
				want = 1
			}
			line := fmt.Sprintf("narumq_mqtt_connect_rejected_total{protocol=%q,reason=%q} %d\n", protocol, reason, want)
			index := strings.Index(output, line)
			if index < 0 {
				t.Fatalf("missing series %q:\n%s", line, output)
			}
			if index <= lastIndex {
				t.Fatalf("series order is not fixed at %q", line)
			}
			if count := strings.Count(output, line); count != 1 {
				t.Fatalf("series %q occurs %d times, want once", line, count)
			}
			lastIndex = index
			seriesCount++
		}
	}
	if seriesCount != 15 {
		t.Fatalf("rendered %d CONNECT rejection series, want 15", seriesCount)
	}
}

// Traceability: UI-03; docs/specs/management-api-and-observability.md §7.
func TestRegistryRendersMessageDrops(t *testing.T) {
	registry := NewRegistry()
	registry.MessageDropped(ports.MessageDropReasonQueueFull)
	registry.MessageDropped(ports.MessageDropReasonPacketTooLarge)
	registry.MessageDropped(ports.MessageDropReasonPacketTooLarge)
	registry.MessageDropped("unknown")

	output := string(registry.Render(true))
	for _, line := range []string{
		"# HELP narumq_messages_dropped_total Messages discarded after routing before delivery to one subscriber.\n",
		"# TYPE narumq_messages_dropped_total counter\n",
		`narumq_messages_dropped_total{reason="queue_full"} 1` + "\n",
		`narumq_messages_dropped_total{reason="packet_too_large"} 2` + "\n",
	} {
		if count := strings.Count(output, line); count != 1 {
			t.Fatalf("line %q occurs %d times, want once:\n%s", line, count, output)
		}
	}
	if strings.Contains(output, `narumq_messages_dropped_total{reason="unknown"}`) {
		t.Fatalf("unknown message drop reason was rendered:\n%s", output)
	}
}

// Traceability: UI-05; docs/specs/management-api-and-observability.md §7.
func TestRegistryRendersNodeDrainProgress(t *testing.T) {
	registry := NewRegistry()
	requireGauge := func(output string, active int, values []uint64) {
		t.Helper()
		if !strings.Contains(output, fmt.Sprintf("narumq_node_drain_active %d\n", active)) {
			t.Fatalf("active gauge = %d missing:\n%s", active, output)
		}
		quantities := []string{
			"connections_initial",
			"connections_remaining",
			"sessions_ready_for_takeover",
			"sessions_taken_over",
			"sessions_failed",
			"pending_wal",
			"under_replicated",
		}
		for index, quantity := range quantities {
			line := fmt.Sprintf("narumq_node_drain_progress{quantity=%q} %d\n", quantity, values[index])
			if !strings.Contains(output, line) {
				t.Errorf("missing progress gauge %q:\n%s", line, output)
			}
		}
	}
	requireGauge(string(registry.Render(false)), 0, make([]uint64, 7))

	registry.SetNodeDrainProgress(true, NodeDrainProgress{
		ConnectionsInitial:       11,
		ConnectionsRemaining:     10,
		SessionsReadyForTakeover: 9,
		SessionsTakenOver:        8,
		SessionsFailed:           7,
		PendingWAL:               6,
		UnderReplicated:          5,
	})
	requireGauge(string(registry.Render(false)), 1, []uint64{11, 10, 9, 8, 7, 6, 5})

	registry.SetNodeDrainProgress(false, NodeDrainProgress{})
	requireGauge(string(registry.Render(false)), 0, make([]uint64, 7))
}

func TestRegistryRendersMQTT310UnsupportedVersionRejection(t *testing.T) {
	reg := NewRegistry()
	reg.ConnectRejected(ports.ProtocolMQTT310, ports.ConnectRejectReasonUnsupportedProtocolVersion)
	reg.ConnectRejected("other", ports.ConnectRejectReasonUnsupportedProtocolVersion)
	reg.ConnectRejected(ports.ProtocolMQTT310, "other")

	output := string(reg.Render(true))
	line := `narumq_mqtt_connect_rejected_total{protocol="3.1.0",reason="unsupported_protocol_version"} 1`
	if strings.Count(output, line) != 1 {
		t.Fatalf("expected one MQTT 3.1.0 rejection counter, got %d:\n%s", strings.Count(output, line), output)
	}
}

// Traceability: DEP-01; docs/specs/drain-and-rolling-update.md §6, §10.
func TestRegistryNodeDrainTransitionMetricUsesFixedPhaseLabels(t *testing.T) {
	registry := NewRegistry()
	phases := []string{
		"EXCLUDING_TRAFFIC", "WAITING_FOR_LB", "VERIFYING_SESSION_STATE", "TRANSFERRING_SESSIONS",
		"VERIFYING_BARRIER", "COMPLETED", "FAILED", "CANCELLED",
	}
	for _, phase := range phases {
		registry.NodeDrainTransition(phase)
	}
	registry.NodeDrainTransition("UNSPECIFIED")
	registry.NodeDrainTransition("arbitrary")

	output := string(registry.Render(true))
	if !strings.Contains(output, "# HELP narumq_node_drain_transitions_total Committed node drain phase transitions observed for the local node.\n") ||
		!strings.Contains(output, "# TYPE narumq_node_drain_transitions_total counter\n") {
		t.Fatalf("drain transition metric metadata missing:\n%s", output)
	}
	for _, phase := range phases {
		line := `narumq_node_drain_transitions_total{phase="` + phase + `"} 1`
		if !strings.Contains(output, line+"\n") {
			t.Errorf("drain transition metric missing %q:\n%s", line, output)
		}
	}
	if strings.Contains(output, `narumq_node_drain_transitions_total{phase="UNSPECIFIED"}`) ||
		strings.Contains(output, `narumq_node_drain_transitions_total{phase="arbitrary"}`) {
		t.Fatalf("metric accepted a non-phase label:\n%s", output)
	}
}

// Traceability: TEN-08; docs/specs/tenancy-and-security.md §5.1.
func TestRegistryTenantQuotaMetricsAndProfileGauges(t *testing.T) {
	registry := NewRegistry()
	if label := registry.RecordTenantQuotaRejection("acme", "retained_bytes"); label != "acme" {
		t.Fatalf("tenant metric label = %q, want acme", label)
	}
	registry.RecordTenantQuotaRejection("acme", "retained_bytes")
	registry.RecordTenantQuotaRejection(`tenant"west`, "offline_queue_bytes")
	if label := registry.RecordTenantQuotaRejection("bad\ntenant", "storage_bytes"); label != tenantQuotaOverflowLabel {
		t.Fatalf("control-character tenant label = %q, want %q", label, tenantQuotaOverflowLabel)
	}
	if label := registry.RecordTenantQuotaRejection("acme", "unbounded_dimension"); label != "" {
		t.Fatalf("unknown quota dimension label = %q, want empty", label)
	}
	registry.SetTenantQuotaProfile(true, 7, true)

	output := string(registry.Render(true))
	for _, line := range []string{
		"narumq_tenant_quota_profile_committed 1\n",
		"narumq_tenant_quota_profile_version 7\n",
		"narumq_tenant_quota_config_mismatch 1\n",
		`narumq_tenant_quota_rejections_total{tenant="acme",dimension="retained_bytes"} 2`,
		`narumq_tenant_quota_rejections_total{tenant="tenant\"west",dimension="offline_queue_bytes"} 1`,
		`narumq_tenant_quota_rejections_total{tenant="_overflow",dimension="storage_bytes"} 1`,
	} {
		if !strings.Contains(output, line) {
			t.Errorf("missing tenant quota metric %q:\n%s", line, output)
		}
	}
	if strings.Contains(output, "bad\ntenant") || strings.Contains(output, "unbounded_dimension") {
		t.Fatalf("invalid tenant or dimension escaped metric normalization:\n%s", output)
	}

	registry.SetTenantQuotaProfile(false, 99, true)
	output = string(registry.Render(false))
	for _, line := range []string{
		"narumq_tenant_quota_profile_committed 0\n",
		"narumq_tenant_quota_profile_version 0\n",
		"narumq_tenant_quota_config_mismatch 0\n",
	} {
		if !strings.Contains(output, line) {
			t.Errorf("uncommitted profile gauge did not reset: missing %q", line)
		}
	}
	if strings.Contains(output, "# EOF") {
		t.Fatalf("Prometheus rendering unexpectedly included OpenMetrics EOF")
	}
}

// Traceability: TEN-08; docs/specs/tenancy-and-security.md §5.1.
func TestRegistryTenantQuotaMetricBoundsTenantLabels(t *testing.T) {
	registry := NewRegistry()
	for index := 0; index < maxTenantQuotaMetricTenants; index++ {
		label := fmt.Sprintf("tenant-%04d", index)
		if got := registry.RecordTenantQuotaRejection(label, "retained_bytes"); got != label {
			t.Fatalf("tenant %d label = %q, want %q", index, got, label)
		}
	}
	if got := registry.RecordTenantQuotaRejection("tenant-overflow-a", "retained_bytes"); got != tenantQuotaOverflowLabel {
		t.Fatalf("first overflow label = %q, want %q", got, tenantQuotaOverflowLabel)
	}
	if got := registry.RecordTenantQuotaRejection("tenant-overflow-b", "retained_bytes"); got != tenantQuotaOverflowLabel {
		t.Fatalf("second overflow label = %q, want %q", got, tenantQuotaOverflowLabel)
	}

	output := string(registry.Render(false))
	if !strings.Contains(output, `narumq_tenant_quota_rejections_total{tenant="_overflow",dimension="retained_bytes"} 2`) {
		t.Fatalf("overflow tenants did not share one counter:\n%s", output)
	}
	labels := make(map[string]struct{})
	for _, line := range strings.Split(output, "\n") {
		if !strings.HasPrefix(line, "narumq_tenant_quota_rejections_total{tenant=") {
			continue
		}
		start := strings.Index(line, `tenant="`)
		end := strings.Index(line, `",dimension=`)
		if start < 0 || end < 0 {
			t.Fatalf("malformed tenant quota metric line %q", line)
		}
		labels[line[start+len(`tenant="`):end]] = struct{}{}
	}
	if got, want := len(labels), maxTenantQuotaMetricTenants+1; got != want {
		t.Fatalf("distinct rendered tenant labels = %d, want %d including _overflow", got, want)
	}
	if got := len(registry.tenantQuotaTenants); got != maxTenantQuotaMetricTenants {
		t.Fatalf("retained tenant label count = %d, want bounded at %d", got, maxTenantQuotaMetricTenants)
	}
}

func TestRegistry_Concurrency(t *testing.T) {
	reg := NewRegistry()
	var wg sync.WaitGroup

	const goroutines = 10
	const iterations = 500

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				reg.ConnectionOpened(ports.ProtocolMQTT50, ports.TransportTCP)
				reg.PacketReceived("PUBLISH")
				reg.PacketSent("PUBACK")
				reg.ObservePublishDuration(time.Millisecond)
				reg.ConnectionClosed(ports.ProtocolMQTT50, ports.TransportTCP)
			}
		}()
	}

	wg.Wait()

	if reg.ActiveConnections() != 0 {
		t.Errorf("expected 0 active connections after balanced open/close, got %d", reg.ActiveConnections())
	}
	output := reg.Render(true)
	if len(output) == 0 {
		t.Errorf("empty rendered output")
	}
}

// Traceability: API-06; docs/specs/management-api-and-observability.md §9.
func TestRegistryNormalizesUnboundedLabels(t *testing.T) {
	reg := NewRegistry()
	reg.ConnectionOpened("client-id-secret", "10.0.0.1")
	reg.PacketReceived("topic/with/client-id")
	reg.AuthAttempt("bearer-token", false, "request-id-secret")

	output := string(reg.Render(true))
	forbidden := []string{"client-id-secret", "10.0.0.1", "topic/with/client-id", "bearer-token", "request-id-secret"}
	for _, value := range forbidden {
		if strings.Contains(output, value) {
			t.Fatalf("unbounded label value %q leaked into metrics: %s", value, output)
		}
	}
	if !strings.Contains(output, `protocol="UNKNOWN",transport="unknown"`) {
		t.Fatalf("unknown protocol and transport were not normalized: %s", output)
	}
	if !strings.Contains(output, `packet_type="UNKNOWN"`) {
		t.Fatalf("unknown packet type was not normalized: %s", output)
	}
}

func TestRegistryRendersWillOutcomes(t *testing.T) {
	registry := NewRegistry()
	for _, openMetrics := range []bool{false, true} {
		text := string(registry.Render(openMetrics))
		for _, reason := range willOutcomeReasons {
			if !strings.Contains(text, fmt.Sprintf("narumq_will_outcomes_total{reason=\"%s\"} 0\n", reason)) {
				t.Fatalf("missing zero series for %s", reason)
			}
		}
	}
	for _, reason := range willOutcomeReasons {
		registry.WillOutcome(reason)
	}
	registry.WillOutcome("bogus")
	text := string(registry.Render(false))
	for _, reason := range willOutcomeReasons {
		if !strings.Contains(text, fmt.Sprintf("narumq_will_outcomes_total{reason=\"%s\"} 1\n", reason)) {
			t.Fatalf("missing count for %s", reason)
		}
	}
	if strings.Contains(text, "bogus") {
		t.Fatal("unknown reason created a series")
	}
}

func TestRegistryRendersAuditMetrics(t *testing.T) {
	registry := NewRegistry()
	for _, openMetrics := range []bool{false, true} {
		text := string(registry.Render(openMetrics))
		var previousIndex = -1
		for _, outcome := range auditOutcomes {
			series := fmt.Sprintf("narumq_audit_events_total{outcome=\"%s\"} 0\n", outcome)
			position := strings.Index(text, series)
			if position < 0 || position <= previousIndex {
				t.Fatalf("missing or unordered zero series for %q: %s", outcome, text)
			}
			previousIndex = position
		}
		if !strings.Contains(text, "narumq_audit_publisher_ready 0\n") {
			t.Fatalf("missing zero readiness gauge: %s", text)
		}
	}

	registry.AuditOutcome(ports.AuditOutcomePublished)
	registry.AuditOutcome(ports.AuditOutcomePublished)
	registry.AuditOutcome(ports.AuditOutcomeInvalid)
	registry.AuditOutcome("unknown")
	registry.SetAuditPublisherReady(true)
	text := string(registry.Render(false))
	if !strings.Contains(text, "narumq_audit_events_total{outcome=\"published\"} 2\n") ||
		!strings.Contains(text, "narumq_audit_events_total{outcome=\"invalid\"} 1\n") ||
		!strings.Contains(text, "narumq_audit_publisher_ready 1\n") {
		t.Fatalf("audit metrics did not reflect known outcomes and readiness: %s", text)
	}
	if strings.Contains(text, `outcome="unknown"`) {
		t.Fatalf("unknown outcome created a series: %s", text)
	}
}

// Traceability: PRD MQTT-09/MQTT-11; docs/specs/mqtt-protocol-and-session.md §7.1.
func TestRegistryRendersListenerAdmissionRejections(t *testing.T) {
	registry := NewRegistry()
	for _, openMetrics := range []bool{false, true} {
		text := string(registry.Render(openMetrics))
		want := "narumq_mqtt_listener_admission_rejected_total{transport=\"tcp\",reason=\"max_connections\"} 0\n" +
			"narumq_mqtt_listener_admission_rejected_total{transport=\"tcp\",reason=\"connection_rate\"} 0\n" +
			"narumq_mqtt_listener_admission_rejected_total{transport=\"tls\",reason=\"max_connections\"} 0\n" +
			"narumq_mqtt_listener_admission_rejected_total{transport=\"tls\",reason=\"connection_rate\"} 0\n"
		if !strings.Contains(text, want) {
			t.Fatalf("missing ordered zero series: %s", text)
		}
	}
	registry.ListenerAdmissionRejected(ports.TransportTCP, ports.ListenerAdmissionReasonMaxConnections)
	registry.ListenerAdmissionRejected(ports.TransportTCP, ports.ListenerAdmissionReasonMaxConnections)
	registry.ListenerAdmissionRejected(ports.TransportTLS, ports.ListenerAdmissionReasonConnectionRate)
	registry.ListenerAdmissionRejected("ws", ports.ListenerAdmissionReasonMaxConnections)
	registry.ListenerAdmissionRejected(ports.TransportTCP, "other")
	want := "narumq_mqtt_listener_admission_rejected_total{transport=\"tcp\",reason=\"max_connections\"} 2\n" +
		"narumq_mqtt_listener_admission_rejected_total{transport=\"tcp\",reason=\"connection_rate\"} 0\n" +
		"narumq_mqtt_listener_admission_rejected_total{transport=\"tls\",reason=\"max_connections\"} 0\n" +
		"narumq_mqtt_listener_admission_rejected_total{transport=\"tls\",reason=\"connection_rate\"} 1\n"
	for _, openMetrics := range []bool{false, true} {
		text := string(registry.Render(openMetrics))
		if !strings.Contains(text, want) ||
			strings.Contains(text, `narumq_mqtt_listener_admission_rejected_total{transport="tcp",reason="other"}`) {
			t.Fatalf("unexpected admission series: %s", text)
		}
	}
}

// Traceability: TEN-06, TEN-07; docs/specs/tenancy-and-security.md §5.2.
func TestRegistryTenantLimitRejectionsShareBoundedTenantLabels(t *testing.T) {
	registry := NewRegistry()
	for index := range maxTenantQuotaMetricTenants {
		tenantID := fmt.Sprintf("limit-tenant-%04d", index)
		if got := registry.RecordTenantQuotaRejection(tenantID, "retained_bytes"); got != tenantID {
			t.Fatalf("quota label = %q, want %q", got, tenantID)
		}
	}
	if got := registry.RecordTenantLimitRejection("unseen-limit-tenant", "connections"); got != tenantQuotaOverflowLabel {
		t.Fatalf("limit label = %q, want shared overflow label", got)
	}
	if got := registry.RecordTenantLimitRejection("invalid", "connections"); got != tenantQuotaOverflowLabel {
		t.Fatalf("invalid tenant label = %q, want overflow label", got)
	}
	if got := registry.RecordTenantLimitRejection("invalid", "sessions"); got != tenantQuotaOverflowLabel {
		t.Fatalf("second invalid tenant label = %q, want overflow label", got)
	}
	if got := registry.RecordTenantLimitRejection("tenant-a", "unknown"); got != "" {
		t.Fatalf("unknown dimension label = %q, want empty", got)
	}

	output := string(registry.Render(false))
	if !strings.Contains(output, "# HELP narumq_tenant_limit_rejections_total Requests rejected by tenant connection, session, subscription, payload, or rate limits.\n") ||
		!strings.Contains(output, "# TYPE narumq_tenant_limit_rejections_total counter\n") ||
		!strings.Contains(output, "narumq_tenant_limit_rejections_total{tenant=\"_overflow\",dimension=\"connections\"} 2\n") ||
		!strings.Contains(output, "narumq_tenant_limit_rejections_total{tenant=\"_overflow\",dimension=\"sessions\"} 1\n") {
		t.Fatalf("tenant limit metric metadata or overflow series missing:\n%s", output)
	}
	first := strings.Index(output, `narumq_tenant_limit_rejections_total{tenant="_overflow",dimension="connections"}`)
	second := strings.Index(output, `narumq_tenant_limit_rejections_total{tenant="_overflow",dimension="sessions"}`)
	if first >= 0 && second >= 0 && first >= second {
		t.Fatal("tenant limit series are not sorted by dimension")
	}
	if len(registry.tenantQuotaTenants) != maxTenantQuotaMetricTenants {
		t.Fatalf("bounded tenant label set size = %d, want %d", len(registry.tenantQuotaTenants), maxTenantQuotaMetricTenants)
	}
}

// Traceability: TEN-06, TEN-07; docs/specs/tenancy-and-security.md §5.2.
func TestRegistryTenantLimitMetricDimensionAllowlist(t *testing.T) {
	registry := NewRegistry()
	for _, dimension := range []string{"connections", "sessions", "subscriptions", "message_rate", "byte_rate", "payload_size"} {
		if got := registry.RecordTenantLimitRejection("tenant-a", dimension); got != "tenant-a" {
			t.Errorf("dimension %q returned tenant label %q", dimension, got)
		}
	}
	if got := registry.RecordTenantLimitRejection("tenant-a", "unknown"); got != "" {
		t.Fatalf("unknown dimension label = %q, want empty", got)
	}
	output := string(registry.Render(false))
	for _, dimension := range []string{"connections", "sessions", "subscriptions", "message_rate", "byte_rate", "payload_size"} {
		if !strings.Contains(output, `narumq_tenant_limit_rejections_total{tenant="tenant-a",dimension="`+dimension+`"} 1`) {
			t.Errorf("missing tenant limit dimension %q:\n%s", dimension, output)
		}
	}
}
