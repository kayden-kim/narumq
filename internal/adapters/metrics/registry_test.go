package metrics

import (
	"strings"
	"sync"
	"testing"
	"time"

	ports "narumq/internal/application/ports/metrics"
)

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
	reg.SetSubscriptionsActive(100)

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
