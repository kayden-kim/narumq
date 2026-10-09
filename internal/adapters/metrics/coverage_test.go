package metrics

import (
	"strings"
	"testing"
	"time"

	ports "narumq/internal/application/ports/metrics"
)

func TestCmmHistogramClampsNegativeObservationsToZero(t *testing.T) {
	h := NewHistogram([]float64{0.5, 0.001})
	h.Observe(-3)

	buckets, counts, count, sum := h.Snapshot()
	if buckets[0] != 0.001 || buckets[1] != 0.5 {
		t.Fatalf("buckets = %v, want sorted [0.001 0.5]", buckets)
	}
	if count != 1 || sum != 0 {
		t.Fatalf("count=%d sum=%v, want 1 and 0 after negative observation", count, sum)
	}
	if counts[0] != 1 || counts[1] != 1 {
		t.Fatalf("counts = %v, want clamped zero in every bucket", counts)
	}
}

func TestCmmRegistryNormalizesKnownLabelsCaseInsensitively(t *testing.T) {
	reg := NewRegistry()
	reg.ConnectionOpened(" mqtt50 ", " WS ")
	reg.ConnectionOpened("MQTT311", "WSS")
	reg.ConnectionClosed(ports.ProtocolMQTT50, ports.TransportWS)
	reg.AuthAttempt(" TLS_CERT ", true, "NONE")
	reg.AuthAttempt("JWT", false, " Invalid_Credentials ")
	reg.AuthAttempt(ports.AuthMethodJWT, false, "custom-reason")
	reg.PacketReceived(" pingreq ")
	reg.PacketSent("suback")

	output := string(reg.Render(false))
	for _, line := range []string{
		`narumq_connections_total{protocol="MQTT_5_0",transport="ws",result="success"} 1`,
		`narumq_connections_total{protocol="MQTT_5_0",transport="ws",result="closed"} 1`,
		`narumq_connections_total{protocol="MQTT_3_1_1",transport="wss",result="success"} 1`,
		`narumq_auth_attempts_total{method="tls_cert",result="success",reason="none"} 1`,
		`narumq_auth_attempts_total{method="jwt",result="failure",reason="invalid_credentials"} 1`,
		`narumq_auth_attempts_total{method="jwt",result="failure",reason="other"} 1`,
		`narumq_packets_received_total{packet_type="PINGREQ"} 1`,
		`narumq_packets_sent_total{packet_type="SUBACK"} 1`,
		"narumq_connections_active 1",
	} {
		if !strings.Contains(output, line+"\n") {
			t.Errorf("rendered output missing line %q\n%s", line, output)
		}
	}
}

func TestCmmRegistryClampsNegativeGauges(t *testing.T) {
	tests := []struct {
		name  string
		apply func(*Registry)
		lines []string
	}{
		{
			name: "persistent sessions",
			apply: func(r *Registry) {
				r.SetSessionsActive(false, 4)
				r.SetSessionsActive(true, -2)
			},
			lines: []string{`narumq_sessions_active{persistent="true"} 0`, `narumq_sessions_active{persistent="false"} 4`},
		},
		{
			name: "non-persistent sessions",
			apply: func(r *Registry) {
				r.SetSessionsActive(true, 3)
				r.SetSessionsActive(false, -1)
			},
			lines: []string{`narumq_sessions_active{persistent="true"} 3`, `narumq_sessions_active{persistent="false"} 0`},
		},
		{
			name:  "retained message count",
			apply: func(r *Registry) { r.SetRetainedStats(-1, 128) },
			lines: []string{"narumq_retained_messages 0", "narumq_retained_bytes 128"},
		},
		{
			name:  "retained bytes",
			apply: func(r *Registry) { r.SetRetainedStats(7, -128) },
			lines: []string{"narumq_retained_messages 7", "narumq_retained_bytes 0"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			reg := NewRegistry()
			test.apply(reg)
			output := string(reg.Render(true))
			for _, line := range test.lines {
				if !strings.Contains(output, line+"\n") {
					t.Errorf("rendered output missing line %q\n%s", line, output)
				}
			}
		})
	}
}

func TestCmmRegistryClampsNegativePublishDuration(t *testing.T) {
	reg := NewRegistry()
	reg.ObservePublishDuration(-time.Second)

	output := string(reg.Render(true))
	for _, line := range []string{
		`narumq_publish_latency_seconds_bucket{le="0.0005"} 1`,
		"narumq_publish_latency_seconds_sum 0.000000",
		"narumq_publish_latency_seconds_count 1",
	} {
		if !strings.Contains(output, line+"\n") {
			t.Errorf("rendered output missing line %q\n%s", line, output)
		}
	}
}
