package metrics

import (
	"errors"
	"testing"
	"time"

	ports "narumq/internal/application/ports/metrics"
)

type fixedSubscriptionCounter int

func (counter fixedSubscriptionCounter) SubscriptionCount() int { return int(counter) }

// scriptedFailureVisitor counts every visitor callback and fails exactly the failAt-th one.
// A failAt of zero never fails, which makes the first pass a full-traversal call count.
type scriptedFailureVisitor struct {
	failAt int
	err    error
	calls  int
}

func (v *scriptedFailureVisitor) step() error {
	v.calls++
	if v.calls == v.failAt {
		return v.err
	}
	return nil
}

func (v *scriptedFailureVisitor) BeginFamily(ports.MetricDescriptorDTO) error { return v.step() }
func (v *scriptedFailureVisitor) Sample(ports.MetricSampleDTO) error          { return v.step() }
func (v *scriptedFailureVisitor) EndFamily() error                            { return v.step() }

// newPopulatedMetricsRegistry gives every dynamic or multi-sample family at least two
// entries, so a failure at any sample can land before the family's last sample.
func newPopulatedMetricsRegistry() *Registry {
	registry := NewRegistry()
	registry.ConnectionOpened(ports.ProtocolMQTT311, ports.TransportTCP)
	registry.ConnectionOpened(ports.ProtocolMQTT50, ports.TransportTLS)
	registry.ConnectionClosed(ports.ProtocolMQTT50, ports.TransportTLS)
	registry.PacketReceived("CONNECT")
	registry.PacketReceived("PUBLISH")
	registry.PacketSent("CONNACK")
	registry.PacketSent("PUBACK")
	registry.AuthAttempt(ports.AuthMethodPassword, true, "")
	registry.AuthAttempt(ports.AuthMethodPassword, false, "invalid_credentials")
	registry.AuthAttempt(ports.AuthMethodJWT, false, "blocked")
	registry.NodeDrainTransition("EXCLUDING_TRAFFIC")
	registry.NodeDrainTransition("COMPLETED")
	registry.RecordTenantQuotaRejection("acme", "retained_bytes")
	registry.RecordTenantQuotaRejection("beta", "storage_bytes")
	registry.RecordTenantLimitRejection("acme", "connections")
	registry.RecordTenantLimitRejection("beta", "sessions")
	registry.SetPeerReplicationLag(map[uint64]uint64{2: 3, 3: 4})
	registry.SetStorageFreeBytes(1 << 30)
	registry.ObserveSubscriptions(fixedSubscriptionCounter(5))
	registry.ObservePublishDuration(2 * time.Millisecond)
	registry.ObserveWALSync(time.Millisecond, 3, 128)
	registry.ObserveRaftStage(RaftInboundQueue, time.Millisecond)
	return registry
}

func TestRegistryVisitMetricsStopsAtTheFirstVisitorFailureAnywhereInTheTraversal(t *testing.T) {
	t.Parallel()

	registry := newPopulatedMetricsRegistry()
	count := scriptedFailureVisitor{}
	if err := registry.VisitMetrics(&count); err != nil {
		t.Fatalf("full traversal: %v", err)
	}
	if count.calls < 100 {
		t.Fatalf("full traversal made %d visitor calls, want the populated registry to exercise every family", count.calls)
	}

	wantErr := errors.New("visitor stopped")
	for failAt := 1; failAt <= count.calls; failAt++ {
		visitor := scriptedFailureVisitor{failAt: failAt, err: wantErr}
		if err := registry.VisitMetrics(&visitor); !errors.Is(err, wantErr) {
			t.Fatalf("failure at visitor call %d: VisitMetrics() error = %v, want %v", failAt, err, wantErr)
		}
		if visitor.calls != failAt {
			t.Fatalf("failure at visitor call %d: visitor was called %d times, want traversal to stop immediately", failAt, visitor.calls)
		}
	}
}

func TestRegistryVisitMetricsRejectsNilRegistryAndNilVisitor(t *testing.T) {
	t.Parallel()

	var nilRegistry *Registry
	if err := nilRegistry.VisitMetrics(&scriptedFailureVisitor{}); !errors.Is(err, errNilMetricRegistry) {
		t.Fatalf("nil registry VisitMetrics() error = %v, want %v", err, errNilMetricRegistry)
	}
	if err := NewRegistry().VisitMetrics(nil); !errors.Is(err, errNilMetricVisitor) {
		t.Fatalf("nil visitor VisitMetrics() error = %v, want %v", err, errNilMetricVisitor)
	}
	snapshot, err := nilRegistry.CaptureMetrics()
	if !errors.Is(err, errNilMetricRegistry) {
		t.Fatalf("nil registry CaptureMetrics() error = %v, want %v", err, errNilMetricRegistry)
	}
	if snapshot.Families != nil {
		t.Fatalf("nil registry CaptureMetrics() returned families alongside the error: %+v", snapshot)
	}
}

func TestRegistryCaptureMetricsFailsClosedOnCatalogFamilyWithoutExporter(t *testing.T) {
	t.Parallel()

	registry := NewRegistry()
	registry.metricCatalog = append(append([]ports.MetricDescriptorDTO(nil), registry.metricCatalog...),
		ports.MetricDescriptorDTO{Name: "narumq_family_without_exporter"})

	visitor := scriptedFailureVisitor{}
	if err := registry.VisitMetrics(&visitor); err == nil {
		t.Fatal("VisitMetrics() accepted a catalog family that has no exporter")
	}
	snapshot, err := registry.CaptureMetrics()
	if err == nil {
		t.Fatal("CaptureMetrics() accepted a catalog family that has no exporter")
	}
	if snapshot.Families != nil {
		t.Fatalf("CaptureMetrics() returned a partial snapshot with the error: %d families", len(snapshot.Families))
	}
}

func TestRegistryCaptureMetricsOrdersAuthAttemptsByMethodThenResultThenReason(t *testing.T) {
	t.Parallel()

	registry := NewRegistry()
	registry.AuthAttempt(ports.AuthMethodPassword, true, "")
	registry.AuthAttempt(ports.AuthMethodPassword, false, "invalid_credentials")
	registry.AuthAttempt(ports.AuthMethodJWT, true, "")
	registry.AuthAttempt(ports.AuthMethodPassword, false, "blocked")

	snapshot, err := registry.CaptureMetrics()
	if err != nil {
		t.Fatal(err)
	}
	family := findMetricFamily(t, snapshot, "narumq_auth_attempts_total")
	want := []string{
		"jwt|success|none",
		"password|failure|blocked",
		"password|failure|invalid_credentials",
		"password|success|none",
	}
	if len(family.Samples) != len(want) {
		t.Fatalf("auth attempt samples = %d, want %d", len(family.Samples), len(want))
	}
	for index, sample := range family.Samples {
		values := map[string]string{}
		for _, label := range sample.Labels {
			values[label.Name] = label.Value
		}
		got := values["method"] + "|" + values["result"] + "|" + values["reason"]
		if got != want[index] {
			t.Fatalf("auth attempt sample %d = %s, want %s", index, got, want[index])
		}
		if sample.Value.Uint64 != 1 {
			t.Fatalf("auth attempt sample %d count = %d, want 1", index, sample.Value.Uint64)
		}
	}
}
