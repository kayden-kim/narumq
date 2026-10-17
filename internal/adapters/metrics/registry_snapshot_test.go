package metrics

import (
	"errors"
	"testing"
	"time"

	ports "narumq/internal/application/ports/metrics"
)

func TestRegistryCaptureMetricsPreservesIntegerPrecisionAndOwnedLabels(t *testing.T) {
	t.Parallel()

	registry := NewRegistry()
	registry.walWrittenBytes.Store(uint64(1<<53) + 1)
	registry.RecordTenantQuotaRejection("acme", "retained_bytes")
	registry.RecordTenantQuotaRejection("beta", "storage_bytes")

	snapshot, err := registry.CaptureMetrics()
	if err != nil {
		t.Fatalf("CaptureMetrics(): %v", err)
	}

	walFamily := findMetricFamily(t, snapshot, "narumq_raft_wal_written_bytes_total")
	if len(walFamily.Samples) != 1 {
		t.Fatalf("WAL written bytes samples = %d, want 1", len(walFamily.Samples))
	}
	walSample := walFamily.Samples[0]
	if walSample.Value.Kind != ports.MetricValueInteger || walSample.Value.Uint64 != uint64(1<<53)+1 {
		t.Fatalf("WAL written bytes value = (%q, %d), want exact integer %d", walSample.Value.Kind, walSample.Value.Uint64, uint64(1<<53)+1)
	}

	quotaFamily := findMetricFamily(t, snapshot, "narumq_tenant_quota_rejections_total")
	if len(quotaFamily.Samples) != 2 {
		t.Fatalf("tenant quota samples = %d, want 2", len(quotaFamily.Samples))
	}
	for _, sample := range quotaFamily.Samples {
		if len(sample.Labels) != 2 || sample.Labels[0].Name != "dimension" || sample.Labels[1].Name != "tenant" {
			t.Fatalf("tenant quota labels = %#v, want sorted dimension and tenant labels", sample.Labels)
		}
	}
	if quotaFamily.Samples[0].Labels[1].Value != "acme" || quotaFamily.Samples[1].Labels[1].Value != "beta" {
		t.Fatalf("tenant samples are not deterministically ordered: %#v", quotaFamily.Samples)
	}

	quotaFamily.Samples[0].Labels[1].Value = "changed"
	again, err := registry.CaptureMetrics()
	if err != nil {
		t.Fatalf("second CaptureMetrics(): %v", err)
	}
	againQuota := findMetricFamily(t, again, "narumq_tenant_quota_rejections_total")
	if againQuota.Samples[0].Labels[1].Value != "acme" {
		t.Fatalf("mutating a captured snapshot changed a later capture: %#v", againQuota.Samples[0].Labels)
	}
}

func TestRegistryVisitMetricsReturnsVisitorErrors(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("visitor stopped")
	visitor := failingMetricVisitor{err: wantErr}
	if err := NewRegistry().VisitMetrics(&visitor); !errors.Is(err, wantErr) {
		t.Fatalf("VisitMetrics() error = %v, want %v", err, wantErr)
	}
	if visitor.beginCalls != 1 {
		t.Fatalf("BeginFamily calls = %d, want 1", visitor.beginCalls)
	}
}

func TestRegistryStartedAtIsSetToUTC(t *testing.T) {
	t.Parallel()

	startedAt := NewRegistry().StartedAt()
	if startedAt.IsZero() || startedAt.Location() != time.UTC {
		t.Fatalf("StartedAt() = %v, want non-zero UTC time", startedAt)
	}
}

type failingMetricVisitor struct {
	err        error
	beginCalls int
}

func (v *failingMetricVisitor) BeginFamily(ports.MetricDescriptorDTO) error {
	v.beginCalls++
	return v.err
}

func (*failingMetricVisitor) Sample(ports.MetricSampleDTO) error { return nil }
func (*failingMetricVisitor) EndFamily() error                   { return nil }

func findMetricFamily(t *testing.T, snapshot ports.MetricsSnapshotDTO, name string) ports.MetricFamilyDTO {
	t.Helper()
	for _, family := range snapshot.Families {
		if family.Descriptor.Name == name {
			return family
		}
	}
	t.Fatalf("snapshot has no metric family %q", name)
	return ports.MetricFamilyDTO{}
}
