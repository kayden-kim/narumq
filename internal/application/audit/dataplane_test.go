package audit

import (
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	portsaudit "narumq/internal/application/ports/audit"
	portsmetrics "narumq/internal/application/ports/metrics"
	"narumq/internal/domain/security"
	"narumq/internal/domain/tenancy"
)

type dataPlaneTestMetrics struct {
	mu       sync.Mutex
	outcomes map[string]int
	ready    bool
}

func (metrics *dataPlaneTestMetrics) AuditOutcome(outcome string) {
	metrics.mu.Lock()
	defer metrics.mu.Unlock()
	if metrics.outcomes == nil {
		metrics.outcomes = make(map[string]int)
	}
	metrics.outcomes[outcome]++
}

func (metrics *dataPlaneTestMetrics) SetAuditPublisherReady(ready bool) {
	metrics.mu.Lock()
	metrics.ready = ready
	metrics.mu.Unlock()
}

func (metrics *dataPlaneTestMetrics) outcome(outcome string) int {
	metrics.mu.Lock()
	defer metrics.mu.Unlock()
	return metrics.outcomes[outcome]
}

func (metrics *dataPlaneTestMetrics) isReady() bool {
	metrics.mu.Lock()
	defer metrics.mu.Unlock()
	return metrics.ready
}

type dataPlaneTestPublisher struct {
	mu         sync.Mutex
	events     []portsaudit.AuditEventDTO
	publishErr error
	entered    chan struct{}
	release    chan struct{}
	blockOnce  sync.Once
}

func (publisher *dataPlaneTestPublisher) Ready() error { return nil }

func (publisher *dataPlaneTestPublisher) Publish(event portsaudit.AuditEventDTO) error {
	publisher.mu.Lock()
	publisher.events = append(publisher.events, event)
	publisher.mu.Unlock()
	if publisher.entered != nil {
		publisher.blockOnce.Do(func() {
			publisher.entered <- struct{}{}
			<-publisher.release
		})
	}
	return publisher.publishErr
}

func (publisher *dataPlaneTestPublisher) snapshot() []portsaudit.AuditEventDTO {
	publisher.mu.Lock()
	defer publisher.mu.Unlock()
	return append([]portsaudit.AuditEventDTO(nil), publisher.events...)
}

func newDataPlaneTestRecorder(t *testing.T, publisher portsaudit.AuditPublisher, clock *auditTestClock, metrics *dataPlaneTestMetrics, rate, capacity int) *DataPlaneRecorder {
	t.Helper()
	recorder, err := NewDataPlaneRecorder(DataPlaneRecorderConfig{
		ClusterID: "test-cluster", NodeID: 4, ReleaseVersion: "1.2.3",
		EventsPerSecond: rate, QueueCapacity: capacity,
	}, publisher, clock, metrics)
	if err != nil {
		t.Fatalf("NewDataPlaneRecorder() error = %v", err)
	}
	return recorder
}

func validDataPlaneDenial(action portsaudit.DataPlaneAction) portsaudit.DataPlaneDenialDTO {
	return portsaudit.DataPlaneDenialDTO{
		Action: action, Reason: "not_authorized", TenantID: "tenant-a", Subject: "mqtt-user",
		CredentialID: "credential-1", Roles: []string{"Publisher"}, ClientID: "client-a",
		SourceIP: "192.0.2.10", Topic: "sensor/reading",
	}
}

// Traceability: SEC-11/NFR-17; docs/specs/tenancy-and-security.md §8.
func TestDataPlaneRecorderLimitsEventsPerSecond(t *testing.T) {
	clock := &auditTestClock{now: time.Unix(100, 0).UTC()}
	publisher := &dataPlaneTestPublisher{}
	metrics := &dataPlaneTestMetrics{}
	recorder := newDataPlaneTestRecorder(t, publisher, clock, metrics, 20, 24)
	recorder.Start()
	for range 21 {
		recorder.RecordDataPlaneDenial(validDataPlaneDenial(portsaudit.DataPlaneActionPublish))
	}
	if got := metrics.outcome(portsmetrics.AuditOutcomeDataPlaneSuppressed); got != 1 {
		t.Fatalf("suppressed outcomes = %d, want 1", got)
	}
	clock.now = clock.now.Add(time.Second)
	recorder.RecordDataPlaneDenial(validDataPlaneDenial(portsaudit.DataPlaneActionPublish))
	recorder.Close()
	if got := len(publisher.snapshot()); got != 21 {
		t.Fatalf("published events = %d, want 21", got)
	}
	if got := metrics.outcome(portsmetrics.AuditOutcomePublished); got != 21 {
		t.Fatalf("published outcomes = %d, want 21", got)
	}
}

// Traceability: SEC-11/NFR-17; docs/specs/tenancy-and-security.md §8.
func TestDataPlaneRecorderDropsWhenQueueIsFullAndDrainsOnClose(t *testing.T) {
	clock := &auditTestClock{now: time.Unix(100, 0).UTC()}
	publisher := &dataPlaneTestPublisher{entered: make(chan struct{}, 1), release: make(chan struct{})}
	metrics := &dataPlaneTestMetrics{}
	recorder := newDataPlaneTestRecorder(t, publisher, clock, metrics, 10, 1)
	recorder.Start()
	recorder.Start()
	recorder.RecordDataPlaneDenial(validDataPlaneDenial(portsaudit.DataPlaneActionPublish))
	select {
	case <-publisher.entered:
	case <-time.After(time.Second):
		t.Fatal("publisher worker did not start")
	}
	recorder.RecordDataPlaneDenial(validDataPlaneDenial(portsaudit.DataPlaneActionPublish))
	recorder.RecordDataPlaneDenial(validDataPlaneDenial(portsaudit.DataPlaneActionPublish))
	if got := metrics.outcome(portsmetrics.AuditOutcomeDataPlaneDropped); got != 1 {
		t.Fatalf("dropped outcomes before Close() = %d, want 1", got)
	}

	closed := make(chan struct{})
	go func() {
		recorder.Close()
		close(closed)
	}()
	select {
	case <-closed:
		t.Fatal("Close() returned before the blocked publish completed")
	case <-time.After(20 * time.Millisecond):
	}
	close(publisher.release)
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("Close() did not finish after the publisher was released")
	}
	recorder.Close()
	recorder.RecordDataPlaneDenial(validDataPlaneDenial(portsaudit.DataPlaneActionPublish))
	if got := len(publisher.snapshot()); got != 2 {
		t.Fatalf("published events = %d, want queue drained to 2", got)
	}
	if got := metrics.outcome(portsmetrics.AuditOutcomeDataPlaneDropped); got != 2 {
		t.Fatalf("total dropped outcomes = %d, want queue-full and post-close events", got)
	}
}

// Traceability: SEC-11/NFR-17; docs/specs/tenancy-and-security.md §8.
func TestDataPlaneRecorderDropsQueuedEventsWhenClosedBeforeStart(t *testing.T) {
	clock := &auditTestClock{now: time.Unix(100, 0).UTC()}
	publisher := &dataPlaneTestPublisher{}
	metrics := &dataPlaneTestMetrics{}
	recorder := newDataPlaneTestRecorder(t, publisher, clock, metrics, 10, 4)
	for range 2 {
		recorder.RecordDataPlaneDenial(validDataPlaneDenial(portsaudit.DataPlaneActionPublish))
	}
	recorder.Close()
	recorder.Close()
	recorder.Start()
	recorder.RecordDataPlaneDenial(validDataPlaneDenial(portsaudit.DataPlaneActionPublish))
	if got := len(publisher.snapshot()); got != 0 {
		t.Fatalf("published events = %d, want 0", got)
	}
	if got := metrics.outcome(portsmetrics.AuditOutcomeDataPlaneDropped); got != 3 {
		t.Fatalf("dropped outcomes = %d, want 2 queued and 1 post-close", got)
	}
}

// Traceability: SEC-11/NFR-17; docs/specs/tenancy-and-security.md §8.
func TestDataPlaneRecorderPublishFailureMarksPublisherUnavailable(t *testing.T) {
	clock := &auditTestClock{now: time.Unix(100, 0).UTC()}
	publisher := &dataPlaneTestPublisher{publishErr: errors.New("disk sync failed")}
	metrics := &dataPlaneTestMetrics{}
	recorder := newDataPlaneTestRecorder(t, publisher, clock, metrics, 10, 4)
	recorder.Start()
	recorder.RecordDataPlaneDenial(validDataPlaneDenial(portsaudit.DataPlaneActionPublish))
	recorder.Close()
	if got := metrics.outcome(portsmetrics.AuditOutcomePublishFailed); got != 1 || metrics.isReady() {
		t.Fatalf("publish metrics = failed:%d ready:%t, want failed:1 ready:false", got, metrics.isReady())
	}
}

// Traceability: SEC-11/NFR-17; docs/specs/tenancy-and-security.md §8.
func TestDataPlaneRecorderMapsAndTruncatesDenialFields(t *testing.T) {
	clock := &auditTestClock{now: time.Unix(100, 0).UTC()}
	publisher := &dataPlaneTestPublisher{}
	metrics := &dataPlaneTestMetrics{}
	recorder := newDataPlaneTestRecorder(t, publisher, clock, metrics, 20, 8)
	recorder.Start()
	unauthenticated := portsaudit.DataPlaneDenialDTO{
		Action: portsaudit.DataPlaneActionConnect, Reason: "invalid_credentials", ClientID: "bad-client",
		SourceIP: "192.0.2.10", AuthMethod: "jwt",
	}
	recorder.RecordDataPlaneDenial(unauthenticated)

	tenantID, err := tenancy.NewTenantID("tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	longRoleA := strings.Repeat("r", 64) + "a"
	longRoleB := strings.Repeat("r", 64) + "b"
	principal, err := security.NewPrincipal(tenantID, "client-a", "mqtt-user", "credential-1", []security.Role{security.Role(longRoleA), security.Role(longRoleB)}, time.Unix(200, 0))
	if err != nil {
		t.Fatal(err)
	}
	topic := strings.Repeat("界", 600)
	actions := []portsaudit.DataPlaneAction{
		portsaudit.DataPlaneActionPublish,
		portsaudit.DataPlaneActionSubscribe,
		portsaudit.DataPlaneActionUnsubscribe,
		portsaudit.DataPlaneActionWill,
	}
	for _, action := range actions {
		recorder.RecordDataPlaneDenial(ACLDenial(action, principal, topic, "198.51.100.4"))
	}
	recorder.Close()

	events := publisher.snapshot()
	if len(events) != 5 {
		t.Fatalf("published events = %d, want 5", len(events))
	}
	connectEvent := events[0]
	if connectEvent.Action != string(portsaudit.DataPlaneActionConnect) || connectEvent.Actor.Subject != "unauthenticated" ||
		len(connectEvent.Actor.Roles) != 0 || connectEvent.Target.Type != "mqtt_client" || connectEvent.Target.ID != "bad-client" ||
		connectEvent.Details["authMethod"] != "jwt" || connectEvent.SourceIP != "192.0.2.10" {
		t.Fatalf("unauthenticated connect event = %+v", connectEvent)
	}
	wantTypes := []string{"mqtt_topic", "mqtt_topic_filter", "mqtt_topic_filter", "mqtt_topic"}
	for index, event := range events[1:] {
		if event.Action != string(actions[index]) || event.Target.Type != wantTypes[index] || event.Target.ID != strings.Repeat("界", 512) {
			t.Fatalf("event %d target/action = %s/%s/%q", index+1, event.Action, event.Target.Type, event.Target.ID)
		}
		if event.Actor.Subject != "mqtt-user" || event.Actor.TenantID != "tenant-a" || event.Actor.CredentialID != "credential-1" ||
			len(event.Actor.Roles) != 1 || event.Actor.Roles[0] != strings.Repeat("r", 64) {
			t.Fatalf("event %d actor = %+v", index+1, event.Actor)
		}
		if event.Target.TenantID != "tenant-a" || event.SourceIP != "198.51.100.4" || event.Details["truncated"] != true ||
			event.Details["clientId"] != "client-a" || event.RequestID != event.EventID || event.PolicyVersion != "" {
			t.Fatalf("event %d common fields = %+v", index+1, event)
		}
	}
}

// Traceability: SEC-11/NFR-17; docs/specs/tenancy-and-security.md §8.
func TestDataPlaneRecorderRejectsInvalidReason(t *testing.T) {
	clock := &auditTestClock{now: time.Unix(100, 0).UTC()}
	publisher := &dataPlaneTestPublisher{}
	metrics := &dataPlaneTestMetrics{}
	recorder := newDataPlaneTestRecorder(t, publisher, clock, metrics, 10, 4)
	recorder.RecordDataPlaneDenial(portsaudit.DataPlaneDenialDTO{
		Action: portsaudit.DataPlaneActionConnect, Reason: "Not Authorized", ClientID: "bad-client", AuthMethod: "jwt",
	})
	recorder.Close()
	if got := metrics.outcome(portsmetrics.AuditOutcomeInvalid); got != 1 {
		t.Fatalf("invalid outcomes = %d, want 1", got)
	}
	if got := metrics.outcome(portsmetrics.AuditOutcomeDataPlaneDropped); got != 0 {
		t.Fatalf("dropped outcomes = %d, want 0", got)
	}
	if got := len(publisher.snapshot()); got != 0 {
		t.Fatalf("published events = %d, want 0", got)
	}
}
