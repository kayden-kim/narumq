package audit

import (
	"errors"
	"strings"
	"testing"
	"time"

	portsaudit "narumq/internal/application/ports/audit"
	portsmetrics "narumq/internal/application/ports/metrics"
)

type auditTestClock struct{ now time.Time }

func (clock *auditTestClock) Now() time.Time { return clock.now }

type auditTestPublisher struct {
	readyErr   error
	publishErr error
	events     []portsaudit.AuditEventDTO
}

func (publisher *auditTestPublisher) Ready() error { return publisher.readyErr }

func (publisher *auditTestPublisher) Publish(event portsaudit.AuditEventDTO) error {
	if publisher.publishErr != nil {
		return publisher.publishErr
	}
	publisher.events = append(publisher.events, event)
	return nil
}

type auditTestMetrics struct {
	outcomes map[string]int
	ready    bool
}

func (metrics *auditTestMetrics) AuditOutcome(outcome string) {
	if metrics.outcomes == nil {
		metrics.outcomes = make(map[string]int)
	}
	metrics.outcomes[outcome]++
}

func (metrics *auditTestMetrics) SetAuditPublisherReady(ready bool) { metrics.ready = ready }

func newAuditTestService(t *testing.T, publisher *auditTestPublisher, clock *auditTestClock, rate int) (*Service, *auditTestMetrics) {
	t.Helper()
	metrics := &auditTestMetrics{}
	service, err := NewService(ServiceConfig{
		ClusterID:             "test-cluster",
		NodeID:                4,
		ReleaseVersion:        "1.2.3",
		DenialEventsPerSecond: rate,
	}, publisher, clock, metrics)
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}
	return service, metrics
}

func validAuditCommand() RecordCommandDTO {
	return RecordCommandDTO{
		RequestID: "request-1",
		Actor:     portsaudit.AuditActorDTO{Subject: "management-bearer-token", Roles: []string{"Operator"}},
		Action:    "node.drain.start",
		Target:    portsaudit.AuditTargetDTO{Type: "node", ID: "3"},
		Result:    portsaudit.AuditResultAllowed,
		Reason:    "succeeded",
		Details:   map[string]any{"httpStatus": 202, "method": "POST"},
	}
}

func TestNewServiceRejectsInvalidConfigurationAndDependencies(t *testing.T) {
	base := ServiceConfig{ClusterID: "cluster", NodeID: 1, ReleaseVersion: "1.0.0", DenialEventsPerSecond: 20}
	publisher := &auditTestPublisher{}
	clock := &auditTestClock{now: time.Now()}
	metrics := &auditTestMetrics{}
	tests := []struct {
		name   string
		config ServiceConfig
		pub    *auditTestPublisher
		clock  *auditTestClock
		stats  *auditTestMetrics
	}{
		{name: "empty cluster", config: ServiceConfig{NodeID: base.NodeID, ReleaseVersion: base.ReleaseVersion, DenialEventsPerSecond: base.DenialEventsPerSecond}, pub: publisher, clock: clock, stats: metrics},
		{name: "zero node", config: ServiceConfig{ClusterID: base.ClusterID, ReleaseVersion: base.ReleaseVersion, DenialEventsPerSecond: base.DenialEventsPerSecond}, pub: publisher, clock: clock, stats: metrics},
		{name: "empty version", config: ServiceConfig{ClusterID: base.ClusterID, NodeID: base.NodeID, DenialEventsPerSecond: base.DenialEventsPerSecond}, pub: publisher, clock: clock, stats: metrics},
		{name: "long version", config: ServiceConfig{ClusterID: base.ClusterID, NodeID: base.NodeID, ReleaseVersion: strings.Repeat("a", 65), DenialEventsPerSecond: base.DenialEventsPerSecond}, pub: publisher, clock: clock, stats: metrics},
		{name: "invalid UTF-8 version", config: ServiceConfig{ClusterID: base.ClusterID, NodeID: base.NodeID, ReleaseVersion: string([]byte{0xff}), DenialEventsPerSecond: base.DenialEventsPerSecond}, pub: publisher, clock: clock, stats: metrics},
		{name: "zero denial limit", config: ServiceConfig{ClusterID: base.ClusterID, NodeID: base.NodeID, ReleaseVersion: base.ReleaseVersion}, pub: publisher, clock: clock, stats: metrics},
		{name: "nil publisher", config: base, clock: clock, stats: metrics},
		{name: "nil clock", config: base, pub: publisher, stats: metrics},
		{name: "nil metrics", config: base, pub: publisher, clock: clock},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if service, err := NewService(test.config, test.pub, test.clock, test.stats); err == nil || service != nil {
				t.Fatalf("NewService() = (%v, %v), want configuration error", service, err)
			}
		})
	}
}

func TestRecordBuildsAndPublishesSchemaEvent(t *testing.T) {
	clock := &auditTestClock{now: time.Date(2026, 10, 8, 12, 30, 0, 0, time.FixedZone("test", 2*60*60))}
	publisher := &auditTestPublisher{}
	service, metrics := newAuditTestService(t, publisher, clock, 20)

	if err := service.Record(validAuditCommand()); err != nil {
		t.Fatalf("Record() error = %v", err)
	}
	if len(publisher.events) != 1 {
		t.Fatalf("published events = %d, want 1", len(publisher.events))
	}
	event := publisher.events[0]
	if event.SchemaVersion != "1.0" || event.Producer != (portsaudit.AuditProducerDTO{Component: "BROKER", InstanceID: "test-cluster/4", Version: "1.2.3"}) {
		t.Fatalf("event schema/producer = %q/%+v", event.SchemaVersion, event.Producer)
	}
	if event.OccurredAt.Location() != time.UTC || !event.OccurredAt.Equal(clock.now.UTC()) {
		t.Fatalf("occurredAt = %v, want UTC %v", event.OccurredAt, clock.now.UTC())
	}
	if len(event.EventID) != 36 || event.EventID[8] != '-' || event.EventID[13] != '-' || event.EventID[14] != '4' || event.EventID[18] != '-' || event.EventID[19] < '8' || event.EventID[19] > 'b' || event.EventID[23] != '-' {
		t.Fatalf("eventId = %q, want UUIDv4", event.EventID)
	}
	if metrics.outcomes[portsmetrics.AuditOutcomePublished] != 1 || !metrics.ready {
		t.Fatalf("metrics = %+v, ready = %t", metrics.outcomes, metrics.ready)
	}
}

func TestRecordRejectsInvalidEventWithoutPublishing(t *testing.T) {
	service, metrics := newAuditTestService(t, &auditTestPublisher{}, &auditTestClock{now: time.Now()}, 20)
	command := validAuditCommand()
	command.Action = "Node.Drain"
	if err := service.Record(command); !errors.Is(err, ErrInvalidEvent) {
		t.Fatalf("Record() error = %v, want ErrInvalidEvent", err)
	}
	if metrics.outcomes[portsmetrics.AuditOutcomeInvalid] != 1 {
		t.Fatalf("invalid outcome count = %d, want 1", metrics.outcomes[portsmetrics.AuditOutcomeInvalid])
	}
}

func TestRecordPublishFailureMarksPublisherUnavailable(t *testing.T) {
	publishErr := errors.New("disk sync failed")
	publisher := &auditTestPublisher{publishErr: publishErr}
	service, metrics := newAuditTestService(t, publisher, &auditTestClock{now: time.Now()}, 20)

	if err := service.Record(validAuditCommand()); !errors.Is(err, publishErr) {
		t.Fatalf("Record() error = %v, want wrapped publish error", err)
	}
	if metrics.outcomes[portsmetrics.AuditOutcomePublishFailed] != 1 || metrics.ready {
		t.Fatalf("metrics = %+v, ready = %t", metrics.outcomes, metrics.ready)
	}
}

func TestAdmitMutationRejectsUnavailablePublisher(t *testing.T) {
	readyErr := errors.New("publisher open failed")
	publisher := &auditTestPublisher{readyErr: readyErr}
	service, metrics := newAuditTestService(t, publisher, &auditTestClock{now: time.Now()}, 20)

	if err := service.AdmitMutation(); !errors.Is(err, ErrPublisherUnavailable) {
		t.Fatalf("AdmitMutation() error = %v, want ErrPublisherUnavailable", err)
	}
	if metrics.outcomes[portsmetrics.AuditOutcomeMutationRejected] != 1 || metrics.ready {
		t.Fatalf("metrics = %+v, ready = %t", metrics.outcomes, metrics.ready)
	}
}

func TestRecordLimitsDenialsPerSecondOnly(t *testing.T) {
	clock := &auditTestClock{now: time.Unix(100, 0).UTC()}
	publisher := &auditTestPublisher{}
	service, metrics := newAuditTestService(t, publisher, clock, 20)
	denied := validAuditCommand()
	denied.Result = portsaudit.AuditResultDenied
	denied.Reason = "forbidden"

	for index := 0; index < 20; index++ {
		if err := service.Record(denied); err != nil {
			t.Fatalf("Record(denied %d) error = %v", index, err)
		}
	}
	if err := service.Record(denied); !errors.Is(err, ErrDenialSuppressed) {
		t.Fatalf("Record(denied 21) error = %v, want ErrDenialSuppressed", err)
	}
	clock.now = clock.now.Add(time.Second)
	if err := service.Record(denied); err != nil {
		t.Fatalf("Record(denied in next second) error = %v", err)
	}
	failed := validAuditCommand()
	failed.Result = portsaudit.AuditResultFailed
	failed.Reason = "internal_error"
	for index := 0; index < 21; index++ {
		if err := service.Record(failed); err != nil {
			t.Fatalf("Record(failed %d) error = %v", index, err)
		}
	}
	if got, want := len(publisher.events), 42; got != want {
		t.Fatalf("published events = %d, want %d", got, want)
	}
	if metrics.outcomes[portsmetrics.AuditOutcomeDenialSuppressed] != 1 {
		t.Fatalf("denial_suppressed count = %d, want 1", metrics.outcomes[portsmetrics.AuditOutcomeDenialSuppressed])
	}
}

type auditValidationEmbeddedDetails struct {
	Value string `json:"value"`
}

type auditValidationEmbeddedWrapper struct {
	auditValidationEmbeddedDetails
}

func TestValidateEventSchemaBoundaries(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*RecordCommandDTO)
		valid  bool
	}{
		{name: "request ID 128", mutate: func(command *RecordCommandDTO) { command.RequestID = strings.Repeat("a", 128) }, valid: true},
		{name: "request ID 129", mutate: func(command *RecordCommandDTO) { command.RequestID = strings.Repeat("a", 129) }},
		{name: "target ID 512", mutate: func(command *RecordCommandDTO) { command.Target.ID = strings.Repeat("a", 512) }, valid: true},
		{name: "target ID 513", mutate: func(command *RecordCommandDTO) { command.Target.ID = strings.Repeat("a", 513) }},
		{name: "duplicate roles", mutate: func(command *RecordCommandDTO) { command.Actor.Roles = []string{"Operator", "Operator"} }},
		{name: "uppercase reason", mutate: func(command *RecordCommandDTO) { command.Reason = "Forbidden" }},
		{name: "invalid nested UTF-8 value", mutate: func(command *RecordCommandDTO) {
			command.Details["nested"] = []any{map[string]any{"value": string([]byte{0xff})}}
		}},
		{name: "invalid nested UTF-8 map key", mutate: func(command *RecordCommandDTO) {
			command.Details["nested"] = map[string]any{string([]byte{0xff}): "value"}
		}},
		{name: "invalid typed nested UTF-8 value", mutate: func(command *RecordCommandDTO) {
			command.Details["nested"] = map[string][]string{"values": {string([]byte{0xff})}}
		}},
		{name: "invalid UTF-8 in promoted field of unexported embedded struct", mutate: func(command *RecordCommandDTO) {
			command.Details["nested"] = auditValidationEmbeddedWrapper{
				auditValidationEmbeddedDetails{Value: string([]byte{0xff})},
			}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			command := validAuditCommand()
			test.mutate(&command)
			publisher := &auditTestPublisher{}
			service, _ := newAuditTestService(t, publisher, &auditTestClock{now: time.Now()}, 20)
			err := service.Record(command)
			if test.valid && err != nil {
				t.Fatalf("Record() error = %v, want success", err)
			}
			if !test.valid && !errors.Is(err, ErrInvalidEvent) {
				t.Fatalf("Record() error = %v, want ErrInvalidEvent", err)
			}
			wantPublished := 0
			if test.valid {
				wantPublished = 1
			}
			if len(publisher.events) != wantPublished {
				t.Fatalf("published events = %d, want %d", len(publisher.events), wantPublished)
			}
		})
	}
}
