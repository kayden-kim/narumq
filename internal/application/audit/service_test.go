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
	publisher := &auditTestPublisher{}
	service, metrics := newAuditTestService(t, publisher, &auditTestClock{now: time.Now()}, 20)
	command := validAuditCommand()
	command.Action = "Node.Drain"
	if err := service.Record(command); !errors.Is(err, ErrInvalidEvent) {
		t.Fatalf("Record() error = %v, want ErrInvalidEvent", err)
	}
	if len(publisher.events) != 0 {
		t.Fatalf("published events = %d, want 0", len(publisher.events))
	}
	if service.denials.initialized || service.denials.count != 0 {
		t.Fatalf("denial state = initialized:%t count:%d, want unchanged", service.denials.initialized, service.denials.count)
	}
	if metrics.outcomes[portsmetrics.AuditOutcomeInvalid] != 1 {
		t.Fatalf("invalid outcome count = %d, want 1", metrics.outcomes[portsmetrics.AuditOutcomeInvalid])
	}
}

func TestNewServiceRejectsNilInterfaceDependency(t *testing.T) {
	config := ServiceConfig{ClusterID: "cluster", NodeID: 1, ReleaseVersion: "1.0.0", DenialEventsPerSecond: 20}
	clock := &auditTestClock{now: time.Now()}
	metrics := &auditTestMetrics{}
	service, err := NewService(config, nil, clock, metrics)
	if err == nil || service != nil || !strings.Contains(err.Error(), "dependencies must not be nil") {
		t.Fatalf("NewService() = (%v, %v), want nil service and dependency error", service, err)
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
func TestRecordValidatesCommandFieldsAndBoundaries(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*RecordCommandDTO)
		valid  bool
	}{
		{name: "empty request ID", mutate: func(command *RecordCommandDTO) { command.RequestID = "" }},
		{name: "invalid UTF-8 request ID", mutate: func(command *RecordCommandDTO) { command.RequestID = string([]byte{0xff}) }},
		{name: "request ID exact rune limit", mutate: func(command *RecordCommandDTO) { command.RequestID = strings.Repeat("界", 128) }, valid: true},
		{name: "empty actor subject", mutate: func(command *RecordCommandDTO) { command.Actor.Subject = "" }},
		{name: "actor subject over limit", mutate: func(command *RecordCommandDTO) { command.Actor.Subject = strings.Repeat("a", 257) }},
		{name: "actor tenant exact rune limit", mutate: func(command *RecordCommandDTO) { command.Actor.TenantID = strings.Repeat("界", 128) }, valid: true},
		{name: "actor tenant over limit", mutate: func(command *RecordCommandDTO) { command.Actor.TenantID = strings.Repeat("a", 129) }},
		{name: "actor credential exact rune limit", mutate: func(command *RecordCommandDTO) { command.Actor.CredentialID = strings.Repeat("a", 256) }, valid: true},
		{name: "actor credential invalid UTF-8", mutate: func(command *RecordCommandDTO) { command.Actor.CredentialID = string([]byte{0xff}) }},
		{name: "empty role", mutate: func(command *RecordCommandDTO) { command.Actor.Roles = []string{""} }},
		{name: "role over limit", mutate: func(command *RecordCommandDTO) { command.Actor.Roles = []string{strings.Repeat("a", 65)} }},
		{name: "invalid UTF-8 role", mutate: func(command *RecordCommandDTO) { command.Actor.Roles = []string{string([]byte{0xff})} }},
		{name: "role exact rune limit", mutate: func(command *RecordCommandDTO) { command.Actor.Roles = []string{strings.Repeat("R", 64)} }, valid: true},
		{name: "action missing namespace", mutate: func(command *RecordCommandDTO) { command.Action = "drain" }},
		{name: "action invalid segment", mutate: func(command *RecordCommandDTO) { command.Action = "node.Drain.start" }},
		{name: "action exact rune limit", mutate: func(command *RecordCommandDTO) { command.Action = "a." + strings.Repeat("b", 126) }, valid: true},
		{name: "action over limit", mutate: func(command *RecordCommandDTO) { command.Action = "a." + strings.Repeat("b", 127) }},
		{name: "target type invalid format", mutate: func(command *RecordCommandDTO) { command.Target.Type = "Node-Type" }},
		{name: "target type exact limit", mutate: func(command *RecordCommandDTO) { command.Target.Type = strings.Repeat("n", 64) }, valid: true},
		{name: "target type over limit", mutate: func(command *RecordCommandDTO) { command.Target.Type = strings.Repeat("n", 65) }},
		{name: "target ID invalid UTF-8", mutate: func(command *RecordCommandDTO) { command.Target.ID = string([]byte{0xff}) }},
		{name: "target tenant exact rune limit", mutate: func(command *RecordCommandDTO) { command.Target.TenantID = strings.Repeat("界", 128) }, valid: true},
		{name: "target tenant over limit", mutate: func(command *RecordCommandDTO) { command.Target.TenantID = strings.Repeat("a", 129) }},
		{name: "unsupported result", mutate: func(command *RecordCommandDTO) { command.Result = portsaudit.AuditResult("PENDING") }},
		{name: "invalid reason format", mutate: func(command *RecordCommandDTO) { command.Reason = "not-allowed" }},
		{name: "reason exact rune limit", mutate: func(command *RecordCommandDTO) { command.Reason = strings.Repeat("a", 128) }, valid: true},
		{name: "reason over limit", mutate: func(command *RecordCommandDTO) { command.Reason = strings.Repeat("a", 129) }},
		{name: "details value cannot be JSON encoded", mutate: func(command *RecordCommandDTO) { command.Details["bad"] = make(chan int) }},
		{name: "details contains unsupported key type", mutate: func(command *RecordCommandDTO) { command.Details["bad"] = map[struct{}]string{{}: "value"} }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			command := validAuditCommand()
			test.mutate(&command)
			publisher := &auditTestPublisher{}
			service, _ := newAuditTestService(t, publisher, &auditTestClock{now: time.Date(2026, time.October, 8, 12, 0, 0, 0, time.UTC)}, 20)
			err := service.Record(command)
			wantPublished := 0
			if test.valid {
				if err != nil {
					t.Fatalf("Record() error = %v, want success", err)
				}
				wantPublished = 1
			} else if !errors.Is(err, ErrInvalidEvent) {
				t.Fatalf("Record() error = %v, want ErrInvalidEvent", err)
			}
			if len(publisher.events) != wantPublished {
				t.Fatalf("published events = %d, want %d (valid=%t)", len(publisher.events), wantPublished, test.valid)
			}
		})
	}
}

func TestValidateEventRequiredFields(t *testing.T) {
	validEvent := func() portsaudit.AuditEventDTO {
		return portsaudit.AuditEventDTO{
			SchemaVersion: "1.0",
			EventID:       "event-1",
			OccurredAt:    time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC),
			RequestID:     "request-1",
			Producer:      portsaudit.AuditProducerDTO{Component: "BROKER", InstanceID: "cluster/1", Version: "1.2.3"},
			Actor:         portsaudit.AuditActorDTO{Subject: "operator", Roles: []string{"Operator"}},
			Action:        "node.drain.start",
			Target:        portsaudit.AuditTargetDTO{Type: "node", ID: "1"},
			Result:        portsaudit.AuditResultAllowed,
			Reason:        "succeeded",
		}
	}
	tests := []struct {
		name      string
		mutate    func(*portsaudit.AuditEventDTO)
		wantError string
		valid     bool
	}{
		{name: "unsupported schema", mutate: func(event *portsaudit.AuditEventDTO) { event.SchemaVersion = "2.0" }, wantError: "schemaVersion must be 1.0"},
		{name: "empty event ID", mutate: func(event *portsaudit.AuditEventDTO) { event.EventID = "" }, wantError: "eventId must contain 1 to 128 valid UTF-8 characters"},
		{name: "event ID invalid UTF-8", mutate: func(event *portsaudit.AuditEventDTO) { event.EventID = string([]byte{0xff}) }, wantError: "eventId must contain 1 to 128 valid UTF-8 characters"},
		{name: "event ID over limit", mutate: func(event *portsaudit.AuditEventDTO) { event.EventID = strings.Repeat("a", 129) }, wantError: "eventId must contain 1 to 128 valid UTF-8 characters"},
		{name: "zero occurrence time", mutate: func(event *portsaudit.AuditEventDTO) { event.OccurredAt = time.Time{} }, wantError: "occurredAt must not be zero"},
		{name: "empty producer component", mutate: func(event *portsaudit.AuditEventDTO) { event.Producer.Component = "" }, wantError: "producer.component is not supported"},
		{name: "empty producer instance", mutate: func(event *portsaudit.AuditEventDTO) { event.Producer.InstanceID = "" }, wantError: "producer.instanceId must contain 1 to 256 valid UTF-8 characters"},
		{name: "producer version over limit", mutate: func(event *portsaudit.AuditEventDTO) { event.Producer.Version = strings.Repeat("a", 65) }, wantError: "producer.version must contain 1 to 64 valid UTF-8 characters"},
		{name: "empty actor subject", mutate: func(event *portsaudit.AuditEventDTO) { event.Actor.Subject = "" }, wantError: "actor.subject must contain 1 to 256 valid UTF-8 characters"},
		{name: "empty action", mutate: func(event *portsaudit.AuditEventDTO) { event.Action = "" }, wantError: "action does not match the audit action format"},
		{name: "empty target type", mutate: func(event *portsaudit.AuditEventDTO) { event.Target.Type = "" }, wantError: "target.type does not match the audit target format"},
		{name: "empty target ID", mutate: func(event *portsaudit.AuditEventDTO) { event.Target.ID = "" }, wantError: "target.id must contain 1 to 512 valid UTF-8 characters"},
		{name: "unsupported result", mutate: func(event *portsaudit.AuditEventDTO) { event.Result = portsaudit.AuditResult("UNKNOWN") }, wantError: "result is not supported"},
		{name: "policy version exact rune limit", mutate: func(event *portsaudit.AuditEventDTO) { event.PolicyVersion = strings.Repeat("界", 128) }, valid: true},
		{name: "policy version over limit", mutate: func(event *portsaudit.AuditEventDTO) { event.PolicyVersion = strings.Repeat("a", 129) }, wantError: "policyVersion must be empty or contain at most 128 valid UTF-8 characters"},
		{name: "policy version invalid UTF-8", mutate: func(event *portsaudit.AuditEventDTO) { event.PolicyVersion = string([]byte{0xff}) }, wantError: "policyVersion must be empty or contain at most 128 valid UTF-8 characters"},
		{name: "source IP exact rune limit", mutate: func(event *portsaudit.AuditEventDTO) { event.SourceIP = strings.Repeat("界", 128) }, valid: true},
		{name: "source IP over limit", mutate: func(event *portsaudit.AuditEventDTO) { event.SourceIP = strings.Repeat("a", 129) }, wantError: "sourceIp must be empty or contain at most 128 valid UTF-8 characters"},
		{name: "source IP invalid UTF-8", mutate: func(event *portsaudit.AuditEventDTO) { event.SourceIP = string([]byte{0xff}) }, wantError: "sourceIp must be empty or contain at most 128 valid UTF-8 characters"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			event := validEvent()
			test.mutate(&event)
			err := validateEvent(event)
			if test.valid {
				if err != nil {
					t.Fatalf("validateEvent() error = %v, want success", err)
				}
			} else if err == nil || err.Error() != test.wantError {
				t.Fatalf("validateEvent() error = %v, want %q", err, test.wantError)
			}
		})
	}
}

type auditValidationIgnoredDetails struct {
	Ignored string `json:"-"`
	hidden  string
}

type auditValidationEmbeddedPointerDetails struct {
	*auditValidationEmbeddedDetails
}

type auditValidationEmbeddedCount int

type auditValidationEmbeddedNonStructDetails struct {
	auditValidationEmbeddedCount
}

func TestValidateEventDetailsJSONStringTraversal(t *testing.T) {
	tests := []struct {
		name    string
		details any
		wantErr bool
	}{
		{name: "nil interface value in slice", details: []any{nil}},
		{name: "nil map", details: map[string]string(nil)},
		{name: "map with non-string keys", details: map[int]string{1: "valid"}},
		{name: "ignored and unexported fields", details: auditValidationIgnoredDetails{
			Ignored: string([]byte{0xff}),
			hidden:  string([]byte{0xff}),
		}},
		{name: "nil unexported embedded pointer", details: auditValidationEmbeddedPointerDetails{}},
		{name: "unexported embedded non-struct", details: auditValidationEmbeddedNonStructDetails{auditValidationEmbeddedCount: 1}},
		{name: "invalid UTF-8 in string map key", details: map[string]string{string([]byte{0xff}): "value"}, wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			event := portsaudit.AuditEventDTO{
				SchemaVersion: "1.0",
				EventID:       "event-1",
				OccurredAt:    time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC),
				RequestID:     "request-1",
				Producer:      portsaudit.AuditProducerDTO{Component: "BROKER", InstanceID: "cluster/1", Version: "1.2.3"},
				Actor:         portsaudit.AuditActorDTO{Subject: "operator"},
				Action:        "node.drain.start",
				Target:        portsaudit.AuditTargetDTO{Type: "node", ID: "1"},
				Result:        portsaudit.AuditResultAllowed,
				Reason:        "succeeded",
				Details:       map[string]any{"value": test.details},
			}
			err := validateEvent(event)
			if test.wantErr {
				if err == nil || !strings.Contains(err.Error(), "details contains invalid UTF-8") {
					t.Fatalf("validateEvent() error = %v, want invalid UTF-8 details rejection", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("validateEvent() error = %v, want accepted event", err)
			}
		})
	}
}
