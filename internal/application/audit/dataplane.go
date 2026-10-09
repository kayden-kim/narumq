package audit

import (
	"errors"
	"sync"
	"unicode/utf8"

	portsaudit "narumq/internal/application/ports/audit"
	portsclock "narumq/internal/application/ports/clock"
	portsmetrics "narumq/internal/application/ports/metrics"
	"narumq/internal/domain/security"
)

// DataPlaneRecorderConfig bounds asynchronous MQTT denial audit volume.
type DataPlaneRecorderConfig struct {
	ClusterID       string
	NodeID          uint64
	ReleaseVersion  string
	EventsPerSecond int
	QueueCapacity   int
}

// DataPlaneRecorder asynchronously publishes bounded MQTT denial audit events.
type DataPlaneRecorder struct {
	config     DataPlaneRecorderConfig
	publisher  portsaudit.AuditPublisher
	clock      portsclock.Clock
	metrics    portsmetrics.AuditOutcomeRecorder
	queue      chan portsaudit.AuditEventDTO
	limiter    perSecondLimiter
	workerDone chan struct{}

	mu      sync.Mutex
	started bool
	closed  bool
}

// NewDataPlaneRecorder validates dependencies and creates a bounded event queue.
func NewDataPlaneRecorder(config DataPlaneRecorderConfig, publisher portsaudit.AuditPublisher, clock portsclock.Clock, metrics portsmetrics.AuditOutcomeRecorder) (*DataPlaneRecorder, error) {
	if !validProducerIdentity(config.ClusterID, config.NodeID, config.ReleaseVersion) || config.EventsPerSecond <= 0 || config.QueueCapacity <= 0 {
		return nil, errors.New("invalid data-plane audit recorder configuration")
	}
	if isNilDependency(publisher) || isNilDependency(clock) || isNilDependency(metrics) {
		return nil, errors.New("data-plane audit recorder dependencies must not be nil")
	}
	return &DataPlaneRecorder{
		config:     config,
		publisher:  publisher,
		clock:      clock,
		metrics:    metrics,
		queue:      make(chan portsaudit.AuditEventDTO, config.QueueCapacity),
		limiter:    perSecondLimiter{limit: config.EventsPerSecond},
		workerDone: make(chan struct{}),
	}, nil
}

// Start starts the single publisher worker once.
func (recorder *DataPlaneRecorder) Start() {
	recorder.mu.Lock()
	if recorder.closed || recorder.started {
		recorder.mu.Unlock()
		return
	}
	recorder.started = true
	recorder.mu.Unlock()
	go recorder.run()
}

// RecordDataPlaneDenial queues one MQTT denial without waiting for the publisher.
func (recorder *DataPlaneRecorder) RecordDataPlaneDenial(denial portsaudit.DataPlaneDenialDTO) {
	recorder.mu.Lock()
	if recorder.closed {
		recorder.mu.Unlock()
		recorder.metrics.AuditOutcome(portsmetrics.AuditOutcomeDataPlaneDropped)
		return
	}
	recorder.mu.Unlock()

	if !recorder.limiter.admit(recorder.clock.Now().Unix()) {
		recorder.metrics.AuditOutcome(portsmetrics.AuditOutcomeDataPlaneSuppressed)
		return
	}

	event, truncated, err := recorder.event(denial)
	if err != nil {
		recorder.metrics.AuditOutcome(portsmetrics.AuditOutcomePublishFailed)
		return
	}
	if truncated {
		event.Details["truncated"] = true
	}
	if err := validateEvent(event); err != nil {
		recorder.metrics.AuditOutcome(portsmetrics.AuditOutcomeInvalid)
		return
	}

	recorder.mu.Lock()
	if recorder.closed {
		recorder.mu.Unlock()
		recorder.metrics.AuditOutcome(portsmetrics.AuditOutcomeDataPlaneDropped)
		return
	}
	select {
	case recorder.queue <- event:
		recorder.mu.Unlock()
	default:
		recorder.mu.Unlock()
		recorder.metrics.AuditOutcome(portsmetrics.AuditOutcomeDataPlaneDropped)
	}
}

// Close stops admission and waits for queued events to finish publishing.
func (recorder *DataPlaneRecorder) Close() {
	recorder.mu.Lock()
	if recorder.closed {
		started := recorder.started
		recorder.mu.Unlock()
		if started {
			<-recorder.workerDone
		}
		return
	}
	recorder.closed = true
	close(recorder.queue)
	started := recorder.started
	queued := 0
	if !started {
		queued = len(recorder.queue)
	}
	recorder.mu.Unlock()

	if started {
		<-recorder.workerDone
		return
	}
	for range queued {
		recorder.metrics.AuditOutcome(portsmetrics.AuditOutcomeDataPlaneDropped)
	}
}

func (recorder *DataPlaneRecorder) run() {
	defer close(recorder.workerDone)
	for event := range recorder.queue {
		if err := recorder.publisher.Publish(event); err != nil {
			recorder.metrics.SetAuditPublisherReady(false)
			recorder.metrics.AuditOutcome(portsmetrics.AuditOutcomePublishFailed)
			continue
		}
		recorder.metrics.SetAuditPublisherReady(true)
		recorder.metrics.AuditOutcome(portsmetrics.AuditOutcomePublished)
	}
}

func (recorder *DataPlaneRecorder) event(denial portsaudit.DataPlaneDenialDTO) (portsaudit.AuditEventDTO, bool, error) {
	denial, truncated := truncateDenial(denial)
	eventID, err := newEventID()
	if err != nil {
		return portsaudit.AuditEventDTO{}, truncated, err
	}

	actor := portsaudit.AuditActorDTO{Subject: denial.Subject}
	if actor.Subject == "" {
		actor.Subject = "unauthenticated"
	} else {
		actor.TenantID = denial.TenantID
		actor.CredentialID = denial.CredentialID
		actor.Roles = denial.Roles
	}

	target := portsaudit.AuditTargetDTO{ID: denial.Topic, TenantID: denial.TenantID}
	details := make(map[string]any, 2)
	switch denial.Action {
	case portsaudit.DataPlaneActionConnect:
		target.Type = "mqtt_client"
		target.ID = denial.ClientID
		if denial.AuthMethod != "" {
			details["authMethod"] = denial.AuthMethod
		}
		if denial.Reason == "invalid_will_topic" || denial.Reason == "will_unauthorized" {
			details["willTopic"] = denial.Topic
		}
	case portsaudit.DataPlaneActionPublish, portsaudit.DataPlaneActionWill:
		target.Type = "mqtt_topic"
		details["clientId"] = denial.ClientID
	case portsaudit.DataPlaneActionSubscribe, portsaudit.DataPlaneActionUnsubscribe:
		target.Type = "mqtt_topic_filter"
		details["clientId"] = denial.ClientID
	}

	return portsaudit.AuditEventDTO{
		SchemaVersion: "1.0",
		EventID:       eventID,
		OccurredAt:    recorder.clock.Now().UTC(),
		RequestID:     eventID,
		Producer:      brokerProducer(recorder.config.ClusterID, recorder.config.NodeID, recorder.config.ReleaseVersion),
		Actor:         actor,
		Action:        string(denial.Action),
		Target:        target,
		Result:        portsaudit.AuditResultDenied,
		Reason:        denial.Reason,
		SourceIP:      denial.SourceIP,
		Details:       details,
	}, truncated, nil
}

// ACLDenial creates an audit DTO from an authenticated MQTT principal.
func ACLDenial(action portsaudit.DataPlaneAction, principal security.Principal, topic, sourceIP string) portsaudit.DataPlaneDenialDTO {
	principalRoles := principal.Roles()
	roles := make([]string, len(principalRoles))
	for index, role := range principalRoles {
		roles[index] = string(role)
	}
	return portsaudit.DataPlaneDenialDTO{
		Action:       action,
		Reason:       portsaudit.DataPlaneReasonNotAuthorized,
		TenantID:     principal.TenantID().String(),
		Subject:      principal.Subject(),
		CredentialID: principal.CredentialID(),
		Roles:        roles,
		ClientID:     principal.ClientID(),
		SourceIP:     sourceIP,
		Topic:        topic,
	}
}

func truncateDenial(denial portsaudit.DataPlaneDenialDTO) (portsaudit.DataPlaneDenialDTO, bool) {
	truncated := false
	if denial.Subject == "" {
		denial.TenantID = ""
		denial.CredentialID = ""
		denial.Roles = nil
	} else {
		denial.Subject, truncated = truncateRunes(denial.Subject, 256, truncated)
		denial.TenantID, truncated = truncateRunes(denial.TenantID, 128, truncated)
		denial.CredentialID, truncated = truncateRunes(denial.CredentialID, 256, truncated)
		if len(denial.Roles) != 0 {
			roles := make([]string, 0, len(denial.Roles))
			seen := make(map[string]struct{}, len(denial.Roles))
			for _, role := range denial.Roles {
				role, truncated = truncateRunes(role, 64, truncated)
				if _, exists := seen[role]; exists {
					continue
				}
				seen[role] = struct{}{}
				roles = append(roles, role)
			}
			denial.Roles = roles
		}
	}
	denial.ClientID, truncated = truncateRunes(denial.ClientID, 512, truncated)
	denial.SourceIP, truncated = truncateRunes(denial.SourceIP, 128, truncated)
	denial.Topic, truncated = truncateRunes(denial.Topic, 512, truncated)
	denial.AuthMethod, truncated = truncateRunes(denial.AuthMethod, 512, truncated)
	return denial, truncated
}

func truncateRunes(value string, maxRunes int, alreadyTruncated bool) (string, bool) {
	if utf8.RuneCountInString(value) <= maxRunes {
		return value, alreadyTruncated
	}
	byteIndex := 0
	for runeCount := 0; runeCount < maxRunes; runeCount++ {
		_, size := utf8.DecodeRuneInString(value[byteIndex:])
		byteIndex += size
	}
	return value[:byteIndex], true
}

var _ portsaudit.DataPlaneDenialRecorder = (*DataPlaneRecorder)(nil)
