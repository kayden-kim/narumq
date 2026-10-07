package audit

import (
	"errors"
	"fmt"
	"reflect"
	"sync"
	"unicode/utf8"

	portsaudit "narumq/internal/application/ports/audit"
	portsclock "narumq/internal/application/ports/clock"
	portsmetrics "narumq/internal/application/ports/metrics"
)

var (
	ErrPublisherUnavailable = errors.New("audit publisher unavailable")
	ErrInvalidEvent         = errors.New("invalid audit event")
	ErrDenialSuppressed     = errors.New("audit denial event suppressed")
)

// ServiceConfig identifies this node and bounds security-denial event volume.
type ServiceConfig struct {
	ClusterID             string
	NodeID                uint64
	ReleaseVersion        string
	DenialEventsPerSecond int
}

// RecordCommandDTO contains the request-specific values for one audit event.
type RecordCommandDTO struct {
	RequestID string
	Actor     portsaudit.AuditActorDTO
	Action    string
	Target    portsaudit.AuditTargetDTO
	Result    portsaudit.AuditResult
	Reason    string
	Details   map[string]any
}

// Service creates, validates, and publishes management audit events.
type Service struct {
	config    ServiceConfig
	publisher portsaudit.AuditPublisher
	clock     portsclock.Clock
	metrics   portsmetrics.AuditOutcomeRecorder

	denialMu                sync.Mutex
	denialWindow            int64
	denialCount             int
	denialWindowInitialized bool
}

// NewService creates a service and records the publisher's initial readiness.
func NewService(config ServiceConfig, publisher portsaudit.AuditPublisher, clock portsclock.Clock, metrics portsmetrics.AuditOutcomeRecorder) (*Service, error) {
	if config.ClusterID == "" || config.NodeID == 0 || !utf8.ValidString(config.ReleaseVersion) || utf8.RuneCountInString(config.ReleaseVersion) == 0 || utf8.RuneCountInString(config.ReleaseVersion) > 64 || config.DenialEventsPerSecond <= 0 {
		return nil, errors.New("invalid audit service configuration")
	}
	if isNilDependency(publisher) || isNilDependency(clock) || isNilDependency(metrics) {
		return nil, errors.New("audit service dependencies must not be nil")
	}

	service := &Service{config: config, publisher: publisher, clock: clock, metrics: metrics}
	metrics.SetAuditPublisherReady(publisher.Ready() == nil)
	return service, nil
}

// AdmitMutation rejects a mutation when the audit sink cannot accept a record.
func (service *Service) AdmitMutation() error {
	if err := service.publisher.Ready(); err != nil {
		service.metrics.SetAuditPublisherReady(false)
		service.metrics.AuditOutcome(portsmetrics.AuditOutcomeMutationRejected)
		return fmt.Errorf("%w: %v", ErrPublisherUnavailable, err)
	}
	service.metrics.SetAuditPublisherReady(true)
	return nil
}

// Record creates and publishes one validated audit event.
func (service *Service) Record(command RecordCommandDTO) error {
	if command.Result == portsaudit.AuditResultDenied && !service.admitDenial() {
		service.metrics.AuditOutcome(portsmetrics.AuditOutcomeDenialSuppressed)
		return ErrDenialSuppressed
	}

	eventID, err := newEventID()
	if err != nil {
		service.metrics.AuditOutcome(portsmetrics.AuditOutcomePublishFailed)
		return fmt.Errorf("create audit event ID: %w", err)
	}
	event := portsaudit.AuditEventDTO{
		SchemaVersion: "1.0",
		EventID:       eventID,
		OccurredAt:    service.clock.Now().UTC(),
		RequestID:     command.RequestID,
		Producer: portsaudit.AuditProducerDTO{
			Component:  "BROKER",
			InstanceID: fmt.Sprintf("%s/%d", service.config.ClusterID, service.config.NodeID),
			Version:    service.config.ReleaseVersion,
		},
		Actor:   command.Actor,
		Action:  command.Action,
		Target:  command.Target,
		Result:  command.Result,
		Reason:  command.Reason,
		Details: command.Details,
	}
	if err := validateEvent(event); err != nil {
		service.metrics.AuditOutcome(portsmetrics.AuditOutcomeInvalid)
		return fmt.Errorf("%w: %v", ErrInvalidEvent, err)
	}
	if err := service.publisher.Publish(event); err != nil {
		service.metrics.SetAuditPublisherReady(false)
		service.metrics.AuditOutcome(portsmetrics.AuditOutcomePublishFailed)
		return fmt.Errorf("publish audit event: %w", err)
	}
	service.metrics.SetAuditPublisherReady(true)
	service.metrics.AuditOutcome(portsmetrics.AuditOutcomePublished)
	return nil
}

func (service *Service) admitDenial() bool {
	window := service.clock.Now().Unix()
	service.denialMu.Lock()
	defer service.denialMu.Unlock()

	if !service.denialWindowInitialized || service.denialWindow != window {
		service.denialWindow = window
		service.denialCount = 0
		service.denialWindowInitialized = true
	}
	if service.denialCount >= service.config.DenialEventsPerSecond {
		return false
	}
	service.denialCount++
	return true
}

func isNilDependency(dependency any) bool {
	if dependency == nil {
		return true
	}
	value := reflect.ValueOf(dependency)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}
