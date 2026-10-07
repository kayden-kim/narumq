package audit

import "time"

// AuditResult is the outcome of one management-plane action.
type AuditResult string

const (
	AuditResultAllowed AuditResult = "ALLOWED"
	AuditResultDenied  AuditResult = "DENIED"
	AuditResultFailed  AuditResult = "FAILED"
)

// AuditProducerDTO identifies the component that created an audit event.
type AuditProducerDTO struct {
	Component  string
	InstanceID string
	Version    string
}

// AuditActorDTO identifies the authenticated or rejected management actor.
type AuditActorDTO struct {
	Subject      string
	TenantID     string
	CredentialID string
	Roles        []string
}

// AuditTargetDTO identifies the management resource affected by an action.
type AuditTargetDTO struct {
	Type     string
	ID       string
	TenantID string
}

// AuditEventDTO contains one validated management audit record.
type AuditEventDTO struct {
	SchemaVersion string
	EventID       string
	OccurredAt    time.Time
	RequestID     string
	Producer      AuditProducerDTO
	Actor         AuditActorDTO
	Action        string
	Target        AuditTargetDTO
	Result        AuditResult
	Reason        string
	PolicyVersion string
	SourceIP      string
	Details       map[string]any
}

// AuditPublisher appends validated audit events to a durable sink.
type AuditPublisher interface {
	Ready() error
	Publish(event AuditEventDTO) error
}
