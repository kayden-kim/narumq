package audit

import (
	"context"
	"errors"
	"time"
)

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

// DataPlaneAction identifies one audited MQTT data-plane operation.
type DataPlaneAction string

const (
	DataPlaneActionConnect     DataPlaneAction = "mqtt.connect"
	DataPlaneActionPublish     DataPlaneAction = "mqtt.publish"
	DataPlaneActionSubscribe   DataPlaneAction = "mqtt.subscribe"
	DataPlaneActionUnsubscribe DataPlaneAction = "mqtt.unsubscribe"
	DataPlaneActionWill        DataPlaneAction = "mqtt.will"
)

// DataPlaneReasonNotAuthorized is the reason for every MQTT ACL denial.
const DataPlaneReasonNotAuthorized = "not_authorized"

// DataPlaneDenialDTO describes one rejected MQTT data-plane request.
type DataPlaneDenialDTO struct {
	Action       DataPlaneAction
	Reason       string
	TenantID     string // empty before authentication
	Subject      string // empty means unauthenticated
	CredentialID string
	Roles        []string
	ClientID     string
	SourceIP     string // remote host without port; empty when unknown
	AuthMethod   string // mqtt.connect only
	Topic        string // topic or filter; Will topic for mqtt.connect Will reasons
}

// DataPlaneDenialRecorder accepts MQTT denial events without blocking the caller.
type DataPlaneDenialRecorder interface {
	RecordDataPlaneDenial(denial DataPlaneDenialDTO)
}

// AuditPublisher appends validated audit events to a durable sink.
type AuditPublisher interface {
	Ready() error
	Publish(event AuditEventDTO) error
}

// ErrAuditRecordsUnavailable indicates that stored audit records could not be read reliably.
var ErrAuditRecordsUnavailable = errors.New("audit records unavailable")

// AuditRecordQueryDTO selects stored records with Seq < BeforeSeq (0 = no bound), newest first.
type AuditRecordQueryDTO struct {
	BeforeSeq                                             uint64
	Limit                                                 int
	ActorSubject, Action, TargetType, TargetID, RequestID string
	Result                                                AuditResult
	From, To                                              time.Time
}

// AuditRecordDTO contains the exact stored event JSON and its hash-chain metadata.
type AuditRecordDTO struct {
	Seq       uint64
	PrevHash  string
	Hash      string
	EventJSON []byte
}

// AuditRecordPageDTO contains one page and whether additional matching records exist.
type AuditRecordPageDTO struct {
	Records []AuditRecordDTO
	HasMore bool
}

// AuditRecordReader reads records from a durable audit chain.
type AuditRecordReader interface {
	ReadAuditRecords(ctx context.Context, query AuditRecordQueryDTO) (AuditRecordPageDTO, error)
}
