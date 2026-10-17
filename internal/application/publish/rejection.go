package publish

import (
	"errors"

	"narumq/internal/application/ports/retained"
)

// RejectionClass tells whether a refused publish hit a tenant limit or protects the server.
type RejectionClass string

const (
	// RejectionClassTenantLimit means the publisher's tenant exceeded one of its own limits.
	RejectionClassTenantLimit RejectionClass = "tenant_limit"
	// RejectionClassServerBackpressure means the server refused new publish state to protect itself.
	RejectionClassServerBackpressure RejectionClass = "server_backpressure"
)

// Rejection causes. They match the table in the MQTT protocol spec section 7.2.
const (
	RejectionCauseTenantQuota      = "tenant_quota"
	RejectionCauseCommitLag        = "commit_lag"
	RejectionCauseDiskFree         = "disk_free"
	RejectionCauseDeliveryCapacity = "delivery_capacity"
	RejectionCauseReceiveCapacity  = "receive_capacity"
	RejectionCauseRetainedCapacity = "retained_capacity"
)

// RejectionError wraps a publish refusal with its class and cause. Error keeps the
// "<wrapped>: <cause>" text and errors.Is still matches the wrapped error.
type RejectionError struct {
	Err   error
	Class RejectionClass
	Cause string
}

// NewRejectionError wraps err with the class and cause of the refusal.
func NewRejectionError(err error, class RejectionClass, cause string) error {
	return &RejectionError{Err: err, Class: class, Cause: cause}
}

func (rejection *RejectionError) Error() string {
	return rejection.Err.Error() + ": " + rejection.Cause
}

func (rejection *RejectionError) Unwrap() error { return rejection.Err }

// FormatRejectionReason returns the MQTT 5 Reason String "<class>:<cause>".
func FormatRejectionReason(class RejectionClass, cause string) string {
	return string(class) + ":" + cause
}

// RejectionReason returns the Reason String for a refused publish, or an empty string
// when the cause is unknown. It never guesses a cause.
func RejectionReason(err error) string {
	var rejection *RejectionError
	if errors.As(err, &rejection) {
		return FormatRejectionReason(rejection.Class, rejection.Cause)
	}
	if errors.Is(err, retained.ErrFull) {
		return FormatRejectionReason(RejectionClassServerBackpressure, RejectionCauseRetainedCapacity)
	}
	return ""
}
