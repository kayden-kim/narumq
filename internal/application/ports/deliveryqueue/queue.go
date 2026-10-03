// Package deliveryqueue defines the application-owned delivery queue port.
package deliveryqueue

import (
	"errors"
	"time"

	"narumq/internal/domain/delivery"
	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/routing"
)

var (
	// ErrFull means that a registered session reached its queued-item or outbound-state capacity.
	ErrFull = errors.New("delivery queue full")
	// ErrUnavailable means that a session or requested delivery state is unavailable.
	ErrUnavailable = errors.New("delivery queue unavailable")
	// ErrRetryableUnavailable means a durable promotion could not be confirmed because quorum was unavailable.
	// The transition may have committed; callers must retry with the same Packet Identifier.
	ErrRetryableUnavailable = errors.New("delivery transition temporarily unavailable")
	// ErrExpired means that the delivery at the queue head passed its deadline.
	ErrExpired = errors.New("delivery expired")
	// ErrPacketIDConflict means that an outbound Packet Identifier is already stored.
	ErrPacketIDConflict = errors.New("delivery packet identifier conflict")
)

// PendingTransition owns one queued outbound change. Wait is called exactly
// once and updates local queue state only after the durable barrier succeeds.
type PendingTransition interface {
	Wait() error
}

// CompletedTransition is the already-completed result of an in-memory change.
// It performs no I/O and needs no background worker or channel.
type CompletedTransition struct{}

func (CompletedTransition) Wait() error { return nil }

// DeliveryDTO is the application boundary value handed to one session queue.
// DurableSequence is zero for process-local deliveries and positive for
// replicated durable deliveries. DurableEpoch identifies the owner epoch that
// loaded a durable delivery; it is zero for process-local deliveries.
type DeliveryDTO struct {
	SessionID       routing.SessionID
	Topic           string
	Payload         []byte
	QoS             mqtt.QoS
	Retain          bool
	ExpiresAt       time.Time
	DurableSequence uint64
	DurableEpoch    uint64
}

// Expired reports whether this delivery has reached its absolute expiry
// deadline. A zero deadline means that the publication had no expiry property.
func (delivery DeliveryDTO) Expired(now time.Time) bool {
	return !delivery.ExpiresAt.IsZero() && !now.Before(delivery.ExpiresAt)
}

// RemainingExpiryInterval returns the positive whole-second interval that
// remains until ExpiresAt, rounded upward. The caller must check Expired first.
func (delivery DeliveryDTO) RemainingExpiryInterval(now time.Time) (uint32, bool) {
	if delivery.ExpiresAt.IsZero() {
		return 0, false
	}
	remaining := delivery.ExpiresAt.Sub(now)
	if remaining <= 0 {
		return 0, true
	}
	seconds := uint64(remaining / time.Second)
	if remaining%time.Second != 0 {
		seconds++
	}
	if seconds > uint64(^uint32(0)) {
		return ^uint32(0), true
	}
	return uint32(seconds), true
}

// Queue is the bounded delivery capability shared by publish and connection
// lifecycle use cases. TryEnqueue and TryEnqueueBatch read DeliveryDTO.Payload
// synchronously; implementations must copy it before retaining delivery.
type Queue interface {
	Register(sessionID routing.SessionID) error
	Remove(sessionID routing.SessionID)
	TryEnqueue(delivery DeliveryDTO, now time.Time) error
	TryEnqueueBatch(deliveries []DeliveryDTO, now time.Time) error
	// Peek reports an empty registered queue as (zero, false, nil). ErrUnavailable means the session or queue state is missing.
	// PeekBatch fills at most len(destination) owned FIFO values.
	PeekBatch(sessionID routing.SessionID, now time.Time, destination []DeliveryDTO) (int, error)
	Peek(sessionID routing.SessionID, now time.Time) (DeliveryDTO, bool, error)
	CompleteHead(sessionID routing.SessionID) error
	// PromoteBatch returns the successfully promoted contiguous prefix. Every
	// pending durable result is drained even when a preceding item fails.
	// A nonzero prefix does not suppress an error. Callers retain every suffix
	// PacketID until retry or connection teardown; later SENDs may have committed.
	PromoteBatch(outbound []OutboundDTO, now time.Time) (int, error)
	PromoteHead(outbound OutboundDTO, now time.Time) error
	ListQueued(sessionID routing.SessionID, now time.Time) ([]DeliveryDTO, error)
	ListOutbound(sessionID routing.SessionID) ([]OutboundDTO, error)
	ReplaceOutbound(outbound OutboundDTO) error
	BeginReplaceOutbound(outbound OutboundDTO) (PendingTransition, error)
	// ResetDurable atomically replaces one session's durable queue state after
	// attachment. Durable queued items must be in strictly increasing sequence
	// order and carry the supplied positive owner epoch. Durable outbound items
	// must carry that epoch, a positive source sequence, and be packet-ID
	// ordered. Expired durable queued items are omitted. Existing process-local
	// queued items are retained after the durable items, trimming only their
	// tail to capacity. Process-local outbound items are retained unless their
	// packet ID conflicts with durable outbound state; conflicts, invalid input,
	// or capacity failures leave the queue unchanged. Implementations must clone
	// retained payloads before returning.
	ResetDurable(sessionID routing.SessionID, epoch uint64, queued []DeliveryDTO, outbound []OutboundDTO, now time.Time) error
	CompleteOutbound(sessionID routing.SessionID, packetID mqtt.PacketIdentifier) error
	BeginCompleteOutbound(outbound OutboundDTO) (PendingTransition, error)
}

// OutboundDTO is the application boundary value for one incomplete sender-side
// QoS 1 or QoS 2 exchange. DurableSequence identifies its source durable queue
// item, while DurableEpoch identifies the owner epoch; both are zero for
// process-local outbound state.
type OutboundDTO struct {
	SessionID       routing.SessionID
	PacketID        mqtt.PacketIdentifier
	State           delivery.Outbound
	ExpiresAt       time.Time
	DurableSequence uint64
	DurableEpoch    uint64
}
