// Package deliveryqueue provides a deterministic, fixed-capacity in-memory
// implementation of the publish application's delivery queue port.
package deliveryqueue

import (
	"bytes"
	"errors"
	"narumq/internal/application/ports/deliveryqueue"
	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/routing"
	"sort"
	"sync"
	"time"
)

var (
	// ErrInvalidCapacity means that a queue cannot enforce a positive bound.
	ErrInvalidCapacity = errors.New("delivery queue capacity must be positive")
)

// sessionState keeps queued FIFO deliveries and incomplete outbound exchanges
// under one registered session boundary.
type sessionState struct {
	queued   []deliveryqueue.DeliveryDTO
	outbound map[mqtt.PacketIdentifier]deliveryqueue.OutboundDTO
}

// Queue stores independent bounded delivery state for registered sessions.
type Queue struct {
	mu       sync.Mutex
	capacity int
	sessions map[routing.SessionID]*sessionState
}

// New creates a queue with an independent fixed capacity for each session's
// queued entries and outbound exchanges.
func New(capacity int) (*Queue, error) {
	if capacity <= 0 {
		return nil, ErrInvalidCapacity
	}
	return &Queue{
		capacity: capacity,
		sessions: make(map[routing.SessionID]*sessionState),
	}, nil
}

// Register makes sessionID available for nonblocking enqueue and recovery.
// Re-registering an existing session preserves queued and outbound state.
func (queue *Queue) Register(sessionID routing.SessionID) error {
	if err := validateSessionID(sessionID); err != nil {
		return err
	}
	if queue == nil {
		return deliveryqueue.ErrUnavailable
	}
	queue.mu.Lock()
	defer queue.mu.Unlock()
	if state, exists := queue.sessions[sessionID]; exists {
		if state.outbound == nil {
			state.outbound = make(map[mqtt.PacketIdentifier]deliveryqueue.OutboundDTO)
		}
		return nil
	}
	queue.sessions[sessionID] = &sessionState{
		outbound: make(map[mqtt.PacketIdentifier]deliveryqueue.OutboundDTO),
	}
	return nil
}

// Remove makes a session unavailable and discards all of its queued and
// outbound delivery state.
func (queue *Queue) Remove(sessionID routing.SessionID) {
	if queue == nil {
		return
	}
	queue.mu.Lock()
	delete(queue.sessions, sessionID)
	queue.mu.Unlock()
}

// TryEnqueue appends one owned copy or returns immediately when unavailable or
// full. It never waits for a consumer.
func (queue *Queue) TryEnqueue(delivery deliveryqueue.DeliveryDTO, now time.Time) error {
	if queue == nil {
		return deliveryqueue.ErrUnavailable
	}
	queue.mu.Lock()
	defer queue.mu.Unlock()
	return queue.tryEnqueueBatchLocked([]deliveryqueue.DeliveryDTO{delivery}, now)
}

// TryEnqueueBatch appends a complete owned batch for one session or appends
// nothing when the session is unavailable, the batch is invalid, or capacity
// is insufficient.
func (queue *Queue) TryEnqueueBatch(deliveries []deliveryqueue.DeliveryDTO, now time.Time) error {
	if queue == nil {
		return deliveryqueue.ErrUnavailable
	}
	queue.mu.Lock()
	defer queue.mu.Unlock()
	return queue.tryEnqueueBatchLocked(deliveries, now)
}

func (queue *Queue) tryEnqueueBatchLocked(deliveries []deliveryqueue.DeliveryDTO, now time.Time) error {
	if len(deliveries) == 0 {
		return nil
	}
	sessionID := deliveries[0].SessionID
	if err := validateSessionID(sessionID); err != nil {
		return deliveryqueue.ErrUnavailable
	}
	for _, item := range deliveries[1:] {
		if item.SessionID != sessionID {
			return deliveryqueue.ErrUnavailable
		}
		if err := validateSessionID(item.SessionID); err != nil {
			return deliveryqueue.ErrUnavailable
		}
	}
	state, exists := queue.sessions[sessionID]
	if !exists {
		return deliveryqueue.ErrUnavailable
	}
	state.queued = purgeExpired(state.queued, now)
	live := make([]deliveryqueue.DeliveryDTO, 0, len(deliveries))
	for _, item := range deliveries {
		if !item.Expired(now) {
			live = append(live, item)
		}
	}
	if len(live) == 0 {
		return nil
	}
	if len(live) > queue.capacity-len(state.queued) {
		return deliveryqueue.ErrFull
	}
	copied := make([]deliveryqueue.DeliveryDTO, len(live))
	for index, item := range live {
		item.Payload = cloneBytes(item.Payload)
		copied[index] = item
	}
	state.queued = append(state.queued, copied...)
	return nil
}

// Peek returns the live FIFO head without removing it. Expired entries before
// the head are purged; an empty registered queue returns ok=false without an error.
// The returned payload is independent of stored state.
func (queue *Queue) Peek(sessionID routing.SessionID, now time.Time) (deliveryqueue.DeliveryDTO, bool, error) {
	if queue == nil {
		return deliveryqueue.DeliveryDTO{}, false, deliveryqueue.ErrUnavailable
	}
	if err := validateSessionID(sessionID); err != nil {
		return deliveryqueue.DeliveryDTO{}, false, err
	}
	queue.mu.Lock()
	defer queue.mu.Unlock()
	state, exists := queue.sessions[sessionID]
	if !exists {
		return deliveryqueue.DeliveryDTO{}, false, deliveryqueue.ErrUnavailable
	}
	state.queued = purgeExpired(state.queued, now)
	if len(state.queued) == 0 {
		return deliveryqueue.DeliveryDTO{}, false, nil
	}
	head := state.queued[0]
	head.Payload = cloneBytes(head.Payload)
	return head, true, nil
}

// CompleteHead removes the current FIFO head after a successful QoS 0 write.
func (queue *Queue) CompleteHead(sessionID routing.SessionID) error {
	if queue == nil {
		return deliveryqueue.ErrUnavailable
	}
	if err := validateSessionID(sessionID); err != nil {
		return err
	}
	queue.mu.Lock()
	defer queue.mu.Unlock()
	state, exists := queue.sessions[sessionID]
	if !exists || len(state.queued) == 0 {
		return deliveryqueue.ErrUnavailable
	}
	clear(state.queued[:1])
	state.queued = state.queued[1:]
	return nil
}

// PromoteHead atomically removes the live FIFO head and stores its outbound
// QoS state. The head remains available when promotion cannot proceed.
func (queue *Queue) PromoteHead(outbound deliveryqueue.OutboundDTO, now time.Time) error {
	if queue == nil {
		return deliveryqueue.ErrUnavailable
	}
	sessionID := outbound.SessionID
	if err := validateSessionID(sessionID); err != nil {
		return err
	}
	if outbound.PacketID == 0 {
		return deliveryqueue.ErrPacketIDConflict
	}
	queue.mu.Lock()
	defer queue.mu.Unlock()
	state, exists := queue.sessions[sessionID]
	if !exists {
		return deliveryqueue.ErrUnavailable
	}
	if len(state.queued) == 0 {
		return deliveryqueue.ErrUnavailable
	}
	head := state.queued[0]
	if head.Expired(now) {
		clear(state.queued[:1])
		state.queued = state.queued[1:]
		return deliveryqueue.ErrExpired
	}
	if _, exists := state.outbound[outbound.PacketID]; exists {
		return deliveryqueue.ErrPacketIDConflict
	}
	if len(state.outbound) >= queue.capacity {
		return deliveryqueue.ErrFull
	}
	if head.SessionID != sessionID {
		return deliveryqueue.ErrUnavailable
	}
	clear(state.queued[:1])
	state.queued = state.queued[1:]
	outbound.DurableSequence = head.DurableSequence
	outbound.DurableEpoch = head.DurableEpoch
	state.outbound[outbound.PacketID] = outbound
	return nil
}

// ResetDurable atomically replaces durable state while preserving process-local
// state that remains compatible with the restored durable queue.
func (queue *Queue) ResetDurable(
	sessionID routing.SessionID,
	epoch uint64,
	queued []deliveryqueue.DeliveryDTO,
	outbound []deliveryqueue.OutboundDTO,
	now time.Time,
) error {
	if queue == nil {
		return deliveryqueue.ErrUnavailable
	}
	if err := validateSessionID(sessionID); err != nil || epoch == 0 {
		return deliveryqueue.ErrUnavailable
	}
	queue.mu.Lock()
	defer queue.mu.Unlock()
	state, exists := queue.sessions[sessionID]
	if !exists {
		return deliveryqueue.ErrUnavailable
	}
	if len(queued) > queue.capacity || len(outbound) > queue.capacity {
		return deliveryqueue.ErrFull
	}

	var previousSequence uint64
	liveQueued := make([]deliveryqueue.DeliveryDTO, 0, len(queued))
	for _, item := range queued {
		if item.SessionID != sessionID ||
			item.DurableSequence == 0 ||
			item.DurableEpoch != epoch ||
			item.DurableSequence <= previousSequence {
			return deliveryqueue.ErrUnavailable
		}
		previousSequence = item.DurableSequence
		if !item.Expired(now) {
			item.Payload = cloneBytes(item.Payload)
			liveQueued = append(liveQueued, item)
		}
	}
	if len(liveQueued) > queue.capacity {
		return deliveryqueue.ErrFull
	}

	durableOutbound := make(map[mqtt.PacketIdentifier]deliveryqueue.OutboundDTO, len(outbound))
	var previousPacketID mqtt.PacketIdentifier
	for _, item := range outbound {
		if item.SessionID != sessionID ||
			item.PacketID == 0 ||
			item.DurableSequence == 0 ||
			item.DurableEpoch != epoch ||
			item.PacketID <= previousPacketID {
			if item.PacketID == 0 || item.PacketID <= previousPacketID {
				return deliveryqueue.ErrPacketIDConflict
			}
			return deliveryqueue.ErrUnavailable
		}
		previousPacketID = item.PacketID
		durableOutbound[item.PacketID] = item
	}

	localQueued := make([]deliveryqueue.DeliveryDTO, 0, len(state.queued))
	for _, item := range state.queued {
		if item.DurableSequence != 0 || item.Expired(now) {
			continue
		}
		item.Payload = cloneBytes(item.Payload)
		localQueued = append(localQueued, item)
	}
	if len(liveQueued)+len(localQueued) > queue.capacity {
		localQueued = localQueued[:queue.capacity-len(liveQueued)]
	}

	nextOutbound := make(map[mqtt.PacketIdentifier]deliveryqueue.OutboundDTO, len(durableOutbound)+len(state.outbound))
	for packetID, item := range state.outbound {
		if item.DurableEpoch != 0 {
			continue
		}
		if _, conflict := durableOutbound[packetID]; conflict {
			return deliveryqueue.ErrPacketIDConflict
		}
		nextOutbound[packetID] = item
	}
	for packetID, item := range durableOutbound {
		nextOutbound[packetID] = item
	}
	if len(nextOutbound) > queue.capacity {
		return deliveryqueue.ErrFull
	}

	nextQueued := make([]deliveryqueue.DeliveryDTO, 0, len(liveQueued)+len(localQueued))
	nextQueued = append(nextQueued, liveQueued...)
	nextQueued = append(nextQueued, localQueued...)
	state.queued = nextQueued
	state.outbound = nextOutbound
	return nil
}

// ListQueued returns unexpired queued deliveries in FIFO order. It copies
// the deliveries and payloads without mutating the queue state.
func (queue *Queue) ListQueued(sessionID routing.SessionID, now time.Time) ([]deliveryqueue.DeliveryDTO, error) {
	if queue == nil {
		return nil, deliveryqueue.ErrUnavailable
	}
	if err := validateSessionID(sessionID); err != nil {
		return nil, err
	}
	queue.mu.Lock()
	defer queue.mu.Unlock()
	state, exists := queue.sessions[sessionID]
	if !exists {
		return nil, deliveryqueue.ErrUnavailable
	}
	result := make([]deliveryqueue.DeliveryDTO, 0, len(state.queued))
	for _, item := range state.queued {
		if item.Expired(now) {
			continue
		}
		copied := item
		copied.Payload = bytes.Clone(item.Payload)
		result = append(result, copied)
	}
	return result, nil
}

// ListOutbound returns incomplete outbound state sorted by Packet Identifier.
// delivery.Outbound is immutable by value, so its private payload is not
// cloned for each lookup.
func (queue *Queue) ListOutbound(sessionID routing.SessionID) ([]deliveryqueue.OutboundDTO, error) {
	if queue == nil {
		return nil, deliveryqueue.ErrUnavailable
	}
	if err := validateSessionID(sessionID); err != nil {
		return nil, err
	}
	queue.mu.Lock()
	defer queue.mu.Unlock()
	state, exists := queue.sessions[sessionID]
	if !exists {
		return nil, deliveryqueue.ErrUnavailable
	}
	outbound := make([]deliveryqueue.OutboundDTO, 0, len(state.outbound))
	for _, item := range state.outbound {
		outbound = append(outbound, item)
	}
	sort.Slice(outbound, func(left, right int) bool {
		return outbound[left].PacketID < outbound[right].PacketID
	})
	return outbound, nil
}

// ReplaceOutbound applies an acknowledgement transition to an existing state.
func (queue *Queue) ReplaceOutbound(outbound deliveryqueue.OutboundDTO) error {
	if queue == nil {
		return deliveryqueue.ErrUnavailable
	}
	if err := validateSessionID(outbound.SessionID); err != nil {
		return err
	}
	if outbound.PacketID == 0 {
		return deliveryqueue.ErrUnavailable
	}
	queue.mu.Lock()
	defer queue.mu.Unlock()
	state, exists := queue.sessions[outbound.SessionID]
	if !exists {
		return deliveryqueue.ErrUnavailable
	}
	if _, exists := state.outbound[outbound.PacketID]; !exists {
		return deliveryqueue.ErrUnavailable
	}
	state.outbound[outbound.PacketID] = outbound
	return nil
}

// CompleteOutbound removes one completed outbound exchange.
func (queue *Queue) CompleteOutbound(sessionID routing.SessionID, packetID mqtt.PacketIdentifier) error {
	if queue == nil {
		return deliveryqueue.ErrUnavailable
	}
	if err := validateSessionID(sessionID); err != nil {
		return err
	}
	if packetID == 0 {
		return deliveryqueue.ErrUnavailable
	}
	queue.mu.Lock()
	defer queue.mu.Unlock()
	state, exists := queue.sessions[sessionID]
	if !exists {
		return deliveryqueue.ErrUnavailable
	}
	if _, exists := state.outbound[packetID]; !exists {
		return deliveryqueue.ErrUnavailable
	}
	delete(state.outbound, packetID)
	return nil
}

func purgeExpired(items []deliveryqueue.DeliveryDTO, now time.Time) []deliveryqueue.DeliveryDTO {
	live := 0
	for _, item := range items {
		if item.Expired(now) {
			continue
		}
		items[live] = item
		live++
	}
	clear(items[live:])
	return items[:live]
}

func validateSessionID(sessionID routing.SessionID) error {
	if sessionID == "" {
		return routing.ErrInvalidSessionID
	}
	return nil
}

func cloneBytes(value []byte) []byte {
	if value == nil {
		return nil
	}
	copyOfValue := make([]byte, len(value))
	copy(copyOfValue, value)
	return copyOfValue
}
