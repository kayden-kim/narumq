// Package deliveryqueue provides a deterministic, fixed-capacity in-memory
// implementation of the publish application's delivery queue port.
package deliveryqueue

import (
	"errors"
	"sync"

	"narumq/internal/application/ports/deliveryqueue"
	"narumq/internal/domain/routing"
)

var (
	// ErrInvalidCapacity means that a queue cannot enforce a positive bound.
	ErrInvalidCapacity = errors.New("delivery queue capacity must be positive")
)

// Queue stores independent FIFO item queues for registered sessions.
type Queue struct {
	mu       sync.Mutex
	capacity int
	sessions map[routing.SessionID][]deliveryqueue.DeliveryDTO
}

// New creates a queue with a fixed item capacity per session.
func New(capacity int) (*Queue, error) {
	if capacity <= 0 {
		return nil, ErrInvalidCapacity
	}
	return &Queue{
		capacity: capacity,
		sessions: make(map[routing.SessionID][]deliveryqueue.DeliveryDTO),
	}, nil
}

// Register makes sessionID available for nonblocking enqueue and drain.
// Re-registering an existing session preserves its current FIFO contents.
func (queue *Queue) Register(sessionID routing.SessionID) error {
	if err := validateSessionID(sessionID); err != nil {
		return err
	}
	queue.mu.Lock()
	defer queue.mu.Unlock()
	if _, exists := queue.sessions[sessionID]; !exists {
		queue.sessions[sessionID] = nil
	}
	return nil
}

// Remove makes a session unavailable and discards its queued items.
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
func (queue *Queue) TryEnqueue(delivery deliveryqueue.DeliveryDTO) error {
	if queue == nil {
		return deliveryqueue.ErrUnavailable
	}
	queue.mu.Lock()
	defer queue.mu.Unlock()
	return queue.tryEnqueueBatchLocked([]deliveryqueue.DeliveryDTO{delivery})
}

// TryEnqueueBatch appends a complete owned batch for one session or appends
// nothing when the session is unavailable, the batch is invalid, or capacity
// is insufficient.
func (queue *Queue) TryEnqueueBatch(deliveries []deliveryqueue.DeliveryDTO) error {
	if queue == nil {
		return deliveryqueue.ErrUnavailable
	}
	queue.mu.Lock()
	defer queue.mu.Unlock()
	return queue.tryEnqueueBatchLocked(deliveries)
}

func (queue *Queue) tryEnqueueBatchLocked(deliveries []deliveryqueue.DeliveryDTO) error {
	if len(deliveries) == 0 {
		return nil
	}
	sessionID := deliveries[0].SessionID
	if err := validateSessionID(sessionID); err != nil {
		return deliveryqueue.ErrUnavailable
	}
	for _, delivery := range deliveries[1:] {
		if delivery.SessionID != sessionID {
			return deliveryqueue.ErrUnavailable
		}
		if err := validateSessionID(delivery.SessionID); err != nil {
			return deliveryqueue.ErrUnavailable
		}
	}
	items, exists := queue.sessions[sessionID]
	if !exists {
		return deliveryqueue.ErrUnavailable
	}
	if len(deliveries) > queue.capacity-len(items) {
		return deliveryqueue.ErrFull
	}
	copied := make([]deliveryqueue.DeliveryDTO, len(deliveries))
	for index, delivery := range deliveries {
		delivery.Payload = cloneBytes(delivery.Payload)
		copied[index] = delivery
	}
	queue.sessions[sessionID] = append(items, copied...)
	return nil
}

// Drain returns a FIFO snapshot and clears the registered session queue. The
// returned payload bytes are independent from the adapter's stored values.
func (queue *Queue) Drain(sessionID routing.SessionID) ([]deliveryqueue.DeliveryDTO, error) {
	if queue == nil {
		return nil, deliveryqueue.ErrUnavailable
	}
	if err := validateSessionID(sessionID); err != nil {
		return nil, err
	}
	queue.mu.Lock()
	defer queue.mu.Unlock()
	items, exists := queue.sessions[sessionID]
	if !exists {
		return nil, deliveryqueue.ErrUnavailable
	}
	drained := make([]deliveryqueue.DeliveryDTO, len(items))
	for index, item := range items {
		drained[index] = item
		drained[index].Payload = cloneBytes(item.Payload)
	}
	queue.sessions[sessionID] = nil
	return drained, nil
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
