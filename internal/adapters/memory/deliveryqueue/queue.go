// Package deliveryqueue provides a deterministic, fixed-capacity in-memory
// implementation of the publish application's delivery queue port.
package deliveryqueue

import (
	"errors"
	"sync"

	"narumq/internal/application/publish"
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
	sessions map[routing.SessionID][]publish.DeliveryDTO
}

// New creates a queue with a fixed item capacity per session.
func New(capacity int) (*Queue, error) {
	if capacity <= 0 {
		return nil, ErrInvalidCapacity
	}
	return &Queue{
		capacity: capacity,
		sessions: make(map[routing.SessionID][]publish.DeliveryDTO),
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
func (queue *Queue) TryEnqueue(delivery publish.DeliveryDTO) error {
	if queue == nil {
		return publish.ErrQueueUnavailable
	}
	if err := validateSessionID(delivery.SessionID); err != nil {
		return publish.ErrQueueUnavailable
	}
	queue.mu.Lock()
	defer queue.mu.Unlock()
	items, exists := queue.sessions[delivery.SessionID]
	if !exists {
		return publish.ErrQueueUnavailable
	}
	if len(items) >= queue.capacity {
		return publish.ErrQueueFull
	}
	delivery.Payload = cloneBytes(delivery.Payload)
	queue.sessions[delivery.SessionID] = append(items, delivery)
	return nil
}

// Drain returns a FIFO snapshot and clears the registered session queue. The
// returned payload bytes are independent from the adapter's stored values.
func (queue *Queue) Drain(sessionID routing.SessionID) ([]publish.DeliveryDTO, error) {
	if queue == nil {
		return nil, publish.ErrQueueUnavailable
	}
	if err := validateSessionID(sessionID); err != nil {
		return nil, err
	}
	queue.mu.Lock()
	defer queue.mu.Unlock()
	items, exists := queue.sessions[sessionID]
	if !exists {
		return nil, publish.ErrQueueUnavailable
	}
	drained := make([]publish.DeliveryDTO, len(items))
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
