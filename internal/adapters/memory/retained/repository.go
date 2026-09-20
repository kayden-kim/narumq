// Package retained provides a bounded process-local retained-message repository.
package retained

import (
	"errors"
	"sort"
	"sync"

	applicationretained "narumq/internal/application/ports/retained"
	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/routing"
)

var (
	// ErrInvalidCapacity means that a retained repository cannot enforce a
	// positive entry and byte bound.
	ErrInvalidCapacity = errors.New("retained repository capacity must be positive")
	// ErrInvalidMessage means that a retained value is not storable.
	ErrInvalidMessage = errors.New("retained message is invalid")
)

// Repository stores one owned retained value per exact topic.
type Repository struct {
	mu          sync.Mutex
	maxMessages int
	maxBytes    int
	bytes       int
	messages    map[string]applicationretained.MessageDTO
}

// New creates a retained repository bounded by message count and aggregate
// topic, payload, and publisher-session identity bytes.
func New(maxMessages, maxBytes int) (*Repository, error) {
	if maxMessages <= 0 || maxBytes <= 0 {
		return nil, ErrInvalidCapacity
	}
	return &Repository{
		maxMessages: maxMessages,
		maxBytes:    maxBytes,
		messages:    make(map[string]applicationretained.MessageDTO),
	}, nil
}

// Replace atomically replaces the exact-topic value or returns ErrFull without
// changing the previous value. Payload is copied before it is retained.
func (repository *Repository) Replace(message applicationretained.MessageDTO) error {
	if repository == nil {
		return ErrInvalidCapacity
	}
	if err := validateMessage(message); err != nil {
		return err
	}
	messageBytes := retainedBytes(message)

	repository.mu.Lock()
	defer repository.mu.Unlock()
	previous, exists := repository.messages[message.Topic]
	currentCount := len(repository.messages)
	currentBytes := repository.bytes
	if exists {
		currentBytes -= retainedBytes(previous)
	} else {
		currentCount++
	}
	if currentCount > repository.maxMessages || currentBytes > repository.maxBytes-messageBytes {
		return applicationretained.ErrFull
	}
	message.Payload = cloneBytes(message.Payload)
	repository.messages[message.Topic] = message
	repository.bytes = currentBytes + messageBytes
	return nil
}

// Delete removes the exact-topic value. Deleting an absent value succeeds.
func (repository *Repository) Delete(topic string) error {
	if repository == nil {
		return ErrInvalidCapacity
	}
	if err := routing.ValidateTopicName(topic); err != nil {
		return err
	}
	repository.mu.Lock()
	defer repository.mu.Unlock()
	message, exists := repository.messages[topic]
	if !exists {
		return nil
	}
	delete(repository.messages, topic)
	repository.bytes -= retainedBytes(message)
	return nil
}

// List returns a topic-sorted snapshot with payloads independent from storage
// and from one another.
func (repository *Repository) List() ([]applicationretained.MessageDTO, error) {
	if repository == nil {
		return nil, ErrInvalidCapacity
	}
	repository.mu.Lock()
	defer repository.mu.Unlock()
	messages := make([]applicationretained.MessageDTO, 0, len(repository.messages))
	for _, message := range repository.messages {
		message.Payload = cloneBytes(message.Payload)
		messages = append(messages, message)
	}
	sort.Slice(messages, func(left, right int) bool {
		return messages[left].Topic < messages[right].Topic
	})
	return messages, nil
}

func validateMessage(message applicationretained.MessageDTO) error {
	if err := routing.ValidateTopicName(message.Topic); err != nil {
		return err
	}
	if message.Payload == nil || len(message.Payload) == 0 {
		return ErrInvalidMessage
	}
	if err := routing.ValidateSessionID(message.PublisherSessionID); err != nil {
		return err
	}
	if message.QoS > mqtt.QoSExactlyOnce {
		return routing.ErrInvalidQoS
	}
	return nil
}

func retainedBytes(message applicationretained.MessageDTO) int {
	return len(message.Topic) + len(message.Payload) + len(message.PublisherSessionID)
}

func cloneBytes(value []byte) []byte {
	if value == nil {
		return nil
	}
	copyOfValue := make([]byte, len(value))
	copy(copyOfValue, value)
	return copyOfValue
}
