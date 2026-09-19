// Package sessionstore provides an in-memory session lifecycle repository for
// the single-node development profile.
package sessionstore

import (
	"errors"
	"sync"

	"narumq/internal/domain/session"
)

var (
	// ErrInvalidClientID means that a repository key is empty.
	ErrInvalidClientID = errors.New("session repository client ID is empty")
)

// MemorySessionRepository stores lifecycle values behind a mutex. Lifecycle is
// a value type, so Load and Save do not share mutable aggregate storage.
type MemorySessionRepository struct {
	mu       sync.Mutex
	sessions map[string]session.Lifecycle
}

// New creates an empty in-memory session repository.
func New() *MemorySessionRepository {
	return &MemorySessionRepository{sessions: make(map[string]session.Lifecycle)}
}

// Load returns a copied lifecycle when clientID exists.
func (repository *MemorySessionRepository) Load(clientID string) (session.Lifecycle, bool, error) {
	if repository == nil {
		return session.Lifecycle{}, false, ErrInvalidClientID
	}
	if clientID == "" {
		return session.Lifecycle{}, false, ErrInvalidClientID
	}
	repository.mu.Lock()
	defer repository.mu.Unlock()
	state, exists := repository.sessions[clientID]
	return state, exists, nil
}

// Save stores a lifecycle under its Client ID.
func (repository *MemorySessionRepository) Save(state session.Lifecycle) error {
	if repository == nil {
		return ErrInvalidClientID
	}
	clientID := state.ClientID()
	if clientID == "" {
		return ErrInvalidClientID
	}
	repository.mu.Lock()
	defer repository.mu.Unlock()
	if repository.sessions == nil {
		repository.sessions = make(map[string]session.Lifecycle)
	}
	repository.sessions[clientID] = state
	return nil
}

// Delete removes a lifecycle. Deleting an absent key is idempotent.
func (repository *MemorySessionRepository) Delete(clientID string) error {
	if repository == nil {
		return ErrInvalidClientID
	}
	if clientID == "" {
		return ErrInvalidClientID
	}
	repository.mu.Lock()
	defer repository.mu.Unlock()
	delete(repository.sessions, clientID)
	return nil
}
