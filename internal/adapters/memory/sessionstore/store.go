// Package sessionstore provides an in-memory session lifecycle repository for
// the single-node development profile.
package sessionstore

import (
	"errors"
	"sync"

	"narumq/internal/domain/session"
	"narumq/internal/domain/tenancy"
)

var (
	// ErrInvalidClientID means that a repository key is empty.
	ErrInvalidClientID = errors.New("session repository client ID is empty")
	// ErrInvalidSessionKey means that a session key is incomplete or invalid.
	ErrInvalidSessionKey = tenancy.ErrInvalidSessionKey
)

// MemorySessionRepository stores lifecycle values behind a mutex. Lifecycle is
// a value type, so Load and Save do not share mutable aggregate storage.
type MemorySessionRepository struct {
	mu          sync.Mutex
	sessions    map[string]session.Lifecycle
	sessionsKey map[tenancy.SessionKey]session.Lifecycle
}

// New creates an empty in-memory session repository.
func New() *MemorySessionRepository {
	return &MemorySessionRepository{
		sessions:    make(map[string]session.Lifecycle),
		sessionsKey: make(map[tenancy.SessionKey]session.Lifecycle),
	}
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

// LoadKey returns a copied lifecycle for a tenant-scoped session key.
func (repository *MemorySessionRepository) LoadKey(key tenancy.SessionKey) (session.Lifecycle, bool, error) {
	if repository == nil {
		return session.Lifecycle{}, false, ErrInvalidSessionKey
	}
	if key == (tenancy.SessionKey{}) {
		return session.Lifecycle{}, false, ErrInvalidSessionKey
	}
	repository.mu.Lock()
	defer repository.mu.Unlock()
	state, exists := repository.sessionsKey[key]
	return state, exists, nil
}

// Save stores a lifecycle under its Client ID and SessionKey if present.
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
	if repository.sessionsKey == nil {
		repository.sessionsKey = make(map[tenancy.SessionKey]session.Lifecycle)
	}
	repository.sessions[clientID] = state
	if state.SessionKey() != (tenancy.SessionKey{}) {
		repository.sessionsKey[state.SessionKey()] = state
	}
	return nil
}

// SaveKey stores a lifecycle under an explicit tenant-scoped session key.
func (repository *MemorySessionRepository) SaveKey(key tenancy.SessionKey, state session.Lifecycle) error {
	if repository == nil {
		return ErrInvalidSessionKey
	}
	if key == (tenancy.SessionKey{}) {
		return ErrInvalidSessionKey
	}
	repository.mu.Lock()
	defer repository.mu.Unlock()
	if repository.sessions == nil {
		repository.sessions = make(map[string]session.Lifecycle)
	}
	if repository.sessionsKey == nil {
		repository.sessionsKey = make(map[tenancy.SessionKey]session.Lifecycle)
	}
	repository.sessionsKey[key] = state
	repository.sessions[key.ClientID()] = state
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

// DeleteKey removes a lifecycle for a tenant-scoped session key.
func (repository *MemorySessionRepository) DeleteKey(key tenancy.SessionKey) error {
	if repository == nil {
		return ErrInvalidSessionKey
	}
	if key == (tenancy.SessionKey{}) {
		return ErrInvalidSessionKey
	}
	repository.mu.Lock()
	defer repository.mu.Unlock()
	delete(repository.sessionsKey, key)
	delete(repository.sessions, key.ClientID())
	return nil
}
