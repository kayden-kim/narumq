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
	mu           sync.Mutex
	sessions     map[string]session.Lifecycle
	sessionsKey  map[tenancy.SessionKey]session.Lifecycle
	tenantCounts map[tenancy.TenantID]tenantSessionCount
}

type tenantSessionCount struct {
	sessions  int
	connected int
}

func (repository *MemorySessionRepository) adjustTenantSessionCountLocked(
	tenantID tenancy.TenantID,
	state session.Lifecycle,
	delta int,
) {
	if repository.tenantCounts == nil {
		repository.tenantCounts = make(map[tenancy.TenantID]tenantSessionCount)
	}
	count := repository.tenantCounts[tenantID]
	count.sessions += delta
	if state.Connected() {
		count.connected += delta
	}
	if count.sessions == 0 {
		delete(repository.tenantCounts, tenantID)
		return
	}
	repository.tenantCounts[tenantID] = count
}

// TenantSessionCounts returns the stored and connected lifecycle counts for tenantID.
func (repository *MemorySessionRepository) TenantSessionCounts(tenantID tenancy.TenantID) (sessions, connected int) {
	if repository == nil {
		return 0, 0
	}
	repository.mu.Lock()
	defer repository.mu.Unlock()
	count := repository.tenantCounts[tenantID]
	return count.sessions, count.connected
}

// New creates an empty in-memory session repository.
func New() *MemorySessionRepository {
	return &MemorySessionRepository{
		sessions:     make(map[string]session.Lifecycle),
		sessionsKey:  make(map[tenancy.SessionKey]session.Lifecycle),
		tenantCounts: make(map[tenancy.TenantID]tenantSessionCount),
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
	if key := state.SessionKey(); key != (tenancy.SessionKey{}) {
		if previous, found := repository.sessionsKey[key]; found {
			repository.adjustTenantSessionCountLocked(key.TenantID(), previous, -1)
		}
		repository.sessionsKey[key] = state
		repository.adjustTenantSessionCountLocked(key.TenantID(), state, 1)
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
	if previous, found := repository.sessionsKey[key]; found {
		repository.adjustTenantSessionCountLocked(key.TenantID(), previous, -1)
	}
	repository.sessionsKey[key] = state
	repository.adjustTenantSessionCountLocked(key.TenantID(), state, 1)
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
	if previous, found := repository.sessionsKey[key]; found {
		repository.adjustTenantSessionCountLocked(key.TenantID(), previous, -1)
	}
	delete(repository.sessionsKey, key)
	delete(repository.sessions, key.ClientID())
	return nil
}

// Snapshot returns a copy of all stored lifecycles.
func (repository *MemorySessionRepository) Snapshot() []session.Lifecycle {
	if repository == nil {
		return nil
	}
	repository.mu.Lock()
	defer repository.mu.Unlock()
	list := make([]session.Lifecycle, 0, len(repository.sessions))
	for _, s := range repository.sessions {
		list = append(list, s)
	}
	return list
}
