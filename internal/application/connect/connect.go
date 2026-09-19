// Package connect coordinates MQTT session activation and connection cleanup.
package connect

import (
	"errors"
	"sync"
	"time"

	"narumq/internal/application/ports/deliveryqueue"
	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/routing"
	"narumq/internal/domain/session"
)

var (
	// ErrNilClock means that session expiry cannot be evaluated.
	ErrNilClock = errors.New("connect clock is nil")
	// ErrNilSessionRepository means that session state cannot be loaded or saved.
	ErrNilSessionRepository = errors.New("connect session repository is nil")
	// ErrNilDeliveryQueue means that active delivery state cannot be managed.
	ErrNilDeliveryQueue = errors.New("connect delivery queue is nil")
	// ErrNilRoutingTable means that stale subscriptions cannot be removed.
	ErrNilRoutingTable = errors.New("connect routing table is nil")
	// ErrSessionNotFound means that a disconnect target has no stored session.
	ErrSessionNotFound = errors.New("connect session not found")
)

// Clock supplies the instant used by session expiry policy.
type Clock interface {
	Now() time.Time
}

// SessionRepository stores the lifecycle value for one Client ID.
type SessionRepository interface {
	Load(clientID string) (session.Lifecycle, bool, error)
	Save(state session.Lifecycle) error
	Delete(clientID string) error
}

// ResultDTO reports the connection response and accepted session identity.
type ResultDTO struct {
	SessionID routing.SessionID
	ConnAck   mqtt.ConnAck
	Accepted  bool
}

// UseCase serializes lifecycle persistence, routing cleanup and queue
// registration so one process cannot activate or disconnect the same session
// through interleaved operations.
type UseCase struct {
	mu         sync.Mutex
	clock      Clock
	sessions   SessionRepository
	deliveries deliveryqueue.Queue
	routes     *routing.Table
}

// New creates a connection use case over the supplied lifecycle boundaries.
func New(clock Clock, sessions SessionRepository, deliveries deliveryqueue.Queue, routes *routing.Table) (*UseCase, error) {
	if clock == nil {
		return nil, ErrNilClock
	}
	if sessions == nil {
		return nil, ErrNilSessionRepository
	}
	if deliveries == nil {
		return nil, ErrNilDeliveryQueue
	}
	if routes == nil {
		return nil, ErrNilRoutingTable
	}
	return &UseCase{
		clock:      clock,
		sessions:   sessions,
		deliveries: deliveries,
		routes:     routes,
	}, nil
}

// Connect validates the development listener's supported session policy and
// opens a new or resumable lifecycle. Unsupported credentials and Will state
// are returned as negative CONNACK results rather than silently ignored.
func (useCase *UseCase) Connect(packet mqtt.Connect) (ResultDTO, error) {
	if useCase == nil {
		return ResultDTO{}, ErrNilSessionRepository
	}
	useCase.mu.Lock()
	defer useCase.mu.Unlock()

	result := ResultDTO{ConnAck: mqtt.ConnAck{Version: packet.Version}}
	if packet.Version != mqtt.ProtocolV311 && packet.Version != mqtt.ProtocolV5 {
		return ResultDTO{}, mqtt.ErrUnsupportedProtocolVersion
	}
	if reasonCode, rejected := rejectedConnectReason(packet); rejected {
		result.ConnAck.ReasonCode = reasonCode
		return result, nil
	}

	now := useCase.clock.Now()
	existing, found, err := useCase.sessions.Load(packet.ClientID)
	if err != nil {
		return ResultDTO{}, err
	}
	var existingState *session.Lifecycle
	if found {
		existingState = &existing
	}
	opened, present, err := session.Open(existingState, packet, now)
	if err != nil {
		if errors.Is(err, session.ErrAlreadyConnected) {
			result.ConnAck.ReasonCode = duplicateConnectReason(packet.Version)
			return result, nil
		}
		return ResultDTO{}, err
	}
	if err := useCase.sessions.Save(opened); err != nil {
		return ResultDTO{}, err
	}

	sessionID := routing.SessionID(packet.ClientID)
	if !present {
		useCase.deliveries.Remove(sessionID)
		useCase.routes.RemoveSession(sessionID)
	}
	if err := useCase.deliveries.Register(sessionID); err != nil {
		var rollbackErr error
		if found {
			rollbackErr = useCase.sessions.Save(existing)
		} else {
			rollbackErr = useCase.sessions.Delete(packet.ClientID)
		}
		if rollbackErr != nil {
			return ResultDTO{}, errors.Join(err, rollbackErr)
		}
		return ResultDTO{}, err
	}
	return ResultDTO{
		SessionID: sessionID,
		ConnAck: mqtt.ConnAck{
			Version:        packet.Version,
			SessionPresent: present,
			ReasonCode:     0,
		},
		Accepted: true,
	}, nil
}

// Disconnect closes one active lifecycle and always removes its active
// delivery queue. Persistent session subscriptions remain available for a
// later resume; immediate-expiry sessions remove both stored state and routes.
func (useCase *UseCase) Disconnect(sessionID routing.SessionID) error {
	if useCase == nil {
		return ErrNilSessionRepository
	}
	useCase.mu.Lock()
	defer useCase.mu.Unlock()

	useCase.deliveries.Remove(sessionID)
	state, found, err := useCase.sessions.Load(string(sessionID))
	if err != nil {
		return err
	}
	if !found {
		return ErrSessionNotFound
	}
	closed, removed, err := state.Disconnect(useCase.clock.Now())
	if err != nil {
		return err
	}
	if removed {
		if err := useCase.sessions.Delete(string(sessionID)); err != nil {
			return err
		}
		useCase.routes.RemoveSession(sessionID)
		return nil
	}
	return useCase.sessions.Save(closed)
}

func rejectedConnectReason(packet mqtt.Connect) (byte, bool) {
	if packet.ClientID == "" {
		return clientIDRejectedReason(packet.Version), true
	}
	if packet.HasUsername || packet.HasPassword || packet.Will != nil {
		return unsupportedDevelopmentReason(packet.Version), true
	}
	return 0, false
}

func clientIDRejectedReason(version mqtt.ProtocolVersion) byte {
	if version == mqtt.ProtocolV5 {
		return 0x85
	}
	return 0x02
}

func unsupportedDevelopmentReason(version mqtt.ProtocolVersion) byte {
	if version == mqtt.ProtocolV5 {
		return 0x83
	}
	return 0x03
}

func duplicateConnectReason(version mqtt.ProtocolVersion) byte {
	return clientIDRejectedReason(version)
}
