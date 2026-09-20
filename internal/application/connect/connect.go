// Package connect coordinates MQTT session activation and connection cleanup.
package connect

import (
	"errors"
	"sync"
	"time"

	applicationclock "narumq/internal/application/ports/clock"
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
	// ErrNilScheduler means that timers cannot be scheduled.
	ErrNilScheduler = errors.New("connect scheduler is nil")
	// ErrNilWillPublisher means that will messages cannot be published.
	ErrNilWillPublisher = errors.New("connect will publisher is nil")
	// ErrSessionNotFound means that a disconnect target has no stored session.
	ErrSessionNotFound = errors.New("connect session not found")
)

// Timer allows cancelling a scheduled callback.
type Timer interface {
	Stop() bool
}

// Scheduler dispatches callbacks after a specified delay.
type Scheduler interface {
	AfterFunc(d time.Duration, f func()) Timer
}

// WillPublisher routes published will messages to subscribers.
type WillPublisher interface {
	PublishWill(sessionID routing.SessionID, will mqtt.Will) error
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
type clientTimers struct {
	willTimer   Timer
	expiryTimer Timer
}

type UseCase struct {
	mu            sync.Mutex
	clock         applicationclock.Clock
	sessions      SessionRepository
	deliveries    deliveryqueue.Queue
	routes        *routing.Table
	scheduler     Scheduler
	willPublisher WillPublisher
	activeTimers  map[string]*clientTimers
	closed        bool
}

// New creates a connection use case over the supplied lifecycle boundaries.
func New(
	clock applicationclock.Clock,
	sessions SessionRepository,
	deliveries deliveryqueue.Queue,
	routes *routing.Table,
	scheduler Scheduler,
	willPublisher WillPublisher,
) (*UseCase, error) {
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
	if scheduler == nil {
		return nil, ErrNilScheduler
	}
	if willPublisher == nil {
		return nil, ErrNilWillPublisher
	}
	return &UseCase{
		clock:         clock,
		sessions:      sessions,
		deliveries:    deliveries,
		routes:        routes,
		scheduler:     scheduler,
		willPublisher: willPublisher,
		activeTimers:  make(map[string]*clientTimers),
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
	if timers, ok := useCase.activeTimers[packet.ClientID]; ok {
		if timers.willTimer != nil {
			timers.willTimer.Stop()
		}
		if timers.expiryTimer != nil {
			timers.expiryTimer.Stop()
		}
		delete(useCase.activeTimers, packet.ClientID)
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

// Disconnect closes one active lifecycle and evaluates session retention,
// timer scheduling, and will message publication.
func (useCase *UseCase) Disconnect(sessionID routing.SessionID, reason session.DisconnectReason) error {
	if useCase == nil {
		return ErrNilSessionRepository
	}
	useCase.mu.Lock()

	clientID := string(sessionID)
	if timers, ok := useCase.activeTimers[clientID]; ok {
		if timers.willTimer != nil {
			timers.willTimer.Stop()
		}
		if timers.expiryTimer != nil {
			timers.expiryTimer.Stop()
		}
		delete(useCase.activeTimers, clientID)
	}

	useCase.deliveries.Remove(sessionID)
	state, found, err := useCase.sessions.Load(clientID)
	if err != nil {
		useCase.mu.Unlock()
		return err
	}
	if !found {
		useCase.mu.Unlock()
		return ErrSessionNotFound
	}
	result, err := state.Disconnect(useCase.clock.Now(), reason)
	if err != nil {
		useCase.mu.Unlock()
		return err
	}
	if result.SessionRemoved {
		if err := useCase.sessions.Delete(clientID); err != nil {
			useCase.mu.Unlock()
			return err
		}
		useCase.routes.RemoveSession(sessionID)
	} else {
		if err := useCase.sessions.Save(result.Lifecycle); err != nil {
			useCase.mu.Unlock()
			return err
		}
	}

	if !result.SessionRemoved && (result.ScheduleWill || result.ScheduleExpiry) {
		timers := &clientTimers{}
		if result.ScheduleWill {
			timers.willTimer = useCase.scheduler.AfterFunc(result.WillDelay, func() {
				useCase.onWillTimer(sessionID)
			})
		}
		if result.ScheduleExpiry {
			timers.expiryTimer = useCase.scheduler.AfterFunc(result.ExpiryDuration, func() {
				useCase.onExpiryTimer(sessionID)
			})
		}
		useCase.activeTimers[clientID] = timers
	}

	willToPublish := result.Will
	shouldPublish := result.PublishWillNow && willToPublish != nil
	useCase.mu.Unlock()

	if shouldPublish {
		if err := useCase.willPublisher.PublishWill(sessionID, *willToPublish); err != nil {
			return err
		}
	}
	return nil
}

func (useCase *UseCase) onWillTimer(sessionID routing.SessionID) {
	useCase.mu.Lock()
	clientID := string(sessionID)
	if timers, ok := useCase.activeTimers[clientID]; ok {
		timers.willTimer = nil
		if timers.expiryTimer == nil {
			delete(useCase.activeTimers, clientID)
		}
	}
	state, found, err := useCase.sessions.Load(clientID)
	if err != nil || !found {
		useCase.mu.Unlock()
		return
	}
	if state.Connected() {
		useCase.mu.Unlock()
		return
	}
	updated, will, claimed := state.ClaimWill()
	if !claimed {
		useCase.mu.Unlock()
		return
	}
	if err := useCase.sessions.Save(updated); err != nil {
		useCase.mu.Unlock()
		return
	}
	useCase.mu.Unlock()

	_ = useCase.willPublisher.PublishWill(sessionID, *will)
}

func (useCase *UseCase) onExpiryTimer(sessionID routing.SessionID) {
	useCase.mu.Lock()
	clientID := string(sessionID)
	if timers, ok := useCase.activeTimers[clientID]; ok {
		if timers.willTimer != nil {
			timers.willTimer.Stop()
		}
		delete(useCase.activeTimers, clientID)
	}
	state, found, err := useCase.sessions.Load(clientID)
	if err != nil || !found {
		useCase.mu.Unlock()
		return
	}
	if state.Connected() {
		useCase.mu.Unlock()
		return
	}
	now := useCase.clock.Now()
	_, will, expired := state.Expire(now)
	if !expired {
		useCase.mu.Unlock()
		return
	}
	_ = useCase.sessions.Delete(clientID)
	useCase.deliveries.Remove(sessionID)
	useCase.routes.RemoveSession(sessionID)
	useCase.mu.Unlock()

	if will != nil {
		_ = useCase.willPublisher.PublishWill(sessionID, *will)
	}
}

// Close stops and cancels all pending timers across all sessions.
func (useCase *UseCase) Close() error {
	if useCase == nil {
		return nil
	}
	useCase.mu.Lock()
	defer useCase.mu.Unlock()
	useCase.closed = true
	for _, timers := range useCase.activeTimers {
		if timers.willTimer != nil {
			timers.willTimer.Stop()
		}
		if timers.expiryTimer != nil {
			timers.expiryTimer.Stop()
		}
	}
	useCase.activeTimers = make(map[string]*clientTimers)
	return nil
}

func rejectedConnectReason(packet mqtt.Connect) (byte, bool) {
	if packet.ClientID == "" {
		return clientIDRejectedReason(packet.Version), true
	}
	if packet.HasUsername || packet.HasPassword {
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
