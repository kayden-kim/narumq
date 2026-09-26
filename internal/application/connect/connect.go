// Package connect coordinates MQTT session activation and connection cleanup.
package connect

import (
	"context"
	"errors"
	"sync"
	"time"

	applicationauthentication "narumq/internal/application/ports/authentication"
	applicationclock "narumq/internal/application/ports/clock"
	"narumq/internal/application/ports/deliveryqueue"
	portsmetrics "narumq/internal/application/ports/metrics"
	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/routing"
	"narumq/internal/domain/security"
	"narumq/internal/domain/session"
	"narumq/internal/domain/tenancy"
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

// OwnershipCoordinator is the application-facing committed ownership boundary.
type OwnershipCoordinator interface {
	AcquireOwnership(context.Context, string, string, uint64) (uint64, error)
}

// ResultDTO reports the connection response, accepted session, and committed owner epoch.
type ResultDTO struct {
	SessionID      routing.SessionID
	Principal      security.Principal
	ConnAck        mqtt.ConnAck
	Accepted       bool
	OwnershipEpoch uint64
}

// ConnectRequestDTO carries the MQTT CONNECT packet and optional verified certificate identity.
type ConnectRequestDTO struct {
	Packet            mqtt.Connect
	ClientCertificate *applicationauthentication.ClientCertificateDTO
}

// Option configures optional parameters for the connection use case.
type Option func(*UseCase)

// WithSecurity configures JWT and mTLS authentication providers and the security policy.
func WithSecurity(
	jwtProvider applicationauthentication.AuthenticationProvider,
	mtlsProvider applicationauthentication.ClientCertificateAuthenticationProvider,
	policy security.Policy,
) Option {
	return func(useCase *UseCase) {
		useCase.authenticator = jwtProvider
		useCase.certificateAuthenticator = mtlsProvider
		useCase.policy = policy
	}
}

// WithMetrics configures a metrics recorder for connection and authentication events.
func WithMetrics(metrics portsmetrics.MetricsRecorder) Option {
	return func(useCase *UseCase) {
		useCase.metrics = metrics
	}
}

// WithOwnershipCoordinator commits cluster ownership before local session activation.
func WithOwnershipCoordinator(coordinator OwnershipCoordinator, ownerNodeID uint64) Option {
	return func(useCase *UseCase) {
		useCase.ownershipCoordinator = coordinator
		useCase.ownerNodeID = ownerNodeID
	}
}

const offlineDetachTimeout = 5 * time.Second

// DetachRequestDTO carries a persistent session's committed detach and its pending local queue.
type DetachRequestDTO struct {
	TenantID, ClientID string
	OwnerNodeID, Epoch uint64
	SessionExpiresAt   time.Time // zero means the session never expires
	DetachedAt         time.Time
	Pending            []deliveryqueue.DeliveryDTO
}

// OfflineClaimDTO is the committed offline state claimed by a new owner epoch.
type OfflineClaimDTO struct {
	Found      bool
	Deliveries []deliveryqueue.DeliveryDTO
}

// OfflineCoordinator replicates detached-session offline queues in cluster mode.
type OfflineCoordinator interface {
	DetachSession(ctx context.Context, request DetachRequestDTO) error
	ClaimOffline(ctx context.Context, tenantID, clientID string, ownerNodeID, epoch uint64, claimedAt time.Time) (OfflineClaimDTO, error)
}

// WithOfflineCoordinator configures offline queue replication for cluster mode.
func WithOfflineCoordinator(coordinator OfflineCoordinator) Option {
	return func(useCase *UseCase) {
		useCase.offlineCoordinator = coordinator
	}
}

// UseCase serializes lifecycle persistence, routing cleanup and queue
// registration so one process cannot activate or disconnect the same session
// through interleaved operations.
type clientTimers struct {
	willTimer   Timer
	expiryTimer Timer
}

type UseCase struct {
	mu                       sync.Mutex
	clock                    applicationclock.Clock
	authenticator            applicationauthentication.AuthenticationProvider
	certificateAuthenticator applicationauthentication.ClientCertificateAuthenticationProvider
	policy                   security.Policy
	sessions                 SessionRepository
	principals               map[routing.SessionID]security.Principal
	sessionKeys              map[routing.SessionID]tenancy.SessionKey
	deliveries               deliveryqueue.Queue
	routes                   *routing.Table
	scheduler                Scheduler
	willPublisher            WillPublisher
	activeTimers             map[string]*clientTimers
	closed                   bool
	metrics                  portsmetrics.MetricsRecorder
	ownershipCoordinator     OwnershipCoordinator
	ownerNodeID              uint64
	offlineCoordinator       OfflineCoordinator
	ownershipEpochs          map[routing.SessionID]uint64
}

// ClusterOwnershipConfigured reports whether a node-specific committed ownership barrier is installed.
func (useCase *UseCase) ClusterOwnershipConfigured(ownerNodeID uint64) bool {
	if useCase == nil || ownerNodeID == 0 {
		return false
	}
	useCase.mu.Lock()
	defer useCase.mu.Unlock()
	return useCase.ownershipCoordinator != nil && useCase.ownerNodeID == ownerNodeID
}

func (useCase *UseCase) recordAuth(method string, success bool, reason string) {
	if useCase != nil && useCase.metrics != nil {
		useCase.metrics.AuthAttempt(method, success, reason)
	}
}

func (useCase *UseCase) recordSessionGauge() {
	if useCase == nil || useCase.metrics == nil {
		return
	}
	snapshotter, ok := useCase.sessions.(interface{ Snapshot() []session.Lifecycle })
	if !ok {
		return
	}
	var persistent, nonPersistent int64
	for _, state := range snapshotter.Snapshot() {
		if state.ExpiryInterval() > 0 {
			persistent++
		} else {
			nonPersistent++
		}
	}
	useCase.metrics.SetSessionsActive(true, persistent)
	useCase.metrics.SetSessionsActive(false, nonPersistent)
}

// New creates a connection use case over the supplied lifecycle boundaries.
func New(
	clock applicationclock.Clock,
	sessions SessionRepository,
	deliveries deliveryqueue.Queue,
	routes *routing.Table,
	scheduler Scheduler,
	willPublisher WillPublisher,
	opts ...Option,
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
	useCase := &UseCase{
		clock:           clock,
		sessions:        sessions,
		principals:      make(map[routing.SessionID]security.Principal),
		sessionKeys:     make(map[routing.SessionID]tenancy.SessionKey),
		ownershipEpochs: make(map[routing.SessionID]uint64),
		deliveries:      deliveries,
		routes:          routes,
		scheduler:       scheduler,
		willPublisher:   willPublisher,
		activeTimers:    make(map[string]*clientTimers),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(useCase)
		}
	}
	return useCase, nil
}

// Connect validates the development listener's supported session policy and
// opens a new or resumable lifecycle. Unsupported credentials and Will state
// are returned as negative CONNACK results rather than silently ignored.
func (useCase *UseCase) Connect(packet mqtt.Connect) (ResultDTO, error) {
	return useCase.ConnectContext(context.Background(), ConnectRequestDTO{Packet: packet})
}

// ConnectContext validates authentication, tenant binding, and Will authorization
// before granting session ownership.
func (useCase *UseCase) ConnectContext(ctx context.Context, request ConnectRequestDTO) (ResultDTO, error) {
	if useCase == nil {
		return ResultDTO{}, ErrNilSessionRepository
	}

	packet := request.Packet
	result := ResultDTO{ConnAck: mqtt.ConnAck{Version: packet.Version}}
	if packet.Version != mqtt.ProtocolV311 && packet.Version != mqtt.ProtocolV5 {
		return ResultDTO{}, mqtt.ErrUnsupportedProtocolVersion
	}
	if packet.ClientID == "" {
		result.ConnAck.ReasonCode = clientIDRejectedReason(packet.Version)
		return result, nil
	}

	var (
		principal     security.Principal
		authResult    applicationauthentication.Result
		authMethod    = portsmetrics.AuthMethodAnonymous
		authenticated bool
	)
	switch {
	case request.ClientCertificate != nil:
		authMethod = portsmetrics.AuthMethodTLS
		if useCase.certificateAuthenticator == nil {
			useCase.recordAuth(authMethod, false, "unsupported_credentials")
			result.ConnAck.ReasonCode = notAuthorizedReason(packet.Version)
			return result, nil
		}
		if packet.HasUsername || packet.HasPassword {
			useCase.recordAuth(authMethod, false, "dual_credentials")
			result.ConnAck.ReasonCode = notAuthorizedReason(packet.Version)
			return result, nil
		}
		var err error
		authResult, err = useCase.certificateAuthenticator.AuthenticateClientCertificate(ctx, *request.ClientCertificate)
		if err != nil {
			useCase.recordAuth(authMethod, false, "invalid_credentials")
			result.ConnAck.ReasonCode = notAuthorizedReason(packet.Version)
			return result, nil
		}
		authenticated = true
	case useCase.authenticator != nil:
		authMethod = portsmetrics.AuthMethodJWT
		if !packet.HasUsername || !packet.HasPassword || len(packet.Password) == 0 {
			useCase.recordAuth(authMethod, false, "missing_credentials")
			result.ConnAck.ReasonCode = notAuthorizedReason(packet.Version)
			return result, nil
		}
		var err error
		authResult, err = useCase.authenticator.Authenticate(ctx, applicationauthentication.Credential{Token: packet.Password})
		if err != nil {
			useCase.recordAuth(authMethod, false, "invalid_credentials")
			result.ConnAck.ReasonCode = notAuthorizedReason(packet.Version)
			return result, nil
		}
		authenticated = true
	case useCase.certificateAuthenticator != nil:
		authMethod = portsmetrics.AuthMethodTLS
		useCase.recordAuth(authMethod, false, "missing_certificate")
		result.ConnAck.ReasonCode = notAuthorizedReason(packet.Version)
		return result, nil
	default:
		if packet.HasUsername || packet.HasPassword {
			useCase.recordAuth(portsmetrics.AuthMethodAnonymous, false, "unsupported_credentials")
			result.ConnAck.ReasonCode = unsupportedDevelopmentReason(packet.Version)
			return result, nil
		}
		useCase.recordAuth(portsmetrics.AuthMethodAnonymous, true, "none")
	}

	if authenticated {
		allowed := false
		for _, allowedID := range authResult.AllowedClientIDs {
			if allowedID == packet.ClientID {
				allowed = true
				break
			}
		}
		if !allowed {
			useCase.recordAuth(authMethod, false, "client_id_not_allowed")
			result.ConnAck.ReasonCode = notAuthorizedReason(packet.Version)
			return result, nil
		}
		p, err := security.NewPrincipal(
			authResult.TenantID,
			packet.ClientID,
			authResult.Subject,
			authResult.CredentialID,
			authResult.Roles,
			authResult.ExpiresAt,
		)
		if err != nil {
			useCase.recordAuth(authMethod, false, "not_authorized")
			result.ConnAck.ReasonCode = notAuthorizedReason(packet.Version)
			return result, nil
		}
		principal = p
		if packet.Will != nil {
			willTopic, err := tenancy.NewTopic(authResult.TenantID, packet.Will.Topic)
			if err != nil {
				useCase.recordAuth(authMethod, false, "invalid_will_topic")
				result.ConnAck.ReasonCode = notAuthorizedReason(packet.Version)
				return result, nil
			}
			if err := useCase.policy.AuthorizeTopic(principal, security.ActionWill, willTopic); err != nil {
				useCase.recordAuth(authMethod, false, "will_unauthorized")
				result.ConnAck.ReasonCode = notAuthorizedReason(packet.Version)
				return result, nil
			}
		}
		useCase.recordAuth(authMethod, true, "none")
	}

	sessionID := routing.SessionID(packet.ClientID)
	var (
		sessionKey    tenancy.SessionKey
		sessionTenant tenancy.TenantID
	)
	if principal.Subject() != "" {
		sessionTenant = principal.TenantID()
	} else if useCase.ownershipCoordinator != nil {
		var err error
		sessionTenant, err = tenancy.NewTenantID("default")
		if err != nil {
			return ResultDTO{}, err
		}
	}
	if principal.Subject() != "" || useCase.ownershipCoordinator != nil {
		var err error
		sessionKey, err = tenancy.NewSessionKey(sessionTenant, packet.ClientID)
		if err != nil {
			return ResultDTO{}, err
		}
		if useCase.ownershipCoordinator != nil {
			sessionID = routing.SessionID(sessionKey.RoutingID())
		} else {
			sessionID = routing.SessionID(sessionTenant.String() + "/" + packet.ClientID)
		}
	}

	useCase.mu.Lock()
	defer useCase.mu.Unlock()

	timerKey := string(sessionID)
	if useCase.ownershipCoordinator == nil {
		useCase.stopSessionTimers(timerKey)
	}

	now := useCase.clock.Now()
	existing, found, err := useCase.loadSession(sessionKey, timerKey)
	if err != nil {
		return ResultDTO{}, err
	}
	var existingState *session.Lifecycle
	if found {
		existingState = &existing
	}
	opened, present, err := session.Open(existingState, packet, now, sessionKey)
	if err != nil {
		if errors.Is(err, session.ErrAlreadyConnected) {
			result.ConnAck.ReasonCode = duplicateConnectReason(packet.Version)
			return result, nil
		}
		return ResultDTO{}, err
	}
	var ownershipEpoch uint64
	ownershipTenantID := "default"
	if principal.Subject() != "" {
		ownershipTenantID = principal.TenantID().String()
	}
	if useCase.ownershipCoordinator != nil {
		ownershipEpoch, err = useCase.ownershipCoordinator.AcquireOwnership(ctx, ownershipTenantID, packet.ClientID, useCase.ownerNodeID)
		if err != nil || ownershipEpoch == 0 {
			result.ConnAck.ReasonCode = ownershipUnavailableReason(packet.Version)
			return result, nil
		}
	}
	if err := useCase.saveSession(sessionKey, timerKey, opened); err != nil {
		return ResultDTO{}, err
	}

	useCase.principals[sessionID] = principal
	if !present {
		useCase.deliveries.Remove(sessionID)
		if useCase.ownershipCoordinator == nil {
			useCase.routes.RemoveSession(sessionID)
		}
	}
	if principal.Subject() != "" || useCase.ownershipCoordinator != nil {
		_ = useCase.routes.BindSessionTenant(sessionID, sessionTenant)
	}
	if err := useCase.deliveries.Register(sessionID); err != nil {
		var rollbackErr error
		if found {
			rollbackErr = useCase.saveSession(sessionKey, timerKey, existing)
		} else {
			rollbackErr = useCase.deleteSession(sessionKey, timerKey)
		}
		delete(useCase.principals, sessionID)
		if rollbackErr != nil {
			return ResultDTO{}, errors.Join(err, rollbackErr)
		}
		return ResultDTO{}, err
	}
	if useCase.ownershipCoordinator != nil && useCase.offlineCoordinator == nil {
		useCase.stopSessionTimers(timerKey)
	}
	if useCase.offlineCoordinator != nil && ownershipEpoch != 0 {
		claim, err := useCase.offlineCoordinator.ClaimOffline(ctx, ownershipTenantID, packet.ClientID, useCase.ownerNodeID, ownershipEpoch, now)
		if err != nil {
			var rollbackErr error
			if found {
				rollbackErr = useCase.saveSession(sessionKey, timerKey, existing)
			} else {
				rollbackErr = useCase.deleteSession(sessionKey, timerKey)
			}
			delete(useCase.principals, sessionID)
			if !found {
				useCase.deliveries.Remove(sessionID)
				useCase.routes.RemoveSession(sessionID)
			}
			if rollbackErr != nil {
				return ResultDTO{}, errors.Join(err, rollbackErr)
			}
			result.ConnAck.ReasonCode = ownershipUnavailableReason(packet.Version)
			return result, nil
		}
		// Keep prior timers armed until CLAIM succeeds; callbacks wait on useCase.mu.
		useCase.stopSessionTimers(timerKey)
		if claim.Found && !packet.CleanStart {
			present = true
			if len(claim.Deliveries) > 0 {
				_ = useCase.deliveries.TryEnqueueBatch(claim.Deliveries, now)
			}
		}
	}
	if ownershipEpoch != 0 {
		useCase.ownershipEpochs[sessionID] = ownershipEpoch
	}
	if sessionKey != (tenancy.SessionKey{}) {
		useCase.sessionKeys[sessionID] = sessionKey
	} else {
		delete(useCase.sessionKeys, sessionID)
	}
	useCase.recordSessionGauge()
	return ResultDTO{
		SessionID: sessionID, Principal: principal, OwnershipEpoch: ownershipEpoch,
		ConnAck: mqtt.ConnAck{
			Version:        packet.Version,
			SessionPresent: present,
			ReasonCode:     0,
		},
		Accepted: true,
	}, nil
}
func (useCase *UseCase) stopSessionTimers(timerKey string) {
	if timers, ok := useCase.activeTimers[timerKey]; ok {
		if timers.willTimer != nil {
			timers.willTimer.Stop()
		}
		if timers.expiryTimer != nil {
			timers.expiryTimer.Stop()
		}
		delete(useCase.activeTimers, timerKey)
	}
}

// Disconnect closes one active lifecycle and evaluates session retention,
// timer scheduling, and will message publication.
func (useCase *UseCase) Disconnect(sessionID routing.SessionID, reason session.DisconnectReason) error {
	if useCase == nil {
		return ErrNilSessionRepository
	}
	useCase.mu.Lock()

	timerKey := string(sessionID)
	if timers, ok := useCase.activeTimers[timerKey]; ok {
		if timers.willTimer != nil {
			timers.willTimer.Stop()
		}
		if timers.expiryTimer != nil {
			timers.expiryTimer.Stop()
		}
		delete(useCase.activeTimers, timerKey)
	}

	sessionKey, hasSessionKey := useCase.sessionKeys[sessionID]
	if !hasSessionKey {
		if principal, ok := useCase.principals[sessionID]; ok && principal.Subject() != "" {
			sessionKey, _ = tenancy.NewSessionKey(principal.TenantID(), principal.ClientID())
		}
	}
	state, found, err := useCase.loadSession(sessionKey, timerKey)
	if err != nil {
		useCase.mu.Unlock()
		return err
	}
	if !found {
		useCase.mu.Unlock()
		return ErrSessionNotFound
	}
	now := useCase.clock.Now()
	result, err := state.Disconnect(now, reason)
	if err != nil {
		useCase.mu.Unlock()
		return err
	}
	if result.SessionRemoved {
		if err := useCase.deleteSession(sessionKey, timerKey); err != nil {
			useCase.mu.Unlock()
			return err
		}
		useCase.deliveries.Remove(sessionID)
		if useCase.ownershipCoordinator == nil {
			useCase.routes.RemoveSession(sessionID)
		}
	} else {
		if err := useCase.saveSession(sessionKey, timerKey, result.Lifecycle); err != nil {
			useCase.mu.Unlock()
			return err
		}
		epoch, hasEpoch := useCase.ownershipEpochs[sessionID]
		if useCase.offlineCoordinator != nil && hasEpoch && hasSessionKey {
			pending, err := useCase.deliveries.ListQueued(sessionID, now)
			if err == nil {
				var sessionExpiresAt time.Time
				if result.ScheduleExpiry {
					sessionExpiresAt = now.Add(result.ExpiryDuration)
				}
				detachCtx, cancel := context.WithTimeout(context.Background(), offlineDetachTimeout)
				detachErr := useCase.offlineCoordinator.DetachSession(detachCtx, DetachRequestDTO{
					TenantID:         sessionKey.TenantID().String(),
					ClientID:         sessionKey.ClientID(),
					OwnerNodeID:      useCase.ownerNodeID,
					Epoch:            epoch,
					SessionExpiresAt: sessionExpiresAt,
					DetachedAt:       now,
					Pending:          pending,
				})
				cancel()
				if detachErr == nil {
					useCase.deliveries.Remove(sessionID)
				}
			}
		}
	}

	if !result.SessionRemoved && (result.ScheduleWill || result.ScheduleExpiry) {
		timers := &clientTimers{}
		if result.ScheduleWill {
			timers.willTimer = useCase.scheduler.AfterFunc(result.WillDelay, func() {
				useCase.onWillTimerForTimer(sessionID, timers)
			})
		}
		if result.ScheduleExpiry {
			timers.expiryTimer = useCase.scheduleExpiryTimerLocked(sessionID, result.ExpiryDuration, timers)
		}
		useCase.activeTimers[timerKey] = timers
	}

	willToPublish := result.Will
	shouldPublish := result.PublishWillNow && willToPublish != nil
	if shouldPublish && useCase.authenticator != nil {
		principal, ok := useCase.principals[sessionID]
		if !ok {
			shouldPublish = false
		} else {
			willTopic, err := tenancy.NewTopic(principal.TenantID(), willToPublish.Topic)
			if err != nil || useCase.policy.AuthorizeTopic(principal, security.ActionWill, willTopic) != nil {
				shouldPublish = false
			}
		}
	}
	if result.SessionRemoved {
		delete(useCase.principals, sessionID)
		delete(useCase.sessionKeys, sessionID)
	}
	delete(useCase.ownershipEpochs, sessionID)
	useCase.mu.Unlock()
	useCase.recordSessionGauge()

	if shouldPublish {
		if err := useCase.willPublisher.PublishWill(sessionID, *willToPublish); err != nil {
			return err
		}
	}
	return nil
}

func (useCase *UseCase) onWillTimer(sessionID routing.SessionID) {
	useCase.onWillTimerForTimer(sessionID, nil)
}

func (useCase *UseCase) onWillTimerForTimer(sessionID routing.SessionID, expected *clientTimers) {
	useCase.mu.Lock()
	timerKey := string(sessionID)
	timers, ok := useCase.activeTimers[timerKey]
	if expected != nil && (!ok || timers != expected) {
		useCase.mu.Unlock()
		return
	}
	if ok {
		timers.willTimer = nil
		if timers.expiryTimer == nil {
			delete(useCase.activeTimers, timerKey)
		}
	}
	var sessionKey tenancy.SessionKey
	if principal, ok := useCase.principals[sessionID]; ok && principal.Subject() != "" {
		sessionKey, _ = tenancy.NewSessionKey(principal.TenantID(), principal.ClientID())
	}
	state, found, err := useCase.loadSession(sessionKey, timerKey)
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
	if err := useCase.saveSession(sessionKey, timerKey, updated); err != nil {
		useCase.mu.Unlock()
		return
	}
	shouldPublish := true
	if useCase.authenticator != nil {
		principal, ok := useCase.principals[sessionID]
		if !ok {
			shouldPublish = false
		} else {
			willTopic, err := tenancy.NewTopic(principal.TenantID(), will.Topic)
			if err != nil || useCase.policy.AuthorizeTopic(principal, security.ActionWill, willTopic) != nil {
				shouldPublish = false
			}
		}
	}
	useCase.mu.Unlock()

	if shouldPublish {
		_ = useCase.willPublisher.PublishWill(sessionID, *will)
	}
}

func (useCase *UseCase) scheduleExpiryTimerLocked(sessionID routing.SessionID, delay time.Duration, timers *clientTimers) Timer {
	return useCase.scheduler.AfterFunc(delay, func() {
		useCase.onExpiryTimerForTimer(sessionID, timers)
	})
}

func (useCase *UseCase) onExpiryTimer(sessionID routing.SessionID) {
	useCase.onExpiryTimerForTimer(sessionID, nil)
}

func (useCase *UseCase) onExpiryTimerForTimer(sessionID routing.SessionID, expected *clientTimers) {
	useCase.mu.Lock()
	timerKey := string(sessionID)
	timers, ok := useCase.activeTimers[timerKey]
	if expected != nil && (!ok || timers != expected) {
		useCase.mu.Unlock()
		return
	}
	if ok {
		if timers.willTimer != nil {
			timers.willTimer.Stop()
		}
		delete(useCase.activeTimers, timerKey)
	}
	sessionKey, hasSessionKey := useCase.sessionKeys[sessionID]
	if !hasSessionKey {
		if principal, ok := useCase.principals[sessionID]; ok && principal.Subject() != "" {
			sessionKey, _ = tenancy.NewSessionKey(principal.TenantID(), principal.ClientID())
		}
	}
	state, found, err := useCase.loadSession(sessionKey, timerKey)
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
	if err := useCase.deleteSession(sessionKey, timerKey); err != nil {
		if !useCase.closed {
			retryTimers := &clientTimers{}
			retryTimers.expiryTimer = useCase.scheduleExpiryTimerLocked(sessionID, time.Second, retryTimers)
			useCase.activeTimers[timerKey] = retryTimers
		}
		useCase.mu.Unlock()
		return
	}
	useCase.deliveries.Remove(sessionID)
	if useCase.ownershipCoordinator == nil {
		useCase.routes.RemoveSession(sessionID)
	}
	shouldPublish := will != nil
	if shouldPublish && useCase.authenticator != nil {
		principal, ok := useCase.principals[sessionID]
		if !ok {
			shouldPublish = false
		} else {
			willTopic, err := tenancy.NewTopic(principal.TenantID(), will.Topic)
			if err != nil || useCase.policy.AuthorizeTopic(principal, security.ActionWill, willTopic) != nil {
				shouldPublish = false
			}
		}
	}
	delete(useCase.principals, sessionID)
	delete(useCase.sessionKeys, sessionID)
	useCase.mu.Unlock()

	if shouldPublish {
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

func notAuthorizedReason(version mqtt.ProtocolVersion) byte {
	if version == mqtt.ProtocolV5 {
		return 0x87
	}
	return 0x05
}
func ownershipUnavailableReason(version mqtt.ProtocolVersion) byte {
	if version == mqtt.ProtocolV5 {
		return 0x88
	}
	return 0x03
}

func duplicateConnectReason(version mqtt.ProtocolVersion) byte {
	return clientIDRejectedReason(version)
}

func (useCase *UseCase) loadSession(sessionKey tenancy.SessionKey, clientID string) (session.Lifecycle, bool, error) {
	if sessionKey != (tenancy.SessionKey{}) {
		if keyed, ok := useCase.sessions.(interface {
			LoadKey(tenancy.SessionKey) (session.Lifecycle, bool, error)
		}); ok {
			return keyed.LoadKey(sessionKey)
		}
		return useCase.sessions.Load(sessionKey.ClientID())
	}
	return useCase.sessions.Load(clientID)
}

func (useCase *UseCase) saveSession(sessionKey tenancy.SessionKey, clientID string, state session.Lifecycle) error {
	if sessionKey != (tenancy.SessionKey{}) {
		if keyed, ok := useCase.sessions.(interface {
			SaveKey(tenancy.SessionKey, session.Lifecycle) error
		}); ok {
			return keyed.SaveKey(sessionKey, state)
		}
	}
	return useCase.sessions.Save(state)
}

func (useCase *UseCase) deleteSession(sessionKey tenancy.SessionKey, clientID string) error {
	if sessionKey != (tenancy.SessionKey{}) {
		if keyed, ok := useCase.sessions.(interface {
			DeleteKey(tenancy.SessionKey) error
		}); ok {
			return keyed.DeleteKey(sessionKey)
		}
		return useCase.sessions.Delete(sessionKey.ClientID())
	}
	return useCase.sessions.Delete(clientID)
}
