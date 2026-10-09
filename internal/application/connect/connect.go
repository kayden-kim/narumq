// Package connect coordinates MQTT session activation and connection cleanup.
package connect

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"narumq/internal/application/audit"
	"narumq/internal/application/membership"
	portsaudit "narumq/internal/application/ports/audit"
	applicationauthentication "narumq/internal/application/ports/authentication"
	applicationclock "narumq/internal/application/ports/clock"
	"narumq/internal/application/ports/deliveryqueue"
	"narumq/internal/application/ports/deliverystate"
	portsmetrics "narumq/internal/application/ports/metrics"
	"narumq/internal/application/tenantlimit"
	"narumq/internal/domain/delivery"
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
	// ErrDurableDeliveryRejected means a committed delivery transition could not be applied.
	ErrDurableDeliveryRejected = errors.New("connect durable delivery transition rejected")
	// ErrNilTenantSessionCounter means configured tenant limits cannot count lifecycles.
	ErrNilTenantSessionCounter = errors.New("connect tenant session counter is nil")
	// ErrInvalidAuthenticationMethod means the configured credential provider has no supported metric and audit method.
	ErrInvalidAuthenticationMethod = errors.New("connect authentication method is invalid")
)

// Timer allows cancelling a scheduled callback.
type Timer interface {
	Stop() bool
}

// Scheduler dispatches callbacks after a specified delay.
type Scheduler interface {
	AfterFunc(d time.Duration, f func()) Timer
}

// WillPublisher routes published will messages using their authenticated session identity.
type WillPublisher interface {
	PublishWill(sessionID routing.SessionID, publisherKey tenancy.SessionKey, will mqtt.Will) error
}

// SessionRepository stores the lifecycle value for one Client ID.
type SessionRepository interface {
	Load(clientID string) (session.Lifecycle, bool, error)
	Save(state session.Lifecycle) error
	Delete(clientID string) error
}

// TenantSessionCounter reports the session and connected lifecycle counts for one tenant.
type TenantSessionCounter interface {
	TenantSessionCounts(tenancy.TenantID) (sessions, connected int)
}

// OwnershipCoordinator is the application-facing committed ownership boundary.
type OwnershipCoordinator interface {
	AcquireOwnership(context.Context, string, string, uint64) (uint64, error)
}

// CredentialRevocationChecker reports the committed revocation status for one credential.
type CredentialRevocationChecker interface {
	IsCredentialRevoked(context.Context, string) (bool, error)
}

// ResultDTO reports the connection response, accepted session, committed owner
// epoch, and recovered durable delivery state.
type ResultDTO struct {
	SessionID       routing.SessionID
	Principal       security.Principal
	ConnAck         mqtt.ConnAck
	Accepted        bool
	OwnershipEpoch  uint64
	DeliverySession *membership.DeliverySessionStateDTO
}

// ConnectRequestDTO carries the MQTT CONNECT packet and optional verified certificate identity.
type ConnectRequestDTO struct {
	Packet            mqtt.Connect
	ClientCertificate *applicationauthentication.ClientCertificateDTO
	SourceIP          string // remote host without port; empty when unknown
}

// Option configures optional parameters for the connection use case.
type Option func(*UseCase)

// WithSecurity configures one credential provider, optional mTLS authentication, and the security policy.
func WithSecurity(
	credentialProvider applicationauthentication.AuthenticationProvider,
	mtlsProvider applicationauthentication.ClientCertificateAuthenticationProvider,
	policy security.Policy,
	credentialMethod string,
) Option {
	return func(useCase *UseCase) {
		useCase.authenticator = credentialProvider
		useCase.certificateAuthenticator = mtlsProvider
		useCase.policy = policy
		useCase.credentialMethod = credentialMethod
	}
}

// WithTenantLimits configures tenant admission before ownership and state changes.
func WithTenantLimits(limits *tenantlimit.Service, counter TenantSessionCounter) Option {
	return func(useCase *UseCase) {
		useCase.tenantLimits = limits
		useCase.tenantCounter = counter
	}
}

// WithMetrics configures a metrics recorder for connection and authentication events.
func WithMetrics(metrics portsmetrics.MetricsRecorder) Option {
	return func(useCase *UseCase) {
		useCase.metrics = metrics
	}
}

// WithDataPlaneAudit records MQTT authentication and authorization denials.
func WithDataPlaneAudit(recorder portsaudit.DataPlaneDenialRecorder) Option {
	return func(useCase *UseCase) { useCase.dataPlaneAudit = recorder }
}

// WithOwnershipCoordinator commits cluster ownership before local session activation.
func WithOwnershipCoordinator(coordinator OwnershipCoordinator, ownerNodeID uint64) Option {
	return func(useCase *UseCase) {
		useCase.ownershipCoordinator = coordinator
		useCase.ownerNodeID = ownerNodeID
	}
}

// WithCredentialRevocationChecker configures committed-state checks before CONNECT ownership and Will publication.
func WithCredentialRevocationChecker(checker CredentialRevocationChecker) Option {
	return func(useCase *UseCase) {
		useCase.credentialRevocationChecker = checker
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

// WithDurableDeliveryCommitter commits attachment and cleanup transitions for
// the session's replicated QoS 1/2 state.
func WithDurableDeliveryCommitter(committer deliverystate.Committer) Option {
	return func(useCase *UseCase) {
		useCase.durableDeliveryCommitter = committer
	}
}

// UseCase serializes lifecycle persistence, routing cleanup and queue
// registration so one process cannot activate or disconnect the same session
// through interleaved operations.
type clientTimers struct {
	willTimer   Timer
	expiryTimer Timer
	willOrigin  willOrigin
}

type UseCase struct {
	mu                          sync.Mutex
	clock                       applicationclock.Clock
	authenticator               applicationauthentication.AuthenticationProvider
	credentialMethod            string
	certificateAuthenticator    applicationauthentication.ClientCertificateAuthenticationProvider
	policy                      security.Policy
	sessions                    SessionRepository
	tenantLimits                *tenantlimit.Service
	tenantCounter               TenantSessionCounter
	principals                  map[routing.SessionID]security.Principal
	sessionKeys                 map[routing.SessionID]tenancy.SessionKey
	deliveries                  deliveryqueue.Queue
	routes                      *routing.Table
	scheduler                   Scheduler
	willPublisher               WillPublisher
	activeTimers                map[string]*clientTimers
	closed                      bool
	metrics                     portsmetrics.MetricsRecorder
	willOutcomes                portsmetrics.WillOutcomeRecorder
	dataPlaneAudit              portsaudit.DataPlaneDenialRecorder
	ownershipCoordinator        OwnershipCoordinator
	credentialRevocationChecker CredentialRevocationChecker
	ownerNodeID                 uint64
	offlineCoordinator          OfflineCoordinator
	durableDeliveryCommitter    deliverystate.Committer
	sessionMetadataCoordinator  SessionMetadataCoordinator
	ownershipEpochs             map[routing.SessionID]uint64
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

func (useCase *UseCase) rejectAuthentication(request ConnectRequestDTO, method, reason string, identity *applicationauthentication.Result) {
	useCase.recordAuth(method, false, reason)
	if useCase == nil || useCase.dataPlaneAudit == nil {
		return
	}
	denial := portsaudit.DataPlaneDenialDTO{
		Action:     portsaudit.DataPlaneActionConnect,
		Reason:     reason,
		ClientID:   request.Packet.ClientID,
		SourceIP:   request.SourceIP,
		AuthMethod: method,
	}
	if identity != nil {
		denial.TenantID = identity.TenantID.String()
		denial.Subject = identity.Subject
		denial.CredentialID = identity.CredentialID
		denial.Roles = make([]string, len(identity.Roles))
		for index, role := range identity.Roles {
			denial.Roles[index] = string(role)
		}
	}
	if (reason == "invalid_will_topic" || reason == "will_unauthorized") && request.Packet.Will != nil {
		denial.Topic = request.Packet.Will.Topic
	}
	useCase.dataPlaneAudit.RecordDataPlaneDenial(denial)
}

// RecordCredentialRevocationDenial records a CONNECT rejected by the TCP post-ownership check.
func (useCase *UseCase) RecordCredentialRevocationDenial(request ConnectRequestDTO, principal security.Principal) {
	if useCase == nil {
		return
	}
	method := portsmetrics.AuthMethodAnonymous
	if request.ClientCertificate != nil {
		method = portsmetrics.AuthMethodTLS
	} else if useCase.authenticator != nil {
		method = useCase.credentialMethod
	}
	identity := &applicationauthentication.Result{
		TenantID: principal.TenantID(), Subject: principal.Subject(), CredentialID: principal.CredentialID(),
		Roles: principal.Roles(), ExpiresAt: principal.ExpiresAt(),
	}
	useCase.rejectAuthentication(request, method, "credential_revoked", identity)
}

func (useCase *UseCase) credentialRevoked(ctx context.Context, principal security.Principal) (bool, error) {
	if principal.CredentialID() == "" || useCase.credentialRevocationChecker == nil {
		return false, nil
	}
	return useCase.credentialRevocationChecker.IsCredentialRevoked(ctx, principal.CredentialID())
}

func (useCase *UseCase) recordCredentialRevokedWill(principal security.Principal, will *mqtt.Will) {
	if useCase.dataPlaneAudit == nil || will == nil {
		return
	}
	roles := principal.Roles()
	roleNames := make([]string, len(roles))
	for index, role := range roles {
		roleNames[index] = string(role)
	}
	useCase.dataPlaneAudit.RecordDataPlaneDenial(portsaudit.DataPlaneDenialDTO{
		Action: portsaudit.DataPlaneActionWill, Reason: "credential_revoked",
		TenantID: principal.TenantID().String(), Subject: principal.Subject(), CredentialID: principal.CredentialID(),
		Roles: roleNames, ClientID: principal.ClientID(), Topic: will.Topic,
	})
}

func (useCase *UseCase) authorizeWillPublication(principal security.Principal, topic string) bool {
	willTopic, err := tenancy.NewTopic(principal.TenantID(), topic)
	if err == nil && useCase.policy.AuthorizeTopic(principal, security.ActionWill, willTopic) == nil {
		return true
	}
	if useCase.dataPlaneAudit != nil {
		useCase.dataPlaneAudit.RecordDataPlaneDenial(audit.ACLDenial(portsaudit.DataPlaneActionWill, principal, topic, ""))
	}
	return false
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
	if useCase.authenticator != nil &&
		useCase.credentialMethod != portsmetrics.AuthMethodJWT &&
		useCase.credentialMethod != portsmetrics.AuthMethodPassword {
		return nil, ErrInvalidAuthenticationMethod
	}

	if useCase.durableDeliveryCommitter != nil && (useCase.ownershipCoordinator == nil || useCase.ownerNodeID == 0) {
		return nil, ErrDurableDeliveryRejected
	}
	if useCase.tenantLimits != nil && useCase.tenantCounter == nil {
		return nil, ErrNilTenantSessionCounter
	}
	return useCase, nil
}
func (useCase *UseCase) commitDurableDelivery(ctx context.Context, change membership.DeliveryChangeDTO) (membership.ApplyResultDTO, error) {
	if useCase.durableDeliveryCommitter == nil {
		return membership.ApplyResultDTO{}, ErrDurableDeliveryRejected
	}
	result, err := useCase.durableDeliveryCommitter.CommitDelivery(ctx, change)
	if err != nil {
		return membership.ApplyResultDTO{}, err
	}
	if result.Outcome != membership.ApplyOutcomeApplied {
		return membership.ApplyResultDTO{}, fmt.Errorf("%w: outcome=%d conflict=%s", ErrDurableDeliveryRejected, result.Outcome, result.Conflict)
	}
	return result, nil
}

func (useCase *UseCase) attachDurableDelivery(
	ctx context.Context,
	sessionKey tenancy.SessionKey,
	sessionID routing.SessionID,
	epoch uint64,
	version mqtt.ProtocolVersion,
	cleanStart bool,
	now time.Time,
) (*membership.DeliverySessionStateDTO, bool, error) {
	proposedAt := now.UnixMilli()
	if proposedAt <= 0 {
		return nil, false, ErrDurableDeliveryRejected
	}
	result, err := useCase.commitDurableDelivery(ctx, membership.DeliveryChangeDTO{
		TenantID:             sessionKey.TenantID().String(),
		ClientID:             sessionKey.ClientID(),
		OwnerNodeID:          useCase.ownerNodeID,
		Epoch:                epoch,
		Action:               membership.DeliveryActionAttach,
		ProposedAtUnixMillis: uint64(proposedAt),
		Reset:                cleanStart,
	})
	if err != nil {
		return nil, false, err
	}
	if result.Delivery == nil || result.Delivery.Session == nil {
		return nil, false, ErrDurableDeliveryRejected
	}
	state := result.Delivery.Session
	if err := useCase.resetDurableQueue(sessionID, epoch, version, *state, now); err != nil {
		return nil, false, err
	}
	return state, result.Delivery.Found, nil
}

func (useCase *UseCase) resetDurableQueue(
	sessionID routing.SessionID,
	epoch uint64,
	version mqtt.ProtocolVersion,
	state membership.DeliverySessionStateDTO,
	now time.Time,
) error {
	queued := make([]deliveryqueue.DeliveryDTO, 0, len(state.Messages))
	outbound := make([]deliveryqueue.OutboundDTO, 0, len(state.Messages))
	for _, message := range state.Messages {
		expiresAt, err := deliveryExpiryTime(message.ExpiresAtUnixMillis)
		if err != nil {
			return err
		}
		switch message.Phase {
		case membership.DeliveryPhaseQueued:
			if message.PacketID != 0 || message.QoS == 0 || message.QoS > uint32(mqtt.QoSExactlyOnce) {
				return ErrDurableDeliveryRejected
			}
			queued = append(queued, deliveryqueue.DeliveryDTO{
				SessionID: sessionID, Topic: message.Topic, Payload: message.Payload,
				QoS: mqtt.QoS(message.QoS), Retain: message.Retain, ExpiresAt: expiresAt,
				DurableSequence: message.Sequence, DurableEpoch: epoch,
			})
		case membership.DeliveryPhaseAwaitPubAck, membership.DeliveryPhaseAwaitPubRec, membership.DeliveryPhaseAwaitPubComp:
			if message.PacketID == 0 || message.QoS == 0 || message.QoS > uint32(mqtt.QoSExactlyOnce) {
				return ErrDurableDeliveryRejected
			}
			packetID := mqtt.PacketIdentifier(message.PacketID)
			publish := mqtt.Publish{
				Version: version, QoS: mqtt.QoS(message.QoS), PacketID: packetID,
				Topic: message.Topic, Payload: message.Payload, Retain: message.Retain,
			}
			if version == mqtt.ProtocolV5 && !expiresAt.IsZero() {
				publish.HasMessageExpiryInterval = true
				publish.MessageExpiryInterval, _ = (deliveryqueue.DeliveryDTO{ExpiresAt: expiresAt}).RemainingExpiryInterval(now)
			}
			outboundState, _, err := delivery.NewOutbound(publish)
			if err != nil {
				return fmt.Errorf("%w: %v", ErrDurableDeliveryRejected, err)
			}
			switch message.Phase {
			case membership.DeliveryPhaseAwaitPubAck:
				if message.QoS != uint32(mqtt.QoSAtLeastOnce) {
					return ErrDurableDeliveryRejected
				}
			case membership.DeliveryPhaseAwaitPubRec:
				if message.QoS != uint32(mqtt.QoSExactlyOnce) {
					return ErrDurableDeliveryRejected
				}
			case membership.DeliveryPhaseAwaitPubComp:
				if message.QoS != uint32(mqtt.QoSExactlyOnce) {
					return ErrDurableDeliveryRejected
				}
				outboundState, _, err = outboundState.Receive(mqtt.PubRec{Version: version, PacketID: packetID})
				if err != nil {
					return fmt.Errorf("%w: %v", ErrDurableDeliveryRejected, err)
				}
			}
			outbound = append(outbound, deliveryqueue.OutboundDTO{
				SessionID: sessionID, PacketID: packetID, State: outboundState, ExpiresAt: expiresAt,
				DurableSequence: message.Sequence, DurableEpoch: epoch,
			})
		default:
			return ErrDurableDeliveryRejected
		}
	}
	sort.Slice(outbound, func(i, j int) bool { return outbound[i].PacketID < outbound[j].PacketID })
	if err := useCase.deliveries.ResetDurable(sessionID, epoch, queued, outbound, now); err != nil {
		return fmt.Errorf("reset durable delivery queue: %w", err)
	}
	if activator, ok := useCase.durableDeliveryCommitter.(deliverystate.Activator); ok {
		if err := activator.ActivateDelivery(sessionID, epoch, state.NextSequence-1); err != nil {
			return fmt.Errorf("activate durable delivery projection: %w", err)
		}
	}
	return nil
}

func deliveryExpiryTime(unixMillis uint64) (time.Time, error) {
	if unixMillis == 0 {
		return time.Time{}, nil
	}
	if unixMillis > uint64(1<<63-1) {
		return time.Time{}, ErrDurableDeliveryRejected
	}
	return time.UnixMilli(int64(unixMillis)), nil
}

func (useCase *UseCase) commitDeliveryLifecycle(
	ctx context.Context,
	sessionKey tenancy.SessionKey,
	epoch uint64,
	action membership.DeliveryAction,
	proposedAt time.Time,
	sessionExpiresAt time.Time,
) error {
	if useCase.durableDeliveryCommitter == nil {
		return nil
	}
	proposedMillis := proposedAt.UnixMilli()
	if epoch == 0 || proposedMillis <= 0 {
		return ErrDurableDeliveryRejected
	}
	expiresMillis := uint64(0)
	if !sessionExpiresAt.IsZero() {
		millis := sessionExpiresAt.UnixMilli()
		if millis <= 0 {
			return ErrDurableDeliveryRejected
		}
		expiresMillis = uint64(millis)
	}
	_, err := useCase.commitDurableDelivery(ctx, membership.DeliveryChangeDTO{
		TenantID:                   sessionKey.TenantID().String(),
		ClientID:                   sessionKey.ClientID(),
		OwnerNodeID:                useCase.ownerNodeID,
		Epoch:                      epoch,
		Action:                     action,
		ProposedAtUnixMillis:       uint64(proposedMillis),
		SessionExpiresAtUnixMillis: expiresMillis,
	})
	return err
}

// Connect validates the development listener's supported session policy and
// opens a new or resumable lifecycle. Unsupported credentials and Will state
// are returned as negative CONNACK results rather than silently ignored.
func (useCase *UseCase) Connect(packet mqtt.Connect) (ResultDTO, error) {
	return useCase.ConnectContext(context.Background(), ConnectRequestDTO{Packet: packet})
}

// ConnectContext validates authentication, tenant binding, revocation, and Will authorization
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
			useCase.rejectAuthentication(request, authMethod, "unsupported_credentials", nil)
			result.ConnAck.ReasonCode = notAuthorizedReason(packet.Version)
			return result, nil
		}
		if packet.HasUsername || packet.HasPassword {
			useCase.rejectAuthentication(request, authMethod, "dual_credentials", nil)
			result.ConnAck.ReasonCode = notAuthorizedReason(packet.Version)
			return result, nil
		}
		var err error
		authResult, err = useCase.certificateAuthenticator.AuthenticateClientCertificate(ctx, *request.ClientCertificate)
		if err != nil {
			useCase.rejectAuthentication(request, authMethod, "invalid_credentials", nil)
			result.ConnAck.ReasonCode = notAuthorizedReason(packet.Version)
			return result, nil
		}
		authenticated = true
	case useCase.authenticator != nil:
		authMethod = useCase.credentialMethod
		if !packet.HasUsername || !packet.HasPassword || len(packet.Password) == 0 {
			useCase.rejectAuthentication(request, authMethod, "missing_credentials", nil)
			result.ConnAck.ReasonCode = notAuthorizedReason(packet.Version)
			return result, nil
		}
		var err error
		authResult, err = useCase.authenticator.Authenticate(ctx, applicationauthentication.Credential{
			Username: packet.Username,
			Password: packet.Password,
		})
		if err != nil {
			useCase.rejectAuthentication(request, authMethod, "invalid_credentials", nil)
			result.ConnAck.ReasonCode = notAuthorizedReason(packet.Version)
			return result, nil
		}
		authenticated = true
	case useCase.certificateAuthenticator != nil:
		authMethod = portsmetrics.AuthMethodTLS
		useCase.rejectAuthentication(request, authMethod, "missing_certificate", nil)
		result.ConnAck.ReasonCode = notAuthorizedReason(packet.Version)
		return result, nil
	default:
		if packet.HasUsername || packet.HasPassword {
			useCase.rejectAuthentication(request, portsmetrics.AuthMethodAnonymous, "unsupported_credentials", nil)
			result.ConnAck.ReasonCode = unsupportedDevelopmentReason(packet.Version)
			return result, nil
		}
		useCase.recordAuth(portsmetrics.AuthMethodAnonymous, true, "none")
	}

	if authenticated {
		if authResult.CredentialID != "" && useCase.credentialRevocationChecker != nil {
			revoked, checkErr := useCase.credentialRevocationChecker.IsCredentialRevoked(ctx, authResult.CredentialID)
			if checkErr != nil {
				result.ConnAck.ReasonCode = ownershipUnavailableReason(packet.Version)
				return result, nil
			}
			if revoked {
				useCase.rejectAuthentication(request, authMethod, "credential_revoked", &authResult)
				result.ConnAck.ReasonCode = notAuthorizedReason(packet.Version)
				return result, nil
			}
		}
		allowed := false
		for _, allowedID := range authResult.AllowedClientIDs {
			if allowedID == packet.ClientID {
				allowed = true
				break
			}
		}
		if !allowed {
			useCase.rejectAuthentication(request, authMethod, "client_id_not_allowed", &authResult)
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
			useCase.rejectAuthentication(request, authMethod, "not_authorized", &authResult)
			result.ConnAck.ReasonCode = notAuthorizedReason(packet.Version)
			return result, nil
		}
		principal = p
		if packet.Will != nil {
			willTopic, err := tenancy.NewTopic(authResult.TenantID, packet.Will.Topic)
			if err != nil {
				useCase.rejectAuthentication(request, authMethod, "invalid_will_topic", &authResult)
				result.ConnAck.ReasonCode = notAuthorizedReason(packet.Version)
				return result, nil
			}
			if err := useCase.policy.AuthorizeTopic(principal, security.ActionWill, willTopic); err != nil {
				useCase.rejectAuthentication(request, authMethod, "will_unauthorized", &authResult)
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
	var supersededPublish *mqtt.Will
	var superseded *membership.SupersededWillDTO
	defer func() {
		useCase.mu.Unlock()
		if supersededPublish != nil {
			_ = useCase.willPublisher.PublishWill(sessionID, sessionKey, *supersededPublish)
		}
	}()

	timerKey := string(sessionID)
	localWillPending := useCase.activeTimers[timerKey] != nil && useCase.activeTimers[timerKey].willTimer != nil

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
	if useCase.tenantLimits != nil && principal.Subject() != "" {
		tenantID := principal.TenantID()
		limits := useCase.tenantLimits.Limits(tenantID)
		sessions, connected := useCase.tenantCounter.TenantSessionCounts(tenantID)
		if limits.MaxConnections > 0 && !(found && existing.Connected()) && uint64(connected) >= limits.MaxConnections {
			useCase.tenantLimits.Reject(tenantID, tenantlimit.DimensionConnections)
			result.ConnAck.ReasonCode = quotaExceededReason(packet.Version)
			return result, nil
		}
		if limits.MaxSessions > 0 && !found && uint64(sessions) >= limits.MaxSessions {
			useCase.tenantLimits.Reject(tenantID, tenantlimit.DimensionSessions)
			result.ConnAck.ReasonCode = quotaExceededReason(packet.Version)
			return result, nil
		}
	}
	if useCase.ownershipCoordinator == nil {
		if localWillPending {
			useCase.recordWillOutcome(portsmetrics.WillOutcomeSuppressedReconnectedWithinDelay)
		}
		useCase.stopSessionTimers(timerKey)
	}
	var ownershipEpoch uint64
	ownershipTenantID := "default"
	if principal.Subject() != "" {
		ownershipTenantID = principal.TenantID().String()
	}
	if useCase.ownershipCoordinator != nil {
		if coordinator, ok := useCase.ownershipCoordinator.(SessionOwnershipCoordinator); ok {
			request := SessionOwnershipRequestDTO{
				TenantID: ownershipTenantID, ClientID: packet.ClientID, OwnerNodeID: useCase.ownerNodeID,
				Session: sessionConnectDTO(opened, principal),
			}
			acquired, acquireErr := coordinator.AcquireSessionOwnership(ctx, request)
			ownershipEpoch, err = acquired.Epoch, acquireErr
			superseded = acquired.SupersededWill
		} else {
			ownershipEpoch, err = useCase.ownershipCoordinator.AcquireOwnership(ctx, ownershipTenantID, packet.ClientID, useCase.ownerNodeID)
		}
		if err != nil || ownershipEpoch == 0 {
			result.ConnAck.ReasonCode = ownershipUnavailableReason(packet.Version)
			return result, nil
		}
		if superseded != nil {
			supersededPublish = useCase.resolveSupersededWillLocked(sessionKey, superseded)
		}
	}
	if ownershipEpoch != 0 {
		useCase.ownershipEpochs[sessionID] = ownershipEpoch
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

	var (
		durableDeliverySession *membership.DeliverySessionStateDTO
		durableDeliveryPresent bool
		offlinePresent         bool
	)
	if useCase.durableDeliveryCommitter != nil {
		durableDeliverySession, durableDeliveryPresent, err = useCase.attachDurableDelivery(
			ctx, sessionKey, sessionID, ownershipEpoch, packet.Version, packet.CleanStart, now,
		)
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
		if packet.CleanStart || !durableDeliveryPresent {
			useCase.routes.RemoveSession(sessionID)
			_ = useCase.routes.BindSessionTenant(sessionID, sessionTenant)
		}
	}
	if useCase.ownershipCoordinator != nil && useCase.offlineCoordinator == nil {
		if localWillPending && superseded == nil {
			useCase.recordWillOutcome(portsmetrics.WillOutcomeSuppressedReconnectedWithinDelay)
		}
		useCase.stopSessionTimers(timerKey)
	}
	if useCase.offlineCoordinator != nil && ownershipEpoch != 0 {
		claim, err := useCase.offlineCoordinator.ClaimOffline(ctx, ownershipTenantID, packet.ClientID, useCase.ownerNodeID, ownershipEpoch, now)
		if err != nil {
			if deactivator, ok := useCase.durableDeliveryCommitter.(deliverystate.Deactivator); ok {
				deactivator.DeactivateDelivery(sessionID, ownershipEpoch)
			}
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
		if localWillPending && superseded == nil {
			useCase.recordWillOutcome(portsmetrics.WillOutcomeSuppressedReconnectedWithinDelay)
		}
		useCase.stopSessionTimers(timerKey)
		offlinePresent = claim.Found
		if claim.Found && !packet.CleanStart {
			present = true
			if len(claim.Deliveries) > 0 {
				_ = useCase.deliveries.TryEnqueueBatch(claim.Deliveries, now)
			}
		}
	}
	if useCase.durableDeliveryCommitter != nil {
		present = !packet.CleanStart && (durableDeliveryPresent || offlinePresent)
	}
	if sessionKey != (tenancy.SessionKey{}) {
		useCase.sessionKeys[sessionID] = sessionKey
	} else {
		delete(useCase.sessionKeys, sessionID)
	}
	useCase.recordSessionGauge()
	return ResultDTO{
		SessionID: sessionID, Principal: principal, OwnershipEpoch: ownershipEpoch, DeliverySession: durableDeliverySession,
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
	return useCase.disconnect(sessionID, reason, 0, false)
}

// DisconnectIfOwnershipEpoch closes one lifecycle only while its local
// ownership epoch still matches the connection that is being cleaned up.
func (useCase *UseCase) DisconnectIfOwnershipEpoch(
	sessionID routing.SessionID, reason session.DisconnectReason, expectedEpoch uint64,
) error {
	return useCase.disconnect(sessionID, reason, expectedEpoch, true)
}

func (useCase *UseCase) disconnect(
	sessionID routing.SessionID, reason session.DisconnectReason, expectedEpoch uint64, checkOwnershipEpoch bool,
) error {
	if useCase == nil {
		return ErrNilSessionRepository
	}
	useCase.mu.Lock()
	if checkOwnershipEpoch {
		currentEpoch, exists := useCase.ownershipEpochs[sessionID]
		if currentEpoch != expectedEpoch || exists != (expectedEpoch != 0) {
			useCase.mu.Unlock()
			return nil
		}
	}
	outcome, err := useCase.disconnectLocked(sessionID, reason)
	useCase.mu.Unlock()
	if err != nil {
		return err
	}
	if outcome.will != nil {
		revoked, checkErr := useCase.credentialRevoked(context.Background(), outcome.principal)
		if revoked || checkErr != nil {
			if revoked {
				useCase.recordCredentialRevokedWill(outcome.principal, outcome.will)
			}
			return outcome.transitionErr
		}
		if err := useCase.willPublisher.PublishWill(outcome.sessionID, outcome.sessionKey, *outcome.will); err != nil {
			return errors.Join(outcome.transitionErr, err)
		}
	}
	return outcome.transitionErr
}

type disconnectOutcome struct {
	sessionID     routing.SessionID
	sessionKey    tenancy.SessionKey
	principal     security.Principal
	will          *mqtt.Will
	transitionErr error
}

// disconnectLocked mutates local lifecycle state while the caller owns mu.
func (useCase *UseCase) disconnectLocked(sessionID routing.SessionID, reason session.DisconnectReason) (disconnectOutcome, error) {
	timerKey := string(sessionID)
	outcome := disconnectOutcome{sessionID: sessionID}
	outcome.principal = useCase.principals[sessionID]
	useCase.stopSessionTimers(timerKey)
	sessionKey, hasSessionKey := useCase.sessionKeys[sessionID]
	if !hasSessionKey {
		if principal, ok := useCase.principals[sessionID]; ok && principal.Subject() != "" {
			sessionKey, _ = tenancy.NewSessionKey(principal.TenantID(), principal.ClientID())
		}
	}
	outcome.sessionKey = sessionKey
	state, found, err := useCase.loadSession(sessionKey, timerKey)
	if err != nil {
		return outcome, err
	}
	if !found {
		return outcome, ErrSessionNotFound
	}
	now := useCase.clock.Now()
	hadPendingWill := state.HasPendingWill()
	origin := willOriginFor(reason)
	result, err := state.Disconnect(now, reason)
	if err != nil {
		return outcome, err
	}
	epoch, hasEpoch := useCase.ownershipEpochs[sessionID]
	var expires time.Time
	if result.ScheduleExpiry {
		expires = now.Add(result.ExpiryDuration)
	}
	if result.SessionRemoved {
		if useCase.durableDeliveryCommitter != nil {
			if !hasSessionKey || !hasEpoch {
				return outcome, ErrDurableDeliveryRejected
			}
			if err := useCase.saveSession(sessionKey, timerKey, result.Lifecycle); err != nil {
				return outcome, err
			}
			if err := useCase.commitDeliveryLifecycle(context.Background(), sessionKey, epoch, membership.DeliveryActionDiscard, now, time.Time{}); err != nil {
				return outcome, err
			}
		}
		if err := useCase.deleteSession(sessionKey, timerKey); err != nil {
			return outcome, err
		}
		useCase.deliveries.Remove(sessionID)
		if useCase.ownershipCoordinator == nil || useCase.durableDeliveryCommitter != nil {
			useCase.routes.RemoveSession(sessionID)
		}
	} else {
		if err := useCase.saveSession(sessionKey, timerKey, result.Lifecycle); err != nil {
			return outcome, err
		}
		if useCase.durableDeliveryCommitter != nil {
			if !hasSessionKey || !hasEpoch {
				outcome.transitionErr = ErrDurableDeliveryRejected
			} else {
				outcome.transitionErr = useCase.commitDeliveryLifecycle(context.Background(), sessionKey, epoch, membership.DeliveryActionDetach, now, expires)
			}
		}
		if useCase.offlineCoordinator != nil && hasEpoch && hasSessionKey {
			pending, beginErr := useCase.deliveries.BeginDetach(sessionID, now)
			if beginErr == nil {
				qosZeroPending := pending[:0]
				for _, item := range pending {
					if item.DurableSequence == 0 && item.QoS == mqtt.QoSAtMostOnce {
						qosZeroPending = append(qosZeroPending, item)
					}
				}
				detachCtx, cancel := context.WithTimeout(context.Background(), offlineDetachTimeout)
				detachErr := useCase.offlineCoordinator.DetachSession(detachCtx, DetachRequestDTO{
					TenantID: sessionKey.TenantID().String(), ClientID: sessionKey.ClientID(), OwnerNodeID: useCase.ownerNodeID,
					Epoch: epoch, SessionExpiresAt: expires, DetachedAt: now, Pending: qosZeroPending,
				})
				cancel()
				useCase.deliveries.FinishDetach(sessionID, detachErr == nil)
			}
		}
	}
	will := result.Will
	shouldPublish := result.PublishWillNow && will != nil
	if shouldPublish && useCase.authenticator != nil {
		principal, ok := useCase.principals[sessionID]
		if !ok {
			shouldPublish = false
		} else {
			if !useCase.authorizeWillPublication(principal, will.Topic) {
				shouldPublish = false
			}
		}
	}
	metadataCommitFailed := false
	if coordinator := useCase.sessionMetadataCoordinator; coordinator != nil && hasEpoch && hasSessionKey {
		action := membership.SessionMetadataActionDetach
		if result.SessionRemoved {
			action = membership.SessionMetadataActionDiscard
		}
		disposition := membership.SessionWillDispositionDiscard
		due := uint64(0)
		if shouldPublish {
			disposition = membership.SessionWillDispositionClaim
		} else if result.ScheduleWill {
			disposition = membership.SessionWillDispositionSchedule
			due = uint64(now.Add(result.WillDelay).UnixMilli())
		}
		expiresAt := uint64(0)
		if !result.SessionRemoved && !expires.IsZero() {
			expiresAt = uint64(expires.UnixMilli())
		}
		change := membership.SessionMetadataChangeDTO{
			TenantID: sessionKey.TenantID().String(), ClientID: sessionKey.ClientID(), OwnerNodeID: useCase.ownerNodeID,
			Epoch: epoch, Action: action, WillDisposition: disposition, ProposedAtUnixMillis: uint64(now.UnixMilli()),
			SessionExpiresAtUnixMillis: expiresAt, WillDueAtUnixMillis: due,
		}
		ctx, cancel := context.WithTimeout(context.Background(), sessionMetadataCommitTimeout)
		committed, commitErr := coordinator.CommitSessionMetadata(ctx, change)
		cancel()
		if commitErr != nil {
			metadataCommitFailed = true
			outcome.transitionErr = errors.Join(outcome.transitionErr, commitErr)
			shouldPublish = false
		} else if committed.Replicated {
			will = committed.Will
			shouldPublish = will != nil
		}
	}
	if reason == session.DisconnectNormal && hadPendingWill && !metadataCommitFailed {
		useCase.recordWillOutcome(portsmetrics.WillOutcomeSuppressedNormalDisconnect)
	}
	if shouldPublish {
		useCase.recordWillOutcome(immediateWillReason(origin, result.Lifecycle.Version(), will.DelayInterval, result.Lifecycle.Retention() == session.RetentionImmediate))
	}
	if result.SessionRemoved {
		delete(useCase.principals, sessionID)
		delete(useCase.sessionKeys, sessionID)
		delete(useCase.ownershipEpochs, sessionID)
	}
	if shouldPublish {
		outcome.will = will
	}
	if !result.SessionRemoved && (result.ScheduleWill || result.ScheduleExpiry) {
		timers := &clientTimers{willOrigin: origin}
		if result.ScheduleWill {
			timers.willTimer = useCase.scheduler.AfterFunc(result.WillDelay, func() { useCase.onWillTimerForTimer(sessionID, timers) })
		}
		if result.ScheduleExpiry {
			timers.expiryTimer = useCase.scheduleExpiryTimerLocked(sessionID, result.ExpiryDuration, timers)
		}
		useCase.activeTimers[timerKey] = timers
	}
	useCase.recordSessionGauge()
	return outcome, nil
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
	origin := willOriginUnplanned
	if ok {
		origin = timers.willOrigin
	}
	if ok {
		timers.willTimer = nil
		if timers.expiryTimer == nil {
			delete(useCase.activeTimers, timerKey)
		}
	}
	sessionKey, hasSessionKey := useCase.sessionKeys[sessionID]
	if !hasSessionKey {
		if principal, ok := useCase.principals[sessionID]; ok && principal.Subject() != "" {
			sessionKey, _ = tenancy.NewSessionKey(principal.TenantID(), principal.ClientID())
			hasSessionKey = sessionKey != (tenancy.SessionKey{})
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
	updated, will, claimed := state.ClaimWill()
	if !claimed {
		useCase.mu.Unlock()
		return
	}
	if err := useCase.saveSession(sessionKey, timerKey, updated); err != nil {
		useCase.mu.Unlock()
		return
	}
	shouldPublish := will != nil
	principal, hasPrincipal := useCase.principals[sessionID]
	if shouldPublish && useCase.authenticator != nil {
		if !hasPrincipal || !useCase.authorizeWillPublication(principal, will.Topic) {
			shouldPublish = false
		}
	}
	if shouldPublish && hasPrincipal {
		revoked, checkErr := useCase.credentialRevoked(context.Background(), principal)
		if revoked || checkErr != nil {
			if revoked {
				useCase.recordCredentialRevokedWill(principal, will)
			}
			shouldPublish = false
		}
	}
	if coordinator := useCase.sessionMetadataCoordinator; coordinator != nil && hasSessionKey {
		if epoch, hasEpoch := useCase.ownershipEpochs[sessionID]; hasEpoch {
			now := useCase.clock.Now()
			ctx, cancel := context.WithTimeout(context.Background(), sessionMetadataCommitTimeout)
			outcome, commitErr := coordinator.CommitSessionMetadata(ctx, membership.SessionMetadataChangeDTO{
				TenantID: sessionKey.TenantID().String(), ClientID: sessionKey.ClientID(), OwnerNodeID: useCase.ownerNodeID,
				Epoch: epoch, Action: membership.SessionMetadataActionClaimWill, ProposedAtUnixMillis: uint64(now.UnixMilli()),
			})
			cancel()
			if commitErr != nil {
				shouldPublish = false
			} else if outcome.Replicated {
				will = outcome.Will
				shouldPublish = will != nil
			}
		}
	}
	useCase.mu.Unlock()
	if shouldPublish {
		useCase.recordWillOutcome(delayedWillReason(origin))
		_ = useCase.willPublisher.PublishWill(sessionID, sessionKey, *will)
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
	origin := willOriginUnplanned
	if ok {
		origin = timers.willOrigin
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
	if coordinator := useCase.sessionMetadataCoordinator; coordinator != nil {
		epoch, hasEpoch := useCase.ownershipEpochs[sessionID]
		if hasSessionKey && hasEpoch {
			ctx, cancel := context.WithTimeout(context.Background(), sessionMetadataCommitTimeout)
			metadataOutcome, metadataErr := coordinator.CommitSessionMetadata(ctx, membership.SessionMetadataChangeDTO{
				TenantID: sessionKey.TenantID().String(), ClientID: sessionKey.ClientID(), OwnerNodeID: useCase.ownerNodeID,
				Epoch: epoch, Action: membership.SessionMetadataActionDiscard, WillDisposition: membership.SessionWillDispositionClaim,
				ProposedAtUnixMillis: uint64(now.UnixMilli()),
			})
			cancel()
			if errors.Is(metadataErr, ErrSessionOwnershipStale) {
				_ = useCase.deleteSession(sessionKey, timerKey)
				useCase.deliveries.Remove(sessionID)
				if useCase.ownershipCoordinator == nil || useCase.durableDeliveryCommitter != nil {
					useCase.routes.RemoveSession(sessionID)
				}
				delete(useCase.principals, sessionID)
				delete(useCase.sessionKeys, sessionID)
				delete(useCase.ownershipEpochs, sessionID)
				useCase.mu.Unlock()
				return
			}
			if metadataErr != nil {
				if !useCase.closed {
					retryTimers := &clientTimers{willOrigin: origin}
					retryTimers.expiryTimer = useCase.scheduleExpiryTimerLocked(sessionID, time.Second, retryTimers)
					useCase.activeTimers[timerKey] = retryTimers
				}
				useCase.mu.Unlock()
				return
			}
			if metadataOutcome.Replicated {
				will = metadataOutcome.Will
			}
		}
	}
	if useCase.durableDeliveryCommitter != nil {
		epoch, hasEpoch := useCase.ownershipEpochs[sessionID]
		if !hasSessionKey || !hasEpoch || useCase.commitDeliveryLifecycle(
			context.Background(), sessionKey, epoch, membership.DeliveryActionDiscard, now, time.Time{},
		) != nil {
			if !useCase.closed {
				retryTimers := &clientTimers{willOrigin: origin}
				retryTimers.expiryTimer = useCase.scheduleExpiryTimerLocked(sessionID, time.Second, retryTimers)
				useCase.activeTimers[timerKey] = retryTimers
			}
			useCase.mu.Unlock()
			return
		}
	}
	if err := useCase.deleteSession(sessionKey, timerKey); err != nil {
		if !useCase.closed {
			retryTimers := &clientTimers{willOrigin: origin}
			retryTimers.expiryTimer = useCase.scheduleExpiryTimerLocked(sessionID, time.Second, retryTimers)
			useCase.activeTimers[timerKey] = retryTimers
		}
		useCase.mu.Unlock()
		return
	}
	useCase.deliveries.Remove(sessionID)
	if useCase.ownershipCoordinator == nil || useCase.durableDeliveryCommitter != nil {
		useCase.routes.RemoveSession(sessionID)
	}
	shouldPublish := will != nil
	if shouldPublish && useCase.authenticator != nil {
		principal, ok := useCase.principals[sessionID]
		if !ok {
			shouldPublish = false
		} else {
			if !useCase.authorizeWillPublication(principal, will.Topic) {
				shouldPublish = false
			}
		}
	}
	delete(useCase.principals, sessionID)
	delete(useCase.sessionKeys, sessionID)
	delete(useCase.ownershipEpochs, sessionID)
	useCase.mu.Unlock()

	if shouldPublish {
		useCase.recordWillOutcome(delayedWillReason(origin))
		_ = useCase.willPublisher.PublishWill(sessionID, sessionKey, *will)
	}
}

// PendingWillCount returns the number of active delayed Will timers.
func (useCase *UseCase) PendingWillCount() int {
	if useCase == nil {
		return 0
	}
	useCase.mu.Lock()
	defer useCase.mu.Unlock()
	count := 0
	for _, timers := range useCase.activeTimers {
		if timers != nil && timers.willTimer != nil {
			count++
		}
	}
	return count
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

func quotaExceededReason(version mqtt.ProtocolVersion) byte {
	if version == mqtt.ProtocolV5 {
		return 0x97
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
