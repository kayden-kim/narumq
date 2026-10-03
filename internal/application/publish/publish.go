// Package publish routes validated PUBLISH values to an application-owned
// bounded delivery queue without choosing a concrete adapter or connection
// policy.
package publish

import (
	"context"
	"errors"
	"time"

	"narumq/internal/application/membership"
	applicationclock "narumq/internal/application/ports/clock"
	"narumq/internal/application/ports/deliveryqueue"
	"narumq/internal/application/ports/deliverystate"
	portsmetrics "narumq/internal/application/ports/metrics"
	"narumq/internal/application/ports/retained"
	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/routing"
	"narumq/internal/domain/security"
	"narumq/internal/domain/tenancy"
)

var (
	// ErrNilClock means that publication expiry cannot be evaluated.
	ErrNilClock = errors.New("publish clock is nil")
	// ErrNilRoutingTable means that publish has no route policy.
	ErrNilRoutingTable = errors.New("publish routing table is nil")
	// ErrNilDeliveryQueue means that publish has no delivery port.
	ErrNilDeliveryQueue = errors.New("publish delivery queue is nil")
	// ErrNilRetainedRepository means that publish has no retained repository.
	ErrNilRetainedRepository = errors.New("publish retained repository is nil")
	// ErrDurableDeliveryCapacity means a bounded replicated QoS delivery transition cannot fit.
	ErrDurableDeliveryCapacity = errors.New("durable delivery capacity exceeded")
	ErrDurableDeliveryRejected = errors.New("durable delivery change rejected")
	errInvalidDurableSession   = errors.New("durable delivery requires a tenant-scoped session")
	errInvalidDurableTimestamp = errors.New("durable delivery requires a positive Unix timestamp")
)

// ResultDTO reports each session-level delivery outcome in routing order.
type ResultDTO struct {
	Delivered   []routing.SessionID
	Saturated   []routing.SessionID
	Unavailable []routing.SessionID
}

// Option configures optional parameters for the publish use case.
type Option func(*UseCase)

// WithPolicy configures an authorization policy for publication.
func WithPolicy(policy security.Policy) Option {
	return func(u *UseCase) {
		u.policy = policy
	}
}

// WithMetrics configures a metrics recorder for publication latency.
func WithMetrics(metrics portsmetrics.MetricsRecorder) Option {
	return func(u *UseCase) {
		u.metrics = metrics
	}
}

// RetainedCommitter commits a retained mutation to replicated cluster state
// before routing. When it returns nil the committed value is already visible
// in the node's retained projection.
type RetainedCommitter interface {
	CommitRetained(ctx context.Context, change RetainedChangeDTO) error
}

// RetainedChangeDTO is one retained replace or delete requested by PUBLISH.
// Message.TenantID, PublisherSessionID and Topic are always set; Payload, QoS
// and ExpiresAt are zero for Delete.
type RetainedChangeDTO struct {
	Message    retained.MessageDTO
	Delete     bool
	ProposedAt time.Time
}

// WithRetainedCommitter configures the committed-state barrier for cluster retained changes.
func WithRetainedCommitter(committer RetainedCommitter) Option {
	return func(useCase *UseCase) {
		useCase.retainedCommitter = committer
	}
}

// OfflineCommitter durably queues a delivery whose target session is detached in committed cluster state.
// handled=false means the target is not detached and the caller uses the local queue.
type OfflineCommitter interface {
	CommitOffline(ctx context.Context, delivery deliveryqueue.DeliveryDTO, proposedAt time.Time) (handled bool, err error)
}

// WithOfflineCommitter configures the committer for detached persistent sessions in cluster mode.
func WithOfflineCommitter(committer OfflineCommitter) Option {
	return func(useCase *UseCase) {
		useCase.offlineCommitter = committer
	}
}

// RemoteForwarder hands one matched delivery to the cluster node that owns the
// subscriber session. handled=false keeps the delivery on the local queue path.
type RemoteForwarder interface {
	ForwardRemote(ctx context.Context, delivery deliveryqueue.DeliveryDTO) (handled bool, err error)
}

// WithRemoteForwarder configures cross-node live forwarding in cluster mode.
func WithRemoteForwarder(forwarder RemoteForwarder) Option {
	return func(useCase *UseCase) {
		useCase.remoteForwarder = forwarder
	}
}

// WithDurableDeliveryCommitter configures the replicated QoS 1/2 delivery commit and quorum barrier.
func WithDurableDeliveryCommitter(committer deliverystate.PublishCommitter) Option {
	return func(useCase *UseCase) {
		useCase.durableDeliveryCommitter = committer
	}
}

// UseCase commits retained state before routing, then uses the replicated
// durable queue for cluster QoS 1/2 and the bounded local queue for QoS 0.
type UseCase struct {
	clock                    applicationclock.Clock
	routes                   *routing.Table
	queue                    deliveryqueue.Queue
	retained                 retained.Repository
	retainedCommitter        RetainedCommitter
	durableDeliveryCommitter deliverystate.PublishCommitter
	policy                   security.Policy
	offlineCommitter         OfflineCommitter
	remoteForwarder          RemoteForwarder
	metrics                  portsmetrics.MetricsRecorder
}

// New creates a publish use case over the supplied route, queue, and retained
// repository ports.
func New(clock applicationclock.Clock, routes *routing.Table, queue deliveryqueue.Queue, retainedRepository retained.Repository, opts ...Option) (*UseCase, error) {
	if clock == nil {
		return nil, ErrNilClock
	}
	if routes == nil {
		return nil, ErrNilRoutingTable
	}
	if queue == nil {
		return nil, ErrNilDeliveryQueue
	}
	if retainedRepository == nil {
		return nil, ErrNilRetainedRepository
	}
	useCase := &UseCase{clock: clock, routes: routes, queue: queue, retained: retainedRepository}
	for _, opt := range opts {
		if opt != nil {
			opt(useCase)
		}
	}
	return useCase, nil
}

// DurableEnabled reports whether QoS 1/2 delivery transitions use the replicated commit barrier.
func (useCase *UseCase) DurableEnabled() bool {
	return useCase != nil && useCase.durableDeliveryCommitter != nil
}

// Publish validates and routes one PUBLISH. Cluster QoS 1/2 changes commit
// before success can be acknowledged; QoS 0 retains the existing delivery path.
func (useCase *UseCase) Publish(publisherSessionID routing.SessionID, packet mqtt.Publish, receivedAt time.Time, principal ...security.Principal) (ResultDTO, error) {
	return useCase.publish(publisherSessionID, packet, receivedAt, nil, nil, principal...)
}

// PublishWill routes a server-generated Will using the authenticated session identity.
func (useCase *UseCase) PublishWill(
	publisherSessionID routing.SessionID,
	publisherKey tenancy.SessionKey,
	packet mqtt.Publish,
	receivedAt time.Time,
) (ResultDTO, error) {
	if publisherKey.RoutingID() == "" {
		return useCase.Publish(publisherSessionID, packet, receivedAt)
	}
	if publisherKey.RoutingID() != string(publisherSessionID) {
		return ResultDTO{}, errInvalidDurableSession
	}
	return useCase.publish(publisherSessionID, packet, receivedAt, nil, &publisherKey)
}

// ReceiveQoS2 persists the publisher-side QoS 2 PUBLISH before the caller sends
// PUBREC. The committed inbound record is resumed by the next owner epoch.
func (useCase *UseCase) ReceiveQoS2(
	publisherSessionID routing.SessionID,
	ownerNodeID, epoch uint64,
	packet mqtt.Publish,
	receivedAt time.Time,
	principal ...security.Principal,
) error {
	pending, err := useCase.BeginReceiveQoS2(publisherSessionID, ownerNodeID, epoch, packet, receivedAt, principal...)
	if err != nil {
		return err
	}
	return pending.Wait()
}

// BeginReceiveQoS2 submits in caller order without acknowledging durability.
// The caller must wait for completion before sending PUBREC.
func (useCase *UseCase) BeginReceiveQoS2(
	publisherSessionID routing.SessionID,
	ownerNodeID, epoch uint64,
	packet mqtt.Publish,
	receivedAt time.Time,
	principal ...security.Principal,
) (PendingReceive, error) {
	if useCase == nil {
		return PendingReceive{}, ErrNilClock
	}
	if packet.QoS != mqtt.QoSExactlyOnce || packet.PacketID == 0 {
		return PendingReceive{}, routing.ErrInvalidQoS
	}
	if err := routing.ValidateSessionID(publisherSessionID); err != nil {
		return PendingReceive{}, err
	}
	if err := routing.ValidateTopicName(packet.Topic); err != nil {
		return PendingReceive{}, err
	}
	if len(principal) > 0 && principal[0].Subject() != "" {
		topic, err := tenancy.NewTopic(principal[0].TenantID(), packet.Topic)
		if err != nil {
			return PendingReceive{}, security.ErrNotAuthorized
		}
		if err := useCase.policy.AuthorizeTopic(principal[0], security.ActionPublish, topic); err != nil {
			return PendingReceive{}, security.ErrNotAuthorized
		}
	}
	if useCase.durableDeliveryCommitter == nil {
		return PendingReceive{}, ErrDurableDeliveryRejected
	}
	if ownerNodeID == 0 || epoch == 0 {
		return PendingReceive{}, ErrDurableDeliveryRejected
	}
	publisherKey, err := useCase.durablePublisherKey(publisherSessionID, nil)
	if err != nil {
		return PendingReceive{}, err
	}
	now := useCase.clock.Now()
	proposedMillis := now.UnixMilli()
	if proposedMillis <= 0 {
		return PendingReceive{}, errInvalidDurableTimestamp
	}
	var expiresAt time.Time
	if packet.HasMessageExpiryInterval {
		expiresAt = receivedAt.Add(time.Duration(packet.MessageExpiryInterval) * time.Second)
	}
	expiresMillis := uint64(0)
	if !expiresAt.IsZero() {
		millis := expiresAt.UnixMilli()
		if millis <= 0 {
			return PendingReceive{}, errInvalidDurableTimestamp
		}
		expiresMillis = uint64(millis)
	}
	pending, err := useCase.durableDeliveryCommitter.BeginDelivery(context.Background(), membership.DeliveryChangeDTO{
		TenantID:             publisherKey.TenantID().String(),
		ClientID:             publisherKey.ClientID(),
		OwnerNodeID:          ownerNodeID,
		Epoch:                epoch,
		Action:               membership.DeliveryActionReceive,
		ProposedAtUnixMillis: uint64(proposedMillis),
		Topic:                packet.Topic,
		Payload:              packet.Payload,
		Retain:               packet.Retain,
		ExpiresAtUnixMillis:  expiresMillis,
		PacketID:             uint32(packet.PacketID),
	})
	if err != nil {
		_, err = durableDeliveryResult(membership.ApplyResultDTO{}, err)
		return PendingReceive{}, err
	}
	return PendingReceive{pending: pending}, nil
}

// PublishQoS2Release commits the publisher's QoS 2 RELEASE transition before
// the caller sends PUBCOMP. packet is the original PUBLISH value.
func (useCase *UseCase) PublishQoS2Release(
	publisherSessionID routing.SessionID,
	ownerNodeID, epoch uint64,
	packet mqtt.Publish,
	receivedAt time.Time,
	principal ...security.Principal,
) (ResultDTO, error) {
	if packet.QoS != mqtt.QoSExactlyOnce {
		return ResultDTO{}, routing.ErrInvalidQoS
	}
	if useCase == nil {
		return ResultDTO{}, ErrNilClock
	}
	if useCase.durableDeliveryCommitter == nil {
		return useCase.Publish(publisherSessionID, packet, receivedAt, principal...)
	}
	if ownerNodeID == 0 || epoch == 0 || packet.PacketID == 0 {
		return ResultDTO{}, ErrDurableDeliveryRejected
	}
	release := &durableRelease{ownerNodeID: ownerNodeID, epoch: epoch}
	return useCase.publish(publisherSessionID, packet, receivedAt, release, nil, principal...)
}

type durableRelease struct {
	ownerNodeID uint64
	epoch       uint64
}

type durableTarget struct {
	clientID  string
	sessionID routing.SessionID
	retain    bool
}

func needsDeliveryCommand(release *durableRelease, targets []durableTarget) bool {
	return release != nil || len(targets) != 0
}

type durableOutcome uint8

const (
	durableOutcomeSkipped durableOutcome = iota
	durableOutcomeEnqueued
	durableOutcomeSaturated
)

func (useCase *UseCase) publish(
	publisherSessionID routing.SessionID,
	packet mqtt.Publish,
	receivedAt time.Time,
	release *durableRelease,
	trustedPublisherKey *tenancy.SessionKey,
	principal ...security.Principal,
) (ResultDTO, error) {
	if useCase != nil && useCase.metrics != nil {
		start := time.Now()
		defer func() {
			useCase.metrics.ObservePublishDuration(time.Since(start))
		}()
	}
	prepared, err := useCase.preparePublication(publisherSessionID, packet, receivedAt, trustedPublisherKey, principal...)
	if err != nil {
		return ResultDTO{}, err
	}
	if prepared.durable {
		change, targets, err := useCase.prepareDurableDelivery(publisherSessionID, trustedPublisherKey, packet, prepared.expiresAt, prepared.now, prepared.matches, release)
		if err != nil {
			return ResultDTO{}, err
		}
		var deliveryResult membership.DeliveryResultDTO
		if needsDeliveryCommand(release, targets) {
			committed, commitErr := useCase.durableDeliveryCommitter.CommitDelivery(context.Background(), change)
			deliveryResult, err = durableDeliveryResult(committed, commitErr)
			if err != nil {
				return ResultDTO{}, err
			}
		} else {
			barrier, err := useCase.durableDeliveryCommitter.BeginQuorumBarrier(context.Background())
			if err != nil {
				return ResultDTO{}, err
			}
			if err := barrier.Wait(); err != nil {
				return ResultDTO{}, err
			}
		}
		return useCase.completePublication(prepared, &deliveryResult, targets, release != nil)
	}
	if prepared.expired {
		return ResultDTO{}, nil
	}
	return useCase.completePublication(prepared, nil, nil, false)
}

type preparedPublication struct {
	packet           mqtt.Publish
	now, expiresAt   time.Time
	expired, durable bool
	matches          []routing.Match
}

func (useCase *UseCase) preparePublication(
	publisherSessionID routing.SessionID,
	packet mqtt.Publish,
	receivedAt time.Time,
	trustedPublisherKey *tenancy.SessionKey,
	principal ...security.Principal,
) (preparedPublication, error) {
	if err := routing.ValidateSessionID(publisherSessionID); err != nil {
		return preparedPublication{}, err
	}
	if err := routing.ValidateTopicName(packet.Topic); err != nil {
		return preparedPublication{}, err
	}
	if packet.QoS > mqtt.QoSExactlyOnce {
		return preparedPublication{}, routing.ErrInvalidQoS
	}
	if len(principal) > 0 && principal[0].Subject() != "" {
		topic, err := tenancy.NewTopic(principal[0].TenantID(), packet.Topic)
		if err != nil {
			return preparedPublication{}, security.ErrNotAuthorized
		}
		if err := useCase.policy.AuthorizeTopic(principal[0], security.ActionPublish, topic); err != nil {
			return preparedPublication{}, security.ErrNotAuthorized
		}
	}
	var expiresAt time.Time
	if packet.HasMessageExpiryInterval {
		expiresAt = receivedAt.Add(time.Duration(packet.MessageExpiryInterval) * time.Second)
	}
	now := useCase.clock.Now()
	expired := !expiresAt.IsZero() && !now.Before(expiresAt)
	durable := useCase.durableDeliveryCommitter != nil && packet.QoS > mqtt.QoSAtMostOnce
	if packet.Retain {
		tenantID := useCase.routes.SessionTenant(publisherSessionID)
		if trustedPublisherKey != nil {
			tenantID = trustedPublisherKey.TenantID()
		}
		deleteValue := len(packet.Payload) == 0 || expired
		message := retained.MessageDTO{
			TenantID: tenantID, PublisherSessionID: publisherSessionID, Topic: packet.Topic,
		}
		if !deleteValue {
			message.Payload = packet.Payload
			message.QoS = packet.QoS
			message.ExpiresAt = expiresAt
		}
		var err error
		if useCase.retainedCommitter != nil {
			err = useCase.retainedCommitter.CommitRetained(context.Background(), RetainedChangeDTO{
				Message: message, Delete: deleteValue, ProposedAt: now,
			})
		} else if deleteValue {
			err = useCase.retained.Delete(tenantID, packet.Topic)
		} else {
			err = useCase.retained.Replace(message, now)
		}
		if err != nil {
			return preparedPublication{}, err
		}
	}

	var matches []routing.Match
	if !expired {
		var err error
		if trustedPublisherKey != nil {
			matches, err = useCase.routes.MatchForTenant(publisherSessionID, trustedPublisherKey.TenantID(), packet.Topic, packet.QoS, packet.Retain)
		} else {
			matches, err = useCase.routes.Match(publisherSessionID, packet.Topic, packet.QoS, packet.Retain)
		}
		if err != nil {
			return preparedPublication{}, err
		}
	}
	return preparedPublication{
		packet: packet, now: now, expiresAt: expiresAt,
		expired: expired, durable: durable, matches: matches,
	}, nil
}

func (useCase *UseCase) completePublication(
	prepared preparedPublication,
	deliveryResult *membership.DeliveryResultDTO,
	targets []durableTarget,
	release bool,
) (ResultDTO, error) {
	packet := prepared.packet
	now, expiresAt, matches := prepared.now, prepared.expiresAt, prepared.matches
	durable, expired := prepared.durable, prepared.expired
	result := ResultDTO{
		Delivered:   make([]routing.SessionID, 0, len(matches)),
		Saturated:   make([]routing.SessionID, 0),
		Unavailable: make([]routing.SessionID, 0),
	}
	var durableOutcomes map[routing.SessionID]durableOutcome
	if durable {
		durableOutcomes = make(map[routing.SessionID]durableOutcome, len(matches))
	}
	if durable {
		targetSessionByClient := make(map[string]routing.SessionID, len(targets))
		for _, target := range targets {
			durableOutcomes[target.sessionID] = durableOutcomeSkipped
			targetSessionByClient[target.clientID] = target.sessionID
		}
		for _, item := range deliveryResult.Enqueued {
			if sessionID, ok := targetSessionByClient[item.ClientID]; ok {
				durableOutcomes[sessionID] = durableOutcomeEnqueued
			}
		}
		for _, clientID := range deliveryResult.Saturated {
			if sessionID, ok := targetSessionByClient[clientID]; ok {
				durableOutcomes[sessionID] = durableOutcomeSaturated
			}
		}
		if release && !deliveryResult.Found {
			return result, nil
		}
	}
	if expired {
		return result, nil
	}

	var recordedDurable map[routing.SessionID]struct{}
	if durable {
		recordedDurable = make(map[routing.SessionID]struct{}, len(durableOutcomes))
	}
	for _, match := range matches {
		if durable && match.Primary.QoS > mqtt.QoSAtMostOnce {
			if _, recorded := recordedDurable[match.Primary.SessionID]; !recorded {
				switch durableOutcomes[match.Primary.SessionID] {
				case durableOutcomeEnqueued:
					result.Delivered = append(result.Delivered, match.Primary.SessionID)
				case durableOutcomeSaturated:
					result.Saturated = append(result.Saturated, match.Primary.SessionID)
				}
				recordedDurable[match.Primary.SessionID] = struct{}{}
			}
			continue
		}

	candidateLoop:
		for candidateIndex := -1; candidateIndex < len(match.Fallbacks); candidateIndex++ {
			candidate := match.Primary
			if candidateIndex >= 0 {
				candidate = match.Fallbacks[candidateIndex]
			}
			if durable && candidate.QoS > mqtt.QoSAtMostOnce {
				continue
			}
			delivery := deliveryqueue.DeliveryDTO{
				SessionID: candidate.SessionID,
				Topic:     packet.Topic,
				Payload:   packet.Payload,
				QoS:       candidate.QoS,
				Retain:    candidate.Retain,
				ExpiresAt: expiresAt,
			}
			var err error
			handled := false
			if useCase.offlineCommitter != nil {
				handled, err = useCase.offlineCommitter.CommitOffline(context.Background(), delivery, now)
			}
			if !handled && useCase.remoteForwarder != nil {
				handled, err = useCase.remoteForwarder.ForwardRemote(context.Background(), delivery)
			}
			if !handled {
				err = useCase.queue.TryEnqueue(delivery, now)
			}
			switch {
			case err == nil:
				result.Delivered = append(result.Delivered, candidate.SessionID)
				break candidateLoop
			case errors.Is(err, deliveryqueue.ErrFull):
				result.Saturated = append(result.Saturated, candidate.SessionID)
			case errors.Is(err, deliveryqueue.ErrUnavailable):
				result.Unavailable = append(result.Unavailable, candidate.SessionID)
			default:
				return result, err
			}
		}
	}
	return result, nil
}

func (useCase *UseCase) prepareDurableDelivery(
	publisherSessionID routing.SessionID,
	trustedPublisherKey *tenancy.SessionKey,
	packet mqtt.Publish,
	expiresAt, proposedAt time.Time,
	matches []routing.Match,
	release *durableRelease,
) (membership.DeliveryChangeDTO, []durableTarget, error) {
	publisherKey, err := useCase.durablePublisherKey(publisherSessionID, trustedPublisherKey)
	if err != nil {
		return membership.DeliveryChangeDTO{}, nil, err
	}
	proposedMillis := proposedAt.UnixMilli()
	if proposedMillis <= 0 {
		return membership.DeliveryChangeDTO{}, nil, errInvalidDurableTimestamp
	}
	expiresMillis := uint64(0)
	if !expiresAt.IsZero() {
		millis := expiresAt.UnixMilli()
		if millis <= 0 {
			return membership.DeliveryChangeDTO{}, nil, errInvalidDurableTimestamp
		}
		expiresMillis = uint64(millis)
	}

	targets := make([]durableTarget, 0, len(matches))
	targetsByClient := make(map[string]int, len(matches))
	for _, match := range matches {
		candidate := match.Primary
		if candidate.QoS <= mqtt.QoSAtMostOnce {
			continue
		}
		key, err := tenancy.ParseSessionKeyRoutingID(string(candidate.SessionID))
		if err != nil || key.TenantID() != publisherKey.TenantID() {
			return membership.DeliveryChangeDTO{}, nil, errInvalidDurableSession
		}
		clientID := key.ClientID()
		if index, exists := targetsByClient[clientID]; exists {
			targets[index].retain = targets[index].retain || candidate.Retain
			continue
		}
		targetsByClient[clientID] = len(targets)
		targets = append(targets, durableTarget{
			clientID: clientID, sessionID: candidate.SessionID, retain: candidate.Retain,
		})
	}

	deliveryTargets := make([]membership.DeliveryTargetDTO, len(targets))
	for i, target := range targets {
		deliveryTargets[i] = membership.DeliveryTargetDTO{ClientID: target.clientID, Retain: target.retain}
	}
	change := membership.DeliveryChangeDTO{
		TenantID:             publisherKey.TenantID().String(),
		ClientID:             publisherKey.ClientID(),
		Action:               membership.DeliveryActionPublish,
		ProposedAtUnixMillis: uint64(proposedMillis),
		Topic:                packet.Topic,
		Payload:              packet.Payload,
		Retain:               packet.Retain,
		ExpiresAtUnixMillis:  expiresMillis,
		PublishQoS:           uint32(packet.QoS),
		Targets:              deliveryTargets,
	}
	if release != nil {
		change = membership.DeliveryChangeDTO{
			TenantID:             publisherKey.TenantID().String(),
			ClientID:             publisherKey.ClientID(),
			OwnerNodeID:          release.ownerNodeID,
			Epoch:                release.epoch,
			Action:               membership.DeliveryActionRelease,
			ProposedAtUnixMillis: uint64(proposedMillis),
			PacketID:             uint32(packet.PacketID),
			PublishQoS:           uint32(mqtt.QoSExactlyOnce),
			Targets:              deliveryTargets,
		}
	}
	return change, targets, nil
}
func (useCase *UseCase) durablePublisherKey(publisherSessionID routing.SessionID, trustedPublisherKey *tenancy.SessionKey) (tenancy.SessionKey, error) {
	if trustedPublisherKey != nil {
		if trustedPublisherKey.RoutingID() != string(publisherSessionID) {
			return tenancy.SessionKey{}, errInvalidDurableSession
		}
		if boundTenant := useCase.routes.SessionTenant(publisherSessionID); boundTenant != (tenancy.TenantID{}) && boundTenant != trustedPublisherKey.TenantID() {
			return tenancy.SessionKey{}, errInvalidDurableSession
		}
		return *trustedPublisherKey, nil
	}
	key, err := tenancy.ParseSessionKeyRoutingID(string(publisherSessionID))
	if err != nil || useCase.routes.SessionTenant(publisherSessionID) != key.TenantID() {
		return tenancy.SessionKey{}, errInvalidDurableSession
	}
	return key, nil
}

// AcceptForwarded enqueues one QoS 0 delivery that another cluster node matched for a
// session connected to this node. Expired deliveries are dropped without error.
func (useCase *UseCase) AcceptForwarded(delivery deliveryqueue.DeliveryDTO, now time.Time) error {
	if err := routing.ValidateSessionID(delivery.SessionID); err != nil {
		return err
	}
	if err := routing.ValidateTopicName(delivery.Topic); err != nil {
		return err
	}
	if delivery.QoS != mqtt.QoSAtMostOnce {
		return routing.ErrInvalidQoS
	}
	if delivery.Expired(now) {
		return nil
	}
	return useCase.queue.TryEnqueue(delivery, now)
}
