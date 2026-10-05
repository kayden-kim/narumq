// Package subscribe orchestrates ordered MQTT subscription acknowledgements and
// mutates the domain routing table without owning wire or adapter details.
package subscribe

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"narumq/internal/application/ports/deliveryqueue"
	portsmetrics "narumq/internal/application/ports/metrics"
	"narumq/internal/application/ports/retained"
	"narumq/internal/application/tenantlimit"
	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/routing"
	"narumq/internal/domain/security"
	"narumq/internal/domain/tenancy"
)

var (
	// ErrNilRoutingTable means that a use case was constructed without routing.
	ErrNilRoutingTable = errors.New("subscribe routing table is nil")
	// ErrNilDeliveryQueue means that retained replay has no queue port.
	ErrNilDeliveryQueue = errors.New("subscribe delivery queue is nil")
	// ErrNilRetainedRepository means that retained replay has no state port.
	ErrNilRetainedRepository = errors.New("subscribe retained repository is nil")
)

// ResultDTO reports the ordered SUBACK and the number of retained deliveries
// committed to the queue for this SUBSCRIBE request.
type ResultDTO struct {
	Ack            mqtt.SubAck
	RetainedQueued int
}

// Option configures optional parameters for the subscribe use case.
type Option func(*UseCase)

// WithPolicy configures an authorization policy for subscriptions.
func WithPolicy(policy security.Policy) Option {
	return func(u *UseCase) {
		u.policy = policy
	}
}

// WithMetrics configures a recorder for active subscription gauges.
func WithMetrics(metrics portsmetrics.MetricsRecorder) Option {
	return func(u *UseCase) {
		u.metrics = metrics
	}
}

// WithTenantLimits configures tenant subscription admission before commit.
func WithTenantLimits(limits *tenantlimit.Service) Option {
	return func(useCase *UseCase) {
		useCase.tenantLimits = limits
	}
}

// SessionOwnership identifies the fenced owner allowed to mutate cluster session state.
type SessionOwnership struct {
	TenantID    string
	ClientID    string
	OwnerNodeID uint64
	Epoch       uint64
}

// SubscriptionCommitter commits a fenced subscription mutation before local routing changes.
type SubscriptionCommitter interface {
	CommitSubscription(
		context.Context, string, string, uint64, uint64, string, uint32, bool, bool, uint32, uint32,
	) error
}

const (
	subscriptionActionAdd    uint32 = 1
	subscriptionActionRemove uint32 = 2
)

// WithCommitter configures the committed-state barrier for cluster subscription changes.
func WithCommitter(committer SubscriptionCommitter) Option {
	return func(useCase *UseCase) {
		useCase.committer = committer
	}
}

// UseCase applies subscription commands to one routing table and replays
// retained values through the application-owned delivery queue.
type UseCase struct {
	routes       *routing.Table
	queue        deliveryqueue.Queue
	retained     retained.Repository
	policy       security.Policy
	metrics      portsmetrics.MetricsRecorder
	committer    SubscriptionCommitter
	tenantLimits *tenantlimit.Service
	// tenantSubscriptionMu keeps local quota checks and route mutations ordered.
	// Remote committed projections remain outside this admission lock.
	tenantSubscriptionMu sync.Mutex
}

// New creates a subscription use case over routes, queue, and retained state.
func New(routes *routing.Table, queue deliveryqueue.Queue, retainedRepository retained.Repository, opts ...Option) (*UseCase, error) {
	if routes == nil {
		return nil, ErrNilRoutingTable
	}
	if queue == nil {
		return nil, ErrNilDeliveryQueue
	}
	if retainedRepository == nil {
		return nil, ErrNilRetainedRepository
	}
	useCase := &UseCase{routes: routes, queue: queue, retained: retainedRepository}
	for _, opt := range opts {
		if opt != nil {
			opt(useCase)
		}
	}
	return useCase, nil
}

// ClusterCommitterConfigured reports whether persistent mutations require a committed-state barrier.
func (useCase *UseCase) ClusterCommitterConfigured() bool {
	return useCase != nil && useCase.committer != nil
}

// Subscribe validates and applies every entry independently.
func (useCase *UseCase) Subscribe(sessionID routing.SessionID, packet mqtt.Subscribe, now time.Time, principal ...security.Principal) (ResultDTO, error) {
	return useCase.SubscribeCommitted(context.Background(), sessionID, packet, now, SessionOwnership{}, principal...)
}

// SubscribeCommitted orders each fenced state mutation before replay and route changes.
func (useCase *UseCase) SubscribeCommitted(
	ctx context.Context,
	sessionID routing.SessionID,
	packet mqtt.Subscribe,
	now time.Time,
	ownership SessionOwnership,
	principal ...security.Principal,
) (ResultDTO, error) {
	if len(principal) > 0 && principal[0].Subject() != "" && useCase.tenantLimits.Limits(principal[0].TenantID()).MaxSubscriptions > 0 {
		useCase.tenantSubscriptionMu.Lock()
		defer useCase.tenantSubscriptionMu.Unlock()
	}
	result := ResultDTO{
		Ack: mqtt.SubAck{Version: packet.Version, PacketID: packet.PacketID, ReasonCodes: make([]byte, 0, len(packet.Subscriptions))},
	}
	for _, subscription := range packet.Subscriptions {
		reasonCode, retainedQueued, err := useCase.subscribeOne(ctx, sessionID, packet.Version, subscription, now, ownership, principal...)
		if err != nil {
			return result, err
		}
		result.Ack.ReasonCodes = append(result.Ack.ReasonCodes, reasonCode)
		result.RetainedQueued += retainedQueued
	}
	return result, nil
}

// Unsubscribe removes filters only after their fenced mutations commit.
func (useCase *UseCase) UnsubscribeCommitted(
	ctx context.Context,
	sessionID routing.SessionID,
	packet mqtt.Unsubscribe,
	ownership SessionOwnership,
	principal ...security.Principal,
) (mqtt.UnsubAck, error) {
	ack := mqtt.UnsubAck{Version: packet.Version, PacketID: packet.PacketID}
	var activePrincipal security.Principal
	if len(principal) > 0 {
		activePrincipal = principal[0]
	}
	if activePrincipal.Subject() != "" && useCase.tenantLimits.Limits(activePrincipal.TenantID()).MaxSubscriptions > 0 {
		useCase.tenantSubscriptionMu.Lock()
		defer useCase.tenantSubscriptionMu.Unlock()
	}
	if packet.Version != mqtt.ProtocolV311 {
		ack.ReasonCodes = make([]byte, 0, len(packet.Filters))
	}
	for _, filter := range packet.Filters {
		if !validUnsubscribeFilter(filter) {
			if packet.Version != mqtt.ProtocolV311 {
				ack.ReasonCodes = append(ack.ReasonCodes, 0x8F)
			}
			continue
		}
		if activePrincipal.Subject() != "" {
			topicFilter, err := tenancy.NewTopicFilter(activePrincipal.TenantID(), filter)
			if err != nil || useCase.policy.AuthorizeFilter(activePrincipal, security.ActionUnsubscribe, topicFilter) != nil {
				if packet.Version != mqtt.ProtocolV311 {
					ack.ReasonCodes = append(ack.ReasonCodes, 0x87)
				}
				continue
			}
		}
		if err := useCase.commitSubscription(ctx, ownership, filter, 0, false, false, 0, subscriptionActionRemove); err != nil {
			return ack, err
		}
		if packet.Version != mqtt.ProtocolV311 {
			ack.ReasonCodes = append(ack.ReasonCodes, useCase.unsubscribeReason(sessionID, filter))
		} else {
			_, _ = useCase.routes.Unsubscribe(sessionID, filter)
		}
	}
	useCase.recordSubscriptionGauge()
	return ack, nil
}

// Unsubscribe keeps the standalone API; cluster adapters use UnsubscribeCommitted
// so a failed quorum barrier can close the connection without a success packet.
func (useCase *UseCase) Unsubscribe(sessionID routing.SessionID, packet mqtt.Unsubscribe, principal ...security.Principal) mqtt.UnsubAck {
	ack, err := useCase.UnsubscribeCommitted(context.Background(), sessionID, packet, SessionOwnership{}, principal...)
	if err != nil {
		return mqtt.UnsubAck{Version: packet.Version, PacketID: packet.PacketID}
	}
	return ack
}

func (useCase *UseCase) subscribeOne(
	ctx context.Context,
	sessionID routing.SessionID,
	version mqtt.ProtocolVersion,
	subscription mqtt.Subscription,
	now time.Time,
	ownership SessionOwnership,
	principal ...security.Principal,
) (byte, int, error) {
	if err := routing.ValidateSessionID(sessionID); err != nil {
		return invalidSubscriptionReason(version), 0, nil
	}
	if err := routing.ValidateTopicFilter(subscription.Filter); err != nil {
		return invalidSubscriptionReason(version), 0, nil
	}
	if subscription.QoS > mqtt.QoSExactlyOnce || subscription.RetainHandling > mqtt.DoNotSendRetained {
		return invalidSubscriptionReason(version), 0, nil
	}
	if version == mqtt.ProtocolV311 && (subscription.NoLocal || subscription.RetainAsPublished || subscription.RetainHandling != mqtt.SendRetainedAtSubscribe) {
		return invalidSubscriptionReason(version), 0, nil
	}
	isShared := routing.IsSharedFilter(subscription.Filter)
	if isShared && subscription.NoLocal {
		return protocolErrorReason(version), 0, nil
	}
	if len(principal) > 0 && principal[0].Subject() != "" {
		topicFilter, err := tenancy.NewTopicFilter(principal[0].TenantID(), subscription.Filter)
		if err != nil || useCase.policy.AuthorizeFilter(principal[0], security.ActionSubscribe, topicFilter) != nil {
			return notAuthorizedSubscriptionReason(version), 0, nil
		}
	}
	// Capture the prior state before a cluster commit can project this route locally.
	_, existed := useCase.routes.Lookup(sessionID, subscription.Filter)
	if !existed && useCase.tenantLimits != nil && len(principal) > 0 && principal[0].Subject() != "" {
		tenantID := principal[0].TenantID()
		max := useCase.tenantLimits.Limits(tenantID).MaxSubscriptions
		if max > 0 && uint64(useCase.routes.TenantSubscriptionCount(tenantID)) >= max {
			useCase.tenantLimits.Reject(tenantID, tenantlimit.DimensionSubscriptions)
			return quotaExceededSubscriptionReason(version), 0, nil
		}
	}
	if err := useCase.commitSubscription(
		ctx, ownership, subscription.Filter, uint32(subscription.QoS),
		subscription.NoLocal, subscription.RetainAsPublished, uint32(subscription.RetainHandling),
		subscriptionActionAdd,
	); err != nil {
		return 0, 0, err
	}
	replay := version == mqtt.ProtocolV311
	switch subscription.RetainHandling {
	case mqtt.SendRetainedAtSubscribe:
		replay = true
	case mqtt.SendRetainedIfNew:
		replay = !existed
	case mqtt.DoNotSendRetained:
		replay = false
	}
	if isShared {
		replay = false
	}
	retainedQueued := 0
	if replay {
		batch, err := useCase.retainedBatch(sessionID, subscription, now)
		if err != nil {
			return 0, 0, err
		}
		if err := useCase.queue.TryEnqueueBatch(batch, now); err != nil {
			if errors.Is(err, deliveryqueue.ErrFull) {
				return replayFullReason(version), 0, nil
			}
			return 0, 0, err
		}
		retainedQueued = len(batch)
	}
	if _, err := useCase.routes.Subscribe(sessionID, subscription); err != nil {
		return invalidSubscriptionReason(version), 0, nil
	}
	useCase.recordSubscriptionGauge()
	return byte(subscription.QoS), retainedQueued, nil
}

func (useCase *UseCase) commitSubscription(
	ctx context.Context,
	ownership SessionOwnership,
	filter string,
	qos uint32,
	noLocal, retainAsPublished bool,
	retainHandling uint32,
	action uint32,
) error {
	if useCase.committer == nil {
		return nil
	}
	return useCase.committer.CommitSubscription(
		ctx, ownership.TenantID, ownership.ClientID, ownership.OwnerNodeID, ownership.Epoch,
		filter, qos, noLocal, retainAsPublished, retainHandling, action,
	)
}

func (useCase *UseCase) retainedBatch(sessionID routing.SessionID, subscription mqtt.Subscription, now time.Time) ([]deliveryqueue.DeliveryDTO, error) {
	messages, err := useCase.retained.List(now)
	if err != nil {
		return nil, err
	}
	subscriberTenant := useCase.routes.SessionTenant(sessionID)
	batch := make([]deliveryqueue.DeliveryDTO, 0, len(messages))
	for _, message := range messages {
		if message.TenantID != subscriberTenant {
			continue
		}
		matched, err := routing.TopicMatchesFilter(message.Topic, subscription.Filter)
		if err != nil {
			return nil, err
		}
		if !matched || (subscription.NoLocal && message.PublisherSessionID == sessionID) {
			continue
		}
		qos := message.QoS
		if subscription.QoS < qos {
			qos = subscription.QoS
		}
		batch = append(batch, deliveryqueue.DeliveryDTO{
			SessionID: sessionID,
			Topic:     message.Topic,
			Payload:   message.Payload,
			QoS:       qos,
			Retain:    true,
			ExpiresAt: message.ExpiresAt,
		})
	}
	sort.Slice(batch, func(left, right int) bool {
		return batch[left].Topic < batch[right].Topic
	})
	return batch, nil
}

func (useCase *UseCase) unsubscribeReason(sessionID routing.SessionID, filter string) byte {
	if err := routing.ValidateTopicFilter(filter); err != nil {
		return 0x8F
	}
	removed, err := useCase.routes.Unsubscribe(sessionID, filter)
	if err != nil {
		return 0x8F
	}
	if !removed {
		return 0x11
	}
	return 0x00
}

func validUnsubscribeFilter(filter string) bool {
	return routing.ValidateTopicFilter(filter) == nil
}

func invalidSubscriptionReason(version mqtt.ProtocolVersion) byte {
	if version == mqtt.ProtocolV5 {
		return 0x8F
	}
	return 0x80
}

func replayFullReason(version mqtt.ProtocolVersion) byte {
	if version == mqtt.ProtocolV5 {
		return 0x97
	}
	return 0x80
}

func quotaExceededSubscriptionReason(version mqtt.ProtocolVersion) byte {
	if version == mqtt.ProtocolV5 {
		return 0x97
	}
	return 0x80
}

func protocolErrorReason(version mqtt.ProtocolVersion) byte {
	if version == mqtt.ProtocolV5 {
		return 0x82
	}
	return 0x80
}

func (useCase *UseCase) recordSubscriptionGauge() {
	if useCase == nil || useCase.metrics == nil || useCase.routes == nil {
		return
	}
	useCase.metrics.SetSubscriptionsActive(int64(len(useCase.routes.Snapshot())))
}

func notAuthorizedSubscriptionReason(version mqtt.ProtocolVersion) byte {
	if version == mqtt.ProtocolV5 {
		return 0x87
	}
	return 0x80
}
