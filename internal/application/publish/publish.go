// Package publish routes validated PUBLISH values to an application-owned
// bounded delivery queue without choosing a concrete adapter or connection
// policy.
package publish

import (
	"context"
	"errors"
	"time"

	applicationclock "narumq/internal/application/ports/clock"
	"narumq/internal/application/ports/deliveryqueue"
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
	// ErrNilRetainedRepository means that publish has no retained state port.
	ErrNilRetainedRepository = errors.New("publish retained repository is nil")
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

// UseCase stores retained state before routing one publication to all matching
// sessions through the bounded delivery queue.
type UseCase struct {
	clock             applicationclock.Clock
	routes            *routing.Table
	queue             deliveryqueue.Queue
	retained          retained.Repository
	retainedCommitter RetainedCommitter
	policy            security.Policy
	offlineCommitter  OfflineCommitter
	metrics           portsmetrics.MetricsRecorder
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

// Publish validates the publisher and publication, commits retained mutation
// before route matching, and continues after per-candidate saturation or
// unavailability. Unknown queue errors stop the fan-out and return partial
// results so successful earlier deliveries are not hidden.
func (useCase *UseCase) Publish(publisherSessionID routing.SessionID, packet mqtt.Publish, receivedAt time.Time, principal ...security.Principal) (ResultDTO, error) {
	if useCase != nil && useCase.metrics != nil {
		start := time.Now()
		defer func() {
			useCase.metrics.ObservePublishDuration(time.Since(start))
		}()
	}
	if err := routing.ValidateSessionID(publisherSessionID); err != nil {
		return ResultDTO{}, err
	}
	if err := routing.ValidateTopicName(packet.Topic); err != nil {
		return ResultDTO{}, err
	}
	if packet.QoS > mqtt.QoSExactlyOnce {
		return ResultDTO{}, routing.ErrInvalidQoS
	}
	if len(principal) > 0 && principal[0].Subject() != "" {
		topic, err := tenancy.NewTopic(principal[0].TenantID(), packet.Topic)
		if err != nil {
			return ResultDTO{}, security.ErrNotAuthorized
		}
		if err := useCase.policy.AuthorizeTopic(principal[0], security.ActionPublish, topic); err != nil {
			return ResultDTO{}, security.ErrNotAuthorized
		}
	}
	var expiresAt time.Time
	if packet.HasMessageExpiryInterval {
		expiresAt = receivedAt.Add(time.Duration(packet.MessageExpiryInterval) * time.Second)
	}
	now := useCase.clock.Now()
	expired := !expiresAt.IsZero() && !now.Before(expiresAt)
	if packet.Retain {
		tenantID := useCase.routes.SessionTenant(publisherSessionID)
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
			return ResultDTO{}, err
		}
	}
	if expired {
		return ResultDTO{}, nil
	}

	matches, err := useCase.routes.Match(publisherSessionID, packet.Topic, packet.QoS, packet.Retain)
	if err != nil {
		return ResultDTO{}, err
	}
	result := ResultDTO{
		Delivered:   make([]routing.SessionID, 0, len(matches)),
		Saturated:   make([]routing.SessionID, 0),
		Unavailable: make([]routing.SessionID, 0),
	}
	for _, match := range matches {
	candidateLoop:
		for candidateIndex := -1; candidateIndex < len(match.Fallbacks); candidateIndex++ {
			candidate := match.Primary
			if candidateIndex >= 0 {
				candidate = match.Fallbacks[candidateIndex]
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
