// Package publish routes validated PUBLISH values to an application-owned
// bounded delivery queue without choosing a concrete adapter or connection
// policy.
package publish

import (
	"errors"
	"time"

	applicationclock "narumq/internal/application/ports/clock"
	"narumq/internal/application/ports/deliveryqueue"
	"narumq/internal/application/ports/retained"
	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/routing"
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

// UseCase stores retained state before routing one publication to all matching
// sessions through the bounded delivery queue.
type UseCase struct {
	clock    applicationclock.Clock
	routes   *routing.Table
	queue    deliveryqueue.Queue
	retained retained.Repository
}

// New creates a publish use case over the supplied route, queue, and retained
// repository ports.
func New(clock applicationclock.Clock, routes *routing.Table, queue deliveryqueue.Queue, retainedRepository retained.Repository) (*UseCase, error) {
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
	return &UseCase{clock: clock, routes: routes, queue: queue, retained: retainedRepository}, nil
}

// Publish validates the publisher and publication, commits retained mutation
// before route matching, and continues after per-session saturation or
// unavailability. Unknown queue errors stop the fan-out and return partial
// results so successful earlier deliveries are not hidden.
func (useCase *UseCase) Publish(publisherSessionID routing.SessionID, packet mqtt.Publish, receivedAt time.Time) (ResultDTO, error) {
	if err := routing.ValidateSessionID(publisherSessionID); err != nil {
		return ResultDTO{}, err
	}
	if err := routing.ValidateTopicName(packet.Topic); err != nil {
		return ResultDTO{}, err
	}
	if packet.QoS > mqtt.QoSExactlyOnce {
		return ResultDTO{}, routing.ErrInvalidQoS
	}
	var expiresAt time.Time
	if packet.HasMessageExpiryInterval {
		expiresAt = receivedAt.Add(time.Duration(packet.MessageExpiryInterval) * time.Second)
	}
	now := useCase.clock.Now()
	expired := !expiresAt.IsZero() && !now.Before(expiresAt)
	if packet.Retain {
		var err error
		if len(packet.Payload) == 0 || expired {
			err = useCase.retained.Delete(packet.Topic)
		} else {
			err = useCase.retained.Replace(retained.MessageDTO{
				PublisherSessionID: publisherSessionID,
				Topic:              packet.Topic,
				Payload:            packet.Payload,
				QoS:                packet.QoS,
				ExpiresAt:          expiresAt,
			}, now)
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
		delivery := deliveryqueue.DeliveryDTO{
			SessionID: match.SessionID,
			Topic:     packet.Topic,
			Payload:   packet.Payload,
			QoS:       match.QoS,
			Retain:    match.Retain,
			ExpiresAt: expiresAt,
		}
		err := useCase.queue.TryEnqueue(delivery, now)
		switch {
		case err == nil:
			result.Delivered = append(result.Delivered, match.SessionID)
		case errors.Is(err, deliveryqueue.ErrFull):
			result.Saturated = append(result.Saturated, match.SessionID)
		case errors.Is(err, deliveryqueue.ErrUnavailable):
			result.Unavailable = append(result.Unavailable, match.SessionID)
		default:
			return result, err
		}
	}
	return result, nil
}
