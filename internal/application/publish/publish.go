// Package publish routes validated PUBLISH values to an application-owned
// bounded delivery queue without choosing a concrete adapter or connection
// policy.
package publish

import (
	"errors"

	"narumq/internal/application/ports/deliveryqueue"
	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/routing"
)

var (
	// ErrNilRoutingTable means that publish has no route policy.
	ErrNilRoutingTable = errors.New("publish routing table is nil")
	// ErrNilDeliveryQueue means that publish has no delivery port.
	ErrNilDeliveryQueue = errors.New("publish delivery queue is nil")
)

// ResultDTO reports each session-level delivery outcome in routing order.
type ResultDTO struct {
	Delivered   []routing.SessionID
	Saturated   []routing.SessionID
	Unavailable []routing.SessionID
}

// UseCase routes one validated publication to all matching sessions.
type UseCase struct {
	routes *routing.Table
	queue  deliveryqueue.Queue
}

// New creates a publish use case over the supplied route table and queue port.
func New(routes *routing.Table, queue deliveryqueue.Queue) (*UseCase, error) {
	if routes == nil {
		return nil, ErrNilRoutingTable
	}
	if queue == nil {
		return nil, ErrNilDeliveryQueue
	}
	return &UseCase{routes: routes, queue: queue}, nil
}

// Publish matches the topic once and continues after per-session saturation or
// unavailability. Unknown queue errors stop the fan-out and return partial
// results so successful earlier deliveries are not hidden.
func (useCase *UseCase) Publish(packet mqtt.Publish) (ResultDTO, error) {
	matches, err := useCase.routes.Match(packet.Topic, packet.QoS)
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
			Retain:    packet.Retain,
		}
		err := useCase.queue.TryEnqueue(delivery)
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
