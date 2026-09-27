package main

import (
	"context"
	"errors"
	"time"

	"narumq/internal/adapters/tcp"
	applicationdeliveryqueue "narumq/internal/application/ports/deliveryqueue"
	"narumq/internal/application/publish"
	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/routing"
	"narumq/internal/domain/tenancy"
)

type clusterPeerForwarder interface {
	Forward(tcp.PeerMessageDTO) error
}

type clusterForwardSink interface {
	DeliverForwarded(applicationdeliveryqueue.DeliveryDTO) error
}

var _ publish.RemoteForwarder = (*clusterRuntime)(nil)

func (runtime *clusterRuntime) ForwardRemote(_ context.Context, delivery applicationdeliveryqueue.DeliveryDTO) (bool, error) {
	if delivery.QoS != mqtt.QoSAtMostOnce {
		return false, nil
	}
	if runtime == nil || runtime.machine == nil || runtime.forwarder == nil {
		return true, applicationdeliveryqueue.ErrUnavailable
	}

	sessionKey, err := tenancy.ParseSessionKeyRoutingID(string(delivery.SessionID))
	if err != nil {
		return false, nil
	}
	ownership, found := runtime.machine.GetOwnership(sessionKey.TenantID().String(), sessionKey.ClientID())
	if !found || ownership.OwnerNodeID == runtime.config.NodeID {
		return false, nil
	}

	var expiresAtUnixMillis uint64
	if !delivery.ExpiresAt.IsZero() {
		expiresAtUnixMillis = uint64(delivery.ExpiresAt.UnixMilli())
	}
	payload, err := tcp.EncodeForwardedDelivery(tcp.ForwardedDeliveryDTO{
		SessionID: string(delivery.SessionID), Topic: delivery.Topic, Payload: delivery.Payload,
		Retain: delivery.Retain, ExpiresAtUnixMillis: expiresAtUnixMillis,
	})
	if err != nil {
		return true, applicationdeliveryqueue.ErrUnavailable
	}
	if err := runtime.forwarder.Forward(tcp.PeerMessageDTO{
		From: runtime.config.NodeID, To: ownership.OwnerNodeID, Data: payload,
	}); err != nil {
		if errors.Is(err, tcp.ErrPeerBackpressure) {
			return true, applicationdeliveryqueue.ErrFull
		}
		return true, applicationdeliveryqueue.ErrUnavailable
	}
	return true, nil
}

func (runtime *clusterRuntime) runForwardReceiver(ctx context.Context) error {
	if runtime == nil || runtime.transport == nil {
		return errors.New("cluster delivery forwarding transport is unavailable")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ready := runtime.transport.ForwardReady()
	for {
		select {
		case <-ctx.Done():
			return nil
		case _, ok := <-ready:
			if !ok {
				if ctx.Err() != nil || (runtime.storage != nil && runtime.storage.view().health == clusterReplicationFailed) {
					return nil
				}
				return errors.New("dynamic peer transport forwarding-ready channel closed unexpectedly")
			}
			for {
				inbound, received := runtime.transport.TakeForward()
				if !received {
					break
				}
				runtime.acceptForwardedFrame(inbound)
			}
		}
	}
}

func (runtime *clusterRuntime) acceptForwardedFrame(inbound tcp.PeerInboundDelivery) {
	defer inbound.Complete()
	if runtime == nil || runtime.machine == nil {
		return
	}

	message := inbound.Message
	forwarded, err := tcp.DecodeForwardedDelivery(message.Data)
	if err != nil {
		if runtime.logger != nil {
			runtime.logger.Warn("drop malformed cluster forwarded delivery", "from", message.From, "error", err)
		}
		return
	}
	sessionKey, err := tenancy.ParseSessionKeyRoutingID(forwarded.SessionID)
	if err != nil {
		if runtime.logger != nil {
			runtime.logger.Warn("drop cluster forwarded delivery with invalid session", "from", message.From, "error", err)
		}
		return
	}
	ownership, found := runtime.machine.GetOwnership(sessionKey.TenantID().String(), sessionKey.ClientID())
	if !found || ownership.OwnerNodeID != runtime.config.NodeID {
		return
	}
	sink := runtime.forwardSink
	if sink == nil {
		return
	}

	var expiresAt time.Time
	if forwarded.ExpiresAtUnixMillis != 0 {
		expiresAt = time.UnixMilli(int64(forwarded.ExpiresAtUnixMillis))
	}
	err = sink.DeliverForwarded(applicationdeliveryqueue.DeliveryDTO{
		SessionID: routing.SessionID(forwarded.SessionID), Topic: forwarded.Topic,
		Payload: forwarded.Payload, QoS: mqtt.QoSAtMostOnce, Retain: forwarded.Retain,
		ExpiresAt: expiresAt,
	})
	if err != nil && !errors.Is(err, applicationdeliveryqueue.ErrUnavailable) && runtime.logger != nil {
		runtime.logger.Warn("cluster forwarded delivery was not admitted",
			"from", message.From, "session_id", forwarded.SessionID, "error", err)
	}
}
