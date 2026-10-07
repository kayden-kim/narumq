package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"narumq/internal/domain/mqtt"
	"narumq/tests/benchmark/internal/mqttclient"
)

type runClient struct {
	client          *mqttclient.Client
	id              string
	class           string
	nodeIndex       int
	subscriberIndex int
	streamIndex     int
	isPublisher     bool
	isRoute         bool
	done            chan struct{}
	stop            chan struct{}
	stopOnce        sync.Once
	closeOnce       sync.Once
	failureMu       sync.Mutex
	readFailure     error
	closeErr        error
	routeSubAcks    chan routeSubAck
	routeDeliveries chan routeDelivery
}

type routeSubAck struct {
	packetID    mqtt.PacketIdentifier
	reasonCodes []byte
	at          time.Time
}

type routeDelivery struct {
	topic string
	at    time.Time
}

type connectInfo struct {
	id              string
	class           string
	nodeIndex       int
	subscriberIndex int
	streamIndex     int
	isPublisher     bool
	isRoute         bool
}

func connectClient(ctx context.Context, address string, info connectInfo, profile Profile, creds bp04RunnerCredentials) (*runClient, time.Time, error) {
	client, timing, err := mqttclient.Connect(ctx, mqttclient.ConnectOptions{
		Address:               address,
		ClientID:              info.id,
		ProtocolVersion:       mqtt.ProtocolV5,
		KeepAlive:             profile.KeepAliveSeconds,
		HandshakeTimeout:      5 * time.Second,
		CleanStart:            true,
		SessionExpiryInterval: 0,
		Username:              creds.Username,
		Password:              []byte(creds.Password),
	})
	if err != nil {
		return nil, time.Time{}, err
	}
	return &runClient{
		client: client, id: info.id, class: info.class,
		nodeIndex: info.nodeIndex, subscriberIndex: info.subscriberIndex,
		streamIndex: info.streamIndex, isPublisher: info.isPublisher, isRoute: info.isRoute,
		stop: make(chan struct{}), routeSubAcks: make(chan routeSubAck, 4),
		routeDeliveries: make(chan routeDelivery, 64),
	}, timing.ConnAckAt, nil
}

func subscribeClient(client *runClient, filters []string) (time.Time, time.Time, []byte, error) {
	if client == nil || client.client == nil {
		return time.Time{}, time.Time{}, nil, errors.New("subscriber client is required")
	}
	if len(filters) == 0 {
		return time.Time{}, time.Time{}, nil, errors.New("at least one subscription filter is required")
	}
	subscriptions := make([]mqtt.Subscription, len(filters))
	for index, filter := range filters {
		subscriptions[index] = mqtt.Subscription{Filter: filter, QoS: mqtt.QoSAtMostOnce}
	}
	request := &mqtt.Subscribe{Version: mqtt.ProtocolV5, PacketID: 1, Subscriptions: subscriptions}
	sentAt := time.Now()
	if err := client.client.Write(request, 5*time.Second); err != nil {
		return sentAt, time.Time{}, nil, err
	}
	packet, err := client.client.Read(5 * time.Second)
	if err != nil {
		return sentAt, time.Time{}, nil, fmt.Errorf("read SUBACK: %w", err)
	}
	ack, ok := packet.(*mqtt.SubAck)
	ackedAt := time.Now()
	if !ok {
		return sentAt, ackedAt, nil, fmt.Errorf("expected SUBACK, received %T", packet)
	}
	if ack.PacketID != request.PacketID || len(ack.ReasonCodes) != len(filters) {
		return sentAt, ackedAt, append([]byte(nil), ack.ReasonCodes...), fmt.Errorf("unexpected SUBACK packet id=%d reason codes=%v", ack.PacketID, ack.ReasonCodes)
	}
	for index, reason := range ack.ReasonCodes {
		if reason != 0x00 {
			return sentAt, ackedAt, append([]byte(nil), ack.ReasonCodes...), fmt.Errorf("SUBACK reason code %d for filter %q is %#02x", index, filters[index], reason)
		}
	}
	return sentAt, ackedAt, append([]byte(nil), ack.ReasonCodes...), nil
}

func (client *runClient) startReader(profile Profile, col *collector, cancel context.CancelCauseFunc) {
	client.done = make(chan struct{})
	go func() {
		defer close(client.done)
		for {
			packet, err := client.client.Read(100 * time.Millisecond)
			if err != nil {
				if mqttclient.IsTimeout(err) {
					continue
				}
				select {
				case <-client.stop:
					return
				default:
				}
				client.failRead(col, cancel, fmt.Errorf("client %s read: %w", client.id, err), false)
				return
			}
			switch value := packet.(type) {
			case *mqtt.Publish:
				if client.isPublisher {
					col.recordUnexpectedDelivery(client.class, 0, value.Topic, time.Now())
					continue
				}
				if value.QoS != mqtt.QoSAtMostOnce || value.DUP || value.Retain {
					client.failRead(col, cancel, fmt.Errorf("client %s received PUBLISH with invalid QoS or flags", client.id), true)
					return
				}
				streamIndex := client.streamIndex
				if client.isRoute {
					streamIndex = 4
				}
				sequence, payloadErr := validateStreamPayload(profile, streamIndex, value.Payload)
				if payloadErr != nil {
					client.failRead(col, cancel, fmt.Errorf("client %s received invalid payload: %w", client.id, payloadErr), true)
					return
				}
				receivedAt := time.Now()
				if client.isRoute {
					if !validRouteTopic(value.Topic) {
						client.failRead(col, cancel, fmt.Errorf("route subscriber %s received unexpected topic %q", client.id, value.Topic), true)
						return
					}
					col.recordRouteDelivery(value.Topic, receivedAt)
					select {
					case client.routeDeliveries <- routeDelivery{topic: value.Topic, at: receivedAt}:
					default:
					}
					continue
				}
				if value.Topic != publishTopic(client.class, sequence) {
					client.failRead(col, cancel, fmt.Errorf("client %s received topic %q for sequence %d", client.id, value.Topic, sequence), true)
					return
				}
				col.recordDelivery(client.subscriberIndex, streamIndex, sequence, value.Topic, receivedAt)
			case *mqtt.PingResp:
				continue
			case *mqtt.SubAck:
				if !client.isRoute {
					client.failRead(col, cancel, fmt.Errorf("client %s received unexpected SUBACK", client.id), true)
					return
				}
				select {
				case client.routeSubAcks <- routeSubAck{packetID: value.PacketID, reasonCodes: append([]byte(nil), value.ReasonCodes...), at: time.Now()}:
				default:
					client.failRead(col, cancel, fmt.Errorf("route subscriber %s SUBACK queue is full", client.id), true)
					return
				}
			case *mqtt.Disconnect:
				client.failRead(col, cancel, fmt.Errorf("client %s received DISCONNECT reason %#02x", client.id, value.ReasonCode), true)
				return
			default:
				client.failRead(col, cancel, fmt.Errorf("client %s received unexpected packet %T", client.id, packet), true)
				return
			}
		}
	}()
}

func validRouteTopic(topic string) bool {
	const prefix = "bp04/route/"
	if len(topic) != len(prefix)+3 || topic[:len(prefix)] != prefix {
		return false
	}
	for _, digit := range topic[len(prefix):] {
		if digit < '0' || digit > '9' {
			return false
		}
	}
	return true
}

func (client *runClient) failRead(col *collector, cancel context.CancelCauseFunc, err error, protocol bool) {
	client.failureMu.Lock()
	firstFailure := client.readFailure == nil
	if firstFailure {
		client.readFailure = err
	}
	failure := client.readFailure
	client.failureMu.Unlock()
	if firstFailure {
		if protocol {
			col.protocolError()
		} else {
			col.transportError()
		}
	}
	cancel(failure)
}

func (client *runClient) failure() error {
	if client == nil {
		return nil
	}
	client.failureMu.Lock()
	defer client.failureMu.Unlock()
	return client.readFailure
}

func (client *runClient) stopReader() {
	if client == nil {
		return
	}
	client.stopOnce.Do(func() { close(client.stop) })
}

func (client *runClient) disconnectAndClose(timeout time.Duration) error {
	if client == nil {
		return nil
	}
	client.closeOnce.Do(func() {
		client.stopReader()
		if client.client != nil {
			disconnectErr := client.client.Disconnect(timeout)
			closeErr := client.client.Close()
			client.closeErr = errors.Join(disconnectErr, closeErr)
		}
		if client.done != nil {
			<-client.done
		}
	})
	return client.closeErr
}

func closeClients(clients []*runClient, timeout time.Duration) error {
	var errs []error
	for _, client := range clients {
		if err := client.disconnectAndClose(timeout); err != nil {
			errs = append(errs, fmt.Errorf("client %s disconnect and close: %w", client.id, err))
		}
	}
	return errors.Join(errs...)
}

func startKeepAlive(ctx context.Context, clients []*runClient, interval time.Duration, col *collector, cancel context.CancelCauseFunc) func() {
	keepAliveCtx, stop := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-keepAliveCtx.Done():
				return
			case <-ticker.C:
				for _, client := range clients {
					if err := client.client.Write(&mqtt.PingReq{Version: mqtt.ProtocolV5}, time.Second); err != nil {
						client.failRead(col, cancel, fmt.Errorf("client %s keepalive write: %w", client.id, err), false)
						return
					}
				}
			}
		}
	}()
	return func() {
		stop()
		<-done
	}
}
