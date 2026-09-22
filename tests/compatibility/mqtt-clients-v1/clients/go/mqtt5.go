package main

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"time"

	autopaho "github.com/eclipse/paho.golang/autopaho"
	paho5 "github.com/eclipse/paho.golang/paho"
)

type mqtt5Message struct {
	topic   string
	payload []byte
}

type mqtt5Client struct {
	manager  *autopaho.ConnectionManager
	connacks chan bool
	messages chan mqtt5Message
	lost     chan error
	jitter   []jitterRecord
}

func runMQTT5(ctx context.Context, item profileCase, role profileRole, f fixture, p profile, output, address string) (map[string]any, error) {
	if role.Role == "subscriber" || role.Role == "publisher" {
		return runMQTT5Cross(ctx, item, role, f, p, output, address)
	}
	switch {
	case containsCase(item.CaseID, "basic-roundtrip"):
		return runMQTT5Basic(ctx, f, p, address)
	case containsCase(item.CaseID, "persistent-resume"):
		return runMQTT5Persistent(ctx, f, p, address)
	case containsCase(item.CaseID, "qos1-retransmit"):
		return runMQTT5Retransmit(ctx, f, p, output, address, 1)
	case containsCase(item.CaseID, "qos2-retransmit"):
		return runMQTT5Retransmit(ctx, f, p, output, address, 2)
	case containsCase(item.CaseID, "will-delay-resume"):
		return runMQTT5WillDelay(ctx, f, p, address)
	default:
		return nil, fmt.Errorf("unsupported MQTT 5 case %q", item.CaseID)
	}
}

func newMQTT5Client(ctx context.Context, address, clientID string, cleanStart bool, p profile, handler func(paho5.PublishReceived) (bool, error), willTopic string, willPayload []byte, willDelay uint32) (*mqtt5Client, error) {
	brokerURL, err := url.Parse("mqtt://" + address)
	if err != nil {
		return nil, err
	}
	client := &mqtt5Client{connacks: make(chan bool, 8), messages: make(chan mqtt5Message, 16), lost: make(chan error, 8)}
	backoff := newJitter(p)
	config := autopaho.ClientConfig{
		ServerUrls:                    []*url.URL{brokerURL},
		KeepAlive:                     5,
		CleanStartOnInitialConnection: cleanStart,
		SessionExpiryInterval:         uint32(p.SessionExpirySeconds),
		ConnectTimeout:                p.connectTimeout(),
		ReconnectBackoff: func(attempt int) time.Duration {
			record := backoff(attempt)
			client.jitter = append(client.jitter, record)
			return time.Duration(record.ChosenDelayMillis) * time.Millisecond
		},
		OnConnectError: func(connectErr error) {
			fmt.Printf("MQTT 5 connection attempt failed: %v\n", connectErr)
		},
		ConnectPacketBuilder: func(connect *paho5.Connect, _ *url.URL) (*paho5.Connect, error) {
			if connect.Properties == nil {
				connect.Properties = &paho5.ConnectProperties{}
			}
			connect.Properties.RequestProblemInfo = true
			return connect, nil
		},
		OnConnectionUp: func(_ *autopaho.ConnectionManager, connack *paho5.Connack) {
			select {
			case client.connacks <- connack.SessionPresent:
			default:
			}
		},
		OnConnectionDown: func() bool {
			select {
			case client.lost <- nil:
			default:
			}
			return true
		},
		ClientConfig: paho5.ClientConfig{
			ClientID: clientID,
			OnPublishReceived: []func(paho5.PublishReceived) (bool, error){func(received paho5.PublishReceived) (bool, error) {
				if received.Packet != nil {
					select {
					case client.messages <- mqtt5Message{topic: received.Packet.Topic, payload: append([]byte(nil), received.Packet.Payload...)}:
					default:
					}
				}
				if handler != nil {
					return handler(received)
				}
				return false, nil
			}},
		},
	}
	if willTopic != "" {
		config.WillMessage = &paho5.WillMessage{Topic: willTopic, Payload: willPayload, QoS: 0}
		config.WillProperties = &paho5.WillProperties{WillDelayInterval: &willDelay}
	}
	manager, err := autopaho.NewConnection(ctx, config)
	if err != nil {
		return nil, err
	}
	client.manager = manager
	return client, nil
}

func (client *mqtt5Client) await(ctx context.Context) (bool, error) {
	if err := client.manager.AwaitConnection(ctx); err != nil {
		return false, err
	}
	select {
	case present := <-client.connacks:
		return present, nil
	case <-ctx.Done():
		return false, ctx.Err()
	}
}

func (client *mqtt5Client) disconnect(ctx context.Context) error {
	return client.manager.Disconnect(ctx)
}

func subscribeMQTT5(ctx context.Context, client *mqtt5Client, topic string, qos byte) error {
	_, err := client.manager.Subscribe(ctx, &paho5.Subscribe{Subscriptions: []paho5.SubscribeOptions{{Topic: topic, QoS: qos}}})
	return err
}

func publishMQTT5(ctx context.Context, client *mqtt5Client, topic string, qos byte, payload []byte) error {
	_, err := client.manager.Publish(ctx, &paho5.Publish{Topic: topic, QoS: qos, Payload: payload})
	return err
}

func runMQTT5Basic(ctx context.Context, f fixture, p profile, address string) (map[string]any, error) {
	client, err := newMQTT5Client(ctx, address, f.ClientID, true, p, nil, "", nil, 0)
	if err != nil {
		return nil, err
	}
	defer client.disconnect(context.Background())
	if _, err := client.await(ctx); err != nil {
		return nil, err
	}
	if err := subscribeMQTT5(ctx, client, f.Topic, 2); err != nil {
		return nil, err
	}
	observed := [3]bool{}
	for qos := byte(0); qos <= 2; qos++ {
		if err := publishMQTT5(ctx, client, f.Topic, qos, f.Payload); err != nil {
			return nil, fmt.Errorf("QoS %d publish: %w", qos, err)
		}
		if err := waitForPayload(ctx, client.messages, f.Topic, f.Hash); err != nil {
			return nil, fmt.Errorf("QoS %d delivery: %w", qos, err)
		}
		observed[qos] = true
	}
	return map[string]any{"qos0RoundTrip": observed[0], "qos1RoundTrip": observed[1], "qos2RoundTrip": observed[2], "disconnectReason": "normal"}, nil
}

func runMQTT5Persistent(ctx context.Context, f fixture, p profile, address string) (map[string]any, error) {
	first, err := newMQTT5Client(ctx, address, f.ClientID, true, p, nil, "", nil, 0)
	if err != nil {
		return nil, err
	}
	if _, err := first.await(ctx); err != nil {
		return nil, err
	}
	if err := subscribeMQTT5(ctx, first, f.Topic, 1); err != nil {
		first.disconnect(context.Background())
		return nil, err
	}
	if err := first.disconnect(context.Background()); err != nil {
		return nil, err
	}
	publisher, err := newMQTT5Client(ctx, address, f.ClientID+"-p", true, p, nil, "", nil, 0)
	if err != nil {
		return nil, err
	}
	if _, err := publisher.await(ctx); err != nil {
		return nil, err
	}
	if err := publishMQTT5(ctx, publisher, f.Topic, 1, f.Payload); err != nil {
		publisher.disconnect(context.Background())
		return nil, err
	}
	if err := publisher.disconnect(context.Background()); err != nil {
		return nil, err
	}
	resumed, err := newMQTT5Client(ctx, address, f.ClientID, false, p, nil, "", nil, 0)
	if err != nil {
		return nil, err
	}
	defer resumed.disconnect(context.Background())
	present, err := resumed.await(ctx)
	if err != nil {
		return nil, err
	}
	if !present {
		return map[string]any{"sessionPresent": false, "offlineDelivery": false}, errors.New("MQTT 5 Session Present was false")
	}
	if err := waitForPayload(ctx, resumed.messages, f.Topic, f.Hash); err != nil {
		return map[string]any{"sessionPresent": true, "offlineDelivery": false}, err
	}
	return map[string]any{"sessionPresent": true, "offlineDelivery": true}, nil
}

func runMQTT5Retransmit(ctx context.Context, f fixture, p profile, output, address string, qos byte) (map[string]any, error) {
	observer, err := newMQTT5Client(ctx, address, f.ClientID+"-o", true, p, nil, "", nil, 0)
	if err != nil {
		return nil, err
	}
	defer observer.disconnect(context.Background())
	if _, err := observer.await(ctx); err != nil {
		return nil, err
	}
	if err := subscribeMQTT5(ctx, observer, f.Topic, qos); err != nil {
		return nil, err
	}
	publisher, err := newMQTT5Client(ctx, address, f.ClientID, false, p, nil, "", nil, 0)
	if err != nil {
		return nil, err
	}
	defer publisher.disconnect(context.Background())
	if _, err := publisher.await(ctx); err != nil {
		return nil, err
	}
	publishErr := make(chan error, 1)
	go func() { publishErr <- publishMQTT5(ctx, publisher, f.Topic, qos, f.Payload) }()
	var disconnectReason string
	select {
	case <-publisher.lost:
		disconnectReason = "connection_lost"
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case err := <-publishErr:
		if err != nil && !errors.Is(err, context.DeadlineExceeded) {
			return nil, err
		}
	default:
	}
	deadline := time.NewTimer(p.operationTimeout())
	defer deadline.Stop()
	for {
		if len(publisher.jitter) > 0 {
			break
		}
		select {
		case <-deadline.C:
			return nil, errors.New("MQTT 5 reconnect did not start")
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
	if err := <-publishErr; err != nil {
		return nil, err
	}
	if err := waitForPayload(ctx, observer.messages, f.Topic, f.Hash); err != nil {
		return nil, err
	}
	reconnect := publisher.jitter[len(publisher.jitter)-1]
	dup, same, err := readProxyTrace(ctx, filepath.Join("/proxy-output", "proxy-trace.jsonl"), 0, qos)
	if err != nil {
		return nil, err
	}
	return map[string]any{"samePacketIdentifier": same, "dupObserved": dup, "completion": true, "subscriberDeliveries": 1,
		"reconnectAttempt": reconnect.Attempt + 1, "reconnectUpperBoundMillis": reconnect.UpperBoundMillis,
		"reconnectDelayMillis": reconnect.ChosenDelayMillis, "disconnectReason": disconnectReason, "sessionPresent": true}, nil
}

func runMQTT5WillDelay(ctx context.Context, f fixture, p profile, address string) (map[string]any, error) {
	observer, err := newMQTT5Client(ctx, address, f.ClientID+"-o", true, p, nil, "", nil, 0)
	if err != nil {
		return nil, err
	}
	defer observer.disconnect(context.Background())
	if _, err := observer.await(ctx); err != nil {
		return nil, err
	}
	if err := subscribeMQTT5(ctx, observer, f.WillTopic, 0); err != nil {
		return nil, err
	}
	publisher, err := newMQTT5Client(ctx, address, f.ClientID, true, p, nil, f.WillTopic, f.Payload, uint32(p.WillDelaySeconds))
	if err != nil {
		return nil, err
	}
	if _, err := publisher.await(ctx); err != nil {
		return nil, err
	}
	publisher.manager.TerminateConnectionForTest()
	if err := waitContext(ctx, time.Duration(p.ReconnectWithinMillis)*time.Millisecond); err != nil {
		return nil, err
	}
	present, err := publisher.await(ctx)
	if err != nil {
		return nil, err
	}
	if err := waitContext(ctx, time.Duration(p.WillDelaySeconds)*time.Second+500*time.Millisecond); err != nil {
		return nil, err
	}
	select {
	case <-observer.messages:
		return map[string]any{"willReceived": true, "sessionPresent": present, "willDelaySeconds": p.WillDelaySeconds}, errors.New("MQTT 5 Will was published after resume")
	default:
	}
	return map[string]any{"willReceived": false, "sessionPresent": present, "willDelaySeconds": p.WillDelaySeconds}, nil
}

func runMQTT5Cross(ctx context.Context, item profileCase, role profileRole, f fixture, p profile, output, address string) (map[string]any, error) {
	client, err := newMQTT5Client(ctx, address, f.ClientID, true, p, nil, "", nil, 0)
	if err != nil {
		return nil, err
	}
	defer client.disconnect(context.Background())
	if _, err := client.await(ctx); err != nil {
		return nil, err
	}
	if role.Role == "subscriber" {
		if err := subscribeMQTT5(ctx, client, f.Topic, 1); err != nil {
			return nil, err
		}
		if err := writeReady(output, item.CaseID, "go", item.Protocol); err != nil {
			return nil, err
		}
		if err := waitForPayload(ctx, client.messages, f.Topic, f.Hash); err != nil {
			return nil, err
		}
		return map[string]any{"readinessSignaled": true, "topic": f.Topic, "payloadHash": f.Hash, "qos": 1, "deliveryCompleted": true}, nil
	}
	if err := requireReadiness(item); err != nil {
		return nil, err
	}
	if err := publishMQTT5(ctx, client, f.Topic, 1, f.Payload); err != nil {
		return nil, err
	}
	return map[string]any{"topic": f.Topic, "payloadHash": f.Hash, "qos": 1, "ackCompleted": true}, nil
}

func waitForPayload(ctx context.Context, messages <-chan mqtt5Message, expectedTopic, expectedHash string) error {
	for {
		select {
		case message := <-messages:
			if requirePayloadHash(message.payload, expectedHash) {
				if message.topic != expectedTopic {
					return fmt.Errorf("payload arrived on topic %q, want %q", message.topic, expectedTopic)
				}
				return nil
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
