package main

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	mqtt311 "github.com/eclipse/paho.mqtt.golang"
)

func runMQTT311(ctx context.Context, item profileCase, role profileRole, f fixture, p profile, address string, output string) (map[string]any, error) {
	switch role.Role {
	case "subscriber", "publisher":
		return runMQTT311Cross(ctx, item, role, f, p, address, output)
	}
	switch {
	case item.CaseID == "go-mqtt31-rejected":
		return runMQTT31Rejected(ctx, f, p, address)
	case containsCase(item.CaseID, "basic-roundtrip"):
		return runMQTT311Basic(ctx, f, p, address)
	case containsCase(item.CaseID, "persistent-resume"):
		return runMQTT311Persistent(ctx, f, p, address)
	case containsCase(item.CaseID, "qos1-retransmit"):
		return runMQTT311Retransmit(ctx, f, p, address, 1)
	case containsCase(item.CaseID, "qos2-retransmit"):
		return runMQTT311Retransmit(ctx, f, p, address, 2)
	case containsCase(item.CaseID, "abrupt-will"):
		return runMQTT311AbruptWill(ctx, f, p, address)
	default:
		return nil, fmt.Errorf("unsupported MQTT 3.1.1 case %q", item.CaseID)
	}
}

func connectMQTT311(ctx context.Context, address, clientID string, clean bool, protocol uint, handler mqtt311.MessageHandler, lost mqtt311.ConnectionLostHandler, willTopic string, willPayload []byte, p profile, notifications ...mqtt311.ConnectionNotificationHandler) (mqtt311.Client, *mqtt311.ConnectToken, error) {
	options := mqtt311.NewClientOptions()
	options.AddBroker("tcp://" + address)
	options.SetClientID(clientID)
	options.SetProtocolVersion(protocol)
	options.SetCleanSession(clean)
	options.SetOrderMatters(false)
	options.SetKeepAlive(5 * time.Second)
	options.SetConnectTimeout(p.connectTimeout())
	options.SetAutoReconnect(false)
	options.SetConnectRetry(false)
	options.SetResumeSubs(true)
	options.SetStore(mqtt311.NewMemoryStore())
	if handler != nil {
		options.SetDefaultPublishHandler(handler)
	}
	if lost != nil {
		options.SetConnectionLostHandler(lost)
	}
	if len(notifications) > 0 && notifications[0] != nil {
		options.SetConnectionNotificationHandler(notifications[0])
	}
	if willTopic != "" {
		options.SetBinaryWill(willTopic, willPayload, 0, false)
	}
	client := mqtt311.NewClient(options)
	token := client.Connect()
	if !token.WaitTimeout(p.operationTimeout()) {
		return client, nil, errors.New("MQTT 3.1.1 CONNECT timed out")
	}
	if err := token.Error(); err != nil {
		return client, nil, err
	}
	connectToken, ok := token.(*mqtt311.ConnectToken)
	if !ok {
		return client, nil, errors.New("MQTT 3.1.1 CONNECT returned unexpected token")
	}
	return client, connectToken, nil
}

func subscribeMQTT311(client mqtt311.Client, topic string, qos byte, handler mqtt311.MessageHandler, p profile) error {
	token := client.Subscribe(topic, qos, handler)
	if !token.WaitTimeout(p.operationTimeout()) {
		return errors.New("MQTT 3.1.1 SUBSCRIBE timed out")
	}
	return token.Error()
}

func publishMQTT311(client mqtt311.Client, topic string, qos byte, payload []byte, p profile) (uint16, error) {
	token := client.Publish(topic, qos, false, payload)
	publishToken, ok := token.(*mqtt311.PublishToken)
	if !ok {
		return 0, errors.New("MQTT 3.1.1 PUBLISH returned unexpected token")
	}
	packetID := publishToken.MessageID()
	if !token.WaitTimeout(p.operationTimeout()) {
		return packetID, errors.New("MQTT 3.1.1 PUBLISH timed out")
	}
	return packetID, token.Error()
}

func runMQTT311Basic(ctx context.Context, f fixture, p profile, address string) (map[string]any, error) {
	messages := make(chan mqtt311.Message, 8)
	handler := func(_ mqtt311.Client, message mqtt311.Message) { messages <- message }
	client, _, err := connectMQTT311(ctx, address, f.ClientID, true, 4, handler, nil, "", nil, p)
	if err != nil {
		return nil, err
	}
	defer client.Disconnect(0)
	if err := subscribeMQTT311(client, f.Topic, 2, handler, p); err != nil {
		return nil, err
	}
	observed := [3]bool{}
	for qos := byte(0); qos <= 2; qos++ {
		if _, err := publishMQTT311(client, f.Topic, qos, f.Payload, p); err != nil {
			return nil, fmt.Errorf("QoS %d publish: %w", qos, err)
		}
		if err := waitForMQTT311Payload(ctx, messages, f.Topic, f.Hash); err != nil {
			return nil, fmt.Errorf("QoS %d delivery: %w", qos, err)
		}
		observed[qos] = true
	}
	return map[string]any{"qos0RoundTrip": observed[0], "qos1RoundTrip": observed[1], "qos2RoundTrip": observed[2], "disconnectReason": "normal"}, nil
}

func runMQTT311Persistent(ctx context.Context, f fixture, p profile, address string) (map[string]any, error) {
	messages := make(chan mqtt311.Message, 4)
	handler := func(_ mqtt311.Client, message mqtt311.Message) { messages <- message }
	first, _, err := connectMQTT311(ctx, address, f.ClientID, false, 4, handler, nil, "", nil, p)
	if err != nil {
		return nil, err
	}
	if err := subscribeMQTT311(first, f.Topic, 1, handler, p); err != nil {
		first.Disconnect(0)
		return nil, err
	}
	first.Disconnect(0)
	publisher, _, err := connectMQTT311(ctx, address, f.ClientID+"-p", true, 4, nil, nil, "", nil, p)
	if err != nil {
		return nil, err
	}
	if _, err := publishMQTT311(publisher, f.Topic, 1, f.Payload, p); err != nil {
		publisher.Disconnect(0)
		return nil, err
	}
	publisher.Disconnect(0)
	resumed, token, err := connectMQTT311(ctx, address, f.ClientID, false, 4, handler, nil, "", nil, p)
	if err != nil {
		return nil, err
	}
	defer resumed.Disconnect(0)
	if !token.SessionPresent() {
		return map[string]any{"sessionPresent": false, "offlineDelivery": false}, errors.New("MQTT 3.1.1 Session Present was false")
	}
	if err := waitForMQTT311Payload(ctx, messages, f.Topic, f.Hash); err != nil {
		return map[string]any{"sessionPresent": true, "offlineDelivery": false}, err
	}
	return map[string]any{"sessionPresent": true, "offlineDelivery": true}, nil
}

func runMQTT311Retransmit(ctx context.Context, f fixture, p profile, address string, qos byte) (map[string]any, error) {
	observerMessages := make(chan mqtt311.Message, 16)
	observerHandler := func(_ mqtt311.Client, message mqtt311.Message) { observerMessages <- message }
	observerID := f.ClientID + "-o"
	observer, _, err := connectMQTT311(ctx, address, observerID, true, 4, observerHandler, nil, "", nil, p)
	if err != nil {
		return nil, err
	}
	defer observer.Disconnect(0)
	if err := subscribeMQTT311(observer, f.Topic, qos, observerHandler, p); err != nil {
		return nil, err
	}
	lost := make(chan error, 1)
	lostHandler := func(_ mqtt311.Client, reason error) {
		select {
		case lost <- reason:
		default:
		}
	}
	publisher, _, err := connectMQTT311(ctx, address, f.ClientID, false, 4, nil, lostHandler, "", nil, p)
	if err != nil {
		return nil, err
	}
	defer publisher.Disconnect(0)
	publishToken := publisher.Publish(f.Topic, qos, false, f.Payload)
	publishState, ok := publishToken.(*mqtt311.PublishToken)
	if !ok {
		return nil, errors.New("MQTT 3.1.1 retransmission token type mismatch")
	}
	packetID := publishState.MessageID()
	var disconnectReason string
	select {
	case reason := <-lost:
		disconnectReason = "connection_lost"
		if reason != nil {
			disconnectReason = "connection_lost:" + reason.Error()
		}
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	jitter := newJitter(p)
	var reconnect jitterRecord
	var sessionPresent bool
	var connectErr error
	for attempt := 0; attempt < p.MaxReconnectAttempts; attempt++ {
		reconnect = jitter(attempt)
		if reconnect.ChosenDelayMillis > 0 {
			if err := waitContext(ctx, time.Duration(reconnect.ChosenDelayMillis)*time.Millisecond); err != nil {
				return nil, err
			}
		}
		token := publisher.Connect()
		if !token.WaitTimeout(p.operationTimeout()) {
			connectErr = errors.New("MQTT 3.1.1 reconnect timed out")
			continue
		}
		if err := token.Error(); err != nil {
			connectErr = err
			continue
		}
		connectToken, ok := token.(*mqtt311.ConnectToken)
		if !ok {
			connectErr = errors.New("MQTT 3.1.1 reconnect token type mismatch")
			continue
		}
		sessionPresent = connectToken.SessionPresent()
		connectErr = nil
		break
	}
	if connectErr != nil {
		return nil, connectErr
	}
	if !publishToken.WaitTimeout(p.operationTimeout()) {
		return nil, errors.New("MQTT 3.1.1 retransmitted PUBLISH timed out")
	}
	if err := publishToken.Error(); err != nil {
		return nil, err
	}
	deliveryCount := 0
	deadline := time.NewTimer(p.operationTimeout())
	defer deadline.Stop()
	for deliveryCount == 0 {
		select {
		case message := <-observerMessages:
			if string(message.Payload()) == string(f.Payload) || requirePayloadHash(message.Payload(), f.Hash) {
				deliveryCount++
			}
		case <-deadline.C:
			return nil, errors.New("retransmitted message was not delivered")
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	dup, same, err := readProxyTrace(ctx, filepath.Join("/proxy-output", "proxy-trace.jsonl"), packetID, qos)
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"samePacketIdentifier": same, "dupObserved": dup, "completion": true, "subscriberDeliveries": deliveryCount,
		"reconnectAttempt": reconnect.Attempt + 1, "reconnectUpperBoundMillis": reconnect.UpperBoundMillis,
		"reconnectDelayMillis": reconnect.ChosenDelayMillis, "disconnectReason": disconnectReason, "sessionPresent": sessionPresent,
	}, nil
}

func runMQTT311AbruptWill(ctx context.Context, f fixture, p profile, address string) (map[string]any, error) {
	messages := make(chan mqtt311.Message, 2)
	handler := func(_ mqtt311.Client, message mqtt311.Message) { messages <- message }
	observer, _, err := connectMQTT311(ctx, address, f.ClientID+"-o", true, 4, handler, nil, "", nil, p)
	if err != nil {
		return nil, err
	}
	defer observer.Disconnect(0)
	if err := subscribeMQTT311(observer, f.WillTopic, 0, handler, p); err != nil {
		return nil, err
	}
	publisher, _, err := connectMQTT311(ctx, address, f.ClientID, true, 4, nil, nil, f.WillTopic, f.Payload, p)
	if err != nil {
		return nil, err
	}
	if terminator, ok := publisher.(interface{ TerminateConnectionForTest() }); ok {
		terminator.TerminateConnectionForTest()
	} else {
		publisher.Disconnect(0)
	}
	if err := waitForMQTT311Payload(ctx, messages, f.WillTopic, f.Hash); err != nil {
		return map[string]any{"willReceived": false, "willClassification": "expected_mqtt311_server_disconnect"}, err
	}
	return map[string]any{"willReceived": true, "willClassification": "expected_mqtt311_server_disconnect"}, nil
}

func runMQTT31Rejected(ctx context.Context, f fixture, p profile, address string) (map[string]any, error) {
	failed := make(chan struct{}, 1)
	notification := func(_ mqtt311.Client, event mqtt311.ConnectionNotification) {
		if _, ok := event.(mqtt311.ConnectionNotificationFailed); !ok {
			return
		}
		select {
		case failed <- struct{}{}:
		default:
		}
	}
	client, _, err := connectMQTT311(ctx, address, f.ClientID, true, 3, nil, nil, "", nil, p, notification)
	if err == nil {
		client.Disconnect(0)
		return map[string]any{"connackAccepted": true, "connectionClosed": false}, errors.New("MQTT 3.1.0 CONNECT was accepted")
	}
	if client == nil {
		return map[string]any{"connackAccepted": false, "connectionClosed": false}, errors.New("MQTT 3.1.0 client was unavailable after rejection")
	}
	select {
	case <-failed:
		return map[string]any{"connackAccepted": false, "connectionClosed": true}, nil
	case <-ctx.Done():
		return map[string]any{"connackAccepted": false, "connectionClosed": false}, fmt.Errorf("MQTT 3.1.0 connection did not close: %w", ctx.Err())
	}
}

func runMQTT311Cross(ctx context.Context, item profileCase, role profileRole, f fixture, p profile, address, output string) (map[string]any, error) {
	messages := make(chan mqtt311.Message, 4)
	handler := func(_ mqtt311.Client, message mqtt311.Message) { messages <- message }
	client, _, err := connectMQTT311(ctx, address, f.ClientID, true, 4, handler, nil, "", nil, p)
	if err != nil {
		return nil, err
	}
	defer client.Disconnect(0)
	if role.Role == "subscriber" {
		if err := subscribeMQTT311(client, f.Topic, 1, handler, p); err != nil {
			return nil, err
		}
		if err := writeReady(output, item.CaseID, "go", item.Protocol); err != nil {
			return nil, err
		}
		if err := waitForMQTT311Payload(ctx, messages, f.Topic, f.Hash); err != nil {
			return nil, err
		}
		return map[string]any{"readinessSignaled": true, "topic": f.Topic, "payloadHash": f.Hash, "qos": 1, "deliveryCompleted": true}, nil
	}
	if err := requireReadiness(item); err != nil {
		return nil, err
	}
	if _, err := publishMQTT311(client, f.Topic, 1, f.Payload, p); err != nil {
		return nil, err
	}
	return map[string]any{"topic": f.Topic, "payloadHash": f.Hash, "qos": 1, "ackCompleted": true}, nil
}

func waitForMQTT311Payload(ctx context.Context, messages <-chan mqtt311.Message, expectedTopic, expectedHash string) error {
	for {
		select {
		case message := <-messages:
			if requirePayloadHash(message.Payload(), expectedHash) {
				if message.Topic() != expectedTopic {
					return fmt.Errorf("payload arrived on topic %q, want %q", message.Topic(), expectedTopic)
				}
				return nil
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func containsCase(caseID, suffix string) bool {
	for _, value := range []string{"-" + suffix, suffix} {
		if len(caseID) >= len(value) && caseID[len(caseID)-len(value):] == value {
			return true
		}
	}
	return false
}
