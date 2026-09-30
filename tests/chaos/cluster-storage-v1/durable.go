package main

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"sort"
	"strings"
	"sync/atomic"
	"time"
)

const (
	durableAckTimeout       = 2 * time.Second
	durableDeliveryTimeout  = 15 * time.Second
	durableDeliveryQuietFor = time.Second
	inflightCutSequence     = 5
)

type durableAckOutcome string

const (
	durableAcked    durableAckOutcome = "acked"
	durableTimedOut durableAckOutcome = "timeout"
	durableClosed   durableAckOutcome = "closed"
	durableUnknown  durableAckOutcome = "unknown"
)

type durableClient struct {
	connection     net.Conn
	stopClose      func() bool
	nextPacketID   uint16
	pubrecReceived map[uint16]bool
}

type inflightPublishResult struct {
	acked          []string
	unacked        []string
	unackedOutcome durableAckOutcome
	unackedErr     error
	err            error
}

func (scenario *storageFaultScenario) startInflightPublisher(ctx context.Context, nodeID string, acked *atomic.Int64, inFlight chan<- string) <-chan inflightPublishResult {
	results := make(chan inflightPublishResult, 1)
	go func() {
		defer close(results)
		result := inflightPublishResult{acked: make([]string, 0, inflightCutSequence-1), unacked: make([]string, 0, 1)}
		address := "127.0.0.1:" + scenario.docker.mqttPorts[nodeID]
		client, _, err := dialDurableClient(ctx, address, scenario.fixture.AccessToken, "chaos-durable-pub-3", true)
		if err != nil {
			result.err = err
			results <- result
			return
		}
		defer client.close()
		for sequence := 1; sequence <= 40; sequence++ {
			topic := durableTopic(scenario.options.ScenarioID, "leader", fmt.Sprintf("qos1-%02d", sequence))
			packetID, err := client.publish(ctx, topic, []byte(topic), 1)
			if err != nil {
				result.unacked = append(result.unacked, topic)
				result.unackedOutcome = durableClosed
				result.unackedErr = err
				results <- result
				return
			}
			if sequence == inflightCutSequence {
				select {
				case inFlight <- topic:
				case <-ctx.Done():
					result.err = ctx.Err()
					results <- result
					return
				}
				<-ctx.Done()
				result.unacked = append(result.unacked, topic)
				result.unackedOutcome = durableUnknown
				result.unackedErr = errors.New("power cut interrupted PUBACK observation; broker commit outcome is unknown")
				results <- result
				return
			}
			ackCtx, cancel := context.WithTimeout(ctx, durableAckTimeout)
			outcome, ackErr := client.awaitAck(ackCtx, packetID, 1)
			cancel()
			if outcome != durableAcked {
				result.unacked = append(result.unacked, topic)
				result.unackedOutcome = outcome
				result.unackedErr = ackErr
				results <- result
				return
			}
			result.acked = append(result.acked, topic)
			acked.Add(1)
			if sequence < inflightCutSequence {
				if err := sleepContext(ctx, 100*time.Millisecond); err != nil {
					result.err = err
					results <- result
					return
				}
			}
		}
		results <- result
	}()
	return results
}

type durableMessage struct {
	topic    string
	payload  []byte
	qos      byte
	packetID uint16
}

func dialDurableClient(ctx context.Context, address, accessToken, clientID string, cleanSession bool) (*durableClient, bool, error) {
	connection, err := (&net.Dialer{Timeout: 2 * time.Second}).DialContext(ctx, "tcp", address)
	if err != nil {
		return nil, false, fmt.Errorf("dial durable MQTT client %s: %w", clientID, err)
	}
	stopClose := context.AfterFunc(ctx, func() { _ = connection.Close() })
	client := &durableClient{connection: connection, stopClose: stopClose, pubrecReceived: make(map[uint16]bool)}
	connectPacket, err := durableConnectPacket(clientID, accessToken, cleanSession)
	if err == nil {
		err = writeMQTTPacket(connection, ctx, connectPacket)
	}
	if err != nil {
		client.close()
		return nil, false, fmt.Errorf("send durable MQTT CONNECT: %w", err)
	}
	header, body, err := readMQTTPacket(connection, ctx)
	if err != nil {
		client.close()
		return nil, false, fmt.Errorf("read durable MQTT CONNACK: %w", err)
	}
	if header != 0x20 || len(body) != 2 || body[1] != 0 {
		client.close()
		return nil, false, fmt.Errorf("durable MQTT client %s CONNACK rejected: header=0x%02x body=%x", clientID, header, body)
	}
	return client, body[0]&1 != 0, nil
}

func (scenario *storageFaultScenario) prepareDurableSubscriber(ctx context.Context, leaderID string) error {
	subscriberNodeID := leaderID
	if scenario.options.ScenarioID == "follower-slow-io" {
		subscriberNodeID = followerNodeIDs(leaderID)[0]
	}
	address := "127.0.0.1:" + scenario.docker.mqttPorts[subscriberNodeID]
	client, _, err := dialDurableClient(ctx, address, scenario.fixture.AccessToken, "chaos-durable-sub", false)
	if err != nil {
		return err
	}
	if err := client.subscribe(ctx, "chaos/durable/#", 2); err != nil {
		client.close()
		return fmt.Errorf("subscribe durable ACK observer: %w", err)
	}
	scenario.durableSubscriber = client
	scenario.evidence.DurableAck = &durableAckEvidence{
		AckedBeforeFault: []string{}, AckedDuringFault: []string{}, UnackedDuringFault: []string{},
		AckOutcomeUnknownDuringFault: []string{}, AckedAfterRecovery: []string{}, MissingAcked: []string{}, Duplicates: []string{},
	}
	if err := waitFor(ctx, 30*time.Second, probeInterval, func() (bool, error) {
		for _, nodeID := range nodeIDs() {
			response, status, _, err := scenario.subscriptions(ctx, nodeID)
			if err != nil {
				return false, err
			}
			if status != 200 || !hasSubscription(response, "chaos-durable-sub", "chaos/durable/#") {
				return false, fmt.Errorf("node %s does not list the durable ACK subscription", nodeID)
			}
		}
		return true, nil
	}); err != nil {
		client.close()
		scenario.durableSubscriber = nil
		return fmt.Errorf("durable ACK subscription did not replicate: %w", err)
	}
	return nil
}

func durableTopic(scenarioID, source, sequence string) string {
	return fmt.Sprintf("chaos/durable/%s/%s/%s", scenarioID, source, sequence)
}

func durableConnectPacket(clientID, accessToken string, cleanSession bool) ([]byte, error) {
	flags := byte(0xc0) // MQTT username and password fields carry the JWT authentication.
	if cleanSession {
		flags |= 0x02
	}
	body := []byte{0, 4, 'M', 'Q', 'T', 'T', 4, flags, 0, 0}
	for _, field := range []string{clientID, "jwt", accessToken} {
		if len(field) > 65_535 {
			return nil, errors.New("MQTT CONNECT field exceeds the protocol length limit")
		}
		body = binary.BigEndian.AppendUint16(body, uint16(len(field)))
		body = append(body, field...)
	}
	return mqttPacket(0x10, body), nil
}

func (client *durableClient) close() error {
	if client == nil || client.connection == nil {
		return nil
	}
	if client.stopClose != nil {
		client.stopClose()
	}
	return client.connection.Close()
}

func (client *durableClient) subscribe(ctx context.Context, topicFilter string, qos byte) error {
	if qos > 2 {
		return fmt.Errorf("invalid MQTT subscription QoS %d", qos)
	}
	if len(topicFilter) > 65_535 {
		return errors.New("MQTT topic filter exceeds the protocol length limit")
	}
	body := []byte{0, 1}
	body = binary.BigEndian.AppendUint16(body, uint16(len(topicFilter)))
	body = append(body, topicFilter...)
	body = append(body, qos)
	if err := writeMQTTPacket(client.connection, ctx, mqttPacket(0x82, body)); err != nil {
		return err
	}
	header, response, err := readMQTTPacket(client.connection, ctx)
	if err != nil {
		return fmt.Errorf("read durable MQTT SUBACK: %w", err)
	}
	if header != 0x90 || len(response) != 3 || binary.BigEndian.Uint16(response[:2]) != 1 || response[2] > qos {
		return fmt.Errorf("invalid durable MQTT SUBACK: header=0x%02x body=%x", header, response)
	}
	return nil
}

func (client *durableClient) publish(ctx context.Context, topic string, payload []byte, qos byte) (uint16, error) {
	if qos != 1 && qos != 2 {
		return 0, fmt.Errorf("durable MQTT publish requires QoS 1 or 2, got %d", qos)
	}
	client.nextPacketID++
	if client.nextPacketID == 0 {
		client.nextPacketID++
	}
	packetID := client.nextPacketID
	return packetID, client.publishPacket(ctx, topic, payload, qos, packetID, false)
}

func (client *durableClient) publishPacket(ctx context.Context, topic string, payload []byte, qos byte, packetID uint16, duplicate bool) error {
	if len(topic) > 65_535 {
		return errors.New("MQTT topic exceeds the protocol length limit")
	}
	if qos > 2 || (qos > 0 && packetID == 0) {
		return fmt.Errorf("invalid MQTT PUBLISH QoS %d or packet identifier %d", qos, packetID)
	}
	body := make([]byte, 0, 2+len(topic)+2+len(payload))
	body = binary.BigEndian.AppendUint16(body, uint16(len(topic)))
	body = append(body, topic...)
	if qos > 0 {
		body = binary.BigEndian.AppendUint16(body, packetID)
	}
	body = append(body, payload...)
	header := byte(0x30 | qos<<1)
	if duplicate {
		header |= 0x08
	}
	return writeMQTTPacket(client.connection, ctx, mqttPacket(header, body))
}

func (client *durableClient) awaitAck(ctx context.Context, packetID uint16, qos byte) (durableAckOutcome, error) {
	for {
		header, body, err := readMQTTPacket(client.connection, ctx)
		if err != nil {
			return classifyDurableAckReadError(ctx, err), err
		}
		if qos == 1 && header == 0x40 && hasPacketID(body, packetID) {
			return durableAcked, nil
		}
		if qos != 2 || header != 0x50 || !hasPacketID(body, packetID) {
			continue
		}
		client.pubrecReceived[packetID] = true
		if err := writeMQTTPacket(client.connection, ctx, mqttControlPacket(0x62, packetID)); err != nil {
			return classifyDurableAckReadError(ctx, err), err
		}
		for {
			header, body, err = readMQTTPacket(client.connection, ctx)
			if err != nil {
				return classifyDurableAckReadError(ctx, err), err
			}
			if header == 0x70 && hasPacketID(body, packetID) {
				delete(client.pubrecReceived, packetID)
				return durableAcked, nil
			}
		}
	}
}

func (client *durableClient) resumeQoS2(ctx context.Context, packetID uint16, topic string, payload []byte, pubrecReceived bool) (durableAckOutcome, error) {
	if pubrecReceived {
		if err := writeMQTTPacket(client.connection, ctx, mqttControlPacket(0x6a, packetID)); err != nil {
			return classifyDurableAckReadError(ctx, err), err
		}
	} else if err := client.publishPacket(ctx, topic, payload, 2, packetID, true); err != nil {
		return classifyDurableAckReadError(ctx, err), err
	}
	return client.awaitAck(ctx, packetID, 2)
}

func mqttControlPacket(header byte, packetID uint16) []byte {
	return mqttPacket(header, []byte{byte(packetID >> 8), byte(packetID)})
}

func hasPacketID(body []byte, packetID uint16) bool {
	return len(body) == 2 && binary.BigEndian.Uint16(body) == packetID
}

func classifyDurableAckReadError(ctx context.Context, err error) durableAckOutcome {
	if ctx.Err() != nil {
		return durableTimedOut
	}
	var networkError net.Error
	if errors.As(err, &networkError) && networkError.Timeout() {
		return durableTimedOut
	}
	return durableClosed
}

func parseDurablePublish(header byte, body []byte) (durableMessage, error) {
	if header&0xf0 != 0x30 || len(body) < 2 {
		return durableMessage{}, errors.New("invalid MQTT PUBLISH packet")
	}
	topicLength := int(binary.BigEndian.Uint16(body[:2]))
	if len(body) < 2+topicLength {
		return durableMessage{}, errors.New("truncated MQTT PUBLISH topic")
	}
	qos := (header >> 1) & 0x03
	if qos == 3 {
		return durableMessage{}, errors.New("invalid MQTT PUBLISH QoS bits")
	}
	position := 2 + topicLength
	message := durableMessage{topic: string(body[2:position]), qos: qos}
	if qos > 0 {
		if len(body) < position+2 {
			return durableMessage{}, errors.New("truncated MQTT PUBLISH packet identifier")
		}
		message.packetID = binary.BigEndian.Uint16(body[position : position+2])
		if message.packetID == 0 {
			return durableMessage{}, errors.New("MQTT PUBLISH packet identifier is zero")
		}
		position += 2
	}
	message.payload = append([]byte(nil), body[position:]...)
	return message, nil
}

func (client *durableClient) receiveQoS2Delivery(ctx context.Context) (durableMessage, error) {
	header, body, err := readMQTTPacket(client.connection, ctx)
	if err != nil {
		return durableMessage{}, err
	}
	message, err := parseDurableQoS2Delivery(header, body)
	if err != nil {
		return durableMessage{}, err
	}
	if err := writeMQTTPacket(client.connection, ctx, mqttControlPacket(0x50, message.packetID)); err != nil {
		return durableMessage{}, err
	}
	for {
		header, body, err = readMQTTPacket(client.connection, ctx)
		if err != nil {
			return durableMessage{}, err
		}
		if header == 0x62 && hasPacketID(body, message.packetID) {
			if err := writeMQTTPacket(client.connection, ctx, mqttControlPacket(0x70, message.packetID)); err != nil {
				return durableMessage{}, err
			}
			return message, nil
		}
	}
}

func (scenario *storageFaultScenario) durablePublish(ctx context.Context, nodeID, clientID, topic string, qos byte, phase string, expectAck bool) error {
	address := "127.0.0.1:" + scenario.docker.mqttPorts[nodeID]
	client, _, err := dialDurableClient(ctx, address, scenario.fixture.AccessToken, clientID, true)
	if err != nil {
		return err
	}
	defer client.close()
	return scenario.publishDurableClient(ctx, nodeID, clientID, client, topic, qos, phase, expectAck)
}

func (scenario *storageFaultScenario) publishDurableClient(ctx context.Context, nodeID, clientID string, client *durableClient, topic string, qos byte, phase string, expectAck bool) error {
	packetID, err := client.publish(ctx, topic, []byte(topic), qos)
	if err != nil {
		return fmt.Errorf("publish durable topic %s: %w", topic, err)
	}
	ackCtx, cancel := context.WithTimeout(ctx, durableAckTimeout)
	outcome, ackErr := client.awaitAck(ackCtx, packetID, qos)
	cancel()
	if err := scenario.recordDurableAck(nodeID, clientID, topic, qos, phase, outcome, ackErr); err != nil {
		return err
	}
	if expectAck && outcome != durableAcked {
		return fmt.Errorf("durable publish %s QoS %d outcome %q, want ACK: %v", topic, qos, outcome, ackErr)
	}
	if !expectAck && outcome == durableAcked {
		return fmt.Errorf("durable publish %s QoS %d received ACK without quorum", topic, qos)
	}
	return nil
}
func (scenario *storageFaultScenario) publishFollowerFaultMessages(ctx context.Context, leaderID, healthyID string) ([]string, error) {
	publishers := []struct {
		nodeID string
		source string
	}{
		{nodeID: leaderID, source: "leader"},
		{nodeID: healthyID, source: "healthy-follower"},
	}
	var expected []string
	for index, publisher := range publishers {
		clientID := fmt.Sprintf("chaos-durable-pub-%d", index+1)
		address := "127.0.0.1:" + scenario.docker.mqttPorts[publisher.nodeID]
		client, _, err := dialDurableClient(ctx, address, scenario.fixture.AccessToken, clientID, true)
		if err != nil {
			return expected, err
		}
		publishErr := func() error {
			defer client.close()
			for sequence := 1; sequence <= 2; sequence++ {
				topic := durableTopic(scenario.options.ScenarioID, publisher.source, fmt.Sprintf("qos1-%d", sequence))
				if err := scenario.publishDurableClient(ctx, publisher.nodeID, clientID, client, topic, 1, "during-fault", true); err != nil {
					return err
				}
				expected = append(expected, topic)
			}
			topic := durableTopic(scenario.options.ScenarioID, publisher.source, "qos2")
			if err := scenario.publishDurableClient(ctx, publisher.nodeID, clientID, client, topic, 2, "during-fault", true); err != nil {
				return err
			}
			expected = append(expected, topic)
			return nil
		}()
		if publishErr != nil {
			return expected, publishErr
		}
	}
	return expected, nil
}

type quorumFaultQoS2State struct {
	packetID       uint16
	pubrecReceived bool
	topic          string
}

type quorumFaultPublisher struct {
	clientID string
	client   *durableClient
	topic    string
	qos      byte
}

type quorumFaultPublishResult struct {
	publisher quorumFaultPublisher
	packetID  uint16
	outcome   durableAckOutcome
	ackErr    error
}

func (scenario *storageFaultScenario) publishDuringQuorumFault(ctx context.Context, leaderID string, cleanClient, persistentClient *durableClient, qos1Topic, qos2Topic string) (quorumFaultQoS2State, error) {
	publishers := []quorumFaultPublisher{
		{clientID: "chaos-durable-halt-1", client: cleanClient, topic: qos1Topic, qos: 1},
		{clientID: "chaos-durable-halt-2", client: persistentClient, topic: qos2Topic, qos: 2},
	}
	results := make(chan quorumFaultPublishResult, len(publishers))
	for _, publisher := range publishers {
		go func(publisher quorumFaultPublisher) {
			packetID, publishErr := publisher.client.publish(ctx, publisher.topic, []byte(publisher.topic), publisher.qos)
			outcome := durableClosed
			ackErr := publishErr
			if publishErr == nil {
				ackCtx, cancel := context.WithTimeout(ctx, durableAckTimeout)
				outcome, ackErr = publisher.client.awaitAck(ackCtx, packetID, publisher.qos)
				cancel()
			}
			results <- quorumFaultPublishResult{publisher: publisher, packetID: packetID, outcome: outcome, ackErr: ackErr}
		}(publisher)
	}
	var state quorumFaultQoS2State
	var publishErrors []error
	for range publishers {
		result := <-results
		if err := scenario.recordDurableAck(leaderID, result.publisher.clientID, result.publisher.topic,
			result.publisher.qos, "during-fault", result.outcome, result.ackErr); err != nil {
			publishErrors = append(publishErrors, err)
		}
		if result.outcome == durableAcked {
			publishErrors = append(publishErrors, fmt.Errorf("durable publish %s received ACK without quorum", result.publisher.topic))
		}
		if result.publisher.qos == 2 {
			state = quorumFaultQoS2State{
				packetID: result.packetID, pubrecReceived: result.publisher.client.pubrecReceived[result.packetID],
				topic: result.publisher.topic,
			}
		}
	}
	return state, errors.Join(publishErrors...)
}

func (scenario *storageFaultScenario) recordDurableAck(nodeID, clientID, topic string, qos byte, phase string, outcome durableAckOutcome, ackErr error) error {
	if scenario.evidence.DurableAck == nil {
		scenario.evidence.DurableAck = &durableAckEvidence{}
	}
	if outcome == durableAcked {
		switch phase {
		case "before-fault":
			scenario.evidence.DurableAck.AckedBeforeFault = append(scenario.evidence.DurableAck.AckedBeforeFault, topic)
		case "during-fault":
			scenario.evidence.DurableAck.AckedDuringFault = append(scenario.evidence.DurableAck.AckedDuringFault, topic)
		case "after-recovery":
			scenario.evidence.DurableAck.AckedAfterRecovery = append(scenario.evidence.DurableAck.AckedAfterRecovery, topic)
		}
	} else if phase == "during-fault" {
		scenario.evidence.DurableAck.UnackedDuringFault = append(scenario.evidence.DurableAck.UnackedDuringFault, topic)
		if outcome == durableUnknown {
			scenario.evidence.DurableAck.AckOutcomeUnknownDuringFault = append(scenario.evidence.DurableAck.AckOutcomeUnknownDuringFault, topic)
		}
	}
	return scenario.recorder.record("durable-ack", nodeID, map[string]any{
		"clientId": clientID, "topic": topic, "qos": qos, "phase": phase,
		"outcome": outcome, "error": errorString(ackErr),
	})
}

func (scenario *storageFaultScenario) verifyDurableDelivery(ctx context.Context, leaderID string, expectedTopics, atMostOnceTopics []string) error {
	evidence := scenario.evidence.DurableAck
	if evidence == nil || scenario.durableSubscriber == nil {
		return errors.New("durable ACK subscriber was not prepared")
	}
	sentinelTopic := durableTopic(scenario.options.ScenarioID, "sentinel", "barrier")
	if err := scenario.durablePublish(ctx, leaderID, "chaos-durable-sentinel", sentinelTopic, 1, "sentinel", true); err != nil {
		return fmt.Errorf("publish durable delivery sentinel: %w", err)
	}
	expected := make(map[string]bool, len(expectedTopics))
	allowed := make(map[string]bool, len(expectedTopics)+len(atMostOnceTopics)+1)
	for _, topic := range expectedTopics {
		expected[topic] = true
		allowed[topic] = true
	}
	for _, topic := range atMostOnceTopics {
		allowed[topic] = true
	}
	allowed[sentinelTopic] = true
	counts := make(map[string]int, len(allowed))
	pendingQoS2 := make(map[uint16]bool)
	deadline := time.Now().Add(durableDeliveryTimeout)
	quietUntil := time.Time{}
	sentinelReceived := false
	for {
		allExpectedReceived := true
		for topic := range expected {
			if counts[topic] == 0 {
				allExpectedReceived = false
				break
			}
		}
		if allExpectedReceived && sentinelReceived && len(pendingQoS2) == 0 {
			if quietUntil.IsZero() {
				quietUntil = time.Now().Add(durableDeliveryQuietFor)
			}
			if !time.Now().Before(quietUntil) {
				break
			}
		}
		readDeadline := deadline
		if !quietUntil.IsZero() && allExpectedReceived && sentinelReceived && len(pendingQoS2) == 0 && quietUntil.Before(readDeadline) {
			readDeadline = quietUntil
		}
		if !time.Now().Before(readDeadline) {
			break
		}
		readCtx, cancel := context.WithDeadline(ctx, readDeadline)
		header, body, err := readMQTTPacket(scenario.durableSubscriber.connection, readCtx)
		cancel()
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) || ctx.Err() != nil || isNetworkTimeout(err) {
				break
			}
			return fmt.Errorf("read durable subscriber packet: %w", err)
		}
		if header&0xf0 == 0x30 {
			message, err := parseDurablePublish(header, body)
			if err != nil {
				return err
			}
			if !strings.HasPrefix(message.topic, "chaos/durable/") || !allowed[message.topic] {
				return fmt.Errorf("unexpected durable delivery topic %q", message.topic)
			}
			if string(message.payload) != message.topic {
				return fmt.Errorf("durable delivery for topic %q carried payload %q", message.topic, message.payload)
			}
			counts[message.topic]++
			if message.topic == sentinelTopic {
				sentinelReceived = true
			}
			if err := scenario.recorder.record("durable-delivery", "", map[string]any{
				"topic": message.topic, "qos": message.qos, "duplicate": header&0x08 != 0,
				"packetId": message.packetID, "payload": string(message.payload),
			}); err != nil {
				return err
			}
			switch message.qos {
			case 0:
			case 1:
				if err := writeMQTTPacket(scenario.durableSubscriber.connection, ctx, mqttControlPacket(0x40, message.packetID)); err != nil {
					return err
				}
			case 2:
				pendingQoS2[message.packetID] = true
				if err := writeMQTTPacket(scenario.durableSubscriber.connection, ctx, mqttControlPacket(0x50, message.packetID)); err != nil {
					return err
				}
			}
			continue
		}
		if header == 0x62 && len(body) == 2 {
			packetID := binary.BigEndian.Uint16(body)
			delete(pendingQoS2, packetID)
			if err := writeMQTTPacket(scenario.durableSubscriber.connection, ctx, mqttControlPacket(0x70, packetID)); err != nil {
				return err
			}
			continue
		}
	}

	evidence.MissingAcked = evidence.MissingAcked[:0]
	evidence.Duplicates = evidence.Duplicates[:0]
	for topic := range expected {
		if counts[topic] == 0 {
			evidence.MissingAcked = append(evidence.MissingAcked, topic)
		}
		if counts[topic] > 1 {
			evidence.Duplicates = append(evidence.Duplicates, topic)
		}
	}
	for _, topic := range atMostOnceTopics {
		if counts[topic] > 1 {
			evidence.Duplicates = append(evidence.Duplicates, topic)
		}
	}
	if counts[sentinelTopic] > 1 {
		evidence.Duplicates = append(evidence.Duplicates, sentinelTopic)
	}
	sort.Strings(evidence.MissingAcked)
	sort.Strings(evidence.Duplicates)
	if !sentinelReceived || len(pendingQoS2) != 0 {
		return fmt.Errorf("durable delivery barrier incomplete: sentinelReceived=%t pendingQoS2=%v missing=%v duplicates=%v",
			sentinelReceived, pendingQoS2, evidence.MissingAcked, evidence.Duplicates)
	}
	if len(evidence.MissingAcked) != 0 || len(evidence.Duplicates) != 0 {
		return fmt.Errorf("durable delivery mismatch: missing ACKed topics=%v duplicate topics=%v", evidence.MissingAcked, evidence.Duplicates)
	}
	return nil
}

func isNetworkTimeout(err error) bool {
	var networkError net.Error
	return errors.As(err, &networkError) && networkError.Timeout()
}

func parseDurableQoS2Delivery(header byte, body []byte) (durableMessage, error) {
	message, err := parseDurablePublish(header, body)
	if err != nil {
		return durableMessage{}, err
	}
	if message.qos != 2 {
		return durableMessage{}, fmt.Errorf("delivery QoS = %d, want 2", message.qos)
	}
	return message, nil
}
