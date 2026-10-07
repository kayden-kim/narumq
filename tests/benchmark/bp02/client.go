package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	"narumq/internal/domain/mqtt"
	"narumq/tests/benchmark/internal/mqttclient"
)

type publisherPending struct {
	sequence         uint64
	sentAt, deadline time.Time
	pubrecSeen       bool
}

type inboundMessage struct {
	stream   int
	sequence uint64
	topic    string
}

type runClient struct {
	spec                  clientSpec
	client                *mqttclient.Client
	done, stop            chan struct{}
	changed               chan struct{}
	stopOnce, closeOnce   sync.Once
	mu                    sync.Mutex
	readFailure, closeErr error
	pending               map[mqtt.PacketIdentifier]publisherPending
	nextPacketID          mqtt.PacketIdentifier
	inbound               map[mqtt.PacketIdentifier]inboundMessage
	completed             map[mqtt.PacketIdentifier]bool
}

func loadBP02TLSConfig(creds bp02RunnerCredentials) (*tls.Config, error) {
	pemBytes, err := os.ReadFile(creds.CAFile)
	if err != nil {
		return nil, fmt.Errorf("read runner CA: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(pemBytes) {
		return nil, errors.New("runner CA contains no certificates")
	}
	return &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}, nil
}

func connectClient(ctx context.Context, spec clientSpec, address string, profile Profile, creds bp02RunnerCredentials, tlsConfig *tls.Config, cleanStart bool) (*runClient, mqttclient.ConnectTiming, error) {
	credential, ok := creds.Tenants[spec.tenant]
	if !ok || credential.Username == "" || credential.Password == "" {
		return nil, mqttclient.ConnectTiming{}, fmt.Errorf("missing credential for tenant %s", spec.tenant)
	}
	options := mqttclient.ConnectOptions{Address: address, ClientID: spec.id, ProtocolVersion: mqtt.ProtocolV5,
		KeepAlive: profile.KeepAliveSeconds, HandshakeTimeout: 5 * time.Second, CleanStart: true,
		Username: credential.Username, Password: []byte(credential.Password)}
	if spec.persistent {
		options.CleanStart = cleanStart
		options.SessionExpiryInterval = profile.SessionExpirySeconds
	}
	if spec.tls {
		if tlsConfig == nil {
			return nil, mqttclient.ConnectTiming{}, errors.New("TLS client requires CA configuration")
		}
		host, _, err := net.SplitHostPort(address)
		if err != nil {
			return nil, mqttclient.ConnectTiming{}, err
		}
		options.TLSConfig = tlsConfig.Clone()
		options.TLSConfig.ServerName = host
	}
	client, timing, err := mqttclient.Connect(ctx, options)
	if err != nil {
		return nil, timing, err
	}
	return &runClient{spec: spec, client: client, stop: make(chan struct{}), changed: make(chan struct{}, 1),
		pending: make(map[mqtt.PacketIdentifier]publisherPending, profile.MaxInflightPerPublisher),
		inbound: make(map[mqtt.PacketIdentifier]inboundMessage), completed: make(map[mqtt.PacketIdentifier]bool)}, timing, nil
}

func subscribeClient(client *runClient, profile Profile, col *collector) (sentAt, ackedAt time.Time, reasons []byte, err error) {
	request := &mqtt.Subscribe{Version: mqtt.ProtocolV5, PacketID: 1,
		Subscriptions: []mqtt.Subscription{{Filter: client.spec.filter, QoS: mqtt.QoS(client.spec.subscribeQoS)}}}
	sentAt = time.Now()
	if client.spec.role == "retained-subscriber" {
		col.recordRetainedStart(client.spec, sentAt)
	}
	if err = client.client.Write(request, 5*time.Second); err != nil {
		return
	}
	deadline := sentAt.Add(5 * time.Second)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			err = errors.New("SUBACK timed out")
			return
		}
		var packet mqtt.Packet
		packet, err = client.client.Read(remaining)
		if err != nil {
			return
		}
		if ack, ok := packet.(*mqtt.SubAck); ok {
			ackedAt = time.Now()
			reasons = append([]byte(nil), ack.ReasonCodes...)
			if ack.PacketID != 1 || len(reasons) != 1 || reasons[0] != client.spec.subscribeQoS {
				err = fmt.Errorf("unexpected SUBACK %#v", ack)
			}
			return
		}
		// MQTT allows retained delivery before SUBACK.
		var response mqtt.Packet
		response, err = client.processInbound(profile, col, packet, time.Now())
		if err != nil {
			return
		}
		if response != nil {
			if err = client.client.Write(response, time.Second); err != nil {
				return
			}
		}
	}
}

func (client *runClient) processInbound(profile Profile, col *collector, packet mqtt.Packet, at time.Time) (mqtt.Packet, error) {
	switch value := packet.(type) {
	case *mqtt.PingResp:
		return nil, nil
	case *mqtt.Publish:
		if client.spec.role != "subscriber" && client.spec.role != "retained-subscriber" {
			return nil, errors.New("non-subscriber received PUBLISH")
		}
		sequence, stream, err := validateStreamPayload(profile, value.Payload)
		if err != nil {
			return nil, err
		}
		if value.QoS == mqtt.QoSExactlyOnce {
			if pending, ok := client.inbound[value.PacketID]; ok {
				if pending.stream != stream || pending.sequence != sequence || pending.topic != value.Topic {
					return nil, errors.New("QoS 2 packet ID reused before PUBREL")
				}
				return &mqtt.PubRec{Version: mqtt.ProtocolV5, PacketID: value.PacketID}, nil
			}
		}
		col.recordDelivery(client.spec, stream, sequence, value.Topic, byte(value.QoS), value.Retain, at)
		if stream != client.spec.stream || !topicMatches(client.spec.filter, value.Topic) || value.QoS != mqtt.QoS(bp02Streams[stream].qos) {
			return nil, errors.New("delivery stream, filter or QoS mismatch")
		}
		if client.spec.role != "retained-subscriber" && value.Retain {
			return nil, errors.New("normal subscription received retained PUBLISH")
		}
		if value.QoS == mqtt.QoSAtMostOnce {
			if value.DUP {
				return nil, errors.New("QoS 0 delivery has DUP flag")
			}
			return nil, nil
		}
		if value.PacketID == 0 {
			return nil, errors.New("QoS 1/2 delivery has zero packet ID")
		}
		if value.QoS == mqtt.QoSAtLeastOnce {
			return &mqtt.PubAck{Version: mqtt.ProtocolV5, PacketID: value.PacketID}, nil
		}
		delete(client.completed, value.PacketID)
		client.inbound[value.PacketID] = inboundMessage{stream: stream, sequence: sequence, topic: value.Topic}
		return &mqtt.PubRec{Version: mqtt.ProtocolV5, PacketID: value.PacketID}, nil
	case *mqtt.PubRel:
		if value.ReasonCode >= 0x80 {
			return nil, fmt.Errorf("PUBREL reason %#x", value.ReasonCode)
		}
		if _, ok := client.inbound[value.PacketID]; ok {
			delete(client.inbound, value.PacketID)
			client.completed[value.PacketID] = true
		} else if !client.completed[value.PacketID] {
			return nil, errors.New("PUBREL for unknown packet ID")
		}
		return &mqtt.PubComp{Version: mqtt.ProtocolV5, PacketID: value.PacketID}, nil
	default:
		return nil, fmt.Errorf("unexpected subscriber packet %T", packet)
	}
}

func (client *runClient) startReader(profile Profile, col *collector, cancel context.CancelCauseFunc) {
	client.done = make(chan struct{})
	go func() {
		defer close(client.done)
		defer client.signal()
		for {
			if client.spec.role == "publisher" && client.expirePublisherAcks(col, time.Now(), cancel) {
				return
			}
			packet, err := client.client.Read(20 * time.Millisecond)
			if err != nil {
				if mqttclient.IsTimeout(err) {
					continue
				}
				select {
				case <-client.stop:
					return
				default:
				}
				client.failRead(col, cancel, fmt.Errorf("client %s read: %w", client.spec.id, err), false)
				return
			}
			if client.spec.role == "publisher" {
				err = client.processPublisherAck(col, packet, time.Now())
			} else {
				var response mqtt.Packet
				response, err = client.processInbound(profile, col, packet, time.Now())
				if err == nil && response != nil {
					if writeErr := client.client.Write(response, time.Second); writeErr != nil {
						client.failRead(col, cancel, writeErr, false)
						return
					}
				}
			}
			if err != nil {
				client.failRead(col, cancel, fmt.Errorf("client %s packet: %w", client.spec.id, err), true)
				return
			}
		}
	}()
}

func (client *runClient) processPublisherAck(col *collector, packet mqtt.Packet, at time.Time) error {
	if _, ok := packet.(*mqtt.PingResp); ok {
		return nil
	}
	var id mqtt.PacketIdentifier
	var reason byte
	var kind byte
	switch ack := packet.(type) {
	case *mqtt.PubAck:
		id, reason, kind = ack.PacketID, ack.ReasonCode, 1
	case *mqtt.PubRec:
		id, reason, kind = ack.PacketID, ack.ReasonCode, 2
	case *mqtt.PubComp:
		id, reason, kind = ack.PacketID, ack.ReasonCode, 3
	default:
		return fmt.Errorf("unexpected publisher packet %T", packet)
	}
	client.mu.Lock()
	pending, ok := client.pending[id]
	if !ok {
		client.mu.Unlock()
		return fmt.Errorf("ACK for unknown packet ID %d", id)
	}
	if !at.Before(pending.deadline) {
		delete(client.pending, id)
		client.mu.Unlock()
		col.ackTimeout(client.spec.stream, pending.sequence)
		client.signal()
		return errors.New("publisher ACK deadline exceeded")
	}
	qos := bp02Streams[client.spec.stream].qos
	if reason >= 0x80 || (qos == 1 && kind != 1) || (qos == 2 && kind == 1) || (kind == 3 && !pending.pubrecSeen) {
		client.mu.Unlock()
		return fmt.Errorf("invalid publisher ACK kind=%d reason=%#x", kind, reason)
	}
	if kind == 2 {
		pending.pubrecSeen = true
		client.pending[id] = pending
		client.mu.Unlock()
		return client.client.Write(&mqtt.PubRel{Version: mqtt.ProtocolV5, PacketID: id}, time.Second)
	}
	delete(client.pending, id)
	client.mu.Unlock()
	col.recordAck(client.spec.stream, pending.sequence, at)
	client.signal()
	return nil
}

func (client *runClient) expirePublisherAcks(col *collector, now time.Time, cancel context.CancelCauseFunc) bool {
	client.mu.Lock()
	var expired []publisherPending
	for id, pending := range client.pending {
		if !now.Before(pending.deadline) {
			expired = append(expired, pending)
			delete(client.pending, id)
		}
	}
	client.mu.Unlock()
	if len(expired) == 0 {
		return false
	}
	for _, pending := range expired {
		col.ackTimeout(client.spec.stream, pending.sequence)
	}
	client.signal()
	client.failRead(col, cancel, fmt.Errorf("publisher %s ACK timeout sequence %d", client.spec.id, expired[0].sequence), false)
	return true
}

func (client *runClient) publish(ctx context.Context, profile Profile, col *collector, sequence uint64, step int, phase, topic string, scheduled time.Time, retain bool) error {
	qos := bp02Streams[client.spec.stream].qos
	var id mqtt.PacketIdentifier
	if qos > 0 {
		for {
			client.mu.Lock()
			failure := client.readFailure
			if failure != nil {
				client.mu.Unlock()
				return failure
			}
			if len(client.pending) < profile.MaxInflightPerPublisher {
				for {
					client.nextPacketID++
					if client.nextPacketID == 0 {
						client.nextPacketID = 1
					}
					if _, exists := client.pending[client.nextPacketID]; !exists {
						id = client.nextPacketID
						break
					}
				}
				client.mu.Unlock()
				break
			}
			client.mu.Unlock()
			select {
			case <-ctx.Done():
				return context.Cause(ctx)
			case <-client.changed:
			}
		}
	}
	if err := col.addMessage(client.spec.stream, sequence, step, phase, topic, scheduled); err != nil {
		return err
	}
	payload := makePayload(profile, client.spec.stream, sequence)
	sentAt := time.Now()
	col.markSent(client.spec.stream, sequence, sentAt)
	if step > 0 {
		col.recordScheduleLag(step, sentAt.Sub(scheduled), time.Second/time.Duration(profile.RatesPerStreamPerSecond[step-1]))
	}
	if qos > 0 {
		client.mu.Lock()
		client.pending[id] = publisherPending{sequence: sequence, sentAt: sentAt, deadline: sentAt.Add(time.Duration(profile.AckTimeoutMillis) * time.Millisecond)}
		client.mu.Unlock()
	}
	if err := client.client.Write(&mqtt.Publish{Version: mqtt.ProtocolV5, QoS: mqtt.QoS(qos), PacketID: id, Topic: topic, Payload: payload, Retain: retain}, 5*time.Second); err != nil {
		col.transportError()
		return fmt.Errorf("publish %s sequence %d: %w", client.spec.id, sequence, err)
	}
	return nil
}

func (client *runClient) waitForAcks(ctx context.Context) error {
	for {
		client.mu.Lock()
		count, failure := len(client.pending), client.readFailure
		client.mu.Unlock()
		if failure != nil {
			return failure
		}
		if count == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-client.changed:
		}
	}
}

func (client *runClient) signal() {
	select {
	case client.changed <- struct{}{}:
	default:
	}
}
func (client *runClient) failRead(col *collector, cancel context.CancelCauseFunc, err error, protocol bool) {
	client.mu.Lock()
	first := client.readFailure == nil
	if first {
		client.readFailure = err
	}
	failure := client.readFailure
	client.mu.Unlock()
	if first {
		if protocol {
			col.protocolError()
		} else {
			col.transportError()
		}
	}
	client.signal()
	cancel(failure)
}
func (client *runClient) failure() error {
	client.mu.Lock()
	defer client.mu.Unlock()
	return client.readFailure
}
func (client *runClient) disconnectAndClose(timeout time.Duration) error {
	if client == nil {
		return nil
	}
	client.closeOnce.Do(func() {
		client.stopOnce.Do(func() { close(client.stop) })
		if client.client != nil {
			client.closeErr = errors.Join(client.client.Disconnect(timeout), client.client.Close())
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
			errs = append(errs, fmt.Errorf("client %s cleanup: %w", client.spec.id, err))
		}
	}
	return errors.Join(errs...)
}
func startKeepAlive(ctx context.Context, clients []*runClient, interval time.Duration, col *collector, cancel context.CancelCauseFunc) func() {
	keepCtx, stop := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-keepCtx.Done():
				return
			case <-ticker.C:
				for _, client := range clients {
					select {
					case <-client.stop:
						continue
					default:
					}
					if err := client.client.Write(&mqtt.PingReq{Version: mqtt.ProtocolV5}, time.Second); err != nil {
						client.failRead(col, cancel, err, false)
						return
					}
				}
			}
		}
	}()
	return func() { stop(); <-done }
}
