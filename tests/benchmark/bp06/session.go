package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"narumq/internal/domain/mqtt"
	"narumq/tests/benchmark/internal/jsonl"
	"narumq/tests/benchmark/internal/mqttclient"
)

type pendingPublish struct {
	sequence uint64
	deadline time.Time
	pubrec   bool
	timedOut bool
}

type sessionEvent struct {
	ClientID               string  `json:"client"`
	Role                   string  `json:"role"`
	Event                  string  `json:"event"`
	Timestamp              string  `json:"at"`
	Generation             uint64  `json:"generation,omitempty"`
	Node                   string  `json:"node,omitempty"`
	Round                  int     `json:"round,omitempty"`
	Reason                 string  `json:"reason,omitempty"`
	ReasonCode             *byte   `json:"reasonCode,omitempty"`
	ServerReference        string  `json:"serverReference,omitempty"`
	SessionPresent         bool    `json:"sessionPresent"`
	ReconnectLatencyMillis float64 `json:"reconnectLatencyMillis,omitempty"`
	Detail                 string  `json:"detail,omitempty"`
}

type sessionClient struct {
	spec    clientSpec
	index   int
	profile Profile
	creds   credentials
	dialer  *readinessDialer
	col     *collector
	round   *currentRound
	events  *jsonl.Writer

	mu                  sync.Mutex
	current             *mqttclient.Client
	node                string
	generation          uint64
	connectSequence     uint64
	nextPacketID        uint32
	pending             map[mqtt.PacketIdentifier]pendingPublish
	subscriberPending   map[mqtt.PacketIdentifier]uint64
	subscriberCompleted map[mqtt.PacketIdentifier]bool
	nextPing            time.Time
	disconnectedAt      time.Time
	reconnectDeadline   time.Time
	reconnectRound      int
	retryBackoff        time.Duration
	ctx                 context.Context
	cancel              context.CancelFunc
	done                chan struct{}
	started             bool
	closing             bool
	closeOnce           sync.Once
	closeErr            error
	random              *rand.Rand
}

func newSessionClient(spec clientSpec, index int, profile Profile, creds credentials, dialer *readinessDialer, col *collector, round *currentRound, events *jsonl.Writer) *sessionClient {
	return &sessionClient{
		spec:                spec,
		index:               index,
		profile:             profile,
		creds:               creds,
		dialer:              dialer,
		col:                 col,
		round:               round,
		events:              events,
		nextPacketID:        1,
		pending:             make(map[mqtt.PacketIdentifier]pendingPublish),
		subscriberPending:   make(map[mqtt.PacketIdentifier]uint64),
		subscriberCompleted: make(map[mqtt.PacketIdentifier]bool),
		retryBackoff:        100 * time.Millisecond,
		random:              rand.New(rand.NewPCG(profile.Seed, uint64(index))),
	}
}

func (s *sessionClient) start(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return errors.New("session client already started")
	}
	s.started = true
	s.ctx, s.cancel = context.WithCancel(ctx)
	generation := s.nextConnectSequenceLocked()
	s.mu.Unlock()

	client, timing, node, err := s.connect(s.ctx, true)
	if err != nil {
		s.cancel()
		return fmt.Errorf("connect %s: %w", s.spec.id, err)
	}
	if s.spec.persistent && timing.SessionPresent && !s.initialConnectAllowsPresent() {
		// Initial MQTT 5 sessions are clean-started; MQTT 3.1.1 persistent clients
		// may encounter a session retained from an earlier run.
		s.col.recordSessionPresentFailure(0)
	}
	if s.needsSubscription() {
		if err := s.subscribe(client); err != nil {
			_ = client.Close()
			s.cancel()
			return fmt.Errorf("subscribe %s: %w", s.spec.id, err)
		}
	}
	s.mu.Lock()
	s.current = client
	s.node = node
	s.generation = generation
	s.nextPing = time.Now().Add(s.pingInterval())
	s.mu.Unlock()
	s.col.recordInitialNode(node)
	s.writeEvent(sessionEvent{
		ClientID: s.spec.id, Role: s.spec.role, Event: "connect",
		Timestamp: timing.ConnAckAt.UTC().Format(time.RFC3339Nano), Generation: generation, Node: node, SessionPresent: timing.SessionPresent,
	})
	s.done = make(chan struct{})
	go s.run()
	return nil
}

func (s *sessionClient) initialConnectAllowsPresent() bool {
	return s.spec.version == mqtt.ProtocolV311
}

func (s *sessionClient) connect(ctx context.Context, initial bool) (*mqttclient.Client, mqttclient.ConnectTiming, string, error) {
	options := s.connectOptions(initial)
	return s.dialer.Dial(ctx, options)
}

func (s *sessionClient) connectOptions(initial bool) mqttclient.ConnectOptions {
	cleanStart := !s.spec.persistent
	if s.spec.version == mqtt.ProtocolV5 && s.spec.persistent {
		cleanStart = initial
	}
	expiry := uint32(0)
	if s.spec.version == mqtt.ProtocolV5 && s.spec.persistent {
		expiry = s.profile.SessionExpirySeconds
	}
	return mqttclient.ConnectOptions{
		ClientID:              s.spec.id,
		ProtocolVersion:       s.spec.version,
		KeepAlive:             s.profile.KeepAliveSeconds,
		HandshakeTimeout:      5 * time.Second,
		CleanStart:            cleanStart,
		SessionExpiryInterval: expiry,
		Username:              s.creds.Username,
		Password:              []byte(s.creds.Password),
	}
}

func (s *sessionClient) needsSubscription() bool {
	return s.spec.role == "subscriber"
}

func (s *sessionClient) subscribe(client *mqttclient.Client) error {
	qos := mqtt.QoS(s.spec.qos)
	if err := client.Write(&mqtt.Subscribe{
		Version:       s.spec.version,
		PacketID:      1,
		Subscriptions: []mqtt.Subscription{{Filter: s.spec.topic, QoS: qos}},
	}, 5*time.Second); err != nil {
		return err
	}
	packet, err := client.Read(5 * time.Second)
	if err != nil {
		return err
	}
	ack, ok := packet.(*mqtt.SubAck)
	if !ok || ack.Version != s.spec.version || ack.PacketID != 1 || len(ack.ReasonCodes) != 1 || ack.ReasonCodes[0] != byte(qos) {
		return fmt.Errorf("unexpected SUBACK for %s: %#v", s.spec.id, packet)
	}
	return nil
}

func (s *sessionClient) run() {
	defer close(s.done)
	for {
		s.mu.Lock()
		if s.closing || s.ctx.Err() != nil {
			s.mu.Unlock()
			return
		}
		client := s.current
		s.mu.Unlock()
		if client == nil {
			if !s.reconnect() {
				return
			}
			continue
		}

		packet, err := client.Read(s.readTimeout())
		if err != nil {
			if mqttclient.IsTimeout(err) {
				s.expireAcknowledgements(time.Now())
				s.sendPingIfDue(client)
				continue
			}
			reason := disconnectReason(err)
			if errors.Is(err, mqtt.ErrMalformedPacket) {
				s.col.protocolError(fmt.Sprintf("malformed packet from %s: %v", s.spec.id, err))
				reason = "protocol_error"
			}
			s.disconnect(client, reason, nil, "", err)
			continue
		}
		s.expireAcknowledgements(time.Now())
		if value, ok := packet.(*mqtt.Disconnect); ok {
			reason := fmt.Sprintf("0x%02X", value.ReasonCode)
			if value.Version == mqtt.ProtocolV311 {
				reason = "eof"
			}
			code := value.ReasonCode
			s.disconnect(client, reason, &code, value.ServerReference, nil)
			continue
		}
		if failure := s.handlePacket(client, packet); failure != nil {
			if failure.protocol && !failure.counted {
				s.col.protocolError(failure.err.Error())
			}
			reason := "write_error"
			if failure.protocol {
				reason = "protocol_error"
			}
			s.disconnect(client, reason, nil, "", failure.err)
		}
		s.expireAcknowledgements(time.Now())
		s.sendPingIfDue(client)
	}
}

type packetFailure struct {
	err      error
	protocol bool
	counted  bool
}

func (s *sessionClient) handlePacket(client *mqttclient.Client, packet mqtt.Packet) *packetFailure {
	s.mu.Lock()
	defer s.mu.Unlock()
	if client != s.current || s.closing {
		return nil
	}
	switch value := packet.(type) {
	case *mqtt.PingResp:
		return nil
	case *mqtt.Publish:
		return s.handleInboundLocked(client, value)
	case *mqtt.PubRel:
		if value.Version != s.spec.version {
			return s.protocolFailure(fmt.Errorf("PUBREL protocol version mismatch for %s", s.spec.id))
		}
		_, pending := s.subscriberPending[value.PacketID]
		if pending {
			delete(s.subscriberPending, value.PacketID)
			s.subscriberCompleted[value.PacketID] = true
		} else if !s.subscriberCompleted[value.PacketID] {
			return s.protocolFailure(fmt.Errorf("PUBREL for unknown packet ID %d on %s", value.PacketID, s.spec.id))
		}
		if err := client.Write(&mqtt.PubComp{Version: s.spec.version, PacketID: value.PacketID}, time.Second); err != nil {
			return &packetFailure{err: fmt.Errorf("write PUBCOMP for %s: %w", s.spec.id, err)}
		}
		return nil
	case *mqtt.PubAck:
		return s.handlePubAckLocked(client, value)
	case *mqtt.PubRec:
		return s.handlePubRecLocked(client, value)
	case *mqtt.PubComp:
		return s.handlePubCompLocked(value)
	default:
		return s.protocolFailure(fmt.Errorf("unexpected packet %T for %s", packet, s.spec.id))
	}
}

func (s *sessionClient) handleInboundLocked(client *mqttclient.Client, packet *mqtt.Publish) *packetFailure {
	if packet.Version != s.spec.version || int(packet.QoS) != s.spec.qos || packet.Retain {
		return s.protocolFailure(fmt.Errorf("unexpected PUBLISH version, QoS, or retain flag for %s", s.spec.id))
	}
	if s.spec.role == "subscriber" {
		sequence, err := validateStreamPayload(s.profile, s.spec.index, packet.Payload)
		if err != nil {
			return s.protocolFailure(fmt.Errorf("subscriber %s payload: %w", s.spec.id, err))
		}
		if packet.Topic != s.spec.topic {
			return s.protocolFailure(fmt.Errorf("subscriber %s received topic %q, want %q", s.spec.id, packet.Topic, s.spec.topic))
		}
		if packet.QoS == mqtt.QoSExactlyOnce {
			if previous, pending := s.subscriberPending[packet.PacketID]; pending {
				if !packet.DUP || previous != sequence {
					return s.protocolFailure(fmt.Errorf("invalid duplicate QoS 2 PUBLISH for packet ID %d on %s", packet.PacketID, s.spec.id))
				}
				if err := client.Write(&mqtt.PubRec{Version: s.spec.version, PacketID: packet.PacketID}, time.Second); err != nil {
					return &packetFailure{err: fmt.Errorf("write repeated PUBREC for %s: %w", s.spec.id, err)}
				}
				return nil
			}
			delete(s.subscriberCompleted, packet.PacketID)
		}
		if _, err := s.col.receive(s.spec.stream, sequence, time.Now()); err != nil {
			return s.protocolFailure(err)
		}
		switch packet.QoS {
		case mqtt.QoSAtMostOnce:
			return nil
		case mqtt.QoSAtLeastOnce:
			if err := client.Write(&mqtt.PubAck{Version: s.spec.version, PacketID: packet.PacketID}, time.Second); err != nil {
				return &packetFailure{err: fmt.Errorf("write PUBACK for %s: %w", s.spec.id, err)}
			}
			return nil
		case mqtt.QoSExactlyOnce:
			s.subscriberPending[packet.PacketID] = sequence
			if err := client.Write(&mqtt.PubRec{Version: s.spec.version, PacketID: packet.PacketID}, time.Second); err != nil {
				return &packetFailure{err: fmt.Errorf("write PUBREC for %s: %w", s.spec.id, err)}
			}
			return nil
		default:
			return s.protocolFailure(fmt.Errorf("unsupported delivered QoS %d", packet.QoS))
		}
	}
	return s.protocolFailure(fmt.Errorf("client %s received PUBLISH in role %q", s.spec.id, s.spec.role))
}

func (s *sessionClient) handlePubAckLocked(client *mqttclient.Client, packet *mqtt.PubAck) *packetFailure {
	if packet.Version != s.spec.version || s.spec.role != "publisher" || s.spec.qos != 1 {
		return s.protocolFailure(fmt.Errorf("unexpected PUBACK for %s", s.spec.id))
	}
	pending, ok := s.pending[packet.PacketID]
	if !ok {
		return s.protocolFailure(fmt.Errorf("PUBACK for unknown packet ID %d on %s", packet.PacketID, s.spec.id))
	}
	delete(s.pending, packet.PacketID)
	if !pending.timedOut {
		at := time.Now()
		s.col.publishAck(s.spec.stream, pending.sequence, at, packet.ReasonCode, true)
	}
	return nil
}

func (s *sessionClient) handlePubRecLocked(client *mqttclient.Client, packet *mqtt.PubRec) *packetFailure {
	if packet.Version != s.spec.version || s.spec.role != "publisher" || s.spec.qos != 2 {
		return s.protocolFailure(fmt.Errorf("unexpected PUBREC for %s", s.spec.id))
	}
	pending, ok := s.pending[packet.PacketID]
	if !ok {
		return s.protocolFailure(fmt.Errorf("PUBREC for unknown packet ID %d on %s", packet.PacketID, s.spec.id))
	}
	if packet.ReasonCode >= 0x80 {
		delete(s.pending, packet.PacketID)
		if !pending.timedOut {
			s.col.publishAck(s.spec.stream, pending.sequence, time.Now(), packet.ReasonCode, true)
		}
		return nil
	}
	if !pending.timedOut {
		s.col.publishAck(s.spec.stream, pending.sequence, time.Now(), packet.ReasonCode, false)
	}
	pending.pubrec = true
	pending.deadline = time.Now().Add(time.Duration(s.profile.AckTimeoutMillis) * time.Millisecond)
	s.pending[packet.PacketID] = pending
	if err := client.Write(&mqtt.PubRel{Version: s.spec.version, PacketID: packet.PacketID}, time.Second); err != nil {
		return &packetFailure{err: fmt.Errorf("write PUBREL for %s: %w", s.spec.id, err)}
	}
	return nil
}

func (s *sessionClient) handlePubCompLocked(packet *mqtt.PubComp) *packetFailure {
	if packet.Version != s.spec.version || s.spec.role != "publisher" || s.spec.qos != 2 {
		return s.protocolFailure(fmt.Errorf("unexpected PUBCOMP for %s", s.spec.id))
	}
	pending, ok := s.pending[packet.PacketID]
	if !ok || !pending.pubrec {
		return s.protocolFailure(fmt.Errorf("PUBCOMP before PUBREC or for unknown packet ID %d on %s", packet.PacketID, s.spec.id))
	}
	delete(s.pending, packet.PacketID)
	if !pending.timedOut {
		s.col.publishAck(s.spec.stream, pending.sequence, time.Now(), packet.ReasonCode, true)
	}
	return nil
}

func (s *sessionClient) protocolFailure(err error) *packetFailure {
	return &packetFailure{err: err, protocol: true}
}

func (s *sessionClient) disconnect(client *mqttclient.Client, reason string, reasonCode *byte, serverReference string, cause error) (bool, int) {
	s.mu.Lock()
	if client == nil || client != s.current || s.closing {
		s.mu.Unlock()
		return false, 0
	}
	node := s.node
	generation := s.generation
	at := time.Now()
	marker := s.round.snapshot()
	roundNumber, _ := s.col.recordDisconnect(s.spec.id, node, marker, reason, serverReference, at)
	for packetID, pending := range s.pending {
		if !pending.timedOut {
			s.col.publishInterrupted(s.spec.stream, pending.sequence)
		}
		delete(s.pending, packetID)
	}
	s.current = nil
	s.node = ""
	s.disconnectedAt = at
	s.reconnectDeadline = at.Add(time.Duration(s.profile.ReconnectTimeoutMillis) * time.Millisecond)
	s.reconnectRound = roundNumber
	s.retryBackoff = 100 * time.Millisecond
	s.mu.Unlock()

	_ = client.Close()
	detail := ""
	if cause != nil {
		detail = cause.Error()
	}
	s.writeEvent(sessionEvent{
		ClientID: s.spec.id, Role: s.spec.role, Event: "disconnect",
		Timestamp: at.UTC().Format(time.RFC3339Nano), Generation: generation,
		Node: node, Round: roundNumber, Reason: reason, ReasonCode: reasonCode,
		ServerReference: serverReference, Detail: detail,
	})
	return true, roundNumber
}

func (s *sessionClient) reconnect() bool {
	s.mu.Lock()
	if s.closing || s.ctx.Err() != nil {
		s.mu.Unlock()
		return false
	}
	disconnectedAt := s.disconnectedAt
	deadline := s.reconnectDeadline
	roundNumber := s.reconnectRound
	backoff := s.retryBackoff
	s.mu.Unlock()
	if disconnectedAt.IsZero() {
		s.col.failure("session entered reconnect without a disconnect timestamp")
		return false
	}
	if !time.Now().Before(deadline) {
		s.reconnectTimedOut(roundNumber, deadline)
		return false
	}
	jitter := time.Duration(s.random.Int64N(int64(backoff) + 1))
	if err := waitForContext(s.ctx, jitter); err != nil {
		return false
	}
	if !time.Now().Before(deadline) {
		s.reconnectTimedOut(roundNumber, deadline)
		return false
	}

	s.mu.Lock()
	if s.closing || s.ctx.Err() != nil || s.current != nil {
		s.mu.Unlock()
		return !s.closing
	}
	generation := s.nextConnectSequenceLocked()
	s.mu.Unlock()
	attemptCtx, cancel := context.WithDeadline(s.ctx, deadline)
	client, timing, node, err := s.connect(attemptCtx, false)
	cancel()
	if err != nil {
		if reason, refused := connectRefusalReason(err); refused {
			s.col.recordConnectRejection(roundNumber)
			s.writeEvent(sessionEvent{
				ClientID: s.spec.id, Role: s.spec.role, Event: "reject",
				Timestamp: time.Now().UTC().Format(time.RFC3339Nano), Generation: generation,
				Node: node, Round: roundNumber, Reason: "connack_refused", ReasonCode: &reason, Detail: err.Error(),
			})
		}
		s.increaseBackoff()
		return true
	}
	if s.spec.persistent && !timing.SessionPresent {
		s.col.recordSessionPresentFailure(roundNumber)
	}
	if s.needsSubscription() && !timing.SessionPresent {
		if err := s.subscribe(client); err != nil {
			_ = client.Close()
			s.col.protocolError(fmt.Sprintf("resubscribe %s: %v", s.spec.id, err))
			s.increaseBackoff()
			return true
		}
	}
	if !time.Now().Before(deadline) {
		_ = client.Close()
		s.reconnectTimedOut(roundNumber, deadline)
		return false
	}

	latency := timing.ConnAckAt.Sub(disconnectedAt)
	s.mu.Lock()
	if s.closing || s.ctx.Err() != nil {
		s.mu.Unlock()
		_ = client.Close()
		return false
	}
	s.current = client
	s.node = node
	s.generation = generation
	s.nextPing = time.Now().Add(s.pingInterval())
	s.retryBackoff = 100 * time.Millisecond
	s.disconnectedAt = time.Time{}
	s.reconnectDeadline = time.Time{}
	s.reconnectRound = 0
	s.mu.Unlock()
	s.col.recordReconnect(roundNumber, node, s.spec.id, latency, timing.ConnAckAt)
	s.writeEvent(sessionEvent{
		ClientID: s.spec.id, Role: s.spec.role, Event: "connect",
		Timestamp: timing.ConnAckAt.UTC().Format(time.RFC3339Nano), Generation: generation,
		Node: node, Round: roundNumber, SessionPresent: timing.SessionPresent,
		ReconnectLatencyMillis: float64(latency) / float64(time.Millisecond),
	})
	return true
}

func (s *sessionClient) reconnectTimedOut(roundNumber int, deadline time.Time) {
	s.col.failure(fmt.Sprintf("client %s reconnect timed out at %s", s.spec.id, deadline.UTC().Format(time.RFC3339Nano)))
}

func (s *sessionClient) increaseBackoff() {
	s.mu.Lock()
	if s.retryBackoff < 2*time.Second {
		s.retryBackoff *= 2
		if s.retryBackoff > 2*time.Second {
			s.retryBackoff = 2 * time.Second
		}
	}
	s.mu.Unlock()
}

func (s *sessionClient) sendPingIfDue(client *mqttclient.Client) {
	s.mu.Lock()
	if client != s.current || s.closing || time.Now().Before(s.nextPing) {
		s.mu.Unlock()
		return
	}
	err := client.Write(&mqtt.PingReq{Version: s.spec.version}, time.Second)
	if err == nil {
		s.nextPing = time.Now().Add(s.pingInterval())
	}
	s.mu.Unlock()
	if err != nil {
		s.disconnect(client, "write_error", nil, "", err)
	}
}

func (s *sessionClient) expireAcknowledgements(now time.Time) {
	s.mu.Lock()
	for packetID, pending := range s.pending {
		if pending.timedOut || now.Before(pending.deadline) {
			continue
		}
		pending.timedOut = true
		s.pending[packetID] = pending
		s.col.publishTimeout(s.spec.stream, pending.sequence)
	}
	s.mu.Unlock()
}

func (s *sessionClient) readTimeout() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	timeout := time.Until(s.nextPing)
	for _, pending := range s.pending {
		if pending.timedOut || pending.deadline.IsZero() {
			continue
		}
		if deadline := time.Until(pending.deadline); deadline < timeout {
			timeout = deadline
		}
	}
	if timeout <= 0 {
		return time.Millisecond
	}
	if timeout > s.pingInterval() {
		return time.Until(now.Add(s.pingInterval()))
	}
	return timeout
}

func (s *sessionClient) publish(sequence uint64, scheduledAt time.Time) error {
	if s.spec.role != "publisher" {
		return fmt.Errorf("client %s is not a publisher", s.spec.id)
	}
	interval := time.Second / time.Duration(s.profile.PublishRatePerStreamPerSecond)
	s.mu.Lock()
	client := s.current
	if client == nil || s.closing {
		s.col.skipDisconnected(s.spec.stream)
		s.mu.Unlock()
		return nil
	}
	active := 0
	for _, pending := range s.pending {
		if !pending.timedOut {
			active++
		}
	}
	if s.spec.qos > 0 && active >= s.profile.MaxInflightPerPublisher {
		s.col.skipInflightFull(s.spec.stream)
		s.mu.Unlock()
		return nil
	}
	if time.Since(scheduledAt) >= interval {
		s.col.recordOverload(s.spec.stream)
	}
	var packetID mqtt.PacketIdentifier
	if s.spec.qos > 0 {
		var found bool
		for attempts := 0; attempts < 65535; attempts++ {
			packetID = mqtt.PacketIdentifier(s.nextPacketID)
			s.nextPacketID = s.nextPacketID%65535 + 1
			if _, used := s.pending[packetID]; !used {
				found = true
				break
			}
		}
		if !found {
			s.mu.Unlock()
			return errors.New("no MQTT packet identifier is available")
		}
	}
	protocol := "MQTT 5"
	qos := mqtt.QoS(s.spec.qos)
	if s.spec.version == mqtt.ProtocolV311 {
		protocol = "MQTT 3.1.1"
	}
	record := messageRecord{
		Stream: s.spec.stream, Protocol: protocol, QoS: s.spec.qos,
		Sequence: sequence, scheduled: scheduledAt,
	}
	if err := s.col.beginPublish(record); err != nil {
		s.mu.Unlock()
		s.col.failure(err.Error())
		return err
	}
	payload := makePayload(s.profile.Seed, sequence, s.spec.index, s.profile.PayloadBytes)
	sentAt := time.Now()
	if s.spec.qos > 0 {
		s.pending[packetID] = pendingPublish{
			sequence: sequence,
			deadline: sentAt.Add(time.Duration(s.profile.AckTimeoutMillis) * time.Millisecond),
		}
	}
	err := client.Write(&mqtt.Publish{
		Version: s.spec.version, QoS: qos, PacketID: packetID,
		Topic: s.spec.topic, Payload: payload,
	}, time.Second)
	if err != nil {
		delete(s.pending, packetID)
		s.col.publishFailed(s.spec.stream, sequence)
		s.mu.Unlock()
		handled, roundNumber := s.disconnect(client, "write_error", nil, "", err)
		if handled && roundNumber != 0 {
			return nil
		}
		return fmt.Errorf("publish %s sequence %d: %w", s.spec.id, sequence, err)
	}
	if !s.col.publishSent(s.spec.stream, sequence, sentAt) {
		delete(s.pending, packetID)
		s.mu.Unlock()
		err := fmt.Errorf("record successful PUBLISH %s sequence %d", s.spec.id, sequence)
		s.col.failure(err.Error())
		return err
	}
	s.mu.Unlock()
	return nil
}

func (s *sessionClient) connected() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.current != nil && !s.closing
}

func (s *sessionClient) close() error {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closing = true
		client := s.current
		s.current = nil
		cancel := s.cancel
		done := s.done
		s.mu.Unlock()
		if cancel != nil {
			cancel()
		}
		if client != nil {
			disconnectErr := client.Disconnect(time.Second)
			closeErr := client.Close()
			s.closeErr = errors.Join(disconnectErr, closeErr)
		}
		if done != nil {
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				s.closeErr = errors.Join(s.closeErr, errors.New("session reader did not stop"))
			}
		}
	})
	return s.closeErr
}

func (s *sessionClient) nextConnectSequenceLocked() uint64 {
	s.connectSequence++
	return s.connectSequence
}

func (s *sessionClient) pingInterval() time.Duration {
	return time.Duration(s.profile.KeepAliveSeconds) * time.Second / 2
}

func (s *sessionClient) writeEvent(event sessionEvent) {
	if s.events == nil {
		return
	}
	if err := s.events.Write(event); err != nil {
		s.col.failure("write connection event: " + err.Error())
	}
}

func connectRefusalReason(err error) (byte, bool) {
	if err == nil {
		return 0, false
	}
	text := err.Error()
	index := strings.Index(text, "ReasonCode:0x")
	if index < 0 {
		return 0, false
	}
	hexadecimal := text[index+len("ReasonCode:0x"):]
	end := 0
	for end < len(hexadecimal) && ((hexadecimal[end] >= '0' && hexadecimal[end] <= '9') || (hexadecimal[end] >= 'a' && hexadecimal[end] <= 'f') || (hexadecimal[end] >= 'A' && hexadecimal[end] <= 'F')) {
		end++
	}
	if end == 0 {
		return 0, false
	}
	reason, parseErr := strconv.ParseUint(hexadecimal[:end], 16, 8)
	return byte(reason), parseErr == nil && reason != 0
}

func disconnectReason(err error) string {
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, net.ErrClosed) {
		return "eof"
	}
	return "read_error"
}
