package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"sync"
	"time"

	"narumq/internal/domain/mqtt"
	"narumq/tests/benchmark/internal/jsonl"
	"narumq/tests/benchmark/internal/mqttclient"
)

const (
	reconnectBackoffMin = 100 * time.Millisecond
	reconnectBackoffMax = 2 * time.Second
	handshakeTimeout    = 5 * time.Second
	writeTimeout        = 5 * time.Second
)

type credential struct {
	Username string `json:"username"`
	Token    string `json:"token"`
}

// connectionRecord is one line of connections.jsonl.
type connectionRecord struct {
	At             string `json:"at"`
	ClientID       string `json:"clientId"`
	Tenant         string `json:"tenant"`
	Transport      string `json:"transport"`
	Phase          string `json:"phase"`
	Event          string `json:"event"`
	LatencyNanos   int64  `json:"connectLatencyNanoseconds,omitempty"`
	SessionPresent bool   `json:"sessionPresent"`
	Reason         string `json:"reason,omitempty"`
}

// stepEnv is everything the clients of one step-run share.
type stepEnv struct {
	profile      Profile
	col          *collector
	endpoints    *endpoints
	creds        map[string]credential
	events       *jsonl.Writer
	rejections   *jsonl.Writer
	rejectionMu  sync.Mutex
	rejectionErr error
}

// rejectQuota classifies one quota-class rejection, counts it in the current window and writes
// it to quota-rejections.jsonl. The verdict reads the counts; the record keeps the evidence.
func (env *stepEnv) rejectQuota(spec clientSpec, packet quotaPacket, reason byte, reasonString string) {
	class, cause := classifyQuotaRejection(packet, reason, reasonString)
	window := env.col.window()
	env.col.quota.add(window, class)
	if env.rejections == nil {
		return
	}
	err := env.rejections.Write(quotaRejectionRecord{
		At: formatTime(time.Now()), Phase: window.String(), ClientID: spec.id, Tenant: spec.tenant,
		Packet: string(packet), ReasonCode: int(reason), ReasonString: reasonString,
		Classification: class.String(), Cause: cause,
	})
	if err != nil {
		env.rejectionMu.Lock()
		if env.rejectionErr == nil {
			env.rejectionErr = err
		}
		env.rejectionMu.Unlock()
	}
}

func (env *stepEnv) record(spec clientSpec, event string, latency time.Duration, present bool, reason string) {
	if env.events == nil {
		return
	}
	_ = env.events.Write(connectionRecord{
		At: formatTime(time.Now()), ClientID: spec.id, Tenant: spec.tenant, Transport: transportName(spec.tls),
		Phase: env.col.window().String(), Event: event, LatencyNanos: int64(latency), SessionPresent: present, Reason: reason,
	})
}

func transportName(useTLS bool) string {
	if useTLS {
		return "tls"
	}
	return "tcp"
}

func (env *stepEnv) connectOptions(spec clientSpec, cleanStart bool) mqttclient.ConnectOptions {
	cred := env.creds[spec.tenant]
	options := mqttclient.ConnectOptions{
		ClientID: spec.id, ProtocolVersion: mqtt.ProtocolV5, KeepAlive: env.profile.KeepAliveSeconds,
		HandshakeTimeout: handshakeTimeout, CleanStart: cleanStart,
		Username: cred.Username, Password: []byte(cred.Token),
	}
	if spec.persistent {
		options.SessionExpiryInterval = env.profile.SessionExpirySeconds
	}
	return options
}

type pendingPublish struct {
	seq      uint64
	deadline time.Time
	pubrec   bool
	timedOut bool
	credited bool
}

// sessClient owns one MQTT client for a whole step-run: its reader/reconnect goroutine,
// QoS state machines and, for publishers, the inflight window.
type sessClient struct {
	env  *stepEnv
	spec clientSpec
	plan *cellPlan
	rng  *rand.Rand

	mu       sync.Mutex
	current  *mqttclient.Client
	closing  bool
	pending  map[mqtt.PacketIdentifier]*pendingPublish
	nextPID  uint32
	nextPing time.Time

	credit chan struct{}

	// reader-goroutine-only QoS 2 receive state; it survives reconnects with the session.
	subPending   map[mqtt.PacketIdentifier]uint64
	subCompleted map[mqtt.PacketIdentifier]struct{}

	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
	closed sync.Once
}

func newSessClient(env *stepEnv, spec clientSpec, plan *cellPlan, globalIndex int) *sessClient {
	s := &sessClient{
		env: env, spec: spec, plan: plan, nextPID: 1,
		rng:          rand.New(rand.NewPCG(env.profile.Seed, uint64(globalIndex))),
		pending:      make(map[mqtt.PacketIdentifier]*pendingPublish),
		subPending:   make(map[mqtt.PacketIdentifier]uint64),
		subCompleted: make(map[mqtt.PacketIdentifier]struct{}),
	}
	if spec.role == rolePublisher && spec.qos > 0 {
		s.credit = make(chan struct{}, env.profile.MaxInflightPerPublisher)
	}
	return s
}

func (s *sessClient) pingInterval() time.Duration {
	return time.Duration(s.env.profile.KeepAliveSeconds) * time.Second / 2
}

// trackedOutage reports whether subscriber outages must be recorded for the gap excuse rule.
func (s *sessClient) trackedOutage() bool {
	return s.spec.role == roleSubscriber || s.spec.role == roleStateSubscriber
}

func (s *sessClient) cleanSessionSubscriber() bool {
	return s.trackedOutage() && !s.spec.persistent && s.spec.qos > 0
}

// start connects, subscribes (subscribers) and launches the reader goroutine. connectCtx bounds
// the dial only; lifetime ends the client's reconnect loop.
func (s *sessClient) start(connectCtx, lifetime context.Context) error {
	s.ctx, s.cancel = context.WithCancel(lifetime)
	client, timing, err := s.env.endpoints.connect(connectCtx, s.rng, s.spec.tls, s.env.connectOptions(s.spec, true))
	if err != nil {
		s.cancel()
		s.noteRefusal(err)
		s.env.record(s.spec, "connect-failed", 0, false, err.Error())
		return fmt.Errorf("connect %s: %w", s.spec.id, err)
	}
	if s.trackedOutage() {
		if err := s.subscribe(client); err != nil {
			_ = client.Close()
			s.cancel()
			s.env.record(s.spec, "subscribe-failed", 0, false, err.Error())
			return fmt.Errorf("subscribe %s: %w", s.spec.id, err)
		}
	}
	s.mu.Lock()
	s.current = client
	s.nextPing = time.Now().Add(s.pingInterval())
	s.mu.Unlock()
	s.env.record(s.spec, "connect", timing.Latency, timing.SessionPresent, "")
	s.done = make(chan struct{})
	go s.run()
	return nil
}

func (s *sessClient) noteRefusal(err error) {
	if reason, ok := connackReason(err); ok && isQuotaRejection(reason) {
		s.env.rejectQuota(s.spec, quotaPacketConnAck, reason, "")
	}
}

func (s *sessClient) subscribe(client *mqttclient.Client) error {
	qos := mqtt.QoS(s.spec.qos)
	if err := client.Write(&mqtt.Subscribe{
		Version: mqtt.ProtocolV5, PacketID: 1,
		Subscriptions: []mqtt.Subscription{{Filter: s.spec.filter, QoS: qos}},
	}, writeTimeout); err != nil {
		return err
	}
	deadline := time.Now().Add(handshakeTimeout)
	for time.Now().Before(deadline) {
		packet, err := client.Read(time.Until(deadline))
		if err != nil {
			return err
		}
		switch ack := packet.(type) {
		case *mqtt.PingResp:
			continue
		case *mqtt.SubAck:
			if ack.PacketID != 1 || len(ack.ReasonCodes) != 1 {
				return fmt.Errorf("unexpected SUBACK %#v", ack)
			}
			if reason := ack.ReasonCodes[0]; isQuotaRejection(reason) {
				s.env.rejectQuota(s.spec, quotaPacketSubAck, reason, "")
				return fmt.Errorf("SUBACK refused with reason 0x%02X", reason)
			} else if reason != byte(qos) {
				return fmt.Errorf("SUBACK granted 0x%02X, want %d", reason, qos)
			}
			return nil
		default:
			return fmt.Errorf("expected SUBACK, received %T", packet)
		}
	}
	return errors.New("SUBACK timed out")
}

func (s *sessClient) run() {
	defer close(s.done)
	for {
		s.mu.Lock()
		client, closing := s.current, s.closing
		s.mu.Unlock()
		if closing || s.ctx.Err() != nil {
			return
		}
		if client == nil {
			if !s.reconnect() {
				return
			}
			continue
		}
		packet, err := client.Read(s.readTimeout())
		if err != nil {
			if mqttclient.IsTimeout(err) {
				s.expireAcks(time.Now())
				s.sendPing(client)
				continue
			}
			reason := "read_error"
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, net.ErrClosed) {
				reason = "eof"
			}
			if errors.Is(err, mqtt.ErrMalformedPacket) {
				s.env.col.counters().protocolErrors.Add(1)
				reason = "protocol_error"
			}
			s.dropConn(client, reason, err)
			continue
		}
		s.expireAcks(time.Now())
		if disconnect, ok := packet.(*mqtt.Disconnect); ok {
			s.dropConn(client, fmt.Sprintf("server_disconnect_0x%02X", disconnect.ReasonCode), nil)
			continue
		}
		if failure := s.handlePacket(client, packet); failure != nil {
			reason := "write_error"
			if failure.protocol {
				s.env.col.counters().protocolErrors.Add(1)
				reason = "protocol_error"
			}
			s.dropConn(client, reason, failure.err)
		}
		s.sendPing(client)
	}
}

type packetFailure struct {
	err      error
	protocol bool
}

func protocolFailure(format string, args ...any) *packetFailure {
	return &packetFailure{err: fmt.Errorf(format, args...), protocol: true}
}

func (s *sessClient) handlePacket(client *mqttclient.Client, packet mqtt.Packet) *packetFailure {
	switch value := packet.(type) {
	case *mqtt.PingResp:
		return nil
	case *mqtt.Publish:
		return s.handlePublish(client, value)
	case *mqtt.PubRel:
		_, pending := s.subPending[value.PacketID]
		if pending {
			delete(s.subPending, value.PacketID)
			s.subCompleted[value.PacketID] = struct{}{}
		} else if _, done := s.subCompleted[value.PacketID]; !done {
			return protocolFailure("PUBREL for unknown packet ID %d on %s", value.PacketID, s.spec.id)
		}
		if err := client.Write(&mqtt.PubComp{Version: mqtt.ProtocolV5, PacketID: value.PacketID}, writeTimeout); err != nil {
			return &packetFailure{err: err}
		}
		return nil
	case *mqtt.PubAck:
		return s.handlePubAck(value)
	case *mqtt.PubRec:
		return s.handlePubRec(client, value)
	case *mqtt.PubComp:
		return s.handlePubComp(value)
	default:
		return protocolFailure("unexpected %T on %s", packet, s.spec.id)
	}
}

// handlePublish validates one delivery, records its first copy, and completes the QoS handshake.
func (s *sessClient) handlePublish(client *mqttclient.Client, packet *mqtt.Publish) *packetFailure {
	spec, col := s.spec, s.env.col
	if spec.role != roleSubscriber && spec.role != roleStateSubscriber {
		return protocolFailure("client %s received PUBLISH in role %q", spec.id, spec.role)
	}
	if mqtt.QoS(spec.qos) != packet.QoS {
		return protocolFailure("subscriber %s received QoS %d, want %d", spec.id, packet.QoS, spec.qos)
	}
	record := true
	var seq uint64
	switch {
	case packet.Retain:
		// Retained replay on subscribe: only state streams are retained; not part of the oracle.
		if spec.role != roleStateSubscriber {
			return protocolFailure("subscriber %s received a retained PUBLISH", spec.id)
		}
		col.retainedReplays.Add(1)
		record = false
	default:
		header, err := parsePayloadHeader(packet.Payload)
		if err != nil {
			return protocolFailure("subscriber %s payload: %v", spec.id, err)
		}
		if header.token != col.token {
			col.stale.Add(1)
			record = false
			break
		}
		if header.cell != spec.cell || header.stream != spec.stream {
			return protocolFailure("subscriber %s received cell %d stream %d", spec.id, header.cell, header.stream)
		}
		if err := verifyPayload(s.env.profile, header, packet.Payload); err != nil {
			return protocolFailure("subscriber %s payload: %v", spec.id, err)
		}
		if want := s.plan.publishTopic(spec.stream, header.seq); packet.Topic != want {
			return protocolFailure("subscriber %s received topic %q, want %q", spec.id, packet.Topic, want)
		}
		seq = header.seq
	}
	switch packet.QoS {
	case mqtt.QoSAtMostOnce:
		if record {
			s.deliver(seq)
		}
		return nil
	case mqtt.QoSAtLeastOnce:
		if record {
			s.deliver(seq)
		}
		if err := client.Write(&mqtt.PubAck{Version: mqtt.ProtocolV5, PacketID: packet.PacketID}, writeTimeout); err != nil {
			return &packetFailure{err: err}
		}
		return nil
	default:
		if previous, pending := s.subPending[packet.PacketID]; pending {
			// Retransmission before PUBREL: answer with PUBREC again and do not count a copy.
			if record && previous != seq {
				return protocolFailure("QoS 2 packet ID %d reused for another message on %s", packet.PacketID, spec.id)
			}
		} else {
			delete(s.subCompleted, packet.PacketID)
			if record {
				s.deliver(seq)
			}
			s.subPending[packet.PacketID] = seq
		}
		if err := client.Write(&mqtt.PubRec{Version: mqtt.ProtocolV5, PacketID: packet.PacketID}, writeTimeout); err != nil {
			return &packetFailure{err: err}
		}
		return nil
	}
}

func (s *sessClient) deliver(seq uint64) {
	if s.env.col.deliver(s.spec.cell, s.spec.stream, s.spec.index, seq) == deliverUnexpected {
		s.env.col.counters().protocolErrors.Add(1)
	}
}

func (s *sessClient) releaseCredit(entry *pendingPublish) {
	if entry.credited && s.credit != nil {
		entry.credited = false
		select {
		case <-s.credit:
		default:
		}
	}
}

func (s *sessClient) handlePubAck(packet *mqtt.PubAck) *packetFailure {
	s.mu.Lock()
	entry, ok := s.pending[packet.PacketID]
	if ok {
		delete(s.pending, packet.PacketID)
		s.releaseCredit(entry)
	}
	s.mu.Unlock()
	if !ok || s.spec.qos != 1 {
		return protocolFailure("unexpected PUBACK %d on %s", packet.PacketID, s.spec.id)
	}
	s.finishAck(entry, quotaPacketPubAck, packet.ReasonCode, packet.ReasonString)
	return nil
}

func (s *sessClient) handlePubRec(client *mqttclient.Client, packet *mqtt.PubRec) *packetFailure {
	s.mu.Lock()
	entry, ok := s.pending[packet.PacketID]
	if ok && packet.ReasonCode >= 0x80 {
		delete(s.pending, packet.PacketID)
		s.releaseCredit(entry)
	} else if ok {
		entry.pubrec = true
		entry.deadline = time.Now().Add(time.Duration(s.env.profile.AckTimeoutMillis) * time.Millisecond)
	}
	s.mu.Unlock()
	if !ok || s.spec.qos != 2 {
		return protocolFailure("unexpected PUBREC %d on %s", packet.PacketID, s.spec.id)
	}
	if packet.ReasonCode >= 0x80 {
		s.finishAck(entry, quotaPacketPubRec, packet.ReasonCode, packet.ReasonString)
		return nil
	}
	s.finishAck(entry, quotaPacketPubRec, packet.ReasonCode, packet.ReasonString)
	if err := client.Write(&mqtt.PubRel{Version: mqtt.ProtocolV5, PacketID: packet.PacketID}, writeTimeout); err != nil {
		return &packetFailure{err: err}
	}
	return nil
}

func (s *sessClient) handlePubComp(packet *mqtt.PubComp) *packetFailure {
	s.mu.Lock()
	entry, ok := s.pending[packet.PacketID]
	if ok && entry.pubrec {
		delete(s.pending, packet.PacketID)
		s.releaseCredit(entry)
	}
	s.mu.Unlock()
	if !ok || !entry.pubrec || s.spec.qos != 2 {
		return protocolFailure("unexpected PUBCOMP %d on %s", packet.PacketID, s.spec.id)
	}
	s.env.col.complete(s.spec.cell, s.spec.stream)
	return nil
}

// finishAck records a PUBACK/PUBREC outcome. Reason codes >= 0x80 are errors. Quota-class
// refusals are classified from the packet (§3.9); the other error codes are protocol errors.
// A refused publish is not an ACK, so it is a failed publish and never a gap.
func (s *sessClient) finishAck(entry *pendingPublish, packet quotaPacket, reason byte, reasonString string) {
	col := s.env.col
	switch {
	case isQuotaRejection(reason):
		s.env.rejectQuota(s.spec, packet, reason, reasonString)
		col.publishFailed(s.spec.cell, s.spec.stream, entry.seq)
	case reason >= 0x80:
		col.counters().protocolErrors.Add(1)
		col.publishFailed(s.spec.cell, s.spec.stream, entry.seq)
	default:
		col.ack(s.spec.cell, s.spec.stream, entry.seq)
	}
}

func (s *sessClient) expireAcks(now time.Time) {
	s.mu.Lock()
	for _, entry := range s.pending {
		if entry.timedOut || now.Before(entry.deadline) {
			continue
		}
		entry.timedOut = true
		s.releaseCredit(entry)
		s.env.col.counters().ackTimeouts.Add(1)
	}
	s.mu.Unlock()
}

func (s *sessClient) readTimeout() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	timeout := time.Until(s.nextPing)
	for _, entry := range s.pending {
		if !entry.timedOut {
			timeout = min(timeout, time.Until(entry.deadline))
		}
	}
	return max(timeout, time.Millisecond)
}

func (s *sessClient) sendPing(client *mqttclient.Client) {
	s.mu.Lock()
	due := client == s.current && !s.closing && !time.Now().Before(s.nextPing)
	if due {
		s.nextPing = time.Now().Add(s.pingInterval())
	}
	s.mu.Unlock()
	if !due {
		return
	}
	if err := client.Write(&mqtt.PingReq{Version: mqtt.ProtocolV5}, writeTimeout); err != nil {
		s.dropConn(client, "write_error", err)
	}
}

// dropConn handles loss of the current connection; the reader goroutine then reconnects.
func (s *sessClient) dropConn(client *mqttclient.Client, reason string, cause error) {
	s.mu.Lock()
	if client == nil || client != s.current || s.closing {
		s.mu.Unlock()
		return
	}
	s.current = nil
	for packetID, entry := range s.pending {
		if !entry.timedOut {
			s.env.col.publishInterrupted(s.spec.cell, s.spec.stream, entry.seq)
		}
		s.releaseCredit(entry)
		delete(s.pending, packetID)
	}
	s.mu.Unlock()
	_ = client.Close()
	detail := reason
	if cause != nil {
		detail += ": " + cause.Error()
	}
	s.env.col.unplannedDisconnect()
	if s.cleanSessionSubscriber() {
		s.env.col.subscriberDown(s.spec.cell, s.spec.stream, s.spec.index)
	}
	s.env.record(s.spec, "disconnect", 0, false, detail)
}

// reconnect retries with exponential full-jitter backoff (100 ms to 2 s) seeded by
// (seed, client index) until it succeeds or the step ends. Persistent sessions reconnect with
// CleanStart=false and must report SessionPresent=true.
func (s *sessClient) reconnect() bool {
	backoff := reconnectBackoffMin
	failureRecorded := false
	for {
		jitter := time.Duration(s.rng.Int64N(int64(backoff) + 1))
		select {
		case <-s.ctx.Done():
			return false
		case <-time.After(jitter):
		}
		s.mu.Lock()
		closing := s.closing
		s.mu.Unlock()
		if closing {
			return false
		}
		attemptCtx, cancel := context.WithTimeout(s.ctx, handshakeTimeout*2)
		client, timing, err := s.env.endpoints.connect(attemptCtx, s.rng, s.spec.tls, s.env.connectOptions(s.spec, !s.spec.persistent))
		cancel()
		if err == nil && s.trackedOutage() && (!s.spec.persistent || !timing.SessionPresent) {
			if s.spec.persistent {
				s.env.col.sessionPresentFailures.Add(1)
				s.env.record(s.spec, "session-lost", timing.Latency, false, "SessionPresent=false on persistent reconnect")
			}
			if subErr := s.subscribe(client); subErr != nil {
				_ = client.Close()
				err = subErr
			}
		} else if err == nil && s.spec.persistent && !timing.SessionPresent {
			s.env.col.sessionPresentFailures.Add(1)
			s.env.record(s.spec, "session-lost", timing.Latency, false, "SessionPresent=false on persistent reconnect")
		}
		if err != nil {
			s.noteRefusal(err)
			if !failureRecorded {
				failureRecorded = true
				s.env.record(s.spec, "reconnect-failed", 0, false, err.Error())
			}
			backoff = min(backoff*2, reconnectBackoffMax)
			continue
		}
		s.mu.Lock()
		if s.closing || s.ctx.Err() != nil {
			s.mu.Unlock()
			_ = client.Close()
			return false
		}
		s.current = client
		s.nextPing = time.Now().Add(s.pingInterval())
		s.mu.Unlock()
		if s.cleanSessionSubscriber() {
			s.env.col.subscriberUp(s.spec.cell, s.spec.stream, s.spec.index)
		}
		s.env.col.reconnected()
		s.env.record(s.spec, "reconnect", timing.Latency, timing.SessionPresent, "")
		return true
	}
}

// publish sends one message on the absolute schedule. It returns immediately when the client
// is disconnected, waits for an inflight credit otherwise, and classifies schedule lag.
func (s *sessClient) publish(ctx context.Context, scheduled time.Time, interval time.Duration) {
	col := s.env.col
	if !s.connected() {
		col.counters().skippedDisconnected.Add(1)
		return
	}
	var waited time.Duration
	if s.credit != nil {
		select {
		case s.credit <- struct{}{}:
		default:
			started := time.Now()
			if !s.waitCredit(ctx) {
				col.counters().skippedDisconnected.Add(1)
				return
			}
			waited = time.Since(started)
		}
	}
	if lag := time.Since(scheduled); lag >= interval {
		if waited > 0 && lag-waited < interval {
			col.counters().creditStarvations.Add(1)
		} else {
			col.counters().scheduleOverloads.Add(1)
		}
	}
	s.mu.Lock()
	client := s.current
	if client == nil || s.closing {
		s.mu.Unlock()
		s.returnCredit()
		col.counters().skippedDisconnected.Add(1)
		return
	}
	seq, _ := col.begin(s.spec.cell, s.spec.stream)
	var packetID mqtt.PacketIdentifier
	var entry *pendingPublish
	if s.spec.qos > 0 {
		for range 65535 {
			packetID = mqtt.PacketIdentifier(s.nextPID)
			s.nextPID = s.nextPID%65535 + 1
			if _, used := s.pending[packetID]; !used {
				break
			}
		}
		entry = &pendingPublish{seq: seq, credited: true, deadline: time.Now().Add(time.Duration(s.env.profile.AckTimeoutMillis) * time.Millisecond)}
		s.pending[packetID] = entry
	}
	s.mu.Unlock()
	payload := makePayload(s.env.profile, payloadHeader{seq: seq, token: col.token, cell: s.spec.cell, stream: s.spec.stream})
	err := client.Write(&mqtt.Publish{
		Version: mqtt.ProtocolV5, QoS: mqtt.QoS(s.spec.qos), Retain: streamDefs[s.spec.stream].retained,
		PacketID: packetID, Topic: s.plan.publishTopic(s.spec.stream, seq), Payload: payload,
	}, writeTimeout)
	if err != nil {
		s.mu.Lock()
		if entry != nil {
			if _, still := s.pending[packetID]; still {
				delete(s.pending, packetID)
				s.releaseCredit(entry)
			}
		}
		s.mu.Unlock()
		col.publishFailed(s.spec.cell, s.spec.stream, seq)
		s.dropConn(client, "write_error", err)
	}
}

func (s *sessClient) connected() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.current != nil && !s.closing
}

func (s *sessClient) returnCredit() {
	select {
	case <-s.credit:
	default:
	}
}

// waitCredit blocks until an inflight credit is acquired; false means the client disconnected or the context ended.
func (s *sessClient) waitCredit(ctx context.Context) bool {
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case s.credit <- struct{}{}:
			return true
		case <-ctx.Done():
			return false
		case <-ticker.C:
			if !s.connected() {
				return false
			}
		}
	}
}

// close ends the client: planned DISCONNECT, socket close and reader goroutine shutdown.
func (s *sessClient) close() {
	s.closed.Do(func() {
		s.mu.Lock()
		s.closing = true
		client := s.current
		s.current = nil
		s.mu.Unlock()
		if s.cancel != nil {
			s.cancel()
		}
		if client != nil {
			_ = client.Disconnect(time.Second)
			_ = client.Close()
		}
		if s.done != nil {
			select {
			case <-s.done:
			case <-time.After(5 * time.Second):
			}
		}
	})
}
