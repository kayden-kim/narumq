package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"narumq/internal/domain/mqtt"
	"narumq/tests/benchmark/internal/jsonl"
	"narumq/tests/benchmark/internal/mqttclient"
)

type credentials struct {
	Username        string `json:"username"`
	Password        string `json:"password"`
	ManagementToken string `json:"managementToken"`
}
type streamSpec struct {
	index, qos                         int
	name, topic, publisher, subscriber string
	pubAddr, subAddr, reconnectAddr    string
}
type pendingAck struct {
	sequence   uint64
	deadline   time.Time
	pubrecSeen bool
	timedOut   bool
}

type expiredAck struct {
	packetID mqtt.PacketIdentifier
	sequence uint64
}
type runClient struct {
	client         *mqttclient.Client
	stream         streamSpec
	stop           chan struct{}
	done           chan struct{}
	stopOnce       sync.Once
	closeOnce      sync.Once
	ackMu          sync.Mutex
	pending        map[mqtt.PacketIdentifier]pendingAck
	readFailure    error
	readFailureAt  time.Time
	changed        chan struct{}
	SessionPresent bool
	connAckAt      time.Time
	closeErr       error
}

type phaseEvent struct {
	Phase     string `json:"phase"`
	Event     string `json:"event"`
	Timestamp string `json:"timestamp"`
}

func runCommand(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("run", flag.ContinueOnError)
	flags.SetOutput(stderr)
	profilePath := flags.String("profile", "", "profile JSON")
	target := flags.String("target", "", "narumq or emqx")
	nodeList := flags.String("nodes", "", "three MQTT host:port endpoints")
	managementList := flags.String("management", "", "three management host:port endpoints")
	credentialPath := flags.String("credentials", "", "credentials JSON")
	outputDir := flags.String("output", "", "artifact output directory")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected run arguments: %v", flags.Args())
	}
	if *profilePath == "" || *target == "" || *nodeList == "" || *credentialPath == "" || *outputDir == "" {
		return errors.New("run requires --profile, --target, --nodes, --credentials and --output")
	}
	if *target != "narumq" && *target != "emqx" {
		return fmt.Errorf("unsupported target %q", *target)
	}
	nodes, err := parseEndpoints(*nodeList)
	if err != nil {
		return fmt.Errorf("--nodes: %w", err)
	}
	var management []string
	if *target == "narumq" {
		if *managementList == "" {
			return errors.New("--management is required for narumq")
		}
		management, err = parseEndpoints(*managementList)
		if err != nil {
			return fmt.Errorf("--management: %w", err)
		}
	} else if *managementList != "" {
		return errors.New("--management is not accepted for emqx")
	}
	profile, _, _, err := LoadProfile(*profilePath)
	if err != nil {
		return err
	}
	credentialBytes, err := os.ReadFile(*credentialPath)
	if err != nil {
		return fmt.Errorf("read credentials: %w", err)
	}
	var creds credentials
	if err := json.Unmarshal(credentialBytes, &creds); err != nil {
		return fmt.Errorf("decode credentials: %w", err)
	}
	if creds.Username == "" || creds.Password == "" {
		return errors.New("credentials username and password are required")
	}
	if *target == "narumq" && creds.ManagementToken == "" {
		return errors.New("managementToken is required for narumq")
	}
	ctx := context.Background()
	return executeBP03(ctx, profile, *target, nodes, management, creds, *outputDir)
}

func parseEndpoints(raw string) ([]string, error) {
	parts := strings.Split(raw, ",")
	if len(parts) != 3 {
		return nil, errors.New("exactly three comma-separated host:port endpoints are required")
	}
	seen := make(map[string]struct{}, len(parts))
	for index, part := range parts {
		address := strings.TrimSpace(part)
		if address == "" {
			return nil, errors.New("empty endpoint")
		}
		host, port, err := net.SplitHostPort(address)
		if err != nil {
			return nil, fmt.Errorf("invalid endpoint %q: %w", part, err)
		}
		if strings.TrimSpace(host) == "" || port == "" {
			return nil, fmt.Errorf("invalid endpoint %q: host and port are required", part)
		}
		for _, character := range port {
			if character < '0' || character > '9' {
				return nil, fmt.Errorf("invalid endpoint %q: port must be numeric", part)
			}
		}
		portNumber, err := strconv.ParseUint(port, 10, 16)
		if err != nil || portNumber == 0 {
			return nil, fmt.Errorf("invalid endpoint %q: port must be between 1 and 65535", part)
		}
		if _, exists := seen[address]; exists {
			return nil, fmt.Errorf("duplicate endpoint %q", address)
		}
		seen[address] = struct{}{}
		parts[index] = address
	}
	return parts, nil
}

func executeBP03(ctx context.Context, profile Profile, target string, nodes, management []string, creds credentials, outputDir string) (runErr error) {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(context.Canceled)
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		return err
	}
	if target == "narumq" {
		if err := os.MkdirAll(filepath.Join(outputDir, "metrics"), 0o755); err != nil {
			return err
		}
	}
	phaseWriter, err := jsonl.Open(filepath.Join(outputDir, "phases.jsonl"))
	if err != nil {
		return err
	}
	messages := newCollector(profile.MaxMessages)
	measureBounds := make(map[int][2]time.Time)
	walWindows := make([]walWindowInput, 0, len(profile.RatesPerStreamPerSecond)+1)
	var clients []*runClient
	var cleanupOnce sync.Once
	cleanup := func() {
		cleanupOnce.Do(func() {
			_ = closeClients(clients, time.Second)
			if err := messages.writeMessages(filepath.Join(outputDir, "messages.jsonl")); err != nil {
				runErr = errors.Join(runErr, fmt.Errorf("write messages.jsonl: %w", err))
			}
			summary := messages.summarize(profile, target, measureBounds)
			if runErr != nil {
				summary.Valid = false
				summary.FailureReasons = append(summary.FailureReasons, "workload execution failed")
			}
			if err := writeWorkloadSummary(outputDir, summary); err != nil {
				runErr = errors.Join(runErr, fmt.Errorf("write workload-summary.json: %w", err))
			}
			if !summary.Valid {
				runErr = errors.Join(runErr, errors.New("workload summary is invalid"))
			}
			if target == "narumq" {
				wal, err := writeWALSummary(outputDir, target, walWindows)
				if err != nil {
					runErr = errors.Join(runErr, err)
				} else if !wal.Valid {
					runErr = errors.Join(runErr, errors.New("WAL summary is invalid"))
				}
			} else {
				_, err := writeWALSummary(outputDir, target, nil)
				runErr = errors.Join(runErr, err)
			}
			runErr = errors.Join(runErr, phaseWriter.Close())
		})
	}
	defer cleanup()

	if err := runRecorded(phaseWriter, "readiness", func(start time.Time) error {
		return readiness(ctx, target, nodes, management, creds.ManagementToken, start.Add(90*time.Second), creds)
	}); err != nil {
		return err
	}
	streams := makeStreams(profile, nodes)
	nextSequences := make(map[string]*uint64, len(streams))
	for _, stream := range streams {
		sequence := uint64(0)
		nextSequences[stream.name] = &sequence
	}
	subscribers := make(map[string]*runClient, len(streams))
	publishers := make(map[string]*runClient, len(streams))
	if err := runRecorded(phaseWriter, "setup", func(time.Time) error {
		for _, stream := range streams {
			subscriber, err := connectClient(ctx, stream, stream.subscriber, stream.subAddr, true, profile.SessionExpirySeconds, creds)
			if err != nil {
				return fmt.Errorf("connect subscriber %s: %w", stream.name, err)
			}
			clients = append(clients, subscriber)
			if err := subscribe(subscriber, stream); err != nil {
				return fmt.Errorf("subscribe %s: %w", stream.name, err)
			}
			subscribers[stream.name] = subscriber
			startReader(subscriber, messages, profile, cancel)
		}
		for _, stream := range streams {
			publisher, err := connectClient(ctx, stream, stream.publisher, stream.pubAddr, true, 0, creds)
			if err != nil {
				return fmt.Errorf("connect publisher %s: %w", stream.name, err)
			}
			clients = append(clients, publisher)
			publishers[stream.name] = publisher
			startPublisherReader(publisher, messages, profile.AckTimeoutMillis, cancel)
		}
		return nil
	}); err != nil {
		return err
	}

	for index, rate := range profile.RatesPerStreamPerSecond {
		step := index + 1
		warmupName := fmt.Sprintf("step-%02d-warmup", step)
		if err := runRecorded(phaseWriter, warmupName, func(start time.Time) error {
			return publishWindow(ctx, profile, streams, publishers, messages, nextSequences, step, rate, profile.WarmupMillis, warmupName)
		}); err != nil {
			return err
		}
		if err := waitForPublisherExchanges(ctx, publishers, profile.AckTimeoutMillis); err != nil {
			return fmt.Errorf("settle warmup step %02d: %w", step, err)
		}
		measureName := fmt.Sprintf("step-%02d-measure", step)
		var walWindowStarted time.Time
		var scrapeStart []walNodeInput
		if target == "narumq" {
			walWindowStarted = time.Now()
			scrapeStart, err = scrapeBoundary(management, creds.ManagementToken, outputDir, fmt.Sprintf("step-%02d", step), "start")
			if err != nil {
				return err
			}
		}
		if err := runRecorded(phaseWriter, measureName, func(start time.Time) error {
			bounds := measureBounds[step]
			bounds[0] = start
			measureBounds[step] = bounds
			return publishWindow(ctx, profile, streams, publishers, messages, nextSequences, step, rate, profile.MeasureMillis, measureName)
		}); err != nil {
			return err
		}
		bounds := measureBounds[step]
		bounds[1] = time.Now()
		measureBounds[step] = bounds
		if err := waitForPublisherExchanges(ctx, publishers, profile.AckTimeoutMillis); err != nil {
			return fmt.Errorf("settle measured step %02d: %w", step, err)
		}
		if target == "narumq" {
			ackedMessages := messages.ackedForStep(step)
			end, err := scrapeBoundary(management, creds.ManagementToken, outputDir, fmt.Sprintf("step-%02d", step), "end")
			if err != nil {
				return err
			}
			walWindows = append(walWindows, walWindowInput{
				Name: fmt.Sprintf("step-%02d", step), Duration: time.Since(walWindowStarted),
				AckedMessages: ackedMessages, Nodes: pairScrapes(scrapeStart, end),
			})
		}
	}

	if err := runRecorded(phaseWriter, "drain", func(start time.Time) error {
		return waitForDrain(ctx, messages, profile.DrainMillis)
	}); err != nil {
		return err
	}
	offlineStarted := time.Now()
	var offlineStart []walNodeInput
	if target == "narumq" {
		offlineStart, err = scrapeBoundary(management, creds.ManagementToken, outputDir, "offline", "start")
		if err != nil {
			return err
		}
	}
	if err := runRecorded(phaseWriter, "offline", func(time.Time) error {
		if err := closeClients(mapValues(subscribers), 2*time.Second); err != nil {
			return fmt.Errorf("disconnect subscribers before offline publish window: %w", err)
		}
		subscribers = make(map[string]*runClient, len(streams))
		if err := waitForContext(ctx, time.Duration(profile.OfflineSettleMillis)*time.Millisecond); err != nil {
		}
		if err := publishWindow(ctx, profile, streams, publishers, messages, nextSequences, 0, profile.RatesPerStreamPerSecond[0], int64(profile.OfflineMessagesPerStream)*1000/int64(profile.RatesPerStreamPerSecond[0]), "offline"); err != nil {
			return err
		}
		return waitForPublisherExchanges(ctx, publishers, profile.AckTimeoutMillis)
	}); err != nil {
		return err
	}
	if target == "narumq" {
		ackedMessages := messages.ackedForStep(0)
		end, err := scrapeBoundary(management, creds.ManagementToken, outputDir, "offline", "end")
		if err != nil {
			return err
		}
		walWindows = append(walWindows, walWindowInput{
			Name: "offline", Duration: time.Since(offlineStarted),
			AckedMessages: ackedMessages, Nodes: pairScrapes(offlineStart, end),
		})
	}

	if err := runRecorded(phaseWriter, "reconnect", func(start time.Time) error {
		deadline := start.Add(time.Duration(profile.ReconnectTimeoutMillis) * time.Millisecond)
		reconnectCtx, cancelReconnect := context.WithDeadline(ctx, deadline)
		defer cancelReconnect()
		reconnectStarts := make(map[string]time.Time, len(streams))
		for _, stream := range streams {
			subscriber, err := connectClient(reconnectCtx, stream, stream.subscriber, stream.reconnectAddr, false, profile.SessionExpirySeconds, creds)
			if err != nil {
				return fmt.Errorf("reconnect subscriber %s: %w", stream.name, err)
			}
			if !subscriber.SessionPresent {
				messages.recordOfflineSession(false)
				if err := subscriber.disconnectAndClose(time.Second); err != nil {
					return fmt.Errorf("disconnect subscriber %s without a persistent session: %w", stream.name, err)
				}
				continue
			}
			messages.recordOfflineSession(true)
			clients = append(clients, subscriber)
			subscribers[stream.name] = subscriber
			reconnectStarts[stream.name] = subscriber.connAckAt
			startReader(subscriber, messages, profile, cancel)
		}
		if err := waitForOfflineCompletion(reconnectCtx, messages, streams, profile.OfflineMessagesPerStream, deadline); err != nil {
			return err
		}
		for stream, began := range reconnectStarts {
			if finished, ok := messages.lastOfflineReceipt(stream); ok {
				messages.reconnectMillis = append(messages.reconnectMillis, float64(finished.Sub(began))/float64(time.Millisecond))
			}
		}
		return nil
	}); err != nil {
		return err
	}
	if err := runRecorded(phaseWriter, "cleanup", func(time.Time) error {
		_ = closeClients(clients, time.Second)
		return nil
	}); err != nil {
		return err
	}
	cleanup()
	return runErr
}

func mapValues(clientsByName map[string]*runClient) []*runClient {
	clients := make([]*runClient, 0, len(clientsByName))
	for _, client := range clientsByName {
		clients = append(clients, client)
	}
	return clients
}

func makeStreams(profile Profile, nodes []string) []streamSpec {
	streams := make([]streamSpec, 0, profile.PairsPerQoS*2)
	for qos := 1; qos <= 2; qos++ {
		for ordinal := 1; ordinal <= profile.PairsPerQoS; ordinal++ {
			index := len(streams)
			suffix := fmt.Sprintf("%02d", ordinal)
			name := fmt.Sprintf("q%d-%s", qos, suffix)
			streams = append(streams, streamSpec{
				index:         index,
				qos:           qos,
				name:          name,
				topic:         fmt.Sprintf("bp03/durable/q%d/%s", qos, suffix),
				publisher:     fmt.Sprintf("bp03-pub-%s", name),
				subscriber:    fmt.Sprintf("bp03-sub-%s", name),
				pubAddr:       nodes[index%3],
				subAddr:       nodes[(index+1)%3],
				reconnectAddr: nodes[(index+2)%3],
			})
		}
	}
	return streams
}

func connectClient(ctx context.Context, stream streamSpec, id, address string, clean bool, expiry uint32, creds credentials) (*runClient, error) {
	client, timing, err := mqttclient.Connect(ctx, mqttclient.ConnectOptions{
		Address:               address,
		ClientID:              id,
		KeepAlive:             30,
		HandshakeTimeout:      5 * time.Second,
		CleanStart:            clean,
		SessionExpiryInterval: expiry,
		Username:              creds.Username,
		Password:              []byte(creds.Password),
	})
	if err != nil {
		return nil, err
	}
	return &runClient{
		client:         client,
		stream:         stream,
		stop:           make(chan struct{}),
		pending:        make(map[mqtt.PacketIdentifier]pendingAck),
		changed:        make(chan struct{}, 1),
		SessionPresent: timing.SessionPresent,
		connAckAt:      timing.ConnAckAt,
	}, nil
}

func subscribe(client *runClient, stream streamSpec) error {
	qos := mqtt.QoSAtLeastOnce
	if stream.qos == 2 {
		qos = mqtt.QoSExactlyOnce
	}
	request := &mqtt.Subscribe{
		Version:       mqtt.ProtocolV5,
		PacketID:      1,
		Subscriptions: []mqtt.Subscription{{Filter: stream.topic, QoS: qos}},
	}
	if err := client.client.Write(request, 5*time.Second); err != nil {
		return err
	}
	packet, err := client.client.Read(5 * time.Second)
	if err != nil {
		return err
	}
	ack, ok := packet.(*mqtt.SubAck)
	if !ok || len(ack.ReasonCodes) != 1 || ack.ReasonCodes[0] != byte(qos) {
		return fmt.Errorf("unexpected SUBACK %#v", packet)
	}
	return nil
}

func (client *runClient) stopReader() {
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
		if client == nil {
			continue
		}
		if err := client.disconnectAndClose(timeout); err != nil {
			errs = append(errs, fmt.Errorf("client %s disconnect and close: %w", client.stream.name, err))
		}
	}
	return errors.Join(errs...)
}

func (client *runClient) signal() {
	select {
	case client.changed <- struct{}{}:
	default:
	}
}

func (client *runClient) failRead(err error) error {
	client.ackMu.Lock()
	if client.readFailure == nil {
		client.readFailureAt = time.Now().UTC()
		client.readFailure = fmt.Errorf("runner reader %s failed at %s: %w",
			client.stream.name, client.readFailureAt.Format(time.RFC3339Nano), err)
	}
	failure := client.readFailure
	client.ackMu.Unlock()
	client.signal()
	return failure
}

func failReader(client *runClient, cancel context.CancelCauseFunc, err error) {
	cancel(client.failRead(err))
}

func failProtocolReader(client *runClient, col *collector, cancel context.CancelCauseFunc, err error) {
	col.protocolError()
	failReader(client, cancel, err)
}

func startReader(client *runClient, col *collector, profile Profile, cancel context.CancelCauseFunc) {
	client.done = make(chan struct{})
	go func() {
		defer close(client.done)
		defer client.signal()
		pending := make(map[mqtt.PacketIdentifier]bool)
		completed := make(map[mqtt.PacketIdentifier]bool)
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
					failReader(client, cancel, fmt.Errorf("subscriber %s read: %w", client.stream.name, err))
					return
				}
			}
			switch value := packet.(type) {
			case *mqtt.Publish:
				delete(completed, value.PacketID)
				response, err := processSubscriberPublish(client.stream, profile, value, col, pending, time.Now())
				if err != nil {
					cancel(client.failRead(fmt.Errorf("subscriber %s publish: %w", client.stream.name, err)))
					return
				}
				if err := client.client.Write(response, time.Second); err != nil {
					failReader(client, cancel, fmt.Errorf("subscriber %s response: %w", client.stream.name, err))
					return
				}
			case *mqtt.PubRel:
				if pending[value.PacketID] {
					delete(pending, value.PacketID)
					completed[value.PacketID] = true
				} else if !completed[value.PacketID] {
					failProtocolReader(client, col, cancel, fmt.Errorf("subscriber %s received PUBREL for unknown packet id %d", client.stream.name, value.PacketID))
					return
				}
				response := &mqtt.PubComp{Version: mqtt.ProtocolV5, PacketID: value.PacketID}
				if err := client.client.Write(response, time.Second); err != nil {
					failReader(client, cancel, fmt.Errorf("subscriber %s PUBCOMP response: %w", client.stream.name, err))
					return
				}
			default:
				failProtocolReader(client, col, cancel, fmt.Errorf("subscriber %s received unexpected packet %T", client.stream.name, packet))
				return
			}
		}
	}()
}

func processSubscriberPublish(stream streamSpec, profile Profile, packet *mqtt.Publish, col *collector, pending map[mqtt.PacketIdentifier]bool, now time.Time) (mqtt.Packet, error) {
	sequence, err := validateStreamPayload(profile, stream.index, packet.Payload)
	if err != nil {
		col.protocolError()
		return nil, err
	}
	if packet.Topic != stream.topic || int(packet.QoS) != stream.qos {
		col.protocolError()
		return nil, errors.New("unexpected topic or QoS")
	}
	if stream.qos == 2 {
		if pending[packet.PacketID] {
			return &mqtt.PubRec{Version: mqtt.ProtocolV5, PacketID: packet.PacketID}, nil
		}
		pending[packet.PacketID] = true
	}
	col.receive(stream.name, sequence, now)
	if stream.qos == 1 {
		return &mqtt.PubAck{Version: mqtt.ProtocolV5, PacketID: packet.PacketID}, nil
	}
	return &mqtt.PubRec{Version: mqtt.ProtocolV5, PacketID: packet.PacketID}, nil
}

func validateStreamPayload(profile Profile, index int, payload []byte) (uint64, error) {
	if len(payload) != profile.PayloadBytes || len(payload) < 9 || int(payload[8]) != index {
		return 0, errors.New("delivery payload size or stream index mismatch")
	}
	sequence := binary.BigEndian.Uint64(payload[:8])
	if sequence == 0 {
		return 0, errors.New("delivery sequence is zero")
	}
	for offset := 9; offset < len(payload); offset++ {
		expected := deterministicByte(profile.Seed, uint64(index), sequence, offset-9)
		if payload[offset] != expected {
			return 0, errors.New("delivery payload deterministic bytes mismatch")
		}
	}
	return sequence, nil
}
func startPublisherReader(client *runClient, col *collector, ackTimeoutMillis int64, cancel context.CancelCauseFunc) {
	client.done = make(chan struct{})
	go func() {
		defer close(client.done)
		defer client.signal()
		expiryTicker := time.NewTicker(10 * time.Millisecond)
		defer expiryTicker.Stop()
		for {
			select {
			case now := <-expiryTicker.C:
				if expirePublisherAcks(client, col, now, cancel) {
					return
				}
			default:
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
					failReader(client, cancel, fmt.Errorf("publisher %s read: %w", client.stream.name, err))
					return
				}
			}
			if err := processPublisherAck(client, col, packet, ackTimeoutMillis); err != nil {
				failReader(client, cancel, fmt.Errorf("publisher %s acknowledgement: %w", client.stream.name, err))
				return
			}
		}
	}()
}

func (client *runClient) expirePending(now time.Time) []expiredAck {
	client.ackMu.Lock()
	defer client.ackMu.Unlock()
	var expired []expiredAck
	for packetID, pending := range client.pending {
		if pending.timedOut || now.Before(pending.deadline) {
			continue
		}
		pending.timedOut = true
		client.pending[packetID] = pending
		expired = append(expired, expiredAck{packetID: packetID, sequence: pending.sequence})
	}
	return expired
}

func expirePublisherAcks(client *runClient, col *collector, now time.Time, cancel context.CancelCauseFunc) bool {
	expired := client.expirePending(now)
	if len(expired) == 0 {
		return false
	}
	for _, pending := range expired {
		col.timeout(client.stream.name, pending.sequence)
	}
	first := expired[0]
	cancel(fmt.Errorf("publisher %s acknowledgement timeout for packet ID %d sequence %d",
		client.stream.name, first.packetID, first.sequence))
	client.signal()
	return true
}

func processPublisherAck(client *runClient, col *collector, packet mqtt.Packet, ackTimeoutMillis int64) error {
	var packetID mqtt.PacketIdentifier
	var reason byte
	packetKind := ""
	switch ack := packet.(type) {
	case *mqtt.PubAck:
		packetID, reason, packetKind = ack.PacketID, ack.ReasonCode, "puback"
	case *mqtt.PubRec:
		packetID, reason, packetKind = ack.PacketID, ack.ReasonCode, "pubrec"
	case *mqtt.PubComp:
		packetID, reason, packetKind = ack.PacketID, ack.ReasonCode, "pubcomp"
	default:
		return protocolError(col, fmt.Errorf("unexpected publisher packet %T", packet))
	}
	if client.stream.qos == 1 && packetKind != "puback" {
		return protocolError(col, fmt.Errorf("unexpected QoS 2 acknowledgement %s for QoS 1", packetKind))
	}
	if client.stream.qos == 2 && packetKind == "puback" {
		return protocolError(col, errors.New("unexpected PUBACK for QoS 2"))
	}

	client.ackMu.Lock()
	pending, ok := client.pending[packetID]
	if !ok {
		client.ackMu.Unlock()
		return protocolError(col, fmt.Errorf("acknowledgement for unknown packet id %d", packetID))
	}
	if pending.timedOut || !time.Now().Before(pending.deadline) {
		pending.timedOut = true
		client.pending[packetID] = pending
		client.ackMu.Unlock()
		col.timeout(client.stream.name, pending.sequence)
		return fmt.Errorf("publisher %s acknowledgement timeout for packet ID %d sequence %d",
			client.stream.name, packetID, pending.sequence)
	}
	if packetKind == "pubcomp" && !pending.pubrecSeen {
		client.ackMu.Unlock()
		return protocolError(col, errors.New("PUBCOMP arrived before PUBREC"))
	}
	if packetKind == "pubrec" && reason < 0x80 {
		if !pending.pubrecSeen {
			pending.pubrecSeen = true
			if !col.acknowledge(client.stream.name, pending.sequence, time.Now(), reason, false) {
				client.ackMu.Unlock()
				return protocolError(col, errors.New("PUBREC did not match an unacknowledged message"))
			}
		}
		pending.deadline = time.Now().Add(time.Duration(ackTimeoutMillis) * time.Millisecond)
		client.pending[packetID] = pending
		client.ackMu.Unlock()
		return client.client.Write(&mqtt.PubRel{Version: mqtt.ProtocolV5, PacketID: packetID}, time.Second)
	}
	delete(client.pending, packetID)
	client.ackMu.Unlock()
	complete := packetKind == "pubcomp"
	if !col.acknowledge(client.stream.name, pending.sequence, time.Now(), reason, complete) {
		return protocolError(col, errors.New("acknowledgement did not match a pending message"))
	}
	client.signal()
	return nil
}

func protocolError(col *collector, err error) error {
	col.protocolError()
	return err
}

func publishWindow(ctx context.Context, profile Profile, streams []streamSpec, publishers map[string]*runClient, col *collector, sequences map[string]*uint64, step int, rate uint64, millis int64, phase string) error {
	if rate == 0 {
		return errors.New("rate must be positive")
	}
	start := time.Now()
	var group sync.WaitGroup
	errorsByStream := make(chan error, len(streams))
	for _, stream := range streams {
		group.Add(1)
		go func(stream streamSpec) {
			defer group.Done()
			publisher := publishers[stream.name]
			count := uint64(rate * uint64(millis) / 1000)
			if phase == "offline" {
				count = uint64(profile.OfflineMessagesPerStream)
			}
			interval := time.Second / time.Duration(rate)
			streamSequence := sequences[stream.name]
			nextPacketID := uint32(1)
			for ordinal := uint64(1); ordinal <= count; ordinal++ {
				scheduled := start.Add(time.Duration(ordinal-1)*interval + time.Duration(stream.index)*interval/time.Duration(len(streams)))
				if err := waitUntil(ctx, scheduled); err != nil {
					errorsByStream <- err
					return
				}
				packetID, err := publisher.waitSlot(ctx, profile.MaxInflightPerPublisher, &nextPacketID)
				if err != nil {
					errorsByStream <- err
					return
				}
				*streamSequence = *streamSequence + 1
				sequence := *streamSequence
				payload := makePayload(profile.Seed, sequence, stream.index, profile.PayloadBytes)
				record := messageRecord{
					Stream:    stream.name,
					QoS:       stream.qos,
					Sequence:  sequence,
					Step:      step,
					Phase:     phase,
					scheduled: scheduled,
					interval:  int64(interval),
				}
				if err := col.add(record); err != nil {
					errorsByStream <- err
					return
				}
				qos := mqtt.QoSAtLeastOnce
				if stream.qos == 2 {
					qos = mqtt.QoSExactlyOnce
				}
				publisher.ackMu.Lock()
				sentAt := time.Now()
				col.markSent(stream.name, sequence, sentAt)
				publisher.pending[packetID] = pendingAck{
					sequence: sequence,
					deadline: sentAt.Add(time.Duration(profile.AckTimeoutMillis) * time.Millisecond),
				}
				err = publisher.client.Write(&mqtt.Publish{
					Version:  mqtt.ProtocolV5,
					QoS:      qos,
					PacketID: packetID,
					Topic:    stream.topic,
					Payload:  payload,
				}, time.Duration(profile.AckTimeoutMillis)*time.Millisecond)
				if err != nil {
					delete(publisher.pending, packetID)
					publisher.ackMu.Unlock()
					errorsByStream <- err
					return
				}
				publisher.ackMu.Unlock()
			}
			if err := waitUntil(ctx, start.Add(time.Duration(millis)*time.Millisecond)); err != nil {
				errorsByStream <- err
			}
		}(stream)
	}
	group.Wait()
	close(errorsByStream)
	for err := range errorsByStream {
		if err != nil {
			return err
		}
	}
	return nil
}

func (client *runClient) waitSlot(ctx context.Context, limit int, next *uint32) (mqtt.PacketIdentifier, error) {
	for {
		client.ackMu.Lock()
		if client.readFailure != nil {
			err := client.readFailure
			client.ackMu.Unlock()
			return 0, err
		}
		if cause := context.Cause(ctx); cause != nil {
			client.ackMu.Unlock()
			return 0, cause
		}
		if len(client.pending) < limit {
			for attempts := 0; attempts < 65535; attempts++ {
				candidate := mqtt.PacketIdentifier(*next)
				*next = *next%65535 + 1
				if _, used := client.pending[candidate]; !used {
					client.ackMu.Unlock()
					return candidate, nil
				}
			}
		}
		client.ackMu.Unlock()
		select {
		case <-ctx.Done():
			return 0, context.Cause(ctx)
		case <-client.changed:
		}
	}
}

func makePayload(seed, sequence uint64, index, size int) []byte {
	payload := make([]byte, size)
	binary.BigEndian.PutUint64(payload[:8], sequence)
	payload[8] = byte(index)
	for offset := 9; offset < len(payload); offset++ {
		payload[offset] = deterministicByte(seed, uint64(index), sequence, offset-9)
	}
	return payload
}

func deterministicByte(seed, index, sequence uint64, offset int) byte {
	value := seed + index*0x9E3779B97F4A7C15 + sequence*0xD1B54A32D192ED03 + uint64(offset)
	value = (value ^ (value >> 30)) * 0xBF58476D1CE4E5B9
	value = (value ^ (value >> 27)) * 0x94D049BB133111EB
	value ^= value >> 31
	return byte(value >> ((offset & 7) * 8))
}

func waitUntil(ctx context.Context, target time.Time) error {
	delay := time.Until(target)
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-timer.C:
		return nil
	}
}

func waitForContext(ctx context.Context, duration time.Duration) error {
	if duration <= 0 {
		return nil
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-timer.C:
		return nil
	}
}

func waitForPublisherExchanges(ctx context.Context, publishers map[string]*runClient, ackTimeoutMillis int64) error {
	deadline := time.Now().Add(2*time.Duration(ackTimeoutMillis)*time.Millisecond + time.Second)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		pending := false
		for _, client := range publishers {
			client.ackMu.Lock()
			failure := client.readFailure
			hasPending := len(client.pending) != 0
			client.ackMu.Unlock()
			if failure != nil {
				return fmt.Errorf("publisher %s reader failed: %w", client.stream.name, failure)
			}
			pending = pending || hasPending
		}
		if !pending {
			return nil
		}
		if !time.Now().Before(deadline) {
			return errors.New("publisher exchanges did not settle before acknowledgement deadline")
		}
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-ticker.C:
		}
	}
}

func waitForOfflineCompletion(ctx context.Context, col *collector, streams []streamSpec, perStream int, deadline time.Time) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := context.Cause(ctx); err != nil {
			return err
		}
		if !time.Now().Before(deadline) {
			return errors.New("offline receipts or QoS exchanges did not complete before reconnect deadline")
		}
		if col.offlineExchangesComplete(streams, perStream) {
			return nil
		}
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-ticker.C:
		}
	}
}

func runRecorded(writer *jsonl.Writer, phase string, execute func(time.Time) error) error {
	start := time.Now()
	if err := writer.Write(phaseEvent{Phase: phase, Event: "start", Timestamp: start.UTC().Format(time.RFC3339Nano)}); err != nil {
		return fmt.Errorf("write %s start: %w", phase, err)
	}
	runErr := execute(start)
	writeErr := writer.Write(phaseEvent{Phase: phase, Event: "end", Timestamp: time.Now().UTC().Format(time.RFC3339Nano)})
	return errors.Join(runErr, writeErr)
}

func waitForDrain(ctx context.Context, col *collector, millis int64) error {
	deadline := time.Now().Add(time.Duration(millis) * time.Millisecond)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for time.Now().Before(deadline) {
		if col.measureDelivered() {
			col.markDrainFinished(time.Now())
			return nil
		}
		select {
		case <-ctx.Done():
			col.markDrainFinished(time.Now())
			return ctx.Err()
		case <-ticker.C:
		}
	}
	col.markDrainFinished(time.Now())
	return nil
}

func readiness(ctx context.Context, target string, nodes, management []string, token string, deadline time.Time, creds credentials) error {
	client := &http.Client{Timeout: 2 * time.Second}
	if target == "narumq" {
		for {
			ready := true
			for _, address := range management {
				request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+address+"/health/ready", nil)
				if err != nil {
					return err
				}
				response, err := client.Do(request)
				if err != nil {
					ready = false
					continue
				}
				response.Body.Close()
				if response.StatusCode != http.StatusOK {
					ready = false
				}
			}
			if ready {
				break
			}
			if time.Now().After(deadline) {
				return errors.New("NaruMQ readiness timed out")
			}
			time.Sleep(250 * time.Millisecond)
		}
		for {
			leader := ""
			formed := true
			for _, address := range management {
				request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+address+"/api/v1/cluster", nil)
				if err != nil {
					return err
				}
				request.Header.Set("Authorization", "Bearer "+token)
				response, err := client.Do(request)
				if err != nil {
					formed = false
					continue
				}
				var body struct {
					Data struct {
						ClusterID    string  `json:"clusterId"`
						LeaderNodeID *string `json:"leaderNodeId"`
					} `json:"data"`
				}
				decodeErr := json.NewDecoder(response.Body).Decode(&body)
				response.Body.Close()
				if response.StatusCode != http.StatusOK || decodeErr != nil || body.Data.ClusterID != "narumq-bp03" || body.Data.LeaderNodeID == nil || *body.Data.LeaderNodeID == "" {
					formed = false
					continue
				}
				if leader == "" {
					leader = *body.Data.LeaderNodeID
				} else if leader != *body.Data.LeaderNodeID {
					formed = false
				}
			}
			if formed {
				break
			}
			if time.Now().After(deadline) {
				return errors.New("NaruMQ cluster formation timed out")
			}
			time.Sleep(250 * time.Millisecond)
		}
	}
	for publisherIndex := range nodes {
		for subscriberIndex := range nodes {
			if publisherIndex == subscriberIndex {
				continue
			}
			topic := fmt.Sprintf("bp03/probe/%d-%d", publisherIndex+1, subscriberIndex+1)
			publisherID := fmt.Sprintf("bp03-probe-pub-%d-%d", publisherIndex+1, subscriberIndex+1)
			subscriberID := fmt.Sprintf("bp03-probe-sub-%d-%d", publisherIndex+1, subscriberIndex+1)
			err := probePair(ctx, nodes[publisherIndex], nodes[subscriberIndex], topic, publisherID, subscriberID, deadline, creds)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func probePair(ctx context.Context, publisherAddress, subscriberAddress, topic, publisherID, subscriberID string, deadline time.Time, creds credentials) error {
	var lastErr error
	for time.Now().Before(deadline) {
		attemptCtx, cancel := context.WithDeadline(ctx, deadline)
		subscriber, _, err := mqttclient.Connect(attemptCtx, mqttclient.ConnectOptions{
			Address:          subscriberAddress,
			ClientID:         subscriberID,
			KeepAlive:        10,
			HandshakeTimeout: time.Second,
			CleanStart:       true,
			Username:         creds.Username,
			Password:         []byte(creds.Password),
		})
		if err == nil {
			request := &mqtt.Subscribe{
				Version:       mqtt.ProtocolV5,
				PacketID:      1,
				Subscriptions: []mqtt.Subscription{{Filter: topic, QoS: mqtt.QoSAtLeastOnce}},
			}
			err = subscriber.Write(request, time.Second)
			if err == nil {
				packet, readErr := subscriber.Read(time.Second)
				ack, ok := packet.(*mqtt.SubAck)
				switch {
				case readErr != nil:
					err = fmt.Errorf("read SUBACK: %w", readErr)
				case !ok:
					err = fmt.Errorf("expected SUBACK, received %T", packet)
				case ack.PacketID != 1 || len(ack.ReasonCodes) != 1 || ack.ReasonCodes[0] != byte(mqtt.QoSAtLeastOnce):
					err = fmt.Errorf("unexpected SUBACK %#v", ack)
				}
			}
		}
		var publisher *mqttclient.Client
		if err == nil {
			publisher, _, err = mqttclient.Connect(attemptCtx, mqttclient.ConnectOptions{
				Address:          publisherAddress,
				ClientID:         publisherID,
				KeepAlive:        10,
				HandshakeTimeout: time.Second,
				CleanStart:       true,
				Username:         creds.Username,
				Password:         []byte(creds.Password),
			})
		}
		payload := []byte(fmt.Sprintf("%s-%d", topic, time.Now().UnixNano()))
		var deliveryDeadline time.Time
		if err == nil {
			deliveryDeadline = time.Now().Add(5 * time.Second)
			err = publisher.Write(&mqtt.Publish{
				Version:  mqtt.ProtocolV5,
				QoS:      mqtt.QoSAtLeastOnce,
				PacketID: 1,
				Topic:    topic,
				Payload:  payload,
			}, time.Second)
		}
		if err == nil {
			ackTimeout := time.Until(deliveryDeadline)
			if ackTimeout <= 0 {
				err = errors.New("probe PUBACK exceeded the five-second deadline")
			} else {
				packet, readErr := publisher.Read(ackTimeout)
				ack, ok := packet.(*mqtt.PubAck)
				switch {
				case readErr != nil:
					err = fmt.Errorf("read PUBACK: %w", readErr)
				case !ok:
					err = fmt.Errorf("expected PUBACK, received %T", packet)
				case ack.PacketID != 1 || ack.ReasonCode >= 0x80:
					err = fmt.Errorf("unexpected PUBACK packet id=%d reason=%#02x", ack.PacketID, ack.ReasonCode)
				}
			}
		}
		if err == nil {
			deliveryTimeout := time.Until(deliveryDeadline)
			if deliveryTimeout <= 0 {
				err = errors.New("probe delivery exceeded the five-second deadline")
			} else {
				packet, readErr := subscriber.Read(deliveryTimeout)
				delivery, ok := packet.(*mqtt.Publish)
				switch {
				case readErr != nil:
					err = fmt.Errorf("read probe delivery: %w", readErr)
				case !ok:
					err = fmt.Errorf("expected probe PUBLISH, received %T", packet)
				case delivery.Topic != topic || delivery.QoS != mqtt.QoSAtLeastOnce || delivery.PacketID == 0 || delivery.DUP || delivery.Retain || string(delivery.Payload) != string(payload):
					err = fmt.Errorf("probe delivery mismatch topic=%q qos=%d packetId=%d dup=%v retain=%v", delivery.Topic, delivery.QoS, delivery.PacketID, delivery.DUP, delivery.Retain)
				default:
					if writeErr := subscriber.Write(&mqtt.PubAck{Version: mqtt.ProtocolV5, PacketID: delivery.PacketID}, time.Second); writeErr != nil {
						err = fmt.Errorf("acknowledge probe delivery: %w", writeErr)
					}
				}
			}
		}
		if publisher != nil {
			_ = publisher.Close()
		}
		if subscriber != nil {
			_ = subscriber.Close()
		}
		cancel()
		if err == nil {
			return nil
		}
		lastErr = err
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
	if lastErr != nil {
		return fmt.Errorf("cross-node MQTT probe %s -> %s timed out: %w", publisherAddress, subscriberAddress, lastErr)
	}
	return fmt.Errorf("cross-node MQTT probe %s -> %s timed out before an attempt", publisherAddress, subscriberAddress)
}

func scrapeBoundary(management []string, token, outputDir, window, boundary string) ([]walNodeInput, error) {
	client := &http.Client{Timeout: 5 * time.Second}
	nodes := make([]walNodeInput, len(management))
	for index, address := range management {
		request, err := http.NewRequest(http.MethodGet, "http://"+address+"/metrics", nil)
		if err != nil {
			return nil, err
		}
		request.Header.Set("Authorization", "Bearer "+token)
		response, err := client.Do(request)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(response.Body)
		response.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if response.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("metrics endpoint %s returned %s", address, response.Status)
		}
		filename := fmt.Sprintf("node%d-%s-%s.prom", index+1, window, boundary)
		path := filepath.Join(outputDir, "metrics", filename)
		if err := os.WriteFile(path, body, 0o644); err != nil {
			return nil, err
		}
		node := walNodeInput{Name: fmt.Sprintf("node%d", index+1)}
		if boundary == "start" {
			node.Start = body
		} else {
			node.End = body
		}
		nodes[index] = node
	}
	return nodes, nil
}

func pairScrapes(start, end []walNodeInput) []walNodeInput {
	if len(start) != len(end) {
		return nil
	}
	nodes := make([]walNodeInput, len(start))
	for index := range start {
		nodes[index] = walNodeInput{
			Name:  start[index].Name,
			Start: start[index].Start,
			End:   end[index].End,
		}
	}
	return nodes
}
