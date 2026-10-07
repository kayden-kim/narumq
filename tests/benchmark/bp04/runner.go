package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
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

type connectionRecord struct {
	ClientID        string `json:"clientId"`
	Node            string `json:"node"`
	ConnectedAt     string `json:"connectedAt,omitempty"`
	SubscribeSentAt string `json:"subscribeSentAt,omitempty"`
	SubAckAt        string `json:"subAckAt,omitempty"`
	ReasonCodes     []int  `json:"reasonCodes,omitempty"`
	Error           string `json:"error,omitempty"`
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
	var creds bp04RunnerCredentials
	if err := json.Unmarshal(credentialBytes, &creds); err != nil {
		return fmt.Errorf("decode credentials: %w", err)
	}
	if creds.Username == "" || creds.Password == "" {
		return errors.New("credentials username and password are required")
	}
	if *target == "narumq" && creds.ManagementToken == "" {
		return errors.New("managementToken is required for narumq")
	}
	timeout, err := bp04Timeout(profile)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return executeBP04(ctx, profile, *target, nodes, management, creds, *outputDir)
}

func parseEndpoints(raw string) ([]string, error) {
	parts := strings.Split(raw, ",")
	if len(parts) != bp04NodeCount {
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

func executeBP04(ctx context.Context, profile Profile, target string, nodes, management []string, creds bp04RunnerCredentials, outputDir string) (runErr error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := profile.Validate(); err != nil {
		return err
	}
	if target != "narumq" && target != "emqx" {
		return fmt.Errorf("unsupported target %q", target)
	}
	if len(nodes) != bp04NodeCount {
		return errors.New("exactly three broker nodes are required")
	}
	if target == "narumq" && len(management) != bp04NodeCount {
		return errors.New("exactly three management endpoints are required for narumq")
	}
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		return fmt.Errorf("create BP04 output directory: %w", err)
	}
	phaseWriter, err := jsonl.Open(filepath.Join(outputDir, "phases.jsonl"))
	if err != nil {
		return err
	}
	connectionWriter, err := jsonl.Open(filepath.Join(outputDir, "connections.jsonl"))
	if err != nil {
		return errors.Join(err, phaseWriter.Close())
	}
	runCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(context.Canceled)
	col := newCollector(profile, target)
	subscribers := workloadSubscribers(profile)
	publishers := workloadPublishers()
	clients := make([]*runClient, 0, len(bp04ClientIDs(profile))-bp04NodeCount*(bp04NodeCount-1)*2)
	publisherClients := make(map[string]*runClient, len(publishers))
	subscriberClients := make([]*runClient, len(subscribers))
	var stopKeepAlive func()
	var shutdownOnce sync.Once
	var shutdownErr error
	shutdown := func() error {
		shutdownOnce.Do(func() {
			if stopKeepAlive != nil {
				stopKeepAlive()
				stopKeepAlive = nil
			}
			shutdownErr = closeClients(clients, time.Second)
		})
		return shutdownErr
	}
	var cleanupOnce sync.Once
	cleanup := func() {
		cleanupOnce.Do(func() {
			if cause := context.Cause(runCtx); cause != nil && !errors.Is(runErr, cause) {
				runErr = errors.Join(cause, runErr)
			}
			runErr = errors.Join(runErr, shutdown())
			if runErr != nil {
				col.executionFailure("workload execution failed")
			}
			if err := col.write(outputDir); err != nil {
				runErr = errors.Join(runErr, fmt.Errorf("write BP04 workload artifacts: %w", err))
			}
			runErr = errors.Join(runErr, connectionWriter.Close(), phaseWriter.Close())
			if !col.valid() {
				runErr = errors.Join(runErr, errors.New("workload validity requirements were not met"))
			}
		})
	}
	defer cleanup()

	readyCtx, cancelReady := context.WithTimeout(runCtx, time.Duration(profile.ReadyTimeoutMillis)*time.Millisecond)
	if err := runRecorded(phaseWriter, "readiness", func(start time.Time) error {
		return readiness(readyCtx, target, nodes, management, creds.ManagementToken, start.Add(time.Duration(profile.ReadyTimeoutMillis)*time.Millisecond), creds)
	}); err != nil {
		cancelReady()
		return err
	}
	cancelReady()

	if err := runRecorded(phaseWriter, "idle-baseline", func(time.Time) error {
		return waitForContext(runCtx, time.Duration(profile.IdleBaselineMillis)*time.Millisecond)
	}); err != nil {
		return err
	}

	if err := runRecorded(phaseWriter, "setup", func(_ time.Time) error {
		setupStarted := time.Now()
		col.recordSetupStart(setupStarted)
		defer func() { col.recordSetupEnd(time.Now()) }()
		setupCtx, cancelSetup := context.WithTimeout(runCtx, time.Duration(profile.SetupTimeoutMillis)*time.Millisecond)
		defer cancelSetup()
		connectAndRecord := func(info connectInfo, address string, filters []string) (*runClient, error) {
			client, connectedAt, connectErr := connectClient(setupCtx, address, info, profile, creds)
			record := connectionRecord{ClientID: info.id, Node: fmt.Sprintf("node%d", info.nodeIndex+1)}
			if connectErr != nil {
				record.Error = connectErr.Error()
				if err := connectionWriter.Write(record); err != nil {
					return nil, errors.Join(connectErr, err)
				}
				return nil, connectErr
			}
			record.ConnectedAt = connectedAt.UTC().Format(time.RFC3339Nano)
			clients = append(clients, client)
			if len(filters) > 0 {
				sentAt, ackedAt, reasons, subscribeErr := subscribeClient(client, filters)
				record.SubscribeSentAt = sentAt.UTC().Format(time.RFC3339Nano)
				if !ackedAt.IsZero() {
					record.SubAckAt = ackedAt.UTC().Format(time.RFC3339Nano)
				}
				record.ReasonCodes = reasonCodesAsInts(reasons)
				if subscribeErr != nil {
					record.Error = subscribeErr.Error()
					if err := connectionWriter.Write(record); err != nil {
						return nil, errors.Join(subscribeErr, err)
					}
					return nil, subscribeErr
				}
				col.recordSetupSubscribe(sentAt, ackedAt, reasons)
			}
			if err := connectionWriter.Write(record); err != nil {
				return nil, err
			}
			col.recordSetupSuccess()
			return client, nil
		}
		for nodeIndex := range nodes {
			info := connectInfo{id: fmt.Sprintf("bp04-route-pub-%d", nodeIndex+1), nodeIndex: nodeIndex, streamIndex: 4, isPublisher: true}
			client, err := connectAndRecord(info, nodes[nodeIndex], nil)
			if err != nil {
				return fmt.Errorf("connect route publisher %s: %w", info.id, err)
			}
			client.startReader(profile, col, cancel)
			publisherClients[info.id] = client
		}
		for _, spec := range publishers {
			info := connectInfo{id: spec.id, class: spec.class, nodeIndex: spec.nodeIndex, streamIndex: spec.streamIndex, isPublisher: true}
			client, err := connectAndRecord(info, nodes[spec.nodeIndex], nil)
			if err != nil {
				return fmt.Errorf("connect workload publisher %s: %w", spec.id, err)
			}
			client.startReader(profile, col, cancel)
			publisherClients[spec.class] = client
		}
		for index, spec := range subscribers {
			info := connectInfo{id: spec.id, class: spec.class, nodeIndex: spec.nodeIndex, subscriberIndex: index, streamIndex: streamIndexForClass(spec.class)}
			client, err := connectAndRecord(info, nodes[spec.nodeIndex], spec.filters)
			if err != nil {
				return fmt.Errorf("connect or subscribe workload subscriber %s: %w", spec.id, err)
			}
			client.startReader(profile, col, cancel)
			subscriberClients[index] = client
		}
		for probe := 0; probe < profile.RouteProbes; probe++ {
			nodeIndex := probe % bp04NodeCount
			info := connectInfo{id: fmt.Sprintf("bp04-route-sub-%03d", probe), nodeIndex: nodeIndex, isRoute: true}
			client, err := connectAndRecord(info, nodes[nodeIndex], nil)
			if err != nil {
				return fmt.Errorf("connect route subscriber %s: %w", info.id, err)
			}
			client.startReader(profile, col, cancel)
			subscriberClients = append(subscriberClients, client)
		}
		all := append([]*runClient(nil), clients...)
		stopKeepAlive = startKeepAlive(runCtx, all, time.Duration(profile.KeepAliveSeconds/2)*time.Second, col, cancel)
		return nil
	}); err != nil {
		return err
	}

	if err := runRecorded(phaseWriter, "subscribed-idle", func(time.Time) error {
		return waitForContext(runCtx, time.Duration(profile.SubscribedIdleMillis)*time.Millisecond)
	}); err != nil {
		return err
	}

	sequences := make(map[string]*uint64, len(publishers))
	for _, publisher := range publishers {
		sequence := uint64(0)
		sequences[publisher.class] = &sequence
	}
	for index, rate := range profile.RatesPerStreamPerSecond {
		step := index + 1
		warmupPhase := fmt.Sprintf("step-%02d-warmup", step)
		if err := runRecorded(phaseWriter, warmupPhase, func(time.Time) error {
			if err := publishWindow(runCtx, profile, publishers, publisherClients, col, sequences, step, rate, profile.WarmupMillis, warmupPhase); err != nil {
				return err
			}
			col.finishWarmup(step, time.Now())
			return nil
		}); err != nil {
			return err
		}
		measurePhase := fmt.Sprintf("step-%02d-measure", step)
		if err := runRecorded(phaseWriter, measurePhase, func(time.Time) error {
			return publishWindow(runCtx, profile, publishers, publisherClients, col, sequences, step, rate, profile.MeasureMillis, measurePhase)
		}); err != nil {
			return err
		}
		drainPhase := fmt.Sprintf("step-%02d-drain", step)
		if err := runRecorded(phaseWriter, drainPhase, func(time.Time) error {
			if err := waitForDrain(runCtx, col, step, profile.DrainMillis); err != nil {
				return err
			}
			col.finishStep(step, time.Now())
			return nil
		}); err != nil {
			return err
		}
	}

	if err := runRecorded(phaseWriter, "route-propagation", func(time.Time) error {
		return runRouteProbes(runCtx, profile, nodes, publisherClients, subscriberClients[len(subscribers):], col)
	}); err != nil {
		return err
	}
	if err := runRecorded(phaseWriter, "final_check", func(time.Time) error {
		for _, client := range clients {
			if failure := client.failure(); failure != nil {
				col.recordFinalCheck(false)
				return fmt.Errorf("client %s is disconnected at final_check: %w", client.id, failure)
			}
		}
		if !col.allRouteProbesSucceeded() {
			col.recordFinalCheck(false)
			return errors.New("route propagation probes did not all succeed at final_check")
		}
		col.recordFinalCheck(true)
		return nil
	}); err != nil {
		return err
	}
	if err := runRecorded(phaseWriter, "cleanup", func(time.Time) error { return shutdown() }); err != nil {
		return err
	}
	cleanup()
	return runErr
}

func reasonCodesAsInts(codes []byte) []int {
	result := make([]int, len(codes))
	for index, code := range codes {
		result[index] = int(code)
	}
	return result
}

func streamIndexForClass(class string) int {
	switch class {
	case classFanout:
		return 0
	case classWildcard:
		return 1
	case classOverlap:
		return 2
	case classShared:
		return 3
	default:
		return -1
	}
}

func publishWindow(ctx context.Context, profile Profile, streams []publisherSpec, publishers map[string]*runClient, col *collector, sequences map[string]*uint64, step int, rate uint64, millis int64, phase string) error {
	if rate == 0 {
		return errors.New("rate must be positive")
	}
	start := time.Now()
	var group sync.WaitGroup
	errorsByStream := make(chan error, len(streams))
	for _, stream := range streams {
		group.Add(1)
		go func(stream publisherSpec) {
			defer group.Done()
			publisher := publishers[stream.class]
			if publisher == nil {
				errorsByStream <- fmt.Errorf("publisher for class %s is missing", stream.class)
				return
			}
			sequence := sequences[stream.class]
			if sequence == nil {
				errorsByStream <- fmt.Errorf("sequence counter for class %s is missing", stream.class)
				return
			}
			count := rate * uint64(millis) / 1000
			interval := time.Second / time.Duration(rate)
			for ordinal := uint64(1); ordinal <= count; ordinal++ {
				scheduled := start.Add(time.Duration(ordinal-1)*interval + time.Duration(stream.streamIndex)*interval/4)
				if err := waitUntil(ctx, scheduled); err != nil {
					errorsByStream <- err
					return
				}
				*sequence++
				messageSequence := *sequence
				topic := publishTopic(stream.class, messageSequence)
				if err := col.addMessage(stream.class, messageSequence, step, phase, topic, scheduled); err != nil {
					errorsByStream <- err
					return
				}
				sentAt := time.Now()
				col.markSent(stream.class, messageSequence, sentAt)
				col.recordScheduleLag(step, sentAt.Sub(scheduled), interval)
				if err := publisher.client.Write(&mqtt.Publish{
					Version: mqtt.ProtocolV5,
					QoS:     mqtt.QoSAtMostOnce,
					Topic:   topic,
					Payload: makePayload(profile.Seed, messageSequence, stream.streamIndex, profile.PayloadBytes),
				}, 5*time.Second); err != nil {
					col.transportError()
					errorsByStream <- fmt.Errorf("publish %s sequence %d: %w", stream.class, messageSequence, err)
					return
				}
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

func waitForDrain(ctx context.Context, col *collector, step int, millis int64) error {
	deadline := time.Now().Add(time.Duration(millis) * time.Millisecond)
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		if col.stepComplete(step) {
			return nil
		}
		if !time.Now().Before(deadline) {
			return nil
		}
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-ticker.C:
		}
	}
}

func readiness(ctx context.Context, target string, nodes, management []string, token string, deadline time.Time, creds bp04RunnerCredentials) error {
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
				_ = response.Body.Close()
				if response.StatusCode != http.StatusOK {
					ready = false
				}
			}
			if ready {
				break
			}
			if !time.Now().Before(deadline) {
				return errors.New("NaruMQ readiness timed out")
			}
			if err := waitForContext(ctx, 250*time.Millisecond); err != nil {
				return err
			}
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
				_ = response.Body.Close()
				if response.StatusCode != http.StatusOK || decodeErr != nil || body.Data.ClusterID != bp04ClusterID || body.Data.LeaderNodeID == nil || *body.Data.LeaderNodeID == "" {
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
			if !time.Now().Before(deadline) {
				return errors.New("NaruMQ cluster formation timed out")
			}
			if err := waitForContext(ctx, 250*time.Millisecond); err != nil {
				return err
			}
		}
	}
	for publisherIndex := range nodes {
		for subscriberIndex := range nodes {
			if publisherIndex == subscriberIndex {
				continue
			}
			topic := fmt.Sprintf("bp04/probe/%d-%d", publisherIndex+1, subscriberIndex+1)
			publisherID := fmt.Sprintf("bp04-probe-pub-%d-%d", publisherIndex+1, subscriberIndex+1)
			subscriberID := fmt.Sprintf("bp04-probe-sub-%d-%d", publisherIndex+1, subscriberIndex+1)
			if err := probePair(ctx, nodes[publisherIndex], nodes[subscriberIndex], topic, publisherID, subscriberID, deadline, creds); err != nil {
				return err
			}
		}
	}
	return nil
}

func probePair(ctx context.Context, publisherAddress, subscriberAddress, topic, publisherID, subscriberID string, deadline time.Time, creds bp04RunnerCredentials) error {
	var lastErr error
	for time.Now().Before(deadline) {
		attemptCtx, cancel := context.WithDeadline(ctx, deadline)
		subscriber, _, err := mqttclient.Connect(attemptCtx, mqttclient.ConnectOptions{
			Address: subscriberAddress, ClientID: subscriberID, ProtocolVersion: mqtt.ProtocolV5,
			KeepAlive: 10, HandshakeTimeout: time.Second, CleanStart: true,
			Username: creds.Username, Password: []byte(creds.Password),
		})
		if err == nil {
			err = subscriber.Write(&mqtt.Subscribe{Version: mqtt.ProtocolV5, PacketID: 1, Subscriptions: []mqtt.Subscription{{Filter: topic, QoS: mqtt.QoSAtLeastOnce}}}, time.Second)
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
				Address: publisherAddress, ClientID: publisherID, ProtocolVersion: mqtt.ProtocolV5,
				KeepAlive: 10, HandshakeTimeout: time.Second, CleanStart: true,
				Username: creds.Username, Password: []byte(creds.Password),
			})
		}
		payload := []byte(fmt.Sprintf("%s-%d", topic, time.Now().UnixNano()))
		var deliveryDeadline time.Time
		if err == nil {
			deliveryDeadline = time.Now().Add(5 * time.Second)
			err = publisher.Write(&mqtt.Publish{Version: mqtt.ProtocolV5, QoS: mqtt.QoSAtLeastOnce, PacketID: 1, Topic: topic, Payload: payload}, time.Second)
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
			return context.Cause(ctx)
		case <-time.After(200 * time.Millisecond):
		}
	}
	if lastErr != nil {
		return fmt.Errorf("cross-node MQTT probe %s -> %s timed out: %w", publisherAddress, subscriberAddress, lastErr)
	}
	return fmt.Errorf("cross-node MQTT probe %s -> %s timed out before an attempt", publisherAddress, subscriberAddress)
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

func validateStreamPayload(profile Profile, index int, payload []byte) (uint64, error) {
	if len(payload) != profile.PayloadBytes || len(payload) < 9 || int(payload[8]) != index {
		return 0, errors.New("delivery payload size or stream index mismatch")
	}
	sequence := binary.BigEndian.Uint64(payload[:8])
	if sequence == 0 {
		return 0, errors.New("delivery sequence is zero")
	}
	for offset := 9; offset < len(payload); offset++ {
		if payload[offset] != deterministicByte(profile.Seed, uint64(index), sequence, offset-9) {
			return 0, errors.New("delivery payload deterministic bytes mismatch")
		}
	}
	return sequence, nil
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

func runRecorded(writer *jsonl.Writer, phase string, execute func(time.Time) error) error {
	start := time.Now()
	if err := writer.Write(phaseEvent{Phase: phase, Event: "start", Timestamp: start.UTC().Format(time.RFC3339Nano)}); err != nil {
		return fmt.Errorf("write %s start: %w", phase, err)
	}
	executeErr := execute(start)
	writeErr := writer.Write(phaseEvent{Phase: phase, Event: "end", Timestamp: time.Now().UTC().Format(time.RFC3339Nano)})
	return errors.Join(executeErr, writeErr)
}

func bp04Timeout(profile Profile) (time.Duration, error) {
	components := []time.Duration{
		time.Duration(profile.ReadyTimeoutMillis) * time.Millisecond,
		time.Duration(profile.IdleBaselineMillis) * time.Millisecond,
		time.Duration(profile.SetupTimeoutMillis) * time.Millisecond,
		time.Duration(profile.SubscribedIdleMillis) * time.Millisecond,
	}
	for range profile.RatesPerStreamPerSecond {
		components = append(components,
			time.Duration(profile.WarmupMillis)*time.Millisecond,
			time.Duration(profile.MeasureMillis)*time.Millisecond,
			time.Duration(profile.DrainMillis)*time.Millisecond,
		)
	}
	for range profile.RouteProbes {
		components = append(components, time.Duration(profile.RouteTimeoutMillis)*time.Millisecond)
	}
	components = append(components, 3*time.Minute)
	var total time.Duration
	for _, component := range components {
		if component < 0 || total > time.Duration(math.MaxInt64)-component {
			return 0, errors.New("profile target timeout exceeds supported range")
		}
		total += component
	}
	return total, nil
}
