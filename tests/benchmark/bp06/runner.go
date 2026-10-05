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
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"narumq/internal/domain/mqtt"
	"narumq/tests/benchmark/internal/jsonl"
	"narumq/tests/benchmark/internal/mqttclient"
)

const bp06AckPath = "round-%02d-ack.json"

type credentials struct {
	Username        string `json:"username"`
	Password        string `json:"password"`
	ManagementToken string `json:"managementToken"`
}

type clientSpec struct {
	id, role, stream, topic string
	index, qos              int
	version                 mqtt.ProtocolVersion
	persistent              bool
}

type roundMarker struct {
	Round     int    `json:"round"`
	Node      string `json:"node"`
	State     string `json:"state"`
	WrittenAt string `json:"writtenAt"`
}

type currentRound struct {
	mu     sync.Mutex
	marker roundMarker
}

func (r *currentRound) snapshot() roundMarker { r.mu.Lock(); defer r.mu.Unlock(); return r.marker }
func (r *currentRound) set(marker roundMarker) {
	r.mu.Lock()
	if marker.Round >= r.marker.Round {
		r.marker = marker
	}
	r.mu.Unlock()
}

type phaseEvent struct {
	Phase     string `json:"phase"`
	Event     string `json:"event"`
	Timestamp string `json:"timestamp"`
}

type failureCompletion struct {
	Status    string `json:"status"`
	Reason    string `json:"reason"`
	WrittenAt string `json:"writtenAt"`
}

func workloadClients(profile Profile) []clientSpec {
	streams := []string{"v5-q0", "v5-q1", "v5-q2", "v311-q1"}
	specs := make([]clientSpec, 0, profile.clients())
	for _, role := range []string{"publisher", "subscriber"} {
		for index, stream := range streams {
			version := mqtt.ProtocolV5
			qos := index
			if index == 3 {
				version = mqtt.ProtocolV311
				qos = 1
			}
			prefix := "bp06-pub-"
			if role == "subscriber" {
				prefix = "bp06-sub-"
			}
			specs = append(specs, clientSpec{id: prefix + stream, role: role, stream: stream, topic: "bp06/stream/" + stream, index: index, qos: qos, version: version, persistent: role == "subscriber"})
		}
	}
	for i := 1; i <= profile.IdleClients; i++ {
		specs = append(specs, clientSpec{id: fmt.Sprintf("bp06-idle-%02d", i), role: "idle", version: mqtt.ProtocolV5, persistent: true})
	}
	return specs
}

func bp06Timeout(profile Profile) time.Duration {
	roundMillis := profile.MarkerAckTimeoutMillis + profile.KillTimeoutMillis + profile.DownMillis + profile.ReadyTimeoutMillis + profile.ResyncTimeoutMillis + profile.InterRoundSettleMillis
	return 180*time.Second + time.Duration(profile.WarmupMillis+profile.SteadyMillis+int64(profile.rounds())*roundMillis+profile.SettleMillis)*time.Millisecond
}

func runCommand(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("run", flag.ContinueOnError)
	flags.SetOutput(stderr)
	profilePath := flags.String("profile", "", "profile JSON")
	target := flags.String("target", "", "narumq or emqx")
	nodeList := flags.String("nodes", "", "three MQTT endpoints")
	managementList := flags.String("management", "", "three NaruMQ management endpoints")
	credentialPath := flags.String("credentials", "", "runner credentials JSON")
	outputDir := flags.String("output", "", "artifact output directory")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected run arguments: %v", flags.Args())
	}
	if *profilePath == "" || *nodeList == "" || *credentialPath == "" || *outputDir == "" {
		return errors.New("run requires --profile, --target, --nodes, --credentials and --output")
	}
	if *target != "narumq" && *target != "emqx" {
		return fmt.Errorf("unsupported target %q", *target)
	}
	nodes, err := parseEndpoints(*nodeList)
	if err != nil {
		return err
	}
	var management []string
	if *target == "narumq" {
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
	data, err := os.ReadFile(*credentialPath)
	if err != nil {
		return err
	}
	var creds credentials
	if err := json.Unmarshal(data, &creds); err != nil {
		return err
	}
	if creds.Username == "" || creds.Password == "" || (*target == "narumq" && creds.ManagementToken == "") {
		return errors.New("runner credentials are incomplete")
	}
	ctx, cancel := context.WithTimeout(context.Background(), bp06Timeout(profile))
	defer cancel()
	err = executeBP06(ctx, profile, *target, nodes, management, creds, *outputDir)
	if err == nil {
		_, err = fmt.Fprintln(stdout, "BP-06 workload valid")
	}
	return err
}

func executeBP06(ctx context.Context, profile Profile, target string, nodes, management []string, creds credentials, outputDir string) (runErr error) {
	if err := profile.Validate(); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Join(outputDir, "control"), 0o755); err != nil {
		return err
	}
	phases, err := jsonl.Open(filepath.Join(outputDir, "phases.jsonl"))
	if err != nil {
		return err
	}
	defer func() { runErr = errors.Join(runErr, phases.Close()) }()
	health, err := jsonl.Open(filepath.Join(outputDir, "health.jsonl"))
	if err != nil {
		return err
	}
	defer func() { runErr = errors.Join(runErr, health.Close()) }()
	events, err := jsonl.Open(filepath.Join(outputDir, "connections.jsonl"))
	if err != nil {
		return err
	}
	defer func() { runErr = errors.Join(runErr, events.Close()) }()
	col := newCollector(target, profile)
	var clients []*sessionClient
	var round currentRound
	healthURLs := make([]string, len(nodes))
	for i, node := range nodes {
		if target == "narumq" {
			healthURLs[i] = "http://" + management[i] + "/health/ready"
		} else {
			host, _, err := net.SplitHostPort(node)
			if err != nil {
				return err
			}
			healthURLs[i] = "http://" + net.JoinHostPort(host, "18083") + "/api/v5/load_rebalance/availability_check"
		}
	}
	dialer := newReadinessDialer(target, nodes, healthURLs, creds.ManagementToken, time.Duration(profile.HealthPollMillis)*time.Millisecond, health)
	dialCtx, stopDial := context.WithCancel(ctx)
	defer func() { stopDial(); dialer.Close() }()
	sessionCtx, stopSessions := context.WithCancel(ctx)
	var publishCancel context.CancelFunc
	var publishers sync.WaitGroup
	defer func() {
		if publishCancel != nil {
			publishCancel()
			publishers.Wait()
		}
		cleanupErr := runRecorded(phases, "cleanup", func(time.Time) error {
			var failures []error
			for _, client := range clients {
				if err := client.close(); err != nil {
					failures = append(failures, err)
				}
			}
			stopSessions()
			return errors.Join(failures...)
		})
		runErr = errors.Join(runErr, cleanupErr)
		if runErr != nil {
			col.failure(runErr.Error())
		}
		if err := col.write(outputDir); err != nil {
			runErr = errors.Join(runErr, err)
		}
		if !col.summary().Valid && runErr == nil {
			runErr = errors.New("workload validity requirements were not met")
		}
	}()
	if err := runRecorded(phases, "readiness", func(time.Time) error {
		readyCtx, cancel := context.WithTimeout(ctx, time.Duration(profile.ReadyTimeoutMillis)*time.Millisecond)
		defer cancel()
		if err := readiness(readyCtx, target, nodes, management, creds.ManagementToken, time.Now().Add(time.Duration(profile.ReadyTimeoutMillis)*time.Millisecond), creds); err != nil {
			return err
		}
		if target == "narumq" {
			if err := activateSessionMetadata(readyCtx, management[0], creds.ManagementToken); err != nil {
				return err
			}
		}
		dialer.Start(dialCtx)
		return dialer.WaitAllReady(readyCtx)
	}); err != nil {
		return err
	}
	if err := runRecorded(phases, "setup", func(time.Time) error {
		setupCtx, cancel := context.WithTimeout(sessionCtx, time.Duration(profile.ReadyTimeoutMillis)*time.Millisecond)
		defer cancel()
		for index, spec := range workloadClients(profile) {
			if err := setupCtx.Err(); err != nil {
				return err
			}
			client := newSessionClient(spec, index, profile, creds, dialer, col, &round, events)
			if err := client.start(sessionCtx); err != nil {
				return fmt.Errorf("setup %s: %w", spec.id, err)
			}
			clients = append(clients, client)
		}
		return nil
	}); err != nil {
		return err
	}
	pubCtx, cancelPublish := context.WithCancel(ctx)
	publishCancel = cancelPublish
	for _, client := range clients[:4] {
		publishers.Add(1)
		go func(client *sessionClient) {
			defer publishers.Done()
			if err := publishSchedule(pubCtx, client, profile); err != nil && !errors.Is(err, context.Canceled) {
				col.failure(err.Error())
			}
		}(client)
	}
	if err := runRecorded(phases, "warmup", func(time.Time) error {
		if err := waitForContext(ctx, time.Duration(profile.WarmupMillis)*time.Millisecond); err != nil {
			return err
		}
		return writeAtomicJSON(filepath.Join(outputDir, "control", "workload-ready.json"), struct {
			WrittenAt   string `json:"writtenAt"`
			Connections int    `json:"connections"`
		}{time.Now().UTC().Format(time.RFC3339Nano), len(clients)})
	}); err != nil {
		return err
	}
	if err := runRecorded(phases, "failure", func(time.Time) error { return watchFailure(ctx, profile, outputDir, &round, col) }); err != nil {
		return err
	}
	publishCancel()
	publishers.Wait()
	if err := runRecorded(phases, "settle", func(time.Time) error {
		settleCtx, cancel := context.WithTimeout(ctx, time.Duration(profile.SettleMillis)*time.Millisecond)
		defer cancel()
		for !col.settled() {
			if err := waitForContext(settleCtx, 20*time.Millisecond); err != nil {
				return fmt.Errorf("settle: %w", err)
			}
		}
		return nil
	}); err != nil {
		return err
	}
	return runRecorded(phases, "final_check", func(time.Time) error {
		for index, client := range clients {
			if !client.connected() {
				return fmt.Errorf("client %s is disconnected at final_check", workloadClients(profile)[index].id)
			}
		}
		return nil
	})
}

func publishSchedule(ctx context.Context, client *sessionClient, profile Profile) error {
	interval := time.Second / time.Duration(profile.PublishRatePerStreamPerSecond)
	start := time.Now()
	var sequence uint64
	for tick := uint64(0); ; tick++ {
		scheduled := start.Add(time.Duration(tick) * interval)
		if delay := time.Until(scheduled); delay > 0 {
			if err := waitForContext(ctx, delay); err != nil {
				return err
			}
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		sequence++
		if err := client.publish(sequence, scheduled); err != nil {
			return err
		}
	}
}

func watchFailure(ctx context.Context, profile Profile, outputDir string, round *currentRound, col *collector) error {
	acknowledged := make(map[int]bool, profile.rounds())
	downObserved := make(map[int]bool, profile.rounds())
	for {
		for n := 1; n <= profile.rounds(); n++ {
			var marker roundMarker
			path := filepath.Join(outputDir, "control", fmt.Sprintf("round-%02d.json", n))
			if readControlJSON(path, &marker) != nil || marker.Round != n {
				continue
			}
			round.set(marker)
			if marker.State == "killing" && !acknowledged[n] {
				observedAt := time.Now()
				ack := struct {
					Round      int    `json:"round"`
					State      string `json:"state"`
					ObservedAt string `json:"observedAt"`
				}{Round: n, State: "killing", ObservedAt: observedAt.UTC().Format(time.RFC3339Nano)}
				if err := writeAtomicJSON(filepath.Join(outputDir, "control", fmt.Sprintf(bp06AckPath, n)), ack); err != nil {
					return fmt.Errorf("acknowledge round %02d killing marker: %w", n, err)
				}
				acknowledged[n] = true
			}
			if marker.State == "down" && !downObserved[n] {
				col.recordDownObserved(n, time.Now())
				downObserved[n] = true
			}
		}
		var complete failureCompletion
		if readControlJSON(filepath.Join(outputDir, "control", "failure-complete.json"), &complete) == nil {
			if complete.Status != "success" {
				return fmt.Errorf("failure controller failed: %s", complete.Reason)
			}
			return nil
		}
		if err := waitForContext(ctx, 100*time.Millisecond); err != nil {
			return err
		}
	}
}

func readControlJSON(path string, value any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, value)
}

func writeAtomicJSON(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	temp := path + ".tmp"
	if err := os.WriteFile(temp, append(data, '\n'), 0o644); err != nil {
		return err
	}
	deadline := time.Now().Add(time.Second)
	for {
		err := os.Rename(temp, path)
		if err == nil {
			return nil
		}
		// Windows readers can temporarily prevent replacement of an open marker.
		sharingViolation := errors.Is(err, syscall.Errno(32))
		if runtime.GOOS != "windows" || (!errors.Is(err, os.ErrPermission) && !sharingViolation) || !time.Now().Before(deadline) {
			_ = os.Remove(temp)
			return err
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func activateSessionMetadata(ctx context.Context, address, token string) error {
	client := &http.Client{Timeout: 5 * time.Second}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+address+"/api/v1/cluster/features/session-metadata", nil)
	if err != nil {
		return err
	}
	request.Header.Set("Authorization", "Bearer "+token)
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	_, readErr := io.Copy(io.Discard, response.Body)
	response.Body.Close()
	if readErr != nil {
		return readErr
	}
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("activate session metadata: HTTP %d", response.StatusCode)
	}
	request, err = http.NewRequestWithContext(ctx, http.MethodGet, "http://"+address+"/api/v1/cluster/features", nil)
	if err != nil {
		return err
	}
	request.Header.Set("Authorization", "Bearer "+token)
	response, err = client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	var body struct {
		Data struct {
			ActiveFeatures []string `json:"activeFeatures"`
		} `json:"data"`
	}
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		return err
	}
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("read cluster features: HTTP %d", response.StatusCode)
	}
	for _, feature := range body.Data.ActiveFeatures {
		if feature == "session-metadata" {
			return nil
		}
	}
	return errors.New("session-metadata was not active after activation")
}

func waitForContext(ctx context.Context, duration time.Duration) error {
	if duration <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
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
		expected := deterministicByte(profile.Seed, uint64(index), sequence, offset-9)
		if payload[offset] != expected {
			return 0, errors.New("delivery payload deterministic bytes mismatch")
		}
	}
	return sequence, nil
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
				request.Header.Set("Authorization", "Bearer "+token)
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
				if response.StatusCode != http.StatusOK || decodeErr != nil || body.Data.ClusterID != "narumq-bp06" || body.Data.LeaderNodeID == nil || *body.Data.LeaderNodeID == "" {
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
			topic := fmt.Sprintf("bp06/probe/%d-%d", publisherIndex+1, subscriberIndex+1)
			publisherID := fmt.Sprintf("bp06-probe-pub-%d-%d", publisherIndex+1, subscriberIndex+1)
			subscriberID := fmt.Sprintf("bp06-probe-sub-%d-%d", publisherIndex+1, subscriberIndex+1)
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
