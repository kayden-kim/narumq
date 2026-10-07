package main

import (
	"context"
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

type phaseEvent struct {
	Phase     string `json:"phase"`
	Event     string `json:"event"`
	Timestamp string `json:"timestamp"`
}

type connectionRecord struct {
	ClientID                  string `json:"clientId"`
	Node                      string `json:"node"`
	ConnectedAt               string `json:"connectedAt,omitempty"`
	SubscribeSentAt           string `json:"subscribeSentAt,omitempty"`
	SubAckAt                  string `json:"subAckAt,omitempty"`
	ReasonCodes               []int  `json:"reasonCodes,omitempty"`
	Error                     string `json:"error,omitempty"`
	Tenant                    string `json:"tenant"`
	Transport                 string `json:"transport"`
	Phase                     string `json:"phase"`
	ConnectLatencyNanoseconds int64  `json:"connectLatencyNanoseconds,omitempty"`
	SessionPresent            bool   `json:"sessionPresent"`
}

func runCommand(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("run", flag.ContinueOnError)
	flags.SetOutput(stderr)
	profilePath := flags.String("profile", "", "profile JSON")
	target := flags.String("target", "", "narumq or emqx")
	nodeList := flags.String("nodes", "", "three TCP broker host:port endpoints")
	tlsNodeList := flags.String("tls-nodes", "", "three TLS broker host:port endpoints")
	managementList := flags.String("management", "", "three management host:port endpoints")
	credentialPath := flags.String("credentials", "", "credentials JSON")
	outputDir := flags.String("output", "", "artifact output directory")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected run arguments: %v", flags.Args())
	}
	if *profilePath == "" || *target == "" || *nodeList == "" || *tlsNodeList == "" || *credentialPath == "" || *outputDir == "" {
		return errors.New("run requires --profile, --target, --nodes, --tls-nodes, --credentials and --output")
	}
	if *target != "narumq" && *target != "emqx" {
		return fmt.Errorf("unsupported target %q", *target)
	}
	nodes, err := parseEndpoints(*nodeList)
	if err != nil {
		return fmt.Errorf("--nodes: %w", err)
	}
	tlsNodes, err := parseEndpoints(*tlsNodeList)
	if err != nil {
		return fmt.Errorf("--tls-nodes: %w", err)
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
	data, err := os.ReadFile(*credentialPath)
	if err != nil {
		return fmt.Errorf("read credentials: %w", err)
	}
	var creds bp02RunnerCredentials
	if err := json.Unmarshal(data, &creds); err != nil {
		return fmt.Errorf("decode credentials: %w", err)
	}
	if len(creds.Tenants) != 2 || creds.Tenants["bp02-a"].Username == "" || creds.Tenants["bp02-a"].Password == "" || creds.Tenants["bp02-b"].Username == "" || creds.Tenants["bp02-b"].Password == "" {
		return errors.New("credentials for both BP02 tenants are required")
	}
	if *target == "narumq" && creds.ManagementToken == "" {
		return errors.New("managementToken is required for narumq")
	}
	timeout, err := bp02Timeout(profile)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return executeBP02(ctx, profile, *target, nodes, tlsNodes, management, creds, *outputDir)
}

func parseEndpoints(raw string) ([]string, error) {
	parts := strings.Split(raw, ",")
	if len(parts) != bp02NodeCount {
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
		n, err := strconv.ParseUint(port, 10, 16)
		if err != nil || n == 0 {
			return nil, fmt.Errorf("invalid endpoint %q: port must be between 1 and 65535", part)
		}
		if _, ok := seen[address]; ok {
			return nil, fmt.Errorf("duplicate endpoint %q", address)
		}
		seen[address] = struct{}{}
		parts[index] = address
	}
	return parts, nil
}

func executeBP02(ctx context.Context, profile Profile, target string, nodes, tlsNodes, management []string, creds bp02RunnerCredentials, outputDir string) (runErr error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := profile.Validate(); err != nil {
		return err
	}
	if target != "narumq" && target != "emqx" {
		return fmt.Errorf("unsupported target %q", target)
	}
	if len(nodes) != bp02NodeCount || len(tlsNodes) != bp02NodeCount {
		return errors.New("exactly three TCP and TLS broker nodes are required")
	}
	if target == "narumq" && len(management) != bp02NodeCount {
		return errors.New("exactly three management endpoints are required for narumq")
	}
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		return fmt.Errorf("create BP02 output directory: %w", err)
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
	tlsConfig, tlsErr := loadBP02TLSConfig(creds)
	clients := make([]*runClient, 0, len(bp02SetupClients(profile))+4)
	byID := make(map[string]*runClient)
	publishers := make(map[int]*runClient, 8)
	var stopKeepAlive func()
	var shutOnce sync.Once
	var shutdownErr error
	shutdown := func() error {
		shutOnce.Do(func() {
			if stopKeepAlive != nil {
				stopKeepAlive()
				stopKeepAlive = nil
			}
			shutdownErr = closeClients(clients, time.Second)
		})
		return shutdownErr
	}
	defer func() {
		if cause := context.Cause(runCtx); cause != nil && !errors.Is(runErr, cause) {
			runErr = errors.Join(runErr, cause)
		}
		runErr = errors.Join(runErr, shutdown())
		if runErr != nil {
			col.executionFailure("workload execution failed")
			col.recordFinalCheck(false)
		}
		if err := col.write(outputDir); err != nil {
			runErr = errors.Join(runErr, fmt.Errorf("write BP02 workload artifacts: %w", err))
		}
		runErr = errors.Join(runErr, connectionWriter.Close(), phaseWriter.Close())
		if !col.valid() {
			runErr = errors.Join(runErr, errors.New("workload validity requirements were not met"))
		}
	}()
	if tlsErr != nil {
		return tlsErr
	}
	readyCtx, cancelReady := context.WithTimeout(runCtx, time.Duration(profile.ReadyTimeoutMillis)*time.Millisecond)
	if err := runRecorded(phaseWriter, "readiness", func(start time.Time) error {
		return readiness(readyCtx, target, nodes, tlsNodes, management, creds.ManagementToken, start.Add(time.Duration(profile.ReadyTimeoutMillis)*time.Millisecond), creds, profile)
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
	if err := runRecorded(phaseWriter, "setup", func(time.Time) error {
		setupCtx, cancelSetup := context.WithTimeout(runCtx, time.Duration(profile.SetupTimeoutMillis)*time.Millisecond)
		defer cancelSetup()
		col.recordSetupStart(time.Now())
		defer func() { col.recordSetupEnd(time.Now()) }()
		for _, spec := range bp02SetupClients(profile) {
			address := nodes[spec.nodeIndex]
			if spec.tls {
				address = tlsNodes[spec.nodeIndex]
			}
			client, timing, connectErr := connectClient(setupCtx, spec, address, profile, creds, tlsConfig, true)
			record := connectionRecord{ClientID: spec.id, Node: fmt.Sprintf("node%d", spec.nodeIndex+1), Tenant: spec.tenant, Transport: transportName(spec.tls), Phase: "setup", ConnectLatencyNanoseconds: int64(timing.Latency), SessionPresent: timing.SessionPresent}
			if connectErr != nil {
				record.Error = connectErr.Error()
				return errors.Join(connectErr, connectionWriter.Write(record))
			}
			clients = append(clients, client)
			byID[spec.id] = client
			record.ConnectedAt = timing.ConnAckAt.UTC().Format(time.RFC3339Nano)
			if spec.role == "subscriber" {
				sent, acked, reasons, subErr := subscribeClient(client, profile, col)
				record.SubscribeSentAt = sent.UTC().Format(time.RFC3339Nano)
				if !acked.IsZero() {
					record.SubAckAt = acked.UTC().Format(time.RFC3339Nano)
				}
				record.ReasonCodes = reasonCodesAsInts(reasons)
				if subErr != nil {
					record.Error = subErr.Error()
					return errors.Join(subErr, connectionWriter.Write(record))
				}
				col.recordSetupSubscribe(sent, acked, reasons)
			}
			if err := connectionWriter.Write(record); err != nil {
				return err
			}
			col.recordSetupClient(spec, timing)
			client.startReader(profile, col, cancel)
			if spec.role == "publisher" {
				publishers[spec.stream] = client
			}
		}
		all := append([]*runClient(nil), clients...)
		stopKeepAlive = startKeepAlive(runCtx, all, time.Duration(profile.KeepAliveSeconds)*time.Second/2, col, cancel)
		return nil
	}); err != nil {
		return err
	}
	if err := runRecorded(phaseWriter, "subscribed-idle", func(time.Time) error {
		return waitForContext(runCtx, time.Duration(profile.SubscribedIdleMillis)*time.Millisecond)
	}); err != nil {
		return err
	}
	sequences := make(map[int]uint64, 8)
	for stepIndex, rate := range profile.RatesPerStreamPerSecond {
		step := stepIndex + 1
		for _, phase := range []struct {
			name   string
			millis int64
		}{{fmt.Sprintf("step-%02d-warmup", step), profile.WarmupMillis}, {fmt.Sprintf("step-%02d-measure", step), profile.MeasureMillis}} {
			if err := runRecorded(phaseWriter, phase.name, func(time.Time) error {
				return publishWindow(runCtx, profile, publishers, col, sequences, step, rate, phase.millis, phase.name)
			}); err != nil {
				return err
			}
			if strings.HasSuffix(phase.name, "warmup") {
				col.finishWarmup(step, time.Now())
			}
		}
		if err := runRecorded(phaseWriter, fmt.Sprintf("step-%02d-drain", step), func(time.Time) error {
			if err := waitForDrain(runCtx, col, step, profile.DrainMillis); err != nil {
				return err
			}
			col.finishStep(step, time.Now())
			return nil
		}); err != nil {
			return err
		}
	}
	if err := runRecorded(phaseWriter, "offline-disconnect", func(time.Time) error {
		if stopKeepAlive != nil {
			stopKeepAlive()
			stopKeepAlive = nil
		}
		for _, spec := range bp02Subscribers(profile) {
			if !spec.persistent {
				continue
			}
			client := byID[spec.id]
			if client == nil {
				return fmt.Errorf("persistent subscriber %s missing", spec.id)
			}
			if err := client.disconnectAndClose(time.Second); err != nil {
				return fmt.Errorf("disconnect %s: %w", spec.id, err)
			}
			delete(byID, spec.id)
		}
		all := liveClientList(profile, byID)
		stopKeepAlive = startKeepAlive(runCtx, all, time.Duration(profile.KeepAliveSeconds)*time.Second/2, col, cancel)
		return waitForContext(runCtx, time.Duration(profile.OfflineSettleMillis)*time.Millisecond)
	}); err != nil {
		return err
	}
	if err := runRecorded(phaseWriter, "offline-publish", func(start time.Time) error {
		interval := time.Second / time.Duration(profile.OfflinePublishRatePerSecond)
		for ordinal := range profile.OfflineMessagesPerStream {
			scheduled := start.Add(time.Duration(ordinal) * interval)
			if err := waitUntil(runCtx, scheduled); err != nil {
				return err
			}
			for _, stream := range []int{1, 2, 5} {
				sequences[stream]++
				topic := publishTopic(stream, sequences[stream])
				if err := publishers[stream].publish(runCtx, profile, col, sequences[stream], 0, "offline-publish", topic, scheduled, false); err != nil {
					return err
				}
			}
		}
		if err := waitPublishers(runCtx, publishers); err != nil {
			return err
		}
		return waitForContext(runCtx, time.Duration(profile.OfflineSettleMillis)*time.Millisecond)
	}); err != nil {
		return err
	}
	if err := runRecorded(phaseWriter, "offline-reconnect", func(start time.Time) error {
		deadline := start.Add(time.Duration(profile.ReconnectTimeoutMillis) * time.Millisecond)
		for _, spec := range bp02Subscribers(profile) {
			if !spec.persistent {
				continue
			}
			updated := spec
			node := (spec.nodeIndex + 1) % bp02NodeCount
			updated.nodeIndex = node
			address := nodes[node]
			if spec.tls {
				address = tlsNodes[node]
			}
			ctx, cancelConnect := context.WithDeadline(runCtx, deadline)
			client, timing, err := connectClient(ctx, updated, address, profile, creds, tlsConfig, false)
			cancelConnect()
			record := connectionRecord{ClientID: spec.id, Node: fmt.Sprintf("node%d", node+1), Tenant: spec.tenant, Transport: transportName(spec.tls), Phase: "offline-reconnect", ConnectLatencyNanoseconds: int64(timing.Latency), SessionPresent: timing.SessionPresent}
			if err != nil {
				record.Error = err.Error()
				_ = connectionWriter.Write(record)
				return err
			}
			clients = append(clients, client)
			byID[spec.id] = client
			record.ConnectedAt = timing.ConnAckAt.UTC().Format(time.RFC3339Nano)
			if !timing.SessionPresent {
				record.Error = "broker did not resume persistent session"
				_ = connectionWriter.Write(record)
				col.recordOfflineConnect(spec, timing)
				return fmt.Errorf("persistent session for %s was not present", spec.id)
			}
			if err := connectionWriter.Write(record); err != nil {
				return err
			}
			col.recordOfflineConnect(spec, timing)
			client.startReader(profile, col, cancel)
			if stopKeepAlive != nil {
				stopKeepAlive()
			}
			all := liveClientList(profile, byID)
			stopKeepAlive = startKeepAlive(runCtx, all, time.Duration(profile.KeepAliveSeconds)*time.Second/2, col, cancel)
		}
		for {
			if col.offlineComplete() {
				return nil
			}
			if !time.Now().Before(deadline) {
				return errors.New("offline delivery deadline exceeded")
			}
			if err := waitForContext(runCtx, 20*time.Millisecond); err != nil {
				return err
			}
		}
	}); err != nil {
		return err
	}
	if err := runRecorded(phaseWriter, "retained-publish", func(time.Time) error {
		for round := 1; round <= 2; round++ {
			for _, stream := range []int{6, 7} {
				topicCount := profile.RetainedTopicsTenantA
				if stream == 7 {
					topicCount = profile.RetainedTopicsTenantB
				}
				for index := range topicCount {
					sequence := uint64((round-1)*topicCount + index + 1)
					sequences[stream] = sequence
					topic := retainedTopic(profile, stream, sequence)
					scheduled := time.Now()
					if err := publishers[stream].publish(runCtx, profile, col, sequence, 0, "retained-publish", topic, scheduled, true); err != nil {
						return err
					}
				}
			}
			for _, stream := range []int{6, 7} {
				if err := publishers[stream].waitForAcks(runCtx); err != nil {
					return err
				}
			}
		}
		return nil
	}); err != nil {
		return err
	}
	if err := runRecorded(phaseWriter, "retained-replay", func(start time.Time) error {
		retained := bp02ClientPlan(profile)
		added := make([]*runClient, 0, 4)
		deadline := start.Add(time.Duration(profile.RetainedReplayTimeoutMillis) * time.Millisecond)
		for _, spec := range retained {
			if spec.role != "retained-subscriber" {
				continue
			}
			address := nodes[spec.nodeIndex]
			if spec.tls {
				address = tlsNodes[spec.nodeIndex]
			}
			ctx, cancelConnect := context.WithDeadline(runCtx, deadline)
			client, timing, err := connectClient(ctx, spec, address, profile, creds, tlsConfig, true)
			cancelConnect()
			record := connectionRecord{ClientID: spec.id, Node: fmt.Sprintf("node%d", spec.nodeIndex+1), Tenant: spec.tenant, Transport: transportName(spec.tls), Phase: "retained-replay", ConnectLatencyNanoseconds: int64(timing.Latency), SessionPresent: timing.SessionPresent}
			if err != nil {
				record.Error = err.Error()
				_ = connectionWriter.Write(record)
				return err
			}
			clients = append(clients, client)
			byID[spec.id] = client
			record.ConnectedAt = timing.ConnAckAt.UTC().Format(time.RFC3339Nano)
			added = append(added, client)
			sent, acked, codes, err := subscribeClient(client, profile, col)
			record.SubscribeSentAt = sent.UTC().Format(time.RFC3339Nano)
			if !acked.IsZero() {
				record.SubAckAt = acked.UTC().Format(time.RFC3339Nano)
			}
			record.ReasonCodes = reasonCodesAsInts(codes)
			if err != nil {
				record.Error = err.Error()
				_ = connectionWriter.Write(record)
				return err
			}
			if err := connectionWriter.Write(record); err != nil {
				return err
			}
			client.startReader(profile, col, cancel)
			if stopKeepAlive != nil {
				stopKeepAlive()
			}
			all := liveClientList(profile, byID)
			stopKeepAlive = startKeepAlive(runCtx, all, time.Duration(profile.KeepAliveSeconds)*time.Second/2, col, cancel)
		}
		for _, client := range added {
			for !col.retainedComplete(client.spec.id) {
				if !time.Now().Before(deadline) {
					return fmt.Errorf("retained replay deadline exceeded for %s", client.spec.id)
				}
				if err := waitForContext(runCtx, 20*time.Millisecond); err != nil {
					return err
				}
			}
		}
		if err := waitForContext(runCtx, bp02RetainedQuietPeriod); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	if err := runRecorded(phaseWriter, "final_check", func(time.Time) error {
		for _, client := range liveClientList(profile, byID) {
			select {
			case <-client.done:
				col.recordFinalCheck(false)
				return fmt.Errorf("client %s reader exited before final_check", client.spec.id)
			default:
			}
			if failure := client.failure(); failure != nil {
				col.recordFinalCheck(false)
				return fmt.Errorf("client %s failed at final_check: %w", client.spec.id, failure)
			}
		}
		summary := col.summary()
		if !measureResultsValid(summary) || summary.ProtocolErrors != 0 || summary.TransportErrors != 0 || summary.UnexpectedDeliveries != 0 || !col.offlineAndRetainedValid() {
			col.recordFinalCheck(false)
			return errors.New("workload, offline, or retained result invalid")
		}
		col.recordFinalCheck(true)
		if !col.valid() {
			col.recordFinalCheck(false)
			return errors.New("workload validity requirements failed")
		}
		return nil
	}); err != nil {
		return err
	}
	if err := runRecorded(phaseWriter, "cleanup", func(time.Time) error { return shutdown() }); err != nil {
		return err
	}
	return runErr
}

const bp02RetainedQuietPeriod = 500 * time.Millisecond

func transportName(tls bool) string {
	if tls {
		return "tls"
	}
	return "tcp"
}
func reasonCodesAsInts(codes []byte) []int {
	out := make([]int, len(codes))
	for i, c := range codes {
		out[i] = int(c)
	}
	return out
}

func publishWindow(ctx context.Context, profile Profile, publishers map[int]*runClient, col *collector, sequences map[int]uint64, step int, rate uint64, millis int64, phase string) error {
	start := time.Now()
	interval := time.Second / time.Duration(rate)
	var group sync.WaitGroup
	var sequenceMu sync.Mutex
	errs := make(chan error, 6)
	for stream := range 6 {
		group.Add(1)
		go func(stream int) {
			defer group.Done()
			publisher := publishers[stream]
			if publisher == nil {
				errs <- fmt.Errorf("publisher for stream %d is missing", stream)
				return
			}
			count := rate * uint64(millis) / 1000
			for ordinal := uint64(1); ordinal <= count; ordinal++ {
				scheduled := start.Add(time.Duration(ordinal-1)*interval + time.Duration(stream)*interval/6)
				if err := waitUntil(ctx, scheduled); err != nil {
					errs <- err
					return
				}
				sequenceMu.Lock()
				sequences[stream]++
				seq := sequences[stream]
				sequenceMu.Unlock()
				topic := publishTopic(stream, seq)
				if err := publisher.publish(ctx, profile, col, seq, step, phase, topic, scheduled, false); err != nil {
					errs <- err
					return
				}
			}
			if err := waitUntil(ctx, start.Add(time.Duration(millis)*time.Millisecond)); err != nil {
				errs <- err
			}
		}(stream)
	}
	group.Wait()
	close(errs)
	for err := range errs {
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
func waitPublishers(ctx context.Context, publishers map[int]*runClient) error {
	for _, client := range publishers {
		if err := client.waitForAcks(ctx); err != nil {
			return err
		}
	}
	return nil
}
func waitUntil(ctx context.Context, target time.Time) error {
	d := time.Until(target)
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-timer.C:
		return nil
	}
}
func waitForContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
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
func bp02Timeout(p Profile) (time.Duration, error) {
	components := []time.Duration{time.Duration(p.ReadyTimeoutMillis) * time.Millisecond, time.Duration(p.IdleBaselineMillis) * time.Millisecond, time.Duration(p.SetupTimeoutMillis) * time.Millisecond, time.Duration(p.SubscribedIdleMillis) * time.Millisecond}
	for range p.RatesPerStreamPerSecond {
		components = append(components, time.Duration(p.WarmupMillis+p.MeasureMillis+p.DrainMillis)*time.Millisecond)
	}
	offline := time.Duration(2*p.OfflineSettleMillis)*time.Millisecond + time.Duration(math.Ceil(float64(p.OfflineMessagesPerStream)*1000/float64(p.OfflinePublishRatePerSecond)))*time.Millisecond + time.Duration(p.ReconnectTimeoutMillis)*time.Millisecond
	retained := time.Duration(2*p.AckTimeoutMillis+p.RetainedReplayTimeoutMillis)*time.Millisecond + bp02RetainedQuietPeriod
	components = append(components, offline, retained, 3*time.Minute)
	var total time.Duration
	for _, d := range components {
		if d < 0 || total > time.Duration(math.MaxInt64)-d {
			return 0, errors.New("profile target timeout exceeds supported range")
		}
		total += d
	}
	return total, nil
}
func liveClientList(profile Profile, clients map[string]*runClient) []*runClient {
	ordered := make([]*runClient, 0, len(clients))
	for _, spec := range bp02ClientPlan(profile) {
		if client := clients[spec.id]; client != nil {
			ordered = append(ordered, client)
		}
	}
	return ordered
}
func readiness(ctx context.Context, target string, nodes, tlsNodes, management []string, token string, deadline time.Time, creds bp02RunnerCredentials, profile Profile) error {
	client := &http.Client{Timeout: 2 * time.Second}
	if target == "narumq" {
		for {
			ready := true
			for _, address := range management {
				req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+address+"/health/ready", nil)
				if err != nil {
					return err
				}
				resp, err := client.Do(req)
				if err != nil {
					ready = false
					continue
				}
				_ = resp.Body.Close()
				if resp.StatusCode != http.StatusOK {
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
				req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+address+"/api/v1/cluster", nil)
				if err != nil {
					return err
				}
				req.Header.Set("Authorization", "Bearer "+token)
				resp, err := client.Do(req)
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
				decodeErr := json.NewDecoder(resp.Body).Decode(&body)
				_ = resp.Body.Close()
				if resp.StatusCode != http.StatusOK || decodeErr != nil || body.Data.ClusterID != bp02ClusterID || body.Data.LeaderNodeID == nil || *body.Data.LeaderNodeID == "" {
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
	credential := creds.Tenants["bp02-a"]
	for from := range nodes {
		for to := range nodes {
			if from == to {
				continue
			}
			topic := fmt.Sprintf("bp02/a/probe/%d-%d", from+1, to+1)
			pubID := fmt.Sprintf("bp02-probe-pub-%d-%d", from+1, to+1)
			subID := fmt.Sprintf("bp02-probe-sub-%d-%d", from+1, to+1)
			if err := probePair(ctx, nodes[from], nodes[to], topic, pubID, subID, deadline, credential); err != nil {
				return err
			}
		}
	}
	tlsConfig, err := loadBP02TLSConfig(creds)
	if err != nil {
		return err
	}
	for node, address := range tlsNodes {
		host, _, err := net.SplitHostPort(address)
		if err != nil {
			return err
		}
		config := tlsConfig.Clone()
		config.ServerName = host
		spec := clientSpec{id: fmt.Sprintf("bp02-probe-tls-%d", node+1), tenant: "bp02-a", tls: true}
		attempt, cancel := context.WithDeadline(ctx, deadline)
		connected, _, err := connectClient(attempt, spec, address, profile, creds, config, true)
		if err == nil && connected != nil {
			err = errors.Join(connected.client.Write(&mqtt.Disconnect{Version: mqtt.ProtocolV5}, time.Second), connected.client.Close())
		}
		cancel()
		if err != nil {
			return fmt.Errorf("TLS readiness probe node%d: %w", node+1, err)
		}
	}
	return nil
}

func probePair(ctx context.Context, publisherAddress, subscriberAddress, topic, publisherID, subscriberID string, deadline time.Time, credential bp02TenantCredential) error {
	var lastErr error
	for time.Now().Before(deadline) {
		attempt, cancel := context.WithDeadline(ctx, deadline)
		subscriber, _, err := mqttclient.Connect(attempt, mqttclient.ConnectOptions{Address: subscriberAddress, ClientID: subscriberID, ProtocolVersion: mqtt.ProtocolV5, KeepAlive: 10, HandshakeTimeout: time.Second, CleanStart: true, Username: credential.Username, Password: []byte(credential.Password)})
		if err == nil {
			err = subscriber.Write(&mqtt.Subscribe{Version: mqtt.ProtocolV5, PacketID: 1, Subscriptions: []mqtt.Subscription{{Filter: topic, QoS: mqtt.QoSAtLeastOnce}}}, time.Second)
			if err == nil {
				packet, readErr := subscriber.Read(time.Second)
				ack, ok := packet.(*mqtt.SubAck)
				if readErr != nil {
					err = readErr
				} else if !ok || ack.PacketID != 1 || len(ack.ReasonCodes) != 1 || ack.ReasonCodes[0] != byte(mqtt.QoSAtLeastOnce) {
					err = fmt.Errorf("unexpected probe SUBACK %T", packet)
				}
			}
		}
		var publisher *mqttclient.Client
		if err == nil {
			publisher, _, err = mqttclient.Connect(attempt, mqttclient.ConnectOptions{Address: publisherAddress, ClientID: publisherID, ProtocolVersion: mqtt.ProtocolV5, KeepAlive: 10, HandshakeTimeout: time.Second, CleanStart: true, Username: credential.Username, Password: []byte(credential.Password)})
		}
		payload := []byte(fmt.Sprintf("%s-%d", topic, time.Now().UnixNano()))
		if err == nil {
			err = publisher.Write(&mqtt.Publish{Version: mqtt.ProtocolV5, QoS: mqtt.QoSAtLeastOnce, PacketID: 1, Topic: topic, Payload: payload}, time.Second)
		}
		if err == nil {
			packet, readErr := publisher.Read(5 * time.Second)
			ack, ok := packet.(*mqtt.PubAck)
			if readErr != nil {
				err = readErr
			} else if !ok || ack.PacketID != 1 || ack.ReasonCode >= 0x80 {
				err = fmt.Errorf("unexpected probe PUBACK %T", packet)
			}
		}
		if err == nil {
			packet, readErr := subscriber.Read(5 * time.Second)
			delivery, ok := packet.(*mqtt.Publish)
			if readErr != nil {
				err = readErr
			} else if !ok || delivery.Topic != topic || delivery.QoS != mqtt.QoSAtLeastOnce || delivery.Retain || delivery.DUP || string(delivery.Payload) != string(payload) {
				err = fmt.Errorf("unexpected probe delivery %T", packet)
			} else {
				err = subscriber.Write(&mqtt.PubAck{Version: mqtt.ProtocolV5, PacketID: delivery.PacketID}, time.Second)
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
func measureResultsValid(summary workloadSummary) bool {
	for _, step := range summary.Steps {
		if step.GeneratorOverload {
			return false
		}
		for _, stream := range step.Streams {
			if stream.Missing != 0 || stream.Duplicates != 0 || stream.Unexpected != 0 || stream.QoSMismatches != 0 || stream.AckTimeouts != 0 {
				return false
			}
		}
	}
	return true
}
