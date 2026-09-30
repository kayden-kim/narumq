package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"narumq/internal/domain/mqtt"
	"narumq/tests/benchmark/internal/jsonl"
	"narumq/tests/benchmark/internal/mqttclient"
)

const (
	pingResponseTimeout = 2 * time.Second
)

// RunOptions controls one in-container BP-05 target execution.
type RunOptions struct {
	Profile   Profile
	Target    string
	Address   string
	OutputDir string
}

type connectionRecord struct {
	clientID string
	index    int
	client   *mqttclient.Client
}

type attemptResult struct {
	clientID    string
	index       int
	success     bool
	record      *connectionRecord
	scheduleLag time.Duration
	latency     time.Duration
	overloaded  bool
	err         error
	eventErr    error
}

type pingResult struct {
	record          *connectionRecord
	sent            uint64
	received        uint64
	errors          uint64
	protocolErrors  uint64
	transportErrors uint64
	survivor        bool
	survivors       []*connectionRecord
}

type finalPingResult struct {
	succeeded       bool
	protocolErrors  uint64
	transportErrors uint64
}

type workloadCounters struct {
	initialAttempted int
	initialSucceeded int
	initialFailed    int
	initialLatencies []int64
	initialDuration  time.Duration

	reconnectAttempted int
	reconnectSucceeded int
	reconnectFailed    int
	reconnectLatencies []int64
	reconnectDuration  time.Duration

	maxScheduleLag    time.Duration
	idleSurvivors     int
	pingSent          uint64
	pingReceived      uint64
	pingErrors        uint64
	protocolErrors    uint64
	transportErrors   uint64
	generatorOverload uint64

	finalPingRequested int
	finalPingSucceeded int
	finalPingErrors    uint64
}

// Run executes the connection ramp, idle keepalive, reconnect subset and
// final-check phases, preserving raw events even when individual clients fail.
func Run(ctx context.Context, options RunOptions) (WorkloadSummary, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := options.Profile.Validate(); err != nil {
		return WorkloadSummary{}, err
	}
	if options.Target != "narumq" && options.Target != "emqx" {
		return WorkloadSummary{}, fmt.Errorf("unsupported benchmark target %q", options.Target)
	}
	if strings.TrimSpace(options.Address) == "" {
		return WorkloadSummary{}, errors.New("MQTT address is required")
	}
	if strings.TrimSpace(options.OutputDir) == "" {
		return WorkloadSummary{}, errors.New("output directory is required")
	}
	if err := os.MkdirAll(options.OutputDir, 0o755); err != nil {
		return WorkloadSummary{}, fmt.Errorf("create output directory: %w", err)
	}
	connectionsWriter, err := jsonl.Open(filepath.Join(options.OutputDir, "connections.jsonl"))

	if err != nil {
		return WorkloadSummary{}, err
	}
	phasesWriter, err := jsonl.Open(filepath.Join(options.OutputDir, "phases.jsonl"))

	if err != nil {
		_ = connectionsWriter.Close()
		return WorkloadSummary{}, err
	}
	counters := workloadCounters{}
	runErr := executeWorkload(ctx, options, connectionsWriter, phasesWriter, &counters)
	if err := connectionsWriter.Close(); err != nil {
		runErr = errors.Join(runErr, fmt.Errorf("close connections.jsonl: %w", err))
	}
	if err := phasesWriter.Close(); err != nil {
		runErr = errors.Join(runErr, fmt.Errorf("close phases.jsonl: %w", err))
	}
	summary := buildWorkloadSummary(options.Profile, options.Target, counters)
	if err := WriteWorkloadSummary(options.OutputDir, summary); err != nil {
		return summary, err
	}
	if runErr != nil {
		return summary, runErr
	}
	if !summary.Valid {
		return summary, fmt.Errorf("target %s produced an invalid BP-05 result", options.Target)
	}
	return summary, nil
}

func executeWorkload(ctx context.Context, options RunOptions, connectionsWriter, phasesWriter *jsonl.Writer, counters *workloadCounters) (runErr error) {

	profile := options.Profile
	alive := make(map[string]*connectionRecord, profile.ConnectionCount)
	defer func() {
		if err := writePhase(phasesWriter, "cleanup", "start", len(alive), time.Now()); err != nil && runErr == nil {
			runErr = fmt.Errorf("write cleanup start: %w", err)
		}
		for clientID, record := range alive {
			if err := record.client.Disconnect(pingResponseTimeout); err != nil {
				protocol, transport := classifyError(err)
				counters.protocolErrors += protocol
				counters.transportErrors += transport
			}
			if err := record.client.Close(); err != nil {
				counters.transportErrors++
			}
			delete(alive, clientID)
		}
		if err := writePhase(phasesWriter, "cleanup", "end", 0, time.Now()); err != nil && runErr == nil {
			runErr = fmt.Errorf("write cleanup end: %w", err)
		}
	}()

	if err := writePhase(phasesWriter, "ramp", "start", 0, time.Now()); err != nil {
		return fmt.Errorf("write ramp start: %w", err)
	}
	initialIDs := make([]string, profile.ConnectionCount)
	initialIndices := make([]int, profile.ConnectionCount)
	for index := range initialIDs {
		initialIDs[index] = fmt.Sprintf("bp05-%06d", index)
		initialIndices[index] = index
	}
	initialResults, initialDuration, rampErr := runConnectionRamp(ctx, profile, options.Address, "initial", initialIDs, initialIndices, profile.RampConnectionsPerSecond, connectionsWriter)
	counters.initialDuration = initialDuration
	if err := collectAttemptResults(initialResults, false, alive, counters); err != nil {
		return err
	}
	if err := writePhase(phasesWriter, "ramp", "end", len(alive), time.Now()); err != nil {
		return fmt.Errorf("write ramp end: %w", err)
	}
	if rampErr != nil {
		return rampErr
	}

	if err := writePhase(phasesWriter, "idle", "start", len(alive), time.Now()); err != nil {
		return fmt.Errorf("write idle start: %w", err)
	}
	idleResult, idleErr := runIdle(ctx, profile, alive)
	counters.pingSent += idleResult.sent
	counters.pingReceived += idleResult.received
	counters.pingErrors += idleResult.errors
	counters.protocolErrors += idleResult.protocolErrors
	counters.transportErrors += idleResult.transportErrors
	counters.idleSurvivors = len(idleResult.survivors)
	alive = make(map[string]*connectionRecord, len(idleResult.survivors))
	for _, record := range idleResult.survivors {
		alive[record.clientID] = record
	}
	if err := writePhase(phasesWriter, "idle", "end", len(alive), time.Now()); err != nil {
		return fmt.Errorf("write idle end: %w", err)
	}
	if idleErr != nil {
		return idleErr
	}

	if err := writePhase(phasesWriter, "reconnect", "start", len(alive), time.Now()); err != nil {
		return fmt.Errorf("write reconnect start: %w", err)
	}
	selected := selectReconnectRecords(alive, profile.ReconnectCount)
	selectedIDs := make([]string, len(selected))
	selectedIndices := make([]int, len(selected))
	for index, record := range selected {
		selectedIDs[index] = record.clientID
		selectedIndices[index] = record.index
		if err := record.client.Disconnect(pingResponseTimeout); err != nil {
			protocol, transport := classifyError(err)
			counters.protocolErrors += protocol
			counters.transportErrors += transport
		}
		if err := record.client.Close(); err != nil {
			counters.transportErrors++
		}
		delete(alive, record.clientID)
	}
	reconnectResults, reconnectDuration, reconnectErr := runConnectionRamp(ctx, profile, options.Address, "reconnect", selectedIDs, selectedIndices, profile.ReconnectRatePerSecond, connectionsWriter)
	counters.reconnectDuration = reconnectDuration
	if err := collectAttemptResults(reconnectResults, true, alive, counters); err != nil {
		return err
	}
	if err := writePhase(phasesWriter, "reconnect", "end", len(alive), time.Now()); err != nil {
		return fmt.Errorf("write reconnect end: %w", err)
	}
	if reconnectErr != nil {
		return reconnectErr
	}

	if err := writePhase(phasesWriter, "final_check", "start", len(alive), time.Now()); err != nil {
		return fmt.Errorf("write final_check start: %w", err)
	}
	counters.finalPingRequested = len(alive)
	finalResults := runFinalChecks(ctx, profile, alive)
	for _, result := range finalResults {
		if result.succeeded {
			counters.finalPingSucceeded++
		} else {
			counters.finalPingErrors++
		}
		counters.protocolErrors += result.protocolErrors
		counters.transportErrors += result.transportErrors
	}
	if err := writePhase(phasesWriter, "final_check", "end", len(alive), time.Now()); err != nil {
		return fmt.Errorf("write final_check end: %w", err)
	}
	return nil
}

func runConnectionRamp(ctx context.Context, profile Profile, address, phase string, clientIDs []string, indices []int, rate uint64, writer *jsonl.Writer) ([]attemptResult, time.Duration, error) {
	if len(clientIDs) != len(indices) {
		return nil, 0, errors.New("client ID and index counts differ")
	}
	startedAt := time.Now()
	interval := scheduleInterval(rate)
	results := make(chan attemptResult, len(clientIDs))
	var waitGroup sync.WaitGroup
	var scheduleErr error
	for position, clientID := range clientIDs {
		scheduledAt := scheduledTime(startedAt, position, rate)
		if err := waitUntil(ctx, scheduledAt); err != nil {
			scheduleErr = err
			break
		}
		waitGroup.Add(1)
		go func(clientID string, index int, scheduledAt time.Time) {
			defer waitGroup.Done()
			started := time.Now()
			lag := started.Sub(scheduledAt)
			client, timing, err := mqttclient.Connect(ctx, mqttclient.ConnectOptions{
				Address:          address,
				ClientID:         clientID,
				KeepAlive:        uint16(profile.KeepAliveSeconds),
				HandshakeTimeout: mqttclient.DefaultHandshakeTimeout,
				CleanStart:       true,
			})
			finished := time.Now()
			latency := finished.Sub(started)
			event := connectionEvent{
				ClientID:               clientID,
				Phase:                  phase,
				ScheduledAt:            utcString(scheduledAt),
				StartedAt:              utcString(started),
				ScheduleLagNanoseconds: lag.Nanoseconds(),
				LatencyNanoseconds:     latency.Nanoseconds(),
				Success:                err == nil && client != nil,
			}
			if err == nil && client == nil {
				err = errors.New("MQTT client connect returned nil client")
			}
			if err == nil {
				event.ConnAckAt = utcString(timing.ConnAckAt)
				event.LatencyNanoseconds = timing.Latency.Nanoseconds()
				latency = timing.Latency
			} else {
				event.Error = err.Error()
				if client != nil {
					_ = client.Close()
				}
			}
			if event.Success && event.Error == "" {
				event.Error = ""
			}
			eventErr := writer.Write(event)
			results <- attemptResult{
				clientID: clientID,
				index:    index,
				success:  event.Success,
				record: func() *connectionRecord {
					if event.Success {
						return &connectionRecord{clientID: clientID, index: index, client: client}
					}
					return nil
				}(),
				scheduleLag: lag,
				latency:     latency,
				overloaded:  lag >= interval,
				err:         err,
				eventErr:    eventErr,
			}
		}(clientID, indices[position], scheduledAt)
	}
	waitGroup.Wait()
	close(results)
	collected := make([]attemptResult, 0, len(results))
	for result := range results {
		collected = append(collected, result)
	}
	return collected, time.Since(startedAt), scheduleErr
}

func collectAttemptResults(results []attemptResult, reconnect bool, alive map[string]*connectionRecord, counters *workloadCounters) error {
	for _, result := range results {
		if result.eventErr != nil {
			return fmt.Errorf("write connection event: %w", result.eventErr)
		}
		if result.scheduleLag > counters.maxScheduleLag {
			counters.maxScheduleLag = result.scheduleLag
		}
		if result.overloaded {
			counters.generatorOverload++
		}
		if reconnect {
			counters.reconnectAttempted++
			if result.success {
				counters.reconnectSucceeded++
				counters.reconnectLatencies = append(counters.reconnectLatencies, result.latency.Nanoseconds())
			} else {
				counters.reconnectFailed++
				protocol, transport := classifyError(result.err)
				counters.protocolErrors += protocol
				counters.transportErrors += transport
			}
		} else {
			counters.initialAttempted++
			if result.success {
				counters.initialSucceeded++
				counters.initialLatencies = append(counters.initialLatencies, result.latency.Nanoseconds())
			} else {
				counters.initialFailed++
				protocol, transport := classifyError(result.err)
				counters.protocolErrors += protocol
				counters.transportErrors += transport
			}
		}
		if result.success {
			if _, exists := alive[result.clientID]; exists {
				_ = result.record.client.Close()
				counters.protocolErrors++
				if reconnect {
					counters.reconnectSucceeded--
					counters.reconnectFailed++
				} else {
					counters.initialSucceeded--
					counters.initialFailed++
				}
				continue
			}
			alive[result.clientID] = result.record
		}
	}
	return nil
}

func runIdle(ctx context.Context, profile Profile, alive map[string]*connectionRecord) (pingResult, error) {
	idleContext, cancel := context.WithCancel(ctx)
	defer cancel()
	startedAt := time.Now()
	results := make(chan pingResult, len(alive))
	var waitGroup sync.WaitGroup
	for _, record := range alive {
		waitGroup.Add(1)
		go func(record *connectionRecord) {
			defer waitGroup.Done()
			results <- runPingLoop(idleContext, profile, record, startedAt)
		}(record)
	}
	idleEnd := startedAt.Add(time.Duration(profile.IdleMillis) * time.Millisecond)
	waitErr := waitUntil(ctx, idleEnd)
	cancel()
	waitGroup.Wait()
	close(results)
	aggregate := pingResult{}
	for result := range results {
		aggregate.sent += result.sent
		aggregate.received += result.received
		aggregate.errors += result.errors
		aggregate.protocolErrors += result.protocolErrors
		aggregate.transportErrors += result.transportErrors
		if result.survivor && result.record != nil {
			aggregate.survivors = append(aggregate.survivors, result.record)
		}
	}
	return aggregate, waitErr
}

func runPingLoop(ctx context.Context, profile Profile, record *connectionRecord, startedAt time.Time) pingResult {
	result := pingResult{record: record, survivor: true}
	interval := time.Duration(profile.PingIntervalMillis) * time.Millisecond
	firstAt := startedAt.Add(pingOffset(record.index, profile.ConnectionCount, interval))
	if err := waitUntil(ctx, firstAt); err != nil {
		return result
	}
	nextAt := firstAt
	for {
		result.sent++
		if err := record.client.Write(&mqtt.PingReq{Version: mqtt.ProtocolV5}, interval); err != nil {
			result.errors++
			result.survivor = false
			result.protocolErrors, result.transportErrors = classifyError(err)
			_ = record.client.Close()
			return result
		}
		packet, err := record.client.Read(pingResponseTimeout)
		if err != nil {
			result.errors++
			result.survivor = false
			result.protocolErrors, result.transportErrors = classifyError(err)
			_ = record.client.Close()
			return result
		}
		response, ok := packet.(*mqtt.PingResp)
		if !ok || response.Version != mqtt.ProtocolV5 {
			result.errors++
			result.survivor = false
			result.protocolErrors++
			_ = record.client.Close()
			return result
		}
		result.received++
		nextAt = nextAt.Add(interval)
		if err := waitUntil(ctx, nextAt); err != nil {
			return result
		}
	}
}

func runFinalChecks(ctx context.Context, profile Profile, alive map[string]*connectionRecord) []finalPingResult {
	results := make(chan finalPingResult, len(alive))
	var waitGroup sync.WaitGroup
	for _, record := range alive {
		waitGroup.Add(1)
		go func(record *connectionRecord) {
			defer waitGroup.Done()
			if err := record.client.Write(&mqtt.PingReq{Version: mqtt.ProtocolV5}, pingResponseTimeout); err != nil {
				protocol, transport := classifyError(err)
				results <- finalPingResult{protocolErrors: protocol, transportErrors: transport}
				return
			}
			packet, err := record.client.Read(pingResponseTimeout)
			if err != nil {
				protocol, transport := classifyError(err)
				results <- finalPingResult{protocolErrors: protocol, transportErrors: transport}
				return
			}
			response, ok := packet.(*mqtt.PingResp)
			if !ok || response.Version != mqtt.ProtocolV5 {
				results <- finalPingResult{protocolErrors: 1}
				return
			}
			results <- finalPingResult{succeeded: true}
		}(record)
	}
	waitGroup.Wait()
	close(results)
	collected := make([]finalPingResult, 0, len(alive))
	for result := range results {
		collected = append(collected, result)
	}
	return collected
}

func selectReconnectRecords(alive map[string]*connectionRecord, count int) []*connectionRecord {
	records := make([]*connectionRecord, 0, len(alive))
	for _, record := range alive {
		records = append(records, record)
	}
	sort.Slice(records, func(left, right int) bool { return records[left].index < records[right].index })
	if count < len(records) {
		records = records[:count]
	}
	return records
}

func scheduleInterval(rate uint64) time.Duration {
	interval := time.Second / time.Duration(rate)
	if interval <= 0 {
		return time.Nanosecond
	}
	return interval
}

func scheduledTime(start time.Time, index int, rate uint64) time.Time {
	return start.Add(time.Duration((uint64(index) * uint64(time.Second)) / rate))
}

func pingOffset(index, total int, interval time.Duration) time.Duration {
	if total <= 0 {
		return 0
	}
	return time.Duration((uint64(index) * uint64(interval)) / uint64(total))
}

func waitUntil(ctx context.Context, target time.Time) error {
	for {
		delay := time.Until(target)
		if delay <= 0 {
			return nil
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func classifyError(err error) (protocol, transport uint64) {
	if err == nil {
		return 0, 0
	}
	if errors.Is(err, mqtt.ErrMalformedPacket) || errors.Is(err, mqtt.ErrIncompletePacket) || errors.Is(err, mqtt.ErrPacketTooLarge) || strings.Contains(strings.ToLower(err.Error()), "packet") || strings.Contains(strings.ToLower(err.Error()), "connack") || strings.Contains(strings.ToLower(err.Error()), "pingresp") {
		return 1, 0
	}
	var netErr net.Error
	if errors.As(err, &netErr) || errors.Is(err, os.ErrDeadlineExceeded) {
		return 0, 1
	}
	return 0, 1
}
