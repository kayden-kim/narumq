package main

import (
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"narumq/tests/benchmark/internal/jsonl"
)

type messageKey struct {
	stream   string
	sequence uint64
}

type messageRecord struct {
	Stream          string `json:"stream"`
	Protocol        string `json:"protocol"`
	QoS             int    `json:"qos"`
	Sequence        uint64 `json:"sequence"`
	ScheduledAt     string `json:"scheduledAt"`
	SentAt          string `json:"sentAt,omitempty"`
	AckAt           string `json:"ackAt,omitempty"`
	CompleteAt      string `json:"completeAt,omitempty"`
	FirstReceivedAt string `json:"firstReceivedAt,omitempty"`
	ReceiveCount    int    `json:"receiveCount"`
	Interrupted     bool   `json:"interrupted"`
	AckTimeout      bool   `json:"ackTimeout"`
	Acked           bool   `json:"acked"`
	AckReasonCode   byte   `json:"ackReasonCode,omitempty"`

	scheduled   time.Time
	sent        time.Time
	ackedAt     time.Time
	completedAt time.Time
	receivedAt  time.Time
	prepared    bool
	sentOK      bool
	pubrecSeen  bool
	acked       bool
	timedOut    bool
	rejected    bool
	aborted     bool
}

type streamSummary struct {
	Stream              string `json:"stream"`
	Protocol            string `json:"protocol"`
	QoS                 int    `json:"qos"`
	Sent                uint64 `json:"sent"`
	Acked               uint64 `json:"acked"`
	Interrupted         uint64 `json:"interrupted"`
	SkippedDisconnected uint64 `json:"skippedDisconnected"`
	SkippedInflightFull uint64 `json:"skippedInflightFull"`
	Received            uint64 `json:"received"`
	Gaps                uint64 `json:"gaps"`
	Duplicates          uint64 `json:"duplicates"`
	Reorders            uint64 `json:"reorders"`
	QoS0Lost            uint64 `json:"qos0Lost"`
	AckTimeouts         uint64 `json:"ackTimeouts"`
}

type latencySummary struct {
	P50 float64 `json:"p50"`
	P95 float64 `json:"p95"`
	P99 float64 `json:"p99"`
	Max float64 `json:"max"`
}

type recoverySummary struct {
	InjectionSource          string             `json:"injectionSource"`
	InjectionAt              string             `json:"injectionAt"`
	Recovered                bool               `json:"recovered"`
	RecoveredAt              string             `json:"recoveredAt,omitempty"`
	RTOMillis                float64            `json:"rtoMillis,omitempty"`
	ReconnectMaxMillis       float64            `json:"reconnectMaxMillis"`
	StreamFirstSuccessMillis map[string]float64 `json:"streamFirstSuccessMillis"`
}

type roundSummary struct {
	Round                  int               `json:"round"`
	Node                   string            `json:"node"`
	Disconnects            uint64            `json:"disconnects"`
	Reconnects             uint64            `json:"reconnects"`
	ReconnectByNode        map[string]uint64 `json:"reconnectByNode"`
	ReconnectLatencyMillis latencySummary    `json:"reconnectLatencyMillis"`
	ConnectRejections      uint64            `json:"connectRejections"`
	SessionPresentFailures uint64            `json:"sessionPresentFailures"`
	DisconnectReasons      map[string]uint64 `json:"disconnectReasons"`
	Recovery               recoverySummary   `json:"recovery"`
	latencies              []float64
	disconnectsByClient    []plannedDisconnect
	downObservedAt         time.Time
}

type plannedDisconnect struct {
	ClientID      string
	At            time.Time
	ReconnectedAt time.Time
}

type totalsSummary struct {
	Gaps                 uint64 `json:"gaps"`
	QoS1Duplicates       uint64 `json:"qos1Duplicates"`
	QoS2Duplicates       uint64 `json:"qos2Duplicates"`
	UnplannedDisconnects uint64 `json:"unplannedDisconnects"`
	ProtocolErrors       uint64 `json:"protocolErrors"`
	Overload             uint64 `json:"overload"`
}

type workloadSummary struct {
	SchemaVersion  int               `json:"schemaVersion"`
	Target         string            `json:"target"`
	Valid          bool              `json:"valid"`
	FailureReasons []string          `json:"failureReasons"`
	Clients        int               `json:"clients"`
	InitialByNode  map[string]uint64 `json:"initialByNode"`
	Rounds         []roundSummary    `json:"rounds"`
	Streams        []streamSummary   `json:"streams"`
	Totals         totalsSummary     `json:"totals"`
}

type streamCounters struct {
	streamSummary
	highestReceived uint64
}

type collector struct {
	mu sync.Mutex

	target          string
	profile         Profile
	failureSet      map[string]struct{}
	initialByNode   map[string]uint64
	rounds          map[int]*roundSummary
	streams         map[string]*streamCounters
	records         map[messageKey]*messageRecord
	order           []*messageRecord
	activeExchanges int
	totals          totalsSummary
}

func newCollector(target string, profile Profile) *collector {
	c := &collector{
		target:        target,
		profile:       profile,
		failureSet:    make(map[string]struct{}),
		initialByNode: map[string]uint64{"node1": 0, "node2": 0, "node3": 0},
		rounds:        make(map[int]*roundSummary),
		streams:       make(map[string]*streamCounters, 4),
		records:       make(map[messageKey]*messageRecord),
	}
	for index := 1; index <= profile.rounds(); index++ {
		nodeNumber := 0
		if len(profile.KillOrder) != 0 {
			nodeNumber = profile.KillOrder[(index-1)%len(profile.KillOrder)]
		}
		c.rounds[index] = &roundSummary{
			Round: index, Node: fmt.Sprintf("node%d", nodeNumber),
			ReconnectByNode: make(map[string]uint64), DisconnectReasons: make(map[string]uint64),
		}
	}
	for _, stream := range []struct {
		name     string
		protocol string
		qos      int
	}{
		{name: "v5-q0", protocol: "MQTT 5", qos: 0},
		{name: "v5-q1", protocol: "MQTT 5", qos: 1},
		{name: "v5-q2", protocol: "MQTT 5", qos: 2},
		{name: "v311-q1", protocol: "MQTT 3.1.1", qos: 1},
	} {
		c.streams[stream.name] = &streamCounters{streamSummary: streamSummary{
			Stream: stream.name, Protocol: stream.protocol, QoS: stream.qos,
		}}
	}
	return c
}

func (c *collector) failure(reason string) {
	if c == nil {
		return
	}
	if strings.TrimSpace(reason) == "" {
		reason = "workload failure"
	}
	c.mu.Lock()
	c.failureSet[reason] = struct{}{}
	c.mu.Unlock()
}

func (c *collector) protocolError(reason string) {
	c.mu.Lock()
	c.totals.ProtocolErrors++
	if strings.TrimSpace(reason) == "" {
		reason = "MQTT protocol error"
	}
	c.failureSet[reason] = struct{}{}
	c.mu.Unlock()
}

func (c *collector) recordInitialNode(node string) {
	c.mu.Lock()
	if _, ok := c.initialByNode[node]; !ok {
		c.initialByNode[node] = 0
	}
	c.initialByNode[node]++
	c.mu.Unlock()
}

func (c *collector) recordDisconnect(clientID, node string, marker roundMarker, reason, serverReference string, at time.Time) (int, bool) {
	planned := marker.Round > 0 && marker.Node == node && (marker.State == "killing" || marker.State == "down")
	c.mu.Lock()
	defer c.mu.Unlock()
	if serverReference != "" {
		c.totals.ProtocolErrors++
		c.failureSet[fmt.Sprintf("unexpected MQTT server reference %q", serverReference)] = struct{}{}
	}
	if !planned {
		c.totals.UnplannedDisconnects++
		return 0, false
	}
	round := c.rounds[marker.Round]
	if round == nil {
		c.totals.UnplannedDisconnects++
		c.failureSet[fmt.Sprintf("disconnect attributed to unknown round %d", marker.Round)] = struct{}{}
		return 0, false
	}
	round.Disconnects++
	round.DisconnectReasons[reason]++
	round.disconnectsByClient = append(round.disconnectsByClient, plannedDisconnect{ClientID: clientID, At: at})
	return marker.Round, true
}

func (c *collector) recordDownObserved(roundNumber int, at time.Time) {
	c.mu.Lock()
	if round := c.rounds[roundNumber]; round != nil && round.downObservedAt.IsZero() {
		round.downObservedAt = at
	}
	c.mu.Unlock()
}

func (c *collector) recordReconnect(roundNumber int, node, clientID string, latency time.Duration, at time.Time) {
	if roundNumber == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	round := c.rounds[roundNumber]
	if round == nil {
		return
	}
	round.Reconnects++
	round.ReconnectByNode[node]++
	round.latencies = append(round.latencies, float64(latency)/float64(time.Millisecond))
	for index := range round.disconnectsByClient {
		if round.disconnectsByClient[index].ClientID == clientID && round.disconnectsByClient[index].ReconnectedAt.IsZero() {
			round.disconnectsByClient[index].ReconnectedAt = at
			break
		}
	}
}

func (c *collector) recordConnectRejection(roundNumber int) {
	if roundNumber == 0 {
		return
	}
	c.mu.Lock()
	if round := c.rounds[roundNumber]; round != nil {
		round.ConnectRejections++
	}
	c.mu.Unlock()
}

func (c *collector) recordSessionPresentFailure(roundNumber int) {
	if roundNumber == 0 {
		return
	}
	c.mu.Lock()
	if round := c.rounds[roundNumber]; round != nil {
		round.SessionPresentFailures++
	}
	c.failureSet["persistent session was not present after reconnect"] = struct{}{}
	c.mu.Unlock()
}

func (c *collector) beginPublish(record messageRecord) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.profile.MaxMessages > 0 && uint64(len(c.order)) >= c.profile.MaxMessages {
		return fmt.Errorf("message capacity %d exceeded", c.profile.MaxMessages)
	}
	key := messageKey{stream: record.Stream, sequence: record.Sequence}
	if c.records[key] != nil {
		return fmt.Errorf("duplicate message sequence %s/%d", record.Stream, record.Sequence)
	}
	record.prepared = true
	record.ScheduledAt = record.scheduled.UTC().Format(time.RFC3339Nano)
	c.records[key] = &record
	c.order = append(c.order, &record)
	if record.QoS > 0 {
		c.activeExchanges++
	}
	return nil
}

func (c *collector) publishSent(stream string, sequence uint64, at time.Time) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	record := c.records[messageKey{stream: stream, sequence: sequence}]
	if record == nil || record.aborted || record.sentOK {
		return false
	}
	record.sent = at
	record.sentOK = true
	record.SentAt = at.UTC().Format(time.RFC3339Nano)
	if counters := c.streams[stream]; counters != nil {
		counters.Sent++
	}
	return true
}

func (c *collector) publishFailed(stream string, sequence uint64) {
	c.mu.Lock()
	key := messageKey{stream: stream, sequence: sequence}
	record := c.records[key]
	if record != nil && !record.aborted {
		record.aborted = true
		delete(c.records, key)
		if record.QoS > 0 && !record.acked && !record.timedOut && !record.rejected {
			c.activeExchanges--
		}
	}
	c.mu.Unlock()
}

func (c *collector) publishAck(stream string, sequence uint64, at time.Time, reason byte, complete bool) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	record := c.records[messageKey{stream: stream, sequence: sequence}]
	if record == nil || !record.sentOK || record.aborted || record.timedOut || record.rejected {
		return false
	}
	if reason >= 0x80 {
		record.rejected = true
		record.AckReasonCode = reason
		c.activeExchanges--
		c.failureSet[fmt.Sprintf("broker rejected %s sequence %d with reason %#02x", stream, sequence, reason)] = struct{}{}
		return false
	}
	if record.ackedAt.IsZero() {
		record.ackedAt = at
		record.AckAt = at.UTC().Format(time.RFC3339Nano)
		record.AckReasonCode = reason
	}
	if !complete {
		record.pubrecSeen = true
		return true
	}
	if record.QoS == 2 && !record.pubrecSeen {
		return false
	}
	if record.acked {
		return false
	}
	record.acked = true
	record.Acked = true
	record.completedAt = at
	record.CompleteAt = at.UTC().Format(time.RFC3339Nano)
	if counters := c.streams[stream]; counters != nil {
		counters.Acked++
	}
	if record.QoS > 0 {
		c.activeExchanges--
	}
	return true
}

func (c *collector) publishInterrupted(stream string, sequence uint64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	record := c.records[messageKey{stream: stream, sequence: sequence}]
	if record == nil || !record.sentOK || record.acked || record.timedOut || record.rejected || record.Interrupted {
		return false
	}
	record.Interrupted = true
	if counters := c.streams[stream]; counters != nil {
		counters.Interrupted++
	}
	if record.QoS > 0 {
		c.activeExchanges--
	}
	return true
}

func (c *collector) publishTimeout(stream string, sequence uint64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	record := c.records[messageKey{stream: stream, sequence: sequence}]
	if record == nil || !record.sentOK || record.acked || record.timedOut || record.Interrupted || record.rejected {
		return false
	}
	record.timedOut = true
	record.AckTimeout = true
	if counters := c.streams[stream]; counters != nil {
		counters.AckTimeouts++
	}
	if record.QoS > 0 {
		c.activeExchanges--
	}
	c.failureSet[fmt.Sprintf("ACK timeout for %s sequence %d", stream, sequence)] = struct{}{}
	return true
}

func (c *collector) skipDisconnected(stream string) {
	c.mu.Lock()
	if counters := c.streams[stream]; counters != nil {
		counters.SkippedDisconnected++
	}
	c.mu.Unlock()
}

func (c *collector) skipInflightFull(stream string) {
	c.mu.Lock()
	if counters := c.streams[stream]; counters != nil {
		counters.SkippedInflightFull++
	}
	c.mu.Unlock()
}

func (c *collector) recordOverload(stream string) {
	c.mu.Lock()
	c.totals.Overload++
	c.failureSet["publisher schedule overload"] = struct{}{}
	c.mu.Unlock()
}

func (c *collector) receive(stream string, sequence uint64, at time.Time) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	record := c.records[messageKey{stream: stream, sequence: sequence}]
	if record == nil || !record.prepared || record.aborted {
		return false, fmt.Errorf("received unknown %s sequence %d", stream, sequence)
	}
	counters := c.streams[stream]
	if counters == nil {
		return false, fmt.Errorf("received unknown stream %q", stream)
	}
	if record.receivedAt.IsZero() {
		record.receivedAt = at
		record.FirstReceivedAt = at.UTC().Format(time.RFC3339Nano)
		if counters.highestReceived > sequence {
			counters.Reorders++
		} else if sequence > counters.highestReceived {
			counters.highestReceived = sequence
		}
	} else {
		record.ReceiveCount++
		if record.QoS == 2 {
			c.totals.QoS2Duplicates++
			c.failureSet[fmt.Sprintf("QoS 2 duplicate for %s sequence %d", stream, sequence)] = struct{}{}
		} else if record.QoS == 1 {
			c.totals.QoS1Duplicates++
		}
		return true, nil
	}
	record.ReceiveCount = 1
	return false, nil
}
func computeRecovery(round roundSummary, disconnects []plannedDisconnect, downObservedAt time.Time, records []*messageRecord, profile Profile) (recoverySummary, []string) {
	result := recoverySummary{
		StreamFirstSuccessMillis: make(map[string]float64, 4),
	}
	var failures []string
	var injectionAt time.Time
	for _, disconnect := range disconnects {
		if !disconnect.At.IsZero() && (injectionAt.IsZero() || disconnect.At.Before(injectionAt)) {
			injectionAt = disconnect.At
		}
	}
	if !injectionAt.IsZero() {
		result.InjectionSource = "disconnect"
	} else if !downObservedAt.IsZero() {
		injectionAt = downObservedAt
		result.InjectionSource = "marker"
	} else {
		failures = append(failures, fmt.Sprintf("failure injection was not observed for round %02d", round.Round))
		return result, failures
	}
	result.InjectionAt = injectionAt.UTC().Format(time.RFC3339Nano)
	var recoveredAt time.Time
	for _, disconnect := range disconnects {
		if disconnect.ReconnectedAt.IsZero() {
			failures = appendUnique(failures, fmt.Sprintf("client %s did not reconnect after failure round %02d", disconnect.ClientID, round.Round))
			continue
		}
		offset := float64(disconnect.ReconnectedAt.Sub(disconnect.At)) / float64(time.Millisecond)
		if offset > result.ReconnectMaxMillis {
			result.ReconnectMaxMillis = offset
		}
		if disconnect.ReconnectedAt.After(recoveredAt) {
			recoveredAt = disconnect.ReconnectedAt
		}
	}
	streams := []struct {
		name string
		qos  int
	}{
		{name: "v5-q0", qos: 0},
		{name: "v5-q1", qos: 1},
		{name: "v5-q2", qos: 2},
		{name: "v311-q1", qos: 1},
	}
	for _, stream := range streams {
		var first time.Time
		for _, record := range records {
			if record.Stream != stream.name {
				continue
			}
			if record.sent.IsZero() || record.sent.Before(injectionAt) {
				continue
			}
			at := record.ackedAt
			if stream.qos == 0 {
				at = record.receivedAt
			} else if stream.qos == 2 {
				at = record.completedAt
			}
			if !at.IsZero() && (first.IsZero() || at.Before(first)) {
				first = at
			}
		}
		if first.IsZero() {
			failures = appendUnique(failures, fmt.Sprintf("stream %s had no successful message after failure round %02d injection", stream.name, round.Round))
			continue
		}
		result.StreamFirstSuccessMillis[stream.name] = float64(first.Sub(injectionAt)) / float64(time.Millisecond)
		if first.After(recoveredAt) {
			recoveredAt = first
		}
	}
	result.Recovered = len(failures) == 0
	if result.Recovered {
		result.RecoveredAt = recoveredAt.UTC().Format(time.RFC3339Nano)
		result.RTOMillis = float64(recoveredAt.Sub(injectionAt)) / float64(time.Millisecond)
		if result.RTOMillis > float64(profile.RecoveryTimeoutMillis) {
			failures = append(failures, fmt.Sprintf("recovery RTO %.3fms exceeded recovery timeout %dms for round %02d", result.RTOMillis, profile.RecoveryTimeoutMillis, round.Round))
		}
	}
	return result, failures
}

func (c *collector) settled() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.activeExchanges != 0 {
		return false
	}
	for _, record := range c.order {
		if record.aborted || !record.sentOK || record.QoS == 0 || !record.acked || record.Interrupted || record.timedOut {
			continue
		}
		if record.ReceiveCount == 0 {
			return false
		}
	}
	return true
}

func (c *collector) summary() workloadSummary {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.summaryLocked()
}

func (c *collector) summaryLocked() workloadSummary {
	failures := make([]string, 0, len(c.failureSet))
	for reason := range c.failureSet {
		failures = append(failures, reason)
	}
	sort.Strings(failures)
	streams := make([]streamSummary, 0, len(c.streams))
	var total totalsSummary = c.totals
	for _, name := range []string{"v5-q0", "v5-q1", "v5-q2", "v311-q1"} {
		counters := c.streams[name]
		if counters == nil {
			continue
		}
		stream := counters.streamSummary
		var uniqueReceived uint64
		for _, record := range c.order {
			if record.Stream != name || record.aborted {
				continue
			}
			if record.ReceiveCount > 0 {
				uniqueReceived++
				if record.ReceiveCount > 1 {
					stream.Duplicates += uint64(record.ReceiveCount - 1)
				}
			}
			if record.QoS > 0 && record.acked && record.ReceiveCount == 0 && !record.Interrupted {
				stream.Gaps++
			}
		}
		stream.Received = uniqueReceived
		stream.Reorders = counters.Reorders
		if stream.QoS == 0 && stream.Sent > uniqueReceived {
			stream.QoS0Lost = stream.Sent - uniqueReceived
		}
		total.Gaps += stream.Gaps
		streams = append(streams, stream)
	}
	rounds := make([]roundSummary, 0, len(c.rounds))
	for index := 1; index <= c.profile.rounds(); index++ {
		round := c.rounds[index]
		if round == nil {
			continue
		}
		copyRound := *round
		copyRound.ReconnectByNode = cloneCounts(round.ReconnectByNode)
		copyRound.DisconnectReasons = cloneCounts(round.DisconnectReasons)
		copyRound.ReconnectLatencyMillis = latencyQuantiles(round.latencies)
		copyRound.latencies = nil
		recovery, recoveryFailures := computeRecovery(copyRound, round.disconnectsByClient, round.downObservedAt, c.order, c.profile)
		copyRound.Recovery = recovery
		for _, reason := range recoveryFailures {
			failures = appendUnique(failures, reason)
		}
		rounds = append(rounds, copyRound)
	}
	initial := make(map[string]uint64, len(c.initialByNode))
	for node, count := range c.initialByNode {
		initial[node] = count
	}
	for _, round := range rounds {
		if round.SessionPresentFailures > 0 {
			failures = appendUnique(failures, "persistent session was not present after reconnect")
		}
	}
	valid := len(failures) == 0 && total.Gaps == 0 && total.QoS2Duplicates == 0 && total.UnplannedDisconnects == 0 && total.ProtocolErrors == 0 && total.Overload == 0
	for _, stream := range streams {
		if stream.AckTimeouts > 0 {
			valid = false
		}
	}
	return workloadSummary{
		SchemaVersion: 1, Target: c.target, Valid: valid, FailureReasons: failures,
		Clients: c.profile.clients(), InitialByNode: initial, Rounds: rounds,
		Streams: streams, Totals: total,
	}
}

func appendUnique(values []string, value string) []string {
	for _, existing := range values {
		if existing == value {
			return values
		}
	}
	return append(values, value)
}

func cloneCounts(values map[string]uint64) map[string]uint64 {
	cloned := make(map[string]uint64, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}

func latencyQuantiles(values []float64) latencySummary {
	if len(values) == 0 {
		return latencySummary{}
	}
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	return latencySummary{
		P50: nearestRankFloat(sorted, 0.50),
		P95: nearestRankFloat(sorted, 0.95),
		P99: nearestRankFloat(sorted, 0.99),
		Max: sorted[len(sorted)-1],
	}
}

func nearestRankFloat(sorted []float64, percentile float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	index := int(math.Ceil(percentile*float64(len(sorted)))) - 1
	if index < 0 {
		index = 0
	}
	if index >= len(sorted) {
		index = len(sorted) - 1
	}
	return sorted[index]
}

func (c *collector) write(outputDir string) error {
	c.mu.Lock()
	messages := make([]messageRecord, 0, len(c.order))
	for _, record := range c.order {
		if record.aborted {
			continue
		}
		messages = append(messages, *record)
	}
	c.mu.Unlock()

	messageErr := writeJSONL(filepath.Join(outputDir, "messages.jsonl"), messages)
	if messageErr != nil {
		c.failure("write messages.jsonl: " + messageErr.Error())
	}
	summary := c.summary()
	if err := writeAtomicJSON(filepath.Join(outputDir, "workload-summary.json"), summary); err != nil {
		c.failure("write workload-summary.json: " + err.Error())
		return errors.Join(messageErr, err)
	}
	return messageErr
}

func writeJSONL[T any](path string, values []T) error {
	writer, err := jsonl.Open(path)
	if err != nil {
		return err
	}
	var writeErr error
	for _, value := range values {
		if err := writer.Write(value); err != nil {
			writeErr = errors.Join(writeErr, err)
			break
		}
	}
	return errors.Join(writeErr, writer.Close())
}
