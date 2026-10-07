package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"narumq/tests/benchmark/internal/jsonl"
	"narumq/tests/benchmark/internal/mqttclient"
)

type collector struct {
	mu                                                    sync.Mutex
	profile                                               Profile
	target                                                string
	subscribers                                           []clientSpec
	subscriberIndex                                       map[string]int
	messages                                              map[messageKey]*messageState
	recipientPlans                                        map[recipientPlanKey]*recipientPlan
	messageOrder                                          []*messageState
	steps                                                 map[int]*stepState
	setupClients                                          map[string]mqttclient.ConnectTiming
	setupStart, setupEnd                                  time.Time
	setupTLS, setupTCP, setupSubscribe                    []int64
	protocolErrors, transportErrors, unexpectedDeliveries uint64
	executionFailures                                     []string
	offline                                               map[string]*offlineState
	retained                                              map[string]*retainedState
	finalCheck, finalCheckRecorded                        bool
}

type messageKey struct {
	stream   int
	sequence uint64
}

type recipientPlanKey struct {
	stream int
	topic  string
}
type recipientPlan struct {
	expected []uint64
	count    int
}
type messageState struct {
	stream                                                 int
	sequence                                               uint64
	step                                                   int
	phase, topic                                           string
	scheduledAt, sentAt                                    time.Time
	expected                                               []uint64
	expectedCount                                          int
	received                                               []uint64
	deliveries                                             uint64
	duplicates, unexpected, qosMismatches, ackTimeouts     uint64
	ackLatency                                             []int64
	firstLatency                                           []int64
	memberDeliveries                                       map[string]uint64
	warmupMissing, warmupDuplicates                        uint64
	snapshot                                               bool
	snapshotMissing, snapshotDelivered, snapshotDuplicates uint64
}
type stepState struct {
	maxScheduleLag, scheduleInterval time.Duration
	overload                         bool
	latency                          map[int][]int64
	ackLatency                       map[int][]int64
}
type offlineState struct {
	spec           clientSpec
	expected       uint64
	sessionPresent bool
	seen           map[messageKey]uint64
	connected      time.Time
	drain          time.Duration
	connectLatency int64
	unexpected     uint64
}
type retainedState struct {
	spec                                                 clientSpec
	started, completed                                   time.Time
	seen                                                 map[messageKey]uint64
	delivered, duplicates, unexpected, stale, wrongFlags uint64
	latest                                               map[string]messageKey
	records                                              []retainedRecord
}
type workloadSummary struct {
	SchemaVersion        int             `json:"schemaVersion"`
	Target               string          `json:"target"`
	ProfileID            string          `json:"profileId"`
	Purpose              string          `json:"purpose"`
	ReleaseGating        bool            `json:"releaseGating"`
	Valid                bool            `json:"valid"`
	FailureReasons       []string        `json:"failureReasons"`
	Setup                setupSummary    `json:"setup"`
	Steps                []stepSummary   `json:"steps"`
	Offline              offlineSummary  `json:"offline"`
	Retained             retainedSummary `json:"retained"`
	ProtocolErrors       uint64          `json:"protocolErrors"`
	TransportErrors      uint64          `json:"transportErrors"`
	UnexpectedDeliveries uint64          `json:"unexpectedDeliveries"`
	FinalCheck           bool            `json:"finalCheck"`
}
type setupSummary struct {
	Clients             int            `json:"clients"`
	ExpectedClients     int            `json:"expectedClients"`
	TLSClients          int            `json:"tlsClients"`
	TCPClients          int            `json:"tcpClients"`
	TLSConnectLatency   latencySummary `json:"tlsConnectLatency"`
	TCPConnectLatency   latencySummary `json:"tcpConnectLatency"`
	SubscribeAckLatency latencySummary `json:"subscribeAckLatency"`
	SubscriberSessions  int            `json:"subscriberSessions"`
	SubscriptionFilters int            `json:"subscriptionFilters"`
	DurationMillis      int64          `json:"durationMillis"`
}
type stepSummary struct {
	Step                        int                      `json:"step"`
	RatePerStreamPerSecond      uint64                   `json:"ratePerStreamPerSecond"`
	MeasureSeconds              float64                  `json:"measureSeconds"`
	MaxScheduleLagNanoseconds   int64                    `json:"maxScheduleLagNanoseconds"`
	ScheduleIntervalNanoseconds int64                    `json:"scheduleIntervalNanoseconds"`
	GeneratorOverload           bool                     `json:"generatorOverload"`
	DeliveredPerSecond          float64                  `json:"deliveredPerSecond"`
	Streams                     map[string]streamSummary `json:"streams"`
}
type streamSummary struct {
	Sent               uint64            `json:"sent"`
	ExpectedDeliveries uint64            `json:"expectedDeliveries"`
	Delivered          uint64            `json:"delivered"`
	Missing            uint64            `json:"missing"`
	Duplicates         uint64            `json:"duplicates"`
	Unexpected         uint64            `json:"unexpected"`
	QoSMismatches      uint64            `json:"qosMismatches"`
	AckTimeouts        uint64            `json:"ackTimeouts"`
	WarmupSent         uint64            `json:"warmupSent"`
	WarmupMissing      uint64            `json:"warmupMissing"`
	WarmupDuplicates   uint64            `json:"warmupDuplicates"`
	SentPerSecond      float64           `json:"sentPerSecond"`
	DeliveredPerSecond float64           `json:"deliveredPerSecond"`
	DeliveryLatency    latencySummary    `json:"deliveryLatency"`
	PublishAckLatency  latencySummary    `json:"publishAckLatency"`
	MemberDeliveries   map[string]uint64 `json:"memberDeliveries,omitempty"`
}
type latencySummary struct {
	P50Nanoseconds  int64 `json:"p50Nanoseconds"`
	P95Nanoseconds  int64 `json:"p95Nanoseconds"`
	P99Nanoseconds  int64 `json:"p99Nanoseconds"`
	P999Nanoseconds int64 `json:"p999Nanoseconds"`
	MaxNanoseconds  int64 `json:"maxNanoseconds"`
}
type offlineSummary struct {
	Clients                     int                        `json:"clients"`
	SessionPresent              int                        `json:"sessionPresent"`
	Missing                     uint64                     `json:"missing"`
	Duplicates                  uint64                     `json:"duplicates"`
	Unexpected                  uint64                     `json:"unexpected"`
	ReconnectConnAckLatency     latencySummary             `json:"reconnectConnAckLatency"`
	DrainDurationMaxNanoseconds int64                      `json:"drainDurationMaxNanoseconds"`
	Subscribers                 []offlineSubscriberSummary `json:"subscribers"`
}
type offlineSubscriberSummary struct {
	ClientID                 string `json:"clientId"`
	SessionPresent           bool   `json:"sessionPresent"`
	Expected                 uint64 `json:"expected"`
	Delivered                uint64 `json:"delivered"`
	Missing                  uint64 `json:"missing"`
	Duplicates               uint64 `json:"duplicates"`
	Unexpected               uint64 `json:"unexpected"`
	DrainDurationNanoseconds int64  `json:"drainDurationNanoseconds"`
}
type retainedSummary struct {
	Subscribers                  []retainedSubscriberSummary `json:"subscribers"`
	Missing                      uint64                      `json:"missing"`
	Duplicates                   uint64                      `json:"duplicates"`
	Unexpected                   uint64                      `json:"unexpected"`
	Stale                        uint64                      `json:"stale"`
	WrongFlags                   uint64                      `json:"wrongFlags"`
	ReplayDurationMaxNanoseconds int64                       `json:"replayDurationMaxNanoseconds"`
}
type retainedSubscriberSummary struct {
	ClientID                  string `json:"clientId"`
	Expected                  uint64 `json:"expected"`
	Delivered                 uint64 `json:"delivered"`
	Missing                   uint64 `json:"missing"`
	Duplicates                uint64 `json:"duplicates"`
	Unexpected                uint64 `json:"unexpected"`
	Stale                     uint64 `json:"stale"`
	WrongFlags                uint64 `json:"wrongFlags"`
	ReplayDurationNanoseconds int64  `json:"replayDurationNanoseconds"`
}
type messageRecord struct {
	Stream            string   `json:"stream"`
	Sequence          uint64   `json:"sequence"`
	Step              int      `json:"step"`
	Phase             string   `json:"phase"`
	Topic             string   `json:"topic"`
	ScheduledAt       string   `json:"scheduledAt,omitempty"`
	SentAt            string   `json:"sentAt,omitempty"`
	Expected          uint64   `json:"expected"`
	Delivered         uint64   `json:"delivered"`
	Missing           uint64   `json:"missing"`
	Duplicates        uint64   `json:"duplicates"`
	Unexpected        uint64   `json:"unexpected"`
	QoSMismatches     uint64   `json:"qosMismatches"`
	AckTimeouts       uint64   `json:"ackTimeouts"`
	MissingRecipients []string `json:"missingRecipients,omitempty"`
}
type retainedRecord struct {
	ClientID   string `json:"clientId"`
	Stream     string `json:"stream"`
	Sequence   uint64 `json:"sequence"`
	Topic      string `json:"topic"`
	Retain     bool   `json:"retain"`
	QoS        byte   `json:"qos"`
	At         string `json:"at"`
	Stale      bool   `json:"stale,omitempty"`
	Unexpected bool   `json:"unexpected,omitempty"`
	Duplicate  bool   `json:"duplicate,omitempty"`
	WrongFlags bool   `json:"wrongFlags,omitempty"`
}

func newCollector(profile Profile, target string) *collector {
	subs := bp02Subscribers(profile)
	byID := make(map[string]int, len(subs))
	for i, spec := range subs {
		byID[spec.id] = i
	}
	c := &collector{
		profile: profile, target: target, subscribers: subs, subscriberIndex: byID,
		messages: make(map[messageKey]*messageState), recipientPlans: make(map[recipientPlanKey]*recipientPlan),
		steps:        make(map[int]*stepState),
		setupClients: make(map[string]mqttclient.ConnectTiming), offline: make(map[string]*offlineState),
		retained: make(map[string]*retainedState),
	}
	for _, spec := range subs {
		if spec.persistent {
			c.offline[spec.id] = &offlineState{spec: spec, expected: expectedOfflineDeliveries(profile, spec), seen: make(map[messageKey]uint64)}
		}
	}
	for _, spec := range bp02ClientPlan(profile) {
		if spec.role == "retained-subscriber" {
			c.retained[spec.id] = &retainedState{spec: spec, seen: make(map[messageKey]uint64), latest: make(map[string]messageKey)}
		}
	}

	c.configureRetainedOracle()
	return c
}
func expectedOfflineDeliveries(profile Profile, spec clientSpec) uint64 {
	var normalMessages uint64
	for _, rate := range profile.RatesPerStreamPerSecond {
		normalMessages += rate * uint64(profile.WarmupMillis) / 1000
		normalMessages += rate * uint64(profile.MeasureMillis) / 1000
	}
	var expected uint64
	for offset := range profile.OfflineMessagesPerStream {
		sequence := normalMessages + uint64(offset) + 1
		if topicMatches(spec.filter, publishTopic(spec.stream, sequence)) {
			expected++
		}
	}
	return expected
}

func (c *collector) addMessage(stream int, sequence uint64, step int, phase, topic string, scheduledAt time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if stream < 0 || stream >= len(bp02Streams) || sequence == 0 {
		return fmt.Errorf("invalid message stream=%d sequence=%d", stream, sequence)
	}
	if uint64(len(c.messages)) >= c.profile.MaxMessages {
		return fmt.Errorf("message count exceeds profile maxMessages %d", c.profile.MaxMessages)
	}
	key := messageKey{stream, sequence}
	if _, ok := c.messages[key]; ok {
		return fmt.Errorf("duplicate message key stream=%s sequence=%d", bp02Streams[stream].name, sequence)
	}
	var plan *recipientPlan
	if stream < 6 {
		plan = c.recipientPlan(stream, topic)
	}
	m := &messageState{stream: stream, sequence: sequence, step: step, phase: phase, topic: topic, scheduledAt: scheduledAt}
	if plan != nil {
		m.expected, m.expectedCount = plan.expected, plan.count
	}
	if stream == 3 {
		m.memberDeliveries = make(map[string]uint64)
	}
	c.messages[key] = m
	c.messageOrder = append(c.messageOrder, m)
	return nil
}

func (c *collector) recipientPlan(stream int, topic string) *recipientPlan {
	key := recipientPlanKey{stream: stream, topic: topic}
	if plan := c.recipientPlans[key]; plan != nil {
		return plan
	}
	plan := &recipientPlan{expected: make([]uint64, (len(c.subscribers)+63)/64)}
	for _, index := range expectedRecipients(c.subscribers, stream, topic) {
		plan.expected[index/64] |= uint64(1) << uint(index%64)
		plan.count++
	}
	if stream == 3 && plan.count > 0 {
		plan.count = 1
	}
	c.recipientPlans[key] = plan
	return plan
}
func (c *collector) markSent(stream int, sequence uint64, sentAt time.Time) {
	c.mu.Lock()
	if m := c.messages[messageKey{stream, sequence}]; m != nil {
		m.sentAt = sentAt
	}
	c.mu.Unlock()
}
func (c *collector) recordScheduleLag(step int, lag, interval time.Duration) {
	c.mu.Lock()
	s := c.stepLocked(step)
	if lag > s.maxScheduleLag {
		s.maxScheduleLag = lag
	}
	if interval > s.scheduleInterval {
		s.scheduleInterval = interval
	}
	if lag >= interval {
		s.overload = true
	}
	c.mu.Unlock()
}
func (c *collector) recordDelivery(spec clientSpec, stream int, sequence uint64, topic string, qos byte, retain bool, at time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if spec.role == "retained-subscriber" {
		c.recordRetainedLocked(spec, stream, sequence, topic, qos, retain, at)
		return
	}
	key := messageKey{stream, sequence}
	m := c.messages[key]
	if m == nil || stream < 0 || stream >= 6 {
		c.unexpectedDeliveries++
		if m != nil {
			m.unexpected++
		}
		return
	}
	idx, exists := c.subscriberIndex[spec.id]
	if !exists || m.topic != topic || !topicMatches(spec.filter, topic) || !bitSet(m.expected, idx) {
		m.unexpected++
		c.unexpectedDeliveries++
		if spec.persistent && m.phase == "offline-publish" {
			if state := c.offline[spec.id]; state != nil {
				state.unexpected++
			}
		}
		return
	}
	if retain {
		m.unexpected++
		c.unexpectedDeliveries++
		if spec.persistent && m.phase == "offline-publish" {
			if state := c.offline[spec.id]; state != nil {
				state.unexpected++
			}
		}
	}
	if qos != bp02Streams[stream].qos {
		m.qosMismatches++
	}
	if spec.persistent && m.phase == "offline-publish" {
		if state := c.offline[spec.id]; state != nil {
			state.seen[key]++
			if !state.connected.IsZero() && at.After(state.connected) && at.Sub(state.connected) > state.drain {
				state.drain = at.Sub(state.connected)
			}
		}
	}
	already := bitSet(m.received, idx)
	if already {
		m.duplicates++
	} else {
		if len(m.received) == 0 {
			m.received = make([]uint64, (len(c.subscribers)+63)/64)
		}
		setBit(m.received, idx)
		firstGroupCopy := m.deliveries == 0
		if stream == 3 {
			if firstGroupCopy {
				m.deliveries = 1
			} else {
				m.duplicates++
			}
		} else {
			m.deliveries++
		}
		if (stream != 3 || firstGroupCopy) && !m.sentAt.IsZero() && !at.Before(m.sentAt) {
			d := at.Sub(m.sentAt).Nanoseconds()
			if isMeasurePhase(m.phase) {
				s := c.stepLocked(m.step)
				s.latency[stream] = append(s.latency[stream], d)
			}
			m.firstLatency = append(m.firstLatency, d)
		}
	}
	if stream == 3 {
		m.memberDeliveries[spec.id]++
	}
}
func (c *collector) recordRetainedLocked(spec clientSpec, stream int, sequence uint64, topic string, qos byte, retain bool, at time.Time) {
	r := c.retained[spec.id]
	if r == nil || stream < 0 || stream >= len(bp02Streams) {
		c.unexpectedDeliveries++
		return
	}
	key := messageKey{stream, sequence}
	expected, ok := r.latest[topic]
	unexpected := !ok || stream != spec.stream || !topicMatches(spec.filter, topic)
	stale := !unexpected && key != expected
	wrong := !unexpected && (!retain || qos != 1)
	duplicate := r.seen[key] > 0
	r.seen[key]++
	if stale {
		r.stale++
	}
	if duplicate {
		r.duplicates++
	}
	if wrong {
		r.wrongFlags++
	}
	if unexpected {
		r.unexpected++
		c.unexpectedDeliveries++
	}
	if !duplicate && !stale && !unexpected {
		r.delivered++
	}
	r.records = append(r.records, retainedRecord{ClientID: spec.id, Stream: bp02Streams[stream].name, Sequence: sequence, Topic: topic, Retain: retain, QoS: qos, At: at.UTC().Format(time.RFC3339Nano), Stale: stale, Unexpected: unexpected, Duplicate: duplicate, WrongFlags: wrong})
	if r.delivered == uint64(len(r.latest)) && r.completed.IsZero() {
		r.completed = at
	}
}
func (c *collector) recordAck(stream int, sequence uint64, at time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	m := c.messages[messageKey{stream, sequence}]
	if m == nil || m.sentAt.IsZero() || at.Before(m.sentAt) {
		return
	}
	d := at.Sub(m.sentAt).Nanoseconds()
	m.ackLatency = append(m.ackLatency, d)
	if isMeasurePhase(m.phase) {
		s := c.stepLocked(m.step)
		s.ackLatency[stream] = append(s.ackLatency[stream], d)
	}
}
func (c *collector) ackTimeout(stream int, sequence uint64) {
	c.mu.Lock()
	if m := c.messages[messageKey{stream, sequence}]; m != nil {
		m.ackTimeouts++
	}
	c.mu.Unlock()
}
func (c *collector) protocolError()  { c.mu.Lock(); c.protocolErrors++; c.mu.Unlock() }
func (c *collector) transportError() { c.mu.Lock(); c.transportErrors++; c.mu.Unlock() }
func (c *collector) executionFailure(reason string) {
	c.mu.Lock()
	c.executionFailures = appendUnique(c.executionFailures, reason)
	c.mu.Unlock()
}
func (c *collector) recordSetupStart(at time.Time) { c.mu.Lock(); c.setupStart = at; c.mu.Unlock() }
func (c *collector) recordSetupEnd(at time.Time)   { c.mu.Lock(); c.setupEnd = at; c.mu.Unlock() }
func (c *collector) recordSetupClient(spec clientSpec, timing mqttclient.ConnectTiming) {
	c.mu.Lock()
	c.setupClients[spec.id] = timing
	if spec.tls {
		c.setupTLS = append(c.setupTLS, timing.Latency.Nanoseconds())
	} else {
		c.setupTCP = append(c.setupTCP, timing.Latency.Nanoseconds())
	}
	c.mu.Unlock()
}
func (c *collector) recordSetupSubscribe(sentAt, ackedAt time.Time, reasons []byte) {
	c.mu.Lock()
	if len(reasons) > 0 && !ackedAt.Before(sentAt) {
		c.setupSubscribe = append(c.setupSubscribe, ackedAt.Sub(sentAt).Nanoseconds())
	}
	c.mu.Unlock()
}
func (c *collector) recordOfflineConnect(spec clientSpec, timing mqttclient.ConnectTiming) {
	c.mu.Lock()
	defer c.mu.Unlock()
	o := c.offline[spec.id]
	if o == nil {
		return
	}
	o.sessionPresent = timing.SessionPresent
	o.connected = timing.ConnAckAt
	o.connectLatency = timing.Latency.Nanoseconds()
}
func (c *collector) offlineComplete() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, state := range c.offline {
		if !state.sessionPresent {
			return false
		}
		var delivered uint64
		for _, count := range state.seen {
			delivered += min(count, uint64(1))
		}
		if delivered < state.expected {
			return false
		}
	}
	return true
}
func (c *collector) recordRetainedStart(spec clientSpec, at time.Time) {
	c.mu.Lock()
	if r := c.retained[spec.id]; r != nil {
		r.started = at
	}
	c.mu.Unlock()
}
func (c *collector) retainedComplete(clientID string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	r := c.retained[clientID]
	return r != nil && uint64(len(r.latest)) == uint64(r.spec.streamCount(c.profile)) && r.delivered >= uint64(len(r.latest))
}
func (c *collector) offlineAndRetainedValid() bool {
	s := c.summary()
	return s.Offline.Clients == 10 && s.Offline.SessionPresent == 10 && s.Offline.Missing == 0 && s.Offline.Duplicates == 0 && s.Offline.Unexpected == 0 && s.Retained.Missing == 0 && s.Retained.Duplicates == 0 && s.Retained.Unexpected == 0 && s.Retained.Stale == 0 && s.Retained.WrongFlags == 0
}
func (c *collector) finishWarmup(step int, at time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, m := range c.messageOrder {
		if m.step == step && strings.HasSuffix(m.phase, "-warmup") {
			m.warmupMissing = uint64(max(0, m.expectedCount-int(m.deliveries)))
			m.warmupDuplicates = m.duplicates
			m.snapshot = true
			m.snapshotDelivered = m.deliveries
			m.snapshotDuplicates = m.duplicates
			_ = at
		}
	}
}
func (c *collector) finishStep(step int, at time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, m := range c.messageOrder {
		if m.step == step && isMeasurePhase(m.phase) && !m.snapshot {
			m.snapshot = true
			m.snapshotMissing = uint64(max(0, m.expectedCount-int(m.deliveries)))
			m.snapshotDelivered = m.deliveries
			m.snapshotDuplicates = m.duplicates
		}
		_ = at
	}
}
func (c *collector) stepComplete(step int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	found := false
	for _, m := range c.messageOrder {
		if m.step != step || !isMeasurePhase(m.phase) {
			continue
		}
		found = true
		if int(m.deliveries) < m.expectedCount || bp02Streams[m.stream].qos > 0 && len(m.ackLatency) == 0 {
			return false
		}
	}
	return found
}
func (c *collector) recordFinalCheck(success bool) {
	c.mu.Lock()
	c.finalCheck = success
	c.finalCheckRecorded = true
	c.mu.Unlock()
}
func (c *collector) valid() bool { return c.summary().Valid }
func appendUnique(values []string, value string) []string {
	for _, v := range values {
		if v == value {
			return values
		}
	}
	return append(values, value)
}
func bitSet(words []uint64, i int) bool {
	return i >= 0 && i/64 < len(words) && words[i/64]&(uint64(1)<<uint(i%64)) != 0
}
func setBit(words []uint64, i int) { words[i/64] |= uint64(1) << uint(i%64) }
func (c *collector) stepLocked(step int) *stepState {
	s := c.steps[step]
	if s == nil {
		s = &stepState{latency: make(map[int][]int64), ackLatency: make(map[int][]int64)}
		c.steps[step] = s
	}
	return s
}
func isMeasurePhase(phase string) bool { return strings.HasSuffix(phase, "-measure") }
func (spec clientSpec) streamCount(p Profile) int {
	if spec.stream == 6 {
		return p.RetainedTopicsTenantA
	}
	return p.RetainedTopicsTenantB
}

func (c *collector) configureRetainedOracle() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for id, r := range c.retained {
		count := r.spec.streamCount(c.profile)
		for i := range count {
			sequence := uint64(count + i + 1)
			topic := retainedTopic(c.profile, r.spec.stream, sequence)
			r.latest[topic] = messageKey{r.spec.stream, sequence}
		}
		c.retained[id] = r
	}
}
func (c *collector) buildStepSummaryLocked(step int, rate uint64) stepSummary {
	state := c.stepLocked(step)
	seconds := float64(c.profile.MeasureMillis) / 1000
	streams := make(map[string]streamSummary, 6)
	var total uint64
	for stream := range 6 {
		summary := streamSummary{}
		if stream == 3 {
			summary.MemberDeliveries = make(map[string]uint64)
		}
		for _, message := range c.messageOrder {
			if message.step != step || message.stream != stream {
				continue
			}
			if isMeasurePhase(message.phase) {
				summary.Sent++
				summary.ExpectedDeliveries += uint64(message.expectedCount)
				if message.snapshot {
					summary.Delivered += message.snapshotDelivered
					summary.Missing += message.snapshotMissing
				} else {
					summary.Delivered += message.deliveries
					summary.Missing += uint64(max(0, message.expectedCount-int(message.deliveries)))
				}
				summary.Duplicates += message.duplicates
				summary.Unexpected += message.unexpected
				summary.QoSMismatches += message.qosMismatches
				summary.AckTimeouts += message.ackTimeouts
				for id, count := range message.memberDeliveries {
					summary.MemberDeliveries[id] += count
				}
			} else if strings.HasSuffix(message.phase, "-warmup") {
				summary.WarmupSent++
				if message.snapshot {
					summary.WarmupMissing += message.warmupMissing
					summary.WarmupDuplicates += message.warmupDuplicates
				} else {
					summary.WarmupMissing += uint64(max(0, message.expectedCount-int(message.deliveries)))
					summary.WarmupDuplicates += message.duplicates
				}
			}
		}
		summary.SentPerSecond = float64(summary.Sent) / seconds
		summary.DeliveredPerSecond = float64(summary.Delivered) / seconds
		summary.DeliveryLatency = summarizeLatency(state.latency[stream])
		summary.PublishAckLatency = summarizeLatency(state.ackLatency[stream])
		if stream != 3 {
			summary.MemberDeliveries = nil
		}
		streams[bp02Streams[stream].name] = summary
		total += summary.Delivered
	}
	return stepSummary{
		Step: step, RatePerStreamPerSecond: rate, MeasureSeconds: seconds,
		MaxScheduleLagNanoseconds:   state.maxScheduleLag.Nanoseconds(),
		ScheduleIntervalNanoseconds: state.scheduleInterval.Nanoseconds(),
		GeneratorOverload:           state.overload, DeliveredPerSecond: float64(total) / seconds, Streams: streams,
	}
}
func (c *collector) summary() workloadSummary {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.summaryLocked()
}
func (c *collector) summaryLocked() workloadSummary {
	reasons := append([]string(nil), c.executionFailures...)
	planned := len(bp02SetupClients(c.profile))
	if len(c.setupClients) != planned {
		reasons = append(reasons, fmt.Sprintf("setup client count %d does not match planned count %d", len(c.setupClients), planned))
	}
	if c.protocolErrors > 0 {
		reasons = append(reasons, fmt.Sprintf("protocolErrors=%d", c.protocolErrors))
	}
	if c.transportErrors > 0 {
		reasons = append(reasons, fmt.Sprintf("transportErrors=%d", c.transportErrors))
	}
	if c.unexpectedDeliveries > 0 {
		reasons = append(reasons, fmt.Sprintf("unexpectedDeliveries=%d", c.unexpectedDeliveries))
	}
	if !c.finalCheckRecorded || !c.finalCheck {
		reasons = append(reasons, "final_check did not succeed")
	}
	steps := make([]stepSummary, 0, len(c.profile.RatesPerStreamPerSecond))
	for i, r := range c.profile.RatesPerStreamPerSecond {
		s := c.buildStepSummaryLocked(i+1, r)
		steps = append(steps, s)
		if s.GeneratorOverload {
			reasons = append(reasons, fmt.Sprintf("step %02d generator overload", i+1))
		}
		for name, v := range s.Streams {
			if v.Missing+v.Duplicates+v.Unexpected+v.QoSMismatches+v.AckTimeouts > 0 {
				reasons = append(reasons, fmt.Sprintf("step %02d stream %s has missing=%d duplicates=%d unexpected=%d qosMismatches=%d ackTimeouts=%d", i+1, name, v.Missing, v.Duplicates, v.Unexpected, v.QoSMismatches, v.AckTimeouts))
			}
		}
	}
	off := c.offlineSummaryLocked()
	ret := c.retainedSummaryLocked()
	if off.Clients != 10 || off.SessionPresent != 10 || off.Missing+off.Duplicates+off.Unexpected > 0 {
		reasons = append(reasons, "offline session delivery invariant failed")
	}
	if ret.Missing+ret.Duplicates+ret.Unexpected+ret.Stale+ret.WrongFlags > 0 || len(ret.Subscribers) != 4 {
		reasons = append(reasons, "retained replay invariant failed")
	}
	filterCount := 0
	for _, s := range c.subscribers {
		if s.filter != "" {
			filterCount++
		}
	}
	duration := int64(0)
	if !c.setupStart.IsZero() && !c.setupEnd.Before(c.setupStart) {
		duration = c.setupEnd.Sub(c.setupStart).Milliseconds()
	}
	tlsN, tcpN := len(c.setupTLS), len(c.setupTCP)
	return workloadSummary{SchemaVersion: 1, Target: c.target, ProfileID: c.profile.ProfileID, Purpose: c.profile.Purpose, ReleaseGating: false, Valid: len(reasons) == 0, FailureReasons: reasons, Setup: setupSummary{Clients: len(c.setupClients), ExpectedClients: planned, TLSClients: tlsN, TCPClients: tcpN, TLSConnectLatency: summarizeLatency(c.setupTLS), TCPConnectLatency: summarizeLatency(c.setupTCP), SubscribeAckLatency: summarizeLatency(c.setupSubscribe), SubscriberSessions: len(c.subscribers), SubscriptionFilters: filterCount, DurationMillis: duration}, Steps: steps, Offline: off, Retained: ret, ProtocolErrors: c.protocolErrors, TransportErrors: c.transportErrors, UnexpectedDeliveries: c.unexpectedDeliveries, FinalCheck: c.finalCheckRecorded && c.finalCheck}
}
func (c *collector) retainedSummaryLocked() retainedSummary {
	out := retainedSummary{}
	for _, r := range c.retained {
		x := retainedSubscriberSummary{ClientID: r.spec.id, Expected: uint64(len(r.latest)), Delivered: r.delivered, Duplicates: r.duplicates, Unexpected: r.unexpected, Stale: r.stale, WrongFlags: r.wrongFlags}
		if x.Delivered < x.Expected {
			x.Missing = x.Expected - x.Delivered
		}
		if !r.started.IsZero() && !r.completed.IsZero() && !r.completed.Before(r.started) {
			x.ReplayDurationNanoseconds = r.completed.Sub(r.started).Nanoseconds()
		}
		out.Missing += x.Missing
		out.Duplicates += x.Duplicates
		out.Unexpected += x.Unexpected
		out.Stale += x.Stale
		out.WrongFlags += x.WrongFlags
		if x.ReplayDurationNanoseconds > out.ReplayDurationMaxNanoseconds {
			out.ReplayDurationMaxNanoseconds = x.ReplayDurationNanoseconds
		}
		out.Subscribers = append(out.Subscribers, x)
	}
	sort.Slice(out.Subscribers, func(i, j int) bool { return out.Subscribers[i].ClientID < out.Subscribers[j].ClientID })
	return out
}

func (c *collector) offlineSummaryLocked() offlineSummary {
	out := offlineSummary{Clients: len(c.offline)}
	connectLatencies := make([]int64, 0, len(c.offline))
	for _, state := range c.offline {
		item := offlineSubscriberSummary{
			ClientID: state.spec.id, SessionPresent: state.sessionPresent, Expected: state.expected,
			Unexpected: state.unexpected, DrainDurationNanoseconds: state.drain.Nanoseconds(),
		}
		if state.sessionPresent {
			out.SessionPresent++
		}
		connectLatencies = append(connectLatencies, state.connectLatency)
		for _, count := range state.seen {
			item.Delivered += min(count, uint64(1))
			if count > 1 {
				item.Duplicates += count - 1
			}
		}
		if item.Delivered < item.Expected {
			item.Missing = item.Expected - item.Delivered
		}
		out.Missing += item.Missing
		out.Duplicates += item.Duplicates
		out.Unexpected += item.Unexpected
		if item.DrainDurationNanoseconds > out.DrainDurationMaxNanoseconds {
			out.DrainDurationMaxNanoseconds = item.DrainDurationNanoseconds
		}
		out.Subscribers = append(out.Subscribers, item)
	}
	sort.Slice(out.Subscribers, func(i, j int) bool { return out.Subscribers[i].ClientID < out.Subscribers[j].ClientID })
	out.ReconnectConnAckLatency = summarizeLatency(connectLatencies)
	return out
}
func (c *collector) write(outputDir string) error {
	c.mu.Lock()
	summary := c.summaryLocked()
	messages := append([]*messageState(nil), c.messageOrder...)
	retained := make([]retainedRecord, 0)
	for _, r := range c.retained {
		retained = append(retained, r.records...)
	}
	c.mu.Unlock()
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		return err
	}
	if err := writeBP02Messages(filepath.Join(outputDir, "messages.jsonl"), messages, c.subscribers); err != nil {
		return err
	}
	sort.Slice(retained, func(i, j int) bool {
		if retained[i].ClientID != retained[j].ClientID {
			return retained[i].ClientID < retained[j].ClientID
		}
		if retained[i].At != retained[j].At {
			return retained[i].At < retained[j].At
		}
		return retained[i].Sequence < retained[j].Sequence
	})
	w, err := jsonl.Open(filepath.Join(outputDir, "retained.jsonl"))
	if err != nil {
		return err
	}
	var writeErr error
	for _, record := range retained {
		writeErr = errors.Join(writeErr, w.Write(record))
	}
	if err := errors.Join(writeErr, w.Close()); err != nil {
		return err
	}
	data, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if err := os.WriteFile(filepath.Join(outputDir, "workload-summary.json"), data, 0o644); err != nil {
		return fmt.Errorf("write workload summary: %w", err)
	}
	return nil
}
func writeBP02Messages(path string, messages []*messageState, subs []clientSpec) error {
	sort.Slice(messages, func(i, j int) bool {
		if messages[i].step != messages[j].step {
			return messages[i].step < messages[j].step
		}
		if messages[i].stream != messages[j].stream {
			return messages[i].stream < messages[j].stream
		}
		return messages[i].sequence < messages[j].sequence
	})
	writer, err := jsonl.Open(path)
	if err != nil {
		return err
	}
	var writeErr error
	for _, m := range messages {
		delivered, missing, duplicates := m.deliveries, uint64(max(0, m.expectedCount-int(m.deliveries))), m.duplicates
		if m.snapshot {
			delivered = m.snapshotDelivered
			missing = m.snapshotMissing
			if strings.HasSuffix(m.phase, "-warmup") {
				missing = m.warmupMissing
				duplicates = m.snapshotDuplicates
			} else {
				duplicates = m.duplicates
			}
		}
		record := messageRecord{
			Stream: bp02Streams[m.stream].name, Sequence: m.sequence, Step: m.step, Phase: m.phase,
			Topic: m.topic, Expected: uint64(m.expectedCount), Delivered: delivered, Missing: missing,
			Duplicates: duplicates, Unexpected: m.unexpected, QoSMismatches: m.qosMismatches, AckTimeouts: m.ackTimeouts,
		}
		if !m.scheduledAt.IsZero() {
			record.ScheduledAt = m.scheduledAt.UTC().Format(time.RFC3339Nano)
		}
		if !m.sentAt.IsZero() {
			record.SentAt = m.sentAt.UTC().Format(time.RFC3339Nano)
		}
		if missing > 0 {
			for i, subscriber := range subs {
				if bitSet(m.expected, i) && !bitSet(m.received, i) {
					record.MissingRecipients = append(record.MissingRecipients, subscriber.id)
				}
			}
		}
		writeErr = errors.Join(writeErr, writer.Write(record))
	}
	return errors.Join(writeErr, writer.Close())
}
func summarizeLatency(values []int64) latencySummary {
	if len(values) == 0 {
		return latencySummary{}
	}
	sorted := append([]int64(nil), values...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	return latencySummary{
		P50Nanoseconds: nearest(sorted, 50, 100), P95Nanoseconds: nearest(sorted, 95, 100),
		P99Nanoseconds: nearest(sorted, 99, 100), P999Nanoseconds: nearest(sorted, 999, 1000),
		MaxNanoseconds: sorted[len(sorted)-1],
	}
}
func nearest(v []int64, n, d int) int64 {
	rank := (n*len(v) + d - 1) / d
	if rank < 1 {
		rank = 1
	}
	return v[rank-1]
}
