package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/bits"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"narumq/tests/benchmark/internal/jsonl"
)

type collector struct {
	mu                    sync.Mutex
	profile               Profile
	target                string
	subscribers           []subscriberSpec
	classSubscribers      map[string][]subscriberSpec
	globalToLocal         []int
	recipientPlans        map[planKey]*recipientPlan
	messages              map[messageKey]*messageState
	messageOrder          []*messageState
	steps                 map[int]*stepState
	setupClients          int
	setupStart            time.Time
	setupEnd              time.Time
	setupSubscribeLatency []int64
	protocolErrors        uint64
	transportErrors       uint64
	unexpectedDeliveries  uint64
	routeDeliveries       uint64
	routeProbes           []routeProbeRecord
	routeProbeIndex       map[string]int
	routeProbeDeadlines   map[string]time.Time
	routeDeliveryCounts   map[string]int
	routeLateCounts       map[string]int
	finalCheck            bool
	finalCheckRecorded    bool
	executionFailures     []string
}

type planKey struct {
	class string
	topic string
}

type recipientPlan struct {
	expectedBits       []uint64
	expectedRecipientN int
	expectedDeliveries int
}

type messageKey struct {
	class    string
	sequence uint64
}

type messageState struct {
	class              string
	sequence           uint64
	step               int
	phase              string
	topic              string
	scheduledAt        time.Time
	sentAt             time.Time
	plan               *recipientPlan
	receivedBits       []uint64
	overlapCopies      []uint8
	sharedCopies       []uint32
	firstDeliveryCount int
	duplicates         uint64
	extraCopies        uint64
	unexpected         uint64
	warmupDelivered    int
	warmupMissing      int
	warmupDuplicates   uint64
	warmupExtraCopies  uint64
	warmupMissingBits  []uint64
	drainDelivered     int
	drainMissing       int
	drainDuplicates    uint64
	drainExtraCopies   uint64
	drainMissingBits   []uint64
	drainFinished      bool
}

type stepState struct {
	maxScheduleLag time.Duration
	generatorLoad  bool
	latencies      map[string][]int64
	warmupFinished time.Time
	drainFinished  time.Time
}

type workloadSummary struct {
	SchemaVersion        int                     `json:"schemaVersion"`
	Target               string                  `json:"target"`
	ProfileID            string                  `json:"profileId"`
	Purpose              string                  `json:"purpose"`
	ReleaseGating        bool                    `json:"releaseGating"`
	Valid                bool                    `json:"valid"`
	FailureReasons       []string                `json:"failureReasons"`
	Setup                setupSummary            `json:"setup"`
	Steps                []stepSummary           `json:"steps"`
	RoutePropagation     routePropagationSummary `json:"routePropagation"`
	ProtocolErrors       uint64                  `json:"protocolErrors"`
	TransportErrors      uint64                  `json:"transportErrors"`
	UnexpectedDeliveries uint64                  `json:"unexpectedDeliveries"`
	FinalCheck           bool                    `json:"finalCheck"`
}

type setupSummary struct {
	Clients             int            `json:"clients"`
	SubscriberSessions  int            `json:"subscriberSessions"`
	SubscriptionFilters int            `json:"subscriptionFilters"`
	SubscribeAckLatency latencySummary `json:"subscribeAckLatency"`
	DurationMillis      int64          `json:"durationMillis"`
}

type stepSummary struct {
	Step                        int                     `json:"step"`
	RatePerStreamPerSecond      uint64                  `json:"ratePerStreamPerSecond"`
	MeasureSeconds              float64                 `json:"measureSeconds"`
	MaxScheduleLagNanoseconds   int64                   `json:"maxScheduleLagNanoseconds"`
	ScheduleIntervalNanoseconds int64                   `json:"scheduleIntervalNanoseconds"`
	GeneratorOverload           bool                    `json:"generatorOverload"`
	DeliveredPerSecond          float64                 `json:"deliveredPerSecond"`
	Classes                     map[string]classSummary `json:"classes"`
}

type classSummary struct {
	Sent               uint64            `json:"sent"`
	ExpectedDeliveries uint64            `json:"expectedDeliveries"`
	Delivered          uint64            `json:"delivered"`
	Missing            uint64            `json:"missing"`
	Duplicates         uint64            `json:"duplicates"`
	ExtraCopies        uint64            `json:"extraCopies"`
	Unexpected         uint64            `json:"unexpected"`
	SentPerSecond      float64           `json:"sentPerSecond"`
	DeliveredPerSecond float64           `json:"deliveredPerSecond"`
	DeliveryLatency    latencySummary    `json:"deliveryLatency"`
	WarmupSent         uint64            `json:"warmupSent"`
	WarmupMissing      uint64            `json:"warmupMissing"`
	WarmupDuplicates   uint64            `json:"warmupDuplicates"`
	WarmupExtraCopies  uint64            `json:"warmupExtraCopies"`
	MemberDeliveries   map[string]uint64 `json:"memberDeliveries,omitempty"`
}

type latencySummary struct {
	P50Nanoseconds  int64 `json:"p50Nanoseconds"`
	P95Nanoseconds  int64 `json:"p95Nanoseconds"`
	P99Nanoseconds  int64 `json:"p99Nanoseconds"`
	P999Nanoseconds int64 `json:"p999Nanoseconds"`
	MaxNanoseconds  int64 `json:"maxNanoseconds"`
}

type routePropagationSummary struct {
	Probes              int            `json:"probes"`
	Succeeded           int            `json:"succeeded"`
	DeliveriesObserved  int            `json:"deliveriesObserved"`
	ProbeIntervalMillis int64          `json:"probeIntervalMillis"`
	SubscribeAckLatency latencySummary `json:"subscribeAckLatency"`
	RouteVisibleLatency latencySummary `json:"routeVisibleLatency"`
}

type messageRecord struct {
	Class             string   `json:"class"`
	Sequence          uint64   `json:"sequence"`
	Step              int      `json:"step"`
	Phase             string   `json:"phase"`
	Topic             string   `json:"topic"`
	ScheduledAt       string   `json:"scheduledAt"`
	SentAt            string   `json:"sentAt"`
	Expected          int      `json:"expected"`
	Delivered         int      `json:"delivered"`
	ExtraCopies       uint64   `json:"extraCopies"`
	Duplicates        uint64   `json:"duplicates"`
	MissingRecipients []string `json:"missingRecipients,omitempty"`
}

func newCollector(profile Profile, target string) *collector {
	subscribers := workloadSubscribers(profile)
	classes := make(map[string][]subscriberSpec, 4)
	globalToLocal := make([]int, len(subscribers))
	for globalIndex, subscriber := range subscribers {
		members := classes[subscriber.class]
		globalToLocal[globalIndex] = len(members)
		classes[subscriber.class] = append(members, subscriber)
	}
	return &collector{
		profile:             profile,
		target:              target,
		subscribers:         subscribers,
		classSubscribers:    classes,
		globalToLocal:       globalToLocal,
		recipientPlans:      make(map[planKey]*recipientPlan),
		messages:            make(map[messageKey]*messageState),
		messageOrder:        make([]*messageState, 0, min(int(profile.MaxMessages), 8192)),
		steps:               make(map[int]*stepState, len(profile.RatesPerStreamPerSecond)),
		routeProbeIndex:     make(map[string]int, profile.RouteProbes),
		routeProbeDeadlines: make(map[string]time.Time, profile.RouteProbes),
		routeDeliveryCounts: make(map[string]int, profile.RouteProbes),
		routeLateCounts:     make(map[string]int, profile.RouteProbes),
	}
}

func (c *collector) addMessage(class string, sequence uint64, step int, phase, topic string, scheduledAt time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if uint64(len(c.messages)) >= c.profile.MaxMessages {
		return fmt.Errorf("message count exceeds profile maxMessages %d", c.profile.MaxMessages)
	}
	key := messageKey{class: class, sequence: sequence}
	if _, exists := c.messages[key]; exists {
		return fmt.Errorf("duplicate message key class=%s sequence=%d", class, sequence)
	}
	members, exists := c.classSubscribers[class]
	if !exists {
		return fmt.Errorf("unknown message class %q", class)
	}
	planKey := planKey{class: class, topic: topic}
	plan := c.recipientPlans[planKey]
	if plan == nil {
		plan = c.buildRecipientPlan(class, topic)
		c.recipientPlans[planKey] = plan
	}
	state := &messageState{
		class:        class,
		sequence:     sequence,
		step:         step,
		phase:        phase,
		topic:        topic,
		scheduledAt:  scheduledAt,
		plan:         plan,
		receivedBits: make([]uint64, len(plan.expectedBits)),
	}
	switch class {
	case classOverlap:
		state.overlapCopies = make([]uint8, len(members))
	case classShared:
		state.sharedCopies = make([]uint32, len(members))
	}
	c.messages[key] = state
	c.messageOrder = append(c.messageOrder, state)
	return nil
}

func (c *collector) buildRecipientPlan(class, topic string) *recipientPlan {
	members := c.classSubscribers[class]
	words := (len(members) + 63) / 64
	plan := &recipientPlan{expectedBits: make([]uint64, words)}
	for _, globalIndex := range expectedRecipients(c.subscribers, class, topic) {
		localIndex := c.globalToLocal[globalIndex]
		plan.expectedBits[localIndex/64] |= uint64(1) << (localIndex % 64)
		plan.expectedRecipientN++
	}
	plan.expectedDeliveries = plan.expectedRecipientN
	if class == classShared && plan.expectedRecipientN > 0 {
		plan.expectedDeliveries = 1
	}
	return plan
}

func (c *collector) markSent(class string, sequence uint64, sentAt time.Time) {
	c.mu.Lock()
	if message := c.messages[messageKey{class: class, sequence: sequence}]; message != nil {
		message.sentAt = sentAt
	}
	c.mu.Unlock()
}

func (c *collector) recordScheduleLag(step int, lag, interval time.Duration) {
	c.mu.Lock()
	state := c.stepLocked(step)
	if lag > state.maxScheduleLag {
		state.maxScheduleLag = lag
	}
	if lag >= interval {
		state.generatorLoad = true
	}
	c.mu.Unlock()
}

func (c *collector) recordDelivery(subscriberIndex, streamIndex int, sequence uint64, topic string, receivedAt time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	class := classForStreamIndex(streamIndex)
	if class == "" || subscriberIndex < 0 || subscriberIndex >= len(c.subscribers) {
		c.unexpectedDeliveries++
		return
	}
	message := c.messages[messageKey{class: class, sequence: sequence}]
	if message == nil {
		c.unexpectedDeliveries++
		return
	}
	if message.topic != topic || c.subscribers[subscriberIndex].class != class {
		message.unexpected++
		c.unexpectedDeliveries++
		return
	}
	localIndex := c.globalToLocal[subscriberIndex]
	if localIndex < 0 || !bitSet(message.plan.expectedBits, localIndex) {
		message.unexpected++
		c.unexpectedDeliveries++
		return
	}
	alreadyDelivered := bitSet(message.receivedBits, localIndex)
	if !alreadyDelivered {
		setBit(message.receivedBits, localIndex)
		message.firstDeliveryCount++
		if isMeasurePhase(message.phase) && !message.sentAt.IsZero() && !receivedAt.Before(message.sentAt) {
			step := c.stepLocked(message.step)
			step.latencies[class] = append(step.latencies[class], receivedAt.Sub(message.sentAt).Nanoseconds())
		}
	}
	switch class {
	case classOverlap:
		if message.overlapCopies[localIndex] < 4 {
			message.overlapCopies[localIndex]++
		}
		copies := message.overlapCopies[localIndex]
		if copies > 3 {
			message.duplicates++
		} else if copies > 1 {
			message.extraCopies++
		}
	case classShared:
		message.sharedCopies[localIndex]++
		if alreadyDelivered || message.firstDeliveryCount > 1 {
			message.duplicates++
		}
	default:
		if alreadyDelivered {
			message.duplicates++
		}
	}
}

func isMeasurePhase(phase string) bool {
	return len(phase) >= len("step-01-measure") && phase[len(phase)-len("-measure"):] == "-measure"
}

func bitSet(bits []uint64, index int) bool {
	return index >= 0 && index/64 < len(bits) && bits[index/64]&(uint64(1)<<(index%64)) != 0
}

func setBit(bits []uint64, index int) {
	bits[index/64] |= uint64(1) << (index % 64)
}

func bitCount(words []uint64) int {
	count := 0
	for _, word := range words {
		count += bits.OnesCount64(word)
	}
	return count
}

func (c *collector) recordUnexpectedDelivery(class string, sequence uint64, topic string, at time.Time) {
	c.mu.Lock()
	c.unexpectedDeliveries++
	if class != "" && sequence != 0 {
		if message := c.messages[messageKey{class: class, sequence: sequence}]; message != nil {
			message.unexpected++
		}
	}
	c.mu.Unlock()
}

func (c *collector) beginRouteProbe(topic string, deadline time.Time) {
	c.mu.Lock()
	c.routeProbeDeadlines[topic] = deadline
	c.mu.Unlock()
}

func (c *collector) recordRouteDelivery(topic string, at time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.routeDeliveries++
	previous := c.routeDeliveryCounts[topic]
	c.routeDeliveryCounts[topic] = previous + 1
	deadline, hasDeadline := c.routeProbeDeadlines[topic]
	late := hasDeadline && !at.Before(deadline)
	if late {
		c.routeLateCounts[topic]++
	}
	if index, exists := c.routeProbeIndex[topic]; exists {
		probe := &c.routeProbes[index]
		probe.DeliveriesObserved++
		if previous > 0 {
			probe.AdditionalDeliveries++
		}
		if late {
			probe.LateDeliveries++
		}
	}
}

func (c *collector) protocolError() {
	c.mu.Lock()
	c.protocolErrors++
	c.mu.Unlock()
}

func (c *collector) transportError() {
	c.mu.Lock()
	c.transportErrors++
	c.mu.Unlock()
}

func (c *collector) recordSetupStart(at time.Time) {
	c.mu.Lock()
	c.setupStart = at
	c.mu.Unlock()
}

func (c *collector) recordSetupEnd(at time.Time) {
	c.mu.Lock()
	c.setupEnd = at
	c.mu.Unlock()
}

func (c *collector) recordSetupSubscribe(sentAt, ackedAt time.Time, reasonCodes []byte) {
	c.mu.Lock()
	if len(reasonCodes) > 0 && sentAt.Before(ackedAt) {
		c.setupSubscribeLatency = append(c.setupSubscribeLatency, ackedAt.Sub(sentAt).Nanoseconds())
	}
	c.mu.Unlock()
}

func (c *collector) recordSetupSuccess() {
	c.mu.Lock()
	c.setupClients++
	c.mu.Unlock()
}

func (c *collector) finishWarmup(step int, at time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stepLocked(step).warmupFinished = at
	for _, message := range c.messageOrder {
		if message.step != step || !isWarmupPhase(message.phase) {
			continue
		}
		message.warmupDelivered = deliveredCount(message, message.receivedBits)
		message.warmupMissing = missingCount(message, message.receivedBits)
		message.warmupDuplicates = message.duplicates
		message.warmupExtraCopies = message.extraCopies
		if message.warmupMissing > 0 {
			message.warmupMissingBits = missingBits(message)
		}
	}
}

func isWarmupPhase(phase string) bool {
	return len(phase) >= len("step-01-warmup") && phase[len(phase)-len("-warmup"):] == "-warmup"
}

func (c *collector) stepComplete(step int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	seenMessages := 0
	for _, message := range c.messageOrder {
		if message.step != step || message.phase != fmt.Sprintf("step-%02d-measure", step) {
			continue
		}
		seenMessages++
		if message.class == classShared {
			if message.firstDeliveryCount == 0 {
				return false
			}
		} else if message.firstDeliveryCount < message.plan.expectedRecipientN {
			return false
		}
	}
	return seenMessages > 0
}

func (c *collector) finishStep(step int, at time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stepLocked(step).drainFinished = at
	for _, message := range c.messageOrder {
		if message.step != step || message.phase != fmt.Sprintf("step-%02d-measure", step) {
			continue
		}
		message.drainFinished = true
		message.drainDelivered = deliveredCount(message, message.receivedBits)
		message.drainMissing = missingCount(message, message.receivedBits)
		message.drainDuplicates = message.duplicates
		message.drainExtraCopies = message.extraCopies
		if message.drainMissing > 0 {
			message.drainMissingBits = missingBits(message)
		}
	}
}

func deliveredCount(message *messageState, bits []uint64) int {
	count := bitCount(bits)
	if message.class == classShared && count > 0 {
		return 1
	}
	return count
}

func missingCount(message *messageState, received []uint64) int {
	if message.class == classShared {
		if bitCount(received) == 0 {
			return 1
		}
		return 0
	}
	missing := 0
	for wordIndex, expected := range message.plan.expectedBits {
		missing += bits.OnesCount64(expected &^ received[wordIndex])
	}
	return missing
}

func missingBits(message *messageState) []uint64 {
	bits := make([]uint64, len(message.plan.expectedBits))
	for index, expected := range message.plan.expectedBits {
		bits[index] = expected &^ message.receivedBits[index]
	}
	return bits
}

func (c *collector) recordRouteProbe(record routeProbeRecord) {
	c.mu.Lock()
	record.DeliveriesObserved = c.routeDeliveryCounts[record.Topic]
	if record.DeliveriesObserved > 0 {
		record.AdditionalDeliveries = record.DeliveriesObserved - 1
	}
	record.LateDeliveries = c.routeLateCounts[record.Topic]
	c.routeProbeIndex[record.Topic] = len(c.routeProbes)
	c.routeProbes = append(c.routeProbes, record)
	c.mu.Unlock()
}

func (c *collector) allRouteProbesSucceeded() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.routeProbes) != c.profile.RouteProbes {
		return false
	}
	for _, probe := range c.routeProbes {
		if !probe.Succeeded {
			return false
		}
	}
	return true
}

func (c *collector) recordFinalCheck(success bool) {
	c.mu.Lock()
	c.finalCheck = success
	c.finalCheckRecorded = true
	c.mu.Unlock()
}

func (c *collector) executionFailure(reason string) {
	c.mu.Lock()
	c.executionFailures = appendUnique(c.executionFailures, reason)
	c.mu.Unlock()
}

func appendUnique(values []string, value string) []string {
	for _, existing := range values {
		if existing == value {
			return values
		}
	}
	return append(values, value)
}

func (c *collector) valid() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.failureReasonsLocked()) == 0
}

func (c *collector) failureReasonsLocked() []string {
	reasons := append([]string(nil), c.executionFailures...)
	expectedClients := bp04NodeCount + len(workloadPublishers()) + len(c.subscribers) + c.profile.RouteProbes
	if c.setupClients != expectedClients {
		reasons = append(reasons, fmt.Sprintf("setup client count %d does not match planned count %d", c.setupClients, expectedClients))
	}
	if c.protocolErrors != 0 {
		reasons = append(reasons, fmt.Sprintf("protocolErrors=%d", c.protocolErrors))
	}
	if c.transportErrors != 0 {
		reasons = append(reasons, fmt.Sprintf("transportErrors=%d", c.transportErrors))
	}
	if c.unexpectedDeliveries != 0 {
		reasons = append(reasons, fmt.Sprintf("unexpectedDeliveries=%d", c.unexpectedDeliveries))
	}
	if !c.finalCheckRecorded || !c.finalCheck {
		reasons = append(reasons, "final_check did not succeed")
	}
	if len(c.routeProbes) != c.profile.RouteProbes {
		reasons = append(reasons, fmt.Sprintf("route probe count %d does not match planned count %d", len(c.routeProbes), c.profile.RouteProbes))
	} else {
		for index, probe := range c.routeProbes {
			if !probe.Succeeded {
				reasons = append(reasons, fmt.Sprintf("route probe %d failed", index))
			}
		}
	}
	for index := range c.profile.RatesPerStreamPerSecond {
		stepNumber := index + 1
		step := c.buildStepSummaryLocked(stepNumber, c.profile.RatesPerStreamPerSecond[index])
		if step.GeneratorOverload {
			reasons = append(reasons, fmt.Sprintf("step %02d generator overload", stepNumber))
		}
		for _, className := range []string{classFanout, classWildcard, classOverlap, classShared} {
			summary := step.Classes[className]
			if summary.Missing != 0 || summary.Duplicates != 0 || summary.Unexpected != 0 {
				reasons = append(reasons, fmt.Sprintf("step %02d class %s has missing=%d duplicates=%d unexpected=%d", stepNumber, className, summary.Missing, summary.Duplicates, summary.Unexpected))
			}
		}
	}
	return reasons
}

func (c *collector) summaryLocked() workloadSummary {
	failureReasons := c.failureReasonsLocked()
	filterCount := 0
	for _, subscriber := range c.subscribers {
		filterCount += len(subscriber.filters)
	}
	setupDuration := int64(0)
	if !c.setupStart.IsZero() && !c.setupEnd.IsZero() && !c.setupEnd.Before(c.setupStart) {
		setupDuration = c.setupEnd.Sub(c.setupStart).Milliseconds()
	}
	routeAck := make([]int64, 0, len(c.routeProbes))
	routeVisible := make([]int64, 0, len(c.routeProbes))
	succeeded := 0
	for _, probe := range c.routeProbes {
		if probe.SubscribeAckNanoseconds > 0 {
			routeAck = append(routeAck, probe.SubscribeAckNanoseconds)
		}
		if probe.Succeeded {
			succeeded++
			routeVisible = append(routeVisible, probe.RouteVisibleNanoseconds)
		}
	}
	steps := make([]stepSummary, 0, len(c.profile.RatesPerStreamPerSecond))
	for index, rate := range c.profile.RatesPerStreamPerSecond {
		steps = append(steps, c.buildStepSummaryLocked(index+1, rate))
	}
	return workloadSummary{
		SchemaVersion:  1,
		Target:         c.target,
		ProfileID:      c.profile.ProfileID,
		Purpose:        c.profile.Purpose,
		ReleaseGating:  false,
		Valid:          len(failureReasons) == 0,
		FailureReasons: failureReasons,
		Setup: setupSummary{
			Clients:             c.setupClients,
			SubscriberSessions:  len(c.subscribers),
			SubscriptionFilters: filterCount,
			SubscribeAckLatency: summarizeLatency(c.setupSubscribeLatency),
			DurationMillis:      setupDuration,
		},
		Steps: steps,
		RoutePropagation: routePropagationSummary{
			Probes:              len(c.routeProbes),
			Succeeded:           succeeded,
			DeliveriesObserved:  int(c.routeDeliveries),
			ProbeIntervalMillis: c.profile.RouteProbeIntervalMillis,
			SubscribeAckLatency: summarizeLatency(routeAck),
			RouteVisibleLatency: summarizeLatency(routeVisible),
		},
		ProtocolErrors:       c.protocolErrors,
		TransportErrors:      c.transportErrors,
		UnexpectedDeliveries: c.unexpectedDeliveries,
		FinalCheck:           c.finalCheckRecorded && c.finalCheck,
	}
}

func (c *collector) buildStepSummaryLocked(stepNumber int, rate uint64) stepSummary {
	state := c.stepLocked(stepNumber)
	seconds := float64(c.profile.MeasureMillis) / 1000
	classes := make(map[string]classSummary, 4)
	for _, class := range []string{classFanout, classWildcard, classOverlap, classShared} {
		classes[class] = classSummary{MemberDeliveries: memberMapIfShared(class)}
	}
	for _, message := range c.messageOrder {
		if message.step != stepNumber {
			continue
		}
		class := classes[message.class]
		if isWarmupPhase(message.phase) {
			class.WarmupSent++
			class.WarmupMissing += uint64(message.warmupMissing)
			class.WarmupDuplicates += message.warmupDuplicates
			class.WarmupExtraCopies += message.warmupExtraCopies
			classes[message.class] = class
			continue
		}
		if message.phase != fmt.Sprintf("step-%02d-measure", stepNumber) {
			continue
		}
		class.Sent++
		class.ExpectedDeliveries += uint64(message.plan.expectedDeliveries)
		if message.drainFinished {
			class.Delivered += uint64(message.drainDelivered)
			class.Missing += uint64(message.drainMissing)
			class.Duplicates += message.drainDuplicates
			class.ExtraCopies += message.drainExtraCopies
		} else {
			class.Delivered += uint64(deliveredCount(message, message.receivedBits))
			class.Missing += uint64(missingCount(message, message.receivedBits))
			class.Duplicates += message.duplicates
			class.ExtraCopies += message.extraCopies
		}
		class.Unexpected += message.unexpected
		if message.class == classShared {
			for localIndex, member := range c.classSubscribers[classShared] {
				class.MemberDeliveries[member.id] += uint64(message.sharedCopies[localIndex])
			}
		}
		classes[message.class] = class
	}
	var totalDelivered uint64
	for className, class := range classes {
		class.SentPerSecond = float64(class.Sent) / seconds
		class.DeliveredPerSecond = float64(class.Delivered) / seconds
		class.DeliveryLatency = summarizeLatency(state.latencies[className])
		totalDelivered += class.Delivered
		classes[className] = class
	}
	return stepSummary{
		Step:                        stepNumber,
		RatePerStreamPerSecond:      rate,
		MeasureSeconds:              seconds,
		MaxScheduleLagNanoseconds:   state.maxScheduleLag.Nanoseconds(),
		ScheduleIntervalNanoseconds: int64(time.Second / time.Duration(rate)),
		GeneratorOverload:           state.generatorLoad,
		DeliveredPerSecond:          float64(totalDelivered) / seconds,
		Classes:                     classes,
	}
}

func memberMapIfShared(class string) map[string]uint64 {
	if class != classShared {
		return nil
	}
	return make(map[string]uint64)
}

func (c *collector) stepLocked(step int) *stepState {
	state := c.steps[step]
	if state == nil {
		state = &stepState{latencies: make(map[string][]int64, 4)}
		c.steps[step] = state
	}
	return state
}

func (c *collector) summary() workloadSummary {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.summaryLocked()
}

func (c *collector) write(outputDir string) error {
	c.mu.Lock()
	summary := c.summaryLocked()
	messages := append([]*messageState(nil), c.messageOrder...)
	probes := append([]routeProbeRecord(nil), c.routeProbes...)
	c.mu.Unlock()
	if err := writeMessageRecords(filepath.Join(outputDir, "messages.jsonl"), messages, c.classSubscribers); err != nil {
		return err
	}
	sort.Slice(probes, func(i, j int) bool { return probes[i].Probe < probes[j].Probe })
	probeWriter, err := jsonl.Open(filepath.Join(outputDir, "route-probes.jsonl"))
	if err != nil {
		return err
	}
	var probeErr error
	for _, probe := range probes {
		probeErr = errors.Join(probeErr, probeWriter.Write(probe))
	}
	probeErr = errors.Join(probeErr, probeWriter.Close())
	if probeErr != nil {
		return probeErr
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

func writeMessageRecords(path string, messages []*messageState, classSubscribers map[string][]subscriberSpec) error {
	sort.Slice(messages, func(i, j int) bool {
		if messages[i].step != messages[j].step {
			return messages[i].step < messages[j].step
		}
		if messages[i].class != messages[j].class {
			return streamIndexForClass(messages[i].class) < streamIndexForClass(messages[j].class)
		}
		return messages[i].sequence < messages[j].sequence
	})
	writer, err := jsonl.Open(path)
	if err != nil {
		return err
	}
	var writeErr error
	for _, message := range messages {
		missingRecipientBits := message.drainMissingBits
		delivered := message.drainDelivered
		duplicates := message.drainDuplicates
		extraCopies := message.drainExtraCopies
		if isWarmupPhase(message.phase) {
			missingRecipientBits = message.warmupMissingBits
			delivered = message.warmupDelivered
			duplicates = message.warmupDuplicates
			extraCopies = message.warmupExtraCopies
		} else if !message.drainFinished {
			delivered = deliveredCount(message, message.receivedBits)
			duplicates = message.duplicates
			extraCopies = message.extraCopies
			if missingCount(message, message.receivedBits) > 0 {
				missingRecipientBits = missingBits(message)
			}
		}
		record := messageRecord{
			Class:       message.class,
			Sequence:    message.sequence,
			Step:        message.step,
			Phase:       message.phase,
			Topic:       message.topic,
			ScheduledAt: message.scheduledAt.UTC().Format(time.RFC3339Nano),
			SentAt:      message.sentAt.UTC().Format(time.RFC3339Nano),
			Expected:    message.plan.expectedDeliveries,
			Delivered:   delivered,
			ExtraCopies: extraCopies,
			Duplicates:  duplicates,
		}
		if len(missingRecipientBits) > 0 {
			if message.class == classShared {
				record.MissingRecipients = []string{"$share/bp04g"}
			} else {
				for localIndex, member := range classSubscribers[message.class] {
					if bitSet(missingRecipientBits, localIndex) {
						record.MissingRecipients = append(record.MissingRecipients, member.id)
					}
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
		P50Nanoseconds:  percentile(sorted, 50, 100),
		P95Nanoseconds:  percentile(sorted, 95, 100),
		P99Nanoseconds:  percentile(sorted, 99, 100),
		P999Nanoseconds: percentile(sorted, 999, 1000),
		MaxNanoseconds:  sorted[len(sorted)-1],
	}
}

func percentile(sorted []int64, numerator, denominator int) int64 {
	rank := (numerator*len(sorted) + denominator - 1) / denominator
	if rank < 1 {
		rank = 1
	}
	return sorted[rank-1]
}

func classForStreamIndex(index int) string {
	switch index {
	case 0:
		return classFanout
	case 1:
		return classWildcard
	case 2:
		return classOverlap
	case 3:
		return classShared
	default:
		return ""
	}
}
