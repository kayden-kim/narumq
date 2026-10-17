package main

import (
	"fmt"
	"testing"
	"time"

	"narumq/tests/benchmark/internal/mqttclient"
)

// TestSharedGroupSecondMemberDeliveryCountsDuplicate verifies a second shared member delivery counts as a duplicate.
// Scenarios: performance-and-verification.md:PRF-SCN-044
func TestSharedGroupSecondMemberDeliveryCountsDuplicate(t *testing.T) {
	p := bp02SmokeProfile(t)
	c := newCollector(p, "narumq")
	topic := publishTopic(3, 1)
	sent := time.Now()
	if err := c.addMessage(3, 1, 1, "step-01-measure", topic, sent); err != nil {
		t.Fatal(err)
	}
	c.markSent(3, 1, sent)
	members := expectedRecipients(c.subscribers, 3, topic)
	if len(members) < 2 {
		t.Fatalf("shared recipients = %d, want multiple members", len(members))
	}
	c.recordDelivery(c.subscribers[members[0]], 3, 1, topic, 1, false, sent.Add(time.Millisecond))
	c.recordDelivery(c.subscribers[members[1]], 3, 1, topic, 1, false, sent.Add(2*time.Millisecond))
	c.finishStep(1, sent.Add(time.Second))
	got := c.summary().Steps[0].Streams["a_job"]
	if got.ExpectedDeliveries != 1 || got.Delivered != 1 || got.Missing != 0 || got.Duplicates != 1 {
		t.Fatalf("shared stream counters = expected %d delivered %d missing %d duplicates %d", got.ExpectedDeliveries, got.Delivered, got.Missing, got.Duplicates)
	}
	if got.MemberDeliveries[c.subscribers[members[0]].id] != 1 || got.MemberDeliveries[c.subscribers[members[1]].id] != 1 {
		t.Fatalf("shared members not tracked: %v", got.MemberDeliveries)
	}
}

// TestQoSMismatchInvalidatesStep verifies a QoS mismatch invalidates the step.
// Scenarios: performance-and-verification.md:PRF-SCN-044
func TestQoSMismatchInvalidatesStep(t *testing.T) {
	p := bp02SmokeProfile(t)
	c := newCollector(p, "narumq")
	topic := publishTopic(2, 1)
	now := time.Now()
	if err := c.addMessage(2, 1, 1, "step-01-measure", topic, now); err != nil {
		t.Fatal(err)
	}
	c.markSent(2, 1, now)
	for _, i := range expectedRecipients(c.subscribers, 2, topic) {
		c.recordDelivery(c.subscribers[i], 2, 1, topic, 1, false, now.Add(time.Millisecond))
	}
	c.finishStep(1, now.Add(time.Second))
	s := c.summary()
	if s.Valid || s.Steps[0].Streams["a_command"].QoSMismatches != 1 {
		t.Fatalf("QoS mismatch was not invalidated: %#v", s)
	}
}

// TestOfflineSessionNotPresentInvalidatesTarget verifies a missing offline session invalidates the target.
// Scenarios: performance-and-verification.md:PRF-SCN-044
func TestOfflineSessionNotPresentInvalidatesTarget(t *testing.T) {
	p := bp02SmokeProfile(t)
	c := newCollector(p, "narumq")
	c.recordOfflineConnect(c.offline["bp02-a-sub-alert-00"].spec, mqttclient.ConnectTiming{SessionPresent: false, Latency: time.Millisecond, ConnAckAt: time.Now()})
	if c.offlineComplete() {
		t.Fatal("offline workload complete without resumed persistent session")
	}
	if c.valid() {
		t.Fatal("target valid without session-present for all persistent subscribers")
	}
}

// TestRetainedReplayRejectsStaleRoundAndMissingRetainFlag verifies stale round and missing retain flag rejection.
// Scenarios: performance-and-verification.md:PRF-SCN-044
func TestRetainedReplayRejectsStaleRoundAndMissingRetainFlag(t *testing.T) {
	p := bp02SmokeProfile(t)
	c := newCollector(p, "narumq")
	c.configureRetainedOracle()
	spec := c.retained["bp02-a-sub-state-00"].spec
	latestSeq := uint64(p.RetainedTopicsTenantA + 1)
	staleSeq := uint64(1)
	c.recordDelivery(spec, 6, staleSeq, retainedTopic(p, 6, staleSeq), 1, true, time.Now())
	latestTopic := retainedTopic(p, 6, latestSeq)
	c.recordDelivery(spec, 6, latestSeq, latestTopic, 1, false, time.Now())
	s := c.summary()
	if s.Retained.Stale != 1 || s.Retained.WrongFlags != 1 || s.Retained.Missing != uint64(2*p.RetainedTopicsTenantA+2*p.RetainedTopicsTenantB-1) {
		t.Fatalf("retained checks = %#v", s.Retained)
	}
	if c.retainedComplete(spec.id) {
		t.Fatal("retained replay completed with stale and wrong-flag publications")
	}
}
func TestSharedDeliveryLatencyUsesFirstGroupCopy(t *testing.T) {
	p := bp02SmokeProfile(t)
	c := newCollector(p, "narumq")
	topic := publishTopic(3, 1)
	sent := time.Now()
	if err := c.addMessage(3, 1, 1, "step-01-measure", topic, sent); err != nil {
		t.Fatal(err)
	}
	c.markSent(3, 1, sent)
	members := expectedRecipients(c.subscribers, 3, topic)
	c.recordDelivery(c.subscribers[members[0]], 3, 1, topic, 1, false, sent.Add(time.Millisecond))
	c.recordDelivery(c.subscribers[members[1]], 3, 1, topic, 1, false, sent.Add(2*time.Millisecond))
	c.finishStep(1, sent.Add(time.Second))
	got := c.summary().Steps[0].Streams["a_job"].DeliveryLatency
	if got.P99Nanoseconds != int64(time.Millisecond) {
		t.Fatalf("shared delivery P99 = %d ns, want first-copy latency %d ns", got.P99Nanoseconds, time.Millisecond)
	}
}

func TestCrossTenantDeliveryCountsUnexpected(t *testing.T) {
	p := bp02SmokeProfile(t)
	c := newCollector(p, "narumq")
	topic := publishTopic(0, 1)
	now := time.Now()
	if err := c.addMessage(0, 1, 1, "step-01-measure", topic, now); err != nil {
		t.Fatal(err)
	}
	c.recordDelivery(c.subscribers[c.subscriberIndex["bp02-b-sub-telemetry-00"]], 0, 1, topic, 0, false, now.Add(time.Millisecond))
	if got := c.summary().UnexpectedDeliveries; got != 1 {
		t.Fatalf("cross-tenant unexpected deliveries = %d, want 1", got)
	}
}

// TestOfflineCommandExpectationsFollowExactTopics verifies offline command expectations follow exact topics.
// Scenarios: performance-and-verification.md:PRF-SCN-045
func TestOfflineCommandExpectationsFollowExactTopics(t *testing.T) {
	p := bp02SmokeProfile(t)
	c := newCollector(p, "narumq")
	for _, state := range c.offline {
		c.recordOfflineConnect(state.spec, mqttclient.ConnectTiming{SessionPresent: true, Latency: time.Millisecond, ConnAckAt: time.Now()})
	}
	// The fixed smoke publishes 24 messages at rate 2 and 60 at rate 5 per stream.
	const normal uint64 = 84
	for _, stream := range []int{1, 2, 5} {
		for offset := range p.OfflineMessagesPerStream {
			sequence := normal + uint64(offset) + 1
			topic := publishTopic(stream, sequence)
			if err := c.addMessage(stream, sequence, 0, "offline-publish", topic, time.Time{}); err != nil {
				t.Fatal(err)
			}
			sent := time.Now()
			c.markSent(stream, sequence, sent)
			for _, index := range expectedRecipients(c.subscribers, stream, topic) {
				c.recordDelivery(c.subscribers[index], stream, sequence, topic, bp02Streams[stream].qos, false, sent.Add(time.Millisecond))
			}
		}
	}
	summary := c.summary().Offline
	if summary.Clients != 10 || summary.SessionPresent != 10 || summary.Missing != 0 || summary.Duplicates != 0 || summary.Unexpected != 0 {
		t.Fatalf("offline summary = %#v", summary)
	}
	expected := make(map[string]uint64)
	for _, subscriber := range summary.Subscribers {
		expected[subscriber.ClientID] = subscriber.Expected
	}
	for _, id := range []string{"bp02-a-sub-alert-00", "bp02-a-sub-alert-01", "bp02-a-sub-alert-02", "bp02-a-sub-alert-03", "bp02-b-sub-alert-00", "bp02-b-sub-alert-01"} {
		if expected[id] != uint64(p.OfflineMessagesPerStream) {
			t.Errorf("%s expected %d, want %d", id, expected[id], p.OfflineMessagesPerStream)
		}
	}
	for index := range 4 {
		id := fmt.Sprintf("bp02-a-sub-command-%02d", index)
		if expected[id] != uint64(p.OfflineMessagesPerStream/4) {
			t.Errorf("%s expected %d, want %d", id, expected[id], p.OfflineMessagesPerStream/4)
		}
	}
	if !c.offlineComplete() {
		t.Fatal("offline phase did not complete after every planned recipient delivery")
	}
}

// TestOfflineCommandOracleUsesPerPhasePublishCounts verifies the per-phase publish count oracle.
// Scenarios: performance-and-verification.md:PRF-SCN-045
func TestOfflineCommandOracleUsesPerPhasePublishCounts(t *testing.T) {
	cases := []struct {
		name          string
		rates         []uint64
		warmupMillis  int64
		measureMillis int64
		normalCount   uint64
		commandIndex  int
	}{
		{"fractional-total", []uint64{3}, 1001, 3001, 12, 0},
		{"separate-phase-floor", []uint64{1}, 1500, 3500, 4, 0},
		{"multiple-rates", []uint64{1, 3}, 1500, 3500, 18, 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := bp02SmokeProfile(t)
			p.RatesPerStreamPerSecond = tc.rates
			p.WarmupMillis = tc.warmupMillis
			p.MeasureMillis = tc.measureMillis
			p.OfflineMessagesPerStream = 1
			c := newCollector(p, "narumq")
			now := time.Now()
			for _, state := range c.offline {
				c.recordOfflineConnect(state.spec, mqttclient.ConnectTiming{SessionPresent: true, ConnAckAt: now})
			}
			sequence := tc.normalCount + 1
			for _, stream := range []int{1, 2, 5} {
				topic := publishTopic(stream, sequence)
				if err := c.addMessage(stream, sequence, 0, "offline-publish", topic, now); err != nil {
					t.Fatal(err)
				}
				c.markSent(stream, sequence, now)
				for _, index := range expectedRecipients(c.subscribers, stream, topic) {
					c.recordDelivery(c.subscribers[index], stream, sequence, topic, bp02Streams[stream].qos, false, now.Add(time.Millisecond))
				}
			}
			summary := c.summary().Offline
			if summary.Missing != 0 || summary.Duplicates != 0 || summary.Unexpected != 0 || !c.offlineComplete() {
				t.Fatalf("offline replay did not complete for the actual published sequence: %#v", summary)
			}
			wantID := fmt.Sprintf("bp02-a-sub-command-%02d", tc.commandIndex)
			for _, subscriber := range summary.Subscribers {
				if subscriber.ClientID == wantID && (subscriber.Expected != 1 || subscriber.Delivered != 1) {
					t.Errorf("command replay = %#v, want one expected and delivered message", subscriber)
				}
			}
		})
	}
}

// TestRetainedOracleSupportsProfileTopicCounts verifies the retained oracle follows profile topic counts.
// Scenarios: performance-and-verification.md:PRF-SCN-045
func TestRetainedOracleSupportsProfileTopicCounts(t *testing.T) {
	p := bp02SmokeProfile(t)
	p.RetainedTopicsTenantA = 3
	p.RetainedTopicsTenantB = 2
	c := newCollector(p, "narumq")
	for id, state := range c.retained {
		count := state.spec.streamCount(p)
		for index := range count {
			sequence := uint64(count + index + 1)
			topic := retainedTopic(p, state.spec.stream, sequence)
			c.recordDelivery(state.spec, state.spec.stream, sequence, topic, 1, true, time.Now())
		}
		if !c.retainedComplete(id) {
			t.Errorf("%s did not complete latest-round replay", id)
		}
	}
	summary := c.summary().Retained
	if summary.Missing != 0 || summary.Duplicates != 0 || summary.Unexpected != 0 || summary.Stale != 0 || summary.WrongFlags != 0 {
		t.Fatalf("retained summary = %#v", summary)
	}
}

// TestLateMeasureDeliveryPreservesDrainMissingSnapshot verifies the drain-time missing snapshot is preserved.
// Scenarios: performance-and-verification.md:PRF-SCN-045
func TestLateMeasureDeliveryPreservesDrainMissingSnapshot(t *testing.T) {
	p := bp02SmokeProfile(t)
	c := newCollector(p, "narumq")
	topic := publishTopic(0, 1)
	sent := time.Now()
	if err := c.addMessage(0, 1, 1, "step-01-measure", topic, sent); err != nil {
		t.Fatal(err)
	}
	c.markSent(0, 1, sent)
	recipients := expectedRecipients(c.subscribers, 0, topic)
	for _, index := range recipients[:len(recipients)-1] {
		c.recordDelivery(c.subscribers[index], 0, 1, topic, 0, false, sent.Add(time.Millisecond))
	}
	c.finishStep(1, sent.Add(time.Second))
	c.recordDelivery(c.subscribers[recipients[0]], 0, 1, topic, 0, false, sent.Add(2*time.Second))
	c.recordDelivery(c.subscribers[recipients[len(recipients)-1]], 0, 1, topic, 0, false, sent.Add(3*time.Second))
	got := c.summary().Steps[0].Streams["a_telemetry"]
	if got.Missing != 1 || got.Duplicates != 1 {
		t.Fatalf("late delivery counters missing=%d duplicates=%d, want 1/1", got.Missing, got.Duplicates)
	}
}

// TestInterruptedStepReportsObservedMissingDeliveries verifies an interrupted step reports observed missing deliveries.
// Scenarios: performance-and-verification.md:PRF-SCN-045
func TestInterruptedStepReportsObservedMissingDeliveries(t *testing.T) {
	profile := bp02SmokeProfile(t)
	col := newCollector(profile, "narumq")
	topic := publishTopic(0, 1)
	sent := time.Now()
	if err := col.addMessage(0, 1, 1, "step-01-measure", topic, sent); err != nil {
		t.Fatal(err)
	}
	col.markSent(0, 1, sent)
	recipients := expectedRecipients(col.subscribers, 0, topic)
	for _, index := range recipients[:2] {
		col.recordDelivery(col.subscribers[index], 0, 1, topic, 0, false, sent.Add(time.Millisecond))
	}
	col.executionFailure("interrupted before drain")
	observed := col.summary().Steps[0].Streams["a_telemetry"]
	if observed.ExpectedDeliveries != 6 || observed.Delivered != 2 || observed.Missing != 4 {
		t.Fatalf("interrupted counters = expected %d delivered %d missing %d", observed.ExpectedDeliveries, observed.Delivered, observed.Missing)
	}
}
