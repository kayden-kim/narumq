package main

import (
	"context"
	"testing"
	"time"

	"narumq/internal/domain/mqtt"
)

func collectorProfile() Profile {
	profile := validBP03Profile()
	profile.RatesPerStreamPerSecond = []uint64{1}
	profile.PairsPerQoS = 1
	profile.MeasureMillis = 1000
	profile.OfflineMessagesPerStream = 1
	profile.MaxMessages = 64
	return profile
}

func addTestRecord(t *testing.T, c *collector, qos, step int, sequence uint64, interval time.Duration) {
	t.Helper()
	now := time.Now()
	record := messageRecord{Stream: "q", QoS: qos, Sequence: sequence, Step: step, Phase: "step-01-measure", scheduled: now, interval: int64(interval)}
	if err := c.add(record); err != nil {
		t.Fatal(err)
	}
	c.markSent("q", sequence, now)
}
func addValidOfflineRecords(t *testing.T, collector *collector) {
	t.Helper()
	for _, qos := range []int{1, 2} {
		stream := "offline-q" + string(rune('0'+qos))
		now := time.Now()
		record := messageRecord{Stream: stream, QoS: qos, Sequence: 2, Step: 0, Phase: "offline", scheduled: now, interval: int64(time.Second)}
		if err := collector.add(record); err != nil {
			t.Fatal(err)
		}
		collector.markSent(stream, 2, now)
		collector.acknowledge(stream, 2, now.Add(time.Millisecond), 0, qos == 2)
		collector.receive(stream, 2, now.Add(time.Millisecond))
	}
	for index := 0; index < 2; index++ {
		collector.recordOfflineSession(true)
	}
}
func TestMissingOfflineSessionIsCounted(t *testing.T) {
	collector := newCollector(4)
	collector.recordOfflineSession(false)

	summary := collector.summarize(collectorProfile(), "narumq", nil)
	if summary.SessionNotPresent != 1 {
		t.Fatalf("sessionNotPresent = %d, want 1", summary.SessionNotPresent)
	}
	found := false
	for _, reason := range summary.FailureReasons {
		if reason == "one or more persistent sessions were not present after reconnect" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("missing-session failure reason not reported: %v", summary.FailureReasons)
	}
}

// TestAckedUnreceivedMessageIsGapAndInvalid verifies an ACKed but never received message is a gap and invalidates the summary.
// Scenarios: performance-and-verification.md:PRF-SCN-004
func TestAckedUnreceivedMessageIsGapAndInvalid(t *testing.T) {
	collector := newCollector(4)
	addTestRecord(t, collector, 1, 1, 1, time.Second)
	collector.acknowledge("q", 1, time.Now(), 0, false)
	summary := collector.summarize(collectorProfile(), "narumq", nil)
	if got := summary.Steps[0].QoS["1"].Gap; got != 1 {
		t.Fatalf("gap = %d, want 1", got)
	}
	if summary.Valid {
		t.Fatal("summary valid with an acked message never received")
	}
}
func TestMeasureDeliveryAfterDrainDoesNotCount(t *testing.T) {
	collector := newCollector(4)
	addTestRecord(t, collector, 1, 1, 1, time.Second)
	now := time.Now()
	collector.acknowledge("q", 1, now, 0, false)
	collector.markDrainFinished(now)
	if collector.receive("q", 1, now.Add(time.Nanosecond)) {
		t.Fatal("late measure receipt was reported as a duplicate")
	}
	summary := collector.summarize(collectorProfile(), "narumq", nil)
	stats := summary.Steps[0].QoS["1"]
	if stats.Delivered != 0 || stats.Gap != 1 {
		t.Fatalf("late receipt stats = delivered %d, gap %d; want 0 and 1", stats.Delivered, stats.Gap)
	}
}

// TestQoS2DuplicateInvalid verifies a QoS 2 duplicate invalidates the summary.
// Scenarios: performance-and-verification.md:PRF-SCN-004
func TestQoS2DuplicateInvalid(t *testing.T) {
	collector := newCollector(4)
	addTestRecord(t, collector, 2, 1, 1, time.Second)
	now := time.Now()
	collector.acknowledge("q", 1, now, 0, false)
	collector.acknowledge("q", 1, now.Add(time.Millisecond), 0, true)
	collector.receive("q", 1, now)
	collector.receive("q", 1, now.Add(time.Millisecond))
	addValidOfflineRecords(t, collector)
	summary := collector.summarize(collectorProfile(), "narumq", nil)
	if got := summary.Steps[0].QoS["2"].Duplicate; got != 1 {
		t.Fatalf("duplicate = %d, want 1", got)
	}
	if summary.Valid {
		t.Fatal("summary valid with a QoS 2 duplicate")
	}
}

// TestQoS1DuplicateReportedButAllowed verifies a QoS 1 duplicate is reported but allowed.
// Scenarios: performance-and-verification.md:PRF-SCN-004
func TestQoS1DuplicateReportedButAllowed(t *testing.T) {
	collector := newCollector(4)
	addTestRecord(t, collector, 1, 1, 1, time.Second)
	now := time.Now()
	collector.acknowledge("q", 1, now, 0, false)
	collector.receive("q", 1, now)
	addTestRecord(t, collector, 2, 1, 3, time.Second)
	qos2At := time.Now()
	collector.acknowledge("q", 3, qos2At, 0, true)
	collector.receive("q", 3, qos2At)
	collector.receive("q", 1, now.Add(time.Millisecond))
	addValidOfflineRecords(t, collector)
	summary := collector.summarize(collectorProfile(), "narumq", nil)
	if got := summary.Steps[0].QoS["1"].Duplicate; got != 1 {
		t.Fatalf("duplicate = %d, want 1", got)
	}
	if !summary.Valid {
		t.Fatalf("QoS 1 duplicate should be allowed: %v", summary.FailureReasons)
	}
}

// TestScheduleLagEqualToIntervalIsOverload verifies schedule lag equal to the interval counts as overload.
// Scenarios: performance-and-verification.md:PRF-SCN-004
func TestScheduleLagEqualToIntervalIsOverload(t *testing.T) {
	collector := newCollector(4)
	scheduled := time.Now()
	record := messageRecord{Stream: "q", QoS: 1, Sequence: 1, Step: 1, Phase: "step-01-measure", scheduled: scheduled, interval: int64(time.Millisecond)}
	if err := collector.add(record); err != nil {
		t.Fatal(err)
	}
	collector.markSent("q", 1, scheduled.Add(time.Millisecond))
	if !collector.overload[1] {
		t.Fatal("schedule lag equal to interval did not count as overload")
	}
}

func TestDrainWaitsForPendingAckAndDelivery(t *testing.T) {
	collector := newCollector(4)
	addTestRecord(t, collector, 1, 1, 1, time.Second)
	if collector.measureDelivered() {
		t.Fatal("drain completed while PUBACK was pending")
	}
	now := time.Now()
	collector.acknowledge("q", 1, now, 0, false)
	if collector.measureDelivered() {
		t.Fatal("drain completed before the acknowledged message was received")
	}
	collector.receive("q", 1, now.Add(time.Millisecond))
	if !collector.measureDelivered() {
		t.Fatal("drain did not complete after ACK and delivery")
	}
}

func TestQoS2PendingPacketRetransmitDoesNotRecordDuplicate(t *testing.T) {
	profile := collectorProfile()
	collector := newCollector(4)
	sequence := uint64(1)
	addTestRecord(t, collector, 2, 1, sequence, time.Second)
	stream := streamSpec{index: 0, qos: 2, name: "q", topic: "bp03/durable/q2/01"}
	packet := &mqtt.Publish{Version: mqtt.ProtocolV5, QoS: mqtt.QoSExactlyOnce, PacketID: 7, Topic: stream.topic, Payload: makePayload(profile.Seed, sequence, stream.index, profile.PayloadBytes)}
	pending := make(map[mqtt.PacketIdentifier]bool)
	firstResponse, err := processSubscriberPublish(stream, profile, packet, collector, pending, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := firstResponse.(*mqtt.PubRec); !ok {
		t.Fatalf("first response = %T, want PUBREC", firstResponse)
	}
	secondResponse, err := processSubscriberPublish(stream, profile, packet, collector, pending, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if ack, ok := secondResponse.(*mqtt.PubRec); !ok || ack.PacketID != packet.PacketID {
		t.Fatalf("retransmit response = %#v, want PUBREC for packet id %d", secondResponse, packet.PacketID)
	}
	if got := collector.records[messageKey{"q", sequence}].ReceiveCount; got != 1 {
		t.Fatalf("receive count = %d, want 1", got)
	}
}

func TestStepStatsExcludeWarmupAndNonMeasurePhases(t *testing.T) {
	collector := newCollector(8)
	phases := []string{"step-01-warmup", "step-01-measure-shadow", "step-01-measure"}
	for index, phase := range phases {
		scheduled := time.Now()
		sequence := uint64(index + 1)
		if err := collector.add(messageRecord{
			Stream: "q", QoS: 1, Sequence: sequence, Step: 1, Phase: phase,
			scheduled: scheduled, interval: int64(time.Millisecond),
		}); err != nil {
			t.Fatal(err)
		}
		sent := scheduled
		if phase == "step-01-measure-shadow" {
			sent = sent.Add(time.Second)
		}
		collector.markSent("q", sequence, sent)
		collector.acknowledge("q", sequence, sent, 0, false)
		collector.receive("q", sequence, sent)
	}
	stats := collector.stats(1, 1, false, 1, 1)
	if stats.Sent != 1 || stats.Acked != 1 || stats.Delivered != 1 {
		t.Fatalf("measure stats = sent %d, acked %d, delivered %d; want 1 each", stats.Sent, stats.Acked, stats.Delivered)
	}
	if collector.overload[1] {
		t.Fatal("non-measure schedule lag marked the measured step overloaded")
	}
}

// TestQoS2PUBCOMPTimeoutCountsIncompleteExchange verifies a PUBCOMP timeout counts as an incomplete exchange.
// Scenarios: performance-and-verification.md:PRF-SCN-004
func TestQoS2PUBCOMPTimeoutCountsIncompleteExchange(t *testing.T) {
	collector := newCollector(4)
	addTestRecord(t, collector, 2, 1, 1, time.Second)
	now := time.Now()
	if !collector.acknowledge("q", 1, now, 0, false) {
		t.Fatal("PUBREC was not recorded")
	}
	if !collector.timeout("q", 1) {
		t.Fatal("incomplete QoS 2 exchange did not time out")
	}
	if collector.timeout("q", 1) {
		t.Fatal("timed-out QoS 2 exchange timed out twice")
	}
	addValidOfflineRecords(t, collector)
	stats := collector.summarize(collectorProfile(), "narumq", nil).Steps[0].QoS["2"]
	if stats.Acked != 1 || stats.AckTimeouts != 1 || stats.completeCount != 0 {
		t.Fatalf("QoS 2 stats after PUBCOMP timeout = %#v", stats)
	}
}

// TestOfflineCompletionWaitsForQoS2PUBCOMP verifies offline completion waits for QoS 2 PUBCOMP.
// Scenarios: performance-and-verification.md:PRF-SCN-004
func TestOfflineCompletionWaitsForQoS2PUBCOMP(t *testing.T) {
	collector := newCollector(4)
	streams := []streamSpec{{name: "q1", qos: 1}, {name: "q2", qos: 2}}
	now := time.Now()
	for _, stream := range streams {
		record := messageRecord{Stream: stream.name, QoS: stream.qos, Sequence: 1, Step: 0, Phase: "offline", scheduled: now}
		if err := collector.add(record); err != nil {
			t.Fatal(err)
		}
		collector.markSent(stream.name, 1, now)
		if !collector.acknowledge(stream.name, 1, now.Add(time.Millisecond), 0, false) {
			t.Fatalf("PUBACK/PUBREC for %s was not recorded", stream.name)
		}
		collector.receive(stream.name, 1, now.Add(time.Millisecond))
	}
	if collector.offlineExchangesComplete(streams, 1) {
		t.Fatal("offline completion passed before QoS 2 PUBCOMP")
	}
	if !collector.acknowledge("q2", 1, now.Add(2*time.Millisecond), 0, true) {
		t.Fatal("PUBCOMP was not recorded")
	}
	if !collector.offlineExchangesComplete(streams, 1) {
		t.Fatal("offline completion did not pass after QoS 2 PUBCOMP")
	}
}

// TestOfflineCompletionRequiresEveryExpectedReceipt verifies offline completion requires every expected receipt.
// Scenarios: performance-and-verification.md:PRF-SCN-004
func TestOfflineCompletionRequiresEveryExpectedReceipt(t *testing.T) {
	collector := newCollector(2)
	streams := []streamSpec{{name: "q1", qos: 1}}
	now := time.Now()
	record := messageRecord{Stream: "q1", QoS: 1, Sequence: 1, Step: 0, Phase: "offline", scheduled: now}
	if err := collector.add(record); err != nil {
		t.Fatal(err)
	}
	collector.markSent("q1", 1, now)
	if !collector.acknowledge("q1", 1, now.Add(time.Millisecond), 0, false) {
		t.Fatal("offline PUBACK was not recorded")
	}
	if collector.offlineExchangesComplete(streams, 1) {
		t.Fatal("offline completion passed before the subscriber receipt")
	}
	collector.receive("q1", 1, now.Add(2*time.Millisecond))
	if !collector.offlineExchangesComplete(streams, 1) {
		t.Fatal("offline completion did not pass after receipt")
	}
	if collector.offlineExchangesComplete(streams, 2) {
		t.Fatal("offline completion passed without the expected second message")
	}
}

// TestOfflineCompletionFailsWhenReceiptMissesDeadline verifies a missed receipt deadline fails offline completion.
// Scenarios: performance-and-verification.md:PRF-SCN-004
func TestOfflineCompletionFailsWhenReceiptMissesDeadline(t *testing.T) {
	collector := newCollector(1)
	streams := []streamSpec{{name: "q1", qos: 1}}
	now := time.Now()
	if err := collector.add(messageRecord{Stream: "q1", QoS: 1, Sequence: 1, Step: 0, Phase: "offline", scheduled: now}); err != nil {
		t.Fatal(err)
	}
	collector.markSent("q1", 1, now)
	collector.acknowledge("q1", 1, now.Add(time.Millisecond), 0, false)
	err := waitForOfflineCompletion(context.Background(), collector, streams, 1, now)
	if err == nil {
		t.Fatal("offline completion succeeded despite a missing receipt at the deadline")
	}
	collector.receive("q1", 1, now.Add(2*time.Millisecond))
	if err := waitForOfflineCompletion(context.Background(), collector, streams, 1, now); err == nil {
		t.Fatal("offline completion succeeded after its deadline")
	}
}

// TestMessageSnapshotDoesNotObserveLaterAcknowledgements verifies a message snapshot is isolated from later acknowledgements.
// Scenarios: performance-and-verification.md:PRF-SCN-004
func TestMessageSnapshotDoesNotObserveLaterAcknowledgements(t *testing.T) {
	collector := newCollector(1)
	now := time.Now()
	if err := collector.add(messageRecord{
		Stream: "q", QoS: 1, Sequence: 1, Step: 1, Phase: "step-01-measure", scheduled: now,
	}); err != nil {
		t.Fatal(err)
	}
	collector.markSent("q", 1, now)
	snapshot := collector.messageSnapshot()
	if len(snapshot) != 1 {
		t.Fatalf("message snapshot length = %d, want 1", len(snapshot))
	}
	if !collector.acknowledge("q", 1, now.Add(time.Millisecond), 0, false) {
		t.Fatal("PUBACK was not recorded")
	}
	if snapshot[0].AckAt != "" {
		t.Fatalf("snapshot ACK timestamp changed after collector update: %q", snapshot[0].AckAt)
	}
}
