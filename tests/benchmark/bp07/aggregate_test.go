package main

import (
	"testing"
	"time"
)

func collectorTestProfile() Profile {
	return Profile{
		ReplacementOrder: []int{3, 1, 2},
		Cycles:           1,
		MaxMessages:      16,
	}
}

// TestCollectorAttributesDisconnectOnlyToMatchingDrainNode verifies only a disconnect from the draining node is attributed to the round.
// Scenarios: performance-and-verification.md:PRF-SCN-020
func TestCollectorAttributesDisconnectOnlyToMatchingDrainNode(t *testing.T) {
	collector := newCollector("narumq", collectorTestProfile())
	at := time.Now()
	marker := roundMarker{Round: 1, Node: "node3", State: "draining"}
	if round, planned := collector.recordDisconnect("bp07-pub-v5-q1", "node3", marker, "0x9C", bp07LBReference, at); !planned || round != 1 {
		t.Fatalf("matching draining node attribution = (%d, %v), want (1, true)", round, planned)
	}
	if _, planned := collector.recordDisconnect("bp07-pub-v5-q1", "node1", marker, "eof", "", at); planned {
		t.Fatal("disconnect from a different node was attributed to the drain round")
	}
	marker.State = "ready"
	if _, planned := collector.recordDisconnect("bp07-pub-v5-q1", "node3", marker, "eof", "", at); planned {
		t.Fatal("disconnect while the marker was ready was attributed to the drain round")
	}

	summary := collector.summary()
	if len(summary.Rounds) != 3 || summary.Rounds[0].Disconnects != 1 {
		t.Fatalf("round disconnects = %#v, want one disconnect in round 1", summary.Rounds)
	}
	if summary.Totals.UnplannedDisconnects != 2 || summary.Valid {
		t.Fatalf("unplanned disconnect total/valid = %d/%v, want 2/false", summary.Totals.UnplannedDisconnects, summary.Valid)
	}
}

// TestCollectorExcludesInterruptedPublisherExchangeFromGaps verifies an interrupted publisher exchange is not counted as a gap.
// Scenarios: performance-and-verification.md:PRF-SCN-020
func TestCollectorExcludesInterruptedPublisherExchangeFromGaps(t *testing.T) {
	collector := newCollector("emqx", collectorTestProfile())
	now := time.Now()
	record := messageRecord{Stream: "v5-q1", Protocol: "MQTT 5", QoS: 1, Sequence: 1, scheduled: now}
	if err := collector.beginPublish(record); err != nil {
		t.Fatal(err)
	}
	if !collector.publishSent(record.Stream, record.Sequence, now) {
		t.Fatal("publishSent rejected the prepared exchange")
	}
	if !collector.publishInterrupted(record.Stream, record.Sequence) {
		t.Fatal("publishInterrupted rejected the outstanding exchange")
	}

	summary := collector.summary()
	stream := findStreamSummary(t, summary, record.Stream)
	if stream.Interrupted != 1 || stream.Gaps != 0 || summary.Totals.Gaps != 0 {
		t.Fatalf("interrupted stream summary = %#v, want interrupted=1 and zero gaps", stream)
	}
	if !collector.settled() {
		t.Fatal("collector remained unsettled after its exchange was interrupted")
	}
}

// TestCollectorInvalidatesQoS2DuplicatesButRecordsQoS1Duplicates verifies QoS 2 duplicates invalidate and QoS 1 duplicates are recorded.
// Scenarios: performance-and-verification.md:PRF-SCN-020
func TestCollectorInvalidatesQoS2DuplicatesButRecordsQoS1Duplicates(t *testing.T) {
	collector := newCollector("emqx", collectorTestProfile())
	now := time.Now()
	for _, item := range []struct {
		stream string
		qos    int
	}{
		{stream: "v5-q1", qos: 1},
		{stream: "v5-q2", qos: 2},
	} {
		record := messageRecord{Stream: item.stream, Protocol: "MQTT 5", QoS: item.qos, Sequence: 1, scheduled: now}
		if err := collector.beginPublish(record); err != nil {
			t.Fatal(err)
		}
		if !collector.publishSent(item.stream, 1, now) {
			t.Fatalf("publishSent rejected %s", item.stream)
		}
		if item.qos == 2 && !collector.publishAck(item.stream, 1, now.Add(time.Millisecond), 0, false) {
			t.Fatalf("PUBREC rejected %s", item.stream)
		}
		if !collector.publishAck(item.stream, 1, now.Add(2*time.Millisecond), 0, true) {
			t.Fatalf("completed acknowledgement rejected %s", item.stream)
		}
		if duplicate, err := collector.receive(item.stream, 1, now.Add(2*time.Millisecond)); err != nil || duplicate {
			t.Fatalf("first %s delivery = (%v, %v), want (false, nil)", item.stream, duplicate, err)
		}
		if duplicate, err := collector.receive(item.stream, 1, now.Add(3*time.Millisecond)); err != nil || !duplicate {
			t.Fatalf("second %s delivery = (%v, %v), want (true, nil)", item.stream, duplicate, err)
		}
	}

	summary := collector.summary()
	if summary.Totals.QoS1Duplicates != 1 || summary.Totals.QoS2Duplicates != 1 {
		t.Fatalf("duplicate totals = QoS1:%d QoS2:%d, want 1 each", summary.Totals.QoS1Duplicates, summary.Totals.QoS2Duplicates)
	}
	if summary.Valid {
		t.Fatal("QoS 2 duplicate did not invalidate workload")
	}
	if stream := findStreamSummary(t, summary, "v5-q1"); stream.Duplicates != 1 {
		t.Fatalf("QoS 1 duplicate count = %d, want 1", stream.Duplicates)
	}
}

// TestCollectorValidatesNaruMQWillAndRecordsEMQXWillOnly verifies NaruMQ Will validation and EMQX Will recorded-only handling.
// Scenarios: performance-and-verification.md:PRF-SCN-020
func TestCollectorValidatesNaruMQWillAndRecordsEMQXWillOnly(t *testing.T) {
	marker := roundMarker{Round: 1, Node: "node3", State: "restarting"}
	at := time.Now()

	naru := newCollector("narumq", collectorTestProfile())
	naru.recordDisconnect("bp07-will-v311", "node3", marker, "eof", "", at)
	payload := "bp07-will-v311#1"
	naru.expectWill("bp07-will-v311", payload)
	if err := naru.recordWill("bp07/will/v311", payload, at.Add(time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	naruSummary := naru.summary()
	if !naruSummary.Valid || naruSummary.Wills.Applicability != "validated" ||
		naruSummary.Wills.V311ServerCloses != 1 || naruSummary.Wills.V311UniqueReceived != 1 {
		t.Fatalf("NaruMQ Will summary = %#v, want a valid matching Will", naruSummary.Wills)
	}

	emqx := newCollector("emqx", collectorTestProfile())
	emqx.recordDisconnect("bp07-will-v311", "node3", marker, "eof", "", at)
	emqxSummary := emqx.summary()
	if !emqxSummary.Valid || emqxSummary.Wills.Applicability != "recorded_only" ||
		emqxSummary.Wills.V311ServerCloses != 1 || emqxSummary.Wills.V311UniqueReceived != 0 {
		t.Fatalf("EMQX Will summary = %#v, want recorded_only without equality validation", emqxSummary.Wills)
	}
}

func findStreamSummary(t *testing.T, summary workloadSummary, name string) streamSummary {
	t.Helper()
	for _, stream := range summary.Streams {
		if stream.Stream == name {
			return stream
		}
	}
	t.Fatalf("stream %q is absent from summary", name)
	return streamSummary{}
}
