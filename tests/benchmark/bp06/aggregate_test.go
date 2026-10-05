package main

import (
	"testing"
	"time"
)

func collectorTestProfile() Profile {
	return Profile{Seed: 7, PayloadBytes: 32, MaxMessages: 4096, RecoveryTimeoutMillis: 30000, KillOrder: []int{3, 1, 2}, Cycles: 1}
}

func TestRecoveryRTOUsesEarliestInjectionAndLatestRequiredSuccess(t *testing.T) {
	base := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	profile := collectorTestProfile()
	round := roundSummary{Round: 1, Node: "node3"}
	disconnects := []plannedDisconnect{
		{ClientID: "pub-a", At: base.Add(2 * time.Second), ReconnectedAt: base.Add(4 * time.Second)},
		{ClientID: "pub-b", At: base.Add(time.Second), ReconnectedAt: base.Add(3 * time.Second)},
	}
	records := []*messageRecord{
		{Stream: "v5-q0", QoS: 0, sent: base.Add(1500 * time.Millisecond), receivedAt: base.Add(5 * time.Second)},
		{Stream: "v5-q1", QoS: 1, sent: base.Add(1500 * time.Millisecond), ackedAt: base.Add(6 * time.Second)},
		{Stream: "v5-q2", QoS: 2, sent: base.Add(1500 * time.Millisecond), completedAt: base.Add(7 * time.Second)},
		{Stream: "v311-q1", QoS: 1, sent: base.Add(1500 * time.Millisecond), ackedAt: base.Add(8 * time.Second)},
	}
	got, reasons := computeRecovery(round, disconnects, time.Time{}, records, profile)
	if len(reasons) != 0 || !got.Recovered {
		t.Fatalf("recovery = %#v, reasons %v", got, reasons)
	}
	if got.InjectionSource != "disconnect" || got.InjectionAt != base.Add(time.Second).Format(time.RFC3339Nano) || got.RTOMillis != 7000 {
		t.Fatalf("recovery injection/RTO = %#v, want earliest disconnect and 7000ms", got)
	}
	if got.StreamFirstSuccessMillis["v5-q0"] != 4000 || got.StreamFirstSuccessMillis["v5-q1"] != 5000 || got.StreamFirstSuccessMillis["v5-q2"] != 6000 || got.StreamFirstSuccessMillis["v311-q1"] != 7000 {
		t.Fatalf("stream first-success offsets = %#v", got.StreamFirstSuccessMillis)
	}
	if got.ReconnectMaxMillis != 2000 {
		t.Fatalf("maximum client reconnect latency = %v, want 2000ms", got.ReconnectMaxMillis)
	}
}

func TestRecoveryRTOUsesDownMarkerFallbackAndRequiresPostInjectionSuccess(t *testing.T) {
	base := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	profile := collectorTestProfile()
	down := base.Add(2 * time.Second)
	old := &messageRecord{Stream: "v5-q1", QoS: 1, sent: base, ackedAt: base.Add(time.Second)}
	got, reasons := computeRecovery(roundSummary{Round: 1}, nil, down, []*messageRecord{old}, profile)
	if got.InjectionSource != "marker" || got.InjectionAt != down.Format(time.RFC3339Nano) || got.Recovered || len(reasons) == 0 {
		t.Fatalf("fallback/missing stream recovery = %#v reasons=%v", got, reasons)
	}
}
func TestRecoveryRTOIgnoresAcknowledgementForMessageSentBeforeInjection(t *testing.T) {
	base := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	got, reasons := computeRecovery(
		roundSummary{Round: 1},
		nil,
		base.Add(time.Second),
		[]*messageRecord{{Stream: "v5-q1", QoS: 1, sent: base, ackedAt: base.Add(2 * time.Second)}},
		collectorTestProfile(),
	)
	if _, counted := got.StreamFirstSuccessMillis["v5-q1"]; counted || got.Recovered || len(reasons) == 0 {
		t.Fatalf("pre-injection sent ACK counted as recovery: %#v reasons=%v", got, reasons)
	}
}

func TestRecoveryRTOTimeoutInvalidatesRound(t *testing.T) {
	base := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	profile := collectorTestProfile()
	profile.RecoveryTimeoutMillis = 1000
	records := make([]*messageRecord, 0, 4)
	for _, stream := range []struct {
		name string
		qos  int
	}{{"v5-q0", 0}, {"v5-q1", 1}, {"v5-q2", 2}, {"v311-q1", 1}} {
		record := &messageRecord{Stream: stream.name, QoS: stream.qos, sent: base.Add(time.Second)}
		if stream.qos == 2 {
			record.completedAt = base.Add(3 * time.Second)
		} else if stream.qos == 1 {
			record.ackedAt = base.Add(3 * time.Second)
		} else {
			record.receivedAt = base.Add(3 * time.Second)
		}
		records = append(records, record)
	}
	got, reasons := computeRecovery(roundSummary{Round: 1}, nil, base, records, profile)
	if !got.Recovered || got.RTOMillis != 3000 || len(reasons) == 0 {
		t.Fatalf("timeout recovery = %#v reasons=%v", got, reasons)
	}
}

func TestCollectorExcludesInterruptedPublisherExchangeFromGaps(t *testing.T) {
	col := newCollector("emqx", collectorTestProfile())
	now := time.Now()
	record := messageRecord{Stream: "v5-q1", Protocol: "MQTT 5", QoS: 1, Sequence: 1, scheduled: now}
	if err := col.beginPublish(record); err != nil {
		t.Fatal(err)
	}
	if !col.publishSent(record.Stream, record.Sequence, now) || !col.publishInterrupted(record.Stream, record.Sequence) {
		t.Fatal("outstanding exchange did not transition to interrupted")
	}
	summary := col.summary()
	stream := findStreamSummary(t, summary, record.Stream)
	if stream.Interrupted != 1 || stream.Gaps != 0 || summary.Totals.Gaps != 0 || !col.settled() {
		t.Fatalf("interrupted exchange retained a gap or pending work: %#v", stream)
	}
}

func TestCollectorRecordsQoS1DuplicatesAndRejectsQoS2Duplicates(t *testing.T) {
	col := newCollector("emqx", collectorTestProfile())
	now := time.Now()
	for _, item := range []struct {
		stream string
		qos    int
	}{{"v5-q1", 1}, {"v5-q2", 2}} {
		record := messageRecord{Stream: item.stream, Protocol: "MQTT 5", QoS: item.qos, Sequence: 1, scheduled: now}
		if err := col.beginPublish(record); err != nil {
			t.Fatal(err)
		}
		if !col.publishSent(item.stream, 1, now) {
			t.Fatal("prepared PUBLISH was rejected")
		}
		if item.qos == 2 && !col.publishAck(item.stream, 1, now.Add(time.Millisecond), 0, false) {
			t.Fatal("PUBREC was rejected")
		}
		if !col.publishAck(item.stream, 1, now.Add(2*time.Millisecond), 0, true) {
			t.Fatal("completed ACK was rejected")
		}
		if duplicate, err := col.receive(item.stream, 1, now.Add(2*time.Millisecond)); err != nil || duplicate {
			t.Fatalf("first delivery = (%v, %v)", duplicate, err)
		}
		if duplicate, err := col.receive(item.stream, 1, now.Add(3*time.Millisecond)); err != nil || !duplicate {
			t.Fatalf("repeat delivery = (%v, %v)", duplicate, err)
		}
	}
	summary := col.summary()
	if summary.Totals.QoS1Duplicates != 1 || summary.Totals.QoS2Duplicates != 1 {
		t.Fatalf("duplicate totals = %#v", summary.Totals)
	}
	if _, rejected := col.failureSet["QoS 2 duplicate for v5-q2 sequence 1"]; !rejected {
		t.Fatal("QoS 2 duplicate was not a validity failure")
	}
	if _, rejected := col.failureSet["QoS 1 duplicate for v5-q1 sequence 1"]; rejected {
		t.Fatal("QoS 1 duplicate became a validity failure")
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

func TestRecoveryMissingReconnectAndInjectionInvalidateWorkload(t *testing.T) {
	profile := collectorTestProfile()
	col := newCollector("narumq", profile)
	at := time.Now()
	col.recordDisconnect("bp06-idle-01", "node3", roundMarker{Round: 1, Node: "node3", State: "killing"}, "eof", "", at)
	summary := col.summary()
	if summary.Valid || summary.Rounds[0].Recovery.Recovered || summary.Rounds[1].Recovery.InjectionSource != "" {
		t.Fatalf("missing reconnect/injection allowed recovery: %#v", summary.Rounds)
	}
}
