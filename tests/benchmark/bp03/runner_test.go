package main

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"narumq/internal/domain/mqtt"
	"narumq/tests/benchmark/internal/mqttclient"
)

func TestParseEndpointsRequiresThreeDistinctNumericHostPorts(t *testing.T) {
	got, err := parseEndpoints("node1:1883,node2:1884,node3:1885")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"node1:1883", "node2:1884", "node3:1885"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("parseEndpoints() = %v, want %v", got, want)
	}

	for _, raw := range []string{
		"node1:1883,node2:1884",
		":1883,node2:1884,node3:1885",
		"node1:1883,node2:service,node3:1885",
		"node1:0,node2:1884,node3:1885",
		"node1:1883,node1:1883,node3:1885",
	} {
		t.Run(raw, func(t *testing.T) {
			if _, err := parseEndpoints(raw); err == nil {
				t.Fatalf("parseEndpoints(%q) accepted invalid node endpoints", raw)
			}
		})
	}
}

func TestMakeStreamsAssignsCrossNodeRoles(t *testing.T) {
	profile := validBP03Profile()
	profile.PairsPerQoS = 2
	streams := makeStreams(profile, []string{"node1:1883", "node2:1883", "node3:1883"})
	if len(streams) != 4 {
		t.Fatalf("makeStreams() returned %d streams, want 4", len(streams))
	}
	first := streams[0]
	if first.name != "q1-01" || first.topic != "bp03/durable/q1/01" || first.publisher != "bp03-pub-q1-01" || first.subscriber != "bp03-sub-q1-01" || first.pubAddr != "node1:1883" || first.subAddr != "node2:1883" || first.reconnectAddr != "node3:1883" {
		t.Fatalf("first stream = %+v, want q1-01 with cross-node publisher/subscriber/reconnect mapping", first)
	}
	second, third := streams[1], streams[2]
	if second.name != "q1-02" || second.subscriber != "bp03-sub-q1-02" || second.subAddr != "node3:1883" ||
		third.name != "q2-01" || third.publisher != "bp03-pub-q2-01" || third.pubAddr != "node3:1883" {
		t.Fatalf("node 3 stream roles = second:%+v third:%+v, want q1-02 subscriber and q2-01 publisher", second, third)
	}
	if second.subscriber == third.publisher || second.subAddr != third.pubAddr {
		t.Fatalf("node 3 observations do not identify distinct client connections: q1-02=%+v q2-01=%+v", second, third)
	}
}

func TestWaitSlotReturnsReaderFailure(t *testing.T) {
	want := errors.New("publisher reader failed")
	client := &runClient{
		pending: map[mqtt.PacketIdentifier]pendingAck{
			1: {},
		},
		changed: make(chan struct{}, 1),
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := client.waitSlot(ctx, 1, new(uint32))
		result <- err
	}()
	client.failRead(want)

	client.ackMu.Lock()
	firstAt := client.readFailureAt
	firstFailure := client.readFailure
	client.ackMu.Unlock()
	if firstAt.IsZero() || !strings.Contains(firstFailure.Error(), firstAt.Format(time.RFC3339Nano)) {
		t.Fatalf("first reader error lacks its observation timestamp: at=%v error=%v", firstAt, firstFailure)
	}

	client.failRead(errors.New("later reader failure"))
	client.ackMu.Lock()
	firstFailureKept := client.readFailureAt.Equal(firstAt) && errors.Is(client.readFailure, want)
	client.ackMu.Unlock()
	if !firstFailureKept {
		t.Fatal("later reader failure replaced the first error or timestamp")
	}

	select {
	case err := <-result:
		if !errors.Is(err, want) {
			t.Fatalf("waitSlot() error = %v, want %v", err, want)
		}
	case <-time.After(time.Second):
		t.Fatal("waitSlot() did not wake after the reader failed")
	}
}

func TestCloseClientsReturnsDisconnectFailure(t *testing.T) {
	client := &runClient{
		client: &mqttclient.Client{},
		stream: streamSpec{name: "q1-01"},
		stop:   make(chan struct{}),
	}
	err := closeClients([]*runClient{client}, time.Second)
	if err == nil || !strings.Contains(err.Error(), "client q1-01") ||
		!strings.Contains(err.Error(), "MQTT client is closed") {
		t.Fatalf("closeClients() error = %v, want disconnect failure for q1-01", err)
	}
}

func TestPublisherACKTimeoutQuarantinesPacketIdentifier(t *testing.T) {
	now := time.Now()
	packetID := mqtt.PacketIdentifier(7)
	client := &runClient{
		stream:  streamSpec{name: "q1-01", qos: 1},
		pending: map[mqtt.PacketIdentifier]pendingAck{packetID: {sequence: 9, deadline: now.Add(-time.Millisecond)}},
		changed: make(chan struct{}, 1),
	}
	collector := newCollector(1)
	if err := collector.add(messageRecord{
		Stream: "q1-01", QoS: 1, Sequence: 9, Phase: "step-01-measure",
		scheduled: now, interval: int64(time.Second),
	}); err != nil {
		t.Fatal(err)
	}
	collector.markSent("q1-01", 9, now)

	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(context.Canceled)
	if !expirePublisherAcks(client, collector, now, cancel) {
		t.Fatal("expired acknowledgement did not cancel the run")
	}
	cause := context.Cause(ctx)
	if cause == nil || !strings.Contains(cause.Error(), "packet ID 7") {
		t.Fatalf("cancellation cause = %v, want packet ID timeout", cause)
	}

	client.ackMu.Lock()
	pending, quarantined := client.pending[packetID]
	client.ackMu.Unlock()
	if !quarantined || !pending.timedOut {
		t.Fatalf("timed-out packet ID state = %+v present=%t, want quarantined", pending, quarantined)
	}
	if _, err := client.waitSlot(ctx, 2, new(uint32)); !errors.Is(err, cause) {
		t.Fatalf("waitSlot() after ACK timeout = %v, want %v", err, cause)
	}
	if err := processPublisherAck(client, collector, &mqtt.PubAck{Version: mqtt.ProtocolV5, PacketID: packetID}, 100); err == nil ||
		!strings.Contains(err.Error(), "acknowledgement timeout") {
		t.Fatalf("late acknowledgement error = %v, want timeout without accepting the packet", err)
	}
	if expirePublisherAcks(client, collector, now.Add(time.Second), cancel) {
		t.Fatal("same expired acknowledgement was counted twice")
	}

	collector.mu.Lock()
	record := collector.records[messageKey{stream: "q1-01", sequence: 9}]
	timedOut := record.timedOut
	protocolErrors := collector.protocolErrors
	collector.mu.Unlock()
	if !timedOut {
		t.Fatal("timed-out acknowledgement was not recorded in workload summary state")
	}
	if protocolErrors != 0 {
		t.Fatalf("ACK timeout incremented protocolErrors to %d", protocolErrors)
	}
}
