package main

import (
	"context"
	"net"
	"testing"
	"time"
)

func TestMQTTProbeClassifiesBrokerResponses(t *testing.T) {
	tests := []struct {
		name            string
		response        string
		wantClass       string
		wantConnackCode byte
		wantSubackCode  byte
	}{
		{name: "accepted", response: "accepted", wantClass: "accepted"},
		{name: "CONNACK rejected", response: "connack-rejected", wantClass: "rejected", wantConnackCode: 5},
		{name: "SUBACK rejected", response: "suback-rejected", wantClass: "rejected", wantSubackCode: 0x80},
		{name: "closed after CONNECT", response: "closed-after-connect", wantClass: "closed", wantConnackCode: 0},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := runMQTTProbeExchange(t, test.response)
			if result.Classification != test.wantClass {
				t.Fatalf("classification = %q, want %q; error=%q", result.Classification, test.wantClass, result.Error)
			}
			if test.wantConnackCode != 0 && (result.ConnackCode == nil || *result.ConnackCode != test.wantConnackCode) {
				t.Fatalf("CONNACK code = %v, want %d", result.ConnackCode, test.wantConnackCode)
			}
			if test.wantSubackCode != 0 && (result.SubackCode == nil || *result.SubackCode != test.wantSubackCode) {
				t.Fatalf("SUBACK code = %v, want %d", result.SubackCode, test.wantSubackCode)
			}
		})
	}
}

func runMQTTProbeExchange(t *testing.T, response string) mqttProbeResult {
	t.Helper()
	client, broker := net.Pipe()
	brokerResult := make(chan error, 1)
	go func() {
		defer broker.Close()
		_, _, err := readMQTTPacket(broker, context.Background())
		if err != nil {
			brokerResult <- err
			return
		}
		connackCode := byte(0)
		if response == "connack-rejected" {
			connackCode = 5
		}
		if _, err := broker.Write([]byte{0x20, 2, 0, connackCode}); err != nil {
			brokerResult <- err
			return
		}
		if response == "connack-rejected" || response == "closed-after-connect" {
			brokerResult <- nil
			return
		}
		_, _, err = readMQTTPacket(broker, context.Background())
		if err != nil {
			brokerResult <- err
			return
		}
		subackCode := byte(0)
		if response == "suback-rejected" {
			subackCode = 0x80
		}
		_, err = broker.Write([]byte{0x90, 3, 0, 1, subackCode})
		brokerResult <- err
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result := mqttProbeConnection(ctx, client, "test-token", "chaos-probe-1", "chaos/probe/#")
	_ = client.Close()
	select {
	case err := <-brokerResult:
		if response != "closed-after-connect" && err != nil {
			t.Fatalf("fake broker exchange failed: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("fake broker did not finish the exchange")
	}
	return result
}

func TestQueueSamplesEnforceTransportBounds(t *testing.T) {
	tests := []struct {
		name           string
		sample         queueSample
		wantErr        bool
		wantCapReached bool
	}{
		{
			name: "exact configured boundary", sample: queueSample{OutboundMessages: 32, OutboundBytes: 4_194_304},
			wantCapReached: true,
		},
		{
			name: "message count exceeded", sample: queueSample{OutboundMessages: 33, OutboundBytes: 4_194_304},
			wantErr: true,
		},
		{
			name: "byte count exceeded", sample: queueSample{OutboundMessages: 31, OutboundBytes: 4_194_305},
			wantErr: true,
		},
		{
			name: "transport rejected a message", sample: queueSample{OutboundMessages: 4, OutboundBytes: 1024, RejectedMessages: 1},
			wantCapReached: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateQueueSample(test.sample)
			if test.wantErr && err == nil {
				t.Fatal("validateQueueSample() returned nil for an out-of-bounds sample")
			}
			if !test.wantErr && err != nil {
				t.Fatalf("validateQueueSample() = %v, want nil", err)
			}
			if got := queueCapReached(test.sample); got != test.wantCapReached {
				t.Fatalf("queueCapReached() = %t, want %t", got, test.wantCapReached)
			}
		})
	}
}
