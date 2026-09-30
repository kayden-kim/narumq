package main

import (
	"context"
	"fmt"
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

func TestDurableClientAwaitAckClassifiesOutcomes(t *testing.T) {
	tests := []struct {
		name       string
		server     func(net.Conn, context.Context) error
		timeout    time.Duration
		want       durableAckOutcome
		wantAckErr bool
	}{
		{
			name:    "matching PUBACK after unrelated packet identifier",
			timeout: time.Second,
			want:    durableAcked,
			server: func(connection net.Conn, ctx context.Context) error {
				if err := writeMQTTPacket(connection, ctx, mqttControlPacket(0x40, 2)); err != nil {
					return err
				}
				return writeMQTTPacket(connection, ctx, mqttControlPacket(0x40, 1))
			},
		},
		{
			name:       "connection closed",
			timeout:    time.Second,
			want:       durableClosed,
			wantAckErr: true,
			server:     func(net.Conn, context.Context) error { return nil },
		},
		{
			name:       "no response before deadline",
			timeout:    50 * time.Millisecond,
			want:       durableTimedOut,
			wantAckErr: true,
			server: func(_ net.Conn, ctx context.Context) error {
				<-ctx.Done()
				return nil
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			clientConnection, brokerConnection := net.Pipe()
			client := &durableClient{connection: clientConnection}
			ctx, cancel := context.WithTimeout(context.Background(), test.timeout)
			defer cancel()
			serverDone := make(chan error, 1)
			go func() {
				defer brokerConnection.Close()
				serverDone <- test.server(brokerConnection, ctx)
			}()
			outcome, err := client.awaitAck(ctx, 1, 1)
			client.close()
			if outcome != test.want {
				t.Fatalf("awaitAck() outcome = %q, want %q; error=%v", outcome, test.want, err)
			}
			if (err != nil) != test.wantAckErr {
				t.Fatalf("awaitAck() error = %v, want error=%t", err, test.wantAckErr)
			}
			select {
			case serverErr := <-serverDone:
				if serverErr != nil {
					t.Fatalf("fake broker exchange failed: %v", serverErr)
				}
			case <-time.After(time.Second):
				t.Fatal("fake broker did not finish the exchange")
			}
		})
	}
}

func TestDurableClientReadsQoS2Delivery(t *testing.T) {
	clientConnection, brokerConnection := net.Pipe()
	client := &durableClient{connection: clientConnection}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	serverDone := make(chan error, 1)
	go func() {
		defer brokerConnection.Close()
		topic := "chaos/durable/test"
		publishBody := []byte{0, byte(len(topic))}
		publishBody = append(publishBody, topic...)
		publishBody = append(publishBody, 0, 7)
		publishBody = append(publishBody, "payload"...)
		if err := writeMQTTPacket(brokerConnection, ctx, mqttPacket(0x34, publishBody)); err != nil {
			serverDone <- err
			return
		}
		header, body, err := readMQTTPacket(brokerConnection, ctx)
		if err != nil {
			serverDone <- err
			return
		}
		if header != 0x50 || !hasPacketID(body, 7) {
			serverDone <- fmt.Errorf("PUBREC = header 0x%02x body %x, want packet identifier 7", header, body)
			return
		}
		if err := writeMQTTPacket(brokerConnection, ctx, mqttControlPacket(0x62, 7)); err != nil {
			serverDone <- err
			return
		}
		header, body, err = readMQTTPacket(brokerConnection, ctx)
		if err == nil && (header != 0x70 || !hasPacketID(body, 7)) {
			err = fmt.Errorf("PUBCOMP = header 0x%02x body %x, want packet identifier 7", header, body)
		}
		serverDone <- err
	}()
	message, err := client.receiveQoS2Delivery(ctx)
	client.close()
	if err != nil {
		t.Fatalf("receiveQoS2Delivery() error = %v", err)
	}
	if message.topic != "chaos/durable/test" || string(message.payload) != "payload" || message.qos != 2 || message.packetID != 7 {
		t.Fatalf("received message = %#v, want expected topic, payload, QoS 2 and packet identifier", message)
	}
	select {
	case serverErr := <-serverDone:
		if serverErr != nil {
			t.Fatalf("fake broker exchange failed: %v", serverErr)
		}
	case <-time.After(time.Second):
		t.Fatal("fake broker did not finish the QoS 2 exchange")
	}
}
func TestValidateTornWALCut(t *testing.T) {
	valid := powerCutEvidence{
		Torn: true, BrokerExitCode: 137, ReleasedFsyncs: 1,
		Files: []rolledBackFile{{Path: "node/wal.bin", DurableBytes: 16, UnsyncedBytes: 8, KeptBytes: 4}},
	}
	tests := []struct {
		name     string
		evidence powerCutEvidence
		wantErr  bool
	}{
		{name: "valid torn WAL prefix", evidence: valid},
		{name: "no WAL file", evidence: powerCutEvidence{Torn: true, BrokerExitCode: 137, ReleasedFsyncs: 1}, wantErr: true},
		{name: "unrelated WAL file", evidence: func() powerCutEvidence {
			v := valid
			v.Files = append([]rolledBackFile(nil), valid.Files...)
			v.Files[0].Path = "other/wal.bin"
			return v
		}(), wantErr: true},
		{name: "empty kept prefix", evidence: func() powerCutEvidence {
			v := valid
			v.Files = append([]rolledBackFile(nil), valid.Files...)
			v.Files[0].KeptBytes = 0
			return v
		}(), wantErr: true},
		{name: "entire unsynced tail kept", evidence: func() powerCutEvidence {
			v := valid
			v.Files = append([]rolledBackFile(nil), valid.Files...)
			v.Files[0].KeptBytes = v.Files[0].UnsyncedBytes
			return v
		}(), wantErr: true},
		{name: "not torn", evidence: func() powerCutEvidence { v := valid; v.Torn = false; return v }(), wantErr: true},
		{name: "no held fsync released", evidence: func() powerCutEvidence { v := valid; v.ReleasedFsyncs = 0; return v }(), wantErr: true},
		{name: "clean broker exit", evidence: func() powerCutEvidence { v := valid; v.BrokerExitCode = 0; return v }(), wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateTornWALCut(test.evidence)
			if (err != nil) != test.wantErr {
				t.Fatalf("validateTornWALCut() error = %v, want error=%t", err, test.wantErr)
			}
		})
	}
}
func TestRecordDurableAckOnlyMarksExplicitUnknown(t *testing.T) {
	recorder, err := newEventRecorder(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := recorder.close(); err != nil {
			t.Errorf("close event recorder: %v", err)
		}
	})
	evidence := &durableAckEvidence{
		AckedBeforeFault: []string{}, AckedDuringFault: []string{}, UnackedDuringFault: []string{},
		AckOutcomeUnknownDuringFault: []string{}, AckedAfterRecovery: []string{}, MissingAcked: []string{}, Duplicates: []string{},
	}
	scenario := &storageFaultScenario{
		evidence: scenarioEvidence{DurableAck: evidence},
		recorder: recorder,
	}
	for _, result := range []struct {
		topic   string
		outcome durableAckOutcome
	}{
		{topic: "known-timeout", outcome: durableTimedOut},
		{topic: "known-closed", outcome: durableClosed},
		{topic: "unknown", outcome: durableUnknown},
	} {
		if err := scenario.recordDurableAck("1", "publisher", result.topic, 1, "during-fault", result.outcome, nil); err != nil {
			t.Fatal(err)
		}
	}
	if len(evidence.UnackedDuringFault) != 3 {
		t.Fatalf("unacked topics = %v, want all three non-ACK outcomes", evidence.UnackedDuringFault)
	}
	if len(evidence.AckOutcomeUnknownDuringFault) != 1 || evidence.AckOutcomeUnknownDuringFault[0] != "unknown" {
		t.Fatalf("unknown ACK outcomes = %v, want only the explicitly unknown topic", evidence.AckOutcomeUnknownDuringFault)
	}
}
