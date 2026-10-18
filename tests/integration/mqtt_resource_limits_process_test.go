package integration

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"narumq/internal/domain/mqtt"
)

type mqttLimitsProcessFixture struct {
	mqttAddress string
	httpAddress string
	encoder     *mqtt.Encoder
	decoder     *mqtt.Decoder
}

func startMQTTLimitsProcess(t *testing.T, extraArgs ...string) *mqttLimitsProcessFixture {
	t.Helper()
	root := clusterMembershipProcessRepoRoot(t)
	binary := clusterMembershipProcessSharedBinary(t, root, "")
	addresses := clusterMembershipProcessAddresses(t, 2)
	args := []string{"-tcp-listen", addresses[0], "-allow-insecure-anonymous-mqtt", "-http-listen", addresses[1]}
	args = append(args, extraArgs...)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	logPath := filepath.Join(t.TempDir(), "broker.log")
	logFile, err := os.Create(logPath)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	command := exec.CommandContext(ctx, binary, args...)
	command.Dir = root
	command.Stdout, command.Stderr = logFile, logFile
	if err := command.Start(); err != nil {
		cancel()
		_ = logFile.Close()
		t.Fatalf("start broker: %v", err)
	}
	done := make(chan struct{})
	var waitErr error
	go func() {
		waitErr = command.Wait()
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
		_ = logFile.Close()
		if t.Failed() {
			data, _ := os.ReadFile(logPath)
			t.Logf("broker output:\n%s", data)
		}
	})

	client := &http.Client{Timeout: 200 * time.Millisecond}
	deadline := time.Now().Add(5 * time.Second)
	for {
		response, err := client.Get("http://" + addresses[1] + "/health/live")
		if err == nil {
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK {
				break
			}
		}
		select {
		case <-done:
			t.Fatalf("broker exited during startup: %v", waitErr)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("broker management listener did not become live: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	encoder, err := mqtt.NewEncoder(mqtt.MaxRemainingLength)
	if err != nil {
		t.Fatal(err)
	}
	decoder, err := mqtt.NewDecoder(mqtt.MaxRemainingLength)
	if err != nil {
		t.Fatal(err)
	}
	return &mqttLimitsProcessFixture{mqttAddress: addresses[0], httpAddress: addresses[1], encoder: encoder, decoder: decoder}
}

func (fixture *mqttLimitsProcessFixture) connect(t *testing.T, id string, version mqtt.ProtocolVersion) (net.Conn, *mqtt.ConnAck) {
	t.Helper()
	connection, err := net.DialTimeout("tcp", fixture.mqttAddress, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	writePacket(t, connection, fixture.encoder, &mqtt.Connect{Version: version, ClientID: id, CleanStart: true})
	packet := readPacket(t, connection, fixture.decoder, version)
	ack, ok := packet.(*mqtt.ConnAck)
	if !ok || ack.ReasonCode != 0 {
		t.Fatalf("CONNACK = %#v, want success", packet)
	}
	return connection, ack
}

func (fixture *mqttLimitsProcessFixture) metrics(t *testing.T) string {
	t.Helper()
	client := &http.Client{Timeout: time.Second}
	response, err := client.Get("http://" + fixture.httpAddress + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil || response.StatusCode != http.StatusOK {
		t.Fatalf("metrics status=%d err=%v body=%s", response.StatusCode, err, body)
	}
	return string(body)
}

func requireMQTTLimitsStartupError(t *testing.T, args []string, want string) {
	t.Helper()
	root := clusterMembershipProcessRepoRoot(t)
	binary := clusterMembershipProcessSharedBinary(t, root, "")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, binary, args...)
	command.Dir = root
	output, err := command.CombinedOutput()
	if err == nil || ctx.Err() != nil || !strings.Contains(string(output), want) {
		t.Fatalf("broker args %v: err=%v output=%s, want nonzero startup exit with %q", args, err, output, want)
	}
}

// Traceability: PRD MQTT-11; docs/specs/mqtt-protocol-and-session.md §7.1.
// Scenarios: mqtt-protocol-and-session.md:MQTT-SCN-035
func TestMQTTBinaryAcceptsMaximumRemainingLengthAndAdvertisesLimits(t *testing.T) {
	fixture := startMQTTLimitsProcess(t, "-max-packet-size", "268435455", "-receive-maximum", "2")
	connection, ack := fixture.connect(t, "maximum-remaining-length", mqtt.ProtocolV5)
	if ack.ReceiveMaximum != 2 || ack.MaximumPacketSize != 268435460 {
		t.Fatalf("CONNACK limits = %#v, want Receive Maximum=2 and Maximum Packet Size=268435460", ack)
	}
	writePacket(t, connection, fixture.encoder, &mqtt.PingReq{Version: mqtt.ProtocolV5})
	if packet := readPacket(t, connection, fixture.decoder, mqtt.ProtocolV5); packet == nil {
		t.Fatal("PING returned no packet")
	} else if _, ok := packet.(*mqtt.PingResp); !ok {
		t.Fatalf("PING response = %#v, want PINGRESP", packet)
	}
}

// Traceability: PRD MQTT-11/MQTT-12; docs/specs/mqtt-protocol-and-session.md §7.1.
// Scenarios: mqtt-protocol-and-session.md:MQTT-SCN-041
func TestMQTTBinaryEnforcesReceiveMaximumPartialPacketTimeoutAndFlagBounds(t *testing.T) {
	fixture := startMQTTLimitsProcess(t, "-receive-maximum", "2", "-packet-read-timeout", "250ms")
	connection, ack := fixture.connect(t, "receive-maximum", mqtt.ProtocolV5)
	if ack.ReceiveMaximum != 2 || ack.MaximumPacketSize != 1048580 {
		t.Fatalf("CONNACK limits = %#v, want Receive Maximum=2 and Maximum Packet Size=1048580", ack)
	}
	for id := mqtt.PacketIdentifier(1); id <= 2; id++ {
		writePacket(t, connection, fixture.encoder, &mqtt.Publish{Version: mqtt.ProtocolV5, QoS: mqtt.QoSExactlyOnce, PacketID: id, Topic: "quota"})
		packet := readPacket(t, connection, fixture.decoder, mqtt.ProtocolV5)
		if rec, ok := packet.(*mqtt.PubRec); !ok || rec.PacketID != id {
			t.Fatalf("PUBREC = %#v, want packet ID %d", packet, id)
		}
	}
	writePacket(t, connection, fixture.encoder, &mqtt.Publish{Version: mqtt.ProtocolV5, QoS: mqtt.QoSExactlyOnce, PacketID: 3, Topic: "quota"})
	packet := readPacket(t, connection, fixture.decoder, mqtt.ProtocolV5)
	if disconnect, ok := packet.(*mqtt.Disconnect); !ok || disconnect.ReasonCode != 0x93 {
		t.Fatalf("Receive Maximum violation response = %#v, want DISCONNECT 0x93", packet)
	}
	requireResourceClosed(t, connection, time.Second)

	partial, _ := fixture.connect(t, "partial-packet", mqtt.ProtocolV5)
	startedAt := time.Now()
	if _, err := partial.Write([]byte{0x30, 0x10, 0x00}); err != nil {
		t.Fatal(err)
	}
	requireResourceClosed(t, partial, 2*time.Second)
	if elapsed := time.Since(startedAt); elapsed < 200*time.Millisecond {
		t.Fatalf("partial packet closed after %v, want configured 250ms timeout", elapsed)
	}
	requireMQTTLimitsStartupError(t, []string{"-tcp-listen", "127.0.0.1:0", "-allow-insecure-anonymous-mqtt", "-receive-maximum", "0"}, "-receive-maximum must be between 1 and 65535")
}

// Traceability: PRD MQTT-09/MQTT-11; docs/specs/mqtt-protocol-and-session.md §7.1.
// Scenarios: mqtt-protocol-and-session.md:MQTT-SCN-053
func TestMQTTBinaryListenerAdmissionClosesWithoutBytesCountsAndRecovers(t *testing.T) {
	for _, test := range []struct {
		flag   string
		reason string
	}{
		{flag: "-tcp-max-connections", reason: "max_connections"},
		{flag: "-tcp-connection-rate", reason: "connection_rate"},
	} {
		t.Run(test.reason, func(t *testing.T) {
			fixture := startMQTTLimitsProcess(t, test.flag, "1")
			first, _ := fixture.connect(t, "admission-first", mqtt.ProtocolV311)
			rejected, err := net.DialTimeout("tcp", fixture.mqttAddress, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = rejected.Close() })
			wire, err := fixture.encoder.Append(nil, &mqtt.Connect{Version: mqtt.ProtocolV311, ClientID: "admission-rejected", CleanStart: true})
			if err != nil {
				t.Fatal(err)
			}
			_, _ = rejected.Write(wire)
			requireResourceClosed(t, rejected, time.Second)
			wantMetric := fmt.Sprintf("narumq_mqtt_listener_admission_rejected_total{transport=\"tcp\",reason=\"%s\"} 1\n", test.reason)
			deadline := time.Now().Add(2 * time.Second)
			for !strings.Contains(fixture.metrics(t), wantMetric) {
				if time.Now().After(deadline) {
					t.Fatalf("missing admission rejection Metric %q", wantMetric)
				}
				time.Sleep(5 * time.Millisecond)
			}
			_ = first.Close()
			deadline = time.Now().Add(2 * time.Second)
			for !strings.Contains(fixture.metrics(t), "narumq_connections_active 0\n") {
				if time.Now().After(deadline) {
					t.Fatal("admitted connection was not reclaimed")
				}
				time.Sleep(5 * time.Millisecond)
			}
			if test.reason == "connection_rate" {
				// Real binary time refills one token per second. No admission retry is used.
				time.Sleep(time.Second)
			}
			_, ack := fixture.connect(t, "admission-recovered", mqtt.ProtocolV311)
			if ack.SessionPresent || ack.ReasonCode != 0 {
				t.Fatalf("recovered slot CONNACK = %#v", ack)
			}
			if !strings.Contains(fixture.metrics(t), wantMetric) {
				t.Fatalf("admission rejection Metric changed after recovery, want %q", wantMetric)
			}
			requireMQTTLimitsStartupError(t, []string{"-tcp-listen", "127.0.0.1:0", "-allow-insecure-anonymous-mqtt", test.flag, "1000001"}, test.flag+" must be between 0 and 1000000")
		})
	}
}
