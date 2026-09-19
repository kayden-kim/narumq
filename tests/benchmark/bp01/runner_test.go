package main

import (
	"encoding/binary"
	"net"
	"strings"
	"testing"
	"time"

	"narumq/internal/domain/mqtt"
)

func TestPayloadSequenceAndDeterministicBody(t *testing.T) {
	profile := validProfile()
	payload := makePayload(profile.Seed, 42, profile.PayloadBytes)
	sequence, err := validatePayload(profile, payload)
	if err != nil {
		t.Fatal(err)
	}
	if sequence != 42 {
		t.Fatalf("validatePayload() sequence = %d, want 42", sequence)
	}
	if binary.BigEndian.Uint64(payload[:8]) != 42 {
		t.Fatal("payload sequence is not big-endian")
	}
	second := makePayload(profile.Seed, 42, profile.PayloadBytes)
	for index := range payload {
		if payload[index] != second[index] {
			t.Fatalf("payload is not deterministic at byte %d", index)
		}
	}
	payload[profile.PayloadBytes-1] ^= 1
	if _, err := validatePayload(profile, payload); err == nil || !strings.Contains(err.Error(), "deterministic validation") {
		t.Fatalf("validatePayload() error = %v, want deterministic validation failure", err)
	}
}

func TestReadConnAckAcceptsBrokerProperties(t *testing.T) {
	reader, writer := net.Pipe()
	defer reader.Close()
	defer writer.Close()
	wire := []byte{
		0x20, 0x08,
		0x00, 0x00,
		0x05, 0x21, 0x00, 0x0A, 0x24, 0x00,
	}
	go func() {
		_, _ = writer.Write(wire)
	}()
	connAck, err := readConnAck(reader, mqtt.ProtocolV5, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if connAck.Version != mqtt.ProtocolV5 || connAck.SessionPresent || connAck.ReasonCode != 0 {
		t.Fatalf("readConnAck() = %#v, want successful MQTT 5 CONNACK", connAck)
	}
}

func TestReadWireFrameMidFrameTimeoutIsFatal(t *testing.T) {
	reader, writer := net.Pipe()
	defer reader.Close()
	defer writer.Close()
	go func() {
		_, _ = writer.Write([]byte{0x30})
	}()
	_, err := readWireFrame(reader, time.Second, 20*time.Millisecond)
	if err == nil || isTimeout(err) {
		t.Fatalf("readWireFrame() error = %v, want fatal non-timeout mid-frame error", err)
	}
}

func TestValidatePayloadRejectsShortPayload(t *testing.T) {
	profile := validProfile()
	if _, err := validatePayload(profile, make([]byte, profile.PayloadBytes-1)); err == nil || !strings.Contains(err.Error(), "length") {
		t.Fatalf("validatePayload() error = %v, want length failure", err)
	}
}
