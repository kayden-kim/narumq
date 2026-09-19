package main

import (
	"encoding/binary"
	"strings"
	"testing"
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

func TestValidatePayloadRejectsShortPayload(t *testing.T) {
	profile := validProfile()
	if _, err := validatePayload(profile, make([]byte, profile.PayloadBytes-1)); err == nil || !strings.Contains(err.Error(), "length") {
		t.Fatalf("validatePayload() error = %v, want length failure", err)
	}
}
