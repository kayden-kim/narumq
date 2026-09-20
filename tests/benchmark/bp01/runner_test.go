package main

import (
	"context"
	"encoding/binary"
	"errors"
	"strings"
	"testing"
	"time"

	"narumq/internal/domain/mqtt"
)

type synchronousDeliveryWriter struct {
	profile            Profile
	collector          *Collector
	writeErr           error
	deliverBeforeError bool
	captureAfterError  bool
	capturedSequence   chan uint64
}

func (writer *synchronousDeliveryWriter) Write(packet mqtt.Packet, _ time.Duration) error {
	publish, ok := packet.(*mqtt.Publish)
	if !ok {
		return errors.New("benchmark writer received a non-PUBLISH packet")
	}
	sequence, err := validatePayload(writer.profile, publish.Payload)
	if err != nil {
		return err
	}
	if writer.captureAfterError {
		writer.capturedSequence <- sequence
		return writer.writeErr
	}
	if writer.deliverBeforeError {
		if err := writer.collector.RecordDelivery(sequence, time.Now()); err != nil {
			return err
		}
		return writer.writeErr
	}
	if writer.writeErr != nil {
		return writer.writeErr
	}
	return writer.collector.RecordDelivery(sequence, time.Now())
}

func TestRunPhaseRegistersSequenceBeforeWrite(t *testing.T) {
	profile := validProfile()
	profile.MeasureMillis = 50
	collector, err := NewCollector(profile, "narumq", t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	collector.SetOffered(1)
	writer := &synchronousDeliveryWriter{profile: profile, collector: collector}
	nextSequence := uint64(1)
	if err := runPhase(context.Background(), writer, profile, collector, phaseMeasurement, time.Now(), profile.MeasureMillis, &nextSequence, make(chan error)); err != nil {
		t.Fatalf("runPhase() error = %v", err)
	}
	summary, err := collector.Finalize()
	if err != nil {
		t.Fatal(err)
	}
	if summary.Sent != 1 || summary.Delivered != 1 || summary.OutOfRange != 0 || summary.Missing != 0 || !summary.Valid {
		t.Fatalf("summary = %#v, want one valid synchronous delivery", summary)
	}
}

func TestRunPhaseDoesNotCountFailedWrite(t *testing.T) {
	profile := validProfile()
	profile.MeasureMillis = 50
	collector, err := NewCollector(profile, "narumq", t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	collector.SetOffered(1)
	writer := &synchronousDeliveryWriter{
		profile:   profile,
		collector: collector,
		writeErr:  errors.New("write failed"),
	}
	nextSequence := uint64(1)
	if err := runPhase(context.Background(), writer, profile, collector, phaseMeasurement, time.Now().Add(-time.Second), profile.MeasureMillis, &nextSequence, make(chan error)); err == nil {
		t.Fatal("runPhase() error = nil, want write failure")
	}
	summary, err := collector.Finalize()
	if err != nil {
		t.Fatal(err)
	}
	if summary.Sent != 0 || summary.Delivered != 0 || summary.Missing != 0 || summary.OutOfRange != 0 || summary.GeneratorOverload != 0 {
		t.Fatalf("summary = %#v, want failed write excluded from delivery counts", summary)
	}
}

func TestRunPhaseCommitsDeliveryBeforeWriteError(t *testing.T) {
	profile := validProfile()
	profile.MeasureMillis = 50
	collector, err := NewCollector(profile, "narumq", t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	collector.SetOffered(1)
	writer := &synchronousDeliveryWriter{
		profile:            profile,
		collector:          collector,
		writeErr:           errors.New("write failed after delivery"),
		deliverBeforeError: true,
	}
	nextSequence := uint64(1)
	if err := runPhase(context.Background(), writer, profile, collector, phaseMeasurement, time.Now().Add(-time.Second), profile.MeasureMillis, &nextSequence, make(chan error)); err == nil {
		t.Fatal("runPhase() error = nil, want write failure")
	}
	summary, err := collector.Finalize()
	if err != nil {
		t.Fatal(err)
	}
	if summary.Sent != 1 || summary.Delivered != 1 || summary.Missing != 0 || summary.OutOfRange != 0 || summary.GeneratorOverload != 1 {
		t.Fatalf("summary = %#v, want delivered pending send committed", summary)
	}
}

func TestRunPhaseCommitsLateDeliveryAfterWriteError(t *testing.T) {
	profile := validProfile()
	profile.MeasureMillis = 50
	collector, err := NewCollector(profile, "narumq", t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	collector.SetOffered(1)
	capturedSequence := make(chan uint64, 1)
	writer := &synchronousDeliveryWriter{
		profile:           profile,
		collector:         collector,
		writeErr:          errors.New("write failed before receiver ran"),
		captureAfterError: true,
		capturedSequence:  capturedSequence,
	}
	nextSequence := uint64(1)
	if err := runPhase(context.Background(), writer, profile, collector, phaseMeasurement, time.Now().Add(-time.Second), profile.MeasureMillis, &nextSequence, make(chan error)); err == nil {
		t.Fatal("runPhase() error = nil, want write failure")
	}
	sequence := <-capturedSequence
	if err := collector.RecordDelivery(sequence, time.Now()); err != nil {
		t.Fatalf("late RecordDelivery() error = %v", err)
	}
	summary, err := collector.Finalize()
	if err != nil {
		t.Fatal(err)
	}
	if summary.Sent != 1 || summary.Delivered != 1 || summary.Missing != 0 || summary.OutOfRange != 0 || summary.GeneratorOverload != 1 {
		t.Fatalf("summary = %#v, want late delivery to commit uncertain send", summary)
	}
}

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
