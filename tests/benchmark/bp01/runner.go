package main

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"narumq/internal/domain/mqtt"
	"narumq/tests/benchmark/internal/jsonl"
	"narumq/tests/benchmark/internal/mqttclient"
)

const (
	benchmarkMaxPacketSize = mqttclient.MaxPacketSize
	handshakeTimeout       = mqttclient.DefaultHandshakeTimeout
	readPollTimeout        = 100 * time.Millisecond
	phaseDrain             = "drain"
)

// RunOptions controls one in-container target execution.
type RunOptions struct {
	Profile   Profile
	Target    string
	Address   string
	OutputDir string
}

// Run executes the MQTT workload and writes samples.jsonl and summary.json.
// Invalid delivery counts return the summary together with an error so an
// orchestrator cannot mistake an incomplete target for a successful run.
func Run(ctx context.Context, options RunOptions) (Summary, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := options.Profile.Validate(); err != nil {
		return Summary{}, err
	}
	if options.Target != "narumq" && options.Target != "emqx" {
		return Summary{}, fmt.Errorf("unsupported benchmark target %q", options.Target)
	}
	if options.Address == "" {
		return Summary{}, errors.New("MQTT address is required")
	}
	if options.OutputDir == "" {
		return Summary{}, errors.New("output directory is required")
	}
	if err := os.MkdirAll(options.OutputDir, 0o755); err != nil {
		return Summary{}, fmt.Errorf("create output directory: %w", err)
	}
	phasesWriter, err := jsonl.Open(filepath.Join(options.OutputDir, "phases.jsonl"))
	if err != nil {
		return Summary{}, err
	}
	collector, err := NewCollector(options.Profile, options.Target, options.OutputDir)
	if err != nil {
		_ = phasesWriter.Close()
		return Summary{}, err
	}

	runErr := executeWorkload(ctx, options, collector, phasesWriter)
	if closeErr := phasesWriter.Close(); closeErr != nil {
		runErr = errors.Join(runErr, fmt.Errorf("close phases.jsonl: %w", closeErr))
	}
	if runErr != nil {
		collector.RecordProtocolError()
	}
	summary, finalizeErr := collector.Finalize()
	if finalizeErr != nil {
		return summary, errors.Join(runErr, finalizeErr)
	}
	if err := WriteSummary(options.OutputDir, summary); err != nil {
		return summary, errors.Join(runErr, err)
	}
	if runErr != nil {
		return summary, runErr
	}
	if !summary.Valid {
		return summary, fmt.Errorf("target %s produced an invalid BP-01 result", options.Target)
	}
	return summary, nil
}

func executeWorkload(ctx context.Context, options RunOptions, collector *Collector, phasesWriter *jsonl.Writer) error {
	profile := options.Profile
	subscriber, _, err := mqttclient.Connect(ctx, mqttclient.ConnectOptions{
		Address:          options.Address,
		ClientID:         "bp01-subscriber",
		KeepAlive:        30,
		HandshakeTimeout: handshakeTimeout,
	})
	if err != nil {
		return fmt.Errorf("connect subscriber: %w", err)
	}
	defer subscriber.Close()
	if err := completeSubscribe(subscriber, profile); err != nil {
		return fmt.Errorf("subscriber SUBSCRIBE: %w", err)
	}

	stopReceiver := make(chan struct{})
	receiverDone := make(chan struct{})
	fatalErrors := make(chan error, 1)
	var stopOnce sync.Once
	signalFatal := func(err error) {
		if err == nil {
			return
		}
		select {
		case fatalErrors <- err:
		default:
		}
	}
	go func() {
		defer close(receiverDone)
		receiveDeliveries(subscriber, profile, collector, stopReceiver, signalFatal)
	}()

	publisher, _, err := mqttclient.Connect(ctx, mqttclient.ConnectOptions{
		Address:          options.Address,
		ClientID:         "bp01-publisher",
		KeepAlive:        30,
		HandshakeTimeout: handshakeTimeout,
	})
	if err != nil {
		stopOnce.Do(func() { close(stopReceiver) })
		<-receiverDone
		return fmt.Errorf("connect publisher: %w", err)
	}
	defer publisher.Close()

	collector.SetOffered(profile.OfferedMeasurementSamples())
	nextSequence := uint64(1)
	if err := runRecordedPhase(phasesWriter, phaseWarmup, func(start time.Time) error {
		return runPhase(ctx, publisher, profile, collector, phaseWarmup, start, profile.WarmupMillis, &nextSequence, fatalErrors)
	}); err != nil {
		stopOnce.Do(func() { close(stopReceiver) })
		<-receiverDone
		return err
	}
	if err := runRecordedPhase(phasesWriter, phaseMeasurement, func(start time.Time) error {
		return runPhase(ctx, publisher, profile, collector, phaseMeasurement, start, profile.MeasureMillis, &nextSequence, fatalErrors)
	}); err != nil {
		stopOnce.Do(func() { close(stopReceiver) })
		<-receiverDone
		return err
	}
	if err := publisher.Disconnect(handshakeTimeout); err != nil {
		stopOnce.Do(func() { close(stopReceiver) })
		<-receiverDone
		return fmt.Errorf("publisher DISCONNECT: %w", err)
	}

	if err := runRecordedPhase(phasesWriter, phaseDrain, func(start time.Time) error {
		return waitForDrain(ctx, start, profile.DrainMillis, fatalErrors)
	}); err != nil {
		stopOnce.Do(func() { close(stopReceiver) })
		<-receiverDone
		return err
	}
	if err := subscriber.Disconnect(handshakeTimeout); err != nil {
		stopOnce.Do(func() { close(stopReceiver) })
		<-receiverDone
		return fmt.Errorf("subscriber DISCONNECT: %w", err)
	}
	stopOnce.Do(func() { close(stopReceiver) })
	if err := subscriber.Close(); err != nil {
		return fmt.Errorf("close subscriber: %w", err)
	}
	<-receiverDone
	select {
	case err := <-fatalErrors:
		return err
	default:
		return nil
	}
}

type phaseEvent struct {
	Phase     string `json:"phase"`
	Event     string `json:"event"`
	Timestamp string `json:"timestamp"`
}

func runRecordedPhase(writer *jsonl.Writer, phase string, execute func(time.Time) error) error {
	startedAt := time.Now()
	if err := writer.Write(phaseEvent{Phase: phase, Event: "start", Timestamp: startedAt.UTC().Format(time.RFC3339Nano)}); err != nil {
		return fmt.Errorf("write %s start: %w", phase, err)
	}
	runErr := execute(startedAt)
	finishedAt := time.Now()
	endErr := writer.Write(phaseEvent{Phase: phase, Event: "end", Timestamp: finishedAt.UTC().Format(time.RFC3339Nano)})
	if runErr != nil && endErr != nil {
		return errors.Join(runErr, fmt.Errorf("write %s end: %w", phase, endErr))
	}
	if runErr != nil {
		return runErr
	}
	if endErr != nil {
		return fmt.Errorf("write %s end: %w", phase, endErr)
	}
	return nil
}

func completeSubscribe(connection *mqttclient.Client, profile Profile) error {
	packet := &mqtt.Subscribe{
		Version:  mqtt.ProtocolV5,
		PacketID: 1,
		Subscriptions: []mqtt.Subscription{{
			Filter:            profile.Topic,
			QoS:               mqtt.QoSAtMostOnce,
			RetainHandling:    mqtt.SendRetainedAtSubscribe,
			NoLocal:           false,
			RetainAsPublished: false,
		}},
	}
	if err := connection.Write(packet, handshakeTimeout); err != nil {
		return err
	}
	response, err := connection.Read(handshakeTimeout)
	if err != nil {
		return err
	}
	subAck, ok := response.(*mqtt.SubAck)
	if !ok || subAck.Version != profile.ProtocolVersion() || subAck.PacketID != packet.PacketID || len(subAck.ReasonCodes) != 1 || subAck.ReasonCodes[0] != 0 {
		return fmt.Errorf("unexpected SUBACK %#v", response)
	}
	return nil
}

func runPhase(ctx context.Context, connection *mqttclient.Client, profile Profile, collector *Collector, phase string, phaseStart time.Time, millis int64, nextSequence *uint64, fatalErrors <-chan error) error {
	sampleCount := samplesForDuration(profile.PublishRatePerSecond, millis)
	interval := time.Second / time.Duration(profile.PublishRatePerSecond)
	for index := uint64(0); index < sampleCount; index++ {
		select {
		case err := <-fatalErrors:
			return err
		default:
		}
		scheduledAt := phaseStart.Add(time.Duration(index) * interval)
		if err := waitUntil(ctx, scheduledAt); err != nil {
			return err
		}
		payload := makePayload(profile.Seed, *nextSequence, profile.PayloadBytes)
		sentAt := time.Now()
		if err := connection.Write(&mqtt.Publish{
			Version: mqtt.ProtocolV5,
			QoS:     mqtt.QoSAtMostOnce,
			Topic:   profile.Topic,
			Payload: payload,
		}, handshakeTimeout); err != nil {
			return fmt.Errorf("publish sequence %d: %w", *nextSequence, err)
		}
		scheduleLag := sentAt.Sub(scheduledAt)
		collector.RecordSent(*nextSequence, phase, scheduledAt, sentAt, scheduleLag, scheduleLag >= interval)
		*nextSequence++
	}
	return nil
}

func waitUntil(ctx context.Context, target time.Time) error {
	for {
		delay := time.Until(target)
		if delay <= 0 {
			return nil
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func waitForDrain(ctx context.Context, phaseStart time.Time, drainMillis int64, fatalErrors <-chan error) error {
	deadline := phaseStart.Add(time.Duration(drainMillis) * time.Millisecond)
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case err := <-fatalErrors:
			return err
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if time.Now().After(deadline) {
				return nil
			}
		}
	}
}

func receiveDeliveries(connection *mqttclient.Client, profile Profile, collector *Collector, stop <-chan struct{}, signalFatal func(error)) {
	for {
		select {
		case <-stop:
			return
		default:
		}
		packet, err := connection.Read(readPollTimeout)
		if err != nil {
			if mqttclient.IsTimeout(err) {
				continue
			}
			select {
			case <-stop:
				return
			default:
			}
			signalFatal(fmt.Errorf("subscriber read: %w", err))
			return
		}
		publish, ok := packet.(*mqtt.Publish)
		if !ok || publish.Version != profile.ProtocolVersion() || publish.Topic != profile.Topic || publish.QoS != profile.QoS() || publish.DUP || publish.Retain {
			signalFatal(fmt.Errorf("unexpected delivery packet %#v", packet))
			return
		}
		sequence, err := validatePayload(profile, publish.Payload)
		if err != nil {
			signalFatal(err)
			return
		}
		if err := collector.RecordDelivery(sequence, time.Now()); err != nil {
			signalFatal(err)
			return
		}
	}
}

func makePayload(seed, sequence uint64, size int) []byte {
	payload := make([]byte, size)
	binary.BigEndian.PutUint64(payload[:8], sequence)
	for index := 8; index < len(payload); index++ {
		payload[index] = deterministicByte(seed, sequence, index-8)
	}
	return payload
}

func deterministicByte(seed, sequence uint64, offset int) byte {
	value := seed + sequence*0x9E3779B97F4A7C15 + uint64(offset)*0xD1B54A32D192ED03
	value = (value ^ (value >> 30)) * 0xBF58476D1CE4E5B9
	value = (value ^ (value >> 27)) * 0x94D049BB133111EB
	value ^= value >> 31
	return byte(value >> ((offset & 7) * 8))
}

func validatePayload(profile Profile, payload []byte) (uint64, error) {
	if len(payload) != profile.PayloadBytes {
		return 0, fmt.Errorf("delivery payload length %d does not match profile %d", len(payload), profile.PayloadBytes)
	}
	sequence := binary.BigEndian.Uint64(payload[:8])
	if sequence == 0 {
		return 0, errors.New("delivery payload sequence is zero")
	}
	for index := 8; index < len(payload); index++ {
		if payload[index] != deterministicByte(profile.Seed, sequence, index-8) {
			return 0, fmt.Errorf("delivery payload sequence %d failed deterministic validation", sequence)
		}
	}
	return sequence, nil
}
