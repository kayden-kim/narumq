package main

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"narumq/internal/domain/mqtt"
)

const (
	benchmarkMaxPacketSize = 1 << 20
	handshakeTimeout       = 30 * time.Second
	readPollTimeout        = 100 * time.Millisecond
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
	collector, err := NewCollector(options.Profile, options.Target, options.OutputDir)
	if err != nil {
		return Summary{}, err
	}

	runErr := executeWorkload(ctx, options, collector)
	if runErr != nil {
		collector.RecordProtocolError()
	}
	summary, finalizeErr := collector.Finalize()
	if finalizeErr != nil {
		return summary, finalizeErr
	}
	if err := WriteSummary(options.OutputDir, summary); err != nil {
		return summary, err
	}
	if runErr != nil {
		return summary, runErr
	}
	if !summary.Valid {
		return summary, fmt.Errorf("target %s produced an invalid BP-01 result", options.Target)
	}
	return summary, nil
}

func executeWorkload(ctx context.Context, options RunOptions, collector *Collector) error {
	profile := options.Profile
	decoder, err := mqtt.NewDecoder(benchmarkMaxPacketSize)
	if err != nil {
		return fmt.Errorf("create MQTT decoder: %w", err)
	}
	encoder, err := mqtt.NewEncoder(benchmarkMaxPacketSize)
	if err != nil {
		return fmt.Errorf("create MQTT encoder: %w", err)
	}

	subscriber, err := dialClient(ctx, options.Address, "bp01-subscriber")
	if err != nil {
		return fmt.Errorf("connect subscriber: %w", err)
	}
	defer subscriber.Close()
	if err := completeConnect(subscriber, encoder, decoder, profile, "bp01-subscriber"); err != nil {
		return fmt.Errorf("subscriber CONNECT: %w", err)
	}
	if err := completeSubscribe(subscriber, encoder, decoder, profile); err != nil {
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
		receiveDeliveries(subscriber, decoder, profile, collector, stopReceiver, signalFatal)
	}()

	publisher, err := dialClient(ctx, options.Address, "bp01-publisher")
	if err != nil {
		stopOnce.Do(func() { close(stopReceiver) })
		<-receiverDone
		return fmt.Errorf("connect publisher: %w", err)
	}
	defer publisher.Close()
	if err := completeConnect(publisher, encoder, decoder, profile, "bp01-publisher"); err != nil {
		stopOnce.Do(func() { close(stopReceiver) })
		<-receiverDone
		return fmt.Errorf("publisher CONNECT: %w", err)
	}

	collector.SetOffered(profile.OfferedMeasurementSamples())
	runStart := time.Now()
	nextSequence := uint64(1)
	if err := runPhase(ctx, publisher, encoder, profile, collector, phaseWarmup, runStart, profile.WarmupMillis, &nextSequence, fatalErrors); err != nil {
		stopOnce.Do(func() { close(stopReceiver) })
		<-receiverDone
		return err
	}
	measurementStart := runStart.Add(time.Duration(profile.WarmupMillis) * time.Millisecond)
	if err := runPhase(ctx, publisher, encoder, profile, collector, phaseMeasurement, measurementStart, profile.MeasureMillis, &nextSequence, fatalErrors); err != nil {
		stopOnce.Do(func() { close(stopReceiver) })
		<-receiverDone
		return err
	}
	if err := writePacket(publisher, encoder, &mqtt.Disconnect{Version: mqtt.ProtocolV5}); err != nil {
		stopOnce.Do(func() { close(stopReceiver) })
		<-receiverDone
		return fmt.Errorf("publisher DISCONNECT: %w", err)
	}

	if err := waitForDrain(ctx, collector, profile.DrainMillis, fatalErrors); err != nil {
		stopOnce.Do(func() { close(stopReceiver) })
		<-receiverDone
		return err
	}
	if err := writePacket(subscriber, encoder, &mqtt.Disconnect{Version: mqtt.ProtocolV5}); err != nil {
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

func dialClient(ctx context.Context, address, clientID string) (net.Conn, error) {
	dialer := net.Dialer{Timeout: handshakeTimeout}
	connection, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		return nil, err
	}
	return connection, nil
}

func completeConnect(connection net.Conn, encoder *mqtt.Encoder, decoder *mqtt.Decoder, profile Profile, clientID string) error {
	packet := &mqtt.Connect{
		Version:               mqtt.ProtocolV5,
		CleanStart:            true,
		SessionExpiryInterval: 0,
		KeepAlive:             30,
		ClientID:              clientID,
	}
	if err := writePacket(connection, encoder, packet); err != nil {
		return err
	}
	connAck, err := readConnAck(connection, mqtt.ProtocolV5, handshakeTimeout)
	if err != nil {
		return err
	}
	if connAck.Version != profile.ProtocolVersion() || connAck.SessionPresent || connAck.ReasonCode != 0 {
		return fmt.Errorf("unexpected CONNACK %#v", connAck)
	}
	return nil
}

func completeSubscribe(connection net.Conn, encoder *mqtt.Encoder, decoder *mqtt.Decoder, profile Profile) error {
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
	if err := writePacket(connection, encoder, packet); err != nil {
		return err
	}
	response, err := readPacket(connection, decoder, mqtt.ProtocolV5, handshakeTimeout)
	if err != nil {
		return err
	}
	subAck, ok := response.(*mqtt.SubAck)
	if !ok || subAck.Version != profile.ProtocolVersion() || subAck.PacketID != packet.PacketID || len(subAck.ReasonCodes) != 1 || subAck.ReasonCodes[0] != 0 {
		return fmt.Errorf("unexpected SUBACK %#v", response)
	}
	return nil
}

func runPhase(ctx context.Context, connection net.Conn, encoder *mqtt.Encoder, profile Profile, collector *Collector, phase string, phaseStart time.Time, millis int64, nextSequence *uint64, fatalErrors <-chan error) error {
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
		if err := writePacket(connection, encoder, &mqtt.Publish{
			Version: mqtt.ProtocolV5,
			QoS:     mqtt.QoSAtMostOnce,
			Topic:   profile.Topic,
			Payload: payload,
		}); err != nil {
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

func waitForDrain(ctx context.Context, collector *Collector, drainMillis int64, fatalErrors <-chan error) error {
	deadline := time.Now().Add(time.Duration(drainMillis) * time.Millisecond)
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

func receiveDeliveries(connection net.Conn, decoder *mqtt.Decoder, profile Profile, collector *Collector, stop <-chan struct{}, signalFatal func(error)) {
	for {
		select {
		case <-stop:
			return
		default:
		}
		packet, err := readPacket(connection, decoder, profile.ProtocolVersion(), readPollTimeout)
		if err != nil {
			if isTimeout(err) {
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

func isTimeout(err error) bool {
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

func writePacket(connection net.Conn, encoder *mqtt.Encoder, packet mqtt.Packet) error {
	wire, err := encoder.Append(nil, packet)
	if err != nil {
		return err
	}
	if err := connection.SetWriteDeadline(time.Now().Add(handshakeTimeout)); err != nil {
		return err
	}
	for len(wire) > 0 {
		written, err := connection.Write(wire)
		if written > 0 {
			wire = wire[written:]
		}
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

type wireFrame struct {
	header byte
	body   []byte
	wire   []byte
}

func readPacket(connection net.Conn, decoder *mqtt.Decoder, version mqtt.ProtocolVersion, timeout time.Duration) (mqtt.Packet, error) {
	frame, err := readWireFrame(connection, timeout, handshakeTimeout)
	if err != nil {
		return nil, err
	}
	packet, consumed, err := decoder.Decode(frame.wire, version)
	if err != nil {
		return nil, err
	}
	if consumed != len(frame.wire) {
		return nil, mqtt.ErrMalformedPacket
	}
	return packet, nil
}

func readConnAck(connection net.Conn, version mqtt.ProtocolVersion, timeout time.Duration) (*mqtt.ConnAck, error) {
	frame, err := readWireFrame(connection, timeout, handshakeTimeout)
	if err != nil {
		return nil, err
	}
	if frame.header != 0x20 || len(frame.body) < 2 {
		return nil, mqtt.ErrMalformedPacket
	}
	if frame.body[0]&0xFE != 0 {
		return nil, mqtt.ErrMalformedPacket
	}
	if version == mqtt.ProtocolV311 {
		if len(frame.body) != 2 {
			return nil, mqtt.ErrMalformedPacket
		}
		return &mqtt.ConnAck{
			Version:        version,
			SessionPresent: frame.body[0]&0x01 != 0,
			ReasonCode:     frame.body[1],
		}, nil
	}
	if version != mqtt.ProtocolV5 || len(frame.body) < 3 {
		return nil, mqtt.ErrMalformedPacket
	}
	propertyLength, consumed, err := decodeWireVariableByteInteger(frame.body[2:])
	if err != nil || 2+consumed+propertyLength != len(frame.body) {
		return nil, mqtt.ErrMalformedPacket
	}
	return &mqtt.ConnAck{
		Version:        version,
		SessionPresent: frame.body[0]&0x01 != 0,
		ReasonCode:     frame.body[1],
	}, nil
}

func readWireFrame(connection net.Conn, firstByteTimeout, frameTimeout time.Duration) (wireFrame, error) {
	if err := connection.SetReadDeadline(time.Now().Add(firstByteTimeout)); err != nil {
		return wireFrame{}, err
	}
	var header [1]byte
	if _, err := io.ReadFull(connection, header[:]); err != nil {
		return wireFrame{}, err
	}
	if err := connection.SetReadDeadline(time.Now().Add(frameTimeout)); err != nil {
		return wireFrame{}, err
	}
	wire := []byte{header[0]}
	remainingLength := 0
	multiplier := 1
	for index := 0; index < 4; index++ {
		var encoded [1]byte
		if _, err := io.ReadFull(connection, encoded[:]); err != nil {
			if isTimeout(err) {
				return wireFrame{}, fmt.Errorf("MQTT frame read timed out after header: %v", err)
			}
			return wireFrame{}, err
		}
		wire = append(wire, encoded[0])
		digit := int(encoded[0] & 0x7F)
		if digit > 0 && remainingLength > 268435455-digit*multiplier {
			return wireFrame{}, mqtt.ErrMalformedPacket
		}
		remainingLength += digit * multiplier
		if encoded[0]&0x80 == 0 {
			if index > 0 && digit == 0 {
				return wireFrame{}, mqtt.ErrMalformedPacket
			}
			break
		}
		if index == 3 {
			return wireFrame{}, mqtt.ErrMalformedPacket
		}
		multiplier *= 128
	}
	if remainingLength > benchmarkMaxPacketSize {
		return wireFrame{}, mqtt.ErrPacketTooLarge
	}
	body := make([]byte, remainingLength)
	if _, err := io.ReadFull(connection, body); err != nil {
		if isTimeout(err) {
			return wireFrame{}, fmt.Errorf("MQTT frame read timed out after header: %v", err)
		}
		return wireFrame{}, err
	}
	wire = append(wire, body...)
	return wireFrame{header: header[0], body: body, wire: wire}, nil
}

func decodeWireVariableByteInteger(encoded []byte) (int, int, error) {
	value := 0
	multiplier := 1
	for index := 0; index < 4; index++ {
		if index >= len(encoded) {
			return 0, 0, mqtt.ErrMalformedPacket
		}
		digit := int(encoded[index] & 0x7F)
		if digit > 0 && value > 268435455-digit*multiplier {
			return 0, 0, mqtt.ErrMalformedPacket
		}
		value += digit * multiplier
		if encoded[index]&0x80 == 0 {
			if index > 0 && digit == 0 {
				return 0, 0, mqtt.ErrMalformedPacket
			}
			return value, index + 1, nil
		}
		multiplier *= 128
	}
	return 0, 0, mqtt.ErrMalformedPacket
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
