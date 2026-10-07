package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"narumq/internal/domain/mqtt"
)

func TestSubscriberQoS2RetransmitBeforePubRelCountsOnce(t *testing.T) {
	profile := bp02SmokeProfile(t)
	col := newCollector(profile, "narumq")
	sequence := uint64(3)
	topic := publishTopic(2, sequence)
	now := time.Now()
	if err := col.addMessage(2, sequence, 1, "step-01-measure", topic, now); err != nil {
		t.Fatal(err)
	}
	col.markSent(2, sequence, now)
	var spec clientSpec
	for _, candidate := range bp02Subscribers(profile) {
		if candidate.id == "bp02-a-sub-command-02" {
			spec = candidate
		}
	}
	client := &runClient{spec: spec, inbound: make(map[mqtt.PacketIdentifier]inboundMessage), completed: make(map[mqtt.PacketIdentifier]bool)}
	runner, broker := net.Pipe()
	defer runner.Close()
	defer broker.Close()
	_ = runner.SetDeadline(time.Now().Add(3 * time.Second))
	_ = broker.SetDeadline(time.Now().Add(3 * time.Second))
	encoder, err := mqtt.NewEncoder(1 << 20)
	if err != nil {
		t.Fatal(err)
	}
	decoder, err := mqtt.NewDecoder(1 << 20)
	if err != nil {
		t.Fatal(err)
	}
	runnerErr := make(chan error, 1)
	go func() {
		reader := bufio.NewReader(runner)
		for range 3 {
			packet, err := readBP02TestPacket(reader, decoder)
			if err != nil {
				runnerErr <- err
				return
			}
			response, err := client.processInbound(profile, col, packet, time.Now())
			if err != nil {
				runnerErr <- err
				return
			}
			wire, err := encoder.Append(nil, response)
			if err == nil {
				_, err = runner.Write(wire)
			}
			if err != nil {
				runnerErr <- err
				return
			}
		}
		runnerErr <- nil
	}()
	reader := bufio.NewReader(broker)
	publish := &mqtt.Publish{Version: mqtt.ProtocolV5, QoS: mqtt.QoSExactlyOnce, PacketID: 17, Topic: topic, Payload: makePayload(profile, 2, sequence)}
	for attempt := range 2 {
		publish.DUP = attempt > 0
		wire, err := encoder.Append(nil, publish)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := broker.Write(wire); err != nil {
			t.Fatal(err)
		}
		packet, err := readBP02TestPacket(reader, decoder)
		if err != nil {
			t.Fatal(err)
		}
		ack, ok := packet.(*mqtt.PubRec)
		if !ok || ack.PacketID != 17 {
			t.Fatalf("response %d = %#v, want PUBREC17", attempt, packet)
		}
	}
	wire, err := encoder.Append(nil, &mqtt.PubRel{Version: mqtt.ProtocolV5, PacketID: 17})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := broker.Write(wire); err != nil {
		t.Fatal(err)
	}
	packet, err := readBP02TestPacket(reader, decoder)
	if err != nil {
		t.Fatal(err)
	}
	ack, ok := packet.(*mqtt.PubComp)
	if !ok || ack.PacketID != 17 {
		t.Fatalf("final response = %#v, want PUBCOMP17", packet)
	}
	if err := <-runnerErr; err != nil {
		t.Fatal(err)
	}
	col.finishStep(1, time.Now())
	observed := col.summary().Steps[0].Streams["a_command"]
	if observed.Delivered != 1 || observed.Duplicates != 0 {
		t.Fatalf("delivery=%d duplicates=%d, want1/0", observed.Delivered, observed.Duplicates)
	}
	if len(client.inbound) != 0 {
		t.Fatal("PUBREL left pending delivery")
	}
}

func readBP02TestPacket(reader *bufio.Reader, decoder *mqtt.Decoder) (mqtt.Packet, error) {
	header, err := reader.ReadByte()
	if err != nil {
		return nil, err
	}
	wire := []byte{header}
	remaining, multiplier := 0, 1
	for range 4 {
		digit, err := reader.ReadByte()
		if err != nil {
			return nil, err
		}
		wire = append(wire, digit)
		remaining += int(digit&127) * multiplier
		if digit&128 == 0 {
			start := len(wire)
			wire = append(wire, make([]byte, remaining)...)
			if _, err := io.ReadFull(reader, wire[start:]); err != nil {
				return nil, err
			}
			packet, consumed, err := decoder.Decode(wire, mqtt.ProtocolV5)
			if err == nil && consumed != len(wire) {
				err = fmt.Errorf("decoder consumed %d of %d", consumed, len(wire))
			}
			return packet, err
		}
		multiplier *= 128
	}
	return nil, fmt.Errorf("invalid remaining length")
}
