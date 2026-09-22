package main

import (
	"encoding/binary"
	"strings"
	"testing"
)

func encodeProxyRemainingLength(value int) []byte {
	encoded := make([]byte, 0, 4)
	for {
		part := byte(value % 128)
		value /= 128
		if value > 0 {
			part |= 0x80
		}
		encoded = append(encoded, part)
		if value == 0 {
			return encoded
		}
	}
}

func proxyPublishFrame(qos byte, duplicate bool, packetID uint16) []byte {
	first := byte(packetTypePublish<<4) | qos<<1
	if duplicate {
		first |= 0x08
	}
	body := make([]byte, 2+len("compat/topic")+2+1)
	binary.BigEndian.PutUint16(body[:2], uint16(len("compat/topic")))
	copy(body[2:], "compat/topic")
	binary.BigEndian.PutUint16(body[2+len("compat/topic"):], packetID)
	body[len(body)-1] = 'x'
	return append(append([]byte{first}, encodeProxyRemainingLength(len(body))...), body...)
}

func proxyAckFrame(packetType byte, packetID uint16) []byte {
	body := make([]byte, 2)
	binary.BigEndian.PutUint16(body, packetID)
	return append(append([]byte{packetType << 4, 2}, body...), nil...)
}

func TestParseMQTTFrameMetadataTracksDuplicateAndPacketIdentifier(t *testing.T) {
	metadata, err := parseMQTTFrameMetadata(proxyPublishFrame(1, true, 42))
	if err != nil {
		t.Fatal(err)
	}
	if metadata.PacketType != packetTypePublish || metadata.QoS != 1 || !metadata.DUP || metadata.PacketID != 42 {
		t.Fatalf("metadata = %+v", metadata)
	}
	if _, err := parseMQTTFrameMetadata([]byte{0x32, 0x80, 0x00}); err == nil {
		t.Fatal("accepted non-canonical remaining length")
	}
}

func TestProxyStateDropsAckOnceAndRequiresDUPResend(t *testing.T) {
	state := &proxyState{dropPacketType: packetTypePubAck, dropQoS: 1}
	firstFrame := proxyPublishFrame(1, false, 7)
	if _, err := parseMQTTFrameMetadata(firstFrame); err != nil {
		t.Fatal(err)
	}
	if _, err := state.validateAndRecord("client_to_broker", 1, firstFrame); err != nil {
		t.Fatal(err)
	}
	ackFrame := proxyAckFrame(packetTypePubAck, 7)
	if _, err := parseMQTTFrameMetadata(ackFrame); err != nil {
		t.Fatal(err)
	}
	if drop, err := state.validateAndRecord("broker_to_client", 1, ackFrame); err != nil || !drop {
		t.Fatalf("first ack drop=%v err=%v", drop, err)
	}
	resentFrame := proxyPublishFrame(1, true, 7)
	if _, err := state.validateAndRecord("client_to_broker", 2, resentFrame); err != nil {
		t.Fatal(err)
	}
	if drop, err := state.validateAndRecord("broker_to_client", 2, ackFrame); err != nil || drop {
		t.Fatalf("second ack drop=%v err=%v", drop, err)
	}
	wrongFrame := proxyPublishFrame(1, false, 8)
	if _, err := state.validateAndRecord("client_to_broker", 3, wrongFrame); err == nil || !strings.Contains(err.Error(), "packet identifier") {
		t.Fatalf("wrong resend error = %v", err)
	}
}

func TestProxyStateQoS2WaitsForPUBCOMP(t *testing.T) {
	state := &proxyState{dropPacketType: packetTypePubRec, dropQoS: 2}
	publishFrame := proxyPublishFrame(2, false, 9)
	if _, err := state.validateAndRecord("client_to_broker", 1, publishFrame); err != nil {
		t.Fatal(err)
	}
	pubrecFrame := proxyAckFrame(packetTypePubRec, 9)
	if drop, err := state.validateAndRecord("broker_to_client", 1, pubrecFrame); err != nil || !drop {
		t.Fatalf("PUBREC drop=%v err=%v", drop, err)
	}
	pubcompFrame := proxyAckFrame(packetTypePubComp, 9)
	if drop, err := state.validateAndRecord("broker_to_client", 2, pubcompFrame); err != nil || drop {
		t.Fatalf("PUBCOMP drop=%v err=%v", drop, err)
	}
}
