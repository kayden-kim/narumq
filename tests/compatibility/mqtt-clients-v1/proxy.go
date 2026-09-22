package main

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"time"
)

const (
	maxProxyRemainingLength = 1 << 20
	packetTypePublish       = 3
	packetTypePubAck        = 4
	packetTypePubRec        = 5
	packetTypePubComp       = 7
)

// MQTTFrameMetadata contains only safe packet metadata; payload bytes never enter traces.
type MQTTFrameMetadata struct {
	PacketType byte   `json:"packetType"`
	DUP        bool   `json:"dup"`
	QoS        byte   `json:"qos"`
	PacketID   uint16 `json:"packetId,omitempty"`
}

type proxyTraceEvent struct {
	At         string            `json:"at"`
	Direction  string            `json:"direction"`
	Action     string            `json:"action"`
	Connection int               `json:"connection"`
	Metadata   MQTTFrameMetadata `json:"metadata"`
}

type proxyState struct {
	mu             sync.Mutex
	dropPacketType byte
	dropQoS        byte
	expectedID     uint16
	dropped        bool
	resentObserved bool
	completionSeen bool
	firstSession   int
	trace          []proxyTraceEvent
}

func (state *proxyState) traceSnapshot() []proxyTraceEvent {
	state.mu.Lock()
	defer state.mu.Unlock()
	return append([]proxyTraceEvent(nil), state.trace...)
}

func (s *proxyState) validateAndRecord(direction string, connection int, frame []byte) (bool, error) {
	metadata, err := parseMQTTFrameMetadata(frame)
	if err != nil {
		return false, err
	}
	action := "forward"
	s.mu.Lock()
	defer s.mu.Unlock()
	if direction == "broker_to_client" && metadata.PacketType == s.dropPacketType {
		if s.expectedID == 0 {
			return false, errors.New("matching acknowledgment arrived before publish identifier")
		}
		if metadata.PacketID != s.expectedID {
			return false, fmt.Errorf("acknowledgment packet identifier %d does not match expected %d", metadata.PacketID, s.expectedID)
		}
		if !s.dropped {
			s.dropped = true
			action = "drop_and_close"
		} else if s.dropQoS == 1 {
			s.completionSeen = true
		}
	}
	if direction == "broker_to_client" && s.dropQoS == 2 && metadata.PacketType == packetTypePubComp && s.expectedID != 0 {
		if metadata.PacketID != s.expectedID {
			return false, fmt.Errorf("PUBCOMP packet identifier %d does not match expected %d", metadata.PacketID, s.expectedID)
		}
		if s.dropped {
			s.completionSeen = true
		}
	}
	if direction == "client_to_broker" && metadata.PacketType == packetTypePublish && metadata.QoS == s.dropQoS {
		if s.expectedID == 0 {
			s.expectedID = metadata.PacketID
			s.firstSession = connection
		} else if connection > s.firstSession {
			if metadata.PacketID != s.expectedID {
				return false, fmt.Errorf("resent publish packet identifier %d does not match expected %d", metadata.PacketID, s.expectedID)
			}
			if !metadata.DUP {
				return false, errors.New("resent publish did not set DUP")
			}
			s.resentObserved = true
		}
	}
	s.trace = append(s.trace, proxyTraceEvent{
		At: time.Now().UTC().Format(time.RFC3339Nano), Direction: direction, Action: action,
		Connection: connection, Metadata: metadata,
	})
	return action == "drop_and_close", nil
}

func parseMQTTFrameMetadata(frame []byte) (MQTTFrameMetadata, error) {
	if len(frame) < 2 {
		return MQTTFrameMetadata{}, errors.New("truncated MQTT frame")
	}
	first := frame[0]
	packetType := first >> 4
	if packetType == 0 || packetType > 14 {
		return MQTTFrameMetadata{}, fmt.Errorf("invalid MQTT packet type %d", packetType)
	}
	remaining, consumed, err := decodeRemainingLength(frame[1:])
	if err != nil {
		return MQTTFrameMetadata{}, err
	}
	if remaining > maxProxyRemainingLength {
		return MQTTFrameMetadata{}, errors.New("MQTT frame exceeds proxy limit")
	}
	if 1+consumed+remaining != len(frame) {
		return MQTTFrameMetadata{}, errors.New("MQTT frame length mismatch")
	}
	metadata := MQTTFrameMetadata{PacketType: packetType, DUP: first&0x08 != 0, QoS: (first >> 1) & 0x03}
	if packetType == packetTypePublish {
		if metadata.QoS == 3 {
			return MQTTFrameMetadata{}, errors.New("PUBLISH has reserved QoS")
		}
		body := frame[1+consumed:]
		if len(body) < 2 {
			return MQTTFrameMetadata{}, errors.New("PUBLISH topic length is truncated")
		}
		topicLength := int(binary.BigEndian.Uint16(body[:2]))
		if len(body) < 2+topicLength {
			return MQTTFrameMetadata{}, errors.New("PUBLISH topic is truncated")
		}
		offset := 2 + topicLength
		if metadata.QoS > 0 {
			if len(body) < offset+2 {
				return MQTTFrameMetadata{}, errors.New("PUBLISH packet identifier is truncated")
			}
			metadata.PacketID = binary.BigEndian.Uint16(body[offset : offset+2])
			if metadata.PacketID == 0 {
				return MQTTFrameMetadata{}, errors.New("PUBLISH packet identifier is zero")
			}
		}
	} else if packetType == packetTypePubAck || packetType == packetTypePubRec || packetType == packetTypePubComp {
		body := frame[1+consumed:]
		if len(body) < 2 {
			return MQTTFrameMetadata{}, errors.New("acknowledgment packet identifier is truncated")
		}
		metadata.PacketID = binary.BigEndian.Uint16(body[:2])
		if metadata.PacketID == 0 {
			return MQTTFrameMetadata{}, errors.New("acknowledgment packet identifier is zero")
		}
	}
	return metadata, nil
}

func decodeRemainingLength(data []byte) (int, int, error) {
	value := 0
	multiplier := 1
	for index := 0; index < 4; index++ {
		if index >= len(data) {
			return 0, 0, errors.New("truncated MQTT remaining length")
		}
		part := data[index]
		value += int(part&0x7f) * multiplier
		if value > maxProxyRemainingLength {
			return 0, 0, errors.New("MQTT remaining length exceeds proxy limit")
		}
		if part&0x80 == 0 {
			if index > 0 && part&0x7f == 0 && value < multiplier {
				return 0, 0, errors.New("non-canonical MQTT remaining length")
			}
			return value, index + 1, nil
		}
		multiplier *= 128
	}
	return 0, 0, errors.New("MQTT remaining length uses more than four bytes")
}

func readMQTTFrame(reader *bufio.Reader) ([]byte, error) {
	first, err := reader.ReadByte()
	if err != nil {
		return nil, err
	}
	lengthBytes := make([]byte, 0, 4)
	for len(lengthBytes) < 4 {
		value, readErr := reader.ReadByte()
		if readErr != nil {
			return nil, fmt.Errorf("read MQTT remaining length: %w", readErr)
		}
		lengthBytes = append(lengthBytes, value)
		if value&0x80 == 0 {
			break
		}
	}
	remaining, consumed, err := decodeRemainingLength(lengthBytes)
	if err != nil {
		return nil, err
	}
	if consumed != len(lengthBytes) {
		return nil, errors.New("non-canonical MQTT remaining length")
	}
	frame := make([]byte, 1+len(lengthBytes)+remaining)
	frame[0] = first
	copy(frame[1:], lengthBytes)
	if _, err := io.ReadFull(reader, frame[1+len(lengthBytes):]); err != nil {
		return nil, fmt.Errorf("read MQTT frame body: %w", err)
	}
	if _, err := parseMQTTFrameMetadata(frame); err != nil {
		return nil, err
	}
	return frame, nil
}

func writeProxyTrace(path string, events []proxyTraceEvent) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()
	encoder := json.NewEncoder(file)
	for _, event := range events {
		if err := encoder.Encode(event); err != nil {
			return err
		}
	}
	return file.Sync()
}

// RunACKDropProxy drops one matching PUBACK/PUBREC and verifies the DUP resend.
func RunACKDropProxy(ctx context.Context, listenAddress, brokerAddress, tracePath string, qos byte) error {
	if qos != 1 && qos != 2 {
		return errors.New("proxy QoS must be 1 or 2")
	}
	listener, err := net.Listen("tcp", listenAddress)
	if err != nil {
		return fmt.Errorf("listen for MQTT proxy: %w", err)
	}
	defer listener.Close()
	state := &proxyState{dropPacketType: packetTypePubAck, dropQoS: qos}
	if qos == 2 {
		state.dropPacketType = packetTypePubRec
	}
	for connectionNumber := 1; ; connectionNumber++ {
		if err := listener.(*net.TCPListener).SetDeadline(time.Now().Add(250 * time.Millisecond)); err != nil {
			return err
		}
		client, acceptErr := listener.Accept()
		if acceptErr != nil {
			if netErr, ok := acceptErr.(net.Error); ok && netErr.Timeout() {
				select {
				case <-ctx.Done():
					_ = writeProxyTrace(tracePath, state.traceSnapshot())
					return ctx.Err()
				default:
				}
				continue
			}
			if errors.Is(acceptErr, net.ErrClosed) {
				return acceptErr
			}
			return fmt.Errorf("accept proxy client: %w", acceptErr)
		}
		broker, dialErr := net.DialTimeout("tcp", brokerAddress, 2*time.Second)
		if dialErr != nil {
			_ = client.Close()
			return fmt.Errorf("connect proxy broker: %w", dialErr)
		}
		connectionErr := proxyConnection(ctx, client, broker, state, connectionNumber)
		_ = client.Close()
		_ = broker.Close()
		if connectionErr != nil && !errors.Is(connectionErr, errProxyIntentionalClose) {
			_ = writeProxyTrace(tracePath, state.traceSnapshot())
			return connectionErr
		}
		state.mu.Lock()
		complete := state.dropped && state.resentObserved && state.completionSeen
		state.mu.Unlock()
		if complete {
			if err := writeProxyTrace(tracePath, state.traceSnapshot()); err != nil {
				return err
			}
			return nil
		}
	}
}

var errProxyIntentionalClose = errors.New("proxy intentionally closed after dropped acknowledgment")

func proxyConnection(ctx context.Context, client, broker net.Conn, state *proxyState, connectionNumber int) error {
	type directionResult struct{ err error }
	results := make(chan directionResult, 2)
	forward := func(direction string, source, destination net.Conn) {
		reader := bufio.NewReader(source)
		for {
			select {
			case <-ctx.Done():
				results <- directionResult{err: ctx.Err()}
				return
			default:
			}
			frame, err := readMQTTFrame(reader)
			if err != nil {
				results <- directionResult{err: err}
				return
			}
			drop, err := state.validateAndRecord(direction, connectionNumber, frame)
			if err != nil {
				results <- directionResult{err: err}
				return
			}
			if drop {
				_ = client.Close()
				_ = broker.Close()
				results <- directionResult{err: errProxyIntentionalClose}
				return
			}
			if _, err := destination.Write(frame); err != nil {
				results <- directionResult{err: err}
				return
			}
		}
	}
	go forward("client_to_broker", client, broker)
	go forward("broker_to_client", broker, client)
	first := <-results
	_ = client.Close()
	_ = broker.Close()
	second := <-results
	if first.err == nil || errors.Is(first.err, io.EOF) || errors.Is(first.err, net.ErrClosed) {
		return second.err
	}
	return first.err
}
