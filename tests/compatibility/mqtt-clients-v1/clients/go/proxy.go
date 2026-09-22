package main

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"time"
)

const proxyMaxRemainingLength = 1 << 20

const (
	proxyPublish = 3
	proxyPubAck  = 4
	proxyPubRec  = 5
	proxyPubComp = 7
)

type proxyMetadata struct {
	PacketType byte   `json:"packetType"`
	DUP        bool   `json:"dup"`
	QoS        byte   `json:"qos"`
	PacketID   uint16 `json:"packetId,omitempty"`
}

type proxyTrace struct {
	At         string        `json:"at"`
	Direction  string        `json:"direction"`
	Action     string        `json:"action"`
	Connection int           `json:"connection"`
	Metadata   proxyMetadata `json:"metadata"`
}

type proxyState struct {
	mu             sync.Mutex
	traceMu        sync.Mutex
	dropPacketType byte
	dropQoS        byte
	expectedID     uint16
	dropped        bool
	firstSession   int
	trace          []proxyTrace
}

func runProxyCLI(args []string) error {
	flags := flag.NewFlagSet("mqtt-compatibility-proxy", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	listen := flags.String("listen", "", "proxy listen address")
	broker := flags.String("broker", "", "broker address")
	qos := flags.Int("qos", 0, "QoS whose acknowledgment is dropped")
	tracePath := flags.String("trace", "", "JSONL trace path")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *listen == "" || *broker == "" || *tracePath == "" || (*qos != 1 && *qos != 2) || flags.NArg() != 0 {
		return errors.New("--listen, --broker, --qos (1 or 2), and --trace are required")
	}
	listener, err := net.Listen("tcp", *listen)
	if err != nil {
		return err
	}
	defer listener.Close()
	state := &proxyState{dropQoS: byte(*qos), dropPacketType: proxyPubAck}
	if *qos == 2 {
		state.dropPacketType = proxyPubRec
	}
	for connection := 1; ; connection++ {
		client, acceptErr := listener.Accept()
		if acceptErr != nil {
			return acceptErr
		}
		go func(number int, client net.Conn) {
			if err := proxyConnection(client, *broker, number, state, *tracePath); err != nil {
				_, _ = fmt.Fprintln(os.Stderr, err)
			}
		}(connection, client)
	}
}

func proxyConnection(client net.Conn, brokerAddress string, connection int, state *proxyState, tracePath string) error {
	defer client.Close()
	broker, err := net.DialTimeout("tcp", brokerAddress, 5*time.Second)
	if err != nil {
		return err
	}
	defer broker.Close()
	defer func() { _ = writeProxyTrace(tracePath, state) }()
	errs := make(chan error, 2)
	go proxyDirection(client, broker, "client_to_broker", connection, state, errs, tracePath)
	go proxyDirection(broker, client, "broker_to_client", connection, state, errs, tracePath)
	return <-errs
}
func proxyDirection(source net.Conn, destination net.Conn, direction string, connection int, state *proxyState, errs chan<- error, tracePath string) {
	reader := bufio.NewReader(source)
	for {
		frame, err := readProxyFrame(reader)
		if err != nil {
			errs <- err
			return
		}
		metadata, err := parseProxyMetadata(frame)
		if err != nil {
			errs <- err
			return
		}
		action, drop, err := state.record(direction, connection, metadata)
		if err != nil {
			errs <- err
			return
		}
		state.appendTrace(proxyTrace{At: time.Now().UTC().Format(time.RFC3339Nano), Direction: direction, Action: action, Connection: connection, Metadata: metadata})
		if err := writeProxyTrace(tracePath, state); err != nil {
			errs <- err
			return
		}
		if drop {
			_ = source.Close()
			_ = destination.Close()
			errs <- errors.New("dropped matching acknowledgment")
			return
		}
		if _, err := destination.Write(frame); err != nil {
			errs <- err
			return
		}
	}
}

func (state *proxyState) record(direction string, connection int, metadata proxyMetadata) (string, bool, error) {
	state.mu.Lock()
	defer state.mu.Unlock()
	action := "forward"
	if direction == "client_to_broker" && metadata.PacketType == proxyPublish && metadata.QoS == state.dropQoS {
		if state.expectedID == 0 {
			state.expectedID = metadata.PacketID
			state.firstSession = connection
		} else if connection > state.firstSession {
			if metadata.PacketID != state.expectedID || !metadata.DUP {
				return "", false, errors.New("retransmitted PUBLISH did not preserve packet identifier and DUP")
			}
		}
	}
	if direction == "broker_to_client" && metadata.PacketType == state.dropPacketType {
		if metadata.PacketID != state.expectedID || state.expectedID == 0 {
			return "", false, errors.New("acknowledgment packet identifier did not match PUBLISH")
		}
		if !state.dropped {
			state.dropped = true
			action = "drop_and_close"
			return action, true, nil
		}
	}
	return action, false, nil
}

func (state *proxyState) appendTrace(event proxyTrace) {
	state.mu.Lock()
	state.trace = append(state.trace, event)
	state.mu.Unlock()
}

func readProxyFrame(reader *bufio.Reader) ([]byte, error) {
	first, err := reader.ReadByte()
	if err != nil {
		return nil, err
	}
	length := make([]byte, 0, 4)
	for len(length) < 4 {
		value, readErr := reader.ReadByte()
		if readErr != nil {
			return nil, readErr
		}
		length = append(length, value)
		if value&0x80 == 0 {
			break
		}
	}
	remaining, consumed, err := decodeProxyRemainingLength(length)
	if err != nil || consumed != len(length) {
		if err == nil {
			err = errors.New("non-canonical remaining length")
		}
		return nil, err
	}
	frame := make([]byte, 1+len(length)+remaining)
	frame[0] = first
	copy(frame[1:], length)
	if _, err := io.ReadFull(reader, frame[1+len(length):]); err != nil {
		return nil, err
	}
	return frame, nil
}

func parseProxyMetadata(frame []byte) (proxyMetadata, error) {
	if len(frame) < 2 {
		return proxyMetadata{}, errors.New("truncated MQTT frame")
	}
	remaining, consumed, err := decodeProxyRemainingLength(frame[1:])
	if err != nil || 1+consumed+remaining != len(frame) {
		if err == nil {
			err = errors.New("MQTT frame length mismatch")
		}
		return proxyMetadata{}, err
	}
	metadata := proxyMetadata{PacketType: frame[0] >> 4, DUP: frame[0]&0x08 != 0, QoS: (frame[0] >> 1) & 0x03}
	if metadata.PacketType == proxyPublish {
		body := frame[1+consumed:]
		if len(body) < 2 {
			return proxyMetadata{}, errors.New("truncated PUBLISH topic")
		}
		topicLength := int(binary.BigEndian.Uint16(body[:2]))
		offset := 2 + topicLength
		if len(body) < offset || metadata.QoS == 3 {
			return proxyMetadata{}, errors.New("invalid PUBLISH")
		}
		if metadata.QoS > 0 {
			if len(body) < offset+2 {
				return proxyMetadata{}, errors.New("truncated PUBLISH packet identifier")
			}
			metadata.PacketID = binary.BigEndian.Uint16(body[offset : offset+2])
			if metadata.PacketID == 0 {
				return proxyMetadata{}, errors.New("zero PUBLISH packet identifier")
			}
		}
	} else if metadata.PacketType == proxyPubAck || metadata.PacketType == proxyPubRec || metadata.PacketType == proxyPubComp {
		body := frame[1+consumed:]
		if len(body) < 2 {
			return proxyMetadata{}, errors.New("truncated acknowledgment")
		}
		metadata.PacketID = binary.BigEndian.Uint16(body[:2])
		if metadata.PacketID == 0 {
			return proxyMetadata{}, errors.New("zero acknowledgment packet identifier")
		}
	}
	return metadata, nil
}

func decodeProxyRemainingLength(data []byte) (int, int, error) {
	value := 0
	multiplier := 1
	for index := 0; index < 4; index++ {
		if index >= len(data) {
			return 0, 0, errors.New("truncated remaining length")
		}
		part := data[index]
		value += int(part&0x7f) * multiplier
		if value > proxyMaxRemainingLength {
			return 0, 0, errors.New("remaining length exceeds proxy limit")
		}
		if part&0x80 == 0 {
			if index > 0 && part&0x7f == 0 && value < multiplier {
				return 0, 0, errors.New("non-canonical remaining length")
			}
			return value, index + 1, nil
		}
		multiplier *= 128
	}
	return 0, 0, errors.New("remaining length uses more than four bytes")
}

func writeProxyTrace(path string, state *proxyState) error {
	state.mu.Lock()
	events := append([]proxyTrace(nil), state.trace...)
	state.mu.Unlock()
	state.traceMu.Lock()
	defer state.traceMu.Unlock()
	tempPath := path + ".tmp"
	file, err := os.Create(tempPath)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(file)
	for _, event := range events {
		if err := encoder.Encode(event); err != nil {
			_ = file.Close()
			_ = os.Remove(tempPath)
			return err
		}
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		_ = os.Remove(tempPath)
		return err
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(tempPath)
		return err
	}
	if err := os.Rename(tempPath, path); err != nil {
		_ = os.Remove(tempPath)
		return err
	}
	return nil
}
