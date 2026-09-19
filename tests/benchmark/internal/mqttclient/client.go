package mqttclient

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"narumq/internal/domain/mqtt"
)

const (
	MaxPacketSize           = 1 << 20
	DefaultHandshakeTimeout = 30 * time.Second
)

// ConnectOptions fixes the MQTT contract shared by benchmark runners.
type ConnectOptions struct {
	Address          string
	ClientID         string
	KeepAlive        uint16
	HandshakeTimeout time.Duration
}

// ConnectTiming records the wall-clock points used by a connection attempt.
type ConnectTiming struct {
	StartedAt time.Time
	ConnAckAt time.Time
	Latency   time.Duration
}

// Client owns one MQTT TCP connection and its packet codecs.
type Client struct {
	connection       net.Conn
	encoder          *mqtt.Encoder
	decoder          *mqtt.Decoder
	handshakeTimeout time.Duration
	writeMu          sync.Mutex
	readMu           sync.Mutex
	closeOnce        sync.Once
	closeErr         error
}

// Connect opens a MQTT 5, Clean Start connection with zero session expiry.
func Connect(ctx context.Context, options ConnectOptions) (*Client, ConnectTiming, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if options.Address == "" {
		return nil, ConnectTiming{}, errors.New("MQTT address is required")
	}
	if options.ClientID == "" {
		return nil, ConnectTiming{}, errors.New("MQTT client ID is required")
	}
	handshakeTimeout := options.HandshakeTimeout
	if handshakeTimeout <= 0 {
		handshakeTimeout = DefaultHandshakeTimeout
	}
	encoder, err := mqtt.NewEncoder(MaxPacketSize)
	if err != nil {
		return nil, ConnectTiming{}, fmt.Errorf("create MQTT encoder: %w", err)
	}
	decoder, err := mqtt.NewDecoder(MaxPacketSize)
	if err != nil {
		return nil, ConnectTiming{}, fmt.Errorf("create MQTT decoder: %w", err)
	}
	startedAt := time.Now()
	dialer := net.Dialer{Timeout: handshakeTimeout}
	connection, err := dialer.DialContext(ctx, "tcp", options.Address)
	if err != nil {
		return nil, ConnectTiming{}, err
	}
	client := &Client{connection: connection, encoder: encoder, decoder: decoder, handshakeTimeout: handshakeTimeout}
	packet := &mqtt.Connect{
		Version:               mqtt.ProtocolV5,
		CleanStart:            true,
		SessionExpiryInterval: 0,
		KeepAlive:             options.KeepAlive,
		ClientID:              options.ClientID,
	}
	if err := client.Write(packet, handshakeTimeout); err != nil {
		_ = client.Close()
		return nil, ConnectTiming{}, err
	}
	connAck, err := client.readConnAck(handshakeTimeout)
	if err != nil {
		_ = client.Close()
		return nil, ConnectTiming{}, err
	}
	if connAck.Version != mqtt.ProtocolV5 || connAck.SessionPresent || connAck.ReasonCode != 0 {
		_ = client.Close()
		return nil, ConnectTiming{}, fmt.Errorf("unexpected CONNACK %#v", connAck)
	}
	connAckAt := time.Now()
	return client, ConnectTiming{StartedAt: startedAt, ConnAckAt: connAckAt, Latency: connAckAt.Sub(startedAt)}, nil
}

// Write encodes and writes one complete MQTT packet.
func (client *Client) Write(packet mqtt.Packet, timeout time.Duration) error {
	if client == nil || client.connection == nil {
		return errors.New("MQTT client is closed")
	}
	if timeout <= 0 {
		timeout = client.handshakeTimeout
	}
	wire, err := client.encoder.Append(nil, packet)
	if err != nil {
		return err
	}
	client.writeMu.Lock()
	defer client.writeMu.Unlock()
	if err := client.connection.SetWriteDeadline(time.Now().Add(timeout)); err != nil {
		return err
	}
	for len(wire) > 0 {
		written, err := client.connection.Write(wire)
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

// Read reads one complete MQTT packet. A first-byte timeout is returned as a
// net.Error; a timeout after a frame has started is wrapped as a fatal error.
func (client *Client) Read(timeout time.Duration) (mqtt.Packet, error) {
	if client == nil || client.connection == nil {
		return nil, errors.New("MQTT client is closed")
	}
	if timeout <= 0 {
		timeout = client.handshakeTimeout
	}
	client.readMu.Lock()
	defer client.readMu.Unlock()
	return client.readPacket(timeout)
}

// Disconnect sends a normal MQTT 5 DISCONNECT packet.
func (client *Client) Disconnect(timeout time.Duration) error {
	return client.Write(&mqtt.Disconnect{Version: mqtt.ProtocolV5}, timeout)
}

// Close closes the underlying TCP connection and is safe to call repeatedly.
func (client *Client) Close() error {
	if client == nil || client.connection == nil {
		return nil
	}
	client.closeOnce.Do(func() { client.closeErr = client.connection.Close() })
	return client.closeErr
}

// IsTimeout reports only a first-byte read timeout.
func IsTimeout(err error) bool {
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

type wireFrame struct {
	header byte
	body   []byte
	wire   []byte
}

func (client *Client) readPacket(timeout time.Duration) (mqtt.Packet, error) {
	frame, err := readWireFrame(client.connection, timeout, client.handshakeTimeout)
	if err != nil {
		return nil, err
	}
	packet, consumed, err := client.decoder.Decode(frame.wire, mqtt.ProtocolV5)
	if err != nil {
		return nil, err
	}
	if consumed != len(frame.wire) {
		return nil, mqtt.ErrMalformedPacket
	}
	return packet, nil
}

func (client *Client) readConnAck(timeout time.Duration) (*mqtt.ConnAck, error) {
	frame, err := readWireFrame(client.connection, timeout, client.handshakeTimeout)
	if err != nil {
		return nil, err
	}
	if frame.header != 0x20 || len(frame.body) < 2 {
		return nil, mqtt.ErrMalformedPacket
	}
	if frame.body[0]&0xFE != 0 {
		return nil, mqtt.ErrMalformedPacket
	}
	if len(frame.body) < 3 {
		return nil, mqtt.ErrMalformedPacket
	}
	propertyLength, consumed, err := decodeWireVariableByteInteger(frame.body[2:])
	if err != nil || 2+consumed+propertyLength != len(frame.body) {
		return nil, mqtt.ErrMalformedPacket
	}
	return &mqtt.ConnAck{
		Version:        mqtt.ProtocolV5,
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
	terminated := false
	for index := 0; index < 4; index++ {
		var encoded [1]byte
		if _, err := io.ReadFull(connection, encoded[:]); err != nil {
			if IsTimeout(err) {
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
			terminated = true
			break
		}
		if index == 3 {
			return wireFrame{}, mqtt.ErrMalformedPacket
		}
		multiplier *= 128
	}
	if !terminated {
		return wireFrame{}, mqtt.ErrMalformedPacket
	}
	if remainingLength > MaxPacketSize {
		return wireFrame{}, mqtt.ErrPacketTooLarge
	}
	body := make([]byte, remainingLength)
	if _, err := io.ReadFull(connection, body); err != nil {
		if IsTimeout(err) {
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
