package mqttclient

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
	"unicode/utf8"

	"narumq/internal/domain/mqtt"
)

const (
	MaxPacketSize           = 1 << 20
	DefaultHandshakeTimeout = 30 * time.Second
)

// ConnectOptions configures an MQTT CONNECT with caller-selected protocol, session, credentials, and Will.
type ConnectOptions struct {
	Address  string
	ClientID string
	// ProtocolVersion selects MQTT 3.1.1 or 5.0; zero defaults to MQTT 5.0.
	ProtocolVersion       mqtt.ProtocolVersion
	KeepAlive             uint16
	HandshakeTimeout      time.Duration
	CleanStart            bool
	SessionExpiryInterval uint32
	Username              string
	Password              []byte
	Will                  *mqtt.Will
	// TLSConfig enables MQTT over TLS when non-nil; nil keeps plain TCP.
	TLSConfig *tls.Config
}

// ConnectTiming records the wall-clock points used by a connection attempt.
type ConnectTiming struct {
	StartedAt      time.Time
	ConnAckAt      time.Time
	Latency        time.Duration
	SessionPresent bool
}

// Client owns one MQTT TCP or TLS connection and its packet codecs.
type Client struct {
	connection       net.Conn
	encoder          *mqtt.Encoder
	decoder          *mqtt.Decoder
	protocolVersion  mqtt.ProtocolVersion
	handshakeTimeout time.Duration
	writeMu          sync.Mutex
	readMu           sync.Mutex
	closeOnce        sync.Once
	closeErr         error
}

// Connect performs an MQTT CONNECT with caller-selected protocol, session, credentials, and Will.
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
	protocolVersion := options.ProtocolVersion
	if protocolVersion == mqtt.ProtocolUnknown {
		protocolVersion = mqtt.ProtocolV5
	}
	if protocolVersion != mqtt.ProtocolV311 && protocolVersion != mqtt.ProtocolV5 {
		return nil, ConnectTiming{}, mqtt.ErrUnsupportedProtocolVersion
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
	var connection net.Conn
	if options.TLSConfig != nil {
		tlsDialer := tls.Dialer{NetDialer: &dialer, Config: options.TLSConfig}
		connection, err = tlsDialer.DialContext(ctx, "tcp", options.Address)
	} else {
		connection, err = dialer.DialContext(ctx, "tcp", options.Address)
	}
	if err != nil {
		return nil, ConnectTiming{}, err
	}
	client := &Client{
		connection:       connection,
		encoder:          encoder,
		decoder:          decoder,
		protocolVersion:  protocolVersion,
		handshakeTimeout: handshakeTimeout,
	}
	packet := &mqtt.Connect{
		Version:               protocolVersion,
		CleanStart:            options.CleanStart,
		SessionExpiryInterval: options.SessionExpiryInterval,
		KeepAlive:             options.KeepAlive,
		ClientID:              options.ClientID,
		HasUsername:           options.Username != "",
		Username:              options.Username,
		HasPassword:           len(options.Password) > 0,
		Password:              options.Password,
		Will:                  options.Will,
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
	if connAck.Version != protocolVersion || (options.CleanStart && connAck.SessionPresent) || connAck.ReasonCode != 0 {
		_ = client.Close()
		return nil, ConnectTiming{}, fmt.Errorf("unexpected CONNACK %#v", connAck)
	}
	connAckAt := time.Now()
	return client, ConnectTiming{
		StartedAt:      startedAt,
		ConnAckAt:      connAckAt,
		Latency:        connAckAt.Sub(startedAt),
		SessionPresent: connAck.SessionPresent,
	}, nil
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

// Disconnect sends a normal DISCONNECT packet for the negotiated protocol.
func (client *Client) Disconnect(timeout time.Duration) error {
	if client == nil {
		return errors.New("MQTT client is closed")
	}
	return client.Write(&mqtt.Disconnect{Version: client.protocolVersion}, timeout)
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
	if client.protocolVersion == mqtt.ProtocolV5 && frame.header == 0xE0 {
		return client.readServerDisconnect(frame)
	}
	packet, consumed, err := client.decoder.Decode(frame.wire, client.protocolVersion)
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
	if frame.header != 0x20 || len(frame.body) < 2 || frame.body[0]&0xFE != 0 {
		return nil, mqtt.ErrMalformedPacket
	}
	if client.protocolVersion == mqtt.ProtocolV311 {
		if len(frame.body) != 2 {
			return nil, mqtt.ErrMalformedPacket
		}
		return &mqtt.ConnAck{
			Version:        mqtt.ProtocolV311,
			SessionPresent: frame.body[0]&0x01 != 0,
			ReasonCode:     frame.body[1],
		}, nil
	}
	if client.protocolVersion != mqtt.ProtocolV5 {
		return nil, mqtt.ErrUnsupportedProtocolVersion
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

func (client *Client) readServerDisconnect(frame wireFrame) (mqtt.Packet, error) {
	if len(frame.body) == 0 {
		packet, _, err := client.decoder.Decode(frame.wire, mqtt.ProtocolV5)
		return packet, err
	}
	reasonFrame := [3]byte{0xE0, 0x01, frame.body[0]}
	if _, _, err := client.decoder.Decode(reasonFrame[:], mqtt.ProtocolV5); err != nil {
		return nil, err
	}
	if len(frame.body) == 1 {
		packet, _, err := client.decoder.Decode(frame.wire, mqtt.ProtocolV5)
		return packet, err
	}
	propertyLength, consumed, err := decodeWireVariableByteInteger(frame.body[1:])
	if err != nil {
		return nil, mqtt.ErrMalformedPacket
	}
	propertiesStart := 1 + consumed
	if propertyLength != len(frame.body)-propertiesStart {
		return nil, mqtt.ErrMalformedPacket
	}
	if propertyLength == 0 {
		packet, _, err := client.decoder.Decode(frame.wire, mqtt.ProtocolV5)
		return packet, err
	}

	properties := frame.body[propertiesStart:]
	position := 0
	serverReference := ""
	hasSessionExpiry := false
	hasReasonString := false
	hasServerReference := false
	for position < len(properties) {
		property := properties[position]
		position++
		switch property {
		case 0x11: // Session Expiry Interval
			if hasSessionExpiry || len(properties)-position < 4 {
				return nil, mqtt.ErrMalformedPacket
			}
			hasSessionExpiry = true
			position += 4
		case 0x1C: // Server Reference
			if hasServerReference {
				return nil, mqtt.ErrMalformedPacket
			}
			value, err := readServerDisconnectUTF8Field(properties, &position)
			if err != nil {
				return nil, err
			}
			serverReference = string(value)
			hasServerReference = true
		case 0x1F: // Reason String
			if hasReasonString {
				return nil, mqtt.ErrMalformedPacket
			}
			if _, err := readServerDisconnectUTF8Field(properties, &position); err != nil {
				return nil, err
			}
			hasReasonString = true
		case 0x26: // User Property
			if _, err := readServerDisconnectUTF8Field(properties, &position); err != nil {
				return nil, err
			}
			if _, err := readServerDisconnectUTF8Field(properties, &position); err != nil {
				return nil, err
			}
		default:
			return nil, mqtt.ErrMalformedPacket
		}
	}
	return &mqtt.Disconnect{
		Version:         mqtt.ProtocolV5,
		ReasonCode:      frame.body[0],
		ServerReference: serverReference,
	}, nil
}

func readServerDisconnectUTF8Field(body []byte, position *int) ([]byte, error) {
	if len(body)-*position < 2 {
		return nil, mqtt.ErrMalformedPacket
	}
	length := int(binary.BigEndian.Uint16(body[*position : *position+2]))
	*position += 2
	if length > len(body)-*position {
		return nil, mqtt.ErrMalformedPacket
	}
	value := body[*position : *position+length]
	*position += length
	if !validServerDisconnectUTF8(value) {
		return nil, mqtt.ErrMalformedPacket
	}
	return value, nil
}

func validServerDisconnectUTF8(value []byte) bool {
	if !utf8.Valid(value) {
		return false
	}
	for len(value) > 0 {
		codePoint, size := utf8.DecodeRune(value)
		if codePoint == 0 || (codePoint >= 0xD800 && codePoint <= 0xDFFF) ||
			(codePoint >= 0xFDD0 && codePoint <= 0xFDEF) ||
			codePoint&0xFFFF == 0xFFFE || codePoint&0xFFFF == 0xFFFF {
			return false
		}
		value = value[size:]
	}
	return true
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
