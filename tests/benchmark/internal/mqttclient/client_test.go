package mqttclient

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"math/big"
	"net"
	"testing"
	"time"

	"narumq/internal/domain/mqtt"
)

// TestConnectOverTLSVerifiesServerCertificate verifies the TLS client verifies the server certificate.
// Scenarios: performance-and-verification.md:PRF-SCN-042
func TestConnectOverTLSVerifiesServerCertificate(t *testing.T) {
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	caTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(1), NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	ca, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatal(err)
	}
	serverKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	serverTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2), NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour),
		DNSNames: []string{"broker.test"}, KeyUsage: x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	serverDER, err := x509.CreateCertificate(rand.Reader, serverTemplate, ca, &serverKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{{Certificate: [][]byte{serverDER, caDER}, PrivateKey: serverKey}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	serverErr := make(chan error, 1)
	go func() {
		for attempt := range 2 {
			connection, err := listener.Accept()
			if err != nil {
				serverErr <- err
				return
			}
			frame, readErr := readWireFrame(connection, time.Second, time.Second)
			if attempt == 0 {
				if readErr == nil && frame.header != 0x10 {
					readErr = fmt.Errorf("expected CONNECT, received %#x", frame.header)
				}
				if readErr == nil {
					_, readErr = connection.Write([]byte{0x20, 0x03, 0x00, 0x00, 0x00})
				}
			} else if readErr == nil {
				readErr = errors.New("untrusted client unexpectedly completed TLS handshake")
			} else {
				readErr = nil
			}
			_ = connection.Close()
			if readErr != nil {
				serverErr <- readErr
				return
			}
		}
		serverErr <- nil
	}()
	roots := x509.NewCertPool()
	roots.AddCert(ca)
	options := ConnectOptions{
		Address: listener.Addr().String(), ClientID: "tls-client", CleanStart: true,
		HandshakeTimeout: time.Second,
		TLSConfig:        &tls.Config{RootCAs: roots, ServerName: "broker.test", MinVersion: tls.VersionTLS12},
	}
	client, timing, err := Connect(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	_ = client.Close()
	if timing.Latency <= 0 {
		t.Fatalf("connect latency = %s, want positive TLS-inclusive latency", timing.Latency)
	}
	options.TLSConfig = &tls.Config{RootCAs: x509.NewCertPool(), ServerName: "broker.test", MinVersion: tls.VersionTLS12}
	_, _, err = Connect(context.Background(), options)
	var unknownAuthority x509.UnknownAuthorityError
	if !errors.As(err, &unknownAuthority) {
		t.Fatalf("untrusted CA error = %v, want x509.UnknownAuthorityError", err)
	}
	if err := <-serverErr; err != nil {
		t.Fatal(err)
	}
}

func TestConnectResumesSessionWithSelectedOptions(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	serverErr := make(chan error, 1)
	go func() {
		connection, err := listener.Accept()
		if err != nil {
			serverErr <- err
			return
		}
		defer connection.Close()
		frame, err := readWireFrame(connection, time.Second, time.Second)
		if err != nil {
			serverErr <- err
			return
		}
		decoder, err := mqtt.NewDecoder(MaxPacketSize)
		if err != nil {
			serverErr <- err
			return
		}
		packet, consumed, err := decoder.Decode(frame.wire, mqtt.ProtocolV5)
		if err != nil {
			serverErr <- err
			return
		}
		connect, ok := packet.(*mqtt.Connect)
		if !ok || consumed != len(frame.wire) {
			serverErr <- errors.New("server did not receive a complete CONNECT packet")
			return
		}
		if len(frame.body) < 8 || frame.body[7] != 0xC0 {
			var flags byte
			if len(frame.body) >= 8 {
				flags = frame.body[7]
			}
			serverErr <- fmt.Errorf("CONNECT flags = %#x, want %#x", flags, 0xC0)
			return
		}
		if connect.CleanStart || connect.SessionExpiryInterval != 300 || !connect.HasUsername || connect.Username != "jwt" ||
			!connect.HasPassword || string(connect.Password) != "token" {
			serverErr <- fmt.Errorf("decoded CONNECT = %#v, want selected session and credentials", connect)
			return
		}
		_, err = connection.Write([]byte{0x20, 0x03, 0x01, 0x00, 0x00})
		serverErr <- err
	}()

	client, timing, err := Connect(context.Background(), ConnectOptions{
		Address:               listener.Addr().String(),
		ClientID:              "resume-client",
		HandshakeTimeout:      time.Second,
		CleanStart:            false,
		SessionExpiryInterval: 300,
		Username:              "jwt",
		Password:              []byte("token"),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if !timing.SessionPresent {
		t.Fatal("ConnectTiming.SessionPresent = false, want true")
	}
	if err := <-serverErr; err != nil {
		t.Fatal(err)
	}
}

func TestConnectV311WithWillReadsTwoByteConnAck(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	serverErr := make(chan error, 1)
	go func() {
		connection, err := listener.Accept()
		if err != nil {
			serverErr <- err
			return
		}
		defer connection.Close()
		frame, err := readWireFrame(connection, time.Second, time.Second)
		if err != nil {
			serverErr <- err
			return
		}
		decoder, err := mqtt.NewDecoder(MaxPacketSize)
		if err != nil {
			serverErr <- err
			return
		}
		packet, consumed, err := decoder.Decode(frame.wire, mqtt.ProtocolV311)
		if err != nil {
			serverErr <- err
			return
		}
		connect, ok := packet.(*mqtt.Connect)
		if !ok || consumed != len(frame.wire) || connect.Version != mqtt.ProtocolV311 ||
			connect.CleanStart || connect.Will == nil || connect.Will.QoS != byte(mqtt.QoSAtLeastOnce) ||
			connect.Will.Retain || connect.Will.Topic != "bp07/will/v311" || string(connect.Will.Payload) != "will" {
			serverErr <- fmt.Errorf("decoded CONNECT = %#v, want MQTT 3.1.1 persistent Will CONNECT", packet)
			return
		}
		if _, err := connection.Write([]byte{0x20, 0x02, 0x00, 0x00}); err != nil {
			serverErr <- err
			return
		}
		frame, err = readWireFrame(connection, time.Second, time.Second)
		if err != nil {
			serverErr <- err
			return
		}
		if frame.header != 0xE0 || len(frame.body) != 0 {
			serverErr <- fmt.Errorf("DISCONNECT header/body = %#x/%x, want MQTT 3.1.1 empty body", frame.header, frame.body)
			return
		}
		serverErr <- nil
	}()

	client, _, err := Connect(context.Background(), ConnectOptions{
		Address:          listener.Addr().String(),
		ClientID:         "will-v311-client",
		ProtocolVersion:  mqtt.ProtocolV311,
		HandshakeTimeout: time.Second,
		Will: &mqtt.Will{
			QoS:     byte(mqtt.QoSAtLeastOnce),
			Topic:   "bp07/will/v311",
			Payload: []byte("will"),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if client.protocolVersion != mqtt.ProtocolV311 {
		t.Fatalf("client protocol version = %d, want MQTT 3.1.1", client.protocolVersion)
	}
	if err := client.Disconnect(time.Second); err != nil {
		t.Fatal(err)
	}
	if err := <-serverErr; err != nil {
		t.Fatal(err)
	}
}

func TestConnectRejectsSessionPresentForCleanStart(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	serverErr := make(chan error, 1)
	go func() {
		connection, err := listener.Accept()
		if err != nil {
			serverErr <- err
			return
		}
		defer connection.Close()
		if _, err := readWireFrame(connection, time.Second, time.Second); err != nil {
			serverErr <- err
			return
		}
		_, err = connection.Write([]byte{0x20, 0x03, 0x01, 0x00, 0x00})
		serverErr <- err
	}()

	client, _, err := Connect(context.Background(), ConnectOptions{
		Address:          listener.Addr().String(),
		ClientID:         "clean-client",
		HandshakeTimeout: time.Second,
		CleanStart:       true,
	})
	if err == nil {
		if client != nil {
			_ = client.Close()
		}
		t.Fatal("Connect() succeeded, want Session Present rejection")
	}
	if serverErr := <-serverErr; serverErr != nil {
		t.Fatal(serverErr)
	}
}

func TestReadConnAckAcceptsBrokerProperties(t *testing.T) {
	reader, writer := net.Pipe()
	defer reader.Close()
	defer writer.Close()
	wire := []byte{
		0x20, 0x08,
		0x00, 0x00,
		0x05, 0x21, 0x00, 0x0A, 0x24, 0x00,
	}
	go func() { _, _ = writer.Write(wire) }()
	decoder, err := mqtt.NewDecoder(MaxPacketSize)
	if err != nil {
		t.Fatal(err)
	}
	client := &Client{connection: reader, decoder: decoder, protocolVersion: mqtt.ProtocolV5, handshakeTimeout: time.Second}
	connAck, err := client.readConnAck(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if connAck.Version != mqtt.ProtocolV5 || connAck.SessionPresent || connAck.ReasonCode != 0 {
		t.Fatalf("readConnAck() = %#v, want successful MQTT 5 CONNACK", connAck)
	}
}

// TestReadKeepsBufferedBytesOfBackToBackFrames verifies back-to-back frames in one segment are each read intact.
// Scenarios: performance-and-verification.md:PRF-SCN-024
func TestReadKeepsBufferedBytesOfBackToBackFrames(t *testing.T) {
	reader, writer := net.Pipe()
	defer reader.Close()
	defer writer.Close()
	// Two PUBACK frames (packet IDs 1 and 2) arrive in one segment; the second must survive the first Read.
	go func() { _, _ = writer.Write([]byte{0x40, 0x02, 0x00, 0x01, 0x40, 0x02, 0x00, 0x02}) }()
	decoder, err := mqtt.NewDecoder(MaxPacketSize)
	if err != nil {
		t.Fatal(err)
	}
	client := &Client{connection: reader, reader: bufio.NewReader(reader), decoder: decoder, protocolVersion: mqtt.ProtocolV5, handshakeTimeout: time.Second}
	for want := mqtt.PacketIdentifier(1); want <= 2; want++ {
		packet, err := client.Read(time.Second)
		if err != nil {
			t.Fatalf("Read() for packet %d: %v", want, err)
		}
		if ack, ok := packet.(*mqtt.PubAck); !ok || ack.PacketID != want {
			t.Fatalf("Read() = %#v, want PUBACK %d", packet, want)
		}
	}
	if _, err := client.Read(20 * time.Millisecond); !IsTimeout(err) {
		t.Fatalf("Read() on an idle buffered connection = %v, want first-byte timeout", err)
	}
}

func TestReadServerDisconnectWithReference(t *testing.T) {
	serverReference := []byte("lb.bp07.test:1883")
	reasonString := []byte("server moved")
	properties := []byte{
		0x11, 0x00, 0x00, 0x00, 0x01,
		0x1C, 0x00, byte(len(serverReference)),
	}
	properties = append(properties, serverReference...)
	properties = append(properties, 0x1F, 0x00, byte(len(reasonString)))
	properties = append(properties, reasonString...)
	properties = append(properties, 0x26, 0x00, 0x04, 'z', 'o', 'n', 'e', 0x00, 0x04, 'b', 'l', 'u', 'e')
	body := []byte{0x9C, byte(len(properties))}
	body = append(body, properties...)
	validWire := []byte{0xE0, byte(len(body))}
	validWire = append(validWire, body...)

	tests := []struct {
		name          string
		wire          []byte
		wantReference string
		wantReason    byte
		wantError     error
	}{
		{
			name:          "reference with reason string",
			wire:          validWire,
			wantReference: string(serverReference),
			wantReason:    0x9C,
		},
		{
			name:      "duplicate server reference",
			wire:      []byte{0xE0, 0x0A, 0x9C, 0x08, 0x1C, 0x00, 0x01, 'a', 0x1C, 0x00, 0x01, 'b'},
			wantError: mqtt.ErrMalformedPacket,
		},
		{
			name:      "truncated server reference",
			wire:      []byte{0xE0, 0x06, 0x9C, 0x04, 0x1C, 0x00, 0x02, 'a'},
			wantError: mqtt.ErrMalformedPacket,
		},
		{
			name:      "unknown property",
			wire:      []byte{0xE0, 0x04, 0x9C, 0x02, 0x2B, 0x00},
			wantError: mqtt.ErrMalformedPacket,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()

			serverErr := make(chan error, 1)
			serverRelease := make(chan struct{})
			defer close(serverRelease)
			go func() {
				connection, err := listener.Accept()
				if err != nil {
					serverErr <- err
					return
				}
				defer connection.Close()
				if _, err := readWireFrame(connection, time.Second, time.Second); err != nil {
					serverErr <- err
					return
				}
				if _, err := connection.Write([]byte{0x20, 0x03, 0x00, 0x00, 0x00}); err != nil {
					serverErr <- err
					return
				}
				if _, err := connection.Write(test.wire); err != nil {
					serverErr <- err
					return
				}
				serverErr <- nil
				<-serverRelease
			}()

			client, _, err := Connect(context.Background(), ConnectOptions{
				Address:          listener.Addr().String(),
				ClientID:         "server-disconnect-client",
				HandshakeTimeout: time.Second,
			})
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			packet, err := client.Read(time.Second)
			if test.wantError != nil {
				if err != test.wantError {
					t.Fatalf("Read() error = %v, want %v", err, test.wantError)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				disconnect, ok := packet.(*mqtt.Disconnect)
				if !ok || disconnect.Version != mqtt.ProtocolV5 || disconnect.ReasonCode != test.wantReason ||
					disconnect.ServerReference != test.wantReference {
					t.Fatalf("Read() packet = %#v, want server reference %q and reason %#x", packet, test.wantReference, test.wantReason)
				}
			}
			if err := <-serverErr; err != nil {
				t.Fatal(err)
			}
		})
	}
}

// TestReadWireFrameMidFrameTimeoutIsFatal verifies a timeout in the middle of a frame is fatal rather than an idle timeout.
// Scenarios: performance-and-verification.md:PRF-SCN-024
func TestReadWireFrameMidFrameTimeoutIsFatal(t *testing.T) {
	reader, writer := net.Pipe()
	defer reader.Close()
	defer writer.Close()
	go func() { _, _ = writer.Write([]byte{0x30}) }()
	_, err := readWireFrame(reader, time.Second, 20*time.Millisecond)
	if err == nil || IsTimeout(err) {
		t.Fatalf("readWireFrame() error = %v, want fatal non-timeout mid-frame error", err)
	}
}

// TestReadWireFrameRejectsUnterminatedRemainingLength verifies an unterminated Remaining Length is a malformed packet.
// Scenarios: performance-and-verification.md:PRF-SCN-024
func TestReadWireFrameRejectsUnterminatedRemainingLength(t *testing.T) {
	reader, writer := net.Pipe()
	defer reader.Close()
	defer writer.Close()
	go func() { _, _ = writer.Write([]byte{0x30, 0x80, 0x80, 0x80, 0x80}) }()
	_, err := readWireFrame(reader, time.Second, time.Second)
	if err != mqtt.ErrMalformedPacket {
		t.Fatalf("readWireFrame() error = %v, want malformed packet", err)
	}
}
