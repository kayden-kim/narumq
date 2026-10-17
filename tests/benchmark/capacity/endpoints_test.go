package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"testing"
	"time"

	"narumq/internal/domain/mqtt"
	"narumq/tests/benchmark/internal/mqttclient"
)

// readEndpointConnect establishes that the peer received the entire CONNECT,
// rather than just accepting a TCP connection.
func readEndpointConnect(conn net.Conn) error {
	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return err
	}
	var header [1]byte
	if _, err := io.ReadFull(conn, header[:]); err != nil {
		return err
	}
	wire := []byte{header[0]}
	length, multiplier := 0, 1
	for range 4 {
		var value [1]byte
		if _, err := io.ReadFull(conn, value[:]); err != nil {
			return err
		}
		wire = append(wire, value[0])
		length += int(value[0]&0x7f) * multiplier
		if value[0]&0x80 == 0 {
			body := make([]byte, length)
			if _, err := io.ReadFull(conn, body); err != nil {
				return err
			}
			decoder, err := mqtt.NewDecoder(mqttclient.MaxPacketSize)
			if err != nil {
				return err
			}
			packet, _, err := decoder.Decode(append(wire, body...), mqtt.ProtocolV5)
			if err != nil {
				return err
			}
			if _, ok := packet.(*mqtt.Connect); !ok {
				return fmt.Errorf("expected CONNECT, got %T", packet)
			}
			return nil
		}
		multiplier *= 128
	}
	return mqtt.ErrMalformedPacket
}

// Scenarios: performance-and-verification.md:PRF-SCN-120
func TestEndpointsOnlyFallThroughBeforeConnect(t *testing.T) {
	for _, script := range []string{"dial-failure", "connack-timeout", "connack-refused", "partial-connect-reset"} {
		t.Run(script, func(t *testing.T) {
			listener, err := net.Listen("tcp4", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = listener.Close() })
			type observation struct {
				accepted int
				err      error
			}
			done := make(chan observation, 1)
			go func() {
				var observed observation
				defer func() { done <- observed }()
				for {
					conn, err := listener.Accept()
					if err != nil {
						if !errors.Is(err, net.ErrClosed) {
							observed.err = err
						}
						return
					}
					observed.accepted++
					if script == "partial-connect-reset" && observed.accepted == 1 {
						_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
						var first [1]byte
						_, observed.err = io.ReadFull(conn, first[:])
						if observed.err == nil && first[0] != 0x10 {
							observed.err = fmt.Errorf("first MQTT byte = 0x%02x, want CONNECT", first[0])
						}
						_ = conn.(*net.TCPConn).SetLinger(0)
						_ = conn.Close()
						if observed.err != nil {
							return
						}
						continue
					}
					if err := readEndpointConnect(conn); err != nil {
						_ = conn.Close()
						observed.err = err
						return
					}
					if script == "connack-timeout" && observed.accepted == 1 {
						// No reply: the client must time out and close, not send
						// another CONNECT to the next address in the same call.
						_, _ = io.Copy(io.Discard, conn)
					} else {
						code := byte(0)
						if script == "connack-refused" && observed.accepted == 1 {
							code = 0x97
						}
						_, observed.err = conn.Write([]byte{0x20, 3, 0, code, 0})
					}
					_ = conn.Close()
					if observed.err != nil {
						return
					}
				}
			}()

			_, port, err := net.SplitHostPort(listener.Addr().String())
			if err != nil {
				t.Fatal(err)
			}
			const host = "localhost"
			addresses := []string{"127.0.0.1", "127.0.0.1"}
			if script == "dial-failure" {
				first := rand.New(rand.NewPCG(1, 2)).IntN(len(addresses))
				addresses[first] = "127.0.0.2"
			}
			endpoints := &endpoints{
				plain: net.JoinHostPort(host, port),
				cache: map[string]resolvedHost{host: {addresses: addresses, at: time.Now()}},
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			client, _, connectErr := endpoints.connect(ctx, rand.New(rand.NewPCG(1, 2)), false, mqttclient.ConnectOptions{
				ClientID: "endpoint-boundary", CleanStart: true, HandshakeTimeout: 250 * time.Millisecond,
			})
			if client != nil {
				_ = client.Close()
			}
			_ = listener.Close()
			observed := <-done
			if observed.err != nil {
				t.Fatalf("scripted listener: %v", observed.err)
			}
			if observed.accepted != 1 {
				t.Fatalf("received %d connections, want exactly one CONNECT-bearing connection", observed.accepted)
			}
			if script == "dial-failure" {
				if connectErr != nil {
					t.Fatalf("pre-CONNECT dial failure did not fall through: %v", connectErr)
				}
			} else {
				if connectErr == nil {
					t.Fatal("CONNECT-stage failure was hidden by address fallback")
				}
				var dialError *mqttclient.DialError
				if errors.As(connectErr, &dialError) {
					t.Fatalf("CONNECT-stage failure was labeled as a pre-CONNECT dial error: %v", connectErr)
				}
				if script == "connack-refused" {
					if reason, ok := connackReason(connectErr); !ok || reason != 0x97 {
						t.Fatalf("CONNACK refusal lost its reason code: %v", connectErr)
					}
				}
			}
		})
	}
}
