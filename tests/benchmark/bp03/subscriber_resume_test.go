package main

import (
	"context"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"narumq/internal/domain/mqtt"
)

// TestSubscriberReaderResumesQoS2Release verifies the subscriber reader resumes the QoS 2 release flow.
// Scenarios: performance-and-verification.md:PRF-SCN-003
func TestSubscriberReaderResumesQoS2Release(t *testing.T) {
	for _, completed := range []bool{false, true} {
		name := "awaiting-PUBREL"
		if completed {
			name = "PUBCOMP-retransmit"
		}
		t.Run(name, func(t *testing.T) {
			profile := collectorProfile()
			col := newCollector(4)
			addTestRecord(t, col, 2, 1, 1, time.Second)
			stream := streamSpec{index: 0, qos: 2, name: "q", topic: "bp03/durable/q2/01"}
			client, server := connectSubscriberResumeFixture(t, stream, false)
			ctx, cancel := context.WithCancelCause(context.Background())
			defer cancel(context.Canceled)
			startReader(client, col, profile, cancel)
			writeSubscriberResumePacket(t, server, &mqtt.Publish{Version: mqtt.ProtocolV5, QoS: mqtt.QoSExactlyOnce,
				PacketID: 7, Topic: stream.topic, Payload: makePayload(profile.Seed, 1, stream.index, profile.PayloadBytes)})
			assertSubscriberResumeAck(t, server, 0x50)
			if completed {
				writeSubscriberResumePacket(t, server, &mqtt.PubRel{Version: mqtt.ProtocolV5, PacketID: 7})
				assertSubscriberResumeAck(t, server, 0x70)
			}
			client.stopReader()
			_ = client.client.Close()
			<-client.done
			_ = server.Close()

			resumed, nextServer := connectSubscriberResumeFixture(t, stream, true)
			// Reuse the same session owner, replacing only the closed connection.
			client.client, client.stop = resumed.client, resumed.stop
			client.stopOnce = sync.Once{}
			startReader(client, col, profile, cancel)
			t.Cleanup(func() {
				client.stopReader()
				_ = client.client.Close()
				<-client.done
			})
			writeSubscriberResumePacket(t, nextServer, &mqtt.PubRel{Version: mqtt.ProtocolV5, PacketID: 7})
			assertSubscriberResumeAck(t, nextServer, 0x70)
			if cause := context.Cause(ctx); cause != nil {
				t.Fatalf("resumed known PUBREL failed: %v", cause)
			}
			col.mu.Lock()
			received := col.records[messageKey{"q", 1}].ReceiveCount
			col.mu.Unlock()
			if received != 1 {
				t.Fatalf("QoS 2 delivery count = %d, want exactly one", received)
			}
			if !completed {
				writeSubscriberResumePacket(t, nextServer, &mqtt.PubRel{Version: mqtt.ProtocolV5, PacketID: 8})
				select {
				case <-ctx.Done():
				case <-time.After(time.Second):
					t.Fatal("unknown PUBREL did not terminate the workload")
				}
				col.mu.Lock()
				protocolErrors := col.protocolErrors
				col.mu.Unlock()
				if protocolErrors != 1 {
					t.Fatalf("unknown PUBREL protocol errors = %d, want 1", protocolErrors)
				}
			}
		})
	}
}

func connectSubscriberResumeFixture(t *testing.T, stream streamSpec, present bool) (*runClient, net.Conn) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			accepted <- nil
			return
		}
		_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
		_, err = readSubscriberResumeFrame(conn)
		flags := byte(0)
		if present {
			flags = 1
		}
		if err == nil {
			_, err = conn.Write([]byte{0x20, 3, flags, 0, 0})
		}
		if err != nil {
			_ = conn.Close()
			accepted <- nil
			return
		}
		accepted <- conn
	}()
	client, err := connectClient(context.Background(), stream, "resume-subscriber", listener.Addr().String(), !present, 300, credentials{})
	if err != nil {
		t.Fatal(err)
	}
	conn := <-accepted
	if conn == nil {
		t.Fatal("fixture handshake failed")
	}
	t.Cleanup(func() { _ = client.client.Close(); _ = conn.Close() })
	return client, conn
}

func readSubscriberResumeFrame(conn net.Conn) ([]byte, error) {
	var first [1]byte
	if _, err := io.ReadFull(conn, first[:]); err != nil {
		return nil, err
	}
	wire := []byte{first[0]}
	size := 0
	for shift := 0; shift < 28; shift += 7 {
		var encoded [1]byte
		_, err := io.ReadFull(conn, encoded[:])
		value := encoded[0]
		if err != nil {
			return nil, err
		}
		wire = append(wire, value)
		size |= int(value&127) << shift
		if value&128 == 0 {
			body := make([]byte, size)
			_, err := io.ReadFull(conn, body)
			return append(wire, body...), err
		}
	}
	return nil, io.ErrUnexpectedEOF
}

func writeSubscriberResumePacket(t *testing.T, conn net.Conn, packet mqtt.Packet) {
	t.Helper()
	encoder, err := mqtt.NewEncoder(1 << 20)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := encoder.Append(nil, packet)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write(wire); err != nil {
		t.Fatal(err)
	}
}

func assertSubscriberResumeAck(t *testing.T, conn net.Conn, kind byte) {
	t.Helper()
	wire, err := readSubscriberResumeFrame(conn)
	if err != nil {
		t.Fatalf("read resumed ACK: %v", err)
	}
	decoder, err := mqtt.NewDecoder(1 << 20)
	if err != nil {
		t.Fatal(err)
	}
	packet, _, err := decoder.Decode(wire, mqtt.ProtocolV5)
	if err != nil {
		t.Fatal(err)
	}
	if kind == 0x50 {
		ack, ok := packet.(*mqtt.PubRec)
		if !ok || ack.PacketID != 7 || ack.ReasonCode != 0 {
			t.Fatalf("ACK = %#v, want successful PUBREC 7", packet)
		}
	} else {
		ack, ok := packet.(*mqtt.PubComp)
		if !ok || ack.PacketID != 7 || ack.ReasonCode != 0 {
			t.Fatalf("ACK = %#v, want successful PUBCOMP 7", packet)
		}
	}
}
