package tcp

import (
	"bytes"
	"context"
	"errors"
	"net"
	"reflect"
	"testing"
	"time"
)

func TestForwardedDeliveryCodecGoldenBytes(t *testing.T) {
	want := []byte{
		0x01, 0x00, 0x01, 0xff, 0x00, 0x03, 't', '/', '1', 0x01,
		0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
		0x00, 0x00, 0x00, 0x01, 'p',
	}
	delivery := ForwardedDeliveryDTO{
		SessionID: string([]byte{0xff}), Topic: "t/1", Payload: []byte("p"), Retain: true,
		ExpiresAtUnixMillis: 0x0102030405060708,
	}
	encoded, err := EncodeForwardedDelivery(delivery)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(encoded, want) {
		t.Fatalf("encoded delivery = %x, want %x", encoded, want)
	}
	decoded, err := DecodeForwardedDelivery(want)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded, delivery) {
		t.Fatalf("decoded delivery = %+v, want %+v", decoded, delivery)
	}
	want[len(want)-1] = 'x'
	if !bytes.Equal(decoded.Payload, []byte("p")) {
		t.Fatal("decoded payload aliases the input frame")
	}
}

func TestForwardedDeliveryCodecRejectsMalformed(t *testing.T) {
	valid, err := EncodeForwardedDelivery(ForwardedDeliveryDTO{SessionID: "s", Topic: "t", Payload: []byte("p")})
	if err != nil {
		t.Fatal(err)
	}
	mutate := func(change func([]byte)) []byte {
		payload := bytes.Clone(valid)
		change(payload)
		return payload
	}
	emptySession := []byte{1, 0, 0, 0, 1, 't', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	emptyTopic := []byte{1, 0, 1, 's', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	invalidUTF8Topic := []byte{1, 0, 1, 's', 0, 1, 0xff, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	cases := map[string][]byte{
		"unsupported version": mutate(func(payload []byte) { payload[0] = 2 }),
		"empty session":       emptySession,
		"empty topic":         emptyTopic,
		"invalid UTF-8 topic": invalidUTF8Topic,
		"unknown flag bits":   mutate(func(payload []byte) { payload[7] = 2 }),
		"truncated payload":   valid[:len(valid)-1],
		"trailing byte":       append(bytes.Clone(valid), 0),
		"oversized frame":     make([]byte, dynamicForwardFrameLimit+1),
	}
	for name, payload := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := DecodeForwardedDelivery(payload); !errors.Is(err, ErrInvalidPeerMessage) {
				t.Fatalf("DecodeForwardedDelivery error = %v, want ErrInvalidPeerMessage", err)
			}
		})
	}

	identity := dynamicTestIdentity(1, "n:1", "n", PeerPhaseVoter, "", 0x11)
	raftHello, err := encodeDynamicPeerHello(identity, dynamicRaftMagic)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeDynamicPeerHello(raftHello, dynamicForwardMagic); !errors.Is(err, ErrInvalidPeerMessage) {
		t.Fatalf("decode Raft hello on forwarding lane error = %v, want ErrInvalidPeerMessage", err)
	}
}

func TestReadDynamicIdleFrameIgnoresPriorDeadline(t *testing.T) {
	reader, writer := net.Pipe()
	defer reader.Close()
	defer writer.Close()
	if err := reader.SetReadDeadline(time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	frame := []byte{1, 2, 3}
	written := make(chan error, 1)
	go func() {
		time.Sleep(50 * time.Millisecond)
		written <- writeDynamicFrame(writer, frame, 16)
	}()
	got, err := readDynamicIdleFrame(reader, 16)
	if err != nil {
		t.Fatalf("read idle frame after clearing old deadline: %v", err)
	}
	if !bytes.Equal(got, frame) {
		t.Fatalf("read frame = %x, want %x", got, frame)
	}
	if err := <-written; err != nil {
		t.Fatalf("write frame: %v", err)
	}
}

func TestDynamicPeerTransportForwardLaneIsIndependent(t *testing.T) {
	t.Run("authenticated streams dispatch to separate inbound queues", func(t *testing.T) {
		transport1, transport2, local1, local2 := newForwardTestTransportPair(t)
		ctx, cancel := context.WithCancel(context.Background())
		serveResults := make(chan error, 2)
		go func() { serveResults <- transport1.Serve(ctx) }()
		go func() { serveResults <- transport2.Serve(ctx) }()
		defer func() {
			cancel()
			for range 2 {
				select {
				case err := <-serveResults:
					if err != nil {
						t.Errorf("peer transport Serve: %v", err)
					}
				case <-time.After(time.Second):
					t.Error("peer transport did not stop")
				}
			}
			_ = transport1.Close()
			_ = transport2.Close()
		}()

		forwardPayload := []byte("forward-frame")
		if err := transport1.Forward(PeerMessageDTO{From: local1.MemberID, To: local2.MemberID, Data: forwardPayload}); err != nil {
			t.Fatalf("queue forwarding frame: %v", err)
		}
		select {
		case <-transport2.ForwardReady():
		case <-time.After(2 * time.Second):
			t.Fatal("forward frame was not signaled")
		}
		forwarded, ok := transport2.TakeForward()
		if !ok {
			t.Fatal("forward frame was not available")
		}
		if forwarded.Message.From != local1.MemberID || !bytes.Equal(forwarded.Message.Data, forwardPayload) {
			t.Fatalf("forwarded message = %+v, want source %d and bytes %q", forwarded.Message, local1.MemberID, forwardPayload)
		}
		forwarded.Complete()
		if delivery, ok := transport2.TakeInbound(); ok {
			delivery.Complete()
			t.Fatalf("forward frame also entered Raft queue: %+v", delivery.Message)
		}

		raftPayload := []byte{0x08, 0x01, 0x12, 0x01, 0x7f}
		if err := transport1.Send(PeerMessageDTO{From: local1.MemberID, To: local2.MemberID, Data: raftPayload}); err != nil {
			t.Fatalf("queue Raft frame: %v", err)
		}
		select {
		case <-transport2.InboundReady():
		case <-time.After(2 * time.Second):
			t.Fatal("Raft frame was not signaled")
		}
		raft, ok := transport2.TakeInbound()
		if !ok {
			t.Fatal("Raft frame was not available")
		}
		if raft.Message.From != local1.MemberID || !bytes.Equal(raft.Message.Data, raftPayload) {
			t.Fatalf("Raft message = %+v, want source %d and bytes %x", raft.Message, local1.MemberID, raftPayload)
		}
		raft.Complete()
	})

	t.Run("Raft and forwarding quotas are independent", func(t *testing.T) {
		transport, remoteTransport, local, remote := newForwardTestTransportPair(t)
		defer transport.Close()
		defer remoteTransport.Close()
		message := PeerMessageDTO{From: local.MemberID, To: remote.MemberID, Data: []byte{1}}
		for range peerQueueMessages {
			if err := transport.Send(message); err != nil {
				t.Fatalf("Send within Raft quota: %v", err)
			}
		}
		if err := transport.Send(message); !errors.Is(err, ErrPeerBackpressure) {
			t.Fatalf("Send over Raft quota error = %v, want ErrPeerBackpressure", err)
		}
		if err := transport.Forward(message); err != nil {
			t.Fatalf("Forward with Raft quota full: %v", err)
		}
		for range forwardQueueMessages - 1 {
			if err := transport.Forward(message); err != nil {
				t.Fatalf("Forward within forwarding quota: %v", err)
			}
		}
		if err := transport.Forward(message); !errors.Is(err, ErrPeerBackpressure) {
			t.Fatalf("Forward over forwarding quota error = %v, want ErrPeerBackpressure", err)
		}
	})
}

func newForwardTestTransportPair(t *testing.T) (
	*DynamicTCPPeerTransport,
	*DynamicTCPPeerTransport,
	PeerIdentityDTO,
	PeerIdentityDTO,
) {
	t.Helper()
	serverTLS1, clientTLS1 := testPeerTLSConfigs(t, "node1.example")
	serverTLS2, clientTLS2 := testPeerTLSConfigs(t, "node2.example")
	serverTLS1.ClientCAs.AddCert(serverTLS2.Certificates[0].Leaf)
	clientTLS1.RootCAs.AddCert(serverTLS2.Certificates[0].Leaf)
	serverTLS2.ClientCAs.AddCert(serverTLS1.Certificates[0].Leaf)
	clientTLS2.RootCAs.AddCert(serverTLS1.Certificates[0].Leaf)

	listener1, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	listener2, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		_ = listener1.Close()
		t.Fatal(err)
	}
	local1 := identityForTestCertificate(1, listener1.Addr().String(), "node1.example",
		PeerPhaseVoter, "", "1.0.0", serverTLS1.Certificates[0])
	local2 := identityForTestCertificate(2, listener2.Addr().String(), "node2.example",
		PeerPhaseVoter, "", "1.0.0", serverTLS2.Certificates[0])
	rejectingHandler := func(context.Context, PeerIdentityDTO) (SeedAdmissionResponseDTO, error) {
		return SeedAdmissionResponseDTO{}, errors.New("candidate is not a seed")
	}
	transport1, err := NewDynamicTCPPeerTransport(DynamicPeerTransportConfig{
		LocalIdentity: local1, Listener: listener1, ServerTLS: serverTLS1, ClientTLS: clientTLS1,
		Peers: []PeerIdentityDTO{local2}, AdmissionHandler: rejectingHandler,
	})
	if err != nil {
		_ = listener1.Close()
		_ = listener2.Close()
		t.Fatal(err)
	}
	transport2, err := NewDynamicTCPPeerTransport(DynamicPeerTransportConfig{
		LocalIdentity: local2, Listener: listener2, ServerTLS: serverTLS2, ClientTLS: clientTLS2,
		Peers: []PeerIdentityDTO{local1}, AdmissionHandler: rejectingHandler,
	})
	if err != nil {
		_ = transport1.Close()
		_ = listener2.Close()
		t.Fatal(err)
	}
	return transport1, transport2, local1, local2
}
