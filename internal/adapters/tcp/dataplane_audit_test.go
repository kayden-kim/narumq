package tcp

import (
	"net"
	"reflect"
	"testing"
	"time"

	"narumq/internal/application/ports/audit"
	"narumq/internal/application/subscribe"
	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/security"
	"narumq/internal/domain/tenancy"
)

type tcpDataPlaneAuditRecorder struct {
	denials []audit.DataPlaneDenialDTO
}

func (recorder *tcpDataPlaneAuditRecorder) RecordDataPlaneDenial(denial audit.DataPlaneDenialDTO) {
	recorder.denials = append(recorder.denials, denial)
}

func tcpDataPlanePrincipal(t *testing.T) security.Principal {
	t.Helper()
	tenant, err := tenancy.NewTenantID("tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	principal, err := security.NewPrincipal(
		tenant, "client-a", "device-a", "credential-a", []security.Role{"publisher"}, time.Date(2027, time.January, 1, 0, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	return principal
}

func tcpDataPlaneConnection(t *testing.T, recorder *tcpDataPlaneAuditRecorder) (*connection, net.Conn) {
	t.Helper()
	deps := csvNewServer(t, newPipeListener(), csvServerOptions{})
	deps.server.config.DataPlaneAudit = recorder
	subscribe.WithPolicy(security.Policy{})(deps.server.subscriptions)
	serverSocket, clientSocket := net.Pipe()
	connection := newConnection(deps.server, serverSocket)
	connection.version = mqtt.ProtocolV5
	connection.sessionID = "client-a"
	connection.principal = tcpDataPlanePrincipal(t)
	go connection.writerLoop()
	t.Cleanup(func() {
		connection.requestClose()
		<-connection.writerDone
		_ = serverSocket.Close()
		_ = clientSocket.Close()
	})
	return connection, clientSocket
}

// Traceability: SEC-11/NFR-17; docs/specs/tenancy-and-security.md §8.
func TestTCPPublishAuthorizationDenialRecordsOnlyAuthenticatedPrincipal(t *testing.T) {
	for _, authenticated := range []bool{true, false} {
		name := "authenticated"
		if !authenticated {
			name = "unauthenticated"
		}
		t.Run(name, func(t *testing.T) {
			recorder := &tcpDataPlaneAuditRecorder{}
			connection, _ := tcpDataPlaneConnection(t, recorder)
			if !authenticated {
				connection.principal = security.Principal{}
			}
			if err := connection.dispatchPublishAt(mqtt.Publish{
				Version: mqtt.ProtocolV5, Topic: "denied/topic", QoS: mqtt.QoSAtMostOnce,
			}, time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)); err != nil {
				t.Fatalf("dispatchPublishAt() error = %v", err)
			}
			if !authenticated {
				if len(recorder.denials) != 0 {
					t.Fatalf("unauthenticated publish recorded denial: %+v", recorder.denials)
				}
				return
			}
			if len(recorder.denials) != 1 {
				t.Fatalf("recorded denials = %d, want 1: %+v", len(recorder.denials), recorder.denials)
			}
			denial := recorder.denials[0]
			if denial.Action != audit.DataPlaneActionPublish || denial.Reason != audit.DataPlaneReasonNotAuthorized ||
				denial.Subject != "device-a" || denial.ClientID != "client-a" || denial.Topic != "denied/topic" || denial.SourceIP != "pipe" {
				t.Fatalf("publish denial = %+v", denial)
			}
		})
	}
}

// Traceability: SEC-11/NFR-17; docs/specs/tenancy-and-security.md §8.
func TestTCPSubscribeAndUnsubscribeDispatchRecordEveryDeniedFilter(t *testing.T) {
	for _, test := range []struct {
		name    string
		packet  mqtt.Packet
		action  audit.DataPlaneAction
		wantAck mqtt.Packet
	}{
		{
			name: "subscribe",
			packet: &mqtt.Subscribe{Version: mqtt.ProtocolV5, PacketID: 3, Subscriptions: []mqtt.Subscription{
				{Filter: "denied/one"}, {Filter: "denied/two"},
			}},
			action:  audit.DataPlaneActionSubscribe,
			wantAck: &mqtt.SubAck{Version: mqtt.ProtocolV5, PacketID: 3, ReasonCodes: []byte{0x87, 0x87}},
		},
		{
			name:    "unsubscribe",
			packet:  &mqtt.Unsubscribe{Version: mqtt.ProtocolV5, PacketID: 4, Filters: []string{"denied/one", "denied/two"}},
			action:  audit.DataPlaneActionUnsubscribe,
			wantAck: &mqtt.UnsubAck{Version: mqtt.ProtocolV5, PacketID: 4, ReasonCodes: []byte{0x87, 0x87}},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			recorder := &tcpDataPlaneAuditRecorder{}
			connection, client := tcpDataPlaneConnection(t, recorder)
			result := make(chan error, 1)
			go func() { result <- connection.dispatch(test.packet) }()
			gotFrame := readTestFrame(t, client)
			wantFrame := encodeTestPacket(t, test.wantAck)
			if !reflect.DeepEqual(gotFrame, wantFrame) {
				t.Fatalf("wire acknowledgment = % X, want % X", gotFrame, wantFrame)
			}
			if err := <-result; err != nil {
				t.Fatalf("dispatch() error = %v", err)
			}
			if len(recorder.denials) != 2 {
				t.Fatalf("recorded denials = %d, want 2: %+v", len(recorder.denials), recorder.denials)
			}
			wantFilters := []string{"denied/one", "denied/two"}
			for index, denial := range recorder.denials {
				if denial.Action != test.action || denial.Reason != audit.DataPlaneReasonNotAuthorized || denial.Topic != wantFilters[index] ||
					denial.Subject != "device-a" || denial.SourceIP != "pipe" {
					t.Fatalf("denial %d = %+v", index, denial)
				}
			}
		})
	}
}

type fixedTCPRemoteAddr string

func (address fixedTCPRemoteAddr) Network() string { return "test" }
func (address fixedTCPRemoteAddr) String() string  { return string(address) }

type fixedTCPRemoteConn struct {
	net.Conn
	remote net.Addr
}

func (connection fixedTCPRemoteConn) RemoteAddr() net.Addr { return connection.remote }

// Traceability: SEC-11/NFR-17; docs/specs/tenancy-and-security.md §8.
func TestTCPRemoteHostRemovesPortAndPreservesUnparsedAddress(t *testing.T) {
	for _, test := range []struct {
		name string
		conn net.Conn
		want string
	}{
		{name: "ipv4", conn: fixedTCPRemoteConn{remote: &net.TCPAddr{IP: net.ParseIP("192.0.2.1"), Port: 1234}}, want: "192.0.2.1"},
		{name: "ipv6", conn: fixedTCPRemoteConn{remote: &net.TCPAddr{IP: net.ParseIP("2001:db8::1"), Port: 80}}, want: "2001:db8::1"},
		{name: "unparsed pipe address", conn: fixedTCPRemoteConn{remote: fixedTCPRemoteAddr("pipe")}, want: "pipe"},
		{name: "nil connection", conn: nil, want: ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := remoteHost(test.conn); got != test.want {
				t.Fatalf("remoteHost() = %q, want %q", got, test.want)
			}
		})
	}
}
