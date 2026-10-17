package tcp

import (
	"net"
	"testing"
	"time"

	adapterclock "narumq/internal/adapters/clock"
	"narumq/internal/application/connect"
	applicationauthentication "narumq/internal/application/ports/authentication"
	"narumq/internal/application/publish"
	"narumq/internal/application/subscribe"
	"narumq/internal/application/tenantlimit"
	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/security"
	"narumq/internal/domain/tenancy"
)

func tcpTenantLimitService(t *testing.T, limits tenantlimit.Limits, wait time.Duration) *tenantlimit.Service {
	t.Helper()
	service, err := tenantlimit.New(adapterclock.SystemClock{}, tenantlimit.Profile{MaxWait: wait, Default: limits}, nil)
	if err != nil {
		t.Fatal(err)
	}
	return service
}
func tcpTenantPrincipal(t *testing.T) security.Principal {
	t.Helper()
	tenant, _ := tenancy.NewTenantID("tenant-a")
	principal, err := security.NewPrincipal(tenant, "publisher", "subject", "credential", []security.Role{"client"}, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	return principal
}
func tcpTenantRuntime(t *testing.T, version mqtt.ProtocolVersion, service *tenantlimit.Service, phased bool) *adapterTestRuntime {
	t.Helper()
	config := defaultAdapterConfig()
	config.TenantLimits = service
	config.ReceiveMaximum = 1
	if phased {
		config.ClusterMode = true
		config.LocalMemberID = 4
		config.ClusterReady = func() bool { return true }
		config.ClusterAdmissionOpen = func() bool { return true }
		config.ClusterProjectionReady = func() bool { return true }
		config.CredentialRevocationChecker = &tcpCredentialRevocationChecker{}
	}
	runtime := newAdapterTestRuntime(t, 8, config, func(server *Server) {
		tenant, _ := tenancy.NewTenantID("tenant-a")
		filter, _ := tenancy.NewTopicFilter(tenant, "allowed/#")
		rule, err := security.NewRule(tenant, "client", security.EffectAllow, security.ActionPublish, filter)
		if err != nil {
			t.Fatal(err)
		}
		policy, err := security.NewPolicy([]security.Rule{rule})
		if err != nil {
			t.Fatal(err)
		}
		auth := &testServerAuthenticator{result: applicationauthentication.Result{TenantID: tenant, Subject: "subject", CredentialID: "credential", AllowedClientIDs: []string{"publisher"}, Roles: []security.Role{"client"}, ExpiresAt: server.config.Clock.Now().Add(time.Hour)}}
		connect.WithSecurity(auth, nil, policy, "jwt")(server.connections)
		subscribe.WithPolicy(policy)(server.subscriptions)
		publish.WithPolicy(policy)(server.publications)
		server.config.Policy = policy
	})
	t.Cleanup(func() { runtime.stop(t) })
	writeTestPackets(t, runtime.client, &mqtt.Connect{Version: version, CleanStart: true, ClientID: "publisher", HasUsername: true, HasPassword: true, Password: []byte("authenticated")})
	frame := readTestFrame(t, runtime.client)
	if frame[0] != 0x20 || frame[3] != 0 {
		t.Fatalf("CONNACK=%x", frame)
	}
	return runtime
}

// Traceability: TEN-06, TEN-07; docs/specs/tenancy-and-security.md §5.2.
// Scenarios: tenancy-and-security.md:SEC-SCN-026, tenancy-and-security.md:SEC-SCN-027
func TestTCPPublishTenantLimitReasonCodes(t *testing.T) {
	for _, tc := range []struct {
		name    string
		version mqtt.ProtocolVersion
		qos     mqtt.QoS
		rate    bool
		want    byte
		header  byte
		phased  bool
	}{
		{"payload-qos0", mqtt.ProtocolV5, 0, false, 0x97, 0xe0, false},
		{"payload-qos1", mqtt.ProtocolV5, 1, false, 0x97, 0x40, false},
		{"payload-qos2", mqtt.ProtocolV5, 2, false, 0x97, 0x50, false},
		{"rate-qos0", mqtt.ProtocolV5, 0, true, 0x96, 0xe0, false},
		{"rate-qos1", mqtt.ProtocolV5, 1, true, 0x97, 0x40, false},
		{"rate-qos2", mqtt.ProtocolV5, 2, true, 0x97, 0x50, false},
		{"v311-qos0", mqtt.ProtocolV311, 0, false, 0, 0, false},
		{"v311-qos1", mqtt.ProtocolV311, 1, false, 0, 0, false},
		{"v311-qos2", mqtt.ProtocolV311, 2, true, 0, 0, false},
		{"phased-payload-qos1", mqtt.ProtocolV5, 1, false, 0x97, 0x40, true},
		{"phased-payload-qos2", mqtt.ProtocolV5, 2, false, 0x97, 0x50, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			limits := tenantlimit.Limits{MaxPayloadBytes: 1}
			if tc.rate {
				limits = tenantlimit.Limits{BytesPerSecond: 1}
			}
			runtime := tcpTenantRuntime(t, tc.version, tcpTenantLimitService(t, limits, 0), tc.phased)
			id := mqtt.PacketIdentifier(4)
			if tc.qos == 0 {
				id = 0
			}
			writeTestPackets(t, runtime.client, &mqtt.Publish{Version: tc.version, QoS: tc.qos, PacketID: id, Topic: "allowed/rejected", Payload: []byte("xx")})
			if tc.header == 0 {
				requireConnectionClosed(t, runtime.client)
				return
			}
			frame := readTestFrame(t, runtime.client)
			index := 4
			if tc.qos == 0 {
				index = 2
			}
			if frame[0] != tc.header || len(frame) <= index || frame[index] != tc.want {
				t.Fatalf("rejection=%x want header=%x reason=%x", frame, tc.header, tc.want)
			}
			if tc.qos == 0 {
				requireConnectionClosed(t, runtime.client)
				return
			}
			// A negative ACK returns Receive Maximum capacity, so another ID must be read.
			writeTestPackets(t, runtime.client, &mqtt.Publish{Version: tc.version, QoS: tc.qos, PacketID: 5, Topic: "allowed/rejected", Payload: []byte("xx")})
			frame = readTestFrame(t, runtime.client)
			if frame[0] != tc.header || frame[3] != 5 || frame[4] != 0x97 {
				t.Fatalf("second rejection after released receive slot=%x", frame)
			}
			writeTestPackets(t, runtime.client, &mqtt.PingReq{Version: tc.version})
			frame = readTestFrame(t, runtime.client)
			if frame[0] != 0xd0 {
				t.Fatalf("PINGRESP=%x", frame)
			}
		})
	}
}

func tcpTenantAckReason(t *testing.T, runtime *adapterTestRuntime, frame []byte) byte {
	t.Helper()
	packet, _, err := runtime.server.limits.Load().decoder.Decode(frame, mqtt.ProtocolV5)
	if err != nil {
		t.Fatal(err)
	}
	switch packet := packet.(type) {
	case *mqtt.PubAck:
		return packet.ReasonCode
	case *mqtt.PubRec:
		return packet.ReasonCode
	default:
		t.Fatalf("expected publish ACK, got %T", packet)
		return 0
	}
}

// Traceability: TEN-06, TEN-07; docs/specs/tenancy-and-security.md §5.2.
// Scenarios: tenancy-and-security.md:SEC-SCN-027
func TestTCPTenantPublishRateWaitThenReject(t *testing.T) {
	// Freeze policy time so all five reservations represent one tenant burst.
	clock := &adapterTestClock{now: time.Unix(100, 0)}
	service, err := tenantlimit.New(clock, tenantlimit.Profile{MaxWait: time.Second, Default: tenantlimit.Limits{MessagesPerSecond: 2}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	runtime := tcpTenantRuntime(t, mqtt.ProtocolV5, service, false)
	for i := 1; i <= 5; i++ {
		started := time.Now()
		writeTestPackets(t, runtime.client, &mqtt.Publish{Version: mqtt.ProtocolV5, QoS: 1, PacketID: mqtt.PacketIdentifier(i), Topic: "allowed/rate"})
		frame := readTestFrame(t, runtime.client, 3*time.Second)
		want := byte(0)
		if i == 5 {
			want = 0x97
		}
		if frame[0] != 0x40 || frame[3] != byte(i) || tcpTenantAckReason(t, runtime, frame) != want {
			t.Fatalf("publish %d ACK=%x want reason=%x", i, frame, want)
		}
		if i == 3 && time.Since(started) < 400*time.Millisecond {
			t.Fatalf("third ACK after %s; want >=400ms", time.Since(started))
		}
	}
}

// Traceability: TEN-06, TEN-07; docs/specs/tenancy-and-security.md §5.2.
// Scenarios: tenancy-and-security.md:SEC-SCN-027
func TestTCPTenantPublishQoS2RetransmitIsNotCharged(t *testing.T) {
	clock := &adapterTestClock{now: time.Unix(100, 0)}
	service, err := tenantlimit.New(clock, tenantlimit.Profile{Default: tenantlimit.Limits{MessagesPerSecond: 1}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	runtime := tcpTenantRuntime(t, mqtt.ProtocolV5, service, false)
	packet := &mqtt.Publish{Version: mqtt.ProtocolV5, QoS: 2, PacketID: 7, Topic: "allowed/qos2", Payload: []byte("kept")}
	writeTestPackets(t, runtime.client, packet)
	frame := readTestFrame(t, runtime.client)
	if frame[0] != 0x50 || tcpTenantAckReason(t, runtime, frame) != 0 {
		t.Fatalf("initial PUBREC=%x", frame)
	}
	packet.DUP = true
	writeTestPackets(t, runtime.client, packet)
	frame = readTestFrame(t, runtime.client)
	if frame[0] != 0x50 || tcpTenantAckReason(t, runtime, frame) != 0 {
		t.Fatalf("DUP PUBREC=%x; unfinished ID was charged", frame)
	}
	writeTestPackets(t, runtime.client, &mqtt.PubRel{Version: mqtt.ProtocolV5, PacketID: 7})
	frame = readTestFrame(t, runtime.client)
	if frame[0] != 0x70 {
		t.Fatalf("PUBCOMP=%x", frame)
	}
	packet.DUP = false
	packet.PacketID = 8
	writeTestPackets(t, runtime.client, packet)
	frame = readTestFrame(t, runtime.client)
	if frame[0] != 0x50 || frame[4] != 0x97 {
		t.Fatalf("new ID PUBREC=%x", frame)
	}
}

// Traceability: TEN-06, TEN-07; docs/specs/tenancy-and-security.md §5.2.
// Scenarios: tenancy-and-security.md:SEC-SCN-027
func TestTCPTenantPublishWaitCancelledByConnectionStop(t *testing.T) {
	service := tcpTenantLimitService(t, tenantlimit.Limits{MessagesPerSecond: 1}, 5*time.Second)
	runtime := tcpTenantRuntime(t, mqtt.ProtocolV5, service, false)
	serverSocket, clientSocket := net.Pipe()
	defer clientSocket.Close()
	conn := newConnection(runtime.server, serverSocket)
	conn.principal = tcpTenantPrincipal(t)
	defer conn.requestClose()
	if admitted, err := conn.admitTenantPublish(&mqtt.Publish{}); err != nil || !admitted {
		t.Fatal(admitted, err)
	}
	result := make(chan bool, 1)
	go func() { admitted, _ := conn.admitTenantPublish(&mqtt.Publish{}); result <- admitted }()
	time.Sleep(20 * time.Millisecond)
	conn.requestClose()
	select {
	case admitted := <-result:
		if admitted {
			t.Fatal("closed connection admitted waiting publish")
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("wait did not stop promptly")
	}
}

// Traceability: TEN-06, TEN-07; docs/specs/tenancy-and-security.md §5.2.
// Scenarios: tenancy-and-security.md:SEC-SCN-027
func TestTCPTenantPublishWaitStopsBeforeTimerWhenConnectionIsAlreadyStopped(t *testing.T) {
	service := tcpTenantLimitService(t, tenantlimit.Limits{MessagesPerSecond: 1}, 5*time.Second)
	runtime := tcpTenantRuntime(t, mqtt.ProtocolV5, service, false)
	serverSocket, clientSocket := net.Pipe()
	defer clientSocket.Close()
	defer serverSocket.Close()
	conn := newConnection(runtime.server, serverSocket)
	conn.principal = tcpTenantPrincipal(t)
	defer conn.requestClose()

	if admitted, err := conn.admitTenantPublish(&mqtt.Publish{}); err != nil || !admitted {
		t.Fatalf("first publish admission = (%v, %v), want (true, nil)", admitted, err)
	}
	conn.requestClose()
	if admitted, err := conn.admitTenantPublish(&mqtt.Publish{}); err != nil || admitted {
		t.Fatalf("stopped publish admission = (%v, %v), want (false, nil)", admitted, err)
	}
}

// Traceability: TEN-06, TEN-07; docs/specs/tenancy-and-security.md §5.2.
// Scenarios: tenancy-and-security.md:SEC-SCN-027
func TestTCPTenantLimitsSkipAnonymous(t *testing.T) {
	config := defaultAdapterConfig()
	config.TenantLimits = tcpTenantLimitService(t, tenantlimit.Limits{MaxPayloadBytes: 1}, 0)
	runtime := newAdapterTestRuntime(t, 8, config)
	defer runtime.stop(t)
	writeTestPackets(t, runtime.client, &mqtt.Connect{Version: mqtt.ProtocolV5, CleanStart: true, ClientID: "anonymous"})
	frame := readTestFrame(t, runtime.client)
	if frame[0] != 0x20 || frame[3] != 0 {
		t.Fatalf("anonymous CONNACK=%x", frame)
	}
	writeTestPackets(t, runtime.client, &mqtt.Publish{Version: mqtt.ProtocolV5, QoS: 1, PacketID: 1, Topic: "anonymous", Payload: []byte("larger than authenticated limit")})
	frame = readTestFrame(t, runtime.client)
	if frame[0] != 0x40 || tcpTenantAckReason(t, runtime, frame) != 0 {
		t.Fatalf("anonymous PUBACK=%x", frame)
	}
}

// Traceability: TEN-06, TEN-07; docs/specs/tenancy-and-security.md §5.2.
// Scenarios: tenancy-and-security.md:SEC-SCN-026, tenancy-and-security.md:SEC-SCN-027
func TestTCPTenantLimitsAdmissionOrder(t *testing.T) {
	t.Run("ReceiveMaximumBeforePayload", func(t *testing.T) {
		runtime := tcpTenantRuntime(t, mqtt.ProtocolV5, tcpTenantLimitService(t, tenantlimit.Limits{MaxPayloadBytes: 1}, 0), false)
		writeTestPackets(t, runtime.client, &mqtt.Publish{Version: mqtt.ProtocolV5, QoS: 2, PacketID: 1, Topic: "allowed/one", Payload: []byte("x")})
		frame := readTestFrame(t, runtime.client)
		if frame[0] != 0x50 || tcpTenantAckReason(t, runtime, frame) != 0 {
			t.Fatalf("first PUBREC=%x", frame)
		}
		writeTestPackets(t, runtime.client, &mqtt.Publish{Version: mqtt.ProtocolV5, QoS: 2, PacketID: 2, Topic: "allowed/two", Payload: []byte("xx")})
		frame = readTestFrame(t, runtime.client)
		if frame[0] != 0xe0 || frame[2] != 0x93 {
			t.Fatalf("receive maximum must precede payload admission: %x", frame)
		}
	})
	t.Run("ACLRejectionConsumesRateToken", func(t *testing.T) {
		service, err := tenantlimit.New(&adapterTestClock{now: time.Unix(100, 0)}, tenantlimit.Profile{Default: tenantlimit.Limits{MessagesPerSecond: 1}}, nil)
		if err != nil {
			t.Fatal(err)
		}
		runtime := tcpTenantRuntime(t, mqtt.ProtocolV5, service, false)
		writeTestPackets(t, runtime.client, &mqtt.Publish{Version: mqtt.ProtocolV5, QoS: 1, PacketID: 1, Topic: "forbidden/one"})
		frame := readTestFrame(t, runtime.client)
		if frame[0] != 0x40 || tcpTenantAckReason(t, runtime, frame) != 0x87 {
			t.Fatalf("ACL PUBACK=%x", frame)
		}
		writeTestPackets(t, runtime.client, &mqtt.Publish{Version: mqtt.ProtocolV5, QoS: 1, PacketID: 2, Topic: "allowed/two"})
		frame = readTestFrame(t, runtime.client)
		if frame[0] != 0x40 || tcpTenantAckReason(t, runtime, frame) != 0x97 {
			t.Fatalf("ACL-rejected publish did not consume its token: %x", frame)
		}
	})
}
