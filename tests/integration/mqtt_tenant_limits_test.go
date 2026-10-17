package integration

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/lestrrat-go/jwx/v3/jwa"
	"github.com/lestrrat-go/jwx/v3/jwk"
	"github.com/lestrrat-go/jwx/v3/jwt"
	authenticationadapter "narumq/internal/adapters/authentication"
	"narumq/internal/adapters/clock"
	"narumq/internal/adapters/memory/deliveryqueue"
	memoryretained "narumq/internal/adapters/memory/retained"
	"narumq/internal/adapters/memory/sessionstore"
	metricsadapter "narumq/internal/adapters/metrics"
	tcpadapter "narumq/internal/adapters/tcp"
	"narumq/internal/application/connect"
	"narumq/internal/application/publish"
	"narumq/internal/application/subscribe"
	"narumq/internal/application/tenantlimit"
	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/routing"
	"narumq/internal/domain/security"
	"narumq/internal/domain/tenancy"
)

// Traceability: TEN-06, TEN-07; docs/specs/tenancy-and-security.md §5.2.
// Scenarios: tenancy-and-security.md:SEC-SCN-024, tenancy-and-security.md:SEC-SCN-025, tenancy-and-security.md:SEC-SCN-026, tenancy-and-security.md:SEC-SCN-027
func TestMQTTTenantLimits(t *testing.T) {
	t.Run("ConnectionQuota", func(t *testing.T) {
		f := newTenantLimitsFixture(t, tenantlimit.Limits{MaxConnections: 2})
		defer f.close()
		first := f.connect(t, "tenant-a", "conn-one", true)
		defer first.Close()
		second := f.connect(t, "tenant-a", "conn-two", true)
		defer second.Close()
		checkTenantConnAck(t, f, "tenant-a", "conn-three", true, 0x97)
		other := f.connect(t, "tenant-b", "other", true)
		defer other.Close()
		f.assertMetric(t, "tenant-a", "connections", 1)
	})

	t.Run("SessionQuota", func(t *testing.T) {
		f := newTenantLimitsFixture(t, tenantlimit.Limits{MaxSessions: 3})
		defer f.close()
		first := f.connect(t, "tenant-a", "persistent", false, false)
		second := f.connect(t, "tenant-a", "persistent-2", false, false)
		defer second.Close()
		third := f.connect(t, "tenant-a", "persistent-3", false, false)
		defer third.Close()
		writePacket(t, first, f.encoder, &mqtt.Disconnect{Version: mqtt.ProtocolV5})
		_ = first.Close()
		f.waitConnectedCount(t, "tenant-a", 2)
		resumed := f.connect(t, "tenant-a", "persistent", false, true)
		defer resumed.Close()
		checkTenantConnAck(t, f, "tenant-a", "new-session", false, 0x97)
		other := f.connect(t, "tenant-b", "other", true)
		defer other.Close()
		f.assertMetric(t, "tenant-a", "sessions", 1)
	})

	t.Run("SubscriptionQuota", func(t *testing.T) {
		f := newTenantLimitsFixture(t, tenantlimit.Limits{MaxSubscriptions: 2})
		defer f.close()
		socket := f.connect(t, "tenant-a", "sub-a", true)
		defer socket.Close()
		writePacket(t, socket, f.encoder, &mqtt.Subscribe{Version: mqtt.ProtocolV5, PacketID: 1, Subscriptions: []mqtt.Subscription{{Filter: "tenant-a/one", QoS: mqtt.QoSAtMostOnce}, {Filter: "tenant-a/two", QoS: mqtt.QoSAtMostOnce}, {Filter: "tenant-a/three", QoS: mqtt.QoSAtMostOnce}}})
		ack := readPacket(t, socket, f.decoder, mqtt.ProtocolV5).(*mqtt.SubAck)
		if len(ack.ReasonCodes) != 3 || ack.ReasonCodes[0] != 0 || ack.ReasonCodes[1] != 0 || ack.ReasonCodes[2] != 0x97 {
			t.Fatalf("tenant-a SUBACK reasons = %v, want [0 0 0x97]", ack.ReasonCodes)
		}
		other := f.connect(t, "tenant-b", "sub-b", true)
		defer other.Close()
		writePacket(t, other, f.encoder, &mqtt.Subscribe{Version: mqtt.ProtocolV5, PacketID: 1, Subscriptions: []mqtt.Subscription{{Filter: "tenant-b/ok", QoS: mqtt.QoSAtMostOnce}}})
		otherAck := readPacket(t, other, f.decoder, mqtt.ProtocolV5).(*mqtt.SubAck)
		if len(otherAck.ReasonCodes) != 1 || otherAck.ReasonCodes[0] != 0 {
			t.Fatalf("tenant-b SUBACK = %v, want [0]", otherAck.ReasonCodes)
		}
		f.assertMetric(t, "tenant-a", "subscriptions", 1)
	})

	t.Run("PayloadSize", func(t *testing.T) {
		f := newTenantLimitsFixture(t, tenantlimit.Limits{MaxPayloadBytes: 16})
		defer f.close()
		subscriber := f.connect(t, "tenant-a", "payload-sub", true)
		defer subscriber.Close()
		writePacket(t, subscriber, f.encoder, &mqtt.Subscribe{Version: mqtt.ProtocolV5, PacketID: 1, Subscriptions: []mqtt.Subscription{{Filter: "tenant-a/payload", QoS: mqtt.QoSAtLeastOnce}}})
		_ = readPacket(t, subscriber, f.decoder, mqtt.ProtocolV5).(*mqtt.SubAck)
		publisher := f.connect(t, "tenant-a", "payload-pub", true)
		defer publisher.Close()
		writePacket(t, publisher, f.encoder, &mqtt.Publish{Version: mqtt.ProtocolV5, QoS: mqtt.QoSAtLeastOnce, PacketID: 1, Topic: "tenant-a/payload", Payload: []byte(strings.Repeat("x", 17))})
		ack := readPacket(t, publisher, f.decoder, mqtt.ProtocolV5).(*mqtt.PubAck)
		if ack.ReasonCode != 0x97 {
			t.Fatalf("oversize PUBACK reason = %#x, want 0x97", ack.ReasonCode)
		}
		expectNoPacket(t, subscriber)
		f.assertMetric(t, "tenant-a", "payload_size", 1)
	})

	t.Run("RateWaitThenReject", func(t *testing.T) {
		// Multiple connections reserve together: sequential publishes refill while blocked on ACK.
		f := newTenantLimitsFixtureWithWait(t, tenantlimit.Limits{MessagesPerSecond: 2}, time.Second)
		defer f.close()
		pubA := f.connect(t, "tenant-a", "rate-a", true)
		defer pubA.Close()
		publishers := []net.Conn{pubA, f.connect(t, "tenant-a", "rate-a-2", true), f.connect(t, "tenant-a", "rate-a-3", true)}
		pubB := f.connect(t, "tenant-b", "rate-b", true)
		defer pubB.Close()
		for id := uint16(1); id <= 2; id++ {
			writePacket(t, pubA, f.encoder, &mqtt.Publish{Version: mqtt.ProtocolV5, QoS: mqtt.QoSAtLeastOnce, PacketID: mqtt.PacketIdentifier(id), Topic: fmt.Sprintf("tenant-a/rate/%d", id), Payload: []byte("x")})
			ack := readPacket(t, pubA, f.decoder, mqtt.ProtocolV5).(*mqtt.PubAck)
			if ack.ReasonCode != 0 {
				t.Fatalf("burst PUBACK = %#x, want success", ack.ReasonCode)
			}
		}
		started := time.Now()
		for index := range publishers {
			publisher := publishers[index]
			writePacket(t, publisher, f.encoder, &mqtt.Publish{Version: mqtt.ProtocolV5, QoS: mqtt.QoSAtLeastOnce, PacketID: mqtt.PacketIdentifier(index + 3), Topic: fmt.Sprintf("tenant-a/rate/concurrent/%d", index), Payload: []byte("x")})
		}
		writePacket(t, pubB, f.encoder, &mqtt.Publish{Version: mqtt.ProtocolV5, QoS: mqtt.QoSAtLeastOnce, PacketID: 1, Topic: "tenant-b/rate/ok", Payload: []byte("x")})
		otherAck := readPacket(t, pubB, f.decoder, mqtt.ProtocolV5).(*mqtt.PubAck)
		if otherAck.ReasonCode != 0 {
			t.Fatalf("tenant-b PUBACK = %#x, want success", otherAck.ReasonCode)
		}
		successfulWait, rejections := false, 0
		for _, publisher := range publishers {
			ack := readPacket(t, publisher, f.decoder, mqtt.ProtocolV5).(*mqtt.PubAck)
			switch ack.ReasonCode {
			case 0:
				if elapsed := time.Since(started); elapsed < 400*time.Millisecond {
					t.Fatalf("successful rate-limited ACK arrived after %s; want >=400ms", elapsed)
				}
				successfulWait = true
			case 0x97:
				rejections++
			default:
				t.Fatalf("tenant-a concurrent PUBACK reason = %#x", ack.ReasonCode)
			}
		}
		if elapsed := time.Since(started); elapsed < 400*time.Millisecond || !successfulWait {
			t.Fatalf("rate wait = %s, successful delayed publish=%v; want >=400ms", elapsed, successfulWait)
		}
		if rejections != 1 {
			t.Fatalf("rate rejections = %d, want exactly 1", rejections)
		}
		for _, publisher := range publishers[1:] {
			_ = publisher.Close()
		}
		f.assertMetric(t, "tenant-a", "message_rate", 1)
	})

	t.Run("OtherTenantUnaffected", func(t *testing.T) {
		f := newTenantLimitsFixtureWithWait(t, tenantlimit.Limits{MessagesPerSecond: 1}, 0)
		defer f.close()
		pubA := f.connect(t, "tenant-a", "isolated-a", true)
		defer pubA.Close()
		writePacket(t, pubA, f.encoder, &mqtt.Publish{Version: mqtt.ProtocolV5, QoS: mqtt.QoSAtLeastOnce, PacketID: 1, Topic: "tenant-a/isolated/1", Payload: []byte("x")})
		_ = readPacket(t, pubA, f.decoder, mqtt.ProtocolV5).(*mqtt.PubAck)
		writePacket(t, pubA, f.encoder, &mqtt.Publish{Version: mqtt.ProtocolV5, QoS: mqtt.QoSAtLeastOnce, PacketID: 2, Topic: "tenant-a/isolated/2", Payload: []byte("x")})
		// Tenant-a is refused without waiting; tenant-b must still make progress.
		pubB := f.connect(t, "tenant-b", "isolated-b", true)
		defer pubB.Close()
		writePacket(t, pubB, f.encoder, &mqtt.Publish{Version: mqtt.ProtocolV5, QoS: mqtt.QoSAtLeastOnce, PacketID: 1, Topic: "tenant-b/isolated", Payload: []byte("x")})
		ack := readPacket(t, pubB, f.decoder, mqtt.ProtocolV5).(*mqtt.PubAck)
		if ack.ReasonCode != 0 {
			t.Fatalf("tenant-b PUBACK = %#x, want success", ack.ReasonCode)
		}
		refused := readPacket(t, pubA, f.decoder, mqtt.ProtocolV5).(*mqtt.PubAck)
		if refused.ReasonCode != 0x97 {
			t.Fatalf("tenant-a PUBACK = %#x, want 0x97", refused.ReasonCode)
		}
		f.assertMetric(t, "tenant-a", "message_rate", 1)
	})
}

type tenantLimitsFixture struct {
	listener   net.Listener
	cancel     context.CancelFunc
	done       chan error
	encoder    *mqtt.Encoder
	decoder    *mqtt.Decoder
	registry   *metricsadapter.Registry
	tokens     map[string][]byte
	repository *sessionstore.MemorySessionRepository
}

func newTenantLimitsFixture(t *testing.T, limits tenantlimit.Limits) *tenantLimitsFixture {
	return newTenantLimitsFixtureWithWait(t, limits, time.Second)
}

func newTenantLimitsFixtureWithWait(t *testing.T, limits tenantlimit.Limits, maxWait time.Duration) *tenantLimitsFixture {
	t.Helper()
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	privateJWK, err := jwk.Import(privateKey)
	if err != nil {
		t.Fatal(err)
	}
	_ = privateJWK.Set("kid", "tenant-limit-key")
	_ = privateJWK.Set("alg", "RS256")
	publicJWK, err := jwk.PublicKeyOf(privateJWK)
	if err != nil {
		t.Fatal(err)
	}
	_ = publicJWK.Set("kid", "tenant-limit-key")
	_ = publicJWK.Set("alg", "RS256")
	set := jwk.NewSet()
	_ = set.AddKey(publicJWK)
	jwks, err := json.Marshal(set)
	if err != nil {
		t.Fatal(err)
	}
	jwksServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(jwks)
	}))
	t.Cleanup(jwksServer.Close)

	tenantA, _ := tenancy.NewTenantID("tenant-a")
	tenantB, _ := tenancy.NewTenantID("tenant-b")
	makeToken := func(subject string) []byte {
		tok := jwt.New()
		_ = tok.Set("iss", "https://tenant-limits.example")
		_ = tok.Set("aud", []string{"narumq"})
		_ = tok.Set("sub", subject)
		_ = tok.Set("exp", time.Now().Add(time.Hour))
		signed, err := jwt.Sign(tok, jwt.WithKey(jwa.RS256(), privateJWK))
		if err != nil {
			t.Fatal(err)
		}
		return signed
	}
	clientIDs := []string{"conn-one", "conn-two", "conn-three", "persistent", "persistent-2", "persistent-3", "new-session", "sub-a", "sub-b", "payload-sub", "payload-pub", "rate-a", "rate-a-2", "rate-a-3", "rate-b", "isolated-a", "isolated-b", "other"}
	bindings := []authenticationadapter.IdentityBinding{
		{Issuer: "https://tenant-limits.example", Subject: "subject-a", TenantID: tenantA, CredentialID: "credential-a", AllowedClientIDs: clientIDs, Roles: []security.Role{"tenant-limit-client"}},
		{Issuer: "https://tenant-limits.example", Subject: "subject-b", TenantID: tenantB, CredentialID: "credential-b", AllowedClientIDs: clientIDs, Roles: []security.Role{"tenant-limit-client"}},
	}
	auth, err := authenticationadapter.NewJWKSAuthenticationProvider(authenticationadapter.Config{Issuer: "https://tenant-limits.example", Audience: "narumq", Algorithms: []string{"RS256"}, JWKSURL: jwksServer.URL, CacheTTL: time.Minute, RefreshCooldown: time.Second, ClockSkew: time.Second, HTTPTimeout: time.Second, HTTPClient: &http.Client{Timeout: time.Second}, Clock: clock.SystemClock{}, Bindings: bindings})
	if err != nil {
		t.Fatal(err)
	}
	filterA, _ := tenancy.NewTopicFilter(tenantA, "#")
	filterB, _ := tenancy.NewTopicFilter(tenantB, "#")
	rules := []security.Rule{}
	for _, tenantFilter := range []struct {
		tenant tenancy.TenantID
		filter tenancy.TopicFilter
	}{{tenantA, filterA}, {tenantB, filterB}} {
		for _, action := range []security.Action{security.ActionPublish, security.ActionSubscribe} {
			rule, ruleErr := security.NewRule(tenantFilter.tenant, "tenant-limit-client", security.EffectAllow, action, tenantFilter.filter)
			if ruleErr != nil {
				t.Fatal(ruleErr)
			}
			rules = append(rules, rule)
		}
	}
	policy, err := security.NewPolicy(rules)
	if err != nil {
		t.Fatal(err)
	}
	registry := metricsadapter.NewRegistry()
	profile := tenantlimit.Profile{MaxWait: maxWait, Default: limits, Tenants: map[tenancy.TenantID]tenantlimit.Limits{tenantB: {}}}
	service, err := tenantlimit.New(clock.SystemClock{}, profile, tenantLimitMetricReporter{registry: registry})
	if err != nil {
		t.Fatal(err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	routes := routing.NewTable()
	deliveries, err := deliveryqueue.New(64)
	if err != nil {
		t.Fatal(err)
	}
	retained, err := memoryretained.New(64, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	repository := sessionstore.New()
	willBridge := tcpadapter.NewWillBridge()
	connections, err := connect.New(clock.SystemClock{}, repository, deliveries, routes, clock.SystemScheduler{}, willBridge, connect.WithSecurity(auth, nil, policy, "jwt"), connect.WithTenantLimits(service, repository))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := connections.Close(); err != nil {
			t.Error(err)
		}
	})
	subscriptions, err := subscribe.New(routes, deliveries, retained, subscribe.WithPolicy(policy), subscribe.WithTenantLimits(service))
	if err != nil {
		t.Fatal(err)
	}
	publications, err := publish.New(clock.SystemClock{}, routes, deliveries, retained, publish.WithPolicy(policy))
	if err != nil {
		t.Fatal(err)
	}
	decoder, err := mqtt.NewDecoder(1 << 20)
	if err != nil {
		t.Fatal(err)
	}
	encoder, err := mqtt.NewEncoder(1 << 20)
	if err != nil {
		t.Fatal(err)
	}
	server, err := tcpadapter.NewServer(listener, tcpadapter.Config{MaxPacketSize: 1 << 20, Clock: clock.SystemClock{}, ConnectTimeout: time.Second, WriteTimeout: time.Second, ControlQueueCapacity: 16, ReceiveMaximum: 64, PacketReadTimeout: 10 * time.Second, Policy: policy, TenantLimits: service}, decoder, encoder, connections, subscriptions, publications, deliveries, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	willBridge.Bind(server)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx) }()
	fixture := &tenantLimitsFixture{listener: listener, cancel: cancel, done: done, encoder: encoder, decoder: decoder, registry: registry, repository: repository, tokens: map[string][]byte{"tenant-a": makeToken("subject-a"), "tenant-b": makeToken("subject-b")}}
	return fixture
}

type tenantLimitMetricReporter struct{ registry *metricsadapter.Registry }

func (reporter tenantLimitMetricReporter) TenantLimitRejected(tenantID tenancy.TenantID, dimension tenantlimit.Dimension) {
	reporter.registry.RecordTenantLimitRejection(tenantID.String(), string(dimension))
}

func (f *tenantLimitsFixture) connect(t *testing.T, tenant, clientID string, clean bool, wantPresent ...bool) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", f.listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	writePacket(t, conn, f.encoder, &mqtt.Connect{Version: mqtt.ProtocolV5, ClientID: clientID, CleanStart: clean, SessionExpiryInterval: 60, HasUsername: true, HasPassword: true, Password: f.tokens[tenant]})
	ack := readPacket(t, conn, f.decoder, mqtt.ProtocolV5).(*mqtt.ConnAck)
	if ack.ReasonCode != 0 {
		_ = conn.Close()
		t.Fatalf("%s CONNECT reason = %#x, want success", tenant, ack.ReasonCode)
	}
	if len(wantPresent) > 0 && ack.SessionPresent != wantPresent[0] {
		_ = conn.Close()
		t.Fatalf("%s CONNECT SessionPresent=%t, want %t", clientID, ack.SessionPresent, wantPresent[0])
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func checkTenantConnAck(t *testing.T, f *tenantLimitsFixture, tenant, clientID string, clean bool, want byte) {
	t.Helper()
	conn, err := net.Dial("tcp", f.listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	writePacket(t, conn, f.encoder, &mqtt.Connect{Version: mqtt.ProtocolV5, ClientID: clientID, CleanStart: clean, SessionExpiryInterval: 60, HasUsername: true, HasPassword: true, Password: f.tokens[tenant]})
	ack := readPacket(t, conn, f.decoder, mqtt.ProtocolV5).(*mqtt.ConnAck)
	if ack.ReasonCode != want {
		t.Fatalf("%s CONNECT reason = %#x, want %#x", tenant, ack.ReasonCode, want)
	}
}

func (f *tenantLimitsFixture) assertMetric(t *testing.T, tenant, dimension string, want int) {
	t.Helper()
	metric := fmt.Sprintf("narumq_tenant_limit_rejections_total{tenant=%q,dimension=%q} %d\n", tenant, dimension, want)
	if rendered := string(f.registry.Render(false)); !strings.Contains(rendered, metric) {
		t.Fatalf("metrics missing %q", metric)
	}
}

func (f *tenantLimitsFixture) close() {
	f.cancel()
	_ = f.listener.Close()
	<-f.done
}

func (f *tenantLimitsFixture) waitConnectedCount(t *testing.T, tenant string, want int) {
	t.Helper()
	tenantID, err := tenancy.NewTenantID(tenant)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		_, connected := f.repository.TenantSessionCounts(tenantID)
		if connected == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("connected lifecycles=%d, want %d", connected, want)
		}
		time.Sleep(time.Millisecond)
	}
}
