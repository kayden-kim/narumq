package integration

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"io"
	"log/slog"
	"math/big"
	authentication "narumq/internal/adapters/authentication"
	clock "narumq/internal/adapters/clock"
	"narumq/internal/adapters/memory/deliveryqueue"
	memoryretained "narumq/internal/adapters/memory/retained"
	"narumq/internal/adapters/memory/sessionstore"
	metricsadapter "narumq/internal/adapters/metrics"
	tcpadapter "narumq/internal/adapters/tcp"
	tlsadapter "narumq/internal/adapters/tls"
	websocketadapter "narumq/internal/adapters/websocket"
	"narumq/internal/application/connect"
	authenticationports "narumq/internal/application/ports/authentication"
	portsmetrics "narumq/internal/application/ports/metrics"
	"narumq/internal/application/publish"
	"narumq/internal/application/subscribe"
	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/routing"
	"narumq/internal/domain/security"
	"narumq/internal/domain/tenancy"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type testCert struct {
	certPEM []byte
	keyPEM  []byte
	cert    *x509.Certificate
	key     *ecdsa.PrivateKey
}

func generateIntegrationCA(t *testing.T, commonName string) testCert {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate CA key: %v", err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		t.Fatalf("generate serial: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName:   commonName,
			Organization: []string{"NaruMQ Integration Test"},
		},
		NotBefore:             time.Now().Add(-1 * time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("create CA cert: %v", err)
	}
	parsed, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse CA cert: %v", err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyBytes, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		t.Fatalf("marshal CA key: %v", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyBytes})
	return testCert{certPEM: certPEM, keyPEM: keyPEM, cert: parsed, key: priv}
}

func generateIntegrationServerCert(t *testing.T, ca testCert, hosts []string) testCert {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate server key: %v", err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		t.Fatalf("generate serial: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName:   "localhost",
			Organization: []string{"NaruMQ Integration Test"},
		},
		NotBefore:   time.Now().Add(-1 * time.Hour),
		NotAfter:    time.Now().Add(24 * time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	for _, host := range hosts {
		if ip := net.ParseIP(host); ip != nil {
			template.IPAddresses = append(template.IPAddresses, ip)
		} else {
			template.DNSNames = append(template.DNSNames, host)
		}
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca.cert, &priv.PublicKey, ca.key)
	if err != nil {
		t.Fatalf("create server cert: %v", err)
	}
	parsed, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse server cert: %v", err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyBytes, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		t.Fatalf("marshal server key: %v", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyBytes})
	return testCert{certPEM: certPEM, keyPEM: keyPEM, cert: parsed, key: priv}
}

func generateIntegrationClientCert(t *testing.T, ca testCert, commonName string, expired bool) testCert {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate client key: %v", err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		t.Fatalf("generate serial: %v", err)
	}
	notBefore := time.Now().Add(-1 * time.Hour)
	notAfter := time.Now().Add(24 * time.Hour)
	if expired {
		notBefore = time.Now().Add(-48 * time.Hour)
		notAfter = time.Now().Add(-24 * time.Hour)
	}
	template := &x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName:   commonName,
			Organization: []string{"NaruMQ Integration Test"},
		},
		NotBefore:   notBefore,
		NotAfter:    notAfter,
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca.cert, &priv.PublicKey, ca.key)
	if err != nil {
		t.Fatalf("create client cert: %v", err)
	}
	parsed, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse client cert: %v", err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyBytes, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		t.Fatalf("marshal client key: %v", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyBytes})
	return testCert{certPEM: certPEM, keyPEM: keyPEM, cert: parsed, key: priv}
}

type tlsFixtureOptions struct {
	ClientAuth       tlsadapter.ClientAuthMode
	CAFile           string
	MinVersion       string
	CipherSuites     []string
	MTLSIdentity     string
	PasswordBindings []authentication.PasswordIdentityBinding
	Blocklist        []security.BlockEntry
	CA               *testCert
	DualTCP          bool
	WS               bool
	WSS              bool
	WSMaxConnections int
	Metrics          *metricsadapter.Registry
	SecurityPolicy   *security.Policy
	LogOutput        io.Writer
}

type staticTLSBlocklistMatcher struct {
	list *security.Blocklist
}

func (matcher staticTLSBlocklistMatcher) MatchBlocklist(subject security.BlockSubject) (security.BlockKind, bool) {
	return matcher.list.Match(subject)
}

type tlsFixture struct {
	server      *tcpadapter.Server
	tlsListener net.Listener
	tcpListener net.Listener
	wsListener  net.Listener
	wssListener net.Listener
	tlsAddr     string
	tcpAddr     string
	wsAddr      string
	wssAddr     string
	ca          testCert
	serverCert  testCert
	cancel      context.CancelFunc
	done        chan error
	encoder     *mqtt.Encoder
	decoder     *mqtt.Decoder
}

func newTLSFixture(t *testing.T, opts tlsFixtureOptions) *tlsFixture {
	t.Helper()
	tempDir := t.TempDir()
	ca := generateIntegrationCA(t, "NaruMQ Test Root CA")
	if opts.CA != nil {
		ca = *opts.CA
	}
	serverCert := generateIntegrationServerCert(t, ca, []string{"127.0.0.1", "localhost"})

	certPath := filepath.Join(tempDir, "server.crt")
	keyPath := filepath.Join(tempDir, "server.key")
	if err := os.WriteFile(certPath, serverCert.certPEM, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyPath, serverCert.keyPEM, 0600); err != nil {
		t.Fatal(err)
	}

	caPath := opts.CAFile
	if opts.ClientAuth == tlsadapter.ClientAuthRequest || opts.ClientAuth == tlsadapter.ClientAuthRequire {
		if caPath == "" {
			caPath = filepath.Join(tempDir, "ca.crt")
			if err := os.WriteFile(caPath, ca.certPEM, 0600); err != nil {
				t.Fatal(err)
			}
		}
	}

	minVer := opts.MinVersion
	if minVer == "" {
		minVer = "1.2"
	}

	tlsCfg, err := tlsadapter.BuildTLSConfig(tlsadapter.Config{
		CertFile:     certPath,
		KeyFile:      keyPath,
		CAFile:       caPath,
		ClientAuth:   opts.ClientAuth,
		MinVersion:   minVer,
		CipherSuites: opts.CipherSuites,
	})
	if err != nil {
		t.Fatalf("build TLS config: %v", err)
	}

	inner, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen TCP for TLS: %v", err)
	}
	tlsListener, err := tlsadapter.NewListener(inner, tlsCfg)
	if err != nil {
		_ = inner.Close()
		t.Fatalf("listen TLS: %v", err)
	}

	var wsListener, wssListener net.Listener
	if opts.WS || opts.WSS {
		if opts.Metrics == nil {
			opts.Metrics = metricsadapter.NewRegistry()
		}
	}
	if opts.WS {
		wsInner, listenErr := net.Listen("tcp", "127.0.0.1:0")
		if listenErr != nil {
			tlsListener.Close()
			t.Fatalf("listen WS: %v", listenErr)
		}
		if opts.WSMaxConnections > 0 {
			wsInner, listenErr = tcpadapter.NewAdmissionListener(wsInner, tcpadapter.ListenerAdmissionConfig{
				Transport: portsmetrics.TransportWS, MaxConnections: opts.WSMaxConnections,
				Clock: clock.SystemClock{}, Recorder: opts.Metrics,
			})
			if listenErr != nil {
				t.Fatal(listenErr)
			}
		}
		wsListener, listenErr = websocketadapter.NewListener(wsInner, websocketadapter.Config{
			HandshakeTimeout: websocketadapter.HandshakeTimeout, Logger: slog.Default(),
		})
		if listenErr != nil {
			wsInner.Close()
			t.Fatalf("listen WS: %v", listenErr)
		}
	}
	if opts.WSS {
		wssInner, listenErr := net.Listen("tcp", "127.0.0.1:0")
		if listenErr != nil {
			tlsListener.Close()
			if wsListener != nil {
				wsListener.Close()
			}
			t.Fatalf("listen WSS: %v", listenErr)
		}
		wssTLS, listenErr := tlsadapter.NewListener(wssInner, tlsCfg)
		if listenErr != nil {
			wssInner.Close()
			t.Fatalf("listen WSS TLS: %v", listenErr)
		}
		wssListener, listenErr = websocketadapter.NewListener(wssTLS, websocketadapter.Config{
			HandshakeTimeout: websocketadapter.HandshakeTimeout, Logger: slog.Default(),
		})
		if listenErr != nil {
			wssTLS.Close()
			t.Fatalf("listen WSS: %v", listenErr)
		}
	}

	var tcpListener net.Listener
	if opts.DualTCP {
		var err error
		tcpListener, err = net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			tlsListener.Close()
			t.Fatalf("listen TCP: %v", err)
		}
	}

	routes := routing.NewTable()
	deliveries, err := deliveryqueue.New(64)
	if err != nil {
		tlsListener.Close()
		if tcpListener != nil {
			tcpListener.Close()
		}
		t.Fatal(err)
	}
	retainedRepo, err := memoryretained.New(1024, 1<<20)
	if err != nil {
		tlsListener.Close()
		if tcpListener != nil {
			tcpListener.Close()
		}
		t.Fatal(err)
	}
	repository := sessionstore.New()
	willBridge := tcpadapter.NewWillBridge()
	systemClock := clock.SystemClock{}
	var (
		connectOpts        []connect.Option
		subscribeOpts      []subscribe.Option
		publishOpts        []publish.Option
		credentialProvider authenticationports.AuthenticationProvider
		credentialMethod   string
		mtlsProvider       *authentication.MTLSAuthenticationProvider
	)
	if len(opts.PasswordBindings) > 0 {
		provider, err := authentication.NewPasswordAuthenticationProvider(opts.PasswordBindings)
		if err != nil {
			t.Fatalf("create password provider: %v", err)
		}
		credentialProvider = provider
		credentialMethod = portsmetrics.AuthMethodPassword
	}
	if opts.MTLSIdentity != "" {
		tenantID, err := tenancy.NewTenantID("tenant-a")
		if err != nil {
			t.Fatalf("create mTLS tenant ID: %v", err)
		}
		mtlsProvider, err = authentication.NewMTLSAuthenticationProvider(systemClock, []authentication.ClientCertificateIdentityBinding{{
			Kind:             authentication.ClientCertificateIdentityKindCommonName,
			Value:            opts.MTLSIdentity,
			TenantID:         tenantID,
			Subject:          opts.MTLSIdentity,
			CredentialID:     opts.MTLSIdentity + "-cert",
			AllowedClientIDs: []string{opts.MTLSIdentity},
			Roles:            []security.Role{security.Role(opts.MTLSIdentity)},
		}})
		if err != nil {
			t.Fatalf("create mTLS provider: %v", err)
		}
	}
	if credentialProvider != nil || mtlsProvider != nil {
		policy := security.Policy{}
		if opts.SecurityPolicy != nil {
			policy = *opts.SecurityPolicy
		}
		connectOpts = append(connectOpts, connect.WithSecurity(credentialProvider, mtlsProvider, policy, credentialMethod))
		subscribeOpts = append(subscribeOpts, subscribe.WithPolicy(policy))
		publishOpts = append(publishOpts, publish.WithPolicy(policy))
	}
	if len(opts.Blocklist) > 0 {
		blocklist := security.NewBlocklist()
		for _, entry := range opts.Blocklist {
			blocklist.Set(entry, true)
		}
		connectOpts = append(connectOpts, connect.WithBlocklistMatcher(staticTLSBlocklistMatcher{list: blocklist}))
	}
	connections, err := connect.New(systemClock, repository, deliveries, routes, clock.SystemScheduler{}, willBridge, connectOpts...)
	if err != nil {
		tlsListener.Close()
		if tcpListener != nil {
			tcpListener.Close()
		}
		t.Fatal(err)
	}
	subscriptions, err := subscribe.New(routes, deliveries, retainedRepo, subscribeOpts...)
	if err != nil {
		tlsListener.Close()
		if tcpListener != nil {
			tcpListener.Close()
		}
		t.Fatal(err)
	}
	publications, err := publish.New(systemClock, routes, deliveries, retainedRepo, publishOpts...)
	if err != nil {
		tlsListener.Close()
		if tcpListener != nil {
			tcpListener.Close()
		}
		t.Fatal(err)
	}
	decoder, err := mqtt.NewDecoder(1048576)
	if err != nil {
		tlsListener.Close()
		if tcpListener != nil {
			tcpListener.Close()
		}
		t.Fatal(err)
	}
	encoder, err := mqtt.NewEncoder(1048576)
	if err != nil {
		tlsListener.Close()
		if tcpListener != nil {
			tcpListener.Close()
		}
		t.Fatal(err)
	}

	primaryListener := tlsListener
	if tcpListener != nil {
		primaryListener = tcpListener
	}

	var logOutput io.Writer = io.Discard
	if opts.LogOutput != nil {
		logOutput = opts.LogOutput
	}
	var metricRecorder portsmetrics.MetricsRecorder
	if opts.Metrics != nil {
		metricRecorder = opts.Metrics
	}
	server, err := tcpadapter.NewServer(
		primaryListener,
		tcpadapter.Config{
			MaxPacketSize:        1048576,
			Clock:                systemClock,
			ConnectTimeout:       time.Second,
			WriteTimeout:         time.Second,
			ControlQueueCapacity: 16,
			ReceiveMaximum:       64, PacketReadTimeout: 10 * time.Second,
			ClientCertificateExtractor: tlsadapter.ExtractVerifiedClientCertificateDTO,
			Metrics:                    metricRecorder,
		},
		decoder,
		encoder,
		connections,
		subscriptions,
		publications,
		deliveries,
		slog.New(slog.NewTextHandler(logOutput, nil)),
	)
	if err != nil {
		tlsListener.Close()
		if tcpListener != nil {
			tcpListener.Close()
		}
		if wsListener != nil {
			wsListener.Close()
		}
		if wssListener != nil {
			wssListener.Close()
		}
		t.Fatal(err)
	}
	listeners := []net.Listener{tlsListener}
	if tcpListener != nil {
		listeners = append(listeners, tcpListener)
	}
	if wsListener != nil {
		listeners = append(listeners, wsListener)
	}
	if wssListener != nil {
		listeners = append(listeners, wssListener)
	}
	for _, listener := range listeners {
		if listener == primaryListener {
			continue
		}
		if err := server.AddListener(listener); err != nil {
			for _, opened := range listeners {
				_ = opened.Close()
			}
			t.Fatal(err)
		}
	}
	willBridge.Bind(server)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx) }()

	var tcpAddr, wsAddr, wssAddr string
	if tcpListener != nil {
		tcpAddr = tcpListener.Addr().String()
	}
	if wsListener != nil {
		wsAddr = wsListener.Addr().String()
	}
	if wssListener != nil {
		wssAddr = wssListener.Addr().String()
	}

	return &tlsFixture{
		server:      server,
		tlsListener: tlsListener,
		tcpListener: tcpListener,
		wsListener:  wsListener,
		wssListener: wssListener,
		tlsAddr:     tlsListener.Addr().String(),
		tcpAddr:     tcpAddr,
		wsAddr:      wsAddr,
		wssAddr:     wssAddr,
		ca:          ca,
		serverCert:  serverCert,
		cancel:      cancel,
		done:        done,
		encoder:     encoder,
		decoder:     decoder,
	}
}

func (f *tlsFixture) close(t *testing.T) {
	t.Helper()
	f.cancel()
	select {
	case err := <-f.done:
		if err != nil {
			t.Fatalf("Serve() error = %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Serve() did not stop")
	}
	for _, listener := range []net.Listener{f.wsListener, f.wssListener} {
		if listener != nil {
			_ = listener.Close()
		}
	}
}

func (f *tlsFixture) dialTLS(t *testing.T, clientCert *testCert, configModifier func(*tls.Config)) net.Conn {
	t.Helper()
	rootPool := x509.NewCertPool()
	rootPool.AddCert(f.ca.cert)

	tlsClientConfig := &tls.Config{
		RootCAs:    rootPool,
		ServerName: "127.0.0.1",
	}
	if clientCert != nil {
		cert, err := tls.X509KeyPair(clientCert.certPEM, clientCert.keyPEM)
		if err != nil {
			t.Fatalf("load client cert pair: %v", err)
		}
		tlsClientConfig.Certificates = []tls.Certificate{cert}
	}
	if configModifier != nil {
		configModifier(tlsClientConfig)
	}

	conn, err := tls.DialWithDialer(&net.Dialer{Timeout: time.Second}, "tcp", f.tlsAddr, tlsClientConfig)
	if err != nil {
		t.Fatalf("dial TLS on %s: %v", f.tlsAddr, err)
	}
	return conn
}

// TestMQTTTLS_ConnectionAndDelivery verifies full MQTT CONNECT -> SUBSCRIBE -> PUBLISH
// roundtrip over a TLS 1.2 and TLS 1.3 encrypted socket.
func TestMQTTTLS_ConnectionAndDelivery(t *testing.T) {
	fixture := newTLSFixture(t, tlsFixtureOptions{})
	defer fixture.close(t)

	versions := []struct {
		name    string
		version mqtt.ProtocolVersion
	}{
		{name: "MQTT 5", version: mqtt.ProtocolV5},
		{name: "MQTT 3.1.1", version: mqtt.ProtocolV311},
	}

	for _, tc := range versions {
		t.Run(tc.name, func(t *testing.T) {
			subConn := fixture.dialTLS(t, nil, nil)
			defer subConn.Close()

			subClientID := "subscriber-" + tc.name
			writePacket(t, subConn, fixture.encoder, &mqtt.Connect{
				Version:    tc.version,
				CleanStart: true,
				ClientID:   subClientID,
			})
			connackPacket := readPacket(t, subConn, fixture.decoder, tc.version)
			connack, ok := connackPacket.(*mqtt.ConnAck)
			if !ok || connack.ReasonCode != 0 {
				t.Fatalf("expected accepted ConnAck, got %#v", connackPacket)
			}

			topicFilter := "tls/delivery/" + tc.name
			writePacket(t, subConn, fixture.encoder, &mqtt.Subscribe{
				Version:  tc.version,
				PacketID: 1,
				Subscriptions: []mqtt.Subscription{
					{Filter: topicFilter, QoS: mqtt.QoSAtMostOnce},
				},
			})
			subackPacket := readPacket(t, subConn, fixture.decoder, tc.version)
			suback, ok := subackPacket.(*mqtt.SubAck)
			if !ok || len(suback.ReasonCodes) != 1 || suback.ReasonCodes[0] != 0 {
				t.Fatalf("expected accepted SubAck, got %#v", subackPacket)
			}

			pubConn := fixture.dialTLS(t, nil, nil)
			defer pubConn.Close()

			pubClientID := "publisher-" + tc.name
			writePacket(t, pubConn, fixture.encoder, &mqtt.Connect{
				Version:    tc.version,
				CleanStart: true,
				ClientID:   pubClientID,
			})
			pubConnackPacket := readPacket(t, pubConn, fixture.decoder, tc.version)
			if _, ok := pubConnackPacket.(*mqtt.ConnAck); !ok {
				t.Fatalf("publisher expected ConnAck, got %#v", pubConnackPacket)
			}

			payload := []byte("encrypted-payload-" + tc.name)
			writePacket(t, pubConn, fixture.encoder, &mqtt.Publish{
				Version: tc.version,
				Topic:   topicFilter,
				Payload: payload,
				QoS:     mqtt.QoSAtMostOnce,
			})

			deliveredPacket := readPacket(t, subConn, fixture.decoder, tc.version)
			publishMsg, ok := deliveredPacket.(*mqtt.Publish)
			if !ok {
				t.Fatalf("expected Publish, got %#v", deliveredPacket)
			}
			if publishMsg.Topic != topicFilter {
				t.Errorf("expected topic %q, got %q", topicFilter, publishMsg.Topic)
			}
			if string(publishMsg.Payload) != string(payload) {
				t.Errorf("expected payload %q, got %q", string(payload), string(publishMsg.Payload))
			}
		})
	}
}

// Traceability: SEC-05; docs/specs/tenancy-and-security.md §2.3, §2.4, §9.
func TestMQTTTLS_PasswordAuthentication(t *testing.T) {
	password := []byte("SEC05-tls-password")
	encodedHash, err := authentication.HashPassword(password)
	if err != nil {
		t.Fatalf("hash TLS test password: %v", err)
	}
	passwordHash, err := authentication.ParsePasswordHash(encodedHash)
	if err != nil {
		t.Fatalf("parse TLS test password hash: %v", err)
	}
	tenantID, err := tenancy.NewTenantID("tenant-a")
	if err != nil {
		t.Fatalf("create TLS password tenant ID: %v", err)
	}
	fixture := newTLSFixture(t, tlsFixtureOptions{
		ClientAuth:   tlsadapter.ClientAuthRequest,
		MTLSIdentity: "tls-cert-client",
		PasswordBindings: []authentication.PasswordIdentityBinding{{
			Username: "tls-password-user", PasswordHash: passwordHash, TenantID: tenantID,
			Subject: "tls-password-device", CredentialID: "tls-password-credential",
			AllowedClientIDs: []string{"tls-password-client"}, Roles: []security.Role{"publisher"},
		}},
	})
	defer fixture.close(t)

	connect := func(clientCert *testCert, version mqtt.ProtocolVersion, username string, suppliedPassword []byte, hasUsername, hasPassword bool, clientID string) byte {
		t.Helper()
		connection := fixture.dialTLS(t, clientCert, nil)
		defer connection.Close()
		if err := connection.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
			t.Fatalf("set TLS MQTT deadline: %v", err)
		}
		writePacket(t, connection, fixture.encoder, &mqtt.Connect{
			Version: version, CleanStart: true, ClientID: clientID,
			HasUsername: hasUsername, Username: username,
			HasPassword: hasPassword, Password: suppliedPassword,
		})
		packet := readPacket(t, connection, fixture.decoder, version)
		connack, ok := packet.(*mqtt.ConnAck)
		if !ok {
			t.Fatalf("MQTT %d TLS CONNECT response = %#v, want CONNACK", version, packet)
		}
		return connack.ReasonCode
	}

	certificate := generateIntegrationClientCert(t, fixture.ca, "tls-cert-client", false)
	for _, version := range []mqtt.ProtocolVersion{mqtt.ProtocolV311, mqtt.ProtocolV5} {
		wantRejectReason := byte(0x87)
		if version == mqtt.ProtocolV311 {
			wantRejectReason = 0x05
		}
		if reason := connect(nil, version, "tls-password-user", password, true, true, "tls-password-client"); reason != 0 {
			t.Fatalf("MQTT %d TLS password CONNACK reason=0x%02x, want success", version, reason)
		}
		wrongReason := connect(nil, version, "tls-password-user", []byte("SEC05-wrong-password"), true, true, "tls-password-client")
		unknownReason := connect(nil, version, "tls-unknown-user", password, true, true, "tls-password-client")
		if wrongReason != wantRejectReason || unknownReason != wrongReason {
			t.Fatalf("MQTT %d TLS wrong-password reason=0x%02x, unknown-user reason=0x%02x; want same 0x%02x", version, wrongReason, unknownReason, wantRejectReason)
		}
		if reason := connect(nil, version, "", nil, false, false, "tls-password-client"); reason != wantRejectReason {
			t.Fatalf("MQTT %d TLS missing credentials CONNACK reason=0x%02x, want 0x%02x", version, reason, wantRejectReason)
		}
		if reason := connect(&certificate, version, "", nil, false, false, "tls-cert-client"); reason != 0 {
			t.Fatalf("MQTT %d TLS mTLS client without MQTT credentials CONNACK reason=0x%02x, want success", version, reason)
		}
		if reason := connect(&certificate, version, "tls-password-user", password, true, true, "tls-cert-client"); reason != wantRejectReason {
			t.Fatalf("MQTT %d TLS client with certificate and password credentials reason=0x%02x, want 0x%02x", version, reason, wantRejectReason)
		}
	}
}

// TestMQTTTLS_RejectInsecureTLSVersion verifies that client connections attempting
// TLS 1.0 or TLS 1.1 are immediately rejected during the TLS handshake ([SEC-01]).
// Traceability: SEC-01; docs/specs/tenancy-and-security.md §9.
func TestMQTTTLS_RejectInsecureTLSVersion(t *testing.T) {
	fixture := newTLSFixture(t, tlsFixtureOptions{MinVersion: "1.2"})
	defer fixture.close(t)

	rootPool := x509.NewCertPool()
	rootPool.AddCert(fixture.ca.cert)

	insecureTLSConfigs := []struct {
		name       string
		maxVersion uint16
	}{
		{name: "TLS 1.0", maxVersion: tls.VersionTLS10},
		{name: "TLS 1.1", maxVersion: tls.VersionTLS11},
	}

	for _, tc := range insecureTLSConfigs {
		t.Run(tc.name, func(t *testing.T) {
			clientConfig := &tls.Config{
				RootCAs:    rootPool,
				ServerName: "127.0.0.1",
				MaxVersion: tc.maxVersion,
			}
			conn, err := tls.DialWithDialer(&net.Dialer{Timeout: time.Second}, "tcp", fixture.tlsAddr, clientConfig)
			if err == nil {
				conn.Close()
				t.Fatalf("expected TLS handshake to fail for %s, but connection succeeded", tc.name)
			}
		})
	}
}

// TestMQTTTLS_mTLS_Success verifies that a client presenting a valid X.509 certificate
// signed by the trusted CA is mapped to its configured mTLS identity and authenticates
// successfully ([SEC-03]).
// Traceability: SEC-03; docs/specs/tenancy-and-security.md §9.
func TestMQTTTLS_mTLS_Success(t *testing.T) {
	fixture := newTLSFixture(t, tlsFixtureOptions{
		ClientAuth:   tlsadapter.ClientAuthRequire,
		MTLSIdentity: "sensor-client-01",
	})
	defer fixture.close(t)

	clientCert := generateIntegrationClientCert(t, fixture.ca, "sensor-client-01", false)

	conn := fixture.dialTLS(t, &clientCert, nil)
	defer conn.Close()

	writePacket(t, conn, fixture.encoder, &mqtt.Connect{
		Version:    mqtt.ProtocolV5,
		CleanStart: true,
		ClientID:   "sensor-client-01",
	})
	connackPacket := readPacket(t, conn, fixture.decoder, mqtt.ProtocolV5)
	connack, ok := connackPacket.(*mqtt.ConnAck)
	if !ok || connack.ReasonCode != 0 {
		t.Fatalf("expected accepted ConnAck, got %#v", connackPacket)
	}
}

// Traceability: SEC-12; docs/specs/tenancy-and-security.md §7.1, §9; ADR-0050.
func TestMQTTTLSBlockedCertificateFingerprintRejected(t *testing.T) {
	const identity = "sensor-client-01"
	ca := generateIntegrationCA(t, "NaruMQ Test Root CA")
	blockedCert := generateIntegrationClientCert(t, ca, identity, false)
	allowedCert := generateIntegrationClientCert(t, ca, identity, false)
	blockedFingerprint := sha256.Sum256(blockedCert.cert.Raw)
	allowedFingerprint := sha256.Sum256(allowedCert.cert.Raw)
	if blockedFingerprint == allowedFingerprint {
		t.Fatal("generated client certificates have identical fingerprints")
	}
	entry, err := security.NewBlockEntry(
		security.BlockKindCertificateSHA256,
		"",
		hex.EncodeToString(blockedFingerprint[:]),
	)
	if err != nil {
		t.Fatalf("create certificate fingerprint blocklist entry: %v", err)
	}

	fixture := newTLSFixture(t, tlsFixtureOptions{
		CA:           &ca,
		ClientAuth:   tlsadapter.ClientAuthRequire,
		MTLSIdentity: identity,
		Blocklist:    []security.BlockEntry{entry},
	})
	defer fixture.close(t)

	assertConnAck := func(clientCert *testCert, want byte) {
		conn := fixture.dialTLS(t, clientCert, nil)
		defer conn.Close()
		writePacket(t, conn, fixture.encoder, &mqtt.Connect{
			Version:    mqtt.ProtocolV5,
			CleanStart: true,
			ClientID:   identity,
		})
		packet := readPacket(t, conn, fixture.decoder, mqtt.ProtocolV5)
		connack, ok := packet.(*mqtt.ConnAck)
		if !ok || connack.ReasonCode != want {
			t.Fatalf("certificate fingerprint CONNACK = %#v, want reason 0x%02x", packet, want)
		}
	}
	assertConnAck(&blockedCert, 0x8A)
	assertConnAck(&allowedCert, 0)
}

// TestMQTTTLS_mTLS_UntrustedCA_Rejected verifies that a client presenting a certificate
// signed by an untrusted CA is rejected during the TLS handshake ([SEC-03]).
// Traceability: SEC-03; docs/specs/tenancy-and-security.md §9.
func TestMQTTTLS_mTLS_UntrustedCA_Rejected(t *testing.T) {
	fixture := newTLSFixture(t, tlsFixtureOptions{
		ClientAuth: tlsadapter.ClientAuthRequire,
	})
	defer fixture.close(t)

	untrustedCA := generateIntegrationCA(t, "Untrusted Rogue CA")
	clientCert := generateIntegrationClientCert(t, untrustedCA, "impostor-client", false)

	rootPool := x509.NewCertPool()
	rootPool.AddCert(fixture.ca.cert)

	certPair, err := tls.X509KeyPair(clientCert.certPEM, clientCert.keyPEM)
	if err != nil {
		t.Fatalf("load cert pair: %v", err)
	}

	clientConfig := &tls.Config{
		RootCAs:      rootPool,
		ServerName:   "127.0.0.1",
		Certificates: []tls.Certificate{certPair},
	}

	conn, err := tls.DialWithDialer(&net.Dialer{Timeout: time.Second}, "tcp", fixture.tlsAddr, clientConfig)
	if err == nil {
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(time.Second))
		_, _ = conn.Write([]byte{0x10, 0x02, 0x00, 0x00})
		buf := make([]byte, 10)
		_, readErr := conn.Read(buf)
		if readErr == nil {
			t.Fatal("expected handshake or read failure with untrusted client certificate, but succeeded")
		}
	}
}

// TestMQTTTLS_mTLS_ExpiredCert_Rejected verifies that a client presenting an expired
// certificate is rejected during the TLS handshake ([SEC-03]).
// Traceability: SEC-03; docs/specs/tenancy-and-security.md §9.
func TestMQTTTLS_mTLS_ExpiredCert_Rejected(t *testing.T) {
	fixture := newTLSFixture(t, tlsFixtureOptions{
		ClientAuth: tlsadapter.ClientAuthRequire,
	})
	defer fixture.close(t)

	expiredCert := generateIntegrationClientCert(t, fixture.ca, "expired-device", true)

	rootPool := x509.NewCertPool()
	rootPool.AddCert(fixture.ca.cert)

	certPair, err := tls.X509KeyPair(expiredCert.certPEM, expiredCert.keyPEM)
	if err != nil {
		t.Fatalf("load cert pair: %v", err)
	}

	clientConfig := &tls.Config{
		RootCAs:      rootPool,
		ServerName:   "127.0.0.1",
		Certificates: []tls.Certificate{certPair},
	}

	conn, err := tls.DialWithDialer(&net.Dialer{Timeout: time.Second}, "tcp", fixture.tlsAddr, clientConfig)
	if err == nil {
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(time.Second))
		_, _ = conn.Write([]byte{0x10, 0x02, 0x00, 0x00})
		buf := make([]byte, 10)
		_, readErr := conn.Read(buf)
		if readErr == nil {
			t.Fatal("expected handshake or read failure with expired client certificate, but succeeded")
		}
	}
}

// TestMQTTTLS_mTLS_ClientIDBinding_Rejected verifies that a trusted certificate
// is authenticated only for its configured Client ID ([SEC-04]).
// Traceability: SEC-04; docs/specs/tenancy-and-security.md §9.
func TestMQTTTLS_mTLS_ClientIDBinding_Rejected(t *testing.T) {
	certCN := "legit-device-007"
	fixture := newTLSFixture(t, tlsFixtureOptions{
		ClientAuth:   tlsadapter.ClientAuthRequire,
		MTLSIdentity: certCN,
	})
	defer fixture.close(t)

	clientCert := generateIntegrationClientCert(t, fixture.ca, certCN, false)
	t.Run("MQTT 5 mismatch rejected with 0x87", func(t *testing.T) {
		conn := fixture.dialTLS(t, &clientCert, nil)
		defer conn.Close()

		writePacket(t, conn, fixture.encoder, &mqtt.Connect{
			Version:    mqtt.ProtocolV5,
			CleanStart: true,
			ClientID:   "spoofed-client-id",
		})
		connackPacket := readPacket(t, conn, fixture.decoder, mqtt.ProtocolV5)
		connack, ok := connackPacket.(*mqtt.ConnAck)
		if !ok {
			t.Fatalf("expected ConnAck, got %#v", connackPacket)
		}
		if connack.ReasonCode != 0x87 {
			t.Fatalf("expected ReasonCode 0x87 (Not Authorized), got 0x%02X", connack.ReasonCode)
		}

		// Socket should be closed by broker
		buf := make([]byte, 1)
		_ = conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		_, err := conn.Read(buf)
		if err == nil {
			t.Fatal("expected socket to be closed after authorization failure")
		}
	})

	t.Run("MQTT 3.1.1 mismatch rejected with 0x05", func(t *testing.T) {
		conn := fixture.dialTLS(t, &clientCert, nil)
		defer conn.Close()

		writePacket(t, conn, fixture.encoder, &mqtt.Connect{
			Version:    mqtt.ProtocolV311,
			CleanStart: true,
			ClientID:   "spoofed-client-id",
		})
		connackPacket := readPacket(t, conn, fixture.decoder, mqtt.ProtocolV311)
		connack, ok := connackPacket.(*mqtt.ConnAck)
		if !ok {
			t.Fatalf("expected ConnAck, got %#v", connackPacket)
		}
		if connack.ReasonCode != 0x05 {
			t.Fatalf("expected ReasonCode 0x05 (Refused Not Authorized), got 0x%02X", connack.ReasonCode)
		}

		// Socket should be closed by broker
		buf := make([]byte, 1)
		_ = conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		_, err := conn.Read(buf)
		if err == nil {
			t.Fatal("expected socket to be closed after authorization failure")
		}
	})

	t.Run("matching ClientID accepted", func(t *testing.T) {
		conn := fixture.dialTLS(t, &clientCert, nil)
		defer conn.Close()

		writePacket(t, conn, fixture.encoder, &mqtt.Connect{
			Version:    mqtt.ProtocolV5,
			CleanStart: true,
			ClientID:   certCN,
		})
		connackPacket := readPacket(t, conn, fixture.decoder, mqtt.ProtocolV5)
		connack, ok := connackPacket.(*mqtt.ConnAck)
		if !ok || connack.ReasonCode != 0 {
			t.Fatalf("expected accepted ConnAck, got %#v", connackPacket)
		}
	})
}

// TestMQTTTLS_DualListener_TCPAndTLS verifies that a broker serving both plain TCP
// and TLS listeners delivers publications cross-transport seamlessly.
func TestMQTTTLS_DualListener_TCPAndTLS(t *testing.T) {
	fixture := newTLSFixture(t, tlsFixtureOptions{DualTCP: true})
	defer fixture.close(t)

	// TCP Client connects over plain loopback TCP
	tcpConn, err := net.DialTimeout("tcp", fixture.tcpAddr, time.Second)
	if err != nil {
		t.Fatalf("dial TCP on %s: %v", fixture.tcpAddr, err)
	}
	defer tcpConn.Close()

	writePacket(t, tcpConn, fixture.encoder, &mqtt.Connect{
		Version:    mqtt.ProtocolV5,
		CleanStart: true,
		ClientID:   "plain-tcp-subscriber",
	})
	tcpConnackPacket := readPacket(t, tcpConn, fixture.decoder, mqtt.ProtocolV5)
	if connack, ok := tcpConnackPacket.(*mqtt.ConnAck); !ok || connack.ReasonCode != 0 {
		t.Fatalf("TCP subscriber expected accepted ConnAck, got %#v", tcpConnackPacket)
	}

	crossTopic := "dual/cross/channel"
	writePacket(t, tcpConn, fixture.encoder, &mqtt.Subscribe{
		Version:  mqtt.ProtocolV5,
		PacketID: 1,
		Subscriptions: []mqtt.Subscription{
			{Filter: crossTopic, QoS: mqtt.QoSAtMostOnce},
		},
	})
	tcpSubackPacket := readPacket(t, tcpConn, fixture.decoder, mqtt.ProtocolV5)
	if suback, ok := tcpSubackPacket.(*mqtt.SubAck); !ok || len(suback.ReasonCodes) != 1 || suback.ReasonCodes[0] != 0 {
		t.Fatalf("TCP subscriber expected accepted SubAck, got %#v", tcpSubackPacket)
	}

	// TLS Client connects over encrypted TLS socket
	tlsConn := fixture.dialTLS(t, nil, nil)
	defer tlsConn.Close()

	writePacket(t, tlsConn, fixture.encoder, &mqtt.Connect{
		Version:    mqtt.ProtocolV5,
		CleanStart: true,
		ClientID:   "tls-publisher",
	})
	tlsConnackPacket := readPacket(t, tlsConn, fixture.decoder, mqtt.ProtocolV5)
	if connack, ok := tlsConnackPacket.(*mqtt.ConnAck); !ok || connack.ReasonCode != 0 {
		t.Fatalf("TLS publisher expected accepted ConnAck, got %#v", tlsConnackPacket)
	}

	// TLS Client publishes to the cross-transport topic
	messagePayload := []byte("hello from TLS to plain TCP")
	writePacket(t, tlsConn, fixture.encoder, &mqtt.Publish{
		Version: mqtt.ProtocolV5,
		Topic:   crossTopic,
		Payload: messagePayload,
		QoS:     mqtt.QoSAtMostOnce,
	})

	// TCP Client receives the message published by TLS Client
	receivedPacket := readPacket(t, tcpConn, fixture.decoder, mqtt.ProtocolV5)
	publishMsg, ok := receivedPacket.(*mqtt.Publish)
	if !ok {
		t.Fatalf("TCP subscriber expected Publish, got %#v", receivedPacket)
	}
	if string(publishMsg.Payload) != string(messagePayload) {
		t.Fatalf("TCP subscriber expected payload %q, got %q", string(messagePayload), string(publishMsg.Payload))
	}
}
