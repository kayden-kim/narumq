package integration

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"log/slog"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"narumq/internal/adapters/authentication"
	"narumq/internal/adapters/clock"
	"narumq/internal/adapters/memory/deliveryqueue"
	memoryretained "narumq/internal/adapters/memory/retained"
	"narumq/internal/adapters/memory/sessionstore"
	tcpadapter "narumq/internal/adapters/tcp"
	tlsadapter "narumq/internal/adapters/tls"
	"narumq/internal/application/connect"
	"narumq/internal/application/publish"
	"narumq/internal/application/subscribe"
	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/routing"
	"narumq/internal/domain/security"
	"narumq/internal/domain/tenancy"
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
	ClientAuth   tlsadapter.ClientAuthMode
	CAFile       string
	MinVersion   string
	CipherSuites []string
	MTLSIdentity string
	DualTCP      bool
}

type tlsFixture struct {
	server      *tcpadapter.Server
	tlsListener net.Listener
	tcpListener net.Listener
	tlsAddr     string
	tcpAddr     string
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
	var connectOpts []connect.Option
	if opts.MTLSIdentity != "" {
		tenantID, err := tenancy.NewTenantID("tenant-a")
		if err != nil {
			t.Fatalf("create mTLS tenant ID: %v", err)
		}
		mtlsProvider, err := authentication.NewMTLSAuthenticationProvider(systemClock, []authentication.ClientCertificateIdentityBinding{{
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
		connectOpts = append(connectOpts, connect.WithSecurity(nil, mtlsProvider, security.Policy{}))
	}
	connections, err := connect.New(systemClock, repository, deliveries, routes, clock.SystemScheduler{}, willBridge, connectOpts...)
	if err != nil {
		tlsListener.Close()
		if tcpListener != nil {
			tcpListener.Close()
		}
		t.Fatal(err)
	}
	subscriptions, err := subscribe.New(routes, deliveries, retainedRepo)
	if err != nil {
		tlsListener.Close()
		if tcpListener != nil {
			tcpListener.Close()
		}
		t.Fatal(err)
	}
	publications, err := publish.New(systemClock, routes, deliveries, retainedRepo)
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
		},
		decoder,
		encoder,
		connections,
		subscriptions,
		publications,
		deliveries,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	if err != nil {
		tlsListener.Close()
		if tcpListener != nil {
			tcpListener.Close()
		}
		t.Fatal(err)
	}
	if tcpListener != nil {
		if err := server.AddListener(tlsListener); err != nil {
			tlsListener.Close()
			tcpListener.Close()
			t.Fatal(err)
		}
	}
	willBridge.Bind(server)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx) }()

	var tcpAddr string
	if tcpListener != nil {
		tcpAddr = tcpListener.Addr().String()
	}

	return &tlsFixture{
		server:      server,
		tlsListener: tlsListener,
		tcpListener: tcpListener,
		tlsAddr:     tlsListener.Addr().String(),
		tcpAddr:     tcpAddr,
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

// TestMQTTTLS_RejectInsecureTLSVersion verifies that client connections attempting
// TLS 1.0 or TLS 1.1 are immediately rejected during the TLS handshake ([SEC-01]).
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

// TestMQTTTLS_mTLS_UntrustedCA_Rejected verifies that a client presenting a certificate
// signed by an untrusted CA is rejected during the TLS handshake ([SEC-03]).
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
