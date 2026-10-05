package main

import (
	"crypto/tls"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	adaptersmetrics "narumq/internal/adapters/metrics"
	tlsadapter "narumq/internal/adapters/tls"
)

// Traceability: PRD MQTT-09/MQTT-11; docs/specs/mqtt-protocol-and-session.md §7.1.
func TestOpenMQTTListenerAppliesAdmissionInsideTLS(t *testing.T) {
	_, certificates := writePeerOnlyCertificates(t, t.TempDir())
	certificate := certificates[1]
	tlsConfig, err := tlsadapter.BuildTLSConfig(tlsadapter.Config{
		CertFile: certificate.certPath, KeyFile: certificate.keyPath, MinVersion: "1.2",
	})
	if err != nil {
		t.Fatal(err)
	}
	registry := adaptersmetrics.NewRegistry()
	listener, err := openMQTTListener("127.0.0.1:0", tlsConfig, 1, 0, registry)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	type acceptedResult struct {
		conn net.Conn
		err  error
	}
	accepted := make(chan acceptedResult, 1)
	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr == nil {
			secureConn, ok := conn.(*tls.Conn)
			if !ok {
				acceptErr = fmt.Errorf("accepted connection has type %T, want *tls.Conn", conn)
				_ = conn.Close()
			} else {
				_ = secureConn.SetDeadline(time.Now().Add(2 * time.Second))
				acceptErr = secureConn.Handshake()
			}
		}
		accepted <- acceptedResult{conn: conn, err: acceptErr}
	}()
	client, err := tls.DialWithDialer(&net.Dialer{Timeout: 2 * time.Second}, "tcp", listener.Addr().String(), &tls.Config{
		InsecureSkipVerify: true, MinVersion: tls.VersionTLS12,
	})
	if err != nil {
		t.Fatalf("TLS client handshake: %v", err)
	}
	defer client.Close()
	var serverConn net.Conn
	select {
	case result := <-accepted:
		if result.err != nil {
			t.Fatal(result.err)
		}
		serverConn = result.conn
	case <-time.After(2 * time.Second):
		t.Fatal("listener did not accept TLS connection")
	}
	defer serverConn.Close()
	acceptDone := make(chan struct{})
	defer func() {
		_ = listener.Close()
		<-acceptDone
	}()
	go func() {
		defer close(acceptDone)
		conn, _ := listener.Accept()
		if conn != nil {
			_ = conn.Close()
		}
	}()
	if _, ok := serverConn.(*tls.Conn); !ok {
		t.Fatalf("accepted connection has type %T, want *tls.Conn", serverConn)
	}

	second, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	_ = second.SetReadDeadline(time.Now().Add(2 * time.Second))
	var response [1]byte
	if count, readErr := second.Read(response[:]); count != 0 || readErr == nil {
		t.Fatalf("second raw TCP socket read = (%d, %v), want close without bytes", count, readErr)
	} else if networkErr, ok := readErr.(net.Error); ok && networkErr.Timeout() {
		t.Fatalf("second raw TCP socket timed out instead of closing: %v", readErr)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		if got := string(registry.Render(false)); strings.Contains(got, `narumq_mqtt_listener_admission_rejected_total{transport="tls",reason="max_connections"} 1`) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("TLS admission rejection metric missing from render:\n%s", registry.Render(false))
		}
		time.Sleep(time.Millisecond)
	}
}
