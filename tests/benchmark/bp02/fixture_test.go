package main

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestBP02FixtureSeparatesTenantsAndConfiguresTLS(t *testing.T) {
	profile := bp02SmokeProfile(t)
	fixture, err := newBP02Fixture(profile, "bp02-fixture-test")
	if err != nil {
		t.Fatal(err)
	}
	defer fixture.Close()
	directory := fixture.Directory
	readFile := func(path string) []byte {
		t.Helper()
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		return data
	}
	var security bp02SecurityConfig
	if err := json.Unmarshal(readFile(filepath.Join(fixture.NodeConfigDirectories[0], "mqtt-security.json")), &security); err != nil {
		t.Fatal(err)
	}
	if len(security.Identities) != 2 {
		t.Fatalf("identities = %d, want 2", len(security.Identities))
	}
	seen := make(map[string]string)
	for _, identity := range security.Identities {
		for _, id := range identity.AllowedClientIDs {
			if tenant, exists := seen[id]; exists {
				t.Fatalf("client %s belongs to both %s and %s", id, tenant, identity.TenantID)
			}
			seen[id] = identity.TenantID
		}
	}
	var config bp02ClusterConfig
	if err := json.Unmarshal(readFile(filepath.Join(fixture.NodeConfigDirectories[0], "node.json")), &config); err != nil {
		t.Fatal(err)
	}
	if config.MQTT.TLSListenAddress != "0.0.0.0:8883" {
		t.Fatalf("TLS listen = %q", config.MQTT.TLSListenAddress)
	}
	if config.MQTT.TLS == nil || config.MQTT.TLS.CertFile != "/config/mqtt.pem" ||
		config.MQTT.TLS.KeyFile != "/config/mqtt-key.pem" || config.MQTT.TLS.CAFile != "" ||
		config.MQTT.TLS.ClientAuth != "none" || config.MQTT.TLS.MinVersion != "1.2" {
		t.Fatalf("MQTT TLS config = %+v", config.MQTT.TLS)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(readFile(filepath.Join(fixture.EMQXConfigDirectory, "ca.pem"))) {
		t.Fatal("invalid CA")
	}
	peerCertificatePEM := readFile(filepath.Join(fixture.NodeConfigDirectories[0], "node.pem"))
	peerBlock, _ := pem.Decode(peerCertificatePEM)
	if peerBlock == nil {
		t.Fatal("missing peer certificate PEM")
	}
	peerCertificate, err := x509.ParseCertificate(peerBlock.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	mqttCertificatePEM := readFile(filepath.Join(fixture.NodeConfigDirectories[0], "mqtt.pem"))
	mqttBlock, _ := pem.Decode(mqttCertificatePEM)
	if mqttBlock == nil {
		t.Fatal("missing MQTT certificate PEM")
	}
	mqttCertificate, err := x509.ParseCertificate(mqttBlock.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(mqttCertificate.Raw, peerCertificate.Raw) ||
		bytes.Equal(mqttCertificate.RawSubjectPublicKeyInfo, peerCertificate.RawSubjectPublicKeyInfo) {
		t.Fatal("MQTT server certificate reuses peer certificate or public key")
	}
	if _, err := mqttCertificate.Verify(x509.VerifyOptions{
		DNSName: "node1.bp02.test", Roots: roots, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}); err != nil {
		t.Fatalf("verify node MQTT certificate: %v", err)
	}
	if len(mqttCertificate.ExtKeyUsage) != 1 || mqttCertificate.ExtKeyUsage[0] != x509.ExtKeyUsageServerAuth {
		t.Fatalf("MQTT certificate extended key usages = %v, want serverAuth only", mqttCertificate.ExtKeyUsage)
	}
	if _, err := tls.X509KeyPair(mqttCertificatePEM, readFile(filepath.Join(fixture.NodeConfigDirectories[0], "mqtt-key.pem"))); err != nil {
		t.Fatalf("load MQTT server certificate and key: %v", err)
	}
	if _, err := tls.X509KeyPair(peerCertificatePEM, readFile(filepath.Join(fixture.NodeConfigDirectories[0], "node-key.pem"))); err != nil {
		t.Fatalf("load peer certificate and key: %v", err)
	}
	certificatePEM := readFile(filepath.Join(fixture.EMQXConfigDirectory, "server.pem"))
	block, _ := pem.Decode(certificatePEM)
	if block == nil {
		t.Fatal("missing certificate PEM")
	}
	certificate, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	for node := range 3 {
		dns := fmt.Sprintf("node%d.bp02.test", node+1)
		if _, err := certificate.Verify(x509.VerifyOptions{DNSName: dns, Roots: roots}); err != nil {
			t.Fatalf("verify %s: %v", dns, err)
		}
	}
	if _, err := tls.X509KeyPair(certificatePEM, readFile(filepath.Join(fixture.EMQXConfigDirectory, "server-key.pem"))); err != nil {
		t.Fatal(err)
	}
	acl := string(readFile(filepath.Join(fixture.EMQXConfigDirectory, "acl.conf")))
	for _, rule := range []string{"{username, \"bp02-a\"}", "{username, \"bp02-b\"}", "{deny, all}"} {
		if !strings.Contains(acl, rule) {
			t.Fatalf("ACL missing %s", rule)
		}
	}
	if err := fixture.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(directory); !os.IsNotExist(err) {
		t.Fatalf("fixture remains after Close: %v", err)
	}
}
